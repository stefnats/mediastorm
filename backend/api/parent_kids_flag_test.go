package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func kidsFlagAPI(t *testing.T, initial, rights bool, mode string) *publicPGPinFixture {
	t.Helper()
	f := newPublicPGPinFixture(t)
	f.exec(`ALTER TABLE users ADD COLUMN synthetic_future_user_note TEXT NOT NULL DEFAULT 'preserve unknown'`)
	f.exec(`UPDATE users SET is_kids_profile=$2,kids_mode=$3,kids_max_rating='generic-custom',kids_max_movie_rating='movie-custom',kids_max_tv_rating='tv-custom',kids_allowed_lists='["one","one"]'::jsonb,allow_live_takeover=$4,allow_recording_interruption=$4,updated_at=clock_timestamp()-interval '1 day' WHERE id=$1`, f.open, initial, mode, rights)
	f.exec(`INSERT INTO user_settings(user_id,settings) VALUES($1,'{"display":{"simpleMode":false},"future":{"integer":9007199254740993}}'::jsonb)`, f.open)
	if err := f.users.ResyncStorage(t.Context()); err != nil {
		t.Fatal(err)
	}
	return f
}

func kidsFlagRequest(f *publicPGPinFixture, token, body, intent, password string) *http.Request {
	return parentListWritesRequest("PUT", "/api/users/"+f.open+"/kids-profile", token, body, intent, password)
}

func kidsFlagChallenge(t *testing.T, f *publicPGPinFixture, token, body string) string {
	t.Helper()
	before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
	response := parentModeAPIServe(f, kidsFlagRequest(f, token, body, "", ""))
	var challenge struct {
		Code         string `json:"code"`
		ParentIntent string `json:"parentIntent"`
	}
	if response.Code != 403 || json.Unmarshal(response.Body.Bytes(), &challenge) != nil || challenge.Code != "parent_authorization_required" || !strings.HasPrefix(challenge.ParentIntent, "pkf_") || len(challenge.ParentIntent) != 68 {
		t.Fatalf("actual flag route did not require its fresh parent challenge: HTTP %d", response.Code)
	}
	value, err := hex.DecodeString(challenge.ParentIntent[4:])
	if err != nil || len(value) != 32 || hex.EncodeToString(value) != challenge.ParentIntent[4:] {
		t.Fatal("noncanonical flag intent")
	}
	if version != parentListWritesVersion(t, f) || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("unconfirmed flag request wrote a profile")
	}
	return challenge.ParentIntent
}

func TestParentKidsFlagRegisteredCompleteSetterSemantics(t *testing.T) {
	exact := `{"isKidsProfile":true}`
	exact += strings.Repeat(" ", (16<<10)-len(exact))
	for i, item := range []struct {
		name, body, mode        string
		initial, rights, wanted bool
	}{
		{"enable empty mode", `{"isKidsProfile":true}`, "", false, true, true},
		{"enable existing mode", `{"isKidsProfile":true}`, "content_list", false, true, true},
		{"repeat enable still resets rights", `{"isKidsProfile":true}`, "content_list", true, true, true},
		{"repeat enable only timestamp", `{"isKidsProfile":true}`, "content_list", true, false, true},
		{"disable", `{"isKidsProfile":false}`, "content_list", true, false, false},
		{"repeat disable still clears restrictions", `{"isKidsProfile":false}`, "content_list", false, true, false},
		{"root null", `null`, "content_list", true, false, false},
		{"missing bool", `{}`, "content_list", true, false, false},
		{"case folding", `{"ISKIDSPROFILE":true}`, "", false, true, true},
		{"duplicate true then null", `{"isKidsProfile":true,"isKidsProfile":null}`, "", false, true, true},
		{"single decode trailing input", `{"isKidsProfile":false} {"ignored":true}`, "content_list", true, false, false},
		{"exact body limit", exact, "", false, true, true},
	} {
		t.Run(item.name, func(t *testing.T) {
			f := kidsFlagAPI(t, item.initial, item.rights, item.mode)
			master := i%2 == 0
			if master {
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			}
			token := f.session(master)
			before, other := parentModeAPIRow(t, f, f.open), parentModeAPIRow(t, f, f.foreign)
			settingsBefore := recoveryAPIFingerprint(t, f)
			version := parentListWritesVersion(t, f)
			intent := kidsFlagChallenge(t, f, token, item.body)
			response := parentModeAPIServe(f, kidsFlagRequest(f, token, item.body, intent, "synthetic-api-password"))
			if response.Code != 200 {
				t.Fatalf("confirmed flag request: HTTP %d", response.Code)
			}
			wantedMode, wantedGeneric := item.mode, "generic-custom"
			wantedLists := []string{"one", "one"}
			wantedRights := item.rights
			if item.wanted {
				wantedRights = false
				if item.mode == "" {
					wantedMode, wantedGeneric = "rating", "G"
				}
			} else {
				wantedMode, wantedGeneric, wantedLists = "", "", nil
			}
			wanted := map[string]any{"is_kids_profile": item.wanted, "kids_mode": wantedMode, "kids_max_rating": wantedGeneric, "kids_allowed_lists": wantedLists, "allow_live_takeover": wantedRights, "allow_recording_interruption": wantedRights}
			after := parentModeAPIRow(t, f, f.open)
			changedExpected := []string{"updated_at"}
			for field, value := range wanted {
				encoded, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				var stored, expected any
				if json.Unmarshal(after[field], &stored) != nil || json.Unmarshal(encoded, &expected) != nil || !reflect.DeepEqual(stored, expected) {
					t.Fatalf("actual flag field %s differs from established setter semantics", field)
				}
				if !bytes.Equal(before[field], after[field]) {
					changedExpected = append(changedExpected, field)
				}
			}
			var changed []string
			for field, value := range before {
				if !bytes.Equal(value, after[field]) {
					changed = append(changed, field)
				}
			}
			sort.Strings(changed)
			changedExpected = parentExpectedFieldsWithShareRotation(t, before, after, changedExpected)
			if !reflect.DeepEqual(changed, changedExpected) || version == parentListWritesVersion(t, f) || !reflect.DeepEqual(other, parentModeAPIRow(t, f, f.foreign)) {
				t.Fatal("flag changed unrelated user fields or missed the established timestamp write")
			}
			settingsAfter := recoveryAPIFingerprint(t, f)
			if settingsAfter[1] != settingsBefore[1] || settingsAfter[2] != settingsBefore[2] {
				t.Fatal("API flag unexpectedly applied cookie settings defaults")
			}
			profile, ok := f.users.Get(f.open)
			if !ok || profile.IsKidsProfile != item.wanted || profile.KidsMode != wantedMode || profile.KidsMaxRating != wantedGeneric || !reflect.DeepEqual(profile.KidsAllowedLists, wantedLists) || profile.AllowLiveTakeover != wantedRights || profile.AllowRecordingInterruption != wantedRights || profile.KidsMaxMovieRating != "movie-custom" || profile.KidsMaxTVRating != "tv-custom" {
				t.Fatal("known committed flag did not advance only its canonical cache state")
			}
			if replay := parentModeAPIServe(f, kidsFlagRequest(f, token, item.body, intent, "synthetic-api-password")); replay.Code != 409 || !reflect.DeepEqual(after, parentModeAPIRow(t, f, f.open)) {
				t.Fatal("consumed flag intent replayed")
			}
		})
	}
}

func TestParentKidsFlagRegisteredInvalidInputsAndOriginalBody(t *testing.T) {
	f := kidsFlagAPI(t, false, true, "")
	token := f.session(false)
	before := parentModeAPIRow(t, f, f.open)
	for _, item := range []struct {
		body string
		code int
	}{
		{`{"isKidsProfile":true,"unknown":true}`, 400},
		{`{"isKidsProfile":"true"}`, 400},
		{"", 400},
		{`{"isKidsProfile":true}` + strings.Repeat(" ", 16<<10), 413},
	} {
		response := parentModeAPIServe(f, kidsFlagRequest(f, token, item.body, "", ""))
		if response.Code != item.code || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
			t.Fatalf("invalid body changed state or status: HTTP %d", response.Code)
		}
	}
	body := `{"isKidsProfile":true}`
	intent := kidsFlagChallenge(t, f, token, body)
	wrong := parentModeAPIServe(f, kidsFlagRequest(f, token, body, intent, "synthetic-wrong-password"))
	if wrong.Code != 403 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("wrong account password changed the flag")
	}
	intent = kidsFlagChallenge(t, f, token, body)
	changed := parentModeAPIServe(f, kidsFlagRequest(f, token, `{"isKidsProfile":false}`, intent, "synthetic-api-password"))
	if changed.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("original flag body was not bound")
	}
	intent = kidsFlagChallenge(t, f, token, body)
	changed = parentModeAPIServe(f, kidsFlagRequest(f, token, body+" ", intent, "synthetic-api-password"))
	if changed.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("semantically identical replacement bytes bypassed original body binding")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	parentModeAPIServe(f, kidsFlagRequest(f, token, body, "", "").WithContext(ctx))
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("already canceled flag request wrote state")
	}
}
