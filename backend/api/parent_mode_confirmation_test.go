package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The existing fixture supplies actual API registration, migrated private PG
// schema and store-backed services. Its deny transport precedes constructors.
func parentModeAPIFixture(t *testing.T) *publicPGPinFixture {
	t.Helper()
	f := newPublicPGPinFixture(t)
	if _, err := f.users.SetKidsProfile(f.open, true); err != nil {
		t.Fatal(err)
	}
	if _, err := f.users.SetKidsMode(f.open, "rating"); err != nil {
		t.Fatal(err)
	}
	return f
}

func parentModeAPIPath(profile string) string {
	return "/api/users/" + profile + "/kids/mode"
}

func parentModeAPIRequest(t *testing.T, method, path, token, mode, intent, password string) *http.Request {
	t.Helper()
	body, err := json.Marshal(map[string]string{"mode": mode})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Client-ID", "synthetic-pg-api-client")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if intent != "" {
		r.Header.Set("X-Parent-Intent", intent)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}

func parentModeAPIServe(f *publicPGPinFixture, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func parentModeAPIChallenge(t *testing.T, f *publicPGPinFixture, token, mode string) string {
	t.Helper()
	w := parentModeAPIServe(f, parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, mode, "", ""))
	var value struct {
		Code         string `json:"code"`
		ParentIntent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || !strings.HasPrefix(value.ParentIntent, "pmi_") || len(value.ParentIntent) != 68 {
		t.Fatalf("actual registered mode route did not issue a bounded challenge: HTTP %d", w.Code)
	}
	decoded, err := hex.DecodeString(value.ParentIntent[4:])
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value.ParentIntent[4:] {
		t.Fatal("parent challenge token was not canonical")
	}
	return value.ParentIntent
}

func parentModeAPIRow(t *testing.T, f *publicPGPinFixture, profile string) map[string]json.RawMessage {
	t.Helper()
	var raw []byte
	if err := f.ds.Pool().QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id=$1`, profile).Scan(&raw); err != nil {
		t.Fatal("read own mode fixture row")
	}
	var row map[string]json.RawMessage
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}

func TestParentModeRegisteredOriginalConfirmationAndNarrowCommit(t *testing.T) {
	for _, item := range []struct {
		name, mode string
		master     bool
	}{{"regular content list", "content_list", false}, {"master clear", "", true}, {"regular rating", "rating", false}} {
		t.Run(item.name, func(t *testing.T) {
			f := parentModeAPIFixture(t)
			if item.mode == "rating" {
				if _, err := f.users.SetKidsMode(f.open, "content_list"); err != nil {
					t.Fatal(err)
				}
			}
			if item.master {
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			}
			token := f.session(false)
			before := parentModeAPIRow(t, f, f.open)
			other := parentModeAPIRow(t, f, f.foreign)
			intent := parentModeAPIChallenge(t, f, token, item.mode)
			if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
				t.Fatal("first403 changed the target profile")
			}
			w := parentModeAPIServe(f, parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, item.mode, intent, "synthetic-api-password"))
			if w.Code != 200 {
				t.Fatalf("original confirmed mode change failed: HTTP %d", w.Code)
			}
			after := parentModeAPIRow(t, f, f.open)
			var changed []string
			for key, value := range before {
				if !bytes.Equal(value, after[key]) {
					changed = append(changed, key)
				}
			}
			sort.Strings(changed)
			wantedFields := parentExpectedFieldsWithShareRotation(t, before, after, []string{"kids_mode", "updated_at"})
			if !reflect.DeepEqual(changed, wantedFields) || !reflect.DeepEqual(other, parentModeAPIRow(t, f, f.foreign)) {
				t.Fatal("confirmed mode mutation changed another field or profile")
			}
			profile, ok := f.users.Get(f.open)
			if !ok || profile.KidsMode != item.mode {
				t.Fatal("known own mode revision was not acknowledged in the service")
			}
			replay := parentModeAPIServe(f, parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, item.mode, intent, "synthetic-api-password"))
			if replay.Code != 409 || !reflect.DeepEqual(after, parentModeAPIRow(t, f, f.open)) {
				t.Fatal("consumed challenge was replayed")
			}
		})
	}
}

func TestParentModeRegisteredRetryNeverAdoptsChangedContext(t *testing.T) {
	for _, mode := range []string{"desired mode", "target path", "session", "stored client", "actor role", "actual refresh", "duplicate header", "invalid token"} {
		t.Run(mode, func(t *testing.T) {
			f := parentModeAPIFixture(t)
			token := f.session(false)
			intent := parentModeAPIChallenge(t, f, token, "content_list")
			path, desired := parentModeAPIPath(f.open), "content_list"
			switch mode {
			case "desired mode":
				desired = ""
			case "target path":
				other, err := f.users.CreateForAccount(f.account, "Synthetic alternate target")
				if err != nil {
					t.Fatal(err)
				}
				path = parentModeAPIPath(other.ID)
			case "session":
				token = f.session(false)
			case "stored client":
				f.exec(`UPDATE sessions SET client_id='synthetic-replacement-client' WHERE token=$1`, token)
			case "actor role":
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			case "actual refresh":
				response := f.do("POST", "/api/auth/refresh", token, "", nil)
				var value struct {
					Token string `json:"token"`
				}
				if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &value) != nil || value.Token == "" || value.Token == token {
					t.Fatal("actual registered refresh did not rotate its session")
				}
				token = value.Token
			case "invalid token":
				intent = "pmi_" + strings.Repeat("0", 64)
			}
			r := parentModeAPIRequest(t, "PUT", path, token, desired, intent, "synthetic-api-password")
			if mode == "duplicate header" {
				r.Header.Add("X-Parent-Intent", intent)
			}
			before := parentModeAPIRow(t, f, f.open)
			w := parentModeAPIServe(f, r)
			var response struct {
				Code string `json:"code"`
			}
			if w.Code != 409 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Code != "parent_confirmation_changed" || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
				t.Fatalf("retry adopted changed %s context: HTTP %d", mode, w.Code)
			}
		})
	}
}

func TestParentModeRegisteredWrongPasswordRetiresTheIntent(t *testing.T) {
	f := parentModeAPIFixture(t)
	token := f.session(false)
	intent := parentModeAPIChallenge(t, f, token, "content_list")
	before := parentModeAPIRow(t, f, f.open)
	w := parentModeAPIServe(f, parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, "content_list", intent, "synthetic-wrong-password"))
	if w.Code != 403 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("wrong parent password changed mode")
	}
	replay := parentModeAPIServe(f, parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, "content_list", intent, "synthetic-api-password"))
	if replay.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("a failed attempt could reuse its consumed confirmation")
	}
}

func TestParentModeRegisteredCancellationAndInputBoundaries(t *testing.T) {
	f := parentModeAPIFixture(t)
	token := f.session(false)
	intent := parentModeAPIChallenge(t, f, token, "content_list")
	before := parentModeAPIRow(t, f, f.open)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, "content_list", intent, "synthetic-api-password").WithContext(ctx)
	parentModeAPIServe(f, r)
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("an already canceled request performed the mode mutation")
	}
	for _, item := range []struct {
		path, mode string
		want       int
	}{
		{parentModeAPIPath(f.foreign), "content_list", 404},
		{parentModeAPIPath("synthetic-missing-profile"), "content_list", 404},
		{parentModeAPIPath(f.open), "both", 400},
	} {
		w := parentModeAPIServe(f, parentModeAPIRequest(t, "PUT", item.path, token, item.mode, "", ""))
		if w.Code != item.want || strings.Contains(w.Body.String(), "parentIntent") || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
			t.Fatal("invalid or unowned mode request acquired confirmation or changed the target")
		}
	}
	w := parentModeAPIServe(f, parentModeAPIRequest(t, "POST", parentModeAPIPath(f.open), token, "content_list", "", ""))
	if w.Code != 404 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatalf("the established nested PUT-only route boundary changed: HTTP %d", w.Code)
	}
}
