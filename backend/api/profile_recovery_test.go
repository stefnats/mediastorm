package api

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/accounts"
	profilerecovery "novastream/services/profile_recovery"
	"novastream/services/profileunlock"
	"novastream/services/user_settings"
)

type recoveryAPIFixture struct {
	f        *publicPGPinFixture
	settings *user_settings.Service
}

func newRecoveryAPI(t *testing.T) recoveryAPIFixture {
	t.Helper()
	f := newPublicPGPinFixture(t)
	settings, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	accountService, err := accounts.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	pins := profileunlock.NewPGProfilePins(f.ds, f.sessions)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: f.enforce.Load(), ProfileUnlockTTLHours: 1}
	}
	recovery := handlers.NewProfileRecoveryHandler(profilerecovery.NewService(f.ds, f.users, settings, pins), policy)
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	list.SetMetadataService(f.facts)
	list.SetMetadataHandler(meta)
	f.router = mux.NewRouter()
	Register(f.router, nil, meta,
		nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{},
		handlers.NewUsersHandler(f.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		accountService, f.sessions, f.users, nil, nil, nil, "", nil,
		WithProfileUnlocks(profileunlock.NewService(f.ds.ProfileUnlocks()), policy),
		WithPGProfilePins(pins), WithProfileRecovery(recovery))
	return recoveryAPIFixture{f: f, settings: settings}
}

func recoveryAPIRequest(f *publicPGPinFixture, id, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/api/users/"+id+"/state/reload", bytes.NewBufferString(body))
	r.Header.Set("X-Client-ID", "synthetic-pg-api-client")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func recoveryAPIFingerprint(t *testing.T, f *publicPGPinFixture) [3]string {
	t.Helper()
	var result [3]string
	err := f.ds.Pool().QueryRow(t.Context(), `SELECT
	 COALESCE((SELECT jsonb_agg(to_jsonb(u) || jsonb_build_object('physical',concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY u.id) FROM users u),'[]'::jsonb)::text,
	 COALESCE((SELECT jsonb_agg(to_jsonb(s) || jsonb_build_object('physical',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY s.user_id) FROM user_settings s),'[]'::jsonb)::text,
	 COALESCE((SELECT jsonb_agg(to_jsonb(v) ORDER BY v.user_id) FROM user_settings_versions v),'[]'::jsonb)::text`).Scan(&result[0], &result[1], &result[2])
	if err != nil {
		t.Fatal("read synthetic recovery fingerprint")
	}
	return result
}

func recoveryAPIKeys(value map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestProfileRecoveryRegisteredReadsAndAdoptsWithoutWriting(t *testing.T) {
	for _, master := range []bool{false, true} {
		name := "regular"
		if master {
			name = "master"
		}
		t.Run(name, func(t *testing.T) {
			pair := newRecoveryAPI(t)
			f := pair.f
			if master {
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			}
			token := f.session(master)
			old, ok := f.users.Get(f.open)
			if !ok {
				t.Fatal("missing original cached profile")
			}
			// Recovery reports stored facts, including values changed by another
			// writer. These booleans do not grant effective Kids-profile rights.
			f.exec(`UPDATE users SET name='Persisted recovery name',is_kids_profile=true,kids_mode='content_list',kids_max_rating='generic-value',kids_max_movie_rating='movie-value',kids_max_tv_rating='tv-value',kids_allowed_lists='null'::jsonb,allow_live_takeover=$2,allow_recording_interruption=$3,updated_at=clock_timestamp() WHERE id=$1`, f.open, !master, master)
			f.exec(`INSERT INTO user_settings(user_id,settings) VALUES($1,'{"metadata":{"primaryLanguage":"fra"},"display":{"simpleMode":false},"future":{"precise":9007199254740993}}'::jsonb)`, f.open)
			before := recoveryAPIFingerprint(t, f)
			response := recoveryAPIRequest(f, f.open, token, "", nil)
			if response.Code != 200 {
				t.Fatalf("actual registered recovery: HTTP %d", response.Code)
			}
			var root, profile, status map[string]json.RawMessage
			if json.Unmarshal(response.Body.Bytes(), &root) != nil || json.Unmarshal(root["profile"], &profile) != nil || json.Unmarshal(root["recovery"], &status) != nil {
				t.Fatal("invalid recovery DTO")
			}
			wanted := []string{"allowLiveTakeover", "allowRecordingInterruption", "id", "isKidsProfile", "kidsAllowedLists", "kidsMaxMovieRating", "kidsMaxRating", "kidsMaxTVRating", "kidsMode", "name"}
			sort.Strings(wanted)
			if !reflect.DeepEqual(recoveryAPIKeys(root), []string{"profile", "recovery"}) || !reflect.DeepEqual(recoveryAPIKeys(profile), wanted) || !reflect.DeepEqual(recoveryAPIKeys(status), []string{"settingsPendingUncertainty", "usersWriteBlocked"}) {
				t.Fatal("recovery exposed fields outside its safe DTO")
			}
			for key, expected := range map[string]string{"id": f.open, "name": "Persisted recovery name", "kidsMode": "content_list"} {
				var actual string
				if json.Unmarshal(profile[key], &actual) != nil || actual != expected {
					t.Fatal("response did not render its actual saved snapshot")
				}
			}
			if string(profile["isKidsProfile"]) != "true" || string(status["usersWriteBlocked"]) != "false" || string(status["settingsPendingUncertainty"]) != "false" {
				t.Fatal("response changed recovery status or saved profile type")
			}
			for key, want := range map[string]bool{"allowLiveTakeover": !master, "allowRecordingInterruption": master} {
				expected, err := json.Marshal(want)
				if err != nil || !bytes.Equal(profile[key], expected) {
					t.Fatal("recovery did not report the exact stored permission boolean")
				}
			}
			var lists []string
			if json.Unmarshal(profile["kidsAllowedLists"], &lists) != nil || lists == nil || len(lists) != 0 {
				t.Fatal("DTO did not project a nil stored list as an empty array")
			}
			actual, ok := f.users.Get(f.open)
			if !ok || actual.Name == old.Name || actual.Name != "Persisted recovery name" || !actual.IsKidsProfile || actual.KidsMode != "content_list" || actual.KidsMaxRating != "generic-value" || actual.KidsMaxMovieRating != "movie-value" || actual.KidsMaxTVRating != "tv-value" || actual.KidsAllowedLists != nil || actual.AllowLiveTakeover != !master || actual.AllowRecordingInterruption != master {
				t.Fatal("registered recovery failed to adopt only actual saved values")
			}
			saved, err := pair.settings.Get(f.open)
			if err != nil || saved == nil || saved.Metadata.PrimaryLanguage != "fra" || saved.Display.SimpleMode == nil || *saved.Display.SimpleMode {
				t.Fatal("settings cache was not installed with the user snapshot")
			}
			if recoveryAPIFingerprint(t, f) != before {
				t.Fatal("recovery wrote users, settings or lifecycle stamps")
			}
		})
	}
}

func TestProfileRecoveryRegisteredRejectsMutationInputs(t *testing.T) {
	for _, item := range []struct {
		name, body string
		headers    map[string]string
	}{
		{name: "object", body: `{}`},
		{name: "desired state", body: `{"isKidsProfile":false}`},
		{name: "whitespace body", body: " "},
		{name: "parent intent", headers: map[string]string{"X-Parent-Intent": "synthetic-consumed-intent"}},
		{name: "account password", headers: map[string]string{"X-Account-Password": "synthetic-api-password"}},
	} {
		t.Run(item.name, func(t *testing.T) {
			pair := newRecoveryAPI(t)
			f := pair.f
			token := f.session(false)
			old, _ := f.users.Get(f.open)
			f.exec(`UPDATE users SET name='Must remain unadopted' WHERE id=$1`, f.open)
			before := recoveryAPIFingerprint(t, f)
			response := recoveryAPIRequest(f, f.open, token, item.body, item.headers)
			var failure map[string]json.RawMessage
			if response.Code != 400 || json.Unmarshal(response.Body.Bytes(), &failure) != nil || string(failure["code"]) != `"profile_recovery_invalid"` || failure["parentIntent"] != nil {
				t.Fatal("mutation input was accepted or created a parent challenge")
			}
			actual, _ := f.users.Get(f.open)
			if !reflect.DeepEqual(old, actual) || recoveryAPIFingerprint(t, f) != before {
				t.Fatal("invalid recovery adopted or wrote state")
			}
		})
	}
}

func TestProfileRecoveryRegisteredCurrentAuthority(t *testing.T) {
	for _, kind := range []string{"missing session", "expired session", "revoked session", "expired account", "scoped session", "foreign target", "missing target"} {
		t.Run(kind, func(t *testing.T) {
			pair := newRecoveryAPI(t)
			f := pair.f
			token, id, want := f.session(false), f.open, 401
			switch kind {
			case "missing session":
				token = ""
			case "expired session":
				f.exec(`UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, token)
			case "revoked session":
				f.exec(`DELETE FROM sessions WHERE token=$1`, token)
			case "expired account":
				f.exec(`UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, f.account)
			case "scoped session":
				session, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic recovery scope", "127.0.0.1", time.Hour, models.SessionScopeStream, "synthetic-resource")
				if err != nil {
					t.Fatal(err)
				}
				token, want = session.Token, 403
			case "foreign target":
				id, want = f.foreign, 404
			case "missing target":
				id, want = "synthetic-missing-profile", 404
			}
			old, _ := f.users.Get(f.open)
			f.exec(`UPDATE users SET name='Must not pass the guard' WHERE id=$1`, f.open)
			before := recoveryAPIFingerprint(t, f)
			response := recoveryAPIRequest(f, id, token, "", nil)
			if response.Code != want {
				t.Fatalf("current authority: HTTP %d, want %d", response.Code, want)
			}
			actual, _ := f.users.Get(f.open)
			if !reflect.DeepEqual(old, actual) || recoveryAPIFingerprint(t, f) != before {
				t.Fatal("denied recovery adopted or wrote state")
			}
		})
	}
}

func TestProfileRecoveryRegisteredPreservesPINPolicy(t *testing.T) {
	pair := newRecoveryAPI(t)
	f := pair.f
	token := f.session(false)
	if response := recoveryAPIRequest(f, f.locked, token, "", nil); response.Code != 403 {
		t.Fatal("locked API target bypassed existing PIN policy")
	}
	verifiedResponse := f.do("POST", "/api/users/"+f.locked+"/pin/verify", token, "", map[string]string{"pin": "synthetic-é"})
	var verified handlers.VerifyPinResponse
	if verifiedResponse.Code != 200 || json.Unmarshal(verifiedResponse.Body.Bytes(), &verified) != nil || !verified.Valid {
		t.Fatal("actual PIN verification failed")
	}
	before := recoveryAPIFingerprint(t, f)
	if response := recoveryAPIRequest(f, f.locked, token, "", map[string]string{"X-Profile-Unlock": verified.UnlockToken}); response.Code != 200 {
		t.Fatalf("existing PIN grant rejected: HTTP %d", response.Code)
	}
	if recoveryAPIFingerprint(t, f) != before {
		t.Fatal("recovery renewed a PIN or changed its stored user state")
	}
	f.enforce.Store(false)
	if response := recoveryAPIRequest(f, f.locked, token, "", nil); response.Code != 200 {
		t.Fatal("recovery introduced a new enforcement-off rule")
	}
}
