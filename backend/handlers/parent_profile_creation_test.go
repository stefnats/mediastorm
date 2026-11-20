package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/user_settings"
	"novastream/services/users"
)

func creationCookieFixture(t *testing.T, master bool) *kidsCookieFixture {
	t.Helper()
	f := newKidsCookieFixture(t, master, false, "", "absent")
	for _, prefix := range []string{"/admin", "/account"} {
		f.router.HandleFunc(prefix+"/api/profiles", f.admin.RequireAuth(f.admin.CreateProfile)).Methods(http.MethodPost)
	}
	return f
}
func creationCookieRequest(f *kidsCookieFixture, prefix, body string, intent profileunlock.ParentProfileCreationChallengeBundle, password string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.test"+prefix+"/api/profiles", strings.NewReader(body))
	r.Header.Set("Origin", "http://synthetic.test")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.base.session.Token})
	if intent.ParentIntent != "" {
		r.Header.Set("X-Parent-Intent", intent.ParentIntent)
		r.Header.Set("X-Profile-Creation-Operation", intent.CreationOperation)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}
func creationCookieCall(f *kidsCookieFixture, prefix, body string, intent profileunlock.ParentProfileCreationChallengeBundle, password string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, creationCookieRequest(f, prefix, body, intent, password))
	return w
}
func creationCookieRows(t *testing.T, f *kidsCookieFixture) []byte {
	t.Helper()
	var raw []byte
	err := f.base.store.Pool().QueryRow(t.Context(), `SELECT jsonb_build_object('users',(SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM users u),'settings',(SELECT jsonb_agg(to_jsonb(s) ORDER BY user_id) FROM user_settings s),'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY user_id) FROM user_settings_versions v))`).Scan(&raw)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func creationCookieChallenge(t *testing.T, f *kidsCookieFixture, prefix, body string) profileunlock.ParentProfileCreationChallengeBundle {
	t.Helper()
	before := creationCookieRows(t, f)
	w := creationCookieCall(f, prefix, body, profileunlock.ParentProfileCreationChallengeBundle{}, "")
	var value struct{ Code, ParentIntent, CreationOperation string }
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || !strings.HasPrefix(value.ParentIntent, "pcc_") || len(value.ParentIntent) != 68 || !strings.HasPrefix(value.CreationOperation, "pcr_") || len(value.CreationOperation) != 68 {
		t.Fatal("missing typed creation challenge", w.Code)
	}
	if !bytes.Equal(before, creationCookieRows(t, f)) {
		t.Fatal("challenge changed profile/settings")
	}
	return profileunlock.ParentProfileCreationChallengeBundle{ParentIntent: value.ParentIntent, CreationOperation: value.CreationOperation}
}
func TestParentProfileCreationCookieActualDelegationRolesAndNoFollowup(t *testing.T) {
	for _, master := range []bool{false, true} {
		for _, prefix := range []string{"/account", "/admin"} {
			t.Run(fmt.Sprintf("%s/master=%t", prefix, master), func(t *testing.T) {
				f := creationCookieFixture(t, master)
				body := `{"name":" Cookie Child ","color":" ","accountId":"actor","isKidsProfile":true,"future":"kept decode policy"}`
				token := creationCookieChallenge(t, f, prefix, body)
				w := creationCookieCall(f, prefix, body, token, " \t synthetic-parent-password \n ")
				var result ProfileWithPinStatus
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil {
					t.Fatal("actual cookie creation failed", w.Code)
				}
				if result.Name != "Cookie Child" || result.Color != "" || result.AccountID != "actor" || !result.IsKidsProfile || result.KidsMode != "rating" || result.KidsMaxRating != "G" || result.HasPin || result.HasIcon || result.AllowLiveTakeover || result.AllowRecordingInterruption || result.ActivityPrivacy != models.ActivityPrivacySharedAnonymous {
					t.Fatal("safe complete creation projection wrong")
				}
				saved, err := f.base.store.Users().Get(t.Context(), result.ID)
				cached, ok := f.base.users.Get(result.ID)
				settings, settingsErr := f.settings.Get(result.ID)
				if err != nil || !ok || saved == nil || saved.ID != cached.ID || settingsErr != nil || settings == nil || settings.Display.SimpleMode == nil || !*settings.Display.SimpleMode {
					t.Fatal("SQL and cache pair mismatch", err, settingsErr)
				}
				before := creationCookieRows(t, f)
				if replay := creationCookieCall(f, prefix, body, token, "synthetic-parent-password"); replay.Code != 409 {
					t.Fatal("spent create intent replay", replay.Code)
				}
				if !bytes.Equal(before, creationCookieRows(t, f)) {
					t.Fatal("replay created another profile")
				}
			})
		}
	}
}
func TestParentProfileCreationCookieNoAdultDowngradeOrRequestAdoption(t *testing.T) {
	for _, kind := range []string{"false", "missing Kids", "empty name", "body spacing", "alias", "raw query", "cookie", "cross action", "header repeated"} {
		t.Run(kind, func(t *testing.T) {
			f := creationCookieFixture(t, false)
			body := `{"name":"Child","isKidsProfile":true}`
			token := creationCookieChallenge(t, f, "/account", body)
			prefix := "/account"
			switch kind {
			case "false":
				body = `{"name":"Child","isKidsProfile":false}`
			case "missing Kids":
				body = `{"name":"Child"}`
			case "empty name":
				body = `{"name":"","isKidsProfile":true}`
			case "body spacing":
				body += " "
			case "alias":
				prefix = "/admin"
			case "cross action":
				token.ParentIntent = "pkf_" + token.ParentIntent[4:]
			}
			r := creationCookieRequest(f, prefix, body, token, "synthetic-parent-password")
			switch kind {
			case "raw query":
				r.URL.RawQuery = "x=1"
			case "cookie":
				r.Header.Set("Cookie", adminSessionCookieName+"=SYNTHETIC_OTHER_COOKIE")
			case "header repeated":
				r.Header.Add("X-Parent-Intent", token.ParentIntent)
			}
			before := creationCookieRows(t, f)
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, r)
			if w.Code == 200 {
				t.Fatal("changed action adopted or downgraded", kind)
			}
			if !bytes.Equal(before, creationCookieRows(t, f)) {
				t.Fatal("failed bound creation wrote profile/settings")
			}
		})
	}
}
func TestParentProfileCreationCookieExistingBodyOriginAndParentRefusals(t *testing.T) {
	for _, kind := range []string{"trailing", "wrong origin", "cross-site", "no cookie", "body cap", "wrong password"} {
		t.Run(kind, func(t *testing.T) {
			f := creationCookieFixture(t, false)
			body := `{"name":"Child","isKidsProfile":true}`
			if kind == "trailing" {
				body += ` {"second":true}`
			}
			if kind == "body cap" {
				body = `{"name":"Child","isKidsProfile":true,"padding":"` + strings.Repeat("x", 17000) + `"}`
			}
			token := profileunlock.ParentProfileCreationChallengeBundle{}
			password := "synthetic-parent-password"
			if kind == "wrong password" || kind == "wrong origin" || kind == "cross-site" || kind == "no cookie" {
				token = creationCookieChallenge(t, f, "/account", body)
			}
			if kind == "wrong password" {
				password = "incorrect synthetic password"
			}
			r := creationCookieRequest(f, "/account", body, token, password)
			switch kind {
			case "wrong origin":
				r.Header.Set("Origin", "http://other.invalid")
			case "cross-site":
				r.Header.Set("Sec-Fetch-Site", "cross-site")
			case "no cookie":
				r.Header.Del("Cookie")
			}
			before := creationCookieRows(t, f)
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, r)
			if w.Code == 200 {
				t.Fatal("original cookie/body/parent refusal missing", kind)
			}
			if (kind == "wrong origin" || kind == "cross-site") && w.Code != http.StatusForbidden {
				t.Fatal("cookie origin denial must be forbidden", kind, w.Code)
			}
			if kind == "trailing" && w.Code != 400 {
				t.Fatal("whole Cookie-body boundary changed", w.Code)
			}
			if kind == "body cap" && w.Code != 413 {
				t.Fatal("PG-specific body bound missing", w.Code)
			}
			if !bytes.Equal(before, creationCookieRows(t, f)) {
				t.Fatal("denial wrote target")
			}
		})
	}
	for _, prefix := range []string{"/account", "/admin"} {
		f := creationCookieFixture(t, false)
		for _, originCase := range []struct {
			name    string
			origins []string
			sites   []string
		}{
			{"missing", nil, []string{"same-origin"}},
			{"empty", []string{""}, []string{"same-origin"}},
			{"opaque", []string{"null"}, []string{"same-origin"}},
			{"path", []string{"http://synthetic.test/"}, []string{"same-origin"}},
			{"query", []string{"http://synthetic.test?"}, []string{"same-origin"}},
			{"fragment", []string{"http://synthetic.test#"}, []string{"same-origin"}},
			{"userinfo", []string{"http://synthetic@synthetic.test"}, []string{"same-origin"}},
			{"suffix", []string{"http://synthetic.test.other.invalid"}, []string{"same-origin"}},
			{"port", []string{"http://synthetic.test:81"}, []string{"same-origin"}},
			{"scheme", []string{"https://synthetic.test"}, []string{"same-origin"}},
			{"duplicate origin", []string{"http://synthetic.test", "http://synthetic.test"}, []string{"same-origin"}},
			{"duplicate site", []string{"http://synthetic.test"}, []string{"same-origin", "same-origin"}},
		} {
			body := fmt.Sprintf(`{"name":"Origin-bound child %s","isKidsProfile":true}`, originCase.name)
			intent := creationCookieChallenge(t, f, prefix, body)
			before := creationCookieRows(t, f)
			r := creationCookieRequest(f, prefix, body, intent, "synthetic-parent-password")
			r.Header.Del("Origin")
			r.Header.Del("Sec-Fetch-Site")
			for _, value := range originCase.origins {
				r.Header.Add("Origin", value)
			}
			for _, value := range originCase.sites {
				r.Header.Add("Sec-Fetch-Site", value)
			}
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, r)
			if w.Code != http.StatusForbidden || !bytes.Equal(before, creationCookieRows(t, f)) {
				t.Fatal("invalid cookie origin changed profiles/settings", originCase.name, w.Code)
			}
			// Origin refusal precedes the one-shot Claim: the exact original intent
			// remains usable with its original cookie/body and genuine password.
			retry := creationCookieRequest(f, prefix, body, intent, "synthetic-parent-password")
			retry.Header.Set("Origin", "http://SYNTHETIC.test:80")
			retry.Header.Del("Sec-Fetch-Site")
			accepted := httptest.NewRecorder()
			f.router.ServeHTTP(accepted, retry)
			if accepted.Code != http.StatusOK {
				t.Fatal("origin refusal consumed original intent or changed default-port policy", originCase.name, accepted.Code)
			}
		}
	}
}
func TestParentProfileCreationCookieAdultAndJSONRemainLegacy(t *testing.T) {
	for _, pg := range []bool{true, false} {
		t.Run(fmt.Sprintf("pg=%t", pg), func(t *testing.T) {
			f := creationCookieFixture(t, true)
			password := "synthetic-parent-password"
			if !pg {
				f.admin.SetPGProfilePins(nil)
				f.shared.SetPGProfilePins(nil)
			}
			body := `{"name":"Legacy adult","accountId":"actor","isKidsProfile":false}`
			before := creationCookieRows(t, f)
			for _, missingOrWrong := range []string{"", "synthetic-wrong-parent-password"} {
				denied := creationCookieCall(f, "/admin", body, profileunlock.ParentProfileCreationChallengeBundle{}, missingOrWrong)
				if denied.Code != http.StatusForbidden || !bytes.Equal(before, creationCookieRows(t, f)) {
					t.Fatal("unconfirmed adult creation changed profiles/settings")
				}
			}
			w := creationCookieCall(f, "/admin", body, profileunlock.ParentProfileCreationChallengeBundle{}, password)
			if w.Code != 200 {
				t.Fatal("adult no-intent compatibility changed", w.Code)
			}
			var value ProfileWithPinStatus
			if json.Unmarshal(w.Body.Bytes(), &value) != nil || value.IsKidsProfile || value.Name != "Legacy adult" {
				t.Fatal("adult control wrong")
			}
			// This deliberately tests legacy selection in a real PG fixture. It is not
			// a JSON filesystem transaction or a new adult password policy claim.
		})
	}
}

func TestParentProfileCreationActualJSONLegacyMasterKeepsSingleDecoder(t *testing.T) {
	kidsCookieNoHTTP(t)
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-json-creation-parent")
	dir := t.TempDir()
	as, err := accounts.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := as.GetMasterAccount()
	if !ok {
		t.Fatal("synthetic master missing")
	}
	profiles, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := user_settings.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	authSessions, err := sessions.NewService(filepath.Join(dir, "sessions"), 0)
	if err != nil {
		t.Fatal(err)
	}
	session, err := authSessions.Create(master.ID, true, "synthetic-json", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler("", "", nil, profiles, settings, nil)
	admin.SetAccountsService(as)
	admin.SetSessionsService(authSessions)
	admin.SetParentKidsHandler(NewUsersHandler(profiles, as))
	body := `{"name":" Legacy JSON child ","accountId":"` + master.ID + `","isKidsProfile":true,"unknown":"permitted"} {"second":"ignored by legacy decoder"}`
	rows := func() []byte {
		state := map[string]any{}
		for _, user := range profiles.List() {
			preferences, err := settings.Get(user.ID)
			if err != nil {
				t.Fatal(err)
			}
			state[user.ID] = []any{user, preferences}
		}
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := rows()
	for _, password := range []string{"", "synthetic-wrong-json-parent"} {
		denied := httptest.NewRequest(http.MethodPost, "http://synthetic.test/account/api/profiles", strings.NewReader(body))
		denied.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: session.Token})
		denied.Header.Set("X-Account-Password", password)
		w := httptest.NewRecorder()
		admin.RequireAuth(admin.CreateProfile)(w, denied)
		if w.Code != http.StatusForbidden || !bytes.Equal(before, rows()) {
			t.Fatal("unconfirmed JSON master created profile/defaults")
		}
	}
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.test/account/api/profiles", strings.NewReader(body))
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: session.Token})
	r.Header.Set("X-Account-Password", "synthetic-json-creation-parent")
	w := httptest.NewRecorder()
	admin.RequireAuth(admin.CreateProfile)(w, r)
	var response ProfileWithPinStatus
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || strings.Contains(w.Body.String(), "parentIntent") || !response.IsKidsProfile {
		t.Fatal("actual JSON master creation policy/decoder changed", w.Code)
	}
	current, ok := profiles.Get(response.ID)
	pref, err := settings.Get(response.ID)
	if !ok || !current.IsKidsProfile || err != nil || pref == nil || pref.Display.SimpleMode == nil || !*pref.Display.SimpleMode {
		t.Fatal("legacy JSON setter/default chain changed", err)
	}
}
