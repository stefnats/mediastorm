package handlers

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/mux"
	"golang.org/x/crypto/bcrypt"
	"novastream/services/accounts"
	"novastream/services/sessions"
	"novastream/services/user_settings"
	"novastream/services/users"
)

type kidsCookieDenyHTTP struct{ calls atomic.Int32 }

func (d *kidsCookieDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("HTTP denied in the synthetic flag fixture")
}
func kidsCookieNoHTTP(t *testing.T) {
	t.Helper()
	before := http.DefaultTransport
	denied := &kidsCookieDenyHTTP{}
	http.DefaultTransport = denied
	t.Cleanup(func() {
		http.DefaultTransport = before
		if denied.calls.Load() != 0 {
			t.Error("unexpected outgoing HTTP")
		}
	})
}

type kidsCookieFixture struct {
	base          *publicPinCarrierFixture
	settings      *user_settings.Service
	shared        *UsersHandler
	admin         *AdminUIHandler
	router        *mux.Router
	settingsTable string
}

// Actual original-cookie authentication and AdminUI delegation. The package-main
// test separately consumes Main's real shared wiring and registration helpers.
func newKidsCookieFixture(t *testing.T, master, enabled bool, mode, simple string) *kidsCookieFixture {
	t.Helper()
	kidsCookieNoHTTP(t)
	f := &kidsCookieFixture{base: newPublicPinCarrierFixture(t), settingsTable: "user_settings"}
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic-child-pin"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.base.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=$1 WHERE id='actor'`, master); err != nil {
		t.Fatal(err)
	}
	if _, err = f.base.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=$2,kids_mode=$3,kids_max_rating='legacy-generic',kids_max_movie_rating='separate-movie',kids_max_tv_rating='separate-tv',kids_allowed_lists='["https://example.invalid/kept"]',allow_share_links=true,allow_live_takeover=true,allow_recording_interruption=true,pin_hash=$4,pin_length=19 WHERE id=$1`, f.base.profile.ID, enabled, mode, string(hash)); err != nil {
		t.Fatal(err)
	}
	if simple != "absent" {
		raw := `{"futureRoot":{"integer":9007199254740993},"display":{"futureNumber":9007199254740995}}`
		if simple != "nil" {
			raw = `{"futureRoot":{"integer":9007199254740993},"display":{"simpleMode":` + simple + `,"futureNumber":9007199254740995}}`
		}
		if _, err = f.base.store.Pool().Exec(t.Context(), `INSERT INTO user_settings(user_id,settings) VALUES($1,$2)`, f.base.profile.ID, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	f.base.users, err = users.NewServiceWithStore(f.base.store, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.settings, err = user_settings.NewServiceWithStore(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-cookie-flag-master")
	actual, err := accounts.NewServiceWithStore(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	f.shared = NewUsersHandler(f.base.users, actual)
	f.shared.SetPGProfilePins(f.base.pins)
	f.admin = NewAdminUIHandler("", "", nil, f.base.users, f.settings, nil)
	f.admin.SetAccountsService(actual)
	f.admin.SetSessionsService(f.base.sessions)
	f.admin.SetPGProfilePins(f.base.pins)
	f.admin.SetParentKidsHandler(f.shared)
	f.router = mux.NewRouter()
	for _, prefix := range []string{"/admin", "/account"} {
		f.router.HandleFunc(prefix+"/api/profiles/kids", f.admin.RequireAuth(f.admin.SetKidsProfile)).Methods("PUT")
	}
	return f
}

func (f *kidsCookieFixture) request(t *testing.T, prefix, query, body, intent, password string) *http.Request {
	t.Helper()
	if query == "" {
		query = "profileId=" + f.base.profile.ID
	}
	r := httptest.NewRequest("PUT", "http://synthetic.test"+prefix+"/api/profiles/kids?"+query, strings.NewReader(body))
	r.Header.Set("Origin", "http://synthetic.test")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.base.session.Token})
	if intent != "" {
		r.Header.Set("X-Parent-Intent", intent)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}
func (f *kidsCookieFixture) call(t *testing.T, prefix, query, body, intent, password string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, f.request(t, prefix, query, body, intent, password))
	return w
}
func (f *kidsCookieFixture) row(t *testing.T) (map[string]json.RawMessage, string) {
	t.Helper()
	var raw []byte
	var version string
	if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT to_jsonb(u),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM users u WHERE id=$1`, f.base.profile.ID).Scan(&raw, &version); err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil {
		t.Fatal("decode synthetic user")
	}
	return value, version
}
func (f *kidsCookieFixture) settingsRow(t *testing.T) ([]byte, string) {
	t.Helper()
	var raw []byte
	var version string
	// The only alternate name is the exact own-schema relation renamed below.
	table := "user_settings"
	if f.settingsTable == "synthetic_flag_hidden_settings" {
		table = f.settingsTable
	}
	err := f.base.store.Pool().QueryRow(t.Context(), `SELECT s.settings,concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text) FROM users u LEFT JOIN `+table+` s ON s.user_id=u.id WHERE u.id=$1`, f.base.profile.ID).Scan(&raw, &version)
	if err != nil {
		t.Fatal("read own synthetic settings", err)
	}
	return raw, version
}
func (f *kidsCookieFixture) challenge(t *testing.T, prefix, query, body string) string {
	t.Helper()
	before, version := f.row(t)
	raw, settingsVersion := f.settingsRow(t)
	w := f.call(t, prefix, query, body, "", "")
	var value struct {
		Code         string
		ParentIntent string
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || len(value.ParentIntent) != 68 || !strings.HasPrefix(value.ParentIntent, "pkf_") {
		t.Fatal("missing fresh typed cookie challenge", w.Code)
	}
	token, err := hex.DecodeString(value.ParentIntent[4:])
	if err != nil || len(token) != 32 || hex.EncodeToString(token) != value.ParentIntent[4:] {
		t.Fatal("noncanonical flag intent")
	}
	now, current := f.row(t)
	s, sv := f.settingsRow(t)
	if !reflect.DeepEqual(before, now) || version != current || !bytes.Equal(raw, s) || sv != settingsVersion {
		t.Fatal("challenge changed profile/defaults")
	}
	return value.ParentIntent
}

func TestParentKidsFlagCookieFullSemanticsAndAtomicDefaults(t *testing.T) {
	cases := []struct {
		name, body, mode, simple string
		before, wanted           bool
	}{
		{"absent defaults", `{"isKidsProfile":true}`, "", "absent", false, true},
		{"nil defaults", `{"isKidsProfile":true}`, "", "nil", false, true},
		{"explicit false", `{"isKidsProfile":true}`, "content_list", "false", false, true},
		{"explicit true repeated", `{"isKidsProfile":true}`, "rating", "true", true, true},
		{"repeat true empty mode", `{"isKidsProfile":true}`, "", "false", true, true},
		{"disable", `{"isKidsProfile":false}`, "content_list", "false", true, false},
		{"repeat false", `{"isKidsProfile":false}`, "rating", "true", false, false},
		{"missing bool", `{}`, "content_list", "nil", true, false},
		{"root null", `null`, "rating", "absent", true, false},
		{"duplicate null retains bool", `{"isKidsProfile":true,"ISKIDSPROFILE":null}`, "", "nil", false, true},
		{"unknown and duplicate accepted", `{"unknown":{"x":1},"isKidsProfile":null,"isKidsProfile":true}`, "", "false", false, true},
	}
	exactBody := `{"isKidsProfile":true}`
	exactBody += strings.Repeat(" ", (16<<10)-len(exactBody))
	cases = append(cases, struct {
		name, body, mode, simple string
		before, wanted           bool
	}{"exact body cap", exactBody, "", "false", false, true})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, master := range []bool{false, true} {
				t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
					f := newKidsCookieFixture(t, master, tc.before, tc.mode, tc.simple)
					prefix := "/account"
					if master {
						prefix = "/admin"
					}
					before, version := f.row(t)
					oldSettings, settingsVersion := f.settingsRow(t)
					intent := f.challenge(t, prefix, "", tc.body)
					w := f.call(t, prefix, "", tc.body, intent, "synthetic-parent-password")
					if w.Code != 200 {
						t.Fatal("confirmed cookie flag failed", w.Code)
					}
					after, currentVersion := f.row(t)
					expected := make(map[string]json.RawMessage, len(before))
					for key, value := range before {
						expected[key] = value
					}
					expected["is_kids_profile"], _ = json.Marshal(tc.wanted)
					if tc.wanted {
						expected["allow_live_takeover"] = json.RawMessage("false")
						expected["allow_recording_interruption"] = json.RawMessage("false")
						if tc.mode == "" {
							expected["kids_mode"] = json.RawMessage(`"rating"`)
							expected["kids_max_rating"] = json.RawMessage(`"G"`)
						}
					} else {
						expected["kids_mode"] = json.RawMessage(`""`)
						expected["kids_max_rating"] = json.RawMessage(`""`)
						expected["kids_allowed_lists"] = json.RawMessage("null")
					}
					if version == currentVersion || bytes.Equal(before["updated_at"], after["updated_at"]) {
						t.Fatal("equal-value flag skipped its established UPDATE")
					}
					expected["updated_at"] = after["updated_at"]
					parentExpectedCookieShareRotation(t, before, after, expected)
					if !reflect.DeepEqual(expected, after) {
						t.Fatal("cookie flag changed an unrelated field or missed exact defaults")
					}
					settings, sv := f.settingsRow(t)
					if !tc.wanted {
						if !bytes.Equal(oldSettings, settings) || sv != settingsVersion {
							t.Fatal("disable changed settings")
						}
					} else {
						var object map[string]json.RawMessage
						var display map[string]json.RawMessage
						if json.Unmarshal(settings, &object) != nil || json.Unmarshal(object["display"], &display) != nil {
							t.Fatal("missing confirmed defaults")
						}
						wantSimple := "true"
						if tc.simple == "false" {
							wantSimple = "false"
						}
						if string(display["simpleMode"]) != wantSimple {
							t.Fatal("SimpleMode default overrode explicit false")
						}
						if tc.simple != "absent" {
							var old map[string]json.RawMessage
							_ = json.Unmarshal(oldSettings, &old)
							if !bytes.Equal(old["futureRoot"], object["futureRoot"]) || string(display["futureNumber"]) != "9007199254740995" {
								t.Fatal("unknown exact JSON values changed")
							}
						}
					}
					stored, err := f.base.store.Users().Get(t.Context(), f.base.profile.ID)
					if err != nil {
						t.Fatal(err)
					}
					cached, ok := f.base.users.Get(f.base.profile.ID)
					cachedJSON, _ := json.Marshal(cached)
					storedJSON, _ := json.Marshal(stored)
					if !ok || !bytes.Equal(cachedJSON, storedJSON) || cached.PinHash != stored.PinHash || cached.PinLength != stored.PinLength || !cached.UpdatedAt.Equal(stored.UpdatedAt) || !reflect.DeepEqual(cached.KidsAllowedLists, stored.KidsAllowedLists) {
						t.Fatal("User cache ACK incomplete")
					}
					actualSettings, err := f.base.store.UserSettings().Get(t.Context(), f.base.profile.ID)
					if err != nil {
						t.Fatal(err)
					}
					cachedSettings, err := f.settings.Get(f.base.profile.ID)
					if err != nil || !reflect.DeepEqual(actualSettings, cachedSettings) {
						t.Fatal("Settings cache ACK incomplete")
					}
					var dto ProfileWithPinStatus
					if json.Unmarshal(w.Body.Bytes(), &dto) != nil || dto.ID != stored.ID || dto.IsKidsProfile != stored.IsKidsProfile || dto.AllowLiveTakeover != stored.AllowLiveTakeover || dto.AllowRecordingInterruption != stored.AllowRecordingInterruption {
						t.Fatal("cookie response guessed rights or flag")
					}
				})
			}
		})
	}
}

// The management decoder keeps its established one-Decode semantics, but
// the actual PG Cookie ownership middleware validates an entire JSON object
// before delegation. A second JSON value is rejected there, for either role.
// Direct decoder and legacy JSON-master controls remain separate tests.
func TestParentKidsFlagCookieMiddlewareRejectsTrailingJSONWithoutWrites(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f := newKidsCookieFixture(t, master, false, "", "false")
			prefix := "/account"
			if master {
				prefix = "/admin"
			}
			before, version := f.row(t)
			raw, settingsVersion := f.settingsRow(t)
			cachedBefore, _ := f.base.users.Get(f.base.profile.ID)
			settingsBefore, err := f.settings.Get(f.base.profile.ID)
			if err != nil {
				t.Fatal(err)
			}
			body := `{"unknown":{"x":1},"isKidsProfile":null,"isKidsProfile":true} {"ignored":false}`
			w := f.call(t, prefix, "", body, "", "")
			var failure struct{ Code string }
			if w.Code != 400 || json.Unmarshal(w.Body.Bytes(), &failure) != nil || failure.Code != "invalid_request" || strings.Contains(w.Body.String(), "parentIntent") {
				t.Fatal("Cookie middleware accepted a trailing value or issued a confirmation", w.Code)
			}
			after, current := f.row(t)
			settings, currentSettingsVersion := f.settingsRow(t)
			if !reflect.DeepEqual(before, after) || version != current || !bytes.Equal(raw, settings) || settingsVersion != currentSettingsVersion {
				t.Fatal("rejected Cookie envelope changed a stored row")
			}
			cachedAfter, _ := f.base.users.Get(f.base.profile.ID)
			settingsAfter, err := f.settings.Get(f.base.profile.ID)
			if err != nil || !reflect.DeepEqual(cachedBefore, cachedAfter) || !reflect.DeepEqual(settingsBefore, settingsAfter) {
				t.Fatal("rejected Cookie envelope changed a cache")
			}
		})
	}
}

func TestParentKidsFlagCookieBodyCapAndMissingDefaultsFailClosed(t *testing.T) {
	for _, kind := range []string{"oversize", "missing defaults"} {
		t.Run(kind, func(t *testing.T) {
			f := newKidsCookieFixture(t, false, false, "", "nil")
			before, version := f.row(t)
			raw, sv := f.settingsRow(t)
			body := `{"isKidsProfile":true}`
			intent, password := "", ""
			want := 413
			if kind == "oversize" {
				body += strings.Repeat(" ", (16<<10)+1-len(body))
			} else {
				intent = f.challenge(t, "/account", "", body)
				password = "synthetic-parent-password"
				f.admin.userSettingsService = nil
				want = 503
			}
			w := f.call(t, "/account", "", body, intent, password)
			if w.Code != want {
				t.Fatal("missing/bounded PG port did not fail closed", w.Code)
			}
			after, current := f.row(t)
			settings, currentSV := f.settingsRow(t)
			if !reflect.DeepEqual(before, after) || version != current || !bytes.Equal(raw, settings) || sv != currentSV {
				t.Fatal("failed PG path silently ran legacy/default work")
			}
		})
	}
}

func TestParentKidsFlagCookieFalseNeedsNoSettingsPortOrRead(t *testing.T) {
	for _, mode := range []string{"nil service", "renamed relation"} {
		t.Run(mode, func(t *testing.T) {
			f := newKidsCookieFixture(t, false, true, "content_list", "false")
			if mode == "nil service" {
				f.admin.userSettingsService = nil
			} else {
				if _, err := f.base.store.Pool().Exec(t.Context(), `ALTER TABLE user_settings RENAME TO synthetic_flag_hidden_settings`); err != nil {
					t.Fatal(err)
				}
				f.settingsTable = "synthetic_flag_hidden_settings"
			}
			before, version := f.settingsRow(t)
			body := `{"isKidsProfile":false}`
			intent := f.challenge(t, "/account", "", body)
			w := f.call(t, "/account", "", body, intent, "synthetic-parent-password")
			if w.Code != 200 {
				t.Fatal("false flag read or required Settings", w.Code)
			}
			after, current := f.settingsRow(t)
			if !bytes.Equal(before, after) || version != current {
				t.Fatal("false flag touched Settings")
			}
		})
	}
}

func TestParentKidsFlagCookieOriginalQueryBodyAndAuthentication(t *testing.T) {
	for _, change := range []string{"body bytes", "query encoding", "alias", "cookie", "cancel"} {
		t.Run(change, func(t *testing.T) {
			f := newKidsCookieFixture(t, false, false, "", "false")
			body := `{"isKidsProfile":true}`
			query := "profileId=" + f.base.profile.ID + "&profileId=" + f.base.profile.ID
			token := f.challenge(t, "/account", query, body)
			prefix := "/account"
			retryBody, retryQuery := body, query
			if change == "body bytes" {
				retryBody = " " + body
			}
			if change == "query encoding" {
				retryQuery = strings.ReplaceAll(query, "-", "%2D")
			}
			if change == "alias" {
				prefix = "/admin"
			}
			r := f.request(t, prefix, retryQuery, retryBody, token, "synthetic-parent-password")
			if change == "cookie" {
				r.Header.Del("Cookie")
			}
			if change == "cancel" {
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			}
			before, version := f.row(t)
			raw, sv := f.settingsRow(t)
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, r)
			if w.Code == 200 && w.Body.Len() > 0 {
				t.Fatal("changed original operation was published")
			}
			if change != "cookie" && change != "cancel" && w.Code != 409 {
				t.Fatal("changed bound operation did not conflict", w.Code)
			}
			after, current := f.row(t)
			settings, currentSV := f.settingsRow(t)
			if !reflect.DeepEqual(before, after) || version != current || !bytes.Equal(raw, settings) || sv != currentSV {
				t.Fatal("changed operation wrote")
			}
		})
	}
}

func TestParentKidsFlagCookieSecondWriteFailureRollsBackBoth(t *testing.T) {
	f := newKidsCookieFixture(t, false, false, "", "nil")
	// The sequence witnesses the first real Users UPDATE even when the whole
	// transaction subsequently rolls back. It is fixture telemetry, not a receipt.
	for _, sql := range []string{
		`CREATE SEQUENCE synthetic_flag_user_updates`,
		`CREATE FUNCTION synthetic_flag_observe_update() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM nextval('synthetic_flag_user_updates'); RETURN NEW; END$$`,
		`CREATE TRIGGER synthetic_flag_update_seen AFTER UPDATE ON users FOR EACH ROW EXECUTE FUNCTION synthetic_flag_observe_update()`,
		`ALTER TABLE user_settings ADD CONSTRAINT synthetic_flag_reject_default CHECK(settings #>> '{display,simpleMode}' IS DISTINCT FROM 'true')`,
	} {
		if _, err := f.base.store.Pool().Exec(t.Context(), sql); err != nil {
			t.Fatal("install own rollback fixture", err)
		}
	}
	before, version := f.row(t)
	raw, sv := f.settingsRow(t)
	body := `{"isKidsProfile":true}`
	token := f.challenge(t, "/account", "", body)
	w := f.call(t, "/account", "", body, token, "synthetic-parent-password")
	if w.Code != 503 {
		t.Fatal("second write error did not fail closed", w.Code)
	}
	var called bool
	if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT is_called FROM synthetic_flag_user_updates`).Scan(&called); err != nil || !called {
		t.Fatal("fixture never observed the first actual UPDATE")
	}
	after, current := f.row(t)
	settings, currentSV := f.settingsRow(t)
	if !reflect.DeepEqual(before, after) || version != current || !bytes.Equal(raw, settings) || sv != currentSV {
		t.Fatal("second write error partially committed")
	}
	cached, _ := f.base.users.Get(f.base.profile.ID)
	if cached.IsKidsProfile || !cached.AllowLiveTakeover || !cached.AllowRecordingInterruption {
		t.Fatal("failed flag adopted into User cache")
	}
	value, err := f.settings.Get(f.base.profile.ID)
	if err != nil || value == nil || value.Display.SimpleMode != nil {
		t.Fatal("failed default adopted into Settings cache")
	}
}

type kidsCookieLostReply struct {
	header    http.Header
	attempted bool
}

func (w *kidsCookieLostReply) Header() http.Header { return w.header }
func (*kidsCookieLostReply) WriteHeader(int)       {}
func (w *kidsCookieLostReply) Write([]byte) (int, error) {
	w.attempted = true
	return 0, io.ErrClosedPipe
}

func TestParentKidsFlagCookieKnownCommitLostResponseIsNotReplayed(t *testing.T) {
	f := newKidsCookieFixture(t, false, false, "", "nil")
	body := `{"isKidsProfile":true}`
	token := f.challenge(t, "/account", "", body)
	w := &kidsCookieLostReply{header: make(http.Header)}
	f.router.ServeHTTP(w, f.request(t, "/account", "", body, token, "synthetic-parent-password"))
	stored, err := f.base.store.Users().Get(t.Context(), f.base.profile.ID)
	if err != nil || !stored.IsKidsProfile || !w.attempted {
		t.Fatal("actual committed response-write-loss control did not reach publication")
	}
	cached, _ := f.base.users.Get(f.base.profile.ID)
	settings, err := f.settings.Get(f.base.profile.ID)
	if !cached.IsKidsProfile || err != nil || settings == nil || settings.Display.SimpleMode == nil || !*settings.Display.SimpleMode {
		t.Fatal("known commit lost response split cache ACKs")
	}
	before, version := f.row(t)
	raw, sv := f.settingsRow(t)
	replay := f.call(t, "/account", "", body, token, "synthetic-parent-password")
	if replay.Code != 409 {
		t.Fatal("consumed original commit was automatically replaceable", replay.Code)
	}
	after, current := f.row(t)
	settingsRaw, currentSV := f.settingsRow(t)
	if !reflect.DeepEqual(before, after) || version != current || !bytes.Equal(raw, settingsRaw) || sv != currentSV {
		t.Fatal("lost-response replay wrote again")
	}
	// This is known COMMIT plus failed HTTP Write, NOT ambiguous PostgreSQL
	// COMMIT acknowledgment; real unknown/quarantine proof lives in lease tests.
}

func TestParentKidsFlagCookieJSONKeepsLegacyPolicyAndDecoder(t *testing.T) {
	kidsCookieNoHTTP(t)
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-json-flag-parent")
	dir := t.TempDir()
	as, err := accounts.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := as.GetMasterAccount()
	if !ok {
		t.Fatal("synthetic master missing")
	}
	us, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	target, err := us.CreateForAccount(master.ID, "Synthetic legacy target")
	if err != nil {
		t.Fatal(err)
	}
	settings, err := user_settings.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := sessions.NewService(filepath.Join(dir, "sessions"), 0)
	if err != nil {
		t.Fatal(err)
	}
	session, err := ss.Create(master.ID, true, "synthetic-json", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler("", "", nil, us, settings, nil)
	admin.SetAccountsService(as)
	admin.SetSessionsService(ss)
	admin.SetParentKidsHandler(NewUsersHandler(us, as))
	router := mux.NewRouter()
	router.HandleFunc("/account/api/profiles/kids", admin.RequireAuth(admin.SetKidsProfile)).Methods("PUT")
	for _, body := range []string{`{"isKidsProfile":true,"ignored":1} {"ignored":2}`, `null`} {
		before, _ := us.Get(target.ID)
		beforeSettings, err := settings.Get(target.ID)
		if err != nil {
			t.Fatal(err)
		}
		original, _ := json.Marshal([]any{before, beforeSettings})
		for _, password := range []string{"", "synthetic-wrong-json-parent"} {
			r := httptest.NewRequest("PUT", "http://synthetic.test/account/api/profiles/kids?profileId="+target.ID, strings.NewReader(body))
			r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: session.Token})
			r.Header.Set("X-Account-Password", password)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			after, _ := us.Get(target.ID)
			afterSettings, err := settings.Get(target.ID)
			if err != nil {
				t.Fatal(err)
			}
			current, _ := json.Marshal([]any{after, afterSettings})
			if w.Code != http.StatusForbidden || !bytes.Equal(original, current) {
				t.Fatal("unconfirmed JSON master changed profile/defaults")
			}
		}
		r := httptest.NewRequest("PUT", "http://synthetic.test/account/api/profiles/kids?profileId="+target.ID, strings.NewReader(body))
		r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: session.Token})
		r.Header.Set("X-Account-Password", "synthetic-json-flag-parent")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 200 || strings.Contains(w.Body.String(), "parentIntent") {
			t.Fatal("JSON master/one-decode path changed", w.Code)
		}
	}
	saved, _ := us.Get(target.ID)
	value, err := settings.Get(target.ID)
	if saved.IsKidsProfile || err != nil || value == nil || value.Display.SimpleMode == nil || !*value.Display.SimpleMode {
		t.Fatal("legacy true default/false retention changed")
	}
}
