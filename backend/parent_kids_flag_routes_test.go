package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/mux"
	"novastream/handlers"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/user_settings"
)

type mainKidsFlagDenyHTTP struct{ calls atomic.Int32 }

func (d *mainKidsFlagDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	d.calls.Add(1)
	return nil, errors.New("outgoing HTTP denied in the synthetic Main-wiring fixture")
}
func mainKidsFlagNoHTTP(t *testing.T) {
	t.Helper()
	previous := http.DefaultTransport
	denied := &mainKidsFlagDenyHTTP{}
	http.DefaultTransport = denied
	t.Cleanup(func() {
		http.DefaultTransport = previous
		if denied.calls.Load() != 0 {
			t.Error("unexpected outgoing HTTP")
		}
	})
}

// One real bad bcrypt plus bounded fast rejections isolate the common rate
// bucket from fixture CPU speed/refill. Only the verifier's speed is controlled;
// the original PG frame, registry, cookie routing and actual Guard are used.
type mainKidsFlagBudgetAccounts struct {
	*accounts.Service
	calls atomic.Int32
}

func (a *mainKidsFlagBudgetAccounts) AuthenticateContext(ctx context.Context, user, password string) (models.Account, error) {
	if a.calls.Add(1) == 1 {
		return a.Service.AuthenticateContext(ctx, user, password)
	}
	return models.Account{}, accounts.ErrInvalidCredentials
}

type mainKidsFlagFixture struct {
	base     *publicPinWebFixture
	settings *user_settings.Service
	shared   *handlers.UsersHandler
	router   *mux.Router
}

func newMainKidsFlagFixture(t *testing.T, master, budget bool) (*mainKidsFlagFixture, *mainKidsFlagBudgetAccounts) {
	t.Helper()
	mainKidsFlagNoHTTP(t)
	f := &mainKidsFlagFixture{base: newPublicPinWebFixture(t)}
	if _, err := f.base.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=$1 WHERE id='actor'`, master); err != nil {
		t.Fatal(err)
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-main-flag-master")
	actual, err := accounts.NewServiceWithStore(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	controlled := &mainKidsFlagBudgetAccounts{Service: actual}
	if budget {
		f.shared = handlers.NewUsersHandler(f.base.users, controlled)
	} else {
		f.shared = handlers.NewUsersHandler(f.base.users, actual)
	}
	f.shared.SetPGProfilePins(f.base.pins)
	f.settings, err = user_settings.NewServiceWithStore(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	admin := handlers.NewAdminUIHandler("", "", nil, f.base.users, f.settings, nil)
	admin.SetSessionsService(f.base.sessions)
	admin.SetAccountsService(actual)
	admin.SetPGProfilePins(f.base.pins)
	// Both helpers below are consumed by the actual production Main. Rebuilding
	// a separate UsersHandler for either cookie alias would break the rate test.
	wireParentKidsFlagHandler(admin, f.shared)
	f.router = mux.NewRouter()
	registerParentKidsFlagCookieRoutes(f.router, admin)
	// This is the existing API handler on the exact same pointer under a real
	// PG-authenticated frame. Actual api.Register is proved in Root's API tests;
	// this focused Main test makes no substitute Register/startup/login claim.
	f.router.HandleFunc("/api/users/{userID}/kids-profile", func(w http.ResponseWriter, r *http.Request) {
		checked, release, err := f.base.pins.Authenticate(r, f.base.session.Token, "")
		if err != nil {
			http.Error(w, "synthetic original authentication unavailable", 401)
			return
		}
		defer release()
		session, _, err := f.base.pins.Identity(checked)
		if err != nil {
			http.Error(w, "synthetic identity unavailable", 401)
			return
		}
		ctx := context.WithValue(checked.Context(), auth.ContextKeyAccountID, session.AccountID)
		ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, session.IsMaster)
		ctx = context.WithValue(ctx, auth.ContextKeySession, session)
		f.shared.SetKidsProfile(w, checked.WithContext(ctx))
	}).Methods("PUT")
	return f, controlled
}
func (f *mainKidsFlagFixture) path(kind int) string {
	if kind == 2 {
		return "/api/users/" + f.base.profile.ID + "/kids-profile"
	}
	prefix := "/admin"
	if kind == 1 {
		prefix = "/account"
	}
	return prefix + "/api/profiles/kids?profileId=" + f.base.profile.ID
}
func (f *mainKidsFlagFixture) request(kind int, body, intent, password string) *http.Request {
	r := f.base.cookieRequest("PUT", f.path(kind), body, "application/json")
	if intent != "" {
		r.Header.Set("X-Parent-Intent", intent)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}
func (f *mainKidsFlagFixture) call(kind int, body, intent, password string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, f.request(kind, body, intent, password))
	return w
}
func (f *mainKidsFlagFixture) challenge(t *testing.T, kind int, body string) string {
	t.Helper()
	w := f.call(kind, body, "", "")
	var value struct {
		Code         string
		ParentIntent string
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || !strings.HasPrefix(value.ParentIntent, "pkf_") || len(value.ParentIntent) != 68 {
		t.Fatal("Main route missed the shared fresh flag path", w.Code)
	}
	return value.ParentIntent
}
func (f *mainKidsFlagFixture) physical(t *testing.T) []byte {
	t.Helper()
	var raw []byte
	err := f.base.store.Pool().QueryRow(t.Context(), `SELECT jsonb_build_object(
      'users',(SELECT jsonb_agg(to_jsonb(u)||jsonb_build_object('mvcc',concat(u.xmin::text,':',u.ctid::text)) ORDER BY id) FROM users u),
      'settings',(SELECT jsonb_agg(to_jsonb(s)||jsonb_build_object('mvcc',concat(s.xmin::text,':',s.ctid::text)) ORDER BY user_id) FROM user_settings s),
      'versions',(SELECT jsonb_agg(to_jsonb(v)||jsonb_build_object('mvcc',concat(v.xmin::text,':',v.ctid::text)) ORDER BY user_id) FROM user_settings_versions v))`).Scan(&raw)
	if err != nil {
		t.Fatal("observe own Main flag rows", err)
	}
	return raw
}

func TestParentKidsFlagMainActualCookieHelpersUseCurrentSharedHandler(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f, _ := newMainKidsFlagFixture(t, master, false)
			for _, kind := range []int{0, 1} {
				body := `{"isKidsProfile":true,"ignoredByCookie":1}`
				nonce := f.challenge(t, kind, body)
				if response := f.call(kind, body, nonce, "synthetic-web-parent"); response.Code != 200 {
					t.Fatal("actual Main cookie registration did not delegate", response.Code)
				}
				user, err := f.base.store.Users().Get(t.Context(), f.base.profile.ID)
				settings, e := f.settings.Get(f.base.profile.ID)
				if err != nil || !user.IsKidsProfile || e != nil || settings == nil || settings.Display.SimpleMode == nil || !*settings.Display.SimpleMode {
					t.Fatal("Main cookie carrier lost compound defaults")
				}
			}
		})
	}
}

func TestParentKidsFlagMainAPIAndCookieShareActualPasswordBudget(t *testing.T) {
	f, verifier := newMainKidsFlagFixture(t, true, true)
	before := f.physical(t)
	body := `{"isKidsProfile":true}`
	for i := 0; i < 10; i++ {
		kind := i % 3
		nonce := f.challenge(t, kind, body)
		response := f.call(kind, body, nonce, "synthetic-wrong-password")
		if response.Code != 403 {
			t.Fatal("shared budget fixture did not reach the actual wrong-password gate", response.Code)
		}
	}
	for _, kind := range []int{0, 1, 2} {
		nonce := f.challenge(t, kind, body)
		response := f.call(kind, body, nonce, "synthetic-web-parent")
		if response.Code != 429 || response.Header().Get("Retry-After") == "" {
			t.Fatal("cookie/API acquired a second credential allowance", response.Code)
		}
	}
	if verifier.calls.Load() != 10 || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("exhausted shared Guard checked credentials or wrote again")
	}
}

func TestParentKidsFlagMainCookieOriginalAuthAndEntryBinding(t *testing.T) {
	f, _ := newMainKidsFlagFixture(t, false, false)
	body := `{"isKidsProfile":true}`
	before := f.physical(t)
	// Existing Cookie PUT policy is preserved. Origin alone is never parental
	// proof; this test does not invent the POST-only recording Origin wrapper.
	for _, origin := range []string{"http://synthetic.test", "https://foreign.invalid", ""} {
		r := f.request(1, body, "", "")
		if origin == "" {
			r.Header.Del("Origin")
		} else {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		f.router.ServeHTTP(w, r)
		if w.Code == 200 {
			t.Fatal("Origin replaced required fresh confirmation")
		}
	}
	r := f.request(1, body, "", "")
	r.Header.Del("Cookie")
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	if w.Code == 200 {
		t.Fatal("actual registration omitted Cookie authentication")
	}
	nonce := f.challenge(t, 1, body)
	// A management intent cannot be used by the API entry even for the exact
	// same final bool, profile, actor and current original account.
	w = f.call(2, body, nonce, "synthetic-web-parent")
	if w.Code != 409 {
		t.Fatal("management confirmation crossed into API semantics", w.Code)
	}
	if !bytes.Equal(before, f.physical(t)) {
		t.Fatal("unconfirmed or changed-entry request wrote")
	}
}
