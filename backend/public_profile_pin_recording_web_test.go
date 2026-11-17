package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"novastream/handlers"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
)

type publicPinWebFixture struct {
	store    *datastore.DataStore
	users    *users.Service
	sessions *sessions.Service
	pins     *profileunlock.PGProfilePins
	profile  models.User
	session  models.Session
}

func newPublicPinWebFixture(t *testing.T) *publicPinWebFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL PIN fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("synthetic Unix-socket database required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("connect synthetic database")
	}
	schema := fmt.Sprintf("synthetic_public_pin_web_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own schema")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove own schema")
		}
		admin.Close()
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	ds, err := datastore.New(t.Context(), u.String(), datastore.WithMaxConns(4))
	if err != nil {
		t.Fatal("migrate own schema")
	}
	t.Cleanup(ds.Close)
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic-web-parent"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	a := models.Account{ID: "actor", Username: "synthetic-web-actor", PasswordHash: string(hash), CreatedAt: at, UpdatedAt: at}
	if err = ds.Accounts().Create(t.Context(), &a); err != nil {
		t.Fatal(err)
	}
	us, err := users.NewServiceWithStore(ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := us.CreateForAccount("actor", "Synthetic web owner")
	if err != nil {
		t.Fatal(err)
	}
	ss, err := sessions.NewServiceWithStore(ds, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := ss.Create("actor", false, "synthetic-web", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	return &publicPinWebFixture{store: ds, users: us, sessions: ss, pins: profileunlock.NewPGProfilePins(ds), profile: profile, session: session}
}

func (f *publicPinWebFixture) cookieRequest(method, path, body, contentType string) *http.Request {
	r := httptest.NewRequest(method, "http://synthetic.test"+path, strings.NewReader(body))
	r.Header.Set("Origin", "http://synthetic.test")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Content-Type", contentType)
	r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.session.Token})
	return r
}

func (f *publicPinWebFixture) admin(t *testing.T) *handlers.AdminUIHandler {
	t.Helper()
	h := handlers.NewAdminUIHandler("", "", nil, f.users, nil, nil)
	h.SetSessionsService(f.sessions)
	h.SetPGProfilePins(f.pins)
	return h
}

func TestPublicPinRecordingWebOriginalKnownPostTuple(t *testing.T) {
	for _, mode := range []string{"own-length-migration", "external-name", "external-length", "revoke-after-verify", "no-pin", "no-pin-to-pin", "forged-success"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinWebFixture(t)
			if mode != "no-pin" && mode != "no-pin-to-pin" {
				if _, err := f.users.SetPin(f.profile.ID, "1357"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "own-length-migration" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET pin_length=0 WHERE id=$1", f.profile.ID); err != nil {
					t.Fatal(err)
				}
				var err error
				f.users, err = users.NewServiceWithStore(f.store, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			}
			u := handlers.NewUsersHandler(f.users)
			u.SetPGProfilePins(f.pins, func() time.Duration { return time.Hour })
			called := 0
			verify := func(w http.ResponseWriter, r *http.Request) {
				called++
				if mode == "forged-success" {
					json.NewEncoder(w).Encode(handlers.VerifyPinResponse{Valid: true})
					return
				}
				u.VerifyPin(w, r)
				var err error
				switch mode {
				case "external-name":
					_, err = f.store.Pool().Exec(r.Context(), "UPDATE users SET name='Changed after actual VerifyPin',updated_at=clock_timestamp() WHERE id=$1", f.profile.ID)
				case "external-length":
					_, err = f.store.Pool().Exec(r.Context(), "UPDATE users SET pin_length=pin_length+1 WHERE id=$1", f.profile.ID)
				case "revoke-after-verify":
					err = f.store.ProfileUnlocks().DeleteByProfile(r.Context(), f.profile.ID)
				case "no-pin-to-pin":
					_, err = f.users.SetPin(f.profile.ID, "9753")
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			pin, _ := newRecordingWebPOSTHandlers(f.sessions, nil, f.users, verify, nil, f.pins)
			r := f.cookieRequest("POST", "/account/api/users/"+f.profile.ID+"/pin/verify", `{"pin":"1357"}`, "application/json")
			r = mux.SetURLVars(r, map[string]string{"userID": f.profile.ID})
			w := httptest.NewRecorder()
			f.admin(t).RequireAuth(pin)(w, r)
			positive := mode == "own-length-migration" || mode == "no-pin"
			if (w.Code == 200) != positive || called != 1 {
				t.Fatalf("mode%s status%d calls%d", mode, w.Code, called)
			}
			if positive {
				var response handlers.VerifyPinResponse
				if json.Unmarshal(w.Body.Bytes(), &response) != nil || !response.Valid {
					t.Fatal("actual verifier success unavailable")
				}
				if mode == "own-length-migration" {
					if response.UnlockToken == "" {
						t.Fatal("verified bound token missing")
					}
					var length int
					if err := f.store.Pool().QueryRow(t.Context(), "SELECT pin_length FROM users WHERE id=$1", f.profile.ID).Scan(&length); err != nil || length != 4 {
						t.Fatal("own length migration missing")
					}
				}
			} else if strings.Contains(w.Body.String(), "unlockToken") || strings.Contains(w.Body.String(), `"valid":true`) {
				t.Fatal("retired/forged verification published")
			}
		})
	}
}

func TestPublicPinRecordingWebBoundBodyAndOriginalLifetime(t *testing.T) {
	for _, mode := range []string{"positive", "query", "multiple-token", "foreign-origin", "expired-actor", "scoped"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinWebFixture(t)
			if mode == "expired-actor" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "scoped" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET scope='stream',scope_resource='synthetic' WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
			}
			var seen context.Context
			calls := 0
			export := func(w http.ResponseWriter, r *http.Request) {
				calls++
				seen = r.Context()
				if f.pins.CheckOriginal(r) != nil {
					t.Fatal("current original frame not forwarded")
				}
				claim, ok := auth.GetSession(r)
				if !ok || claim.Token != f.session.Token || claim.Scope != "" {
					t.Fatal("original full session changed")
				}
				if r.URL.Query().Get("profileId") != f.profile.ID || r.Header.Get("X-Profile-Unlock") != "" || r.Body != http.NoBody {
					t.Fatal("body-only forwarding changed")
				}
				deadline, ok := r.Context().Deadline()
				if !ok || deadline.After(f.session.ExpiresAt) || time.Until(deadline) < 30*time.Minute {
					t.Fatal("original deadline changed or setup budget leaked")
				}
				w.WriteHeader(204)
			}
			_, download := newRecordingWebPOSTHandlers(f.sessions, nil, f.users, nil, export, f.pins)
			body := url.Values{"profileId": {f.profile.ID}}.Encode()
			if mode == "multiple-token" {
				body += "&profileUnlock=one%2Ctwo"
			}
			r := f.cookieRequest("POST", "/account/api/live/recordings/synthetic/export", body, "application/x-www-form-urlencoded")
			if mode == "query" {
				r.URL.RawQuery = "profileId=" + url.QueryEscape(f.profile.ID)
			}
			if mode == "foreign-origin" {
				r.Header.Set("Origin", "https://foreign.invalid")
			}
			w := httptest.NewRecorder()
			f.admin(t).RequireAuth(download)(w, r)
			if mode == "positive" {
				if w.Code != 204 || calls != 1 || seen == nil || seen.Err() == nil {
					t.Fatal("valid original request/lifetime was not consumed")
				}
			} else if calls != 0 || w.Code < 300 {
				t.Fatal("invalid cookie/form started export")
			}
		})
	}
}

type publicPinLogoutBody struct {
	once    sync.Once
	entered chan struct{}
	resume  chan struct{}
	payload *strings.Reader
}

func (b *publicPinLogoutBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered); <-b.resume })
	return b.payload.Read(p)
}
func (*publicPinLogoutBody) Close() error { return nil }

func TestPublicPinActualAccountLogoutRouteRetiresRefreshLineage(t *testing.T) {
	for _, mode := range []string{"refresh-during-original-cookie-route", "logout-before-refresh"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinWebFixture(t)
			if _, err := f.users.SetPin(f.profile.ID, "1357"); err != nil {
				t.Fatal(err)
			}
			original, release, err := f.pins.Authenticate(httptest.NewRequest("POST", "/api/auth/refresh", nil), f.session.Token, "synthetic-client")
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if _, _, err := f.pins.Verify(original, f.profile.ID, "1357", time.Hour, f.users); err != nil {
				t.Fatal(err)
			}
			accountUI := handlers.NewAccountUIHandler(nil, f.sessions, f.users, nil, nil, nil, nil)
			router := mux.NewRouter()
			registerAccountLogoutRoute(router, accountUI, f.admin(t), f.pins)
			body := &publicPinLogoutBody{entered: make(chan struct{}), resume: make(chan struct{}), payload: strings.NewReader("{}")}
			var once sync.Once
			resume := func() { once.Do(func() { close(body.resume) }) }
			defer resume()
			r := f.cookieRequest("POST", "/account/logout", "{}", "application/json")
			r.Header.Set("X-Client-ID", "synthetic-client")
			r.Body = body
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); router.ServeHTTP(w, r) }()
			t.Cleanup(func() {
				resume()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("registered logout route did not finish")
				}
			})
			select {
			case <-body.entered:
			case <-done:
				t.Fatal("registered PG route did not reach authenticated body boundary")
			case <-time.After(5 * time.Second):
				t.Fatal("registered route authentication did not finish")
			}
			var replacement string
			if mode == "refresh-during-original-cookie-route" {
				next, err := f.sessions.RefreshProfilePinSession(original)
				if err != nil {
					t.Fatal("actual refresh failed", err)
				}
				replacement = next.Token
				var active int
				if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE token=$1", replacement).Scan(&active); err != nil || active != 1 {
					t.Fatal("committed replacement was not present before route release")
				}
			}
			resume()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("registered route did not complete logout")
			}
			if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin/login" {
				t.Fatal("registered PG logout not confirmed")
			}
			if mode == "logout-before-refresh" {
				if _, err := f.sessions.RefreshProfilePinSession(original); err == nil {
					t.Fatal("refresh revived original route after logout")
				}
			}
			var sessionsLeft, grantsLeft int
			if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE token=$1 OR token=$2", f.session.Token, replacement).Scan(&sessionsLeft); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM profile_unlocks").Scan(&grantsLeft); err != nil {
				t.Fatal(err)
			}
			if sessionsLeft != 0 || grantsLeft != 0 {
				t.Fatal("registered account logout left successor authority alive")
			}
		})
	}
}

func TestPublicPinAccountLogoutRoutePreservesJSONHandler(t *testing.T) {
	ss, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := ss.Create("synthetic-json-account", false, "synthetic-json", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	accountUI := handlers.NewAccountUIHandler(nil, ss, nil, nil, nil, nil, nil)
	router := mux.NewRouter()
	// A nil admin handler also proves that JSON routing never enters the PG
	// wrapper or needs any of the new datastore/credential machinery.
	registerAccountLogoutRoute(router, accountUI, nil, nil)
	r := httptest.NewRequest("POST", "/account/logout", nil)
	r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: session.Token})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	if w.Code != http.StatusSeeOther {
		t.Fatal("legacy JSON logout response changed")
	}
	if _, err := ss.Validate(session.Token); err == nil {
		t.Fatal("legacy JSON session was not revoked")
	}
}
