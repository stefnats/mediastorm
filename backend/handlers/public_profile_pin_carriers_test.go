package handlers

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
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
	"novastream/services/users"
)

type publicPinCarrierFixture struct {
	store    *datastore.DataStore
	users    *users.Service
	sessions *sessions.Service
	pins     *profileunlock.PGProfilePins
	profile  models.User
	session  models.Session
}

// The only database accepted by this fixture is the existing explicit local
// synthetic socket opt-in. All migrations and writes stay in its own schema.
func newPublicPinCarrierFixture(t *testing.T) *publicPinCarrierFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("synthetic PostgreSQL PIN fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("dedicated synthetic Unix-socket database required")
	}
	ctx := t.Context()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect synthetic database")
	}
	schema := fmt.Sprintf("synthetic_public_pin_carrier_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own schema")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove own schema")
		}
		admin.Close()
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	ds, err := datastore.New(ctx, u.String(), datastore.WithMaxConns(4))
	if err != nil {
		t.Fatal("migrate own schema")
	}
	t.Cleanup(ds.Close)
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic-parent-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	for _, id := range []string{"actor", "other"} {
		a := models.Account{ID: id, Username: "synthetic-" + id, PasswordHash: string(hash), CreatedAt: at, UpdatedAt: at}
		if err = ds.Accounts().Create(ctx, &a); err != nil {
			t.Fatal(err)
		}
	}
	us, err := users.NewServiceWithStore(ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := us.CreateForAccount("actor", "Synthetic owner")
	if err != nil {
		t.Fatal(err)
	}
	ss, err := sessions.NewServiceWithStore(ds, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := ss.Create("actor", false, "synthetic-carrier", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	return &publicPinCarrierFixture{store: ds, users: us, sessions: ss, pins: profileunlock.NewPGProfilePins(ds), profile: profile, session: session}
}

func (f *publicPinCarrierFixture) request(t *testing.T, method, path string) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, release, err := f.pins.Authenticate(httptest.NewRequest(method, path, nil), f.session.Token, "synthetic-client")
	if err != nil {
		t.Fatal(err)
	}
	return r, release
}

func (f *publicPinCarrierFixture) admin() *AdminUIHandler {
	h := &AdminUIHandler{usersService: f.users, sessionsService: f.sessions}
	h.SetPGProfilePins(f.pins)
	return h
}

func (f *publicPinCarrierFixture) setPIN(t *testing.T, pin string) {
	t.Helper()
	if _, err := f.users.SetPin(f.profile.ID, pin); err != nil {
		t.Fatal(err)
	}
}

func (f *publicPinCarrierFixture) issue(t *testing.T, r *http.Request) string {
	t.Helper()
	token, _, err := f.pins.Verify(r, f.profile.ID, "1357", time.Hour, f.users)
	if err != nil || token == "" {
		t.Fatal("actual PG PIN verification failed", err)
	}
	return token
}

func TestPublicPinCookieOriginalFrameAndCancellation(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	// The stored session role remains an old cache; only the actual actor role
	// normalizes the frame. The caller supplies no role flag to Authenticate.
	if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=true WHERE id='actor'"); err != nil {
		t.Fatal(err)
	}
	h := f.admin()
	var observed context.Context
	r := httptest.NewRequest("GET", "/admin/api/synthetic", nil)
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
	w := httptest.NewRecorder()
	h.RequireMasterAuth(func(w http.ResponseWriter, r *http.Request) {
		observed = r.Context()
		if !auth.IsMaster(r) || f.pins.CheckOriginal(r) != nil {
			t.Fatal("current normalized frame missing")
		}
		deadline, ok := r.Context().Deadline()
		if !ok || deadline.After(f.session.ExpiresAt) || time.Until(deadline) < 30*time.Minute {
			t.Fatal("request inherited setup budget or renewed expiry")
		}
		w.WriteHeader(204)
	})(w, r)
	if w.Code != 204 || observed == nil || observed.Err() == nil {
		t.Fatal("outer cookie wrapper did not own cancellation")
	}
}

func TestPublicPinCookieUsesCurrentOwnerAndRole(t *testing.T) {
	for _, mode := range []string{"reassigned", "cached-master", "actor-expired", "scoped", "positive"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			switch mode {
			case "reassigned":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET account_id='other' WHERE id=$1", f.profile.ID); err != nil {
					t.Fatal(err)
				}
			case "cached-master":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET is_master=true WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET account_id='other' WHERE id=$1", f.profile.ID); err != nil {
					t.Fatal(err)
				}
			case "actor-expired":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'"); err != nil {
					t.Fatal(err)
				}
			case "scoped":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET scope='stream',scope_resource='synthetic' WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
			}
			called := false
			r := httptest.NewRequest("GET", "/account/api/synthetic?profileId="+url.QueryEscape(f.profile.ID), nil)
			r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
			w := httptest.NewRecorder()
			f.admin().RequireAuth(func(w http.ResponseWriter, _ *http.Request) { called = true; w.WriteHeader(204) })(w, r)
			if called != (mode == "positive") {
				t.Fatal("cached ownership/role or scoped authority reached handler")
			}
			if mode != "positive" && w.Code < 300 {
				t.Fatal("denial published success")
			}
		})
	}
}

func TestPublicPinRecordingUsesPGInsteadOfCachedNoPin(t *testing.T) {
	for _, mode := range []string{"no-pin", "locked", "legacy-token", "verified", "policy-off", "kids", "foreign", "scoped", "late-pin", "late-owner", "late-kids", "late-session"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			if mode != "no-pin" && mode != "late-pin" {
				f.setPIN(t, "1357")
			}
			if mode == "kids" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET is_kids_profile=true WHERE id=$1", f.profile.ID); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "foreign" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET account_id='other' WHERE id=$1", f.profile.ID); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "scoped" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET scope='stream',scope_resource='recording:synthetic' WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
			}
			r, release := f.request(t, "GET", "/api/video/stream?path=recording:synthetic")
			defer release()
			if mode == "legacy-token" {
				legacy := profileunlock.NewService(f.store.ProfileUnlocks())
				token, _, err := legacy.Issue(profileunlock.IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: "actor", ProfileID: f.profile.ID, PinFingerprint: f.users.PinFingerprint(f.profile.ID)})
				if err != nil {
					t.Fatal(err)
				}
				r.Header.Set("X-Profile-Unlock", token)
			}
			if mode == "verified" || mode == "late-owner" || mode == "late-kids" || mode == "late-session" {
				r.Header.Set("X-Profile-Unlock", f.issue(t, r))
			}
			calls := 0
			owners := recordingOwnerFunc(func(ctx context.Context, _ string) (recordings.StreamOwner, error) {
				calls++
				if calls == 2 {
					var query string
					switch mode {
					case "late-owner":
						query = "UPDATE users SET account_id='other' WHERE id=$1"
					case "late-kids":
						query = "UPDATE users SET is_kids_profile=true WHERE id=$1"
					case "late-session":
						query = "DELETE FROM sessions WHERE token=$1"
					}
					if query != "" {
						value := f.profile.ID
						if mode == "late-session" {
							value = f.session.Token
						}
						if _, err := f.store.Pool().Exec(ctx, query, value); err != nil {
							t.Fatal(err)
						}
					}
				}
				return recordings.StreamOwner{RecordingID: "synthetic", ProfileID: f.profile.ID}, ctx.Err()
			})
			policyCalls := 0
			guard := NewRecordingStreamAccess(owners, nil, nil, nil, nil, func() (config.SecuritySettings, error) {
				policyCalls++
				if mode == "late-pin" && policyCalls == 1 {
					if _, err := f.users.SetPin(f.profile.ID, "1357"); err != nil {
						t.Fatal(err)
					}
				}
				return config.SecuritySettings{EnforceProfilePins: mode != "policy-off"}, nil
			})
			guard.SetPGProfilePins(f.pins)
			w := httptest.NewRecorder()
			_, allowed := guard.Authorize(w, r, "recording:synthetic")
			want := mode == "no-pin" || mode == "verified" || mode == "policy-off"
			if allowed != want {
				t.Fatalf("mode%s allowed%v status%d", mode, allowed, w.Code)
			}
			if (mode == "late-owner" || mode == "late-kids" || mode == "late-session") && calls < 2 {
				t.Fatal("late mutation barrier was not reached")
			}
		})
	}
}

func TestPublicPinAdministrativeIntentUsesExistingParentPolicy(t *testing.T) {
	for _, mode := range []string{"missing-password", "correct-password", "genuine-master", "stale-master"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			if mode == "genuine-master" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=true WHERE id='actor'"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "stale-master" {
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE sessions SET is_master=true WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
			}
			h := f.admin()
			// Use the actual existing guard, backed by the actual account service.
			t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-carrier-master")
			accountService, err := accounts.NewServiceWithStore(f.store)
			if err != nil {
				t.Fatal(err)
			}
			h.SetAccountsService(accountService)
			for _, clearing := range []bool{false, true} {
				method := "PUT"
				body := `{"pin":" 2468 "}`
				if clearing {
					method = "DELETE"
					body = ""
				}
				r := httptest.NewRequest(method, "/account/api/profiles/pin?profileId="+url.QueryEscape(f.profile.ID), strings.NewReader(body))
				r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
				if mode == "correct-password" {
					r.Header.Set("X-Account-Password", "synthetic-parent-password")
				}
				w := httptest.NewRecorder()
				next := h.SetProfilePin
				if clearing {
					next = h.ClearProfilePin
				}
				h.RequireAuth(next)(w, r)
				want := mode == "correct-password" || mode == "genuine-master"
				if (w.Code == 200) != want {
					t.Fatalf("mode%s clear%v status%d", mode, clearing, w.Code)
				}
				var storedHash string
				var storedLength int
				if err := f.store.Pool().QueryRow(t.Context(), "SELECT pin_hash,pin_length FROM users WHERE id=$1", f.profile.ID).Scan(&storedHash, &storedLength); err != nil {
					t.Fatal(err)
				}
				if want && !clearing && (bcrypt.CompareHashAndPassword([]byte(storedHash), []byte("2468")) != nil || storedLength != 4) {
					t.Fatal("target PIN/length not committed")
				}
				if want && clearing && (storedHash != "" || storedLength != 0) {
					t.Fatal("target PIN not cleared")
				}
				if want {
					var response ProfileWithPinStatus
					if json.Unmarshal(w.Body.Bytes(), &response) != nil || response.ID != f.profile.ID || response.HasPin == clearing {
						t.Fatal("targeted persistence response")
					}
				}
			}
		})
	}
}

type publicPinExportProbe struct {
	recordingService
	inspect func(context.Context, func() error)
}

func (p *publicPinExportProbe) OpenPartsExport(ctx context.Context, _ string, check func() error) (*recordings.PartsExport, error) {
	p.inspect(ctx, check)
	return nil, recordings.ErrRecordingExportBusy
}

func TestPublicPinRecordingExportAndTimingUseOriginalPGOwnerExpiry(t *testing.T) {
	for _, kind := range []string{"export", "timing"} {
		t.Run(kind, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			ownerExpiry := time.Now().UTC().Add(40 * time.Minute).Truncate(time.Microsecond)
			actorExpiry := ownerExpiry.Add(5 * time.Minute)
			if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=true,expires_at=$1 WHERE id='actor'", actorExpiry); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET expires_at=$1 WHERE id='other'", ownerExpiry); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET account_id='other' WHERE id=$1", f.profile.ID); err != nil {
				t.Fatal(err)
			}
			r, release := f.request(t, "GET", "/api/live/recordings/synthetic")
			defer release()
			r = mux.SetURLVars(r, map[string]string{"recordingID": "synthetic"})
			guard := NewRecordingStreamAccess(recordingOwnerFunc(func(ctx context.Context, _ string) (recordings.StreamOwner, error) {
				return recordings.StreamOwner{RecordingID: "synthetic", ProfileID: f.profile.ID}, ctx.Err()
			}), nil, nil, nil, nil, func() (config.SecuritySettings, error) { return config.SecuritySettings{EnforceProfilePins: true}, nil })
			guard.SetPGProfilePins(f.pins)
			checkDeadline := func(ctx context.Context, authorize func() error) {
				at, ok := ctx.Deadline()
				if !ok || !at.Equal(ownerExpiry) {
					t.Fatal("PG owner expiry replaced by actor/session/cache/setup budget")
				}
				if err := authorize(); err != nil {
					t.Fatal("original actual owner denied", err)
				}
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE accounts SET expires_at=$1 WHERE id='other'", ownerExpiry.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
				if err := authorize(); err == nil {
					t.Fatal("new owner row/expiry adopted by original operation")
				}
				at, _ = ctx.Deadline()
				if !at.Equal(ownerExpiry) {
					t.Fatal("original deadline renewed")
				}
			}
			w := httptest.NewRecorder()
			if kind == "export" {
				called := false
				probe := &publicPinExportProbe{inspect: func(ctx context.Context, check func() error) { called = true; checkDeadline(ctx, check) }}
				h := NewRecordingsHandler(probe, nil)
				h.SetRecordingStreamAccess(guard)
				h.ExportParts(w, r)
				if !called || w.Code != http.StatusTooManyRequests {
					t.Fatal("actual export authority did not consume captured deadline")
				}
			} else {
				h := NewRecordingsHandler(nil, nil)
				h.SetRecordingStreamAccess(guard)
				ctx, cancel, check, ok := h.partTimingAuthority(w, r)
				if !ok {
					t.Fatal("actual timing authority denied")
				}
				defer cancel()
				checkDeadline(ctx, check)
			}
		})
	}
}

func TestPublicPinCookieLogoutRetainsOriginalFrameAcrossRefresh(t *testing.T) {
	for _, order := range []string{"refresh-before-logout", "logout-before-refresh"} {
		t.Run(order, func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			f.setPIN(t, "1357")
			original, release := f.request(t, "POST", "/api/auth/refresh")
			defer release()
			f.issue(t, original)
			h := f.admin()
			r := httptest.NewRequest("GET", "/account/logout", nil)
			r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.session.Token})
			r.Header.Set("X-Client-ID", "synthetic-client")
			bound := make(chan struct{})
			resume := make(chan struct{})
			done := make(chan struct{})
			var once sync.Once
			continueLogout := func() { once.Do(func() { close(resume) }) }
			defer continueLogout()
			t.Cleanup(func() {
				continueLogout()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("original cookie request did not finish")
				}
			})
			w := httptest.NewRecorder()
			go func() {
				defer close(done)
				// This is the actual cookie authenticator, not an injected role or
				// replacement token. Hold its original A frame while refresh runs.
				h.RequireAuth(func(w http.ResponseWriter, checked *http.Request) {
					close(bound)
					<-resume
					h.Logout(w, checked)
				})(w, r)
			}()
			select {
			case <-bound:
			case <-time.After(5 * time.Second):
				t.Fatal("actual original cookie frame was not captured")
			}
			var replacement string
			if order == "refresh-before-logout" {
				next, err := f.sessions.RefreshProfilePinSession(original)
				if err != nil {
					t.Fatal("actual A-to-B refresh failed", err)
				}
				replacement = next.Token
				if replacement == f.session.Token {
					t.Fatal("refresh did not rotate the token")
				}
				if _, err := f.store.Sessions().Get(t.Context(), replacement); err != nil {
					t.Fatal("actual committed B session missing")
				}
			}
			continueLogout()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("cookie logout did not finish")
			}
			if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/admin/login" {
				t.Fatal("original cookie logout was not confirmed")
			}
			cleared := false
			for _, cookie := range w.Result().Cookies() {
				if cookie.Name == adminSessionCookieName && cookie.Value == "" && cookie.MaxAge < 0 {
					cleared = true
				}
			}
			if !cleared {
				t.Fatal("confirmed logout did not clear its cookie")
			}
			if order == "logout-before-refresh" {
				if _, err := f.sessions.RefreshProfilePinSession(original); err == nil {
					t.Fatal("refresh revived a logged-out original lineage")
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
				t.Fatal("cookie logout left rotated session or grant alive")
			}
		})
	}
}
