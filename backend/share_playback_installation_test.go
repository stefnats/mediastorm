//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"novastream/api"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/settingsbootstrap"
	"novastream/models"
	"novastream/services/profileunlock"
)

type sharePlaybackMainFixture struct {
	base    *publicPinWebFixture
	startup *settingsbootstrap.Startup
	store   *datastore.DataStore
	pins    *profileunlock.PGProfilePins
	video   *handlers.VideoHandler
	share   *handlers.ShareHandler
	runtime *datastore.ShareLinkRedeemer
	creator *datastore.ShareLinkCreator
}

func newSharePlaybackMainFixture(t *testing.T) *sharePlaybackMainFixture {
	t.Helper()
	if os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL") == "" {
		t.Fatal("dedicated synthetic PostgreSQL fixture required")
	}
	f := &sharePlaybackMainFixture{base: newPublicPinWebFixture(t), video: &handlers.VideoHandler{}}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = f.base.store.Pool().Config().ConnConfig.ConnString()
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	var err error
	f.startup, err = settingsbootstrap.Begin(t.Context(), manager, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.startup.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store, err = f.startup.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.startup.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, f.base.profile.ID); err != nil {
		t.Fatal(err)
	}
	f.pins = profileunlock.NewPGProfilePins(f.store, f.base.sessions)
	// Managed creation/redemption do not use a legacy store or start its janitor.
	f.share = handlers.NewShareHandler(nil, f.base.sessions, f.base.users, "")
	f.creator, err = f.startup.NewShareLinkCreator(f.store)
	if err != nil {
		t.Fatal(err)
	}
	f.share.SetManagedShareCreation(f.creator)
	return f
}
func (f *sharePlaybackMainFixture) install(t *testing.T) {
	t.Helper()
	if err := configureManagedSharePlayback(f.startup, f.store, f.pins, f.video, f.share); err != nil {
		t.Fatal(err)
	}
	var err error
	f.runtime, err = f.startup.NewShareLinkRedeemer(f.store)
	if err != nil {
		t.Fatal(err)
	}
}
func (f *sharePlaybackMainFixture) link(t *testing.T) *models.ShareLink {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "http://synthetic.test/api/share/create", nil)
	checked, release, err := f.pins.Authenticate(request, f.base.session.Token, "")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	link, err := f.creator.Create(checked, f.base.profile.ID, map[string]string{"sourcePath": "synthetic:main-share", "title": "Synthetic Main source"}, time.Hour, 8, "")
	if err != nil {
		t.Fatal("create original Main delegation", err)
	}
	return link
}
func (f *sharePlaybackMainFixture) redeem(t *testing.T, link *models.ShareLink) *datastore.ShareLinkRedemption {
	t.Helper()
	result, err := f.runtime.Redeem(httptest.NewRequest(http.MethodGet, "http://synthetic.test/share/"+link.Token, nil), link.Token)
	if err != nil {
		t.Fatal("actual public Main redemption", err)
	}
	return result
}
func mainShareOpen(t *testing.T, h *handlers.ShareHandler, token string, want int) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.Open(w, httptest.NewRequest(http.MethodGet, "http://synthetic.test/share/"+token, nil))
	if w.Code != want {
		t.Fatalf("public share status=%d, want=%d", w.Code, want)
	}
	return w
}

func TestSharePlaybackMainInstallation(t *testing.T) {
	for _, kind := range []string{"original-startup-factory", "pg-missing-dependency-fail-closed", "foreign-original-store", "file-legacy-constructor", "actual-shared-api-cookie-public-wiring", "original-runtime-close-joins"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "file-legacy-constructor" {
				video := &handlers.VideoHandler{}
				if err := configureManagedSharePlayback(nil, nil, nil, video, nil); err != nil {
					t.Fatal(err)
				}
				request := httptest.NewRequest(http.MethodGet, "http://synthetic.test/api/video/stream", nil)
				// This DATA only tests the explicit non-PG constructor branch, not PG authority.
				request = request.WithContext(context.WithValue(request.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
				reached := false
				video.ManagedSharePlaybackEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true; w.WriteHeader(204) })).ServeHTTP(httptest.NewRecorder(), request)
				if !reached {
					t.Fatal("explicit file mode unexpectedly selected unavailable PG runtime")
				}
				return
			}
			f := newSharePlaybackMainFixture(t)
			switch kind {
			case "original-startup-factory":
				f.install(t)
				again, err := f.startup.NewShareLinkRedeemer(f.store)
				if err != nil || again != f.runtime || !f.pins.SharePlaybackReady(f.runtime) {
					t.Fatal("Main did not install the single original runtime", err)
				}
				link := f.link(t)
				result := f.redeem(t, link)
				if result.Session.IsMaster || result.Session.Scope != models.SessionScopeStream || result.Session.ScopeResource != "synthetic:main-share" || result.Session.AccountID != f.base.session.AccountID {
					t.Fatal("Main minted broader or different source authority")
				}
			case "pg-missing-dependency-fail-closed":
				if err := configureManagedSharePlayback(nil, f.store, f.pins, f.video, f.share); err == nil {
					t.Fatal("missing managed startup accepted")
				}
				mainShareOpen(t, f.share, "missing-original", http.StatusServiceUnavailable)
				scoped, err := f.base.sessions.CreateScopedWithResource(f.base.session.AccountID, false, "synthetic-main", "127.0.0.1", time.Hour, models.SessionScopeStream, "synthetic:main-share")
				if err != nil {
					t.Fatal(err)
				}
				if _, release, err := f.pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), scoped.Token, ""); err == nil {
					release()
					t.Fatal("missing managed verifier selected legacy stream admission")
				}
			case "foreign-original-store":
				other := newPublicPinWebFixture(t)
				foreignPins := profileunlock.NewPGProfilePins(other.store, other.sessions)
				if err := configureManagedSharePlayback(f.startup, other.store, foreignPins, f.video, f.share); err == nil {
					t.Fatal("foreign store selected original startup authority")
				}
				mainShareOpen(t, f.share, "foreign-original", http.StatusServiceUnavailable)
			case "actual-shared-api-cookie-public-wiring":
				f.install(t)
				admin := f.base.admin(t)
				admin.SetPGProfilePins(f.pins)
				body, err := json.Marshal(map[string]any{"params": map[string]string{"sourcePath": "synthetic:main-share", "profileId": f.base.profile.ID}, "maxUses": 8})
				if err != nil {
					t.Fatal(err)
				}
				apiCreate := f.share.BoundedCreateEntry(api.AccountAuthMiddleware(f.base.sessions, nil, f.pins)(http.HandlerFunc(f.share.Create)))
				for _, entry := range []string{"api", "admin", "account"} {
					request := f.base.cookieRequest(http.MethodPost, "/"+entry+"/api/share/create", string(body), "application/json")
					var serve http.Handler
					if entry == "api" {
						request.Header.Set("Authorization", "Bearer "+f.base.session.Token)
						request.URL.Path = "/api/share/create"
						serve = apiCreate
					} else {
						serve = admin.RequireShareCreationAuth(f.share)
					}
					w := httptest.NewRecorder()
					serve.ServeHTTP(w, request)
					if w.Code != http.StatusOK && w.Code != http.StatusCreated {
						t.Fatalf("actual %s creation status=%d", entry, w.Code)
					}
					var created struct {
						Token string `json:"token"`
					}
					if json.Unmarshal(w.Body.Bytes(), &created) != nil || created.Token == "" {
						t.Fatal("actual creator did not return link token")
					}
					opened := mainShareOpen(t, f.share, created.Token, http.StatusSeeOther)
					location, err := url.Parse(opened.Header().Get("Location"))
					if err != nil {
						t.Fatal(err)
					}
					token := location.Query().Get("token")
					if token == "" || location.Query().Get("profileId") != f.base.profile.ID {
						t.Fatal("public redemption lost actual bound profile/session")
					}
					checked, release, err := f.pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), token, "")
					if err != nil {
						t.Fatal(err)
					}
					session, ok := auth.GetSession(checked)
					release()
					if !ok || session.IsMaster || session.AccountID != f.base.session.AccountID || session.ScopeResource != "synthetic:main-share" {
						t.Fatal("shared entries did not use the same restricted original runtime")
					}
				}
			case "original-runtime-close-joins":
				f.install(t)
				link := f.link(t)
				result := f.redeem(t, link)
				request, release, err := f.pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), result.Session.Token, "")
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				owner, err := f.pins.CaptureSharePlaybackOwner(request, f.base.profile.ID, result.Session.ScopeResource)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := owner.Handoff(request)
				if err != nil {
					owner.Close()
					t.Fatal(err)
				}
				// This actual original source consumer ends only after its held work returns.
				work := make(chan struct{})
				consumerDone := make(chan struct{})
				go func() { defer close(consumerDone); <-work; lease.Close() }()
				t.Cleanup(func() {
					select {
					case <-work:
					default:
						close(work)
					}
					<-consumerDone
					owner.Close()
				})
				stopped := make(chan error, 1)
				go func() { stopped <- f.startup.Close() }()
				select {
				case <-lease.Context().Done():
				case <-time.After(5 * time.Second):
					t.Fatal("Main did not signal original consumer")
				}
				select {
				case <-stopped:
					t.Fatal("Main returned before original source consumer ended")
				case <-time.After(20 * time.Millisecond):
				}
				close(work)
				<-consumerDone
				select {
				case err := <-stopped:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Main did not join original consumer")
				}
				owner.Close()
				if _, err = f.startup.NewShareLinkRedeemer(f.store); err == nil {
					t.Fatal("closed startup issued new runtime")
				}
				if _, err = f.runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token); !errors.Is(err, datastore.ErrSharePlaybackUnavailable) {
					t.Fatal("closed runtime unexpectedly available")
				}
			}
		})
	}
}
