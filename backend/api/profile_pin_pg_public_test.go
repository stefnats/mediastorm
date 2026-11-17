package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/hiddenitems"
	"novastream/services/metadata"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
	"novastream/services/watchrooms"
)

// The actual registered API and migrated PostgreSQL services surround a counted
// metadata-only cache port. Password login, cookie carriers and media streaming
// have separate tests; this fixture never permits provider HTTP.
type publicPGPinFixture struct {
	t                     *testing.T
	ds                    *datastore.DataStore
	router                *mux.Router
	users                 *users.Service
	sessions              *sessions.Service
	account, other        string
	open, locked, foreign string
	facts                 *collectionAccessMetadata
	enforce               atomic.Bool
}

func newPublicPGPinFixture(t *testing.T) *publicPGPinFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL API fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Path, "/synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("synthetic local Unix-socket PostgreSQL fixture required")
	}
	guard := &watchingAccessDenyTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = guard
	t.Cleanup(func() {
		http.DefaultTransport = old
		if guard.calls.Load() != 0 {
			t.Error("registered PG API attempted provider HTTP")
		}
	})
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("connect synthetic fixture")
	}
	schema := fmt.Sprintf("synthetic_pin_api_%d", time.Now().UnixNano())
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own API schema")
	}
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("drop own API schema")
		}
		admin.Close()
	})
	q := u.Query()
	q.Set("search_path", pgx.Identifier{schema}.Sanitize()+",pg_catalog")
	u.RawQuery = q.Encode()
	store, err := datastore.New(t.Context(), u.String())
	if err != nil {
		t.Fatal("migrate own API schema")
	}
	t.Cleanup(store.Close)
	f := &publicPGPinFixture{t: t, ds: store}
	f.enforce.Store(true)
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-pg-api-bootstrap-password")
	accountService, err := accounts.NewServiceWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	for i, target := range []*string{&f.account, &f.other} {
		account, err := accountService.Create([]string{"synthetic-pin-api-owner", "synthetic-pin-api-neighbour"}[i], "synthetic-api-password")
		if err != nil {
			t.Fatal(err)
		}
		*target = account.ID
	}
	f.sessions, err = sessions.NewServiceWithStore(store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.users, err = users.NewServiceWithStore(store, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		target      *string
		owner, name string
	}{
		{&f.open, f.account, "Synthetic open"}, {&f.locked, f.account, "Synthetic locked"}, {&f.foreign, f.other, "Synthetic foreign"},
	} {
		profile, err := f.users.CreateForAccount(item.owner, item.name)
		if err != nil {
			t.Fatal(err)
		}
		*item.target = profile.ID
	}
	if _, err := f.users.SetPin(f.locked, "synthetic-é"); err != nil {
		t.Fatal(err)
	}
	// Exercise the supported historical PinLength migration with a matching
	// freshly loaded revision; unrelated profiles retain their own revisions.
	f.exec(`UPDATE users SET pin_length=0 WHERE id=$1`, f.locked)
	f.users, err = users.NewServiceWithStore(store, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	f.facts = &collectionAccessMetadata{snapshot: &metadata.CachedCollectionSnapshot{
		Version: 1, Provider: "tmdb", CollectionID: 77, Name: "Synthetic cached collection", MetadataLanguage: "en-US", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
		Movies: []metadata.CachedCollectionMovie{{ID: 42, Name: "Synthetic observed film", ReleaseDate: "2020-01-01"}},
	}}
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	list.SetMetadataService(f.facts)
	list.SetMetadataHandler(meta)
	hidden, err := hiddenitems.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	list.SetHiddenItemsService(hidden)
	legacy := profileunlock.NewService(store.ProfileUnlocks())
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: f.enforce.Load(), ProfileUnlockTTLHours: 1}
	}
	f.router = mux.NewRouter()
	Register(f.router, nil, meta,
		nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{},
		handlers.NewUsersHandler(f.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		accountService, f.sessions, f.users, nil, nil, nil, "", nil,
		WithProfileUnlocks(legacy, policy), WithPGProfilePins(profileunlock.NewPGProfilePins(store, f.sessions)))
	return f
}

func TestPGPinPublicRegisteredPartyPlaybackKeepsExistingRoomAuthority(t *testing.T) {
	f := newPublicPGPinFixture(t)
	repo := f.ds.WatchRooms()
	rooms := watchrooms.New(repo, f.users, nil)
	var validations atomic.Int32
	validator := func(ctx context.Context, room, guest, resource string) error {
		validations.Add(1)
		return rooms.ValidateExternalPlayback(ctx, room, guest, resource)
	}
	f.sessions.SetWatchPartyPlaybackValidator(validator)
	const resource = "/synthetic-party.webm"
	for _, mode := range []string{"current room", "wrong requested resource", "revoked guest", "expired room", "ended room", "changed room source", "missing validator"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now().UTC()
			roomID := fmt.Sprintf("synthetic-party-%d", now.UnixNano())
			room := &models.WatchRoom{ID: roomID, CreatorProfileID: f.open, Title: "Synthetic party", MediaType: "movie", ItemID: "synthetic-film", Params: map[string]string{}, Status: models.WatchRoomStatusPlaying, Revision: 1, AnchorUpdatedAt: now, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
			if err := repo.Create(t.Context(), room, nil, "", models.WatchRoomClientCapabilities{}); err != nil {
				t.Fatal("create own actual room")
			}
			if err := repo.JoinExternalGuest(t.Context(), roomID, "synthetic-guest", "Synthetic guest", "", models.WatchRoomClientCapabilities{}, now); err != nil {
				t.Fatal("join own actual guest")
			}
			bound, err := repo.BindExternalSource(t.Context(), roomID, f.open, resource, map[string]string{}, now)
			if err != nil || !bound {
				t.Fatal("bind own actual room resource")
			}
			grant, err := f.sessions.CreateWatchPartyPlayback(f.account, "synthetic party agent", "127.0.0.1", time.Hour, roomID, "synthetic-guest", resource)
			if err != nil {
				t.Fatal(err)
			}
			requested, want := resource, 401
			switch mode {
			case "current room":
				want = 503
			case "wrong requested resource":
				requested, want = "/synthetic-other.webm", 403
			case "revoked guest":
				if err := repo.LeaveExternalGuest(t.Context(), roomID, "synthetic-guest"); err != nil {
					t.Fatal(err)
				}
			case "expired room":
				f.exec(`UPDATE watch_rooms SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, roomID)
			case "ended room":
				f.exec(`UPDATE watch_rooms SET status='ended' WHERE id=$1`, roomID)
			case "changed room source":
				f.exec(`UPDATE watch_room_external_sources SET resource='/synthetic-other.webm' WHERE room_id=$1`, roomID)
			case "missing validator":
				f.sessions.SetWatchPartyPlaybackValidator(nil)
				defer f.sessions.SetWatchPartyPlaybackValidator(validator)
			}
			before := validations.Load()
			w := f.do("HEAD", "/api/video/stream?path="+url.QueryEscape(requested), grant.Token, "", nil)
			if w.Code != want {
				t.Fatalf("registered party gate got %d, want %d", w.Code, want)
			}
			if mode == "current room" && (!strings.Contains(w.Body.String(), "stream provider not configured") || validations.Load() <= before) {
				t.Fatal("normalized party scope did not reach the actual video consumer")
			}
		})
	}
	t.Run("ordinary resource share does not require a party validator", func(t *testing.T) {
		f.sessions.SetWatchPartyPlaybackValidator(nil)
		grant, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic share agent", "127.0.0.1", time.Hour, models.SessionScopeStream, resource)
		if err != nil {
			t.Fatal(err)
		}
		w := f.do("HEAD", "/api/video/stream?path="+url.QueryEscape(resource), grant.Token, "", nil)
		if w.Code != 503 || !strings.Contains(w.Body.String(), "stream provider not configured") {
			t.Fatal("ordinary existing resource shares were narrowed by the party adapter")
		}
	})
}

func (f *publicPGPinFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.ds.Pool().Exec(f.t.Context(), sql, args...); err != nil {
		f.t.Fatal("own API fixture statement failed")
	}
}
func (f *publicPGPinFixture) session(master bool) string {
	f.t.Helper()
	session, err := f.sessions.Create(f.account, master, "synthetic API agent", "127.0.0.1")
	if err != nil {
		f.t.Fatal(err)
	}
	return session.Token
}
func (f *publicPGPinFixture) do(method, target, token, unlock string, body any) *httptest.ResponseRecorder {
	f.t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			f.t.Fatal(err)
		}
	}
	r := httptest.NewRequest(method, target, bytes.NewReader(data))
	r.Header.Set("X-Client-ID", "synthetic-pg-api-client")
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if unlock != "" {
		r.Header.Set("X-Profile-Unlock", unlock)
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func TestPGPinPublicRegisteredCurrentStatePrecedesCachedAbsence(t *testing.T) {
	f := newPublicPGPinFixture(t)
	for _, item := range []struct {
		name, profile, token string
		want                 int
	}{
		{"unauthenticated", f.open, "", 401}, {"invalid session", f.open, "synthetic-invalid-session", 401},
		{"owned open", f.open, f.session(false), 200}, {"owned locked", f.locked, f.session(false), 403},
		{"foreign", f.foreign, f.session(false), 404}, {"missing", "synthetic-missing-profile", f.session(false), 404},
	} {
		t.Run(item.name, func(t *testing.T) {
			before := f.facts.snapshots.Load()
			w := f.do("GET", collectionAccessTarget(item.profile), item.token, "", nil)
			if w.Code != item.want {
				t.Fatalf("registered current-state status: got %d, want %d", w.Code, item.want)
			}
			if item.want != 200 && f.facts.snapshots.Load() != before {
				t.Fatal("denied request entered cache/provider work")
			}
		})
	}
	t.Run("cached no PIN cannot bypass a current PIN", func(t *testing.T) {
		f.exec(`UPDATE users SET pin_hash=(SELECT pin_hash FROM users WHERE id=$1),pin_length=0 WHERE id=$2`, f.locked, f.open)
		defer f.exec(`UPDATE users SET pin_hash='',pin_length=0 WHERE id=$1`, f.open)
		if f.users.HasPin(f.open) {
			t.Fatal("cached no-PIN negative control was not stale")
		}
		before := f.facts.snapshots.Load()
		w := f.do("GET", collectionAccessTarget(f.open), f.session(false), "", nil)
		if w.Code != 403 || !strings.Contains(w.Body.String(), "profile_locked") || f.facts.snapshots.Load() != before {
			t.Fatal("current PIN was bypassed by cached absence")
		}
	})
	t.Run("current no PIN overrides stale cached PIN", func(t *testing.T) {
		f.exec(`UPDATE users SET pin_hash='' WHERE id=$1`, f.locked)
		if !f.users.HasPin(f.locked) {
			t.Fatal("cached PIN positive control was not stale")
		}
		if w := f.do("GET", collectionAccessTarget(f.locked), f.session(false), "", nil); w.Code != 200 {
			t.Fatal("current unprotected profile was denied by the old cache")
		}
	})
}

func TestPGPinPublicRegisteredIssuanceAcknowledgesOwnRevision(t *testing.T) {
	f := newPublicPGPinFixture(t)
	token := f.session(false)
	target := "/api/users/" + f.locked + "/pin/verify"
	wrong := f.do("POST", target, token, "", map[string]string{"pin": " synthetic-é "})
	if wrong.Code != 401 {
		t.Fatal("raw PIN verification silently trimmed the supplied credential")
	}
	response := f.do("POST", target, token, "", map[string]string{"pin": "synthetic-é"})
	var verified handlers.VerifyPinResponse
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &verified) != nil || !verified.Valid || !strings.HasPrefix(verified.UnlockToken, "pu_") {
		t.Fatal("actual registered PIN verification did not release a token")
	}
	var length int
	if err := f.ds.Pool().QueryRow(t.Context(), `SELECT pin_length FROM users WHERE id=$1`, f.locked).Scan(&length); err != nil || length != len("synthetic-é") {
		t.Fatal("known commit did not migrate the byte length")
	}
	profile, ok := f.users.Get(f.locked)
	if !ok || profile.PinLength != length {
		t.Fatal("targeted own users revision was not acknowledged")
	}
	if w := f.do("GET", collectionAccessTarget(f.locked), token, verified.UnlockToken, nil); w.Code != 200 {
		t.Fatal("actual issued token did not authorize its original session")
	}
	if w := f.do("GET", collectionAccessTarget(f.locked), f.session(false), verified.UnlockToken, nil); w.Code != 403 {
		t.Fatal("issued token crossed sessions")
	}
	if err := f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), f.locked); err != nil {
		t.Fatal(err)
	}
	before := f.facts.snapshots.Load()
	if w := f.do("GET", collectionAccessTarget(f.locked), token, verified.UnlockToken, nil); w.Code != 403 || f.facts.snapshots.Load() != before {
		t.Fatal("revoked token or legacy cache reopened provider work")
	}
}

func TestPGPinPublicRegisteredOriginalAuthorityAndPolicy(t *testing.T) {
	f := newPublicPGPinFixture(t)
	t.Run("first client binding published before ownership", func(t *testing.T) {
		token := f.session(false)
		if w := f.do("GET", collectionAccessTarget(f.open), token, "", nil); w.Code != 200 {
			t.Fatal("confirmed first client could not enter actual registered route")
		}
		var client string
		if err := f.ds.Pool().QueryRow(t.Context(), `SELECT client_id FROM sessions WHERE token=$1`, token).Scan(&client); err != nil || client != "synthetic-pg-api-client" {
			t.Fatal("actual route did not commit the first client")
		}
	})
	t.Run("actual promoted role overrides cached regular session", func(t *testing.T) {
		token := f.session(false)
		f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
		defer f.exec(`UPDATE accounts SET is_master=false WHERE id=$1`, f.account)
		if w := f.do("GET", collectionAccessTarget(f.foreign), token, "", nil); w.Code != 200 {
			t.Fatal("actual promoted actor remained blocked by the old session role")
		}
	})
	t.Run("actual demotion overrides cached master session", func(t *testing.T) {
		if w := f.do("GET", collectionAccessTarget(f.foreign), f.session(true), "", nil); w.Code != 404 {
			t.Fatal("cached master session overrode the current regular actor")
		}
	})
	t.Run("actual expired session without cache revocation", func(t *testing.T) {
		token := f.session(false)
		f.exec(`UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, token)
		if w := f.do("GET", collectionAccessTarget(f.open), token, "", nil); w.Code != 401 {
			t.Fatal("cached session bypassed current expiry")
		}
	})
	t.Run("actual expired actor without cache revocation", func(t *testing.T) {
		token := f.session(false)
		f.exec(`UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, f.account)
		defer f.exec(`UPDATE accounts SET expires_at=NULL WHERE id=$1`, f.account)
		if w := f.do("GET", collectionAccessTarget(f.open), token, "", nil); w.Code != 401 {
			t.Fatal("cached actor bypassed current expiry")
		}
	})
	t.Run("policy off preserves ownership and scope", func(t *testing.T) {
		f.enforce.Store(false)
		defer f.enforce.Store(true)
		token := f.session(false)
		if w := f.do("GET", collectionAccessTarget(f.locked), token, "", nil); w.Code != 200 {
			t.Fatal("PIN policy-off setting was ignored")
		}
		if w := f.do("GET", collectionAccessTarget(f.foreign), token, "", nil); w.Code != 404 {
			t.Fatal("policy off disabled ownership")
		}
		scoped, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic agent", "127.0.0.1", time.Hour, models.SessionScopeStream, "https://synthetic.invalid/allowed-resource")
		if err != nil {
			t.Fatal(err)
		}
		if w := f.do("GET", collectionAccessTarget(f.open), scoped.Token, "", nil); w.Code != 403 {
			t.Fatal("policy off widened a delegated stream session")
		}
	})
	t.Run("preflight remains public", func(t *testing.T) {
		if w := f.do("OPTIONS", "/api/users/"+f.locked+"/pin/verify", "", "", nil); w.Code != 200 {
			t.Fatal("current PostgreSQL selection broke public preflight")
		}
	})
	t.Run("actual me route preserves bridge-only token rejection", func(t *testing.T) {
		if w := f.do("GET", "/api/auth/me", f.session(false), "", nil); w.Code != 200 {
			t.Fatal("normal full session cannot read its own account")
		}
		bridge, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic bridge", "127.0.0.1", time.Hour, models.SessionScopeJellyfin, "synthetic-bridge-resource")
		if err != nil {
			t.Fatal(err)
		}
		w := f.do("GET", "/api/auth/me", bridge.Token, "", nil)
		if w.Code != 401 || strings.Contains(w.Body.String(), "username") {
			t.Fatal("bridge-only credentials became account credentials on Me")
		}
	})
	t.Run("missing current storage never selects the old no-PIN cache", func(t *testing.T) {
		token := f.session(false)
		f.exec(`ALTER TABLE users RENAME COLUMN pin_hash TO synthetic_unavailable_pin_hash`)
		defer f.exec(`ALTER TABLE users RENAME COLUMN synthetic_unavailable_pin_hash TO pin_hash`)
		before := f.facts.snapshots.Load()
		w := f.do("GET", collectionAccessTarget(f.open), token, "", nil)
		if w.Code < 500 || f.facts.snapshots.Load() != before {
			t.Fatal("unavailable PG state fell back to an allowing cached profile")
		}
	})
}
