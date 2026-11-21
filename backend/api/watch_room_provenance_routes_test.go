//go:build linux

package api

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http/httptest"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/internal/settingsbootstrap"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/watchrooms"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type watchRoomRouteFixture struct {
	base    *publicPGPinFixture
	startup *settingsbootstrap.Startup
	store   *datastore.DataStore
	rooms   *handlers.WatchRoomsHandler
	party   *handlers.WatchPartyHandler
	router  *mux.Router
	token   string
}

func watchRoomRegisteredPG(t *testing.T, missing bool) *watchRoomRouteFixture {
	t.Helper()
	base := newPublicPGPinFixture(t)
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = base.ds.Pool().Config().ConnConfig.ConnString()
	settings.Server.ExternalBackendURL = "https://synthetic.example/api"
	if e := manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	startup, e := settingsbootstrap.Begin(t.Context(), manager, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := startup.Close(); e != nil {
			t.Error(e)
		}
	})
	store, e := startup.Migrate(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = startup.Finish(); e != nil {
		t.Fatal(e)
	}
	if _, e = store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, base.open); e != nil {
		t.Fatal(e)
	}
	accountService, e := accounts.NewServiceWithStore(store)
	if e != nil {
		t.Fatal(e)
	}
	service := watchrooms.New(store.WatchRooms(), base.users, accountService)
	rooms, party := handlers.NewWatchRoomsHandler(service), handlers.NewWatchPartyHandler(service, base.sessions, "")
	party.SetSettingsProvider(manager)
	selected := startup
	if missing {
		selected = nil
	}
	e = ConfigureManagedWatchRoomAuthority(selected, store, rooms, party, "")
	if missing && e == nil {
		t.Fatal("missing actual startup selected managed authority")
	}
	if !missing && e != nil {
		t.Fatal(e)
	}
	pins := profileunlock.NewPGProfilePins(store, base.sessions)
	meta := handlers.NewMetadataHandler(base.facts, nil)
	meta.UsersService = base.users
	list := handlers.NewDisplayListHandler(nil, nil, base.users)
	router := mux.NewRouter()
	Register(router, nil, meta, nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{}, handlers.NewUsersHandler(base.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		accountService, base.sessions, base.users, nil, rooms, party, "", nil,
		WithPGProfilePins(pins), WithProfileUnlocks(profileunlock.NewService(store.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} }))
	session, e := base.sessions.Create(base.account, false, "synthetic-room-api", "loopback")
	if e != nil {
		t.Fatal(e)
	}
	return &watchRoomRouteFixture{base, startup, store, rooms, party, router, session.Token}
}
func watchRoomRouteInput() models.WatchRoomCreate {
	return models.WatchRoomCreate{Title: "Synthetic registered room", MediaType: "movie", ItemID: "synthetic-title", Params: map[string]string{}, Capabilities: models.WatchRoomClientCapabilities{NativePlayback: true, StateSync: true, ProtocolVersion: 1}}
}
func (f *watchRoomRouteFixture) call(t *testing.T, path string, value any) *httptest.ResponseRecorder {
	t.Helper()
	raw, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	r := httptest.NewRequest("POST", "http://synthetic.test"+path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+f.token)
	r.Header.Set("X-Client-ID", "synthetic-room-api-client")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}
func TestWatchRoomRegisteredAuthority(t *testing.T) {
	for _, kind := range []string{"actual-main-composition", "registered-create", "registered-invite", "registered-source", "missing-factory-denied", "scoped-token-denied", "body-selector-no-authority", "ordinary-native-share-disabled"} {
		t.Run(kind, func(t *testing.T) {
			f := watchRoomRegisteredPG(t, kind == "missing-factory-denied")
			path := "/api/users/" + f.base.open + "/watch-rooms"
			input := watchRoomRouteInput()
			if kind == "actual-main-composition" {
				first, e := f.startup.NewWatchRoomAuthority(f.store)
				if e != nil {
					t.Fatal(e)
				}
				again, e := f.startup.NewWatchRoomAuthority(f.store)
				if e != nil || again != first || !first.MatchesStore(f.store) {
					t.Fatal("actual Main composition did not retain single owned original runtime", e)
				}
			}
			if kind == "ordinary-native-share-disabled" {
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.base.open); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "scoped-token-denied" {
				session, e := f.base.sessions.CreateScopedWithResource(f.base.account, false, "synthetic-room-scope", "loopback", time.Hour, models.SessionScopeJellyfin, f.base.open)
				if e != nil {
					t.Fatal(e)
				}
				f.token = session.Token
			}
			if kind == "body-selector-no-authority" {
				input.Params = map[string]string{"profileId": f.base.foreign, "isMaster": "true", "accountId": f.base.other}
				path = "/api/users/" + f.base.foreign + "/watch-rooms"
			}
			w := f.call(t, path, input)
			if kind == "missing-factory-denied" || kind == "scoped-token-denied" || kind == "body-selector-no-authority" {
				want := 403
				if kind == "missing-factory-denied" {
					want = 503
				}
				if kind == "body-selector-no-authority" {
					want = 404
				}
				if w.Code != want {
					t.Fatal("registered current denial", kind, w.Code, w.Body.String())
				}
				var count int
				if e := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_rooms`).Scan(&count); e != nil || count != 0 {
					t.Fatal("registered denial created room", count, e)
				}
				return
			}
			if w.Code != 201 {
				t.Fatal("actual registered native create", w.Code, w.Body.String())
			}
			var room models.WatchRoom
			if json.Unmarshal(w.Body.Bytes(), &room) != nil || room.ID == "" {
				t.Fatal("missing original registered room")
			}
			if kind == "registered-invite" {
				w = f.call(t, path+"/"+room.ID+"/external-invite", map[string]any{})
				if w.Code != 201 || !strings.Contains(w.Body.String(), `"shortCode"`) {
					t.Fatal("actual registered invite", w.Code, w.Body.String())
				}
			}
			if kind == "registered-source" {
				w = f.call(t, path+"/"+room.ID+"/external-source", map[string]any{"source": "synthetic:registered-room", "params": map[string]string{"profileId": f.base.foreign, "isMaster": "true", "titleId": "synthetic-title"}})
				if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"ready":true}` {
					t.Fatal("actual registered source", w.Code, w.Body.String())
				}
				var source string
				if e := f.store.Pool().QueryRow(t.Context(), `SELECT source_profile_id FROM watch_room_source_provenance WHERE room_id=$1`, room.ID).Scan(&source); e != nil || source != f.base.open {
					t.Fatal("registered body selected foreign source authority", e)
				}
			}
			var owner string
			if e := f.store.Pool().QueryRow(t.Context(), `SELECT creator_account_id FROM watch_room_provenance WHERE room_id=$1`, room.ID).Scan(&owner); e != nil || owner != f.base.account {
				t.Fatal("actual Main route missed provenance writer", e)
			}
		})
	}
}
