//go:build linux

package profileaccess_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/profileaccess"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/profileunlock"
)

type partyAccessFixture struct {
	store                   *datastore.DataStore
	manager                 *config.Manager
	security                *config.ManagedSecurity
	coordinator             *settingscommit.Coordinator
	runtime                 *datastore.WatchRoomAuthority
	pins                    *profileunlock.PGProfilePins
	request                 *http.Request
	owner                   *profileunlock.WatchPartyPlaybackOwner
	profile, resource, full string
}

func partyAccessPG(t *testing.T) *partyAccessFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("own synthetic PostgreSQL source fixture required")
	}
	parsed, e := url.Parse(raw)
	if e != nil || parsed.Scheme != "postgresql" || parsed.Host != "" || parsed.User == nil || !strings.HasPrefix(parsed.User.Username(), "synthetic_") || !strings.HasPrefix(parsed.Query().Get("host"), "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("synthetic persistent Unix PostgreSQL only")
	}
	admin, e := pgxpool.New(t.Context(), raw)
	if e != nil {
		t.Fatal("connect own synthetic fixture")
	}
	schema := fmt.Sprintf("synthetic_party_resource_%d", time.Now().UnixNano())
	roleCreated, schemaCreated := false, false
	f := &partyAccessFixture{profile: "synthetic-resource-profile", resource: "synthetic:resource-party.wav", full: "synthetic-original-resource-full"}
	var objects *settingscommit.LockObjects
	t.Cleanup(func() {
		if f.runtime != nil {
			f.runtime.Close()
		}
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		if objects != nil {
			_ = objects.Close()
		}
		if f.store != nil {
			f.store.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if schemaCreated {
			if _, e := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); e != nil {
				t.Error("drop own resource schema")
			}
		}
		if roleCreated {
			if _, e := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{schema}.Sanitize()); e != nil {
				t.Error("drop own resource role")
			}
		}
		admin.Close()
	})
	if _, e = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{schema}.Sanitize()+" NOLOGIN"); e != nil {
		t.Fatal(e)
	}
	roleCreated = true
	if _, e = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	schemaCreated = true
	q := parsed.Query()
	q.Set("search_path", pgx.Identifier{schema}.Sanitize()+",pg_catalog")
	q.Set("options", "-c role="+schema)
	parsed.RawQuery = q.Encode()
	f.store, e = datastore.New(t.Context(), parsed.String())
	if e != nil {
		t.Fatal("actual current resource migrations")
	}
	if _, e = f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('synthetic-resource-actor','Synthetic resource actor','synthetic-unused-password');INSERT INTO users(id,account_id,name,allow_share_links)VALUES('synthetic-resource-profile','synthetic-resource-actor','Synthetic resource profile',true);INSERT INTO user_settings_versions(user_id,generation)VALUES('synthetic-resource-profile',0) ON CONFLICT(user_id)DO NOTHING`); e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	full := models.Session{Token: f.full, AccountID: "synthetic-resource-actor", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if e = f.store.Sessions().Create(t.Context(), &full); e != nil {
		t.Fatal(e)
	}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = parsed.String()
	if e = f.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	objects, e = settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if e != nil {
		t.Fatal(e)
	}
	f.coordinator, e = settingscommit.NewForPool(t.Context(), f.store.Pool(), objects)
	if e != nil {
		t.Fatal(e)
	}
	guard, e := f.coordinator.TryWrite(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.coordinator.Arm(guard); e != nil {
		_ = guard.Close()
		t.Fatal(e)
	}
	f.security, e = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = guard.Close()
	if e != nil {
		t.Fatal(e)
	}
	f.runtime, e = datastore.NewWatchRoomAuthority(t.Context(), f.store, f.manager, f.security, f.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	f.pins = profileunlock.NewPGProfilePins(f.store)
	if e = f.pins.SetManagedWatchPartyPlayback(f.runtime); e != nil {
		t.Fatal(e)
	}
	original, release, e := f.pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/users/"+f.profile+"/watch-rooms", nil), f.full, "")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	room, e := f.runtime.CreateRoom(original, f.profile, models.WatchRoomCreate{Title: "Synthetic stored resource", MediaType: "movie", ItemID: "synthetic-item", Params: map[string]string{}, Capabilities: models.WatchRoomClientCapabilities{NativePlayback: true, StateSync: true, ProtocolVersion: 1}})
	if e != nil {
		t.Fatal(e)
	}
	invite, e := f.runtime.ReplaceExternalInvite(original, f.profile, room.ID)
	if e != nil {
		t.Fatal(e)
	}
	joined, e := f.runtime.JoinInvitation(httptest.NewRequest("POST", "http://synthetic.test/watch-party/"+invite.Token, nil), invite.Token, false, "Synthetic resource guest", "")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.runtime.BindExternalSource(original, f.profile, room.ID, f.resource, map[string]string{"profileId": f.profile, "titleId": "synthetic-item"}); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Pool().Exec(t.Context(), `UPDATE watch_rooms SET status='playing',waiting_for_ready=false WHERE id=$1`, room.ID); e != nil {
		t.Fatal(e)
	}
	guest, stop, e := f.runtime.AuthenticateGuestRequest(httptest.NewRequest("POST", "http://synthetic.test/api/watch-party/room/"+room.ID+"/playback-grant", nil), joined.Session.Token, room.ID)
	if e != nil {
		t.Fatal(e)
	}
	grant, e := f.runtime.GrantPartyPlayback(guest, room.ID)
	stop()
	if e != nil {
		t.Fatal(e)
	}
	checked, stop, e := f.pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), grant.Session.Token, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(stop)
	owner, e := f.pins.CaptureWatchPartyPlaybackOwner(checked, f.profile, f.resource)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(owner.Close)
	f.owner = owner
	f.request, e = owner.BindResourceRequest(checked)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func TestWatchPartyStoredResourceOwner(t *testing.T) {
	for _, kind := range []string{"actual-stored-resource", "omitted-selector", "substituted-profile", "substituted-resource", "copied-owner", "full-share-behavior-unchanged"} {
		t.Run(kind, func(t *testing.T) {
			f := partyAccessPG(t)
			r := f.request
			profile, resource := f.profile, f.resource
			if kind == "full-share-behavior-unchanged" {
				full, release, e := f.pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil), f.full, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
				reached := false
				profileaccess.CurrentMiddleware(f.pins.Owns)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					reached = profileaccess.CheckResource(w, r, f.profile, "synthetic:full-owned-metadata")
					if reached {
						w.WriteHeader(204)
					}
				})).ServeHTTP(httptest.NewRecorder(), full)
				if !reached {
					t.Fatal("new party key changed ordinary current full ownership")
				}
				creator, e := datastore.NewShareLinkCreator(t.Context(), f.store, f.manager, f.security, f.coordinator)
				if e != nil {
					t.Fatal(e)
				}
				link, e := creator.Create(full, f.profile, map[string]string{"profileId": f.profile, "sourcePath": "synthetic:separate-share.wav"}, time.Hour, 2, "")
				if e != nil {
					t.Fatal(e)
				}
				redeemer, e := datastore.NewShareLinkRedeemer(t.Context(), f.store, f.manager, f.security, f.coordinator)
				if e != nil {
					t.Fatal(e)
				}
				defer redeemer.Close()
				result, e := redeemer.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
				if e != nil {
					t.Fatal(e)
				}
				actual, stop, e := f.store.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), result.Session.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer stop()
				owner, e := redeemer.CaptureSharePlaybackOwner(actual, f.profile, "synthetic:separate-share.wav")
				if e != nil {
					t.Fatal(e)
				}
				defer owner.Close()
				if !profileaccess.CheckResource(httptest.NewRecorder(), profileaccess.WithSharePlaybackOwner(actual, owner), f.profile, "synthetic:separate-share.wav") {
					t.Fatal("party key changed genuine separate share proof")
				}
				return
			}
			switch kind {
			case "omitted-selector":
				profile = ""
			case "substituted-profile":
				profile = "synthetic-other-profile"
			case "substituted-resource":
				resource = "synthetic:other-resource.wav"
			case "copied-owner":
				raw, _ := json.Marshal(f.owner)
				var copied datastore.WatchPartyPlaybackOwner
				if e := json.Unmarshal(raw, &copied); e == nil {
					t.Fatal("opaque source DATA accepted deserialization")
				}
				r = profileaccess.WithWatchPartyPlaybackOwner(r, &copied)
			}
			w := httptest.NewRecorder()
			allowed := profileaccess.CheckResource(w, r, profile, resource)
			if kind == "actual-stored-resource" {
				if !allowed {
					t.Fatal("actual original stored resource denied", w.Code)
				}
			} else if allowed || w.Code != 404 {
				t.Fatal("stored resource selector/opaque-copy bypass", kind, w.Code)
			}
		})
	}
}
