//go:build linux

package settingsbootstrap

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
)

type nativeMaintenanceStartupFixture struct {
	boot      *bootstrapPGFixture
	startup   *Startup
	store     *datastore.DataStore
	authority *datastore.WatchRoomAuthority
	room      string
}

func nativeMaintenanceStartupPG(t *testing.T) *nativeMaintenanceStartupFixture {
	t.Helper()
	f := &nativeMaintenanceStartupFixture{boot: bootstrapPG(t)}
	settings := config.DefaultSettings()
	settings.Database.URL = f.boot.raw
	settings.Database.MaxOpenConns = 3
	settings.Database.MaxIdleConns = 1
	if e := f.boot.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	var e error
	f.startup, e = Begin(t.Context(), f.boot.manager, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := f.startup.Close(); e != nil {
			t.Error(e)
		}
	})
	f.store, e = f.startup.Migrate(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.startup.Finish(); e != nil {
		t.Fatal(e)
	}
	f.authority, e = f.startup.NewWatchRoomAuthority(f.store)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('synthetic-native-startup-actor','Synthetic native startup actor','synthetic-unused-password');INSERT INTO users(id,account_id,name)VALUES('synthetic-native-startup-profile','synthetic-native-startup-actor','Synthetic native startup profile');INSERT INTO user_settings_versions(user_id,generation)VALUES('synthetic-native-startup-profile',0) ON CONFLICT(user_id)DO NOTHING`); e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "synthetic-native-startup-full", AccountID: "synthetic-native-startup-actor", CreatedAt: now, ExpiresAt: now.Add(time.Hour), ClientID: "native-startup"}
	if e = f.store.Sessions().Create(t.Context(), &session); e != nil {
		t.Fatal(e)
	}
	request := httptest.NewRequest("POST", "http://synthetic.test/api/users/synthetic-native-startup-profile/watch-rooms", nil)
	request.Header.Set("X-Client-ID", "native-startup")
	actual, release, e := f.store.AuthenticateProfilePinRequest(request, session.Token, "native-startup")
	if e != nil {
		t.Fatal(e)
	}
	room, e := f.authority.CreateRoom(actual, "synthetic-native-startup-profile", models.WatchRoomCreate{Title: "Synthetic owned maintenance", MediaType: "movie", ItemID: "synthetic", Params: map[string]string{}, Capabilities: models.WatchRoomClientCapabilities{NativePlayback: true, StateSync: true, ProtocolVersion: 1}})
	release()
	if e != nil || room == nil {
		t.Fatal("actual original startup room", e)
	}
	f.room = room.ID
	return f
}
func nativeStartupAwait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case e := <-done:
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("actual original startup maintenance did not join")
		return nil
	}
}
func nativeStartupWaitSQL(t *testing.T, f *nativeMaintenanceStartupFixture) (int, int) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		var count, pid int
		e := f.boot.observer.QueryRow(t.Context(), `SELECT count(*),COALESCE(max(pid),0) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM watch_rooms%' AND query LIKE '%FOR UPDATE%'`, f.boot.name).Scan(&count, &pid)
		if e != nil {
			t.Fatal(e)
		}
		if count > 0 {
			return count, pid
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual startup maintenance SQL did not wait")
	return 0, 0
}

func TestNativeRoomStartupMaintenance(t *testing.T) {
	for _, kind := range []string{"actual-startup-original-authority", "repeated-start-single-worker", "missing-startup-denied", "foreign-store-denied", "closed-startup-denied", "actual-main-owned-maintenance", "shutdown-held-sql-cleanup", "shutdown-before-store-close"} {
		t.Run(kind, func(t *testing.T) {
			f := nativeMaintenanceStartupPG(t)
			if kind == "missing-startup-denied" {
				var absent *Startup
				if e := absent.StartWatchRoomMaintenance(f.store); e == nil {
					t.Fatal("nil startup launched owned maintenance")
				}
				return
			}
			if kind == "foreign-store-denied" {
				foreign, e := datastore.New(t.Context(), f.boot.raw)
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				if e = f.startup.StartWatchRoomMaintenance(foreign); e == nil {
					t.Fatal("foreign actual pool joined original startup")
				}
				return
			}
			if kind == "closed-startup-denied" {
				if e := f.startup.Close(); e != nil {
					t.Fatal(e)
				}
				if e := f.startup.StartWatchRoomMaintenance(f.store); e == nil || f.authority.Context().Err() == nil {
					t.Fatal("closed startup revived original runtime")
				}
				return
			}
			if kind == "actual-startup-original-authority" || kind == "actual-main-owned-maintenance" {
				if _, e := f.store.Pool().Exec(t.Context(), `INSERT INTO watch_rooms(id,creator_profile_id,title,media_type,item_id,expires_at)VALUES('synthetic-native-startup-expired','synthetic-native-startup-profile','Synthetic legacy expiry','movie','synthetic',now()-interval '1 minute')`); e != nil {
					t.Fatal(e)
				}
				if e := f.startup.StartWatchRoomMaintenance(f.store); e != nil {
					t.Fatal(e)
				}
				again, e := f.startup.NewWatchRoomAuthority(f.store)
				if e != nil || again != f.authority || !again.MatchesStore(f.store) {
					t.Fatal("actual Main factory substituted original maintenance owner", e)
				}
				until := time.Now().Add(5 * time.Second)
				ended := false
				for time.Now().Before(until) {
					var status string
					if e = f.boot.observer.QueryRow(t.Context(), `SELECT status FROM watch_rooms WHERE id='synthetic-native-startup-expired'`).Scan(&status); e != nil {
						t.Fatal(e)
					}
					if status == "ended" {
						ended = true
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !ended {
					t.Fatal("actual owned immediate maintenance never ran")
				}
				var original, legacy int
				e = f.boot.observer.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1 AND admission_kind='creator'),(SELECT count(*) FROM watch_room_native_admissions WHERE room_id='synthetic-native-startup-expired')`, f.room).Scan(&original, &legacy)
				if e != nil || original != 1 || legacy != 0 {
					t.Fatal("startup work lost original room or attested legacy DATA", e)
				}
				return
			}
			// A real independent SQL holder blocks the worker before startup Close.
			holder, e := f.boot.observer.Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			var closed chan error
			var entry *datastore.WatchRoomHTTPEntry
			t.Cleanup(func() {
				if entry != nil {
					entry.Close()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = holder.Rollback(ctx)
				if closed != nil {
					if e := nativeStartupAwait(t, closed); e != nil {
						t.Error(e)
					}
				}
			})
			if _, e = holder.Exec(t.Context(), `SELECT id FROM watch_rooms WHERE id=$1 FOR UPDATE`, f.room); e != nil {
				t.Fatal(e)
			}
			if e = f.startup.StartWatchRoomMaintenance(f.store); e != nil {
				t.Fatal(e)
			}
			count, pid := nativeStartupWaitSQL(t, f)
			if kind == "repeated-start-single-worker" {
				if e = f.startup.StartWatchRoomMaintenance(f.store); e != nil {
					t.Fatal(e)
				}
				time.Sleep(30 * time.Millisecond)
				count, _ = nativeStartupWaitSQL(t, f)
				if count != 1 {
					t.Fatal("repeated actual Start launched more than one SQL worker", count)
				}
				return
			}
			if kind == "shutdown-before-store-close" {
				entry, e = f.authority.OpenHTTPEntry(httptest.NewRequest("GET", "http://synthetic.test/api/users/synthetic-native-startup-profile/watch-rooms/"+f.room, nil))
				if e != nil {
					t.Fatal(e)
				}
			}
			closed = make(chan error, 1)
			go func() { closed <- f.startup.Close() }()
			if kind == "shutdown-before-store-close" {
				select {
				case <-entry.Context().Done():
				case <-time.After(5 * time.Second):
					t.Fatal("original ingress lifetime did not cancel with startup")
				}
				until := time.Now().Add(5 * time.Second)
				active := true
				for time.Now().Before(until) {
					if e = f.boot.observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')`, pid).Scan(&active); e != nil {
						t.Fatal(e)
					}
					if !active {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if active {
					t.Fatal("cancelled original maintenance SQL outlived its runtime")
				}
				select {
				case e := <-closed:
					closed = nil
					t.Fatal("startup returned before actual owned ingress end", e)
				default:
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				pingErr := f.store.Pool().Ping(ctx)
				cancel()
				if pingErr != nil {
					t.Fatal("runtime pool closed before owned runtime work joined", pingErr)
				}
				entry.Close()
				entry = nil
			}
			if e = nativeStartupAwait(t, closed); e != nil {
				closed = nil
				t.Fatal(e)
			}
			closed = nil
			var active bool
			if e = f.boot.observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='active')`, pid).Scan(&active); e != nil || active {
				t.Fatal("actual maintenance query survived completed Startup.Close", e)
			}
			if f.authority.Context().Err() == nil {
				t.Fatal("startup did not retire original authority")
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if f.store.Pool().Ping(ctx) == nil {
				t.Fatal("actual source pool survived completed original shutdown")
			}
		})
	}
}
