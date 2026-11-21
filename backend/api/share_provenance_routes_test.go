//go:build linux

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/utils"
)

func TestShareLinkProvenanceRegisteredAPI(t *testing.T) {
	for _, kind := range []string{"current-frame", "cross-account-master", "scoped-denied", "missing-managed-creator"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressRoutePG(t, false)
			profile, owner := f.open, f.account
			if kind == "cross-account-master" {
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id=$1`, f.account); err != nil {
					t.Fatal(err)
				}
				profile, owner = f.foreign, f.other
			}
			if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, profile); err != nil {
				t.Fatal(err)
			}
			token := f.token
			if kind == "scoped-denied" {
				session, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic-share-scope", "loopback", time.Hour, models.SessionScopeJellyfin, profile)
				if err != nil {
					t.Fatal(err)
				}
				token = session.Token
			}
			creator, err := datastore.NewShareLinkCreator(t.Context(), f.ds, f.manager, f.security, f.coordinator)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "missing-managed-creator" {
				creator = nil
			}
			share := handlers.NewShareHandler(handlers.NewShareStore(f.ds.ShareLinks()), f.sessions, f.users, "")
			share.SetManagedShareCreation(creator)
			router := shareProvenanceRegisteredRouter(t, f, share)
			body, _ := json.Marshal(map[string]any{"params": map[string]string{"profileId": profile, "sourcePath": "synthetic:registered-share", "accountId": "forged-actor", "isMaster": "true"}})
			r := httptest.NewRequest("POST", "http://synthetic.test/api/share/create", strings.NewReader(string(body)))
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("X-Client-ID", "synthetic-progress-client")
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if kind == "scoped-denied" || kind == "missing-managed-creator" {
				want := 403
				if kind == "missing-managed-creator" {
					want = 503
				}
				if w.Code != want {
					t.Fatal("registered denial status", w.Code, w.Body.String())
				}
				var n int
				if err := f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 0 {
					t.Fatal("registered denial wrote share", n, err)
				}
				return
			}
			if w.Code != 200 {
				t.Fatal("registered creation status", w.Code, w.Body.String())
			}
			var result struct {
				Token string `json:"token"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.Token == "" {
				t.Fatal("missing registered result", err)
			}
			var version int
			var actor, source, sourceOwner string
			var master bool
			err = f.ds.Pool().QueryRow(t.Context(), `SELECT provenance_version,account_id,is_master,source_profile_id,source_account_id FROM share_links WHERE token=$1`, result.Token).Scan(&version, &actor, &master, &source, &sourceOwner)
			if err != nil || version != 1 || actor != f.account || source != profile || sourceOwner != owner || master != (kind == "cross-account-master") {
				t.Fatal("wrong registered actual actor/source provenance", version, actor, master, source, sourceOwner, err)
			}
		})
	}
}

func shareProvenanceRegisteredRouter(t *testing.T, f *recordingProgressRouteFixture, share *handlers.ShareHandler) *mux.Router {
	t.Helper()
	accountService, err := accounts.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	recordingHandler := handlers.NewRecordingsHandler(f.service, f.users)
	router := mux.NewRouter()
	Register(router, nil, meta, nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{}, handlers.NewUsersHandler(f.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, recordingHandler, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, accountService, f.sessions, f.users, share, nil, nil, "", nil,
		WithPGProfilePins(f.pins), WithProfileUnlocks(profileunlock.NewService(f.ds.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: f.enforce.Load()} }), WithRecordingProgress(f.progress))
	return router
}

type apiShareEntryConn struct {
	net.Conn
	reading  atomic.Int32
	demanded atomic.Bool
}

func (c *apiShareEntryConn) Read(p []byte) (int, error) {
	c.reading.Add(1)
	defer c.reading.Add(-1)
	return c.Conn.Read(p)
}
func (c *apiShareEntryConn) SetReadDeadline(at time.Time) error {
	if !at.IsZero() && !at.After(time.Now()) {
		c.demanded.Store(true)
	}
	return c.Conn.SetReadDeadline(at)
}

type apiShareEntryListener struct {
	net.Listener
	accepted chan *apiShareEntryConn
}

func (l *apiShareEntryListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	v := &apiShareEntryConn{Conn: c}
	l.accepted <- v
	return v, nil
}
func apiShareEntryHeldTCP(t *testing.T, handler http.Handler, headers string, runtimeCancel context.CancelFunc, caller bool) {
	t.Helper()
	done := make(chan struct{})
	ready := make(chan context.CancelFunc, 1)
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ready <- cancel
		handler.ServeHTTP(w, r.WithContext(ctx))
		once.Do(func() { close(done) })
	}))
	listener := &apiShareEntryListener{Listener: server.Listener, accepted: make(chan *apiShareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	var cancel context.CancelFunc
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
		runtimeCancel()
		_ = connection.Close()
		server.Close()
	})
	if _, err = fmt.Fprintf(connection, "POST /api/share/create HTTP/1.1\r\nHost: synthetic.test\r\nOrigin: http://synthetic.test\r\nSec-Fetch-Site: same-origin\r\nContent-Type: application/json\r\nContent-Length: 200\r\n%sConnection: close\r\n\r\n{", headers); err != nil {
		t.Fatal(err)
	}
	var actual *apiShareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no actual accepted socket")
	}
	select {
	case cancel = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no original incoming request")
	}
	until := time.Now().Add(5 * time.Second)
	for actual.reading.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.reading.Load() == 0 {
		t.Fatal("incomplete body did not block original transport")
	}
	select {
	case <-done:
		t.Fatal("incomplete body finished before cancellation")
	default:
	}
	if caller {
		cancel()
	} else {
		runtimeCancel()
	}
	apiShareEntryAwait(t, done)
	if !actual.demanded.Load() {
		t.Fatal("original cancellation failed to demand actual read interruption")
	}
	server.Close()
	if actual.reading.Load() != 0 {
		t.Fatal("actual incoming read survived handler/server cleanup")
	}
}

func apiShareEntryAwait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("actual registered operation did not join")
	}
}

func TestShareLinkProvenanceRegisteredBodyLifetime(t *testing.T) {
	for _, kind := range []string{"caller-cancel", "runtime-cancel", "base-path", "unrelated-profile-guard"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressRoutePG(t, false)
			if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, f.open); err != nil {
				t.Fatal(err)
			}
			life, retire := context.WithCancel(t.Context())
			defer retire()
			creator, err := datastore.NewShareLinkCreator(life, f.ds, f.manager, f.security, f.coordinator)
			if err != nil {
				t.Fatal(err)
			}
			share := handlers.NewShareHandler(handlers.NewShareStore(f.ds.ShareLinks()), f.sessions, f.users, "/synthetic-prefix")
			share.SetManagedShareCreation(creator)
			router := shareProvenanceRegisteredRouter(t, f, share)
			if kind == "caller-cancel" || kind == "runtime-cancel" {
				headers := "Authorization: Bearer " + f.token + "\r\nX-Client-ID: synthetic-progress-client\r\n"
				apiShareEntryHeldTCP(t, router, headers, retire, kind == "caller-cancel")
				var n int
				if err := f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_links`).Scan(&n); err != nil || n != 0 {
					t.Fatal("registered incomplete body wrote share", n, err)
				}
				return
			}
			body, _ := json.Marshal(map[string]any{"params": map[string]string{"profileId": f.open, "sourcePath": "synthetic:base-path"}})
			path := "/synthetic-prefix/api/share/create"
			if kind == "unrelated-profile-guard" {
				path = "/api/auth/logout"
				body, _ = json.Marshal(map[string]string{"profileId": f.foreign})
			}
			r := httptest.NewRequest("POST", "http://synthetic.test"+path, strings.NewReader(string(body)))
			r.Header.Set("Authorization", "Bearer "+f.token)
			r.Header.Set("X-Client-ID", "synthetic-progress-client")
			w := httptest.NewRecorder()
			utils.BasePathHandler("/synthetic-prefix", router).ServeHTTP(w, r)
			if kind == "base-path" {
				if w.Code != 200 {
					t.Fatal("registered normalized base-path failed", w.Code, w.Body.String())
				}
				return
			}
			if w.Code != 404 {
				t.Fatal("unrelated route lost original body ownership denial", w.Code, w.Body.String())
			}
			var n int
			if err := f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE token=$1`, f.token).Scan(&n); err != nil || n != 1 {
				t.Fatal("unrelated body bypass reached logout", n, err)
			}
		})
	}
}
