//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/watchrooms"
	"novastream/utils"
	"strings"
	"sync"
	"testing"
	"time"
)

type watchRoomHTTPFixture struct {
	base      *progressHTTPFixture
	authority *datastore.WatchRoomAuthority
	rooms     *WatchRoomsHandler
	party     *WatchPartyHandler
	profile   string
}

func newWatchRoomHTTP(t *testing.T, life context.Context, basePath string) *watchRoomHTTPFixture {
	t.Helper()
	base := progressHTTPPG(t, false)
	pin := base.base.pin
	if _, e := pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, pin.profile.ID); e != nil {
		t.Fatal(e)
	}
	settings, e := base.manager.Load()
	if e != nil {
		t.Fatal(e)
	}
	settings.Server.ExternalBackendURL = "https://synthetic.example" + basePath + "/api"
	if e = base.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	authority, e := datastore.NewWatchRoomAuthority(life, pin.store, base.manager, base.security, base.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(authority.Close)
	service := watchrooms.New(pin.store.WatchRooms(), pin.users, nil)
	rooms, party := NewWatchRoomsHandler(service), NewWatchPartyHandler(service, pin.sessions, basePath)
	rooms.SetManagedWatchRoomAuthority(authority, basePath)
	party.SetManagedWatchRoomAuthority(authority, basePath)
	party.SetSettingsProvider(base.manager)
	return &watchRoomHTTPFixture{base, authority, rooms, party, pin.profile.ID}
}
func watchRoomHTTPInput() models.WatchRoomCreate {
	return models.WatchRoomCreate{Title: "Synthetic HTTP room", MediaType: "movie", ItemID: "synthetic-item", ClientID: "synthetic-native", Params: map[string]string{}, Capabilities: models.WatchRoomClientCapabilities{NativePlayback: true, StateSync: true, ProtocolVersion: 1}}
}
func (f *watchRoomHTTPFixture) request(t *testing.T, method, path string, body io.Reader, ctx context.Context) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, "http://synthetic.test"+path, body).WithContext(ctx)
	checked, release, e := f.base.base.pin.pins.Authenticate(r, f.base.base.pin.session.Token, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	return mux.SetURLVars(checked, map[string]string{"userID": f.profile})
}
func (f *watchRoomHTTPFixture) create(t *testing.T) *models.WatchRoom {
	t.Helper()
	room, e := f.authority.CreateRoom(f.request(t, "POST", "/api/users/"+f.profile+"/watch-rooms", nil, t.Context()), f.profile, watchRoomHTTPInput())
	if e != nil {
		t.Fatal(e)
	}
	return room
}
func (f *watchRoomHTTPFixture) route(t *testing.T) http.Handler {
	t.Helper()
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/watch-rooms", f.rooms.Create).Methods("POST")
	router.HandleFunc("/api/users/{userID}/watch-rooms/{roomID}/external-invite", f.party.CreateInvitation).Methods("POST")
	router.HandleFunc("/api/users/{userID}/watch-rooms/{roomID}/external-source", f.party.BindSource).Methods("POST")
	// This uses the same original PG authentication and concrete outer+inner
	// entry order as Register; no auth DTO or selector is promoted to a frame.
	framed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checked, release, e := f.base.base.pin.pins.Authenticate(r, f.base.base.pin.session.Token, "")
		if e != nil {
			writeJSONError(w, "unavailable", 503)
			return
		}
		defer release()
		f.rooms.BoundedManagedEntry(f.party.BoundedManagedEntry(router)).ServeHTTP(w, checked)
	})
	return f.rooms.BoundedManagedEntry(f.party.BoundedManagedEntry(framed))
}
func TestWatchRoomManagedMutations(t *testing.T) {
	for _, kind := range []string{"create-native", "create-share-disabled", "invite-known-ack", "source-known-ack", "managed-missing-503", "file-legacy-constructor", "params-no-master-authority", "public-join-unchanged", "legacy-room-recreate", "unrelated-stream-unchanged"} {
		t.Run(kind, func(t *testing.T) {
			f := newWatchRoomHTTP(t, t.Context(), "")
			if kind == "public-join-unchanged" {
				w := httptest.NewRecorder()
				f.party.Landing(w, httptest.NewRequest("GET", "http://synthetic.test/watch-party", nil))
				if w.Code != 200 || !strings.Contains(w.Body.String(), "Join a Watch Together room") {
					t.Fatal("public landing changed")
				}
				return
			}
			if kind == "unrelated-stream-unchanged" {
				r := httptest.NewRequest("POST", "http://synthetic.test/api/video/stream", nil)
				hit := false
				f.rooms.BoundedManagedEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hit = true
					if r.Context().Value(watchRoomEntryKey{}) != nil {
						t.Fatal("ordinary media gained room lifetime")
					}
					w.WriteHeader(204)
				})).ServeHTTP(httptest.NewRecorder(), r)
				if !hit {
					t.Fatal("unrelated stream blocked")
				}
				return
			}
			input := watchRoomHTTPInput()
			var endpoint http.HandlerFunc = f.rooms.Create
			path := "/api/users/" + f.profile + "/watch-rooms"
			status := 201
			if kind == "create-share-disabled" {
				if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profile); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "params-no-master-authority" {
				input.Params = map[string]string{"isMaster": "true", "accountId": "foreign", "creatorProfileId": "foreign"}
			}
			if kind == "file-legacy-constructor" {
				legacy := NewWatchRoomsHandler(f.rooms.service)
				if legacy.managedRoom != nil {
					t.Fatal("legacy constructor selected managed runtime")
				}
				endpoint = legacy.Create
			}
			if kind == "managed-missing-503" {
				f.rooms.SetManagedWatchRoomAuthority(nil, "")
				status = 503
			}
			var body []byte
			body, _ = json.Marshal(input)
			if kind == "invite-known-ack" || kind == "source-known-ack" || kind == "legacy-room-recreate" {
				room := f.create(t)
				path += "/" + room.ID + "/external-invite"
				endpoint = f.party.CreateInvitation
				body = nil
				if kind == "source-known-ack" {
					endpoint = f.party.BindSource
					path = strings.TrimSuffix(path, "external-invite") + "external-source"
					status = 200
					body = []byte(`{"source":"localmedia:room-http-item/room.wav","params":{"titleId":"synthetic-title","profileId":"foreign","isMaster":"true"}}`)
					for _, query := range []string{`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('room-http-lib','Synthetic room HTTP','movie','/synthetic')`, `INSERT INTO local_media_items(id,library_id,relative_path,file_path,library_type)VALUES('room-http-item','room-http-lib','room.wav','/synthetic/room.wav','movie')`, `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('room-http-lib','restricted')`} {
						if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), query); e != nil {
							t.Fatal(e)
						}
					}
					if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `INSERT INTO media_library_profile_grants(library_id,profile_id)VALUES('room-http-lib',$1)`, f.profile); e != nil {
						t.Fatal(e)
					}
					cache := libraryaccess.New(f.base.base.pin.store.LibraryAccess(), f.base.base.pin.store.LocalMedia(), f.base.base.pin.store.RemoteMedia())
					f.party.SetLibraryAccessService(cache)
					if known, allow, e := cache.CanAccessStream(t.Context(), "localmedia:room-http-item/room.wav", f.base.base.pin.session.AccountID, f.profile, false); e != nil || !known || !allow {
						t.Fatal("actual library cached allow not primed", e)
					}
				}
				if kind == "legacy-room-recreate" {
					if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `DELETE FROM watch_room_provenance WHERE room_id=$1`, room.ID); e != nil {
						t.Fatal(e)
					}
					status = 410
				}
				r := f.request(t, "POST", path, bytes.NewReader(body), t.Context())
				r = mux.SetURLVars(r, map[string]string{"userID": f.profile, "roomID": room.ID})
				w := httptest.NewRecorder()
				endpoint(w, r)
				if w.Code != status {
					t.Fatal("managed mutation HTTP", kind, w.Code, w.Body.String())
				}
				if kind == "source-known-ack" {
					if strings.TrimSpace(w.Body.String()) != `{"ready":true}` {
						t.Fatal("source leaked internal deadline/provenance", w.Body.String())
					}
					var profile string
					if e := f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT source_profile_id FROM watch_room_source_provenance WHERE room_id=$1`, room.ID).Scan(&profile); e != nil || profile != f.profile {
						t.Fatal("body DATA replaced actual source owner", e)
					}
				}
				if kind == "source-known-ack" {
					if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `DELETE FROM media_library_profile_grants WHERE library_id='room-http-lib'`); e != nil {
						t.Fatal(e)
					}
					if _, allow, e := f.party.libraryAccess.CanAccessStream(t.Context(), "localmedia:room-http-item/room.wav", f.base.base.pin.session.AccountID, f.profile, false); e != nil || !allow {
						t.Fatal("canary lost intentionally stale library cache", e)
					}
					fresh := f.request(t, "POST", path, bytes.NewReader(body), t.Context())
					fresh = mux.SetURLVars(fresh, map[string]string{"userID": f.profile, "roomID": room.ID})
					denied := httptest.NewRecorder()
					endpoint(denied, fresh)
					if denied.Code != 403 {
						t.Fatal("managed source reused cached library grant", denied.Code, denied.Body.String())
					}
				}
				if kind == "invite-known-ack" {
					var view watchPartyInviteView
					if json.Unmarshal(w.Body.Bytes(), &view) != nil || view.URL == "" || view.ShortCode == "" {
						t.Fatal("knownACK invitation output absent")
					}
				}
				return
			}
			r := f.request(t, "POST", path, bytes.NewReader(body), t.Context())
			w := httptest.NewRecorder()
			endpoint(w, r)
			if w.Code != status {
				t.Fatal("managed create HTTP", kind, w.Code, w.Body.String())
			}
			var n int
			if e := f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_provenance`).Scan(&n); e != nil {
				t.Fatal(e)
			}
			expected := 1
			if kind == "managed-missing-503" || kind == "file-legacy-constructor" {
				expected = 0
			}
			if n != expected {
				t.Fatal("constructor/knownACK provenance mismatch", kind, n)
			}
			if kind == "create-native" {
				server := httptest.NewServer(f.route(t))
				t.Cleanup(server.Close)
				transport := &http.Transport{}
				t.Cleanup(transport.CloseIdleConnections)
				client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
				healthyBody, _ := json.Marshal(watchRoomHTTPInput())
				request, e := http.NewRequestWithContext(t.Context(), "POST", server.URL+path, bytes.NewReader(healthyBody))
				if e != nil {
					t.Fatal(e)
				}
				request.Header.Set("Content-Type", "application/json")
				response, e := client.Do(request)
				if e != nil {
					t.Fatal("healthy actual TCP room response", e)
				}
				raw, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil || response.StatusCode != 201 {
					t.Fatal("healthy final HTTP body did not complete", response.StatusCode, readErr, closeErr)
				}
				var delivered models.WatchRoom
				if e = json.Unmarshal(raw, &delivered); e != nil || delivered.ID == "" || delivered.CreatorProfileID != f.profile || delivered.Title != "Synthetic HTTP room" {
					t.Fatal("healthy actual TCP original room wire", e)
				}
				if e = f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_provenance`).Scan(&n); e != nil || n != 2 {
					t.Fatal("healthy actual TCP room was not committed exactly once", n, e)
				}
			}
			if kind == "params-no-master-authority" {
				var owner string
				if e := f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT creator_account_id FROM watch_room_provenance`).Scan(&owner); e != nil || owner != f.base.base.pin.session.AccountID {
					t.Fatal("params supplied actor authority", e)
				}
			}
		})
	}
}

func watchRoomHeldBody(t *testing.T, f *watchRoomHTTPFixture, path string, caller bool, retire context.CancelFunc) {
	t.Helper()
	done := make(chan struct{})
	ready := make(chan context.CancelFunc, 1)
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ready <- cancel
		f.route(t).ServeHTTP(w, r.WithContext(ctx))
		once.Do(func() { close(done) })
	}))
	listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	conn, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	var cancel context.CancelFunc
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
		retire()
		_ = conn.Close()
		server.Close()
		progressHTTPAwait(t, done)
	})
	if _, e = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: synthetic.test\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{", path); e != nil {
		t.Fatal(e)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no accepted body socket")
	}
	select {
	case cancel = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no incoming original request")
	}
	until := time.Now().Add(3 * time.Second)
	for actual.reading.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.reading.Load() == 0 {
		t.Fatal("actual incomplete body did not block")
	}
	select {
	case <-done:
		t.Fatal("body returned before cancellation")
	default:
	}
	if caller {
		cancel()
	} else {
		retire()
	}
	progressHTTPAwait(t, done)
	if !actual.demanded.Load() {
		t.Fatal("original cancellation did not interrupt actual body read")
	}
	server.Close()
	if actual.reading.Load() != 0 {
		t.Fatal("actual body read survived handler end")
	}
}
func TestWatchRoomEntryOriginalLifetime(t *testing.T) {
	for _, kind := range []string{"incomplete-body-caller-cancel", "incomplete-body-runtime-cancel", "invite-source-before-body", "base-path", "held-write-caller-cancel", "held-write-runtime-cancel", "held-write-source-expiry", "pre-header-cancel-no-200"} {
		t.Run(kind, func(t *testing.T) {
			life, retire := context.WithCancel(t.Context())
			defer retire()
			basePath := ""
			if kind == "base-path" {
				basePath = "/mediastorm"
			}
			f := newWatchRoomHTTP(t, life, basePath)
			path := "/api/users/" + f.profile + "/watch-rooms"
			if strings.HasPrefix(kind, "incomplete-body-") {
				watchRoomHeldBody(t, f, path, strings.Contains(kind, "caller"), retire)
				return
			}
			if kind == "invite-source-before-body" {
				// Missing managed dependencies must deny before even a body ownership
				// reader can consume an incomplete actual TCP request.
				f.rooms.SetManagedWatchRoomAuthority(nil, "")
				f.party.SetManagedWatchRoomAuthority(nil, "")
				for _, suffix := range []string{"external-invite", "external-source"} {
					hit := false
					entry := f.party.BoundedManagedEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hit = true; _, _ = io.ReadAll(r.Body) }))
					w := httptest.NewRecorder()
					entry.ServeHTTP(w, httptest.NewRequest("POST", "http://synthetic.test"+path+"/room/"+suffix, strings.NewReader("{")))
					if w.Code != 503 || hit {
						t.Fatal("missing managed entry reached upstream body", suffix, w.Code)
					}
				}
				return
			}
			if kind == "base-path" {
				var entered int
				downstream := f.route(t)
				observer := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Context().Value(watchRoomEntryKey{}) == nil {
						t.Fatal("actual stripped/direct Main path reached upstream body without entry")
					}
					if r.URL.Path != path {
						t.Fatal("actual Main prefix stripping did not retain expected route", r.URL.Path)
					}
					entered++
					raw, e := io.ReadAll(r.Body)
					if e != nil {
						t.Fatal(e)
					}
					r.Body = io.NopCloser(bytes.NewReader(raw))
					downstream.ServeHTTP(w, r)
				})
				actualMain := utils.BasePathHandler(basePath, f.rooms.BoundedManagedEntry(observer))
				for _, incoming := range []string{basePath + path, path} {
					body, _ := json.Marshal(watchRoomHTTPInput())
					request := httptest.NewRequest("POST", "http://synthetic.test"+incoming, bytes.NewReader(body))
					w := httptest.NewRecorder()
					actualMain.ServeHTTP(w, request)
					if w.Code != 201 {
						t.Fatal("actual configured/direct Main route", incoming, w.Code, w.Body.String())
					}
				}
				if entered != 2 {
					t.Fatal("both actual Main forms did not enter bounded body work")
				}
				return
			}
			if kind == "pre-header-cancel-no-200" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				body, _ := json.Marshal(watchRoomHTTPInput())
				r := f.request(t, "POST", path, bytes.NewReader(body), ctx)
				w := httptest.NewRecorder()
				observer := &sharePublicationCancelWriter{ResponseWriter: w, cancel: cancel, beforeCancel: func() {
					var count int
					if e := f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_rooms`).Scan(&count); e != nil || count != 1 {
						t.Error("publication not after actual knownACK", count, e)
					}
				}}
				f.rooms.Create(observer, r)
				if !observer.signaled.Load() || w.Code == 200 || w.Code == 201 || strings.Contains(w.Body.String(), "creatorProfileId") {
					t.Fatal("pre-header cancellation published success", w.Code, w.Body.String())
				}
				return
			}
			if kind == "held-write-source-expiry" {
				if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=$1 WHERE id=$2`, time.Now().UTC().Add(5*time.Second).Truncate(time.Microsecond), f.base.base.pin.session.AccountID); e != nil {
					t.Fatal(e)
				}
			}
			input := watchRoomHTTPInput()
			input.Params["synthetic-output"] = strings.Repeat("x", 768<<10)
			body, _ := json.Marshal(input)
			done := make(chan struct{})
			ready := make(chan context.CancelFunc, 1)
			var once sync.Once
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx, cancel := context.WithCancel(r.Context())
				defer cancel()
				ready <- cancel
				f.route(t).ServeHTTP(w, r.WithContext(ctx))
				once.Do(func() { close(done) })
			}))
			listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
			server.Listener = listener
			server.Start()
			conn, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if e != nil {
				server.Close()
				t.Fatal(e)
			}
			// Register actual transport cleanup before any later socket assertion
			// can fail; conditional producer joins are added before launch below.
			t.Cleanup(func() { retire(); _ = conn.Close(); server.Close() })
			tcp, ok := conn.(*net.TCPConn)
			if !ok {
				t.Fatal("real TCP unavailable")
			}
			if e = tcp.SetReadBuffer(1024); e != nil {
				t.Fatal(e)
			}
			uploadDone := make(chan struct{})
			started := false
			var cancel context.CancelFunc
			t.Cleanup(func() {
				if cancel != nil {
					cancel()
				}
				retire()
				_ = conn.Close()
				if started {
					progressHTTPAwait(t, uploadDone)
				}
				server.Close()
				progressHTTPAwait(t, done)
			})
			written := make(chan error, 1)
			started = true
			go func() {
				defer close(uploadDone)
				_, err := fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: synthetic.test\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", path, len(body), body)
				written <- err
			}()
			var actual *shareEntryConn
			select {
			case actual = <-listener.accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("no real output socket")
			}
			select {
			case cancel = <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("no original output context")
			}
			select {
			case e = <-written:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("actual input upload incomplete")
			}
			until := time.Now().Add(3 * time.Second)
			for actual.writing.Load() == 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if actual.writing.Load() == 0 {
				t.Fatal("actual knownACK JSON write never held")
			}
			var n int
			if e = f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_provenance`).Scan(&n); e != nil || n != 1 {
				t.Fatal("held write lacked committed original room", n, e)
			}
			closed := make(chan struct{})
			closeStarted := false
			t.Cleanup(func() {
				retire()
				if closeStarted {
					progressHTTPAwait(t, closed)
				}
			})
			if kind == "held-write-caller-cancel" {
				cancel()
			} else if kind == "held-write-runtime-cancel" {
				closeStarted = true
				go func() { defer close(closed); f.authority.Close() }()
			}
			progressHTTPAwait(t, done)
			if closeStarted {
				progressHTTPAwait(t, closed)
			}
			if actual.writing.Load() != 0 {
				t.Fatal("actual socket output survived handler/authority end")
			}
			if kind != "held-write-source-expiry" && !actual.writeDemanded.Load() {
				t.Fatal("cancellation did not demand actual write interrupt")
			}
			if e = f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_rooms`).Scan(&n); e != nil || n != 1 {
				t.Fatal("lost output claimed rollback/retried knownACK", n, e)
			}
		})
	}
}
