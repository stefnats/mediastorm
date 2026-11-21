//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/datastore"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/utils"
)

type nativeHTTPFixture struct {
	f           *watchRoomHTTPFixture
	room        *models.WatchRoom
	participant models.User
	route       http.Handler
}

func newNativeHTTP(t *testing.T, life context.Context, basePath string, large bool) *nativeHTTPFixture {
	t.Helper()
	f := newWatchRoomHTTP(t, life, basePath)
	participant, e := f.base.base.pin.users.CreateForAccount(f.base.base.pin.session.AccountID, "Synthetic native HTTP participant")
	if e != nil {
		t.Fatal(e)
	}
	in := watchRoomHTTPInput()
	in.InviteeProfileIDs = []string{participant.ID}
	if large {
		for i := 0; i < 63; i++ {
			profile, e := f.base.base.pin.users.CreateForAccount(f.base.base.pin.session.AccountID, fmt.Sprintf("Synthetic native %02d %s", i, strings.Repeat("x", 980)))
			if e != nil {
				t.Fatal(e)
			}
			in.InviteeProfileIDs = append(in.InviteeProfileIDs, profile.ID)
		}
	}
	room, e := f.authority.CreateRoom(f.request(t, "POST", "/api/users/"+f.profile+"/watch-rooms", nil, t.Context()), f.profile, in)
	if e != nil || room == nil {
		t.Fatal("actual original native HTTP fixture", e)
	}
	n := &nativeHTTPFixture{f: f, room: room, participant: participant}
	n.route = n.router(t)
	return n
}
func (f *nativeHTTPFixture) path(profile, suffix string) string {
	return "/api/users/" + profile + "/watch-rooms/" + f.room.ID + suffix
}
func (f *nativeHTTPFixture) router(t *testing.T) http.Handler {
	t.Helper()
	h := f.f.rooms
	router := mux.NewRouter()
	for _, item := range []struct {
		method, path string
		next         http.HandlerFunc
	}{
		{"POST", "/api/users/{userID}/watch-rooms", h.Create}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/account-invites", h.InviteAccount}, {"GET", "/api/users/{userID}/watch-rooms/{roomID}/account-invites", h.RoomAccountInvitations}, {"DELETE", "/api/users/{userID}/watch-rooms/{roomID}/account-invites/{inviteID}", h.RevokeAccountInvitation},
		{"GET", "/api/watch-room-account-invitations", h.AccountInvitations}, {"POST", "/api/watch-room-account-invitations/{inviteID}/decline", h.DeclineAccountInvitation}, {"POST", "/api/users/{userID}/watch-room-account-invitations/{inviteID}/accept", h.AcceptAccountInvitation},
		{"GET", "/api/users/{userID}/watch-rooms/invitations", h.Invitations}, {"GET", "/api/users/{userID}/watch-rooms/{roomID}", h.Get}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/join", h.Join}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/ready", h.Ready}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/state", h.State}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/heartbeat", h.Heartbeat}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/leave", h.Leave}, {"POST", "/api/users/{userID}/watch-rooms/{roomID}/end", h.End},
	} {
		router.HandleFunc(item.path, item.next).Methods(item.method)
	}
	framed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		checked, release, e := f.f.base.base.pin.pins.Authenticate(r, token, "")
		if e != nil {
			writeJSONError(w, "native authentication unavailable", 503)
			return
		}
		defer release()
		h.BoundedManagedEntry(profileaccess.CurrentMiddleware(f.f.base.base.pin.pins.Owns)(router)).ServeHTTP(w, checked)
	})
	return h.BoundedManagedEntry(framed)
}
func (f *nativeHTTPFixture) request(method, path, body string) *http.Request {
	r := httptest.NewRequest(method, "http://synthetic.test"+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+f.f.base.base.pin.session.Token)
	r.Header.Set("Content-Type", "application/json")
	return r
}
func (f *nativeHTTPFixture) call(method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.route.ServeHTTP(w, f.request(method, path, body))
	return w
}
func nativeHTTPJoinBody() string {
	return `{"clientId":"synthetic-native-HTTP","capabilities":{"nativePlayback":true,"stateSync":true,"protocolVersion":1}}`
}
func nativeHTTPView(t *testing.T, w *httptest.ResponseRecorder) *models.WatchRoom {
	t.Helper()
	var room models.WatchRoom
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &room) != nil || room.ID == "" {
		t.Fatal("actual bounded native HTTP response", w.Code, w.Body.String())
	}
	return &room
}

func TestNativeRoomManagedDispatch(t *testing.T) {
	for _, kind := range []string{"all-native-methods-use-current-core", "global-account-inbox-no-first-profile", "missing-managed-denied", "legacy-isolated-constructor", "strict-join-body", "strict-heartbeat-body", "revision-wire-unchanged", "invited-unjoined-member-wire", "ended-audit-wire", "no-media-grant-from-room-data"} {
		t.Run(kind, func(t *testing.T) {
			f := newNativeHTTP(t, t.Context(), "", false)
			creator := f.path(f.f.profile, "")
			member := f.path(f.participant.ID, "")
			if kind == "all-native-methods-use-current-core" || kind == "missing-managed-denied" {
				f.f.rooms.SetManagedWatchRoomAuthority(nil, "")
				for _, op := range []struct{ method, path string }{{"POST", creator + "/account-invites"}, {"GET", "/api/watch-room-account-invitations"}, {"GET", creator + "/account-invites"}, {"POST", "/api/users/" + f.participant.ID + "/watch-room-account-invitations/synthetic/accept"}, {"POST", "/api/watch-room-account-invitations/synthetic/decline"}, {"DELETE", creator + "/account-invites/synthetic"}, {"GET", "/api/users/" + f.participant.ID + "/watch-rooms/invitations"}, {"GET", creator}, {"POST", member + "/join"}, {"POST", creator + "/ready"}, {"POST", creator + "/state"}, {"POST", creator + "/heartbeat"}, {"POST", member + "/leave"}, {"POST", creator + "/end"}} {
					w := f.call(op.method, op.path, `{"unknown":true}`)
					if w.Code != 503 {
						t.Fatal("managed native method fell back without original runtime", op.path, w.Code)
					}
				}
				var members int
				if e := f.f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_members WHERE room_id=$1`, f.room.ID).Scan(&members); e != nil || members != 1 {
					t.Fatal("missing native runtime mutated current members", e)
				}
				return
			}
			if kind == "global-account-inbox-no-first-profile" {
				if _, e := f.f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET pin_hash='',pin_length=4 WHERE account_id=$1`, f.f.base.base.pin.session.AccountID); e != nil {
					t.Fatal(e)
				}
				w := f.call("GET", "/api/watch-room-account-invitations", "")
				if w.Code != 200 || strings.TrimSpace(w.Body.String()) != "[]" {
					t.Fatal("account inbox guessed a malformed profile", w.Code, w.Body.String())
				}
				return
			}
			if kind == "legacy-isolated-constructor" {
				legacy := NewWatchRoomsHandler(f.f.rooms.service)
				w := httptest.NewRecorder()
				r := mux.SetURLVars(f.f.request(t, "GET", creator, nil, t.Context()), map[string]string{"userID": f.f.profile, "roomID": f.room.ID})
				legacy.Get(w, r)
				if legacy.managedRoom != nil || nativeHTTPView(t, w).ID != f.room.ID {
					t.Fatal("explicit legacy constructor selected managed authority")
				}
				return
			}
			if kind == "strict-join-body" {
				for _, body := range []string{`{"unknown":true}`, nativeHTTPJoinBody() + ` {}`, `{"clientId":`} {
					w := f.call("POST", member+"/join", body)
					if w.Code != 400 {
						t.Fatal("invalid/trailing native join body accepted", w.Code)
					}
				}
				var count int
				if e := f.f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_members WHERE room_id=$1 AND profile_id=$2`, f.room.ID, f.participant.ID).Scan(&count); e != nil || count != 0 {
					t.Fatal("invalid native body joined a member", e)
				}
				return
			}
			if kind == "strict-heartbeat-body" {
				var before string
				if e := f.f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT client_id FROM watch_room_members WHERE room_id=$1 AND profile_id=$2`, f.room.ID, f.f.profile).Scan(&before); e != nil {
					t.Fatal(e)
				}
				w := f.call("POST", creator+"/heartbeat", `{"clientId":`)
				var after string
				if e := f.f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT client_id FROM watch_room_members WHERE room_id=$1 AND profile_id=$2`, f.room.ID, f.f.profile).Scan(&after); e != nil || w.Code != 400 || before != after {
					t.Fatal("malformed native heartbeat wrote default DATA", w.Code, e)
				}
				return
			}
			if kind == "revision-wire-unchanged" {
				w := f.call("POST", creator+"/state", `{"status":"paused","position":2.5,"duration":90,"expectedRevision":0}`)
				view := nativeHTTPView(t, w)
				if view.Position != 2.5 || view.Revision != 1 || view.Status != "paused" {
					t.Fatal("native revision/position wire changed")
				}
				if stale := f.call("POST", creator+"/state", `{"status":"playing","position":1,"expectedRevision":0}`); stale.Code != 409 {
					t.Fatal("stale native HTTP revision succeeded", stale.Code)
				}
				return
			}
			if kind == "ended-audit-wire" {
				if w := f.call("POST", creator+"/end", `{}`); w.Code != 204 || w.Body.Len() != 0 {
					t.Fatal("native end publication wire", w.Code)
				}
				view := nativeHTTPView(t, f.call("GET", creator, ""))
				if view.EndedAt == nil || view.EndReason != "host_ended" || view.Status != "ended" {
					t.Fatal("native audit fields omitted")
				}
				return
			}
			w := f.call("GET", creator, "")
			view := nativeHTTPView(t, w)
			found := false
			for _, m := range view.Members {
				if m.ProfileID == f.participant.ID {
					found = true
					if m.Joined {
						t.Fatal("invited unjoined native placeholder promoted")
					}
				}
			}
			if !found {
				t.Fatal("native invited placeholder omitted")
			}
			if kind == "no-media-grant-from-room-data" {
				for _, forbidden := range [][]byte{[]byte(`"token"`), []byte(`"playbackGrant"`), []byte(`"admission_nonce"`), []byte(`"room_incarnation"`)} {
					if bytes.Contains(w.Body.Bytes(), forbidden) {
						t.Fatal("native room DATA leaked a private/media grant")
					}
				}
				var streams int
				if e := f.f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE scope='stream'`).Scan(&streams); e != nil || streams != 0 {
					t.Fatal("native metadata minted source authority", e)
				}
			}
		})
	}
}

func nativeHeldHTTPBody(t *testing.T, f *nativeHTTPFixture, path, body string, caller bool) {
	t.Helper()
	done := make(chan struct{})
	ready := make(chan context.CancelFunc, 1)
	var started atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Store(true)
		defer close(done)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ready <- cancel
		f.route.ServeHTTP(w, r.WithContext(ctx))
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
	var closed chan struct{}
	t.Cleanup(func() {
		if cancel != nil {
			cancel()
		}
		_ = conn.Close()
		server.Close()
		if started.Load() {
			progressHTTPAwait(t, done)
		}
		if closed != nil {
			progressHTTPAwait(t, closed)
		}
	})
	if _, e = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: synthetic.test\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n%s", path, f.f.base.base.pin.session.Token, body); e != nil {
		t.Fatal(e)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("actual native incoming connection absent")
	}
	select {
	case cancel = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("original native HTTP context absent")
	}
	until := time.Now().Add(5 * time.Second)
	for actual.reading.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.reading.Load() == 0 {
		t.Fatal("native original upstream body did not physically block")
	}
	select {
	case <-done:
		t.Fatal("incomplete native body ended before cancellation")
	default:
	}
	if caller {
		cancel()
	} else {
		closed = make(chan struct{})
		go func() { defer close(closed); f.f.authority.Close() }()
	}
	progressHTTPAwait(t, done)
	if closed != nil {
		progressHTTPAwait(t, closed)
	}
	if actual.reading.Load() != 0 || !actual.demanded.Load() {
		t.Fatal("native cancellation did not interrupt/join real body/Close read")
	}
}

type nativeACKCancelWriter struct {
	http.ResponseWriter
	store         *datastore.DataStore
	room, profile string
	cancel        context.CancelFunc
	observed      atomic.Bool
}

func (*nativeACKCancelWriter) SetReadDeadline(time.Time) error { return nil }
func (w *nativeACKCancelWriter) SetWriteDeadline(at time.Time) error {
	if !at.After(time.Now()) || w.observed.Load() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var count int
	if e := w.store.Pool().QueryRow(ctx, `SELECT count(*) FROM watch_room_members m JOIN watch_room_native_admissions n ON n.room_id=m.room_id AND n.profile_id=m.profile_id AND n.member_incarnation=m.member_incarnation AND n.member_joined_at=m.joined_at WHERE m.room_id=$1 AND m.profile_id=$2`, w.room, w.profile).Scan(&count); e == nil && count == 1 && w.observed.CompareAndSwap(false, true) {
		w.cancel()
	}
	return nil
}

func TestNativeRoomHTTPPhysicalLifetime(t *testing.T) {
	for _, kind := range []string{"join-body-before-ownership", "state-body-runtime-cancel", "account-invite-body-caller-cancel", "accept-before-body", "decline-before-body", "held-json-runtime-close", "known-ack-preheader-no-success", "healthy-tcp-complete-room", "base-path-prefixed-direct", "non-room-stream-unaffected"} {
		t.Run(kind, func(t *testing.T) {
			basePath := ""
			if kind == "base-path-prefixed-direct" {
				basePath = "/mediastorm"
			}
			f := newNativeHTTP(t, t.Context(), basePath, kind == "held-json-runtime-close")
			creator, member := f.path(f.f.profile, ""), f.path(f.participant.ID, "")
			if kind == "non-room-stream-unaffected" {
				ctx := context.Background()
				hit := false
				r := httptest.NewRequest("POST", "http://synthetic.test/api/video/stream", nil).WithContext(ctx)
				f.f.rooms.BoundedManagedEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					hit = true
					if r.Context() != ctx || r.Context().Value(watchRoomEntryKey{}) != nil {
						t.Fatal("ordinary stream acquired native30s lifetime")
					}
					w.WriteHeader(204)
				})).ServeHTTP(httptest.NewRecorder(), r)
				if !hit {
					t.Fatal("unrelated stream blocked")
				}
				return
			}
			if kind == "base-path-prefixed-direct" {
				f.route = utils.BasePathHandler(basePath, f.route)
				for _, prefix := range []string{basePath, ""} {
					nativeHeldHTTPBody(t, f, prefix+member+"/join", `{"clientId":`, true)
				}
				return
			}
			switch kind {
			case "join-body-before-ownership":
				nativeHeldHTTPBody(t, f, member+"/join", `{"clientId":`, true)
				return
			case "state-body-runtime-cancel":
				nativeHeldHTTPBody(t, f, creator+"/state", `{"status":`, false)
				return
			case "account-invite-body-caller-cancel":
				nativeHeldHTTPBody(t, f, creator+"/account-invites", `{"username":`, true)
				return
			case "accept-before-body":
				nativeHeldHTTPBody(t, f, "/api/users/"+f.participant.ID+"/watch-room-account-invitations/synthetic/accept", `{`, true)
				return
			case "decline-before-body":
				nativeHeldHTTPBody(t, f, "/api/watch-room-account-invitations/synthetic/decline", `{`, true)
				return
			}
			if kind == "known-ack-preheader-no-success" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				record := httptest.NewRecorder()
				writer := &nativeACKCancelWriter{ResponseWriter: record, store: f.f.base.base.pin.store, room: f.room.ID, profile: f.participant.ID, cancel: cancel}
				f.route.ServeHTTP(writer, f.request("POST", member+"/join", nativeHTTPJoinBody()).WithContext(ctx))
				if !writer.observed.Load() || record.Code != 503 || bytes.Contains(record.Body.Bytes(), []byte(f.room.ID)) || record.Header().Get("Location") != "" || record.Header().Get("Set-Cookie") != "" {
					t.Fatal("known-ACK cancelled native output falsely succeeded", record.Code)
				}
				var count int
				if e := f.f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_members WHERE room_id=$1 AND profile_id=$2`, f.room.ID, f.participant.ID).Scan(&count); e != nil || count != 1 {
					t.Fatal("lost native publication falsely implied rollback", e)
				}
				return
			}
			if kind == "healthy-tcp-complete-room" {
				server := httptest.NewServer(f.route)
				client := server.Client()
				client.Timeout = 5 * time.Second
				t.Cleanup(func() { client.CloseIdleConnections(); server.Close() })
				request, e := http.NewRequestWithContext(t.Context(), "GET", server.URL+creator, nil)
				if e != nil {
					t.Fatal(e)
				}
				request.Header.Set("Authorization", "Bearer "+f.f.base.base.pin.session.Token)
				response, e := client.Do(request)
				if e != nil {
					if response != nil {
						_ = response.Body.Close()
					}
					t.Fatal(e)
				}
				raw, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				var room models.WatchRoom
				if readErr != nil || closeErr != nil || response.StatusCode != 200 || json.Unmarshal(raw, &room) != nil || room.ID != f.room.ID || len(room.Members) != 2 {
					t.Fatal("healthy real native final HTTP chunk/body truncated", readErr, closeErr, response.StatusCode)
				}
				return
			}
			if kind == "held-json-runtime-close" {
				done := make(chan struct{})
				var started atomic.Bool
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					started.Store(true)
					defer close(done)
					f.route.ServeHTTP(w, r)
				}))
				listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
				server.Listener = listener
				server.Start()
				conn, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if e != nil {
					server.Close()
					t.Fatal(e)
				}
				var closed chan struct{}
				t.Cleanup(func() {
					_ = conn.Close()
					server.Close()
					if started.Load() {
						progressHTTPAwait(t, done)
					}
					if closed != nil {
						progressHTTPAwait(t, closed)
					}
				})
				if tcp, ok := conn.(*net.TCPConn); ok {
					if e = tcp.SetReadBuffer(1024); e != nil {
						t.Fatal(e)
					}
				}
				if _, e = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: synthetic.test\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n", creator, f.f.base.base.pin.session.Token); e != nil {
					t.Fatal(e)
				}
				var actual *shareEntryConn
				select {
				case actual = <-listener.accepted:
				case <-time.After(5 * time.Second):
					t.Fatal("actual held native output socket absent")
				}
				until := time.Now().Add(5 * time.Second)
				for actual.writing.Load() == 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if actual.writing.Load() == 0 {
					t.Fatal("actual native JSON output did not block")
				}
				closed = make(chan struct{})
				go func() { defer close(closed); f.f.authority.Close() }()
				progressHTTPAwait(t, closed)
				progressHTTPAwait(t, done)
				if actual.writing.Load() != 0 || !actual.writeDemanded.Load() {
					t.Fatal("runtime closed before actual JSON write/Flush callback end")
				}
			}
		})
	}
}
