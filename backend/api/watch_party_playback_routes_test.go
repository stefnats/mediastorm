//go:build linux

package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
)

const partyRoutePayload = "SYNTHETIC-REGISTERED-PARTY"

type partyRouteProvider struct {
	calls, closed atomic.Int32
	hold          bool
	entered       chan struct{}
	canceled      chan struct{}
	returned      chan struct{}
	release       chan struct{}
	enterOnce     sync.Once
	cancelOnce    sync.Once
	returnOnce    sync.Once
	releaseOnce   sync.Once
}

type partyRouteBody struct {
	provider *partyRouteProvider
	ctx      context.Context
	reader   *strings.Reader
	reads    sync.WaitGroup
	once     sync.Once
}

func (p *partyRouteProvider) Stream(ctx context.Context, _ streaming.Request) (*streaming.Response, error) {
	p.calls.Add(1)
	body := &partyRouteBody{provider: p, ctx: ctx, reader: strings.NewReader(partyRoutePayload)}
	return &streaming.Response{Body: body, Status: http.StatusOK, Headers: make(http.Header), ContentLength: int64(len(partyRoutePayload))}, nil
}

func (b *partyRouteBody) Read(dst []byte) (int, error) {
	b.reads.Add(1)
	defer b.reads.Done()
	p := b.provider
	if p.hold {
		p.enterOnce.Do(func() { close(p.entered) })
		<-b.ctx.Done()
		p.cancelOnce.Do(func() { close(p.canceled) })
		<-p.release
		p.returnOnce.Do(func() { close(p.returned) })
		return 0, b.ctx.Err()
	}
	return b.reader.Read(dst)
}

func (b *partyRouteBody) Close() error {
	b.once.Do(func() { b.reads.Wait(); b.provider.closed.Add(1) })
	return nil
}

func partyRouteRegister(t *testing.T, f *watchRoomRouteFixture, video *handlers.VideoHandler, pins *profileunlock.PGProfilePins) *mux.Router {
	t.Helper()
	accountService, e := accounts.NewServiceWithStore(f.store)
	if e != nil {
		t.Fatal(e)
	}
	meta := handlers.NewMetadataHandler(f.base.facts, nil)
	meta.UsersService = f.base.users
	list := handlers.NewDisplayListHandler(nil, nil, f.base.users)
	router := mux.NewRouter()
	Register(router, nil, meta, nil, nil, nil, nil, nil, nil, video, handlers.NewUsersHandler(f.base.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		accountService, f.base.sessions, f.base.users, nil, f.rooms, f.party, "", nil,
		WithPGProfilePins(pins), WithProfileUnlocks(profileunlock.NewService(f.store.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} }))
	RegisterWatchPartyPageRoutes(router, f.party)
	return router
}

func partyRouteWait(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("actual party operation did not finish: " + label)
	}
}

// This complete flow creates the original room/invite/source through the real
// registered native routes and the guest/grant through real public handlers.
func partyRouteGrant(t *testing.T, f *watchRoomRouteFixture, resource string) (string, string, *http.Cookie) {
	t.Helper()
	path := "/api/users/" + f.base.open + "/watch-rooms"
	w := f.call(t, path, watchRoomRouteInput())
	var room models.WatchRoom
	if w.Code != http.StatusCreated || json.Unmarshal(w.Body.Bytes(), &room) != nil || room.ID == "" {
		t.Fatal("registered original room creation failed", w.Code)
	}
	path += "/" + room.ID
	w = f.call(t, path+"/external-invite", map[string]any{})
	var invitation struct {
		URL string `json:"url"`
	}
	if w.Code != http.StatusCreated || json.Unmarshal(w.Body.Bytes(), &invitation) != nil {
		t.Fatal("registered original invitation creation failed", w.Code)
	}
	inviteURL, e := url.Parse(invitation.URL)
	if e != nil || inviteURL.Path == "" {
		t.Fatal("registered invitation URL unavailable")
	}
	join := httptest.NewRequest(http.MethodPost, "http://synthetic.test"+inviteURL.Path, strings.NewReader(url.Values{"name": {"Synthetic API guest"}}.Encode()))
	join.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w = httptest.NewRecorder()
	f.router.ServeHTTP(w, join)
	var cookie *http.Cookie
	for _, candidate := range w.Result().Cookies() {
		if candidate.Name == "strmr_watch_party" {
			cookie = candidate
		}
	}
	if w.Code != http.StatusSeeOther || cookie == nil || !cookie.HttpOnly || cookie.Value == "" {
		t.Fatal("registered guest join did not publish its actual cookie", w.Code)
	}
	grantPath := "/api/watch-party/room/" + room.ID + "/playback-grant"
	request := httptest.NewRequest(http.MethodPost, "http://synthetic.test"+grantPath, nil)
	request.AddCookie(cookie)
	w = httptest.NewRecorder()
	f.router.ServeHTTP(w, request)
	if w.Code == http.StatusOK {
		t.Fatal("lobby cookie acquired media without an original source")
	}
	w = f.call(t, path+"/external-source", map[string]any{"source": resource, "params": map[string]string{"titleId": "synthetic-title"}})
	if w.Code != http.StatusOK {
		t.Fatal("registered original source binding failed", w.Code)
	}
	// This fixture changes ordinary playback DATA only. Original 089 source and
	// 090 guest provenance are created solely by the real operations above.
	if _, e = f.store.Pool().Exec(t.Context(), `UPDATE watch_rooms SET status='playing',waiting_for_ready=false WHERE id=$1`, room.ID); e != nil {
		t.Fatal(e)
	}
	request = httptest.NewRequest(http.MethodPost, "http://synthetic.test"+grantPath, nil)
	request.AddCookie(cookie)
	w = httptest.NewRecorder()
	f.router.ServeHTTP(w, request)
	var result struct {
		URL string `json:"url"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatal("registered restricted playback grant failed", w.Code)
	}
	parsed, e := url.Parse(result.URL)
	if e != nil || parsed.Query().Get("token") == "" || parsed.Query().Get("profileId") != f.base.open {
		t.Fatal("registered playback grant omitted its actual stored source")
	}
	return room.ID, parsed.Query().Get("token"), cookie
}

func TestWatchPartyActualMainPlayback(t *testing.T) {
	for _, kind := range []string{"actual-main-typed-install", "registered-guest-to-grant", "registered-restricted-stream", "missing-party-runtime-denied", "share-runtime-missing-party-valid", "old-generic-playback-no-upgrade", "current-resource-check-consumed", "startup-close-held-real-consumer"} {
		t.Run(kind, func(t *testing.T) {
			f := watchRoomRegisteredPG(t, false)
			p := &partyRouteProvider{hold: kind == "startup-close-held-real-consumer", entered: make(chan struct{}), canceled: make(chan struct{}), returned: make(chan struct{}), release: make(chan struct{})}
			releaseRead := func() { p.releaseOnce.Do(func() { close(p.release) }) }
			video := handlers.NewVideoHandlerWithProvider(false, "", "", t.TempDir(), p)
			t.Cleanup(func() { releaseRead(); video.Shutdown() })
			pins := profileunlock.NewPGProfilePins(f.store, f.base.sessions)
			if e := ConfigureManagedWatchPartyPlayback(f.startup, f.store, pins, video, f.party); e != nil {
				t.Fatal(e)
			}
			f.router = partyRouteRegister(t, f, video, pins)
			resource := "synthetic:registered-party"
			room, token, cookie := partyRouteGrant(t, f, resource)
			runtime, e := f.startup.NewWatchRoomAuthority(f.store)
			if e != nil || !pins.WatchPartyPlaybackReady(runtime) {
				t.Fatal("actual Main typed runtime was not installed", e)
			}
			if kind == "registered-guest-to-grant" {
				var count int
				e := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_party_playback_bindings b JOIN sessions s ON s.token=b.session_token AND s.watch_party_playback_binding_id=b.binding_id JOIN watch_party_guest_bindings g ON g.session_token=b.guest_session_token AND g.binding_id=b.guest_binding_id WHERE s.token=$1 AND s.scope='stream' AND NOT s.is_master AND b.room_id=$2 AND b.resource=$3 AND b.source_profile_id=$4 AND s.expires_at<=g.expires_at`, token, room, resource, f.base.open).Scan(&count)
				if e != nil || count != 1 || p.calls.Load() != 0 {
					t.Fatal("registered known-ACK grant missed its actual private bindings", count, e)
				}
			}
			if kind == "actual-main-typed-install" {
				again, e := f.startup.NewWatchRoomAuthority(f.store)
				if e != nil || again != runtime {
					t.Fatal("actual Main composition replaced its original runtime", e)
				}
			}
			if kind == "missing-party-runtime-denied" {
				if e := video.SetManagedWatchPartyPlayback(nil, pins); e == nil {
					t.Fatal("missing media consumer runtime was accepted")
				}
			}
			if kind == "share-runtime-missing-party-valid" {
				if e := video.SetManagedSharePlayback(nil, pins); e == nil {
					t.Fatal("missing unrelated share runtime was accepted")
				}
			}
			if kind == "old-generic-playback-no-upgrade" {
				var guest string
				if e := f.store.Pool().QueryRow(t.Context(), `SELECT guest_id FROM watch_party_guest_bindings WHERE session_token=$1`, cookie.Value).Scan(&guest); e != nil {
					t.Fatal(e)
				}
				legacy, e := f.base.sessions.CreateWatchPartyPlayback(f.base.account, "Synthetic legacy guest", "127.0.0.1", time.Hour, room, guest, resource)
				if e != nil {
					t.Fatal(e)
				}
				token = legacy.Token
			}
			path := "/api/video/stream?path=" + url.QueryEscape(models.SessionScopeResourcePlaceholder) + "&profileId=" + url.QueryEscape(f.base.open) + "&token=" + url.QueryEscape(token)
			if kind == "current-resource-check-consumed" {
				raw := httptest.NewRequest(http.MethodGet, "http://synthetic.test"+path, nil)
				actual, release, e := pins.Authenticate(raw, token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
				owner, e := pins.CaptureWatchPartyPlaybackOwner(actual, f.base.open, resource)
				if e != nil {
					t.Fatal(e)
				}
				defer owner.Close()
				bound, e := owner.BindResourceRequest(actual)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = f.store.Pool().Exec(t.Context(), `UPDATE watch_room_external_sources SET params=params||'{"audioTrack":"7"}'::jsonb WHERE room_id=$1`, room); e != nil {
					t.Fatal(e)
				}
				w := httptest.NewRecorder()
				if profileaccess.CheckResource(w, bound, f.base.open, resource) || w.Code != http.StatusNotFound || p.calls.Load() != 0 {
					t.Fatal("resource middleware ignored the original revoked party source", w.Code)
				}
				return
			}
			if kind == "startup-close-held-real-consumer" {
				partyRouteHeldConsumer(t, f, p, path, releaseRead)
				return
			}
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "http://synthetic.test"+path, nil))
			if kind == "missing-party-runtime-denied" || kind == "old-generic-playback-no-upgrade" {
				want := http.StatusUnauthorized
				if kind == "missing-party-runtime-denied" {
					want = http.StatusServiceUnavailable
				}
				if w.Code != want || p.calls.Load() != 0 {
					t.Fatal("registered restricted route bypassed missing original authority", w.Code, p.calls.Load())
				}
				var count int
				if e := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_party_playback_bindings`).Scan(&count); e != nil || count != 1 {
					t.Fatal("denied route adopted or recreated a private playback binding", count, e)
				}
				return
			}
			if w.Code != http.StatusOK || w.Body.String() != partyRoutePayload || p.calls.Load() != 1 || p.closed.Load() != 1 {
				t.Fatal("registered original party consumer did not complete actual bytes", w.Code, p.calls.Load(), p.closed.Load())
			}
		})
	}
}

func partyRouteHeldConsumer(t *testing.T, f *watchRoomRouteFixture, p *partyRouteProvider, path string, releaseRead func()) {
	t.Helper()
	server := httptest.NewServer(f.router)
	client := server.Client()
	client.Timeout = 20 * time.Second
	clientDone, closeDone := make(chan struct{}), make(chan struct{})
	var clientStarted, closeStarted bool
	var closeErr error
	var leakedMedia atomic.Bool
	t.Cleanup(func() {
		releaseRead()
		if clientStarted {
			partyRouteWait(t, clientDone, "original TCP consumer cleanup")
		}
		if closeStarted {
			partyRouteWait(t, closeDone, "original Startup.Close cleanup")
		}
		client.CloseIdleConnections()
		server.Close()
	})
	clientStarted = true
	go func() {
		defer close(clientDone)
		response, e := client.Get(server.URL + path)
		if e == nil {
			data, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			leakedMedia.Store(strings.Contains(string(data), partyRoutePayload))
		}
	}()
	partyRouteWait(t, p.entered, "actual provider Read")
	closeStarted = true
	go func() { defer close(closeDone); closeErr = f.startup.Close() }()
	partyRouteWait(t, p.canceled, "actual source context cancellation")
	select {
	case <-closeDone:
		t.Fatal("Startup.Close refunded the held actual source read")
	case <-time.After(30 * time.Millisecond):
	}
	if p.closed.Load() != 0 {
		t.Fatal("actual body closed before its held Read returned")
	}
	releaseRead()
	partyRouteWait(t, p.returned, "actual provider Read return")
	partyRouteWait(t, clientDone, "actual TCP response/body end")
	partyRouteWait(t, closeDone, "actual Startup.Close end")
	if closeErr != nil || p.calls.Load() != 1 || p.closed.Load() != 1 || leakedMedia.Load() {
		t.Fatal("held original party consumer cleanup mismatch", closeErr, p.calls.Load(), p.closed.Load(), leakedMedia.Load())
	}
}
