//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/streaming"
)

func partyMediaRequest(t *testing.T, f *partyHTTPFixture, path string, next http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	f.serveVideo(t, w, httptest.NewRequest("GET", "http://synthetic.test"+path, nil), next)
	return w
}
func partyMediaPath(f *partyHTTPFixture, endpoint string) string {
	return "/api/video/" + endpoint + "?path=" + url.QueryEscape(models.SessionScopeResourcePlaceholder) + "&profileId=" + url.QueryEscape(f.native.profile)
}

// A real opener stays alive after its original context is cancelled. It returns
// a late, physically held body, so pending-open and body refunds are observable.
type partyHeldOpenProvider struct {
	entered                chan context.Context
	returnBody, allowClose chan struct{}
	returnOnce, closeOnce  sync.Once
	calls, closed, closing atomic.Int32
}
type partyLateBody struct {
	p    *partyHeldOpenProvider
	once sync.Once
}

func (p *partyHeldOpenProvider) Stream(ctx context.Context, _ streaming.Request) (*streaming.Response, error) {
	p.calls.Add(1)
	p.entered <- ctx
	<-p.returnBody
	return &streaming.Response{Status: 200, Headers: make(http.Header), Body: &partyLateBody{p: p}}, nil
}
func (b *partyLateBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *partyLateBody) Close() error {
	b.once.Do(func() { b.p.closing.Add(1); <-b.p.allowClose; b.p.closed.Add(1) })
	return nil
}
func (p *partyHeldOpenProvider) release() {
	p.returnOnce.Do(func() { close(p.returnBody) })
	p.closeOnce.Do(func() { close(p.allowClose) })
}

func partyHeldOpenBudget(t *testing.T, f *partyHTTPFixture) {
	t.Helper()
	p := &partyHeldOpenProvider{entered: make(chan context.Context, 33), returnBody: make(chan struct{}), allowClose: make(chan struct{})}
	f.video.streamer = p
	done := make(chan struct{})
	entered := make(chan *shareMediaGuard, 1)
	start := make(chan struct{})
	var worker sync.WaitGroup
	var bad atomic.Int32
	t.Cleanup(func() { p.release(); closeOnceChannel(start); shareHLSTestWait(t, done, "pending-open entry cleanup") })
	go func() {
		defer close(done)
		f.serveVideo(t, httptest.NewRecorder(), httptest.NewRequest("GET", "http://synthetic.test"+partyMediaPath(f, "stream"), nil), http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			g := shareMediaFrom(r.Context())
			entered <- g
			<-start
			for i := 0; i < 32; i++ {
				worker.Add(1)
				go func() {
					defer worker.Done()
					response, e := f.video.vodOpenStream(r.Context(), streaming.Request{Path: f.resource, Method: "GET"})
					if response != nil {
						_ = response.Close()
					}
					if e == nil {
						bad.Add(1)
					}
				}()
				select {
				case <-p.entered:
				case <-time.After(12 * time.Second):
					t.Error("actual pending opener not entered")
					p.release()
					worker.Wait()
					return
				}
			}
			if response, e := f.video.vodOpenStream(r.Context(), streaming.Request{Path: f.resource, Method: "GET"}); e == nil || response != nil || p.calls.Load() != 32 {
				t.Error("pending+active source cap did not deny before the 33rd actual provider")
			}
			worker.Wait()
		}))
	}()
	var g *shareMediaGuard
	select {
	case g = <-entered:
	case <-done:
		t.Fatal("original party source entry denied before pending-open canary")
	case <-time.After(12 * time.Second):
		t.Fatal("original party source entry timed out")
	}
	if g == nil || g.owner.party == nil {
		t.Fatal("missing genuine party source")
	}
	close(start)
	shareHLSTestPoll(t, "all actual pending opens", func() bool { return p.calls.Load() == 32 })
	closed := make(chan struct{})
	t.Cleanup(func() { p.release(); shareHLSTestWait(t, closed, "authority close") })
	go func() { f.native.authority.Close(); close(closed) }()
	shareHLSTestPoll(t, "original source cancellation", func() bool { return g.ctx.Err() != nil })
	shareHLSTestPending(t, closed, "authority with pending real opens")
	p.returnOnce.Do(func() { close(p.returnBody) })
	shareHLSTestPoll(t, "late actual bodies", func() bool { return p.closing.Load() == 32 })
	shareHLSTestPending(t, closed, "authority with physically held late bodies")
	p.closeOnce.Do(func() { close(p.allowClose) })
	shareHLSTestWait(t, done, "all pending source operations")
	shareHLSTestWait(t, closed, "authority physical source join")
	if bad.Load() != 0 || p.closed.Load() != 32 {
		t.Fatal("late response published or source cleanup not physically joined", bad.Load(), p.closed.Load())
	}
}

func closeOnceChannel(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func TestWatchPartyActualMediaOwner(t *testing.T) {
	for _, kind := range []string{"raw-stream", "metadata", "same-app-direct-url", "restricted-profile-resource", "lookalike-party-denied", "held-provider-read-revocation", "held-response-write-revocation", "source-worker-joins", "runtime-close-joins", "global-vod-boundary-nil", "share-and-full-unchanged", "unsupported-secondary-no-job"} {
		t.Run(kind, func(t *testing.T) {
			p := &shareStaticProvider{data: shareWAV()}
			f := newPartyHTTP(t, t.Context(), p, "", false)
			if kind == "restricted-profile-resource" {
				f.resource = "localmedia:party-item/synthetic.wav"
				for _, q := range []string{`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('party-lib','Synthetic party library','movie','/synthetic')`, `INSERT INTO local_media_items(id,library_id,relative_path,file_path,library_type)VALUES('party-item','party-lib','synthetic.wav','/synthetic/synthetic.wav','movie')`, `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('party-lib','restricted')`} {
					if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), q); e != nil {
						t.Fatal(e)
					}
				}
				if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `INSERT INTO media_library_profile_grants(library_id,profile_id)VALUES('party-lib',$1)`, f.native.profile); e != nil {
					t.Fatal(e)
				}
			}
			f.join(t)
			f.bind(t)
			f.grant(t)
			if f.video.vodBoundary != nil {
				t.Fatal("party must consume actual source with global VOD boundary nil")
			}
			if kind == "source-worker-joins" {
				partyHeldOpenBudget(t, f)
				return
			}
			if kind == "held-provider-read-revocation" || kind == "runtime-close-joins" {
				r, w := io.Pipe()
				b := &shareHeldReadBody{r: r, w: w, entered: make(chan struct{})}
				f.video.streamer = &shareHeldReadProvider{body: b}
				done := make(chan struct{})
				var response *httptest.ResponseRecorder
				t.Cleanup(func() { _ = b.Close(); shareHLSTestWait(t, done, "real party Read handler") })
				go func() {
					defer close(done)
					response = partyMediaRequest(t, f, partyMediaPath(f, "stream"), f.video.StreamVideo)
				}()
				shareHLSTestWait(t, b.entered, "actual held party Read")
				if kind == "runtime-close-joins" {
					f.native.authority.Close()
				} else if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.native.profile); e != nil {
					t.Fatal(e)
				}
				shareHLSTestWait(t, done, "retired actual party Read")
				if !b.ended.Load() || response == nil || response.Body.Len() != 0 {
					t.Fatal("source output/refund preceded actual blocked Read end")
				}
				return
			}
			if kind == "held-response-write-revocation" {
				// Actual current DB denial before the first Flush must never become an
				// empty200, and already committed real output must be interrupted.
				w := partyMediaRequest(t, f, partyMediaPath(f, "stream"), func(w http.ResponseWriter, r *http.Request) {
					if _, e := f.native.base.base.pin.store.Pool().Exec(r.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.native.profile); e != nil {
						t.Error(e)
						return
					}
					_ = http.NewResponseController(w).Flush()
				})
				if w.Code != 403 {
					t.Fatal("retired pre-header party Flush emitted success", w.Code)
				}
				// New original room/invite/guest/source after re-enable, never upgrade
				// the retired token or mutate its private provenance.
				if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, f.native.profile); e != nil {
					t.Fatal(e)
				}
				f.room = f.native.create(t)
				var e error
				f.invite, e = f.native.authority.ReplaceExternalInvite(f.native.request(t, "POST", "/api/users/"+f.native.profile+"/watch-rooms/"+f.room.ID+"/external-invite", nil, t.Context()), f.native.profile, f.room.ID)
				if e != nil {
					t.Fatal(e)
				}
				f.cookie = nil
				f.join(t)
				f.bind(t)
				f.grant(t)
				f.video.streamer = &shareStaticProvider{data: bytes.Repeat([]byte("SYNTHETIC-PARTY-OUTPUT"), 1<<19)}
				done := make(chan struct{})
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(done)
					f.serveVideo(t, w, r, http.HandlerFunc(f.video.StreamVideo))
				}))
				listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
				server.Listener = listener
				server.Start()
				c, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if e != nil {
					server.Close()
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = c.Close(); server.Close() })
				if tcp, ok := c.(*net.TCPConn); ok {
					_ = tcp.SetReadBuffer(1024)
				}
				if _, e = io.WriteString(c, "GET "+partyMediaPath(f, "stream")+" HTTP/1.1\r\nHost: synthetic.test\r\nConnection: close\r\n\r\n"); e != nil {
					t.Fatal(e)
				}
				var actual *shareEntryConn
				select {
				case actual = <-listener.accepted:
				case <-time.After(5 * time.Second):
					t.Fatal("missing real output socket")
				}
				shareHLSTestPoll(t, "real blocked party output", func() bool { return actual.writing.Load() > 0 })
				if _, e = f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.native.profile); e != nil {
					t.Fatal(e)
				}
				shareHLSTestWait(t, done, "real response output join")
				if actual.writing.Load() != 0 || !actual.writeDemanded.Load() {
					t.Fatal("party source revoke failed to interrupt and join output")
				}
				return
			}
			if kind == "lookalike-party-denied" {
				legacy, e := f.native.base.base.pin.sessions.CreateWatchPartyPlayback(f.native.base.base.pin.session.AccountID, "Synthetic unproven guest", "192.0.2.9", time.Hour, f.room.ID, watchPartyGuestID(f.cookie.Value), f.resource)
				if e != nil {
					t.Fatal(e)
				}
				f.token = legacy.Token
				w := partyMediaRequest(t, f, partyMediaPath(f, "stream"), f.video.StreamVideo)
				if w.Code != 401 || p.calls.Load() != 0 {
					t.Fatal("lookalike party token opened source", w.Code)
				}
				return
			}
			if kind == "restricted-profile-resource" {
				for _, path := range []string{"/api/video/stream?path=" + url.QueryEscape(f.resource) + "&profileId=synthetic-wrong", "/api/video/stream?path=synthetic:wrong&profileId=" + f.native.profile} {
					w := partyMediaRequest(t, f, path, f.video.StreamVideo)
					if w.Code != 403 || p.calls.Load() != 0 {
						t.Fatal("party selector substitution opened source", w.Code)
					}
				}
				store := f.native.base.base.pin.store
				cached := libraryaccess.New(store.LibraryAccess(), store.LocalMedia(), store.RemoteMedia())
				f.video.SetLibraryAccessService(cached)
				if known, allow, e := cached.CanAccessStream(t.Context(), f.resource, f.native.base.base.pin.session.AccountID, f.native.profile, false); e != nil || !known || !allow {
					t.Fatal("actual party cached allow not primed", e)
				}
				entered := make(chan struct{})
				done := make(chan struct{})
				var guard *shareMediaGuard
				original, cancel := context.WithCancel(t.Context())
				t.Cleanup(func() { cancel(); shareHLSTestWait(t, done, "current SQL ACL owner") })
				go func() {
					defer close(done)
					r := httptest.NewRequest("GET", "http://synthetic.test"+partyMediaPath(f, "stream"), nil).WithContext(original)
					f.serveVideo(t, httptest.NewRecorder(), r, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
						guard = shareMediaFrom(r.Context())
						close(entered)
						<-r.Context().Done()
					}))
				}()
				shareHLSTestWait(t, entered, "actual party owner before ACL revoke")
				if _, e := store.Pool().Exec(t.Context(), `DELETE FROM media_library_profile_grants WHERE library_id='party-lib' AND profile_id=$1`, f.native.profile); e != nil {
					t.Fatal(e)
				}
				if _, allow, e := cached.CanAccessStream(t.Context(), f.resource, f.native.base.base.pin.session.AccountID, f.native.profile, false); e != nil || !allow {
					t.Fatal("lost intentionally stale party cachedallow", e)
				}
				shareHLSTestWait(t, done, "current SQL revoke despite cached allow")
				if guard == nil || guard.ctx.Err() == nil || p.calls.Load() != 0 {
					t.Fatal("cached allow overrode current source SQL")
				}
				return
			}
			if kind == "unsupported-secondary-no-job" {
				// Nested live/YouTube inputs remain outside the owned raw-container
				// capability. Positive crop/subtitle/credits actors are supplied by
				// the separately reviewed ancillary packet, not denied by this case.
				for _, operation := range []struct {
					path string
					next http.HandlerFunc
				}{{"/api/live/hls/start", f.video.StartLiveHLSSession}, {"/api/youtube/hls/start", f.video.StartYouTubeHLSSession}} {
					path := operation.path + "?path=" + url.QueryEscape(models.SessionScopeResourcePlaceholder) + "&profileId=" + url.QueryEscape(f.native.profile)
					w := partyMediaRequest(t, f, path, operation.next)
					if w.Code != 501 || p.calls.Load() != 0 {
						t.Fatal("unsupported party capability created source work", w.Code)
					}
				}
				return
			}
			if kind == "share-and-full-unchanged" {
				// Exercise the actual independent F1b share, then the current full PG
				// session through the same VideoHandler; party never becomes full.
				base := f.native.base
				creator, e := datastore.NewShareLinkCreator(t.Context(), base.base.pin.store, base.manager, base.security, base.coordinator)
				if e != nil {
					t.Fatal(e)
				}
				original, release, e := f.pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil), base.base.pin.session.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				link, e := creator.Create(original, f.native.profile, map[string]string{"profileId": f.native.profile, "sourcePath": f.resource}, time.Hour, 4, "")
				release()
				if e != nil {
					t.Fatal(e)
				}
				runtime, e := datastore.NewShareLinkRedeemer(t.Context(), base.base.pin.store, base.manager, base.security, base.coordinator)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(runtime.Close)
				if e = f.pins.SetManagedSharePlayback(runtime); e != nil {
					t.Fatal(e)
				}
				if e = f.video.SetManagedSharePlayback(runtime, f.pins); e != nil {
					t.Fatal(e)
				}
				redemption, e := runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
				if e != nil {
					t.Fatal(e)
				}
				f.token = redemption.Session.Token
				w := partyMediaRequest(t, f, partyMediaPath(f, "stream"), f.video.StreamVideo)
				if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), p.data) {
					t.Fatal("separate original share changed", w.Code)
				}
				f.token = f.native.base.base.pin.session.Token
			}
			endpoint := "stream"
			next := http.HandlerFunc(f.video.StreamVideo)
			if kind == "metadata" {
				endpoint = "metadata"
				next = f.video.ProbeVideo
			}
			if kind == "same-app-direct-url" {
				endpoint = "direct-url"
				next = f.video.GetDirectURL
			}
			path := partyMediaPath(f, endpoint)
			if kind == "share-and-full-unchanged" {
				path = "/api/video/stream?path=" + url.QueryEscape(f.resource) + "&profileId=" + f.native.profile
			}
			w := partyMediaRequest(t, f, path, next)
			if w.Code != 200 {
				t.Fatal("actual original party media", kind, w.Code, w.Body.String())
			}
			if kind == "metadata" {
				var result videoMetadataResponse
				if json.Unmarshal(w.Body.Bytes(), &result) != nil || result.DurationSeconds <= 0 || p.calls.Load() != 1 || p.closed.Load() != 1 {
					t.Fatal("party metadata did not use joined actual source")
				}
				return
			}
			if kind == "same-app-direct-url" {
				var result map[string]string
				if json.Unmarshal(w.Body.Bytes(), &result) != nil {
					t.Fatal("direct URL wire")
				}
				u, e := url.Parse(result["url"])
				if e != nil || u.IsAbs() || u.Path != "/api/video/stream" || u.Query().Get("path") != models.SessionScopeResourcePlaceholder || u.Query().Get("token") != f.token || p.calls.Load() != 0 {
					t.Fatal("party URL escaped current scoped same-app proxy")
				}
				w = partyMediaRequest(t, f, u.RequestURI(), f.video.StreamVideo)
			}
			if !bytes.Equal(w.Body.Bytes(), p.data) || !strings.Contains(w.Header().Get("Content-Type"), "audio") && w.Body.Len() == 0 {
				t.Fatal("original party bytes missing")
			}
		})
	}
}
