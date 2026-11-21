//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/watchrooms"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
)

// This fixture executes actual current migrations, an original full PG creator,
// the real same-TX public redemption and genuine subsequent scoped frames. No
// response/profile/session DTO is used to manufacture source authority.
type sharePlaybackHTTPFixture struct {
	base                                  *progressHTTPFixture
	store                                 *datastore.DataStore
	manager                               *config.Manager
	security                              *config.ManagedSecurity
	coordinator                           *settingscommit.Coordinator
	runtime                               *datastore.ShareLinkRedeemer
	pins                                  *profileunlock.PGProfilePins
	video                                 *VideoHandler
	share                                 *ShareHandler
	profileID, resource, token, linkToken string
}

func newSharePlaybackHTTPFixture(t *testing.T, resource string, provider streaming.Provider) *sharePlaybackHTTPFixture {
	t.Helper()
	base := progressHTTPPG(t, false)
	store := base.base.pin.store
	if _, err := store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, base.base.pin.profile.ID); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(resource, "localmedia:") {
		queries := []string{
			`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('share-lib','Synthetic library','movie','/synthetic')`,
			`INSERT INTO local_media_items(id,library_id,relative_path,file_path,library_type)VALUES('share-item','share-lib','synthetic.wav','/synthetic/synthetic.wav','movie')`,
			`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('share-lib','restricted')`,
		}
		for _, q := range queries {
			if _, e := store.Pool().Exec(t.Context(), q); e != nil {
				t.Fatal(e)
			}
		}
		if _, e := store.Pool().Exec(t.Context(), `INSERT INTO media_library_profile_grants(library_id,profile_id)VALUES('share-lib',$1)`, base.base.pin.profile.ID); e != nil {
			t.Fatal(e)
		}
	}
	runtime, err := datastore.NewShareLinkRedeemer(t.Context(), store, base.manager, base.security, base.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	pins := profileunlock.NewPGProfilePins(store, base.base.pin.sessions)
	if err = pins.SetManagedSharePlayback(runtime); err != nil {
		t.Fatal(err)
	}
	creator, err := datastore.NewShareLinkCreator(t.Context(), store, base.manager, base.security, base.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	original := httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil)
	checked, release, err := base.base.pin.pins.Authenticate(original, base.base.pin.session.Token, "synthetic-share-origin")
	if err != nil {
		t.Fatal(err)
	}
	link, err := creator.Create(checked, base.base.pin.profile.ID, map[string]string{"profileId": base.base.pin.profile.ID, "sourcePath": resource, "title": "Synthetic shared source"}, time.Hour, 4, "")
	release()
	if err != nil {
		t.Fatal("actual original share creation", err)
	}
	result, err := runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
	if err != nil {
		t.Fatal("actual public redemption", err)
	}
	if provider == nil {
		provider = &rootRawVideoProvider{}
	}
	video := NewVideoHandlerWithProvider(false, "/usr/bin/ffmpeg", "/usr/bin/ffprobe", t.TempDir(), provider)
	if err = video.SetManagedSharePlayback(runtime, pins); err != nil {
		t.Fatal(err)
	}
	share := NewShareHandler(NewShareStore(store.ShareLinks()), base.base.pin.sessions, base.base.pin.users, "")
	share.SetManagedShareCreation(creator)
	share.SetManagedShareRedemption(runtime)
	f := &sharePlaybackHTTPFixture{base: base, store: store, manager: base.manager, security: base.security, coordinator: base.coordinator, runtime: runtime, pins: pins, video: video, share: share, profileID: base.base.pin.profile.ID, resource: normalizeShareResource(resource), token: result.Session.Token, linkToken: link.Token}
	t.Cleanup(func() { video.Shutdown(); runtime.Close() })
	return f
}
func (f *sharePlaybackHTTPFixture) framed(t *testing.T, method, path string) (*http.Request, func()) {
	t.Helper()
	r := httptest.NewRequest(method, "http://synthetic.test"+path, nil)
	checked, release, err := f.pins.Authenticate(r, f.token, "")
	if err != nil {
		t.Fatal("actual original scoped source frame", err)
	}
	return checked, release
}
func (f *sharePlaybackHTTPFixture) serve(t *testing.T, w http.ResponseWriter, r *http.Request, next http.Handler) {
	t.Helper()
	checked, release, err := f.pins.Authenticate(r, f.token, "")
	if err != nil {
		t.Fatal("actual original scoped source frame", err)
	}
	defer release()
	f.video.ManagedSharePlaybackEntry(next).ServeHTTP(w, checked)
}

type shareStaticProvider struct {
	data   []byte
	calls  atomic.Int32
	closed atomic.Int32
}
type shareStaticBody struct {
	io.Reader
	provider *shareStaticProvider
	once     sync.Once
}

func (b *shareStaticBody) Close() error { b.once.Do(func() { b.provider.closed.Add(1) }); return nil }
func (p *shareStaticProvider) Stream(ctx context.Context, req streaming.Request) (*streaming.Response, error) {
	p.calls.Add(1)
	return &streaming.Response{Body: &shareStaticBody{Reader: bytes.NewReader(p.data), provider: p}, Headers: make(http.Header), Status: 200, ContentLength: int64(len(p.data))}, nil
}
func shareWAV() []byte {
	data := make([]byte, 44+16000)
	copy(data, "RIFF")
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)-8))
	copy(data[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(data[16:], 16)
	binary.LittleEndian.PutUint16(data[20:], 1)
	binary.LittleEndian.PutUint16(data[22:], 1)
	binary.LittleEndian.PutUint32(data[24:], 8000)
	binary.LittleEndian.PutUint32(data[28:], 16000)
	binary.LittleEndian.PutUint16(data[32:], 2)
	binary.LittleEndian.PutUint16(data[34:], 16)
	copy(data[36:], "data")
	binary.LittleEndian.PutUint32(data[40:], 16000)
	return data
}
func shareActualPartyHTTP(t *testing.T, f *sharePlaybackHTTPFixture) string {
	t.Helper()
	repo := f.store.WatchRooms()
	rooms := watchrooms.New(repo, f.base.base.pin.users, nil)
	f.base.base.pin.sessions.SetWatchPartyPlaybackValidator(rooms.ValidateExternalPlayback)
	now := time.Now().UTC()
	room := &models.WatchRoom{ID: "synthetic-share-party", CreatorProfileID: f.profileID, Title: "Synthetic party", MediaType: "movie", ItemID: "synthetic-item", Params: map[string]string{}, Status: models.WatchRoomStatusPlaying, Revision: 1, AnchorUpdatedAt: now, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if e := repo.Create(t.Context(), room, nil, "", models.WatchRoomClientCapabilities{}); e != nil {
		t.Fatal(e)
	}
	if e := repo.JoinExternalGuest(t.Context(), room.ID, "synthetic-share-guest", "Synthetic guest", "", models.WatchRoomClientCapabilities{}, now); e != nil {
		t.Fatal(e)
	}
	if yes, e := repo.BindExternalSource(t.Context(), room.ID, f.profileID, f.resource, map[string]string{}, now); e != nil || !yes {
		t.Fatal("actual party source", e)
	}
	s, e := f.base.base.pin.sessions.CreateWatchPartyPlayback(f.base.base.pin.session.AccountID, "Synthetic party", "127.0.0.1", time.Hour, room.ID, "synthetic-share-guest", f.resource)
	if e != nil {
		t.Fatal(e)
	}
	return s.Token
}
func shareInvokeHTTP(t *testing.T, f *sharePlaybackHTTPFixture, path string, handler http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", "http://synthetic.test"+path, nil)
	w := httptest.NewRecorder()
	f.serve(t, w, r, handler)
	return w
}
func TestSharePlaybackActualVideoBoundary(t *testing.T) {
	for _, kind := range []string{"raw-stream", "metadata", "direct-url", "exact-stored-profile", "resource-placeholder", "substituted-profile", "substituted-resource", "same-resource-other-recipient", "provider-held-read-revocation", "response-held-write-revocation", "vod-boundary-nil-real-consumer", "unrelated-full-session-unchanged", "party-unchanged", "library-access-revoked"} {
		t.Run(kind, func(t *testing.T) {
			provider := &shareStaticProvider{data: shareWAV()}
			resource := "synthetic:owned.wav"
			if kind == "library-access-revoked" {
				resource = "localmedia:share-item/synthetic.wav"
			}
			f := newSharePlaybackHTTPFixture(t, resource, provider)
			if f.video.vodBoundary != nil {
				t.Fatal("test accidentally installed global family boundary")
			}
			if kind == "provider-held-read-revocation" {
				shareHeldReadRevocation(t, f)
				return
			}
			if kind == "response-held-write-revocation" {
				shareHeldOutputRevocation(t, f)
				return
			}
			if kind == "same-resource-other-recipient" {
				r, release := f.framed(t, "GET", "/api/video/stream")
				defer release()
				o, e := f.pins.CaptureSharePlaybackOwner(r, f.profileID, f.resource)
				if e != nil {
					t.Fatal(e)
				}
				defer o.Close()
				second, e := f.runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+f.linkToken, nil), f.linkToken)
				if e != nil {
					t.Fatal(e)
				}
				original := f.token
				f.token = second.Session.Token
				other, stop := f.framed(t, "GET", "/api/video/stream")
				defer stop()
				f.token = original
				if o.CheckRequest(other, f.profileID, f.resource) == nil {
					t.Fatal("different actual recipient adopted old source")
				}
				return
			}
			if kind == "library-access-revoked" {
				cached := libraryaccess.New(f.store.LibraryAccess(), f.store.LocalMedia(), f.store.RemoteMedia())
				f.video.SetLibraryAccessService(cached)
				if known, allow, e := cached.CanAccessStream(t.Context(), resource, f.base.base.pin.session.AccountID, f.profileID, false); e != nil || !known || !allow {
					t.Fatal("actual cached allow not primed", e)
				}
				// A current source is already owned before the actual DB grant is revoked.
				r, release := f.framed(t, "GET", "/api/video/stream?path="+url.QueryEscape(resource)+"&profileId="+f.profileID)
				defer release()
				done := make(chan struct{})
				entered := make(chan struct{})
				var old *shareMediaGuard
				go func() {
					defer close(done)
					f.video.ManagedSharePlaybackEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						old = shareMediaFrom(r.Context())
						close(entered)
						<-r.Context().Done()
					})).ServeHTTP(httptest.NewRecorder(), r)
				}()
				progressHTTPAwait(t, entered)
				if _, e := f.store.Pool().Exec(t.Context(), `DELETE FROM media_library_profile_grants WHERE library_id='share-lib' AND profile_id=$1`, f.profileID); e != nil {
					t.Fatal(e)
				}
				if _, allow, e := cached.CanAccessStream(t.Context(), resource, f.base.base.pin.session.AccountID, f.profileID, false); e != nil || !allow {
					t.Fatal("canary lost its intentionally stale cached allow", e)
				}
				progressHTTPAwait(t, done)
				if old == nil || old.ctx.Err() == nil || provider.calls.Load() != 0 {
					t.Fatal("actual current DB revoke failed to retire source before any provider")
				}
				return
			}
			path := "/api/video/stream?path=" + url.QueryEscape(resource) + "&profileId=" + f.profileID
			handler := http.HandlerFunc(f.video.StreamVideo)
			switch kind {
			case "metadata":
				path = "/api/video/metadata?path=" + url.QueryEscape(resource) + "&profileId=" + f.profileID
				handler = f.video.ProbeVideo
			case "direct-url":
				path = "/api/video/direct-url?path=" + url.QueryEscape(resource) + "&profileId=" + f.profileID
				handler = f.video.GetDirectURL
			case "resource-placeholder":
				path = "/api/video/stream?path=%40shared&profileId=" + f.profileID
			case "substituted-profile":
				path = "/api/video/stream?path=" + url.QueryEscape(resource) + "&profileId=synthetic-wrong"
			case "substituted-resource":
				path = "/api/video/stream?path=synthetic:wrong&profileId=" + f.profileID
			case "unrelated-full-session-unchanged":
				f.token = f.base.base.pin.session.Token
			case "party-unchanged":
				f.token = shareActualPartyHTTP(t, f)
			}
			w := shareInvokeHTTP(t, f, path, handler)
			if kind == "substituted-profile" || kind == "substituted-resource" {
				if w.Code != 403 || provider.calls.Load() != 0 {
					t.Fatal("selector substitution opened source", w.Code, provider.calls.Load())
				}
				return
			}
			if w.Code != 200 {
				t.Fatal("actual source status", kind, w.Code, w.Body.String())
			}
			if kind == "direct-url" {
				var v map[string]string
				if e := json.Unmarshal(w.Body.Bytes(), &v); e != nil {
					t.Fatal(e)
				}
				u, e := url.Parse(v["url"])
				if e != nil || u.IsAbs() || u.Path != "/api/video/stream" || u.Query().Get("path") != "@shared" || u.Query().Get("token") != f.token || u.Query().Get("profileId") != f.profileID || provider.calls.Load() != 0 {
					t.Fatal("direct URL escaped genuine scoped proxy")
				}
				following := shareInvokeHTTP(t, f, u.RequestURI(), f.video.StreamVideo)
				if following.Code != 200 || !bytes.Equal(following.Body.Bytes(), provider.data) {
					t.Fatal("actual returned same-app scoped stream failed", following.Code)
				}
			} else if kind == "metadata" {
				var v videoMetadataResponse
				if e := json.Unmarshal(w.Body.Bytes(), &v); e != nil || v.DurationSeconds <= 0 || provider.calls.Load() != 1 || provider.closed.Load() != 1 {
					t.Fatal("actual retained source pipe metadata missing", e, provider.calls.Load(), provider.closed.Load())
				}
			} else if !bytes.Equal(w.Body.Bytes(), provider.data) || provider.calls.Load() != 1 || provider.closed.Load() != 1 {
				t.Fatal("actual original source/body not delivered/joined")
			}
		})
	}
}

type shareHeldReadProvider struct{ body *shareHeldReadBody }
type shareHeldReadBody struct {
	r       *io.PipeReader
	w       *io.PipeWriter
	entered chan struct{}
	once    sync.Once
	ended   atomic.Bool
}

func (b *shareHeldReadBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	defer b.ended.Store(true)
	return b.r.Read(p)
}
func (b *shareHeldReadBody) Close() error { _ = b.w.Close(); return b.r.Close() }
func (p *shareHeldReadProvider) Stream(context.Context, streaming.Request) (*streaming.Response, error) {
	return &streaming.Response{Body: p.body, Headers: make(http.Header), Status: 200}, nil
}
func shareHeldReadRevocation(t *testing.T, f *sharePlaybackHTTPFixture) {
	t.Helper()
	r, w := io.Pipe()
	body := &shareHeldReadBody{r: r, w: w, entered: make(chan struct{})}
	defer body.Close()
	f.video.streamer = &shareHeldReadProvider{body: body}
	done := make(chan struct{})
	go func() {
		defer close(done)
		shareInvokeHTTP(t, f, "/api/video/stream?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID, f.video.StreamVideo)
	}()
	progressHTTPAwait(t, body.entered)
	if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profileID); e != nil {
		t.Fatal(e)
	}
	progressHTTPAwait(t, done)
	if !body.ended.Load() {
		t.Fatal("handler/refund preceded actual blocked Read end")
	}
}
func shareHeldOutputRevocation(t *testing.T, f *sharePlaybackHTTPFixture) {
	t.Helper()
	// Revoke only actual PG state after genuine owner capture. Both an explicit
	// first Flush and the entry's final implicit Flush must intend denial, not200.
	// The recorder observes intended pre-header status; a cancelled real socket
	// may legitimately deliver no response, already covered by the held case.
	for _, explicit := range []bool{true, false} {
		record := httptest.NewRecorder()
		request := httptest.NewRequest("GET", "http://synthetic.test/api/video/stream?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID, nil)
		f.serve(t, record, request, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, e := f.store.Pool().Exec(r.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profileID); e != nil {
				t.Error(e)
				return
			}
			if explicit {
				_ = http.NewResponseController(w).Flush()
			}
		}))
		if record.Code != 403 || bytes.Contains(record.Body.Bytes(), []byte("SYNTHETIC-FRESH-OWNED-BYTES")) {
			t.Fatal("retired pre-header flush falsely emitted successful source", explicit, record.Code)
		}
		// A NEW original delegation after the actual re-enable is required; never
		// mutate a private nonce or revive the old original source owner/token.
		if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, f.profileID); e != nil {
			t.Fatal(e)
		}
		original, release, e := f.base.base.pin.pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil), f.base.base.pin.session.Token, "")
		if e != nil {
			t.Fatal(e)
		}
		link, e := f.share.creator.Create(original, f.profileID, map[string]string{"sourcePath": f.resource, "profileId": f.profileID}, time.Hour, 4, "")
		release()
		if e != nil {
			t.Fatal(e)
		}
		fresh, e := f.runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
		if e != nil {
			t.Fatal(e)
		}
		f.token = fresh.Session.Token
		f.linkToken = link.Token
	}

	f.video.streamer = &shareStaticProvider{data: bytes.Repeat([]byte("synthetic-owned-byte"), 1<<18)}
	done := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		f.serve(t, w, r, http.HandlerFunc(f.video.StreamVideo))
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
	if _, e = io.WriteString(c, "GET /api/video/stream?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID+" HTTP/1.1\r\nHost: synthetic.test\r\nConnection: close\r\n\r\n"); e != nil {
		t.Fatal(e)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no actual socket")
	}
	until := time.Now().Add(5 * time.Second)
	for actual.writing.Load() == 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if actual.writing.Load() == 0 {
		t.Fatal("actual original output did not block")
	}
	if _, e = f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profileID); e != nil {
		t.Fatal(e)
	}
	progressHTTPAwait(t, done)
	if actual.writing.Load() != 0 || !actual.writeDemanded.Load() {
		t.Fatal("source revoke failed to interrupt/join real output")
	}
}

func TestSharePlaybackSecondarySourceConsumers(t *testing.T) {
	for _, kind := range []string{"live-direct-body", "live-hls-worker", "subtitle-extract-worker", "subtitle-stored-profile", "thumbnail-worker", "thumbnail-stored-resource", "credits-source", "retired-source-no-cached-body"} {
		t.Run(kind, func(t *testing.T) {
			p := &shareStaticProvider{data: shareWAV()}
			resource := "synthetic:secondary.wav"
			var upstream *httptest.Server
			if kind == "live-direct-body" {
				upstream = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "video/mp2t")
					_, _ = w.Write([]byte("SYNTHETIC-OWNED-LIVE"))
				}))
				defer upstream.Close()
				resource = upstream.URL + "/synthetic.ts"
			}
			f := newSharePlaybackHTTPFixture(t, resource, p)

			path := "/api/video/subtitles/start?path=" + url.QueryEscape(f.resource) + "&profileId=" + f.profileID
			handler := http.HandlerFunc(f.video.StartSubtitleExtract)
			switch kind {
			case "live-direct-body":
				// Use actual current native configured origins before capturing this real
				// source owner; a client-only policy cannot bypass the handler entry gate.
				settings, e := f.manager.Load()
				if e != nil {
					t.Fatal(e)
				}
				settings.Server.AllowedPrivateMediaOrigins = []string{upstream.URL}
				if e = f.manager.Save(settings); e != nil {
					t.Fatal(e)
				}
				f.video.SetConfigManager(f.manager)
				r, release := f.framed(t, "GET", "/api/live/stream?url="+url.QueryEscape(f.resource)+"&profileId="+f.profileID)
				defer release()
				w := httptest.NewRecorder()
				f.video.ManagedSharePlaybackEntry(http.HandlerFunc((&LiveHandler{}).StreamChannel)).ServeHTTP(w, r)
				if w.Code != 200 || w.Body.String() != "SYNTHETIC-OWNED-LIVE" || p.calls.Load() != 0 {
					t.Fatal("actual bounded live HTTP/body path failed", w.Code, w.Body.String())
				}
				return
			case "live-hls-worker":
				handler = f.video.StartLiveHLSSession
				path = "/api/live/hls/start?profileId=" + f.profileID
			case "subtitle-stored-profile":
				handler = f.video.ServeExtractedSubtitles
				path = "/api/video/subtitles/synthetic-session/subtitles.vtt?profileId=" + f.profileID
			case "thumbnail-worker":
				handler = f.video.StartThumbnails
				path = "/api/video/thumbnails/start?path=" + url.QueryEscape(f.resource) + "&profileId=" + f.profileID
			case "thumbnail-stored-resource":
				handler = f.video.ServeThumbnailImage
				path = "/api/video/thumbnails/synthetic-key/0.jpg?profileId=" + f.profileID
			case "credits-source":
				handler = f.video.DetectCredits
				path = "/api/video/credits/detect?path=" + url.QueryEscape(f.resource) + "&duration=30&profileId=" + f.profileID
			case "retired-source-no-cached-body":
				f.video.setCachedMetadata(f.resource, &videoMetadataResponse{Path: f.resource, DurationSeconds: 123})
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profileID); e != nil {
					t.Fatal(e)
				}
				r := httptest.NewRequest("GET", "http://synthetic.test/api/video/metadata?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID, nil)
				checked, release, e := f.pins.Authenticate(r, f.token, "")
				if e == nil {
					release()
					_ = checked
					t.Fatal("retired share authenticated cached source")
				}
				if p.calls.Load() != 0 {
					t.Fatal("retired cache caused source work")
				}
				return
			}
			w := shareInvokeHTTP(t, f, path, handler)
			expected := http.StatusForbidden
			switch kind {
			case "live-hls-worker":
				expected = http.StatusNotImplemented
			case "subtitle-extract-worker":
				expected = http.StatusBadRequest
			case "thumbnail-worker":
				expected = http.StatusServiceUnavailable
			case "credits-source":
				if w.Code != http.StatusAccepted {
					t.Fatal("actual owned credits admission", w.Code, w.Body.String())
				}
				registry := f.video.ancillaryRegistry()
				registry.mu.Lock()
				var job *shareAncillaryJob
				for _, candidate := range registry.jobs {
					if candidate.kind == "credits" {
						job = candidate
					}
				}
				registry.mu.Unlock()
				if job == nil {
					t.Fatal("actual credits producer was not retained")
				}
				defer job.close()
				select {
				case <-job.producerDone:
				case <-time.After(15 * time.Second):
					t.Fatal("actual credits producer did not join")
				}
				if job.guard.handler != f.video || job.profile != f.profileID || job.resource != f.resource || job.token != f.token || job.guard.owner.Check(job.ctx) != nil {
					t.Fatal("credits producer lost original source/recipient")
				}
				job.mu.Lock()
				actualErr, detected := job.err, job.detected
				job.mu.Unlock()
				if actualErr == nil || detected || p.calls.Load() != 1 || p.closed.Load() != 1 {
					t.Fatal("audio-only source fabricated credits or leaked actual probe body", actualErr, p.calls.Load(), p.closed.Load())
				}
				return
			}
			if w.Code != expected || p.calls.Load() != 0 {
				t.Fatal("invalid/unconfigured ancillary request opened a source/job", kind, w.Code, expected, p.calls.Load())
			}
		})
	}
}
