package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/streaming"
)

type vodInputProvider struct {
	open func(context.Context, streaming.Request) (*streaming.Response, error)
}

func (p *vodInputProvider) Stream(c context.Context, r streaming.Request) (*streaming.Response, error) {
	return p.open(c, r)
}

type vodBlockedInput struct {
	entered     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	ignoreClose bool
	once        sync.Once
	readOnce    sync.Once
	closes      atomic.Int32
	buffer      []byte
}

func newVODBlockedInput(ignore bool) *vodBlockedInput {
	return &vodBlockedInput{entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{}), ignoreClose: ignore}
}
func (b *vodBlockedInput) Read(data []byte) (int, error) {
	b.readOnce.Do(func() { close(b.entered) })
	if b.ignoreClose {
		<-b.release
	} else {
		select {
		case <-b.release:
		case <-b.closed:
			return 0, io.EOF
		}
	}
	n := copy(data, "SYNTHETIC-LATE-PRIVATE-BYTES")
	b.buffer = data[:n]
	return n, io.EOF
}
func (b *vodBlockedInput) Close() error {
	b.closes.Add(1)
	b.once.Do(func() { close(b.closed) })
	return nil
}

func TestVideoVODActualBlockedStreamStopsOnOriginalAuthority(t *testing.T) {
	for _, mode := range []string{"profile invalidation", "parent cancel", "original session expiry"} {
		t.Run(mode, func(t *testing.T) {
			f := newVODTestState(t, vodAccountActive, false)
			if mode == "original session expiry" {
				f.change(func(f *vodTestState) { f.session.ExpiresAt = time.Now().Add(200 * time.Millisecond) })
			}
			body := newVODBlockedInput(false)
			var calls atomic.Int32
			var openedContext context.Context
			p := &vodInputProvider{open: func(ctx context.Context, req streaming.Request) (*streaming.Response, error) {
				openedContext = ctx
				calls.Add(1)
				return &streaming.Response{Body: body, Headers: make(http.Header), Status: 200}, nil
			}}
			h := NewVideoHandlerWithProvider(false, "/missing-ffmpeg", "/missing-ffprobe", t.TempDir(), p)
			h.vodBoundary = f.boundary
			t.Cleanup(h.Shutdown)
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := f.request("GET", "/api/video/stream?profileId=profile&path=owned-input.bin&transmux=0", "")
			// Keep the original authenticated request values and add only cancellation.
			requestCtx, stop := context.WithCancel(r.Context())
			defer stop()
			parentStop := context.AfterFunc(parent, stop)
			defer parentStop()
			r = r.WithContext(requestCtx)
			w := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); h.StreamVideo(w, r) }()
			select {
			case <-body.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("actual provider body was not read")
			}
			switch mode {
			case "profile invalidation":
				f.boundary.invalidateProfile("profile")
			case "parent cancel":
				cancel()
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("owned body remained blocked after cancellation/expiry")
			}
			if w.Body.Len() != 0 || body.closes.Load() != 1 || calls.Load() != 1 || openedContext.Err() == nil {
				t.Fatalf("body=%q closes=%d opens=%d ctx=%v", w.Body.String(), body.closes.Load(), calls.Load(), openedContext.Err())
			}
			f.boundary.mu.Lock()
			remaining := len(f.boundary.active)
			f.boundary.mu.Unlock()
			if remaining != 0 {
				t.Fatal("HTTP completion retained raw owner")
			}
		})
	}
}

func TestVideoVODActualLateReadDiscardsBytesAfterProfileChange(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, false)
	body := newVODBlockedInput(true)
	p := &vodInputProvider{open: func(context.Context, streaming.Request) (*streaming.Response, error) {
		return &streaming.Response{Body: body, Headers: make(http.Header), Status: 200}, nil
	}}
	h := NewVideoHandlerWithProvider(false, "/missing-ffmpeg", "/missing-ffprobe", t.TempDir(), p)
	h.vodBoundary = f.boundary
	t.Cleanup(h.Shutdown)
	r := f.request("GET", "/api/video/stream?profileId=profile&path=owned-input.bin&transmux=0", "")
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { defer close(done); h.StreamVideo(w, r) }()
	select {
	case <-body.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual body not entered")
	}
	f.change(func(f *vodTestState) { p := f.profiles["profile"]; p.PinHash = "changed-pin"; f.profiles[p.ID] = p })
	close(body.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("late body did not stop")
	}
	if w.Body.Len() != 0 || body.closes.Load() != 1 {
		t.Fatalf("late data=%q closes=%d", w.Body.String(), body.closes.Load())
	}
	if !bytes.Equal(body.buffer, make([]byte, len(body.buffer))) {
		t.Fatal("late caller-visible input bytes were not cleared")
	}
}

func TestVideoVODRawLifetimeDoesNotRenewOrChangePreparationLifetime(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, false)
	h := &VideoHandler{vodBoundary: f.boundary}
	r := f.request("GET", "/?profileId=profile", "")
	_, checked, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), r, "owned-input.bin")
	if !ok {
		t.Fatal("valid adult denied")
	}
	defer finish()
	raw := checked.Context().Value(vodRawOwnerKey{}).(*vodRawOwner)
	if !raw.owner.expires.Equal(f.session.ExpiresAt) || time.Until(raw.owner.expires) < 30*time.Minute {
		t.Fatal("raw response inherited preparation15minute lifetime")
	}
	expiry := raw.owner.expires
	for i := 0; i < 3; i++ {
		if h.vodRawCheck(checked.Context()) != nil || raw.owner.expires != expiry {
			t.Fatal("read recheck renewed/changed original lifetime")
		}
	}
	prep, ok := f.boundary.requestOwner(httptest.NewRecorder(), r, vodDemand, "")
	if !ok {
		t.Fatal("preparation control denied")
	}
	defer prep.release()
	if time.Until(prep.expires) > 15*time.Minute || time.Until(prep.expires) < 14*time.Minute {
		t.Fatal("P1 preparation lifetime changed")
	}
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	_, short, closeShort, ok := h.beginVideoVOD(httptest.NewRecorder(), r.WithContext(ctx), "owned-input.bin")
	if !ok {
		t.Fatal("short original request denied")
	}
	defer closeShort()
	shortOwner := short.Context().Value(vodRawOwnerKey{}).(*vodRawOwner)
	if shortOwner.owner.expires != deadline {
		t.Fatal("raw lifetime exceeded original request deadline")
	}
	f.change(func(f *vodTestState) { f.session.ExpiresAt = f.session.ExpiresAt.Add(time.Hour) })
	if h.vodRawCheck(checked.Context()) == nil || raw.owner.expires != expiry {
		t.Fatal("session change refreshed a held request")
	}
}

func TestVideoVODInactiveCannotBecomeChildPermissionOnHeldRead(t *testing.T) {
	f := newVODTestState(t, vodExplicitInactive, true)
	h := &VideoHandler{vodBoundary: f.boundary}
	r := f.request("GET", "/?profileId=profile", "")
	_, checked, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), r, "owned-input.bin")
	if !ok {
		t.Fatal("explicit inactive legacy control denied")
	}
	defer finish()
	f.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "active-v2"}
	})
	if h.vodRawCheck(checked.Context()) == nil {
		t.Fatal("inactive held context became a child grant")
	}
}

func TestVideoVODFailedProviderOpenDisposesOnlyItsOwnedResponse(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil legacy", true: "configured"}[configured], func(t *testing.T) {
			f := newVODTestState(t, vodAccountActive, false)
			body := &rootRawVideoBody{Reader: strings.NewReader("synthetic-owned-bytes")}
			p := &vodInputProvider{open: func(context.Context, streaming.Request) (*streaming.Response, error) {
				return &streaming.Response{Body: body}, errors.New("synthetic failed open")
			}}
			h := &VideoHandler{streamer: p}
			ctx := t.Context()
			if configured {
				h.vodBoundary = f.boundary
				_, r, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), f.request("GET", "/?profileId=profile", ""), "owned-input.bin")
				if !ok {
					t.Fatal("owner denied")
				}
				defer finish()
				ctx = r.Context()
			}
			resp, err := h.vodOpenStream(ctx, streaming.Request{Path: "owned-input.bin", Method: "GET"})
			if err == nil {
				t.Fatal("failed provider open accepted")
			}
			if configured {
				if resp != nil || body.closed.Load() != 1 {
					t.Fatal("failed owned response retained")
				}
			} else {
				if resp == nil || body.closed.Load() != 0 {
					t.Fatal("nil legacy contract changed")
				}
				resp.Close()
			}
		})
	}
}

func TestVideoVODRawContextCannotOpenAnotherSourceOrHandler(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, false)
	var calls atomic.Int32
	p := &vodInputProvider{open: func(context.Context, streaming.Request) (*streaming.Response, error) {
		calls.Add(1)
		return nil, errors.New("must not open")
	}}
	h := &VideoHandler{vodBoundary: f.boundary, streamer: p}
	_, r, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), f.request("GET", "/?profileId=profile", ""), "owned-input.bin")
	if !ok {
		t.Fatal("owner denied")
	}
	defer finish()
	other := &VideoHandler{vodBoundary: f.boundary, streamer: p}
	for _, test := range []struct {
		handler *VideoHandler
		ctx     context.Context
		path    string
	}{{h, r.Context(), "other-input.bin"}, {other, r.Context(), "owned-input.bin"}, {h, t.Context(), "owned-input.bin"}} {
		if _, err := test.handler.vodOpenStream(test.ctx, streaming.Request{Path: test.path, Method: "GET"}); err == nil {
			t.Fatal("unbound original context accepted")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("source/handler mismatch reached provider")
	}
}

func TestVideoVODFFprobeRejectsRetiredOwnerBeforeActualProcessStart(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, false)
	program, marker := writeArgsRecordingFFmpeg(t)
	h := &VideoHandler{vodBoundary: f.boundary, ffprobePath: program}
	_, r, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), f.request("GET", "/?profileId=profile", ""), "owned-input.bin")
	if !ok {
		t.Fatal("owner denied")
	}
	defer finish()
	f.boundary.invalidateProfile("profile")
	if _, err := h.runFFProbeWithHeaders(r.Context(), "owned-input.bin", nil, nil); err == nil {
		t.Fatal("retired original context started ffprobe")
	}
	if _, err := h.runFFProbeFileInner(r.Context(), "owned-input.bin", 1024, 1024); err == nil {
		t.Fatal("retired moov helper started ffprobe")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("ffprobe actually executed after original owner retired")
	}
}

func TestVideoVODKnownLibraryWithoutACLPortDeniesBeforeIO(t *testing.T) {
	for _, source := range []string{"localmedia:item/input.bin", "plexmedia:item/input.bin", "jellyfinmedia:item/input.bin"} {
		for _, action := range []string{"stream", "metadata", "direct"} {
			t.Run(source+"/"+action, func(t *testing.T) {
				h, p, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
				r := f.request("GET", "/?profileId=profile&transmux=0&path="+source, "")
				w := httptest.NewRecorder()
				rootRawVideoInvoke(h, action, w, r)
				if w.Code != 403 || p.streams.Load() != 0 || p.directs.Load() != 0 {
					t.Fatalf("unknown library access status=%d stream=%d direct=%d", w.Code, p.streams.Load(), p.directs.Load())
				}
			})
		}
	}
}

func TestVideoVODRechecksCurrentLibraryACLAfterActualOpen(t *testing.T) {
	for _, scheme := range []string{"localmedia:", "plexmedia:", "jellyfinmedia:"} {
		t.Run(scheme, func(t *testing.T) {
			h, p, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
			repo := &fakeLibraryAccessRepo{policies: map[string]models.LibraryAccessPolicy{"library": {LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"profile"}}}}
			access := libraryaccess.New(repo, fakeLibraryAccessLocalItems{item: &models.LocalMediaItem{ID: "item", LibraryID: "library"}}, fakeLibraryAccessRemoteItems{item: &models.RemoteMediaItem{ID: "item", LibraryID: "library"}})
			h.SetLibraryAccessService(access)
			h.SetUsersService(fakeLocalMediaUsersProvider{user: models.User{ID: "profile", AccountID: "account"}, userOK: true})
			p.after = func() {
				if err := access.Set(t.Context(), models.LibraryAccessPolicy{LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"other"}}); err != nil {
					t.Error(err)
				}
			}
			w := httptest.NewRecorder()
			h.StreamVideo(w, f.request("GET", "/?profileId=profile&transmux=0&path="+scheme+"item/input.bin", ""))
			if w.Code != 403 || p.streams.Load() != 1 || p.body == nil || p.body.closed.Load() != 1 || strings.Contains(w.Body.String(), "SYNTHETIC-FRESH") {
				t.Fatalf("revoked source emitted status=%d opens=%d body=%s", w.Code, p.streams.Load(), w.Body.String())
			}
		})
	}
}

type vodACLCallbackRepo struct {
	datastore.LibraryAccessRepository
	before func()
}

func (r *vodACLCallbackRepo) Get(context.Context, string) (*models.LibraryAccessPolicy, error) {
	r.before()
	return &models.LibraryAccessPolicy{LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"profile"}}, nil
}
func TestVideoVODACLCallbackCannotStampNewPINOntoOriginalOwner(t *testing.T) {
	h, p, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	repo := &vodACLCallbackRepo{before: func() {
		f.change(func(f *vodTestState) {
			profile := f.profiles["profile"]
			profile.PinHash = "changed-by-ACL-callback"
			f.profiles[profile.ID] = profile
		})
	}}
	h.SetLibraryAccessService(libraryaccess.New(repo, fakeLibraryAccessLocalItems{item: &models.LocalMediaItem{ID: "item", LibraryID: "library"}}, nil))
	h.SetUsersService(fakeLocalMediaUsersProvider{user: models.User{ID: "profile", AccountID: "account"}, userOK: true})
	source := "localmedia:item/input.bin"
	_, r, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), f.request("GET", "/?profileId=profile", ""), source)
	if !ok {
		t.Fatal("original owner denied")
	}
	defer finish()
	if _, err := h.vodOpenStream(r.Context(), streaming.Request{Path: source, Method: "GET"}); err == nil || p.streams.Load() != 0 {
		t.Fatal("ACL callback changed owner before unchecked provider open")
	}
}

func TestVideoVODRecordingOwnerChangeCannotRelabelAlreadyOpenedBody(t *testing.T) {
	for _, mode := range []string{"active explicit", "inactive omitted"} {
		t.Run(mode, func(t *testing.T) {
			h, p, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
			selector := "&profileId=profile"
			if mode == "inactive omitted" {
				selector = ""
				f.change(func(f *vodTestState) {
					f.states["account"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v1"}
				})
			}
			var owner atomic.Value
			owner.Store("profile")
			access := NewRecordingStreamAccess(recordingOwnerFunc(func(context.Context, string) (recordings.StreamOwner, error) {
				return recordings.StreamOwner{RecordingID: "recording-one", ProfileID: owner.Load().(string)}, nil
			}), recordingSessionFunc(func(context.Context, string) (*models.Session, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				s := f.session
				return &s, nil
			}), recordingAccessAccountMap{"account": {ID: "account"}}, recordingAccessProfileMap{"profile": {ID: "profile", AccountID: "account"}, "replacement": {ID: "replacement", AccountID: "account"}}, profileunlock.NewService(nil), func() (config.SecuritySettings, error) { return config.SecuritySettings{}, nil })
			h.SetRecordingStreamAccess(access)
			p.after = func() { owner.Store("replacement") }
			w := httptest.NewRecorder()
			h.StreamVideo(w, f.request("GET", "/?transmux=0&path=recording:recording-one/input.bin"+selector, ""))
			if w.Code != 403 || p.streams.Load() != 1 || p.body == nil || p.body.closed.Load() != 1 || strings.Contains(w.Body.String(), "SYNTHETIC-FRESH") {
				t.Fatal("opened recording body adopted a replacement stored owner")
			}
		})
	}

}

type vodCompanionProvider struct{ calls atomic.Int32 }

func (*vodCompanionProvider) Stream(context.Context, streaming.Request) (*streaming.Response, error) {
	return nil, streaming.ErrNotFound
}
func (p *vodCompanionProvider) StreamRelatedFile(context.Context, string, string) (*streaming.Response, error) {
	p.calls.Add(1)
	return nil, streaming.ErrNotFound
}
func TestVideoVODConfiguredProbeNeverOpensUnboundCLPICompanion(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil legacy control", true: "configured"}[configured], func(t *testing.T) {
			p := &vodCompanionProvider{}
			h := &VideoHandler{streamer: p}
			if configured {
				h.vodBoundary = newVODTestState(t, vodAccountActive, false).boundary
			}
			meta := &ffprobeOutput{Streams: []ffprobeStream{{CodecType: "audio", ID: "0x1100"}}}
			h.enrichBluRayStreamLanguages(t.Context(), "/debrid/torbox/1/file/2/Disc/BDMV/STREAM/00001.m2ts", meta)
			want := int32(1)
			if configured {
				want = 0
			}
			if p.calls.Load() != want {
				t.Fatalf("companion calls=%d want=%d", p.calls.Load(), want)
			}
			if normalizeTag(meta.Streams[0].Tags, "language") != "" {
				t.Fatal("missing language became a guessed claim")
			}
		})
	}
}

func TestVideoVODRealProbeUsesFreshCapturedInputAndKeepsSharedCacheUntouched(t *testing.T) {
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe unavailable for synthetic media verification")
	}
	// A complete half-second PCM WAV is generated in memory, with no provider.
	var wav bytes.Buffer
	wav.WriteString("RIFF")
	binary.Write(&wav, binary.LittleEndian, uint32(36+8000))
	wav.WriteString("WAVEfmt ")
	binary.Write(&wav, binary.LittleEndian, uint32(16))
	binary.Write(&wav, binary.LittleEndian, uint16(1))
	binary.Write(&wav, binary.LittleEndian, uint16(1))
	binary.Write(&wav, binary.LittleEndian, uint32(8000))
	binary.Write(&wav, binary.LittleEndian, uint32(16000))
	binary.Write(&wav, binary.LittleEndian, uint16(2))
	binary.Write(&wav, binary.LittleEndian, uint16(16))
	wav.WriteString("data")
	binary.Write(&wav, binary.LittleEndian, uint32(8000))
	wav.Write(make([]byte, 8000))
	var heads, gets atomic.Int32
	p := &vodInputProvider{open: func(_ context.Context, r streaming.Request) (*streaming.Response, error) {
		out := &streaming.Response{Headers: make(http.Header), Status: 200, ContentLength: int64(wav.Len())}
		if r.Method == "HEAD" {
			heads.Add(1)
		} else {
			gets.Add(1)
			out.Body = io.NopCloser(bytes.NewReader(wav.Bytes()))
		}
		return out, nil
	}}
	f := newVODTestState(t, vodAccountActive, false)
	h := NewVideoHandlerWithProvider(false, "/missing-ffmpeg", ffprobe, t.TempDir(), p)
	h.vodBoundary = f.boundary
	t.Cleanup(h.Shutdown)
	path := "owned-synthetic.wav"
	h.setCachedMetadata(path, &videoMetadataResponse{Path: "PRIVATE-OLD-CACHE", DurationSeconds: 42})
	w := httptest.NewRecorder()
	h.ProbeVideo(w, f.request("GET", "/api/video/metadata?profileId=profile&path="+path, ""))
	var out videoMetadataResponse
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != 200 || out.DurationSeconds == 42 || out.FormatName != "wav" || out.FileSizeBytes != int64(wav.Len()) || len(out.AudioStreams) != 1 {
		t.Fatalf("fresh probe status=%d error=%v body=%s", w.Code, err, w.Body.String())
	}
	if heads.Load() != 1 || gets.Load() != 1 {
		t.Fatalf("probe opens HEAD=%d GET=%d", heads.Load(), gets.Load())
	}
	if out.AudioStreams[0].CodecName != "pcm_s16le" || out.AudioStreams[0].Channels != 1 || out.AudioStreams[0].SampleRate != 8000 {
		t.Fatal("actual ffprobe facts do not describe generated PCM input")
	}
	if cached := h.getCachedMetadata(path); cached == nil || cached.Path != "PRIVATE-OLD-CACHE" || cached.DurationSeconds != 42 {
		t.Fatal("configured probe published/cleared unscoped cache")
	}
}
