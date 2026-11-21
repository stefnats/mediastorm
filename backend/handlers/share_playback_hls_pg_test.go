//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/datastore"
	"novastream/services/streaming"
)

// This provider exposes actual retained bodies. A held Close distinguishes
// cancelled HTTP/FFmpeg contexts from the original reader's physical end.
type shareHLSTestBody struct {
	mu                                              sync.Mutex
	data                                            *bytes.Reader
	held                                            bool
	stop, closeEntered, allowClose, joined, reading chan struct{}
	reads                                           sync.WaitGroup
	once, readOnce, releaseOnce                     sync.Once
}

func newShareHLSTestBody(data []byte, held bool) *shareHLSTestBody {
	b := &shareHLSTestBody{data: bytes.NewReader(data), held: held, stop: make(chan struct{}), closeEntered: make(chan struct{}), allowClose: make(chan struct{}), joined: make(chan struct{}), reading: make(chan struct{})}
	if !held {
		b.release()
	}
	return b
}
func (b *shareHLSTestBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	select {
	case <-b.stop:
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	default:
	}
	b.reads.Add(1)
	n, e := b.data.Read(p)
	b.mu.Unlock()
	defer b.reads.Done()
	if e == io.EOF && b.held {
		b.readOnce.Do(func() { close(b.reading) })
		<-b.stop
		return 0, io.ErrClosedPipe
	}
	return n, e
}
func (b *shareHLSTestBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		close(b.stop)
		b.mu.Unlock()
		close(b.closeEntered)
		<-b.allowClose
		b.reads.Wait()
		close(b.joined)
	})
	return nil
}
func (b *shareHLSTestBody) release() {
	b.releaseOnce.Do(func() { close(b.allowClose) })
}

type shareHLSTestProvider struct {
	mu       sync.Mutex
	data     []byte
	holdMain bool
	released bool
	bodies   []*shareHLSTestBody
	contexts []context.Context
	direct   atomic.Int32
}

func (p *shareHLSTestProvider) Stream(ctx context.Context, r streaming.Request) (*streaming.Response, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	p.mu.Lock()
	b := newShareHLSTestBody(p.data, p.holdMain && len(p.bodies) == 1)
	if p.released {
		b.release()
	}
	p.bodies = append(p.bodies, b)
	p.contexts = append(p.contexts, ctx)
	p.mu.Unlock()
	return &streaming.Response{Status: http.StatusOK, Headers: make(http.Header), ContentLength: int64(len(p.data)), Body: b}, nil
}
func (p *shareHLSTestProvider) GetDirectURL(context.Context, string) (string, error) {
	p.direct.Add(1)
	return "", fmt.Errorf("direct URL must not be used by original shared pipe")
}
func (p *shareHLSTestProvider) state() ([]*shareHLSTestBody, []context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*shareHLSTestBody(nil), p.bodies...), append([]context.Context(nil), p.contexts...)
}
func (p *shareHLSTestProvider) release() {
	p.mu.Lock()
	p.released = true
	bodies := append([]*shareHLSTestBody(nil), p.bodies...)
	p.mu.Unlock()
	for _, b := range bodies {
		b.release()
	}
}
func shareHLSTestWait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(12 * time.Second):
		t.Fatalf("actual %s did not end", what)
	}
}
func shareHLSTestPending(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s ended before original reader", what)
	case <-time.After(30 * time.Millisecond):
	}
}
func shareHLSTestPoll(t *testing.T, what string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("actual %s was not observed", what)
}
func shareHLSTestRequest(f *sharePlaybackHTTPFixture, t *testing.T, session *HLSSession, method, suffix string, next http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(method, "http://synthetic.test/api/video/hls/"+session.ID+suffix, nil)
	vars := map[string]string{"sessionID": session.ID, "segment": "segment0.ts", "track": "2"}
	r = mux.SetURLVars(r, vars)
	f.serve(t, w, r, next)
	return w
}
func shareHLSTestInstallManager(t *testing.T, f *sharePlaybackHTTPFixture, p *shareHLSTestProvider) {
	t.Helper()
	manager := NewHLSManager(t.TempDir(), "/usr/bin/ffmpeg", "/usr/bin/ffprobe", p)
	f.video.hlsManager = manager
	// Retire the exact manager before its TempDir cleanup. The fixture's later
	// Shutdown sees nil, so it cannot close the cleanup channel a second time.
	t.Cleanup(func() { manager.Shutdown(); f.video.hlsManager = nil })
}

func shareHLSTestStart(t *testing.T, f *sharePlaybackHTTPFixture, p *shareHLSTestProvider) (*HLSSession, context.Context) {
	t.Helper()
	shareHLSTestInstallManager(t, f, p)
	if f.video.vodBoundary != nil {
		t.Fatal("fixture must exercise plain share HLS with no global VOD boundary")
	}
	// This cleanup is registered after the fixture's, so every held reader is
	// released before actual Shutdown/Close joins it, even when a witness fails.
	t.Cleanup(p.release)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "http://synthetic.test/api/video/hls/start?path="+url.QueryEscape(f.resource)+"&profileId="+url.QueryEscape(f.profileID)+"&target=web&audioTrack=1", nil)
	var original context.Context
	f.serve(t, w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { original = r.Context(); f.video.StartHLSSession(w, r) }))
	if w.Code != http.StatusOK {
		t.Fatalf("actual shared HLS creation: %d %s", w.Code, w.Body.String())
	}
	var reply struct {
		ID string `json:"sessionId"`
	}
	if json.Unmarshal(w.Body.Bytes(), &reply) != nil || reply.ID == "" {
		t.Fatal("actual HLS reply omitted session")
	}
	f.video.hlsManager.mu.RLock()
	s := f.video.hlsManager.sessions[reply.ID]
	f.video.hlsManager.mu.RUnlock()
	if s == nil || s.shareOwned == nil || s.shareOwned.guard.owner == nil {
		t.Fatal("published session has no original owned source")
	}
	shareHLSTestPoll(t, "real child start", func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.FFmpegCmd != nil && s.FFmpegCmd.Process != nil
	})
	if p.direct.Load() != 0 {
		t.Fatal("shared HLS escaped through a direct URL")
	}
	return s, original
}
func shareHLSTestCommand(s *HLSSession) *exec.Cmd {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.FFmpegCmd
}
func shareHLSTestRetired(t *testing.T, s *HLSSession, cmd *exec.Cmd, p *shareHLSTestProvider) {
	t.Helper()
	shareHLSTestWait(t, s.shareOwned.done, "original source cleanup")
	if cmd == nil || cmd.ProcessState == nil || cmd.ProcessState.Pid() != cmd.Process.Pid {
		t.Fatal("original FFmpeg child was not actually Waited")
	}
	bodies, _ := p.state()
	for _, b := range bodies {
		shareHLSTestWait(t, b.joined, "actual provider body join")
	}
	if _, err := os.Stat(s.OutputDir); !os.IsNotExist(err) {
		t.Fatalf("original output still present after cleanup: %v", err)
	}
}
func shareHLSTestSubtitleMedia(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	subtitle := filepath.Join(dir, "synthetic.srt")
	out := filepath.Join(dir, "synthetic.mkv")
	if err := os.WriteFile(subtitle, []byte("1\n00:00:00,000 --> 00:00:01,500\nSynthetic owned subtitle\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-i", subtitle, "-t", "2", "-map", "0:v", "-map", "1:a", "-map", "2:s", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "1", "-g", "10", "-c:a", "aac", "-c:s", "srt", out}
	if output, err := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("synthetic subtitle media: %v %s", err, output)
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSharePlaybackActualHLSOwnership(t *testing.T) {
	media := managedHLSMedia(t)
	for _, name := range []string{"initial-worker", "request-finished-owner-lives", "fresh-segment-request", "cross-binding-segment-denied", "current-source-revokes-worker", "current-household-revokes-worker", "native-policy-retires-worker", "seek-original-owner", "restart-original-owner", "audio-extract-original-owner", "old-child-joined-before-renewal", "stop-joins-exact-child", "runtime-close-joins-worker", "no-background-context-escape"} {
		t.Run(name, func(t *testing.T) {
			if name == "no-background-context-escape" {
				shareHLSTestNestedCanary(t, media)
				return
			}
			held := name == "current-source-revokes-worker" || name == "current-household-revokes-worker" || name == "native-policy-retires-worker" || name == "old-child-joined-before-renewal" || name == "stop-joins-exact-child" || name == "runtime-close-joins-worker"
			payload := media
			if name == "audio-extract-original-owner" {
				payload = shareHLSTestSubtitleMedia(t)
			}
			p := &shareHLSTestProvider{data: payload, holdMain: held}
			f := newSharePlaybackHTTPFixture(t, "/synthetic-shared.ts", p)
			s, original := shareHLSTestStart(t, f, p)
			owner := s.shareOwned
			cmd := shareHLSTestCommand(s)
			expiry, ok := owner.guard.ctx.Deadline()
			if !ok || !expiry.Equal(owner.guard.owner.ExpiresAt()) {
				t.Fatal("source lost original fixed deadline")
			}
			bodies, contexts := p.state()
			if len(bodies) < 2 {
				t.Fatal("actual probe and transcode did not retain independent bodies")
			}
			if held {
				shareHLSTestWait(t, bodies[1].reading, "actual held main reader")
			}
			switch name {
			case "initial-worker":
				if sharedHLSContext(contexts[1]) != owner || cmd.Stdin == nil {
					t.Fatal("child input was not the actual owned pipe")
				}
				if owner.guard.ctx.Err() != nil {
					t.Fatal("active source retired with request")
				}
				args := strings.Join(cmd.Args, " ")
				if !strings.Contains(args, "-protocol_whitelist pipe") || !strings.Contains(args, "-i pipe:0") {
					t.Fatal("actual child input is not confined to retained pipe")
				}
			case "request-finished-owner-lives":
				if original == nil || original.Err() == nil {
					t.Fatal("test did not actually finish the original HTTP lifetime")
				}
				if owner.guard.ctx.Err() != nil || owner.guard.owner.Check(owner.guard.ctx) != nil {
					t.Fatal("original source did not survive request finish")
				}
				w := shareHLSTestRequest(f, t, s, "GET", "/status", f.video.GetHLSSessionStatus)
				if w.Code != http.StatusOK {
					t.Fatalf("fresh status after original request: %d %s", w.Code, w.Body.String())
				}
			case "fresh-segment-request":
				var segment string
				shareHLSTestPoll(t, "actual generated segment", func() bool {
					files, _ := filepath.Glob(filepath.Join(s.OutputDir, "*.ts"))
					for _, file := range files {
						if info, e := os.Stat(file); e == nil && info.Size() > 0 {
							segment = filepath.Base(file)
							return true
						}
					}
					return false
				})
				w := httptest.NewRecorder()
				r := mux.SetURLVars(httptest.NewRequest("GET", "http://synthetic.test/api/video/hls/"+s.ID+"/"+segment, nil), map[string]string{"sessionID": s.ID, "segment": segment})
				f.serve(t, w, r, http.HandlerFunc(f.video.ServeHLSSegment))
				if w.Code != http.StatusOK || w.Body.Len() == 0 {
					t.Fatalf("real fresh segment request: %d %s", w.Code, w.Body.String())
				}
			case "cross-binding-segment-denied":
				second, e := f.runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+f.linkToken, nil), f.linkToken)
				if e != nil {
					t.Fatal(e)
				}
				old := f.token
				f.token = second.Session.Token
				w := shareHLSTestRequest(f, t, s, "GET", "/segment0.ts", f.video.ServeHLSSegment)
				f.token = old
				if w.Code == http.StatusOK {
					t.Fatal("a different actual recipient binding borrowed old source")
				}
				after, _ := p.state()
				if len(after) != len(bodies) || owner.guard.ctx.Err() != nil {
					t.Fatal("denied recipient changed the original source")
				}
			case "current-source-revokes-worker", "current-household-revokes-worker", "native-policy-retires-worker":
				switch name {
				case "current-source-revokes-worker":
					if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profileID); e != nil {
						t.Fatal(e)
					}
				case "current-household-revokes-worker":
					if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id=$1`, f.profileID); e != nil {
						t.Fatal(e)
					}
				default:
					settings, e := f.manager.Load()
					if e != nil {
						t.Fatal(e)
					}
					if e = f.manager.Save(settings); e != nil {
						t.Fatal(e)
					}
				}
				shareHLSTestWait(t, bodies[1].closeEntered, "actual source cancellation")
				shareHLSTestPending(t, owner.done, "original source cleanup")
				if _, e := os.Stat(s.OutputDir); e != nil {
					t.Fatal("output was removed before actual provider end")
				}
				p.release()
				shareHLSTestRetired(t, s, cmd, p)
			case "seek-original-owner", "restart-original-owner", "old-child-joined-before-renewal":
				beforeExpiry := expiry
				finished := make(chan struct{})
				var response *httptest.ResponseRecorder
				go func() {
					defer close(finished)
					if name == "restart-original-owner" {
						// The real cast recovery function is invoked inside a genuine fresh
						// continuation HTTP reference, exactly as segment delivery invokes it.
						response = shareHLSTestRequest(f, t, s, "GET", "/status", func(w http.ResponseWriter, r *http.Request) {
							out, checked, finish, allowed := f.video.beginHLSVOD(w, r, s.ID, false)
							if !allowed {
								return
							}
							defer finish()
							if !f.video.requireHLSSessionAccess(out, checked, s.ID) {
								return
							}
							f.video.hlsManager.restartCastTranscodingAt(s, castSegmentRestart{TranscodingOffset: 0.25}, checked)
						})
					} else {
						response = shareHLSTestRequest(f, t, s, "POST", "/seek?time=0.25", f.video.SeekHLSSession)
					}
				}()
				t.Cleanup(func() { p.release(); shareHLSTestWait(t, finished, "original mutation handler") })
				if held {
					shareHLSTestWait(t, bodies[1].closeEntered, "old reader retirement before seek")
					shareHLSTestPending(t, finished, "seek handler")
					current, _ := p.state()
					if len(current) != len(bodies) {
						t.Fatal("new provider opened before old body physically ended")
					}
					if _, e := os.Stat(s.OutputDir); e != nil {
						t.Fatal("seek deleted whole owned output")
					}
					p.release()
				}
				shareHLSTestWait(t, finished, "actual replacement worker creation")
				if response == nil || response.Code != http.StatusOK {
					t.Fatalf("actual same-owner mutation: %v", response)
				}
				shareHLSTestPoll(t, "new actual provider body", func() bool { all, _ := p.state(); return len(all) > len(bodies) })
				if cmd.ProcessState == nil {
					t.Fatal("old child was not Waited before new source")
				}
				owner.mu.Lock()
				next := owner.run
				owner.mu.Unlock()
				if sharedHLSContext(next) != owner || owner.guard.ctx.Err() != nil {
					t.Fatal("mutation lost the exact original source")
				}
				afterExpiry, _ := next.Deadline()
				if !afterExpiry.Equal(beforeExpiry) {
					t.Fatal("mutation renewed original expiry")
				}
				_, afterContexts := p.state()
				for _, current := range afterContexts[len(contexts):] {
					if sharedHLSContext(current) != owner {
						t.Fatal("replacement source escaped original owner")
					}
				}
			case "audio-extract-original-owner":
				s.mu.RLock()
				selectedAudio := s.AudioTrackIndex
				s.mu.RUnlock()
				if selectedAudio != 1 || !strings.Contains(strings.Join(cmd.Args, " "), "0:1") {
					t.Fatal("actual main child lost the explicitly selected audio stream")
				}
				// Use the actual on-demand alternate text track pipeline, alongside the
				// real audio-bearing main media, rather than a fake extraction callback.
				shareHLSTestPoll(t, "actual subtitle inventory", func() bool {
					s.mu.RLock()
					defer s.mu.RUnlock()
					return s.ProbeData != nil && len(s.ProbeData.SubtitleStreams) > 0
				})
				s.mu.RLock()
				track := s.ProbeData.SubtitleStreams[0].Index
				s.mu.RUnlock()
				shareHLSTestPoll(t, "main child completion before sidecar extraction", func() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.Completed })
				sidecar := filepath.Join(s.OutputDir, fmt.Sprintf("subtitles_%d.vtt", track))
				if e := os.Remove(sidecar); e != nil && !os.IsNotExist(e) {
					t.Fatal(e)
				}
				w := httptest.NewRecorder()
				r := mux.SetURLVars(httptest.NewRequest("GET", "http://synthetic.test/api/video/hls/"+s.ID+"/subtitles/"+fmt.Sprint(track)+"?wait=1", nil), map[string]string{"sessionID": s.ID, "track": fmt.Sprint(track)})
				f.serve(t, w, r, http.HandlerFunc(f.video.ServeHLSSubtitleTrack))
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Synthetic owned subtitle") {
					t.Fatalf("actual owned extraction: %d %s", w.Code, w.Body.String())
				}
				after, afterContexts := p.state()
				if len(after) <= len(bodies) {
					t.Fatal("on-demand extraction did not open a real provider body")
				}
				for i := len(bodies); i < len(after); i++ {
					if sharedHLSContext(afterContexts[i]) != owner {
						t.Fatal("extraction escaped original context")
					}
					shareHLSTestWait(t, after[i].joined, "actual extraction reader join")
				}
			case "stop-joins-exact-child", "runtime-close-joins-worker":
				finished := make(chan struct{})
				var response *httptest.ResponseRecorder
				go func() {
					defer close(finished)
					if name == "runtime-close-joins-worker" {
						f.runtime.Close()
					} else {
						response = shareHLSTestRequest(f, t, s, "POST", "/stop", f.video.StopHLSSession)
					}
				}()
				t.Cleanup(func() { p.release(); shareHLSTestWait(t, finished, "original stop or runtime close") })
				shareHLSTestWait(t, bodies[1].closeEntered, "actual source close signal")
				shareHLSTestPending(t, finished, "stop or runtime close")
				if _, e := os.Stat(s.OutputDir); e != nil {
					t.Fatal("output deleted before actual child/source join")
				}
				p.release()
				shareHLSTestWait(t, finished, "original stop or runtime close")
				if name == "stop-joins-exact-child" && (response == nil || response.Code != http.StatusOK) {
					t.Fatalf("noncounted original stop failed: %v", response)
				}
				shareHLSTestRetired(t, s, cmd, p)
			}
		})
	}
}

// Mint the changed synthetic resource through the actual original creator and
// public redemption. Updating fixture DATA alone never creates source permission.
func shareHLSTestBindHTTP(t *testing.T, f *sharePlaybackHTTPFixture, resource string) {
	t.Helper()
	creator, err := datastore.NewShareLinkCreator(t.Context(), f.store, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	original := httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil)
	framed, release, err := f.base.base.pin.pins.Authenticate(original, f.base.base.pin.session.Token, "synthetic-share-origin")
	if err != nil {
		t.Fatal(err)
	}
	link, err := creator.Create(framed, f.profileID, map[string]string{"profileId": f.profileID, "sourcePath": resource, "title": "Synthetic redirected media"}, time.Hour, 4, "")
	release()
	if err != nil {
		t.Fatal(err)
	}
	redeemed, err := f.runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
	if err != nil {
		t.Fatal(err)
	}
	f.resource = normalizeShareResource(resource)
	f.token = redeemed.Session.Token
	f.linkToken = link.Token
}
func shareHLSTestOpenHTTP(t *testing.T, f *sharePlaybackHTTPFixture) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "http://synthetic.test/api/video/hls/start?path="+url.QueryEscape(f.resource)+"&profileId="+url.QueryEscape(f.profileID)+"&target=web", nil)
	f.serve(t, w, r, http.HandlerFunc(f.video.StartHLSSession))
	return w
}
func shareHLSTestNestedCanary(t *testing.T, media []byte) {
	t.Helper()
	var network atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		network.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	canary := filepath.Join(t.TempDir(), "secondary.ts")
	if e := os.WriteFile(canary, []byte("synthetic secondary input"), 0600); e != nil {
		t.Fatal(e)
	}
	fd, e := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
	if e != nil {
		t.Fatal(e)
	}
	defer syscall.Close(fd)
	if _, e = syscall.InotifyAddWatch(fd, canary, syscall.IN_OPEN|syscall.IN_ACCESS); e != nil {
		t.Fatal(e)
	}
	// Exactly one genuine process-global security installation spans this child.
	// Each failed source is physically joined before changing synthetic DATA.
	p := &shareHLSTestProvider{}
	f := newSharePlaybackHTTPFixture(t, "/synthetic-nested.m3u8", p)
	shareHLSTestInstallManager(t, f, p)
	t.Cleanup(p.release)
	for index, target := range []string{server.URL + "/secondary.ts", "file://" + canary} {
		payload := []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\n" + target + "\n#EXT-X-ENDLIST\n")
		p.mu.Lock()
		p.data = payload
		p.mu.Unlock()
		w := shareHLSTestOpenHTTP(t, f)
		if w.Code == http.StatusOK {
			t.Fatal("unsupported nested playlist published a shared source")
		}
		f.video.hlsManager.mu.RLock()
		count := len(f.video.hlsManager.sessions)
		f.video.hlsManager.mu.RUnlock()
		if count != 0 {
			t.Fatal("failed nested probe retained a session")
		}
		bodies, _ := p.state()
		if len(bodies) != index+1 || p.direct.Load() != 0 {
			t.Fatal("nested source escaped actual confined probe")
		}
		shareHLSTestWait(t, bodies[index].joined, "failed original probe body")
	}
	if network.Load() != 0 {
		t.Fatal("nested playlist opened secondary network input")
	}
	var events [4096]byte
	n, e := syscall.Read(fd, events[:])
	if n > 0 || e != syscall.EAGAIN {
		t.Fatalf("nested playlist opened secondary local input: events=%d error=%v", n, e)
	}

	// A separate genuinely authorized raw HTTP resource uses the same installed
	// runtime and the actual Manager allowlist, never a substituted client grant.
	var targetCalls, originCalls, redirectChecks, mode atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(media)
	}))
	defer target.Close()
	mutationErr := make(chan error, 1)
	var origin *httptest.Server
	origin = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originCalls.Add(1)
		destination := target.URL + "/actual.ts"
		switch mode.Load() {
		case 1:
			destination = server.URL + "/forbidden-secondary.ts"
		case 2:
			destination = origin.URL + "/loop"
		case 3:
			_, err := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.profileID)
			if err != nil {
				select {
				case mutationErr <- err:
				default:
				}
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		http.Redirect(w, r, destination, http.StatusFound)
	}))
	defer origin.Close()
	settings, e := f.manager.Load()
	if e != nil {
		t.Fatal(e)
	}
	settings.Server.AllowedPrivateMediaOrigins = []string{origin.URL, target.URL}
	if e = f.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	f.video.SetConfigManager(f.manager)
	previous := f.video.externalProxyHTTPClient.CheckRedirect
	f.video.externalProxyHTTPClient.CheckRedirect = func(r *http.Request, via []*http.Request) error {
		redirectChecks.Add(1)
		if previous != nil {
			return previous(r, via)
		}
		return nil
	}
	shareHLSTestBindHTTP(t, f, origin.URL+"/original.ts")
	allowed := shareHLSTestOpenHTTP(t, f)
	if allowed.Code != http.StatusOK {
		t.Fatalf("actual permitted raw redirect failed: %d %s", allowed.Code, allowed.Body.String())
	}
	var reply struct {
		ID string `json:"sessionId"`
	}
	if json.Unmarshal(allowed.Body.Bytes(), &reply) != nil || reply.ID == "" {
		t.Fatal("actual redirected session missing")
	}
	f.video.hlsManager.mu.RLock()
	session := f.video.hlsManager.sessions[reply.ID]
	f.video.hlsManager.mu.RUnlock()
	if session == nil || session.shareOwned == nil {
		t.Fatal("redirected source lost original owner")
	}
	shareHLSTestPoll(t, "actual redirected transcode", func() bool { return shareHLSTestCommand(session) != nil && targetCalls.Load() >= 2 })
	if redirectChecks.Load() < 2 {
		t.Fatal("actual installed redirect callback was not chained for probe and transcode")
	}
	child := shareHLSTestCommand(session)
	stopped := shareHLSTestRequest(f, t, session, "POST", "/stop", f.video.StopHLSSession)
	if stopped.Code != http.StatusOK {
		t.Fatalf("actual redirected source stop: %d %s", stopped.Code, stopped.Body.String())
	}
	shareHLSTestWait(t, session.shareOwned.done, "redirected child/source cleanup")
	if child.ProcessState == nil {
		t.Fatal("redirected real child was not Waited")
	}

	before := targetCalls.Load()
	mode.Store(1)
	denied := shareHLSTestOpenHTTP(t, f)
	if denied.Code == http.StatusOK || network.Load() != 0 || targetCalls.Load() != before {
		t.Fatal("redirect escaped actual configured outbound policy")
	}
	mode.Store(2)
	first := originCalls.Load()
	loop := shareHLSTestOpenHTTP(t, f)
	hops := originCalls.Load() - first
	if loop.Code == http.StatusOK || hops < 2 || hops > 10 || targetCalls.Load() != before {
		t.Fatalf("actual original redirect limit failed: hops=%d status=%d", hops, loop.Code)
	}
	mode.Store(3)
	revoked := shareHLSTestOpenHTTP(t, f)
	select {
	case err := <-mutationErr:
		t.Fatal(err)
	default:
	}
	if revoked.Code == http.StatusOK || targetCalls.Load() != before {
		t.Fatal("retired original owner issued a fresh redirected hop")
	}
	f.video.hlsManager.mu.RLock()
	remaining := len(f.video.hlsManager.sessions)
	f.video.hlsManager.mu.RUnlock()
	if remaining != 0 {
		t.Fatal("failed redirect retained a source worker/session")
	}
}
