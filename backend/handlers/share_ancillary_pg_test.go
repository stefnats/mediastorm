//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
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
	"novastream/config"
	"novastream/internal/streamheaders"
)

func shareAncillaryTestFixture(t *testing.T, data []byte) (*sharePlaybackHTTPFixture, *shareHLSTestProvider) {
	t.Helper()
	p := &shareHLSTestProvider{data: data}
	f := newSharePlaybackHTTPFixture(t, "/synthetic-ancillary.mkv", p)
	settings, e := f.manager.Load()
	if e != nil {
		t.Fatal(e)
	}
	settings.Playback.Thumbnails.Enabled = true
	settings.Playback.Thumbnails.Workers = 2
	if e = f.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	f.video.SetConfigManager(f.manager)
	f.video.SetThumbnailCacheDir(t.TempDir())
	if f.video.subtitleExtractManager != nil {
		t.Cleanup(f.video.subtitleExtractManager.Shutdown)
	}
	t.Cleanup(f.video.shutdownShareAncillary)
	t.Cleanup(p.release)
	return f, p
}
func shareAncillaryTestStart(t *testing.T, f *sharePlaybackHTTPFixture, kind string) *shareAncillaryJob {
	t.Helper()
	endpoint := "/api/video/subtitles/start"
	params := "&subtitleTrack=0"
	handler := http.HandlerFunc(f.video.StartSubtitleExtract)
	switch kind {
	case "thumbnail":
		endpoint = "/api/video/thumbnails/start"
		params = ""
		handler = f.video.StartThumbnails
	case "credits":
		endpoint = "/api/video/credits/detect"
		params = ""
		handler = f.video.DetectCredits
	}
	w := shareInvokeHTTP(t, f, endpoint+"?path="+url.QueryEscape(f.resource)+"&profileId="+url.QueryEscape(f.profileID)+params, handler)
	if w.Code != 200 && w.Code != 202 {
		t.Fatalf("actual %s creation %d %s", kind, w.Code, w.Body.String())
	}
	registry := f.video.ancillaryRegistry()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	var newest *shareAncillaryJob
	for _, j := range registry.jobs {
		if j.kind == kind {
			if newest != nil {
				t.Fatal("ambiguous new original job")
			}
			newest = j
		}
	}
	if newest == nil {
		t.Fatal("real handler did not publish owned producer")
	}
	return newest
}
func shareAncillaryTestJoined(t *testing.T, j *shareAncillaryJob, p *shareHLSTestProvider) {
	t.Helper()
	shareHLSTestWait(t, j.producerDone, "actual ancillary producer")
	j.mu.Lock()
	commands := append([]*exec.Cmd(nil), j.commands...)
	j.mu.Unlock()
	for _, cmd := range commands {
		if cmd.Process == nil || cmd.ProcessState == nil || cmd.ProcessState.Pid() != cmd.Process.Pid {
			t.Fatal("actual child Wait did not complete")
		}
	}
	bodies, _ := p.state()
	for _, body := range bodies {
		shareHLSTestWait(t, body.joined, "original provider body")
	}
	if p.direct.Load() != 0 {
		t.Fatal("actual source escaped to direct/WebDAV URL")
	}
}
func shareAncillaryTestFile(t *testing.T, f *sharePlaybackHTTPFixture, j *shareAncillaryJob, name string) *httptest.ResponseRecorder {
	t.Helper()
	endpoint := "/api/video/subtitles/" + j.id + "/" + name
	handler := http.HandlerFunc(f.video.ServeExtractedSubtitles)
	vars := map[string]string{"sessionID": j.id, "file": name}
	if j.kind == "thumbnail" {
		endpoint = "/api/video/thumbnails/image/" + j.id + "/" + name
		handler = f.video.ServeThumbnailImage
		vars = map[string]string{"key": j.id, "file": name}
	}
	w := httptest.NewRecorder()
	r := mux.SetURLVars(httptest.NewRequest("GET", "http://synthetic.test"+endpoint, nil), vars)
	f.serve(t, w, r, handler)
	return w
}
func shareAncillaryTestRevoke(t *testing.T, f *sharePlaybackHTTPFixture, native bool) {
	t.Helper()
	if native {
		settings, e := f.manager.Load()
		if e != nil {
			t.Fatal(e)
		}
		if e = f.manager.Save(settings); e != nil {
			t.Fatal(e)
		}
	} else {
		if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id=$1`, f.profileID); e != nil {
			t.Fatal(e)
		}
	}
}
func shareAncillaryTestOtherToken(t *testing.T, f *sharePlaybackHTTPFixture) string {
	t.Helper()
	r, e := f.runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+f.linkToken, nil), f.linkToken)
	if e != nil {
		t.Fatal(e)
	}
	return r.Session.Token
}
func shareAncillaryTestCreditGlyph(t *testing.T) string {
	t.Helper()
	frame := image.NewGray(image.Rect(0, 0, 160, 90))
	glyphs := map[rune][]string{
		'C': {"01111", "11000", "11000", "11000", "11000", "11000", "01111"},
		'R': {"11110", "11011", "11011", "11110", "11100", "11010", "11001"},
		'E': {"11111", "11000", "11000", "11110", "11000", "11000", "11111"},
		'D': {"11110", "11011", "11011", "11011", "11011", "11011", "11110"},
		'I': {"11111", "00100", "00100", "00100", "00100", "00100", "11111"},
		'T': {"11111", "00100", "00100", "00100", "00100", "00100", "00100"},
		'S': {"01111", "11000", "11000", "01110", "00011", "00011", "11110"},
	}
	for index, letter := range "CREDITS" {
		for y, row := range glyphs[letter] {
			for x, pixel := range row {
				if pixel == '1' {
					for dy := 0; dy < 2; dy++ {
						for dx := 0; dx < 2; dx++ {
							frame.SetGray(38+index*12+x*2+dx, 36+y*2+dy, color.Gray{Y: 255})
						}
					}
				}
			}
		}
	}
	path := filepath.Join(t.TempDir(), "synthetic-credits-text.png")
	file, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	encodeErr := png.Encode(file, frame)
	closeErr := file.Close()
	if e = errors.Join(encodeErr, closeErr); e != nil {
		t.Fatal(e)
	}
	return path
}
func shareAncillaryTestCreditsMedia(t *testing.T, text bool) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-credits.mkv")
	args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=white:size=160x90:rate=2:duration=200"}
	if text {
		args = append(args, "-loop", "1", "-i", shareAncillaryTestCreditGlyph(t), "-filter_complex", "[0:v][1:v]overlay=0:0:enable='gte(t,170)':shortest=1[v]", "-map", "[v]")
	} else {
		args = append(args, "-vf", "drawbox=x=0:y=0:w=iw:h=ih:color=black:t=fill:enable='gte(t,170)'")
	}
	args = append(args, "-t", "200", "-r", "2", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "1", "-g", "4", path)
	if out, e := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).CombinedOutput(); e != nil {
		t.Fatalf("synthetic actual credits glyph movie %v %s", e, out)
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return data
}

func TestShareAncillaryOriginalSource(t *testing.T) {
	data := shareHLSTestSubtitleMedia(t)
	for _, name := range []string{"genuine-original-owner", "foreign-recipient-same-resource-denied", "stored-profile-and-source-required", "original-expiry-never-renewed", "request-finish-keeps-one-job", "native-policy-retires-actual-worker", "current-household-retires-actual-worker", "held-provider-close-before-refund", "held-http-file-before-output-removal", "actual-shutdown-joins-all-jobs", "same-resource-does-not-coalesce-recipients"} {
		t.Run(name, func(t *testing.T) {
			f, p := shareAncillaryTestFixture(t, data)
			if name == "stored-profile-and-source-required" {
				for _, query := range []string{"path=" + url.QueryEscape(f.resource), "path=foreign-resource&profileId=" + url.QueryEscape(f.profileID)} {
					w := shareInvokeHTTP(t, f, "/api/video/subtitles/start?subtitleTrack=0&"+query, f.video.StartSubtitleExtract)
					if w.Code == 200 || w.Code == 202 {
						t.Fatal("substituted/omitted binding accepted")
					}
				}
				bodies, _ := p.state()
				if len(bodies) != 0 {
					t.Fatal("invalid source opened provider")
				}
				return
			}
			if name == "native-policy-retires-actual-worker" || name == "current-household-retires-actual-worker" || name == "held-provider-close-before-refund" || name == "actual-shutdown-joins-all-jobs" {
				p.holdMain = true
			}
			j := shareAncillaryTestStart(t, f, "subtitle")
			switch name {
			case "genuine-original-owner":
				shareAncillaryTestJoined(t, j, p)
				if j.guard.owner.Check(j.ctx) != nil || j.guard.handler != f.video || j.resource != f.resource || j.profile != f.profileID {
					t.Fatal("actual original binding missing")
				}
				w := shareAncillaryTestFile(t, f, j, "subtitles.vtt")
				if w.Code != 200 || !strings.Contains(w.Body.String(), "Synthetic owned subtitle") {
					t.Fatal("actual subtitle not served", w.Code, w.Body.String())
				}
			case "foreign-recipient-same-resource-denied":
				shareAncillaryTestJoined(t, j, p)
				f.token = shareAncillaryTestOtherToken(t, f)
				w := shareAncillaryTestFile(t, f, j, "subtitles.vtt")
				if w.Code != 403 || strings.Contains(w.Body.String(), "Synthetic owned subtitle") {
					t.Fatal("foreign recipient adopted old output")
				}
			case "original-expiry-never-renewed":
				deadline, ok := j.ctx.Deadline()
				if !ok || deadline.After(j.guard.owner.ExpiresAt()) || time.Until(deadline) > 10*time.Minute {
					t.Fatal("actual original deadline renewed")
				}
				original := j.guard.owner.ExpiresAt()
				shareAncillaryTestJoined(t, j, p)
				for i := 0; i < 2; i++ {
					w := shareAncillaryTestFile(t, f, j, "subtitles.vtt")
					if w.Code != 200 {
						t.Fatal(w.Code)
					}
				}
				if !original.Equal(j.guard.owner.ExpiresAt()) {
					t.Fatal("poll renewed original owner")
				}
			case "request-finish-keeps-one-job":
				shareAncillaryTestJoined(t, j, p)
				if j.ctx.Err() != nil || j.guard.original != nil || !j.guard.transferred {
					t.Fatal("real request finish lost original async producer")
				}
				bodies, contexts := p.state()
				if len(bodies) != 2 || len(contexts) != 2 || shareMediaFrom(contexts[1]) != j.guard {
					t.Fatal("producer did not retain same concrete source")
				}
			case "native-policy-retires-actual-worker", "current-household-retires-actual-worker", "held-provider-close-before-refund", "actual-shutdown-joins-all-jobs":
				shareHLSTestPoll(t, "actual ancillary input", func() bool { b, _ := p.state(); return len(b) >= 2 })
				bodies, _ := p.state()
				shareHLSTestWait(t, bodies[1].reading, "actual held provider Read")
				done := make(chan struct{})
				started := false
				t.Cleanup(func() {
					p.release()
					if started {
						shareHLSTestWait(t, done, "actual shutdown/revocation")
					}
				})
				if name == "actual-shutdown-joins-all-jobs" {
					started = true
					go func() { defer close(done); f.video.Shutdown() }()
				} else if name == "held-provider-close-before-refund" {
					started = true
					go func() { defer close(done); j.close() }()
				} else {
					shareAncillaryTestRevoke(t, f, name == "native-policy-retires-actual-worker")
				}
				shareHLSTestWait(t, bodies[1].closeEntered, "actual held provider Close")
				shareHLSTestPending(t, j.done, "actual original output retirement")
				if _, e := os.Stat(j.dir); e != nil {
					t.Fatal("output removed before held source")
				}
				shareAncillaryBudget.Lock()
				retained := shareAncillaryBudget.accounts[j.account]
				shareAncillaryBudget.Unlock()
				if retained == 0 {
					t.Fatal("actual source charge refunded before Close join")
				}
				p.release()
				shareHLSTestWait(t, j.done, "actual source/output end")
				if started {
					shareHLSTestWait(t, done, "actual shutdown")
				}
				shareAncillaryTestJoined(t, j, p)
				if _, e := os.Stat(j.dir); !os.IsNotExist(e) {
					t.Fatal("joined output not removed")
				}
			case "held-http-file-before-output-removal":
				shareAncillaryTestHeldHTTP(t, f, j, p)
			case "same-resource-does-not-coalesce-recipients":
				shareAncillaryTestJoined(t, j, p)
				first := f.token
				f.token = shareAncillaryTestOtherToken(t, f)
				w := shareInvokeHTTP(t, f, "/api/video/subtitles/start?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID+"&subtitleTrack=0", f.video.StartSubtitleExtract)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				registry := f.video.ancillaryRegistry()
				registry.mu.Lock()
				var second *shareAncillaryJob
				for _, next := range registry.jobs {
					if next != j {
						second = next
					}
				}
				registry.mu.Unlock()
				if second == nil || second.id == j.id || second.token == first || second.guard == j.guard {
					t.Fatal("recipients coalesced actual producer")
				}
				shareAncillaryTestJoined(t, second, p)
			}
		})
	}
}

type shareAncillaryBlockingWriter struct {
	header           http.Header
	entered, release chan struct{}
	once             sync.Once
	deadlineMu       sync.Mutex
	deadline         time.Time
	status           atomic.Int32
	prefix           []byte
}

func (w *shareAncillaryBlockingWriter) Header() http.Header { return w.header }
func (w *shareAncillaryBlockingWriter) WriteHeader(status int) {
	w.status.CompareAndSwap(0, int32(status))
}
func (w *shareAncillaryBlockingWriter) Write(p []byte) (int, error) {
	w.status.CompareAndSwap(0, http.StatusOK)
	w.once.Do(func() {
		limit := len(p)
		if limit > 1024 {
			limit = 1024
		}
		w.prefix = append([]byte(nil), p[:limit]...)
		close(w.entered)
	})
	<-w.release
	return len(p), nil
}
func (w *shareAncillaryBlockingWriter) Flush() {}
func (w *shareAncillaryBlockingWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlineMu.Lock()
	w.deadline = deadline
	w.deadlineMu.Unlock()
	return nil
}
func (w *shareAncillaryBlockingWriter) SetReadDeadline(time.Time) error { return nil }
func shareAncillaryTestHeldHTTP(t *testing.T, f *sharePlaybackHTTPFixture, j *shareAncillaryJob, p *shareHLSTestProvider) {
	t.Helper()
	shareAncillaryTestJoined(t, j, p)
	j.mu.Lock()
	producerErr := j.err
	j.mu.Unlock()
	if producerErr != nil {
		t.Fatal("actual held-file producer failed", producerErr)
	}
	exactFile := filepath.Join(j.dir, "subtitles.vtt")
	content, e := os.ReadFile(exactFile)
	if e != nil || !bytes.Contains(content, []byte("Synthetic owned subtitle")) {
		t.Fatal("actual held subtitle file absent", e)
	}
	writer := &shareAncillaryBlockingWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(writer.release) }) }
	served, closed := make(chan struct{}), make(chan struct{})
	closeStarted, serveStarted := false, false
	t.Cleanup(func() {
		unblock()
		if serveStarted {
			shareHLSTestWait(t, served, "actual file handler failure cleanup")
		}
		if closeStarted {
			shareHLSTestWait(t, closed, "actual file retirement failure cleanup")
		}
	})
	r := mux.SetURLVars(httptest.NewRequest("GET", "http://synthetic.test/api/video/subtitles/"+j.id+"/subtitles.vtt", nil), map[string]string{"sessionID": j.id, "file": "subtitles.vtt"})
	incomingDeadline := time.Now().Add(20 * time.Second)
	incomingCtx, incomingCancel := context.WithDeadline(r.Context(), incomingDeadline)
	defer incomingCancel()
	r = r.WithContext(incomingCtx)
	checked, release, e := f.pins.Authenticate(r, f.token, "")
	if e != nil {
		t.Fatal(e)
	}
	serveStarted = true
	go func() {
		defer close(served)
		defer release()
		f.video.ManagedSharePlaybackEntry(http.HandlerFunc(f.video.ServeExtractedSubtitles)).ServeHTTP(writer, checked)
	}()
	shareHLSTestWait(t, writer.entered, "actual HTTP file write")
	if writer.status.Load() != http.StatusOK || !bytes.HasPrefix(writer.prefix, []byte("WEBVTT")) {
		t.Fatalf("held writer observed an error response instead of the actual subtitle: status=%d prefix=%q", writer.status.Load(), writer.prefix)
	}
	writer.deadlineMu.Lock()
	actualDeadline := writer.deadline
	writer.deadlineMu.Unlock()
	expectedDeadline, _ := j.ctx.Deadline()
	if j.guard.owner.ExpiresAt().Before(expectedDeadline) {
		expectedDeadline = j.guard.owner.ExpiresAt()
	}
	if incomingDeadline.Before(expectedDeadline) {
		expectedDeadline = incomingDeadline
	}
	if !actualDeadline.Equal(expectedDeadline) {
		t.Fatal("physical HTTP deadline did not retain earliest original bound", actualDeadline, expectedDeadline)
	}

	descriptors, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		t.Fatal(e)
	}
	heldExact := false
	for _, descriptor := range descriptors {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", descriptor.Name()))
		if e == nil && target == exactFile {
			heldExact = true
			break
		}
	}
	if !heldExact {
		t.Fatal("blocked writer did not retain actual original subtitle descriptor")
	}

	closeStarted = true
	go func() { defer close(closed); j.close() }()
	shareHLSTestPending(t, closed, "actual owned HTTP write")
	if _, e := os.Stat(j.dir); e != nil {
		t.Fatal("held file directory removed")
	}
	unblock()
	shareHLSTestWait(t, served, "actual HTTP file completion")
	shareHLSTestWait(t, closed, "actual file cleanup")
}

func shareAncillaryTestCropMedia(t *testing.T, bars, unknown bool) []byte {
	t.Helper()
	args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x96:rate=10:duration=3"}
	if bars {
		args = append(args, "-vf", "pad=160:128:0:16:black")
	}
	args = append(args, "-c:v", "libx264", "-preset", "ultrafast", "-threads", "1", "-f", "matroska")
	if unknown {
		args = append(args, "-live", "1")
	}
	args = append(args, "pipe:1")
	out, e := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).Output()
	if e != nil {
		t.Fatal(e)
	}
	if !unknown {
		// A real seekable mux writes its duration into the header; the pipeline
		// itself still receives only an owned nonseekable raw body.
		path := filepath.Join(t.TempDir(), "crop.mkv")
		args = args[:len(args)-1]
		args = append(args, path)
		if e = exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).Run(); e != nil {
			t.Fatal(e)
		}
		out, e = os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
	}
	return out
}

type shareAncillaryFailWriter struct {
	http.ResponseWriter
	writes atomic.Int32
}

func (w *shareAncillaryFailWriter) Write([]byte) (int, error) {
	w.writes.Add(1)
	return 0, errors.New("synthetic actual downstream failure")
}
func shareAncillaryTestProbeOutput(t *testing.T, data []byte) *ffprobeOutput {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "/usr/bin/ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", "-i", "pipe:0")
	cmd.Stdin = bytes.NewReader(data)
	raw, e := cmd.Output()
	if e != nil {
		t.Fatalf("actual output probe %v", e)
	}
	var meta ffprobeOutput
	if e = json.Unmarshal(raw, &meta); e != nil {
		t.Fatal(e)
	}
	return &meta
}
func shareAncillaryTestAudioMedia(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-original-audio.mkv")
	args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x96:rate=5", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-f", "lavfi", "-i", "sine=frequency=880:sample_rate=48000", "-t", "2", "-map", "0:v", "-map", "1:a", "-map", "2:a", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "1", "-c:a", "aac", "-metadata:s:a:0", "language=fra", "-metadata:s:a:1", "language=eng", path}
	if out, e := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).CombinedOutput(); e != nil {
		t.Fatalf("real multilingual audio %v %s", e, out)
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return data
}

// A failed original public canary remains fatal. This separate fresh genuine
// request records only bounded synthetic-process diagnostics for Root's RCA.
func shareAncillaryTestTransmuxDiagnostic(t *testing.T, f *sharePlaybackHTTPFixture, path string) {
	t.Helper()
	stage := "metadata"
	var diagnosticErr error
	var diagnosticStderr string
	var output bytes.Buffer
	diagnostic := shareInvokeHTTP(t, f, path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		meta, err := f.video.ancillaryMetadata(r.Context(), f.resource)
		if err != nil {
			diagnosticErr = err
			return
		}
		stage = "actual planned child"
		plan := f.video.buildTransmuxPlan(meta, "pipe:0", true, "")
		diagnosticStderr, diagnosticErr = f.video.runShareSourceCommand(r.Context(), f.resource, f.video.ffmpegPath, plan.args, &shareLimitedWriter{destination: &output, remaining: 16 << 20}, nil)
	}))
	if len(diagnosticStderr) > 4096 {
		diagnosticStderr = diagnosticStderr[:4096]
	}
	t.Logf("actual failed-public-canary diagnostic: stage=%s response=%d outputBytes=%d error=%v stderr=%q", stage, diagnostic.Code, output.Len(), diagnosticErr, diagnosticStderr)
}

func TestShareAncillarySynchronousPipelines(t *testing.T) {
	data := shareHLSTestSubtitleMedia(t)
	for _, name := range []string{"transmux-actual-output-probes", "transmux-original-selected-audio", "transmux-http-write-failure-joins", "transmux-cancel-joins-stdin-and-stderr", "transmux-no-direct-or-webdav-open", "transmux-confined-nested-payload", "crop-actual-letterbox-samples", "crop-actual-no-letterbox", "crop-current-metadata-not-cache", "crop-cancel-joins-both-samples", "crop-unknown-duration-unavailable", "crop-original-context-each-sample", "crop-no-new-source-after-retirement"} {
		t.Run(name, func(t *testing.T) {
			payload := data
			if name == "transmux-original-selected-audio" {
				payload = shareAncillaryTestAudioMedia(t)
			}
			crop := strings.HasPrefix(name, "crop-")
			if crop {
				payload = shareAncillaryTestCropMedia(t, name != "crop-actual-no-letterbox", name == "crop-unknown-duration-unavailable")
			}
			f, p := shareAncillaryTestFixture(t, payload)
			endpoint := "/api/video/stream"
			params := "&transmux=force&target=web"
			handler := http.HandlerFunc(f.video.StreamVideo)
			if crop {
				endpoint = "/api/video/cropdetect"
				params = ""
				handler = f.video.CropDetect
			}
			path := endpoint + "?path=" + url.QueryEscape(f.resource) + "&profileId=" + url.QueryEscape(f.profileID) + params
			switch name {
			case "transmux-confined-nested-payload":
				shareAncillaryTestNested(t, f, p, path, handler)
				return
			case "crop-current-metadata-not-cache":
				f.video.setCachedMetadata(f.resource, &videoMetadataResponse{DurationSeconds: 999999})
			case "transmux-http-write-failure-joins":
				w := &shareAncillaryFailWriter{ResponseWriter: httptest.NewRecorder()}
				f.serve(t, w, httptest.NewRequest("GET", "http://synthetic.test"+path, nil), handler)
				if w.writes.Load() == 0 {
					t.Fatal("actual mux never reached failed writer")
				}
				b, _ := p.state()
				for _, body := range b {
					shareHLSTestWait(t, body.joined, "failed-writer original body")
				}
				return
			case "transmux-cancel-joins-stdin-and-stderr", "crop-cancel-joins-both-samples", "crop-no-new-source-after-retirement":
				p.holdMain = true
				requestCtx, cancel := context.WithCancel(t.Context())
				defer cancel()
				r := httptest.NewRequest("GET", "http://synthetic.test"+path, nil).WithContext(requestCtx)
				checked, release, e := f.pins.Authenticate(r, f.token, "")
				if e != nil {
					t.Fatal(e)
				}
				done := make(chan struct{})
				t.Cleanup(func() { cancel(); p.release(); shareHLSTestWait(t, done, "actual cancelled sync handler") })
				go func() {
					defer close(done)
					defer release()
					f.video.ManagedSharePlaybackEntry(handler).ServeHTTP(httptest.NewRecorder(), checked)
				}()
				shareHLSTestPoll(t, "actual second body", func() bool { b, _ := p.state(); return len(b) >= 2 })
				b, _ := p.state()
				shareHLSTestWait(t, b[1].reading, "actual sync provider Read")
				if name == "crop-no-new-source-after-retirement" {
					shareAncillaryTestRevoke(t, f, false)
				} else {
					cancel()
				}
				shareHLSTestWait(t, b[1].closeEntered, "actual sync provider close")
				shareHLSTestPending(t, done, "original raw source join")
				p.release()
				shareHLSTestWait(t, done, "cancelled actual handler")
				b, _ = p.state()
				if crop && len(b) != 2 {
					t.Fatal("retired crop opened its second new source")
				}
				for _, body := range b {
					shareHLSTestWait(t, body.joined, "cancelled original input")
				}
				return
			}
			w := shareInvokeHTTP(t, f, path, handler)
			bodies, contexts := p.state()
			for _, body := range bodies {
				shareHLSTestWait(t, body.joined, "actual sync original body")
			}
			if p.direct.Load() != 0 {
				t.Fatal("shared sync pipeline used direct/WebDAV source")
			}
			if name == "crop-unknown-duration-unavailable" {
				if w.Code != 503 {
					t.Fatal("unknown duration fabricated success", w.Code, w.Body.String())
				}
				return
			}
			if w.Code != 200 {
				if name == "transmux-actual-output-probes" || name == "transmux-original-selected-audio" || name == "transmux-no-direct-or-webdav-open" {
					shareAncillaryTestTransmuxDiagnostic(t, f, path)
				}
				t.Fatalf("actual %s output %d %s", name, w.Code, w.Body.String())
			}
			if name == "transmux-no-direct-or-webdav-open" {
				ts := shareInvokeHTTP(t, f, "/api/video/stream?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID+"&dlnaProfile=avc-ts", f.video.StreamVideo)
				if ts.Code != 200 || ts.Header().Get("Content-Type") != "video/mpeg" {
					t.Fatal("actual owned TS unavailable", ts.Code, ts.Body.String())
				}
				measured := shareAncillaryTestProbeOutput(t, ts.Body.Bytes())
				video := selectPrimaryVideoStream(measured)
				if video == nil || video.CodecName != "h264" {
					t.Fatal("actual software TS video missing")
				}
				ac3 := false
				for _, stream := range measured.Streams {
					if stream.CodecType == "audio" && stream.CodecName == "ac3" {
						ac3 = true
					}
				}
				if !ac3 {
					t.Fatal("actual TS AC3 audio missing")
				}
				untouched := false
				f.video.dlnaCapsOnce.Do(func() { untouched = true })
				if !untouched || f.video.hlsManager != nil {
					t.Fatal("shared TS entered legacy lazy Background capability probes")
				}
				actualBodies, _ := p.state()
				if len(actualBodies) != 4 || p.direct.Load() != 0 {
					t.Fatal("TS bypassed retained original provider", len(actualBodies))
				}
				for _, body := range actualBodies {
					shareHLSTestWait(t, body.joined, "actual software TS input end")
				}

				shareAncillaryTestCompositeHTTP(t, f, p, payload)
			}
			if crop {
				var result cropDetectResponse
				if e := json.Unmarshal(w.Body.Bytes(), &result); e != nil {
					t.Fatal(e)
				}
				if name == "crop-actual-no-letterbox" {
					if result.LetterboxTop != 0 || result.LetterboxBottom != 0 {
						t.Fatal("plain image cropped", result)
					}
				} else if result.LetterboxTop <= .05 || result.LetterboxBottom <= .05 {
					t.Fatal("actual black bars not measured", result)
				}
				if len(bodies) != 3 {
					t.Fatal("crop did not own fresh metadata and two sample reads", len(bodies))
				}
				if name == "crop-original-context-each-sample" {
					g := shareMediaFrom(contexts[0])
					for _, ctx := range contexts {
						if shareMediaFrom(ctx) != g || g == nil {
							t.Fatal("sample substituted original owner")
						}
					}
				}
			} else {
				meta := shareAncillaryTestProbeOutput(t, w.Body.Bytes())
				if selectPrimaryVideoStream(meta) == nil {
					t.Fatal("actual mux omitted video")
				}
				if name == "transmux-actual-output-probes" {
					textSubtitles := 0
					for _, stream := range meta.Streams {
						if stream.CodecType == "subtitle" && stream.CodecName == "mov_text" {
							textSubtitles++
						}
					}
					if textSubtitles != 1 {
						t.Fatal("actual SRT input was not retained as one mov_text track", textSubtitles)
					}
				}
				if name == "transmux-original-selected-audio" {
					audio := 0
					languages := make(map[string]bool)
					for _, stream := range meta.Streams {
						if stream.CodecType == "audio" {
							audio++
							languages[stream.Tags["language"]] = true
							if stream.CodecName != "aac" {
								t.Fatal("actual source audio mapping changed", stream.CodecName)
							}
						}
					}
					if audio != 2 || !languages["fra"] || !languages["eng"] {
						t.Fatal("actual original multilingual audio lost", audio, languages)
					}
				}
				if len(bodies) != 2 {
					t.Fatal("mux did not own separate fresh probe and actual input", len(bodies))
				}
			}
		})
	}
}
func shareAncillaryTestNested(t *testing.T, f *sharePlaybackHTTPFixture, p *shareHLSTestProvider, path string, handler http.HandlerFunc) {
	t.Helper()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	canary := filepath.Join(t.TempDir(), "nested.ts")
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
	for _, target := range []string{server.URL + "/nested.ts", "file://" + canary} {
		p.mu.Lock()
		p.data = []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\n" + target + "\n#EXT-X-ENDLIST\n")
		p.mu.Unlock()
		w := shareInvokeHTTP(t, f, path, handler)
		if w.Code == 200 {
			t.Fatal("nested playlist admitted")
		}
		b, _ := p.state()
		for _, body := range b {
			shareHLSTestWait(t, body.joined, "nested original input")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("secondary network opened")
	}
	var events [4096]byte
	n, e := syscall.Read(fd, events[:])
	if n > 0 || e != syscall.EAGAIN {
		t.Fatal("secondary file opened", n, e)
	}
}

func shareAncillaryTestThumbnailMedia(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic-thumbnails.mkv")
	cmd := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x96:rate=2:duration=180", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "1", path)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("real thumbnail fixture %v %s", e, out)
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	return data
}

func TestShareAncillaryPublishedJobs(t *testing.T) {
	data := shareHLSTestSubtitleMedia(t)
	for _, name := range []string{"subtitle-real-vtt", "subtitle-real-ass", "subtitle-invalid-track-never-substitutes", "subtitle-stale-original-denies-cached-file", "thumbnail-real-jpeg-and-status", "thumbnail-random-key-no-global-cache-adoption", "thumbnail-worker-queue-cancel-joins", "thumbnail-file-cross-binding-denied", "credits-real-dark-text-transition", "credits-plain-black-not-credits", "credits-original-retirement-between-phases", "credits-no-python-or-network-grandchild", "job-cancel-before-publication", "finished-job-retains-output-owner", "output-bound-failure-joins-before-cleanup", "fresh-frame-correlates-old-owner-only"} {
		t.Run(name, func(t *testing.T) {
			payload := data
			if strings.HasPrefix(name, "thumbnail-") {
				payload = shareAncillaryTestThumbnailMedia(t)
			}
			if strings.HasPrefix(name, "credits-") {
				payload = shareAncillaryTestCreditsMedia(t, name != "credits-plain-black-not-credits")
			}
			if name == "output-bound-failure-joins-before-cleanup" {
				payload = shareAncillaryTestHugeSubtitles(t)
			}
			f, p := shareAncillaryTestFixture(t, payload)
			if name == "subtitle-invalid-track-never-substitutes" {
				w := shareInvokeHTTP(t, f, "/api/video/subtitles/start?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID+"&subtitleTrack=99", f.video.StartSubtitleExtract)
				if w.Code != 400 {
					t.Fatal("invalid actual track substituted", w.Code, w.Body.String())
				}
				r := f.video.ancillaryRegistry()
				r.mu.Lock()
				count := len(r.jobs)
				r.mu.Unlock()
				b, _ := p.state()
				if count != 0 || len(b) != 1 {
					t.Fatal("invalid track created extraction job", count, len(b))
				}
				shareHLSTestWait(t, b[0].joined, "invalid-track actual probe")
				return
			}
			if name == "job-cancel-before-publication" {
				ctx, cancel := context.WithCancel(t.Context())
				r := httptest.NewRequest("POST", "http://synthetic.test/api/video/thumbnails/start?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID, nil).WithContext(ctx)
				checked, release, e := f.pins.Authenticate(r, f.token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
				w := httptest.NewRecorder()
				f.video.ManagedSharePlaybackEntry(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { cancel(); f.video.StartThumbnails(w, r) })).ServeHTTP(w, checked)
				registry := f.video.ancillaryRegistry()
				registry.mu.Lock()
				count := len(registry.jobs)
				registry.mu.Unlock()
				b, _ := p.state()
				if count != 0 || len(b) != 0 || w.Code == 202 {
					t.Fatal("cancelled original published source", count, len(b), w.Code)
				}
				return
			}
			kind := "subtitle"
			if strings.HasPrefix(name, "thumbnail-") {
				kind = "thumbnail"
			}
			if strings.HasPrefix(name, "credits-") {
				kind = "credits"
			}
			if name == "thumbnail-worker-queue-cancel-joins" || name == "credits-original-retirement-between-phases" {
				p.holdMain = true
			}
			var j *shareAncillaryJob
			if name == "subtitle-real-ass" {
				w := shareInvokeHTTP(t, f, "/api/video/subtitles/start?path="+url.QueryEscape(f.resource)+"&profileId="+f.profileID+"&subtitleTrack=0&format=ass", f.video.StartSubtitleExtract)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				registry := f.video.ancillaryRegistry()
				registry.mu.Lock()
				for _, next := range registry.jobs {
					j = next
				}
				registry.mu.Unlock()
				if j == nil {
					t.Fatal("ASS job missing")
				}
			} else {
				j = shareAncillaryTestStart(t, f, kind)
			}
			switch name {
			case "subtitle-real-vtt", "subtitle-real-ass":
				shareAncillaryTestJoined(t, j, p)
				file := "subtitles.vtt"
				if name == "subtitle-real-ass" {
					file = "subtitles.ass"
				}
				w := shareAncillaryTestFile(t, f, j, file)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "Synthetic owned subtitle") {
					t.Fatal("real subtitle payload missing", w.Code, w.Body.String())
				}
				if file == "subtitles.vtt" && !strings.HasPrefix(w.Body.String(), "WEBVTT") {
					t.Fatal("actual VTT header missing")
				}
				if file == "subtitles.ass" && !strings.Contains(w.Body.String(), "[Script Info]") {
					t.Fatal("actual ASS header missing")
				}
			case "subtitle-stale-original-denies-cached-file", "fresh-frame-correlates-old-owner-only":
				shareAncillaryTestJoined(t, j, p)
				if name == "fresh-frame-correlates-old-owner-only" {
					if _, e := f.store.Pool().Exec(t.Context(), `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id=$1`, f.profileID); e != nil {
						t.Fatal(e)
					}
				} else {
					shareAncillaryTestRevoke(t, f, true)
				}
				w := shareAncillaryTestFile(t, f, j, "subtitles.vtt")
				if w.Code == 200 || strings.Contains(w.Body.String(), "Synthetic owned subtitle") {
					t.Fatal("fresh request renewed stale original output")
				}
			case "thumbnail-real-jpeg-and-status", "thumbnail-random-key-no-global-cache-adoption", "thumbnail-file-cross-binding-denied":
				shareAncillaryTestJoined(t, j, p)
				j.mu.Lock()
				thumbs := append([]thumbnailDetails(nil), j.thumbnails...)
				jobErr := j.err
				j.mu.Unlock()
				if jobErr != nil || len(thumbs) == 0 {
					t.Fatal("actual thumbnail generation", jobErr)
				}
				if name == "thumbnail-random-key-no-global-cache-adoption" {
					if j.id == thumbnailKey(f.resource) || len(j.id) != 24 || j.dir == filepath.Join(f.video.thumbnailManager.baseDir, thumbnailKey(f.resource)) {
						t.Fatal("shared job adopted legacy path cache")
					}
					if _, e := os.Stat(filepath.Join(f.video.thumbnailManager.baseDir, thumbnailKey(f.resource))); !os.IsNotExist(e) {
						t.Fatal("shared path populated global cache")
					}
				}
				if name == "thumbnail-file-cross-binding-denied" {
					f.token = shareAncillaryTestOtherToken(t, f)
				}
				w := shareAncillaryTestFile(t, f, j, thumbs[0].File)
				if name == "thumbnail-file-cross-binding-denied" {
					if w.Code != 403 {
						t.Fatal("cross-bound thumbnail served", w.Code)
					}
					return
				}
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				image, e := jpeg.Decode(bytes.NewReader(w.Body.Bytes()))
				if e != nil || image.Bounds().Dx() != 640 {
					t.Fatal("actual JPEG invalid", e)
				}
				status := shareInvokeHTTP(t, f, "/api/video/thumbnails/status?path="+url.QueryEscape(f.resource)+"&key="+j.id, f.video.GetThumbnailsStatus)
				var response thumbnailStatusResponse
				if e = json.Unmarshal(status.Body.Bytes(), &response); e != nil || status.Code != 200 || response.Status != "ready" || response.Generated != len(thumbs) || len(response.Thumbnails) == 0 {
					t.Fatal("actual thumbnail status", status.Code, e, status.Body.String())
				}
			case "thumbnail-worker-queue-cancel-joins", "credits-original-retirement-between-phases":
				shareHLSTestPoll(t, "actual queued source child", func() bool { b, _ := p.state(); return len(b) >= 2 })
				b, _ := p.state()
				if name == "thumbnail-worker-queue-cancel-joins" {
					// A bounded frame may finish before the copier asks for EOF.
					// Observe the real input Read, then retain its held Close.
					shareHLSTestPoll(t, "actual thumbnail input Read", func() bool {
						b[1].mu.Lock()
						defer b[1].mu.Unlock()
						return b[1].data.Len() < len(payload)
					})
				} else {
					shareHLSTestWait(t, b[1].reading, "actual held producer Read")
				}
				shareAncillaryTestRevoke(t, f, false)
				shareHLSTestWait(t, b[1].closeEntered, "held producer input close")
				shareHLSTestPending(t, j.done, "actual queued producer cleanup")
				p.release()
				shareHLSTestWait(t, j.done, "actual queue/phase retirement")
				shareAncillaryTestJoined(t, j, p)
				if name == "credits-original-retirement-between-phases" {
					b, _ = p.state()
					if len(b) != 2 {
						t.Fatal("retired coarse phase opened fine/edge source", len(b))
					}
				}
			case "credits-real-dark-text-transition", "credits-plain-black-not-credits", "credits-no-python-or-network-grandchild":
				shareAncillaryTestJoined(t, j, p)
				j.mu.Lock()
				detected, start, jobErr := j.detected, j.creditsStart, j.err
				commands := append([]*exec.Cmd(nil), j.commands...)
				j.mu.Unlock()
				if jobErr != nil {
					t.Fatal("real credits phases", jobErr)
				}
				if name == "credits-plain-black-not-credits" {
					if detected {
						t.Fatal("plain dark image became credits")
					}
				} else if !detected || start < 165 || start > 180 {
					t.Fatal("actual dark text transition missed", detected, start)
				}
				for _, cmd := range commands {
					args := strings.Join(cmd.Args, " ")
					if strings.Contains(args, "python") || strings.Contains(args, "--url") || !strings.Contains(args, "-protocol_whitelist pipe") || !strings.Contains(args, "-i pipe:0") {
						t.Fatal("actual credits child escaped owned raw input", args)
					}
				}
				if len(commands) < 2 {
					t.Fatal("actual coarse/fine work missing")
				}
				w := shareInvokeHTTP(t, f, "/api/video/credits/status?path="+url.QueryEscape(f.resource), f.video.GetCreditsStatus)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "complete") {
					t.Fatal("real credits status", w.Code, w.Body.String())
				}
			case "finished-job-retains-output-owner":
				shareAncillaryTestJoined(t, j, p)
				if j.ctx.Err() != nil || j.guard.lease == nil {
					t.Fatal("finished output prematurely refunded original owner")
				}
				if _, e := os.Stat(j.dir); e != nil {
					t.Fatal(e)
				}
				shareAncillaryBudget.Lock()
				retained := shareAncillaryBudget.accounts[j.account]
				bytes := j.bytes
				shareAncillaryBudget.Unlock()
				if retained == 0 || bytes == 0 {
					t.Fatal("actual output uncharged")
				}
				j.close()
				if _, e := os.Stat(j.dir); !os.IsNotExist(e) {
					t.Fatal("actual output survived completed close")
				}
			case "output-bound-failure-joins-before-cleanup":
				shareAncillaryTestJoined(t, j, p)
				j.mu.Lock()
				jobErr := j.err
				j.mu.Unlock()
				if jobErr == nil {
					t.Fatal("actual over-limit subtitle succeeded")
				}
				shareAncillaryBudget.Lock()
				charged := j.bytes
				shareAncillaryBudget.Unlock()
				if charged == 0 || charged > 16<<20 {
					t.Fatal("actual encoded output charge incorrect", charged)
				}
				j.close()
				if _, e := os.Stat(j.dir); !os.IsNotExist(e) {
					t.Fatal("failed output survived physical cleanup")
				}
			}
		})
	}
}
func shareAncillaryTestHugeSubtitles(t *testing.T) []byte {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "huge.srt")
	file, e := os.OpenFile(source, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	text := strings.Repeat("Synthetic bounded subtitle ", 80)
	for n := 0; n < 9000; n++ {
		start := n
		_, e = fmt.Fprintf(file, "%d\n%02d:%02d:%02d,000 --> %02d:%02d:%02d,900\n%s\n\n", n+1, start/3600, start/60%60, start%60, start/3600, start/60%60, start%60, text)
		if e != nil {
			_ = file.Close()
			t.Fatal(e)
		}
	}
	if e = file.Close(); e != nil {
		t.Fatal(e)
	}
	output := filepath.Join(dir, "huge.mkv")
	cmd := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", "-nostdin", "-v", "error", "-i", source, "-c:s", "srt", output)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("real bounded subtitle fixture %v %s", e, out)
	}
	data, e := os.ReadFile(output)
	if e != nil {
		t.Fatal(e)
	}
	return data
}

// Real configured wire headers are ordinary source compatibility DATA. The
// exact decorated original remains the only authorized source identity.
func shareAncillaryTestCompositeHTTP(t *testing.T, f *sharePlaybackHTTPFixture, p *shareHLSTestProvider, media []byte) {
	t.Helper()
	var calls, bad, metadataRanges, fullReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		username, password, hasBasic := r.BasicAuth()
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/dav/movie%20file.mkv" || r.URL.RawQuery != "title=Some%20name" || r.URL.Fragment != "" || r.Header.Get("Referer") != "https://synthetic.example/watch" || r.Header.Get("Accept-Language") != "fr-CA" || r.Header.Get("Accept") != "video/x-matroska" || !hasBasic || username != "synthetic-dav" || password != "synthetic-password" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Forwarded-For") != "" {
			bad.Add(1)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		switch r.Header.Get("Range") {
		case "bytes=0-16777215":
			metadataRanges.Add(1)
		case "":
			fullReads.Add(1)
		default:
			bad.Add(1)
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Type", "video/x-matroska")
		w.Header().Set("Content-Length", fmt.Sprint(len(media)))
		_, _ = w.Write(media)
	}))
	t.Cleanup(func() { f.video.externalProxyHTTPClient.CloseIdleConnections(); server.Close() })
	settings, e := f.manager.Load()
	if e != nil {
		t.Fatal(e)
	}
	settings.Server.AllowedPrivateMediaOrigins = []string{server.URL}
	settings.UsenetEngines = []config.UsenetEngineSettings{{Name: "Synthetic original HTTP source", Type: "nzbdav", Enabled: true, BaseURL: server.URL, WebDAVBaseURL: server.URL + "/dav", WebDAVUsername: "synthetic-dav", WebDAVPassword: "synthetic-password"}}
	if e = f.manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	original := streamheaders.Attach(server.URL+"/dav/movie file.mkv?title=Some name", map[string]string{"Referer": "https://synthetic.example/watch", "Accept-Language": "fr-CA", "Accept": "video/x-matroska", "Range": "bytes=999-999", "Authorization": "Bearer forbidden-fragment", "Cookie": "forbidden-fragment", "X-Forwarded-For": "203.0.113.99"})
	shareHLSTestBindHTTP(t, f, original)
	route := "/api/video/stream?path=" + url.QueryEscape(f.resource) + "&profileId=" + url.QueryEscape(f.profileID) + "&transmux=force&target=web"
	w := shareInvokeHTTP(t, f, route, f.video.StreamVideo)
	if w.Code != http.StatusOK || bad.Load() != 0 || calls.Load() != 2 || metadataRanges.Load() != 1 || fullReads.Load() != 1 {
		t.Fatal("actual original composite HTTP wire mismatch", w.Code, calls.Load(), bad.Load(), metadataRanges.Load(), fullReads.Load())
	}
	measured := shareAncillaryTestProbeOutput(t, w.Body.Bytes())
	if selectPrimaryVideoStream(measured) == nil {
		t.Fatal("actual HTTP owned mux output missing")
	}
	bodies, _ := p.state()
	if len(bodies) != 4 || p.direct.Load() != 0 {
		t.Fatal("actual HTTP source escaped to provider/direct URL")
	}
	// Wire normalization is not permission to substitute the clean resource.
	clean, _ := streamheaders.Extract(original)
	denied := shareInvokeHTTP(t, f, "/api/video/stream?path="+url.QueryEscape(clean)+"&profileId="+f.profileID+"&transmux=force", f.video.StreamVideo)
	if denied.Code == http.StatusOK || calls.Load() != 2 {
		t.Fatal("cleaned HTTP URL replaced original composite authority", denied.Code, calls.Load())
	}
}
