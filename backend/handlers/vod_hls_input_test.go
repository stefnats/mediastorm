package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
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
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/services/streaming"
)

func managedHLSMedia(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "synthetic.ts")
	args := []string{"-nostdin", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "2", "-map", "0:v", "-map", "1:a", "-c:v", "libx264", "-preset", "ultrafast", "-threads", "1", "-g", "10", "-c:a", "aac", "-muxrate", "12000000", "-f", "mpegts", path}
	if output, err := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("synthetic media generation failed: %v %s", err, output)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type managedHLSGateBody struct {
	onPrefix              func()
	all                   []byte
	prefixRemaining       int
	data                  *bytes.Reader
	gate, closed, reading chan struct{}
	readOnce, closeOnce   sync.Once
	closes                atomic.Int32
}

func newManagedHLSGate(data []byte) *managedHLSGateBody {
	expanded := bytes.Repeat(data, (2<<20)/len(data)+1)
	return &managedHLSGateBody{all: expanded, prefixRemaining: 1 << 20, data: bytes.NewReader(expanded), gate: make(chan struct{}), closed: make(chan struct{}), reading: make(chan struct{})}
}
func (b *managedHLSGateBody) Read(p []byte) (int, error) {
	if b.prefixRemaining > 0 {
		if len(p) > b.prefixRemaining {
			p = p[:b.prefixRemaining]
		}
		n, err := b.data.Read(p)
		b.prefixRemaining -= n
		if b.prefixRemaining == 0 && b.onPrefix != nil {
			b.onPrefix()
		}
		return n, err
	}
	b.readOnce.Do(func() { close(b.reading) })
	select {
	case <-b.gate:
		return b.data.Read(p)
	case <-b.closed:
		return 0, io.EOF
	}
}
func (b *managedHLSGateBody) Close() error {
	b.closeOnce.Do(func() { b.closes.Add(1); close(b.closed) })
	return nil
}

type managedHLSProvider struct {
	data                      []byte
	main                      *managedHLSGateBody
	streams, direct, duration atomic.Int32
	mainCtx                   context.Context
}

func (p *managedHLSProvider) Stream(ctx context.Context, r streaming.Request) (*streaming.Response, error) {
	p.streams.Add(1)
	var body io.ReadCloser = io.NopCloser(bytes.NewReader(p.data))
	if r.RangeHeader == "" {
		p.mainCtx = ctx
		if p.main != nil {
			body = p.main
		}
	}
	length := len(p.data)
	if p.main != nil {
		length = len(p.main.all)
	}
	return &streaming.Response{Body: body, Status: http.StatusOK, ContentLength: int64(length), Headers: make(http.Header)}, nil
}
func (p *managedHLSProvider) GetDirectURL(context.Context, string) (string, error) {
	p.direct.Add(1)
	panic("configured input requested direct URL")
}
func (p *managedHLSProvider) GetDuration(context.Context, string) (float64, error) {
	p.duration.Add(1)
	panic("configured input requested legacy duration")
}
func managedHLSHandler(t *testing.T, p streaming.Provider) (*VideoHandler, *vodTestState) {
	t.Helper()
	h := NewVideoHandlerWithProvider(true, "/usr/bin/ffmpeg", "/usr/bin/ffprobe", t.TempDir(), p)
	state := newVODTestState(t, vodAccountActive, false)
	if err := h.installHLSInputBoundary(state.boundary); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.hlsManager.Shutdown)
	return h, state
}
func managedHLSRequest(state *vodTestState) *http.Request {
	return state.request(http.MethodGet, "/api/video/hls/start?"+url.Values{"profileId": {"profile"}, "path": {"debrid/synthetic/media.ts"}}.Encode(), "")
}
func TestHLSInputActualPipeSurvivesHTTPHandoffAndProducesPlayableTS(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	p := &managedHLSProvider{data: data, main: body}
	h, state := managedHLSHandler(t, p)
	req := managedHLSRequest(state)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	w := httptest.NewRecorder()
	h.StartHLSSession(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("managed start failed %d %s", w.Code, w.Body.String())
	}
	var response struct {
		ID  string `json:"sessionId"`
		URL string `json:"playlistUrl"`
	}
	if json.Unmarshal(w.Body.Bytes(), &response) != nil || response.ID == "" {
		t.Fatal("missing runtime session")
	}
	if q, err := url.Parse(response.URL); err != nil || q.Query().Get("profileId") != "profile" {
		t.Fatal("initial URL lacks captured owner")
	}
	select {
	case <-body.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("actual process never read opened input")
	}
	cancel()
	if p.mainCtx.Err() != nil {
		t.Fatal("HTTP end canceled handed-off runtime input")
	}
	select {
	case <-body.closed:
		t.Fatal("HTTP end closed handed-off body")
	case <-time.After(20 * time.Millisecond):
	}
	close(body.gate)
	snap, ok := h.hlsManager.snapshotHLSSession(response.ID)
	if !ok || snap.input == nil {
		t.Fatal("missing normal input publication")
	}
	select {
	case <-snap.input.done:
	case <-time.After(10 * time.Second):
		t.Fatal("actual FFmpeg attempt did not end")
	}
	playlist, err := os.ReadFile(filepath.Join(snap.output, "stream.m3u8"))
	if err != nil || !bytes.Contains(playlist, []byte("segment0.ts")) {
		t.Fatalf("missing actual output: %v", err)
	}
	output, err := exec.CommandContext(t.Context(), "/usr/bin/ffprobe", "-v", "error", "-show_entries", "stream=codec_name", "-of", "json", filepath.Join(snap.output, "segment0.ts")).CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("h264")) || !bytes.Contains(output, []byte("aac")) {
		t.Fatalf("output is not playable H264/AAC: %v %s", err, output)
	}
	// Read the actual normal output through the guarded HTTP route, including
	// a legal title/comment containing the letters URI but no URI attribute.
	decorated := strings.Replace(string(playlist), "#EXTM3U", "#EXTM3U\n# Curious music commentary", 1)
	decorated = strings.Replace(decorated, ",\nsegment", ",Curious music\nsegment", 1)
	if err := os.WriteFile(filepath.Join(snap.output, "stream.m3u8"), []byte(decorated), 0600); err != nil {
		t.Fatal(err)
	}
	readReq := state.request(http.MethodGet, "/api/video/hls/"+response.ID+"/stream.m3u8?profileId=profile", "")
	readReq = mux.SetURLVars(readReq, map[string]string{"sessionID": response.ID})
	read := httptest.NewRecorder()
	h.ServeHLSPlaylist(read, readReq)
	if read.Code != http.StatusOK || !strings.Contains(read.Body.String(), "Curious music") || !strings.Contains(read.Body.String(), "profileId=profile") {
		t.Fatalf("actual guarded playlist failed: %d %s", read.Code, read.Body.String())
	}
	var asset string
	for _, line := range strings.Split(read.Body.String(), "\n") {
		if strings.HasPrefix(line, "segment0.ts") {
			asset = line
			break
		}
	}
	target := "/api/video/hls/" + response.ID + "/" + asset
	receiver := func(target string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, target, nil)
		session, err := state.boundary.identity.CurrentSession(request.Context(), request.URL.Query().Get("token"))
		if err != nil {
			t.Fatal("returned URI token did not resolve current synthetic session")
		}
		ctx := context.WithValue(request.Context(), auth.ContextKeySession, session)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
		ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, session.IsMaster)
		request = request.WithContext(ctx)
		router := mux.NewRouter()
		router.Use(profileaccess.Middleware(func(profile, account string) bool {
			state.mu.Lock()
			defer state.mu.Unlock()
			return state.profiles[profile].AccountID == account
		}))
		router.HandleFunc("/api/video/hls/{sessionID}/{segment}", h.ServeHLSSegment)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		return recorder
	}
	parsed, err := url.Parse(target)
	if err != nil || asset == "" || parsed.Query().Get("profileId") != "profile" || parsed.Query().Get("token") != state.session.Token || parsed.Query().Get("profileUnlock") != state.token {
		t.Fatal("receiver URI did not carry exact captured owner/auth/PIN")
	}
	// No source/profile/PIN/auth headers are copied: the returned query alone
	// resolves the current synthetic session and the real current PIN guard.
	receiverResult := receiver(target)
	if receiverResult.Code != http.StatusOK || !bytes.Equal(receiverResult.Body.Bytes(), mustReadManagedHLS(t, filepath.Join(snap.output, "segment0.ts"))) {
		t.Fatalf("returned asset did not pass actual guarded receiver: %d", receiverResult.Code)
	}
	aliased := strings.Replace(target, "token=", "Token=", 1)
	aliased = strings.Replace(aliased, "profileId=", "PROFILEID=", 1)
	aliased = strings.Replace(aliased, "profileUnlock=", "PROFILEUNLOCK=", 1)
	uri, err := url.Parse(aliased)
	if err != nil {
		t.Fatal(err)
	}
	originalAliasQuery := uri.RawQuery
	authValues, err := h.ownedHLSAuth(readReq.Context(), snap.input.owner)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := rewriteOwnedHLSURI(uri.RequestURI(), response.ID, authValues)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reencoded, originalAliasQuery) {
		t.Fatal("alias query bytes lost")
	}
	aliasResult := receiver(reencoded)
	if aliasResult.Code != http.StatusOK || !bytes.Equal(aliasResult.Body.Bytes(), receiverResult.Body.Bytes()) {
		t.Fatalf("alias-only receiver lost canonical auth/profile/PIN: %d", aliasResult.Code)
	}
	denied := receiver(target + "&userId=foreign")
	if denied.Code == http.StatusOK {
		t.Fatal("conflicting receiver profile remained playable")
	}
	if p.streams.Load() != 1 || p.direct.Load() != 0 || p.duration.Load() != 0 || body.closes.Load() != 1 {
		t.Fatal("unexpected opens, legacy escape or body close count")
	}
}

type managedHLSCancelWriter struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *managedHLSCancelWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	w.cancel()
	return n, err
}
func TestHLSInputCancelledBeforeHandoffClosesActualInput(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	p := &managedHLSProvider{data: data, main: body}
	h, state := managedHLSHandler(t, p)
	req := managedHLSRequest(state)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	w := &managedHLSCancelWriter{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
	h.StartHLSSession(w, req)
	select {
	case <-body.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("pre-handoff cancellation did not close opened body")
	}
	if p.mainCtx == nil || p.mainCtx.Err() == nil || body.closes.Load() != 1 {
		t.Fatal("actual input context did not retire exactly once")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.hlsManager.mu.RLock()
		count := len(h.hlsManager.sessions)
		h.hlsManager.mu.RUnlock()
		if count == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("aborted setup left published runtime")
}
func TestHLSInputOriginalDeadlineAndInvalidateSurviveHandoff(t *testing.T) {
	for _, kind := range []string{"deadline", "trusted invalidation"} {
		t.Run(kind, func(t *testing.T) {
			data := managedHLSMedia(t)
			body := newManagedHLSGate(data)
			p := &managedHLSProvider{data: data, main: body}
			h, state := managedHLSHandler(t, p)
			req := managedHLSRequest(state)
			want := time.Now().Add(2 * time.Second)
			ctx, cancel := context.WithDeadline(req.Context(), want)
			defer cancel()
			req = req.WithContext(ctx)
			w := httptest.NewRecorder()
			h.StartHLSSession(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("start failed %d %s", w.Code, w.Body.String())
			}
			actual, ok := p.mainCtx.Deadline()
			if !ok || !actual.Equal(want) {
				t.Fatalf("authority deadline changed: got%v want%v", actual, want)
			}
			if kind == "trusted invalidation" {
				state.boundary.invalidateProfile("profile")
			}
			select {
			case <-body.closed:
			case <-time.After(5 * time.Second):
				t.Fatal("original expiry/invalidation did not close actual input")
			}
			select {
			case <-p.mainCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("original input context did not retire")
			}
			if body.closes.Load() != 1 {
				t.Fatal("input closed more than once")
			}
		})
	}
}
func TestHLSInputSecondAttemptCannotOpenProvider(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	p := &managedHLSProvider{data: data, main: body}
	h, state := managedHLSHandler(t, p)
	w := httptest.NewRecorder()
	h.StartHLSSession(w, managedHLSRequest(state))
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	var response struct {
		ID string `json:"sessionId"`
	}
	if json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatal("invalid response")
	}
	snapshot, _ := h.hlsManager.snapshotHLSSession(response.ID)
	if snapshot.input == nil {
		t.Fatal("missing actual input")
	}
	before := p.streams.Load()
	if snapshot.input.startPipe(0, 1) == nil || p.streams.Load() != before {
		t.Fatal("second attempt performed input work")
	}
	snapshot.input.retire()
}
func TestHLSInputTrackChoiceNeverSubstitutesUnsupportedSelection(t *testing.T) {
	var inventory vodHLSInventory
	raw := `{"streams":[{"index":0,"codec_type":"video","codec_name":"h264","pix_fmt":"yuv420p"},{"index":1,"codec_type":"audio","codec_name":"ac3","disposition":{"default":1}},{"index":2,"codec_type":"audio","codec_name":"aac"}]}`
	if json.Unmarshal([]byte(raw), &inventory) != nil {
		t.Fatal("fixture")
	}
	if _, _, err := chooseVODHLSTracks(inventory, 1); err == nil {
		t.Fatal("unsupported explicit audio substituted")
	}
	if _, _, err := chooseVODHLSTracks(inventory, -1); err == nil {
		t.Fatal("unsupported default audio substituted")
	}
	if v, a, err := chooseVODHLSTracks(inventory, 2); err != nil || v != 0 || a != 2 {
		t.Fatal("supported explicit track not retained")
	}
	inventory.Streams[1].Disposition = nil
	if _, _, err := chooseVODHLSTracks(inventory, -1); err == nil {
		t.Fatal("ambiguous default selected first eligible language")
	}
}

func runHLSCanaryCommand(t *testing.T, binary string, args []string, payload []byte) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Stdin = bytes.NewReader(payload)
	return cmd.CombinedOutput()
}
func TestHLSInputActualNestedHTTPReferencesCannotOpen(t *testing.T) {
	data := managedHLSMedia(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		w.Write(data)
	}))
	defer server.Close()
	manifest := []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\n" + server.URL + "/canary.ts\n#EXT-X-ENDLIST\n")
	control := []string{"-v", "error", "-protocol_whitelist", "pipe,http,tcp", "-allowed_extensions", "ALL", "-f", "hls", "-show_streams", "-i", "pipe:0"}
	output, err := runHLSCanaryCommand(t, "/usr/bin/ffprobe", control, manifest)
	if err != nil || calls.Load() == 0 || !bytes.Contains(output, []byte("h264")) {
		t.Fatalf("nested HTTP control did not actually open canary: %v %s", err, output)
	}
	calls.Store(0)
	for _, binary := range []string{"/usr/bin/ffprobe", "/usr/bin/ffmpeg"} {
		t.Run(filepath.Base(binary), func(t *testing.T) {
			args := append([]string{"-v", "error"}, vodHLSPipeOptions...)
			if filepath.Base(binary) == "ffprobe" {
				args = append(args, "-f", "hls", "-show_streams", "-i", "pipe:0")
			} else {
				args = append(args, "-f", "hls", "-i", "pipe:0", "-f", "null", "-")
			}
			output, err := runHLSCanaryCommand(t, binary, args, manifest)
			if err == nil || calls.Load() != 0 || !bytes.Contains(bytes.ToLower(output), []byte("whitelist")) {
				t.Fatalf("confined nested HTTP escaped: error=%v calls=%d output=%s", err, calls.Load(), output)
			}
		})
	}
}

// FIFO opening is observable: a writer cannot open until a nested reader has
// really opened this local path. Teardown uses its own reader only after the
// assertion and is explicitly excluded from the canary count.
func localHLSCanary(t *testing.T, data []byte) (string, <-chan struct{}, func()) {
	t.Helper()
	fifo := filepath.Join(t.TempDir(), "canary.ts")
	if output, err := exec.CommandContext(t.Context(), "/usr/bin/mkfifo", fifo).CombinedOutput(); err != nil {
		t.Fatalf("create local canary %v %s", err, output)
	}
	opened, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		f, err := os.OpenFile(fifo, os.O_WRONLY, 0600)
		close(opened)
		if err == nil {
			_, _ = f.Write(data)
			_ = f.Close()
		}
	}()
	cleanup := func() {
		select {
		case <-done:
			return
		default:
		}
		f, err := os.OpenFile(fifo, os.O_RDWR, 0600)
		if err == nil {
			select {
			case <-opened:
			case <-time.After(time.Second):
			}
			_ = f.Close()
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("local canary writer did not finish")
		}
	}
	return fifo, opened, cleanup
}
func TestHLSInputActualNestedLocalReferencesCannotOpen(t *testing.T) {
	data := managedHLSMedia(t)
	for _, kind := range []string{"control", "ffprobe", "ffmpeg"} {
		t.Run(kind, func(t *testing.T) {
			fifo, opened, cleanup := localHLSCanary(t, data)
			defer cleanup()
			manifest := []byte("ffconcat version 1.0\nfile 'file:" + fifo + "'\n")
			binary := "/usr/bin/ffprobe"
			args := []string{"-v", "error"}
			if kind == "control" {
				args = append(args, "-protocol_whitelist", "pipe,file", "-safe", "0")
			} else {
				args = append(args, vodHLSPipeOptions...)
			}
			if kind == "ffmpeg" {
				binary = "/usr/bin/ffmpeg"
				args = append(args, "-f", "concat", "-i", "pipe:0", "-f", "null", "-")
			} else {
				args = append(args, "-f", "concat", "-show_streams", "-i", "pipe:0")
			}
			output, err := runHLSCanaryCommand(t, binary, args, manifest)
			select {
			case <-opened:
				if kind != "control" {
					t.Fatal("confined process opened nested local canary")
				}
			default:
				if kind == "control" {
					t.Fatalf("control did not open local canary %v %s", err, output)
				}
			}
			if kind == "control" {
				if err != nil || !bytes.Contains(output, []byte("h264")) {
					t.Fatalf("local control not actual media read: %v %s", err, output)
				}
			} else if err == nil || !bytes.Contains(bytes.ToLower(output), []byte("whitelist")) {
				t.Fatalf("missing confined demuxer rejection %v %s", err, output)
			}
		})
	}
}
func TestHLSInputActualManagedProbeRejectsNestedPayload(t *testing.T) {
	payload := []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\nfile:/not-an-input/canary.ts\n#EXT-X-ENDLIST\n")
	p := &managedHLSProvider{data: payload}
	h, state := managedHLSHandler(t, p)
	w := httptest.NewRecorder()
	h.StartHLSSession(w, managedHLSRequest(state))
	if w.Code != http.StatusForbidden || p.streams.Load() != 1 || p.direct.Load() != 0 || p.duration.Load() != 0 {
		t.Fatalf("nested payload reached a second input/status: %d calls=%d", w.Code, p.streams.Load())
	}
	h.hlsManager.mu.RLock()
	count := len(h.hlsManager.sessions)
	h.hlsManager.mu.RUnlock()
	if count != 0 {
		t.Fatal("rejected payload published session")
	}
}

func mustReadManagedHLS(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func realFFmpegWrapper(t *testing.T, mode string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ffmpeg-wrapper")
	script := "#!/usr/bin/python3\nimport os,sys\nargs=sys.argv[1:]\n"
	switch mode {
	case "invalid":
		script += "import time\nsys.stdin.buffer.read(1<<20)\ntime.sleep(0.05)\nargs=['-synthetic-invalid-option']+args\n"
	case "hls":
		script += "args=['-f','hls','-allowed_extensions','ALL']+args\n"
	case "hls-unconfined-control":
		script += "clean=[]\ni=0\nwhile i<len(args):\n if args[i] in ('-protocol_whitelist','-format_whitelist'): i+=2\n else: clean.append(args[i]);i+=1\nargs=['-f','hls','-allowed_extensions','ALL','-protocol_whitelist','file,pipe,http,tcp']+clean\n"
	}
	script += "os.execv('/usr/bin/ffmpeg',['ffmpeg']+args)\n"
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return path
}
func TestHLSInputEarlyActualProcessExitClosesBlockedBody(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	p := &managedHLSProvider{data: data, main: body}
	h, state := managedHLSHandler(t, p)
	h.hlsManager.ffmpegPath = realFFmpegWrapper(t, "invalid")
	defer func() {
		select {
		case <-body.gate:
		default:
			close(body.gate)
		}
	}()
	w := httptest.NewRecorder()
	h.StartHLSSession(w, managedHLSRequest(state))
	if p.streams.Load() != 1 {
		t.Fatalf("did not reach actual main input: %d", p.streams.Load())
	}
	select {
	case <-body.reading:
	case <-time.After(3 * time.Second):
		t.Fatal("stdin copy never blocked on actual body")
	}
	select {
	case <-body.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("exited FFmpeg left owned stdin body blocked")
	}
}

type managedHLSCountingReader struct {
	reader io.Reader
	read   int64
}

func (r *managedHLSCountingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.read += int64(n)
	return n, err
}
func TestHLSInputActualProbeSuccessWithUnreadPrefix(t *testing.T) {
	data := managedHLSMedia(t)
	prefix := bytes.Repeat(data, (1<<20)/len(data)+1)[:1<<20]
	h, state := managedHLSHandler(t, &managedHLSProvider{data: data})
	req := managedHLSRequest(state)
	c, ok := h.beginVODHLSInput(httptest.NewRecorder(), req, "debrid/synthetic/media.ts")
	if !ok {
		t.Fatal("owner capture")
	}
	defer c.finishSetup()
	reader := &managedHLSCountingReader{reader: bytes.NewReader(prefix)}
	args := append([]string{"-v", "error", "-probesize", "32768", "-analyzeduration", "1", "-read_intervals", "%+0.01"}, vodHLSPipeOptions...)
	args = append(args, "-print_format", "json", "-show_streams", "-i", "pipe:0")
	output, err := c.runPipeCommand(c.ctx, "/usr/bin/ffprobe", args, io.NopCloser(reader), nil)
	if err != nil || !bytes.Contains(output, []byte("h264")) || reader.read >= int64(len(prefix)) {
		t.Fatalf("successful early inventory rejected or prefix fully consumed: err=%v bytes=%d/%d", err, reader.read, len(prefix))
	}
}

func TestHLSInputActualProbeEarlyExitClosesRetainedSingleReader(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	p := &managedHLSProvider{data: data, main: body}
	h, state := managedHLSHandler(t, p)
	wrapper := filepath.Join(t.TempDir(), "ffprobe-early-exit")
	script := "#!/usr/bin/python3\nimport os,sys\nos.execv('/usr/bin/ffprobe',['ffprobe','-synthetic-invalid-option']+sys.argv[1:])\n"
	if err := os.WriteFile(wrapper, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	h.hlsManager.ffprobePath = wrapper
	start := time.Now()
	w := httptest.NewRecorder()
	h.StartHLSSession(w, managedHLSRequest(state))
	if w.Code != http.StatusForbidden || time.Since(start) > 3*time.Second || p.streams.Load() != 1 {
		t.Fatalf("probe exit did not return promptly with one source: status=%d elapsed=%v calls=%d", w.Code, time.Since(start), p.streams.Load())
	}
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("failed real ffprobe retained shared source handle")
	}
	if body.closes.Load() != 1 {
		t.Fatal("probe source closed more than once")
	}
}
func TestHLSInputPrefixSetupBudgetClosesAndJoinsSingleReader(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	body.prefixRemaining = 0
	p := &managedHLSProvider{data: data, main: body}
	h, state := managedHLSHandler(t, p)
	req := managedHLSRequest(state)
	start := time.Now()
	w := httptest.NewRecorder()
	h.StartHLSSession(w, req)
	elapsed := time.Since(start)
	if elapsed < 9*time.Second || elapsed > 14*time.Second || w.Code != http.StatusForbidden || p.streams.Load() != 1 || body.closes.Load() != 1 {
		t.Fatalf("setup budget/close mismatch: elapsed=%v status=%d opens=%d closes=%d", elapsed, w.Code, p.streams.Load(), body.closes.Load())
	}
	if req.Context().Err() != nil {
		t.Fatal("prefix budget cancelled unrelated HTTP parent")
	}
	h.hlsManager.vodInputMu.Lock()
	pending := len(h.hlsManager.vodInputs)
	h.hlsManager.vodInputMu.Unlock()
	if pending != 0 {
		t.Fatal("prefix reader was not joined/released on return")
	}
}

type managedHLSAliasedResponseProvider struct {
	response *streaming.Response
	calls    atomic.Int32
}

func (p *managedHLSAliasedResponseProvider) Stream(context.Context, streaming.Request) (*streaming.Response, error) {
	p.calls.Add(1)
	return p.response, nil
}
func TestHLSInputReplaysExactPrefixAndSameCapturedReader(t *testing.T) {
	data := managedHLSMedia(t)
	body := newManagedHLSGate(data)
	original := &streaming.Response{Body: body, Status: http.StatusOK, ContentLength: int64(len(body.all)), Headers: make(http.Header)}
	p := &managedHLSAliasedResponseProvider{response: original}
	h, state := managedHLSHandler(t, p)
	// Deliberately mutate the provider's returned response after prefix capture.
	// The pipeline must retain its own original handle, not re-read this pointer.
	body.onPrefix = func() { original.Body = io.NopCloser(strings.NewReader("FOREIGN-VERSION")) }
	capture := filepath.Join(t.TempDir(), "capture-stdin")
	script := "#!/usr/bin/python3\nimport pathlib,sys\npathlib.Path(sys.argv[-1]).write_bytes(sys.stdin.buffer.read())\n"
	if err := os.WriteFile(capture, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	// This helper proves byte replay only; real FFmpeg media validation is in
	// TestHLSInputActualPipeSurvivesHTTPHandoffAndProducesPlayableTS.
	h.hlsManager.ffmpegPath = capture
	w := httptest.NewRecorder()
	h.StartHLSSession(w, managedHLSRequest(state))
	if w.Code != http.StatusOK {
		t.Fatalf("start failed: %d %s", w.Code, w.Body.String())
	}
	var response struct {
		ID string `json:"sessionId"`
	}
	if json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatal("invalid response")
	}
	snapshot, ok := h.hlsManager.snapshotHLSSession(response.ID)
	if !ok {
		t.Fatal("missing normal runtime")
	}
	close(body.gate)
	select {
	case <-snapshot.input.done:
	case <-time.After(5 * time.Second):
		t.Fatal("copy helper did not join actual reader")
	}
	captured := mustReadManagedHLS(t, filepath.Join(snapshot.output, "stream.m3u8"))
	if !bytes.Equal(captured, body.all) || p.calls.Load() != 1 || body.closes.Load() != 1 {
		t.Fatalf("prefix/source mixed or reopened: bytes=%d/%d opens=%d closes=%d", len(captured), len(body.all), p.calls.Load(), body.closes.Load())
	}
}

func TestHLSInputOwnedProcessLaunchRejectsNestedReferences(t *testing.T) {
	data := managedHLSMedia(t)
	for _, kind := range []string{"HTTP", "local FIFO"} {
		for _, binary := range []string{"ffprobe", "ffmpeg"} {
			for _, confined := range []bool{false, true} {
				name := kind + "/" + binary + "/control"
				if confined {
					name = kind + "/" + binary + "/confined"
				}
				t.Run(name, func(t *testing.T) {
					var calls atomic.Int32
					var opened <-chan struct{}
					cleanup := func() {}
					var payload []byte
					format := "hls"
					if kind == "HTTP" {
						server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							calls.Add(1)
							w.Header().Set("Content-Type", "video/mp2t")
							w.Write(data)
						}))
						defer server.Close()
						payload = []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\n" + server.URL + "/canary.ts\n#EXT-X-ENDLIST\n")
					} else {
						var fifo string
						fifo, opened, cleanup = localHLSCanary(t, data)
						defer cleanup()
						payload = []byte("ffconcat version 1.0\nfile 'file:" + fifo + "'\n")
						format = "concat"
					}
					provider := &managedHLSProvider{data: payload}
					h, state := managedHLSHandler(t, provider)
					req := managedHLSRequest(state)
					c, ok := h.beginVODHLSInput(httptest.NewRecorder(), req, "debrid/synthetic/media.ts")
					if !ok {
						t.Fatal("real current owner capture failed")
					}
					defer c.finishSetup()
					source, err := c.open(c.ctx, http.MethodGet, "")
					if err != nil {
						t.Fatal(err)
					}
					defer source.Close()
					captured, err := io.ReadAll(io.LimitReader(source.Body, 1<<20))
					if err != nil {
						t.Fatal(err)
					}
					// Force the demuxer in these adversarial process tests. Production never
					// accepts a caller format; this forces its actual launch boundary to enforce
					// the pipe/demuxer restriction rather than rely on sniffing/filename refusal.
					args := []string{"-v", "error"}
					if confined {
						args = append(args, vodHLSPipeOptions...)
					} else {
						args = append(args, "-protocol_whitelist", "pipe,file,http,tcp")
					}
					if format == "concat" {
						args = append(args, "-safe", "0")
					} else {
						args = append(args, "-allowed_extensions", "ALL")
					}
					args = append(args, "-f", format)
					var started func(*exec.Cmd)
					if binary == "ffprobe" {
						args = append(args, "-show_streams", "-i", "pipe:0")
					} else {
						args = append(args, "-i", "pipe:0", "-f", "null", "-")
						started = func(*exec.Cmd) {}
					}
					ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
					defer cancel()
					_, err = c.runPipeCommand(ctx, "/usr/bin/"+binary, args, io.NopCloser(bytes.NewReader(captured)), started)
					accessed := calls.Load() > 0
					if kind == "local FIFO" {
						select {
						case <-opened:
							accessed = true
						default:
						}
					}
					if confined {
						if err == nil || accessed {
							t.Fatalf("actual owned process escaped confinement: err=%v accessed=%v", err, accessed)
						}
					} else if err != nil || !accessed {
						t.Fatalf("unsafe control did not actually consume nested input: err=%v accessed=%v", err, accessed)
					}
					if provider.streams.Load() != 1 {
						t.Fatal("process-launch test reopened original source")
					}
				})
			}
		}
	}
}

type managedHLSFailingReader struct {
	data    *bytes.Reader
	failure error
}

func (r *managedHLSFailingReader) Read(p []byte) (int, error) {
	n, err := r.data.Read(p)
	if err == io.EOF {
		return 0, r.failure
	}
	return n, err
}
func (r *managedHLSFailingReader) Close() error { return nil }
func TestHLSInputProbeDoesNotHideGenuineSourceFailure(t *testing.T) {
	data := managedHLSMedia(t)
	prefix := data[:256<<10]
	sentinel := errors.New("synthetic source read failure")
	h, state := managedHLSHandler(t, &managedHLSProvider{data: data})
	c, ok := h.beginVODHLSInput(httptest.NewRecorder(), managedHLSRequest(state), "debrid/synthetic/media.ts")
	if !ok {
		t.Fatal("owner capture")
	}
	defer c.finishSetup()
	args := append([]string{"-v", "error"}, vodHLSPipeOptions...)
	args = append(args, "-show_streams", "-i", "pipe:0")
	control, err := runHLSCanaryCommand(t, "/usr/bin/ffprobe", args, prefix)
	if err != nil || !bytes.Contains(control, []byte("h264")) {
		t.Fatalf("source-failure control is not valid short inventory: %v %s", err, control)
	}
	_, err = c.runPipeCommand(c.ctx, "/usr/bin/ffprobe", args, &managedHLSFailingReader{data: bytes.NewReader(prefix), failure: sentinel}, nil)
	if !errors.Is(err, sentinel) {
		t.Fatalf("genuine source read error became probe success: %v", err)
	}
}

type managedHLSInvalidateReader struct {
	*bytes.Reader
	once       sync.Once
	invalidate func()
}

func (r *managedHLSInvalidateReader) Close() error { r.once.Do(r.invalidate); return nil }
func TestHLSInputProbeProcessSuccessCannotRenewInvalidatedOwner(t *testing.T) {
	data := managedHLSMedia(t)
	h, state := managedHLSHandler(t, &managedHLSProvider{data: data})
	c, ok := h.beginVODHLSInput(httptest.NewRecorder(), managedHLSRequest(state), "debrid/synthetic/media.ts")
	if !ok {
		t.Fatal("owner capture")
	}
	defer c.finishSetup()
	source := &managedHLSInvalidateReader{Reader: bytes.NewReader(data[:1<<20]), invalidate: func() { state.boundary.invalidateProfile("profile") }}
	args := append([]string{"-v", "error", "-probesize", "32768", "-analyzeduration", "1", "-read_intervals", "%+0.01"}, vodHLSPipeOptions...)
	args = append(args, "-show_streams", "-i", "pipe:0")
	_, err := c.runPipeCommand(c.ctx, "/usr/bin/ffprobe", args, source, nil)
	if err == nil {
		t.Fatal("successful child metadata revived invalidated original owner")
	}
	select {
	case <-c.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("invalidated original context was not cancelled")
	}
}

func TestHLSInputRetirementPreservesCurrentClaimAfterGenerationChange(t *testing.T) {
	f := newRootHLSSessionFixture(t, vodAccountActive, false)
	old, ok := f.manager.snapshotHLSSession(f.session.ID)
	if !ok || old.input == nil {
		t.Fatal("missing genuine consumer owner capture")
	}
	file := filepath.Join(old.output, "segment0.ts")
	before := mustReadManagedHLS(t, file)
	f.session.mu.Lock()
	f.session.SeekGeneration++
	f.session.mu.Unlock()
	// Configured Seek stays unsupported. This is a defensive generation/cleanup
	// invariant, not an observed public generation-change route.
	old.input.finishSetup()
	after := mustReadManagedHLS(t, file)
	f.manager.mu.RLock()
	same := f.manager.sessions[f.session.ID] == f.session
	f.manager.mu.RUnlock()
	if !same || !bytes.Equal(before, after) {
		t.Fatal("retired old generation deleted current claim/files")
	}
	current, ok := f.manager.snapshotHLSSession(f.session.ID)
	if !ok || current.generation != old.generation+1 {
		t.Fatal("current generation lost")
	}
	if !f.manager.cleanupHLSSessionSnapshot(current) {
		t.Fatal("fresh exact generation could not clean owned output")
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatal("fresh exact cleanup failed")
	}
}
