//go:build linux

package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The required fixtures are synthetic, independently decoded by the original
// pinned tools. A missing fixture/tool is not a passing optional media test.
func ccLifeGolden(t *testing.T, positive bool) []byte {
	t.Helper()
	dir := os.Getenv("MEDIASTORM_CC_GOLDEN_DIR")
	if dir == "" {
		t.Fatal("MEDIASTORM_CC_GOLDEN_DIR is required for the actual synthetic CC canaries")
	}
	name, want := "positive.ts", "01c8a23d4ce81ca38e47487af2532e11290c114435e0050ea527153e4452af35"
	if !positive {
		name, want = "negative.ts", "91b532265f6cc1553528c81ef465f772bb6cffe163ff0ef3c6c8854e54580739"
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil || len(data) == 0 || len(data) > maxFallbackWindowBytes {
		t.Fatal("bounded genuine golden fixture unavailable", err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != want {
		t.Fatal("synthetic fixture bytes differ from the independently accepted golden")
	}
	return data
}

func ccLifeDecoded(t *testing.T, data []byte) bool {
	t.Helper()
	text := strings.TrimPrefix(string(data), "\xef\xbb\xbf")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	found, interval := false, false
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "HI" {
			found = true
		}
		match := srtTimestampLineRe.FindStringSubmatch(line)
		if match != nil {
			start := parseSRTTimestamp(match[1], match[2], match[3], match[4])
			end := parseSRTTimestamp(match[5], match[6], match[7], match[8])
			if start < 0 || end <= start {
				t.Fatal("actual decoded cue has no finite increasing interval")
			}
			interval = true
		}
	}
	return found && interval
}

func ccLifeRun(t *testing.T, binary string, args ...string) (*exec.Cmd, []byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	var output, diagnostic shareHLSCapture
	cmd.Stdout, cmd.Stderr = &output, &diagnostic
	err := cmd.Run() // Sole actual Wait also joins exec's real stdout/stderr IO.
	if ctx.Err() != nil {
		t.Fatalf("actual synthetic decoder timed out: %v %.500s", ctx.Err(), diagnostic.Bytes())
	}
	if cmd.Process != nil {
		ordinaryLifeGone(t, cmd)
	}
	if err != nil {
		return cmd, append(output.Bytes(), diagnostic.Bytes()...), err
	}
	return cmd, output.Bytes(), nil
}

func ccLifeDecodeFile(t *testing.T, source string, positive bool) []byte {
	t.Helper()
	binary, err := exec.LookPath("ccextractor")
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "actual-decoded.srt")
	_, diagnostic, err := ccLifeRun(t, binary, source, "--out", "srt", "-o", target)
	result, readErr := os.ReadFile(target)
	if positive {
		if err != nil || readErr != nil || !ccLifeDecoded(t, result) {
			t.Fatalf("actual output lost synthetic HI: %v %v %.500s", err, readErr, diagnostic)
		}
	} else {
		var exited *exec.ExitError
		if !errors.As(err, &exited) || exited.ExitCode() != 10 || !strings.Contains(string(diagnostic), "No captions were found") || len(strings.TrimSpace(string(result))) != 0 {
			t.Fatalf("negative fixture became a decoded caption: %v %.500s", err, diagnostic)
		}
	}
	return result
}

type ccLifeFixture struct {
	manager *HLSManager
	session *HLSSession
	source  *liveLifeHTTP
	server  *httptest.Server
	cancel  context.CancelFunc
}

func ccLifeCreate(t *testing.T, target string, captions bool, original context.Context, tuning LiveTuningSettings) *ccLifeFixture {
	t.Helper()
	data := ccLifeGolden(t, true)
	source, server := liveLifeHTTPServer(t, data, false)
	if original == nil {
		original = t.Context()
	}
	root, cancel := context.WithCancel(original)
	ffmpeg, probe := ordinaryLifeTools(t)
	manager := prepareHLSManager(root, t.TempDir(), ffmpeg, probe, nil)
	f := &ccLifeFixture{manager: manager, source: source, server: server, cancel: cancel}
	t.Cleanup(func() {
		cancel()
		manager.Shutdown()
		if f.session != nil {
			liveLifeAwait(t, f.session.liveSupervisorDone, "original Live supervisor remained after fixture shutdown")
		}
		ordinaryLifeWait(t, "original synthetic provider HTTP work remains", func() bool { return source.active.Load() == 0 })
	})
	if err := manager.activatePreparedMedia(); err != nil {
		t.Fatal("actual prepared original manager activation", err)
	}
	if tuning.MaxStreams == 0 {
		tuning.MaxStreams = 16
	}
	if tuning.ProbeSizeMB == 0 {
		tuning.ProbeSizeMB = 1
	}
	if tuning.AnalyzeDurationSec == 0 {
		tuning.AnalyzeDurationSec = 1
	}
	session, err := manager.CreateLiveSession(t.Context(), server.URL+"/original.ts", "m3u", t.Name(), "synthetic-profile", "Synthetic", "127.0.0.1", target, tuning, captions)
	if err != nil {
		t.Fatal("actual original Live creation", err)
	}
	f.session = session
	if session.ordinary == nil || session.vodIdentity == nil || session.vodIdentity.session != session {
		t.Fatal("real publication did not attach its concrete original lifetime")
	}
	return f
}

func ccLifeDetector(t *testing.T, f *ccLifeFixture) {
	t.Helper()
	ordinaryLifeWait(t, "actual original detector did not produce a positive caption observation", func() bool {
		f.session.mu.RLock()
		defer f.session.mu.RUnlock()
		return f.session.CCDetectionDone && f.session.HasClosedCaptions
	})
}

func ccLifeExtractor(t *testing.T, f *ccLifeFixture) *ccExtractor {
	t.Helper()
	ccLifeDetector(t, f)
	response := httptest.NewRecorder()
	f.manager.ServeLiveCaptions(response, httptest.NewRequest(http.MethodGet, "/captions.srt", nil), f.session.ID)
	if response.Code != http.StatusOK {
		t.Fatal("actual lazy caption request denied", response.Code, response.Body.String())
	}
	var actual *ccExtractor
	ordinaryLifeWait(t, "actual original CC constructor did not publish", func() bool {
		f.session.mu.RLock()
		actual = f.session.ccExtractor
		f.session.mu.RUnlock()
		return actual != nil
	})
	return actual
}

func ccLifeCaption(t *testing.T, actual *ccExtractor) []byte {
	t.Helper()
	var data []byte
	ordinaryLifeWait(t, "actual original decoder did not emit finite synthetic HI", func() bool {
		value, err := os.ReadFile(actual.outputPath)
		if err != nil {
			return false
		}
		data = value
		return ccLifeDecoded(t, value)
	})
	return data
}

func ccLifeProcess(t *testing.T, actual *ccExtractor) *exec.Cmd {
	t.Helper()
	var command *exec.Cmd
	ordinaryLifeWait(t, "actual CC command was never started", func() bool {
		actual.mu.Lock()
		defer actual.mu.Unlock()
		command = actual.cmd
		return command != nil && command.Process != nil
	})
	return command
}

func ccLifeEnd(t *testing.T, f *ccLifeFixture) {
	t.Helper()
	command := ordinaryLifeCommand(t, f.session)
	f.manager.CleanupSession(f.session.ID)
	liveLifeAwait(t, f.session.liveSupervisorDone, "original supervisor returned without completing")
	ordinaryLifeChannel(t, f.session.ordinary.done, "original generation output/worker references remain")
	ordinaryLifeGone(t, command)
	if ordinaryLifeCharged(f.manager) != 0 {
		t.Fatal("actual completed Live output remained charged")
	}
	if _, err := os.Stat(f.session.OutputDir); !os.IsNotExist(err) {
		t.Fatal("actual completed Live output remains", err)
	}
}

func ccLifeActualOutput(t *testing.T, f *ccLifeFixture) string {
	t.Helper()
	var paths []string
	ordinaryLifeWait(t, "actual original encoder did not produce three complete TS segments", func() bool {
		paths = listLiveSegments(f.session.OutputDir)
		return len(paths) >= 4
	})
	target := filepath.Join(t.TempDir(), "actual-live-output.ts")
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var written int64
	for _, path := range paths[:3] {
		file, err := os.Open(path)
		if err != nil {
			_ = output.Close()
			t.Fatal(err)
		}
		count, copyErr := io.Copy(output, io.LimitReader(file, maxFallbackWindowBytes-written))
		closeErr := file.Close()
		written += count
		if copyErr != nil || closeErr != nil || written >= maxFallbackWindowBytes {
			_ = output.Close()
			t.Fatal("actual output copy exceeded its finite budget", copyErr, closeErr)
		}
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	return target
}

func ccLifeAssertClosed(t *testing.T, files ...*os.File) {
	t.Helper()
	for _, file := range files {
		if file == nil {
			t.Fatal("actual descriptor witness is missing")
		}
		if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("actual descriptor remained open", file.Name(), err)
		}
	}
}

func ccLifeStoppedFeeder(t *testing.T, actual *ccExtractor) (*exec.Cmd, []*os.File) {
	t.Helper()
	command := ccLifeProcess(t, actual)
	t.Cleanup(func() { actual.stop() })
	if err := command.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal("stop only our original decoder", err)
	}
	ordinaryLifeWait(t, "actual decoder did not enter its native stopped state", func() bool {
		status, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "status"))
		return err == nil && (strings.Contains(string(status), "State:\tT") || strings.Contains(string(status), "State:\tt"))
	})
	var files []*os.File
	ordinaryLifeWait(t, "real original feeder never blocked with actual retained input and stdin", func() bool {
		actual.mu.Lock()
		files = nil
		for file := range actual.files {
			files = append(files, file)
		}
		actual.mu.Unlock()
		stack := make([]byte, 2<<20)
		count := runtime.Stack(stack, true)
		if len(files) == 0 {
			return false
		}
		// Pinned Go falls back to File.Write for regular-file -> pipe copies;
		// require the blocked write and actual receiver in THIS one goroutine.
		for _, worker := range strings.Split(string(stack[:count]), "\n\n") {
			if strings.Contains(worker, fmt.Sprintf("(*ccExtractor).feedNewSegmentsToCCExtractor(%p", actual)) &&
				strings.Contains(worker, "io.copyBuffer") && strings.Contains(worker, "internal/poll.(*FD).Write") &&
				strings.Contains(worker, "runtime_pollWait") {
				return true
			}
		}
		return false
	})
	return command, files
}

func TestLiveCCActualActorJoin(t *testing.T) {
	if _, err := exec.LookPath("ccextractor"); err != nil {
		t.Fatal("actual ccextractor is required", err)
	}
	t.Run("detector-actual-process-wait", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		ccLifeDetector(t, f)
		actual := ccLifeActualOutput(t, f)
		ccLifeDecodeFile(t, actual, true)
		negative := filepath.Join(t.TempDir(), "actual-negative.ts")
		if err := os.WriteFile(negative, ccLifeGolden(t, false), 0600); err != nil {
			t.Fatal(err)
		}
		ccLifeDecodeFile(t, negative, false)
		_, probe := ordinaryLifeTools(t)
		_, bytes, err := ccLifeRun(t, probe, "-v", "error", "-select_streams", "v:0", "-show_frames", "-show_streams", "-show_entries", "frame=side_data_list:stream=codec_name", "-of", "json", actual)
		if err != nil {
			t.Fatal("actual encoded output could not be probed", err)
		}
		var decoded map[string]json.RawMessage
		if err := json.Unmarshal(bytes, &decoded); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(decoded["streams"]), "h264") || !strings.Contains(string(decoded["frames"]), "ATSC A53 Part 4 Closed Captions") {
			t.Fatal("actual compatibility encoder did not retain native A53 data", string(bytes))
		}
		f.session.mu.RLock()
		encoder := f.session.VideoEncoder
		f.session.mu.RUnlock()
		if encoder != "libx264" {
			t.Fatal("retention witness did not exercise the actual reencoder", encoder)
		}
		ccLifeEnd(t, f)
	})
	t.Run("detector-output-no-late-publication", func(t *testing.T) {
		ffmpeg, probe := ordinaryLifeTools(t)
		root, cancel := context.WithCancel(t.Context())
		defer cancel()
		state, source := liveLifeHTTPServer(t, ccLifeGolden(t, true), true)
		m := prepareHLSManager(root, t.TempDir(), ffmpeg, probe, nil)
		t.Cleanup(m.Shutdown)
		if err := m.activatePreparedMedia(); err != nil {
			t.Fatal(err)
		}
		session, err := m.CreateLiveSession(t.Context(), source.URL+"/pending.ts", "m3u", t.Name(), "synthetic-profile", "Synthetic", "127.0.0.1", "web", LiveTuningSettings{ProbeSizeMB: 1, AnalyzeDurationSec: 1}, true)
		if err != nil {
			t.Fatal(err)
		}
		ordinaryLifeWait(t, "actual original detector/source did not start", func() bool {
			g := session.ordinary.current()
			g.mu.Lock()
			children := g.children
			g.mu.Unlock()
			return state.active.Load() > 0 && children >= 2
		})
		session.mu.RLock()
		before := session.CCDetectionDone
		session.mu.RUnlock()
		if before {
			t.Fatal("silent source unexpectedly produced a detector result")
		}
		cancel()
		m.CleanupSession(session.ID)
		ordinaryLifeChannel(t, session.ordinary.done, "original detector did not physically end")
		liveLifeAwait(t, session.liveSupervisorDone, "original supervisor did not end")
		session.mu.RLock()
		after := session.CCDetectionDone
		session.mu.RUnlock()
		if after {
			t.Fatal("retired original published a late detector observation")
		}
		if _, err := os.Stat(session.OutputDir); !os.IsNotExist(err) {
			t.Fatal("retired detector output still exists", err)
		}
	})
	t.Run("ccextractor-stdin-active-write-interrupted", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		actual := ccLifeExtractor(t, f)
		if actual.mode != "ccextractor" {
			t.Fatal("required actual CCExtractor mode was not used")
		}
		command, files := ccLifeStoppedFeeder(t, actual)
		actual.mu.Lock()
		stdin := actual.stdin
		actual.mu.Unlock()
		if _, err := stdin.Stat(); err != nil {
			t.Fatal("actual blocked stdin already closed", err)
		}
		f.manager.CleanupSession(f.session.ID)
		ordinaryLifeChannel(t, actual.done, "actual blocked feeder/command did not join")
		ordinaryLifeGone(t, command)
		ccLifeAssertClosed(t, append(files, stdin)...)
		if ordinaryLifeCharged(f.manager) != 0 {
			t.Fatal("blocked feeder cleanup retained/refunded the wrong owner")
		}
	})
	t.Run("ccextractor-command-wait-and-both-fds", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		actual := ccLifeExtractor(t, f)
		ccLifeCaption(t, actual)
		command := ccLifeProcess(t, actual)
		actual.mu.Lock()
		input, writer := actual.childInput, actual.stdin
		actual.mu.Unlock()
		ccLifeAssertClosed(t, input)
		if _, err := writer.Stat(); err != nil {
			t.Fatal("original parent stdin writer missing", err)
		}
		if !strings.Contains(strings.Join(command.Args, " "), "--input ts -s 120") {
			t.Fatal("actual known-TS original stream command differs", command.Args)
		}
		actual.stop()
		ordinaryLifeChannel(t, actual.processDone, "actual CCExtractor Wait not completed")
		ordinaryLifeGone(t, command)
		ccLifeAssertClosed(t, input, writer)
		if command.ProcessState == nil {
			t.Fatal("signal/channel substituted for actual command Wait")
		}
		ccLifeEnd(t, f)
	})
	t.Run("fallback-ffmpeg-window-actual-child-joined", func(t *testing.T) {
		ffmpeg, _ := ordinaryLifeTools(t)
		// The real fallback is selected through absent optional CCExtractor,
		// not by setting a lifecycle or source-permission boolean on an owner.
		t.Setenv("PATH", filepath.Dir(ffmpeg))
		if _, err := exec.LookPath("ccextractor"); err == nil {
			t.Fatal("fallback fixture still resolves optional CCExtractor")
		}
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		actual := ccLifeExtractor(t, f)
		if actual.mode != "ffmpeg" {
			t.Fatal("actual optional-tool fallback was not selected")
		}
		command := ccLifeProcess(t, actual)
		ccLifeCaption(t, actual)
		actual.stop()
		ordinaryLifeGone(t, command)
		if _, err := os.Stat(filepath.Join(f.session.OutputDir, "cc_window.ts")); !os.IsNotExist(err) {
			t.Fatal("actual fallback input survived joined cleanup", err)
		}
		if _, err := os.Stat(actual.outputPath + ".tmp"); !os.IsNotExist(err) {
			t.Fatal("actual fallback temporary output survived cleanup", err)
		}
		ccLifeEnd(t, f)
	})
	t.Run("lazy-caption-start-versus-stop", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		ccLifeDetector(t, f)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.manager.ServeLiveCaptions(w, r, f.session.ID) }))
		t.Cleanup(server.Close)
		var workers sync.WaitGroup
		workers.Add(4)
		done := make(chan struct{})
		started := false
		t.Cleanup(func() {
			f.cancel()
			if started {
				ordinaryLifeChannel(t, done, "actual concurrent caption HTTP consumers remained")
			}
		})
		for i := 0; i < 4; i++ {
			go func() {
				defer workers.Done()
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
				response, err := server.Client().Do(req)
				if err == nil {
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
				}
			}()
		}
		go func() { workers.Wait(); close(done) }()
		started = true
		var actual *ccExtractor
		ordinaryLifeWait(t, "actual concurrent lazy constructor never published", func() bool {
			f.session.mu.RLock()
			actual = f.session.ccExtractor
			f.session.mu.RUnlock()
			return actual != nil
		})
		command := ccLifeProcess(t, actual)
		f.manager.CleanupSession(f.session.ID)
		ordinaryLifeChannel(t, done, "actual caption HTTP/body work did not join")
		ordinaryLifeChannel(t, actual.done, "actual constructor/loop did not join")
		ordinaryLifeGone(t, command)
		if ordinaryLifeCharged(f.manager) != 0 {
			t.Fatal("concurrent actual constructor retained its output owner")
		}
	})
	t.Run("same-current-owner-caption-http-fixed-expiry", func(t *testing.T) {
		original, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		expiry, _ := original.Deadline()
		f := ccLifeCreate(t, "web", true, original, LiveTuningSettings{})
		actual := ccLifeExtractor(t, f)
		ccLifeCaption(t, actual)
		owner := f.session.ordinary
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.manager.ServeLiveCaptions(w, r, f.session.ID) }))
		t.Cleanup(server.Close)
		for i := 0; i < 2; i++ {
			response, err := server.Client().Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			data, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if response.StatusCode != http.StatusOK || readErr != nil || closeErr != nil || !ccLifeDecoded(t, data) {
				t.Fatal("actual healthy caption TCP body incomplete", response.StatusCode, readErr, closeErr)
			}
			at, ok := owner.ctx.Deadline()
			if !ok || !at.Equal(expiry) || f.session.ordinary != owner {
				t.Fatal("fresh caption consumer renewed/replaced the original lifetime")
			}
		}
		timer := time.NewTimer(max(time.Second, time.Until(expiry)+time.Second))
		defer timer.Stop()
		select {
		case <-original.Done():
		case <-timer.C:
			t.Fatal("original fixed expiry did not occur")
		}
		response, err := server.Client().Get(server.URL)
		if err != nil {
			t.Fatal("expired route did not produce a bounded explicit response", err)
		}
		data, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable || readErr != nil || strings.Contains(string(data), " --> ") {
			t.Fatal("expired original produced implicit success/captions", response.StatusCode, readErr, string(data))
		}
		ccLifeEnd(t, f)
	})
	t.Run("signal-all-cc-actors-before-join", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		actual := ccLifeExtractor(t, f)
		command, files := ccLifeStoppedFeeder(t, actual)
		actual.mu.Lock()
		writer := actual.stdin
		actual.mu.Unlock()
		if len(files) == 0 {
			t.Fatal("no actual feeder source descriptor was retained")
		}
		actual.stop()
		ordinaryLifeChannel(t, actual.done, "actual feeder/IO/Wait owner did not join")
		ordinaryLifeGone(t, command)
		ccLifeAssertClosed(t, append(files, writer)...)
		ccLifeEnd(t, f)
	})
}
