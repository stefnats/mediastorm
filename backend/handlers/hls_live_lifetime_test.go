//go:build linux

package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/livecapacity"
)

// These observations refer to the actual original objects. They do not create
// source permission or replace native child/descriptor completion with counters.
func encoderLifeStack(parts ...string) bool {
	buf := make([]byte, 2<<20)
	n := runtime.Stack(buf, true)
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		found := true
		for _, part := range parts {
			found = found && strings.Contains(stack, part)
		}
		if found {
			return true
		}
	}
	return false
}

func encoderLifePending(t *testing.T, done <-chan struct{}, f *ccLifeFixture) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("physical cleanup returned while actual work remained")
	default:
	}
	if ordinaryLifeCharged(f.manager) != 1 {
		t.Fatal("live output charge refunded before physical work ended")
	}
	if _, err := os.Stat(f.session.OutputDir); err != nil {
		t.Fatal("output removed while actual work remained", err)
	}
}

func encoderLifeRestart(t *testing.T, f *ccLifeFixture) (*exec.Cmd, *exec.Cmd) {
	t.Helper()
	first := ordinaryLifeCommand(t, f.session)
	_ = liveLifeOutput(t, f.session)
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	var next *exec.Cmd
	ordinaryLifeWait(t, "original supervisor did not start its actual next attempt", func() bool {
		f.session.mu.RLock()
		next = f.session.FFmpegCmd
		ready := next != nil && next != first && next.Process != nil
		f.session.mu.RUnlock()
		return ready
	})
	ordinaryLifeGone(t, first)
	if next.Process.Pid == first.Process.Pid {
		t.Fatal("replacement witness reused the old child")
	}
	return first, next
}

// One nonparallel case temporarily owns the real Default manager. Its cache-only
// reader returns no policy grant, and every actual operation joins before restore.
var encoderLifeDefaultFixture sync.Mutex

func encoderLifeRejectedSpawn(t *testing.T) {
	t.Helper()
	if !encoderLifeDefaultFixture.TryLock() {
		t.Fatal("overlapping Default capacity fixture")
	}
	old := livecapacity.Default
	isolated := livecapacity.NewManager()
	ff, probe := ordinaryLifeTools(t)
	source, server := liveLifeHTTPServer(t, ccLifeGolden(t, true), false)
	root, cancel := context.WithCancel(t.Context())
	manager := prepareHLSManager(root, t.TempDir(), ff, probe, nil)
	control := manager.ordinaryControl()
	entered, allow := make(chan struct{}), make(chan struct{})
	var enterOnce, allowOnce sync.Once
	isolated.SetAutomaticLimitReader(func(string) livecapacity.AutomaticLimit {
		enterOnce.Do(func() { close(entered) })
		<-allow
		return livecapacity.AutomaticLimit{}
	})
	done := make(chan struct{})
	var got *HLSSession
	var result error
	var generation *ordinaryHLSGeneration
	managerLocked, generationLocked, started := false, false, false
	t.Cleanup(func() {
		cancel()
		allowOnce.Do(func() { close(allow) })
		if managerLocked {
			manager.mu.Unlock()
			managerLocked = false
		}
		if generationLocked {
			generation.mu.Unlock()
			generationLocked = false
		}
		if started {
			ordinaryLifeChannel(t, done, "actual pending CreateLiveSession did not return")
		}
		manager.Shutdown()
		ordinaryLifeWait(t, "original pending provider HTTP work remained", func() bool { return source.active.Load() == 0 })
		if livecapacity.Default != isolated {
			t.Error("capacity Default changed during nonparallel fixture")
		} else {
			livecapacity.Default = old
		}
		encoderLifeDefaultFixture.Unlock()
	})
	livecapacity.Default = isolated
	if err := manager.activatePreparedMedia(); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	managerLocked = true
	started = true
	go func() {
		defer close(done)
		got, result = manager.CreateLiveSession(t.Context(), server.URL+"/pending.ts", "m3u", t.Name(), "synthetic-profile", "Synthetic", "127.0.0.1", "native", LiveTuningSettings{MaxStreams: 1, ProbeSizeMB: 1, AnalyzeDurationSec: 1}, false)
	}()
	ordinaryLifeChannel(t, entered, "actual capacity admission was not entered")
	control.mu.Lock()
	var owner *ordinaryHLSSession
	for candidate := range control.pending {
		if owner != nil {
			t.Error("unexpected second pending owner")
		}
		owner = candidate
	}
	control.mu.Unlock()
	if owner == nil {
		t.Fatal("no actual original pending reservation before admission")
	}
	allowOnce.Do(func() { close(allow) })
	var session *HLSSession
	ordinaryLifeWait(t, "actual pending generation was not attached", func() bool {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		session, generation = owner.session, owner.generation
		return session != nil && generation != nil
	})
	generation.mu.Lock()
	generationLocked = true
	manager.mu.Unlock()
	managerLocked = false
	ordinaryLifeWait(t, "actual supervisor registration did not reach its held generation gate", func() bool {
		manager.mu.RLock()
		actual := manager.sessions[session.ID]
		manager.mu.RUnlock()
		return actual == session && encoderLifeStack("(*ordinaryHLSGeneration).spawn", "(*HLSManager).CreateLiveSession")
	})
	if generation.ctx.Err() != nil {
		t.Fatal("original already cancelled before genuine registration witness")
	}
	if current, _ := isolated.Usage(t.Name(), 1); current != 1 {
		t.Fatal("actual pending provider lease absent", current)
	}
	if unexpected, err := isolated.Acquire(t.Name(), "different-source", 1); !errors.Is(err, livecapacity.ErrLimit) {
		unexpected.Release()
		t.Fatal("real provider limit did not retain pending lease", err)
	}
	if source.requests.Load() != 0 {
		t.Fatal("supervisor opened a source before actual registration")
	}
	cancel()
	generation.mu.Unlock()
	generationLocked = false
	ordinaryLifeChannel(t, done, "rejected actual registration did not complete")
	if result == nil || got != nil {
		t.Fatal("cancelled pending registration unexpectedly succeeded", result)
	}
	ordinaryLifeChannel(t, session.liveSupervisorDone, "rejected registration observation remained open")
	if current, _ := isolated.Usage(t.Name(), 1); current != 0 {
		t.Fatal("no-spawn path leaked actual provider capacity", current)
	}
	fresh, err := isolated.Acquire(t.Name(), "fresh-source", 1)
	if err != nil {
		t.Fatal("actual fresh provider reservation remained blocked", err)
	}
	fresh.Release()
	if ordinaryLifeCharged(manager) != 0 || source.requests.Load() != 0 {
		t.Fatal("rejected registration leaked lifetime or opened provider")
	}
	if _, err := os.Stat(session.OutputDir); !os.IsNotExist(err) {
		t.Fatal("rejected original output survived", err)
	}
}

func TestLiveEncoderActualAttemptJoin(t *testing.T) {
	t.Run("initial-current-live-real-ffmpeg", func(t *testing.T) {
		f := ccLifeCreate(t, "web", false, nil, LiveTuningSettings{})
		command := ordinaryLifeCommand(t, f.session)
		path := liveLifeOutput(t, f.session)
		_, probe := ordinaryLifeTools(t)
		_, data, err := ccLifeRun(t, probe, "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=codec_name", "-of", "json", path)
		if err != nil || !strings.Contains(string(data), "h264") {
			t.Fatal("actual original Live encoder output is not playable H264", err, string(data))
		}
		if command.Process == nil || f.session.ordinary.current().ctx.Err() != nil {
			t.Fatal("original active child/lifetime absent")
		}
		ccLifeEnd(t, f)
	})
	t.Run("cancellation-before-start-closes-both-stdio-ends", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		ff, _ := ordinaryLifeTools(t)
		sourcePath := filepath.Join(t.TempDir(), "synthetic-source.ts")
		if err := os.WriteFile(sourcePath, ccLifeGolden(t, true), 0600); err != nil {
			t.Fatal(err)
		}
		source, err := os.Open(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		ctx, run := newOrdinaryHLSCommand(f.session.ordinary.current().context(), f.session)
		command := exec.CommandContext(ctx, ff, "-v", "error", "-i", "pipe:0", "-f", "null", "-")
		pipe, err := newShareHLSPipe(command, source, source)
		if err != nil {
			_ = source.Close()
			run.finish()
			t.Fatal(err)
		}
		stderr, writer, err := ordinaryHLSStderr(command)
		if err != nil {
			pipe.closeAndJoin()
			run.finish()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = writer.Close(); pipe.closeAndJoin(); _ = stderr.Close(); run.finish() })
		if err := run.setCommand(command, source, stderr); err != nil {
			t.Fatal(err)
		}
		run.cancel()
		if err := run.startCommand(); err == nil || command.Process != nil {
			t.Fatal("cancelled original started an actual child", err)
		}
		_ = writer.Close()
		pipe.closeAndJoin()
		_ = stderr.Close()
		run.finish()
		stdin, ok := pipe.writer.(*os.File)
		if !ok {
			t.Fatal("actual owned stdin is not OS descriptor")
		}
		ccLifeAssertClosed(t, source, pipe.childInput, stdin, stderr, writer)
		ccLifeEnd(t, f)
	})
	t.Run("command-wait-before-reader-and-source-refund", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		command := ordinaryLifeCommand(t, f.session)
		upstream := liveLifeOriginal(t, f.manager, f.session)
		var sub *liveUpstreamSubscriber
		ordinaryLifeWait(t, "actual original subscriber missing", func() bool {
			upstream.mu.Lock()
			defer upstream.mu.Unlock()
			for candidate := range upstream.subscribers {
				sub = candidate
				break
			}
			return sub != nil
		})
		sub.readMu.Lock()
		held := true
		done := make(chan struct{})
		started := false
		t.Cleanup(func() {
			f.cancel()
			if held {
				sub.readMu.Unlock()
				held = false
			}
			if started {
				ordinaryLifeChannel(t, done, "actual held-reader cleanup remained")
			}
		})
		ordinaryLifeWait(t, "actual original Read did not enter held reader gate", func() bool {
			return encoderLifeStack(fmt.Sprintf("(*liveUpstreamSubscriber).Read(%p", sub), "lockSlow")
		})
		started = true
		go func() { defer close(done); f.manager.CleanupSession(f.session.ID) }()
		ordinaryLifeWait(t, "original Close did not signal held actual reader", func() bool { return sub.closing.Load() })
		ordinaryLifeGone(t, command)
		encoderLifePending(t, done, f)
		sub.readMu.Unlock()
		held = false
		ordinaryLifeChannel(t, done, "actual Read/source Close did not join")
		liveLifeOriginalDone(t, upstream)
		ccLifeEnd(t, f)
	})
	t.Run("healthy-terminal-stderr-drained-before-recovery", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		ordinaryLifeTerminalStderr(t, f.session)
		first, next := encoderLifeRestart(t, f)
		ccLifeEnd(t, f)
		ordinaryLifeGone(t, first)
		ordinaryLifeGone(t, next)
	})
	t.Run("all-idle-starvation-watchers-joined", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		command := ordinaryLifeCommand(t, f.session)
		_ = liveLifeOutput(t, f.session)
		g := f.session.ordinary.current()
		ordinaryLifeWait(t, "actual stderr/idle/starvation actors absent", func() bool { g.mu.Lock(); defer g.mu.Unlock(); return g.children >= 4 })
		ccLifeEnd(t, f)
		g.mu.Lock()
		children := g.children
		g.mu.Unlock()
		if children != 0 || f.source.active.Load() != 0 {
			t.Fatal("actual original workers or provider remained", children)
		}
		ordinaryLifeGone(t, command)
	})
	t.Run("old-attempt-no-post-restart-publication", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		owner, g := f.session.ordinary, f.session.ordinary.current()
		first, next := encoderLifeRestart(t, f)
		if f.session.ordinary != owner || owner.current() != g {
			t.Fatal("retry replaced original owner")
		}
		ccLifeEnd(t, f)
		ordinaryLifeGone(t, first)
		ordinaryLifeGone(t, next)
		if ordinaryHLSGo(g.context(), f.session, func() { t.Error("retired generation executed late work") }) {
			t.Fatal("retired actor admitted post-cleanup work")
		}
	})
	t.Run("failover-startup-timer-invocation-joined", encoderLifeStartupJoin)
	t.Run("runtime-stop-during-genuine-pending-start", encoderLifeRejectedSpawn)
	t.Run("supervisor-cancel-does-not-self-wait", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		command := ordinaryLifeCommand(t, f.session)
		original := liveLifeOriginal(t, f.manager, f.session)
		f.cancel()
		ordinaryLifeChannel(t, f.session.liveSupervisorDone, "original supervisor self-waited")
		ordinaryLifeGone(t, command)
		liveLifeOriginalDone(t, original)
		ccLifeEnd(t, f)
	})
	t.Run("isolated-legacy-process-fixtures-preserved", func(t *testing.T) {
		ff, _ := ordinaryLifeTools(t)
		data := ccLifeGolden(t, true)
		var requests, active atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			active.Add(1)
			defer active.Add(-1)
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(data) // A real finite HTTP input, with natural body EOF.
		}))
		t.Cleanup(server.Close)
		output := filepath.Join(t.TempDir(), "actual-output")
		session := &HLSSession{ID: "isolated-original", Path: server.URL + "/synthetic-legacy.ts", OutputDir: output, PlaybackTarget: "native", IsLive: true, LastSegmentRequest: time.Now(), LiveTuning: LiveTuningSettings{ProbeSizeMB: 1, AnalyzeDurationSec: 1}}
		manager := &HLSManager{ffmpegPath: ff}
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		done := make(chan struct{})
		started := false
		var err error
		t.Cleanup(func() {
			cancel()
			if started {
				ordinaryLifeChannel(t, done, "isolated real command did not join")
			}
		})
		started = true
		go func() { defer close(done); err = manager.startLiveTranscoding(ctx, session, 0) }()
		command := ordinaryLifeCommand(t, session)
		key := liveUpstreamShareKey(session.Path, session.LiveTuning)
		var original *liveUpstream
		ordinaryLifeWait(t, "actual isolated HTTP source/reader not observed", func() bool {
			sharedLiveUpstreams.mu.Lock()
			original = sharedLiveUpstreams.streams[key]
			sharedLiveUpstreams.mu.Unlock()
			return original != nil
		})
		ordinaryLifeChannel(t, done, "finite isolated original process did not return")
		if err != nil || command.ProcessState == nil {
			t.Fatal("isolated actual legacy process failed", err)
		}
		ordinaryLifeGone(t, command)
		liveLifeOriginalDone(t, original) // Actual body, pump, watchdog and reader end.
		server.Close()                    // Join the real local producer's accepted HTTP handlers too.
		if requests.Load() == 0 || active.Load() != 0 {
			t.Fatal("actual finite HTTP producer was not consumed/joined")
		}
		path := liveLifeOutput(t, session)
		if info, statErr := os.Stat(path); statErr != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			t.Fatal("actual nonempty legacy output absent", statErr)
		}
		if session.ordinary != nil {
			t.Fatal("isolated fixture manufactured original media permission")
		}
	})
}

func encoderLifeStartupJoin(t *testing.T) {
	ff, probe := ordinaryLifeTools(t)
	data := ccLifeGolden(t, true)
	state, media := liveLifeHTTPServer(t, data, false)
	catalog := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "#EXTM3U\n#EXTINF:-1 tvg-id=\"Synthetic.de\" tvg-country=\"DE\",Synthetic HD\n%s/original.ts\n#EXTINF:-1 tvg-id=\"Synthetic.de\" tvg-country=\"DE\",Synthetic FHD\n%s/alternative.ts\n", media.URL, media.URL)
	}))
	t.Cleanup(catalog.Close)
	cfg := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if err := cfg.Save(config.Settings{Live: config.LiveSettings{AutoChannelFailover: true, GroupChannelsByStation: true, Sources: []config.LivePlaylistSource{{ID: "synthetic", Name: "Synthetic", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, catalog.URL+"/get.php?username=synthetic&password=synthetic"), MaxStreams: 2, StreamFormat: "direct"}}}}); err != nil {
		t.Fatal(err)
	}
	live := NewLiveHandler(catalog.Client(), true, ff, 24, 0, 0, false, cfg, nil)
	h := NewVideoHandlerWithProvider(true, ff, probe, t.TempDir(), nil)
	t.Cleanup(h.Shutdown)
	h.SetConfigManager(cfg)
	h.SetLiveChannelProvider(live)
	request := httptest.NewRequest(http.MethodGet, "/api/live/hls/start?sourceId=synthetic&profileId=synthetic-profile&url="+url.QueryEscape(media.URL+"/original.ts"), nil)
	channels, err := live.FetchFilteredChannelsForRequest(request)
	if err != nil || len(channels) != 2 || channels[0].StationID == "" || channels[0].StationID != channels[1].StationID || channels[0].URL == channels[1].URL {
		t.Fatal("actual filtered catalog is not two interchangeable quality variants", err)
	}
	plan := h.liveStationPlan(request, media.URL+"/original.ts")
	if plan == nil {
		t.Fatal("actual configured catalog did not produce original failover plan")
	}
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	m := prepareHLSManager(root, t.TempDir(), ff, probe, nil)
	t.Cleanup(m.Shutdown)
	if err := m.activatePreparedMedia(); err != nil {
		t.Fatal(err)
	}
	h.liveStationHealthMu.Lock()
	held := true
	done := make(chan struct{})
	started := false
	t.Cleanup(func() {
		cancel()
		if held {
			h.liveStationHealthMu.Unlock()
			held = false
		}
		if started {
			ordinaryLifeChannel(t, done, "actual startup callback cleanup remained")
		}
	})
	session, err := m.CreateLiveSession(t.Context(), media.URL+"/original.ts", "m3u", t.Name(), "synthetic-profile", "Synthetic", "127.0.0.1", "native", LiveTuningSettings{MaxStreams: 2, ProbeSizeMB: 1, AnalyzeDurationSec: 1, StationFailover: plan}, false)
	if err != nil {
		t.Fatal(err)
	}
	command := ordinaryLifeCommand(t, session)
	_ = liveLifeOutput(t, session)
	ordinaryLifeWait(t, "actual startup timer callback was not invoked", func() bool { return encoderLifeStack("(*liveStationFailover).recordSuccess", "lockSlow") })
	started = true
	go func() { defer close(done); m.CleanupSession(session.ID) }()
	ordinaryLifeGone(t, command)
	encoderLifePending(t, done, &ccLifeFixture{manager: m, session: session})
	h.liveStationHealthMu.Unlock()
	held = false
	ordinaryLifeChannel(t, done, "actual startup timer invocation did not join")
	ordinaryLifeChannel(t, session.liveSupervisorDone, "original failover supervisor did not end")
	ordinaryLifeWait(t, "actual catalog source remained", func() bool { return state.active.Load() == 0 })
	if ordinaryLifeCharged(m) != 0 {
		t.Fatal("joined startup callback retained output charge")
	}
}

type encoderLifeHeldWriter struct {
	http.ResponseWriter
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (w *encoderLifeHeldWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *encoderLifeHeldWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if flushErr := http.NewResponseController(w.ResponseWriter).Flush(); err == nil {
		err = flushErr
	}
	w.once.Do(func() { close(w.entered); <-w.release })
	return n, err
}

func TestLiveOutputActualRetirement(t *testing.T) {
	t.Run("held-caption-read-before-directory-removal", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		actual := ccLifeExtractor(t, f)
		ccLifeCaption(t, actual)
		entered, release := make(chan struct{}), make(chan struct{})
		var unblock sync.Once
		handlerDone, clientDone, cleanupDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		clientStarted, cleanupStarted := false, false
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerDone)
			f.manager.ServeLiveCaptions(&encoderLifeHeldWriter{ResponseWriter: w, entered: entered, release: release}, r, f.session.ID)
		}))
		t.Cleanup(func() {
			unblock.Do(func() { close(release) })
			f.cancel()
			server.Close()
			if clientStarted {
				ordinaryLifeChannel(t, clientDone, "actual TCP body consumer remained")
			}
			if cleanupStarted {
				ordinaryLifeChannel(t, cleanupDone, "actual held output cleanup remained")
			}
		})
		clientStarted = true
		go func() {
			defer close(clientDone)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			response, err := server.Client().Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
			}
		}()
		ordinaryLifeChannel(t, entered, "actual caption HTTP writer not reached")
		if ordinaryLifeFDs(actual.outputPath) == 0 {
			t.Fatal("actual caption file descriptor not held through Write")
		}
		cleanupStarted = true
		go func() { defer close(cleanupDone); f.manager.CleanupSession(f.session.ID) }()
		ordinaryLifeWait(t, "actual original cleanup did not begin", func() bool { f.session.mu.RLock(); defer f.session.mu.RUnlock(); return f.session.Stopped })
		encoderLifePending(t, cleanupDone, f)
		if ordinaryLifeFDs(actual.outputPath) == 0 {
			t.Fatal("caption descriptor released before actual HTTP Write returned")
		}
		unblock.Do(func() { close(release) })
		ordinaryLifeChannel(t, handlerDone, "actual caption handler did not join")
		ordinaryLifeChannel(t, clientDone, "actual caption TCP body did not end")
		ordinaryLifeChannel(t, cleanupDone, "actual caption/output cleanup did not join")
		if ordinaryLifeFDs(actual.outputPath) != 0 || ordinaryLifeCharged(f.manager) != 0 {
			t.Fatal("actual caption/output lease retained")
		}
	})
	t.Run("held-cc-constructor-before-output-refund", func(t *testing.T) {
		f := ccLifeCreate(t, "web", true, nil, LiveTuningSettings{})
		ccLifeDetector(t, f)
		_, bound, finish, ok := f.manager.ordinaryHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/captions.srt", nil), f.session.ID)
		if !ok {
			t.Fatal("actual original output reference denied")
		}
		var finishOnce sync.Once
		ctorDone, cleanupDone := make(chan struct{}), make(chan struct{})
		ctorStarted, cleanupStarted, held := false, false, false
		t.Cleanup(func() {
			f.cancel()
			if held {
				f.manager.mu.Unlock()
				held = false
			}
			if ctorStarted {
				ordinaryLifeChannel(t, ctorDone, "actual held constructor remained")
			}
			finishOnce.Do(finish)
			if cleanupStarted {
				ordinaryLifeChannel(t, cleanupDone, "actual constructor cleanup remained")
			}
		})
		f.manager.mu.Lock()
		held = true
		var actual *ccExtractor
		var err error
		ctorStarted = true
		go func() {
			defer close(ctorDone)
			actual, err = f.manager.ensureLiveCCExtractor(bound.Context(), f.session)
		}()
		ordinaryLifeWait(t, "actual original constructor did not reach held runtime gate", func() bool {
			f.session.mu.RLock()
			pending := f.session.ccStarting
			f.session.mu.RUnlock()
			return pending != nil && encoderLifeStack("(*HLSManager).ensureLiveCCExtractor", "(*ordinaryHLSSession).originalRuntime")
		})
		f.cancel()
		cleanupStarted = true
		go func() { defer close(cleanupDone); f.manager.CleanupSession(f.session.ID) }()
		// m.mu blocks actual cleanup lookup; the original output is still owned.
		if _, statErr := os.Stat(f.session.OutputDir); statErr != nil {
			t.Fatal("constructor output disappeared", statErr)
		}
		f.manager.mu.Unlock()
		held = false
		ordinaryLifeChannel(t, ctorDone, "actual rejected constructor did not return")
		if actual != nil || err == nil {
			t.Fatal("cancelled original constructor published decoder", err)
		}
		finishOnce.Do(finish)
		ordinaryLifeChannel(t, cleanupDone, "actual constructor/output references did not join")
		ordinaryLifeChannel(t, f.session.ordinary.done, "actual original generation remained")
		if ordinaryLifeCharged(f.manager) != 0 {
			t.Fatal("actual constructor output remained charged")
		}
	})
	t.Run("prior-encoder-attempt-no-output-recreation", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		first, next := encoderLifeRestart(t, f)
		ccLifeEnd(t, f)
		ordinaryLifeGone(t, first)
		ordinaryLifeGone(t, next)
		if _, err := os.Stat(f.session.OutputDir); !os.IsNotExist(err) {
			t.Fatal("retired attempt recreated output", err)
		}
	})
	t.Run("repeated-concurrent-cleanup-and-shutdown", func(t *testing.T) {
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		command := ordinaryLifeCommand(t, f.session)
		var work sync.WaitGroup
		work.Add(4)
		done := make(chan struct{})
		started := false
		t.Cleanup(func() {
			f.cancel()
			if started {
				ordinaryLifeChannel(t, done, "actual concurrent cleanup remained")
			}
		})
		started = true
		for i := 0; i < 4; i++ {
			go func(i int) {
				defer work.Done()
				if i%2 == 0 {
					f.manager.CleanupSession(f.session.ID)
				} else {
					f.manager.Shutdown()
				}
			}(i)
		}
		go func() { work.Wait(); close(done) }()
		ordinaryLifeChannel(t, done, "actual repeated cleanup/shutdown did not join")
		ordinaryLifeChannel(t, f.session.liveSupervisorDone, "original supervisor remained")
		ordinaryLifeGone(t, command)
		if ordinaryLifeCharged(f.manager) != 0 || f.source.active.Load() != 0 {
			t.Fatal("concurrent cleanup refunded before source end")
		}
	})
	t.Run("failed-removal-keeps-output-charge-after-actors-end", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Fatal("real EACCES witness requires the non-root validation user")
		}
		f := ccLifeCreate(t, "native", false, nil, LiveTuningSettings{})
		command := ordinaryLifeCommand(t, f.session)
		_ = liveLifeOutput(t, f.session)
		retained := filepath.Join(f.session.OutputDir, "actual-retained-evidence")
		if err := os.WriteFile(retained, []byte("synthetic retained output"), 0600); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(f.session.OutputDir, 0700); f.manager.CleanupSession(f.session.ID) })
		if err := os.Chmod(f.session.OutputDir, 0500); err != nil {
			t.Fatal(err)
		}
		f.manager.Shutdown()
		ordinaryLifeChannel(t, f.session.ordinary.done, "ended runtime blocked on storage removal failure")
		ordinaryLifeGone(t, command)
		control := f.manager.ordinaryControl()
		control.mu.Lock()
		failure := control.retired[f.session.ordinary]
		control.mu.Unlock()
		if !errors.Is(failure, os.ErrPermission) || ordinaryLifeCharged(f.manager) != 1 {
			t.Fatal("real failed output removal lost evidence/charge", failure)
		}
		if _, err := os.Stat(retained); err != nil {
			t.Fatal("failed-removal evidence absent", err)
		}
		if err := os.Chmod(f.session.OutputDir, 0700); err != nil {
			t.Fatal(err)
		}
		f.manager.CleanupSession(f.session.ID)
		if ordinaryLifeCharged(f.manager) != 0 {
			t.Fatal("confirmed retry removal did not refund exact output")
		}
	})
	t.Run("managed-unknown-live-cannot-gain-source-by-life-data", func(t *testing.T) {
		fixture := newPublicPinCarrierFixture(t)
		installed := familyActualPGInstall(t, fixture)
		ff, probe := ordinaryLifeTools(t)
		poison := &rootHLSInputPoisonProvider{}
		handler := NewVideoHandlerWithProvider(true, ff, probe, t.TempDir(), poison)
		defer handler.Shutdown()
		if err := handler.installHLSInputBoundary(installed.Boundary.boundary); err != nil {
			t.Fatal(err)
		}
		var requests atomic.Int32
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); _, _ = w.Write(tsPacket(1)) }))
		defer source.Close()
		session, err := handler.hlsManager.CreateLiveSession(t.Context(), source.URL+"/unknown.ts", "m3u", t.Name(), fixture.profile.ID, "Synthetic", "127.0.0.1", "native", LiveTuningSettings{}, true)
		if session != nil || err == nil || requests.Load() != 0 || poison.streams.Load() != 0 || ordinaryLifeCharged(handler.hlsManager) != 0 || liveLifeManagerRefs(handler.hlsManager) != 0 {
			t.Fatal("lifetime DATA bypassed actual managed source denial")
		}
	})
}
