//go:build linux

package handlers

import (
	"context"
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
	"sync/atomic"
	"testing"
	"time"

	"novastream/services/streaming"
)

// Regular synthetic files exercise process ownership, not a media/PIN grant.
type ordinaryLifeProvider struct {
	path        string
	opens       atomic.Int32
	mu          sync.Mutex
	holdBodies  bool
	held        []*ordinaryLifeCloseGate
	probePipe   bool
	pipeReady   chan *os.File
	pipeWriters []*os.File
}

func (p *ordinaryLifeProvider) GetDirectURL(ctx context.Context, _ string) (string, error) {
	return p.path, ctx.Err()
}
func (p *ordinaryLifeProvider) Stream(ctx context.Context, r streaming.Request) (*streaming.Response, error) {
	p.opens.Add(1)
	p.mu.Lock()
	probePipe := p.probePipe
	p.mu.Unlock()
	if probePipe {
		reader, writer, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		p.pipeWriters = append(p.pipeWriters, writer)
		ready := p.pipeReady
		p.mu.Unlock()
		if ready != nil {
			ready <- reader
		}
		return &streaming.Response{Body: reader, Status: 200, Headers: make(http.Header), ContentLength: -1}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if r.Method == http.MethodHead {
		_ = f.Close()
		return &streaming.Response{Status: 200, Headers: make(http.Header), ContentLength: info.Size()}, nil
	}
	p.mu.Lock()
	held := p.holdBodies
	var body io.ReadCloser = f
	if held {
		gate := &ordinaryLifeCloseGate{file: f, entered: make(chan struct{}), release: make(chan struct{})}
		p.held = append(p.held, gate)
		body = gate
	}
	p.mu.Unlock()
	return &streaming.Response{Body: body, Status: 200, Headers: make(http.Header), ContentLength: info.Size()}, nil
}
func ordinaryLifeTools(t *testing.T) (string, string) {
	t.Helper()
	a, e := exec.LookPath("ffmpeg")
	if e != nil {
		t.Fatal(e)
	}
	b, e := exec.LookPath("ffprobe")
	if e != nil {
		t.Fatal(e)
	}
	return a, b
}
func ordinaryLifeMedia(t *testing.T) string {
	t.Helper()
	ff, _ := ordinaryLifeTools(t)
	dir := t.TempDir()
	srt := filepath.Join(dir, "source.srt")
	if e := os.WriteFile(srt, []byte("1\n00:00:00,000 --> 00:00:30,000\nSynthetic ownership witness\n"), 0600); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "source.mkv")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ff, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=64x64:r=5", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-i", srt, "-t", "60", "-map", "0:v", "-map", "1:a", "-map", "2:s", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-g", "10", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "32k", "-c:s", "srt", path)
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("synthetic media: %v: %.1000s", e, out)
	}
	return path
}
func ordinaryLifeWait(t *testing.T, label string, ready func() bool) {
	t.Helper()
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-timer.C:
			t.Fatal(label)
		case <-tick.C:
		}
	}
}
func ordinaryLifeChannel(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(15 * time.Second):
		t.Fatal(label)
	}
}
func ordinaryLifeManager(t *testing.T, path string, slow bool) (*HLSManager, *HLSSession, *ordinaryLifeProvider) {
	t.Helper()
	ff, probe := ordinaryLifeTools(t)
	if slow {
		wrapper := filepath.Join(t.TempDir(), "ffmpeg-owned.sh")
		quoted := "'" + strings.ReplaceAll(ff, "'", "'\\''") + "'"
		if e := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec "+quoted+" -readrate 1 \"$@\"\n"), 0700); e != nil {
			t.Fatal(e)
		}
		ff = wrapper
	}
	p := &ordinaryLifeProvider{path: path}
	m := NewHLSManager(t.TempDir(), ff, probe, p)
	t.Cleanup(m.Shutdown)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	s, e := m.CreateSession(ctx, "synthetic-source", "synthetic-source", false, "", false, false, 0, 0, -1, -1, "synthetic-profile", "Synthetic", "127.0.0.1", false, "", "native", 60, "")
	if e != nil || s == nil {
		t.Fatal("actual ordinary CreateSession failed", e)
	}
	if s.ordinary == nil || s.vodIdentity == nil || !s.ordinary.originalRuntime() {
		t.Fatal("actual published original missing")
	}
	if !slow {
		ordinaryLifeWait(t, "actual initial encoder did not complete", func() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.Completed })
		s.mu.RLock()
		failure := s.FatalError
		s.mu.RUnlock()
		if failure != "" {
			t.Fatal("actual initial encoder failed", failure)
		}
		ordinaryLifeOutput(t, s)
	}
	return m, s, p
}
func ordinaryLifeCommand(t *testing.T, s *HLSSession) *exec.Cmd {
	t.Helper()
	var cmd *exec.Cmd
	ordinaryLifeWait(t, "actual encoder PID not observed", func() bool {
		s.mu.RLock()
		defer s.mu.RUnlock()
		cmd = s.FFmpegCmd
		return cmd != nil && cmd.Process != nil
	})
	return cmd
}
func ordinaryLifeGone(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		t.Fatal("no original process")
	}
	ordinaryLifeWait(t, "original process was not physically reaped", func() bool {
		_, e := os.Stat(filepath.Join("/proc", strconv.Itoa(cmd.Process.Pid)))
		return os.IsNotExist(e)
	})
}
func ordinaryLifeOutput(t *testing.T, s *HLSSession) string {
	t.Helper()
	var path string
	ordinaryLifeWait(t, "actual nonempty segment not produced", func() bool {
		s.mu.RLock()
		ext, dir := s.actualSegmentExt, s.OutputDir
		s.mu.RUnlock()
		if ext != ".ts" && ext != ".m4s" && ext != ".mp4" {
			return false
		}
		path = filepath.Join(dir, "segment0"+ext)
		f, e := os.Stat(path)
		return e == nil && f.Mode().IsRegular() && f.Size() > 0
	})
	return path
}
func ordinaryLifeCharged(m *HLSManager) int {
	c := m.ordinaryControl()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}
func ordinaryLifeFDs(path string) int {
	files, _ := os.ReadDir("/proc/self/fd")
	n := 0
	for _, f := range files {
		target, _ := os.Readlink(filepath.Join("/proc/self/fd", f.Name()))
		if target == path {
			n++
		}
	}
	return n
}

type ordinaryLifeCloseGate struct {
	file    *os.File
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	unblock sync.Once
	peer    *ordinaryLifeCloseGate
}

func (g *ordinaryLifeCloseGate) Read(p []byte) (int, error) { return g.file.Read(p) }
func (g *ordinaryLifeCloseGate) Close() error {
	var err error
	g.once.Do(func() { close(g.entered); <-g.release; err = g.file.Close() })
	return err
}
func (g *ordinaryLifeCloseGate) open() {
	g.unblock.Do(func() { close(g.release) })
	if g.peer != nil {
		g.peer.open()
	}
}

// This starts the real command through the production original-generation gate.
// Its source descriptor is inherited by the child, so Cmd.Wait plus Close joins
// both the process and the exact retained input file; no HTTP socket is a proxy
// for an unobserved Usenet worker.
func ordinaryLifeRun(t *testing.T, s *HLSSession, path string, held bool) (*exec.Cmd, <-chan struct{}, *ordinaryLifeCloseGate) {
	t.Helper()
	ff, _ := ordinaryLifeTools(t)
	file, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	gate := &ordinaryLifeCloseGate{file: file, entered: make(chan struct{}), release: make(chan struct{})}
	if !held {
		gate.open()
	}
	workerContext := s.ordinary.current().context()
	ctx, run := newOrdinaryHLSCommand(workerContext, s)
	if run == nil {
		t.Fatal("original command admission missing")
	}
	cmd := exec.CommandContext(ctx, ff, "-hide_banner", "-loglevel", "warning", "-re", "-i", "pipe:0", "-t", "60", "-f", "null", "-")
	cmd.Stdin = file
	stderr, e := cmd.StderrPipe()
	if e != nil {
		gate.open()
		_ = gate.Close()
		run.finish()
		t.Fatal(e)
	}
	if e = run.setCommand(cmd, gate, stderr); e != nil {
		gate.open()
		_ = gate.Close()
		_ = stderr.Close()
		run.finish()
		t.Fatal(e)
	}
	if held {
		// A real second probe descriptor shares this actual command lifetime.
		// Its Close must be signalled even while the stdin descriptor holds Close.
		second, err := os.Open(path)
		if err != nil {
			gate.open()
			_ = gate.Close()
			run.finish()
			t.Fatal(err)
		}
		peer := &ordinaryLifeCloseGate{file: second, entered: make(chan struct{}), release: make(chan struct{})}
		gate.peer = peer
		if err = run.setInput(peer); err != nil {
			gate.open()
			_ = peer.Close()
			_ = gate.Close()
			run.finish()
			t.Fatal(err)
		}
		var header [1]byte
		if _, err = second.Read(header[:]); err != nil {
			gate.open()
			_ = peer.Close()
			_ = gate.Close()
			run.finish()
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	started := make(chan error, 1)
	launched := false
	t.Cleanup(func() {
		gate.open()
		run.cancel()
		if launched {
			<-done
		} else {
			_ = gate.Close()
			_ = stderr.Close()
			run.finish()
		}
	})
	if !ordinaryHLSGo(workerContext, s, func() {
		defer close(done)
		if e := run.startCommand(); e != nil {
			started <- e
			_ = stderr.Close()
			run.finish()
			return
		}
		started <- nil
		if !ordinaryHLSGo(ctx, s, func() { _, _ = io.Copy(io.Discard, stderr) }) {
			run.cancel()
		}
		_ = cmd.Wait()
		_ = stderr.Close()
		_ = ordinaryAfterCommand(ctx, run)
	}) {
		t.Fatal("real command worker denied")
	}
	launched = true
	if e := <-started; e != nil {
		t.Fatal(e)
	}
	return cmd, done, gate
}

func TestOrdinaryHLSActualGenerationJoin(t *testing.T) {
	media := ordinaryLifeMedia(t)
	t.Run("initial-vod-real-process", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, true)
		cmd := ordinaryLifeCommand(t, s)
		ff, _ := ordinaryLifeTools(t)
		actual, e := os.Readlink(fmt.Sprintf("/proc/%d/exe", cmd.Process.Pid))
		resolved, _ := filepath.EvalSymlinks(ff)
		if e != nil || actual != resolved {
			t.Fatal("actual FFmpeg ELF not observed", e)
		}
		ordinaryLifeOutput(t, s)
		m.CleanupSession(s.ID)
		ordinaryLifeGone(t, cmd)
		if ordinaryLifeCharged(m) != 0 {
			t.Fatal("cleaned output retained charge")
		}
	})
	t.Run("cancel-during-process-start", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		g := s.ordinary.current()
		ctx, run := newOrdinaryHLSCommand(g.context(), s)
		run.cancel()
		ff, _ := ordinaryLifeTools(t)
		cmd := exec.CommandContext(ctx, ff, "-version")
		if e := run.setCommand(cmd, nil, nil); e == nil {
			t.Fatal("cancelled real setup accepted command")
		}
		if e := run.startCommand(); e == nil || cmd.Process != nil {
			t.Fatal("cancelled setup started child")
		}
		run.finish()
		// These are real Start/cancel contenders. A particular kernel or Go
		// scheduler interleaving is not assumed or reported as observed.
		for attempt := 0; attempt < 8; attempt++ {
			actual, err := os.Open(media)
			if err != nil {
				t.Fatal(err)
			}
			attemptContext, attemptRun := newOrdinaryHLSCommand(g.context(), s)
			child := exec.CommandContext(attemptContext, ff, "-version")
			child.Stdin = actual
			child.Stdout = io.Discard
			child.Stderr = io.Discard
			if err = attemptRun.setCommand(child, ordinaryInputBody(actual), nil); err != nil {
				_ = actual.Close()
				attemptRun.finish()
				t.Fatal(err)
			}
			entered := make(chan struct{})
			startDone := make(chan error, 1)
			go func() { close(entered); startDone <- attemptRun.startCommand() }()
			<-entered
			runtime.Gosched()
			attemptRun.cancel()
			startErr := <-startDone
			if child.Process != nil {
				_ = child.Wait()
			}
			attemptRun.finish()
			if child.Process != nil {
				ordinaryLifeGone(t, child)
			} else if startErr == nil {
				t.Fatal("actual Start succeeded without a process")
			}
			if _, err = actual.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("real raced input descriptor not physically closed", err)
			}
		}
		m.CleanupSession(s.ID)
	})
	t.Run("cleanup-waits-original-cmd-wait", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		cmd, done, gate := ordinaryLifeRun(t, s, media, true)
		finished := make(chan struct{})
		started := false
		t.Cleanup(func() {
			gate.open()
			if started {
				<-finished
			}
		})
		go func() { m.CleanupSession(s.ID); close(finished) }()
		started = true
		ordinaryLifeChannel(t, gate.entered, "actual input Close not entered")
		if ordinaryLifeFDs(media) == 0 {
			t.Fatal("retained input descriptor vanished early")
		}
		select {
		case <-finished:
			t.Fatal("cleanup returned before real input close")
		default:
		}
		if ordinaryLifeCharged(m) != 1 {
			t.Fatal("charge refunded while original input retained")
		}
		gate.open()
		ordinaryLifeChannel(t, finished, "cleanup did not join original command")
		ordinaryLifeChannel(t, done, "worker did not end")
		ordinaryLifeGone(t, cmd)
	})
	t.Run("seek-waits-old-before-file-reset", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		_, _, gate := ordinaryLifeRun(t, s, media, true)
		sentinel := filepath.Join(s.OutputDir, "original-owned-sentinel")
		if e := os.WriteFile(sentinel, []byte("owned"), 0600); e != nil {
			t.Fatal(e)
		}
		old := s.ordinary.current()
		done := make(chan struct{})
		w := httptest.NewRecorder()
		launched := false
		t.Cleanup(func() {
			gate.open()
			if launched {
				<-done
			}
		})
		go func() { m.Seek(w, httptest.NewRequest("POST", "/?time=1", nil), s.ID); close(done) }()
		launched = true
		ordinaryLifeChannel(t, gate.entered, "seek did not close original actual input")
		if _, e := os.Stat(sentinel); e != nil {
			t.Fatal("seek reset output before input/process join", e)
		}
		gate.open()
		ordinaryLifeChannel(t, done, "seek did not finish")
		if w.Code != 200 || s.ordinary.current() == old {
			t.Fatal("actual seek did not replace joined generation", w.Code, w.Body.String())
		}
		if ordinaryHLSGo(old.context(), s, func() { t.Error("stale worker executed") }) {
			t.Fatal("old generation accepted work")
		}
	})
	t.Run("stable-restart-waits-old", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		_, _, gate := ordinaryLifeRun(t, s, media, true)
		old := s.ordinary.current()
		w := httptest.NewRecorder()
		_, r, finish, ok := m.ordinaryHTTP(w, httptest.NewRequest("GET", "/", nil), s.ID)
		if !ok {
			t.Fatal("actual HTTP ref denied")
		}
		defer finish()
		done := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			gate.open()
			if launched {
				<-done
			}
		})
		go func() {
			m.restartCastTranscodingAt(s, castSegmentRestart{SegmentStartNumber: 0, TranscodingOffset: 1}, r)
			close(done)
		}()
		launched = true
		ordinaryLifeChannel(t, gate.entered, "restart did not reach actual old input close")
		if s.ordinary.current() != old {
			t.Fatal("successor published before old input joined")
		}
		gate.open()
		ordinaryLifeChannel(t, done, "restart did not end")
		if s.ordinary.current() == old {
			t.Fatal("restart kept retired generation")
		}
	})
	t.Run("automatic-recovery-original-lifetime", func(t *testing.T) {
		_, s, _ := ordinaryLifeManager(t, media, false)
		g := s.ordinary.current()
		ctx, run := newOrdinaryHLSCommand(g.context(), s)
		ff, _ := ordinaryLifeTools(t)
		cmd := exec.CommandContext(ctx, ff, "-hide_banner", "-loglevel", "error", "-i", filepath.Join(t.TempDir(), "missing.mkv"), "-f", "null", "-")
		if e := run.setCommand(cmd, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e := run.startCommand(); e != nil {
			t.Fatal(e)
		}
		if e := cmd.Wait(); e == nil {
			t.Fatal("actual failed attempt unexpectedly succeeded")
		}
		next := ordinaryAfterCommand(ctx, run)
		if ordinaryGeneration(next, s) != g || next.Err() != nil {
			t.Fatal("failed attempt replaced/cancelled original generation")
		}
		cmd2 := exec.CommandContext(next, ff, "-version")
		if e := cmd2.Run(); e != nil {
			t.Fatal("same original generation cannot run recovery", e)
		}
		g.cancel()
		if _, e := s.ordinary.beginChange(next); e == nil {
			t.Fatal("revoked original renewed after recovery")
		}
	})
	t.Run("old-generation-no-state-publication", func(t *testing.T) {
		_, s, _ := ordinaryLifeManager(t, media, false)
		old := s.ordinary.current()
		next, end, e := s.ordinary.beginChange(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		end()
		var ran atomic.Bool
		if ordinaryHLSGo(old.context(), s, func() { ran.Store(true) }) || ran.Load() {
			t.Fatal("retired generation published state")
		}
		if !ordinaryHLSGo(next, s, func() {}) {
			t.Fatal("actual successor rejected")
		}
	})
	t.Run("actual-stderr-reader-joined", func(t *testing.T) {
		m, s, p := ordinaryLifeManager(t, media, false)
		// A real synchronous probe must interrupt its actual source/pipe copier
		// on the original deadline, not just kill its ffprobe process.
		p.mu.Lock()
		p.probePipe = true
		p.pipeReady = make(chan *os.File, 1)
		ready := p.pipeReady
		p.mu.Unlock()
		ctx, cancel := context.WithTimeout(s.ordinary.current().context(), 5*time.Second)
		probeDone := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			cancel()
			p.mu.Lock()
			writers := append([]*os.File(nil), p.pipeWriters...)
			p.mu.Unlock()
			for _, file := range writers {
				_ = file.Close()
			}
			if launched {
				<-probeDone
			}
		})
		var probeErr error
		if !ordinaryHLSGo(ctx, s, func() { defer close(probeDone); _, probeErr = m.probeDuration(ctx, "synthetic-blocked-pipe") }) {
			t.Fatal("actual probe worker denied")
		}
		launched = true
		var actualReader *os.File
		select {
		case actualReader = <-ready:
		case <-time.After(15 * time.Second):
			t.Fatal("actual probe source not opened")
		}
		_, probe := ordinaryLifeTools(t)
		resolved, _ := filepath.EvalSymlinks(probe)
		pid := ordinaryLifeChildPID(t, resolved)
		ordinaryLifeChannel(t, probeDone, "original probe timeout did not join actual body/copier")
		if probeErr == nil {
			t.Fatal("empty held real pipe unexpectedly probed")
		}
		if _, err := actualReader.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("probe returned before actual pipe descriptor closed", err)
		}
		ordinaryLifeWait(t, "actual ffprobe process remains after joined timeout", func() bool { _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); return os.IsNotExist(err) })
		p.mu.Lock()
		p.probePipe = false
		p.mu.Unlock()
		ordinaryLifeTerminalStderr(t, s)
		cmd, done, gate := ordinaryLifeRun(t, s, media, false)
		gate.open()
		m.CleanupSession(s.ID)
		ordinaryLifeChannel(t, done, "actual stderr reader or waiter remains")
		ordinaryLifeGone(t, cmd)
		if ordinaryLifeFDs(media) != 0 {
			t.Fatal("input descriptor remains after command reader join")
		}
	})
	t.Run("actual-subtitle-extractor-joined", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		out := filepath.Join(s.OutputDir, "owned-subtitle.vtt")
		g := s.ordinary.current()
		result := make(chan error, 1)
		if !ordinaryHLSGo(g.context(), s, func() { result <- m.extractSubtitleTrackToVTT(g.context(), s, 2, out) }) {
			t.Fatal("original extractor denied")
		}
		select {
		case e := <-result:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("actual subtitle child did not end")
		}
		data, e := os.ReadFile(out)
		if e != nil || !strings.Contains(string(data), "Synthetic ownership witness") {
			t.Fatal("actual subtitle output missing", e)
		}
		m.CleanupSession(s.ID)
		if ordinaryLifeFDs(out) != 0 {
			t.Fatal("subtitle descriptor survived cleanup")
		}
	})
	t.Run("failed-start-no-live-descriptors", func(t *testing.T) {
		_, s, _ := ordinaryLifeManager(t, media, false)
		ff, _ := ordinaryLifeTools(t)
		for attempt := 0; attempt < 2; attempt++ {
			ctx, run := newOrdinaryHLSCommand(s.ordinary.current().context(), s)
			file, err := os.Open(media)
			if err != nil {
				t.Fatal(err)
			}
			wrapped := ordinaryInputBody(file)
			executable := filepath.Join(t.TempDir(), "missing-ffmpeg")
			if attempt == 1 {
				executable = ff
			}
			cmd := exec.CommandContext(ctx, executable, "-version")
			pipe, err := newShareHLSPipe(cmd, wrapped, wrapped)
			if err != nil {
				_ = wrapped.Close()
				run.finish()
				t.Fatal(err)
			}
			reader, writer, err := ordinaryHLSStderr(cmd)
			if err != nil {
				pipe.closeAndJoin()
				run.finish()
				t.Fatal(err)
			}
			if err = run.setCommand(cmd, wrapped, reader); err != nil {
				_ = reader.Close()
				_ = writer.Close()
				pipe.closeAndJoin()
				run.finish()
				t.Fatal(err)
			}
			if attempt == 1 {
				run.cancel()
			}
			startErr := run.startCommand()
			_ = writer.Close()
			if cmd.Process != nil {
				_ = cmd.Wait()
			}
			_ = reader.Close()
			pipe.closeAndJoin()
			run.finish()
			if startErr == nil {
				t.Fatal("failed/cancelled actual startup unexpectedly succeeded")
			}
			for _, descriptor := range []*os.File{file, pipe.childInput, pipe.writer.(*os.File), reader, writer} {
				if _, err = descriptor.Stat(); !errors.Is(err, os.ErrClosed) {
					t.Fatal("unstarted actual command retained a pipe/input descriptor", err)
				}
			}
			pipe.start()
			pipe.mu.Lock()
			started := pipe.started
			pipe.mu.Unlock()
			if started {
				t.Fatal("closed pipe admitted late copier")
			}
		}
		// Exercise the same real pipe gate with a copier contending with Close.
		file, err := os.Open(media)
		if err != nil {
			t.Fatal(err)
		}
		pipe, err := newShareHLSPipe(exec.Command(ff, "-version"), file, file)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		started := make(chan struct{})
		go func() { pipe.start(); close(started) }()
		pipe.closeAndJoin()
		<-started
		for _, descriptor := range []*os.File{file, pipe.childInput, pipe.writer.(*os.File)} {
			if _, err = descriptor.Stat(); !errors.Is(err, os.ErrClosed) {
				t.Fatal("actual copier start/close left a descriptor open", err)
			}
		}
	})
	t.Run("concurrent-seek-cleanup", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		_, _, gate := ordinaryLifeRun(t, s, media, true)
		seekDone := make(chan struct{})
		cleanDone := make(chan struct{})
		started := false
		t.Cleanup(func() {
			gate.open()
			if started {
				<-seekDone
				<-cleanDone
			}
		})
		go func() {
			m.Seek(httptest.NewRecorder(), httptest.NewRequest("POST", "/?time=1", nil), s.ID)
			close(seekDone)
		}()
		go func() { m.CleanupSession(s.ID); close(cleanDone) }()
		started = true
		ordinaryLifeChannel(t, gate.entered, "concurrent cancellation missed actual source")
		gate.open()
		ordinaryLifeChannel(t, seekDone, "seek remains")
		ordinaryLifeChannel(t, cleanDone, "cleanup remains")
		if ordinaryLifeCharged(m) != 0 {
			t.Fatal("concurrent retirement did not release exact output")
		}
	})
	t.Run("stop-from-worker-no-self-wait", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		g := s.ordinary.current()
		done := make(chan struct{})
		if !ordinaryHLSGo(g.context(), s, func() { g.cancel(); close(done) }) {
			t.Fatal("actual worker denied")
		}
		ordinaryLifeChannel(t, done, "worker cancellation waited for itself")
		m.CleanupSession(s.ID)
	})
}

func TestOrdinaryHLSActualShutdown(t *testing.T) {
	media := ordinaryLifeMedia(t)
	t.Run("closed-manager-denies-new-session", func(t *testing.T) {
		ff, probe := ordinaryLifeTools(t)
		p := &ordinaryLifeProvider{path: media}
		m := NewHLSManager(t.TempDir(), ff, probe, p)
		m.Shutdown()
		s, e := m.CreateSession(t.Context(), "synthetic-source", "synthetic-source", false, "", false, false, 0, 0, -1, -1, "profile", "Synthetic", "127.0.0.1", false, "", "native", 60, "")
		if e == nil || s != nil || p.opens.Load() != 0 {
			t.Fatal("closed actual manager opened new source")
		}
	})
	t.Run("start-versus-shutdown-closed-gate", func(t *testing.T) {
		ff, probe := ordinaryLifeTools(t)
		p := &ordinaryLifeLateProvider{path: media, entered: make(chan context.Context, 1), release: make(chan struct{})}
		m := NewHLSManager(t.TempDir(), ff, probe, p)
		createDone := make(chan struct{})
		shutdownDone := make(chan struct{})
		createStarted, shutdownStarted := false, false
		t.Cleanup(func() {
			p.unblock.Do(func() { close(p.release) })
			if createStarted {
				<-createDone
			}
			m.Shutdown()
			if shutdownStarted {
				<-shutdownDone
			}
		})
		go func() {
			defer close(createDone)
			s, err := m.CreateSession(t.Context(), "synthetic-pending", "synthetic-pending", false, "", false, false, 0, 0, -1, -1, "profile", "Synthetic", "127.0.0.1", false, "", "native", 60, "")
			if s != nil || err == nil {
				t.Error("cancelled genuine pending setup published")
			}
		}()
		createStarted = true
		var actual context.Context
		select {
		case actual = <-p.entered:
		case <-time.After(15 * time.Second):
			t.Fatal("real pending provider did not enter")
		}
		go func() { m.Shutdown(); close(shutdownDone) }()
		shutdownStarted = true
		ordinaryLifeChannel(t, actual.Done(), "original setup cancellation did not reach actual provider")
		select {
		case <-shutdownDone:
			t.Fatal("shutdown returned before late actual body was joined")
		default:
		}
		if ordinaryLifeFDs(media) == 0 || ordinaryLifeCharged(m) != 1 {
			t.Fatal("pending real descriptor missing or charge refunded")
		}
		p.unblock.Do(func() { close(p.release) })
		ordinaryLifeChannel(t, createDone, "late original provider did not join")
		ordinaryLifeChannel(t, shutdownDone, "shutdown did not join setup")
		if ordinaryLifeFDs(media) != 0 || ordinaryLifeCharged(m) != 0 {
			t.Fatal("late actual body not physically closed/refunded")
		}
		m.mu.RLock()
		n := len(m.sessions)
		m.mu.RUnlock()
		if n != 0 {
			t.Fatal("late setup crossed closed publication gate")
		}
	})
	t.Run("repeated-concurrent-shutdown", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, true)
		cmd := ordinaryLifeCommand(t, s)
		protected := filepath.Join(s.OutputDir, "retained-after-real-remove-failure")
		if err := os.WriteFile(protected, []byte("retained original output"), 0600); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(s.OutputDir, 0700); m.CleanupSession(s.ID) })
		if err := os.Chmod(s.OutputDir, 0500); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(4)
		for i := 0; i < 4; i++ {
			go func() { defer wg.Done(); m.Shutdown() }()
		}
		wg.Wait()
		ordinaryLifeGone(t, cmd)
		if ordinaryLifeCharged(m) != 1 {
			t.Fatal("real failed removal refunded its output charge")
		}
		control := m.ordinaryControl()
		control.mu.Lock()
		failure := control.retired[s.ordinary]
		control.mu.Unlock()
		if !errors.Is(failure, os.ErrPermission) {
			t.Fatal("actual EACCES evidence not retained", failure)
		}
		if _, err := os.Stat(protected); err != nil {
			t.Fatal("failed original output disappeared", err)
		}
		if err := os.Chmod(s.OutputDir, 0700); err != nil {
			t.Fatal(err)
		}
		m.CleanupSession(s.ID)
		if ordinaryLifeCharged(m) != 0 {
			t.Fatal("explicit actual cleanup did not refund removed output")
		}
	})
	t.Run("actual-cleanup-loop-joined", func(t *testing.T) {
		ff, probe := ordinaryLifeTools(t)
		m := NewHLSManager(t.TempDir(), ff, probe, &ordinaryLifeProvider{path: media})
		m.Shutdown()
		select {
		case <-m.cleanupExited:
		default:
			t.Fatal("Shutdown returned before actual janitor completion")
		}
		select {
		case <-m.shutdownFinished:
		default:
			t.Fatal("Shutdown completion not published after joins")
		}
	})
	t.Run("held-output-reader-before-removal", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		segment := ordinaryLifeOutput(t, s)
		file, e := os.OpenFile(segment, os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		if e = file.Truncate(16 << 20); e != nil {
			_ = file.Close()
			t.Fatal(e)
		}
		_ = file.Close()
		handlerDone := make(chan struct{})
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerDone)
			m.ServeSegment(w, r, s.ID, filepath.Base(segment))
		}))
		server.Start()
		defer server.Close()
		request, e := http.NewRequestWithContext(t.Context(), "GET", server.URL, nil)
		if e != nil {
			t.Fatal(e)
		}
		transport := &http.Transport{}
		defer transport.CloseIdleConnections()
		client := &http.Client{Timeout: 20 * time.Second, Transport: transport}
		response, e := client.Do(request)
		if e != nil {
			t.Fatal(e)
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
			t.Fatalf("actual segment HTTP %d: %s", response.StatusCode, body)
		}
		ordinaryLifeWait(t, "actual segment descriptor was not held by HTTP body", func() bool { return ordinaryLifeFDs(segment) > 0 })
		if ordinaryLifeCharged(m) != 1 {
			t.Fatal("actual HTTP output lost original charge")
		}
		m.CleanupSession(s.ID)
		_ = response.Body.Close()
		ordinaryLifeChannel(t, handlerDone, "actual cancelled TCP output did not finish")
		if ordinaryLifeFDs(segment) != 0 {
			t.Fatal("output removed/refunded while exact HTTP descriptor remains")
		}
		if _, e = os.Stat(s.OutputDir); !os.IsNotExist(e) || ordinaryLifeCharged(m) != 0 {
			t.Fatal("actual output not removed after join", e)
		}
	})
	t.Run("held-known-input-body-before-refund", func(t *testing.T) {
		m, s, p := ordinaryLifeManager(t, media, false)
		p.mu.Lock()
		p.holdBodies = true
		p.mu.Unlock()
		t.Cleanup(func() {
			p.mu.Lock()
			held := append([]*ordinaryLifeCloseGate(nil), p.held...)
			p.mu.Unlock()
			for _, gate := range held {
				gate.open()
			}
		})
		for i := 0; i < 2; i++ {
			response, err := m.ordinaryProviderStream(s.ordinary.current().context(), streaming.Request{Path: "synthetic-source", Method: http.MethodGet})
			if err != nil {
				t.Fatal(err)
			}
			var b [1]byte
			if _, err = response.Body.Read(b[:]); err != nil {
				t.Fatal(err)
			}
		}
		p.mu.Lock()
		held := append([]*ordinaryLifeCloseGate(nil), p.held...)
		p.mu.Unlock()
		if len(held) != 2 {
			t.Fatal("actual source body registration missing")
		}
		ended := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			for _, gate := range held {
				gate.open()
			}
			if launched {
				<-ended
			}
		})
		go func() { m.Shutdown(); close(ended) }()
		launched = true
		for _, gate := range held {
			ordinaryLifeChannel(t, gate.entered, "all real body Close jobs must enter before one held Close joins")
		}
		if ordinaryLifeFDs(media) != 2 || ordinaryLifeCharged(m) != 1 {
			t.Fatal("actual source descriptors/charge retired before close join")
		}
		select {
		case <-ended:
			t.Fatal("shutdown returned before retained sources closed")
		default:
		}
		for _, gate := range held {
			gate.open()
		}
		ordinaryLifeChannel(t, ended, "shutdown did not join original sources")
		if ordinaryLifeFDs(media) != 0 || ordinaryLifeCharged(m) != 0 {
			t.Fatal("actual source/file cleanup incomplete")
		}
	})
	t.Run("same-id-successor-keeps-distinct-owner", func(t *testing.T) {
		m, s, _ := ordinaryLifeManager(t, media, false)
		old := s.ordinary.current()
		id := s.ID
		w := httptest.NewRecorder()
		m.Seek(w, httptest.NewRequest("POST", "/?time=1", nil), id)
		if w.Code != 200 {
			t.Fatal("actual same-id seek failed", w.Code, w.Body.String())
		}
		next := s.ordinary.current()
		if next == old || next.owner != old.owner || s.ID != id {
			t.Fatal("seek lost exact original owner or failed to retire generation")
		}
		if ordinaryHLSGo(old.context(), s, func() { t.Error("old actor executed") }) {
			t.Fatal("same textual id adopted stale actor")
		}
		m.CleanupSession(id)
	})
	t.Run("runtime-close-before-source-teardown", func(t *testing.T) {
		m, s, p := ordinaryLifeManager(t, media, true)
		cmd := ordinaryLifeCommand(t, s)
		source, e := os.Open(media)
		if e != nil {
			t.Fatal(e)
		}
		defer source.Close()
		m.Shutdown()
		ordinaryLifeGone(t, cmd)
		if _, e = source.Stat(); e != nil {
			t.Fatal("manager closed unrelated caller source descriptor", e)
		}
		if ordinaryLifeCharged(m) != 0 || p.path != media {
			t.Fatal("manager did not finish exact process before caller dependency teardown")
		}
		_ = source.Close()
	})
}

type ordinaryLifeLateProvider struct {
	path    string
	entered chan context.Context
	release chan struct{}
	unblock sync.Once
}

func (p *ordinaryLifeLateProvider) Stream(ctx context.Context, _ streaming.Request) (*streaming.Response, error) {
	file, err := os.Open(p.path)
	if err != nil {
		return nil, err
	}
	select {
	case p.entered <- ctx:
	default:
	}
	<-p.release
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &streaming.Response{Body: file, Status: 200, Headers: make(http.Header), ContentLength: info.Size()}, nil
}

func ordinaryLifeChildPID(t *testing.T, executable string) int {
	t.Helper()
	var result int
	ordinaryLifeWait(t, "actual owned ffprobe ELF was not observed", func() bool {
		threads, _ := os.ReadDir("/proc/self/task")
		for _, thread := range threads {
			data, _ := os.ReadFile(filepath.Join("/proc/self/task", thread.Name(), "children"))
			for _, value := range strings.Fields(string(data)) {
				pid, err := strconv.Atoi(value)
				if err != nil {
					continue
				}
				target, err := os.Readlink(filepath.Join("/proc", value, "exe"))
				if err == nil && target == executable {
					result = pid
					return true
				}
			}
		}
		return false
	})
	return result
}

// A real terminal error is left in the owned kernel pipe until the original
// child has exited. The closed parent writer makes its actual EOF observable;
// neither early reader.Close nor intentional run cancellation supplies bytes.
func ordinaryLifeTerminalStderr(t *testing.T, s *HLSSession) {
	t.Helper()
	ff, _ := ordinaryLifeTools(t)
	ctx, run := newOrdinaryHLSCommand(s.ordinary.current().context(), s)
	missing := filepath.Join(t.TempDir(), "terminal-stderr-witness.mkv")
	cmd := exec.CommandContext(ctx, ff, "-hide_banner", "-loglevel", "error", "-i", missing, "-f", "null", "-")
	reader, writer, err := ordinaryHLSStderr(cmd)
	if err != nil {
		run.finish()
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var unblock sync.Once
	done := make(chan struct{})
	started := false
	var observed []byte
	var readErr error
	t.Cleanup(func() {
		unblock.Do(func() { close(gate) })
		_ = writer.Close()
		run.cancel()
		if started {
			<-done
		}
		_ = reader.Close()
		run.finish()
	})
	if err = run.setCommand(cmd, nil, reader); err != nil {
		t.Fatal(err)
	}
	if !ordinaryHLSGo(ctx, s, func() { defer close(done); <-gate; observed, readErr = io.ReadAll(reader) }) {
		t.Fatal("actual terminal stderr reader denied")
	}
	started = true
	startErr := run.startCommand()
	_ = writer.Close()
	if startErr != nil {
		t.Fatal(startErr)
	}
	waitErr := cmd.Wait()
	if waitErr == nil || ctx.Err() != nil {
		t.Fatal("real missing input must fail with healthy original context", waitErr, ctx.Err())
	}
	// This releases the real reader only after actual child Wait/EOF, so the
	// assertion observes terminal bytes retained in the real owned descriptor.
	unblock.Do(func() { close(gate) })
	ordinaryLifeChannel(t, done, "actual terminal stderr EOF not joined")
	if readErr != nil || !strings.Contains(string(observed), filepath.Base(missing)) || !strings.HasSuffix(string(observed), "\n") {
		t.Fatalf("actual terminal stderr lost: %v %.500s", readErr, observed)
	}
	_ = reader.Close()
	run.cancel()
	run.finish()
	ordinaryLifeGone(t, cmd)
	if _, err = reader.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("actual terminal reader descriptor remains", err)
	}
}
