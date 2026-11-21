//go:build linux

package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/livecapacity"
)

// Actual legacy CreateLiveSession publication is used below. Its ownership is
// not a positive protected/child/source grant; configured unknown Live remains
// denied. The private supervisor receipt proves only that actual outer actor.
type liveLifeHTTP struct {
	requests, active atomic.Int32
	ended            chan struct{}
}

func liveLifeMedia(t *testing.T) []byte {
	t.Helper()
	ff, _ := ordinaryLifeTools(t)
	path := filepath.Join(t.TempDir(), "channel.ts")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ff, "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=64x64:r=5", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "6", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-g", "10", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "32k", "-f", "mpegts", path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual synthetic TS: %v %.1000s", err, output)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func liveLifeHTTPServer(t *testing.T, data []byte, stall bool) (*liveLifeHTTP, *httptest.Server) {
	t.Helper()
	state := &liveLifeHTTP{ended: make(chan struct{}, 128)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state.requests.Add(1)
		state.active.Add(1)
		defer func() {
			state.active.Add(-1)
			select {
			case state.ended <- struct{}{}:
			default:
			}
		}()
		w.Header().Set("Content-Type", "video/mp2t")
		flusher := w.(http.Flusher)
		if stall {
			_, _ = w.Write(tsPacket(1))
			flusher.Flush()
			<-r.Context().Done()
			return
		}
		for {
			for offset := 0; offset < len(data); offset += 188 * 40 {
				end := offset + 188*40
				if end > len(data) {
					end = len(data)
				}
				if _, err := w.Write(data[offset:end]); err != nil {
					return
				}
				flusher.Flush()
				select {
				case <-r.Context().Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
	}))
	t.Cleanup(server.Close)
	return state, server
}
func liveLifeCreate(t *testing.T, m *HLSManager, url, bucket string) *HLSSession {
	t.Helper()
	s, err := m.CreateLiveSession(t.Context(), url, "m3u", bucket, "synthetic-profile", "Synthetic", "127.0.0.1", "native", LiveTuningSettings{MaxStreams: 16}, false)
	if err != nil {
		t.Fatal("actual live publication failed", err)
	}
	if s.liveSupervisorDone == nil || s.vodIdentity == nil || s.vodIdentity.session != s {
		t.Fatal("genuine original publication/supervisor observation missing")
	}
	t.Cleanup(func() {
		m.CleanupSession(s.ID)
		liveLifeAwait(t, s.liveSupervisorDone, "original outer live supervisor not joined during fixture cleanup")
	})
	return s
}
func liveLifeManager(t *testing.T, url string) (*HLSManager, *HLSSession) {
	t.Helper()
	ff, probe := ordinaryLifeTools(t)
	m := NewHLSManager(t.TempDir(), ff, probe, nil)
	t.Cleanup(m.Shutdown)
	return m, liveLifeCreate(t, m, url, t.Name())
}
func liveLifeOriginal(t *testing.T, m *HLSManager, s *HLSSession) *liveUpstream {
	t.Helper()
	var result *liveUpstream
	ordinaryLifeWait(t, "actual original producer not registered", func() bool {
		s.mu.RLock()
		key := liveUpstreamShareKey(s.Path, s.LiveTuning)
		s.mu.RUnlock()
		sharedLiveUpstreams.mu.Lock()
		result = sharedLiveUpstreams.streams[key]
		sharedLiveUpstreams.mu.Unlock()
		if result == nil {
			return false
		}
		select {
		case <-result.ready:
			return result.openErr == nil
		default:
			return false
		}
	})
	if result.control != m.ordinaryControl() {
		t.Fatal("producer lost actual first manager root")
	}
	return result
}
func liveLifeManagerRefs(m *HLSManager) int {
	c := m.ordinaryControl()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.liveReferences
}
func liveLifeOriginalDone(t *testing.T, u *liveUpstream) {
	t.Helper()
	liveLifeAwait(t, u.done, "original source read/body/watchdog not physically joined")
	u.mu.Lock()
	body, ended := u.body, u.ended
	u.mu.Unlock()
	if body != nil || !ended {
		t.Fatal("source completion retained actual body/pump")
	}
}

func TestLiveUpstreamActualRuntimeClosure(t *testing.T) {
	media := liveLifeMedia(t)
	t.Run("original-manager-closed-gate-before-open", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/original.ts")
		u := liveLifeOriginal(t, m, s)
		m.Shutdown()
		liveLifeOriginalDone(t, u)
		before := state.requests.Load()
		sub, _, err := m.subscribeOriginalLiveUpstream(t.Context(), s)
		if err == nil || sub != nil || state.requests.Load() != before || liveLifeManagerRefs(m) != 0 {
			t.Fatal("closed original manager admitted new source/reference")
		}
	})
	t.Run("manager-shutdown-joins-actual-http-body", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, true)
		m, s := liveLifeManager(t, server.URL+"/silent.ts")
		u := liveLifeOriginal(t, m, s)
		if state.active.Load() != 1 {
			t.Fatal("actual TCP body was not open")
		}
		m.Shutdown()
		liveLifeOriginalDone(t, u)
		liveLifeAwait(t, s.liveSupervisorDone, "original outer actor remains after source shutdown")
		ordinaryLifeWait(t, "original loopback TCP handler did not observe disconnect", func() bool { return state.active.Load() == 0 })
		if liveLifeManagerRefs(m) != 0 {
			t.Fatal("actual completed source/viewer reference retained")
		}
	})
	t.Run("original-source-owner-not-renewed-by-fresh-viewer", func(t *testing.T) {
		_, server := liveLifeHTTPServer(t, media, false)
		first, a := liveLifeManager(t, server.URL+"/same.ts")
		u := liveLifeOriginal(t, first, a)
		originalRoot, originalControl := u.ctx, u.control
		second, b := liveLifeManager(t, server.URL+"/same.ts")
		ordinaryLifeWait(t, "second genuine manager viewer not attached", func() bool { return sharedLiveUpstreams.Subscribers(liveUpstreamShareKey(b.Path, b.LiveTuning)) >= 2 })
		if u.ctx != originalRoot || u.control != originalControl || u.control == second.ordinaryControl() {
			t.Fatal("fresh viewer substituted original producer root")
		}
		first.Shutdown()
		liveLifeOriginalDone(t, u)
		if originalRoot.Err() == nil {
			t.Fatal("original first manager shutdown renewed source through another viewer")
		}
		second.Shutdown()
	})
	t.Run("same-key-different-original-is-not-adoption", func(t *testing.T) {
		_, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/same.ts")
		original := liveLifeOriginal(t, m, s)
		ctx, cancel := context.WithCancel(t.Context())
		extra, shared, err := m.subscribeOriginalLiveUpstream(ctx, s)
		if err != nil || !shared {
			t.Fatal("genuine current subscriber denied", err)
		}
		t.Cleanup(func() { cancel(); _ = extra.Close() })
		if extra.upstream != original {
			t.Fatal("same key replaced original source")
		}
		copyData := &HLSSession{ID: s.ID, Path: s.Path, IsLive: true}
		if fake, _, err := m.subscribeOriginalLiveUpstream(t.Context(), copyData); err == nil || fake != nil {
			t.Fatal("textual key/session DATA adopted current producer")
		}
		cancel()
		liveLifeAwait(t, extra.closed, "fresh viewer did not retire its own original reference")
		if original.ctx.Err() != nil {
			t.Fatal("fresh viewer cancellation killed original source")
		}
	})
	t.Run("two-current-viewers-keep-one-original-upstream", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/shared.ts")
		original := liveLifeOriginal(t, m, s)
		second := liveLifeCreate(t, m, server.URL+"/shared.ts", t.Name())
		ordinaryLifeWait(t, "two actual sessions not subscribed", func() bool { return sharedLiveUpstreams.Subscribers(liveUpstreamShareKey(s.Path, s.LiveTuning)) == 2 })
		if state.requests.Load() != 1 {
			t.Fatal("two current viewers opened multiple original HTTP sources")
		}
		m.CleanupSession(second.ID)
		liveLifeAwait(t, second.liveSupervisorDone, "second outer supervisor not joined")
		if original.ctx.Err() != nil || state.active.Load() != 1 {
			t.Fatal("nonfinal viewer close retired source still held by original viewer")
		}
	})
	t.Run("new-original-after-completed-old-retirement", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, false)
		first, a := liveLifeManager(t, server.URL+"/again.ts")
		old := liveLifeOriginal(t, first, a)
		first.Shutdown()
		liveLifeOriginalDone(t, old)
		ordinaryLifeWait(t, "old TCP provider request remains", func() bool { return state.active.Load() == 0 })
		second, b := liveLifeManager(t, server.URL+"/again.ts")
		fresh := liveLifeOriginal(t, second, b)
		if fresh == old || fresh.control != second.ordinaryControl() || state.requests.Load() != 2 {
			t.Fatal("new genuine source adopted retired producer/key")
		}
	})
	t.Run("repeated-concurrent-last-close", func(t *testing.T) {
		_, server := liveLifeHTTPServer(t, media, true)
		m, s := liveLifeManager(t, server.URL+"/close.ts")
		u := liveLifeOriginal(t, m, s)
		var wg sync.WaitGroup
		wg.Add(4)
		for i := 0; i < 4; i++ {
			go func() { defer wg.Done(); m.Shutdown() }()
		}
		wg.Wait()
		liveLifeOriginalDone(t, u)
		if liveLifeManagerRefs(m) != 0 {
			t.Fatal("concurrent shutdown retained/refunded the same reference twice")
		}
	})
	t.Run("bounded-pending-and-active-admission", func(t *testing.T) {
		_, server := liveLifeHTTPServer(t, media, true)
		m, first := liveLifeManager(t, server.URL+"/0.ts")
		liveLifeOriginal(t, m, first)
		sessions := []*HLSSession{first}
		for i := 1; i < 4; i++ {
			s := liveLifeCreate(t, m, server.URL+"/"+string(rune('0'+i))+".ts", t.Name())
			liveLifeOriginal(t, m, s)
			sessions = append(sessions, s)
		}
		extras := make([]*liveUpstreamSubscriber, 0, 60)
		t.Cleanup(func() {
			for _, sub := range extras {
				_ = sub.Close()
			}
		})
		for _, s := range sessions {
			for i := 1; i < liveUpstreamSubscriberLimit; i++ {
				sub, _, err := m.subscribeOriginalLiveUpstream(t.Context(), s)
				if err != nil {
					t.Fatal("actual bounded reference unexpectedly denied", err)
				}
				extras = append(extras, sub)
			}
		}
		if liveLifeManagerRefs(m) != liveUpstreamManagerReferenceLimit {
			t.Fatal("actual 64-reference budget not charged")
		}
		if extra, _, err := m.subscribeOriginalLiveUpstream(t.Context(), first); !errors.Is(err, livecapacity.ErrLimit) || extra != nil {
			t.Fatal("actual manager reference bound not enforced", err)
		}
		// A waiting same-key operation also consumes the SAME actual budget. Its
		// blocker is a real old source held in Close, not a synthetic pending flag.
		for _, sub := range extras {
			_ = sub.Close()
		}
		extras = nil
		isolated := newTestLiveUpstreamHub()
		held := liveLifePipe(t, true)
		key := liveUpstreamShareKey(first.Path, first.LiveTuning)
		old := liveLifeSubscribe(t, isolated, key, held)
		retired := make(chan struct{})
		waitDone := make(chan struct{})
		closeStarted, waitStarted := false, false
		ctx, cancel := context.WithCancel(t.Context())
		baseline := liveLifeManagerRefs(m)
		t.Cleanup(func() {
			cancel()
			held.unblock()
			if closeStarted {
				<-retired
			}
			if waitStarted {
				<-waitDone
			}
		})
		go func() { _ = old.Close(); close(retired) }()
		closeStarted = true
		liveLifeAwait(t, held.entered, "real retiring source Close not held")
		go func() {
			defer close(waitDone)
			sub, _, err := isolated.subscribe(ctx, m, first, "", "", liveUpstreamSource{})
			if sub != nil || err == nil {
				if sub != nil {
					_ = sub.Close()
				}
				t.Error("cancelled original pending wait was adopted")
			}
		}()
		waitStarted = true
		ordinaryLifeWait(t, "actual pending retiring-source wait did not charge manager", func() bool { return liveLifeManagerRefs(m) == baseline+1 })
		liveLifePending(t, waitDone, "pending source returned before original cancellation")
		cancel()
		liveLifeAwait(t, waitDone, "original pending wait was not physically retired")
		if liveLifeManagerRefs(m) != baseline {
			t.Fatal("pending wait reference not refunded exactly at actual end")
		}
		held.unblock()
		liveLifeAwait(t, retired, "old real source Close not joined")
	})
}

func TestLiveUpstreamActualHLSCanaries(t *testing.T) {
	media := liveLifeMedia(t)
	t.Run("genuine-published-live-session-binds-original-subscriber", func(t *testing.T) {
		_, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/publish.ts")
		u := liveLifeOriginal(t, m, s)
		m.mu.RLock()
		current := m.sessions[s.ID]
		m.mu.RUnlock()
		if current != s || u.control != m.ordinaryControl() || liveLifeManagerRefs(m) != 1 {
			t.Fatal("actual server publication did not bind original source/reference")
		}
		liveLifeOutput(t, s)
	})
	t.Run("real-ts-http-body-and-stdin-copy-close", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/pipe.ts")
		u := liveLifeOriginal(t, m, s)
		cmd := ordinaryLifeCommand(t, s)
		liveLifeOutput(t, s)
		m.CleanupSession(s.ID)
		liveLifeAwait(t, s.liveSupervisorDone, "actual original outer supervisor did not end")
		liveLifeOriginalDone(t, u)
		ordinaryLifeGone(t, cmd)
		ordinaryLifeWait(t, "real original provider TCP body remains", func() bool { return state.active.Load() == 0 })
	})
	t.Run("original-live-failover-waits-source-retirement", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		oldBody, newBody := liveLifePipe(t, true), liveLifePipe(t, false)
		sub := liveLifeSubscribe(t, h, "source-before-failover", oldBody)
		done := make(chan struct{})
		started := false
		t.Cleanup(func() {
			oldBody.unblock()
			if started {
				<-done
			}
		})
		go func() { _ = sub.Close(); close(done) }()
		started = true
		liveLifeAwait(t, oldBody.entered, "actual old candidate not retained in Close")
		liveLifePending(t, sub.upstream.done, "candidate producer retired before body physically closed")
		oldBody.unblock()
		liveLifeAwait(t, done, "old candidate failed to join")
		next := liveLifeSubscribe(t, h, "next-source-candidate", newBody)
		liveLifeRead(t, next)
		if next.upstream == sub.upstream {
			t.Fatal("next source adopted old candidate actor")
		}
	})
	t.Run("nonfinal-viewer-close-keeps-other-viewer-current", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/viewer.ts")
		u := liveLifeOriginal(t, m, s)
		extra, _, err := m.subscribeOriginalLiveUpstream(t.Context(), s)
		if err != nil {
			t.Fatal(err)
		}
		_ = extra.Close()
		if u.ctx.Err() != nil || state.requests.Load() != 1 || state.active.Load() != 1 || liveLifeManagerRefs(m) != 1 {
			t.Fatal("nonfinal physical reader ended the other current viewer")
		}
		liveLifeOutput(t, s)
	})
	t.Run("copied-session-data-cannot-open-upstream", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, false)
		m, s := liveLifeManager(t, server.URL+"/copy.ts")
		liveLifeOriginal(t, m, s)
		before := state.requests.Load()
		copyData := &HLSSession{ID: s.ID, IsLive: true, Path: s.Path, LiveTuning: s.LiveTuning}
		if sub, _, err := m.subscribeOriginalLiveUpstream(t.Context(), copyData); err == nil || sub != nil {
			t.Fatal("copied original DATA opened source")
		}
		if state.requests.Load() != before || liveLifeManagerRefs(m) != 1 {
			t.Fatal("copied original changed actual source/ref budget")
		}
	})
	t.Run("manager-cleanup-cannot-refund-live-source-early", func(t *testing.T) {
		state, server := liveLifeHTTPServer(t, media, true)
		m, s := liveLifeManager(t, server.URL+"/held-read.ts")
		u := liveLifeOriginal(t, m, s)
		liveLifePending(t, u.done, "actual silent HTTP read already ended")
		if liveLifeManagerRefs(m) != 1 || state.active.Load() != 1 {
			t.Fatal("actual held HTTP source was not charged")
		}
		m.Shutdown()
		liveLifeOriginalDone(t, u)
		liveLifeAwait(t, s.liveSupervisorDone, "original outer consumer remains")
		if liveLifeManagerRefs(m) != 0 {
			t.Fatal("actual joined source did not refund viewer reference")
		}
	})
	t.Run("hls-playlist-fallback-stays-explicitly-unconfined", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
			_, _ = io.WriteString(w, "#EXTM3U\n#EXT-X-VERSION:3\n")
		}))
		defer server.Close()
		h := newTestLiveUpstreamHub()
		sub, _, err := h.Subscribe("playlist", "synthetic", openLiveUpstream(liveTestSession(server.URL+"/live")))
		if sub != nil || !errors.Is(err, errLiveUpstreamNotTS) || requests.Load() != 1 || liveLifeCount(h) != 0 {
			t.Fatal("raw TS lifetime falsely certified a nested playlist", err)
		}
		if liveUpstreamShareKey(server.URL+"/live.m3u8", LiveTuningSettings{}) != "" {
			t.Fatal("known nested HLS became raw TS source")
		}
	})
	t.Run("managed-unknown-live-remains-denied-before-work", func(t *testing.T) {
		f := newPublicPinCarrierFixture(t)
		installed := familyActualPGInstall(t, f)
		ff, probe := ordinaryLifeTools(t)
		poison := &rootHLSInputPoisonProvider{}
		handler := NewVideoHandlerWithProvider(true, ff, probe, t.TempDir(), poison)
		defer handler.Shutdown()
		if err := handler.installHLSInputBoundary(installed.Boundary.boundary); err != nil {
			t.Fatal(err)
		}
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); _, _ = w.Write(tsPacket(1)) }))
		defer server.Close()
		s, err := handler.hlsManager.CreateLiveSession(t.Context(), server.URL+"/unknown.ts", "m3u", t.Name(), f.profile.ID, "Synthetic", "127.0.0.1", "native", LiveTuningSettings{}, false)
		if s != nil || err == nil || requests.Load() != 0 || poison.streams.Load() != 0 || liveLifeManagerRefs(handler.hlsManager) != 0 {
			t.Fatal("lifetime DATA enabled an unknown managed Live source")
		}
	})
}

func liveLifeOutput(t *testing.T, s *HLSSession) string {
	t.Helper()
	var result string
	ordinaryLifeWait(t, "actual live segment/playlist missing", func() bool {
		s.mu.RLock()
		dir := s.OutputDir
		s.mu.RUnlock()
		data, err := os.ReadFile(filepath.Join(dir, "stream.m3u8"))
		if err != nil {
			return false
		}
		ext := resolveSegmentExt(s, strings.Split(string(data), "\n"))
		if ext != ".ts" && ext != ".m4s" && ext != ".mp4" {
			return false
		}
		result = filepath.Join(dir, "segment0"+ext)
		info, err := os.Stat(result)
		return err == nil && info.Mode().IsRegular() && info.Size() > 0
	})
	return result
}
