//go:build linux

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/playback"
)

func preparedVideoWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("actual prepared media work did not join")
	}
}
func preparedVideoFixture(t *testing.T, path string) (*VideoHandler, *ordinaryLifeProvider, context.CancelFunc, string) {
	t.Helper()
	ff, probe := ordinaryLifeTools(t)
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	orphan := filepath.Join(root, "hls", "orphan")
	if e := os.MkdirAll(orphan, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(orphan, "segment0.ts"), []byte("synthetic stale output"), 0600); e != nil {
		t.Fatal(e)
	}
	cache := filepath.Join(root, "strmr-thumbnails", thumbnailSharedSourceCacheDir)
	if e := os.MkdirAll(cache, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(cache, "obsolete"), []byte("synthetic stale sample"), 0600); e != nil {
		t.Fatal(e)
	}
	p := &ordinaryLifeProvider{path: path}
	ctx, cancel := context.WithCancel(t.Context())
	h, e := PrepareVideoHandlerWithProvider(ctx, true, ff, probe, filepath.Join(root, "hls"), p)
	if e != nil {
		cancel()
		t.Fatal(e)
	}
	t.Cleanup(func() { cancel(); h.Shutdown() })
	return h, p, cancel, root
}
func TestPreparedVideoLifecycle(t *testing.T) {
	t.Run("prepare-no-metadata-worker", func(t *testing.T) {
		h, p, _, _ := preparedVideoFixture(t, "")
		done := make(chan struct{})
		go func() { h.mediaLifetime.workers.Wait(); close(done) }()
		preparedVideoWait(t, done)
		if p.opens.Load() != 0 {
			t.Fatal("preparation probed a source")
		}
	})
	t.Run("prepare-no-pool-reaper", func(t *testing.T) {
		h, _, _, _ := preparedVideoFixture(t, "")
		done := make(chan struct{})
		go func() { h.streamPool.reaperWork.Wait(); close(done) }()
		preparedVideoWait(t, done)
		h.streamPool.mu.RLock()
		active := h.streamPool.active
		h.streamPool.mu.RUnlock()
		if active {
			t.Fatal("inactive pool launched reaper")
		}
	})
	t.Run("prepare-no-hls-orphan-delete", func(t *testing.T) {
		_, _, _, root := preparedVideoFixture(t, "")
		if _, e := os.Stat(filepath.Join(root, "hls", "orphan", "segment0.ts")); e != nil {
			t.Fatal("preparation removed original stale output", e)
		}
	})
	t.Run("prepare-no-hls-worker", func(t *testing.T) {
		h, _, _, _ := preparedVideoFixture(t, "")
		c := h.hlsManager.ordinaryControl()
		done := make(chan struct{})
		go func() { c.operations.Wait(); close(done) }()
		preparedVideoWait(t, done)
		if _, e := h.hlsManager.CreateSession(t.Context(), "synthetic", "synthetic", false, "", false, false, 0, 0, -1, -1, "profile", "Synthetic", "127.0.0.1", false, "", "native", 0, ""); e == nil {
			t.Fatal("inactive HLS setup admitted")
		}
	})
	t.Run("prepare-no-subtitle-worker", func(t *testing.T) {
		h, p, _, root := preparedVideoFixture(t, "")
		done := make(chan struct{})
		go func() { h.subtitleExtractManager.workers.Wait(); close(done) }()
		preparedVideoWait(t, done)
		if _, e := os.Stat(filepath.Join(root, "strmr-subtitles")); !os.IsNotExist(e) || p.opens.Load() != 0 {
			t.Fatal("preparation started subtitle work", e)
		}
	})
	t.Run("prepare-no-thumbnail-prune", func(t *testing.T) {
		_, _, _, root := preparedVideoFixture(t, "")
		if _, e := os.Stat(filepath.Join(root, "strmr-thumbnails", thumbnailSharedSourceCacheDir, "obsolete")); e != nil {
			t.Fatal("preparation pruned real cache file", e)
		}
	})
	t.Run("prepare-source-entry-unavailable", func(t *testing.T) {
		h, p, _, _ := preparedVideoFixture(t, "")
		if _, e := h.ProbeVideoFull(t.Context(), "synthetic-source"); e == nil {
			t.Fatal("inactive probe admitted")
		}
		if _, e := h.ProbeVideoPath(t.Context(), "synthetic-source"); e == nil {
			t.Fatal("inactive HDR probe admitted")
		}
		w := httptest.NewRecorder()
		h.StreamVideo(w, httptest.NewRequest(http.MethodGet, "http://synthetic.invalid/video/stream?path=synthetic", nil))
		if w.Code != 503 || p.opens.Load() != 0 {
			t.Fatal("inactive entry reached provider", w.Code, p.opens.Load())
		}
	})
	t.Run("activate-once-original-context", func(t *testing.T) {
		h, _, cancel, root := preparedVideoFixture(t, "")
		control := h.hlsManager.ordinaryControl()
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		if h.ActivatePreparedMedia() == nil {
			t.Fatal("second video activation admitted")
		}
		if h.hlsManager.ordinaryControl() != control {
			t.Fatal("activation replaced original source lifetime")
		}
		if _, e := os.Stat(filepath.Join(root, "hls", "orphan")); !os.IsNotExist(e) {
			t.Fatal("actual activation did not clean stale output", e)
		}
		cancel()
		h.Shutdown()
		select {
		case <-h.hlsManager.cleanupExited:
		default:
			t.Fatal("original runtime janitor not joined")
		}
	})
	t.Run("cancel-before-activation", func(t *testing.T) {
		h, p, cancel, root := preparedVideoFixture(t, "")
		cancel()
		if h.ActivatePreparedMedia() == nil || p.opens.Load() != 0 {
			t.Fatal("cancelled activation started source")
		}
		if _, e := os.Stat(filepath.Join(root, "hls", "orphan")); e != nil {
			t.Fatal("cancelled activation mutated output", e)
		}
	})
	t.Run("hls-unstarted-shutdown", func(t *testing.T) {
		h, _, _, _ := preparedVideoFixture(t, "")
		done := make(chan struct{})
		go func() { h.hlsManager.Shutdown(); close(done) }()
		preparedVideoWait(t, done)
		select {
		case <-h.hlsManager.cleanupExited:
		default:
			t.Fatal("unstarted cleanup did not acknowledge termination")
		}
		if h.hlsManager.activatePreparedMedia() == nil {
			t.Fatal("closed HLS manager restarted")
		}
	})
	t.Run("subtitle-unstarted-shutdown", func(t *testing.T) {
		h, p, _, _ := preparedVideoFixture(t, "")
		h.subtitleExtractManager.Shutdown()
		if h.subtitleExtractManager.activatePreparedMedia() == nil || p.opens.Load() != 0 {
			t.Fatal("closed subtitle worker restarted")
		}
	})
	t.Run("metadata-worker-physically-joined", func(t *testing.T) {
		h, _, _, _ := preparedVideoFixture(t, "")
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		h.Shutdown()
		done := make(chan struct{})
		go func() { h.mediaLifetime.workers.Wait(); close(done) }()
		preparedVideoWait(t, done)
		if h.mediaLifetime.ctx.Err() == nil {
			t.Fatal("metadata worker retained original live context")
		}
	})
	t.Run("pool-reaper-physically-joined", func(t *testing.T) {
		h, _, _, _ := preparedVideoFixture(t, "")
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		h.Shutdown()
		done := make(chan struct{})
		go func() {
			h.streamPool.openWork.Wait()
			h.streamPool.readWork.Wait()
			h.streamPool.reaperWork.Wait()
			close(done)
		}()
		preparedVideoWait(t, done)
	})
	actualChild := func(t *testing.T, concurrent bool) {
		t.Helper()
		media := ordinaryLifeMedia(t)
		h, _, _, _ := preparedVideoFixture(t, media)
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		session, e := h.hlsManager.CreateSession(t.Context(), "synthetic-source", "synthetic-source", false, "", false, false, 0, 0, -1, -1, "synthetic-profile", "Synthetic", "127.0.0.1", false, "", "native", 60, "")
		if e != nil {
			t.Fatal(e)
		}
		ordinaryLifeWait(t, "actual original encoder did not finish", func() bool { session.mu.RLock(); defer session.mu.RUnlock(); return session.Completed })
		cmd, workerDone, gate := ordinaryLifeRun(t, session, media, true)
		done := make(chan struct{})
		t.Cleanup(func() { gate.open(); preparedVideoWait(t, done) })
		go func() {
			if concurrent {
				var wg sync.WaitGroup
				wg.Add(4)
				for n := 0; n < 4; n++ {
					go func() { defer wg.Done(); h.Shutdown() }()
				}
				wg.Wait()
			} else {
				h.Shutdown()
			}
			close(done)
		}()
		ordinaryLifeChannel(t, gate.entered, "actual held source close not observed")
		select {
		case <-done:
			t.Fatal("shutdown refunded before actual descriptor cleanup")
		case <-time.After(20 * time.Millisecond):
		}
		if ordinaryLifeCharged(h.hlsManager) == 0 {
			t.Fatal("held actual output/source owner lost its charge")
		}
		gate.open()
		preparedVideoWait(t, workerDone)
		preparedVideoWait(t, done)
		ordinaryLifeGone(t, cmd)
		if ordinaryLifeFDs(media) != 0 {
			t.Fatal("source descriptor survived actual child and manager join")
		}
	}
	t.Run("actual-child-and-source-before-refund", func(t *testing.T) { actualChild(t, false) })
	t.Run("repeated-concurrent-shutdown", func(t *testing.T) { actualChild(t, true) })
	t.Run("legacy-constructor-unchanged", func(t *testing.T) {
		ff, probe := ordinaryLifeTools(t)
		root := t.TempDir()
		orphan := filepath.Join(root, "orphan")
		if e := os.MkdirAll(orphan, 0700); e != nil {
			t.Fatal(e)
		}
		h := NewVideoHandlerWithProvider(true, ff, probe, root, &ordinaryLifeProvider{})
		t.Cleanup(h.Shutdown)
		if h.preparedMediaCheck() != nil || !h.hlsManager.activated {
			t.Fatal("legacy video no longer activates")
		}
		if _, e := os.Stat(orphan); !os.IsNotExist(e) {
			t.Fatal("legacy constructor skipped orphan cleanup", e)
		}
	})
}

func TestPreparedPrequeueHandlerLifecycle(t *testing.T) {
	fresh := func(t *testing.T) (*PrequeueHandler, context.CancelFunc) {
		t.Helper()
		ctx, cancel := context.WithCancel(t.Context())
		h, e := PreparePrequeueHandler(ctx, nil, nil, nil, nil, nil, false)
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		t.Cleanup(func() { cancel(); h.Shutdown() })
		return h, cancel
	}
	t.Run("prepared-original-store", func(t *testing.T) {
		h, _ := fresh(t)
		s := h.GetStore()
		if s == nil {
			t.Fatal("original store missing")
		}
		if entry, _ := s.Create("synthetic", "Synthetic", "profile", "movie", 2026, nil, "details"); entry != nil {
			t.Fatal("inactive original store admitted work")
		}
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		if h.GetStore() != s {
			t.Fatal("activation replaced original store")
		}
	})
	t.Run("prepared-worker-denied", func(t *testing.T) {
		h, _ := fresh(t)
		if _, e := h.RunWorkerSyncScoped(t.Context(), "title", "Synthetic", "", "movie", 2026, "profile", "", "scope", nil); e == nil {
			t.Fatal("inactive worker admitted")
		}
		w := httptest.NewRecorder()
		h.Prequeue(w, httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/prequeue", nil))
		if w.Code != 503 {
			t.Fatal("inactive public entry read body", w.Code)
		}
	})
	t.Run("actual-activate-store-once", func(t *testing.T) {
		h, _ := fresh(t)
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		s := h.GetStore()
		if e := h.ActivatePreparedMedia(); e != nil || h.GetStore() != s {
			t.Fatal("activation did not retain same store", e)
		}
		entry, created := s.Create("synthetic", "Synthetic", "profile", "movie", 2026, nil, "details")
		if entry == nil || !created {
			t.Fatal("activated original store unavailable")
		}
	})
	t.Run("unstarted-shutdown", func(t *testing.T) {
		h, _ := fresh(t)
		h.Shutdown()
		if h.workerLife.runtime.Err() == nil || h.ActivatePreparedMedia() == nil {
			t.Fatal("unstarted handler survived shutdown")
		}
		if entry, _ := h.GetStore().Create("late", "Late", "profile", "movie", 2026, nil, "details"); entry != nil {
			t.Fatal("original store outlived handler")
		}
	})
	t.Run("original-runtime-cancel", func(t *testing.T) {
		g := newPrequeueHTTPGate(t)
		h, cancel := fresh(t)
		h.movieMetadataSvc = g
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		var workerDone <-chan struct{}
		var result *error
		var shutdownDone chan struct{}
		t.Cleanup(func() {
			cancel()
			g.unblock()
			if workerDone != nil {
				preparedVideoWait(t, workerDone)
			}
			if shutdownDone != nil {
				preparedVideoWait(t, shutdownDone)
			}
		})
		workerDone, result = prequeueLifeSync(t, h, t.Context())
		prequeueLifeWait(t, g.entered)
		cancel()
		prequeueLifeWait(t, g.closing)
		shutdownDone = make(chan struct{})
		go func() { h.Shutdown(); close(shutdownDone) }()
		prequeueLifePending(t, shutdownDone)
		g.unblock()
		preparedVideoWait(t, workerDone)
		preparedVideoWait(t, shutdownDone)
		if *result == nil {
			t.Fatal("retired actual worker reported ready")
		}
	})
	t.Run("handler-shutdown-joins-store", func(t *testing.T) {
		g := newPrequeueHTTPGate(t)
		h, _ := fresh(t)
		if e := h.ActivatePreparedMedia(); e != nil {
			t.Fatal(e)
		}
		store := h.GetStore()
		root := t.TempDir()
		store.SetStoragePath(root)
		entry := playback.PrequeueEntry{ID: "restored", TitleID: "synthetic", UserID: "profile", Status: playback.PrequeueStatusReady, StreamPath: "synthetic-source.mkv", CreatedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
		data, e := json.Marshal([]playback.PrequeueEntry{entry})
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(root, "prequeue.json"), data, 0600); e != nil {
			t.Fatal(e)
		}
		if e = store.RestorePersisted(t.Context()); e != nil {
			t.Fatal(e)
		}
		store.SetStreamPathValidator(func(ctx context.Context, _ string) error {
			_, e := g.MovieInfo(ctx, models.MovieDetailsQuery{})
			return e
		})
		done := make(chan struct{})
		shutdownDone := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			g.unblock()
			preparedVideoWait(t, done)
			if launched {
				preparedVideoWait(t, shutdownDone)
			}
		})
		go func() { _, _ = store.GetForPlayback("restored"); close(done) }()
		prequeueLifeWait(t, g.entered)
		launched = true
		go func() { h.Shutdown(); close(shutdownDone) }()
		prequeueLifeWait(t, g.closing)
		prequeueLifePending(t, shutdownDone)
		g.unblock()
		preparedVideoWait(t, done)
		preparedVideoWait(t, shutdownDone)
	})
}
