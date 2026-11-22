package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newHLSLifecycleManager(t *testing.T) *HLSManager {
	t.Helper()
	m := NewHLSManager(t.TempDir(), "", "", nil)
	t.Cleanup(m.Shutdown)
	return m
}
func lifecycleHLSSession(t *testing.T, m *HLSManager, id string) *HLSSession {
	t.Helper()
	claim, err := m.claimHLSSessionDirectory(id)
	if err != nil {
		t.Fatal(err)
	}
	return &HLSSession{ID: id, ProfileID: "profile", ClientID: "device", Path: "debrid/synthetic/input.mkv", OutputDir: claim.path, vodDirectory: claim, CreatedAt: time.Now(), StreamStartTime: time.Now(), LastAccess: time.Now().Add(-time.Hour)}
}
func TestHLSBoundarySnapshotIsPurePrivateAndGenerationBound(t *testing.T) {
	m := newHLSLifecycleManager(t)
	s := lifecycleHLSSession(t, m, "pure-snapshot")
	if !m.publishHLSSession(s) {
		t.Fatal("normal publication failed")
	}
	before := s.LastAccess
	for i := 0; i < 20; i++ {
		snap, ok := m.snapshotHLSSession(s.ID)
		if !ok || !snap.valid() || !snap.current() {
			t.Fatal("current private snapshot missing")
		}
	}
	if !s.LastAccess.Equal(before) {
		t.Fatal("pure peek touched LastAccess")
	}
	snap, _ := m.snapshotHLSSession(s.ID)
	changed := snap
	changed.profile = "foreign"
	if changed.valid() || changed.current() {
		t.Fatal("copied public owner claim became authoritative")
	}
	data, err := json.Marshal(snap)
	if err != nil || string(data) != "null" {
		t.Fatal("private snapshot serialized")
	}
	var imported hlsSessionSnapshot
	if json.Unmarshal([]byte(`{"id":"pure-snapshot"}`), &imported) == nil || imported.valid() || imported.current() {
		t.Fatal("imported snapshot became authoritative")
	}
	s.mu.Lock()
	s.SeekGeneration++
	s.mu.Unlock()
	if snap.current() || m.cleanupHLSSessionSnapshot(snap) {
		t.Fatal("old generation could stop current session")
	}
	if _, err := os.Stat(s.OutputDir); err != nil {
		t.Fatal("stale cleanup removed current directory")
	}
}
func TestHLSBoundaryDirectoryClaimsArePrivateOneShot(t *testing.T) {
	m := newHLSLifecycleManager(t)
	other := newHLSLifecycleManager(t)
	s := lifecycleHLSSession(t, m, "one-shot")
	if other.publishHLSSession(s) {
		t.Fatal("another manager accepted private claim")
	}
	if !m.publishHLSSession(s) {
		t.Fatal("own claim rejected")
	}
	duplicate := &HLSSession{ID: s.ID, ProfileID: s.ProfileID, Path: s.Path, OutputDir: s.OutputDir, vodDirectory: s.vodDirectory}
	if m.publishHLSSession(duplicate) || m.publishHLSSession(s) {
		t.Fatal("claim published twice")
	}
	m.discardHLSSessionDirectory(s.vodDirectory)
	if _, err := os.Stat(s.OutputDir); err != nil {
		t.Fatal("discard removed published directory")
	}
	claim, err := m.claimHLSSessionDirectory("unpublished")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(claim.path, "private"), []byte("SYNTHETIC"), 0600); err != nil {
		t.Fatal(err)
	}
	m.discardHLSSessionDirectory(claim)
	m.discardHLSSessionDirectory(claim)
	if _, err := os.Stat(claim.path); !os.IsNotExist(err) {
		t.Fatal("unpublished directory not discarded")
	}
	if m.publishHLSSession(&HLSSession{ID: claim.id, ProfileID: "profile", Path: "synthetic", OutputDir: claim.path, vodDirectory: claim}) {
		t.Fatal("discarded claim revived")
	}
	for _, id := range []string{"", "../foreign", "bad/name", `bad\name`, " spaced ", strings.Repeat("x", 129)} {
		if claim, err := m.claimHLSSessionDirectory(id); err == nil || claim != nil {
			t.Fatalf("invalid directory identity accepted %q", id)
		}
	}
}
func TestHLSBoundaryCleanupPreservesPrepublicationReplacementFiles(t *testing.T) {
	m := newHLSLifecycleManager(t)
	old := lifecycleHLSSession(t, m, "same-id")
	if !m.publishHLSSession(old) {
		t.Fatal("publish old")
	}
	snap, _ := m.snapshotHLSSession(old.ID)
	replacement := lifecycleHLSSession(t, m, old.ID)
	if old.OutputDir == replacement.OutputDir {
		t.Fatal("same ID shared physical output directory")
	}
	path := filepath.Join(replacement.OutputDir, "segment0.ts")
	if err := os.WriteFile(path, []byte("SYNTHETIC-NEW-BYTES"), 0600); err != nil {
		t.Fatal(err)
	}
	// Cancellation callbacks run after detach and may prepare/publish a successor.
	// Cleanup must retain the originally captured path rather than reread it.
	old.Cancel = func() {
		old.mu.Lock()
		old.OutputDir = replacement.OutputDir
		old.mu.Unlock()
		if !m.publishHLSSession(replacement) {
			t.Error("replacement publication rejected")
		}
	}
	if !m.cleanupHLSSessionSnapshot(snap) {
		t.Fatal("exact cleanup rejected")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "SYNTHETIC-NEW-BYTES" {
		t.Fatal("old cleanup removed precreated replacement bytes")
	}
	if _, err := os.Stat(snap.output); !os.IsNotExist(err) {
		t.Fatal("old directory retained")
	}
	if now, ok := m.snapshotHLSSession(old.ID); !ok || now.session != replacement || !now.valid() {
		t.Fatal("replacement not current")
	}
	if m.cleanupHLSSessionSnapshot(snap) {
		t.Fatal("late cleanup accepted replacement")
	}
}

type hlsCancelledSetupProvider struct {
	hlsTestProvider
	cancel   context.CancelFunc
	manager  *HLSManager
	observed bool
}

func (p *hlsCancelledSetupProvider) GetDuration(context.Context, string) (float64, error) {
	entries, err := os.ReadDir(p.manager.baseDir)
	p.observed = err == nil && len(entries) == 1
	p.cancel()
	return 0, context.Canceled
}
func TestHLSBoundaryCancelledNormalSetupDiscardsItsUnpublishedDirectory(t *testing.T) {
	m := newHLSLifecycleManager(t)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		p := &hlsCancelledSetupProvider{cancel: cancel, manager: m}
		m.streamer = p
		session, err := m.CreateSession(ctx, "synthetic/input", "synthetic/input", false, "", false, false, 0, 0, -1, -1, "profile", "", "", false, "", "", 0, "")
		cancel()
		if !p.observed || !errors.Is(err, context.Canceled) || session != nil {
			t.Fatalf("did not exercise post-claim cancelled setup: observed=%v error=%v", p.observed, err)
		}
		entries, err := os.ReadDir(m.baseDir)
		if err != nil || len(entries) != 0 {
			t.Fatal("cancelled setup leaked a unique output directory")
		}
	}
}
func TestHLSBoundaryConfiguredUnsupportedInputBranchesDoNotStartWork(t *testing.T) {
	for _, kind := range []string{"missing subtitle", "stable init", "CC", "seek"} {
		t.Run(kind, func(t *testing.T) {
			f := newRootHLSSessionFixture(t, vodAccountActive, false)
			w := httptest.NewRecorder()
			r := f.request("segment")
			switch kind {
			case "missing subtitle":
				f.handler.ServeHLSSubtitles(w, f.request("subtitles"))
			case "CC":
				f.handler.GetHLSLiveCCStatus(w, f.request("status"))
			case "seek":
				f.handler.SeekHLSSession(w, f.request("seek"))
			case "stable init":
				f.session.mu.Lock()
				f.session.StableCopy = true
				f.session.CopySegmentStarts = []float64{0, 4}
				f.session.CopyPlan.FMP4 = true
				f.session.mu.Unlock()
				checked, request, finish, ok := f.handler.beginHLSVOD(w, r, f.session.ID, false)
				if !ok {
					t.Fatal("adult owner rejected")
				}
				defer finish()
				f.manager.ServeSegment(checked, request, f.session.ID, "init.mp4")
			}
			if w.Code != http.StatusForbidden {
				t.Fatalf("unsupported input branch status=%d", w.Code)
			}
			f.session.mu.RLock()
			cmd := f.session.FFmpegCmd
			f.session.mu.RUnlock()
			f.session.subtitleExtractionMu.Lock()
			extracting := len(f.session.subtitleExtracting)
			f.session.subtitleExtractionMu.Unlock()
			if cmd != nil || extracting != 0 {
				t.Fatal("configured unsupported branch started input work")
			}
		})
	}
}

func TestHLSBoundaryFailedPublishedYouTubeSetupRemovesOnlyItsDirectory(t *testing.T) {
	m := newHLSLifecycleManager(t)
	sibling := lifecycleHLSSession(t, m, "surviving-session")
	if !m.publishHLSSession(sibling) {
		t.Fatal("publish sibling")
	}
	path := filepath.Join(sibling.OutputDir, "segment0.ts")
	if err := os.WriteFile(path, []byte("SYNTHETIC-SIBLING"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		// Missing FFmpeg fails before opening either synthetic input. Empty legacy
		// profile still requires exact-instance cleanup, not a readable HTTP grant.
		session, err := m.CreateYouTubeSession(context.Background(), "synthetic-video", "synthetic-audio", "synthetic-original", "", "", "", "")
		if err == nil || session != nil {
			t.Fatal("unconfigured FFmpeg unexpectedly started")
		}
		entries, err := os.ReadDir(m.baseDir)
		if err != nil || len(entries) != 1 || filepath.Join(m.baseDir, entries[0].Name()) != sibling.OutputDir {
			t.Fatal("failed published setup leaked a unique directory or removed sibling")
		}
		if data, err := os.ReadFile(path); err != nil || string(data) != "SYNTHETIC-SIBLING" {
			t.Fatal("failed startup removed unrelated owned bytes")
		}
		m.mu.RLock()
		count := len(m.sessions)
		m.mu.RUnlock()
		if count != 1 {
			t.Fatal("failed setup retained runtime session")
		}
	}
}
func TestHLSBoundaryAlreadyCancelledCreatorsAllocateNothing(t *testing.T) {
	for _, kind := range []string{"VOD", "YouTube", "live"} {
		t.Run(kind, func(t *testing.T) {
			m := newHLSLifecycleManager(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			var s *HLSSession
			var err error
			switch kind {
			case "VOD":
				s, err = m.CreateSession(ctx, "synthetic", "synthetic", false, "", false, false, 0, 0, -1, -1, "profile", "", "", false, "", "", 0, "")
			case "YouTube":
				s, err = m.CreateYouTubeSession(ctx, "video", "audio", "original", "", "profile", "", "")
			case "live":
				s, err = m.CreateLiveSession(ctx, "synthetic", "m3u", "synthetic", "profile", "", "", "", LiveTuningSettings{}, false)
			}
			if !errors.Is(err, context.Canceled) || s != nil {
				t.Fatal("cancelled creator did not stop")
			}
			entries, err := os.ReadDir(m.baseDir)
			if err != nil || len(entries) != 0 {
				t.Fatal("cancelled creator allocated directory")
			}
		})
	}
}

func TestHLSBoundaryExpiredLiveCreatorAllocatesNothing(t *testing.T) {
	m := newHLSLifecycleManager(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	session, err := m.CreateLiveSession(ctx, "synthetic", "m3u", "synthetic", "profile", "", "", "", LiveTuningSettings{}, false)
	if !errors.Is(err, context.DeadlineExceeded) || session != nil {
		t.Fatalf("expired creator returned session=%v error=%v", session, err)
	}
	entries, err := os.ReadDir(m.baseDir)
	if err != nil || len(entries) != 0 {
		t.Fatal("expired creator allocated directory")
	}
	m.mu.RLock()
	count := len(m.sessions)
	m.mu.RUnlock()
	if count != 0 {
		t.Fatal("expired creator published a session")
	}
}

func TestHLSBoundaryRevokedRuntimeRemainsOwnedCleanupOnly(t *testing.T) {
	f := newRootHLSSessionFixture(t, vodAccountActive, false)
	revokeLiveSession(f.session)
	before := f.session.LastAccess
	w := httptest.NewRecorder()
	f.invoke("segment", w, f.request("segment"))
	if w.Code == http.StatusOK || strings.Contains(w.Body.String(), f.payload) {
		t.Fatal("revoked session remained readable")
	}
	if !f.session.LastAccess.Equal(before) {
		t.Fatal("revoked read touched LastAccess")
	}
	w = httptest.NewRecorder()
	f.invoke("stop", w, f.request("stop"))
	if w.Code != http.StatusOK {
		t.Fatalf("owned revoked-session cleanup denied: %d %s", w.Code, w.Body.String())
	}
	f.manager.mu.RLock()
	_, exists := f.manager.sessions[f.session.ID]
	f.manager.mu.RUnlock()
	if exists {
		t.Fatal("revoked runtime retained after owned stop")
	}
	if _, err := os.Stat(f.session.OutputDir); !os.IsNotExist(err) {
		t.Fatal("revoked runtime directory retained after owned stop")
	}
}
