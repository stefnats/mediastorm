package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/recordings"
	user_settings "novastream/services/user_settings"
)

type derivedWebService struct {
	*recordingWebService
	prepares, reads, streams, cancelled atomic.Int32
	waiting                             atomic.Bool
	mu                                  sync.Mutex
	hold                                chan struct{}
}

func (s *derivedWebService) PrepareDerived(ctx context.Context, id string, authorize func() error) (recordings.DerivedRecording, error) {
	s.prepares.Add(1)
	s.mu.Lock()
	gate := s.hold
	s.mu.Unlock()
	if gate != nil {
		s.waiting.Store(true)
		defer s.waiting.Store(false)
		select {
		case <-ctx.Done():
			s.cancelled.Add(1)
			return recordings.DerivedRecording{}, recordings.ErrDerivedAccess
		case <-gate:
		}
	}
	return s.Service.PrepareDerived(ctx, id, authorize)
}

func (s *derivedWebService) GetDerived(ctx context.Context, id string, authorize func() error) (recordings.DerivedRecording, error) {
	s.reads.Add(1)
	return s.Service.GetDerived(ctx, id, authorize)
}

func (s *derivedWebService) OpenDerived(ctx context.Context, id, derivedID string, authorize func() error) (*recordings.DerivedReader, error) {
	s.streams.Add(1)
	return s.Service.OpenDerived(ctx, id, derivedID, authorize)
}

type derivedWebFixture struct {
	base     *recordingWebFixture
	service  *derivedWebService
	handler  http.Handler
	original map[string]string
}

// The actual web route registration, handler, cache reader and joiner use tiny
// generated local containers. Browser assertions do not claim decoder acceptance.
func newDerivedWebFixture(t *testing.T) *derivedWebFixture {
	t.Helper()
	base := newRecordingWebFixture(t)
	media := make(map[int][]byte)
	for index := 1; index <= 2; index++ {
		cmd := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "2", "-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", "-f", "mpegts", "pipe:1")
		data, err := cmd.Output()
		if err != nil {
			t.Fatalf("synthetic local recording container: %v", err)
		}
		media[index] = data
	}
	original := make(map[string]string)
	base.repo.mu.Lock()
	for id, job := range base.repo.jobs {
		start := job.StartAt
		firstEnd, secondStart, secondEnd := start.Add(2*time.Second), start.Add(3*time.Second), start.Add(5*time.Second)
		job.EndAt = secondEnd
		job.Capture.Parts = []models.RecordingPart{{Index: 1, StartedAt: start, EndedAt: &firstEnd, EndReason: "source_ended", Bytes: int64(len(media[1]))}, {Index: 2, StartedAt: secondStart, EndedAt: &secondEnd, EndReason: "capture_failed", Bytes: int64(len(media[2]))}}
		job.Capture.Gaps = []models.RecordingGap{{StartedAt: firstEnd, EndedAt: &secondStart, Reason: "capacity"}}
		base.repo.jobs[id] = job
		for index, name := range []string{"part-000001.ts", "part-000002.ts"} {
			path := filepath.Join(base.output, job.UserID, id, name)
			if err := os.WriteFile(path, media[index+1], 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(media[index+1])
			original[path] = hex.EncodeToString(sum[:])
		}
	}
	base.repo.mu.Unlock()
	service := &derivedWebService{recordingWebService: base.service}
	tools := recordings.DerivedTools{FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe"}
	if err := service.Service.ConfigureDerivedJoiner(t.Context(), tools, func(ctx context.Context) (recordings.DerivedTools, error) { return tools, ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Stop retires configured derived owners even when capture Start was never called.
		if err := service.Service.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	recordingHandler := handlers.NewRecordingsHandler(service, base.profiles)
	recordingHandler.SetRecordingStreamAccess(handlers.NewRecordingStreamAccess(service.Service, base.sessions, base.accounts, base.profiles, base.unlocks, func() (config.SecuritySettings, error) { return config.SecuritySettings{EnforceProfilePins: true}, nil }))
	root := filepath.Dir(base.output)
	settings := config.NewManager(filepath.Join(root, "settings.json"))
	prefs, err := user_settings.NewService(root)
	if err != nil {
		t.Fatal(err)
	}
	admin := handlers.NewAdminUIHandler(settings.ConfigPath(), "", nil, base.profiles, prefs, settings)
	admin.SetAccountsService(base.accounts)
	admin.SetSessionsService(base.sessions)
	fixture := &derivedWebFixture{base: base, service: service, original: original}
	router := mux.NewRouter()
	registerRecordingDerivedWebRoutes(router, admin, base.sessions, base.accounts, base.profiles, base.verify, recordingHandler, nil)
	router.HandleFunc("/fixture/derived", func(w http.ResponseWriter, r *http.Request) {
		unchanged := true
		for path, expected := range original {
			data, err := os.ReadFile(path)
			sum := sha256.Sum256(data)
			if err != nil || hex.EncodeToString(sum[:]) != expected {
				unchanged = false
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"prepares": service.prepares.Load(), "reads": service.reads.Load(), "streams": service.streams.Load(), "cancelled": service.cancelled.Load(), "waiting": service.waiting.Load(), "originalBytesUnchanged": unchanged})
	})
	router.HandleFunc("/fixture/derived-digest", func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("recordingId")
		base.repo.mu.Lock()
		_, exists := base.repo.jobs[id]
		base.repo.mu.Unlock()
		if r.Method != http.MethodGet || !exists || len(r.URL.Query()) != 1 {
			http.Error(w, "synthetic fixture request unavailable", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		check := func() error { return ctx.Err() }
		value, err := service.Service.GetDerived(ctx, id, check)
		if err != nil || value.State != "ready" || value.Bytes <= 0 || value.Bytes > 4<<20 {
			http.Error(w, "synthetic joined copy unavailable", http.StatusConflict)
			return
		}
		reader, err := service.Service.OpenDerived(ctx, id, value.ID, check)
		if err != nil {
			http.Error(w, "synthetic joined copy unavailable", http.StatusConflict)
			return
		}
		defer reader.Close()
		digest := sha256.New()
		n, err := io.Copy(digest, io.LimitReader(reader, (4<<20)+1))
		if err != nil || n != value.Bytes || n > 4<<20 || check() != nil {
			http.Error(w, "synthetic joined copy changed", http.StatusConflict)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"bytes": n, "sha256": hex.EncodeToString(digest.Sum(nil))})
	})
	router.HandleFunc("/fixture/derived-hold", func(w http.ResponseWriter, r *http.Request) {
		service.mu.Lock()
		if service.hold != nil {
			close(service.hold)
		}
		service.hold = make(chan struct{})
		service.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	router.HandleFunc("/fixture/derived-release", func(w http.ResponseWriter, r *http.Request) {
		service.mu.Lock()
		if service.hold != nil {
			close(service.hold)
			service.hold = nil
		}
		service.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	router.PathPrefix("/").Handler(base.handler)
	fixture.handler = router
	return fixture
}
