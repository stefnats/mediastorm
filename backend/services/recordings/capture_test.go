package recordings

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/livecapacity"
	"novastream/models"
)

type capturePlannerFixture struct {
	sources []RecordingSource
	plan    func(context.Context) ([]RecordingSource, error)
	prepare func(context.Context, RecordingSource) (RecordingInput, error)
}

func (p *capturePlannerFixture) RecordingChannels(context.Context, string) ([]Channel, error) {
	return nil, nil
}
func (p *capturePlannerFixture) PlanRecordingSources(ctx context.Context, _ models.Recording) ([]RecordingSource, error) {
	if p.plan != nil {
		return p.plan(ctx)
	}
	return p.sources, nil
}
func (p *capturePlannerFixture) PrepareRecordingSource(ctx context.Context, _ models.Recording, source RecordingSource) (RecordingInput, error) {
	if p.prepare != nil {
		return p.prepare(ctx, source)
	}
	return RecordingInput{Source: source}, nil
}

func captureTestSources() []RecordingSource {
	return []RecordingSource{{SourceID: "same-playlist", ChannelID: "one", StationID: "station:" + strings.Repeat("a", 24), URL: "synthetic:one", Original: true}, {SourceID: "same-playlist", ChannelID: "two", StationID: "station:" + strings.Repeat("a", 24), URL: "synthetic:two"}}
}

func captureTestBinary(t *testing.T, firstFails bool) string {
	t.Helper()
	first := ""
	if firstFails {
		first = `if [ "$input" = "synthetic:one" ]; then printf 'first-input'; exit 1; fi`
	}
	return writeFakeFFmpeg(t, "#!/bin/sh\ninput=''\nprevious=''\nfor arg do\n if [ \"$previous\" = '-i' ]; then input=\"$arg\"; fi\n previous=\"$arg\"\ndone\n"+first+"\nprintf 'second-input'\ntrap 'exit 0' INT TERM\nwhile :; do sleep 0.02; done\n")
}
func awaitCapture(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not close")
	}
}
func fastCaptureRetries(t *testing.T) {
	t.Helper()
	old := recordingRetryDelay
	recordingRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { recordingRetryDelay = old })
}

func TestCaptureSamePlaylistVariantPreservesPartsAndReleasesOldSlotBeforeNewInput(t *testing.T) {
	fastCaptureRetries(t)
	job := newTestRecording(time.Now().Add(time.Second))
	repo := newFakeRecordingRepo(job)
	s := newTestService(repo, captureTestBinary(t, true), t.TempDir())
	manager := livecapacity.NewManager()
	planner := &capturePlannerFixture{sources: captureTestSources()}
	var admissions atomic.Int32
	planner.prepare = func(ctx context.Context, source RecordingSource) (RecordingInput, error) {
		if manager.Current("provider") != 0 {
			t.Error("new admission ran before old input lease was released")
		}
		if admissions.Add(1) == 2 {
			prior, _ := s.Get(job.ID)
			if len(prior.Capture.Parts) != 1 || prior.Capture.Parts[0].EndedAt == nil {
				t.Error("new input opened before old writer joined and part closed")
			}
		}
		lease, err := manager.AcquireOwned("provider", "", 1, livecapacity.Owner{ProfileID: job.UserID, Kind: livecapacity.KindRecording, RecordingID: job.ID})
		return RecordingInput{Source: source, Lease: lease}, err
	}
	s.channels = planner
	s.startRecording(job)
	got, _ := s.Get(job.ID)
	if got.Status != models.RecordingStatusFailed || got.Capture == nil || !got.Capture.Incomplete || got.Capture.SourceChanges != 1 || len(got.Capture.Parts) != 2 {
		t.Fatalf("honest cross-variant capture state: %+v", got)
	}
	for i, want := range []string{"first-input", "second-input"} {
		path, part, err := s.ResolvePartFile(job.ID, i+1)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want || part.EndedAt == nil {
			t.Fatalf("part%d overwritten/concatenated: %q %v", i+1, data, err)
		}
	}
	if got.OutputSizeBytes != int64(len("first-input")) || manager.Current("provider") != 0 {
		t.Fatal("first-file byte semantics or final lease release failed")
	}
}

func TestCaptureInitialCapacityGapSurvivesQueueAndUnavailableAlternate(t *testing.T) {
	job := newTestRecording(time.Now().Add(time.Second))
	s := newTestService(newFakeRecordingRepo(job), captureTestBinary(t, false), t.TempDir())
	planner := &capturePlannerFixture{sources: captureTestSources()}
	free := false
	planner.prepare = func(_ context.Context, source RecordingSource) (RecordingInput, error) {
		if !source.Original {
			return RecordingInput{}, errors.New("alternate access changed")
		}
		if !free {
			return RecordingInput{}, livecapacity.ErrLimit
		}
		return RecordingInput{Source: source}, nil
	}
	s.channels = planner
	s.startRecording(job)
	waiting, _ := s.Get(job.ID)
	if waiting.Status != models.RecordingStatusPending || waiting.Capture == nil || len(waiting.Capture.Gaps) != 1 || !waiting.Capture.Incomplete {
		t.Fatal("eligible busy primary was replaced by later alternate error or queue gap lost")
	}
	free = true
	s.startRecording(*waiting)
	got, _ := s.Get(job.ID)
	if got.Status != models.RecordingStatusFailed || !got.Capture.Incomplete || len(got.Capture.Gaps) != 1 || got.Capture.Gaps[0].EndedAt == nil || got.OutputSizeBytes == 0 {
		t.Fatal("known initial capacity gap became a completed whole recording")
	}
}

func TestCaptureCancelDuringPlanCannotAcquireOrOpen(t *testing.T) {
	job := newTestRecording(time.Now().Add(time.Minute))
	s := newTestService(newFakeRecordingRepo(job), "must-not-execute", t.TempDir())
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var admitted atomic.Int32
	p := &capturePlannerFixture{plan: func(context.Context) ([]RecordingSource, error) {
		close(entered)
		<-release
		return captureTestSources(), nil
	}, prepare: func(context.Context, RecordingSource) (RecordingInput, error) {
		admitted.Add(1)
		return RecordingInput{}, nil
	}}
	s.channels = p
	go func() { s.startRecording(job); close(done) }()
	awaitCapture(t, entered)
	if err := s.Cancel(job.ID); err != nil {
		t.Fatal(err)
	}
	close(release)
	awaitCapture(t, done)
	got, _ := s.Get(job.ID)
	if got.Status != models.RecordingStatusCancelled || admitted.Load() != 0 {
		t.Fatal("cancelled pending owner acquired a new input")
	}
	dir, _ := s.recordingDirectory(job)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("cancelled plan created media directory")
	}
}

func TestCaptureFlushFailureCannotCompleteOrRetryAndCancelledDeleteWaits(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush-error", true: "cancel-delete"}[cancelled], func(t *testing.T) {
			job := newTestRecording(time.Now().Add(500 * time.Millisecond))
			s := newTestService(newFakeRecordingRepo(job), captureTestBinary(t, false), t.TempDir())
			s.channels = &capturePlannerFixture{sources: captureTestSources()[:1]}
			closing, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			s.syncCaptureFile = func(*os.File) error {
				close(closing)
				if cancelled {
					<-release
					return nil
				}
				return errors.New("synthetic disk flush failure")
			}
			go func() { s.startRecording(job); close(done) }()
			if cancelled {
				deadline := time.Now().Add(time.Second)
				for time.Now().Before(deadline) {
					r, _ := s.Get(job.ID)
					if r.Capture != nil && len(r.Capture.Parts) > 0 && r.Capture.Parts[0].Bytes > 0 {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				if err := s.Cancel(job.ID); err != nil {
					t.Fatal(err)
				}
			}
			awaitCapture(t, closing)
			if cancelled {
				if err := s.Delete(job.ID); !errors.Is(err, ErrCannotDeleteActive) {
					t.Fatalf("cancelled writer was deletable before file closure: %v", err)
				}
				close(release)
			}
			awaitCapture(t, done)
			got, _ := s.Get(job.ID)
			want := models.RecordingStatusFailed
			if cancelled {
				want = models.RecordingStatusCancelled
			}
			if got.Status != want || got.Capture == nil || !got.Capture.Incomplete || got.OutputSizeBytes == 0 {
				t.Fatalf("flush/cancel finalization: %+v", got)
			}
		})
	}
}

func TestCaptureCollisionAndSymlinkNeverAdoptOrDeleteExistingBytes(t *testing.T) {
	for _, kind := range []string{"collision", "root-symlink", "profile-symlink"} {
		t.Run(kind, func(t *testing.T) {
			job := newTestRecording(time.Now().Add(time.Second))
			root := t.TempDir()
			s := newTestService(newFakeRecordingRepo(job), "must-not-execute", root)
			s.channels = &capturePlannerFixture{sources: captureTestSources()[:1]}
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel.ts")
			if err := os.WriteFile(sentinel, []byte("existing-bytes"), 0600); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "collision":
				dir, _ := s.recordingDirectory(job)
				if err := os.MkdirAll(dir, 0755); err != nil {
					t.Fatal(err)
				}
				sentinel, _ = s.partPath(job, 1)
				if err := os.WriteFile(sentinel, []byte("existing-bytes"), 0600); err != nil {
					t.Fatal(err)
				}
			case "root-symlink":
				s.outputDir = filepath.Join(t.TempDir(), "recordings")
				if err := os.Symlink(outside, s.outputDir); err != nil {
					t.Fatal(err)
				}
			case "profile-symlink":
				if err := os.Symlink(outside, filepath.Join(root, job.UserID)); err != nil {
					t.Fatal(err)
				}
			}
			s.startRecording(job)
			got, _ := s.Get(job.ID)
			if got.Status != models.RecordingStatusFailed || got.OutputPath != "" || len(got.Capture.Parts) != 0 {
				t.Fatal("unowned pre-existing file gained capture attribution")
			}
			if _, _, err := s.ResolvePartFile(job.ID, 1); err == nil {
				t.Fatal("unowned collision part became readable")
			}
			if err := s.Delete(job.ID); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "existing-bytes" {
				t.Fatal("collision/symlink touched existing media")
			}
		})
	}
}

func TestCaptureRestoreRegistrationBarrierAndRunGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done, err := registerCaptureOwner(ctx, cancel)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	timed, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if release, err := QuiesceForRestore(timed); err == nil {
		release()
		t.Fatal("restore passed a writer that had not joined")
	}
	done()
	release, err := QuiesceForRestore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waiting, stopWaiting := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stopWaiting()
	if unregister, err := registerCaptureOwner(waiting, func() {}); err == nil {
		unregister()
		release()
		t.Fatal("new owner entered during restore")
	}
	release()
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if release, err := QuiesceForRestore(cancelled); err == nil {
		release()
		t.Fatal("cancelled restore acquired authority")
	}
	job := newTestRecording(time.Now().Add(time.Minute))
	job.Capture = &models.RecordingCapture{Version: 1, Phase: "capturing", RunID: "new-owner", Parts: []models.RecordingPart{}, Gaps: []models.RecordingGap{}}
	s := newTestService(newFakeRecordingRepo(job), "", t.TempDir())
	changed := false
	if _, err := s.mutateCaptureOwned(job.ID, "old-owner", func(*models.Recording) { changed = true }); !errors.Is(err, ErrCaptureSuperseded) || changed {
		t.Fatal("late owner mutated a replaced/imported ledger")
	}
}
