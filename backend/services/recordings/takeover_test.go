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

type managedRecordingChannels struct{ manager *livecapacity.Manager }

func (p managedRecordingChannels) RecordingChannels(context.Context, string) ([]Channel, error) {
	return nil, nil
}
func (p managedRecordingChannels) ReserveManagedRecording(_ context.Context, r models.Recording) (*livecapacity.Lease, error) {
	return p.manager.AcquireOwned("provider", "", 1, livecapacity.Owner{AccountID: "household", ProfileID: r.UserID, Kind: livecapacity.KindRecording, RecordingID: r.ID})
}

func TestConfirmedTakeoverStopsActualRecorderAndPreservesPartialMedia(t *testing.T) {
	script := writeFakeFFmpeg(t, "#!/bin/sh\nprintf 'synthetic-partial-media'\nexec sleep 30\n")
	recording := newTestRecording(time.Now().Add(time.Minute))
	repo := newFakeRecordingRepo(recording)
	service := newTestService(repo, script, t.TempDir())
	service.ctx = context.Background()
	manager := livecapacity.NewManager()
	service.channels = managedRecordingChannels{manager}
	repaired := make(chan struct{})
	allowRepair := make(chan struct{})
	var repairBlocked atomic.Bool
	service.remuxRecording = func(string) error {
		close(repaired)
		if repairBlocked.Load() {
			<-allowRepair
		}
		return nil
	}
	done := make(chan struct{})
	go func() { defer close(done); service.startRecording(recording) }()
	t.Cleanup(func() {
		repairBlocked.Store(false)
		select {
		case <-allowRepair:
		default:
			close(allowRepair)
		}
		_ = service.Cancel(recording.ID)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("recorder did not stop")
		}
	})
	deadline := time.Now().Add(3 * time.Second)
	var output string
	for {
		current, err := service.Get(recording.ID)
		if err == nil && current.OutputPath != "" {
			if body, err := os.ReadFile(current.OutputPath); err == nil && len(body) > 0 {
				output = current.OutputPath
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("recorder did not write synthetic media")
		}
		time.Sleep(5 * time.Millisecond)
	}
	actor := livecapacity.Owner{AccountID: "household", ProfileID: "viewer", Kind: livecapacity.KindLive, RevocationKey: "explicit-play-one"}
	policy := livecapacity.TakeoverPolicy{AccountID: "household", ProfileID: "viewer", Intent: "synthetic-tune", AllowRecording: true, Authorized: func() bool { return true }}
	lease, err := manager.AcquireWithTakeover(context.Background(), "provider", "", 1, actor, policy)
	var prompt *livecapacity.ConfirmationRequired
	if lease != nil || !errors.As(err, &prompt) {
		t.Fatalf("expected a recording warning, got %v", err)
	}
	current, _ := service.Get(recording.ID)
	if current.Status != models.RecordingStatusRunning || current.OutputPath != output || manager.Current("provider") != 1 {
		t.Fatal("warning interrupted recorder")
	}
	// No confirmation request means cancel/dismiss and has no server mutation.
	repairBlocked.Store(true)
	policy.Confirmation = prompt.Token
	bounded, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	replacement, err := manager.AcquireWithTakeover(bounded, "provider", "", 1, actor, policy)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Release()
	// The input has closed, even if local file repair is still pending.
	select {
	case <-repaired:
	case <-time.After(time.Second):
		t.Fatal("partial media repair was not reached")
	}
	current, _ = service.Get(recording.ID)
	if current.Status != models.RecordingStatusCancelled || !strings.Contains(current.Error, "incomplete recording") || current.OutputPath != output {
		t.Fatal("interrupted job did not retain incomplete status and output")
	}
	if body, err := os.ReadFile(output); err != nil || string(body) != "synthetic-partial-media" {
		t.Fatal("partial recording was changed or removed")
	}
	close(allowRepair)
	<-done
	current, _ = service.Get(recording.ID)
	if current.OutputSizeBytes != int64(len("synthetic-partial-media")) || current.Status != models.RecordingStatusCancelled || !strings.Contains(current.Error, "incomplete recording") {
		t.Fatal("final repair lost interruption result")
	}
}

type failingInterruptionRepo struct {
	*fakeRecordingRepo
	fail bool
}

func (r failingInterruptionRepo) Update(ctx context.Context, recording *models.Recording) error {
	if r.fail {
		return errors.New("controlled persistence failure")
	}
	return r.fakeRecordingRepo.Update(ctx, recording)
}

func TestFailedInterruptionPersistenceNeverStopsOrChangesAnOutput(t *testing.T) {
	recording := newTestRecording(time.Now().Add(time.Minute))
	recording.Status = models.RecordingStatusRunning
	recording.OutputPath = filepath.Join(t.TempDir(), "partial.ts")
	if err := os.WriteFile(recording.OutputPath, []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	repo := newFakeRecordingRepo(recording)
	service := NewService(failingInterruptionRepo{repo, true}, "", t.TempDir())
	var stopped atomic.Bool
	service.active[recording.ID] = func() { stopped.Store(true) }
	if err := service.InterruptForTakeover(context.Background(), recording.ID); err == nil {
		t.Fatal("expected persistence error")
	}
	current, _ := repo.Get(context.Background(), recording.ID)
	if stopped.Load() || current.Status != models.RecordingStatusRunning || current.Error != "" {
		t.Fatal("failed persistence stopped or changed the recorder")
	}
	if body, err := os.ReadFile(recording.OutputPath); err != nil || string(body) != "preserved" {
		t.Fatal("failed interruption touched media")
	}
}

func TestInterruptionRejectsInactiveFinishedExpiredAndCancelledContext(t *testing.T) {
	for _, name := range []string{"inactive", "completed", "pending", "schedule-ended", "cancelled-context"} {
		t.Run(name, func(t *testing.T) {
			recording := newTestRecording(time.Now().Add(time.Minute))
			recording.Status = models.RecordingStatusRunning
			repo := newFakeRecordingRepo(recording)
			service := NewService(repo, "", t.TempDir())
			var stopped atomic.Bool
			service.active[recording.ID] = func() { stopped.Store(true) }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "inactive":
				delete(service.active, recording.ID)
			case "completed":
				recording.Status = models.RecordingStatusCompleted
			case "pending":
				recording.Status = models.RecordingStatusPending
			case "schedule-ended":
				recording.EndAt = time.Now().Add(-time.Hour)
			case "cancelled-context":
				cancel()
			}
			if err := repo.Update(context.Background(), &recording); err != nil {
				t.Fatal(err)
			}
			if err := service.InterruptForTakeover(ctx, recording.ID); err == nil {
				t.Fatal("inactive interruption accepted")
			}
			current, _ := service.Get(recording.ID)
			if stopped.Load() || current.Status != recording.Status || current.Error != "" {
				t.Fatal("rejected interruption changed recording")
			}
		})
	}
}
