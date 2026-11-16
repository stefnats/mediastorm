package recordings

import (
	"context"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
)

type ownerSnapshotRepo struct {
	datastore.RecordingRepository
	item  models.Recording
	calls int
}

func (r *ownerSnapshotRepo) Get(ctx context.Context, id string) (*models.Recording, error) {
	r.calls++
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if id != r.item.ID {
		return nil, nil
	}
	return &r.item, nil
}

func TestRecordingOwnerSnapshotIsRepositoryOnlyAndStableAcrossCaptureUpdates(t *testing.T) {
	r := &ownerSnapshotRepo{item: models.Recording{ID: "recording-one", UserID: "owner", CreatedAt: time.Now(), OutputPath: "/unreadable/private/missing.ts", Capture: &models.RecordingCapture{Version: 999}}}
	s := NewService(r, "", "/unreadable/private/output")
	first, err := s.SnapshotStreamOwner(t.Context(), "recording:recording-one/parts/2")
	if err != nil || first.ProfileID != "owner" || r.calls != 1 {
		t.Fatalf("owner=%v error=%v calls=%d", first, err, r.calls)
	}
	r.item.OutputPath = "/another/missing/file.ts"
	r.item.UpdatedAt = time.Now().Add(time.Minute)
	r.item.Status = models.RecordingStatusRunning
	second, err := s.SnapshotStreamOwner(t.Context(), "recording:recording-one/legacy.ts")
	if err != nil || first != second {
		t.Fatal("media checkpoints changed owner identity")
	}
	r.item.UserID = "replacement"
	third, err := s.SnapshotStreamOwner(t.Context(), "recording:recording-one")
	if err != nil || third.ProfileID != "replacement" || first.ProfileID != "owner" {
		t.Fatal("snapshot was stale or mutable")
	}
}

func TestRecordingOwnerSnapshotRejectsInvalidSelectorsBeforeRepository(t *testing.T) {
	r := &ownerSnapshotRepo{item: models.Recording{ID: "recording-one", UserID: "owner"}}
	s := NewService(r, "", "unused")
	for _, path := range []string{"foreign:recording-one", "recording:", "recording:recording-one/parts/0", "recording:recording-one/parts/01", "recording:recording-one/parts%2f2"} {
		if _, err := s.SnapshotStreamOwner(t.Context(), path); err == nil {
			t.Fatalf("invalid selector accepted: %s", path)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.SnapshotStreamOwner(ctx, "recording:recording-one"); err == nil || r.calls != 0 {
		t.Fatal("invalid/cancelled snapshot reached repository")
	}
}
