package recordings

import (
	"context"
	"time"
)

// StreamOwner contains only repository ownership, never a readable file grant.
// Capture checkpoints deliberately do not change this stable identity.
type StreamOwner struct {
	RecordingID string
	ProfileID   string
	CreatedAt   time.Time
}

// SnapshotStreamOwner does not call Get: Get projects capture files and can
// stat them before HTTP authorization. This lookup never touches media files.
func (s *Service) SnapshotStreamOwner(ctx context.Context, path string) (StreamOwner, error) {
	if ctx.Err() != nil {
		return StreamOwner{}, ctx.Err()
	}
	id, _, _, valid := parsePartStreamPath(path)
	if s == nil || s.repo == nil || !valid {
		return StreamOwner{}, ErrRecordingNotFound
	}
	r, err := s.repo.Get(ctx, id)
	if err != nil {
		return StreamOwner{}, err
	}
	if r == nil || r.ID != id || r.UserID == "" || ctx.Err() != nil {
		return StreamOwner{}, ErrRecordingNotFound
	}
	return StreamOwner{RecordingID: r.ID, ProfileID: r.UserID, CreatedAt: r.CreatedAt}, nil
}
