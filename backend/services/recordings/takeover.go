package recordings

import (
	"context"
	"errors"
	"time"

	"novastream/models"
)

var ErrRecordingNotActive = errors.New("recording is no longer actively receiving media")

// InterruptForTakeover is invoked only by a server-selected owned lease after
// separate permission and confirmation checks. Persist incomplete/cancelled
// status before asking FFmpeg to close; never remove or truncate its output.
func (s *Service) InterruptForTakeover(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	s.mu.Lock()
	cancel := s.active[id]
	s.mu.Unlock()
	if cancel == nil {
		return ErrRecordingNotActive
	}
	recording, err := s.Get(id)
	if err != nil {
		return err
	}
	if recording.Status != models.RecordingStatusRunning && recording.Status != models.RecordingStatusStarting {
		return ErrRecordingNotActive
	}
	now := time.Now().UTC()
	if !now.Before(recording.EndAt.Add(time.Duration(recording.PaddingAfterSeconds) * time.Second)) {
		return ErrRecordingNotActive
	}
	recording.Status = models.RecordingStatusCancelled
	recording.ActualEndAt = &now
	recording.UpdatedAt = now
	recording.Error = "Stopped for another IPTV playback; incomplete recording"
	if err := s.repo.Update(ctx, recording); err != nil {
		return err
	}
	cancel()
	return nil
}
