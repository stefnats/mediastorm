package recordings

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"log"
	"os"
	"sync"
	"time"

	"novastream/internal/livecapacity"
	"novastream/internal/liveusage"
	"novastream/models"
)

var ErrCaptureSuperseded = errors.New("recording capture owner was replaced")

func (s *Service) mutateCapture(id string, change func(*models.Recording)) (*models.Recording, error) {
	return s.mutateCaptureOwned(id, "", change)
}

func (s *Service) mutateCaptureOwned(id, generation string, change func(*models.Recording)) (*models.Recording, error) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	recording, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if generation != "" && (recording.Capture == nil || recording.Capture.Restored || recording.Capture.RunID != generation) {
		return nil, ErrCaptureSuperseded
	}
	change(recording)
	recording.UpdatedAt = time.Now().UTC()
	if err = s.repo.Update(context.Background(), recording); err != nil {
		return nil, err
	}
	return recording, nil
}

func (s *Service) captureInput(ctx context.Context, recording models.Recording, source RecordingSource) (RecordingInput, error) {
	if planner, ok := s.channels.(RecordingSourcePlanner); ok {
		return planner.PrepareRecordingSource(ctx, recording, source)
	}
	// Legacy/internal channel providers keep their own authorization and slot
	// interface; they cannot invent alternate stations. The production provider
	// implements RecordingSourcePlanner and always supplies current owned proof.
	recording.SourceURL = source.URL
	if err := s.authorizeSource(&recording); err != nil {
		return RecordingInput{}, err
	}
	source.URL = recording.SourceURL
	if managed, ok := s.channels.(ManagedCapacityReserver); ok {
		lease, err := managed.ReserveManagedRecording(ctx, recording)
		return RecordingInput{Source: source, Lease: lease}, err
	}
	if reserver, ok := s.channels.(CapacityReserver); ok {
		release, err := reserver.ReserveRecording(ctx, recording)
		var once sync.Once
		return RecordingInput{Source: source, releaseLegacy: func() {
			if release != nil {
				once.Do(release)
			}
		}}, err
	}
	return RecordingInput{Source: source}, nil
}

func (s *Service) startCaptureRecording(recording models.Recording) {
	s.mu.Lock()
	if s.starting[recording.ID] {
		s.mu.Unlock()
		return
	}
	if s.starting == nil {
		s.starting = map[string]bool{}
	}
	if s.active == nil {
		s.active = map[string]context.CancelFunc{}
	}
	base := s.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)
	s.starting[recording.ID] = true
	s.active[recording.ID] = cancel
	s.mu.Unlock()
	unregister := func() {}
	defer func() {
		cancel()
		s.mu.Lock()
		delete(s.active, recording.ID)
		delete(s.starting, recording.ID)
		s.mu.Unlock()
		unregister()
	}()
	registered, registerErr := registerCaptureOwner(ctx, cancel)
	if registerErr != nil {
		return
	}
	unregister = registered
	latest, err := s.Get(recording.ID)
	if err != nil || latest.Status != models.RecordingStatusPending {
		return
	}
	recording = *latest
	stopAt := recording.EndAt.Add(time.Duration(recording.PaddingAfterSeconds) * time.Second)
	if !stopAt.After(time.Now()) {
		s.finishCapture(recording.ID, false, "recording window expired before start")
		return
	}
	if recording.Capture != nil && (recording.Capture.Restored || len(recording.Capture.Parts) > 0) {
		s.finishCapture(recording.ID, false, "existing capture was not overwritten or resumed")
		return
	}
	if s.ffmpegPath == "" {
		s.finishCapture(recording.ID, false, "ffmpeg is not configured")
		return
	}
	lookup, stopLookup := context.WithTimeout(ctx, min(5*time.Second, time.Until(stopAt)))
	sources := []RecordingSource{{ChannelID: recording.ChannelID, Name: recording.ChannelName, URL: recording.SourceURL, Original: true}}
	if planner, ok := s.channels.(RecordingSourcePlanner); ok {
		sources, err = planner.PlanRecordingSources(lookup, recording)
	} else {
		err = s.authorizeSource(&recording)
		sources[0].URL = recording.SourceURL
	}
	stopLookup()
	if err != nil || len(sources) == 0 {
		s.finishCapture(recording.ID, false, "recording channel is not available to this profile")
		return
	}
	if len(sources) > 4 {
		sources = sources[:4]
	}
	if ctx.Err() != nil {
		s.finishCapture(recording.ID, false, "recording interrupted")
		return
	}
	generation := uuid.NewString()
	transition, err := s.mutateCapture(recording.ID, func(current *models.Recording) {
		if current.Status == models.RecordingStatusCancelled {
			return
		}
		current.Status = models.RecordingStatusStarting
		if current.Capture == nil {
			current.Capture = &models.RecordingCapture{Version: 1, Parts: []models.RecordingPart{}, Gaps: []models.RecordingGap{}}
		}
		current.Capture.RunID = generation
		current.Capture.Phase = "waiting_capacity"
		current.Capture.StationID = sources[0].StationID
		current.Error = ""
	})
	if err != nil || transition.Status == models.RecordingStatusCancelled || transition.Capture == nil {
		return
	}
	directoryOwned := false
	attempt := 0
	nextSource := 0
	lastSource := [2]string{}
	haveLastSource := false
	reachedSchedule := false
	lastFailure := ""
	liveusage.GetTracker().StartRecording(recording.ID, recording.UserID)
	defer liveusage.GetTracker().EndRecording(recording.ID)
	for ctx.Err() == nil && time.Now().Before(stopAt) {
		owner, e := s.Get(recording.ID)
		if e != nil || owner.UserID != recording.UserID || owner.Capture == nil || owner.Capture.Restored || owner.Capture.RunID != generation {
			return
		}
		if owner.Status == models.RecordingStatusCancelled {
			break
		}
		if attempt >= models.MaxRecordingParts {
			lastFailure = "recording part safety limit reached; preserved media is incomplete"
			break
		}
		var input RecordingInput
		var admissionErr error
		capacityBlocked := false
		for n := 0; n < len(sources); n++ {
			if ctx.Err() != nil || !time.Now().Before(stopAt) {
				break
			}
			selected := (nextSource + n) % len(sources)
			admission, stopAdmission := context.WithTimeout(ctx, min(5*time.Second, time.Until(stopAt)))
			input, admissionErr = s.captureInput(admission, recording, sources[selected])
			stopAdmission()
			if admissionErr == nil {
				nextSource = (selected + 1) % len(sources)
				break
			}
			input.release()
			if errors.Is(admissionErr, livecapacity.ErrLimit) {
				capacityBlocked = true
			}
		}
		if ctx.Err() != nil {
			input.release()
			break
		}
		if admissionErr != nil {
			if capacityBlocked {
				admissionErr = livecapacity.ErrLimit
			}
			if attempt == 0 && errors.Is(admissionErr, livecapacity.ErrLimit) {
				_, _ = s.mutateCaptureOwned(recording.ID, generation, func(current *models.Recording) {
					if current.Status != models.RecordingStatusCancelled {
						current.Status = models.RecordingStatusPending
						current.Error = "Provider connection limit reached; waiting for a free IPTV connection"
						if current.Capture != nil {
							openCaptureGap(current.Capture, "capacity")
						}
					}
				})
				return
			}
			if !errors.Is(admissionErr, livecapacity.ErrLimit) {
				lastFailure = "recording source access is unavailable"
				break
			}
			_, _ = s.mutateCaptureOwned(recording.ID, generation, func(current *models.Recording) {
				if current.Capture != nil {
					current.Capture.Phase = "waiting_capacity"
					openCaptureGap(current.Capture, "capacity")
				}
			})
			if !captureDelay(ctx, stopAt) {
				break
			}
			continue
		}
		owner, e = s.Get(recording.ID)
		if e != nil || owner.UserID != recording.UserID || owner.Capture == nil || owner.Capture.Restored || owner.Capture.RunID != generation {
			input.release()
			return
		}
		if !time.Now().Before(stopAt) {
			input.release()
			break
		}
		if !directoryOwned {
			if pathErr := s.createCaptureDirectory(recording); pathErr != nil {
				input.release()
				lastFailure = "recording directory is unavailable; existing files were preserved"
				break
			}
			directoryOwned = true
		}
		attempt++
		path, pathErr := s.partPath(recording, attempt)
		var file *os.File
		if pathErr == nil {
			file, pathErr = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		}
		if pathErr != nil {
			input.release()
			lastFailure = "recording part is unavailable; existing files were preserved"
			break
		}
		started := time.Now().UTC()
		part := models.RecordingPart{Index: attempt, SourceID: input.Source.SourceID, ChannelID: input.Source.ChannelID, StartedAt: started}
		_, err = s.mutateCaptureOwned(recording.ID, generation, func(current *models.Recording) {
			if current.Capture == nil {
				return
			}
			closeCaptureGap(current.Capture, started)
			current.Capture.Phase = "capturing"
			if input.Source.Original && input.Source.StationID == "" {
				current.Capture.StationID = ""
			}
			current.Capture.Parts = append(current.Capture.Parts, part)
			if haveLastSource && lastSource != [2]string{input.Source.SourceID, input.Source.ChannelID} {
				current.Capture.SourceChanges++
			}
			if current.Status != models.RecordingStatusCancelled {
				current.Status = models.RecordingStatusRunning
			}
			if current.ActualStartAt == nil {
				current.ActualStartAt = &started
			}
		})
		if err != nil {
			file.Close()
			os.Remove(path)
			input.release()
			return
		}
		lastSource = [2]string{input.Source.SourceID, input.Source.ChannelID}
		haveLastSource = true
		if input.Source.Original && input.Source.StationID == "" {
			sources = []RecordingSource{input.Source}
			nextSource = 0
		}
		if input.Lease != nil {
			input.Lease.SetStop(func(stop context.Context) error { return s.InterruptForTakeover(stop, recording.ID) })
		}
		capture := recording
		capture.SourceURL = input.Source.URL
		capture.OutputPath = path
		result := recordingAttemptResult{err: ctx.Err()}
		if ctx.Err() == nil {
			result = s.runCaptureProcess(ctx, capture, stopAt, attempt, file)
		}
		// Wait has joined FFmpeg and its copy goroutines. Flush and close the
		// old output before releasing its private provider connection.
		var syncErr error
		if s.syncCaptureFile != nil {
			syncErr = s.syncCaptureFile(file)
		} else {
			syncErr = file.Sync()
		}
		closeErr := file.Close()
		outputFailure := syncErr != nil || closeErr != nil
		if outputFailure {
			result.err = errors.Join(result.err, syncErr, closeErr)
			result.stoppedAtSchedule = false
		}
		input.release()
		ended := time.Now().UTC()
		reason := "source_ended"
		if result.err != nil {
			reason = "capture_failed"
		}
		if result.stoppedAtSchedule {
			reason = "schedule"
			reachedSchedule = true
		}
		if ctx.Err() != nil {
			reason = "shutdown"
			if latest, e := s.Get(recording.ID); e == nil && latest.Status == models.RecordingStatusCancelled {
				reason = "cancelled"
			}
		}
		_, err = s.mutateCaptureOwned(recording.ID, generation, func(current *models.Recording) {
			if current.Capture == nil {
				return
			}
			for i := range current.Capture.Parts {
				if current.Capture.Parts[i].Index == attempt {
					p := &current.Capture.Parts[i]
					p.Bytes = result.written
					if p.Bytes < 0 {
						p.Bytes = 0
					}
					closeCapturePart(p, ended, reason)
				}
			}
			if result.written > 0 && current.OutputPath == "" {
				current.OutputPath = path
				current.OutputSizeBytes = result.written
			}
			if !result.stoppedAtSchedule || attempt > 1 {
				current.Capture.Incomplete = true
			}
			if !result.stoppedAtSchedule && ctx.Err() == nil {
				openCaptureGap(current.Capture, "source_retry")
			}
		})
		if err != nil {
			return
		}
		if outputFailure {
			lastFailure = "recording output could not be finalized; preserved media is incomplete"
			break
		}
		if result.stoppedAtSchedule || ctx.Err() != nil {
			break
		}
		lastFailure = "source stream ended before scheduled stop; preserved parts are incomplete"
		if !captureDelay(ctx, stopAt) {
			break
		}
	}
	s.finishCapture(recording.ID, reachedSchedule, lastFailure, generation)
}

func openCaptureGap(capture *models.RecordingCapture, reason string) {
	if len(capture.Gaps) > 0 && capture.Gaps[len(capture.Gaps)-1].EndedAt == nil {
		return
	}
	if len(capture.Gaps) >= models.MaxRecordingGaps {
		capture.Incomplete = true
		return
	}
	capture.Gaps = append(capture.Gaps, models.RecordingGap{StartedAt: time.Now().UTC(), Reason: reason})
	capture.Incomplete = true
}
func closeCaptureGap(capture *models.RecordingCapture, at time.Time) {
	if len(capture.Gaps) > 0 && capture.Gaps[len(capture.Gaps)-1].EndedAt == nil {
		capture.Gaps[len(capture.Gaps)-1].EndedAt = &at
	}
}
func captureDelay(ctx context.Context, stopAt time.Time) bool {
	delay := min(recordingRetryDelay, time.Until(stopAt))
	if delay <= 0 {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) finishCapture(id string, reachedSchedule bool, message string, generations ...string) {
	generation := ""
	if len(generations) > 0 {
		generation = generations[0]
	}
	_, err := s.mutateCaptureOwned(id, generation, func(current *models.Recording) {
		now := time.Now().UTC()
		current.ActualEndAt = &now
		if current.Capture != nil {
			closeCaptureGap(current.Capture, now)
			current.Capture.Phase = "finished"
		}
		if current.Status == models.RecordingStatusCancelled {
			if current.Capture != nil {
				current.Capture.Phase = "interrupted"
				current.Capture.Incomplete = true
			}
			return
		}
		if reachedSchedule && current.OutputSizeBytes > 0 && current.Capture != nil && !current.Capture.Incomplete {
			current.Status = models.RecordingStatusCompleted
			current.Error = ""
			return
		}
		current.Status = models.RecordingStatusFailed
		if message == "" {
			message = "recording interrupted or ended without media data"
		}
		current.Error = message
		if current.Capture != nil {
			current.Capture.Incomplete = true
			if !reachedSchedule {
				current.Capture.Phase = "interrupted"
			}
		}
	})
	if err != nil {
		log.Printf("[recordings] finalize capture failed id=%s", id)
	}
}
