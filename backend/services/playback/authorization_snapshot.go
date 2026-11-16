package playback

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"novastream/models"
	"time"
)

// AuthorizationStamp correlates a normal worker attempt inside this store
// instance. It is not a media grant and cannot survive serialization/restore.
type AuthorizationStamp struct {
	store      *PrequeueStore
	attempt    uint64
	generation uint64
}

func (s AuthorizationStamp) Valid() bool                  { return s.store != nil && s.attempt != 0 }
func (s AuthorizationStamp) String() string               { return "private preparation attempt" }
func (s AuthorizationStamp) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (s *AuthorizationStamp) UnmarshalJSON([]byte) error {
	return errors.New("preparation attempts cannot be imported")
}

// BeginAuthorizationAttempt is called only by an optional private observer at
// the beginning of the existing normal worker. It opens/probes/persists nothing.
func (s *PrequeueStore) BeginAuthorizationAttempt(id string) (AuthorizationStamp, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.entries[id]
	if !ok || entry.MigrationAdopted || entry.ReuseDisabled || !entry.ExpiresAt.After(time.Now()) {
		return AuthorizationStamp{}, false
	}
	s.authorizationSequence++
	entry.authorizationAttempt = s.authorizationSequence
	return AuthorizationStamp{s, s.authorizationSequence, s.generation.Load()}, true
}

// SnapshotForAuthorization is deliberately separate from Get/GetForPlayback:
// those methods may invoke a stream validator. This method only copies current
// memory and observes expiry. It never refreshes TTL, validates or opens input.
func (s *PrequeueStore) SnapshotForAuthorization(id string) (*PrequeueEntry, AuthorizationStamp, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.entries[id]
	if !ok || !entry.ExpiresAt.After(time.Now()) {
		return nil, AuthorizationStamp{}, false
	}
	copy := copyPrequeueEntryForRead(entry)
	if copy.TargetEpisode != nil && copy.TargetEpisode.Numbering != nil {
		numbering := *copy.TargetEpisode.Numbering
		copy.TargetEpisode.Numbering = &numbering
	}
	if copy.MediaEvidence != nil && copy.MediaEvidence.TargetEpisode != nil && copy.MediaEvidence.TargetEpisode.Image != nil {
		image := *copy.MediaEvidence.TargetEpisode.Image
		copy.MediaEvidence.TargetEpisode.Image = &image
	}
	if copy.TrackSelection != nil {
		selection := *copy.TrackSelection
		clone := func(track *models.SelectedTrack) *models.SelectedTrack {
			if track == nil {
				return nil
			}
			value := *track
			return &value
		}
		selection.Audio = clone(selection.Audio)
		selection.Subtitle = clone(selection.Subtitle)
		selection.TextSubtitle = clone(selection.TextSubtitle)
		copy.TrackSelection = &selection
	}
	return copy, AuthorizationStamp{s, entry.authorizationAttempt, s.generation.Load()}, true
}

// Revision is an opaque process/attempt discriminator, not an authorization.
func (s AuthorizationStamp) Revision() string {
	if !s.Valid() {
		return ""
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%p:%d:%d", s.store, s.attempt, s.generation)))
	return hex.EncodeToString(sum[:])
}
