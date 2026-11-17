package users

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"novastream/internal/datastore"
	"novastream/models"
)

var ErrPersistenceUncertain = errors.New("profile persistence outcome unknown; explicit resync required")

const userPersistenceBudget = 10 * time.Second

// This is a last-confirmed local projection, not an authoritative policy reader.
// An unchanged stale profile is never written merely because another changes.
type persistenceState struct {
	confirmed       map[string]models.User
	revisions       map[string]datastore.UserRevision
	uncertain       bool
	unknownTargets  map[string]*storageUnknownMarker
	unknownUnscoped bool
}

func cloneUser(u models.User) models.User {
	if u.KidsAllowedLists != nil {
		u.KidsAllowedLists = append([]string{}, u.KidsAllowedLists...)
	}
	return u
}
func cloneUsers(input map[string]models.User) map[string]models.User {
	result := make(map[string]models.User, len(input))
	for id, u := range input {
		result[id] = cloneUser(u)
	}
	return result
}
func (s *Service) ensurePersistenceLocked() {
	if s.persistence == nil {
		s.persistence = &persistenceState{confirmed: map[string]models.User{}, revisions: map[string]datastore.UserRevision{}}
	}
}
func (s *Service) confirmJSONLocked() {
	s.ensurePersistenceLocked()
	s.users = cloneUsers(s.users)
	s.persistence.confirmed = cloneUsers(s.users)
}
func (s *Service) installDatabaseSnapshotLocked(records []datastore.UserWriteRecord) {
	// Operation-owned unknown creation cannot be settled by a generic reset.
	if s.hasCreationUnknownLocked() {
		return
	}
	users := make(map[string]models.User, len(records))
	revisions := make(map[string]datastore.UserRevision, len(records))
	for _, record := range records {
		users[record.User.ID] = cloneUser(record.User)
		revisions[record.User.ID] = record.Revision
	}
	s.users = users
	s.persistence = &persistenceState{confirmed: cloneUsers(users), revisions: revisions}
}
func (s *Service) loadDatabaseLocked(ctx context.Context) error {
	if s.hasCreationUnknownLocked() {
		return ErrPersistenceUncertain
	}
	records, err := s.store.LoadUserWriteSnapshot(ctx)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	s.installDatabaseSnapshotLocked(records)
	return nil
}

// ResyncStorage explicitly replaces the PG cache and revisions. It never
// replays a mutation, refreshes auth or falls back to JSON on a DB error.
func (s *Service) ResyncStorage(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || s == nil || !s.useDB() {
		return datastore.ErrUserWriteUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadDatabaseLocked(ctx)
}
func (s *Service) bootstrapDefaultLocked() error {
	if s.hasCreationUnknownLocked() {
		return ErrPersistenceUncertain
	}
	now := time.Now().UTC()
	candidate := models.User{ID: uuid.NewString(), AccountID: models.MasterAccountID, Name: models.DefaultUserName, ActivityPrivacy: models.ActivityPrivacySharedAnonymous, CreatedAt: now, UpdatedAt: now}
	ctx, cancel := context.WithTimeout(context.Background(), userPersistenceBudget)
	defer cancel()
	records, err := s.store.EnsureDefaultUserSnapshot(ctx, candidate)
	if err != nil {
		return err
	}
	s.installDatabaseSnapshotLocked(records)
	return nil
}

// saveLocked is called with mu held by existing setters. They stage changes
// behind that lock; only an acknowledged persistence result is published.
// Legacy setters have no request context, so this budget is NOT caller cancel
// or fresh actor binding. Protected parent operations remain a separate seam.
func (s *Service) saveLocked() error {
	s.ensurePersistenceLocked()
	rollback := func() { s.users = cloneUsers(s.persistence.confirmed) }
	if s.useDB() {
		if s.persistence.uncertain {
			rollback()
			return ErrPersistenceUncertain
		}
		changes := []datastore.UserChange{}
		for id, user := range s.users {
			previous, known := s.persistence.confirmed[id]
			if known && reflect.DeepEqual(cloneUser(previous), cloneUser(user)) {
				continue
			}
			kind := datastore.UserInsert
			if known {
				kind = datastore.UserReplace
			}
			changes = append(changes, datastore.UserChange{Kind: kind, ID: id, User: cloneUser(user), Expected: s.persistence.revisions[id]})
		}
		for id := range s.persistence.confirmed {
			if _, exists := s.users[id]; !exists {
				changes = append(changes, datastore.UserChange{Kind: datastore.UserDelete, ID: id, Expected: s.persistence.revisions[id]})
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), userPersistenceBudget)
		defer cancel()
		result, err := s.store.CommitUserChanges(ctx, changes)
		if err != nil {
			rollback()
			if errors.Is(err, datastore.ErrUserWriteOutcomeUnknown) {
				ids := make([]string, 0, len(changes))
				for _, change := range changes {
					ids = append(ids, change.ID)
				}
				s.markStorageUnknownLocked(ids)
				return ErrPersistenceUncertain
			}
			return err
		}
		for _, record := range result.Records {
			s.users[record.User.ID] = cloneUser(record.User)
			s.persistence.revisions[record.User.ID] = record.Revision
		}
		for _, id := range result.Deleted {
			delete(s.persistence.revisions, id)
		}
		s.users = cloneUsers(s.users)
		s.persistence.confirmed = cloneUsers(s.users)
		return nil
	}
	// Same JSON format/migrations/atomic rename as before. This is not a new
	// multi-process JSON CAS or a transaction over profile icon files.
	if err := s.saveJSONLocked(); err != nil {
		rollback()
		return err
	}
	s.confirmJSONLocked()
	return nil
}

// AcknowledgePinIssuance advances only the exact local profile revision changed
// by this request's known PG PinLength migration. It never reloads other rows or
// replays a pending edit, and it does not publish a profile-unlock grant.
func (s *Service) AcknowledgePinIssuance(ctx context.Context, profileID string, receipt *datastore.PGPinCommitReceipt) error {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return datastore.ErrPGPinUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return ErrPersistenceUncertain
	}
	current, ok := s.users[profileID]
	confirmed, known := s.persistence.confirmed[profileID]
	revision, present := s.persistence.revisions[profileID]
	if !ok || !known || !present || !reflect.DeepEqual(current, confirmed) {
		return datastore.ErrUserWriteConflict
	}
	record, changed, err := s.store.PinIssuanceUserRecord(ctx, profileID, revision, receipt)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if !changed {
		return nil
	}
	s.users[profileID] = cloneUser(record.User)
	s.persistence.confirmed[profileID] = cloneUser(record.User)
	s.persistence.revisions[profileID] = record.Revision
	return nil
}

// CommitPGPinChange confirms only the row returned by this exact PIN intent.
// Other last-confirmed profiles and waiting local edits are not reloaded.
func (s *Service) CommitPGPinChange(ctx context.Context, intent *datastore.PGPinChangeIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrPGPinUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	// Only the original confirmed local revision may be advanced. A PIN
	// transaction is not a general resync for dirty or externally changed rows.
	targetID, err := s.store.PinChangeTarget(intent)
	if err != nil {
		return models.User{}, err
	}
	revision, found := s.persistence.revisions[targetID]
	if !found {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if _, err = s.store.CheckPinChangeRevision(intent, revision); err != nil {
		return models.User{}, err
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	if !known || !present || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	record, err := s.store.CommitPinChange(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrPGPinOutcomeUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	s.users[record.User.ID] = cloneUser(record.User)
	s.persistence.confirmed[record.User.ID] = cloneUser(record.User)
	s.persistence.revisions[record.User.ID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentMode advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentMode(ctx context.Context, targetID string, intent *datastore.ParentModeIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentModeUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentModeTarget(intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	record, err := s.store.CommitParentMode(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentModeUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentListRemoval advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentListRemoval(ctx context.Context, targetID string, intent *datastore.ParentListRemovalIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentListRemovalUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentListRemovalTarget(intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	record, err := s.store.CommitParentListRemoval(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentListRemovalUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentListAdd advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentListAdd(ctx context.Context, targetID string, intent *datastore.ParentListWriteIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentListWriteUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentListAddTarget(ctx, intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	receipt, err := s.store.CommitParentListAdd(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentListWriteUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	record, err := s.store.AcknowledgeParentListAdd(ctx, intent, receipt, revision)
	if err != nil {
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentListReplace advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentListReplace(ctx context.Context, targetID string, intent *datastore.ParentListWriteIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentListWriteUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentListReplaceTarget(ctx, intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	receipt, err := s.store.CommitParentListReplace(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentListWriteUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	record, err := s.store.AcknowledgeParentListReplace(ctx, intent, receipt, revision)
	if err != nil {
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentRatingGeneric advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentRatingGeneric(ctx context.Context, targetID string, intent *datastore.ParentRatingIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentRatingUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentRatingGenericTarget(ctx, intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	receipt, err := s.store.CommitParentRatingGeneric(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentRatingUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	record, err := s.store.AcknowledgeParentRatingGeneric(ctx, intent, receipt, revision)
	if err != nil {
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentRatingMovie advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentRatingMovie(ctx context.Context, targetID string, intent *datastore.ParentRatingIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentRatingUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentRatingMovieTarget(ctx, intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	receipt, err := s.store.CommitParentRatingMovie(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentRatingUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	record, err := s.store.AcknowledgeParentRatingMovie(ctx, intent, receipt, revision)
	if err != nil {
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentRatingTV advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentRatingTV(ctx context.Context, targetID string, intent *datastore.ParentRatingIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentRatingUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentRatingTVTarget(ctx, intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	receipt, err := s.store.CommitParentRatingTV(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentRatingUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	record, err := s.store.AcknowledgeParentRatingTV(ctx, intent, receipt, revision)
	if err != nil {
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// CommitParentRatingPair advances only the exact confirmed target projection. A known
// server commit may remain after cancellation; unknown outcomes quarantine all
// later local writes until the existing explicit storage resync.
func (s *Service) CommitParentRatingPair(ctx context.Context, targetID string, intent *datastore.ParentRatingIntent) (models.User, error) {
	if s == nil || !s.useDB() || ctx == nil || ctx.Err() != nil {
		return models.User{}, datastore.ErrParentRatingUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return models.User{}, ErrPersistenceUncertain
	}
	current, known := s.users[targetID]
	confirmed, present := s.persistence.confirmed[targetID]
	revision, exists := s.persistence.revisions[targetID]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return models.User{}, datastore.ErrUserWriteConflict
	}
	if selected, err := s.store.ParentRatingPairTarget(ctx, intent, revision); err != nil || selected != targetID {
		if err != nil {
			return models.User{}, err
		}
		return models.User{}, datastore.ErrUserWriteConflict
	}
	receipt, err := s.store.CommitParentRatingPair(intent)
	if err != nil {
		if errors.Is(err, datastore.ErrParentRatingUnknown) {
			s.markStorageUnknownLocked([]string{targetID})
		}
		return models.User{}, err
	}
	record, err := s.store.AcknowledgeParentRatingPair(ctx, intent, receipt, revision)
	if err != nil {
		return models.User{}, err
	}
	s.users[targetID] = cloneUser(record.User)
	s.persistence.confirmed[targetID] = cloneUser(record.User)
	s.persistence.revisions[targetID] = record.Revision
	if err = ctx.Err(); err != nil {
		return models.User{}, err
	}
	return cloneUser(record.User), nil
}

// storageUnknownMarker distinguishes successive uncertain operations even for
// the same ID. The old global fail-closed write gate remains authoritative.
type storageUnknownMarker struct {
	identity byte
	creation string
}

func (s *Service) markStorageUnknownLocked(ids []string) {
	p := s.persistence
	if p.uncertain && p.unknownTargets == nil {
		p.unknownUnscoped = true
	}
	if p.unknownTargets == nil {
		p.unknownTargets = make(map[string]*storageUnknownMarker)
	}
	p.uncertain = true
	if len(ids) == 0 {
		p.unknownUnscoped = true
	}
	for _, id := range ids {
		marker := &storageUnknownMarker{identity: 1}
		if prior := p.unknownTargets[id]; prior != nil {
			marker.creation = prior.creation
		}
		p.unknownTargets[id] = marker
	}
}

type StorageRecoveryTicket struct{ state *storageRecoveryTicketState }
type storageRecoveryTicketState struct {
	creation                                          string
	creationRead                                      *datastore.ProfileCreationRead
	self                                              *StorageRecoveryTicket
	service                                           *Service
	ctx                                               context.Context
	target                                            string
	slot                                              *datastore.ProfileRecoverySlot
	current, confirmed                                models.User
	currentPresent, confirmedPresent, revisionPresent bool
	revision                                          datastore.UserRevision
	marker                                            *storageUnknownMarker
	closed                                            atomic.Bool
}

func (StorageRecoveryTicket) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (StorageRecoveryTicket) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("StorageRecoveryTicket(opaque)"))
}
func (t *StorageRecoveryTicket) Close() {
	if t != nil && t.state != nil && t.state.self == t {
		t.state.closed.Store(true)
		t.state.slot.Close()
	}
}

// PrepareStorageRecovery captures only a clean target before the shared read.
// No authentication or SQL takes place here. Later public coordination must
// provide its own original actor/PIN checks and shared installation locks.
func (s *Service) PrepareStorageRecovery(ctx context.Context, read *datastore.ProfileRecoveryRead) (*StorageRecoveryTicket, error) {
	return s.prepareStorageRecovery(ctx, read, "", nil)
}
func (s *Service) prepareStorageRecovery(ctx context.Context, read *datastore.ProfileRecoveryRead, creation string, creationRead *datastore.ProfileCreationRead) (*StorageRecoveryTicket, error) {
	if s == nil || !s.useDB() || ctx == nil {
		return nil, datastore.ErrProfileRecoveryUnavailable
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensurePersistenceLocked()
	target, err := s.store.ProfileRecoveryTarget(ctx, read)
	if err != nil {
		return nil, err
	}
	if marker := s.persistence.unknownTargets[target]; marker != nil && (creation == "" && marker.creation != "" || creation != "" && marker.creation != creation) {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	current, present := s.users[target]
	confirmed, known := s.persistence.confirmed[target]
	if present != known || !reflect.DeepEqual(current, confirmed) {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	var slot *datastore.ProfileRecoverySlot
	if creationRead != nil {
		slot, err = s.store.ReserveProfileCreationUsersRecovery(ctx, creationRead)
	} else {
		slot, err = s.store.ReserveUsersRecovery(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	if s.persistence.uncertain && s.persistence.unknownTargets == nil {
		s.persistence.unknownUnscoped = true
	}
	revision, hasRevision := s.persistence.revisions[target]
	ticket := &StorageRecoveryTicket{}
	ticket.state = &storageRecoveryTicketState{creation: creation, creationRead: creationRead, self: ticket, service: s, ctx: ctx, target: target, slot: slot, current: cloneUser(current), confirmed: cloneUser(confirmed), currentPresent: present, confirmedPresent: known, revision: revision, revisionPresent: hasRevision, marker: s.persistence.unknownTargets[target]}
	return ticket, nil
}
func (s *Service) validateStorageRecoveryLocked(ctx context.Context, ticket *StorageRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) error {
	if ticket == nil || ticket.state == nil || ticket.state.self != ticket || ticket.state.service != s || ticket.state.ctx != ctx || ticket.state.closed.Load() {
		return datastore.ErrProfileRecoveryChanged
	}
	if ctx == nil {
		return datastore.ErrProfileRecoveryChanged
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	t := ticket.state
	p := s.persistence
	if p != nil {
		if marker := p.unknownTargets[t.target]; marker != nil && (t.creation == "" && marker.creation != "" || t.creation != "" && marker.creation != t.creation) {
			return datastore.ErrProfileRecoveryChanged
		}
	}
	if p == nil {
		return datastore.ErrProfileRecoveryChanged
	}
	current, present := s.users[t.target]
	confirmed, known := p.confirmed[t.target]
	revision, hasRevision := p.revisions[t.target]
	if present != t.currentPresent || known != t.confirmedPresent || hasRevision != t.revisionPresent || revision != t.revision || !reflect.DeepEqual(current, t.current) || !reflect.DeepEqual(confirmed, t.confirmed) || p.unknownTargets[t.target] != t.marker {
		return datastore.ErrProfileRecoveryChanged
	}
	if t.creationRead != nil {
		return s.store.ValidateProfileCreationRecoverySlot(ctx, t.creationRead, t.slot, snapshot)
	}
	return s.store.ValidateProfileRecoverySlot(ctx, t.slot, snapshot)
}

// ValidateStorageRecovery does not consume or install. It cannot make a later
// installation atomic with a second service; Install always rechecks locally.
func (s *Service) ValidateStorageRecovery(ctx context.Context, ticket *StorageRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) error {
	if s == nil || !s.useDB() {
		return datastore.ErrProfileRecoveryUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.validateStorageRecoveryLocked(ctx, ticket, snapshot)
}
func (s *Service) InstallStorageRecovery(ctx context.Context, ticket *StorageRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) (datastore.ProfileRecoveryStatus, error) {
	var status datastore.ProfileRecoveryStatus
	if s == nil || !s.useDB() {
		return status, datastore.ErrProfileRecoveryUnavailable
	}
	if ticket != nil && ticket.state != nil && ticket.state.creation != "" {
		return status, datastore.ErrProfileRecoveryChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateStorageRecoveryLocked(ctx, ticket, snapshot); err != nil {
		return status, err
	}
	if ticket.state.creation != "" {
		return status, datastore.ErrProfileRecoveryChanged
	}
	t := ticket.state
	record, present, err := s.store.TakeUsersRecovery(ctx, t.slot, snapshot)
	if err != nil {
		return status, err
	}
	t.closed.Store(true)
	if ctx.Err() != nil {
		return status, ctx.Err()
	}
	// Historical read bookkeeping only. No global reload, bootstrap or retry.
	if present {
		s.users[t.target] = cloneUser(record.User)
		s.persistence.confirmed[t.target] = cloneUser(record.User)
	} else {
		delete(s.users, t.target)
		delete(s.persistence.confirmed, t.target)
	}
	s.persistence.revisions[t.target] = record.Revision
	if t.marker != nil {
		delete(s.persistence.unknownTargets, t.target)
	}
	if !s.persistence.unknownUnscoped && s.persistence.unknownTargets != nil && len(s.persistence.unknownTargets) == 0 {
		s.persistence.uncertain = false
	}
	status.TargetPresent = present
	status.PendingUncertainty = s.persistence.uncertain
	return status, nil
}

// StorageRecoveryLease owns this service's mutex until Close. The coordinator
// acquires Users before UserSettings and closes in reverse order. It exposes no
// mutex, and ordinary recovery Install cannot bypass its reserved datastore slot.
type StorageRecoveryLease struct{ state *storageRecoveryLeaseState }
type storageRecoveryLeaseState struct {
	creationProof              *datastore.ProfileCreationRecoveryLease
	mu                         sync.Mutex
	self                       *StorageRecoveryLease
	service                    *Service
	ticket                     *StorageRecoveryTicket
	proof                      *datastore.ProfileRecoveryLease
	current, confirmed         models.User
	revision                   datastore.UserRevision
	present, installed, closed bool
	status                     datastore.ProfileRecoveryStatus
}

func (StorageRecoveryLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (StorageRecoveryLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("StorageRecoveryLease(opaque)"))
}
func (s *Service) AcquireStorageRecoveryLease(ctx context.Context, ticket *StorageRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) (*StorageRecoveryLease, error) {
	if s == nil || !s.useDB() {
		return nil, datastore.ErrProfileRecoveryUnavailable
	}
	s.mu.Lock()
	if err := s.validateStorageRecoveryLocked(ctx, ticket, snapshot); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var proof *datastore.ProfileRecoveryLease
	var creationProof *datastore.ProfileCreationRecoveryLease
	var record datastore.UserWriteRecord
	var present bool
	var err error
	if ticket.state.creationRead != nil {
		creationProof, record, present, err = s.store.BeginProfileCreationUsersRecoveryLease(ctx, ticket.state.creationRead, ticket.state.slot, snapshot)
	} else {
		proof, record, present, err = s.store.BeginUsersRecoveryLease(ctx, ticket.state.slot, snapshot)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	lease := &StorageRecoveryLease{}
	lease.state = &storageRecoveryLeaseState{self: lease, service: s, ticket: ticket, proof: proof, creationProof: creationProof, current: cloneUser(record.User), confirmed: cloneUser(record.User), revision: record.Revision, present: present}
	return lease, nil
}
func (l *StorageRecoveryLease) PairLease() *datastore.ProfileRecoveryLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	if l.state.ticket.state.creation != "" {
		return nil
	}
	return l.state.proof
}

// Profile is a private server-side prepared value. HTTP uses an explicit safe
// DTO and must never serialize this full model, its credentials or revision.
func (l *StorageRecoveryLease) Profile() (models.User, bool, error) {
	if l == nil || l.state == nil {
		return models.User{}, false, datastore.ErrProfileRecoveryChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed {
		return models.User{}, false, datastore.ErrProfileRecoveryChanged
	}
	return cloneUser(st.current), st.present, nil
}
func (st *storageRecoveryLeaseState) completeLocked() datastore.ProfileRecoveryStatus {
	if st.installed {
		return st.status
	}
	if !(st.creationProof != nil && st.creationProof.Paired() || st.creationProof == nil && st.proof.Paired()) {
		return datastore.ProfileRecoveryStatus{PendingUncertainty: true}
	}
	s := st.service
	t := st.ticket.state
	if t.creation != "" && !st.present && t.marker == nil && !t.currentPresent && !t.confirmedPresent && !t.revisionPresent {
		st.status = datastore.ProfileRecoveryStatus{PendingUncertainty: s.persistence.uncertain}
		t.closed.Store(true)
		st.installed = true
		return st.status
	}
	target := st.ticket.state.target
	if st.present {
		s.users[target] = st.current
		s.persistence.confirmed[target] = st.confirmed
	} else {
		delete(s.users, target)
		delete(s.persistence.confirmed, target)
	}
	s.persistence.revisions[target] = st.revision
	if st.ticket.state.marker != nil {
		delete(s.persistence.unknownTargets, target)
	}
	if !s.persistence.unknownUnscoped && s.persistence.unknownTargets != nil && len(s.persistence.unknownTargets) == 0 {
		s.persistence.uncertain = false
	}
	st.status = datastore.ProfileRecoveryStatus{TargetPresent: st.present, PendingUncertainty: s.persistence.uncertain}
	st.ticket.state.closed.Store(true)
	st.installed = true
	return st.status
}

// Complete has no context check, clone, query or fallible work after PairTake.
func (l *StorageRecoveryLease) Complete() datastore.ProfileRecoveryStatus {
	if l == nil || l.state == nil {
		return datastore.ProfileRecoveryStatus{PendingUncertainty: true}
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed && !st.installed {
		return datastore.ProfileRecoveryStatus{PendingUncertainty: true}
	}
	return st.completeLocked()
}
func (l *StorageRecoveryLease) Close() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed {
		return
	}
	// Pair consumption is irreversible local bookkeeping. Even late cancellation
	// or ordinary unwinding completes this owner's prepared ACK before unlocking.
	if st.creationProof != nil && st.creationProof.Retire() || st.creationProof == nil && st.proof.Retire() {
		st.completeLocked()
	}
	st.closed = true
	st.service.mu.Unlock()
}

// ParentKidsFlagLease holds only the selected confirmed Users projection. It is
// acquired before Settings and remains held through both compound cache ACKs.
type ParentKidsFlagLease struct{ state *parentKidsFlagLeaseState }
type parentKidsFlagLeaseState struct {
	mu                           sync.Mutex
	self                         *ParentKidsFlagLease
	service                      *Service
	intent                       *datastore.ParentKidsFlagIntent
	proof                        *datastore.ParentKidsFlagCacheLease
	target                       string
	current, confirmed, response models.User
	revision                     datastore.UserRevision
	staged, installed, closed    bool
}

func (*ParentKidsFlagLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentKidsFlagLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsFlagLease(opaque)"))
}
func (s *Service) AcquireParentKidsFlagLease(ctx context.Context, target string, intent *datastore.ParentKidsFlagIntent) (*ParentKidsFlagLease, error) {
	if s == nil || !s.useDB() || ctx == nil {
		return nil, datastore.ErrParentKidsFlagUnavailable
	}
	s.mu.Lock()
	fail := func(err error) (*ParentKidsFlagLease, error) { s.mu.Unlock(); return nil, err }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return fail(ErrPersistenceUncertain)
	}
	current, known := s.users[target]
	confirmed, present := s.persistence.confirmed[target]
	revision, exists := s.persistence.revisions[target]
	if !known || !present || !exists || !reflect.DeepEqual(current, confirmed) {
		return fail(datastore.ErrUserWriteConflict)
	}
	proof, err := s.store.BeginParentKidsFlagUsersLease(ctx, intent, revision)
	if err != nil {
		return fail(err)
	}
	value := &ParentKidsFlagLease{}
	value.state = &parentKidsFlagLeaseState{self: value, service: s, intent: intent, proof: proof, target: target}
	return value, nil
}
func (l *ParentKidsFlagLease) ReceiptLease() *datastore.ParentKidsFlagCacheLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	return l.state.proof
}
func (l *ParentKidsFlagLease) Commit() (*datastore.ParentKidsFlagReceipt, error) {
	if l == nil || l.state == nil {
		return nil, datastore.ErrParentKidsFlagChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed {
		return nil, datastore.ErrParentKidsFlagChanged
	}
	return st.service.store.CommitParentKidsFlag(st.intent)
}

// Stage performs every fallible validation and prepares isolated cache/response
// values before the single datastore receipt consumption point.
func (l *ParentKidsFlagLease) Stage(r *datastore.ParentKidsFlagReceipt) (models.User, error) {
	if l == nil || l.state == nil {
		return models.User{}, datastore.ErrParentKidsFlagChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed || st.staged {
		return models.User{}, datastore.ErrParentKidsFlagChanged
	}
	record, err := st.service.store.StageParentKidsFlagUsers(st.proof, r)
	if err != nil {
		return models.User{}, err
	}
	st.current = cloneUser(record.User)
	st.confirmed = cloneUser(record.User)
	st.response = cloneUser(record.User)
	st.revision = record.Revision
	st.staged = true
	return st.response, nil
}
func (st *parentKidsFlagLeaseState) completeLocked() {
	if st.installed || !st.staged || !st.proof.Taken() {
		return
	}
	s := st.service
	s.users[st.target] = st.current
	s.persistence.confirmed[st.target] = st.confirmed
	s.persistence.revisions[st.target] = st.revision
	st.installed = true
}

// Complete cannot fail or skip accepted bookkeeping due to late cancellation.
func (l *ParentKidsFlagLease) Complete() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self == l && !st.closed {
		st.completeLocked()
	}
}
func (l *ParentKidsFlagLease) Quarantine() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self == l && !st.closed && !st.proof.Taken() {
		st.service.markStorageUnknownLocked([]string{st.target})
	}
}
func (l *ParentKidsFlagLease) Close() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed {
		return
	}
	if st.proof.Retire() {
		st.completeLocked()
	}
	st.closed = true
	st.service.mu.Unlock()
}

// ParentProfileCreationLease holds only the selected confirmed Users projection. It is
// acquired before Settings and remains held through both compound cache ACKs.
type ParentProfileCreationLease struct {
	state *parentProfileCreationLeaseState
}
type parentProfileCreationLeaseState struct {
	mu                           sync.Mutex
	self                         *ParentProfileCreationLease
	service                      *Service
	intent                       *datastore.ParentProfileCreationIntent
	proof                        *datastore.ParentProfileCreationCacheLease
	target, operation            string
	current, confirmed, response models.User
	revision                     datastore.UserRevision
	staged, installed, closed    bool
}

func (*ParentProfileCreationLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentProfileCreationLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentProfileCreationLease(opaque)"))
}
func (s *Service) AcquireParentProfileCreationLease(ctx context.Context, intent *datastore.ParentProfileCreationIntent) (*ParentProfileCreationLease, error) {
	if s == nil || !s.useDB() || ctx == nil {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	s.mu.Lock()
	fail := func(err error) (*ParentProfileCreationLease, error) { s.mu.Unlock(); return nil, err }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	s.ensurePersistenceLocked()
	if s.persistence.uncertain {
		return fail(ErrPersistenceUncertain)
	}
	target, operation, err := s.store.ParentProfileCreationTarget(ctx, intent)
	if err != nil {
		return fail(err)
	}
	_, current := s.users[target]
	_, confirmed := s.persistence.confirmed[target]
	_, revision := s.persistence.revisions[target]
	if current || confirmed || revision || s.persistence.unknownTargets[target] != nil {
		return fail(datastore.ErrUserWriteConflict)
	}
	proof, err := s.store.BeginParentProfileCreationUsersLease(ctx, intent)
	if err != nil {
		return fail(err)
	}
	value := &ParentProfileCreationLease{}
	value.state = &parentProfileCreationLeaseState{self: value, service: s, intent: intent, proof: proof, target: target, operation: operation}
	return value, nil
}
func (l *ParentProfileCreationLease) ReceiptLease() *datastore.ParentProfileCreationCacheLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	return l.state.proof
}
func (l *ParentProfileCreationLease) Commit() (*datastore.ParentProfileCreationReceipt, error) {
	if l == nil || l.state == nil {
		return nil, datastore.ErrParentProfileCreationChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed {
		return nil, datastore.ErrParentProfileCreationChanged
	}
	return st.service.store.CommitParentProfileCreation(st.intent)
}

// Stage performs every fallible validation and prepares isolated cache/response
// values before the single datastore receipt consumption point.
func (l *ParentProfileCreationLease) Stage(r *datastore.ParentProfileCreationReceipt) (models.User, error) {
	if l == nil || l.state == nil {
		return models.User{}, datastore.ErrParentProfileCreationChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed || st.staged {
		return models.User{}, datastore.ErrParentProfileCreationChanged
	}
	record, err := st.service.store.StageParentProfileCreationUsers(st.proof, r)
	if err != nil {
		return models.User{}, err
	}
	st.current = cloneUser(record.User)
	st.confirmed = cloneUser(record.User)
	st.response = cloneUser(record.User)
	st.revision = record.Revision
	st.staged = true
	return st.response, nil
}
func (st *parentProfileCreationLeaseState) completeLocked() {
	if st.installed || !st.staged || !st.proof.Taken() {
		return
	}
	s := st.service
	s.users[st.target] = st.current
	s.persistence.confirmed[st.target] = st.confirmed
	s.persistence.revisions[st.target] = st.revision
	st.installed = true
}

// Complete cannot fail or skip accepted bookkeeping due to late cancellation.
func (l *ParentProfileCreationLease) Complete() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self == l && !st.closed {
		st.completeLocked()
	}
}
func (l *ParentProfileCreationLease) Quarantine() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self == l && !st.closed && !st.proof.Taken() {
		// The held clean absence lease owns this exact operation marker. Never
		// overwrite a foreign, unassigned or successor uncertainty marker.
		if st.service.persistence.unknownTargets[st.target] == nil {
			st.service.markStorageUnknownLocked([]string{st.target})
			st.service.persistence.unknownTargets[st.target].creation = st.operation
		}
	}
}
func (l *ParentProfileCreationLease) Close() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed {
		return
	}
	if st.proof.Retire() {
		st.completeLocked()
	}
	st.closed = true
	st.service.mu.Unlock()
}

func (s *Service) hasCreationUnknownLocked() bool {
	if s.persistence != nil {
		for _, marker := range s.persistence.unknownTargets {
			if marker != nil && marker.creation != "" {
				return true
			}
		}
	}
	return false
}

// PrepareProfileCreationRecovery captures the operation-bound local target
// before SQL. Only this typed path may resolve its matching creation marker.
func (s *Service) PrepareProfileCreationRecovery(ctx context.Context, read *datastore.ProfileCreationRead) (*StorageRecoveryTicket, error) {
	if s == nil || !s.useDB() {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	inner, operation, err := s.store.ProfileCreationRecoveryRead(ctx, read)
	if err != nil {
		return nil, err
	}
	return s.prepareStorageRecovery(ctx, inner, operation, read)
}
func (s *Service) AcquireProfileCreationRecoveryLease(ctx context.Context, t *StorageRecoveryTicket, snapshot *datastore.ProfileCreationResultSnapshot) (*StorageRecoveryLease, error) {
	if s == nil || !s.useDB() || t == nil || t.state == nil || t.state.self != t || t.state.creation == "" {
		return nil, datastore.ErrParentProfileCreationChanged
	}
	projection, err := s.store.ProfileCreationRecoverySnapshot(ctx, t.state.creationRead, snapshot)
	if err != nil {
		return nil, err
	}
	lease, err := s.AcquireStorageRecoveryLease(ctx, t, projection)
	if err != nil {
		return nil, err
	}
	return lease, nil
}

func (l *StorageRecoveryLease) CreationPairLease() *datastore.ProfileCreationRecoveryLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	return l.state.creationProof
}
