package users

import (
	"context"
	"errors"
	"reflect"
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
	confirmed map[string]models.User
	revisions map[string]datastore.UserRevision
	uncertain bool
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
				s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
			s.persistence.uncertain = true
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
