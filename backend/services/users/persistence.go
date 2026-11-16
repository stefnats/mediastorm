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
