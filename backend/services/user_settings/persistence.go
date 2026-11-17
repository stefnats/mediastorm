package user_settings

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
)

type settingsPersistence struct {
	records   map[string]datastore.UserSettingsRecord
	confirmed map[string]models.UserSettings
	uncertain map[string]bool
}

// cloneSettingsValue isolates every mutable pointer, map, slice and raw field.
// The original untyped JSON remains private to its datastore revision.
func cloneSettingsValue(value models.UserSettings) models.UserSettings {
	return cloneSettingsReflect(reflect.ValueOf(value)).Interface().(models.UserSettings)
}
func cloneSettingsReflect(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type().Elem())
		result.Elem().Set(cloneSettingsReflect(value.Elem()))
		return result
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.New(value.Type()).Elem()
		result.Set(cloneSettingsReflect(value.Elem()))
		return result
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneSettingsReflect(value.Index(i)))
		}
		return result
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		result := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			result.SetMapIndex(iter.Key(), cloneSettingsReflect(iter.Value()))
		}
		return result
	case reflect.Struct:
		result := reflect.New(value.Type()).Elem()
		result.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).PkgPath == "" {
				result.Field(i).Set(cloneSettingsReflect(value.Field(i)))
			}
		}
		return result
	case reflect.Array:
		result := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			result.Index(i).Set(cloneSettingsReflect(value.Index(i)))
		}
		return result
	default:
		return value
	}
}
func settingsStorageContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func (s *Service) installPGRecordsLocked(records []datastore.UserSettingsRecord) {
	s.pg = &settingsPersistence{records: make(map[string]datastore.UserSettingsRecord), confirmed: make(map[string]models.UserSettings), uncertain: make(map[string]bool)}
	s.settings = make(map[string]models.UserSettings)
	for _, r := range records {
		s.installPGRecordLocked(r)
	}
}
func (s *Service) installPGRecordLocked(r datastore.UserSettingsRecord) {
	s.pg.records[r.ID] = r
	delete(s.pg.confirmed, r.ID)
	delete(s.settings, r.ID)
	if r.Settings != nil {
		s.pg.confirmed[r.ID] = cloneSettingsValue(*r.Settings)
		s.settings[r.ID] = cloneSettingsValue(*r.Settings)
	}
}
func (s *Service) restorePGConfirmedLocked() {
	s.settings = make(map[string]models.UserSettings, len(s.pg.confirmed))
	for id, value := range s.pg.confirmed {
		s.settings[id] = cloneSettingsValue(value)
	}
}
func (s *Service) pgTargetErrorLocked(id string) error {
	if !s.useDB() {
		return nil
	}
	if s.pg == nil {
		return datastore.ErrUserSettingsUnavailable
	}
	if s.pg.uncertain[id] {
		return datastore.ErrUserSettingsOutcomeUnknown
	}
	return nil
}

// Known and quarantined targets never acquire a replacement snapshot here.
// A genuinely new user is read before any of its desired values are calculated.
func (s *Service) ensurePGTargetLocked(ctx context.Context, id string) error {
	if err := s.pgTargetErrorLocked(id); err != nil {
		return err
	}
	if !s.useDB() {
		return nil
	}
	if _, ok := s.pg.records[id]; ok {
		return nil
	}
	r, err := s.store.LoadUserSettingsTarget(ctx, id)
	if err != nil {
		return err
	}
	s.installPGRecordLocked(r)
	return nil
}

// ResyncUserSettings is an explicit reconciliation, never a retry after conflict
// or unknown commit. It performs no migration write and makes no auth decision.
func (s *Service) ResyncUserSettings(ctx context.Context) error {
	if !s.useDB() {
		return datastore.ErrUserSettingsUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.store.LoadUserSettingsSnapshot(ctx)
	if err != nil {
		return err
	}
	s.installPGRecordsLocked(records)
	return nil
}

func (s *Service) loadPGLocked() error {
	ctx, cancel := settingsStorageContext()
	defer cancel()
	// Existing cleanup is a targeted JSONB path removal, not typed replacement.
	if _, err := s.store.UserSettings().ClearCleanPostersOverrides(ctx); err != nil {
		return err
	}
	records, err := s.store.LoadUserSettingsSnapshot(ctx)
	if err != nil {
		return err
	}
	s.installPGRecordsLocked(records)
	for id, us := range s.settings {
		if !us.Display.NavigationTabVisibilityIncludesSports {
			if tabs, changed := models.AddMissingSportsNavigationTab(us.Display.NavigationTabVisibility); changed {
				us.Display.NavigationTabVisibility = tabs
			}
			us.Display.NavigationTabVisibilityIncludesSports = true
		}
		_ = reconcileProfileHomeShelves(&us)
		for i := range us.HomeShelves.Shelves {
			shelf := &us.HomeShelves.Shelves[i]
			if shelf.ID == "trending-movies" || shelf.ID == "trending-tv" {
				shelf.HideUnreleased = false
			}
		}
		s.settings[id] = us
	}
	return s.persistPGLocked(ctx, nil, nil)
}

func (s *Service) persistPGLocked(ctx context.Context, checks, deletes []string) error {
	if s.pg == nil {
		return datastore.ErrUserSettingsUnavailable
	}
	checkSet, deleteSet := map[string]bool{}, map[string]bool{}
	for _, id := range checks {
		checkSet[id] = true
	}
	for _, id := range deletes {
		deleteSet[id] = true
	}
	ids := map[string]bool{}
	for id := range s.pg.confirmed {
		ids[id] = true
	}
	for id := range s.settings {
		ids[id] = true
	}
	for id := range checkSet {
		ids[id] = true
	}
	for id := range deleteSet {
		ids[id] = true
	}
	var changes []datastore.UserSettingsChange
	for id := range ids {
		before, existed := s.pg.confirmed[id]
		after, present := s.settings[id]
		changed := existed != present || !reflect.DeepEqual(before, after)
		if !changed && !checkSet[id] && !deleteSet[id] {
			continue
		}
		if err := s.pgTargetErrorLocked(id); err != nil {
			s.restorePGConfirmedLocked()
			return err
		}
		r, ok := s.pg.records[id]
		if !ok {
			s.restorePGConfirmedLocked()
			return datastore.ErrUserSettingsConflict
		}
		kind := datastore.UserSettingsCheck
		if changed {
			kind = datastore.UserSettingsPatch
		}
		if deleteSet[id] {
			kind = datastore.UserSettingsDelete
		}
		changes = append(changes, datastore.UserSettingsChange{Kind: kind, Expected: r.Revision, Settings: after})
	}
	if len(changes) == 0 {
		return nil
	}
	// The datastore sorts by original ID. Keep the expected revisions in that
	// same stable order for the receipt comparison, without exposing revision data.
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		before, existed := s.pg.confirmed[id]
		after, present := s.settings[id]
		if existed != present || !reflect.DeepEqual(before, after) || checkSet[id] || deleteSet[id] {
			ordered = append(ordered, id)
		}
	}
	sort.Strings(ordered)
	expected := make([]datastore.UserSettingsRevision, len(ordered))
	for i, id := range ordered {
		expected[i] = s.pg.records[id].Revision
	}
	receipt, err := s.store.CommitUserSettingsChanges(ctx, changes)
	if err != nil {
		s.restorePGConfirmedLocked()
		if errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
			for _, id := range ordered {
				s.pg.uncertain[id] = true
			}
		}
		return err
	}
	defer receipt.Close()
	records, err := s.store.AcceptUserSettingsCommit(ctx, receipt, expected)
	if err != nil {
		s.restorePGConfirmedLocked()
		for _, id := range ordered {
			s.pg.uncertain[id] = true
		}
		return datastore.ErrUserSettingsOutcomeUnknown
	}
	for _, r := range records {
		s.installPGRecordLocked(r)
	}
	return ctx.Err()
}

func (s *Service) applyPGKidsDefaultsLocked(ids []string) (int, error) {
	ctx, cancel := settingsStorageContext()
	defer cancel()
	for _, id := range ids {
		if err := s.ensurePGTargetLocked(ctx, id); err != nil {
			return 0, err
		}
	}
	changed := 0
	for _, id := range ids {
		value := s.settings[id]
		if value.Display.SimpleMode != nil {
			continue
		}
		value.Display.SimpleMode = models.BoolPtr(true)
		s.settings[id] = value
		changed++
	}
	if err := s.persistPGLocked(ctx, ids, nil); err != nil {
		return 0, err
	}
	return changed, nil
}

func (s *Service) deletePGLocked(id string) error {
	ctx, cancel := settingsStorageContext()
	defer cancel()
	if err := s.ensurePGTargetLocked(ctx, id); err != nil {
		return err
	}
	delete(s.settings, id)
	return s.persistPGLocked(ctx, nil, []string{id})
}
