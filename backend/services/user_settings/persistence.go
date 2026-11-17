package user_settings

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
)

type settingsPersistence struct {
	records        map[string]datastore.UserSettingsRecord
	confirmed      map[string]models.UserSettings
	uncertain      map[string]bool
	unknownMarkers map[string]*userSettingsUnknownMarker
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
	if s.hasCreationUnknownLocked() {
		return
	}
	s.pg = &settingsPersistence{records: make(map[string]datastore.UserSettingsRecord), confirmed: make(map[string]models.UserSettings), uncertain: make(map[string]bool), unknownMarkers: make(map[string]*userSettingsUnknownMarker)}
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
	if s.hasCreationUnknownLocked() {
		return datastore.ErrUserSettingsOutcomeUnknown
	}
	records, err := s.store.LoadUserSettingsSnapshot(ctx)
	if err != nil {
		return err
	}
	s.installPGRecordsLocked(records)
	return nil
}

func (s *Service) loadPGLocked() error {
	if s.hasCreationUnknownLocked() {
		return datastore.ErrUserSettingsOutcomeUnknown
	}
	ctx, cancel := settingsStorageContext()
	defer cancel()
	// Existing cleanup is a targeted JSONB path removal, not typed replacement.
	if _, err := s.store.UserSettings().ClearCleanPostersOverrides(ctx); err != nil {
		return err
	}
	if s.hasCreationUnknownLocked() {
		return datastore.ErrUserSettingsOutcomeUnknown
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
				s.markUserSettingsUnknownLocked([]string{id})
			}
		}
		return err
	}
	defer receipt.Close()
	records, err := s.store.AcceptUserSettingsCommit(ctx, receipt, expected)
	if err != nil {
		s.restorePGConfirmedLocked()
		for _, id := range ordered {
			s.markUserSettingsUnknownLocked([]string{id})
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

type userSettingsUnknownMarker struct {
	identity byte
	creation string
}

func (s *Service) markUserSettingsUnknownLocked(ids []string) {
	if s.pg.unknownMarkers == nil {
		s.pg.unknownMarkers = make(map[string]*userSettingsUnknownMarker)
	}
	for _, id := range ids {
		s.pg.uncertain[id] = true
		marker := &userSettingsUnknownMarker{identity: 1}
		if prior := s.pg.unknownMarkers[id]; prior != nil {
			marker.creation = prior.creation
		}
		s.pg.unknownMarkers[id] = marker
	}
}

type UserSettingsRecoveryTicket struct {
	state *userSettingsRecoveryTicketState
}
type userSettingsRecoveryTicketState struct {
	self                                            *UserSettingsRecoveryTicket
	service                                         *Service
	ctx                                             context.Context
	target                                          string
	slot                                            *datastore.ProfileRecoverySlot
	current, confirmed                              models.UserSettings
	currentPresent, confirmedPresent, recordPresent bool
	revision                                        datastore.UserSettingsRevision
	uncertain                                       bool
	marker                                          *userSettingsUnknownMarker
	closed                                          atomic.Bool
}

func (UserSettingsRecoveryTicket) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (UserSettingsRecoveryTicket) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("UserSettingsRecoveryTicket(opaque)"))
}
func (t *UserSettingsRecoveryTicket) Close() {
	if t != nil && t.state != nil && t.state.self == t {
		t.state.closed.Store(true)
		t.state.slot.Close()
	}
}

// PrepareUserSettingsRecovery reserves a clean local target before the common
// read. It never invokes first-target acquire, migrations or a whole-cache load.
func (s *Service) PrepareUserSettingsRecovery(ctx context.Context, read *datastore.ProfileRecoveryRead) (*UserSettingsRecoveryTicket, error) {
	if s == nil || !s.useDB() || ctx == nil {
		return nil, datastore.ErrProfileRecoveryUnavailable
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pg == nil {
		return nil, datastore.ErrProfileRecoveryUnavailable
	}
	target, err := s.store.ProfileRecoveryTarget(ctx, read)
	if err != nil {
		return nil, err
	}
	if marker := s.pg.unknownMarkers[target]; marker != nil && marker.creation != "" {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	current, present := s.settings[target]
	confirmed, known := s.pg.confirmed[target]
	if present != known || !reflect.DeepEqual(current, confirmed) {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	slot, err := s.store.ReserveUserSettingsRecovery(ctx, read)
	if err != nil {
		return nil, err
	}
	record, recordPresent := s.pg.records[target]
	ticket := &UserSettingsRecoveryTicket{}
	ticket.state = &userSettingsRecoveryTicketState{self: ticket, service: s, ctx: ctx, target: target, slot: slot, current: cloneSettingsValue(current), confirmed: cloneSettingsValue(confirmed), currentPresent: present, confirmedPresent: known, recordPresent: recordPresent, revision: record.Revision, uncertain: s.pg.uncertain[target], marker: s.pg.unknownMarkers[target]}
	return ticket, nil
}
func (s *Service) validateUserSettingsRecoveryLocked(ctx context.Context, ticket *UserSettingsRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) error {
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
	p := s.pg
	if p != nil {
		if marker := p.unknownMarkers[t.target]; marker != nil && marker.creation != "" {
			return datastore.ErrProfileRecoveryChanged
		}
	}
	if p == nil {
		return datastore.ErrProfileRecoveryChanged
	}
	current, present := s.settings[t.target]
	confirmed, known := p.confirmed[t.target]
	record, recordPresent := p.records[t.target]
	if present != t.currentPresent || known != t.confirmedPresent || recordPresent != t.recordPresent || record.Revision != t.revision || !reflect.DeepEqual(current, t.current) || !reflect.DeepEqual(confirmed, t.confirmed) || p.uncertain[t.target] != t.uncertain || p.unknownMarkers[t.target] != t.marker {
		return datastore.ErrProfileRecoveryChanged
	}
	return s.store.ValidateProfileRecoverySlot(ctx, t.slot, snapshot)
}

// Validation is not a shared installation lock. A future coordinator must hold
// both caches together; these separate installs make no atomicity guarantee.
func (s *Service) ValidateUserSettingsRecovery(ctx context.Context, ticket *UserSettingsRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) error {
	if s == nil || !s.useDB() {
		return datastore.ErrProfileRecoveryUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.validateUserSettingsRecoveryLocked(ctx, ticket, snapshot)
}
func (s *Service) InstallUserSettingsRecovery(ctx context.Context, ticket *UserSettingsRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) (datastore.ProfileRecoveryStatus, error) {
	var status datastore.ProfileRecoveryStatus
	if s == nil || !s.useDB() {
		return status, datastore.ErrProfileRecoveryUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateUserSettingsRecoveryLocked(ctx, ticket, snapshot); err != nil {
		return status, err
	}
	t := ticket.state
	record, present, err := s.store.TakeUserSettingsRecovery(ctx, t.slot, snapshot)
	if err != nil {
		return status, err
	}
	t.closed.Store(true)
	if ctx.Err() != nil {
		return status, ctx.Err()
	}
	// An absent user's record remains observed. Normal first-target acquire must
	// not silently adopt a later reuse of this ID as a completely new profile.
	s.installPGRecordLocked(record)
	delete(s.pg.uncertain, t.target)
	delete(s.pg.unknownMarkers, t.target)
	status.TargetPresent = present
	for _, unknown := range s.pg.uncertain {
		if unknown {
			status.PendingUncertainty = true
			break
		}
	}
	return status, nil
}

// UserSettingsRecoveryLease is acquired only after the matching Users lease.
// Its prepared values are fully copied before the single shared PairTake.
type UserSettingsRecoveryLease struct {
	state *userSettingsRecoveryLeaseState
}
type userSettingsRecoveryLeaseState struct {
	mu                                      sync.Mutex
	self                                    *UserSettingsRecoveryLease
	service                                 *Service
	ticket                                  *UserSettingsRecoveryTicket
	proof                                   *datastore.ProfileRecoveryLease
	record                                  datastore.UserSettingsRecord
	current, confirmed                      models.UserSettings
	present, hasSettings, installed, closed bool
	status                                  datastore.ProfileRecoveryStatus
}

func (UserSettingsRecoveryLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (UserSettingsRecoveryLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("UserSettingsRecoveryLease(opaque)"))
}
func (s *Service) AcquireUserSettingsRecoveryLease(ctx context.Context, ticket *UserSettingsRecoveryTicket, snapshot *datastore.ProfileRecoverySnapshot) (*UserSettingsRecoveryLease, error) {
	if s == nil || !s.useDB() {
		return nil, datastore.ErrProfileRecoveryUnavailable
	}
	s.mu.Lock()
	if err := s.validateUserSettingsRecoveryLocked(ctx, ticket, snapshot); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	proof, record, present, err := s.store.BeginUserSettingsRecoveryLease(ctx, ticket.state.slot, snapshot)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	lease := &UserSettingsRecoveryLease{}
	st := &userSettingsRecoveryLeaseState{self: lease, service: s, ticket: ticket, proof: proof, record: record, present: present, hasSettings: record.Settings != nil}
	if record.Settings != nil {
		st.current = cloneSettingsValue(*record.Settings)
		st.confirmed = cloneSettingsValue(*record.Settings)
	}
	lease.state = st
	return lease, nil
}
func (l *UserSettingsRecoveryLease) PairLease() *datastore.ProfileRecoveryLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	return l.state.proof
}
func (st *userSettingsRecoveryLeaseState) completeLocked() datastore.ProfileRecoveryStatus {
	if st.installed {
		return st.status
	}
	if !st.proof.Paired() {
		return datastore.ProfileRecoveryStatus{PendingUncertainty: true}
	}
	s := st.service
	target := st.ticket.state.target
	s.pg.records[target] = st.record
	if st.hasSettings {
		s.settings[target] = st.current
		s.pg.confirmed[target] = st.confirmed
	} else {
		delete(s.settings, target)
		delete(s.pg.confirmed, target)
	}
	delete(s.pg.uncertain, target)
	delete(s.pg.unknownMarkers, target)
	st.status.TargetPresent = st.present
	for _, unknown := range s.pg.uncertain {
		if unknown {
			st.status.PendingUncertainty = true
			break
		}
	}
	st.ticket.state.closed.Store(true)
	st.installed = true
	return st.status
}
func (l *UserSettingsRecoveryLease) Complete() datastore.ProfileRecoveryStatus {
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
func (l *UserSettingsRecoveryLease) Close() {
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

// ParentKidsFlagDefaultsLease participates only for management enable. It holds
// this service's selected clean projection after the matching Users lease.
type ParentKidsFlagDefaultsLease struct {
	state *parentKidsFlagDefaultsLeaseState
}
type parentKidsFlagDefaultsLeaseState struct {
	mu                                 sync.Mutex
	self                               *ParentKidsFlagDefaultsLease
	service                            *Service
	proof                              *datastore.ParentKidsFlagCacheLease
	target                             string
	record                             datastore.UserSettingsRecord
	current, confirmed                 models.UserSettings
	present, staged, installed, closed bool
}

func (*ParentKidsFlagDefaultsLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentKidsFlagDefaultsLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsFlagDefaultsLease(opaque)"))
}
func (s *Service) AcquireParentKidsFlagDefaultsLease(ctx context.Context, target string, intent *datastore.ParentKidsFlagIntent, users *datastore.ParentKidsFlagCacheLease) (*ParentKidsFlagDefaultsLease, error) {
	if s == nil || !s.useDB() || ctx == nil {
		return nil, datastore.ErrParentKidsFlagUnavailable
	}
	s.mu.Lock()
	fail := func(err error) (*ParentKidsFlagDefaultsLease, error) { s.mu.Unlock(); return nil, err }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if err := s.ensurePGTargetLocked(ctx, target); err != nil {
		return fail(err)
	}
	before, known := s.pg.confirmed[target]
	current, present := s.settings[target]
	record, exists := s.pg.records[target]
	if !exists || known != present || !reflect.DeepEqual(before, current) {
		return fail(datastore.ErrUserSettingsConflict)
	}
	proof, err := s.store.BeginParentKidsFlagSettingsLease(ctx, intent, users, record.Revision)
	if err != nil {
		return fail(err)
	}
	value := &ParentKidsFlagDefaultsLease{}
	value.state = &parentKidsFlagDefaultsLeaseState{self: value, service: s, proof: proof, target: target}
	return value, nil
}
func (l *ParentKidsFlagDefaultsLease) ReceiptLease() *datastore.ParentKidsFlagCacheLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	return l.state.proof
}
func (l *ParentKidsFlagDefaultsLease) Stage(r *datastore.ParentKidsFlagReceipt) error {
	if l == nil || l.state == nil {
		return datastore.ErrParentKidsFlagChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed || st.staged {
		return datastore.ErrParentKidsFlagChanged
	}
	record, err := st.service.store.StageParentKidsFlagSettings(st.proof, r)
	if err != nil {
		return err
	}
	st.record = record
	st.present = record.Settings != nil
	if st.present {
		st.current = cloneSettingsValue(*record.Settings)
		st.confirmed = cloneSettingsValue(*record.Settings)
	}
	st.staged = true
	return nil
}
func (st *parentKidsFlagDefaultsLeaseState) completeLocked() {
	if st.installed || !st.staged || !st.proof.Taken() {
		return
	}
	s := st.service
	s.pg.records[st.target] = st.record
	if st.present {
		s.settings[st.target] = st.current
		s.pg.confirmed[st.target] = st.confirmed
	} else {
		delete(s.settings, st.target)
		delete(s.pg.confirmed, st.target)
	}
	st.installed = true
}
func (l *ParentKidsFlagDefaultsLease) Complete() {
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
func (l *ParentKidsFlagDefaultsLease) Quarantine() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self == l && !st.closed && !st.proof.Taken() {
		st.service.markUserSettingsUnknownLocked([]string{st.target})
	}
}
func (l *ParentKidsFlagDefaultsLease) Close() {
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

// ApplyStartupKidsProfileDefaults uses the initial startup ID cut only as a
// candidate list. The datastore checks the current User Kids state and original
// Settings revision together. This PG-only operation neither rebases targets nor
// flushes unrelated dirty settings; generic and JSON defaults keep their policy.
func (s *Service) ApplyStartupKidsProfileDefaults(ctx context.Context, userIDs ...string) (int, error) {
	if ctx == nil || !s.useDB() {
		return 0, datastore.ErrUserSettingsUnavailable
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	// Retain the existing PG storage budget without renewing a shorter caller
	// deadline. Mutex acquisition itself is not actively cancellable.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ids := []string{}
	seen := map[string]bool{}
	for _, raw := range userIDs {
		id := strings.TrimSpace(raw)
		if id != "" && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	sort.Strings(ids)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if s.pg == nil {
		return 0, datastore.ErrUserSettingsUnavailable
	}
	expected := make([]datastore.UserSettingsRevision, len(ids))
	for i, id := range ids {
		if err := s.pgTargetErrorLocked(id); err != nil {
			return 0, err
		}
		record, ok := s.pg.records[id]
		if !ok {
			return 0, datastore.ErrUserSettingsConflict
		}
		before, confirmed := s.pg.confirmed[id]
		after, present := s.settings[id]
		if confirmed != present || !reflect.DeepEqual(before, after) {
			return 0, datastore.ErrUserSettingsConflict
		}
		expected[i] = record.Revision
	}
	receipt, changed, participants, err := s.store.CommitKidsProfileDefaults(ctx, expected)
	if err != nil {
		if errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
			s.markUserSettingsUnknownLocked(participants)
		}
		return 0, err
	}
	if receipt == nil {
		return 0, ctx.Err()
	}
	defer receipt.Close()
	accepted := make([]datastore.UserSettingsRevision, len(participants))
	for i, id := range participants {
		accepted[i] = s.pg.records[id].Revision
	}
	records, err := s.store.AcceptUserSettingsCommit(ctx, receipt, accepted)
	if err != nil {
		s.markUserSettingsUnknownLocked(participants)
		return 0, datastore.ErrUserSettingsOutcomeUnknown
	}
	// Complete the known historical ACK even if cancellation follows COMMIT.
	// Report cancellation afterwards, without claiming a database rollback.
	for _, record := range records {
		s.installPGRecordLocked(record)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return changed, nil
}

// ParentProfileCreationDefaultsLease participates only for management enable. It holds
// this service's selected clean projection after the matching Users lease.
type ParentProfileCreationDefaultsLease struct {
	state *parentProfileCreationDefaultsLeaseState
}
type parentProfileCreationDefaultsLeaseState struct {
	mu                                 sync.Mutex
	self                               *ParentProfileCreationDefaultsLease
	service                            *Service
	proof                              *datastore.ParentProfileCreationCacheLease
	target, operation                  string
	record                             datastore.UserSettingsRecord
	current, confirmed                 models.UserSettings
	present, staged, installed, closed bool
}

func (*ParentProfileCreationDefaultsLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentProfileCreationDefaultsLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentProfileCreationDefaultsLease(opaque)"))
}
func (s *Service) AcquireParentProfileCreationDefaultsLease(ctx context.Context, intent *datastore.ParentProfileCreationIntent, users *datastore.ParentProfileCreationCacheLease) (*ParentProfileCreationDefaultsLease, error) {
	if s == nil || !s.useDB() || ctx == nil {
		return nil, datastore.ErrParentProfileCreationUnavailable
	}
	s.mu.Lock()
	fail := func(err error) (*ParentProfileCreationDefaultsLease, error) { s.mu.Unlock(); return nil, err }
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	target, operation, err := s.store.ParentProfileCreationTarget(ctx, intent)
	if err != nil {
		return fail(err)
	}
	if s.pg == nil {
		return fail(datastore.ErrUserSettingsUnavailable)
	}
	_, current := s.settings[target]
	_, confirmed := s.pg.confirmed[target]
	_, record := s.pg.records[target]
	if current || confirmed || record || s.pg.uncertain[target] || s.pg.unknownMarkers[target] != nil {
		return fail(datastore.ErrUserSettingsConflict)
	}
	proof, err := s.store.BeginParentProfileCreationSettingsLease(ctx, intent, users)
	if err != nil {
		return fail(err)
	}
	value := &ParentProfileCreationDefaultsLease{}
	value.state = &parentProfileCreationDefaultsLeaseState{self: value, service: s, proof: proof, target: target, operation: operation}
	return value, nil
}
func (l *ParentProfileCreationDefaultsLease) ReceiptLease() *datastore.ParentProfileCreationCacheLease {
	if l == nil || l.state == nil || l.state.self != l {
		return nil
	}
	return l.state.proof
}
func (l *ParentProfileCreationDefaultsLease) Stage(r *datastore.ParentProfileCreationReceipt) error {
	if l == nil || l.state == nil {
		return datastore.ErrParentProfileCreationChanged
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self != l || st.closed || st.staged {
		return datastore.ErrParentProfileCreationChanged
	}
	record, err := st.service.store.StageParentProfileCreationSettings(st.proof, r)
	if err != nil {
		return err
	}
	st.record = record
	st.present = record.Settings != nil
	if st.present {
		st.current = cloneSettingsValue(*record.Settings)
		st.confirmed = cloneSettingsValue(*record.Settings)
	}
	st.staged = true
	return nil
}
func (st *parentProfileCreationDefaultsLeaseState) completeLocked() {
	if st.installed || !st.staged || !st.proof.Taken() {
		return
	}
	s := st.service
	s.pg.records[st.target] = st.record
	if st.present {
		s.settings[st.target] = st.current
		s.pg.confirmed[st.target] = st.confirmed
	} else {
		delete(s.settings, st.target)
		delete(s.pg.confirmed, st.target)
	}
	st.installed = true
}
func (l *ParentProfileCreationDefaultsLease) Complete() {
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
func (l *ParentProfileCreationDefaultsLease) Quarantine() {
	if l == nil || l.state == nil {
		return
	}
	st := l.state
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.self == l && !st.closed && !st.proof.Taken() {
		if st.service.pg.unknownMarkers[st.target] == nil {
			st.service.markUserSettingsUnknownLocked([]string{st.target})
			st.service.pg.unknownMarkers[st.target].creation = st.operation
		}
	}
}
func (l *ParentProfileCreationDefaultsLease) Close() {
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
	if s.pg != nil {
		for _, marker := range s.pg.unknownMarkers {
			if marker != nil && marker.creation != "" {
				return true
			}
		}
	}
	return false
}
