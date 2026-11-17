package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"novastream/models"
)

var (
	ErrUserSettingsConflict       = errors.New("profile settings changed; explicitly reload before saving")
	ErrUserSettingsUnavailable    = errors.New("profile settings storage unavailable")
	ErrUserSettingsOutcomeUnknown = errors.New("profile settings commit outcome unknown")
	ErrUserSettingsUnsupported    = errors.New("profile settings contain an unsupported changed value")
)

type userSettingsSnapshot struct {
	ds          *DataStore
	id, version string
	generation  int64
	raw         json.RawMessage
	settings    models.UserSettings
	userPresent bool
}

// UserSettingsRevision is an immutable storage snapshot, not an ownership or
// parental-authorization capability. Tombstones bind absent settings as well.
type UserSettingsRevision struct{ snapshot *userSettingsSnapshot }

func (UserSettingsRevision) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (UserSettingsRevision) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("UserSettingsRevision(opaque)"))
}

type UserSettingsRecord struct {
	ID       string
	Settings *models.UserSettings
	Revision UserSettingsRevision
}
type UserSettingsChangeKind uint8

const (
	UserSettingsPatch UserSettingsChangeKind = iota + 1
	UserSettingsDelete
	UserSettingsCheck
)

type UserSettingsChange struct {
	Kind     UserSettingsChangeKind
	Expected UserSettingsRevision
	Settings models.UserSettings
}

// UserSettingsCommit is sealed historical bookkeeping. Accepting it does not
// assert that its row is still current, or authorize another mutation.
type UserSettingsCommit struct {
	self     *UserSettingsCommit
	ds       *DataStore
	ctx      context.Context
	expected []*userSettingsSnapshot
	records  []UserSettingsRecord
	used     atomic.Bool
}

func (*UserSettingsCommit) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*UserSettingsCommit) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("UserSettingsCommit(opaque)"))
}
func (c *UserSettingsCommit) Close() {
	if c != nil && c.self == c {
		c.used.Store(true)
	}
}

const userSettingsVersion = `concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)`
const userSettingsSelect = `SELECT u.id,v.generation,s.settings,
 CASE WHEN s.user_id IS NULL THEN '' ELSE ` + userSettingsVersion + ` END
 FROM users u LEFT JOIN user_settings_versions v ON v.user_id=u.id
 LEFT JOIN user_settings s ON s.user_id=u.id`

func cloneSettings(value models.UserSettings) (models.UserSettings, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return models.UserSettings{}, ErrUserSettingsUnsupported
	}
	var result models.UserSettings
	if json.Unmarshal(data, &result) != nil {
		return result, ErrUserSettingsUnsupported
	}
	return result, nil
}
func (ds *DataStore) scanSettings(row pgx.Row) (UserSettingsRecord, error) {
	var id, version string
	var generation *int64
	var raw []byte
	if err := row.Scan(&id, &generation, &raw, &version); err != nil {
		return UserSettingsRecord{}, settingsError(nil, err)
	}
	if generation == nil || *generation < 0 || id == "" {
		return UserSettingsRecord{}, ErrUserSettingsUnavailable
	}
	s := &userSettingsSnapshot{ds: ds, id: id, version: version, generation: *generation, raw: append(json.RawMessage(nil), raw...), userPresent: true}
	if version != "" {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil || object == nil || json.Unmarshal(raw, &s.settings) != nil {
			return UserSettingsRecord{}, ErrUserSettingsUnsupported
		}
	}
	r := UserSettingsRecord{ID: id, Revision: UserSettingsRevision{snapshot: s}}
	if version != "" {
		copy, err := cloneSettings(s.settings)
		if err != nil {
			return r, err
		}
		r.Settings = &copy
	}
	return r, nil
}

// LoadUserSettingsSnapshot reads every existing user, including those without a
// settings row, in one SQL snapshot. A newly created user needs an explicit load.
func (ds *DataStore) LoadUserSettingsSnapshot(ctx context.Context) ([]UserSettingsRecord, error) {
	if ds == nil || ds.pool == nil || ctx == nil {
		return nil, ErrUserSettingsUnavailable
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	rows, err := ds.pool.Query(ctx, userSettingsSelect+` ORDER BY u.id`)
	if err != nil {
		return nil, settingsError(ctx, err)
	}
	defer rows.Close()
	result := []UserSettingsRecord{}
	for rows.Next() {
		r, e := ds.scanSettings(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	if rows.Err() != nil {
		return nil, settingsError(ctx, rows.Err())
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return result, nil
}

// LoadUserSettingsTarget establishes a first snapshot for a previously unseen
// user. Services must call it before calculating that user's first mutation;
// this is not a stale-revision or unknown-commit recovery mechanism.
func (ds *DataStore) LoadUserSettingsTarget(ctx context.Context, id string) (UserSettingsRecord, error) {
	if ds == nil || ds.pool == nil || ctx == nil {
		return UserSettingsRecord{}, ErrUserSettingsUnavailable
	}
	if ctx.Err() != nil {
		return UserSettingsRecord{}, ctx.Err()
	}
	r, e := ds.scanSettings(ds.pool.QueryRow(ctx, userSettingsSelect+` WHERE u.id=$1`, id))
	if ctx.Err() != nil {
		return UserSettingsRecord{}, ctx.Err()
	}
	return r, e
}

func settingsError(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	var pg *pgconn.PgError
	if errors.Is(err, pgx.ErrNoRows) || errors.As(err, &pg) && (pg.Code == "40001" || pg.Code == "40P01" || pg.Code == "23505") {
		return ErrUserSettingsConflict
	}
	return ErrUserSettingsUnavailable
}

// CommitUserSettingsChanges never derives deletions from the database minus a
// cache. RR and the write triggers detect changes even after insert/delete ABA.
// Existing row locks precede version-row locks; unchanged old writers remain
// observable, not CAS-safe. Deadlocks abort; no automatic retry is attempted.
func (ds *DataStore) CommitUserSettingsChanges(ctx context.Context, input []UserSettingsChange) (*UserSettingsCommit, error) {
	if ds == nil || ds.pool == nil || ctx == nil {
		return nil, ErrUserSettingsUnavailable
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(input) == 0 {
		return nil, ErrUserSettingsConflict
	}
	changes := append([]UserSettingsChange(nil), input...)
	for _, c := range changes {
		if c.Expected.snapshot == nil || c.Expected.snapshot.ds != ds || !c.Expected.snapshot.userPresent || c.Kind < UserSettingsPatch || c.Kind > UserSettingsCheck {
			return nil, ErrUserSettingsConflict
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Expected.snapshot.id < changes[j].Expected.snapshot.id })
	for i, c := range changes {
		if i > 0 && changes[i-1].Expected.snapshot.id == c.Expected.snapshot.id {
			return nil, ErrUserSettingsConflict
		}
	}
	// Compute patches privately before SQL. Unknown array/object contents are
	// never discarded just because the current typed model cannot represent them.
	patched := make([]json.RawMessage, len(changes))
	for i, c := range changes {
		if c.Kind == UserSettingsPatch {
			raw, e := patchSettings(c.Expected.snapshot, c.Settings)
			if e != nil {
				return nil, e
			}
			patched[i] = raw
		}
	}
	tx, err := ds.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, settingsError(ctx, err)
	}
	defer userWriteRollback(tx)
	result := &UserSettingsCommit{ds: ds, ctx: ctx}
	result.self = result
	for i, c := range changes {
		s := c.Expected.snapshot
		var raw []byte
		var version string
		err = tx.QueryRow(ctx, `SELECT settings,`+userSettingsVersion+` FROM user_settings s WHERE user_id=$1 FOR UPDATE`, s.id).Scan(&raw, &version)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, settingsError(ctx, err)
		}
		if version != s.version || !bytes.Equal(raw, s.raw) {
			return nil, ErrUserSettingsConflict
		}
		var generation int64
		if err = tx.QueryRow(ctx, `SELECT v.generation FROM users u JOIN user_settings_versions v ON v.user_id=u.id WHERE u.id=$1`, s.id).Scan(&generation); err != nil {
			return nil, settingsError(ctx, err)
		}
		if generation != s.generation {
			return nil, ErrUserSettingsConflict
		}
		if c.Kind == UserSettingsDelete && version != "" {
			_, err = tx.Exec(ctx, `DELETE FROM user_settings WHERE user_id=$1`, s.id)
		} else if c.Kind == UserSettingsPatch && !bytes.Equal(patched[i], s.raw) && !(version == "" && bytes.Equal(patched[i], []byte("{}"))) {
			if version == "" {
				_, err = tx.Exec(ctx, `INSERT INTO user_settings(user_id,settings,updated_at) VALUES($1,$2,now())`, s.id, []byte(patched[i]))
			} else {
				_, err = tx.Exec(ctx, `UPDATE user_settings SET settings=$2,updated_at=now() WHERE user_id=$1`, s.id, []byte(patched[i]))
			}
		} else {
			// A checked no-op also fences concurrent lifecycle and settings writes.
			err = tx.QueryRow(ctx, `SELECT generation FROM user_settings_versions WHERE user_id=$1 FOR UPDATE`, s.id).Scan(&generation)
			if err == nil && generation != s.generation {
				return nil, ErrUserSettingsConflict
			}
		}
		if err != nil {
			return nil, settingsError(ctx, err)
		}
		r, e := ds.scanSettings(tx.QueryRow(ctx, userSettingsSelect+` WHERE u.id=$1`, s.id))
		if e != nil {
			return nil, e
		}
		result.expected = append(result.expected, s)
		result.records = append(result.records, r)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err = tx.Commit(ctx); err != nil {
		var pg *pgconn.PgError
		if errors.Is(err, pgx.ErrTxCommitRollback) || errors.As(err, &pg) {
			return nil, settingsError(ctx, err)
		}
		return nil, ErrUserSettingsOutcomeUnknown
	}
	return result, nil
}

var (
	ErrProfileRecoveryChanged     = errors.New("profile recovery selection changed")
	ErrProfileRecoveryUnavailable = errors.New("profile recovery storage unavailable")
)

// These are storage-read objects, never actor, PIN or ownership grants. Slots
// must be reserved by the services before the one SQL snapshot is read.
type ProfileRecoveryRead struct{ state *profileRecoveryReadState }
type ProfileRecoverySnapshot struct {
	state    *profileRecoveryReadState
	self     *ProfileRecoverySnapshot
	present  bool
	user     UserWriteRecord
	settings UserSettingsRecord
}
type ProfileRecoverySlot struct {
	state        *profileRecoveryReadState
	self         *ProfileRecoverySlot
	kind         uint8
	used, closed bool
	lease        *ProfileRecoveryLease
}
type ProfileRecoveryStatus struct {
	TargetPresent      bool
	PendingUncertainty bool
}
type profileRecoveryReadState struct {
	mu           sync.Mutex
	ds           *DataStore
	self         *ProfileRecoveryRead
	ctx, run     context.Context
	cancel       context.CancelFunc
	id           string
	read, closed bool
	slots        [2]*ProfileRecoverySlot
	snapshot     *ProfileRecoverySnapshot
	leases       [2]*ProfileRecoveryLease
}

func (ProfileRecoveryRead) MarshalJSON() ([]byte, error)     { return []byte("null"), nil }
func (ProfileRecoverySnapshot) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (ProfileRecoverySlot) MarshalJSON() ([]byte, error)     { return []byte("null"), nil }
func (ProfileRecoveryRead) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileRecoveryRead(opaque)"))
}
func (ProfileRecoverySnapshot) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileRecoverySnapshot(opaque)"))
}
func (ProfileRecoverySlot) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileRecoverySlot(opaque)"))
}
func (r *ProfileRecoveryRead) Close() {
	if r == nil || r.state == nil {
		return
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self == r {
		s.closed = true
		s.cancel()
	}
}
func (r *ProfileRecoverySnapshot) Close() {
	if r == nil || r.state == nil {
		return
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.self == r && s.snapshot == r {
		s.closed = true
		s.cancel()
	}
}
func (r *ProfileRecoverySlot) Close() {
	if r == nil || r.state == nil {
		return
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.self == r {
		r.closed = true
	}
}
func recoveryContextError(s *profileRecoveryReadState) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if s.closed {
		return ErrProfileRecoveryChanged
	}
	if s.run.Err() != nil {
		return s.run.Err()
	}
	if deadline, ok := s.run.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	if s.closed {
		return ErrProfileRecoveryChanged
	}
	return nil
}

// PrepareProfileRecovery performs no SQL and cannot refresh authentication.
// Its read window is at most ten seconds and never extends its original ctx.
func (ds *DataStore) PrepareProfileRecovery(ctx context.Context, id string) (*ProfileRecoveryRead, error) {
	if ds == nil || ds.pool == nil || ctx == nil {
		return nil, ErrProfileRecoveryUnavailable
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if id == "" || id != strings.TrimSpace(id) {
		return nil, ErrProfileRecoveryChanged
	}
	run, cancel := context.WithTimeout(ctx, 10*time.Second)
	r := &ProfileRecoveryRead{}
	r.state = &profileRecoveryReadState{ds: ds, self: r, ctx: ctx, run: run, cancel: cancel, id: id}
	if err := recoveryContextError(r.state); err != nil {
		cancel()
		return nil, err
	}
	return r, nil
}
func (ds *DataStore) ProfileRecoveryTarget(ctx context.Context, r *ProfileRecoveryRead) (string, error) {
	if r == nil || r.state == nil {
		return "", ErrProfileRecoveryChanged
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ds != ds || s.self != r || s.ctx != ctx || s.read {
		return "", ErrProfileRecoveryChanged
	}
	if err := recoveryContextError(s); err != nil {
		return "", err
	}
	return s.id, nil
}
func (ds *DataStore) reserveRecovery(ctx context.Context, r *ProfileRecoveryRead, kind uint8) (*ProfileRecoverySlot, error) {
	if r == nil || r.state == nil {
		return nil, ErrProfileRecoveryChanged
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ds != ds || s.self != r || s.ctx != ctx || s.read || kind > 1 || s.slots[kind] != nil {
		return nil, ErrProfileRecoveryChanged
	}
	if err := recoveryContextError(s); err != nil {
		return nil, err
	}
	slot := &ProfileRecoverySlot{state: s, kind: kind}
	slot.self = slot
	s.slots[kind] = slot
	return slot, nil
}
func (ds *DataStore) ReserveUsersRecovery(ctx context.Context, r *ProfileRecoveryRead) (*ProfileRecoverySlot, error) {
	return ds.reserveRecovery(ctx, r, 0)
}
func (ds *DataStore) ReserveUserSettingsRecovery(ctx context.Context, r *ProfileRecoveryRead) (*ProfileRecoverySlot, error) {
	return ds.reserveRecovery(ctx, r, 1)
}

// ReadProfileRecovery is one SQL snapshot, including successful absence and its
// persistent lifecycle stamp. No missing-row fallback, bootstrap or DML occurs.
// A later database change is not a locally detected change or durable permit.
func (ds *DataStore) ReadProfileRecovery(r *ProfileRecoveryRead) (*ProfileRecoverySnapshot, error) {
	if r == nil || r.state == nil {
		return nil, ErrProfileRecoveryChanged
	}
	s := r.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ds != ds || s.self != r || s.read {
		return nil, ErrProfileRecoveryChanged
	}
	if err := recoveryContextError(s); err != nil {
		return nil, err
	}
	registered := false
	for _, slot := range s.slots {
		if slot != nil {
			if slot.closed {
				return nil, ErrProfileRecoveryChanged
			}
			registered = true
		}
	}
	if !registered {
		return nil, ErrProfileRecoveryChanged
	}
	s.read = true
	var present bool
	var userRaw, settingsRaw []byte
	var userVersion, settingsVersion string
	var generation *int64
	err := ds.pool.QueryRow(s.run, `SELECT u.id IS NOT NULL,to_jsonb(u),
 CASE WHEN u.id IS NULL THEN '' ELSE `+userWriteVersion+` END,s.settings,
 CASE WHEN s.user_id IS NULL THEN '' ELSE `+userSettingsVersion+` END,v.generation
 FROM (SELECT $1::text AS id) selected
 LEFT JOIN users u ON u.id=selected.id
 LEFT JOIN user_settings s ON s.user_id=selected.id
 LEFT JOIN user_settings_versions v ON v.user_id=selected.id`, s.id).Scan(&present, &userRaw, &userVersion, &settingsRaw, &settingsVersion, &generation)
	if e := recoveryContextError(s); e != nil {
		return nil, e
	}
	if err != nil {
		return nil, ErrProfileRecoveryUnavailable
	}
	if generation != nil && *generation < 0 || present && (generation == nil || userVersion == "") || !present && (userVersion != "" || settingsVersion != "" || len(settingsRaw) > 0) {
		return nil, ErrProfileRecoveryUnavailable
	}
	stamp := int64(0)
	if generation != nil {
		stamp = *generation
	}
	value := &ProfileRecoverySnapshot{state: s, present: present}
	value.self = value
	if !present {
		value.user = UserWriteRecord{User: models.User{ID: s.id}, Revision: UserRevision{owner: ds, id: s.id, version: fmt.Sprintf("absent:%d", stamp)}}
	}
	if present {
		value.user, err = ds.recoveryUserRecord(userRaw, userVersion)
		if err != nil || value.user.User.ID != s.id {
			return nil, ErrProfileRecoveryUnavailable
		}
	}
	settings := &userSettingsSnapshot{ds: ds, id: s.id, version: settingsVersion, generation: stamp, raw: append(json.RawMessage(nil), settingsRaw...), userPresent: present}
	value.settings = UserSettingsRecord{ID: s.id, Revision: UserSettingsRevision{snapshot: settings}}
	if settingsVersion != "" {
		var object map[string]json.RawMessage
		if json.Unmarshal(settingsRaw, &object) != nil || object == nil || json.Unmarshal(settingsRaw, &settings.settings) != nil {
			return nil, ErrProfileRecoveryUnavailable
		}
		copy, e := cloneSettings(settings.settings)
		if e != nil {
			return nil, ErrProfileRecoveryUnavailable
		}
		value.settings.Settings = &copy
	}
	if e := recoveryContextError(s); e != nil {
		return nil, e
	}
	s.snapshot = value
	return value, nil
}
func checkRecoverySlotLocked(ds *DataStore, ctx context.Context, slot *ProfileRecoverySlot, value *ProfileRecoverySnapshot) error {
	if slot == nil || slot.state == nil || slot.self != slot || value == nil || value.self != value || value.state != slot.state {
		return ErrProfileRecoveryChanged
	}
	s := slot.state
	if s.ds != ds || s.ctx != ctx || s.snapshot != value || !s.read || slot.used || slot.closed || slot.lease != nil || slot.kind > 1 || s.slots[slot.kind] != slot {
		return ErrProfileRecoveryChanged
	}
	return recoveryContextError(s)
}

// ProfileRecoveryLease proves only preparation and pair consumption for a
// cache owner. Paired is historical completion state, never an authorization.
type ProfileRecoveryLease struct {
	state          *profileRecoveryReadState
	self           *ProfileRecoveryLease
	slot           *ProfileRecoverySlot
	closed, paired bool
}

func (ProfileRecoveryLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (ProfileRecoveryLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ProfileRecoveryLease(opaque)"))
}

// Retire closes before-pair consumption and reports an already consumed pair
// under the same state lock. Owners cannot miss a concurrent consumption while
// deciding whether Close must finish their prepared cache bookkeeping.
func (l *ProfileRecoveryLease) Retire() bool {
	if l == nil || l.state == nil {
		return false
	}
	s := l.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.self != l {
		return false
	}
	l.closed = true
	return l.paired
}
func (l *ProfileRecoveryLease) Close() { _ = l.Retire() }

func (l *ProfileRecoveryLease) Paired() bool {
	if l == nil || l.state == nil {
		return false
	}
	s := l.state
	s.mu.Lock()
	defer s.mu.Unlock()
	// Once consumed, close/cancellation must not prevent the already prepared
	// cache bookkeeping from completing under its owners' original locks.
	return l.self == l && l.slot != nil && l.slot.kind < 2 && s.leases[l.slot.kind] == l && l.paired
}
func (ds *DataStore) BeginUsersRecoveryLease(ctx context.Context, slot *ProfileRecoverySlot, value *ProfileRecoverySnapshot) (*ProfileRecoveryLease, UserWriteRecord, bool, error) {
	var empty UserWriteRecord
	if slot == nil || slot.state == nil {
		return nil, empty, false, ErrProfileRecoveryChanged
	}
	s := slot.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkRecoverySlotLocked(ds, ctx, slot, value); err != nil {
		return nil, empty, false, err
	}
	if slot.kind != 0 || s.leases[0] != nil || s.leases[1] != nil {
		return nil, empty, false, ErrProfileRecoveryChanged
	}
	record := value.user
	if record.User.KidsAllowedLists != nil {
		record.User.KidsAllowedLists = append([]string{}, record.User.KidsAllowedLists...)
	}
	if err := recoveryContextError(s); err != nil {
		return nil, empty, false, err
	}
	lease := &ProfileRecoveryLease{state: s, slot: slot}
	lease.self = lease
	s.leases[0] = lease
	slot.lease = lease
	return lease, record, value.present, nil
}
func (ds *DataStore) BeginUserSettingsRecoveryLease(ctx context.Context, slot *ProfileRecoverySlot, value *ProfileRecoverySnapshot) (*ProfileRecoveryLease, UserSettingsRecord, bool, error) {
	var empty UserSettingsRecord
	if slot == nil || slot.state == nil {
		return nil, empty, false, ErrProfileRecoveryChanged
	}
	s := slot.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkRecoverySlotLocked(ds, ctx, slot, value); err != nil {
		return nil, empty, false, err
	}
	if slot.kind != 1 || s.leases[1] != nil || s.leases[0] == nil || s.leases[0].closed || s.leases[0].paired {
		return nil, empty, false, ErrProfileRecoveryChanged
	}
	record := value.settings
	if record.Settings != nil {
		copy, err := cloneSettings(*record.Settings)
		if err != nil {
			return nil, empty, false, ErrProfileRecoveryUnavailable
		}
		record.Settings = &copy
	}
	if err := recoveryContextError(s); err != nil {
		return nil, empty, false, err
	}
	lease := &ProfileRecoveryLease{state: s, slot: slot}
	lease.self = lease
	s.leases[1] = lease
	slot.lease = lease
	return lease, record, value.present, nil
}

// TakeProfileRecoveryPair is the sole consumption point. Both cache owners
// already hold their locks and prepared all fallible copies. No individual
// slot can be consumed after leasing. Later cancellation cannot undo this point.
func (ds *DataStore) TakeProfileRecoveryPair(ctx context.Context, users, settings *ProfileRecoveryLease, value *ProfileRecoverySnapshot) error {
	if users == nil || settings == nil || users.state == nil || users.state != settings.state {
		return ErrProfileRecoveryChanged
	}
	s := users.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ds != ds || s.ctx != ctx || value == nil || value.self != value || value.state != s || s.snapshot != value {
		return ErrProfileRecoveryChanged
	}
	for index, lease := range []*ProfileRecoveryLease{users, settings} {
		if lease.self != lease || lease.closed || lease.paired || s.leases[index] != lease || lease.slot == nil || lease.slot.kind != uint8(index) || s.slots[index] != lease.slot || lease.slot.lease != lease || lease.slot.closed || lease.slot.used {
			return ErrProfileRecoveryChanged
		}
	}
	if err := recoveryContextError(s); err != nil {
		return err
	}
	users.slot.used = true
	settings.slot.used = true
	users.paired = true
	settings.paired = true
	return nil
}
func (ds *DataStore) ValidateProfileRecoverySlot(ctx context.Context, slot *ProfileRecoverySlot, value *ProfileRecoverySnapshot) error {
	if slot == nil || slot.state == nil {
		return ErrProfileRecoveryChanged
	}
	s := slot.state
	s.mu.Lock()
	defer s.mu.Unlock()
	return checkRecoverySlotLocked(ds, ctx, slot, value)
}
func (ds *DataStore) TakeUsersRecovery(ctx context.Context, slot *ProfileRecoverySlot, value *ProfileRecoverySnapshot) (UserWriteRecord, bool, error) {
	if slot == nil || slot.state == nil {
		return UserWriteRecord{}, false, ErrProfileRecoveryChanged
	}
	s := slot.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkRecoverySlotLocked(ds, ctx, slot, value); err != nil {
		return UserWriteRecord{}, false, err
	}
	if slot.kind != 0 {
		return UserWriteRecord{}, false, ErrProfileRecoveryChanged
	}
	slot.used = true
	record := value.user
	if record.User.KidsAllowedLists != nil {
		record.User.KidsAllowedLists = append([]string{}, record.User.KidsAllowedLists...)
	}
	return record, value.present, nil
}
func (ds *DataStore) TakeUserSettingsRecovery(ctx context.Context, slot *ProfileRecoverySlot, value *ProfileRecoverySnapshot) (UserSettingsRecord, bool, error) {
	if slot == nil || slot.state == nil {
		return UserSettingsRecord{}, false, ErrProfileRecoveryChanged
	}
	s := slot.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkRecoverySlotLocked(ds, ctx, slot, value); err != nil {
		return UserSettingsRecord{}, false, err
	}
	if slot.kind != 1 {
		return UserSettingsRecord{}, false, ErrProfileRecoveryChanged
	}
	record := value.settings
	if record.Settings != nil {
		copy, err := cloneSettings(*record.Settings)
		if err != nil {
			return UserSettingsRecord{}, false, ErrProfileRecoveryUnavailable
		}
		record.Settings = &copy
	}
	slot.used = true
	return record, value.present, nil
}

// AcceptUserSettingsCommit performs no SQL and consumes exactly the original
// receipt and expected revisions. Cancellation after known commit does not
// invalidate canonical bookkeeping, but consumers must gate their publication.
func (ds *DataStore) AcceptUserSettingsCommit(ctx context.Context, c *UserSettingsCommit, expected []UserSettingsRevision) ([]UserSettingsRecord, error) {
	if c == nil || c.self != c || c.ds != ds || c.ctx != ctx || len(expected) != len(c.expected) {
		return nil, ErrUserSettingsConflict
	}
	for i, r := range expected {
		if r.snapshot != c.expected[i] {
			return nil, ErrUserSettingsConflict
		}
	}
	if c.used.Swap(true) {
		return nil, ErrUserSettingsConflict
	}
	result := make([]UserSettingsRecord, len(c.records))
	for i, r := range c.records {
		result[i] = r
		if r.Settings != nil {
			copy, e := cloneSettings(*r.Settings)
			if e != nil {
				return nil, e
			}
			result[i].Settings = &copy
		}
	}
	return result, nil
}

func patchSettings(s *userSettingsSnapshot, desired models.UserSettings) (json.RawMessage, error) {
	old, err := json.Marshal(s.settings)
	if err != nil {
		return nil, ErrUserSettingsUnsupported
	}
	next, err := json.Marshal(desired)
	if err != nil {
		return nil, ErrUserSettingsUnsupported
	}
	raw := s.raw
	if s.version == "" {
		raw = []byte("{}")
	}
	return patchSettingsValue(raw, old, next, reflect.TypeOf(desired))
}

var rawMessageType = reflect.TypeOf(json.RawMessage{})

func settingsObject(raw []byte) (map[string]json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return map[string]json.RawMessage{}, nil
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, ErrUserSettingsUnsupported
	}
	return object, nil
}
func settingsFieldName(f reflect.StructField) string {
	name := strings.Split(f.Tag.Get("json"), ",")[0]
	if name == "" {
		name = f.Name
	}
	return name
}
func settingsUnknown(raw []byte, t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || t == rawMessageType {
		return false
	}
	if t.Kind() == reflect.Struct {
		object, e := settingsObject(raw)
		if e != nil {
			return true
		}
		for key, value := range object {
			found := false
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if f.PkgPath == "" && strings.EqualFold(key, settingsFieldName(f)) {
					found = true
					if settingsUnknown(value, f.Type) {
						return true
					}
					break
				}
			}
			if !found {
				return true
			}
		}
	} else if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		var array []json.RawMessage
		if json.Unmarshal(raw, &array) != nil {
			return true
		}
		for _, v := range array {
			if settingsUnknown(v, t.Elem()) {
				return true
			}
		}
	} else if t.Kind() == reflect.Map {
		object, e := settingsObject(raw)
		if e != nil {
			return true
		}
		for _, v := range object {
			if settingsUnknown(v, t.Elem()) {
				return true
			}
		}
	}
	return false
}
func patchSettingsValue(raw, old, next json.RawMessage, t reflect.Type) (json.RawMessage, error) {
	if bytes.Equal(old, next) {
		return append(json.RawMessage(nil), raw...), nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rawMessageType {
		return append(json.RawMessage(nil), next...), nil
	}
	if len(next) == 0 || bytes.Equal(next, []byte("null")) {
		if settingsUnknown(raw, t) {
			return nil, ErrUserSettingsUnsupported
		}
		return append(json.RawMessage(nil), next...), nil
	}
	if t.Kind() != reflect.Struct && t.Kind() != reflect.Map {
		if settingsUnknown(raw, t) {
			return nil, ErrUserSettingsUnsupported
		}
		return append(json.RawMessage(nil), next...), nil
	}
	object, err := settingsObject(raw)
	if err != nil {
		return nil, err
	}
	before, err := settingsObject(old)
	if err != nil {
		return nil, err
	}
	after, err := settingsObject(next)
	if err != nil {
		return nil, err
	}
	fields := map[string]reflect.Type{}
	if t.Kind() == reflect.Map {
		for key := range before {
			fields[key] = t.Elem()
		}
		for key := range after {
			fields[key] = t.Elem()
		}
	} else {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := settingsFieldName(f)
			if f.PkgPath == "" && name != "-" {
				fields[name] = f.Type
			}
		}
	}
	for name, ft := range fields {
		if bytes.Equal(before[name], after[name]) {
			continue
		}
		key := name
		found := false
		for candidate := range object {
			match := candidate == name
			if t.Kind() == reflect.Struct {
				match = strings.EqualFold(candidate, name)
			}
			if match {
				if found {
					return nil, ErrUserSettingsUnsupported
				}
				key = candidate
				found = true
			}
		}
		value, e := patchSettingsValue(object[key], before[name], after[name], ft)
		if e != nil {
			return nil, e
		}
		if len(value) == 0 {
			delete(object, key)
		} else {
			object[key] = value
		}
	}
	data, err := json.Marshal(object)
	if err != nil {
		return nil, ErrUserSettingsUnsupported
	}
	return data, nil
}
