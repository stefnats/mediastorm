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
	"sync/atomic"

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
	s := &userSettingsSnapshot{ds: ds, id: id, version: version, generation: *generation, raw: append(json.RawMessage(nil), raw...)}
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
		if c.Expected.snapshot == nil || c.Expected.snapshot.ds != ds || c.Kind < UserSettingsPatch || c.Kind > UserSettingsCheck {
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
