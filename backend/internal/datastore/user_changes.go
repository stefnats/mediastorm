package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/models"
)

// recoveryUserRow decodes the actual complete SQL row through the existing
// scanner, including private credential columns. It is never a public DTO.
type recoveryUserRow struct {
	raw     json.RawMessage
	version string
}

func (r recoveryUserRow) Scan(dest ...any) error {
	columns := strings.Split(userColumns, ",")
	if len(dest) != len(columns)+1 || r.version == "" {
		return ErrProfileRecoveryUnavailable
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(r.raw, &fields) != nil || fields == nil {
		return ErrProfileRecoveryUnavailable
	}
	for i, column := range columns {
		column = strings.TrimSpace(column)
		raw, ok := fields[column]
		if !ok || string(raw) == "null" && column != "kids_allowed_lists" {
			return ErrProfileRecoveryUnavailable
		}
		if column == "kids_allowed_lists" {
			if string(raw) != "null" {
				var items []json.RawMessage
				if json.Unmarshal(raw, &items) != nil || items == nil {
					return ErrProfileRecoveryUnavailable
				}
				for _, item := range items {
					var value string
					if len(item) == 0 || item[0] != '"' || json.Unmarshal(item, &value) != nil {
						return ErrProfileRecoveryUnavailable
					}
				}
			}
			*dest[i].(*[]byte) = append([]byte(nil), raw...)
		} else if json.Unmarshal(raw, dest[i]) != nil {
			return ErrProfileRecoveryUnavailable
		}
	}
	*dest[len(columns)].(*string) = r.version
	return nil
}
func (ds *DataStore) recoveryUserRecord(raw json.RawMessage, version string) (UserWriteRecord, error) {
	record, err := ds.scanUserWrite(recoveryUserRow{raw: raw, version: version})
	if err != nil {
		return UserWriteRecord{}, ErrProfileRecoveryUnavailable
	}
	return record, nil
}

var (
	ErrUserWriteConflict       = errors.New("profile storage revision conflict")
	ErrUserWriteUnavailable    = errors.New("profile storage unavailable")
	ErrUserWriteOutcomeUnknown = errors.New("profile storage commit outcome unknown")
	ErrUserWriteLastProfile    = errors.New("cannot delete the last profile")
)

// UserRevision is storage concurrency state, never an authorization capability.
// It is bound to an actual row and DataStore, and cannot be imported from JSON.
type UserRevision struct {
	owner       *DataStore
	id, version string
}

func (UserRevision) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (UserRevision) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("UserRevision(opaque)")) }

type UserWriteRecord struct {
	User     models.User
	Revision UserRevision
}
type UserChangeKind uint8

const (
	UserInsert UserChangeKind = iota + 1
	UserReplace
	UserDelete
)

type UserChange struct {
	Kind     UserChangeKind
	ID       string
	User     models.User
	Expected UserRevision
}
type UserWriteResult struct {
	Records []UserWriteRecord
	Deleted []string
}

const userWriteVersion = `concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)`

type userVersionRow struct {
	row     pgx.Row
	version *string
}

func (r userVersionRow) Scan(dest ...any) error { return r.row.Scan(append(dest, r.version)...) }
func (ds *DataStore) scanUserWrite(row pgx.Row) (UserWriteRecord, error) {
	var version string
	u, err := scanUser(userVersionRow{row: row, version: &version})
	if err != nil {
		return UserWriteRecord{}, err
	}
	if u == nil {
		return UserWriteRecord{}, ErrUserWriteConflict
	}
	return UserWriteRecord{User: *u, Revision: UserRevision{owner: ds, id: u.ID, version: version}}, nil
}
func (ds *DataStore) loadUserWrites(ctx context.Context, q DB) ([]UserWriteRecord, error) {
	rows, err := q.Query(ctx, `SELECT `+userColumns+`,`+userWriteVersion+` FROM users u ORDER BY created_at,id`)
	if err != nil {
		return nil, ErrUserWriteUnavailable
	}
	defer rows.Close()
	result := []UserWriteRecord{}
	for rows.Next() {
		record, err := ds.scanUserWrite(rows)
		if err != nil {
			return nil, ErrUserWriteUnavailable
		}
		result = append(result, record)
	}
	if rows.Err() != nil || ctx.Err() != nil {
		return nil, ErrUserWriteUnavailable
	}
	return result, nil
}

// LoadUserWriteSnapshot reads the complete finite user repository, including
// private credential fields and actual row revisions, in one SQL snapshot.
func (ds *DataStore) LoadUserWriteSnapshot(ctx context.Context) ([]UserWriteRecord, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUserWriteUnavailable
	}
	return ds.loadUserWrites(ctx, ds.pool)
}
func userChangeArgs(u models.User) []any {
	lists, _ := json.Marshal(u.KidsAllowedLists)
	return []any{u.ID, u.AccountID, u.Name, u.Color, u.IconURL, u.PinHash, u.PinLength, u.TraktAccountID, u.PlexAccountID, u.MdblistAccountID, u.SimklAccountID, u.ScrobAccountID, u.IsKidsProfile, u.KidsMode, u.KidsMaxRating, u.KidsMaxMovieRating, u.KidsMaxTVRating, lists, u.AllowShareLinks, models.NormalizeActivityPrivacy(u.ActivityPrivacy), u.AllowLiveTakeover, u.AllowRecordingInterruption, u.CreatedAt, u.UpdatedAt}
}
func (ds *DataStore) insertUserWrite(ctx context.Context, tx pgx.Tx, u models.User) (UserWriteRecord, error) {
	return ds.scanUserWrite(tx.QueryRow(ctx, `INSERT INTO users AS u (`+userColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24) RETURNING `+userColumns+`,`+userWriteVersion, userChangeArgs(u)...))
}
func userWriteRollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func userWriteCommit(ctx context.Context, tx pgx.Tx) error {
	if err := tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return ErrUserWriteUnavailable
		}
		return ErrUserWriteOutcomeUnknown
	}
	return nil
}

// CommitUserChanges applies only explicitly changed IDs, never a database-minus-
// cache deletion set. Inserts never upsert. Replacements/deletes require their
// actual prior revision. A stale target rejects the entire batch without merge.
func (ds *DataStore) CommitUserChanges(ctx context.Context, input []UserChange) (*UserWriteResult, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUserWriteUnavailable
	}
	changes := append([]UserChange(nil), input...)
	sort.Slice(changes, func(i, j int) bool { return changes[i].ID < changes[j].ID })
	topology := false
	insertions, deletions := 0, 0
	for n, c := range changes {
		if c.ID == "" || n > 0 && changes[n-1].ID == c.ID {
			return nil, ErrUserWriteConflict
		}
		switch c.Kind {
		case UserInsert:
			if c.User.ID != c.ID || c.Expected.owner != nil {
				return nil, ErrUserWriteConflict
			}
			topology = true
			insertions++
		case UserReplace, UserDelete:
			if c.Expected.owner != ds || c.Expected.id != c.ID || c.Expected.version == "" || c.Kind == UserReplace && c.User.ID != c.ID {
				return nil, ErrUserWriteConflict
			}
			if c.Kind == UserDelete {
				topology = true
				deletions++
			}
		default:
			return nil, ErrUserWriteConflict
		}
		if c.Kind != UserDelete {
			type diskUser models.User
			if _, err := json.Marshal(diskUser(c.User)); err != nil {
				return nil, ErrUserWriteUnavailable
			}
		}
	}
	result := &UserWriteResult{Records: []UserWriteRecord{}, Deleted: []string{}}
	if len(changes) == 0 {
		return result, nil
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return nil, ErrUserWriteUnavailable
	}
	defer userWriteRollback(tx)
	// Relation-before-row for EVERY writer avoids a tuple-lock then relation-lock
	// upgrade cycle. Only create/delete topology changes take the stronger mode.
	mode := "ROW EXCLUSIVE"
	if topology {
		mode = "SHARE ROW EXCLUSIVE"
	}
	if _, err = tx.Exec(ctx, "LOCK TABLE users IN "+mode+" MODE"); err != nil {
		return nil, ErrUserWriteUnavailable
	}
	for _, c := range changes {
		if c.Kind == UserInsert {
			continue
		}
		var actual string
		if err = tx.QueryRow(ctx, `SELECT `+userWriteVersion+` FROM users u WHERE id=$1 FOR UPDATE`, c.ID).Scan(&actual); err != nil || actual != c.Expected.version {
			return nil, ErrUserWriteConflict
		}
	}
	if deletions > 0 {
		var count int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&count); err != nil {
			return nil, ErrUserWriteUnavailable
		}
		if count+insertions-deletions < 1 {
			return nil, ErrUserWriteLastProfile
		}
	}
	for _, c := range changes {
		switch c.Kind {
		case UserInsert:
			record, e := ds.insertUserWrite(ctx, tx, c.User)
			if e != nil {
				return nil, ErrUserWriteConflict
			}
			result.Records = append(result.Records, record)
		case UserReplace:
			args := userChangeArgs(c.User)
			args = append(args[:22], c.User.UpdatedAt, c.Expected.version)
			record, e := ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET account_id=$2,name=$3,color=$4,icon_url=$5,pin_hash=$6,pin_length=$7,trakt_account_id=$8,plex_account_id=$9,mdblist_account_id=$10,simkl_account_id=$11,scrob_account_id=$12,is_kids_profile=$13,kids_mode=$14,kids_max_rating=$15,kids_max_movie_rating=$16,kids_max_tv_rating=$17,kids_allowed_lists=$18,allow_share_links=$19,activity_privacy=$20,allow_live_takeover=$21,allow_recording_interruption=$22,updated_at=$23 WHERE id=$1 AND `+userWriteVersion+`=$24 RETURNING `+userColumns+`,`+userWriteVersion, args...))
			if e != nil {
				return nil, ErrUserWriteConflict
			}
			result.Records = append(result.Records, record)
		case UserDelete:
			tag, e := tx.Exec(ctx, `DELETE FROM users u WHERE id=$1 AND `+userWriteVersion+`=$2`, c.ID, c.Expected.version)
			if e != nil || tag.RowsAffected() != 1 {
				return nil, ErrUserWriteConflict
			}
			result.Deleted = append(result.Deleted, c.ID)
		}
	}
	if ctx.Err() != nil {
		return nil, ErrUserWriteUnavailable
	}
	if err = userWriteCommit(ctx, tx); err != nil {
		return nil, err
	}
	return result, nil
}

// EnsureDefaultUserSnapshot atomically bootstraps only an actually empty DB.
// Concurrent constructors adopt the already-created full snapshot; no upsert or
// stale-cache resurrection occurs. It shares topology-before-row lock ordering.
func (ds *DataStore) EnsureDefaultUserSnapshot(ctx context.Context, candidate models.User) ([]UserWriteRecord, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrUserWriteUnavailable
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return nil, ErrUserWriteUnavailable
	}
	defer userWriteRollback(tx)
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return nil, ErrUserWriteUnavailable
	}
	records, err := ds.loadUserWrites(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		record, e := ds.insertUserWrite(ctx, tx, candidate)
		if e != nil {
			return nil, ErrUserWriteUnavailable
		}
		records = append(records, record)
	}
	if ctx.Err() != nil {
		return nil, ErrUserWriteUnavailable
	}
	if err = userWriteCommit(ctx, tx); err != nil {
		return nil, err
	}
	return records, nil
}
