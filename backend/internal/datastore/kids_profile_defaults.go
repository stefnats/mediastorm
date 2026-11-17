package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"novastream/models"
)

// CommitKidsProfileDefaults treats the caller's IDs as startup candidates only.
// Current User rows select the Kids participants inside one RR transaction. The
// Settings revisions bind prior raw values and lifecycle history; no stale or
// unknown target is reloaded here. Only nil SimpleMode becomes true.
//
// User NO KEY UPDATE fences non-key Kids changes while remaining compatible with
// the KEY SHARE acquired by a generic Settings INSERT's foreign-key check. Existing
// Settings rows precede stamp locks; an INSERT reaches its stamp via the existing
// AFTER trigger. Other legacy lock inversions may still abort, without retry.
//
// The participant IDs are bookkeeping for targeted Unknown markers, not grants.
// A successful receipt is historical and must be accepted exactly once through
// AcceptUserSettingsCommit. Adult candidates never produce cache records.
func (ds *DataStore) CommitKidsProfileDefaults(ctx context.Context, expected []UserSettingsRevision) (*UserSettingsCommit, int, []string, error) {
	if ds == nil || ds.pool == nil || ctx == nil {
		return nil, 0, nil, ErrUserSettingsUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, nil, err
	}
	if len(expected) == 0 {
		return nil, 0, nil, nil
	}
	originals := make([]*userSettingsSnapshot, len(expected))
	for i, revision := range expected {
		s := revision.snapshot
		if s == nil || s.ds != ds || !s.userPresent || s.id == "" {
			return nil, 0, nil, ErrUserSettingsConflict
		}
		originals[i] = s
	}
	sort.Slice(originals, func(i, j int) bool { return originals[i].id < originals[j].id })
	for i := 1; i < len(originals); i++ {
		if originals[i-1].id == originals[i].id {
			return nil, 0, nil, ErrUserSettingsConflict
		}
	}
	tx, err := ds.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return nil, 0, nil, settingsError(ctx, err)
	}
	defer userWriteRollback(tx)
	type candidate struct {
		settings    *userSettingsSnapshot
		userRaw     json.RawMessage
		userVersion string
		kids        bool
	}
	candidates := make([]candidate, 0, len(originals))
	participants := []string{}
	// These reads share one MVCC snapshot. The original whole User row (including
	// unknown columns) is compared again after the compatible User fence.
	for _, original := range originals {
		c := candidate{settings: original}
		var raw []byte
		var version string
		var generation *int64
		err = tx.QueryRow(ctx, `SELECT to_jsonb(u),`+userWriteVersion+`,u.is_kids_profile,v.generation,s.settings,
 CASE WHEN s.user_id IS NULL THEN '' ELSE `+userSettingsVersion+` END
 FROM users u LEFT JOIN user_settings_versions v ON v.user_id=u.id
 LEFT JOIN user_settings s ON s.user_id=u.id WHERE u.id=$1`, original.id).Scan(&c.userRaw, &c.userVersion, &c.kids, &generation, &raw, &version)
		if err != nil {
			return nil, 0, nil, settingsError(ctx, err)
		}
		if c.kids {
			if generation == nil || *generation < 0 {
				return nil, 0, nil, ErrUserSettingsUnavailable
			}
			if *generation != original.generation || version != original.version || !bytes.Equal(raw, original.raw) {
				return nil, 0, nil, ErrUserSettingsConflict
			}
			participants = append(participants, original.id)
		}
		candidates = append(candidates, c)
	}
	// Fence all original candidate rows in stable order before Settings writes.
	// An adult is a checked no-op; it cannot silently turn into write authority.
	for _, c := range candidates {
		var raw []byte
		var version string
		err = tx.QueryRow(ctx, `SELECT to_jsonb(u),`+userWriteVersion+` FROM users u WHERE u.id=$1 FOR NO KEY UPDATE`, c.settings.id).Scan(&raw, &version)
		if err != nil {
			return nil, 0, nil, settingsError(ctx, err)
		}
		if version != c.userVersion || !bytes.Equal(raw, c.userRaw) {
			return nil, 0, nil, ErrUserSettingsConflict
		}
	}
	receipt := &UserSettingsCommit{ds: ds, ctx: ctx}
	receipt.self = receipt
	changed := 0
	for _, c := range candidates {
		if !c.kids {
			continue
		}
		original := c.settings
		var raw []byte
		var version string
		err = tx.QueryRow(ctx, `SELECT settings,`+userSettingsVersion+` FROM user_settings s WHERE user_id=$1 FOR UPDATE`, original.id).Scan(&raw, &version)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, nil, settingsError(ctx, err)
		}
		if version != original.version || !bytes.Equal(raw, original.raw) {
			return nil, 0, nil, ErrUserSettingsConflict
		}
		var generation int64
		err = tx.QueryRow(ctx, `SELECT generation FROM user_settings_versions WHERE user_id=$1`, original.id).Scan(&generation)
		if err != nil {
			return nil, 0, nil, settingsError(ctx, err)
		}
		if generation != original.generation {
			return nil, 0, nil, ErrUserSettingsConflict
		}
		if original.settings.Display.SimpleMode == nil {
			desired, e := cloneSettings(original.settings)
			if e != nil {
				return nil, 0, nil, e
			}
			desired.Display.SimpleMode = models.BoolPtr(true)
			patched, e := patchSettings(original, desired)
			if e != nil {
				return nil, 0, nil, e
			}
			if version == "" {
				_, err = tx.Exec(ctx, `INSERT INTO user_settings(user_id,settings,updated_at) VALUES($1,$2,now())`, original.id, []byte(patched))
			} else {
				_, err = tx.Exec(ctx, `UPDATE user_settings SET settings=$2,updated_at=now() WHERE user_id=$1`, original.id, []byte(patched))
			}
			changed++
		} else {
			// The existing Settings row was fenced first. Explicit false/true
			// remain exact checked no-ops, including their UpdatedAt/version.
			err = tx.QueryRow(ctx, `SELECT generation FROM user_settings_versions WHERE user_id=$1 FOR UPDATE`, original.id).Scan(&generation)
			if err == nil && generation != original.generation {
				return nil, 0, nil, ErrUserSettingsConflict
			}
		}
		if err != nil {
			return nil, 0, nil, settingsError(ctx, err)
		}
		record, e := ds.scanSettings(tx.QueryRow(ctx, userSettingsSelect+` WHERE u.id=$1`, original.id))
		if e != nil {
			return nil, 0, nil, e
		}
		receipt.expected = append(receipt.expected, original)
		receipt.records = append(receipt.records, record)
	}
	if err = ctx.Err(); err != nil {
		return nil, 0, nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		var pg *pgconn.PgError
		if errors.Is(err, pgx.ErrTxCommitRollback) || errors.As(err, &pg) {
			return nil, 0, nil, settingsError(ctx, err)
		}
		return nil, 0, participants, ErrUserSettingsOutcomeUnknown
	}
	if len(participants) == 0 {
		return nil, 0, nil, nil
	}
	return receipt, changed, participants, nil
}
