package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func recoveryReadSlots(t *testing.T, ds *DataStore, ctx context.Context, id string) (*ProfileRecoveryRead, *ProfileRecoverySlot, *ProfileRecoverySlot) {
	t.Helper()
	read, e := ds.PrepareProfileRecovery(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(read.Close)
	users, e := ds.ReserveUsersRecovery(ctx, read)
	if e != nil {
		t.Fatal(e)
	}
	settings, e := ds.ReserveUserSettingsRecovery(ctx, read)
	if e != nil {
		t.Fatal(e)
	}
	return read, users, settings
}
func TestProfileRecoveryOneSQLSnapshotAndNullLists(t *testing.T) {
	for _, lists := range []string{"null", "[]", `["one","one"]`} {
		t.Run(lists, func(t *testing.T) {
			f := newSettingsDS(t)
			f.user(t, "a")
			f.exec(t, `UPDATE users SET kids_allowed_lists=$1::jsonb WHERE id='a'`, lists)
			f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"future":9007199254740993,"display":{"simpleMode":false}}')`)
			beforeUser := f.record(t, "a")
			stamp := f.generation(t, "a")
			cfg := f.ds.pool.Config()
			trace := &unlockQueryTrace{}
			cfg.ConnConfig.Tracer = trace
			pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(pool.Close)
			ds := &DataStore{pool: pool}
			ctx := t.Context()
			read, users, settings := recoveryReadSlots(t, ds, ctx, "a")
			if trace.calls.Load() != 0 {
				t.Fatal("prepare performed SQL")
			}
			snapshot, e := ds.ReadProfileRecovery(read)
			if e != nil {
				t.Fatal(e)
			}
			if trace.calls.Load() != 1 {
				t.Fatal("shared recovery was not one SQL read")
			}
			if e = ds.ValidateProfileRecoverySlot(ctx, users, snapshot); e != nil {
				t.Fatal(e)
			}
			user, present, e := ds.TakeUsersRecovery(ctx, users, snapshot)
			if e != nil || !present || user.User.ID != "a" {
				t.Fatal("user snapshot absent", e)
			}
			if lists == "null" && user.User.KidsAllowedLists != nil {
				t.Fatal("legitimate JSON-null list changed")
			}
			if lists == "[]" && user.User.KidsAllowedLists == nil {
				t.Fatal("empty list normalized to nil")
			}
			record, present, e := ds.TakeUserSettingsRecovery(ctx, settings, snapshot)
			if e != nil || !present || record.Settings == nil || *record.Settings.Display.SimpleMode {
				t.Fatal("settings snapshot lost explicit false", e)
			}
			if trace.calls.Load() != 1 || f.generation(t, "a") != stamp || f.record(t, "a").Revision.snapshot.version != beforeUser.Revision.snapshot.version {
				t.Fatal("read/adopt changed stored state")
			}
		})
	}
}
func TestProfileRecoveryPresenceAndDurableAbsence(t *testing.T) {
	for _, kind := range []string{"present-no-settings", "deleted-no-settings", "never-present"} {
		t.Run(kind, func(t *testing.T) {
			f := newSettingsDS(t)
			if kind != "never-present" {
				f.user(t, "a")
			}
			if kind == "deleted-no-settings" {
				f.exec(t, `DELETE FROM users WHERE id='a'`)
			}
			ctx := t.Context()
			read, users, settings := recoveryReadSlots(t, f.ds, ctx, "a")
			snapshot, e := f.ds.ReadProfileRecovery(read)
			if e != nil {
				t.Fatal(e)
			}
			_, present, e := f.ds.TakeUsersRecovery(ctx, users, snapshot)
			if e != nil || present != (kind == "present-no-settings") {
				t.Fatal("SQL absence became a storage error or vice versa", e)
			}
			record, present, e := f.ds.TakeUserSettingsRecovery(ctx, settings, snapshot)
			if e != nil || record.Settings != nil || present != (kind == "present-no-settings") {
				t.Fatal("settings absence not recorded", e)
			}
			if kind == "deleted-no-settings" && record.Revision.snapshot.generation == 0 {
				t.Fatal("lifecycle tombstone lost")
			}
			if !present {
				f.user(t, "a")
				if receipt, e := f.ds.CommitUserSettingsChanges(ctx, []UserSettingsChange{{Kind: UserSettingsPatch, Expected: record.Revision, Settings: settingsDesired(true)}}); receipt != nil || !errors.Is(e, ErrUserSettingsConflict) {
					t.Fatal("observed absence adopted a recreated user", e)
				}
			}
		})
	}
}
func TestProfileRecoveryMalformedRowsRemainErrors(t *testing.T) {
	for _, kind := range []string{"missing-name", "invalid-list", "null-list-element", "bad-settings", "missing-stamp"} {
		t.Run(kind, func(t *testing.T) {
			f := newSettingsDS(t)
			f.user(t, "a")
			switch kind {
			case "missing-name":
				f.exec(t, `ALTER TABLE users DROP COLUMN name`)
			case "invalid-list":
				f.exec(t, `UPDATE users SET kids_allowed_lists='{}' WHERE id='a'`)
			case "null-list-element":
				f.exec(t, `UPDATE users SET kids_allowed_lists='[null]' WHERE id='a'`)
			case "bad-settings":
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"display":{"simpleMode":"bad"}}')`)
			case "missing-stamp":
				f.exec(t, `DROP TABLE user_settings_versions`)
			}
			read, _, _ := recoveryReadSlots(t, f.ds, t.Context(), "a")
			if snapshot, e := f.ds.ReadProfileRecovery(read); snapshot != nil || !errors.Is(e, ErrProfileRecoveryUnavailable) {
				t.Fatal("malformed/read failure became absence", e)
			}
		})
	}
}
func TestProfileRecoveryObjectsAndHistoricalConsume(t *testing.T) {
	f := newSettingsDS(t)
	f.user(t, "a")
	ctx := t.Context()
	read, users, settings := recoveryReadSlots(t, f.ds, ctx, "a")
	snapshot, e := f.ds.ReadProfileRecovery(read)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.ds.ReserveUsersRecovery(ctx, read); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("late ticket registration")
	}
	if _, e = f.ds.ReadProfileRecovery(read); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("read replay")
	}
	copySnapshot := *snapshot
	copySlot := *users
	for _, candidate := range []*ProfileRecoverySnapshot{nil, {}, &copySnapshot} {
		if e = f.ds.ValidateProfileRecoverySlot(ctx, users, candidate); !errors.Is(e, ErrProfileRecoveryChanged) {
			t.Fatal("forged snapshot accepted")
		}
	}
	if e = f.ds.ValidateProfileRecoverySlot(ctx, &copySlot, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("copied slot accepted")
	}
	other := &DataStore{pool: f.ds.pool}
	if e = other.ValidateProfileRecoverySlot(ctx, users, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("wrong store accepted")
	}
	if e = f.ds.ValidateProfileRecoverySlot(context.Background(), users, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("replacement context accepted")
	}
	if _, _, e = f.ds.TakeUsersRecovery(ctx, settings, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("wrong participant accepted")
	}
	for _, value := range []any{*read, *snapshot, *users} {
		encoded, _ := json.Marshal(value)
		if string(encoded) != "null" || strings.Contains(fmt.Sprintf("%#v", value), "password") {
			t.Fatal("private read state leaked")
		}
	}
	// Consume remains a historical local operation even after the actual pool is
	// closed; this deliberately proves no current-database freshness or new query.
	f.ds.pool.Close()
	if _, _, e = f.ds.TakeUsersRecovery(ctx, users, snapshot); e != nil {
		t.Fatal(e)
	}
	if _, _, e = f.ds.TakeUserSettingsRecovery(ctx, settings, snapshot); e != nil {
		t.Fatal(e)
	}
	if e = f.ds.ValidateProfileRecoverySlot(ctx, users, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("consumed slot replay")
	}
}
func TestProfileRecoveryCloseAndActualCancellation(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		f := newSettingsDS(t)
		f.user(t, "a")
		read, users, _ := recoveryReadSlots(t, f.ds, t.Context(), "a")
		snapshot, e := f.ds.ReadProfileRecovery(read)
		if e != nil {
			t.Fatal(e)
		}
		read.Close()
		if e = f.ds.ValidateProfileRecoverySlot(t.Context(), users, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
			t.Fatal("closed read remained usable", e)
		}
	})
	t.Run("blocked", func(t *testing.T) {
		f := newSettingsDS(t)
		f.user(t, "a")
		lock, e := f.ds.pool.Begin(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		defer userWriteRollback(lock)
		if _, e = lock.Exec(t.Context(), `LOCK TABLE users IN ACCESS EXCLUSIVE MODE`); e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		read, _, _ := recoveryReadSlots(t, f.ds, ctx, "a")
		done := make(chan error, 1)
		go func() { _, e := f.ds.ReadProfileRecovery(read); done <- e }()
		t.Cleanup(func() {
			cancel()
			userWriteRollback(lock)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("read worker did not end")
			}
		})
		f.waitBlocked(t)
		cancel()
		select {
		case e = <-done:
			done <- e
		case <-time.After(5 * time.Second):
			t.Fatal("read cancellation delayed")
		}
		if !errors.Is(e, context.Canceled) {
			t.Fatal("read cancellation hidden", e)
		}
	})
}
