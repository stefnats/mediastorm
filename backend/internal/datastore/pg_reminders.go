package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/models"
)

type pgReminderRepo struct{ pool *pgxpool.Pool }
type reminderQuery interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func reminderState(ctx context.Context, q reminderQuery, profile string) (models.ReminderProfileState, error) {
	state := models.ReminderProfileState{Subscriptions: []models.StoredReminder{}, Checkpoints: []models.ReminderCheckpoint{}, Events: []models.ReminderEvent{}}
	for _, table := range []string{"reminder_subscriptions", "reminder_checkpoints", "reminder_events"} {
		rows, err := q.Query(ctx, "SELECT payload"+map[string]string{"reminder_events": ",milestone_key"}[table]+" FROM "+table+" WHERE profile_id=$1", profile)
		if err != nil {
			return state, err
		}
		for rows.Next() {
			var data []byte
			var key string
			if table == "reminder_events" {
				err = rows.Scan(&data, &key)
			} else {
				err = rows.Scan(&data)
			}
			if err != nil {
				rows.Close()
				return state, err
			}
			switch table {
			case "reminder_subscriptions":
				var item models.StoredReminder
				err = json.Unmarshal(data, &item)
				state.Subscriptions = append(state.Subscriptions, item)
			case "reminder_checkpoints":
				var item models.ReminderCheckpoint
				err = json.Unmarshal(data, &item)
				state.Checkpoints = append(state.Checkpoints, item)
			case "reminder_events":
				var item models.ReminderEvent
				err = json.Unmarshal(data, &item)
				item.MilestoneKey = key
				state.Events = append(state.Events, item)
			}
			if err != nil {
				rows.Close()
				return state, err
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return state, err
		}
	}
	return state, nil
}
func (r *pgReminderRepo) Get(ctx context.Context, profile string) (models.ReminderProfileState, error) {
	// A repeatable-read transaction prevents mixed generations across three tables.
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return models.ReminderProfileState{}, err
	}
	defer tx.Rollback(ctx)
	state, err := reminderState(ctx, tx, profile)
	if err != nil {
		return state, err
	}
	return state, tx.Commit(ctx)
}
func (r *pgReminderRepo) Update(ctx context.Context, profile string, change func(*models.ReminderProfileState) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended('reminders:restore',0))`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "reminders:"+profile); err != nil {
		return err
	}
	state, err := reminderState(ctx, tx, profile)
	if err != nil {
		return err
	}
	before, err := reminderRows(profile, state)
	if err != nil {
		return err
	}
	if err = change(&state); err != nil {
		return err
	}
	if err = writeReminderDelta(ctx, tx, profile, before, state); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (t *Tx) RestoreReminders(ctx context.Context, profile string, state models.ReminderProfileState) error {
	return writeReminderState(ctx, t.tx, profile, state)
}

// Restore participates in the caller's existing account/profile transaction.
func writeReminderState(ctx context.Context, tx pgx.Tx, profile string, state models.ReminderProfileState) error {
	for _, table := range []string{"reminder_events", "reminder_checkpoints", "reminder_subscriptions"} {
		if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE profile_id=$1", profile); err != nil {
			return err
		}
	}
	return writeReminderDelta(ctx, tx, profile, map[string]reminderRow{}, state)
}

type reminderRow struct {
	table, key   string
	data         []byte
	insert       string
	args, remove []any
}

func reminderRows(profile string, state models.ReminderProfileState) (map[string]reminderRow, error) {
	rows := map[string]reminderRow{}
	add := func(row reminderRow) error {
		key := row.table + "\x00" + row.key
		if _, ok := rows[key]; ok {
			return fmt.Errorf("duplicate reminder row")
		}
		rows[key] = row
		return nil
	}
	for _, item := range state.Subscriptions {
		data, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		if err = add(reminderRow{"reminder_subscriptions", item.Reminder.ID, data, `INSERT INTO reminder_subscriptions(profile_id,id,payload)VALUES($1,$2,$3) ON CONFLICT(profile_id,id)DO UPDATE SET payload=EXCLUDED.payload`, []any{profile, item.Reminder.ID, data}, []any{profile, item.Reminder.ID}}); err != nil {
			return nil, err
		}
	}
	for _, item := range state.Checkpoints {
		data, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		if err = add(reminderRow{"reminder_checkpoints", item.ReminderID + "\x00" + item.MilestoneKey, data, `INSERT INTO reminder_checkpoints(profile_id,reminder_id,milestone_key,payload)VALUES($1,$2,$3,$4) ON CONFLICT(profile_id,reminder_id,milestone_key)DO UPDATE SET payload=EXCLUDED.payload`, []any{profile, item.ReminderID, item.MilestoneKey, data}, []any{profile, item.ReminderID, item.MilestoneKey}}); err != nil {
			return nil, err
		}
	}
	for _, item := range state.Events {
		if item.MilestoneKey == "" {
			return nil, fmt.Errorf("reminder event identity missing")
		}
		data, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		if err = add(reminderRow{"reminder_events", item.ID, data, `INSERT INTO reminder_events(profile_id,id,reminder_id,event_kind,milestone_key,payload)VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(profile_id,id)DO UPDATE SET payload=EXCLUDED.payload`, []any{profile, item.ID, item.ReminderID, item.EventKind, item.MilestoneKey, data}, []any{profile, item.ID}}); err != nil {
			return nil, err
		}
	}
	return rows, nil
}
func writeReminderDelta(ctx context.Context, tx pgx.Tx, profile string, before map[string]reminderRow, state models.ReminderProfileState) error {
	after, err := reminderRows(profile, state)
	if err != nil {
		return err
	}
	batch := &pgx.Batch{}
	// Remove dependent rows first, then insert parents before children. Unchanged
	// history incurs no writes; pgx batches changed rows into one exchange.
	for _, table := range []string{"reminder_events", "reminder_checkpoints", "reminder_subscriptions"} {
		for key, row := range before {
			if row.table != table {
				continue
			}
			if _, ok := after[key]; ok {
				continue
			}
			where := "profile_id=$1 AND id=$2"
			if table == "reminder_checkpoints" {
				where = "profile_id=$1 AND reminder_id=$2 AND milestone_key=$3"
			}
			batch.Queue("DELETE FROM "+table+" WHERE "+where, row.remove...)
		}
	}
	for _, table := range []string{"reminder_subscriptions", "reminder_checkpoints", "reminder_events"} {
		for key, row := range after {
			if row.table != table {
				continue
			}
			if prior, ok := before[key]; ok && bytes.Equal(prior.data, row.data) {
				continue
			}
			batch.Queue(row.insert, row.args...)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	results := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			results.Close()
			return err
		}
	}
	return results.Close()
}

// LockRemindersForRestore must be the first operation in a backup transaction,
// before deleting users or taking FK row locks. Normal updates take its shared
// counterpart before the profile lock, so restored boundaries cannot be lost.
func (t *Tx) LockRemindersForRestore(ctx context.Context) error {
	_, err := t.tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('reminders:restore',0))`)
	return err
}
