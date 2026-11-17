package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func startupDefaultsRevision(t *testing.T, ds *DataStore, id string) UserSettingsRevision {
	t.Helper()
	record, err := ds.LoadUserSettingsTarget(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	return record.Revision
}
func startupDefaultsUser(t *testing.T, f *settingsVersionsFixture, id string) string {
	t.Helper()
	var raw string
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u)::text||'/'||`+userWriteVersion+` FROM users u WHERE id=$1`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestStartupKidsDefaultsDSNilFalseTrueAndAdult(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		kids      bool
		changed   int
	}{
		{"absent", "", true, 1},
		{"nil", `{"display":{"simpleMode":null,"future":{"n":9007199254740993}},"future":[{"x":123456789012345678901234567890.123456789}]}`, true, 1},
		{"false", `{"display":{"simpleMode":false,"future":1},"future":9007199254740993}`, true, 0},
		{"true", `{"display":{"simpleMode":true},"future":9007199254740993}`, true, 0},
		{"adult", `{"display":{"simpleMode":null},"future":9007199254740993}`, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSettingsVersionsPG(t)
			f.user(t, "a")
			f.exec(t, `UPDATE users SET is_kids_profile=$1 WHERE id='a'`, tc.kids)
			if tc.raw != "" {
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a',$1)`, tc.raw)
			}
			original := startupDefaultsRevision(t, f.ds, "a")
			user := startupDefaultsUser(t, f, "a")
			raw, version := versionsRaw(t, f.ds, "a")
			stamp := versionsGeneration(t, f.ds, "a")
			ctx := t.Context()
			receipt, count, ids, err := f.ds.CommitKidsProfileDefaults(ctx, []UserSettingsRevision{original})
			if err != nil || count != tc.changed {
				t.Fatal("wrong default outcome", count, err)
			}
			if startupDefaultsUser(t, f, "a") != user {
				t.Fatal("startup changed User row or MVCC")
			}
			after, nextVersion := versionsRaw(t, f.ds, "a")
			if !tc.kids {
				if receipt != nil || len(ids) != 0 || !bytes.Equal(after, raw) || nextVersion != version || versionsGeneration(t, f.ds, "a") != stamp {
					t.Fatal("adult candidate wrote or produced an adoption receipt")
				}
				return
			}
			if receipt == nil || len(ids) != 1 || ids[0] != "a" {
				t.Fatal("missing exact Kids participant")
			}
			defer receipt.Close()
			records, err := f.ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{original})
			if err != nil || len(records) != 1 || records[0].Settings == nil || records[0].Settings.Display.SimpleMode == nil {
				t.Fatal("missing checked snapshot", err)
			}
			if tc.changed == 0 {
				if !bytes.Equal(after, raw) || nextVersion != version || versionsGeneration(t, f.ds, "a") != stamp {
					t.Fatal("explicit bool was rewritten")
				}
			} else {
				if !*records[0].Settings.Display.SimpleMode || versionsGeneration(t, f.ds, "a") != stamp+1 {
					t.Fatal("nil default not persisted exactly once")
				}
				var beforeObject, afterObject map[string]json.RawMessage
				if len(raw) != 0 {
					if json.Unmarshal(raw, &beforeObject) != nil {
						t.Fatal("read initial raw")
					}
				}
				if json.Unmarshal(after, &afterObject) != nil {
					t.Fatal("read final raw")
				}
				if future, ok := beforeObject["future"]; ok && !bytes.Equal(future, afterObject["future"]) {
					t.Fatal("unknown root/precise number changed")
				}
				if tc.name == "nil" {
					var display map[string]json.RawMessage
					if json.Unmarshal(afterObject["display"], &display) != nil || !bytes.Contains(display["future"], []byte("9007199254740993")) {
						t.Fatal("unknown nested value lost")
					}
				}
			}
		})
	}
}
func TestStartupKidsDefaultsDSRejectsStaleLifecycleAndRollsBack(t *testing.T) {
	for _, what := range []string{"new false", "settings ABA", "absent ABA", "User recreate", "generation overflow", "second target failure"} {
		t.Run(what, func(t *testing.T) {
			f := newSettingsVersionsPG(t)
			for _, id := range []string{"a", "b"} {
				f.user(t, id)
				f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id=$1`, id)
			}
			if what != "absent ABA" {
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{}'),('b','{}')`)
			}
			if what == "generation overflow" {
				f.exec(t, `UPDATE user_settings_versions SET generation=9223372036854775807 WHERE user_id='a'`)
			}
			original := startupDefaultsRevision(t, f.ds, "a")
			second := startupDefaultsRevision(t, f.ds, "b")
			switch what {
			case "new false":
				f.exec(t, `UPDATE user_settings SET settings='{"display":{"simpleMode":false}}' WHERE user_id='a'`)
			case "settings ABA":
				f.exec(t, `UPDATE user_settings SET settings='{"future":1}' WHERE user_id='a'; UPDATE user_settings SET settings='{}' WHERE user_id='a'`)
			case "absent ABA":
				f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{}'); DELETE FROM user_settings WHERE user_id='a'`)
			case "User recreate":
				f.exec(t, `DELETE FROM users WHERE id='a'; INSERT INTO users(id,name,account_id,is_kids_profile) VALUES('a','a','settings_owner',true); INSERT INTO user_settings(user_id,settings) VALUES('a','{}')`)
			case "second target failure":
				f.exec(t, `CREATE FUNCTION reject_startup_b() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.user_id='b' THEN RAISE EXCEPTION 'synthetic startup rejection'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_startup_b BEFORE UPDATE ON user_settings FOR EACH ROW EXECUTE FUNCTION reject_startup_b()`)
			}
			beforeA, versionA := versionsRaw(t, f.ds, "a")
			beforeB, versionB := versionsRaw(t, f.ds, "b")
			receipt, count, _, err := f.ds.CommitKidsProfileDefaults(t.Context(), []UserSettingsRevision{second, original})
			if receipt != nil {
				receipt.Close()
			}
			if err == nil || count != 0 || receipt != nil {
				t.Fatal("stale/failed batch succeeded")
			}
			if what != "generation overflow" && what != "second target failure" && !errors.Is(err, ErrUserSettingsConflict) {
				t.Fatal("missing strict conflict", err)
			}
			afterA, nextA := versionsRaw(t, f.ds, "a")
			afterB, nextB := versionsRaw(t, f.ds, "b")
			if !bytes.Equal(beforeA, afterA) || !bytes.Equal(beforeB, afterB) || versionA != nextA || versionB != nextB {
				t.Fatal("failed batch partially wrote Settings")
			}
		})
	}
}

type startupDefaultsBarrier struct {
	armed            atomic.Bool
	match            string
	entered, release chan struct{}
	once             sync.Once
}

func (b *startupDefaultsBarrier) unblock() { b.once.Do(func() { close(b.release) }) }
func (b *startupDefaultsBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, b.match) && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*startupDefaultsBarrier) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func startupDefaultsTraced(t *testing.T, f *settingsVersionsFixture, match string) (*DataStore, *startupDefaultsBarrier, string) {
	t.Helper()
	barrier := &startupDefaultsBarrier{match: match, entered: make(chan struct{}), release: make(chan struct{})}
	cfg := f.ds.pool.Config()
	application := f.schema + "_startup"
	cfg.ConnConfig.RuntimeParams["application_name"] = application
	cfg.ConnConfig.Tracer = barrier
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &DataStore{pool: pool}, barrier, application
}

type startupDefaultsResult struct {
	receipt *UserSettingsCommit
	count   int
	ids     []string
	err     error
}

func startupDefaultsRun(t *testing.T, ds *DataStore, barrier *startupDefaultsBarrier, expected UserSettingsRevision) (<-chan startupDefaultsResult, context.CancelFunc, *bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	joined := new(bool)
	done := make(chan startupDefaultsResult, 1)
	if barrier != nil {
		barrier.armed.Store(true)
	}
	go func() {
		r, n, ids, e := ds.CommitKidsProfileDefaults(ctx, []UserSettingsRevision{expected})
		done <- startupDefaultsResult{r, n, ids, e}
	}()
	t.Cleanup(func() {
		cancel()
		if barrier != nil {
			barrier.unblock()
		}
		if !*joined {
			select {
			case result := <-done:
				if result.receipt != nil {
					result.receipt.Close()
				}
			case <-time.After(5 * time.Second):
				t.Error("startup worker did not join")
			}
		}
	})
	return done, cancel, joined
}
func startupDefaultsEntered(t *testing.T, b *startupDefaultsBarrier) {
	t.Helper()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual startup query boundary not reached")
	}
}
func startupDefaultsJoined(t *testing.T, done <-chan startupDefaultsResult, joined *bool) startupDefaultsResult {
	t.Helper()
	select {
	case result := <-done:
		*joined = true
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("startup worker did not finish")
	}
	return startupDefaultsResult{}
}
func TestStartupKidsDefaultsDSOriginalUserFrameBeforeFence(t *testing.T) {
	for _, what := range []string{"Kids disabled", "Kids ABA", "unknown User column", "delete recreate"} {
		t.Run(what, func(t *testing.T) {
			f := newSettingsVersionsPG(t)
			f.user(t, "a")
			f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='a'; ALTER TABLE users ADD COLUMN future_startup text NOT NULL DEFAULT 'before'`)
			ds, barrier, _ := startupDefaultsTraced(t, f, "FOR NO KEY UPDATE")
			expected := startupDefaultsRevision(t, ds, "a")
			done, _, joined := startupDefaultsRun(t, ds, barrier, expected)
			startupDefaultsEntered(t, barrier)
			switch what {
			case "Kids disabled":
				f.exec(t, `UPDATE users SET is_kids_profile=false WHERE id='a'`)
			case "Kids ABA":
				f.exec(t, `UPDATE users SET is_kids_profile=false WHERE id='a'; UPDATE users SET is_kids_profile=true WHERE id='a'`)
			case "unknown User column":
				f.exec(t, `UPDATE users SET future_startup='after' WHERE id='a'`)
			case "delete recreate":
				f.exec(t, `DELETE FROM users WHERE id='a'; INSERT INTO users(id,name,account_id,is_kids_profile) VALUES('a','a','settings_owner',true)`)
			}
			barrier.unblock()
			result := startupDefaultsJoined(t, done, joined)
			if result.receipt != nil {
				result.receipt.Close()
			}
			if !errors.Is(result.err, ErrUserSettingsConflict) || result.receipt != nil || result.count != 0 {
				t.Fatal("replacement User frame adopted", result.err)
			}
			raw, _ := versionsRaw(t, ds, "a")
			if len(raw) != 0 {
				t.Fatal("invalidated User got Settings defaults")
			}
		})
	}
}
func TestStartupKidsDefaultsDSForeignKeyInsertCompatibleFence(t *testing.T) {
	f := newSettingsVersionsPG(t)
	f.user(t, "a")
	f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='a'`)
	ds, barrier, _ := startupDefaultsTraced(t, f, "FROM user_settings s WHERE user_id=$1 FOR UPDATE")
	expected := startupDefaultsRevision(t, ds, "a")
	done, _, joined := startupDefaultsRun(t, ds, barrier, expected)
	startupDefaultsEntered(t, barrier) // The actual NO KEY UPDATE query already returned.
	writer, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	tx, err := f.ds.pool.Begin(writer)
	if err != nil {
		t.Fatal(err)
	}
	defer userWriteRollback(tx)
	var id string
	if err = tx.QueryRow(writer, `SELECT id FROM users WHERE id='a' FOR KEY SHARE`).Scan(&id); err != nil || id != "a" {
		t.Fatal("User fence conflicts with generic FK KEY SHARE", err)
	}
	if _, err = tx.Exec(writer, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"display":{"simpleMode":false}}')`); err != nil {
		t.Fatal("generic Settings INSERT could not finish behind compatible User fence", err)
	}
	if err = tx.Commit(writer); err != nil {
		t.Fatal(err)
	}
	barrier.unblock()
	result := startupDefaultsJoined(t, done, joined)
	if result.receipt != nil {
		result.receipt.Close()
	}
	if !errors.Is(result.err, ErrUserSettingsConflict) || result.receipt != nil {
		t.Fatal("concurrent inserted false was defaulted", result.err)
	}
	raw, _ := versionsRaw(t, ds, "a")
	if !bytes.Contains(raw, []byte("false")) {
		t.Fatal("generic writer's explicit false lost")
	}
}
func TestStartupKidsDefaultsDSCancelActualSettingsWait(t *testing.T) {
	f := newSettingsVersionsPG(t)
	f.user(t, "a")
	f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='a';INSERT INTO user_settings(user_id,settings) VALUES('a','{}')`)
	ds, _, application := startupDefaultsTraced(t, f, "never pause")
	expected := startupDefaultsRevision(t, ds, "a")
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer userWriteRollback(blocker)
	if _, err = blocker.Exec(t.Context(), `SELECT user_id FROM user_settings WHERE user_id='a' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done, cancel, joined := startupDefaultsRun(t, ds, nil, expected)
	observed := false
	observer, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	for observer.Err() == nil {
		if err = f.admin.QueryRow(observer, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM user_settings s WHERE user_id=%')`, application).Scan(&observed); err != nil {
			t.Fatal("observe own Settings waiter", err)
		}
		if observed {
			break
		}
		select {
		case <-observer.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !observed {
		t.Fatal("actual Settings row wait never observed")
	}
	cancel()
	result := startupDefaultsJoined(t, done, joined)
	if !errors.Is(result.err, context.Canceled) || result.receipt != nil {
		t.Fatal("canceled SQL waiter produced receipt", result.err)
	}
	userWriteRollback(blocker)
	raw, _ := versionsRaw(t, ds, "a")
	if !bytes.Equal(raw, []byte("{}")) {
		t.Fatal("canceled SQL waiter wrote Settings")
	}
}
func TestStartupKidsDefaultsDSOpaqueReceiptAndInvalidInputs(t *testing.T) {
	f := newSettingsVersionsPG(t)
	f.user(t, "a")
	f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='a'`)
	expected := startupDefaultsRevision(t, f.ds, "a")
	for _, input := range [][]UserSettingsRevision{{{}}, {expected, expected}, {UserSettingsRevision{snapshot: &userSettingsSnapshot{ds: f.ds, id: "a"}}}} {
		r, _, _, err := f.ds.CommitKidsProfileDefaults(t.Context(), input)
		if r != nil || !errors.Is(err, ErrUserSettingsConflict) {
			t.Fatal("invalid storage snapshot accepted", err)
		}
	}
	other := &DataStore{pool: f.ds.pool}
	if r, _, _, err := other.CommitKidsProfileDefaults(t.Context(), []UserSettingsRevision{expected}); r != nil || !errors.Is(err, ErrUserSettingsConflict) {
		t.Fatal("foreign datastore snapshot accepted", err)
	}
	ctx := t.Context()
	receipt, n, _, err := f.ds.CommitKidsProfileDefaults(ctx, []UserSettingsRevision{expected})
	if err != nil || n != 1 || receipt == nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	copy := *receipt
	if _, err = f.ds.AcceptUserSettingsCommit(ctx, &copy, []UserSettingsRevision{expected}); !errors.Is(err, ErrUserSettingsConflict) {
		t.Fatal("copied receipt accepted")
	}
	if _, err = other.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{expected}); !errors.Is(err, ErrUserSettingsConflict) {
		t.Fatal("wrong DS accepted receipt")
	}
	wrong, stop := context.WithCancel(ctx)
	defer stop()
	if _, err = f.ds.AcceptUserSettingsCommit(wrong, receipt, []UserSettingsRevision{expected}); !errors.Is(err, ErrUserSettingsConflict) {
		t.Fatal("rebound receipt context accepted")
	}
	records, err := f.ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{expected})
	if err != nil || len(records) != 1 {
		t.Fatal(err)
	}
	if _, err = f.ds.AcceptUserSettingsCommit(ctx, receipt, []UserSettingsRevision{expected}); !errors.Is(err, ErrUserSettingsConflict) {
		t.Fatal("receipt replay accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if r, _, _, err := f.ds.CommitKidsProfileDefaults(cancelled, []UserSettingsRevision{records[0].Revision}); r != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled operation accepted", err)
	}
	if r, n, ids, err := f.ds.CommitKidsProfileDefaults(ctx, nil); r != nil || n != 0 || len(ids) != 0 || err != nil {
		t.Fatal(fmt.Sprint("empty candidate set changed state: ", err))
	}
}
