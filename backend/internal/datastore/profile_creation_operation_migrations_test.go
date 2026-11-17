package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func creationOperationExistingRows(t *testing.T, f *settingsDSFixture) string {
	t.Helper()
	var raw string
	e := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_object(
        'users',(SELECT jsonb_agg(to_jsonb(u)||jsonb_build_object('version',concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY id) FROM users u),
        'settings',(SELECT jsonb_agg(to_jsonb(s)||jsonb_build_object('version',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY user_id) FROM user_settings s),
        'settingsVersions',(SELECT jsonb_agg(to_jsonb(v)||jsonb_build_object('version',concat(v.tableoid::oid,':',v.xmin::text,':',v.ctid::text)) ORDER BY user_id) FROM user_settings_versions v),
        'accounts',(SELECT jsonb_agg(to_jsonb(a)||jsonb_build_object('version',concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text)) ORDER BY id) FROM accounts a),
        'sessions',(SELECT jsonb_agg(to_jsonb(s)||jsonb_build_object('version',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY token) FROM sessions s))::text`).Scan(&raw)
	if e != nil {
		t.Fatal("observe only own existing rows", e)
	}
	return raw
}
func creationOperationGoose(t *testing.T) {
	t.Helper()
	goose.SetBaseFS(embedMigrations)
	if e := goose.SetDialect("postgres"); e != nil {
		t.Fatal(e)
	}
}
func creationOperationDown(ctx context.Context, ds *DataStore) error {
	db := stdlib.OpenDBFromPool(ds.pool)
	defer db.Close()
	return goose.DownToContext(ctx, db, "migrations", 79)
}
func creationOperationVersion(t *testing.T, ds *DataStore) int64 {
	t.Helper()
	db := stdlib.OpenDBFromPool(ds.pool)
	defer db.Close()
	version, e := goose.GetDBVersionContext(t.Context(), db)
	if e != nil {
		t.Fatal(e)
	}
	return version
}
func TestProfileCreationOperationsFull080EmptyDownUpgradeAndRestart(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	creationOperationGoose(t)
	if version := creationOperationVersion(t, f.ds); version != 80 {
		t.Fatal("explicit080 fixture version", version)
	}
	if e := creationOperationDown(t.Context(), f.ds); e != nil {
		t.Fatal("empty080 Down", e)
	}
	if version := creationOperationVersion(t, f.ds); version != 79 {
		t.Fatal("empty Down did not preserve079", version)
	}
	var table, function bool
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT to_regclass('profile_creation_operations') IS NOT NULL,to_regprocedure('guard_profile_creation_operation()') IS NOT NULL`).Scan(&table, &function); e != nil || table || function {
		t.Fatal("empty Down retained new objects", e)
	}
	f.user(t, "old_profile")
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('old_profile','{"future":{"integer":9007199254740993},"display":{"simpleMode":false}}')`)
	before := creationOperationExistingRows(t, f)
	f.ds.Close()
	f.ds = f.open(t)
	if creationOperationVersion(t, f.ds) != 81 || creationOperationExistingRows(t, f) != before {
		t.Fatal("080 upgrade changed existing records")
	}
	r := creationOperationReserve(t, f.ds, t.Context(), "correlation_actor")
	a := creationOperationClaim(t, f.ds, t.Context(), r)
	held := creationOperationBegin(t, f.ds, t.Context(), a)
	if e := held.recordUnknown(t.Context()); e != nil {
		t.Fatal(e)
	}
	ref := r.raw
	// A new store can inspect durable ledger history, but cannot reconstruct
	// the old opaque execution handle or imply any current authorization.
	oldDS := f.ds
	f.ds.Close()
	f.ds = f.open(t)
	s := creationOperationObserve(t, f.ds, t.Context(), ref)
	if s.record.state != "unknown" || s.record.metadata.ActorID != "correlation_actor" {
		t.Fatal("durable unresolved record lost across New")
	}
	if _, e := f.ds.beginProfileCreationOperation(t.Context(), a); e != errCreationOperationChanged {
		t.Fatal("restart adopted old execution ticket", e)
	}
	if oldDS == f.ds || creationOperationExistingRows(t, f) != before {
		t.Fatal("restart changed existing data")
	}
}
func TestProfileCreationOperationsDownRefusesEveryRetainedState(t *testing.T) {
	for _, state := range []string{"reserved", "attempting", "unknown", "terminal", "retired_unattempted"} {
		t.Run(state, func(t *testing.T) {
			f := creationOperationFixture(t)
			creationOperationUse080(t, f)
			creationOperationGoose(t)
			_, raw := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), state, true)
			before := creationOperationObserve(t, f.ds, t.Context(), raw)
			if e := creationOperationDown(t.Context(), f.ds); e == nil {
				t.Fatal("Down discarded retained ledger", state)
			}
			if creationOperationVersion(t, f.ds) != 80 || creationOperationRows(t, f) != 1 {
				t.Fatal("refused Down changed version/data")
			}
			after := creationOperationObserve(t, f.ds, t.Context(), raw)
			if !sameCreationOperation(before.record, after.record) {
				t.Fatal("refused Down changed private provenance")
			}
			var helper bool
			if e := f.ds.pool.QueryRow(t.Context(), `SELECT to_regprocedure('guard_profile_creation_operation()') IS NOT NULL`).Scan(&helper); e != nil || !helper {
				t.Fatal("refused Down removed helper", e)
			}
		})
	}
}
func TestProfileCreationOperationsDownActualRelationNamespaceNotLeadingHelper(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	creationOperationGoose(t)
	leading := f.schema + "_function"
	if _, e := f.admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{leading}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := f.admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{leading}.Sanitize()+" CASCADE"); e != nil {
			t.Error(e)
		}
	})
	helper := pgx.Identifier{leading, "guard_profile_creation_operation"}.Sanitize()
	f.exec(t, `CREATE FUNCTION `+helper+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$`)
	alias := creationOperationPool(t, f, leading+",", nil)
	var unrelatedOID uint32
	if e := alias.pool.QueryRow(t.Context(), `SELECT to_regprocedure('guard_profile_creation_operation()')::oid`).Scan(&unrelatedOID); e != nil {
		t.Fatal(e)
	}
	if e := creationOperationDown(t.Context(), alias); e != nil {
		t.Fatal("actual alias-search-path empty Down", e)
	}
	if creationOperationVersion(t, alias) != 79 {
		t.Fatal("actual trailing migration version did not downgrade")
	}
	var preservedOID uint32
	if e := alias.pool.QueryRow(t.Context(), `SELECT to_regprocedure('guard_profile_creation_operation()')::oid`).Scan(&preservedOID); e != nil || preservedOID != unrelatedOID {
		t.Fatal("Down dropped unrelated leading helper", e)
	}
	var ledger, actualHelper bool
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT to_regclass($1) IS NOT NULL,to_regprocedure($2) IS NOT NULL`, pgx.Identifier{f.schema, "profile_creation_operations"}.Sanitize(), pgx.Identifier{f.schema, "guard_profile_creation_operation"}.Sanitize()+"()").Scan(&ledger, &actualHelper); e != nil || ledger || actualHelper {
		t.Fatal("actual ledger namespace retained new objects", e)
	}
}
func TestProfileCreationOperationsSeparateSchemasOwnTheirBudgetsAndRecords(t *testing.T) {
	first, second := creationOperationFixture(t), creationOperationFixture(t)
	for n := 0; n < 8; n++ {
		creationOperationReserve(t, first.ds, t.Context(), "same_actor")
	}
	creationOperationReserve(t, second.ds, t.Context(), "same_actor")
	if creationOperationRows(t, first) != 8 || creationOperationRows(t, second) != 1 {
		t.Fatal("schema records/budgets crossed")
	}
	held, e := first.ds.pool.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(held, t.Context())
	if e = creationOperationBudgetLock(t.Context(), held); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r, e := second.ds.reserveProfileCreationOperation(ctx, creationOperationTestMetadata("other_actor"))
	if e != nil {
		t.Fatal("different relation budget waited", e)
	}
	r.close()
}
func TestProfileCreationOperationsDownTableLockBeforeBudgetWaitAndCancel(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	creationOperationGoose(t)
	held, e := f.ds.pool.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(held, t.Context())
	if _, e = held.Exec(t.Context(), `LOCK TABLE profile_creation_operations IN ACCESS SHARE MODE`); e != nil {
		t.Fatal(e)
	}
	downCtx, cancelDown := context.WithCancel(t.Context())
	defer cancelDown()
	downDone := make(chan error, 1)
	go func() { downDone <- creationOperationDown(downCtx, f.ds) }()
	downJoined := false
	reserveJoined := false
	reserveCtx, cancelReserve := context.WithCancel(t.Context())
	defer cancelReserve()
	reserveDone := make(chan error, 1)
	startedReserve := false
	defer func() {
		cancelDown()
		cancelReserve()
		creationOperationRollback(held, t.Context())
		if !downJoined {
			creationOperationJoin(t, downDone)
		}
		if startedReserve && !reserveJoined {
			creationOperationJoin(t, reserveDone)
		}
	}()
	creationOperationWaitLock(t, f, "ACCESS EXCLUSIVE")
	go func() {
		r, e := f.ds.reserveProfileCreationOperation(reserveCtx, creationOperationTestMetadata("actor"))
		if r != nil {
			r.close()
		}
		reserveDone <- e
	}()
	startedReserve = true
	creationOperationWaitLock(t, f, "ACCESS SHARE")
	cancelDown()
	e = creationOperationJoin(t, downDone)
	downJoined = true
	if e == nil {
		t.Fatal("canceled waiting Down succeeded")
	}
	e = creationOperationJoin(t, reserveDone)
	reserveJoined = true
	if e != nil || creationOperationRows(t, f) != 1 || creationOperationVersion(t, f.ds) != 80 {
		t.Fatal("cancelled DDL blocked/changed admission", e)
	}
}
func TestProfileCreationOperationsUnsupportedLeadingViewFailsClosed(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	leading := f.schema + "_view"
	if _, e := f.admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{leading}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if _, e := f.admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{leading}.Sanitize()+" CASCADE"); e != nil {
			t.Error(e)
		}
	})
	f.exec(t, "CREATE VIEW "+pgx.Identifier{leading, "profile_creation_operations"}.Sanitize()+" AS SELECT * FROM "+pgx.Identifier{f.schema, "profile_creation_operations"}.Sanitize())
	ds := creationOperationPool(t, f, leading+",", nil)
	if r, e := ds.reserveProfileCreationOperation(t.Context(), creationOperationTestMetadata("actor")); r != nil || e == nil {
		if r != nil {
			r.close()
		}
		t.Fatal("unsupported view admitted a reservation")
	}
	if creationOperationRows(t, f) != 0 {
		t.Fatal("unsupported view wrote ledger")
	}
}

func TestProfileCreationOperationsRepeatableReadCounterexampleIsReadOnly(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	// This deliberately bad manual transaction is a CONTROL, not a supported
	// primitive. It demonstrates stale pre-wait RR observation without writes.
	bad, e := f.ds.pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(bad, t.Context())
	var old int
	if e = bad.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&old); e != nil {
		t.Fatal(e)
	}
	creationOperationReserve(t, f.ds, t.Context(), "actor")
	if e = creationOperationBudgetLock(t.Context(), bad); e != nil {
		t.Fatal(e)
	}
	var stale int
	if e = bad.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&stale); e != nil {
		t.Fatal(e)
	}
	if stale != old || creationOperationRows(t, f) != old+1 {
		t.Fatal("RR control failed to demonstrate old snapshot")
	}
	if e = bad.Rollback(t.Context()); e != nil {
		t.Fatal(e)
	}
}

// The080 feature controls deliberately run UpTo080, preserving their strict
// per-migration assertions. New() discovery/latest081 has separate controls.
func creationOperationUse080(t *testing.T, f *settingsDSFixture) {
	t.Helper()
	creationOperationGoose(t)
	db := stdlib.OpenDBFromPool(f.ds.pool)
	defer db.Close()
	if err := goose.DownToContext(t.Context(), db, "migrations", 80); err != nil {
		t.Fatal("prepare exact080 feature fixture", err)
	}
}
