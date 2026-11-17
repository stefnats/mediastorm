package datastore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type creationOperationDenyHTTP struct{}

func (creationOperationDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("HTTP denied in storage-only ledger fixture")
}
func creationOperationFixture(t *testing.T) *settingsDSFixture {
	t.Helper()
	before := http.DefaultTransport
	http.DefaultTransport = creationOperationDenyHTTP{}
	t.Cleanup(func() { http.DefaultTransport = before })
	return newSettingsDS(t) // Real New, all migrations, own schema/NOLOGIN role.
}
func creationOperationTestMetadata(actor string) creationOperationMetadata {
	return creationOperationMetadata{ActorID: actor, OwnerID: "synthetic_owner", Entry: creationOperationManagement,
		SessionDigest: strings.Repeat("1", 64), ClientDigest: strings.Repeat("2", 64), RequestDigest: strings.Repeat("3", 64), BodyDigest: strings.Repeat("4", 64), BodyBytes: 17,
		SessionRowDigest: strings.Repeat("5", 64), ActorRowDigest: strings.Repeat("6", 64), OwnerRowDigest: strings.Repeat("7", 64),
		SessionVersion: "1:2:(0,3)", ActorVersion: "1:3:(0,4)", OwnerVersion: "1:4:(0,5)", SessionExpiry: creationOperationUTC(time.Now().Add(time.Hour))}
}
func creationOperationReserve(t *testing.T, ds *DataStore, ctx context.Context, actor string) *creationOperationReference {
	t.Helper()
	r, e := ds.reserveProfileCreationOperation(ctx, creationOperationTestMetadata(actor))
	if e != nil {
		t.Fatal("reserve private ledger", e)
	}
	t.Cleanup(r.close)
	return r
}
func creationOperationClaim(t *testing.T, ds *DataStore, ctx context.Context, r *creationOperationReference) *creationOperationAttempt {
	t.Helper()
	a, e := ds.claimProfileCreationAttempt(ctx, r)
	if e != nil {
		t.Fatal("claim private ledger", e)
	}
	t.Cleanup(a.close)
	return a
}
func creationOperationBegin(t *testing.T, ds *DataStore, ctx context.Context, a *creationOperationAttempt) *creationOperationFence {
	t.Helper()
	f, e := ds.beginProfileCreationOperation(ctx, a)
	if e != nil {
		t.Fatal("hold private operation fence", e)
	}
	t.Cleanup(f.close)
	return f
}
func creationOperationObserve(t *testing.T, ds *DataStore, ctx context.Context, raw [32]byte) *creationOperationSnapshot {
	t.Helper()
	s, e := ds.readFencedProfileCreationOperation(ctx, raw[:])
	if e != nil {
		t.Fatal("read private ledger", e)
	}
	t.Cleanup(s.close)
	return s
}

type creationOperationExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

// Direct quota/expiry fixtures seed only the NEW private ledger, not users or
// settings. They are deliberately distinct from protocol reservations.
func creationOperationSeed(t *testing.T, q creationOperationExecutor, metadata creationOperationMetadata, state string, aged bool) (string, [32]byte) {
	t.Helper()
	normalized, raw, e := creationOperationCloneMetadata(metadata)
	if e != nil {
		t.Fatal(e)
	}
	var ref [32]byte
	id := uuid.NewString()
	copy(ref[:], []byte(uuid.NewString()))
	now := creationOperationUTC(time.Now())
	if aged {
		now = now.Add(-time.Hour)
	}
	var settled any
	if state == "terminal" || state == "retired_unattempted" {
		settled = now.Add(time.Second)
	}
	_, e = q.Exec(t.Context(), `INSERT INTO profile_creation_operations(operation_id,target_id,marker_id,reference_hash,actor_id,metadata,charged_bytes,captured_at,write_until,read_until,state,version,settled_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,1,$12)`, id, uuid.NewString(), uuid.NewString(), creationOperationDigest(ref[:]), normalized.ActorID, raw, creationOperationCharge(raw, normalized.ActorID), now, now.Add(2*time.Second), now.Add(3*time.Second), state, settled)
	if e != nil {
		t.Fatal("seed owned ledger control", e)
	}
	return id, ref
}
func creationOperationRows(t *testing.T, f *settingsDSFixture) int {
	t.Helper()
	var n int
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func creationOperationWaitLock(t *testing.T, f *settingsDSFixture, sqlFragment string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		var found bool
		e := f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2)`, f.schema, "%"+sqlFragment+"%").Scan(&found)
		if e != nil {
			t.Fatal("own admin observer lock telemetry", e)
		}
		if found {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("actual owned lock waiter not observed")
		case <-tick.C:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
}
func creationOperationJoin(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case e := <-done:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("ledger worker did not join")
		return nil
	}
}
func creationOperationPool(t *testing.T, f *settingsDSFixture, leading string, tracer pgx.QueryTracer) *DataStore {
	t.Helper()
	u, e := url.Parse(f.uri)
	if e != nil {
		t.Fatal(e)
	}
	q := u.Query()
	q.Set("search_path", leading+f.schema+",pg_catalog")
	u.RawQuery = q.Encode()
	cfg, e := pgxpool.ParseConfig(u.String())
	if e != nil {
		t.Fatal(e)
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.ConnConfig.Tracer = tracer
	pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(pool.Close)
	// Variant raw pool on the already fully migrated fixture. Running New on a
	// leading empty schema would create a DIFFERENT ledger and miss this case.
	return &DataStore{pool: pool}
}

func TestProfileCreationOperationsMetadataOpaqueAndNoExistingWrites(t *testing.T) {
	f := creationOperationFixture(t)
	ctx := t.Context()
	f.user(t, "unchanged")
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('unchanged','{"future":{"number":9007199254740993},"display":{"simpleMode":false}}')`)
	before := creationOperationExistingRows(t, f)
	expiry := creationOperationUTC(time.Now().Add(40 * time.Minute))
	metadata := creationOperationTestMetadata("actor")
	metadata.OwnerExpiry = &expiry
	r, e := f.ds.reserveProfileCreationOperation(ctx, metadata)
	if e != nil {
		t.Fatal(e)
	}
	defer r.close()
	expiry = expiry.Add(time.Hour)
	metadata.OwnerID = "changed_input"
	s := creationOperationObserve(t, f.ds, ctx, r.raw)
	if s.record.metadata.OwnerID != "synthetic_owner" || s.record.metadata.OwnerExpiry.Equal(expiry) || s.record.referenceHash != creationOperationDigest(r.raw[:]) {
		t.Fatal("immutable input/ref-hash binding lost")
	}
	var stored string
	if e = f.ds.pool.QueryRow(ctx, `SELECT to_jsonb(p)::text FROM profile_creation_operations p WHERE operation_id=$1`, r.record.id).Scan(&stored); e != nil {
		t.Fatal(e)
	}
	if strings.Contains(stored, hex.EncodeToString(r.raw[:])) {
		t.Fatal("raw read reference persisted")
	}
	for _, value := range []any{r, s} {
		raw, e := json.Marshal(value)
		if e != nil || string(raw) != "null" || !strings.Contains(fmt.Sprintf("%+v", value), "opaque") {
			t.Fatal("opaque handle leaked")
		}
	}
	a := creationOperationClaim(t, f.ds, ctx, r)
	held := creationOperationBegin(t, f.ds, ctx, a)
	if e = held.finish(ctx); e != nil {
		t.Fatal(e)
	}
	if after := creationOperationExistingRows(t, f); after != before {
		t.Fatal("ledger wrote an existing users/settings/auth table")
	}
	if n, e := f.ds.pruneProfileCreationOperations(ctx, 64); e != nil || n != 0 {
		t.Fatal("terminal evidence pruned before original read window", n, e)
	}
}
func TestProfileCreationOperationsMetadataValidationIsNotAuthProof(t *testing.T) {
	f := creationOperationFixture(t)
	for _, kind := range []string{"bad_digest", "oversized_version", "missing_owner", "invalid_entry", "negative_body", "expired_correlation"} {
		t.Run(kind, func(t *testing.T) {
			m := creationOperationTestMetadata("actor")
			switch kind {
			case "bad_digest":
				m.SessionDigest = "not-a-hash"
			case "oversized_version":
				m.ActorVersion = strings.Repeat("1", 8200) + ":1:(0,1)"
			case "missing_owner":
				m.OwnerID = ""
			case "invalid_entry":
				m.Entry = 0
			case "negative_body":
				m.BodyBytes = -1
			case "expired_correlation":
				m.SessionExpiry = time.Now().Add(-time.Hour)
			}
			if r, e := f.ds.reserveProfileCreationOperation(t.Context(), m); e == nil || r != nil {
				t.Fatal("invalid storage metadata accepted")
			}
		})
	}
	// This actor need not exist: the private storage API is NOT authentication.
	r := creationOperationReserve(t, f.ds, t.Context(), "not_an_authenticated_account")
	if r.record.actor != "not_an_authenticated_account" {
		t.Fatal("stored correlation changed")
	}
}
func TestProfileCreationOperationsImmutableSQLAndStateGraph(t *testing.T) {
	f := creationOperationFixture(t)
	ctx := t.Context()
	r := creationOperationReserve(t, f.ds, ctx, "actor")
	for _, set := range []string{"actor_id='other'", "reference_hash=repeat('a',64)", "metadata=metadata-'clientDigest'", "charged_bytes=charged_bytes+1", "state='terminal',settled_at=clock_timestamp()", "version=version+1"} {
		if _, e := f.ds.pool.Exec(ctx, `UPDATE profile_creation_operations SET `+set+` WHERE operation_id=$1`, r.record.id); e == nil {
			t.Fatal("immutable/invalid direct update accepted", set)
		}
	}
	old := creationOperationObserve(t, f.ds, ctx, r.raw)
	a := creationOperationClaim(t, f.ds, ctx, r)
	if _, e := f.ds.beginProfileCreationSettlement(ctx, old); !errors.Is(e, errCreationOperationChanged) {
		t.Fatal("stale ledger version survived", e)
	}
	held := creationOperationBegin(t, f.ds, ctx, a)
	if e := held.recordUnknown(ctx); e != nil {
		t.Fatal(e)
	}
	unknown := creationOperationObserve(t, f.ds, ctx, r.raw)
	if unknown.record.state != "unknown" {
		t.Fatal("unknown not durable")
	}
	settlement, e := f.ds.beginProfileCreationSettlement(ctx, unknown)
	if e != nil {
		t.Fatal(e)
	}
	defer settlement.close()
	if e = settlement.fence.finish(ctx); !errors.Is(e, errCreationOperationChanged) {
		t.Fatal("settlement became execution capability")
	}
	if e = settlement.finish(ctx); e != nil {
		t.Fatal(e)
	}
	if e = settlement.finish(ctx); !errors.Is(e, errCreationOperationChanged) {
		t.Fatal("settlement replay accepted")
	}
	if _, e = f.ds.pool.Exec(ctx, `UPDATE profile_creation_operations SET state='attempting',settled_at=NULL,version=version+1 WHERE operation_id=$1`, r.record.id); e == nil {
		t.Fatal("terminal reactivated")
	}
}

type creationOperationTrace struct{ queries atomic.Int32 }

func (q *creationOperationTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	q.queries.Add(1)
	return ctx
}
func (*creationOperationTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func TestProfileCreationOperationsLocalSealsAndOneShotZeroSQL(t *testing.T) {
	f := creationOperationFixture(t)
	ctx := t.Context()
	trace := &creationOperationTrace{}
	ds := creationOperationPool(t, f, "", trace)
	r := creationOperationReserve(t, ds, ctx, "actor")
	trace.queries.Store(0)
	copyRef := &creationOperationReference{self: r, store: ds, ctx: ctx}
	anotherStore := &DataStore{pool: ds.pool}
	for _, call := range []func() error{
		func() error { _, e := ds.claimProfileCreationAttempt(ctx, copyRef); return e },
		func() error { _, e := anotherStore.claimProfileCreationAttempt(ctx, r); return e },
		func() error {
			_, e := ds.claimProfileCreationAttempt(context.WithValue(ctx, struct{}{}, "other"), r)
			return e
		},
	} {
		if e := call(); !errors.Is(e, errCreationOperationChanged) {
			t.Fatal("local seal accepted", e)
		}
	}
	if trace.queries.Load() != 0 {
		t.Fatal("invalid local seal reached SQL")
	}
	a := creationOperationClaim(t, ds, ctx, r)
	trace.queries.Store(0)
	if _, e := ds.claimProfileCreationAttempt(ctx, r); !errors.Is(e, errCreationOperationChanged) || trace.queries.Load() != 0 {
		t.Fatal("claim replay reached SQL", e)
	}
	copyAttempt := &creationOperationAttempt{self: a, store: ds, ctx: ctx}
	if _, e := ds.beginProfileCreationOperation(ctx, copyAttempt); !errors.Is(e, errCreationOperationChanged) || trace.queries.Load() != 0 {
		t.Fatal("attempt copy reached SQL", e)
	}
	fence := creationOperationBegin(t, ds, ctx, a)
	if e := fence.finish(ctx); e != nil {
		t.Fatal(e)
	}
	trace.queries.Store(0)
	if _, e := ds.beginProfileCreationOperation(ctx, a); !errors.Is(e, errCreationOperationChanged) || trace.queries.Load() != 0 {
		t.Fatal("attempt replay reached SQL", e)
	}
	if e := fence.finish(ctx); !errors.Is(e, errCreationOperationChanged) || trace.queries.Load() != 0 {
		t.Fatal("finish replay reached SQL", e)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	trace.queries.Store(0)
	if _, e := ds.reserveProfileCreationOperation(canceled, creationOperationTestMetadata("actor")); !errors.Is(e, context.Canceled) || trace.queries.Load() != 0 {
		t.Fatal("canceled reserve reached SQL", e)
	}
}
func TestProfileCreationOperationsActorActiveAndRetainedTerminalBounds(t *testing.T) {
	f := creationOperationFixture(t)
	ctx := t.Context()
	for n := 0; n < 7; n++ {
		creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "reserved", false)
	}
	r := creationOperationReserve(t, f.ds, ctx, "actor")
	if _, e := f.ds.reserveProfileCreationOperation(ctx, creationOperationTestMetadata("actor")); !errors.Is(e, errCreationOperationCapacity) {
		t.Fatal("actor cap exceeded", e)
	}
	a := creationOperationClaim(t, f.ds, ctx, r)
	held := creationOperationBegin(t, f.ds, ctx, a)
	if e := held.finish(ctx); e != nil {
		t.Fatal(e)
	}
	creationOperationReserve(t, f.ds, ctx, "actor")
	if creationOperationRows(t, f) != 9 {
		t.Fatal("terminal should release active capacity without deleting retained evidence")
	}
}
func TestProfileCreationOperationsGlobalAndByteBounds(t *testing.T) {
	t.Run("retained_rows", func(t *testing.T) {
		f := creationOperationFixture(t)
		for n := 0; n < creationOperationMaximumRows; n++ {
			creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata(fmt.Sprintf("actor_%d", n)), "terminal", false)
		}
		if _, e := f.ds.reserveProfileCreationOperation(t.Context(), creationOperationTestMetadata("new_actor")); !errors.Is(e, errCreationOperationCapacity) {
			t.Fatal("global retained cap exceeded", e)
		}
	})
	t.Run("retained_bytes", func(t *testing.T) {
		f := creationOperationFixture(t)
		m := creationOperationTestMetadata("bytes_actor")
		long := strings.Repeat("1", 8000) + ":1:(0,1)"
		m.SessionVersion = long
		m.ActorVersion = long
		m.OwnerVersion = long
		_, raw, e := creationOperationCloneMetadata(m)
		if e != nil {
			t.Fatal(e)
		}
		charge := creationOperationCharge(raw, m.ActorID)
		count := creationOperationMaximumBytes / charge
		if count >= creationOperationMaximumRows {
			t.Fatal("byte fixture did not isolate byte limit")
		}
		for n := 0; n < count; n++ {
			creationOperationSeed(t, f.ds.pool, m, "terminal", false)
		}
		if _, e = f.ds.reserveProfileCreationOperation(t.Context(), m); !errors.Is(e, errCreationOperationCapacity) {
			t.Fatal("byte cap exceeded", e)
		}
	})
}
func TestProfileCreationOperationsParallelActorReservations(t *testing.T) {
	f := creationOperationFixture(t)
	var wg sync.WaitGroup
	var accepted atomic.Int32
	outcomes := make(chan error, 16)
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := f.ds.reserveProfileCreationOperation(t.Context(), creationOperationTestMetadata("actor"))
			if e == nil {
				accepted.Add(1)
				r.close()
			}
			outcomes <- e
		}()
	}
	wg.Wait()
	close(outcomes)
	for e := range outcomes {
		if e != nil && !errors.Is(e, errCreationOperationCapacity) {
			t.Fatal(e)
		}
	}
	if accepted.Load() != 8 || creationOperationRows(t, f) != 8 {
		t.Fatal("serialized actor admission not exact", accepted.Load())
	}
}
func TestProfileCreationOperationsBudgetWaitUsesFreshRCCount(t *testing.T) {
	f := creationOperationFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	for n := 0; n < 7; n++ {
		creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "reserved", false)
	}
	blocker, e := f.ds.pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(blocker, t.Context())
	if e = creationOperationBudgetLock(t.Context(), blocker); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		r, e := f.ds.reserveProfileCreationOperation(ctx, creationOperationTestMetadata("actor"))
		if r != nil {
			r.close()
		}
		done <- e
	}()
	joined := false
	defer func() {
		cancel()
		creationOperationRollback(blocker, t.Context())
		if !joined {
			creationOperationJoin(t, done)
		}
	}()
	creationOperationWaitLock(t, f, "pg_advisory_xact_lock")
	creationOperationSeed(t, blocker, creationOperationTestMetadata("actor"), "reserved", false)
	if e = blocker.Commit(t.Context()); e != nil {
		t.Fatal(e)
	}
	e = creationOperationJoin(t, done)
	joined = true
	if !errors.Is(e, errCreationOperationCapacity) || creationOperationRows(t, f) != 8 {
		t.Fatal("post-wait committed row missed", e)
	}
}
func TestProfileCreationOperationsAliasSearchPathsShareActualRelationBudget(t *testing.T) {
	f := creationOperationFixture(t)
	leadingA, leadingB := f.schema+"_a", f.schema+"_b"
	for _, name := range []string{leadingA, leadingB} {
		if _, e := f.admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); e != nil {
			t.Fatal(e)
		}
	}
	t.Cleanup(func() {
		for _, name := range []string{leadingA, leadingB} {
			if _, e := f.admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); e != nil {
				t.Error(e)
			}
		}
	})
	a, b := creationOperationPool(t, f, leadingA+",", nil), creationOperationPool(t, f, leadingB+",", nil)
	for _, ds := range []*DataStore{a, b} {
		var schema string
		var oid, expected uint32
		if e := ds.pool.QueryRow(t.Context(), `SELECT current_schema(),to_regclass('profile_creation_operations')::oid`).Scan(&schema, &oid); e != nil {
			t.Fatal(e)
		}
		if e := f.ds.pool.QueryRow(t.Context(), `SELECT 'profile_creation_operations'::regclass::oid`).Scan(&expected); e != nil || oid != expected || schema == f.schema {
			t.Fatal("alias fixture did not reach same ledger via different leading schema")
		}
	}
	for n := 0; n < 7; n++ {
		creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "reserved", false)
	}
	blocker, e := a.pool.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(blocker, t.Context())
	if e = creationOperationBudgetLock(t.Context(), blocker); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		r, e := b.reserveProfileCreationOperation(ctx, creationOperationTestMetadata("actor"))
		if r != nil {
			r.close()
		}
		done <- e
	}()
	joined := false
	defer func() {
		cancel()
		creationOperationRollback(blocker, t.Context())
		if !joined {
			creationOperationJoin(t, done)
		}
	}()
	creationOperationWaitLock(t, f, "pg_advisory_xact_lock")
	creationOperationSeed(t, blocker, creationOperationTestMetadata("actor"), "reserved", false)
	if e = blocker.Commit(t.Context()); e != nil {
		t.Fatal(e)
	}
	e = creationOperationJoin(t, done)
	joined = true
	if !errors.Is(e, errCreationOperationCapacity) || creationOperationRows(t, f) != 8 {
		t.Fatal("alias paths bypassed shared ledger budget", e)
	}
}
func TestProfileCreationOperationsBudgetCancelJoinsActualWaiter(t *testing.T) {
	f := creationOperationFixture(t)
	blocker, e := f.ds.pool.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(blocker, t.Context())
	if e = creationOperationBudgetLock(t.Context(), blocker); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := f.ds.reserveProfileCreationOperation(ctx, creationOperationTestMetadata("actor"))
		done <- e
	}()
	joined := false
	defer func() {
		cancel()
		creationOperationRollback(blocker, t.Context())
		if !joined {
			creationOperationJoin(t, done)
		}
	}()
	creationOperationWaitLock(t, f, "pg_advisory_xact_lock")
	cancel()
	e = creationOperationJoin(t, done)
	joined = true
	if !errors.Is(e, context.Canceled) || creationOperationRows(t, f) != 0 {
		t.Fatal("canceled actual budget wait wrote", e)
	}
}
func TestProfileCreationOperationsRowFenceFreshReadAndCancel(t *testing.T) {
	for _, mode := range []string{"commit", "rollback", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := creationOperationFixture(t)
			ctx := t.Context()
			r := creationOperationReserve(t, f.ds, ctx, "actor")
			a := creationOperationClaim(t, f.ds, ctx, r)
			held := creationOperationBegin(t, f.ds, ctx, a)
			readCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			var observed string
			go func() {
				s, e := f.ds.readFencedProfileCreationOperation(readCtx, r.raw[:])
				if s != nil {
					observed = s.record.state
					s.close()
				}
				done <- e
			}()
			joined := false
			defer func() {
				cancel()
				held.close()
				if !joined {
					creationOperationJoin(t, done)
				}
			}()
			creationOperationWaitLock(t, f, "FOR UPDATE")
			switch mode {
			case "commit":
				if e := held.finish(ctx); e != nil {
					t.Fatal(e)
				}
			case "rollback":
				held.close()
			case "cancel":
				cancel()
			}
			e := creationOperationJoin(t, done)
			joined = true
			if mode == "cancel" {
				if !errors.Is(e, context.Canceled) || observed != "" {
					t.Fatal("cancelled fenced read published", e)
				}
			} else if e != nil || mode == "commit" && observed != "terminal" || mode == "rollback" && observed != "attempting" {
				t.Fatal("fresh post-wait ledger state wrong", mode, observed, e)
			}
		})
	}
}
func TestProfileCreationOperationsUnresolvedExpiryPruneAndFreshSettlement(t *testing.T) {
	f := creationOperationFixture(t)
	ctx := t.Context()
	_, unknownRef := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "unknown", true)
	_, unusedRef := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("other_actor"), "reserved", true)
	if n, e := f.ds.pruneProfileCreationOperations(ctx, 64); e != nil || n != 0 {
		t.Fatal("unresolved expired evidence deleted", n, e)
	}
	unknown := creationOperationObserve(t, f.ds, ctx, unknownRef)
	settlement, e := f.ds.beginProfileCreationSettlement(ctx, unknown)
	if e != nil {
		t.Fatal(e)
	}
	defer settlement.close()
	if e = settlement.fence.finish(ctx); !errors.Is(e, errCreationOperationChanged) {
		t.Fatal("old write capability renewed")
	}
	if e = settlement.finish(ctx); e != nil {
		t.Fatal(e)
	}
	unused := creationOperationObserve(t, f.ds, ctx, unusedRef)
	if e = f.ds.retireUnusedProfileCreationOperation(ctx, unused); e != nil {
		t.Fatal(e)
	}
	if n, e := f.ds.pruneProfileCreationOperations(ctx, 64); e != nil || n != 2 {
		t.Fatal("safe closed evidence not pruned", n, e)
	}
	if _, e = f.ds.readFencedProfileCreationOperation(ctx, unknownRef[:]); !errors.Is(e, errCreationOperationMissing) {
		t.Fatal("old reference revived", e)
	}
}
func TestProfileCreationOperationsPruneSkipsHeldTerminalWithoutBudgetCycle(t *testing.T) {
	f := creationOperationFixture(t)
	ctx := t.Context()
	id, _ := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "terminal", true)
	held, e := f.ds.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer creationOperationRollback(held, ctx)
	if e = creationOperationLockRow(ctx, held, id); e != nil {
		t.Fatal(e)
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if n, e := f.ds.pruneProfileCreationOperations(bounded, 64); e != nil || n != 0 {
		t.Fatal("prune waited for held operation", n, e)
	}
	creationOperationReserve(t, f.ds, ctx, "new_actor")
	if e = held.Rollback(ctx); e != nil {
		t.Fatal(e)
	}
	if n, e := f.ds.pruneProfileCreationOperations(ctx, 64); e != nil || n != 1 {
		t.Fatal("unlocked terminal not pruned", n, e)
	}
}

// Real PostgreSQL bytes are forwarded; only an actual COMMIT acknowledgement
// is lost after it reaches the client socket. This is not an HTTP response fault.
type creationOperationLostAckConn struct {
	net.Conn
	enabled, dropped *atomic.Bool
	armed            atomic.Bool
}

func (c *creationOperationLostAckConn) Write(raw []byte) (int, error) {
	if c.enabled.Load() {
		for rest := raw; len(rest) >= 5; {
			n := int(uint32(rest[1])<<24 | uint32(rest[2])<<16 | uint32(rest[3])<<8 | uint32(rest[4]))
			if n < 4 || len(rest) < n+1 {
				break
			}
			if rest[0] == 'Q' && strings.EqualFold(strings.TrimRight(string(rest[5:n+1]), "\x00"), "commit") {
				c.armed.Store(true)
			}
			rest = rest[n+1:]
		}
	}
	return c.Conn.Write(raw)
}
func (c *creationOperationLostAckConn) Read(raw []byte) (int, error) {
	n, e := c.Conn.Read(raw)
	if c.armed.Load() && n > 0 {
		c.dropped.Store(true)
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	return n, e
}
func TestProfileCreationOperationsRealLostCommitAckRetainsActualLedgerWrite(t *testing.T) {
	f := creationOperationFixture(t)
	cfg, e := pgxpool.ParseConfig(f.uri)
	if e != nil {
		t.Fatal(e)
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	enabled, dropped := &atomic.Bool{}, &atomic.Bool{}
	expected := filepath.Join(cfg.ConnConfig.Host, fmt.Sprintf(".s.PGSQL.%d", cfg.ConnConfig.Port))
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "unix" || address != expected {
			return nil, errors.New("only exact fixture Unix socket allowed")
		}
		connection, e := (&net.Dialer{}).DialContext(ctx, "unix", address)
		if e != nil {
			return nil, e
		}
		return &creationOperationLostAckConn{Conn: connection, enabled: enabled, dropped: dropped}, nil
	}
	pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	if e = pool.Ping(t.Context()); e != nil {
		t.Fatal(e)
	}
	enabled.Store(true)
	r, e := ds.reserveProfileCreationOperation(t.Context(), creationOperationTestMetadata("actor"))
	if r != nil {
		r.close()
	}
	if !errors.Is(e, errCreationOperationUnknown) || r != nil || !dropped.Load() {
		t.Fatal("real reservation COMMIT acknowledgement not lost", e)
	}
	if creationOperationRows(t, f) != 1 {
		t.Fatal("ambiguous COMMIT was incorrectly treated as no storage write")
	}
	var state string
	if e = f.ds.pool.QueryRow(t.Context(), `SELECT state FROM profile_creation_operations`).Scan(&state); e != nil || state != "reserved" {
		t.Fatal("independent connection did not see actual committed ledger row", e)
	}
}

func TestProfileCreationOperationsCloseCancelsAndJoinsBlockedMutation(t *testing.T) {
	for _, mode := range []string{"reference", "attempt"} {
		t.Run(mode, func(t *testing.T) {
			f := creationOperationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := creationOperationReserve(t, f.ds, ctx, "actor")
			var attempt *creationOperationAttempt
			if mode == "attempt" {
				attempt = creationOperationClaim(t, f.ds, ctx, r)
			}
			blocker, e := f.ds.pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer creationOperationRollback(blocker, t.Context())
			if e = creationOperationLockRow(ctx, blocker, r.record.id); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() {
				if mode == "reference" {
					a, e := f.ds.claimProfileCreationAttempt(ctx, r)
					if a != nil {
						a.close()
					}
					done <- e
				} else {
					held, e := f.ds.beginProfileCreationOperation(ctx, attempt)
					if held != nil {
						held.close()
					}
					done <- e
				}
			}()
			joined := false
			defer func() {
				cancel()
				creationOperationRollback(blocker, t.Context())
				if !joined {
					creationOperationJoin(t, done)
				}
			}()
			creationOperationWaitLock(t, f, "FOR UPDATE")
			if mode == "reference" {
				r.close()
			} else {
				attempt.close()
			}
			e = creationOperationJoin(t, done)
			joined = true
			if e == nil {
				t.Fatal("close did not retire blocked local mutation")
			}
			if e = blocker.Rollback(t.Context()); e != nil {
				t.Fatal(e)
			}
			s := creationOperationObserve(t, f.ds, ctx, r.raw)
			expected := "reserved"
			if mode == "attempt" {
				expected = "attempting"
			}
			if s.record.state != expected {
				t.Fatal("canceled blocked mutation changed ledger state")
			}
		})
	}
}
func TestProfileCreationOperationsCorruptStoredMetadataAndVersionOverflow(t *testing.T) {
	t.Run("required_field_null", func(t *testing.T) {
		f := creationOperationFixture(t)
		id, raw := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "reserved", false)
		// Direct fixture corruption of the NEW table only. Normal methods and
		// its update guard cannot produce missing/null immutable metadata.
		f.exec(t, `ALTER TABLE profile_creation_operations DISABLE TRIGGER profile_creation_operations_immutable`)
		f.exec(t, `UPDATE profile_creation_operations SET metadata=jsonb_set(metadata,'{bodyBytes}','null') WHERE operation_id=$1`, id)
		f.exec(t, `ALTER TABLE profile_creation_operations ENABLE TRIGGER profile_creation_operations_immutable`)
		if s, e := f.ds.readFencedProfileCreationOperation(t.Context(), raw[:]); s != nil || !errors.Is(e, errCreationOperationUnavailable) {
			if s != nil {
				s.close()
			}
			t.Fatal("malformed stored metadata accepted", e)
		}
	})
	t.Run("overflow", func(t *testing.T) {
		f := creationOperationFixture(t)
		r := creationOperationReserve(t, f.ds, t.Context(), "actor")
		f.exec(t, `ALTER TABLE profile_creation_operations DISABLE TRIGGER profile_creation_operations_immutable`)
		f.exec(t, `UPDATE profile_creation_operations SET version=9223372036854775807 WHERE operation_id=$1`, r.record.id)
		f.exec(t, `ALTER TABLE profile_creation_operations ENABLE TRIGGER profile_creation_operations_immutable`)
		r.record.version = 9223372036854775807
		if a, e := f.ds.claimProfileCreationAttempt(t.Context(), r); a != nil || e == nil {
			if a != nil {
				a.close()
			}
			t.Fatal("version overflow admitted")
		}
		var state string
		var version int64
		if e := f.ds.pool.QueryRow(t.Context(), `SELECT state,version FROM profile_creation_operations WHERE operation_id=$1`, r.record.id).Scan(&state, &version); e != nil || state != "reserved" || version != 9223372036854775807 {
			t.Fatal("overflow did not roll back", e)
		}
	})
}
