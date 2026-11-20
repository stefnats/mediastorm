package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgxpool"
)

type timelineWrappedRepository struct{ RecordingRepository }

func timelineIdentity(t *testing.T, f *recordingStatePGFixture, id string) RecordingTimelineIdentity {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	value, err := f.ds.ReadRecordingTimelineIdentity(ctx, id)
	if err != nil {
		t.Fatal("read actual recording identity", err)
	}
	return value
}
func TestRecordingTimelineRepositoryReadinessUsesExactConcretePool(t *testing.T) {
	f := recordingStatePG(t, 84)
	other := recordingStatePG(t, 84)
	if !f.ds.RecordingRepositoryReady(f.ds.Recordings()) || f.ds.RecordingRepositoryReady(other.ds.Recordings()) || f.ds.RecordingRepositoryReady(timelineWrappedRepository{f.ds.Recordings()}) || f.ds.RecordingRepositoryReady(nil) {
		t.Fatal("repository readiness accepted substitute storage")
	}
	tx := f.begin(t)
	if f.ds.RecordingRepositoryReady(&pgRecordingRepo{pool: tx}) {
		t.Fatal("transaction repository accepted as original store")
	}
}
func TestRecordingTimelineIdentityReadDoesNotClaimOrTouchProgress(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "timeline")
	var before, after []byte
	if f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(s) FROM recording_playback_state s WHERE recording_id='timeline'`).Scan(&before) != nil {
		t.Fatal("read original bookkeeping")
	}
	first := timelineIdentity(t, f, "timeline")
	if first.RecordingID() != "timeline" || first.OwnerProfileID() != "one" || first.CreatedAt().IsZero() || first.Incarnation().String() == "00000000-0000-0000-0000-000000000000" {
		t.Fatal("wrong original identity")
	}
	second := timelineIdentity(t, f, "timeline")
	if !first.Same(second) || f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(s) FROM recording_playback_state s WHERE recording_id='timeline'`).Scan(&after) != nil || !bytes.Equal(before, after) {
		t.Fatal("reading identity mutated progress bookkeeping")
	}
	f.mutate(t, f.state(t, "timeline"), recordingPlaybackClaim)
	if !first.Same(timelineIdentity(t, f, "timeline")) {
		t.Fatal("writer counters/tombstone changed media incarnation")
	}
	f.exec(t, `UPDATE recordings SET title='Synthetic checkpoint',output_size_bytes=17 WHERE id='timeline'`)
	if !first.Same(timelineIdentity(t, f, "timeline")) {
		t.Fatal("ordinary checkpoint changed observed identity")
	}
}
func TestRecordingTimelineIdentityRejectsActualOwnerRunAndCreationABA(t *testing.T) {
	for _, kind := range []string{"owner", "created", "run", "delete recreate"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingStatePG(t, 84)
			f.profiles(t)
			r := f.create(t, "timeline")
			before := timelineIdentity(t, f, r.ID)
			switch kind {
			case "owner":
				f.exec(t, `UPDATE recordings SET user_id='two' WHERE id='timeline';UPDATE recordings SET user_id='one' WHERE id='timeline'`)
			case "created":
				f.exec(t, `UPDATE recordings SET created_at=created_at+interval '1 second' WHERE id='timeline';UPDATE recordings SET created_at=created_at-interval '1 second' WHERE id='timeline'`)
			case "run":
				f.exec(t, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-B"') WHERE id='timeline';UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-A"') WHERE id='timeline'`)
			case "delete recreate":
				if err := f.ds.Recordings().Delete(t.Context(), r.ID); err != nil {
					t.Fatal(err)
				}
				if err := f.ds.Recordings().Create(t.Context(), r); err != nil {
					t.Fatal(err)
				}
			}
			if before.Same(timelineIdentity(t, f, r.ID)) {
				t.Fatal("same visible identity revived original incarnation")
			}
		})
	}
}
func TestRecordingTimelineIdentityPreservesNullAndBoundsRawRunData(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "timeline")
	f.exec(t, `UPDATE recordings SET capture_state=NULL WHERE id='timeline'`)
	missing := timelineIdentity(t, f, "timeline")
	f.exec(t, `UPDATE recordings SET capture_state='{}' WHERE id='timeline'`)
	if !missing.Same(timelineIdentity(t, f, "timeline")) {
		t.Fatal("missing run became fabricated JSON null")
	}
	f.exec(t, `UPDATE recordings SET capture_state='{"runId":null}' WHERE id='timeline'`)
	if missing.Same(timelineIdentity(t, f, "timeline")) {
		t.Fatal("explicit JSON null adopted absent run")
	}
	f.exec(t, `UPDATE recordings SET capture_state=jsonb_build_object('runId',repeat('x',65536)) WHERE id='timeline'`)
	if _, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "timeline"); err == nil {
		t.Fatal("oversized raw run accepted")
	}
}

type timelineJoinedQueryKey struct{}
type timelineJoinedQueryTrace struct {
	mu     sync.Mutex
	rows   []int64
	errors []bool
}

func (trace *timelineJoinedQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "SELECT r.id,r.user_id") && strings.Contains(data.SQL, "s.incarnation::text") {
		return context.WithValue(ctx, timelineJoinedQueryKey{}, true)
	}
	return ctx
}
func (trace *timelineJoinedQueryTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if captured, _ := ctx.Value(timelineJoinedQueryKey{}).(bool); !captured {
		return
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.rows = append(trace.rows, data.CommandTag.RowsAffected())
	trace.errors = append(trace.errors, data.Err != nil)
}
func (trace *timelineJoinedQueryTrace) assert(t *testing.T, expected ...int64) {
	t.Helper()
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if len(trace.rows) != len(expected) {
		t.Fatal("actual joined SELECT count differs")
	}
	for i, want := range expected {
		if trace.rows[i] != want || trace.errors[i] {
			t.Fatalf("actual joined SELECT%d returned %d rows, want%d without server error", i, trace.rows[i], want)
		}
	}
}
func TestRecordingTimelineIdentityBoundsCompressedStateBeforeClientScan(t *testing.T) {
	for _, column := range []string{"capture_run_id", "observer_receipt"} {
		t.Run(column, func(t *testing.T) {
			f := recordingStatePG(t, 84)
			f.profiles(t)
			f.create(t, "timeline")
			trace := &timelineJoinedQueryTrace{}
			configuration := f.ds.pool.Config().Copy()
			if configuration.ConnConfig.Tracer == nil {
				t.Fatal("original query lifecycle tracer missing")
			}
			// Extend the copied original tracer chain; never replace its physical-cleanup join.
			configuration.ConnConfig.Tracer = multitracer.New(configuration.ConnConfig.Tracer, trace)
			pool, err := pgxpool.NewWithConfig(t.Context(), configuration)
			if err != nil {
				t.Fatal("open own traced fixture pool", err)
			}
			t.Cleanup(pool.Close)
			observed := &DataStore{pool: pool}
			if _, err := observed.ReadRecordingTimelineIdentity(t.Context(), "timeline"); err != nil {
				t.Fatal("valid joined SELECT control failed", err)
			}
			trace.assert(t, 1)
			// Fixed test-only identifiers; the repeated value is intentionally TOAST-compressible.
			query := `UPDATE recording_playback_state SET ` + pgx.Identifier{column}.Sanitize() + `=to_jsonb(repeat('x',65536)) WHERE recording_id='timeline'`
			f.exec(t, query)
			var stored, wire int
			if err := f.ds.pool.QueryRow(t.Context(), `SELECT pg_column_size(`+pgx.Identifier{column}.Sanitize()+`),octet_length(`+pgx.Identifier{column}.Sanitize()+`::text) FROM recording_playback_state WHERE recording_id='timeline'`).Scan(&stored, &wire); err != nil || stored >= 4096 || wire <= 65536 {
				t.Fatal("fixture did not establish small compressed storage with large JSON wire data", err)
			}
			if _, err := observed.ReadRecordingTimelineIdentity(t.Context(), "timeline"); err == nil {
				t.Fatal("compressed oversized state escaped actual wire-size bounds")
			}
			// A post-Scan tuple mismatch also returns an error; SELECT0 is the required
			// evidence that oversized JSON never entered a returned client row.
			trace.assert(t, 1, 0)
		})
	}
}

func TestRecordingTimelineIdentityMissingStateAndObserverNeverRepair(t *testing.T) {
	for _, kind := range []string{"missing state", "disabled observer", "replaced function"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingStatePG(t, 84)
			f.profiles(t)
			f.create(t, "timeline")
			switch kind {
			case "missing state":
				f.exec(t, `DELETE FROM recording_playback_state WHERE recording_id='timeline'`)
			case "disabled observer":
				f.exec(t, `ALTER TABLE recordings DISABLE TRIGGER recording_playback_identity`)
			case "replaced function":
				f.exec(t, `CREATE OR REPLACE FUNCTION observe_recording_playback_identity() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW;END;$$`)
			}
			if _, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "timeline"); err == nil {
				t.Fatal("unverified history produced identity")
			}
			if kind == "missing state" {
				var count int
				if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_state WHERE recording_id='timeline'`).Scan(&count) != nil || count != 0 {
					t.Fatal("read repaired missing observer state")
				}
			}
		})
	}
}
func TestRecordingTimelineIdentityBlockedQueryCancellationJoinsActualRead(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "timeline")
	lock, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer recordingStateRollback(lock)
	if _, err = lock.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "recording_playback_state"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.ds.ReadRecordingTimelineIdentity(ctx, "timeline"); done <- err }()
	deadline := time.Now().Add(time.Second)
	var blockedPID int
	for {
		var pending bool
		err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%s.incarnation%')`, f.schema).Scan(&pending)
		if err != nil {
			t.Fatal("observe own actual pending identity query", err)
		}
		if pending {
			if err = f.admin.QueryRow(t.Context(), `SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%s.incarnation%' LIMIT 1`, f.schema).Scan(&blockedPID); err != nil {
				t.Fatal("capture own blocked backend identity", err)
			}
			break
		}
		select {
		case <-done:
			t.Fatal("identity returned before actual blocked query")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("own actual query never blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled actual query produced identity")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("actual canceled query/rollback not joined")
	}
	var stillWorking bool
	if err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND (state='active' OR state LIKE 'idle in transaction%'))`, blockedPID).Scan(&stillWorking); err != nil || stillWorking {
		t.Fatal("identity returned while original blocked backend remained active or in transaction", err)
	}
	// The original lock remains held until after this real backend witness.
	if err = lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if timelineIdentity(t, f, "timeline").RecordingID() != "timeline" {
		t.Fatal("canceled read damaged original store")
	}
}
func TestRecordingTimelineIdentityPrivateReceiptFormattingAndEarlyDenial(t *testing.T) {
	v := RecordingTimelineIdentity{recordingID: "PRIVATE_RECORDING", owner: "PRIVATE_OWNER", receipt: json.RawMessage(`"PRIVATE_RECEIPT"`)}
	for _, value := range []any{v, &v} {
		for _, verb := range []string{"%v", "%+v", "%#v"} {
			if strings.Contains(fmt.Sprintf(verb, value), "PRIVATE_") {
				t.Fatal("identity formatting disclosed private bookkeeping")
			}
		}
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != "null" {
			t.Fatal("identity became importable JSON authority")
		}
	}
	var ds *DataStore
	if _, err := ds.ReadRecordingTimelineIdentity(context.Background(), "timeline"); err == nil {
		t.Fatal("nil store produced identity")
	}
	if v.Same(v) {
		t.Fatal("zero incarnation compared as a valid identity")
	}
}
