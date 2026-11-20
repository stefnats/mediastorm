package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"novastream/models"
)

func recordingStateRollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func (f *recordingStatePGFixture) begin(t *testing.T) pgx.Tx {
	t.Helper()
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal("begin own fixture transaction", err)
	}
	t.Cleanup(func() { recordingStateRollback(tx) })
	return tx
}

func (f *recordingStatePGFixture) state(t *testing.T, id string) *recordingPlaybackState {
	t.Helper()
	tx := f.begin(t)
	state, err := lockRecordingPlaybackState(t.Context(), tx, id)
	if err != nil {
		t.Fatal("lock actual recording bookkeeping", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal("finish own read transaction", err)
	}
	return state
}

func (f *recordingStatePGFixture) mutate(t *testing.T, old *recordingPlaybackState, action recordingPlaybackMutation) *recordingPlaybackState {
	t.Helper()
	tx := f.begin(t)
	state, err := compareAndSwapRecordingPlaybackState(t.Context(), tx, old, action)
	if err != nil {
		t.Fatal("private CAS failed", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal("finish own CAS transaction", err)
	}
	return state
}

func (f *recordingStatePGFixture) rejectCAS(t *testing.T, expected *recordingPlaybackState, action recordingPlaybackMutation, want error) {
	t.Helper()
	tx := f.begin(t)
	state, err := compareAndSwapRecordingPlaybackState(t.Context(), tx, expected, action)
	if state != nil || !errors.Is(err, want) {
		t.Fatalf("private CAS did not deny with expected class: %v", err)
	}
	recordingStateRollback(tx)
}

func TestRecordingPlaybackStateGenericWritersAndScheduledConflictRetainIdentity(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	r := f.create(t, "ordinary")
	old := f.mutate(t, f.state(t, r.ID), recordingPlaybackClaim)
	r.Title = "Updated synthetic title"
	r.Status = models.RecordingStatusFailed
	r.Capture.Incomplete = true
	r.Capture.Parts[0].Bytes++
	if err := f.ds.Recordings().Update(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if current := f.state(t, r.ID); !sameRecordingPlaybackState(current, old) {
		t.Fatal("ordinary title/status/ledger update rotated identity or reset counters")
	}
	rule := &models.RecordingRule{ID: "rule", UserID: "one", MatchType: models.RecordingRuleMatchTitle, Pattern: "Synthetic", Enabled: true, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt}
	repo := &pgRecordingRepo{pool: f.ds.pool}
	if err := repo.CreateRule(t.Context(), rule); err != nil {
		t.Fatal(err)
	}
	scheduled := *r
	scheduled.ID, scheduled.RuleID, scheduled.ScheduleKey = "scheduled", rule.ID, "slot"
	scheduled.Status = models.RecordingStatusPending
	if created, err := repo.CreateScheduledRecordingIfAbsent(t.Context(), &scheduled); err != nil || !created {
		t.Fatal("ordinary scheduled INSERT failed", err)
	}
	first := f.state(t, scheduled.ID)
	scheduled.ID, scheduled.Title = "ignored-conflict-id", "Refreshed"
	if updated, err := repo.CreateScheduledRecordingIfAbsent(t.Context(), &scheduled); err != nil || !updated {
		t.Fatal("ordinary scheduled metadata conflict failed", err)
	}
	if current := f.state(t, first.recordingID); !sameRecordingPlaybackState(current, first) {
		t.Fatal("scheduled conflict replaced original recording identity")
	}
	var count int
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_state WHERE recording_id='ignored-conflict-id'`).Scan(&count) != nil || count != 0 {
		t.Fatal("scheduled conflict installed the excluded candidate identity")
	}
}

func TestRecordingPlaybackStateIdentityChangesObserveABA(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	r := f.create(t, "identity")
	old := f.mutate(t, f.state(t, r.ID), recordingPlaybackClaim)
	seen := map[string]bool{old.incarnation: true}
	for _, query := range []string{
		`UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-B"') WHERE id='identity'`,
		`UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-A"') WHERE id='identity'`,
		`UPDATE recordings SET user_id='two' WHERE id='identity'`,
		`UPDATE recordings SET user_id='one' WHERE id='identity'`,
		`UPDATE recordings SET created_at=created_at+interval '1 second' WHERE id='identity'`,
		`UPDATE recordings SET created_at=created_at-interval '1 second' WHERE id='identity'`,
	} {
		f.exec(t, query)
		state := f.state(t, r.ID)
		if seen[state.incarnation] || state.revision != 0 || state.generation != 0 || !state.tombstone || state.operationID != nil {
			t.Fatal("replacement/ABA revived prior state or writer")
		}
		seen[state.incarnation] = true
	}
	f.rejectCAS(t, old, recordingPlaybackTouch, errRecordingPlaybackChanged)
	// The original startup repair intentionally removes private run authority.
	r.Status = models.RecordingStatusRunning
	if err := f.ds.Recordings().Update(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	before := f.state(t, r.ID)
	if count, err := f.ds.Recordings().MarkStaleActiveAsFailed(t.Context(), r.EndAt.Add(time.Hour)); err != nil || count != 1 {
		t.Fatal("original crash repair failed", err)
	}
	if after := f.state(t, r.ID); after.incarnation == before.incarnation || string(after.runID) != `""` {
		t.Fatal("startup run removal did not retire old writer")
	}
}

func TestRecordingPlaybackStateClaimTouchDeleteAreMonotone(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	r := f.create(t, "monotone")
	state := f.state(t, r.ID)
	f.rejectCAS(t, state, recordingPlaybackTouch, errRecordingPlaybackChanged)
	incarnation := state.incarnation
	var previousOperation string
	for _, step := range []struct {
		action          recordingPlaybackMutation
		rev, generation int64
		deleted         bool
	}{
		{recordingPlaybackClaim, 1, 1, false},
		{recordingPlaybackTouch, 2, 1, false},
		{recordingPlaybackDelete, 3, 2, true},
		{recordingPlaybackDelete, 4, 3, true},
		{recordingPlaybackClaim, 5, 4, false},
	} {
		old := state
		state = f.mutate(t, old, step.action)
		if state.incarnation != incarnation || state.revision != step.rev || state.generation != step.generation || state.tombstone != step.deleted || state.operationID == nil || *state.operationID == previousOperation || state.updatedAt.Before(old.updatedAt) {
			t.Fatal("CAS/delete reset identity/counters or reused last-operation ID")
		}
		previousOperation = *state.operationID
		f.rejectCAS(t, old, step.action, errRecordingPlaybackChanged)
	}
	var rows int
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_state WHERE recording_id=$1`, r.ID).Scan(&rows) != nil || rows != 1 {
		t.Fatal("progress deletion removed persistent tombstone row")
	}
}

func TestRecordingPlaybackStateForeignAndInvalidCASDoesNotAdopt(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	r := f.create(t, "binding")
	old := f.mutate(t, f.state(t, r.ID), recordingPlaybackClaim)
	for _, change := range []func(*recordingPlaybackState){
		func(v *recordingPlaybackState) { v.ownerID = "two" },
		func(v *recordingPlaybackState) { v.recordingID = "absent" },
		func(v *recordingPlaybackState) { v.incarnation = "00000000-0000-0000-0000-000000000000" },
		func(v *recordingPlaybackState) { v.revision++ },
		func(v *recordingPlaybackState) { v.generation++ },
		func(v *recordingPlaybackState) { v.revision = -1 },
		func(v *recordingPlaybackState) { v.receipt = json.RawMessage(`[]`) },
	} {
		copy := *old
		change(&copy)
		f.rejectCAS(t, &copy, recordingPlaybackTouch, errRecordingPlaybackChanged)
	}
	for _, invalid := range []recordingPlaybackMutation{0, 4, 255} {
		f.rejectCAS(t, old, invalid, errRecordingPlaybackChanged)
	}
	if !sameRecordingPlaybackState(old, f.state(t, r.ID)) {
		t.Fatal("rejected caller metadata mutated state")
	}
}

func (f *recordingStatePGFixture) waitBlocked(t *testing.T, waiter, holder uint32) {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 3*time.Second)
	defer stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		var waiting bool
		if f.admin.QueryRow(ctx, `SELECT $2::integer=ANY(pg_catalog.pg_blocking_pids($1::integer))`, waiter, holder).Scan(&waiting) != nil {
			t.Fatal("observe exact own pending transaction")
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("exact concurrent CAS never reached held recording lock")
		case <-tick.C:
		}
	}
}

func TestRecordingPlaybackStateConcurrentCASHasOneWinner(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "concurrent")
	expected := f.state(t, "concurrent")
	first, second := f.begin(t), f.begin(t)
	if _, err := compareAndSwapRecordingPlaybackState(t.Context(), first, expected, recordingPlaybackClaim); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	type outcome struct {
		state *recordingPlaybackState
		err   error
	}
	result := make(chan outcome, 1)
	go func() {
		state, err := compareAndSwapRecordingPlaybackState(ctx, second, expected, recordingPlaybackClaim)
		result <- outcome{state, err}
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-result
		}
	}()
	f.waitBlocked(t, second.Conn().PgConn().PID(), first.Conn().PgConn().PID())
	select {
	case <-result:
		joined = true
		t.Fatal("concurrent CAS skipped actual lock")
	default:
	}
	if err := first.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := <-result
	joined = true
	if got.state != nil || !errors.Is(got.err, errRecordingPlaybackChanged) {
		t.Fatal("second writer adopted newer revision instead of CAS conflict", got.err)
	}
	recordingStateRollback(second)
	if state := f.state(t, "concurrent"); state.revision != 1 || state.generation != 1 {
		t.Fatal("more than one concurrent CAS was committed")
	}
}

func TestRecordingPlaybackStateRollbackAndBlockedCancellationDoNotWrite(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "rollback")
	expected := f.state(t, "rollback")
	first := f.begin(t)
	if _, err := compareAndSwapRecordingPlaybackState(t.Context(), first, expected, recordingPlaybackClaim); err != nil {
		t.Fatal(err)
	}
	recordingStateRollback(first)
	if !sameRecordingPlaybackState(expected, f.state(t, "rollback")) {
		t.Fatal("rolled-back CAS became durable")
	}
	holder, waiter := f.begin(t), f.begin(t)
	if _, err := lockRecordingPlaybackState(t.Context(), holder, "rollback"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := compareAndSwapRecordingPlaybackState(ctx, waiter, expected, recordingPlaybackClaim)
		result <- err
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-result
		}
	}()
	f.waitBlocked(t, waiter.Conn().PgConn().PID(), holder.Conn().PgConn().PID())
	cancel()
	if err := <-result; err == nil {
		joined = true
		t.Fatal("blocked canceled write succeeded")
	}
	joined = true
	recordingStateRollback(waiter)
	recordingStateRollback(holder)
	if !sameRecordingPlaybackState(expected, f.state(t, "rollback")) {
		t.Fatal("cancelled pre-COMMIT operation changed durable state")
	}
}

func TestRecordingPlaybackStateCounterExhaustionNeverResets(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "overflow")
	old := f.mutate(t, f.state(t, "overflow"), recordingPlaybackClaim)
	f.exec(t, `UPDATE recording_playback_state SET revision=$1,writer_generation=9 WHERE recording_id='overflow'`, int64(math.MaxInt64))
	maxRevision := f.state(t, "overflow")
	for _, action := range []recordingPlaybackMutation{recordingPlaybackClaim, recordingPlaybackTouch, recordingPlaybackDelete} {
		f.rejectCAS(t, maxRevision, action, errRecordingPlaybackOverflow)
	}
	f.exec(t, `UPDATE recording_playback_state SET revision=9,writer_generation=$1 WHERE recording_id='overflow'`, int64(math.MaxInt64))
	maxGeneration := f.state(t, "overflow")
	f.rejectCAS(t, maxGeneration, recordingPlaybackClaim, errRecordingPlaybackOverflow)
	f.rejectCAS(t, maxGeneration, recordingPlaybackDelete, errRecordingPlaybackOverflow)
	current := f.mutate(t, maxGeneration, recordingPlaybackTouch)
	if current.generation != math.MaxInt64 || current.revision != 10 || current.incarnation != old.incarnation {
		t.Fatal("counter boundary wrapped/reset the same incarnation")
	}
}

func TestRecordingPlaybackStateMissingOrDisabledObserverFailsClosed(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "observer")
	old := f.state(t, "observer")
	f.exec(t, `ALTER TABLE recordings DISABLE TRIGGER recording_playback_identity`)
	tx := f.begin(t)
	if state, err := lockRecordingPlaybackState(t.Context(), tx, "observer"); state != nil || !errors.Is(err, errRecordingPlaybackUnavailable) {
		t.Fatal("disabled observer was accepted")
	}
	recordingStateRollback(tx)
	f.exec(t, `ALTER TABLE recordings ENABLE TRIGGER recording_playback_identity`)
	f.rejectCAS(t, old, recordingPlaybackClaim, errRecordingPlaybackChanged)
	f.exec(t, `UPDATE recordings SET title=title WHERE id='observer'`)
	current := f.state(t, "observer")
	if current.incarnation == old.incarnation {
		t.Fatal("reenabled observer revived old stored receipt")
	}
	f.exec(t, `DELETE FROM recording_playback_state WHERE recording_id='observer'`)
	tx = f.begin(t)
	if state, err := lockRecordingPlaybackState(t.Context(), tx, "observer"); state != nil || !errors.Is(err, errRecordingPlaybackUnavailable) {
		t.Fatal("missing state was implicitly adopted/upserted")
	}
	recordingStateRollback(tx)
	var count int
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_state`).Scan(&count) != nil || count != 0 {
		t.Fatal("read repaired missing state")
	}
	// Only an actual recording observer write establishes a new incarnation.
	f.exec(t, `UPDATE recordings SET title=title WHERE id='observer'`)
	if f.state(t, "observer").incarnation == current.incarnation {
		t.Fatal("missing state repair revived deleted incarnation")
	}
}

func TestRecordingPlaybackStateReplacedFunctionCannotReviveReceipt(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "function")
	old := f.state(t, "function")
	body, err := recordingPlaybackFunctionSource("$recording_identity$")
	if err != nil {
		t.Fatal(err)
	}
	function := pgx.Identifier{f.schema, "observe_recording_playback_identity"}.Sanitize()
	f.exec(t, "CREATE OR REPLACE FUNCTION "+function+"() RETURNS TRIGGER LANGUAGE plpgsql AS $same$"+body+"$same$")
	f.rejectCAS(t, old, recordingPlaybackClaim, errRecordingPlaybackChanged)
	f.exec(t, `UPDATE recordings SET title=title WHERE id='function'`)
	if f.state(t, "function").incarnation == old.incarnation {
		t.Fatal("identical function replacement retained original receipt/identity")
	}
	f.exec(t, "CREATE OR REPLACE FUNCTION "+function+"() RETURNS TRIGGER LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END;'")
	tx := f.begin(t)
	if state, err := lockRecordingPlaybackState(t.Context(), tx, "function"); state != nil || !errors.Is(err, errRecordingPlaybackUnavailable) {
		t.Fatal("unreviewed replacement function was accepted")
	}
	recordingStateRollback(tx)
}

func TestRecordingPlaybackStateUsesQualifiedStateDespiteTempShadow(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "qualified")
	original := f.state(t, "qualified")
	tx := f.begin(t)
	if _, err := tx.Exec(t.Context(), `CREATE TEMP TABLE recording_playback_state(secret text); INSERT INTO recording_playback_state VALUES('untrusted shadow')`); err != nil {
		t.Fatal(err)
	}
	state, err := lockRecordingPlaybackState(t.Context(), tx, "qualified")
	if err != nil || !sameRecordingPlaybackState(original, state) {
		t.Fatal("unqualified shadow displaced original state", err)
	}
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRecordingPlaybackStateActualDeleteRecreateRetiresOldCAS(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	r := f.create(t, "recreated")
	old := f.mutate(t, f.state(t, r.ID), recordingPlaybackClaim)
	if err := f.ds.Recordings().Delete(t.Context(), r.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_state WHERE recording_id=$1`, r.ID).Scan(&count) != nil || count != 0 {
		t.Fatal("actual recording deletion retained old state")
	}
	if err := f.ds.Recordings().Create(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	current := f.state(t, r.ID)
	if current.incarnation == old.incarnation || current.revision != 0 || current.generation != 0 || !current.tombstone {
		t.Fatal("same-ID recreation reused old writer identity")
	}
	f.rejectCAS(t, old, recordingPlaybackTouch, errRecordingPlaybackChanged)
}

func TestRecordingPlaybackStateEmbeddedFunctionDefinitionsAreExact(t *testing.T) {
	for _, delimiter := range []string{"$recording_receipt$", "$recording_identity$"} {
		body, err := recordingPlaybackFunctionSource(delimiter)
		if err != nil || !strings.Contains(body, "BEGIN") || !strings.Contains(body, "END;") {
			t.Fatal("reviewed migration function source unavailable")
		}
	}
	if _, err := recordingPlaybackFunctionSource("$unreviewed$"); !errors.Is(err, errRecordingPlaybackUnavailable) {
		t.Fatal("unknown expected function source accepted")
	}
}

func TestRecordingPlaybackStateLateObserverReplacementDuringActualRowWaitDenies(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "late-observer")
	holder, waiter := f.begin(t), f.begin(t)
	if _, err := lockRecordingPlaybackState(t.Context(), holder, "late-observer"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := lockRecordingPlaybackState(ctx, waiter, "late-observer"); result <- err }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-result
		}
	}()
	f.waitBlocked(t, waiter.Conn().PgConn().PID(), holder.Conn().PgConn().PID())
	// Function replacement does not need the recording table's DML lock. The
	// blocked reader has already captured the original catalog receipt.
	body, err := recordingPlaybackFunctionSource("$recording_identity$")
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, "CREATE OR REPLACE FUNCTION "+pgx.Identifier{f.schema, "observe_recording_playback_identity"}.Sanitize()+"() RETURNS TRIGGER LANGUAGE plpgsql AS $late$"+body+"$late$")
	select {
	case <-result:
		joined = true
		t.Fatal("reader skipped actual held lock")
	default:
	}
	if err := holder.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, errRecordingPlaybackUnavailable) {
		joined = true
		t.Fatal("late catalog replacement was adopted", err)
	}
	joined = true
	recordingStateRollback(waiter)
}
