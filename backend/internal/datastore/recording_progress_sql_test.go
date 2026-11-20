package datastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/settingscommit"
)

func TestRecordingProgressMigrationRoundTripAndConstraints(t *testing.T) {
	f := recordingStatePG(t, 84)
	f.profiles(t)
	f.create(t, "progress")
	var before, after []byte
	if f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(r) FROM recordings r WHERE id='progress'`).Scan(&before) != nil {
		t.Fatal("read original recording")
	}
	f.migrate(t, 85, false)
	if f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(r) FROM recordings r WHERE id='progress'`).Scan(&after) != nil || !bytes.Equal(before, after) {
		t.Fatal("position migration rewrote recording")
	}
	var count int
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_positions`).Scan(&count) != nil || count != 0 {
		t.Fatal("migration invented saved positions")
	}
	for _, pair := range []struct{ name, sql string }{
		{"position", `UPDATE recording_playback_positions SET position_ms=duration_ms+1`},
		{"artifact", `UPDATE recording_playback_positions SET artifact_id='not-an-artifact'`},
		{"end", `UPDATE recording_playback_positions SET at_derived_end=true`},
	} {
		t.Run(pair.name, func(t *testing.T) {
			tx := f.begin(t)
			_, err := tx.Exec(t.Context(), `INSERT INTO recording_playback_positions(recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id) VALUES('progress',gen_random_uuid(),repeat('a',32),repeat('b',64),100,0,false,now(),now()+interval '1 hour',1,1,gen_random_uuid())`)
			if err != nil {
				t.Fatal("insert bounded data")
			}
			if _, err = tx.Exec(t.Context(), pair.sql); err == nil {
				t.Fatal("invalid durable data bypassed schema constraint")
			}
			recordingStateRollback(tx)
		})
	}
	f.migrate(t, 84, true)
	f.migrate(t, 85, false)
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_positions`).Scan(&count) != nil || count != 0 {
		t.Fatal("reapply fabricated or imported position")
	}
}

func (f *recordingProgressFixture) commit(t *testing.T, command RecordingProgressCommand) RecordingProgressResult {
	t.Helper()
	i := f.prepare(t, f.request(t, t.Context()), command)
	defer i.Close()
	value, err := f.core.Commit(i)
	if err != nil {
		t.Fatal("commit actual recording progress", err)
	}
	return value
}

func TestRecordingProgressStartTouchAndFreshResume(t *testing.T) {
	f := progressPG(t, false)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	i := f.prepare(t, f.request(t, ctx), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	start, err := f.core.Commit(i)
	if err != nil || start.PositionMS != 0 || start.WriterGeneration != "1" {
		t.Fatal("start claim failed", err)
	}
	i.Close()
	<-ctx.Done()
	if _, err = f.core.Commit(i); err == nil {
		t.Fatal("old canceled consumed intent revived")
	}
	touched := f.commit(t, progressCommand(RecordingProgressTouch, 1, 1, 35000))
	if touched.PositionMS != 35000 || touched.Revision != "2" || touched.WriterGeneration != "1" {
		t.Fatal("touch did not preserve generation")
	}
	// This is a DIFFERENT actual full session/frame/intent and an unchanged,
	// currently live descriptor. No historical intent or grant is renewed.
	now := time.Now().UTC().Truncate(time.Microsecond)
	f.exec(t, `INSERT INTO sessions(token,account_id,client_id,created_at,expires_at) VALUES('synthetic-progress-new-session','account','progress-client',$1,$2)`, now, now.Add(time.Hour))
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/private-recording-progress", nil).WithContext(t.Context())
	r.Header.Set("X-Client-ID", "progress-client")
	r, release, err := f.ds.AuthenticateProfilePinRequest(r, "synthetic-progress-new-session", "progress-client")
	if err != nil {
		t.Fatal("authenticate new legitimate Resume session", err)
	}
	defer release()
	resumeIntent := f.prepare(t, r, progressCommand(RecordingProgressResume, 2, 1, 0))
	resumed, err := f.core.Commit(resumeIntent)
	resumeIntent.Close()
	if err != nil {
		t.Fatal("new legitimate session could not Resume", err)
	}
	if resumed.PositionMS != 35000 || resumed.WriterGeneration != "2" || resumed.Revision != "3" {
		t.Fatal("new legitimate Resume lost durable position")
	}
	end := f.commit(t, progressCommand(RecordingProgressTouch, 3, 2, 60000))
	if !end.AtDerivedEnd || end.PositionMS != 60000 {
		t.Fatal("exact derived endpoint missing")
	}
	// Historical request expiry did not renew or redate this descriptor. The
	// real original cache TTL is a separate bound, unchanged in the core.
	var observed, expires time.Time
	if f.ds.pool.QueryRow(t.Context(), `SELECT descriptor_observed_at,descriptor_expires_at FROM recording_playback_positions WHERE recording_id='progress'`).Scan(&observed, &expires) != nil || !observed.Equal(f.observation.ObservedAt) || !expires.Equal(f.observation.ExpiresAt) {
		t.Fatal("fresh legitimate request renewed descriptor provenance")
	}
}

func TestRecordingProgressCASHasOneConcurrentWinner(t *testing.T) {
	f := progressPG(t, false)
	r := f.request(t, t.Context())
	one := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	two := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	done := make(chan error, 2)
	for _, i := range []*RecordingProgressIntent{one, two} {
		go func(i *RecordingProgressIntent) { _, err := f.core.Commit(i); done <- err }(i)
	}
	winners := 0
	for range 2 {
		if err := <-done; err == nil {
			winners++
		} else if !errors.Is(err, ErrRecordingProgressChanged) {
			t.Fatal("unexpected CAS failure", err)
		}
	}
	if winners != 1 {
		t.Fatal("same expected revision did not have exactly one winner")
	}
	one.Close()
	two.Close()
	state := f.state(t, "progress")
	if state.revision != 1 || state.generation != 1 {
		t.Fatal("losing intent advanced bookkeeping")
	}
	stale := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressTouch, 0, 0, 1))
	if _, err := f.core.Commit(stale); err == nil {
		t.Fatal("old writer generation overwrote winner")
	}
}

func TestRecordingProgressPayloadFailureRollsBackCAS(t *testing.T) {
	f := progressPG(t, false)
	f.exec(t, `CREATE FUNCTION reject_progress_payload() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'synthetic payload failure';END$$;CREATE TRIGGER reject_progress BEFORE INSERT ON recording_playback_positions FOR EACH ROW EXECUTE FUNCTION reject_progress_payload()`)
	i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	if _, err := f.core.Commit(i); err == nil || errors.Is(err, ErrRecordingProgressUnknown) {
		t.Fatal("failed payload reported commit")
	}
	if state := f.state(t, "progress"); state.revision != 0 || state.generation != 0 || !state.tombstone {
		t.Fatal("payload rollback left claimed CAS")
	}
	var count int
	if f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM recording_playback_positions`).Scan(&count) != nil || count != 0 {
		t.Fatal("failed payload persisted")
	}
}

func TestRecordingProgressSourceIdentityConflicts(t *testing.T) {
	for _, kind := range []string{"artifact", "map", "duration", "owner-ABA", "run-ABA", "expired"} {
		t.Run(kind, func(t *testing.T) {
			f := progressPG(t, false)
			f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0))
			r := f.request(t, t.Context())
			identity, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
			if err != nil {
				t.Fatal(err)
			}
			data := f.observation
			switch kind {
			case "artifact":
				data.ArtifactID = strings.Repeat("c", 32)
			case "map":
				data.MappingSHA256 = strings.Repeat("d", 64)
			case "duration":
				data.DurationMS++
			case "expired":
				data.ObservedAt = time.Now().Add(-25 * time.Hour)
				data.ExpiresAt = time.Now().Add(-time.Hour)
			}
			i, err := f.core.Prepare(r, identity, data, progressCommand(RecordingProgressResume, 1, 1, 0))
			if i != nil {
				defer i.Close()
			}
			if kind == "expired" {
				if err == nil {
					t.Fatal("expired current descriptor accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "owner-ABA" {
				f.exec(t, `UPDATE recordings SET user_id='two' WHERE id='progress';UPDATE recordings SET user_id='one' WHERE id='progress'`)
			}
			if kind == "run-ABA" {
				f.exec(t, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-B"') WHERE id='progress';UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-A"') WHERE id='progress'`)
			}
			if _, err = f.core.Commit(i); err == nil {
				t.Fatal("changed exact tuple auto-rebased Resume")
			}
			var position int64
			if f.ds.pool.QueryRow(t.Context(), `SELECT position_ms FROM recording_playback_positions WHERE recording_id='progress'`).Scan(&position) != nil || position != 0 {
				t.Fatal("conflict deleted/rewrote durable position")
			}
		})
	}
}

func (f *recordingProgressFixture) waiting(t *testing.T, fragment string, want bool) {
	t.Helper()
	until := time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		var pending bool
		err := f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query ILIKE $2)`, f.schema, "%"+fragment+"%").Scan(&pending)
		if err != nil {
			t.Fatal("observe own actual query")
		}
		if pending == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("own actual query wait/cleanup witness missing")
}

func TestRecordingProgressCommitSerializesManagedWrites(t *testing.T) {
	f := progressPG(t, false)
	// A deferred actual payload trigger holds COMMIT on the SAME backend. The
	// observer owns a separate synthetic key; the production serializer key is
	// untouched by this test's control mechanism.
	key := int64(700000000) + int64(time.Now().Nanosecond())
	connection, err := f.admin.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()
	if _, err = connection.Exec(t.Context(), `SELECT pg_advisory_lock($1)`, key); err != nil {
		t.Fatal(err)
	}
	defer connection.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, key)
	f.exec(t, fmt.Sprintf(`CREATE FUNCTION hold_progress_commit() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(%d::bigint);RETURN NEW;END$$;CREATE CONSTRAINT TRIGGER hold_progress_commit AFTER INSERT OR UPDATE ON recording_playback_positions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION hold_progress_commit()`, key))
	i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	done := make(chan error, 1)
	go func() { _, err := f.core.Commit(i); done <- err }()
	completed := false
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = connection.Exec(cleanup, `SELECT pg_advisory_unlock($1)`, key)
		stop()
		i.Close()
		if !completed {
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Error("actual controlled Commit did not join after unlocking own test key")
			}
		}
	}()
	f.waiting(t, "commit", true)
	before, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("same-value Save overtook actual progress Commit", err)
	}
	replacement, err := config.ReserveStoreReplacement(f.manager, f.ds.pool)
	if replacement != nil {
		_ = replacement.Close()
	}
	if !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("real restore reservation overtook actual progress Commit", err)
	}
	after, err := os.ReadFile(f.path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("Busy mutation changed policy")
	}
	if _, err = connection.Exec(t.Context(), `SELECT pg_advisory_unlock($1)`, key); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		completed = true
		if err != nil {
			t.Fatal("bound policy changed during Busy attempts", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("actual Commit did not finish")
	}
	i.Close()
	if err = f.manager.Save(f.settings); err != nil {
		t.Fatal("completed original transaction did not release writes", err)
	}
}

func TestRecordingProgressUnknownACKIsTerminal(t *testing.T) {
	f := progressPG(t, false)
	i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	// Authentication and identity read-only commits completed before arming.
	// This drops a real reply, not a process crash or retained-socket proof.
	f.ackEnabled.Store(true)
	value, err := f.core.Commit(i)
	if !errors.Is(err, ErrRecordingProgressUnknown) || value.RecordingID != "" || !f.ackDropped.Load() {
		t.Fatal("lost actual COMMIT ACK reported success", err)
	}
	i.Close()
	f.ackEnabled.Store(false)
	var revision, generation int64
	if f.admin.QueryRow(t.Context(), `SELECT revision,writer_generation FROM `+pgx.Identifier{f.schema, "recording_playback_positions"}.Sanitize()+` WHERE recording_id='progress'`).Scan(&revision, &generation) != nil || revision != 1 || generation != 1 {
		t.Fatal("unknown ACK did not follow actual durable write")
	}
	if _, err = f.core.Commit(i); err == nil {
		t.Fatal("unknown operation replayed")
	}
}

func TestRecordingProgressBlockedSQLCleanupPrecedesRefund(t *testing.T) {
	f := progressPG(t, false)
	i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	lease, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal("borrow actual original runtime connection")
	}
	physical := lease.Conn().PgConn()
	primary, ok := physical.Conn().(*progressCommitConnection)
	pid := physical.PID()
	lease.Release()
	if !ok || pid == 0 {
		t.Fatal("could not identify exact own primary transport/backend")
	}
	lock, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer recordingStateRollback(lock)
	if _, err = lock.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` WHERE id='account' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := f.core.Commit(i); done <- err }()
	f.waiting(t, "SELECT id FROM accounts", true)
	var ownPrimaryWaiting bool
	if f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND application_name=$2 AND wait_event_type='Lock' AND query ILIKE '%SELECT id FROM accounts%')`, pid, f.schema).Scan(&ownPrimaryWaiting) != nil || !ownPrimaryWaiting {
		t.Fatal("blocked query did not belong to the captured original primary backend")
	}
	// Force an actual failure of that SAME primary socket, not a healthy
	// CancelRequest/ReadyForQuery. Hold its later real Close return and prove
	// PgConn.CleanupDone is still open while original admission remains charged.
	t.Cleanup(func() { f.closeReleaseOnce.Do(func() { close(f.closeRelease) }) })
	primary.holdClose.Store(true)
	if err = primary.Conn.Close(); err != nil {
		t.Fatal("close exact own active primary socket")
	}
	closeDone := make(chan struct{})
	go func() { i.Close(); close(closeDone) }()
	select {
	case <-f.closeEntered:
	case <-time.After(6 * time.Second):
		t.Fatal("actual canceled transport Close was not entered")
	}
	select {
	case <-physical.CleanupDone():
		t.Fatal("physical cleanup completed before held actual Close returned")
	default:
	}
	select {
	case <-closeDone:
		t.Fatal("Close refunded while actual transport cleanup was held")
	default:
	}
	recordingProgressAdmission.Lock()
	held := recordingProgressAdmission.accounts["account"]
	recordingProgressAdmission.Unlock()
	if held != 1 {
		t.Fatal("active cleanup lost original admission charge")
	}
	f.closeReleaseOnce.Do(func() { close(f.closeRelease) })
	select {
	case <-closeDone:
	case <-time.After(6 * time.Second):
		t.Fatal("Close did not join canceled actual query")
	}
	select {
	case <-physical.CleanupDone():
	default:
		t.Fatal("admission returned before actual primary CleanupDone")
	}
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("blocked canceled operation succeeded")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("original operation goroutine did not return after Close joined it")
	}
	// The original row lock is STILL held. The query must actually be gone;
	// unlocking first would allow a spurious late query to hide this defect.
	f.waiting(t, "SELECT id FROM accounts", false)
	recordingProgressAdmission.Lock()
	charged := recordingProgressAdmission.accounts["account"]
	recordingProgressAdmission.Unlock()
	if charged != 0 {
		t.Fatal("completed close retained admission")
	}
	if err = lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0)); got.Revision != "1" {
		t.Fatal("capacity not reusable after actual cleanup")
	}
}

func TestRecordingProgressCounterOverflowDoesNotWrite(t *testing.T) {
	f := progressPG(t, false)
	f.exec(t, `UPDATE recording_playback_state SET revision=$1 WHERE recording_id='progress'`, int64(math.MaxInt64))
	i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, math.MaxInt64, 0, 0))
	if _, err := f.core.Commit(i); err == nil {
		t.Fatal("exhausted counter wrapped")
	}
	state := f.state(t, "progress")
	if strconv.FormatInt(state.revision, 10) != "9223372036854775807" || state.generation != 0 {
		t.Fatal("overflow changed bookkeeping")
	}
}
