package datastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/multitracer"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/settingscommit"
)

func snapshotRequest(t *testing.T, f *recordingProgressFixture, ctx context.Context) *http.Request {
	t.Helper()
	r := f.request(t, ctx)
	r.Method = http.MethodGet
	return r
}
func snapshotRead(t *testing.T, f *recordingProgressFixture, r *http.Request) (RecordingProgressSnapshot, error) {
	t.Helper()
	identity, err := f.ds.ReadRecordingTimelineIdentity(r.Context(), "progress")
	if err != nil {
		return RecordingProgressSnapshot{}, err
	}
	return f.core.ReadSnapshot(r, identity, f.observation)
}
func snapshotPrimary(t *testing.T, f *recordingProgressFixture) (*progressCommitConnection, *pgconn.PgConn) {
	t.Helper()
	lease, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	physical := lease.Conn().PgConn()
	primary, ok := physical.Conn().(*progressCommitConnection)
	lease.Release()
	if !ok || physical.PID() == 0 {
		t.Fatal("exact own primary transport/backend missing")
	}
	return primary, physical
}

type snapshotRollbackKey struct{}
type snapshotClockKey struct{}
type snapshotQueryTrace struct {
	enabled                    atomic.Bool
	mu                         sync.Mutex
	active                     bool
	rollbacks, commits, writes atomic.Int32
	validated                  atomic.Bool
	pause                      bool
	entered, release           chan struct{}
	signal, unblock            sync.Once
}

func newSnapshotTrace() *snapshotQueryTrace {
	return &snapshotQueryTrace{entered: make(chan struct{}), release: make(chan struct{})}
}
func (t *snapshotQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !t.enabled.Load() {
		return ctx
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	sql := strings.ToLower(strings.TrimSpace(data.SQL))
	if strings.Contains(sql, "pg_try_advisory_xact_lock_shared") {
		t.active = true
	}
	if !t.active {
		return ctx
	}
	if sql == "select clock_timestamp()" {
		return context.WithValue(ctx, snapshotClockKey{}, true)
	}
	if sql == "commit" {
		t.commits.Add(1)
	}
	if strings.HasPrefix(sql, "update") && strings.Contains(sql, "recording_playback_state") || strings.HasPrefix(sql, "insert into") && strings.Contains(sql, "recording_playback_positions") {
		t.writes.Add(1)
	}
	if sql == "rollback" {
		return context.WithValue(ctx, snapshotRollbackKey{}, true)
	}
	return ctx
}
func (t *snapshotQueryTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if marked, _ := ctx.Value(snapshotClockKey{}).(bool); marked && data.Err == nil {
		t.validated.Store(true)
	}
	if marked, _ := ctx.Value(snapshotRollbackKey{}).(bool); !marked || data.Err != nil {
		return
	}
	t.rollbacks.Add(1)
	t.mu.Lock()
	t.active = false
	t.mu.Unlock()
	t.signal.Do(func() { close(t.entered) })
	if t.pause {
		<-t.release
	}
}

// Reopen a quiescent scope with the SAME actual source URL/root and original
// real PG domain, adding only a passive tracer. No auth frame is copied/minted.
func snapshotTracingPG(t *testing.T, trace *snapshotQueryTrace) *recordingProgressFixture {
	t.Helper()
	return snapshotTracingPGPolicy(t, trace, false)
}
func snapshotTracingPGPolicy(t *testing.T, trace *snapshotQueryTrace, enforce bool) *recordingProgressFixture {
	t.Helper()
	f := progressPG(t, enforce)
	if f.security.Close() != nil || f.coordinator.Close() != nil {
		t.Fatal("quiesce original own test scope")
	}
	cfg := f.ds.pool.Config().Copy()
	cfg.ConnConfig.Tracer = multitracer.New(cfg.ConnConfig.Tracer, trace)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.ds = &DataStore{pool: pool}
	f.coordinator, err = settingscommit.NewForPool(t.Context(), pool, f.objects)
	if err != nil {
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	if err != nil {
		t.Fatal(err)
	}
	f.core, err = NewRecordingProgressCore(f.ds, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		trace.unblock.Do(func() { close(trace.release) })
		f.closeReleaseOnce.Do(func() { close(f.closeRelease) })
	})
	return f
}

func snapshotDatabase(t *testing.T, f *recordingProgressFixture) []byte {
	t.Helper()
	var value []byte
	if f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_object(
 'epochs',COALESCE((SELECT jsonb_agg(to_jsonb(e) ORDER BY scope,scope_key) FROM profile_unlock_revocations e),'[]'::jsonb),
 'state',COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY recording_id) FROM recording_playback_state s),'[]'::jsonb),
 'positions',COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY recording_id) FROM recording_playback_positions p),'[]'::jsonb))`).Scan(&value) != nil {
		t.Fatal("read own actual durable rows")
	}
	return value
}

func TestRecordingProgressSnapshotMissingPayloadUsesActualCounters(t *testing.T) {
	f := progressPG(t, false)
	r := snapshotRequest(t, f, t.Context())
	value, err := snapshotRead(t, f, r)
	if err != nil || value.PositionState != "none" || value.PositionMS != nil || value.Revision != "0" || value.WriterGeneration != "0" {
		t.Fatal("initial real counters/absent position lost", err)
	}
	f.mutate(t, f.state(t, "progress"), recordingPlaybackClaim)
	value, err = snapshotRead(t, f, r)
	if err != nil || value.PositionState != "none" || value.PositionMS != nil || value.Revision != "1" || value.WriterGeneration != "1" {
		t.Fatal("missing payload defaulted actual nonzero counters", err)
	}
}

func TestRecordingProgressSnapshotMatchingAndConflict(t *testing.T) {
	for _, kind := range []string{"matching", "artifact", "map", "duration", "incarnation", "counters", "tombstone"} {
		t.Run(kind, func(t *testing.T) {
			f := progressPG(t, false)
			f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0))
			f.commit(t, progressCommand(RecordingProgressTouch, 1, 1, 25000))
			switch kind {
			case "artifact":
				f.observation.ArtifactID = strings.Repeat("c", 32)
			case "map":
				f.observation.MappingSHA256 = strings.Repeat("d", 64)
			case "duration":
				f.observation.DurationMS++
			case "incarnation":
				f.exec(t, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"snapshot-new-run"') WHERE id='progress'`)
			case "counters":
				f.mutate(t, f.state(t, "progress"), recordingPlaybackClaim)
			case "tombstone":
				f.mutate(t, f.state(t, "progress"), recordingPlaybackDelete)
			}
			value, err := snapshotRead(t, f, snapshotRequest(t, f, t.Context()))
			if err != nil {
				t.Fatal(err)
			}
			if kind == "matching" {
				if value.PositionState != "matching" || value.PositionMS == nil || *value.PositionMS != 25000 || value.AtDerivedEnd == nil || *value.AtDerivedEnd {
					t.Fatal("matching position changed")
				}
			} else if value.PositionState != "conflict" || value.PositionMS != nil {
				t.Fatal("stale data was rebound or defaulted")
			}
			var stored int64
			if f.ds.pool.QueryRow(t.Context(), `SELECT position_ms FROM recording_playback_positions WHERE recording_id='progress'`).Scan(&stored) != nil || stored != 25000 {
				t.Fatal("read rewrote/deleted durable position")
			}
		})
	}
}

func TestRecordingProgressSnapshotPINPolicyMatrix(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		name := "false"
		if enforce {
			name = "true"
		}
		t.Run(name, func(t *testing.T) {
			f := progressPG(t, enforce)
			for _, kind := range []string{"no-pin", "locked", "unlocked", "master", "child", "malformed"} {
				t.Run(kind, func(t *testing.T) {
					f.exec(t, `UPDATE accounts SET is_master=false WHERE id='account';UPDATE users SET account_id='account',pin_hash='',is_kids_profile=false WHERE id='one'`)
					if kind == "locked" || kind == "unlocked" {
						f.pin(t)
					}
					if kind == "master" {
						f.exec(t, `INSERT INTO accounts(id,username,password_hash) VALUES('snapshot-owner','Snapshot Owner','synthetic') ON CONFLICT(id) DO NOTHING;UPDATE accounts SET is_master=true WHERE id='account';UPDATE users SET account_id='snapshot-owner' WHERE id='one'`)
					}
					if kind == "child" {
						f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='one'`)
					}
					if kind == "malformed" {
						f.exec(t, `UPDATE users SET pin_hash='malformed' WHERE id='one'`)
					}
					r := snapshotRequest(t, f, t.Context())
					if kind == "unlocked" {
						token, _ := f.issue(t, r, time.Minute)
						r.Header.Set("X-Profile-Unlock", token)
					}
					value, err := snapshotRead(t, f, r)
					deny := kind == "child" || kind == "malformed" || enforce && kind == "locked"
					if deny {
						if err == nil || value.RecordingID != "" {
							t.Fatal("denied original read published snapshot")
						}
					} else if err != nil || value.PositionState != "none" {
						t.Fatal("existing DVR allow case denied", err)
					}
				})
			}
		})
	}
}

func TestRecordingProgressSnapshotReaderCannotCommit(t *testing.T) {
	f := progressPG(t, false)
	r := snapshotRequest(t, f, t.Context())
	identity, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	i, err := f.core.prepareOriginal(r, identity, f.observation, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if _, err = f.core.Commit(i); err == nil {
		t.Fatal("private reader intent entered writer")
	}
	i.Close()
	post := r.Clone(r.Context())
	post.Method = http.MethodPost
	if _, err = f.core.ReadSnapshot(post, identity, f.observation); err == nil {
		t.Fatal("reader accepted POST without fixed GET mode")
	}
	plain := httptest.NewRequest(http.MethodGet, "http://synthetic.invalid/snapshot", nil)
	if _, err = f.core.ReadSnapshot(plain, identity, f.observation); err == nil {
		t.Fatal("unminted original frame accepted")
	}
	copyCore := *f.core
	if _, err = copyCore.ReadSnapshot(r, identity, f.observation); err == nil {
		t.Fatal("copied core read data")
	}
	f.exec(t, `DELETE FROM recording_playback_state WHERE recording_id='progress'`)
	if value, err := f.core.ReadSnapshot(r, identity, f.observation); err == nil || value.RecordingID != "" {
		t.Fatal("missing084 invented counters")
	}
}

func TestRecordingProgressSnapshotChangedOriginalDuringRowWait(t *testing.T) {
	f := progressPG(t, false)
	r := snapshotRequest(t, f, t.Context())
	lock, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer recordingStateRollback(lock)
	if _, err = lock.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` WHERE id='account' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := snapshotRead(t, f, r); done <- err }()
	f.waiting(t, "SELECT id FROM accounts", true)
	if _, err = lock.Exec(t.Context(), `UPDATE `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` SET username='Changed original' WHERE id='account'`); err != nil {
		t.Fatal(err)
	}
	if err = lock.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("row-wait adopted new original actor")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("actual changed read did not join")
	}
}

func TestRecordingProgressSnapshotRollbackLeavesDatabaseUnchanged(t *testing.T) {
	for _, kind := range []string{"empty", "saved"} {
		t.Run(kind, func(t *testing.T) {
			trace := newSnapshotTrace()
			f := snapshotTracingPG(t, trace)
			if kind == "saved" {
				f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0))
				f.commit(t, progressCommand(RecordingProgressTouch, 1, 1, 12345))
			}
			r := snapshotRequest(t, f, t.Context())
			before := snapshotDatabase(t, f)
			trace.enabled.Store(true)
			value, err := snapshotRead(t, f, r)
			if err != nil {
				t.Fatal("known rollback did not return snapshot", err)
			}
			if kind == "empty" && value.PositionState != "none" || kind == "saved" && value.PositionState != "matching" {
				t.Fatal("wrong position classification")
			}
			if trace.rollbacks.Load() != 1 || trace.commits.Load() != 0 || trace.writes.Load() != 0 {
				t.Fatal("snapshot dispatched commit/CAS/payload mutation or lacked real rollback ACK")
			}
			if after := snapshotDatabase(t, f); !bytes.Equal(before, after) {
				t.Fatal("snapshot persisted epoch/state/payload changes")
			}
		})
	}
}

type snapshotDropRollback struct {
	net.Conn
	armed   atomic.Bool
	dropped atomic.Bool
}

func (c *snapshotDropRollback) Write(p []byte) (int, error) {
	for rest := p; len(rest) >= 5; {
		n := int(binary.BigEndian.Uint32(rest[1:5]))
		if n < 4 || len(rest) < n+1 {
			break
		}
		if rest[0] == 'Q' && strings.EqualFold(strings.TrimRight(string(rest[5:n+1]), "\x00"), "rollback") {
			c.armed.Store(true)
		}
		rest = rest[n+1:]
	}
	return c.Conn.Write(p)
}
func (c *snapshotDropRollback) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.armed.Load() && n > 0 {
		c.dropped.Store(true)
		return 0, io.EOF
	}
	return n, err
}

func TestRecordingProgressSnapshotKnownRollbackRequired(t *testing.T) {
	f := progressPG(t, false)
	r := snapshotRequest(t, f, t.Context())
	before := snapshotDatabase(t, f)
	primary, _ := snapshotPrimary(t, f)
	fault := &snapshotDropRollback{Conn: primary.Conn}
	primary.parentDropCommit.Conn = fault
	value, err := snapshotRead(t, f, r)
	if err == nil || value.RecordingID != "" || !fault.dropped.Load() {
		t.Fatal("unknown rollback ACK released snapshot")
	}
	if after := snapshotDatabase(t, f); !bytes.Equal(before, after) {
		t.Fatal("lost rollback ACK persisted snapshot mutation")
	}
}

func TestRecordingProgressSnapshotActualCleanupLifetime(t *testing.T) {
	for _, kind := range []string{"live", "expired"} {
		t.Run(kind, func(t *testing.T) {
			trace := newSnapshotTrace()
			trace.pause = true
			f := snapshotTracingPG(t, trace)
			ctx := t.Context()
			cancel := func() {}
			if kind == "expired" {
				ctx, cancel = context.WithTimeout(ctx, 1500*time.Millisecond)
			}
			defer cancel()
			r := snapshotRequest(t, f, ctx)
			primary, physical := snapshotPrimary(t, f)
			trace.enabled.Store(true)
			type result struct {
				value RecordingProgressSnapshot
				err   error
			}
			done := make(chan result, 1)
			go func() { v, e := snapshotRead(t, f, r); done <- result{v, e} }()
			select {
			case <-trace.entered:
			case <-time.After(6 * time.Second):
				t.Fatal("actual successful rollback ACK was not observed")
			}
			if ctx.Err() != nil {
				t.Fatal("original request expired before cleanup witness")
			}
			primary.holdClose.Store(true)
			closed := make(chan error, 1)
			go func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				closed <- physical.Close(cleanup)
			}()
			select {
			case <-f.closeEntered:
			case <-time.After(6 * time.Second):
				t.Fatal("actual idle primary Close did not enter after rollback ACK")
			}
			select {
			case <-physical.CleanupDone():
				t.Fatal("actual cleanup ended despite held Close")
			default:
			}
			trace.unblock.Do(func() { close(trace.release) })
			if kind == "expired" {
				<-ctx.Done()
			}
			select {
			case <-done:
				t.Fatal("snapshot/refund preceded actual physical cleanup")
			default:
			}
			recordingProgressAdmission.Lock()
			charged := recordingProgressAdmission.accounts["account"]
			recordingProgressAdmission.Unlock()
			if charged != 1 {
				t.Fatal("held cleanup lost admission")
			}
			f.closeReleaseOnce.Do(func() { close(f.closeRelease) })
			select {
			case <-closed:
			case <-time.After(6 * time.Second):
				t.Fatal("actual physical Close did not join")
			}
			select {
			case got := <-done:
				if kind == "expired" {
					if got.err == nil || got.value.RecordingID != "" {
						t.Fatal("post-cleanup original expiry published snapshot")
					}
				} else if got.err != nil || got.value.PositionState != "none" {
					t.Fatal("intentional private-context cancellation denied live original", got.err)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("snapshot did not join physical cleanup")
			}
		})
	}
}

func TestRecordingProgressSnapshotAdmissionRefundAfterCancellation(t *testing.T) {
	f := progressPG(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := snapshotRequest(t, f, ctx)
	lock, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer recordingStateRollback(lock)
	if _, err = lock.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` WHERE id='account' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := snapshotRead(t, f, r); done <- e }()
	f.waiting(t, "SELECT id FROM accounts", true)
	cancel()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled original read succeeded")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("canceled actual query did not join")
	}
	f.waiting(t, "SELECT id FROM accounts", false)
	recordingProgressAdmission.Lock()
	charged := recordingProgressAdmission.accounts["account"]
	recordingProgressAdmission.Unlock()
	if charged != 0 {
		t.Fatal("actual-ended read retained admission")
	}
	if err = lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = snapshotRead(t, f, snapshotRequest(t, f, t.Context())); err != nil {
		t.Fatal("capacity not reusable after actual cleanup", err)
	}
}

func TestRecordingProgressSnapshotReadThenWriterConflict(t *testing.T) {
	f := progressPG(t, false)
	value, err := snapshotRead(t, f, snapshotRequest(t, f, t.Context()))
	if err != nil {
		t.Fatal(err)
	}
	if value.Revision != "0" || value.WriterGeneration != "0" {
		t.Fatal("expected actual initial counters")
	}
	first := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	if _, err = f.core.Commit(first); err != nil {
		t.Fatal(err)
	}
	first.Close()
	stale := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
	if _, err = f.core.Commit(stale); !errors.Is(err, ErrRecordingProgressChanged) {
		t.Fatal("old observed counters were silently adopted/retried", err)
	}
	stale.Close()
	latest, err := snapshotRead(t, f, snapshotRequest(t, f, t.Context()))
	if err != nil || latest.Revision != "1" || latest.WriterGeneration != "1" || latest.PositionState != "matching" {
		t.Fatal("fresh explicit read did not observe winner", err)
	}
}

func TestRecordingProgressSnapshotGrantDeadlineAfterRollback(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		name := "false"
		if enforce {
			name = "true"
		}
		t.Run(name, func(t *testing.T) {
			trace := newSnapshotTrace()
			trace.pause = true
			f := snapshotTracingPGPolicy(t, trace, enforce)
			f.pin(t)
			r := snapshotRequest(t, f, t.Context())
			token, expiry := f.issue(t, r, 3*time.Second)
			r.Header.Set("X-Profile-Unlock", token)
			frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
			if frame == nil {
				t.Fatal("original actual PG frame missing")
			}
			hashes, err := parentModeHashes([]string{token})
			if err != nil {
				t.Fatal(err)
			}
			original, outcome, deadline, err := readProfilePinAccess(r.Context(), f.ds.pool, frame.session, "one", hashes, true)
			if err != nil || outcome != ProfilePinUnlocked || !deadline.Equal(expiry) || !expiry.After(time.Now()) || !sameParentMutationRow(original.session, frame.sessionRow) || !sameParentMutationRow(original.actor, frame.actorRow) {
				t.Fatal("actual original grant is not still Unlocked at original expiry")
			}
			trace.enabled.Store(true)
			type result struct {
				value RecordingProgressSnapshot
				err   error
			}
			done := make(chan result, 1)
			go func() { value, err := snapshotRead(t, f, r); done <- result{value, err} }()
			select {
			case <-trace.entered:
			case <-time.After(6 * time.Second):
				t.Fatal("successful original rollback ACK not held")
			}
			if !trace.validated.Load() || !expiry.After(time.Now()) {
				t.Fatal("read did not reach final validation and known rollback while original grant was live")
			}
			timer := time.NewTimer(time.Until(expiry) + 20*time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-t.Context().Done():
				t.Fatal("fixture deadline")
			}
			if r.Context().Err() != nil || frame.ctx.Err() != nil || !f.observation.ExpiresAt.After(time.Now()) {
				t.Fatal("a non-grant original lifetime expired")
			}
			select {
			case <-done:
				t.Fatal("snapshot preceded held actual rollback completion")
			default:
			}
			trace.unblock.Do(func() { close(trace.release) })
			select {
			case got := <-done:
				if enforce {
					if got.err == nil || got.value.RecordingID != "" {
						t.Fatal("expired required grant published after cleanup")
					}
				} else if got.err != nil || got.value.PositionState != "none" {
					t.Fatal("false policy adopted grant-only expiry as base lifetime", got.err)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("actual rollback/cleanup did not join")
			}
		})
	}
}
