package datastore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func creationResultCreate(t *testing.T, f *parentCreationFixture, ds *DataStore) (*ParentProfileCreationIntent, *ParentProfileCreationReceipt) {
	t.Helper()
	i, r := f.prepare(t, ds)
	if err := ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, ds, i, r)
	receipt, err := ds.CommitParentProfileCreation(i)
	if err != nil {
		t.Fatal(err)
	}
	return i, receipt
}
func creationResultRead(t *testing.T, f *parentCreationFixture, ds *DataStore, locator string) (*ProfileCreationRead, *ProfileCreationResultSnapshot, *ProfileRecoverySlot, *ProfileRecoverySlot) {
	t.Helper()
	r, stop := f.request(t, ds, t.Context())
	t.Cleanup(stop)
	r.URL.Path = "/account/api/profiles/creation-operations/" + locator + "/read"
	read, err := ds.PrepareProfileCreationReadManagement(r, locator, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(read.Close)
	u, err := ds.ReserveProfileCreationUsersRecovery(r.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ds.ReserveProfileCreationSettingsRecovery(r.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	result, err := ds.ReadProfileCreationResult(read)
	if err != nil {
		t.Fatal(err)
	}
	return read, result, u, s
}
func creationResultLeases(t *testing.T, ds *DataStore, r *ProfileCreationRead, s *ProfileCreationResultSnapshot, u, v *ProfileRecoverySlot) (*ProfileCreationRecoveryLease, *ProfileCreationRecoveryLease) {
	t.Helper()
	a, _, _, err := ds.BeginProfileCreationUsersRecoveryLease(r.ctx, r, u, s.recovery)
	if err != nil {
		t.Fatal(err)
	}
	b, _, _, err := ds.BeginProfileCreationSettingsRecoveryLease(r.ctx, r, v, s.recovery)
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}
func TestProfileCreationResultsActualCreateAndCurrentProjection(t *testing.T) {
	f := newParentCreationFixture(t)
	i, receipt := creationResultCreate(t, f, f.ds)
	target := receipt.user.User.ID
	var outcome, digests string
	var attempted bool
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT outcome,attempted,initial_user_digest||initial_settings_digest FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&outcome, &attempted, &digests); err != nil || outcome != "created" || !attempted || len(digests) != 128 {
		t.Fatal("actual atomic result missing", err)
	}
	f.exec(t, `UPDATE users SET name='Current saved child',kids_max_movie_rating='PG' WHERE id=$1`, target)
	f.exec(t, `UPDATE user_settings SET settings='{"display":{"simpleMode":false},"future":{"n":9007199254740993}}' WHERE user_id=$1`, target)
	read, result, u, v := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
	if result.Outcome() != "created" || result.recovery.user.User.Name != "Current saved child" || !bytes.Contains(result.recovery.settings.Revision.snapshot.raw, []byte("9007199254740993")) || result.recovery.settings.Settings.Display.SimpleMode == nil || *result.recovery.settings.Settings.Display.SimpleMode {
		t.Fatal("read replayed initial create projection")
	}
	a, b := creationResultLeases(t, f.ds, read, result, u, v)
	if err := f.ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b); err != nil || !a.Paired() || !b.Paired() {
		t.Fatal("qualified pair", err)
	}
	if err := f.ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b); err == nil {
		t.Fatal("pair replay")
	}
}
func TestProfileCreationResultsLocatorEchoAndDistinctAuthority(t *testing.T) {
	for _, kind := range []string{"missing", "wrong", "duplicate", "pcc_as_locator"} {
		t.Run(kind, func(t *testing.T) {
			f := newParentCreationFixture(t)
			r, stop := f.request(t, f.ds, t.Context())
			defer stop()
			i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			locator, err := f.ds.ParentProfileCreationLocator(i)
			if err != nil || !strings.HasPrefix(locator, "pcr_") || len(locator) != 68 {
				t.Fatal("locator format", err)
			}
			switch kind {
			case "wrong":
				r.Header.Set(ProfileCreationOperationHeader, "pcr_"+strings.Repeat("0", 64))
			case "duplicate":
				r.Header.Add(ProfileCreationOperationHeader, locator)
				r.Header.Add(ProfileCreationOperationHeader, locator)
			case "pcc_as_locator":
				r.Header.Set(ProfileCreationOperationHeader, "pcc_"+locator[4:])
			}
			if err = f.ds.BindParentProfileCreationManagementRetry(r, i, []byte(parentCreationTestBody), nil); err == nil {
				t.Fatal("locator echo boundary ignored")
			}
			if u, s := creationTestCounts(t, f, i.state.reference.record.target); u != 0 || s != 0 {
				t.Fatal("echo failure attempted create")
			}
		})
	}
}
func TestProfileCreationResultsActualLostCreateAckStillQualified(t *testing.T) {
	f := newParentCreationFixture(t)
	cfg, err := pgxpool.ParseConfig(f.uri)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2
	cfg.MinConns = 0
	enabled, dropped := &atomic.Bool{}, &atomic.Bool{}
	cfg.ConnConfig.Tracer = &parentCreationInsertTracer{enable: enabled}
	expected := filepath.Join(cfg.ConnConfig.Host, fmt.Sprintf(".s.PGSQL.%d", cfg.ConnConfig.Port))
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "unix" || address != expected {
			return nil, errors.New("only exact synthetic Unix socket allowed")
		}
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &creationOperationLostAckConn{Conn: c, enabled: enabled, dropped: dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	i, r := f.prepare(t, ds)
	if err = ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, ds, i, r)
	if receipt, e := ds.CommitParentProfileCreation(i); receipt != nil || !errors.Is(e, ErrParentProfileCreationUnknown) || !dropped.Load() {
		t.Fatal("did not lose actual Create COMMIT ACK", e)
	}
	_, result, _, _ := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
	if result.Outcome() != "created" || !result.recovery.present {
		t.Fatal("durable actual created result lost with ACK")
	}
	if _, err = ds.CommitParentProfileCreation(i); err == nil {
		t.Fatal("unknown replay")
	}
}
func TestProfileCreationResultsNeverAttemptedReadAndPruneStayFinite(t *testing.T) {
	f := newParentCreationFixture(t)
	for n := 0; n < 10; n++ {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		r, stop := f.request(t, f.ds, ctx)
		i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
		if err != nil {
			stop()
			cancel()
			t.Fatal(err)
		}
		locator := parentProfileCreationLocator(i.state.reference)
		<-i.state.life.Done()
		stop()
		cancel()
		i.Close()
		_, result, _, _ := creationResultRead(t, f, f.ds, locator)
		if result.Outcome() != "not_created" || result.recovery.present {
			t.Fatal("expired never-attempted result")
		}
		if _, err = f.ds.pruneProfileCreationOperations(t.Context(), 64); err != nil {
			t.Fatal(err)
		}
		var count int
		if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_results`).Scan(&count); err != nil || count != 0 {
			t.Fatal("never-attempted read graduated to permanent history", count, err)
		}
		fresh, release := f.request(t, f.ds, t.Context())
		_, err = f.ds.PrepareProfileCreationReadManagement(fresh, locator, nil)
		release()
		if !errors.Is(err, ErrProfileCreationResultMissing) {
			t.Fatal("pruned locator became absence", err)
		}
	}
}
func TestProfileCreationResultsBirthDeletionReinsertMoveAndTruncate(t *testing.T) {
	for _, kind := range []string{"repository_delete", "reinsert", "move", "truncate", "normal_edit"} {
		t.Run(kind, func(t *testing.T) {
			f := newParentCreationFixture(t)
			i, receipt := creationResultCreate(t, f, f.ds)
			target := receipt.user.User.ID
			switch kind {
			case "repository_delete":
				if err := f.ds.Users().Delete(t.Context(), target); err != nil {
					t.Fatal(err)
				}
			case "reinsert":
				if err := f.ds.Users().Delete(t.Context(), target); err != nil {
					t.Fatal(err)
				}
				f.exec(t, `INSERT INTO users(id,name,account_id) VALUES($1,'Replacement','default')`, target)
			case "move":
				f.exec(t, `DELETE FROM user_settings WHERE user_id=$1`, target)
				f.exec(t, `UPDATE users SET id=id||'_moved' WHERE id=$1`, target)
			case "truncate":
				f.exec(t, `TRUNCATE users CASCADE`)
			case "normal_edit":
				f.exec(t, `UPDATE users SET color='updated' WHERE id=$1`, target)
			}
			var invalid bool
			if err := f.ds.pool.QueryRow(t.Context(), `SELECT birth_invalidated FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&invalid); err != nil || invalid != (kind != "normal_edit") {
				t.Fatal("birth stamp", invalid, err)
			}
			_, result, _, _ := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
			if kind == "reinsert" && (result.Outcome() != "changed_or_replaced" || result.recovery != nil) {
				t.Fatal("replacement adopted")
			}
			if kind == "normal_edit" && (result.Outcome() != "created" || result.recovery.user.User.Color != "updated") {
				t.Fatal("normal edit invalidated birth")
			}
		})
	}
}
func TestProfileCreationResultsFinalTakeRejectsAbsentReplacementAndPresentABA(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(fmt.Sprint(absent), func(t *testing.T) {
			f := newParentCreationFixture(t)
			i, receipt := creationResultCreate(t, f, f.ds)
			target := receipt.user.User.ID
			if absent {
				if err := f.ds.Users().Delete(t.Context(), target); err != nil {
					t.Fatal(err)
				}
			}
			read, result, u, v := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
			a, b := creationResultLeases(t, f.ds, read, result, u, v)
			if !absent {
				if err := f.ds.Users().Delete(t.Context(), target); err != nil {
					t.Fatal(err)
				}
			}
			f.exec(t, `INSERT INTO users(id,name,account_id) VALUES($1,'Same ID replacement','default')`, target)
			if err := f.ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b); err == nil || a.Paired() || b.Paired() {
				t.Fatal("old read adopted reincarnation", err)
			}
		})
	}
}
func TestProfileCreationResultsGenericPortsRejectSealedOrigin(t *testing.T) {
	f := newParentCreationFixture(t)
	i, _ := creationResultCreate(t, f, f.ds)
	read, result, u, v := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
	inner := read.recovery
	if _, err := f.ds.ReserveUsersRecovery(read.ctx, inner); err == nil {
		t.Fatal("generic reservation escaped origin")
	}
	if _, err := f.ds.ReserveUserSettingsRecovery(read.ctx, inner); err == nil {
		t.Fatal("generic Settings reservation escaped origin")
	}
	if err := f.ds.ValidateProfileRecoverySlot(read.ctx, u, result.recovery); err == nil {
		t.Fatal("generic slot validation escaped origin")
	}
	if _, err := f.ds.ReadProfileRecovery(inner); err == nil {
		t.Fatal("generic SQL read escaped origin")
	}
	if _, _, _, err := f.ds.BeginUsersRecoveryLease(read.ctx, u, result.recovery); err == nil {
		t.Fatal("generic Users lease escaped origin")
	}
	if _, _, _, err := f.ds.BeginUserSettingsRecoveryLease(read.ctx, v, result.recovery); err == nil {
		t.Fatal("generic Settings lease escaped origin")
	}
	if _, _, err := f.ds.TakeUsersRecovery(read.ctx, u, result.recovery); err == nil {
		t.Fatal("generic Users consume escaped origin")
	}
	if _, _, err := f.ds.TakeUserSettingsRecovery(read.ctx, v, result.recovery); err == nil {
		t.Fatal("generic Settings consume escaped origin")
	}
	a, b := creationResultLeases(t, f.ds, read, result, u, v)
	if err := f.ds.TakeProfileRecoveryPair(read.ctx, a.proof, b.proof, result.recovery); err == nil || a.Paired() || b.Paired() {
		t.Fatal("generic paired consume escaped origin")
	}
	if _, err := f.ds.pool.Exec(t.Context(), `UPDATE accounts SET is_master=NOT is_master WHERE id='default'`); err != nil {
		t.Fatal(err)
	}
	if err := f.ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b); err == nil {
		t.Fatal("typed pair skipped fresh actor change")
	}
}
func TestProfileCreationResultsActualHeldFenceCancellationJoins(t *testing.T) {
	f := newParentCreationFixture(t)
	i, _ := creationResultCreate(t, f, f.ds)
	read, result, u, v := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
	a, b := creationResultLeases(t, f.ds, read, result, u, v)
	held, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer creationTestRollback(held)
	if _, err = held.Exec(t.Context(), `SELECT operation_id FROM profile_creation_operations WHERE operation_id=$1 FOR UPDATE`, read.record.operation); err != nil {
		t.Fatal(err)
	}
	worker := startCreationTestWorker(t, func() { read.cancel(); creationTestRollback(held) }, func() error { return f.ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b) })
	creationOperationWaitLock(t, f.settingsDSFixture, "SELECT operation_id::text FROM profile_creation_operations")
	read.cancel()
	if err = worker.join(t); err == nil || a.Paired() || b.Paired() {
		t.Fatal("canceled fenced read consumed pair", err)
	}
}

func TestProfileCreationResultsQualifiedEnrollmentAndResultFailureRollback(t *testing.T) {
	for _, phase := range []string{"enrollment", "created_result"} {
		t.Run(phase, func(t *testing.T) {
			f := newParentCreationFixture(t)
			f.exec(t, `CREATE FUNCTION synthetic_reject_creation_result() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic result refusal'; END; $$`)
			if phase == "enrollment" {
				f.exec(t, `CREATE TRIGGER synthetic_refuse_result BEFORE INSERT ON profile_creation_results FOR EACH ROW EXECUTE FUNCTION synthetic_reject_creation_result()`)
				r, stop := f.request(t, f.ds, t.Context())
				defer stop()
				if intent, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil); err == nil || intent != nil {
					if intent != nil {
						intent.Close()
					}
					t.Fatal("enrollment failure admitted intent")
				}
				var a, b int
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM profile_creation_operations),(SELECT count(*) FROM profile_creation_results)`).Scan(&a, &b); err != nil || a != 0 || b != 0 {
					t.Fatal("enrollment was not atomic", a, b, err)
				}
			} else {
				i, r := f.prepare(t, f.ds)
				if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
					t.Fatal(err)
				}
				creationTestLeases(t, f.ds, i, r)
				f.exec(t, `CREATE TRIGGER synthetic_refuse_result BEFORE UPDATE ON profile_creation_results FOR EACH ROW WHEN (NEW.outcome='created') EXECUTE FUNCTION synthetic_reject_creation_result()`)
				if receipt, err := f.ds.CommitParentProfileCreation(i); err == nil || receipt != nil {
					t.Fatal("result failure returned committed receipt")
				}
				if a, b := creationTestCounts(t, f, i.state.reference.record.target); a != 0 || b != 0 {
					t.Fatal("result failure did not roll back User+Settings")
				}
				var outcome string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT outcome FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&outcome); err != nil || outcome != "prepared" {
					t.Fatal("failed result became created", err)
				}
			}
		})
	}
}
func TestProfileCreationResultsCreatedSurvivesActualExecutionPrune(t *testing.T) {
	f := newParentCreationFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	r, stop := f.request(t, f.ds, ctx)
	defer stop()
	i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	locator := parentProfileCreationLocator(i.state.reference)
	creationTestEcho(r, i)
	if err = f.ds.BindParentProfileCreationManagementRetry(r, i, []byte(parentCreationTestBody), nil); err != nil {
		t.Fatal(err)
	}
	r, err = f.ds.BoundParentProfileCreationRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", parentCreationTestPassword)
	if err = f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, f.ds, i, r)
	if _, err = f.ds.CommitParentProfileCreation(i); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	count, err := f.ds.pruneProfileCreationOperations(t.Context(), 64)
	if err != nil || count != 1 {
		t.Fatal("actual terminal080 prune failed", count, err)
	}
	_, result, _, _ := creationResultRead(t, f, f.ds, locator)
	if result.Outcome() != "created" || result.recovery == nil || !result.recovery.present {
		t.Fatal("080 prune erased actual created081 proof")
	}
}
func TestProfileCreationResultsNarrowedReadDeadlineReachesFinalPair(t *testing.T) {
	f := newParentCreationFixture(t)
	i, _ := creationResultCreate(t, f, f.ds)
	expiry := time.Now().UTC().Add(200 * time.Millisecond)
	f.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='default'`, expiry)
	read, result, u, v := creationResultRead(t, f, f.ds, parentProfileCreationLocator(i.state.reference))
	a, b := creationResultLeases(t, f.ds, read, result, u, v)
	outer, ok := read.run.Deadline()
	if !ok {
		t.Fatal("read has no actual authority deadline")
	}
	inner, ok := read.recovery.state.run.Deadline()
	if !ok || inner.After(outer) || outer.After(expiry) {
		t.Fatal("inner pair deadline exceeds captured actual authority")
	}
	<-read.run.Done()
	if err := f.ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b); err == nil || a.Paired() || b.Paired() {
		t.Fatal("expired current owner allowed pair", err)
	}
}

func TestProfileCreationResultsPreparedAdmissionSharesRetainedActorAndGlobalBounds(t *testing.T) {
	f := newParentCreationFixture(t)
	for n := 0; n < creationOperationMaximumActorActive; n++ {
		r, stop := f.request(t, f.ds, t.Context())
		i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
		stop()
		if err != nil {
			t.Fatal(err)
		}
		i.Close()
	}
	r, stop := f.request(t, f.ds, t.Context())
	defer stop()
	if i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil); err == nil || i != nil {
		if i != nil {
			i.Close()
		}
		t.Fatal("qualified result admission bypassed actor8")
	}
	var a, b int
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM profile_creation_operations),(SELECT count(*) FROM profile_creation_results WHERE outcome='prepared' AND NOT attempted)`).Scan(&a, &b); err != nil || a != creationOperationMaximumActorActive || b != a {
		t.Fatal("prepared result not charged to retained executable rows", a, b, err)
	}
	// Internal storage admission controls fill the same real global budget using
	// separate synthetic actors, without claiming HTTP credential authorization.
	for n := a; n < creationOperationMaximumRows; n++ {
		metadata := creationOperationTestMetadata(fmt.Sprintf("synthetic_actor_%d", n))
		metadata.OwnerID = "default"
		ref, err := f.ds.reserveProfileCreationOperationQualified(t.Context(), metadata, true)
		if err != nil {
			t.Fatal("fill bounded qualified retained capacity", n, err)
		}
		ref.close()
	}
	metadata := creationOperationTestMetadata("synthetic_over_capacity")
	metadata.OwnerID = "default"
	if ref, err := f.ds.reserveProfileCreationOperationQualified(t.Context(), metadata, true); ref != nil || !errors.Is(err, errCreationOperationCapacity) {
		if ref != nil {
			ref.close()
		}
		t.Fatal("qualified result bypassed retained512", err)
	}
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_results WHERE outcome='prepared'`).Scan(&b); err != nil || b != creationOperationMaximumRows {
		t.Fatal("prepared cardinality escaped512", b, err)
	}
}

type creationResultsTakeBarrier struct {
	enabled atomic.Bool
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}
type creationResultsBarrierKey struct{}

func (b *creationResultsTakeBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, creationResultsBarrierKey{}, b.enabled.Load() && strings.HasPrefix(d.SQL, "SELECT "+creationResultColumns+" FROM profile_creation_results WHERE operation_id=$1 FOR UPDATE"))
}
func (b *creationResultsTakeBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if matched, _ := ctx.Value(creationResultsBarrierKey{}).(bool); matched && d.Err == nil {
		b.once.Do(func() {
			close(b.reached)
			select {
			case <-b.release:
			case <-ctx.Done():
			}
		})
	}
}
func TestProfileCreationResultsActualFinalTakeHoldsUserBirthThroughConsumption(t *testing.T) {
	f := newParentCreationFixture(t)
	i, _ := creationResultCreate(t, f, f.ds)
	barrier := &creationResultsTakeBarrier{reached: make(chan struct{}), release: make(chan struct{})}
	ds := creationOperationPool(t, f.settingsDSFixture, "", barrier)
	read, result, u, v := creationResultRead(t, f, ds, parentProfileCreationLocator(i.state.reference))
	a, b := creationResultLeases(t, ds, read, result, u, v)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
	defer release()
	barrier.enabled.Store(true)
	take := startCreationTestWorker(t, func() { release(); read.cancel() }, func() error { return ds.TakeProfileCreationRecoveryPair(read.ctx, read, result, a, b) })
	select {
	case <-barrier.reached:
	case <-time.After(3 * time.Second):
		t.Fatal("actual final result query barrier not reached")
	}
	target := result.recovery.user.User.ID
	deletion := startCreationTestWorker(t, release, func() error { return f.ds.Users().Delete(t.Context(), target) })
	creationOperationWaitLock(t, f.settingsDSFixture, "DELETE FROM users")
	release()
	if err := take.join(t); err != nil || !a.Paired() || !b.Paired() {
		t.Fatal("stabilized historical pair failed", err)
	}
	if err := deletion.join(t); err != nil {
		t.Fatal("serialized real deletion failed", err)
	}
	var invalid bool
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT birth_invalidated FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&invalid); err != nil || !invalid {
		t.Fatal("later deletion did not invalidate historical birth", err)
	}
}

func TestProfileCreationResultsPruneMismatchedPairRetainsBothRows(t *testing.T) {
	f := newParentCreationFixture(t)
	metadata := creationOperationTestMetadata("default")
	metadata.OwnerID = "default"
	id, _ := creationOperationSeed(t, f.ds.pool, metadata, "retired_unattempted", true)
	record, err := creationOperationRead(t.Context(), f.ds.pool, id)
	if err != nil {
		t.Fatal(err)
	}
	record.referenceHash = strings.Repeat("e", 64)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer creationTestRollback(tx)
	if err = enrollProfileCreationResult(t.Context(), tx, record); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if count, err := f.ds.pruneProfileCreationOperations(t.Context(), 64); err == nil || count != 0 {
		t.Fatal("mismatched081 identity allowed080 prune", count, err)
	}
	var a, b int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM profile_creation_operations),(SELECT count(*) FROM profile_creation_results)`).Scan(&a, &b); err != nil || a != 1 || b != 1 {
		t.Fatal("mismatched pair pruned asymmetrically", a, b, err)
	}
}
func TestProfileCreationResultsAttemptedNotCreatedRetainsAfterExecutablePrune(t *testing.T) {
	f := newParentCreationFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()
	r, stop := f.request(t, f.ds, ctx)
	defer stop()
	i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	locator := parentProfileCreationLocator(i.state.reference)
	creationTestEcho(r, i)
	if err = f.ds.BindParentProfileCreationManagementRetry(r, i, []byte(parentCreationTestBody), nil); err != nil {
		t.Fatal(err)
	}
	r, err = f.ds.BoundParentProfileCreationRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", parentCreationTestPassword)
	if err = f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	// Actual durable claim, deliberately no Create transaction, then old expiry.
	attempt, err := f.ds.claimProfileCreationAttempt(i.state.life, i.state.reference)
	if err != nil {
		t.Fatal(err)
	}
	defer attempt.close()
	<-ctx.Done()
	_, result, _, _ := creationResultRead(t, f, f.ds, locator)
	if result.Outcome() != "not_created" {
		t.Fatal("expired attempted result not settled")
	}
	if count, err := f.ds.pruneProfileCreationOperations(t.Context(), 64); err != nil || count != 1 {
		t.Fatal("closed attempted080 not pruned", count, err)
	}
	var attempted bool
	var outcome string
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT outcome,attempted FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&outcome, &attempted); err != nil || outcome != "not_created" || !attempted {
		t.Fatal("attempted history treated as transient", err)
	}
	_, result, _, _ = creationResultRead(t, f, f.ds, locator)
	if result.Outcome() != "not_created" {
		t.Fatal("pruned executable erased qualified attempted closure")
	}
}
