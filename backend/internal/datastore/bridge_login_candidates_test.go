//go:build linux

package datastore

import (
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/settingscommit"
)

func bridgeCandidateResolve(t *testing.T, f *bridgeSessionFixture, name string) ([]BridgeLoginCandidate, error) {
	t.Helper()
	r, _ := bridgeWorkflowRequest(t)
	policy := bridgeWorkflowObservation(t, f, r)
	return f.issuer.ResolvePasswordCandidates(r, name, "synthetic-device", policy)
}
func bridgeCandidateWant(t *testing.T, f *bridgeSessionFixture, name string, want []BridgeLoginCandidate) {
	t.Helper()
	before := bridgeWorkflowRows(t, f)
	got, err := bridgeCandidateResolve(t, f, name)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("actual PG selector mismatch for %q: got%v want%v err%v", name, got, want, err)
	}
	if !reflect.DeepEqual(before, bridgeWorkflowRows(t, f)) {
		t.Fatal("candidate DATA read mutated storage")
	}
}
func bridgeCandidateInsert(t *testing.T, f *bridgeSessionFixture, id, owner, name string) {
	t.Helper()
	f.exec(t, `INSERT INTO users(id,account_id,name)VALUES($1,$2,$3)`, id, owner, name)
}

func TestBridgeLoginCandidatesActualGoAliasParity(t *testing.T) {
	for _, kind := range []string{"bare", "display", "qualified-spaces", "raw-account-space-display", "profile-with-slash", "account-with-slash", "unicode-trimspace", "long-s-equalfold", "kelvin-equalfold", "sigma-equalfold", "orphan-bare", "orphan-uuid-tag", "empty-account-name-tag", "orphan-tolower-count", "overlapping-alias-one-exact-id", "folded-ids-stay-distinct"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionPG(t, "", false, false, nil, nil)
			f.exec(t, `UPDATE accounts SET username='alice' WHERE id='actor';UPDATE users SET name='Kids' WHERE id='target'`)
			name := "Kids"
			want := []BridgeLoginCandidate{{"actor", "target"}}
			switch kind {
			case "display":
				name = "alice/Kids"
			case "qualified-spaces":
				name = " alice / Kids "
			case "raw-account-space-display":
				f.exec(t, `UPDATE accounts SET username=' alice ' WHERE id='actor'`)
				bridgeCandidateWant(t, f, "alice / Kids", nil) // Third branch does not trim the actual account field.
				name = "alice/Kids"                            // Display branch does.
			case "profile-with-slash":
				f.exec(t, `UPDATE users SET name='Kids/TV' WHERE id='target'`)
				name = "alice/Kids/TV"
			case "account-with-slash":
				f.exec(t, `UPDATE accounts SET username='al/ice' WHERE id='actor'`)
				name = "al/ice/Kids"
			case "unicode-trimspace":
				f.exec(t, `UPDATE users SET name=$1 WHERE id='target'`, "\u2003Kids\u00a0")
				name = "\u2003Kids\u00a0"
			case "long-s-equalfold":
				f.exec(t, `UPDATE users SET name='ſ' WHERE id='target'`)
				name = "S"
			case "kelvin-equalfold":
				f.exec(t, `UPDATE users SET name='K' WHERE id='target'`)
				name = "k"
			case "sigma-equalfold":
				f.exec(t, `UPDATE users SET name='ς' WHERE id='target'`)
				name = "Σ"
			case "orphan-bare":
				f.exec(t, `UPDATE users SET account_id='absent' WHERE id='target'`)
				want[0].AccountID = "absent"
			case "orphan-uuid-tag":
				f.exec(t, `UPDATE users SET account_id='absent' WHERE id='target'`)
				bridgeCandidateInsert(t, f, "second", "other", "Kids")
				want[0].AccountID = "absent"
				name = "Kids (6a89)" // Independently computed fixed UUIDv5 vector for user\x00target.
			case "empty-account-name-tag":
				f.exec(t, `UPDATE accounts SET username='' WHERE id='actor'`)
				bridgeCandidateInsert(t, f, "second", "other", "Kids")
				name = "Kids (6a89)"
			case "orphan-tolower-count":
				f.exec(t, `UPDATE users SET account_id='absent',name='ſ' WHERE id='target'`)
				bridgeCandidateInsert(t, f, "second", "other", "S")
				want[0].AccountID = "absent"
				name = "S"
				// EqualFold matches both, but ToLower does not count these names together.
				bridgeCandidateWant(t, f, "ſ (6a89)", nil)
				want = []BridgeLoginCandidate{{"other", "second"}, {"absent", "target"}}
			case "overlapping-alias-one-exact-id":
				f.exec(t, `UPDATE accounts SET username='' WHERE id='actor'`)
			case "folded-ids-stay-distinct":
				bridgeCandidateInsert(t, f, "TARGET", "actor", "Kids")
				want = []BridgeLoginCandidate{{"actor", "TARGET"}, {"actor", "target"}}
			}
			bridgeCandidateWant(t, f, name, want)
		})
	}
}

func TestBridgeLoginCandidatesCompleteCurrentPGAndAmbiguity(t *testing.T) {
	for _, kind := range []string{"zero", "one", "two", "three", "four-after-complete-snapshot", "cold-cache-second-match", "stale-cache-extra-match", "orphan-match-counted"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionPG(t, "", false, false, nil, nil)
			f.exec(t, `UPDATE users SET name='Shared' WHERE id='target'`)
			// A retained ordinary repository projection is DATA, not an input
			// to this resolver. Keep it genuinely stale across the own PG insert.
			stale, projectionErr := f.ds.Users().List(t.Context())
			if projectionErr != nil || len(stale) != 1 {
				t.Fatal("initial actual name projection missing", projectionErr)
			}
			count := 1
			switch kind {
			case "zero":
				count = 0
			case "two", "cold-cache-second-match", "stale-cache-extra-match", "orphan-match-counted":
				count = 2
			case "three":
				count = 3
			case "four-after-complete-snapshot":
				count = 4
			}
			var want []BridgeLoginCandidate
			for n := 1; n < count; n++ {
				id := string(rune('a' + n))
				owner := "other"
				if kind == "orphan-match-counted" {
					owner = "missing"
				}
				bridgeCandidateInsert(t, f, id, owner, "Shared")
				want = append(want, BridgeLoginCandidate{owner, id})
			}
			if count > 0 {
				want = append(want, BridgeLoginCandidate{"actor", "target"})
			}
			if kind == "stale-cache-extra-match" && len(stale) != 1 {
				t.Fatal("retained projection was silently refreshed")
			}
			if kind == "cold-cache-second-match" {
				stale = nil
				if len(stale) != 0 {
					t.Fatal("cold hints are not empty")
				}
			}
			name := "Shared"
			if count == 0 {
				name = "NoMatch"
			}
			// No service cache is populated/refreshed. Only the original PG issuer
			// exists; real extra rows, even absent from all hints, must participate.
			if count == 4 {
				got, err := bridgeCandidateResolve(t, f, name)
				if !errors.Is(err, ErrBridgeLoginCandidatesAmbiguous) || got != nil || f.count(t) != 0 || bridgeWorkflowCount(f) != 0 {
					t.Fatal("four actual matches were not rejected before credential work", err)
				}
			} else {
				bridgeCandidateWant(t, f, name, want)
			}
		})
	}
}

func TestBridgeLoginCandidatesAllOrNothingSnapshotBounds(t *testing.T) {
	for _, kind := range []string{"profiles4096", "profiles4097-before-matching", "invalid-late-row-before-matching", "oversize-id", "oversize-profile-name", "oversize-account-name", "aggregate-budget-before-four-matches", "compressible-oversize-text", "typed-name-byte-bound"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionPG(t, "", false, false, nil, nil)
			f.exec(t, `UPDATE users SET name='Hit' WHERE id='target'`)
			name := "Hit"
			switch kind {
			case "profiles4096", "profiles4097-before-matching":
				count := 4095
				if kind == "profiles4097-before-matching" {
					count = 4096
				}
				f.exec(t, `INSERT INTO users(id,account_id,name)SELECT 'profile-'||n,'actor','NotHit' FROM generate_series(1,$1::int)n`, count)
			case "invalid-late-row-before-matching":
				for _, id := range []string{"a", "b", "c"} {
					bridgeCandidateInsert(t, f, id, "actor", "Hit")
				}
				bridgeCandidateInsert(t, f, "zz-invalid", "", "Hit")
			case "oversize-id":
				bridgeCandidateInsert(t, f, strings.Repeat("z", 257), "actor", "Hit")
			case "oversize-profile-name":
				f.exec(t, `UPDATE users SET name=repeat('x',4097) WHERE id='target'`)
			case "oversize-account-name":
				f.exec(t, `UPDATE accounts SET username=repeat('x',4097) WHERE id='actor'`)
			case "aggregate-budget-before-four-matches":
				for _, id := range []string{"a", "b", "c"} {
					bridgeCandidateInsert(t, f, id, "actor", "Hit")
				}
				f.exec(t, `INSERT INTO users(id,account_id,name)SELECT 'zz-fill-'||n,'actor',repeat('x',4096) FROM generate_series(1,1100)n`)
			case "compressible-oversize-text":
				f.exec(t, `UPDATE users SET name=repeat('x',1000000) WHERE id='target'`)
				var bytes, stored int
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT octet_length(name),pg_column_size(name) FROM users WHERE id='target'`).Scan(&bytes, &stored); err != nil || bytes != 1000000 || stored >= bytes {
					t.Fatal("own compressible wire-width fixture invalid", err)
				}
			case "typed-name-byte-bound":
				name = strings.Repeat("x", 65537)
			}
			got, err := bridgeCandidateResolve(t, f, name)
			if kind == "profiles4096" {
				if err != nil || !reflect.DeepEqual(got, []BridgeLoginCandidate{{"actor", "target"}}) {
					t.Fatal("complete valid4096 snapshot rejected", err)
				}
			} else if kind == "typed-name-byte-bound" {
				if got != nil || !errors.Is(err, ErrBridgeLoginCandidatesChanged) {
					t.Fatal("unbounded typed selector admitted", err)
				}
			} else if got != nil || !errors.Is(err, ErrBridgeLoginCandidatesUnavailable) {
				t.Fatal("incomplete/invalid snapshot became matching/unique/ambiguity", err)
			}
			if f.count(t) != 0 || bridgeWorkflowCount(f) != 0 {
				t.Fatal("resolver created credential/session authority")
			}
		})
	}
}

// Hooks observe/delay real original query callbacks, never supply rows or
// permission. The copied pool config preserves the existing lifecycle tracer.
type bridgeCandidateTrace struct {
	armed atomic.Bool
	start func(context.Context, *pgx.Conn)
	end   func(context.Context, *pgx.Conn)
}
type bridgeCandidateTraceKey struct{}

func (v *bridgeCandidateTrace) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if d.SQL == bridgeLoginCandidateQuery && v.armed.CompareAndSwap(true, false) {
		if v.start != nil {
			v.start(ctx, c)
		}
		return context.WithValue(ctx, bridgeCandidateTraceKey{}, true)
	}
	return ctx
}
func (v *bridgeCandidateTrace) TraceQueryEnd(ctx context.Context, c *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(bridgeCandidateTraceKey{}) == true && v.end != nil {
		v.end(ctx, c)
	}
}

type bridgeCandidateOperation struct {
	done   chan struct{}
	values []BridgeLoginCandidate
	err    error
}

func bridgeCandidateStart(t *testing.T, f *bridgeSessionFixture, r *http.Request, policy *config.BridgeLoginPolicyObservation, cancel context.CancelFunc, release func()) *bridgeCandidateOperation {
	t.Helper()
	op := &bridgeCandidateOperation{done: make(chan struct{})}
	t.Cleanup(func() { cancel(); release(); bridgeSessionAwait(t, op.done) })
	go func() {
		op.values, op.err = f.issuer.ResolvePasswordCandidates(r, "Synthetic Target", "synthetic-device", policy)
		close(op.done)
	}()
	return op
}
func bridgeCandidateRelease(channel chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(channel) }) }
}
func bridgeCandidateUnfinished(t *testing.T, op *bridgeCandidateOperation) {
	t.Helper()
	select {
	case <-op.done:
		t.Fatal("original resolver ended before controlled cleanup", op.err)
	default:
	}
}
func bridgeCandidateAssertNoCalls(t *testing.T) {
	t.Helper()
	if len(bridgeLoginCandidateCalls) != 0 {
		t.Fatal("actual candidate call charge remains after cleanup")
	}
}

func bridgeCandidateObserveBlocked(t *testing.T, f *bridgeSessionFixture, pid uint32, count int) {
	t.Helper()
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		var pending int
		if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE ($1::int=0 OR pid=$1) AND relation=$2::regclass AND NOT granted`, int32(pid), pgx.Identifier{f.schema, "users"}.Sanitize()).Scan(&pending); err != nil {
			t.Fatal("observe own actual users query", err)
		}
		if pending == count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual own users query wait count missing")
}

func TestBridgeLoginCandidatesOriginalRequestPolicyLifetime(t *testing.T) {
	for _, kind := range []string{"missing-deadline", "overlong-deadline", "context-drift-during-read", "client-carrier-drift", "canceled-entry", "bridge-disabled", "same-value-settings-attempt", "hold-canceled-before-query"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			release := bridgeCandidateRelease(proceed)
			t.Cleanup(release)
			trace := &bridgeCandidateTrace{end: func(context.Context, *pgx.Conn) { close(entered); <-proceed }}
			f := bridgeSessionPG(t, "", false, false, trace, nil)
			r, cancel := bridgeWorkflowRequest(t)
			if kind == "missing-deadline" {
				r = bridgeCredentialRequest(t.Context())
				if _, ok := r.Context().Deadline(); ok {
					t.Fatal("no-deadline control unavailable")
				}
			}
			if kind == "overlong-deadline" {
				ctx, stop := context.WithTimeout(t.Context(), time.Minute)
				t.Cleanup(stop)
				r = bridgeCredentialRequest(ctx)
			}
			if kind == "bridge-disabled" {
				f.settings.JellyfinBridge.Enabled = false
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
			}
			policy := bridgeWorkflowObservation(t, f, r)
			if kind == "canceled-entry" {
				cancel()
			}
			if kind == "hold-canceled-before-query" {
				if err := f.coordinator.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "missing-deadline" || kind == "overlong-deadline" || kind == "canceled-entry" || kind == "bridge-disabled" || kind == "hold-canceled-before-query" {
				values, err := f.issuer.ResolvePasswordCandidates(r, "Synthetic Target", "synthetic-device", policy)
				if values != nil || err == nil {
					t.Fatal("invalid original admitted result", err)
				}
				bridgeCandidateAssertNoCalls(t)
				return
			}
			trace.armed.Store(true)
			op := bridgeCandidateStart(t, f, r, policy, cancel, release)
			bridgeSessionAwait(t, entered)
			bridgeCandidateUnfinished(t, op)
			switch kind {
			case "context-drift-during-read":
				*r = *r.WithContext(context.WithValue(r.Context(), bridgeCandidateTraceKey{}, "drift"))
			case "client-carrier-drift":
				r.Header.Set("X-Client-ID", "other-device")
			case "same-value-settings-attempt":
				if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
					t.Fatal("original native hold did not exclude managed write", err)
				}
			}
			release()
			bridgeSessionAwait(t, op.done)
			if op.values != nil || !errors.Is(op.err, ErrBridgeLoginCandidatesChanged) {
				t.Fatal("original carrier/policy drift adopted", op.err)
			}
			bridgeCandidateAssertNoCalls(t)
		})
	}
}

func TestBridgeLoginCandidatesActualQueryCleanupAndAdmission(t *testing.T) {
	for _, kind := range []string{"actual-blocked-query-cancel", "coordinator-close-blocked-query", "queryend-before-refund", "physical-cleanup-before-hold-refund", "original-expiry-during-cleanup", "global8-nonqueued"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "physical-cleanup-before-hold-refund" {
				bridgeCandidatePhysicalJoin(t)
				return
			}
			if kind == "queryend-before-refund" || kind == "original-expiry-during-cleanup" {
				bridgeCandidateQueryEndJoin(t, kind == "original-expiry-during-cleanup")
				return
			}
			bridgeCandidateBlockedJoin(t, kind)
		})
	}
}

func bridgeCandidateQueryEndJoin(t *testing.T, expire bool) {
	t.Helper()
	entered, proceed := make(chan struct{}), make(chan struct{})
	release := bridgeCandidateRelease(proceed)
	t.Cleanup(release)
	trace := &bridgeCandidateTrace{end: func(context.Context, *pgx.Conn) { close(entered); <-proceed }}
	f := bridgeSessionPG(t, "", false, false, trace, nil)
	r, cancel := bridgeWorkflowRequest(t)
	if expire {
		ctx, stop := context.WithTimeout(t.Context(), 1200*time.Millisecond)
		cancel = stop
		t.Cleanup(stop)
		r = bridgeCredentialRequest(ctx)
	}
	policy := bridgeWorkflowObservation(t, f, r)
	trace.armed.Store(true)
	op := bridgeCandidateStart(t, f, r, policy, cancel, release)
	bridgeSessionAwait(t, entered)
	if r.Context().Err() != nil {
		t.Fatal("original expired before actual query-end witness")
	}
	if len(bridgeLoginCandidateCalls) != 1 {
		t.Fatal("call refunded before actual QueryEnd")
	}
	guard, guardErr := f.coordinator.TryWrite(r.Context())
	if guard != nil {
		_ = guard.Close()
	}
	if !errors.Is(guardErr, settingscommit.ErrBusy) {
		t.Fatal("native hold refunded before actual QueryEnd", guardErr)
	}
	bridgeCandidateUnfinished(t, op)
	if expire {
		<-r.Context().Done()
	}
	release()
	bridgeSessionAwait(t, op.done)
	if expire {
		if op.err == nil || op.values != nil {
			t.Fatal("expired original result published", op.err)
		}
	} else if op.err != nil || !reflect.DeepEqual(op.values, []BridgeLoginCandidate{{"actor", "target"}}) {
		t.Fatal("joined live original result lost", op.err)
	}
	bridgeCandidateAssertNoCalls(t)
}

func bridgeCandidateBlockedJoin(t *testing.T, kind string) {
	t.Helper()
	started := make(chan struct{}, 1)
	var pid atomic.Uint32
	trace := &bridgeCandidateTrace{start: func(_ context.Context, c *pgx.Conn) { pid.Store(c.PgConn().PID()); started <- struct{}{} }}
	f := bridgeSessionPG(t, "", false, false, trace, nil)
	holder, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var unlockOnce sync.Once
	unlock := func() {
		unlockOnce.Do(func() {
			ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = holder.Rollback(ctx)
		})
	}
	t.Cleanup(unlock)
	if _, err = holder.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "users"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	r, cancel := bridgeWorkflowRequest(t)
	policy := bridgeWorkflowObservation(t, f, r)
	trace.armed.Store(true)
	op := bridgeCandidateStart(t, f, r, policy, cancel, unlock)
	bridgeSessionAwait(t, started)
	bridgeCandidateObserveBlocked(t, f, pid.Load(), 1)
	if kind == "global8-nonqueued" {
		var others []*bridgeCandidateOperation
		for range 7 {
			others = append(others, bridgeCandidateStart(t, f, r, policy, cancel, unlock))
		}
		until := time.Now().Add(time.Second)
		for len(bridgeLoginCandidateCalls) != 8 && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if len(bridgeLoginCandidateCalls) != 8 {
			t.Fatal("eight actual admitted calls not witnessed")
		}
		// First witness all eight original queries waiting on this exact own
		// relation. Otherwise an earlier admitted call could still acquire a
		// connection during the ninth call's acquire-counter comparison.
		bridgeCandidateObserveBlocked(t, f, 0, 8)
		bridgeCandidateUnfinished(t, op)
		for _, other := range others {
			bridgeCandidateUnfinished(t, other)
		}
		before := f.ds.pool.Stat().AcquireCount()
		values, err := f.issuer.ResolvePasswordCandidates(r, "Synthetic Target", "synthetic-device", policy)
		if values != nil || !errors.Is(err, ErrBridgeLoginCandidatesBusy) || f.ds.pool.Stat().AcquireCount() != before {
			t.Fatal("ninth call entered query/acquire", err)
		}
		cancel()
		for _, other := range others {
			bridgeSessionAwait(t, other.done)
		}
	} else if kind == "coordinator-close-blocked-query" {
		if err := f.coordinator.Close(); err != nil {
			t.Fatal(err)
		}
		if r.Context().Err() != nil {
			t.Fatal("native-only cancellation also canceled caller")
		}
	} else {
		cancel()
	}
	bridgeSessionAwait(t, op.done)
	if op.err == nil || op.values != nil {
		t.Fatal("canceled original blocked query returned DATA", op.err)
	}
	var pending int
	if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE pid=$1 AND NOT granted`, int32(pid.Load())).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("original query still pending while own lock held", err)
	}
	bridgeCandidateAssertNoCalls(t)
	unlock()
}

func bridgeCandidatePhysicalJoin(t *testing.T) {
	t.Helper()
	var first atomic.Bool
	var mu sync.Mutex
	var actual *queryLifecycleGatedConn
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			gate := &queryLifecycleGatedConn{Conn: c, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			actual = gate
			mu.Unlock()
			return gate, nil
		}
		return c, nil
	}
	f := bridgeSessionPG(t, "", false, false, nil, dial)
	r, cancel := bridgeWorkflowRequest(t)
	policy := bridgeWorkflowObservation(t, f, r)
	mu.Lock()
	gate := actual
	mu.Unlock()
	if gate == nil {
		t.Fatal("own actual primary missing")
	}
	t.Cleanup(gate.release)
	c, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	physical := c.Conn().PgConn()
	c.Release()
	gate.armed.Store(true)
	op := bridgeCandidateStart(t, f, r, policy, cancel, gate.release)
	bridgeSessionAwait(t, gate.closeEntered)
	closed := make(chan struct{})
	go func() { _ = f.coordinator.Close(); close(closed) }()
	select {
	case <-physical.CleanupDone():
		t.Fatal("physical cleanup escaped actual held Close")
	default:
	}
	select {
	case <-closed:
		t.Fatal("native owner refunded before actual socket cleanup")
	default:
	}
	bridgeCandidateUnfinished(t, op)
	if len(bridgeLoginCandidateCalls) != 1 {
		t.Fatal("call charge refunded before actual CleanupDone")
	}
	gate.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, op.done)
	bridgeSessionAwait(t, closed)
	if op.err == nil || op.values != nil || r.Context().Err() != nil {
		t.Fatal("actual socket cleanup changed original caller or published DATA", op.err)
	}
	bridgeCandidateAssertNoCalls(t)
}

func TestBridgeLoginCandidatesSnapshotAndPhysicalDriftDATA(t *testing.T) {
	for _, kind := range []string{"single-statement-rename-snapshot", "single-statement-insert-snapshot", "fresh-rename-observed", "fresh-owner-row-aba-observed"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			release := bridgeCandidateRelease(proceed)
			t.Cleanup(release)
			trace := &bridgeCandidateTrace{end: func(context.Context, *pgx.Conn) { close(entered); <-proceed }}
			f := bridgeSessionPG(t, "", false, false, trace, nil)
			if strings.HasPrefix(kind, "single-statement-") {
				r, cancel := bridgeWorkflowRequest(t)
				policy := bridgeWorkflowObservation(t, f, r)
				trace.armed.Store(true)
				op := bridgeCandidateStart(t, f, r, policy, cancel, release)
				bridgeSessionAwait(t, entered)
				bridgeCandidateUnfinished(t, op)
				query := `UPDATE ` + pgx.Identifier{f.schema, "users"}.Sanitize() + ` SET name='Renamed' WHERE id='target'`
				if kind == "single-statement-insert-snapshot" {
					query = `INSERT INTO ` + pgx.Identifier{f.schema, "users"}.Sanitize() + `(id,account_id,name)VALUES('extra','other','Synthetic Target')`
				}
				if _, err := f.admin.Exec(t.Context(), query); err != nil {
					t.Fatal(err)
				}
				release()
				bridgeSessionAwait(t, op.done)
				if op.err != nil || !reflect.DeepEqual(op.values, []BridgeLoginCandidate{{"actor", "target"}}) {
					t.Fatal("single completed statement DATA was replaced by post-query names", op.err)
				}
				if kind == "single-statement-insert-snapshot" {
					bridgeCandidateWant(t, f, "Synthetic Target", []BridgeLoginCandidate{{"other", "extra"}, {"actor", "target"}})
				} else {
					bridgeCandidateWant(t, f, "Synthetic Target", nil)
					bridgeCandidateWant(t, f, "Renamed", []BridgeLoginCandidate{{"actor", "target"}})
				}
			} else {
				before, err := f.issuer.readBridgeLoginCandidateRows(t.Context())
				if err != nil || len(before) != 1 {
					t.Fatal("initial actual snapshot missing", err)
				}
				if kind == "fresh-rename-observed" {
					f.exec(t, `UPDATE users SET name='Renamed' WHERE id='target'`)
				} else {
					f.exec(t, `UPDATE users SET account_id='other' WHERE id='target';UPDATE users SET account_id='actor' WHERE id='target'`)
				}
				after, err := f.issuer.readBridgeLoginCandidateRows(t.Context())
				if err != nil || len(after) != 1 || before[0].profileRow == after[0].profileRow {
					t.Fatal("fresh physical row change invisible", err)
				}
				name := "Synthetic Target"
				if kind == "fresh-rename-observed" {
					name = "Renamed"
				}
				bridgeCandidateWant(t, f, name, []BridgeLoginCandidate{{"actor", "target"}})
			}
		})
	}
}
