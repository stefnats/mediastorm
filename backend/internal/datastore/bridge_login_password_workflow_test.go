//go:build linux

package datastore

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/settingscommit"
)

func bridgeWorkflowRequest(t *testing.T) (*http.Request, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	t.Cleanup(cancel)
	return bridgeCredentialRequest(ctx), cancel
}
func bridgeWorkflowObservation(t *testing.T, f *bridgeSessionFixture, r *http.Request) *config.BridgeLoginPolicyObservation {
	t.Helper()
	value, err := f.security.CaptureBridgePolicy(r.Context())
	if err != nil {
		t.Fatal("capture actual original workflow policy", err)
	}
	return value
}
func bridgeWorkflowRows(t *testing.T, f *bridgeSessionFixture) []byte {
	t.Helper()
	var raw []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_array(
 (SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM accounts a),
 (SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM users u),
 (SELECT jsonb_agg(to_jsonb(s) ORDER BY token) FROM sessions s),
 (SELECT jsonb_agg(to_jsonb(v) ORDER BY user_id) FROM user_settings_versions v),
 (SELECT jsonb_agg(to_jsonb(p) ORDER BY scope,scope_key) FROM profile_unlock_revocations p))`).Scan(&raw); err != nil {
		t.Fatal("read own workflow rows", err)
	}
	return raw
}
func bridgeWorkflowCount(f *bridgeSessionFixture) int {
	bridgeCredentialAdmission.Lock()
	defer bridgeCredentialAdmission.Unlock()
	n := 0
	for i := range bridgeCredentialAdmission.intents {
		if i.state.store == f.ds {
			n++
		}
	}
	return n
}
func bridgeWorkflowMint(t *testing.T, f *bridgeSessionFixture, r *http.Request, proof *BridgeLoginCredentialProof, policy *config.BridgeLoginPolicyObservation) {
	t.Helper()
	if f.count(t) != 0 {
		t.Fatal("workflow minted before actual Commit")
	}
	receipt, err := f.issuer.Commit(r, proof, policy)
	if err != nil {
		t.Fatal("existing actual mint rejected workflow proof", err)
	}
	defer receipt.Close()
	session, err := f.issuer.Release(r, receipt)
	if err != nil || session.Token == "" || session.AccountID != "actor" || session.ScopeResource != "target" || f.count(t) != 1 {
		t.Fatal("existing one-shot mint/release did not confirm original proof", err)
	}
}

func TestBridgePasswordWorkflowActualStoredPolicyKindMatrix(t *testing.T) {
	for _, tc := range []struct {
		name, pin, raw string
		enforce, allow bool
		want           bool
		kind           bridgeCredentialKind
	}{
		{"no-pin-false", "", parentTestPassword, false, false, true, bridgeCredentialAccount},
		{"no-pin-true", "", parentTestPassword, true, false, true, bridgeCredentialAccount},
		{"pin-account-false", "2580", parentTestPassword, false, false, true, bridgeCredentialAccount},
		{"pin-account-true-rejected", "2580", parentTestPassword, true, false, false, bridgeCredentialAccount},
		{"combined-true", "2580", parentTestPassword + "2580", true, false, true, bridgeCredentialCombined},
		{"optional-pin-false-enabled", "2580", "2580", false, true, true, bridgeCredentialPIN},
		{"optional-pin-true-enabled", "2580", "2580", true, true, true, bridgeCredentialPIN},
		{"optional-pin-disabled", "2580", "2580", false, false, false, bridgeCredentialPIN},
		{"bridge-disabled", "2580", parentTestPassword, false, true, false, bridgeCredentialAccount},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := bridgeSessionPG(t, tc.pin, tc.enforce, tc.allow, nil, nil)
			if tc.name == "bridge-disabled" {
				f.settings.JellyfinBridge.Enabled = false
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
			}
			r, _ := bridgeWorkflowRequest(t)
			policy := bridgeWorkflowObservation(t, f, r)
			before := bridgeWorkflowRows(t, f)
			proof, err := f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", tc.raw, policy)
			if proof != nil {
				defer proof.Close()
			}
			if (err == nil) != tc.want || (proof != nil) != tc.want || !bytes.Equal(before, bridgeWorkflowRows(t, f)) {
				t.Fatal("genuine policy selection changed or workflow wrote storage", err)
			}
			if !tc.want {
				if bridgeWorkflowCount(f) != 0 {
					t.Fatal("denied workflow retained credential admission")
				}
				return
			}
			if proof.state.kind != tc.kind || proof.state.request != r || proof.state.requestContext != r.Context() || proof.Check(r.Context()) != nil {
				t.Fatal("workflow lost original kind/request/lifetime")
			}
			// This is a genuine native/PG write guard, not a bool readiness probe.
			guard, err := f.coordinator.TryWrite(r.Context())
			if err != nil {
				t.Fatal("selection cleanup did not retire native hold", err)
			}
			if err := guard.Close(); err != nil {
				t.Fatal(err)
			}
			bridgeWorkflowMint(t, f, r, proof, policy)
		})
	}
}

func TestBridgePasswordWorkflowActualRawKnownAndUnknownPINWidths(t *testing.T) {
	for _, tc := range []struct {
		name, pin string
		width     int
		want      bool
	}{
		{"known3", "7  ", 3, true}, {"known9", "abcdefghi", 9, true}, {"utf8", "Ä雪 ", len("Ä雪 "), true},
		{"unknown4", "1234", 0, true}, {"unknown5", "12345", 0, true}, {"unknown6", "123456", 0, true},
		{"unknown7", "1234567", 0, true}, {"unknown8", "12345678", 0, true}, {"unknown9-rejected", "123456789", 0, false},
		{"prefix-space-preserved", " 7 ", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := bridgeSessionPG(t, tc.pin, true, false, nil, nil)
			f.exec(t, "UPDATE users SET pin_length=$1 WHERE id='target'", tc.width)
			r, _ := bridgeWorkflowRequest(t)
			policy := bridgeWorkflowObservation(t, f, r)
			before := bridgeWorkflowRows(t, f)
			proof, err := f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", " \t"+parentTestPassword+" \n"+tc.pin, policy)
			if proof != nil {
				defer proof.Close()
			}
			if (err == nil) != tc.want || (proof != nil) != tc.want || !bytes.Equal(before, bridgeWorkflowRows(t, f)) {
				t.Fatal("raw byte split/known length/unknown width contract changed", err)
			}
			if proof != nil && proof.state.kind != bridgeCredentialCombined {
				t.Fatal("combined raw input changed proof kind")
			}
		})
	}
}

type bridgeWorkflowOperation struct {
	done  chan struct{}
	proof *BridgeLoginCredentialProof
	err   error
}

func bridgeWorkflowStart(t *testing.T, f *bridgeSessionFixture, r *http.Request, raw string, policy *config.BridgeLoginPolicyObservation, cancel context.CancelFunc, unblock func()) *bridgeWorkflowOperation {
	t.Helper()
	op := &bridgeWorkflowOperation{done: make(chan struct{})}
	t.Cleanup(func() {
		cancel()
		unblock()
		bridgeSessionAwait(t, op.done)
		if op.proof != nil {
			op.proof.Close()
		}
	})
	go func() {
		op.proof, op.err = f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", raw, policy)
		close(op.done)
	}()
	return op
}
func bridgeWorkflowBlockCPU(t *testing.T) func() {
	t.Helper()
	bridgeCredentialCPU <- struct{}{}
	bridgeCredentialCPU <- struct{}{}
	var once sync.Once
	release := func() { once.Do(func() { <-bridgeCredentialCPU; <-bridgeCredentialCPU }) }
	t.Cleanup(release)
	return release
}
func bridgeWorkflowWaitingIntent(t *testing.T, f *bridgeSessionFixture, r *http.Request, op *bridgeWorkflowOperation) *BridgeLoginCredentialIntent {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		bridgeCredentialAdmission.Lock()
		var own []*BridgeLoginCredentialIntent
		for i := range bridgeCredentialAdmission.intents {
			if i.state.store == f.ds && i.state.request == r {
				own = append(own, i)
			}
		}
		bridgeCredentialAdmission.Unlock()
		for _, i := range own {
			i.state.mu.Lock()
			waiting := i.state.waitingCPU
			i.state.mu.Unlock()
			if waiting {
				return i
			}
		}
		select {
		case <-op.done:
			t.Fatal("workflow ended before real CPU admission witness", op.err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual workflow did not reach bounded real CPU queue")
	return nil
}

func TestBridgePasswordWorkflowActualRejectedOnlyFallbackAndSnapshotDrift(t *testing.T) {
	for _, kind := range []string{"rejected-fallback-success", "changed-not-fallback", "unavailable-not-fallback", "busy-not-fallback", "hash-drift-before-fallback", "pin-drift-before-fallback", "household-drift-before-fallback", "settings-attempt-before-fallback"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionPG(t, "2580", false, true, nil, nil)
			r, cancel := bridgeWorkflowRequest(t)
			policy := bridgeWorkflowObservation(t, f, r)
			if kind == "unavailable-not-fallback" {
				f.exec(t, "UPDATE accounts SET password_hash='unsupported' WHERE id='actor'")
				p, err := f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", "2580", policy)
				if p != nil {
					p.Close()
				}
				if p != nil || !errors.Is(err, ErrBridgeCredentialUnavailable) {
					t.Fatal("unavailable became optional PIN", err)
				}
				return
			}
			if kind == "busy-not-fallback" {
				for range 8 {
					i, err := f.ds.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(i.Close)
				}
				p, err := f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", "2580", policy)
				if p != nil {
					p.Close()
				}
				if p != nil || !errors.Is(err, ErrBridgeCredentialBusy) {
					t.Fatal("busy became optional PIN", err)
				}
				return
			}
			releaseCPU := bridgeWorkflowBlockCPU(t)
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }); releaseCPU() }
			op := bridgeWorkflowStart(t, f, r, "2580", policy, cancel, unblock)
			i := bridgeWorkflowWaitingIntent(t, f, r, op)
			i.state.mu.Lock()
			realCompare := i.state.compare
			i.state.compare = func(hash, raw []byte) error { err := realCompare(hash, raw); close(entered); <-proceed; return err }
			i.state.mu.Unlock()
			releaseCPU()
			bridgeSessionAwait(t, entered)
			switch kind {
			case "changed-not-fallback":
				r.Header.Set("X-Client-ID", "different-current-carrier")
			case "hash-drift-before-fallback":
				hash, err := bcrypt.GenerateFromPassword([]byte("different own account password"), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				f.exec(t, "UPDATE accounts SET password_hash=$1 WHERE id='actor'", string(hash))
			case "pin-drift-before-fallback":
				hash, err := bcrypt.GenerateFromPassword([]byte("2580"), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				f.exec(t, "UPDATE users SET pin_hash=$1 WHERE id='target'", string(hash))
			case "household-drift-before-fallback":
				f.exec(t, "INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic Child',true)")
			case "settings-attempt-before-fallback":
				if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
					t.Fatal("selection did not hold actual native SH", err)
				}
			}
			unblock()
			bridgeSessionAwait(t, op.done)
			if kind == "rejected-fallback-success" {
				if op.err != nil || op.proof == nil || op.proof.state.kind != bridgeCredentialPIN {
					t.Fatal("actual rejected account did not allow genuine same-snapshot PIN", op.err)
				}
			} else if op.err == nil || op.proof != nil {
				t.Fatal("changed/unavailable snapshot became adopted PIN proof", op.err)
			}
			if f.count(t) != 0 {
				t.Fatal("selection minted a session")
			}
		})
	}
}

type bridgeWorkflowContextKey struct{}

func TestBridgePasswordWorkflowOriginalRequestClientAndFixedBudget(t *testing.T) {
	for _, kind := range []string{"bounded-original-handoff", "missing-deadline", "overlong-deadline", "original-request-pointer", "context-clone-denied", "client-header-drift", "fallback-fixed-expiry", "query-drift", "raw-carrier-byte-drift", "canceled-entry"} {
		t.Run(kind, func(t *testing.T) {
			pin, allow, raw := "", false, parentTestPassword
			if kind == "fallback-fixed-expiry" {
				pin, allow, raw = "2580", true, "2580"
			}
			f := bridgeSessionPG(t, pin, false, allow, nil, nil)
			r, cancel := bridgeWorkflowRequest(t)
			if kind == "missing-deadline" {
				r = bridgeCredentialRequest(t.Context())
				if _, ok := r.Context().Deadline(); ok {
					t.Fatal("fixture is not the required no-deadline control")
				}
			}
			if kind == "overlong-deadline" {
				ctx, stop := context.WithTimeout(t.Context(), time.Minute)
				t.Cleanup(stop)
				r = bridgeCredentialRequest(ctx)
			}
			if kind == "raw-carrier-byte-drift" {
				r.Header.Set("User-Agent", string([]byte{255, 65}))
			}
			originalContext := r.Context()
			deadline, _ := originalContext.Deadline()
			policy := bridgeWorkflowObservation(t, f, r)
			if kind == "canceled-entry" {
				cancel()
			}
			proof, err := f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", raw, policy)
			if proof != nil {
				defer proof.Close()
			}
			if kind == "missing-deadline" || kind == "overlong-deadline" || kind == "canceled-entry" {
				if proof != nil || !errors.Is(err, ErrBridgeCredentialChanged) || bridgeWorkflowCount(f) != 0 || f.count(t) != 0 {
					t.Fatal("invalid entry admitted evidence/work", err)
				}
				return
			}
			if err != nil || proof == nil || proof.state.request != r || proof.state.requestContext != originalContext || !proof.ExpiresAt().Equal(deadline) {
				t.Fatal("original pointer/context/fixed effective expiry changed", err)
			}
			bad := r
			switch kind {
			case "original-request-pointer":
				bad = r.Clone(r.Context())
			case "context-clone-denied":
				bad = r.WithContext(context.WithValue(r.Context(), bridgeWorkflowContextKey{}, true))
			case "client-header-drift":
				r.Header.Set("X-Client-ID", "foreign-device")
			case "query-drift":
				r.URL.RawQuery = "different=selector"
			case "raw-carrier-byte-drift":
				r.Header.Set("User-Agent", string([]byte{254, 65}))
			default:
				bridgeWorkflowMint(t, f, r, proof, policy)
				return
			}
			receipt, err := f.issuer.Commit(bad, proof, policy)
			if receipt != nil {
				receipt.Close()
			}
			if err == nil || receipt != nil || f.count(t) != 0 {
				t.Fatal("changed original request became mint permission", err)
			}
		})
	}
}

func TestBridgePasswordWorkflowForeignOwnerChildAndHouseholdDenied(t *testing.T) {
	for _, kind := range []string{"foreign-profile", "child-profile", "adult-household", "master-household", "malformed-account-hash", "expired-account"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionPG(t, "2580", false, true, nil, nil)
			switch kind {
			case "foreign-profile":
				f.exec(t, "UPDATE users SET account_id='other' WHERE id='target'")
			case "child-profile":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='target'")
			case "master-household":
				f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
				fallthrough
			case "adult-household":
				f.exec(t, "INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic Child',true)")
			case "malformed-account-hash":
				f.exec(t, "UPDATE accounts SET password_hash='malformed' WHERE id='actor'")
			case "expired-account":
				f.exec(t, "UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'")
			}
			r, _ := bridgeWorkflowRequest(t)
			policy := bridgeWorkflowObservation(t, f, r)
			before := bridgeWorkflowRows(t, f)
			proof, err := f.issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", "2580", policy)
			if proof != nil {
				proof.Close()
			}
			if err == nil || proof != nil || !bytes.Equal(before, bridgeWorkflowRows(t, f)) || bridgeWorkflowCount(f) != 0 {
				t.Fatal("unsupported actual owner/household/hash became evidence", err)
			}
		})
	}
}

func TestBridgePasswordWorkflowActualHoldCancelCPUAndPGJoin(t *testing.T) {
	for _, kind := range []string{"actual-bcrypt-return", "initial-sql-cancel", "fallback-sql-cancel", "physical-cleanup-before-hold-refund"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "actual-bcrypt-return" {
				bridgeWorkflowCPUJoin(t)
				return
			}
			if kind == "physical-cleanup-before-hold-refund" {
				bridgeWorkflowPhysicalJoin(t)
				return
			}
			var armed atomic.Bool
			var pid atomic.Uint32
			waiting := make(chan struct{}, 1)
			trace := &accountRevokeTracer{start: func(_ context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) {
				if strings.Contains(d.SQL, "FROM accounts a") && armed.CompareAndSwap(true, false) {
					pid.Store(c.PgConn().PID())
					waiting <- struct{}{}
				}
			}}
			f := bridgeSessionPG(t, "2580", false, true, trace, nil)
			r, cancel := bridgeWorkflowRequest(t)
			policy := bridgeWorkflowObservation(t, f, r)
			var holder pgx.Tx
			var releaseCPU = func() {}
			var once sync.Once
			proceed := make(chan struct{})
			unblock := func() { once.Do(func() { close(proceed) }); releaseCPU() }
			if kind == "fallback-sql-cancel" {
				releaseCPU = bridgeWorkflowBlockCPU(t)
			}
			var op *bridgeWorkflowOperation
			if kind == "fallback-sql-cancel" {
				op = bridgeWorkflowStart(t, f, r, "2580", policy, cancel, func() {
					unblock()
					if holder != nil {
						ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
						defer stop()
						_ = holder.Rollback(ctx)
					}
				})
				i := bridgeWorkflowWaitingIntent(t, f, r, op)
				entered := make(chan struct{})
				i.state.mu.Lock()
				realCompare := i.state.compare
				i.state.compare = func(h, p []byte) error { err := realCompare(h, p); close(entered); <-proceed; return err }
				i.state.mu.Unlock()
				releaseCPU()
				bridgeSessionAwait(t, entered)
			}
			var err error
			holder, err = f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			// This cleanup runs before an already-started operation's cleanup.
			t.Cleanup(func() {
				unblock()
				ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_ = holder.Rollback(ctx)
			})
			if _, err = holder.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			if op == nil {
				op = bridgeWorkflowStart(t, f, r, parentTestPassword, policy, cancel, func() {
					unblock()
					ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					_ = holder.Rollback(ctx)
				})
			} else {
				unblock()
			}
			bridgeSessionAwait(t, waiting)
			bridgeSessionObserveBlocked(t, f, pid.Load())
			if r.Context().Err() != nil {
				t.Fatal("caller expired before actual native cancellation")
			}
			if err := f.coordinator.Close(); err != nil {
				t.Fatal(err)
			}
			bridgeSessionAwait(t, op.done)
			if op.err == nil || op.proof != nil || r.Context().Err() != nil || bridgeWorkflowCount(f) != 0 {
				t.Fatal("native retirement did not join original SQL/admission", op.err)
			}
			var pending int
			if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE pid=$1 AND NOT granted", int32(pid.Load())).Scan(&pending); err != nil || pending != 0 {
				t.Fatal("actual original pending SQL survived return", err)
			}
		})
	}
}

func bridgeWorkflowCPUJoin(t *testing.T) {
	t.Helper()
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	r, cancel := bridgeWorkflowRequest(t)
	policy := bridgeWorkflowObservation(t, f, r)
	releaseCPU := bridgeWorkflowBlockCPU(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(proceed) }); releaseCPU() }
	op := bridgeWorkflowStart(t, f, r, parentTestPassword, policy, cancel, unblock)
	i := bridgeWorkflowWaitingIntent(t, f, r, op)
	i.state.mu.Lock()
	realCompare := i.state.compare
	i.state.compare = func(h, p []byte) error { err := realCompare(h, p); close(entered); <-proceed; return err }
	i.state.mu.Unlock()
	releaseCPU()
	bridgeSessionAwait(t, entered)
	if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("actual CPU work had no native selection charge", err)
	}
	closed := make(chan struct{})
	go func() { _ = f.coordinator.Close(); close(closed) }()
	bridgeSessionAwait(t, i.Context().Done())
	select {
	case <-op.done:
		t.Fatal("workflow returned before actual bcrypt wrapper return")
	default:
	}
	select {
	case <-closed:
		t.Fatal("native owner refunded before actual CPU cleanup")
	default:
	}
	if bridgeWorkflowCount(f) != 1 {
		t.Fatal("credential slot refunded during real noninterruptible comparison")
	}
	unblock()
	bridgeSessionAwait(t, op.done)
	bridgeSessionAwait(t, closed)
	if op.err == nil || op.proof != nil || bridgeWorkflowCount(f) != 0 || r.Context().Err() != nil {
		t.Fatal("actual CPU/hold/callback cleanup did not finish", op.err)
	}
}

func bridgeWorkflowPhysicalJoin(t *testing.T) {
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
		t.Fatal("actual primary socket missing")
	}
	t.Cleanup(gate.release)
	connection, err := f.ds.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	physical := connection.Conn().PgConn()
	connection.Release()
	gate.armed.Store(true)
	op := bridgeWorkflowStart(t, f, r, parentTestPassword, policy, cancel, gate.release)
	bridgeSessionAwait(t, gate.closeEntered)
	closed := make(chan struct{})
	go func() { _ = f.coordinator.Close(); close(closed) }()
	select {
	case <-physical.CleanupDone():
		t.Fatal("physical cleanup escaped held actual socket Close")
	default:
	}
	select {
	case <-op.done:
		t.Fatal("workflow returned before actual CleanupDone")
	default:
	}
	select {
	case <-closed:
		t.Fatal("native charge refunded before actual physical join")
	default:
	}
	if bridgeWorkflowCount(f) != 1 {
		t.Fatal("initial credential admission refunded before physical SQL cleanup")
	}
	gate.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, op.done)
	bridgeSessionAwait(t, closed)
	if op.err == nil || op.proof != nil || bridgeWorkflowCount(f) != 0 || r.Context().Err() != nil {
		t.Fatal("physical cleanup/hold retirement failed", op.err)
	}
}
