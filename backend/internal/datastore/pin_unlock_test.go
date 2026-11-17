package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

const pinTestCredential = " 42ß "

type pinInsertContextKey struct{}
type pinQueryTrace struct {
	base        *unlockQueryTrace
	afterInsert atomic.Pointer[func(context.Context)]
}

func (tr *pinQueryTrace) TraceQueryStart(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = tr.base.TraceQueryStart(ctx, c, data)
	if strings.HasPrefix(data.SQL, "INSERT INTO profile_unlocks(") {
		ctx = context.WithValue(ctx, pinInsertContextKey{}, true)
	}
	return ctx
}
func (tr *pinQueryTrace) TraceQueryEnd(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryEndData) {
	tr.base.TraceQueryEnd(ctx, c, data)
	if marked, _ := ctx.Value(pinInsertContextKey{}).(bool); marked && data.Err == nil {
		if hook := tr.afterInsert.Load(); hook != nil {
			(*hook)(ctx)
		}
	}
}

type pinPGFixture struct {
	*unlockPGFixture
	issuer    *PGPinIssuer
	traceHook *pinQueryTrace
}

func pinFixture(t *testing.T) *pinPGFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL PIN issuance fixture required")
	}
	// Reuse the unchanged strict Unix-socket, own NOLOGIN-role/schema fixture.
	// This opt-in is local to this nonparallel test and restored by t.Cleanup.
	t.Setenv("PROFILE_UNLOCK_STATE_TEST_DATABASE_URL", raw)
	base := unlockFixture(t)
	bindingMigration, err := embedMigrations.ReadFile("migrations/078_profile_unlock_bindings.sql")
	if err != nil {
		t.Fatal(err)
	}
	base.exec(t, strings.Split(string(bindingMigration), "-- +goose Down")[0])

	base.exec(t, `ALTER TABLE users ADD COLUMN pin_length INT NOT NULL DEFAULT 0, ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();DELETE FROM profile_unlocks`)
	hash, err := bcrypt.GenerateFromPassword([]byte(pinTestCredential), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	base.pinHash = string(hash)
	base.exec(t, `UPDATE users SET pin_hash=$1 WHERE id='target'`, base.pinHash)
	cfg := base.cfg.Copy()
	tr := &pinQueryTrace{base: base.trace}
	cfg.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("connect own issuer schema")
	}
	t.Cleanup(pool.Close)
	return &pinPGFixture{unlockPGFixture: base, issuer: NewPGPinIssuer(&DataStore{pool: pool}), traceHook: tr}
}
func (f *pinPGFixture) prepare(t *testing.T, ttl time.Duration) (*PGPinIntent, *PGPinProof) {
	t.Helper()
	intent, err := f.issuer.Prepare(f.request(t.Context()), "target", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	proof, err := f.issuer.Verify(intent, pinTestCredential)
	if err != nil {
		t.Fatal(err)
	}
	return intent, proof
}
func (f *pinPGFixture) assertNoGrant(t *testing.T) {
	t.Helper()
	if f.count(t) != 0 {
		t.Fatal("retired attempt inserted grant")
	}
}

func TestPGPinPositiveRawBytesAndIndependentTTL(t *testing.T) {
	f := pinFixture(t)
	intent, proof := f.prepare(t, 10*time.Minute)
	deadline, _ := intent.state.ctx.Deadline()
	if time.Until(deadline) > 30*time.Second {
		t.Fatal("attempt deadline exceeds budget")
	}
	var before, after []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u)-'pin_length'-'updated_at' FROM users u WHERE id='target'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	receipt, err := f.issuer.Commit(intent, proof)
	if err != nil || receipt == nil {
		t.Fatalf("valid original PIN denied: %v", err)
	}
	defer receipt.Close()
	grant := receipt.state.grant
	digest := sha256.Sum256([]byte(f.pinHash))
	if grant.PinFingerprint != hex.EncodeToString(digest[:8]) || !profileUnlockCredential(receipt.state.rawToken) || grant.TokenHash != profileUnlockDigest(receipt.state.rawToken) || grant.SessionHash != profileUnlockDigest(f.session.Token) {
		t.Fatal("stored token/fingerprint format does not match verified original hash")
	}
	if grant.SessionBound || grant.ExpiresAt.Sub(grant.CreatedAt) < 9*time.Minute || grant.ExpiresAt.Sub(grant.CreatedAt) > 10*time.Minute {
		t.Fatal("grant TTL reused short attempt deadline or exceeded configured TTL")
	}
	var length int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT pin_length,to_jsonb(u)-'pin_length'-'updated_at' FROM users u WHERE id='target'`).Scan(&length, &after); err != nil {
		t.Fatal(err)
	}
	if length != len(pinTestCredential) || length == len([]rune(pinTestCredential)) || string(before) != string(after) {
		t.Fatal("PinLength migration changed bytes or unrelated fields")
	}
	var version string
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM users u WHERE id='target'`).Scan(&version); err != nil || version != receipt.state.snapshot.target.version {
		t.Fatal("receipt did not bind post-migration row")
	}
	state, err := f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", receipt.state.rawToken)
	if err != nil {
		t.Fatal(err)
	}
	if current, err := f.ds.CheckProfileUnlockState(state); err != nil || !current {
		t.Fatal("new actual PG grant/current tuple unreadable")
	}
	for _, value := range []any{f.issuer, intent, proof, receipt} {
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != "null" {
			t.Fatal("opaque state exposed JSON")
		}
		for _, render := range []string{fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
			if strings.Contains(render, pinTestCredential) || strings.Contains(render, f.pinHash) || strings.Contains(render, receipt.state.rawToken) {
				t.Fatal("opaque state leaked secret")
			}
		}
	}
}
func TestPGPinWrongRawPINAndMasterNoBypass(t *testing.T) {
	for _, candidate := range []string{"", strings.TrimSpace(pinTestCredential), "wrong target PIN", "synthetic actor password hash"} {
		t.Run(fmt.Sprint(len(candidate)), func(t *testing.T) {
			f := pinFixture(t)
			f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
			f.session.IsMaster = true
			intent, err := f.issuer.Prepare(f.request(t.Context()), "target", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer intent.Close()
			proof, err := f.issuer.Verify(intent, candidate)
			if proof != nil || !errors.Is(err, ErrPGPinCredential) {
				t.Fatal("wrong raw target PIN accepted")
			}
			if _, err = f.issuer.Verify(intent, pinTestCredential); !errors.Is(err, ErrPGPinUsed) {
				t.Fatal("failed credential attempt retried")
			}
			f.assertNoGrant(t)
		})
	}
}
func TestPGPinIdentityAndReplayBeforeIO(t *testing.T) {
	for _, mode := range []string{"intent copy", "proof copy", "zero proof", "wrong intent", "other issuer", "other store", "issuer copy", "closed intent", "closed proof", "replay"} {
		t.Run(mode, func(t *testing.T) {
			f := pinFixture(t)
			intent, proof := f.prepare(t, time.Minute)
			issuer := f.issuer
			switch mode {
			case "intent copy":
				copy := *intent
				intent = &copy
			case "proof copy":
				copy := *proof
				proof = &copy
			case "zero proof":
				proof = &PGPinProof{}
			case "wrong intent":
				other, err := issuer.Prepare(f.request(t.Context()), "target", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				intent = other
			case "other issuer":
				issuer = NewPGPinIssuer(f.issuer.store)
			case "other store":
				issuer = NewPGPinIssuer(&DataStore{pool: f.issuer.store.pool})
			case "issuer copy":
				copy := *issuer
				issuer = &copy
			case "closed intent":
				intent.Close()
			case "closed proof":
				proof.Close()
			case "replay":
				receipt, err := issuer.Commit(intent, proof)
				if err != nil {
					t.Fatal(err)
				}
				defer receipt.Close()
			}
			before := f.trace.calls.Load()
			receipt, err := issuer.Commit(intent, proof)
			if receipt != nil || err == nil || f.trace.calls.Load() != before {
				t.Fatalf("invalid identity performed IO: %v", err)
			}
		})
	}
}
func TestPGPinPostProofChangesRetireOriginal(t *testing.T) {
	changes := map[string]string{
		"PIN replaced":               `UPDATE users SET pin_hash='replacement' WHERE id='target'`,
		"PIN cleared":                `UPDATE users SET pin_hash='',pin_length=0 WHERE id='target'`,
		"profile owner changed":      `UPDATE users SET account_id='other' WHERE id='target'`,
		"profile equal ABA":          `UPDATE users SET pin_hash=pin_hash WHERE id='target'`,
		"actor password reset":       `UPDATE accounts SET password_hash='reset' WHERE id='actor'`,
		"actor role changed":         `UPDATE accounts SET is_master=true WHERE id='actor'`,
		"owner expired":              `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`,
		"session revoked":            `DELETE FROM sessions`,
		"session rotated":            `UPDATE sessions SET token='SYNTHETIC_REPLACEMENT_SESSION'`,
		"session client changed":     `UPDATE sessions SET client_id='replacement'`,
		"session role cache changed": `UPDATE sessions SET is_master=true`,
	}
	for name, sql := range changes {
		t.Run(name, func(t *testing.T) {
			f := pinFixture(t)
			intent, proof := f.prepare(t, time.Minute)
			f.exec(t, sql)
			receipt, err := f.issuer.Commit(intent, proof)
			if receipt != nil || !errors.Is(err, ErrPGPinChanged) {
				t.Fatalf("changed tuple committed: %v", err)
			}
			f.assertNoGrant(t)
		})
	}
}
func TestPGPinRealBcryptOutsideLocksWithLateTargetChange(t *testing.T) {
	for _, action := range []string{"target changed", "request cancelled"} {
		t.Run(action, func(t *testing.T) {
			f := pinFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			intent, err := f.issuer.Prepare(f.request(ctx), "target", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			defer intent.Close()
			verified, release := make(chan struct{}), make(chan struct{})
			actual := f.issuer.compare
			f.issuer.compare = func(hash, pin []byte) error { err := actual(hash, pin); close(verified); <-release; return err }
			type outcome struct {
				proof *PGPinProof
				err   error
			}
			done := make(chan outcome, 1)
			go func() { p, e := f.issuer.Verify(intent, pinTestCredential); done <- outcome{p, e} }()
			<-verified
			var open bool
			if err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND state='idle in transaction')`, f.application).Scan(&open); err != nil || open {
				t.Fatal("bcrypt retained a database transaction")
			}
			if action == "target changed" {
				f.exec(t, `UPDATE users SET pin_hash='replacement' WHERE id='target'`)
			} else {
				cancel()
			}
			close(release)
			result := <-done
			if action == "request cancelled" {
				if result.proof != nil || !errors.Is(result.err, context.Canceled) {
					t.Fatal("cancelled bcrypt emitted proof")
				}
			} else {
				if result.err != nil || result.proof == nil {
					t.Fatal("real original compare failed")
				}
				receipt, err := f.issuer.Commit(intent, result.proof)
				if receipt != nil || !errors.Is(err, ErrPGPinChanged) {
					t.Fatal("old bcrypt used new target hash")
				}
			}
			f.assertNoGrant(t)
		})
	}
}
func TestPGPinEmptyRevokeBetweenPrepareAndCommit(t *testing.T) {
	for _, scope := range []string{"profile", "session"} {
		t.Run(scope, func(t *testing.T) {
			f := pinFixture(t)
			intent, proof := f.prepare(t, time.Minute)
			var err error
			if scope == "profile" {
				err = f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target")
			} else {
				err = f.ds.ProfileUnlocks().DeleteBySession(t.Context(), profileUnlockDigest(f.session.Token))
			}
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := f.issuer.Commit(intent, proof)
			if receipt != nil || !errors.Is(err, ErrPGPinChanged) {
				t.Fatalf("completed empty revoke lost: %v", err)
			}
			f.assertNoGrant(t)
		})
	}
}
func TestPGPinPostInsertRevokeSerializes(t *testing.T) {
	for _, scope := range []string{"profile", "session"} {
		t.Run(scope, func(t *testing.T) {
			f := pinFixture(t)
			intent, proof := f.prepare(t, time.Minute)
			inserted, release := make(chan struct{}), make(chan struct{})
			hook := func(ctx context.Context) {
				close(inserted)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			f.traceHook.afterInsert.Store(&hook)
			type result struct {
				receipt *PGPinCommitReceipt
				err     error
			}
			commit := make(chan result, 1)
			go func() { r, e := f.issuer.Commit(intent, proof); commit <- result{r, e} }()
			<-inserted
			revoked := make(chan error, 1)
			go func() {
				if scope == "profile" {
					revoked <- f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target")
				} else {
					revoked <- f.ds.ProfileUnlocks().DeleteBySession(t.Context(), profileUnlockDigest(f.session.Token))
				}
			}()
			f.waitBlocked(t)
			close(release)
			got := <-commit
			if got.err != nil || got.receipt == nil {
				t.Fatalf("original insert failed: %v", got.err)
			}
			defer got.receipt.Close()
			if err := <-revoked; err != nil {
				t.Fatal(err)
			}
			if f.count(t) != 0 {
				t.Fatal("later revoke failed to delete committed grant")
			}
			if state, err := f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", got.receipt.state.rawToken); state != nil || !errors.Is(err, ErrProfileUnlockStateDenied) {
				t.Fatal("historical receipt revived revoked grant")
			}
		})
	}
}
func TestPGPinMigrationTopologyLockOrder(t *testing.T) {
	f := pinFixture(t)
	intent, proof := f.prepare(t, time.Minute)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err = tx.Exec(t.Context(), `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := f.issuer.Commit(intent, proof); done <- e }()
	f.waitBlocked(t)
	// A topology writer can still take this row while holding its table lock.
	if _, err = tx.Exec(t.Context(), `UPDATE users SET pin_length=pin_length WHERE id='target'`); err != nil {
		t.Fatal("issuer held row before its required table lock")
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; !errors.Is(err, ErrPGPinChanged) {
		t.Fatalf("changed topology context not rejected: %v", err)
	}
	f.assertNoGrant(t)
}
func TestPGPinParallelProofOnce(t *testing.T) {
	f := pinFixture(t)
	intent, proof := f.prepare(t, time.Minute)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := f.issuer.Commit(intent, proof)
			if e == nil {
				successes.Add(1)
				r.Close()
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || f.count(t) != 1 {
		t.Fatal("proof committed more than once")
	}
}
func TestPGPinTTLBoundsAndSessionBoundSemantics(t *testing.T) {
	for _, mode := range []string{"zero TTL", "negative TTL", "session bound", "owner bound", "request cancelled", "original deadline"} {
		t.Run(mode, func(t *testing.T) {
			f := pinFixture(t)
			ttl := time.Hour
			switch mode {
			case "zero TTL":
				ttl = 0
			case "negative TTL":
				ttl = -time.Second
			case "session bound":
				f.session.ExpiresAt = time.Now().UTC().Add(2 * time.Minute).Truncate(time.Microsecond)
				f.exec(t, `UPDATE sessions SET expires_at=$1`, f.session.ExpiresAt)
			case "owner bound":
				f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()+interval '2 minutes' WHERE id='actor'`)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "original deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(ctx, time.Now().Add(3*time.Second))
				defer stop()
			}
			intent, err := f.issuer.Prepare(f.request(ctx), "target", ttl)
			if err != nil {
				t.Fatal(err)
			}
			defer intent.Close()
			proof, err := f.issuer.Verify(intent, pinTestCredential)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "request cancelled" {
				cancel()
				before := f.trace.calls.Load()
				receipt, err := f.issuer.Commit(intent, proof)
				if receipt != nil || !errors.Is(err, context.Canceled) || f.trace.calls.Load() != before {
					t.Fatal("cancelled original request performed commit IO")
				}
				return
			}
			if mode == "original deadline" {
				deadline, _ := intent.state.ctx.Deadline()
				expected, _ := ctx.Deadline()
				if deadline.After(expected) {
					t.Fatal("original deadline renewed")
				}
			}
			receipt, err := f.issuer.Commit(intent, proof)
			if err != nil {
				t.Fatal(err)
			}
			defer receipt.Close()
			grant := receipt.state.grant
			if (mode == "zero TTL" || mode == "negative TTL") && (!grant.SessionBound || !grant.ExpiresAt.Equal(f.session.ExpiresAt)) {
				t.Fatal("session-bound expiry/default fallback changed")
			}
			if (mode == "session bound" || mode == "owner bound") && grant.ExpiresAt.Sub(grant.CreatedAt) > 2*time.Minute {
				t.Fatal("grant exceeds original authority expiry")
			}
			if mode == "original deadline" && grant.ExpiresAt.Sub(grant.CreatedAt) < 30*time.Minute {
				t.Fatal("attempt budget became grant TTL")
			}
		})
	}
}
func TestPGPinUnknownCommitReturnsNoReceiptAndNoRetry(t *testing.T) {
	f := pinFixture(t)
	cfg := f.cfg.Copy()
	var dropped atomic.Bool
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &parentDropCommit{Conn: c, dropped: &dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	issuer := NewPGPinIssuer(&DataStore{pool: pool})
	intent, err := issuer.Prepare(f.request(t.Context()), "target", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := issuer.Verify(intent, pinTestCredential)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := issuer.Commit(intent, proof)
	if receipt != nil || !errors.Is(err, ErrPGPinOutcomeUnknown) || !dropped.Load() {
		t.Fatalf("unknown commit leaked receipt: %v", err)
	}
	if f.count(t) != 1 {
		t.Fatal("lost ACK did not follow actual insert")
	}
	before := f.trace.calls.Load()
	if r, e := issuer.Commit(intent, proof); r != nil || !errors.Is(e, ErrPGPinUsed) || f.trace.calls.Load() != before {
		t.Fatal("unknown commit retried")
	}
}
func TestPGPinPrepareRejectsAbsentOrUntrustedAuthority(t *testing.T) {
	for _, mode := range []string{"no PIN", "malformed PIN", "expired owner", "scope", "client replaced", "missing session", "nil store", "zero session expiry"} {
		t.Run(mode, func(t *testing.T) {
			f := pinFixture(t)
			issuer := f.issuer
			switch mode {
			case "no PIN":
				f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
			case "malformed PIN":
				f.exec(t, `UPDATE users SET pin_hash='invalid' WHERE id='target'`)
			case "expired owner":
				f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`)
			case "scope":
				f.session.Scope = "stream"
			case "client replaced":
				f.exec(t, `UPDATE sessions SET client_id='new-client'`)
			case "missing session":
				f.exec(t, `DELETE FROM sessions`)
			case "nil store":
				issuer = NewPGPinIssuer(nil)
			case "zero session expiry":
				f.session.ExpiresAt = time.Time{}
			}
			intent, err := issuer.Prepare(f.request(t.Context()), "target", time.Minute)
			if intent != nil || err == nil {
				t.Fatal("missing/untrusted original authority accepted")
			}
			f.assertNoGrant(t)
		})
	}
}

func TestPGPinCancelAfterInsertRollsBackMigration(t *testing.T) {
	f := pinFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	intent, err := f.issuer.Prepare(f.request(ctx), "target", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := f.issuer.Verify(intent, pinTestCredential)
	if err != nil {
		t.Fatal(err)
	}
	inserted := make(chan struct{})
	hook := func(ctx context.Context) { close(inserted); <-ctx.Done() }
	f.traceHook.afterInsert.Store(&hook)
	type result struct {
		receipt *PGPinCommitReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() { r, e := f.issuer.Commit(intent, proof); done <- result{r, e} }()
	<-inserted
	cancel()
	got := <-done
	if got.receipt != nil || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("cancel after INSERT emitted receipt: %v", got.err)
	}
	f.assertNoGrant(t)
	var length int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT pin_length FROM users WHERE id='target'`).Scan(&length); err != nil || length != 0 {
		t.Fatal("cancelled INSERT left partial PinLength migration")
	}
}
