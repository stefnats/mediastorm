package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"github.com/jackc/pgx/v5"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/models"
)

// The fixture owns only the existing synthetic Unix-socket schema. All grant
// bindings below are created by the actual PIN protocol, never fabricated.
type publicPinFixture struct {
	*unlockPGFixture
	request *http.Request
	issuer  *PGPinIssuer
}

func publicPinPG(t *testing.T) *publicPinFixture {
	t.Helper()
	base := requestFrameFixture(t)
	base.exec(t, `ALTER TABLE users ADD COLUMN pin_length INT NOT NULL DEFAULT 20,
      ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      ADD COLUMN is_kids_profile BOOLEAN NOT NULL DEFAULT false`)
	base.exec(t, `UPDATE users SET pin_length=$1`, len("synthetic target PIN"))
	request, release, err := base.ds.AuthenticateProfilePinRequest(base.request(t.Context()), base.session.Token, base.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return &publicPinFixture{unlockPGFixture: base, request: request, issuer: NewPGPinIssuer(base.ds)}
}
func (f *publicPinFixture) issue(t *testing.T, ttl time.Duration) (string, time.Time) {
	t.Helper()
	intent, err := f.issuer.Prepare(f.request, "target", ttl)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := f.issuer.Verify(intent, "synthetic target PIN")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.issuer.Commit(intent, proof)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	token, expiry, err := f.issuer.Release(f.request, receipt)
	if err != nil {
		t.Fatal(err)
	}
	return token, expiry
}
func pinBoundOutcome(t *testing.T, ds *DataStore, r *http.Request, target string, tokens ...string) ProfilePinAccessOutcome {
	t.Helper()
	reader := NewBoundProfilePinAccessReader(ds)
	state, err := reader.Read(r, target, tokens)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	outcome, err := reader.Check(state)
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}
func (f *publicPinFixture) authenticate(t *testing.T, token string) *http.Request {
	t.Helper()
	request, release, err := f.ds.AuthenticateProfilePinRequest(f.unlockPGFixture.request(t.Context()), token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return request
}
func nextPinSession(label string) string {
	sum := sha256.Sum256([]byte(label))
	return base64.URLEncoding.EncodeToString(sum[:])
}
func TestPublicPinBindingCutoverAndActualRelease(t *testing.T) {
	f := publicPinPG(t)
	if got := pinBoundOutcome(t, f.ds, f.request, "target", f.token); got != ProfilePinLocked {
		t.Fatal("legacy NULL binding adopted")
	}
	token, _ := f.issue(t, time.Minute)
	if got := pinBoundOutcome(t, f.ds, f.request, "target", token); got != ProfilePinUnlocked {
		t.Fatal("actual verified grant rejected")
	}
	if err := f.issuer.CheckVerifiedProfilePinRequest(f.request, "target"); err != nil {
		t.Fatal(err)
	}
	before := f.trace.calls.Load()
	if err := f.issuer.CheckVerifiedProfilePinRequest(f.request, "target"); !errors.Is(err, ErrPGPinUsed) || f.trace.calls.Load() != before {
		t.Fatal("known-post replay performed IO")
	}
}
func TestPublicPinKnownPostRejectsChangedRowsAfterRelease(t *testing.T) {
	for _, change := range []string{
		`UPDATE users SET updated_at=clock_timestamp() WHERE id='target'`,
		`UPDATE users SET pin_length=pin_length+1 WHERE id='target'`,
		`UPDATE users SET pin_hash=pin_hash WHERE id='target'`,
		`UPDATE accounts SET password_hash=password_hash WHERE id='actor'`,
		`UPDATE sessions SET user_agent='replacement'`,
	} {
		t.Run(change, func(t *testing.T) {
			f := publicPinPG(t)
			f.issue(t, time.Minute)
			f.exec(t, change)
			if err := f.issuer.CheckVerifiedProfilePinRequest(f.request, "target"); err == nil {
				t.Fatal("external post-release row change accepted")
			}
		})
	}
}
func TestPublicPinNoPinKnownPostDoesNotInventGrant(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "new PIN"}[changed], func(t *testing.T) {
			f := publicPinPG(t)
			f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target';DELETE FROM profile_unlocks`)
			if err := f.issuer.RecordNoPinVerification(f.request, "target"); err != nil {
				t.Fatal(err)
			}
			if changed {
				f.exec(t, `UPDATE users SET pin_hash=$1 WHERE id='target'`, f.pinHash)
			}
			err := f.issuer.CheckVerifiedProfilePinRequest(f.request, "target")
			if changed && err == nil || !changed && err != nil {
				t.Fatal("no-PIN post-state mismatch")
			}
			if f.count(t) != 0 {
				t.Fatal("no-PIN reply created a grant")
			}
		})
	}
}
func TestPublicPinRefreshPreservesValidLineageAndBoundedTTL(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Minute} {
		t.Run(map[bool]string{false: "fixed", true: "session bound"}[ttl == 0], func(t *testing.T) {
			f := publicPinPG(t)
			token, originalExpiry := f.issue(t, ttl)
			next, err := f.ds.RefreshProfilePinSession(f.request, nextPinSession("SYNTHETIC_REFRESHED_SESSION"), 2*time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			request := f.authenticate(t, next.Token)
			if got := pinBoundOutcome(t, f.ds, request, "target", token); got != ProfilePinUnlocked {
				t.Fatal("valid refresh lost current grant")
			}
			var expiry time.Time
			var lineage string
			if err = f.ds.pool.QueryRow(t.Context(), `SELECT expires_at,pin_session_lineage FROM profile_unlocks WHERE token_hash=$1`, profileUnlockDigest(token)).Scan(&expiry, &lineage); err != nil {
				t.Fatal(err)
			}
			if lineage != profileUnlockDigest(f.session.Token) {
				t.Fatal("refresh changed original session lineage")
			}
			if ttl > 0 && !expiry.Equal(originalExpiry) {
				t.Fatal("fixed TTL was extended")
			}
			if ttl == 0 && !expiry.Equal(next.ExpiresAt) {
				t.Fatal("valid session-bound refresh was not preserved")
			}
			// The old original request can still revoke its exact refreshed lineage.
			if err = f.ds.LogoutProfilePinSession(f.request); err != nil {
				t.Fatal(err)
			}
			var n int
			if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE pin_unlock_lineage=$1`, lineage).Scan(&n); err != nil || n != 0 || f.count(t) != 0 {
				t.Fatal("logout A failed to revoke refreshed B")
			}
		})
	}
}
func TestPublicPinRefreshNeverResurrectsUnboundExpiredOrRevoked(t *testing.T) {
	for _, mode := range []string{"legacy", "expired", "revoked", "modified binding"} {
		t.Run(mode, func(t *testing.T) {
			f := publicPinPG(t)
			token := f.token
			if mode != "legacy" {
				token, _ = f.issue(t, time.Minute)
			}
			switch mode {
			case "expired":
				f.exec(t, `UPDATE profile_unlocks SET expires_at=clock_timestamp()-interval '1 second' WHERE token_hash=$1`, profileUnlockDigest(token))
			case "revoked":
				if err := f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target"); err != nil {
					t.Fatal(err)
				}
			case "modified binding":
				f.exec(t, `UPDATE profile_unlocks SET session_bound=NOT session_bound WHERE token_hash=$1`, profileUnlockDigest(token))
			}
			next, err := f.ds.RefreshProfilePinSession(f.request, nextPinSession("SYNTHETIC_REFRESHED_SESSION"), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			if got := pinBoundOutcome(t, f.ds, f.authenticate(t, next.Token), "target", token); got != ProfilePinLocked {
				t.Fatal("refresh resurrected invalid/legacy grant")
			}
		})
	}
}
func TestPublicPinPreparedIssueAfterEmptyLogoutCannotInsert(t *testing.T) {
	f := publicPinPG(t)
	f.exec(t, `DELETE FROM profile_unlocks`)
	intent, err := f.issuer.Prepare(f.request, "target", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := f.issuer.Verify(intent, "synthetic target PIN")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ds.LogoutProfilePinSession(f.request); err != nil {
		t.Fatal(err)
	}
	receipt, err := f.issuer.Commit(intent, proof)
	if receipt != nil || !errors.Is(err, ErrPGPinChanged) || f.count(t) != 0 {
		t.Fatal("late issuance survived empty lineage revoke")
	}
}
func TestPublicPinRefreshBlockedBeforeLogoutUsesOriginalLineage(t *testing.T) {
	f := publicPinPG(t)
	f.issue(t, time.Minute)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(tx, t.Context())
	if _, err = tx.Exec(t.Context(), `SELECT generation FROM profile_unlock_revocations WHERE scope='profile' AND scope_key='target' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := f.ds.RefreshProfilePinSession(f.request, nextPinSession("SYNTHETIC_BLOCKED_REFRESH"), time.Hour)
		done <- err
	}()
	f.waitBlocked(t)
	if err = f.ds.LogoutProfilePinSession(f.request); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil {
		t.Fatal("refresh rebound a logged-out original session")
	}
	var n int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions`).Scan(&n); err != nil || n != 0 || f.count(t) != 0 {
		t.Fatal("late refresh resurrected session/grant")
	}
}
func TestPublicPinConcurrentRefreshHasSingleSuccessor(t *testing.T) {
	f := publicPinPG(t)
	token, _ := f.issue(t, time.Minute)
	type result struct {
		session models.Session
		err     error
	}
	done := make(chan result, 2)
	for _, next := range []string{nextPinSession("SYNTHETIC_REFRESH_ONE"), nextPinSession("SYNTHETIC_REFRESH_TWO")} {
		go func(next string) {
			s, e := f.ds.RefreshProfilePinSession(f.request, next, time.Hour)
			done <- result{s, e}
		}(next)
	}
	var winner models.Session
	success := 0
	for range 2 {
		got := <-done
		if got.err == nil {
			success++
			winner = got.session
		}
	}
	if success != 1 {
		t.Fatal("concurrent refresh did not have exactly one successor")
	}
	if got := pinBoundOutcome(t, f.ds, f.authenticate(t, winner.Token), "target", token); got != ProfilePinUnlocked {
		t.Fatal("winner grant is not current")
	}
}
func TestPublicPinRefreshUnknownCommitNoSuccessAndActualCommit(t *testing.T) {
	f := publicPinPG(t)
	f.issue(t, time.Minute)
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
	ds := &DataStore{pool: pool}
	// Authentication does not mutate this already-bound frame, so the proxy's
	// first COMMIT belongs to the actual rotation, not a setup transaction.
	request, release, err := ds.AuthenticateProfilePinRequest(f.unlockPGFixture.request(t.Context()), f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	next, err := ds.RefreshProfilePinSession(request, nextPinSession("SYNTHETIC_ACK_LOST_REFRESH"), time.Hour)
	if next.Token != "" || !errors.Is(err, ErrPGPinOutcomeUnknown) || !dropped.Load() {
		t.Fatal("unknown refresh commit returned success")
	}
	var n int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE token=$1`, nextPinSession("SYNTHETIC_ACK_LOST_REFRESH")).Scan(&n); err != nil || n != 1 {
		t.Fatal("lost ACK did not follow actual rotation")
	}
	// Original row is gone; explicit second invocation cannot create another.
	if _, err = ds.RefreshProfilePinSession(request, nextPinSession("SYNTHETIC_NOT_REPLAYED"), time.Hour); err == nil {
		t.Fatal("unknown rotation adopted a replacement session")
	}
}
func TestPublicPinRefreshCancelRollbackAndNoReplacementAdoption(t *testing.T) {
	f := publicPinPG(t)
	f.issue(t, time.Minute)
	ctx, cancel := context.WithCancel(f.request.Context())
	request := f.request.WithContext(ctx)
	cancel()
	before := f.trace.calls.Load()
	if _, err := f.ds.RefreshProfilePinSession(request, nextPinSession("SYNTHETIC_CANCELLED_REFRESH"), time.Hour); !errors.Is(err, context.Canceled) || f.trace.calls.Load() != before {
		t.Fatal("cancelled refresh performed IO")
	}
	if err := f.ds.LogoutProfilePinSession(request); !errors.Is(err, context.Canceled) || f.trace.calls.Load() != before {
		t.Fatal("cancelled logout performed IO")
	}
	if err := f.ds.CheckProfilePinRequestFrame(f.request); err != nil {
		t.Fatal("cancelled child damaged original session")
	}
}

type refreshExpiryTrace struct {
	base    *unlockQueryTrace
	arrived chan struct{}
	release chan struct{}
	armed   atomic.Bool
}

func (tr *refreshExpiryTrace) TraceQueryStart(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = tr.base.TraceQueryStart(ctx, c, data)
	if strings.HasPrefix(data.SQL, "UPDATE profile_unlocks SET session_hash=") && tr.armed.CompareAndSwap(true, false) {
		close(tr.arrived)
		select {
		case <-tr.release:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (tr *refreshExpiryTrace) TraceQueryEnd(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryEndData) {
	tr.base.TraceQueryEnd(ctx, c, data)
}
func TestPublicPinRefreshDoesNotExtendGrantExpiredAtActualWrite(t *testing.T) {
	f := publicPinPG(t)
	_, expiry := f.issue(t, time.Second)
	cfg := f.cfg.Copy()
	tr := &refreshExpiryTrace{base: f.trace, arrived: make(chan struct{}), release: make(chan struct{})}
	tr.armed.Store(true)
	cfg.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	request, release, err := ds.AuthenticateProfilePinRequest(f.unlockPGFixture.request(t.Context()), f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() {
		_, e := ds.RefreshProfilePinSession(request, nextPinSession("SYNTHETIC_EXPIRY_AT_WRITE"), time.Hour)
		done <- e
	}()
	select {
	case <-tr.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not reach actual grant write")
	}
	// Wait on the actual database clock, not an assumed scheduler delay. No
	// product table is modified to fabricate the expired-before-write boundary.
	if _, err = f.admin.Exec(t.Context(), `SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM($1::timestamptz-clock_timestamp()))))`, expiry.Add(time.Millisecond)); err != nil {
		close(tr.release)
		t.Fatal(err)
	}
	close(tr.release)
	if err = <-done; err == nil {
		t.Fatal("refresh extended grant after its actual expiry")
	}
	var count int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions WHERE token=$1`, nextPinSession("SYNTHETIC_EXPIRY_AT_WRITE")).Scan(&count); err != nil || count != 0 {
		t.Fatal("expired transfer left successor session")
	}
	if err = f.ds.CheckProfilePinRequestFrame(f.request); err != nil {
		t.Fatal("failed transfer damaged original session")
	}
}

func TestPublicPinLogoutUnknownCommitDoesNotReportSuccess(t *testing.T) {
	f := publicPinPG(t)
	f.issue(t, time.Minute)
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
	ds := &DataStore{pool: pool}
	request, release, err := ds.AuthenticateProfilePinRequest(f.unlockPGFixture.request(t.Context()), f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err = ds.LogoutProfilePinSession(request); !errors.Is(err, ErrPGPinOutcomeUnknown) || !dropped.Load() {
		t.Fatal("lost logout ACK reported success")
	}
	var count int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM sessions`).Scan(&count); err != nil || count != 0 || f.count(t) != 0 {
		t.Fatal("unknown ACK did not follow actual logout commit")
	}
	if got := f.generation(t, "session", profileUnlockDigest(f.session.Token)); got != 1 {
		t.Fatal("unknown logout repeated its revocation")
	}
}
