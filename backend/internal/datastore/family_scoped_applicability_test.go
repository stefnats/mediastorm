package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/auth"
	"novastream/models"
)

func familyScopedPG(t *testing.T, tracer pgx.QueryTracer) *ratingPGFixture {
	t.Helper()
	f := familyMediaPG(t)
	cfg := f.cfg.Copy()
	cfg.ConnConfig.Tracer = tracer
	configureQueryLifecycle(cfg.ConnConfig)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("open scoped original production-query lifecycle pool")
	}
	t.Cleanup(pool.Close)
	f.ds = &DataStore{pool: pool}
	f.cfg = cfg
	f.session.Scope = models.SessionScopeJellyfin
	f.session.ScopeResource = "target"
	f.exec(t, "UPDATE sessions SET scope=$1,scope_resource=$2 WHERE token=$3", f.session.Scope, f.session.ScopeResource, f.session.Token)
	return f
}
func familyScopedRequest(t *testing.T, f *ratingPGFixture, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal("authenticate genuine original scoped PG request", err)
	}
	return checked, release
}
func familyScopedCapture(t *testing.T, f *ratingPGFixture, r *http.Request) *FamilyScopedApplicability {
	t.Helper()
	proof, err := f.ds.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		t.Fatal("capture actual scoped inactive applicability", err)
	}
	t.Cleanup(proof.Close)
	return proof
}
func TestFamilyScopedActualInactiveMembershipAndPureLifetimeContext(t *testing.T) {
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, context.WithValue(t.Context(), familyScopedTraceKey{}, "private original value"))
	defer release()
	proof := familyScopedCapture(t, f, r)
	if proof.Check(t.Context()) != nil || proof.CheckRequest(r) != nil || !proof.ExpiresAt().Equal(f.session.ExpiresAt) {
		t.Fatal("genuine inactive original scoped profile denied")
	}
	for _, key := range []any{auth.ContextKeySession, auth.ContextKeyAccountID, auth.ContextKeyIsMaster, pgRequestFrameKey{}, familyScopedTraceKey{}} {
		if proof.Context().Value(key) != nil {
			t.Fatal("lifetime context leaked original authentication/frame values")
		}
	}
	if deadline, ok := proof.Context().Deadline(); !ok || !deadline.Equal(proof.ExpiresAt()) {
		t.Fatal("original fixed deadline missing")
	}
	if full, err := f.ds.CaptureFamilyMediaOwner(r, "target", nil); full != nil || err == nil {
		t.Fatal("scoped applicability promoted to full original source owner")
	}
}
func TestFamilyScopedImportedCopiedAndFormattedValuesStayOpaque(t *testing.T) {
	var zero FamilyScopedApplicability
	if json.Unmarshal([]byte("{}"), &zero) == nil || zero.Check(t.Context()) == nil || zero.CheckRequest(nil) == nil || zero.Handoff(nil) == nil || zero.Context().Err() == nil || !zero.ExpiresAt().IsZero() {
		t.Fatal("imported scoped value became usable")
	}
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	copy := *proof
	copy.Close()
	if copy.Check(t.Context()) == nil || proof.Check(t.Context()) != nil {
		t.Fatal("copy gained or retired original permission")
	}
	for _, value := range []any{proof, *proof, &copy, copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyScopedApplicability(opaque)" {
				t.Fatal("opaque scoped proof leaked fields")
			}
		}
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("scoped proof serialized")
		}
	}
	if p, err := f.ds.CaptureInactiveJellyfinApplicability(f.parentPGFixture.request(t.Context())); p != nil || err == nil {
		t.Fatal("raw auth context replaced genuine PG frame")
	}
}
func TestFamilyScopedRawScopeAndExactOwnedProfileNeverWidenEvenMaster(t *testing.T) {
	for _, kind := range []string{"full", "stream", "party", "missing profile", "foreign master", "child", "household child", "unknown profile"} {
		t.Run(kind, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			switch kind {
			case "full":
				f.session.Scope = ""
				f.session.ScopeResource = ""
			case "stream":
				f.session.Scope = models.SessionScopeStream
			case "party":
				f.session.Scope = models.SessionScopeWatchParty
			case "missing profile":
				f.session.ScopeResource = ""
			case "foreign master":
				f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor';UPDATE users SET account_id='other' WHERE id='target'")
			case "child":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='target'")
			case "household child":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second'")
			case "unknown profile":
				f.session.ScopeResource = "absent-profile"
			}
			f.exec(t, "UPDATE sessions SET scope=$1,scope_resource=$2 WHERE token=$3", f.session.Scope, f.session.ScopeResource, f.session.Token)
			r, release := familyScopedRequest(t, f, t.Context())
			defer release()
			if proof, err := f.ds.CaptureInactiveJellyfinApplicability(r); proof != nil || err == nil {
				if proof != nil {
					proof.Close()
				}
				t.Fatal("unknown/foreign/active/wrong scope became inactive", kind)
			}
		})
	}
}
func TestFamilyScopedCurrentPhysicalAndHouseholdABACannotReviveOriginal(t *testing.T) {
	for _, kind := range []string{"profile", "session", "actor", "member child", "other household", "settings generation", "scope resource"} {
		t.Run(kind, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			r, release := familyScopedRequest(t, f, t.Context())
			defer release()
			proof := familyScopedCapture(t, f, r)
			switch kind {
			case "profile":
				f.exec(t, "UPDATE users SET name='temporary' WHERE id='target';UPDATE users SET name='Synthetic Target' WHERE id='target'")
			case "session":
				f.exec(t, "UPDATE sessions SET user_agent='temporary' WHERE token=$1", f.session.Token)
				f.exec(t, "UPDATE sessions SET user_agent='' WHERE token=$1", f.session.Token)
			case "actor":
				f.exec(t, "UPDATE accounts SET username='temporary' WHERE id='actor';UPDATE accounts SET username='Synthetic Parent' WHERE id='actor'")
			case "member child":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second';UPDATE users SET is_kids_profile=false WHERE id='own-second'")
			case "other household":
				f.exec(t, "UPDATE users SET name=name WHERE id='unrelated'")
			case "settings generation":
				f.exec(t, "UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='target'")
			case "scope resource":
				f.exec(t, "UPDATE sessions SET scope_resource='own-second' WHERE token=$1", f.session.Token)
				f.exec(t, "UPDATE sessions SET scope_resource='target' WHERE token=$1", f.session.Token)
			}
			if proof.Check(t.Context()) == nil || proof.Context().Err() == nil {
				t.Fatal("late physical/household change revived original", kind)
			}
		})
	}
}

type familyScopedQueryCount struct{ queries atomic.Int64 }

func (g *familyScopedQueryCount) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if ctx.Value(familyScopedTraceKey{}) == true {
		g.queries.Add(1)
	}
	return ctx
}
func (*familyScopedQueryCount) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func TestFamilyScopedFreshRequestCancellationDoesNotRetireOriginal(t *testing.T) {
	tracer := &familyScopedQueryCount{}
	f := familyScopedPG(t, tracer)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	fresh, stop := familyScopedRequest(t, f, t.Context())
	defer stop()
	foreignClient := fresh.Clone(fresh.Context())
	foreignClient.Header.Set("X-Client-ID", "other-client")
	if proof.CheckRequest(foreignClient) == nil || proof.Check(t.Context()) != nil {
		t.Fatal("foreign fresh request was accepted or retired valid original")
	}
	caller, cancel := context.WithCancel(t.Context())
	cancel()
	if proof.Check(caller) == nil || proof.Check(t.Context()) != nil {
		t.Fatal("departed consumer retired original")
	}
	if proof.Handoff(r) != nil {
		t.Fatal("actual original handoff failed")
	}
	release()
	if proof.Context().Err() != nil {
		t.Fatal("successful handoff did not retain original")
	}
	original, _ := auth.GetSession(fresh)
	changed := original
	changed.Scope = models.SessionScopeStream
	conflicting := []*http.Request{
		r.WithContext(context.Background()),
		r.WithContext(context.WithoutCancel(r.Context())),
		fresh.WithContext(context.WithValue(fresh.Context(), auth.ContextKeySession, changed)),
		fresh.WithContext(context.WithValue(fresh.Context(), auth.ContextKeyAccountID, "other")),
		fresh.WithContext(context.WithValue(fresh.Context(), auth.ContextKeyIsMaster, true)),
		fresh.WithContext(context.WithValue(fresh.Context(), auth.ContextKeyIsMaster, "false")),
	}
	for _, bad := range conflicting {
		bad = bad.WithContext(context.WithValue(bad.Context(), familyScopedTraceKey{}, true))
		before := tracer.queries.Load()
		if proof.CheckRequest(bad) == nil || proof.Handoff(bad) == nil {
			t.Fatal("departed/contradictory local consumer accepted")
		}
		if tracer.queries.Load() != before {
			t.Fatal("local frame/claim refusal reached PG")
		}
		if proof.Check(t.Context()) != nil {
			t.Fatal("departed/contradictory consumer retired genuine handed-off original")
		}
	}
	// The fresh frame can depart after the initial local check, while the
	// WithoutCancel consumer remains live and its real current query is blocked.
	for _, operation := range []string{"CheckRequest", "Handoff"} {
		consumer, depart := familyScopedRequest(t, f, t.Context())
		consumer = consumer.WithContext(context.WithoutCancel(consumer.Context()))
		tx, relation := familyScopedHoldAccounts(t, f)
		done := make(chan error, 1)
		go func() {
			if operation == "Handoff" {
				done <- proof.Handoff(consumer)
			} else {
				done <- proof.CheckRequest(consumer)
			}
		}()
		familyScopedWaitPending(t, f, relation)
		depart()
		if consumer.Context().Err() != nil {
			t.Fatal("canary did not preserve live WithoutCancel context")
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("late-departed fresh frame accepted")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("fresh frame departure failed to cancel/join actual query")
		}
		if proof.Context().Err() != nil {
			t.Fatal("late consumer departure retired original", operation)
		}
		var pending int
		if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&pending); err != nil || pending != 0 {
			t.Fatal("consumer returned before actual query cancel/drain", err, pending)
		}
		if err := tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		if proof.Check(t.Context()) != nil {
			t.Fatal("original failed after caller-only departure", operation)
		}
	}
	f.exec(t, "UPDATE users SET name='Changed current source profile' WHERE id='target'")
	if proof.CheckRequest(fresh) == nil || proof.Context().Err() == nil {
		t.Fatal("genuine current PG drift under still-live frame failed to retire original")
	}
}

func TestFamilyScopedOriginalSetupCancellationAndOneShotHandoff(t *testing.T) {
	for _, handoff := range []bool{false, true} {
		t.Run(fmt.Sprint(handoff), func(t *testing.T) {
			for _, departure := range []string{"original frame", "capture request"} {
				f := familyScopedPG(t, nil)
				r, release := familyScopedRequest(t, f, t.Context())
				defer release()
				wrappedContext, cancelCapture := context.WithCancel(context.WithoutCancel(r.Context()))
				defer cancelCapture()
				wrapped := r.WithContext(wrappedContext)
				proof := familyScopedCapture(t, f, wrapped)
				if handoff {
					fresh, stop := familyScopedRequest(t, f, t.Context())
					defer stop()
					if proof.Handoff(fresh) == nil {
						t.Fatal("fresh request adopted original setup ownership")
					}
					if proof.Handoff(wrapped) != nil || proof.Handoff(wrapped) == nil {
						t.Fatal("original handoff was not exactly once")
					}
				}
				if departure == "original frame" {
					release()
					if wrapped.Context().Err() != nil {
						t.Fatal("canary did not retain live WithoutCancel capture request")
					}
				} else {
					cancelCapture()
					if r.Context().Err() != nil {
						t.Fatal("capture request cancellation consumed independent original frame")
					}
				}
				if handoff {
					if proof.Check(context.Background()) != nil || proof.Context().Value(pgRequestFrameKey{}) != nil {
						t.Fatal("handoff retained setup cancellation or frame values", departure)
					}
				} else {
					select {
					case <-proof.Context().Done():
					case <-time.After(2 * time.Second):
						t.Fatal("setup link did not interrupt original lifetime", departure)
					}
					if proof.Check(t.Context()) == nil || proof.Handoff(wrapped) == nil {
						t.Fatal("departed setup retained or gained late handoff", departure)
					}
				}
			}
		})
	}
	// Force the real context stop races after genuine PG checks have completed.
	// Only the existing stop handles are wrapped; no authority or PG result is
	// substituted. Each prevented callback must be owned before later refusal.
	for _, failure := range []string{"frame callback started", "frame departs after successful stop", "request callback started"} {
		f := familyScopedPG(t, nil)
		r, release := familyScopedRequest(t, f, t.Context())
		defer release()
		requestContext, cancelCapture := context.WithCancel(context.WithoutCancel(r.Context()))
		defer cancelCapture()
		wrapped := r.WithContext(requestContext)
		proof := familyScopedCapture(t, f, wrapped)
		proof.state.mu.Lock()
		stopRequest := proof.state.setupStop
		stopFrame := proof.state.setupFrameStop
		if failure == "request callback started" {
			proof.state.setupStop = func() bool { cancelCapture(); return stopRequest() }
		} else {
			proof.state.setupFrameStop = func() bool {
				if failure == "frame departs after successful stop" {
					stopped := stopFrame()
					release()
					return stopped
				}
				release()
				return stopFrame()
			}
		}
		proof.state.mu.Unlock()
		if proof.Handoff(wrapped) == nil || proof.Context().Err() == nil {
			t.Fatal("departed setup acquired handoff after stops", failure)
		}
		if failure != "request callback started" {
			select {
			case <-proof.state.setupDone:
			default:
				t.Fatal("prevented incoming callback was not immediately owned", failure)
			}
		}
		if failure != "frame callback started" {
			select {
			case <-proof.state.setupFrameDone:
			default:
				t.Fatal("prevented frame callback was not immediately owned", failure)
			}
		}
		closed := make(chan struct{})
		go func() { proof.Close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("Close waited on prevented callback or failed to join running setup", failure)
		}
	}
}

func TestFamilyScopedOriginalSessionAndActorExpiryRemainFixed(t *testing.T) {
	for _, kind := range []string{"session", "account"} {
		t.Run(kind, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			expiry := time.Now().UTC().Add(750 * time.Millisecond).Truncate(time.Microsecond)
			if kind == "session" {
				f.session.ExpiresAt = expiry
				f.exec(t, "UPDATE sessions SET expires_at=$1 WHERE token=$2", expiry, f.session.Token)
			} else {
				f.exec(t, "UPDATE accounts SET expires_at=$1 WHERE id='actor'", expiry)
			}
			r, release := familyScopedRequest(t, f, t.Context())
			defer release()
			proof := familyScopedCapture(t, f, r)
			if !proof.ExpiresAt().Equal(expiry) || proof.Handoff(r) != nil {
				t.Fatal("original minimum expiry changed")
			}
			select {
			case <-proof.Context().Done():
			case <-time.After(3 * time.Second):
				t.Fatal("original expiry not enforced")
			}
			if proof.Check(t.Context()) == nil || !proof.ExpiresAt().Equal(expiry) {
				t.Fatal("expired original renewed")
			}
		})
	}
}

func familyScopedWaitPending(t *testing.T, f *ratingPGFixture, relation uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&n); err != nil {
			t.Fatal("observe exact owned relation wait", err)
		}
		if n > 0 {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal("test canceled")
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatal("owned current PG check did not reach relation lock")
}
func familyScopedHoldAccounts(t *testing.T, f *ratingPGFixture) (pgx.Tx, uint32) {
	t.Helper()
	var relation uint32
	if err := f.ds.pool.QueryRow(t.Context(), "SELECT 'accounts'::regclass::oid").Scan(&relation); err != nil {
		t.Fatal(err)
	}
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	})
	return tx, relation
}
func TestFamilyScopedCloseCancelsAndJoinsEveryActualHeldPGOperation(t *testing.T) {
	for _, kind := range []string{"Check", "CheckRequest", "Handoff"} {
		t.Run(kind, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			r, release := familyScopedRequest(t, f, t.Context())
			defer release()
			proof := familyScopedCapture(t, f, r)
			_, relation := familyScopedHoldAccounts(t, f)
			checked := make(chan error, 1)
			go func() {
				switch kind {
				case "Check":
					checked <- proof.Check(context.Background())
				case "CheckRequest":
					checked <- proof.CheckRequest(r)
				case "Handoff":
					checked <- proof.Handoff(r)
				}
			}()
			familyScopedWaitPending(t, f, relation)
			closed := make(chan struct{})
			go func() { proof.Close(); close(closed) }()
			select {
			case <-closed:
			case <-time.After(3 * time.Second):
				t.Fatal("Close failed to join actual canceled PG check with lock still held")
			}
			if err := <-checked; err == nil {
				t.Fatal("held query succeeded after original Close")
			}
			var pending int
			if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&pending); err != nil || pending != 0 {
				t.Fatal("owner refunded before actual server cancel/drain", err, pending)
			}
			familyScopedAdmission.Lock()
			_, retained := familyScopedAdmission.owners[proof]
			familyScopedAdmission.Unlock()
			if retained {
				t.Fatal("joined closed proof retained admission")
			}
		})
	}
}

func TestFamilyScopedPerProofAdmissionPrecedesBlockedPGAndCallerCancelRefunds(t *testing.T) {
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	tx, relation := familyScopedHoldAccounts(t, f)
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if proof.Check(caller) == nil {
				t.Error("blocked canceled check accepted")
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		proof.state.mu.Lock()
		n := proof.state.operations
		proof.state.mu.Unlock()
		if n == 8 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("eight operations did not reserve before PG")
		}
		time.Sleep(time.Millisecond)
	}
	familyScopedWaitPending(t, f, relation)
	if err := proof.Check(t.Context()); err == nil {
		t.Fatal("ninth operation bypassed pre-query bound")
	}
	cancel()
	wait.Wait()
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if proof.Check(t.Context()) != nil {
		t.Fatal("caller-only cancellation retired original or leaked operations")
	}
}

type familyScopedTraceKey struct{}
type familyScopedDelayedEnd struct {
	entered chan struct{}
	deliver chan struct{}
	armed   atomic.Bool
}

func (g *familyScopedDelayedEnd) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return ctx
}
func (g *familyScopedDelayedEnd) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(familyScopedTraceKey{}) == true && g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.deliver
	}
}
func TestFamilyScopedCloseRetainsAdmissionUntilActualQueryEndDelivery(t *testing.T) {
	gate := &familyScopedDelayedEnd{entered: make(chan struct{}), deliver: make(chan struct{})}
	var deliver sync.Once
	unblock := func() { deliver.Do(func() { close(gate.deliver) }) }
	defer unblock()
	f := familyScopedPG(t, gate)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	gate.armed.Store(true)
	checked := make(chan error, 1)
	go func() { checked <- proof.Check(context.WithValue(context.Background(), familyScopedTraceKey{}, true)) }()
	select {
	case <-gate.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual PG query-end delivery barrier not reached")
	}
	closed := make(chan struct{})
	go func() { proof.Close(); close(closed) }()
	select {
	case <-proof.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("Close did not signal original")
	}
	select {
	case <-closed:
		t.Fatal("Close returned before actual query-end callback")
	default:
	}
	familyScopedAdmission.Lock()
	_, retained := familyScopedAdmission.owners[proof]
	familyScopedAdmission.Unlock()
	if !retained {
		t.Fatal("original capacity refunded through blocked query callback")
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close failed after real query completion")
	}
	if <-checked == nil {
		t.Fatal("late successful query delivery published closed proof")
	}
}
func TestFamilyScopedCurrentFailureAndConsumerScopeConflictNeverRecapture(t *testing.T) {
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	f.exec(t, "UPDATE sessions SET scope='stream' WHERE token=$1", f.session.Token)
	if proof.Check(t.Context()) == nil {
		t.Fatal("late scope change adopted a different authority")
	}
	if !errors.Is(proof.CheckRequest(r), ErrFamilyMediaChanged) {
		t.Fatal("changed current session did not remain denied")
	}
}

func TestFamilyScopedCaptureCancelJoinsActualPendingFrameQueryBeforeRefund(t *testing.T) {
	f := familyScopedPG(t, nil)
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, release := familyScopedRequest(t, f, caller)
	defer release()
	_, relation := familyScopedHoldAccounts(t, f)
	familyScopedAdmission.Lock()
	before := len(familyScopedAdmission.owners)
	familyScopedAdmission.Unlock()
	type result struct {
		proof *FamilyScopedApplicability
		err   error
	}
	done := make(chan result, 1)
	go func() { proof, err := f.ds.CaptureInactiveJellyfinApplicability(r); done <- result{proof, err} }()
	familyScopedWaitPending(t, f, relation)
	familyScopedAdmission.Lock()
	during := len(familyScopedAdmission.owners)
	familyScopedAdmission.Unlock()
	if during != before+1 {
		t.Fatal("pending original capture was not admitted before frame SQL")
	}
	cancel()
	select {
	case got := <-done:
		if got.proof != nil || got.err == nil {
			if got.proof != nil {
				got.proof.Close()
			}
			t.Fatal("canceled capture published proof")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("capture did not join pending original query/callback before return")
	}
	familyScopedAdmission.Lock()
	after := len(familyScopedAdmission.owners)
	familyScopedAdmission.Unlock()
	var pending int
	if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&pending); err != nil || pending != 0 || after != before {
		t.Fatal("capture returned/refunded before actual query drain", err, pending)
	}
}
func TestFamilyScopedRealAccountAdmissionIsRetainedUntilExplicitClose(t *testing.T) {
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proofs := make([]*FamilyScopedApplicability, 0, 32)
	defer func() {
		for _, proof := range proofs {
			proof.Close()
		}
	}()
	for range 32 {
		proof, err := f.ds.CaptureInactiveJellyfinApplicability(r)
		if err != nil {
			t.Fatal("actual original capacity reservation", err)
		}
		proofs = append(proofs, proof)
	}
	if proof, err := f.ds.CaptureInactiveJellyfinApplicability(r); proof != nil || !errors.Is(err, ErrFamilyMediaBusy) {
		if proof != nil {
			proof.Close()
		}
		t.Fatal("account admission limit not enforced before PG")
	}
	proofs[0].Close()
	proof, err := f.ds.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		t.Fatal("actual closed owner did not refund capacity", err)
	}
	defer proof.Close()
}

func TestFamilyScopedUnavailablePolicyReadNeverUsesPriorInactiveSnapshot(t *testing.T) {
	f := familyScopedPG(t, nil)
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	f.exec(t, "ALTER TABLE regional_profile_policies RENAME TO unavailable_scoped_policies")
	if proof.CheckRequest(r) == nil || proof.Context().Err() == nil {
		t.Fatal("unavailable current data fell back to prior inactive snapshot")
	}
	f.exec(t, "ALTER TABLE unavailable_scoped_policies RENAME TO regional_profile_policies")
	if proof.Check(t.Context()) == nil {
		t.Fatal("later availability revived retired original")
	}
	fresh, err := f.ds.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		t.Fatal("healthy current new capture did not recover", err)
	}
	defer fresh.Close()
}
func TestFamilyScopedMasterOwnedProfileRemainsInactiveWithoutScopePromotion(t *testing.T) {
	f := familyScopedPG(t, nil)
	f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != models.SessionScopeJellyfin || original.ScopeResource != "target" || original.IsMaster || proof.Check(t.Context()) != nil {
		t.Fatal("exact owned scoped profile denied or promoted to master full session")
	}
}

func TestFamilyScopedCloseJoinsActualClosedConnectionCleanupDone(t *testing.T) {
	f := familyScopedPG(t, nil)
	cfg := f.cfg.Copy()
	cfg.MaxConns = 1
	dial := cfg.ConnConfig.DialFunc
	var first atomic.Bool
	var actualMu sync.Mutex
	var actual *queryLifecycleGatedConn
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			gated := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			actualMu.Lock()
			actual = gated
			actualMu.Unlock()
			return gated, nil
		}
		// Real driver cancellation sockets remain ungated.
		return conn, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.ds = &DataStore{pool: pool}
	r, release := familyScopedRequest(t, f, t.Context())
	defer release()
	proof := familyScopedCapture(t, f, r)
	owned, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	pg := owned.Conn().PgConn()
	owned.Release()
	actualMu.Lock()
	gate := actual
	actualMu.Unlock()
	if gate == nil {
		t.Fatal("actual primary PG connection missing")
	}
	// This deferred release is installed before the induced failure or goroutine
	// starts, so failure diagnostics cannot leave the real physical Close held.
	defer gate.release()
	gate.armed.Store(true)
	checked := make(chan error, 1)
	go func() { checked <- proof.Check(context.Background()) }()
	select {
	case <-gate.closeEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual failed query did not reach physical Close")
	}
	if !pg.IsClosed() {
		t.Fatal("physical-cleanup witness was not the actual closed PG connection")
	}
	select {
	case <-pg.CleanupDone():
		t.Fatal("physical driver cleanup completed before its real net Close")
	default:
	}
	closed := make(chan struct{})
	go func() { proof.Close(); close(closed) }()
	select {
	case <-proof.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("proof Close did not signal original")
	}
	select {
	case <-closed:
		t.Fatal("proof Close/refund overtook actual CleanupDone")
	default:
	}
	familyScopedAdmission.Lock()
	_, retained := familyScopedAdmission.owners[proof]
	familyScopedAdmission.Unlock()
	if !retained {
		t.Fatal("capacity refunded before physical query cleanup")
	}
	gate.release()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("proof Close failed after real physical cleanup completed")
	}
	if err := <-checked; err == nil {
		t.Fatal("failed real connection published applicability")
	}
	select {
	case <-pg.CleanupDone():
	default:
		t.Fatal("proof refunded without joining actual driver CleanupDone")
	}
}

func TestFamilyScopedBoundedHouseholdReceiptsAndActualWireOverflow(t *testing.T) {
	for _, kind := range []string{"ordinary equality", "257 rows", "oversized identifier", "wire bytes"} {
		t.Run(kind, func(t *testing.T) {
			f := familyScopedPG(t, nil)
			switch kind {
			case "257 rows":
				f.exec(t, `INSERT INTO users(id,account_id,name) SELECT 'bounded-row-'||g,'actor','Synthetic member' FROM generate_series(1,255) g`)
			case "oversized identifier":
				f.exec(t, `INSERT INTO users(id,account_id,name) VALUES(repeat('w',257),'actor','Synthetic oversized member')`)
			case "wire bytes":
				f.exec(t, `INSERT INTO users(id,account_id,name) SELECT 'wire-'||lpad(g::text,3,'0')||'-'||repeat('x',247),'actor','Synthetic wide bounded member' FROM generate_series(1,240) g`)
			}
			rows, err := f.ds.pool.Query(t.Context(), familyScopedHouseholdSQL, "actor")
			if err != nil {
				t.Fatal("execute actual bounded receipt query", err)
			}
			if !rows.Next() {
				rows.Close()
				t.Fatal("actual account receipt row missing")
			}
			descriptions := rows.FieldDescriptions()
			values := rows.RawValues()
			if len(descriptions) != 1 || descriptions[0].DataTypeOID != pgtype.TextOID || descriptions[0].Format != pgx.TextFormatCode || len(values) != 1 {
				rows.Close()
				t.Fatal("bounded household was not one actual text-format wire column")
			}
			field := values[0]
			isNull := field == nil
			wireBytes := len(field)
			if wireBytes > familyScopedHouseholdBytes {
				rows.Close()
				t.Fatal("actual DataRow value exceeded fixed wire budget", wireBytes)
			}
			if kind == "ordinary equality" && isNull || kind != "ordinary equality" && !isNull {
				rows.Close()
				t.Fatal("actual wire overflow did not fail closed", kind, wireBytes)
			}
			if rows.Next() {
				rows.Close()
				t.Fatal("bounded receipt query returned more than one account row")
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			rows.Close()
			bounded, err := f.ds.readFamilyScopedHousehold(t.Context(), "actor")
			if kind == "ordinary equality" {
				legacy, legacyErr := f.ds.CurrentRegionalAccount(t.Context(), "actor")
				if err != nil || legacyErr != nil || bounded != legacy {
					t.Fatal("small complete canonical household receipt/fence changed", err, legacyErr)
				}
			} else {
				if err == nil {
					t.Fatal("overflow household acquired a receipt", kind)
				}
				if kind == "wire bytes" {
					var candidate int
					if err := f.ds.pool.QueryRow(t.Context(), familyScopedHouseholdCTE+`SELECT octet_length(convert_to(body::text,'UTF8')) FROM bounded_receipt`, "actor").Scan(&candidate); err != nil || candidate <= familyScopedHouseholdBytes {
						t.Fatal("wire canary did not exceed actual PostgreSQL UTF8 serialization budget", err, candidate)
					}
					var count, width int
					if err := f.ds.pool.QueryRow(t.Context(), `SELECT count(*),max(octet_length(convert_to(id,'UTF8'))) FROM users WHERE account_id='actor'`).Scan(&count, &width); err != nil || count > 256 || width > 256 {
						t.Fatal("wire canary accidentally depended on row/identifier overflow", err)
					}
				}
				request, release := familyScopedRequest(t, f, t.Context())
				defer release()
				if proof, err := f.ds.CaptureInactiveJellyfinApplicability(request); proof != nil || err == nil {
					if proof != nil {
						proof.Close()
					}
					t.Fatal("actual overflow household minted scoped applicability", kind)
				}
			}
		})
	}
}
