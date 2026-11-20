package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func familyMediaPG(t *testing.T) *ratingPGFixture {
	t.Helper()
	f := regionalPG(t)
	f.exec(t, "UPDATE users SET pin_hash='',is_kids_profile=false")
	return f
}
func familyMediaRequest(t *testing.T, f *ratingPGFixture, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal("actual PG original frame", err)
	}
	return checked, release
}
func familyMediaCapture(t *testing.T, f *ratingPGFixture, r *http.Request, target string, tokens []string) *FamilyMediaOwner {
	t.Helper()
	owner, err := f.ds.CaptureFamilyMediaOwner(r, target, tokens)
	if err != nil {
		t.Fatal("capture original family owner", err)
	}
	t.Cleanup(owner.Close)
	return owner
}
func familyMediaUnlock(t *testing.T, f *ratingPGFixture, ttl time.Duration) (*http.Request, context.CancelFunc, string, time.Time) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("1357"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, "UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='target'", string(hash))
	request, release := familyMediaRequest(t, f, t.Context())
	issuer := NewPGPinIssuer(f.ds)
	intent, err := issuer.Prepare(request, "target", ttl)
	if err != nil {
		release()
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	proof, err := issuer.Verify(intent, "1357")
	if err != nil {
		release()
		t.Fatal(err)
	}
	receipt, err := issuer.Commit(intent, proof)
	if err != nil {
		release()
		t.Fatal(err)
	}
	t.Cleanup(receipt.Close)
	token, expiry, err := issuer.Release(request, receipt)
	if err != nil || token == "" {
		release()
		t.Fatal("actual issued PG PIN release", err)
	}
	return request, release, token, expiry
}

func TestFamilyMediaUnverifiedImportedAndCopiedOwnerCannotAuthorize(t *testing.T) {
	var zero FamilyMediaOwner
	if json.Unmarshal([]byte("{}"), &zero) == nil || zero.Check(t.Context()) == nil || zero.Handoff(nil) == nil || zero.Mode() != FamilyMediaUnknown || zero.Context().Err() == nil || !zero.ExpiresAt().IsZero() {
		t.Fatal("imported/zero authority became usable")
	}
	f := familyMediaPG(t)
	request, release := familyMediaRequest(t, f, t.Context())
	defer release()
	if owner, e := f.ds.CaptureFamilyMediaOwner(f.parentPGFixture.request(t.Context()), "", nil); owner != nil || e == nil {
		t.Fatal("raw auth keys replaced the actual original PG frame")
	}
	owner := familyMediaCapture(t, f, request, "target", nil)
	copy := *owner
	if copy.Check(t.Context()) == nil || copy.Mode() != FamilyMediaUnknown {
		t.Fatal("copied owner acquired authority")
	}
	copy.Close()
	if owner.Check(t.Context()) != nil {
		t.Fatal("denied copy retired original")
	}
	if raw, e := json.Marshal(owner); e != nil || string(raw) != "null" || fmt.Sprintf("%+v", owner) != "FamilyMediaOwner(opaque)" {
		t.Fatal("private owner serialized")
	}
	for _, value := range []any{owner, *owner, &copy, copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyMediaOwner(opaque)" {
				t.Fatal("pointer/value/copied formatting exposed original private authority")
			}
		}
		if raw, e := json.Marshal(value); e != nil || string(raw) != "null" {
			t.Fatal("pointer/value/copied JSON exposed original private authority")
		}
	}
}
func TestFamilyMediaInactiveOriginalFullSessionSupportsAbsentOrOwnedProfile(t *testing.T) {
	for _, target := range []string{"", "target"} {
		t.Run(fmt.Sprint("profile=", target), func(t *testing.T) {
			f := familyMediaPG(t)
			r, release := familyMediaRequest(t, f, t.Context())
			defer release()
			owner := familyMediaCapture(t, f, r, target, nil)
			if owner.Mode() != FamilyMediaInactive || owner.CheckRequest(r, target) != nil || !owner.ExpiresAt().Equal(f.session.ExpiresAt) {
				t.Fatal("healthy original inactive household denied or expiry changed")
			}
			if owner.CheckRequest(r, "unrelated") == nil {
				t.Fatal("owner retargeted another profile")
			}
		})
	}
}
func TestFamilyMediaActiveHouseholdRequiresExplicitUnlockedAdultEvenMaster(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprint(master), func(t *testing.T) {
			f := familyMediaPG(t)
			f.exec(t, "UPDATE accounts SET is_master=$1 WHERE id='actor'", master)
			f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second'")
			r, release := familyMediaRequest(t, f, t.Context())
			defer release()
			for _, tc := range []struct {
				target string
				want   error
			}{{"", ErrFamilyMediaProfileRequired}, {"target", ErrFamilyMediaPINRequired}, {"own-second", ErrFamilyMediaChildDenied}} {
				owner, e := f.ds.CaptureFamilyMediaOwner(r, tc.target, nil)
				if owner != nil || !errors.Is(e, tc.want) {
					if owner != nil {
						owner.Close()
					}
					t.Fatal("active family gained an implicit adult/child/PIN waiver", tc.target, e)
				}
			}
		})
	}
}
func TestFamilyMediaActualPINOwnerRetainsOnlyOriginalGrantAndExpiry(t *testing.T) {
	f := familyMediaPG(t)
	f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second'")
	r, release, token, expiry := familyMediaUnlock(t, f, time.Minute)
	defer release()
	owner := familyMediaCapture(t, f, r, "target", []string{token})
	if owner.Mode() != FamilyMediaActiveAdult || !owner.ExpiresAt().Equal(expiry) || owner.Check(t.Context()) != nil {
		t.Fatal("actual PG adult unlock did not retain original expiry")
	}
	if err := revokeProfileUnlocks(t.Context(), f.ds.pool, "session", profileUnlockDigest(f.session.Token)); err != nil {
		t.Fatal(err)
	}
	if owner.Check(t.Context()) == nil || owner.Mode() != FamilyMediaUnknown {
		t.Fatal("original PIN revocation did not retire active worker")
	}
	// A later real PIN verification may create a new grant, never repair owner.
	fresh, stop, newToken, _ := familyMediaUnlock(t, f, time.Minute)
	defer stop()
	if newToken == token || owner.CheckRequest(fresh, "target") == nil {
		t.Fatal("later unlock renewed an old owner")
	}
}
func TestFamilyMediaPhysicalRowsAndHouseholdABANeverReviveOwner(t *testing.T) {
	for _, change := range []string{"profile ABA", "session ABA", "actor ABA", "profile epoch", "session epoch", "new child ABA", "other household fence"} {
		t.Run(change, func(t *testing.T) {
			f := familyMediaPG(t)
			r, release := familyMediaRequest(t, f, t.Context())
			defer release()
			owner := familyMediaCapture(t, f, r, "target", nil)
			switch change {
			case "profile ABA":
				f.exec(t, "UPDATE users SET name='temporary' WHERE id='target';UPDATE users SET name='Synthetic Target' WHERE id='target'")
			case "session ABA":
				f.exec(t, "UPDATE sessions SET user_agent='temporary' WHERE token=$1", f.session.Token)
				f.exec(t, "UPDATE sessions SET user_agent='' WHERE token=$1", f.session.Token)
			case "actor ABA":
				f.exec(t, "UPDATE accounts SET username='temporary' WHERE id='actor';UPDATE accounts SET username='Synthetic Parent' WHERE id='actor'")
			case "profile epoch":
				f.exec(t, "INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','target',1)")
			case "session epoch":
				f.exec(t, "INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('session',$1,1)", profileUnlockDigest(f.session.Token))
			case "new child ABA":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second';UPDATE users SET is_kids_profile=false WHERE id='own-second'")
			case "other household fence":
				f.exec(t, "UPDATE users SET name=name WHERE id='unrelated'")
			}
			if owner.Check(t.Context()) == nil {
				t.Fatal("old owner adopted changed/restored physical authority")
			}
		})
	}
}
func TestFamilyMediaOriginalRequestCancellationAndSuccessfulHandoffAreDistinct(t *testing.T) {
	for _, handoff := range []bool{false, true} {
		t.Run(fmt.Sprint(handoff), func(t *testing.T) {
			f := familyMediaPG(t)
			r, release := familyMediaRequest(t, f, t.Context())
			defer release()
			owner := familyMediaCapture(t, f, r, "target", nil)
			if handoff && owner.Handoff(r) != nil {
				t.Fatal("actual original handoff rejected")
			}
			release()
			if handoff {
				if owner.Check(t.Context()) != nil {
					t.Fatal("completed setup canceled owned worker")
				}
				if owner.Handoff(r) == nil {
					t.Fatal("worker handoff replayed")
				}
			} else {
				select {
				case <-owner.Context().Done():
				case <-time.After(time.Second):
					t.Fatal("setup cancellation did not reach owner")
				}
				if owner.Check(t.Context()) == nil || owner.Handoff(r) == nil {
					t.Fatal("canceled original setup adopted a worker")
				}
			}
			done := make(chan struct{})
			go func() { owner.Close(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("actual handoff/cancel retirement did not join callback")
			}
		})
	}
}
func TestFamilyMediaOriginalExpiryCannotBeRenewedByHandoff(t *testing.T) {
	f := familyMediaPG(t)
	expiry := time.Now().UTC().Add(2 * time.Second).Truncate(time.Microsecond)
	f.exec(t, "UPDATE sessions SET expires_at=$1 WHERE token=$2", expiry, f.session.Token)
	f.session.ExpiresAt = expiry
	r, release := familyMediaRequest(t, f, t.Context())
	defer release()
	owner := familyMediaCapture(t, f, r, "target", nil)
	if !owner.ExpiresAt().Equal(expiry) || owner.Handoff(r) != nil {
		t.Fatal("original actual expiry was not retained")
	}
	release()
	select {
	case <-owner.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("original deadline did not stop handoff")
	}
	if owner.Check(t.Context()) == nil {
		t.Fatal("expired original owner remained usable")
	}
	familyMediaAdmission.Lock()
	_, charged := familyMediaAdmission.owners[owner]
	familyMediaAdmission.Unlock()
	if !charged {
		t.Fatal("expiry refunded runtime owner before explicit joined cleanup")
	}
	owner.Close()
}
func TestFamilyMediaRejectedHandoffOwnsSuccessfulCallbackStopBeforeCancellation(t *testing.T) {
	f := familyMediaPG(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, release := familyMediaRequest(t, f, ctx)
	defer release()
	owner := familyMediaCapture(t, f, r, "target", nil)
	owner.mu.Lock()
	originalStop := owner.setupStop
	// The owner and callback are real. Control only the exact race window after
	// the real AfterFunc has been stopped, before handoff checks cancellation.
	owner.setupStop = func() bool { stopped := originalStop(); cancel(); return stopped }
	owner.mu.Unlock()
	if owner.Handoff(r) == nil {
		t.Fatal("cancelled setup completed handoff")
	}
	done := make(chan struct{})
	go func() { owner.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("rejected handoff waited for a callback it had prevented")
	}
}
func TestFamilyMediaFreshFramesCannotLoanOriginalSetupOrChangeClient(t *testing.T) {
	f := familyMediaPG(t)
	r, release := familyMediaRequest(t, f, t.Context())
	defer release()
	owner := familyMediaCapture(t, f, r, "target", nil)
	other, stop := familyMediaRequest(t, f, t.Context())
	defer stop()
	if owner.Handoff(other) == nil {
		t.Fatal("another frame adopted original setup")
	}
	if owner.CheckRequest(other, "target") != nil {
		t.Fatal("unchanged fresh exact session request denied")
	}
	other.Header.Set("X-Client-ID", "foreign-client")
	if owner.CheckRequest(other, "target") == nil {
		t.Fatal("foreign client borrowed owner")
	}
	other.Header.Set("X-Client-ID", f.session.ClientID)
	other.Header.Add("X-Client-ID", f.session.ClientID)
	if owner.CheckRequest(other, "target") == nil {
		t.Fatal("ambiguous client borrowed owner")
	}
	if owner.Check(t.Context()) != nil {
		t.Fatal("foreign request denial canceled original owner")
	}
}
func TestFamilyMediaScopedSessionsRemainExplicitlyUnsupported(t *testing.T) {
	f := familyMediaPG(t)
	f.exec(t, "UPDATE sessions SET scope='stream',scope_resource='exact-private-source' WHERE token=$1", f.session.Token)
	r, release := familyMediaRequest(t, f, t.Context())
	defer release()
	if owner, e := f.ds.CaptureFamilyMediaOwner(r, "target", nil); owner != nil || !errors.Is(e, ErrFamilyMediaScopedUnsupported) {
		if owner != nil {
			owner.Close()
		}
		t.Fatal("scoped session became a full worker owner", e)
	}
}
func TestFamilyMediaAdmissionPrecedesBlockingCaptureAndCancellationRefunds(t *testing.T) {
	f := familyMediaPG(t)
	r, release := familyMediaRequest(t, f, t.Context())
	defer release()
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	familyMediaAdmission.Lock()
	before := len(familyMediaAdmission.owners)
	familyMediaAdmission.Unlock()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		owner, e := f.ds.CaptureFamilyMediaOwner(r.WithContext(ctx), "target", nil)
		if owner != nil {
			owner.Close()
		}
		done <- e
	}()
	seen := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		familyMediaAdmission.Lock()
		seen = len(familyMediaAdmission.owners) == before+1
		familyMediaAdmission.Unlock()
		if seen {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !seen {
		cancel()
		t.Fatal("pending SQL capture was not charged before blocking")
	}
	cancel()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("cancelled blocked capture succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked capture did not join cancellation")
	}
	familyMediaAdmission.Lock()
	after := len(familyMediaAdmission.owners)
	familyMediaAdmission.Unlock()
	if after != before {
		t.Fatal("failed acquisition leaked owner admission")
	}
}
func TestFamilyMediaPerAccountCapacityIncludesOwnersUntilClose(t *testing.T) {
	f := familyMediaPG(t)
	r, release := familyMediaRequest(t, f, t.Context())
	defer release()
	owners := make([]*FamilyMediaOwner, 0, 32)
	for range 32 {
		owners = append(owners, familyMediaCapture(t, f, r, "", nil))
	}
	if owner, e := f.ds.CaptureFamilyMediaOwner(r, "", nil); owner != nil || !errors.Is(e, ErrFamilyMediaBusy) {
		if owner != nil {
			owner.Close()
		}
		t.Fatal("account owner admission exceeded bound", e)
	}
	owners[0].Close()
	next := familyMediaCapture(t, f, r, "", nil)
	if next.Check(t.Context()) != nil {
		t.Fatal("actual close did not refund owner capacity")
	}
}
func TestFamilyMediaCanceledBlockedConsumerCannotRetireHandedOffOriginal(t *testing.T) {
	f := familyMediaPG(t)
	r, release := familyMediaRequest(t, f, t.Context())
	defer release()
	owner := familyMediaCapture(t, f, r, "target", nil)
	if owner.Handoff(r) != nil {
		t.Fatal("actual original handoff rejected")
	}
	release()
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rollback := func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = tx.Rollback(cleanup)
	}
	defer rollback()
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- owner.Check(ctx) }()
	// Prove the real owner check entered PostgreSQL and is blocked in this
	// fixture's own connection, rather than canceling it before Check starts.
	waiting := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if err = f.admin.QueryRow(t.Context(), "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM accounts a WHERE a.id=%')", f.cfg.ConnConfig.RuntimeParams["application_name"]).Scan(&waiting); err != nil {
			cancel()
			t.Fatal("observe own synthetic blocked query", err)
		}
		if waiting {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled blocked consumer authorized")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("consumer query cancellation did not join")
	}
	rollback()
	if !waiting {
		t.Fatal("controlled real PG consumer never reached held authority query")
	}
	if owner.Context().Err() != nil || owner.Mode() != FamilyMediaInactive || owner.Check(t.Context()) != nil {
		t.Fatal("canceled fresh consumer retired a valid original worker")
	}
}
func TestFamilyMediaCancellationCallbacksJoinActualCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	stop := familyMediaAfterFunc(ctx, func() { close(entered); <-release })
	cancel()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cancel callback did not start")
	}
	joined := make(chan struct{})
	go func() { stop(); close(joined) }()
	select {
	case <-joined:
		unblock()
		t.Fatal("running cancel callback was not joined")
	default:
	}
	unblock()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("completed callback join did not finish")
	}
	stop()
}
