package profileunlock

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
)

func familyScopedFacadePG(t *testing.T) (*issuerFacadeFixture, *PGProfilePins) {
	t.Helper()
	f := issuerFacadePG(t)
	f.session.Scope = models.SessionScopeJellyfin
	f.session.ScopeResource = "target"
	if _, err := f.ds.Pool().Exec(t.Context(), "UPDATE sessions SET scope=$1,scope_resource=$2 WHERE token=$3", f.session.Scope, f.session.ScopeResource, f.session.Token); err != nil {
		t.Fatal("seed actual existing Jellyfin scope", err)
	}
	return f, NewPGProfilePins(f.ds)
}
func familyScopedFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(ctx)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal("actual scoped facade authentication", err)
	}
	return checked, release
}
func TestFamilyScopedFacadeZeroAndImportedValuesCannotAcquire(t *testing.T) {
	var pins PGProfilePins
	if proof, err := pins.CaptureInactiveJellyfinApplicability(nil); proof != nil || err == nil {
		t.Fatal("unavailable facade minted scoped proof")
	}
	var zero FamilyScopedApplicability
	if json.Unmarshal([]byte("{}"), &zero) == nil || zero.Check(t.Context()) == nil || zero.CheckRequest(nil) == nil || zero.Handoff(nil) == nil || !zero.ExpiresAt().IsZero() || zero.Context().Err() == nil {
		t.Fatal("imported facade became usable")
	}
	if _, ok := any(&zero).(interface {
		CaptureLocalSource(context.Context, string) (*FamilyLocalSource, error)
	}); ok {
		t.Fatal("scoped applicability exposes full source acquisition")
	}
}
func TestFamilyScopedFacadeActualStoreIdentityOpaqueCopyAndNoAuthContext(t *testing.T) {
	f, pins := familyScopedFacadePG(t)
	r, release := familyScopedFacadeRequest(t, f, pins, t.Context())
	defer release()
	if proof, err := pins.CaptureInactiveJellyfinApplicability(f.request(t.Context())); proof != nil || err == nil {
		t.Fatal("raw session decoration replaced actual PG frame")
	}
	copiedPins := *pins
	if proof, err := copiedPins.CaptureInactiveJellyfinApplicability(r); proof != nil || err == nil {
		t.Fatal("copied facade minted scoped proof")
	}
	if proof, err := NewPGProfilePins(&datastore.DataStore{}).CaptureInactiveJellyfinApplicability(r); proof != nil || err == nil {
		t.Fatal("unavailable store accepted another frame")
	}
	proof, err := pins.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if proof.Check(t.Context()) != nil || proof.CheckRequest(r) != nil {
		t.Fatal("actual inactive original scoped request denied")
	}
	for _, key := range []any{auth.ContextKeySession, auth.ContextKeyAccountID, auth.ContextKeyIsMaster} {
		if proof.Context().Value(key) != nil {
			t.Fatal("exported lifetime context leaked auth values")
		}
	}
	copy := *proof
	copy.Close()
	if copy.Check(t.Context()) == nil || proof.Check(t.Context()) != nil {
		t.Fatal("copy acquired or retired original")
	}
	for _, value := range []any{proof, *proof, &copy, copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyScopedApplicability(opaque)" {
				t.Fatal("facade formatted original private fields")
			}
		}
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("facade serialized original fields")
		}
	}
}
func TestFamilyScopedFacadeForeignActualStoreAndForeignMasterProfileDeny(t *testing.T) {
	f, pins := familyScopedFacadePG(t)
	other, foreign := familyScopedFacadePG(t)
	r, release := familyScopedFacadeRequest(t, f, pins, t.Context())
	defer release()
	if proof, err := foreign.CaptureInactiveJellyfinApplicability(r); proof != nil || err == nil {
		t.Fatal("different actual PG store borrowed original scoped frame")
	}
	if !foreign.FamilyMediaReady(other.ds) {
		t.Fatal("foreign actual fixture was not genuine")
	}
	if _, err := f.ds.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=true WHERE id='actor';UPDATE users SET account_id='other' WHERE id='target'"); err != nil {
		t.Fatal(err)
	}
	current, stop := familyScopedFacadeRequest(t, f, pins, t.Context())
	defer stop()
	if proof, err := pins.CaptureInactiveJellyfinApplicability(current); proof != nil || err == nil {
		t.Fatal("master scoped session widened existing exact bridge profile membership")
	}
}
func TestFamilyScopedFacadeHandoffAndFreshConsumerKeepOriginalLifetime(t *testing.T) {
	f, pins := familyScopedFacadePG(t)
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, release := familyScopedFacadeRequest(t, f, pins, caller)
	defer release()
	proof, err := pins.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	expiry := proof.ExpiresAt()
	fresh, stop := familyScopedFacadeRequest(t, f, pins, t.Context())
	defer stop()
	if proof.Handoff(fresh) == nil {
		t.Fatal("fresh request replaced original handoff frame")
	}
	if proof.Handoff(r) != nil || proof.Handoff(r) == nil {
		t.Fatal("original facade handoff not exactly once")
	}
	cancel()
	release()
	if proof.Check(t.Context()) != nil || proof.CheckRequest(fresh) != nil || !proof.ExpiresAt().Equal(expiry) {
		t.Fatal("original setup cancel/consumer recaptured or extended original")
	}
	if _, err := f.ds.Pool().Exec(t.Context(), "UPDATE users SET pin_hash=pin_hash WHERE id='target'"); err != nil {
		t.Fatal(err)
	}
	if proof.Check(t.Context()) == nil {
		t.Fatal("physical original profile change did not retire scoped proof")
	}
}
func TestFamilyScopedFacadeActiveHouseholdAndChildStayDenied(t *testing.T) {
	for _, kind := range []string{"child", "sibling", "disabled child policy"} {
		t.Run(kind, func(t *testing.T) {
			f, pins := familyScopedFacadePG(t)
			if kind == "sibling" {
				if _, err := f.ds.Pool().Exec(t.Context(), "INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('sibling','actor','Synthetic sibling',true)"); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.ds.Pool().Exec(t.Context(), "UPDATE users SET is_kids_profile=true WHERE id='target'"); err != nil {
					t.Fatal(err)
				}
			}
			// There is no opt-out or policy boolean on the new API. An absent regional
			// policy (the disabled/unconfigured case) cannot erase the stored child flag.
			r, release := familyScopedFacadeRequest(t, f, pins, t.Context())
			defer release()
			if proof, err := pins.CaptureInactiveJellyfinApplicability(r); proof != nil || err == nil {
				t.Fatal("active/child household acquired inactive scoped proof", kind)
			}
		})
	}
}
func TestFamilyScopedFacadeOriginalExpiryCannotBeExtendedByFreshRequest(t *testing.T) {
	f, pins := familyScopedFacadePG(t)
	expiry := time.Now().UTC().Add(750 * time.Millisecond).Truncate(time.Microsecond)
	f.session.ExpiresAt = expiry
	if _, err := f.ds.Pool().Exec(t.Context(), "UPDATE sessions SET expires_at=$1 WHERE token=$2", expiry, f.session.Token); err != nil {
		t.Fatal(err)
	}
	r, release := familyScopedFacadeRequest(t, f, pins, t.Context())
	defer release()
	proof, err := pins.CaptureInactiveJellyfinApplicability(r)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if proof.Handoff(r) != nil || !proof.ExpiresAt().Equal(expiry) {
		t.Fatal("facade lost original expiry")
	}
	select {
	case <-proof.Context().Done():
	case <-time.After(3 * time.Second):
		t.Fatal("facade expiry did not cancel lifetime")
	}
	if proof.Check(t.Context()) == nil {
		t.Fatal("expired original remained applicable")
	}
}
