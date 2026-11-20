package profileunlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"novastream/internal/datastore"
)

func TestFamilyMediaFacadeUnavailableAndOpaqueValuesCannotMintOwners(t *testing.T) {
	var pins PGProfilePins
	if pins.FamilyMediaReady(nil) {
		t.Fatal("zero PG facade reported ready")
	}
	if owner, e := pins.CaptureFamilyMediaOwner(nil, "target"); owner != nil || e == nil {
		t.Fatal("zero facade minted owner")
	}
	var owner FamilyMediaOwner
	if json.Unmarshal([]byte("{}"), &owner) == nil || owner.Check(t.Context()) == nil || owner.CheckRequest(nil, "target") == nil || owner.Handoff(nil) == nil || owner.Mode() != FamilyMediaUnknown || owner.Context().Err() == nil || !owner.ExpiresAt().IsZero() {
		t.Fatal("imported owner became usable")
	}
	if raw, e := json.Marshal(&owner); e != nil || string(raw) != "null" || fmt.Sprintf("%+v", &owner) != "FamilyMediaOwner(opaque)" {
		t.Fatal("opaque facade exposed authority")
	}
}
func TestFamilyMediaFacadeRequiresItsExactActualPGStoreAndOriginalFrame(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	if !pins.FamilyMediaReady(f.ds) || pins.FamilyMediaReady(&datastore.DataStore{}) {
		t.Fatal("facade readiness did not require its exact current store")
	}
	r := publicFacadeRequest(t, f, pins)
	if owner, e := pins.CaptureFamilyMediaOwner(f.request(t.Context()), "target"); owner != nil || e == nil {
		t.Fatal("raw auth context replaced genuine original PG frame")
	}
	owner, e := pins.CaptureFamilyMediaOwner(r, "target")
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	if owner.Mode() != FamilyMediaInactive || owner.Check(t.Context()) != nil {
		t.Fatal("known inactive selected adult lost compatibility")
	}
	copy := *owner
	copy.Close()
	if copy.Check(t.Context()) == nil || owner.Check(t.Context()) != nil {
		t.Fatal("copied facade gained authority or canceled original")
	}
	for _, value := range []any{owner, *owner, &copy, copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyMediaOwner(opaque)" {
				t.Fatal("pointer/value/copied facade formatting exposed authority")
			}
		}
		if raw, e := json.Marshal(value); e != nil || string(raw) != "null" {
			t.Fatal("pointer/value/copied facade JSON exposed authority")
		}
	}
}
func TestFamilyMediaFacadeFreshActiveRequestCannotBorrowWorkerPIN(t *testing.T) {
	f := issuerFacadePG(t)
	if _, e := f.ds.Pool().Exec(t.Context(), "INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic Child',true)"); e != nil {
		t.Fatal(e)
	}
	pins := NewPGProfilePins(f.ds)
	original := publicFacadeRequest(t, f, pins)
	token, expiry, e := pins.Verify(original, "target", "2580", time.Minute, f.users)
	if e != nil {
		t.Fatal(e)
	}
	original.Header.Set(Header, token)
	owner, e := pins.CaptureFamilyMediaOwner(original, "target")
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	if owner.Mode() != FamilyMediaActiveAdult || !owner.ExpiresAt().Equal(expiry) || owner.Handoff(original) != nil {
		t.Fatal("actual unlocked adult owner handoff failed")
	}
	fresh := publicFacadeRequest(t, f, pins)
	if e = owner.CheckRequest(fresh, "target"); !errors.Is(e, ErrFamilyMediaPINRequired) {
		t.Fatal("fresh consumer borrowed server worker PIN without presenting it", e)
	}
	if owner.Check(t.Context()) != nil {
		t.Fatal("denied fresh consumer canceled original worker")
	}
	fresh.Header.Set(Header, token)
	if owner.CheckRequest(fresh, "target") != nil {
		t.Fatal("actual fresh original PIN presentation denied")
	}
	if _, e = f.users.ClearPin("target"); e != nil {
		t.Fatal(e)
	}
	if owner.Check(t.Context()) == nil || owner.CheckRequest(fresh, "target") == nil {
		t.Fatal("PIN removal promoted an old active adult owner to NoPin")
	}
}
func TestFamilyMediaFacadeHandoffRetainsOriginalAfterActualRequestRelease(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	r, release, e := pins.Authenticate(f.request(t.Context()), f.session.Token, f.session.ClientID)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	owner, e := pins.CaptureFamilyMediaOwner(r, "target")
	if e != nil {
		t.Fatal(e)
	}
	if e = owner.Handoff(r); e != nil {
		owner.Close()
		t.Fatal(e)
	}
	release()
	if owner.Check(context.Background()) != nil {
		owner.Close()
		t.Fatal("released setup retired the original bounded worker")
	}
	done := make(chan struct{})
	go func() { owner.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actual successful handoff failed to join close")
	}
}
