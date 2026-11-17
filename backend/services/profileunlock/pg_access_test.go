package profileunlock

import (
	"errors"
	"net/url"
	"os"
	"testing"

	"novastream/internal/datastore"
	"novastream/services/users"
)

func accessFacadePG(t *testing.T) *issuerFacadeFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL PIN access fixture required")
	}
	t.Setenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL", raw)
	return issuerFacadePG(t)
}
func facadeAccessOutcome(t *testing.T, f *issuerFacadeFixture, adapter *PGPinAccess) PGPinAccessOutcome {
	t.Helper()
	state, err := adapter.Read(f.request(t.Context()), "target")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	outcome, err := adapter.Check(state)
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}
func TestPGPinAccessFacadeExternalSetPinIgnoresCachedFalse(t *testing.T) {
	f := accessFacadePG(t)
	if _, err := f.users.ClearPin("target"); err != nil {
		t.Fatal(err)
	}
	if f.users.HasPin("target") {
		t.Fatal("fixture local noPIN not established")
	}
	other, err := users.NewServiceWithStore(f.ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.SetPin("target", "9753"); err != nil {
		t.Fatal(err)
	}
	if f.users.HasPin("target") {
		t.Fatal("test did not leave old HasPin=false")
	}
	if got := facadeAccessOutcome(t, f, NewPGPinAccess(f.ds)); got != PGPinLocked {
		t.Fatal("cached noPIN bypassed current PG PIN")
	}
}
func TestPGPinAccessFacadeNoPinChangesBeforeFinalCheck(t *testing.T) {
	f := accessFacadePG(t)
	if _, err := f.users.ClearPin("target"); err != nil {
		t.Fatal(err)
	}
	adapter := NewPGPinAccess(f.ds)
	state, err := adapter.Read(f.request(t.Context()), "target")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	other, err := users.NewServiceWithStore(f.ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.SetPin("target", "9753"); err != nil {
		t.Fatal(err)
	}
	got, err := adapter.Check(state)
	if got != PGPinUnavailable || err == nil {
		t.Fatal("noPIN snapshot survived external SetPin")
	}
}
func TestPGPinAccessFacadeClearReassignAndMissingPrivacy(t *testing.T) {
	for _, mode := range []string{"external clear", "external reassign", "missing"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFacadePG(t)
			other, err := users.NewServiceWithStore(f.ds, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			adapter := NewPGPinAccess(f.ds)
			if mode == "external clear" {
				if _, err = other.ClearPin("target"); err != nil {
					t.Fatal(err)
				}
				if !f.users.HasPin("target") {
					t.Fatal("old cache unexpectedly current")
				}
				if got := facadeAccessOutcome(t, f, adapter); got != PGPinNoPin {
					t.Fatal("fresh noPIN used stale fingerprint/cache")
				}
				return
			}
			target := "target"
			if mode == "external reassign" {
				if _, err = other.Reassign("target", "other"); err != nil {
					t.Fatal(err)
				}
				if !f.users.BelongsToAccount("target", "actor") {
					t.Fatal("old owner cache unexpectedly current")
				}
			} else {
				target = "missing-target"
			}
			state, err := adapter.Read(f.request(t.Context()), target)
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			got, err := adapter.Check(state)
			if got != PGPinNotFound || err != nil {
				t.Fatal("foreign/missing classification disclosed PIN state")
			}
		})
	}
}
func TestPGPinAccessFacadeRequestTokensAndExternalRevoke(t *testing.T) {
	f := accessFacadePG(t)
	legacy := NewService(f.ds.ProfileUnlocks())
	token, _, err := legacy.Issue(IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: "actor", ProfileID: "target", PinFingerprint: f.users.PinFingerprint("target")})
	if err != nil {
		t.Fatal(err)
	}
	adapter := NewPGPinAccess(f.ds)
	r := f.request(t.Context())
	r.Header.Add(Header, "invalid, "+token)
	r.Header.Add(Header, token)
	r.URL.RawQuery = url.Values{QueryParam: {token}}.Encode()
	state, err := adapter.Read(r, "target")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if got, err := adapter.Check(state); got != PGPinUnlocked || err != nil {
		t.Fatal("actual RequestTokens adapter rejected valid presented token")
	}
	if err = f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	if !legacy.Validate([]string{token}, f.session.Token, "target", f.users.PinFingerprint("target")) {
		t.Fatal("test did not retain stale legacy cached grant")
	}
	state, err = adapter.Read(r, "target")
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if got, err := adapter.Check(state); got != PGPinLocked || err != nil {
		t.Fatal("current access reused stale cached Validate success")
	}
}
func TestPGPinAccessFacadeIdentityNoPGAcquisition(t *testing.T) {
	for _, mode := range []string{"facade copy", "other adapter", "state copy", "zero state", "closed", "replay"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFacadePG(t)
			adapter := NewPGPinAccess(f.ds)
			state, err := adapter.Read(f.request(t.Context()), "target")
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			switch mode {
			case "facade copy":
				copy := *adapter
				adapter = &copy
			case "other adapter":
				adapter = NewPGPinAccess(f.ds)
			case "state copy":
				copy := *state
				state = &copy
			case "zero state":
				state = &PGPinAccessState{}
			case "closed":
				state.Close()
			case "replay":
				if _, err = adapter.Check(state); err != nil {
					t.Fatal(err)
				}
			}
			before := f.ds.Pool().Stat().AcquireCount()
			got, err := adapter.Check(state)
			if got != PGPinUnavailable || !errors.Is(err, datastore.ErrProfilePinAccessUsed) || f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("invalid access adapter acquired PostgreSQL")
			}
		})
	}
}
