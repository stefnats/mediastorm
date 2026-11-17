package profileunlock

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/services/users"
)

func publicFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins) *http.Request {
	t.Helper()
	request, release, err := pins.Authenticate(f.request(t.Context()), f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	return request
}
func TestPublicPGVerifyAcknowledgesOnlyOwnPinLengthMigration(t *testing.T) {
	f := issuerFacadePG(t)
	// Reconstruct a genuinely confirmed old-length projection, not a fake ack.
	if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET pin_length=0 WHERE id='target'`); err != nil {
		t.Fatal(err)
	}
	svc, err := users.NewServiceWithStore(f.ds, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	pins := NewPGProfilePins(f.ds)
	request := publicFacadeRequest(t, f, pins)
	token, expiry, err := pins.Verify(request, "target", "2580", time.Minute, svc)
	if err != nil || token == "" || !expiry.After(time.Now()) {
		t.Fatal("verified PIN did not release after own migration", err)
	}
	profile, ok := svc.Get("target")
	if !ok || profile.PinLength != 4 {
		t.Fatal("own exact migration not acknowledged")
	}
	// Subsequent targeted CAS still works; no blind resync/adoption was needed.
	if _, err = svc.SetPin("target", "8642"); err != nil {
		t.Fatal("own ack left stale users revision", err)
	}
	if err = pins.CheckVerified(request, "target"); err == nil {
		t.Fatal("post-verification external PIN change accepted")
	}
}
func TestPublicPGVerifyRawPINAndNoPinWireInputs(t *testing.T) {
	for _, mode := range []string{"correct", "wrong", "whitespace not normalized", "no PIN"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			if mode == "no PIN" {
				if _, err := f.users.ClearPin("target"); err != nil {
					t.Fatal(err)
				}
			}
			pins := NewPGProfilePins(f.ds)
			request := publicFacadeRequest(t, f, pins)
			raw := "2580"
			if mode == "wrong" {
				raw = "wrong"
			}
			if mode == "whitespace not normalized" {
				raw = " 2580 "
			}
			token, expiry, err := pins.Verify(request, "target", raw, time.Minute, f.users)
			switch mode {
			case "correct":
				if err != nil || token == "" {
					t.Fatal("valid raw PIN denied", err)
				}
			case "no PIN":
				if err != nil || token != "" || !expiry.IsZero() {
					t.Fatal("no-PIN response invented token", err)
				}
				if err = pins.CheckVerified(request, "target"); err != nil {
					t.Fatal("no-PIN post-stamp missing", err)
				}
			default:
				if !errors.Is(err, datastore.ErrPGPinCredential) || token != "" {
					t.Fatal("wrong/raw normalized PIN accepted", err)
				}
			}
		})
	}
}
func TestPublicPGKnownPostIsExactOneShotAndFacadeBound(t *testing.T) {
	for _, mode := range []string{"positive", "target rename", "target length", "other facade", "wrong target", "replay", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			request := publicFacadeRequest(t, f, pins)
			if token, _, err := pins.Verify(request, "target", "2580", time.Minute, f.users); err != nil || token == "" {
				t.Fatal(err)
			}
			target := "target"
			switch mode {
			case "target rename":
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET name='replacement' WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
			case "target length":
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET pin_length=9 WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
			case "other facade":
				pins = NewPGProfilePins(f.ds)
			case "wrong target":
				target = "other"
			case "replay":
				if err := pins.CheckVerified(request, target); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				ctx, cancel := context.WithCancel(request.Context())
				cancel()
				request = request.WithContext(ctx)
			}
			err := pins.CheckVerified(request, target)
			if mode == "positive" && err != nil || mode != "positive" && err == nil {
				t.Fatal("known-post identity mismatch", err)
			}
		})
	}
}
func TestPublicPGPinMutationActualParentProofAndRevocation(t *testing.T) {
	for _, mode := range []string{"adult unlocked change", "adult first PIN", "locked missing password", "locked correct password", "wrong password", "kids first PIN requires parent", "clear without PIN requires parent"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			switch mode {
			case "adult first PIN", "kids first PIN requires parent", "clear without PIN requires parent":
				if _, err := f.users.ClearPin("target"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "kids first PIN requires parent" {
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
			}
			pins := NewPGProfilePins(f.ds)
			request := publicFacadeRequest(t, f, pins)
			if mode == "adult unlocked change" {
				token, _, err := pins.Verify(request, "target", "2580", time.Minute, f.users)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("X-Profile-Unlock", token)
			}
			if mode == "locked correct password" {
				request.Header.Set(parentauth.PasswordHeader, "synthetic actor account password")
			}
			if mode == "wrong password" {
				request.Header.Set(parentauth.PasswordHeader, "wrong")
			}
			clearing := mode == "clear without PIN requires parent"
			intent, err := pins.PrepareOwnedPinChange(request, "target", " 1357 ", clearing)
			if err != nil {
				t.Fatal(err)
			}
			defer intent.Close()
			needs, err := intent.NeedsParent()
			if err != nil {
				t.Fatal(err)
			}
			wantParent := mode != "adult unlocked change" && mode != "adult first PIN"
			if needs != wantParent {
				t.Fatal("changed existing parent predicate")
			}
			err = intent.Verify()
			valid := mode == "adult unlocked change" || mode == "adult first PIN" || mode == "locked correct password"
			if !valid {
				if !errors.Is(err, datastore.ErrPGPinParentRequired) {
					t.Fatal("missing actual parent proof accepted", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			profile, err := f.users.CommitPGPinChange(request.Context(), intent)
			if err != nil {
				t.Fatal(err)
			}
			if profile.PinLength != 4 {
				t.Fatal("SetPin raw creation normalization changed")
			}
			if err = f.users.VerifyPin("target", "1357"); err != nil {
				t.Fatal(err)
			}
			var count int
			if err = f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_unlocks WHERE profile_id='target'`).Scan(&count); err != nil || count != 0 {
				t.Fatal("PIN mutation failed to revoke grants")
			}
		})
	}
}
func TestPublicPGPinMutationRejectsChangedOriginalAndPreservesOtherUser(t *testing.T) {
	for _, mode := range []string{"actor password", "target row", "owner reassign", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			request := publicFacadeRequest(t, f, pins)
			request.Header.Set(parentauth.PasswordHeader, "synthetic actor account password")
			intent, err := pins.PrepareOwnedPinChange(request, "target", "1357", false)
			if err != nil {
				t.Fatal(err)
			}
			defer intent.Close()
			if err = intent.Verify(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "actor password":
				_, err = f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET password_hash=password_hash WHERE id='actor'`)
			case "target row":
				_, err = f.ds.Pool().Exec(t.Context(), `UPDATE users SET name='replacement' WHERE id='target'`)
			case "owner reassign":
				_, err = f.ds.Pool().Exec(t.Context(), `UPDATE users SET account_id='other' WHERE id='target'`)
			case "cancel":
				intent.Close()
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.users.CommitPGPinChange(request.Context(), intent); err == nil {
				t.Fatal("changed original intent committed")
			}
			var hash string
			if err = f.ds.Pool().QueryRow(t.Context(), `SELECT pin_hash FROM users WHERE id='target'`).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			if hash == "" {
				t.Fatal("rejected mutation cleared original PIN")
			}
		})
	}
}

func TestPublicPGOwnedFactsAreCurrentAndExpiryCopies(t *testing.T) {
	f := issuerFacadePG(t)
	expiry := time.Now().UTC().Add(30 * time.Minute).Truncate(time.Microsecond)
	if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, expiry); err != nil {
		t.Fatal(err)
	}
	pins := NewPGProfilePins(f.ds)
	request := publicFacadeRequest(t, f, pins)
	value, err := pins.Current(request, "target")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := value.Owner()
	if err != nil || owner.ID != "actor" || owner.ExpiresAt == nil || !owner.ExpiresAt.Equal(expiry) || owner.PasswordHash != "" {
		t.Fatal("checked owner facts missing or leaked credential")
	}
	*owner.ExpiresAt = owner.ExpiresAt.Add(time.Hour)
	again, err := value.Owner()
	if err != nil || again.ExpiresAt == nil || !again.ExpiresAt.Equal(expiry) {
		t.Fatal("caller mutated captured owner expiry")
	}
}
