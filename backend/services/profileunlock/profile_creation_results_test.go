package profileunlock

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/services/user_settings"
)

func creationResultFacadeSaved(t *testing.T) (*issuerFacadeFixture, *PGProfilePins, *user_settings.Service, ParentProfileCreationChallengeBundle, string) {
	t.Helper()
	creationFacadeNoHTTP(t)
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	settings, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	first, stop := creationFacadeRequest(t, f, pins)
	bundle, err := pins.ParentProfileCreationChallenge(first, []byte(creationFacadeBody))
	stop()
	if err != nil {
		t.Fatal(err)
	}
	retry, release := creationFacadeRequest(t, f, pins)
	defer release()
	retry.Header.Set(datastore.ProfileCreationOperationHeader, bundle.CreationOperation)
	i, err := pins.ClaimParentProfileCreation(retry, []byte(creationFacadeBody), bundle.ParentIntent)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	retry, err = pins.BoundParentProfileCreationRequest(retry, i)
	if err != nil {
		t.Fatal(err)
	}
	retry.Header.Set("X-Account-Password", "synthetic actor account password")
	if err = pins.VerifyParentProfileCreation(retry, i); err != nil {
		t.Fatal(err)
	}
	u, err := f.users.AcquireParentProfileCreationLease(retry.Context(), i)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	s, err := settings.AcquireParentProfileCreationDefaultsLease(retry.Context(), i, u.ReceiptLease())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	receipt, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	value, err := u.Stage(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Stage(receipt); err != nil {
		t.Fatal(err)
	}
	if err = pins.TakeParentProfileCreationReceipt(i, receipt, u.ReceiptLease(), s.ReceiptLease()); err != nil {
		t.Fatal(err)
	}
	u.Complete()
	s.Complete()
	return f, pins, settings, bundle, value.ID
}
func TestProfileCreationResultsFacadeEchoIsSeparateFromParentIntent(t *testing.T) {
	creationFacadeNoHTTP(t)
	for _, kind := range []string{"missing", "wrong", "duplicate", "locator_as_intent"} {
		t.Run(kind, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			first, stop := creationFacadeRequest(t, f, pins)
			bundle, err := pins.ParentProfileCreationChallenge(first, []byte(creationFacadeBody))
			stop()
			if err != nil {
				t.Fatal(err)
			}
			retry, release := creationFacadeRequest(t, f, pins)
			defer release()
			token := bundle.ParentIntent
			switch kind {
			case "wrong":
				retry.Header.Set(datastore.ProfileCreationOperationHeader, "pcr_"+strings.Repeat("0", 64))
			case "duplicate":
				retry.Header.Add(datastore.ProfileCreationOperationHeader, bundle.CreationOperation)
				retry.Header.Add(datastore.ProfileCreationOperationHeader, bundle.CreationOperation)
			case "locator_as_intent":
				token = bundle.CreationOperation
			}
			if i, e := pins.ClaimParentProfileCreation(retry, []byte(creationFacadeBody), token); i != nil || e == nil {
				if i != nil {
					i.Close()
				}
				t.Fatal("locator became authority")
			}
		})
	}
}
func TestProfileCreationResultsFacadeFreshSessionCanReadExpiredOldWrite(t *testing.T) {
	f, pins, settings, bundle, _ := creationResultFacadeSaved(t)
	if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, f.session.Token); err != nil {
		t.Fatal(err)
	}
	next := f.session
	next.Token = "SYNTHETIC_NEW_RESULT_SESSION"
	next.CreatedAt = time.Now().UTC().Truncate(time.Microsecond)
	next.ExpiresAt = next.CreatedAt.Add(time.Hour)
	if err := f.ds.Sessions().Create(t.Context(), &next); err != nil {
		t.Fatal(err)
	}
	f.session = next
	r, stop := creationFacadeRequest(t, f, pins)
	defer stop()
	read, err := pins.PrepareProfileCreationRead(r, bundle.CreationOperation)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	u, err := f.users.PrepareProfileCreationRecovery(r.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	s, err := settings.PrepareProfileCreationRecovery(r.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	result, err := pins.ReadProfileCreationResult(read)
	if err != nil || result.Outcome() != "created" {
		t.Fatal("fresh authorized read tied to expired write session", err)
	}
	if i, err := pins.ClaimParentProfileCreation(r, []byte(creationFacadeBody), bundle.ParentIntent); i != nil || err == nil {
		t.Fatal("fresh read renewed old write")
	}
}
func TestProfileCreationResultsFacadeCurrentOwnerAndFrameChangesReject(t *testing.T) {
	for _, kind := range []string{"actor_role", "owner_expiry", "session_expiry", "scoped", "client", "session_revocation", "target_owner"} {
		t.Run(kind, func(t *testing.T) {
			f, pins, settings, bundle, target := creationResultFacadeSaved(t)
			r, stop := creationFacadeRequest(t, f, pins)
			defer stop()
			read, err := pins.PrepareProfileCreationRead(r, bundle.CreationOperation)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Close()
			u, err := f.users.PrepareProfileCreationRecovery(r.Context(), read)
			if err != nil {
				t.Fatal(err)
			}
			defer u.Close()
			s, err := settings.PrepareProfileCreationRecovery(r.Context(), read)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var sql string
			switch kind {
			case "target_owner":
				sql = `UPDATE users SET account_id='other' WHERE id=$1`
			case "actor_role":
				sql = `UPDATE accounts SET is_master=NOT is_master WHERE id='actor'`
			case "owner_expiry":
				sql = `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`
			case "session_expiry":
				sql = `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`
			case "scoped":
				sql = `UPDATE sessions SET scope='playback',scope_resource='synthetic' WHERE token=$1`
			case "client":
				sql = `UPDATE sessions SET client_id='changed-client' WHERE token=$1`
			case "session_revocation":
				sql = `INSERT INTO profile_unlock_revocations(scope,scope_key,generation) SELECT 'session',COALESCE(NULLIF(pin_unlock_lineage,''),$2),1 FROM sessions WHERE token=$1 ON CONFLICT(scope,scope_key) DO UPDATE SET generation=profile_unlock_revocations.generation+1`
			}
			if kind == "session_revocation" {
				if _, err = f.ds.Pool().Exec(t.Context(), sql, f.session.Token, datastoreDigestResultTest(f.session.Token)); err != nil {
					t.Fatal(err)
				}
			} else if kind == "target_owner" {
				if _, err = f.ds.Pool().Exec(t.Context(), sql, target); err != nil {
					t.Fatal(err)
				}
			} else if strings.Contains(sql, "$1") {
				if _, err = f.ds.Pool().Exec(t.Context(), sql, f.session.Token); err != nil {
					t.Fatal(err)
				}
			} else if _, err = f.ds.Pool().Exec(t.Context(), sql); err != nil {
				t.Fatal(err)
			}
			if _, err = pins.ReadProfileCreationResult(read); kind == "target_owner" && !errors.Is(err, datastore.ErrProfileCreationResultMissing) {
				t.Fatal("former owner result was not uniform missing", err)
			}
			if err == nil {
				t.Fatal("changed current authority read accepted", kind)
			}
		})
	}
}
func datastoreDigestResultTest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
func TestProfileCreationResultsFacadeLegacyAndMalformedLocatorUniform(t *testing.T) {
	f, pins, _, _, _ := creationResultFacadeSaved(t)
	r, stop := creationFacadeRequest(t, f, pins)
	defer stop()
	for _, ref := range []string{"", "pcc_" + strings.Repeat("0", 64), "pcr_" + strings.Repeat("0", 64), "pcr_" + strings.Repeat("A", 64)} {
		if read, err := pins.PrepareProfileCreationRead(r, ref); read != nil || !errors.Is(err, datastore.ErrProfileCreationResultMissing) {
			t.Fatal("invalid locator not uniform", err)
		}
	}
}
