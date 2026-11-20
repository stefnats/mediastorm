package profileunlock

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/internal/auth"
	"novastream/internal/datastore"
)

func bridgeCredentialFacadePG(t *testing.T) (*issuerFacadeFixture, *PGProfilePins) {
	t.Helper()
	f := issuerFacadePG(t)
	if _, err := f.ds.Pool().Exec(t.Context(), "DELETE FROM sessions"); err != nil {
		t.Fatal(err)
	}
	return f, NewPGProfilePins(f.ds)
}
func bridgeCredentialFacadeRequest(ctx context.Context) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/users/authenticatebyname", nil).WithContext(ctx)
	r.Header.Set("Authorization", `MediaBrowser DeviceId="synthetic-device"`)
	return r
}
func TestBridgeCredentialFacadeRealPGWithoutSessionProducesOnlyOpaqueEvidence(t *testing.T) {
	for _, kind := range []string{"account", "combined", "PIN"} {
		t.Run(kind, func(t *testing.T) {
			f, pins := bridgeCredentialFacadePG(t)
			r := bridgeCredentialFacadeRequest(t.Context())
			i, err := pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			var proof *BridgeLoginCredentialProof
			switch kind {
			case "account":
				proof, err = pins.VerifyBridgeAccountPassword(r, i, "synthetic actor account password")
			case "combined":
				proof, err = pins.VerifyBridgePasswordWithPIN(r, i, "synthetic actor account password2580")
			case "PIN":
				proof, err = pins.VerifyBridgeProfilePIN(r, i, "2580")
			}
			if err != nil || proof == nil || proof.Check(t.Context()) != nil {
				t.Fatal("genuine pre-session PG credential evidence failed", err)
			}
			var sessions int
			if err := f.ds.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions").Scan(&sessions); err != nil || sessions != 0 {
				t.Fatal("credential-only facade minted or changed sessions", err)
			}
			for _, key := range []any{auth.ContextKeySession, auth.ContextKeyAccountID, auth.ContextKeyIsMaster} {
				if proof.Context().Value(key) != nil {
					t.Fatal("clean credential lifetime leaked auth values")
				}
			}
		})
	}
}
func TestBridgeCredentialFacadeZeroForeignIssuerAndCopiesCannotTransferEvidence(t *testing.T) {
	var zero BridgeLoginCredentialIntent
	var proof BridgeLoginCredentialProof
	var pins PGProfilePins
	if _, err := pins.PrepareBridgeLoginCredentials(nil, "actor", "target", "device"); err == nil {
		t.Fatal("zero facade prepared credentials")
	}
	if json.Unmarshal([]byte("{}"), &zero) == nil || json.Unmarshal([]byte("{}"), &proof) == nil || zero.Check(t.Context()) == nil || proof.Check(t.Context()) == nil || zero.Context().Err() == nil {
		t.Fatal("imported zero evidence became usable")
	}
	f, actual := bridgeCredentialFacadePG(t)
	other, foreign := bridgeCredentialFacadePG(t)
	_ = other
	r := bridgeCredentialFacadeRequest(t.Context())
	i, err := actual.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if p, err := foreign.VerifyBridgeAccountPassword(r, i, "synthetic actor account password"); p != nil || err == nil {
		t.Fatal("foreign actual facade consumed original intent")
	}
	copiedPins := *actual
	if p, err := copiedPins.VerifyBridgeAccountPassword(r, i, "synthetic actor account password"); p != nil || err == nil {
		t.Fatal("copied facade gained credential authority")
	}
	if _, err := NewPGProfilePins(&datastore.DataStore{}).PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device"); err == nil {
		t.Fatal("unavailable store minted evidence")
	}
	copiedIntent := *i
	copiedIntent.Close()
	if copiedIntent.Check(t.Context()) == nil || i.Check(t.Context()) != nil {
		t.Fatal("copied intent gained or retired original")
	}
	p, err := actual.VerifyBridgeAccountPassword(r, i, "synthetic actor account password")
	if err != nil {
		t.Fatal(err)
	}
	copiedProof := *p
	copiedProof.Close()
	if copiedProof.Check(t.Context()) == nil || p.Check(t.Context()) != nil {
		t.Fatal("copied proof gained or retired original")
	}
	for _, value := range []any{i, *i, &copiedIntent, copiedIntent, p, *p, &copiedProof, copiedProof} {
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("facade evidence serialized")
		}
		for _, format := range []string{"%+v", "%#v"} {
			got := fmt.Sprintf(format, value)
			if got != "BridgeLoginCredentialIntent(opaque)" && got != "BridgeLoginCredentialProof(opaque)" {
				t.Fatal("facade evidence exposed private fields")
			}
		}
	}
	var count int
	if err := f.ds.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions").Scan(&count); err != nil || count != 0 {
		t.Fatal("facade gained a session capability", err)
	}
}
func TestBridgeCredentialFacadeOriginalRequestClientAndCancellationRemainBound(t *testing.T) {
	_, pins := bridgeCredentialFacadePG(t)
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := bridgeCredentialFacadeRequest(caller)
	i, err := pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if p, err := pins.VerifyBridgeAccountPassword(r.WithContext(context.WithoutCancel(r.Context())), i, "synthetic actor account password"); p != nil || err == nil {
		t.Fatal("WithoutCancel replacement transferred original request")
	}
	old := r.Header.Get("Authorization")
	r.Header.Set("Authorization", `MediaBrowser DeviceId="other"`)
	if p, err := pins.VerifyBridgeAccountPassword(r, i, "synthetic actor account password"); p != nil || err == nil {
		t.Fatal("changed original client carrier accepted")
	}
	r.Header.Set("Authorization", old)
	p, err := pins.VerifyBridgeAccountPassword(r, i, "synthetic actor account password")
	if err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(t.Context())
	stop()
	if p.Check(canceled) == nil || p.Check(t.Context()) != nil {
		t.Fatal("caller-only cancel retired intact original")
	}
	cancel()
	select {
	case <-p.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("original request cancellation did not end evidence")
	}
	if p.Check(context.Background()) == nil {
		t.Fatal("evidence escaped original request without Handoff")
	}
}
func TestBridgeCredentialFacadeCurrentPGOverridesStalePINAndPasswordCaches(t *testing.T) {
	f, pins := bridgeCredentialFacadePG(t)
	replacement, err := bcrypt.GenerateFromPassword([]byte("new actual PG password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), "UPDATE accounts SET password_hash=$1 WHERE id='actor'", string(replacement)); err != nil {
		t.Fatal(err)
	}
	r := bridgeCredentialFacadeRequest(t.Context())
	old, err := pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if p, err := pins.VerifyBridgeAccountPassword(r, old, "synthetic actor account password"); p != nil || err == nil {
		t.Fatal("startup password/cache substituted for current PG hash")
	}
	current, err := pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	p, err := pins.VerifyBridgeAccountPassword(r, current, "new actual PG password")
	if err != nil || p.Check(t.Context()) != nil {
		t.Fatal("current actual PG password denied", err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), "UPDATE users SET is_kids_profile=true WHERE id='target'"); err != nil {
		t.Fatal(err)
	}
	if _, err = pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device"); err == nil {
		t.Fatal("cached adult model erased current stored child flag")
	}
}
func TestBridgeCredentialFacadeMasterCannotChooseForeignProfile(t *testing.T) {
	f, pins := bridgeCredentialFacadePG(t)
	if _, err := f.ds.Pool().Exec(t.Context(), "UPDATE accounts SET is_master=true WHERE id='actor';UPDATE users SET account_id='other' WHERE id='target'"); err != nil {
		t.Fatal(err)
	}
	r := bridgeCredentialFacadeRequest(t.Context())
	if i, err := pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device"); i != nil || err == nil {
		if i != nil {
			i.Close()
		}
		t.Fatal("master decoration widened exact credential profile ownership")
	}
}
