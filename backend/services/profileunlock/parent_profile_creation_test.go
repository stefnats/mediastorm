package profileunlock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"novastream/services/user_settings"
)

func creationFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(t.Context())
	r.Method = http.MethodPost
	r.URL.Path = "/account/api/profiles"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, stop, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, stop
}

const creationFacadeBody = `{"name":"Synthetic child","isKidsProfile":true}`

func TestParentProfileCreationFacadeActualHandoffAndBothCacheOwners(t *testing.T) {
	creationFacadeNoHTTP(t)
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	settings, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	first, stop := creationFacadeRequest(t, f, pins)
	body := []byte(creationFacadeBody)
	token, err := pins.ParentProfileCreationChallenge(first, body)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	body[2] = 'X'
	if _, ok := parentConfirmationTokenKey(token, "pcc_"); !ok {
		t.Fatal("new action token format")
	}
	retry, release := creationFacadeRequest(t, f, pins)
	defer release()
	before := f.ds.Pool().Stat().AcquireCount()
	copy := *pins
	if i, err := copy.ClaimParentProfileCreation(retry, []byte(creationFacadeBody), token); i != nil || err == nil {
		t.Fatal("copied facade accepted intent")
	}
	if i, err := NewPGProfilePins(f.ds).ClaimParentProfileCreation(retry, []byte(creationFacadeBody), token); i != nil || err == nil {
		t.Fatal("foreign registry accepted intent")
	}
	if before != f.ds.Pool().Stat().AcquireCount() {
		t.Fatal("invalid facade touched SQL")
	}
	i, err := pins.ClaimParentProfileCreation(retry, []byte(creationFacadeBody), token)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if raw, _ := json.Marshal(i); string(raw) != "null" || fmt.Sprintf("%+v", i) != "ParentProfileCreationIntent(opaque)" {
		t.Fatal("intent exposed credentials")
	}
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
	created, err := u.Stage(receipt)
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
	s.Close()
	u.Close()
	if !created.IsKidsProfile || created.AccountID != "actor" {
		t.Fatal("known child not installed")
	}
	if _, err = pins.ClaimParentProfileCreation(retry, []byte(creationFacadeBody), token); err == nil {
		t.Fatal("spent challenge replay")
	}
	if _, ok := f.users.Get(created.ID); !ok {
		t.Fatal("actual user cache missing")
	}
}
func TestParentProfileCreationFacadeTypedIsolationAndOneShot(t *testing.T) {
	creationFacadeNoHTTP(t)
	for _, kind := range []string{"other prefix", "wrong action", "body changed", "false downgrade", "alias changed", "duplicate query", "missing intent"} {
		t.Run(kind, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			first, stop := creationFacadeRequest(t, f, pins)
			token, err := pins.ParentProfileCreationChallenge(first, []byte(creationFacadeBody))
			stop()
			if err != nil {
				t.Fatal(err)
			}
			r, release := creationFacadeRequest(t, f, pins)
			defer release()
			body := []byte(creationFacadeBody)
			switch kind {
			case "other prefix":
				token = "pmi_" + token[4:]
			case "wrong action":
				key, _ := parentConfirmationTokenKey(token, "pcc_")
				pins.parentModeMu.Lock()
				pins.parentModes[key].action = parentConfirmationKidsFlag
				pins.parentModeMu.Unlock()
			case "body changed":
				body = append(body, ' ')
			case "false downgrade":
				body = []byte(`{"name":"Synthetic child","isKidsProfile":false}`)
			case "alias changed":
				r.URL.Path = "/admin/api/profiles"
			case "duplicate query":
				r.URL.RawQuery = "x=1&x=2"
			case "missing intent":
				token = ""
			}
			if i, err := pins.ClaimParentProfileCreation(r, body, token); i != nil || err == nil {
				if i != nil {
					i.Close()
				}
				t.Fatal("cross-action/context intent accepted", kind)
			}
			if kind == "body changed" || kind == "false downgrade" || kind == "alias changed" || kind == "duplicate query" {
				if i, err := pins.ClaimParentProfileCreation(r, []byte(creationFacadeBody), token); i != nil || err == nil {
					t.Fatal("failed original claim was not consumed")
				}
			}
		})
	}
}
func TestParentProfileCreationFacadeUsesOneExistingActorRegistryBudget(t *testing.T) {
	creationFacadeNoHTTP(t)
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	// Seven existing mode intents and one creation intent consume ONE actor8
	// registry. The durable ledger has its own independent storage quota.
	for n := 0; n < 7; n++ {
		r, stop := flagFacadeRequest(t, f, pins, false)
		r.URL.Path = "/api/users/target/kids/mode"
		_, err := pins.ParentModeChallenge(r, "target", "rating")
		stop()
		if err != nil {
			t.Fatal(err)
		}
	}
	r, stop := creationFacadeRequest(t, f, pins)
	defer stop()
	token, err := pins.ParentProfileCreationChallenge(r, []byte(creationFacadeBody))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(token, "pcc_") {
		t.Fatal("wrong typed action")
	}
	if _, err = pins.ParentProfileCreationChallenge(r, []byte(creationFacadeBody)); err == nil {
		t.Fatal("creation spent a separate registry budget")
	}
	pins.parentModeMu.Lock()
	count := len(pins.parentModes)
	for _, entry := range pins.parentModes {
		entry.timer.Stop()
		entry.close()
	}
	pins.parentModes = nil
	pins.parentModeBytes = 0
	pins.parentModeMu.Unlock()
	if count != parentModeMaximumActorEntries {
		t.Fatal("shared registry count incorrect", count)
	}
}
func TestParentProfileCreationFacadeInvalidTokensBeforeStorage(t *testing.T) {
	creationFacadeNoHTTP(t)
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	r, stop := creationFacadeRequest(t, f, pins)
	defer stop()
	before := f.ds.Pool().Stat().AcquireCount()
	for _, token := range []string{"", "pcc_" + strings.Repeat("A", 64), "pcc_" + strings.Repeat("0", 63), "pcc_" + strings.Repeat("0", 64) + " ", "pkf_" + strings.Repeat("0", 64)} {
		if i, err := pins.ClaimParentProfileCreation(r, bytes.Clone([]byte(creationFacadeBody)), token); i != nil || err == nil {
			t.Fatal("malformed token accepted")
		}
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("invalid token performed SQL")
	}
}

type creationFacadeDenyHTTP struct{}

func (creationFacadeDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("HTTP denied in synthetic creation facade fixture")
}
func creationFacadeNoHTTP(t *testing.T) {
	t.Helper()
	before := http.DefaultTransport
	http.DefaultTransport = creationFacadeDenyHTTP{}
	t.Cleanup(func() { http.DefaultTransport = before })
}
