package profileunlock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"novastream/internal/datastore"
)

func writesFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins, replace bool, body []byte) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(t.Context())
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Method = http.MethodPost
	if replace {
		r.Method = http.MethodPut
	}
	r.URL.Path = "/api/users/target/kids/lists"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func TestParentListWritesFacadeActualHandoffAndUsersCommit(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			body := []byte(`{"url":"x"}`)
			if replace {
				body = []byte(`{"lists":["x","x"]}`)
			}
			first, release := writesFacadeRequest(t, f, pins, replace, body)
			var token string
			var err error
			if replace {
				token, err = pins.ParentListReplaceChallenge(first, "target", body)
			} else {
				token, err = pins.ParentListAddChallenge(first, "target", body)
			}
			release()
			if err != nil {
				t.Fatal(err)
			}
			retry, stop := writesFacadeRequest(t, f, pins, replace, body)
			defer stop()
			var intent *ParentListWriteIntent
			if replace {
				intent, err = pins.ClaimParentListReplace(retry, "target", body, token)
			} else {
				intent, err = pins.ClaimParentListAdd(retry, "target", body, token)
			}
			if err != nil {
				t.Fatal("completed original HTTP destroyed challenge", err)
			}
			defer intent.Close()
			retry, err = pins.BoundParentListWriteRequest(retry, intent)
			if err != nil {
				t.Fatal(err)
			}
			retry.Header.Set("X-Account-Password", "synthetic actor account password")
			if err = pins.VerifyParentListWrite(retry, intent); err != nil {
				t.Fatal(err)
			}
			if replace {
				_, err = f.users.CommitParentListReplace(retry.Context(), "target", intent)
			} else {
				_, err = f.users.CommitParentListAdd(retry.Context(), "target", intent)
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestParentListWritesAllFourActionsAreIsolatedBeforeIO(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	mode, stopMode := modeFacadeRequest(t, f, pins, t.Context())
	defer stopMode()
	removal, stopRemoval := removalFacadeRequest(t, f, pins, t.Context())
	defer stopRemoval()
	add, stopAdd := writesFacadeRequest(t, f, pins, false, nil)
	defer stopAdd()
	replace, stopReplace := writesFacadeRequest(t, f, pins, true, nil)
	defer stopReplace()
	addBody, replaceBody := []byte(`{"url":"x"}`), []byte(`{"lists":["x"]}`)
	var tokens [4]string
	var err error
	tokens[0], err = pins.ParentModeChallenge(mode, "target", "rating")
	if err != nil {
		t.Fatal(err)
	}
	tokens[1], err = pins.ParentListRemovalChallenge(removal, "target", "SYNTHETIC_LIST")
	if err != nil {
		t.Fatal(err)
	}
	tokens[2], err = pins.ParentListAddChallenge(add, "target", addBody)
	if err != nil {
		t.Fatal(err)
	}
	tokens[3], err = pins.ParentListReplaceChallenge(replace, "target", replaceBody)
	if err != nil {
		t.Fatal(err)
	}
	claim := []func(string) error{
		func(token string) error {
			i, e := pins.ClaimParentMode(mode, "target", "rating", token)
			if i != nil {
				i.Close()
			}
			return e
		},
		func(token string) error {
			i, e := pins.ClaimParentListRemoval(removal, "target", "SYNTHETIC_LIST", token)
			if i != nil {
				i.Close()
			}
			return e
		},
		func(token string) error {
			i, e := pins.ClaimParentListAdd(add, "target", addBody, token)
			if i != nil {
				i.Close()
			}
			return e
		},
		func(token string) error {
			i, e := pins.ClaimParentListReplace(replace, "target", replaceBody, token)
			if i != nil {
				i.Close()
			}
			return e
		},
	}
	before := f.ds.Pool().Stat().AcquireCount()
	for source, token := range tokens {
		for target, attempt := range claim {
			if source != target && attempt(token) == nil {
				t.Fatal("cross-action token adopted")
			}
		}
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("cross-action claim performed SQL")
	}
	for n, attempt := range claim {
		if err = attempt(tokens[n]); err != nil {
			t.Fatal("invalid action consumed valid original slot", err)
		}
	}
}
func TestParentListWritesSharedBudgetAcrossAllFourActions(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	mode, sm := modeFacadeRequest(t, f, pins, t.Context())
	defer sm()
	removal, sr := removalFacadeRequest(t, f, pins, t.Context())
	defer sr()
	add, sa := writesFacadeRequest(t, f, pins, false, nil)
	defer sa()
	replace, sp := writesFacadeRequest(t, f, pins, true, nil)
	defer sp()
	actions := []func() (string, error){
		func() (string, error) { return pins.ParentModeChallenge(mode, "target", "rating") },
		func() (string, error) { return pins.ParentListRemovalChallenge(removal, "target", "SYNTHETIC_LIST") },
		func() (string, error) { return pins.ParentListAddChallenge(add, "target", []byte(`{"url":"x"}`)) },
		func() (string, error) {
			return pins.ParentListReplaceChallenge(replace, "target", []byte(`{"lists":["x"]}`))
		},
	}
	t.Cleanup(func() {
		pins.parentModeMu.Lock()
		defer pins.parentModeMu.Unlock()
		for _, entry := range pins.parentModes {
			entry.timer.Stop()
			entry.close()
		}
		pins.parentModes = nil
		pins.parentModeBytes = 0
	})
	for range 2 {
		for _, challenge := range actions {
			if _, err := challenge(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, challenge := range actions {
		if _, err := challenge(); err == nil {
			t.Fatal("action escaped shared eight-entry actor budget")
		}
	}
	pins.parentModeMu.Lock()
	defer pins.parentModeMu.Unlock()
	if len(pins.parentModes) != 8 || pins.parentModeBytes > parentModeMaximumBytes {
		t.Fatal("global budget split by action")
	}
}
func TestParentListWritesFacadeOriginalBodyAndIssuer(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	original := []byte(`{"url":"x"}`)
	body := bytes.Clone(original)
	first, release := writesFacadeRequest(t, f, pins, false, body)
	defer release()
	token, err := pins.ParentListAddChallenge(first, "target", body)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)-3] = 'y'
	retry, stop := writesFacadeRequest(t, f, pins, false, original)
	defer stop()
	copyFacade := *pins
	before := f.ds.Pool().Stat().AcquireCount()
	if i, e := copyFacade.ClaimParentListAdd(retry, "target", original, token); i != nil || e == nil {
		t.Fatal("copied issuer adopted intent")
	}
	if i, e := NewPGProfilePins(f.ds).ClaimParentListAdd(retry, "target", original, token); i != nil || e == nil {
		t.Fatal("foreign issuer adopted intent")
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("foreign issuer performed SQL")
	}
	intent, err := pins.ClaimParentListAdd(retry, "target", original, token)
	if err != nil {
		t.Fatal("caller mutation changed captured body", err)
	}
	defer intent.Close()
	if _, err = pins.ClaimParentListAdd(retry, "target", original, token); err == nil {
		t.Fatal("claimed write intent replayed")
	}
	before = f.ds.Pool().Stat().AcquireCount()
	if _, err = pins.ParentListAddChallenge(first, "target", bytes.Repeat([]byte(" "), datastore.ParentListWriteBodyLimit+1)); !errors.Is(err, datastore.ErrParentListWriteBodyTooLarge) {
		t.Fatal("facade did not preserve bounded raw capture", err)
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("oversized body reached SQL")
	}
}
