package profileunlock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"novastream/internal/datastore"
)

func flagFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins, management bool) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(t.Context())
	r.Method = http.MethodPut
	r.URL.Path = "/api/users/target/kids-profile"
	if management {
		r.URL.Path = "/account/api/profiles/kids"
		r.URL.RawQuery = "profileId=target"
	}
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, stop, e := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if e != nil {
		t.Fatal(e)
	}
	return checked, stop
}
func TestParentKidsFlagFacadeActualHandoffSealedIntentAndUserAck(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	body := []byte(`{"isKidsProfile":true}`)
	original := bytes.Clone(body)
	first, stop := flagFacadeRequest(t, f, pins, false)
	token, e := pins.ParentKidsFlagAPIChallenge(first, "target", body)
	stop()
	if e != nil {
		t.Fatal(e)
	}
	body[2] = 'X'
	if _, valid := parentConfirmationTokenKey(token, "pkf_"); !valid {
		t.Fatal("incorrect agreed token shape")
	}
	retry, release := flagFacadeRequest(t, f, pins, false)
	defer release()
	copied := *pins
	before := f.ds.Pool().Stat().AcquireCount()
	if i, e := copied.ClaimParentKidsFlagAPI(retry, "target", original, token); i != nil || e == nil {
		t.Fatal("copied facade accepted original")
	}
	if i, e := NewPGProfilePins(f.ds).ClaimParentKidsFlagAPI(retry, "target", original, token); i != nil || e == nil {
		t.Fatal("foreign facade accepted original")
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("invalid issuer performed SQL")
	}
	i, e := pins.ClaimParentKidsFlagAPI(retry, "target", original, token)
	if e != nil {
		t.Fatal(e)
	}
	defer i.Close()
	if raw, _ := json.Marshal(i); string(raw) != "null" || fmt.Sprintf("%+v", i) != "ParentKidsFlagIntent(opaque)" {
		t.Fatal("intent leaked original tuple")
	}
	retry, e = pins.BoundParentKidsFlagRequest(retry, i)
	if e != nil {
		t.Fatal(e)
	}
	retry.Header.Set("X-Account-Password", "synthetic actor account password")
	if e = pins.VerifyParentKidsFlag(retry, i); e != nil {
		t.Fatal(e)
	}
	l, e := f.users.AcquireParentKidsFlagLease(retry.Context(), "target", i)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	receipt, e := l.Commit()
	if e != nil {
		t.Fatal(e)
	}
	post, e := l.Stage(receipt)
	if e != nil {
		t.Fatal(e)
	}
	if e = pins.TakeParentKidsFlagReceipt(i, receipt, l.ReceiptLease(), nil); e != nil {
		t.Fatal(e)
	}
	l.Complete()
	l.Close()
	if !post.IsKidsProfile || post.KidsMode != "rating" {
		t.Fatal("actual full flag was not committed")
	}
	if _, e = pins.ClaimParentKidsFlagAPI(retry, "target", original, token); e == nil {
		t.Fatal("original challenge replayed")
	}
}
func TestParentKidsFlagFacadeInvalidEntryAndPrivateActionBeforeIO(t *testing.T) {
	for _, scenario := range []string{"wrong management entry", "changed raw body", "private action mismatch", "copy facade", "zero token"} {
		t.Run(scenario, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			r, stop := flagFacadeRequest(t, f, pins, false)
			defer stop()
			body := []byte(`{"isKidsProfile":true}`)
			token, e := pins.ParentKidsFlagAPIChallenge(r, "target", body)
			if e != nil {
				t.Fatal(e)
			}
			before := f.ds.Pool().Stat().AcquireCount()
			var i *datastore.ParentKidsFlagIntent
			switch scenario {
			case "wrong management entry":
				i, e = pins.ClaimParentKidsFlagManagement(r, "target", body, token)
			case "changed raw body":
				i, e = pins.ClaimParentKidsFlagAPI(r, "target", append(body, ' '), token)
			case "private action mismatch":
				key, _ := parentConfirmationTokenKey(token, "pkf_")
				pins.parentModeMu.Lock()
				pins.parentModes[key].action = parentConfirmationMode
				pins.parentModeMu.Unlock()
				i, e = pins.ClaimParentKidsFlagAPI(r, "target", body, token)
			case "copy facade":
				copy := *pins
				i, e = copy.ClaimParentKidsFlagAPI(r, "target", body, token)
			default:
				i, e = pins.ClaimParentKidsFlagAPI(r, "target", body, "")
			}
			if i != nil || e == nil {
				t.Fatal("invalid flag claim accepted")
			}
			if f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("invalid claim performed SQL")
			}
		})
	}
}
func TestParentKidsFlagFacadeSharedNineActions(t *testing.T) {
	for index := 0; index < 8; index++ {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			actions := ratingAllActions(t, f, pins)
			old := actions[index]
			flag, stop := flagFacadeRequest(t, f, pins, false)
			defer stop()
			body := []byte(`{"isKidsProfile":true}`)
			oldToken, e := old.challenge()
			if e != nil {
				t.Fatal(e)
			}
			flagToken, e := pins.ParentKidsFlagAPIChallenge(flag, "target", body)
			if e != nil {
				t.Fatal(e)
			}
			before := f.ds.Pool().Stat().AcquireCount()
			if i, e := pins.ClaimParentKidsFlagAPI(flag, "target", body, oldToken); i != nil || e == nil {
				t.Fatal("old operation became flag")
			}
			if e := old.claim(flagToken); e == nil {
				t.Fatal("flag operation became old action")
			}
			if f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("cross-action claims reached SQL")
			}
			if e := old.claim(oldToken); e != nil {
				t.Fatal("cross claim consumed other action", e)
			}
			i, e := pins.ClaimParentKidsFlagAPI(flag, "target", body, flagToken)
			if e != nil {
				t.Fatal(e)
			}
			i.Close()
		})
	}
	t.Run("same actor quota", func(t *testing.T) {
		f := issuerFacadePG(t)
		pins := NewPGProfilePins(f.ds)
		actions := ratingAllActions(t, f, pins)
		tokens := make([]string, 0, 8)
		for _, a := range actions {
			token, e := a.challenge()
			if e != nil {
				t.Fatal(e)
			}
			tokens = append(tokens, token)
		}
		flag, stop := flagFacadeRequest(t, f, pins, false)
		defer stop()
		body := []byte(`{"isKidsProfile":true}`)
		if _, e := pins.ParentKidsFlagAPIChallenge(flag, "target", body); e == nil {
			t.Fatal("ninth action bypassed shared actor quota")
		}
		if e := actions[0].claim(tokens[0]); e != nil {
			t.Fatal(e)
		}
		if _, e := pins.ParentKidsFlagAPIChallenge(flag, "target", body); e != nil {
			t.Fatal("one released shared slot not reusable", e)
		}
	})
}
