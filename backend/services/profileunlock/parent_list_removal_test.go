package profileunlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"novastream/internal/datastore"
)

func removalFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(ctx)
	r.Method = http.MethodDelete
	r.URL.Path = "/api/users/target/kids/lists"
	r.URL.RawQuery = "url=SYNTHETIC_LIST"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, stop, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, stop
}
func TestParentListRemovalFacadeActualCanceledFirstHTTPAndTargetedWrite(t *testing.T) {
	f := issuerFacadePG(t)
	if _, err := f.users.SetKidsAllowedLists("target", []string{"SYNTHETIC_LIST", "SYNTHETIC_LIST"}); err != nil {
		t.Fatal(err)
	}
	pins := NewPGProfilePins(f.ds)
	first, stop := removalFacadeRequest(t, f, pins, t.Context())
	token, err := pins.ParentListRemovalChallenge(first, "target", "SYNTHETIC_LIST")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	if _, valid := parentConfirmationTokenKey(token, "pli_"); !valid {
		t.Fatal("challenge does not match the agreed opaque shape")
	}
	retry, release := removalFacadeRequest(t, f, pins, t.Context())
	defer release()
	intent, err := pins.ClaimParentListRemoval(retry, "target", "SYNTHETIC_LIST", token)
	if err != nil {
		t.Fatal("HTTP completion destroyed bounded challenge", err)
	}
	defer intent.Close()
	if raw, _ := json.Marshal(intent); string(raw) != "null" || fmt.Sprintf("%+v", intent) != "ParentListRemovalIntent(opaque)" {
		t.Fatal("intent exposes private state")
	}
	retry, err = pins.BoundParentListRemovalRequest(retry, intent)
	if err != nil {
		t.Fatal(err)
	}
	retry.Header.Set("X-Account-Password", "synthetic actor account password")
	if err = pins.VerifyParentListRemoval(retry, intent); err != nil {
		t.Fatal(err)
	}
	saved, err := f.users.CommitParentListRemoval(retry.Context(), "target", intent)
	if err != nil || len(saved.KidsAllowedLists) != 0 {
		t.Fatal("actual users list removal commit failed", err)
	}
	if _, err = pins.ClaimParentListRemoval(retry, "target", "SYNTHETIC_LIST", token); !errors.Is(err, datastore.ErrParentListRemovalChanged) {
		t.Fatal("claimed operation replayed")
	}
}
func TestParentListRemovalFacadeInvalidClaimsNeverReplaceOriginalOperation(t *testing.T) {
	for _, mode := range []string{"bad token", "copy issuer", "wrong issuer", "wrong mode", "wrong path"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			first, stop := removalFacadeRequest(t, f, pins, t.Context())
			defer stop()
			token, err := pins.ParentListRemovalChallenge(first, "target", "SYNTHETIC_LIST")
			if err != nil {
				t.Fatal(err)
			}
			retry, release := removalFacadeRequest(t, f, pins, t.Context())
			defer release()
			current := pins
			wanted := "SYNTHETIC_LIST"
			switch mode {
			case "bad token":
				token = "pli_bad"
			case "copy issuer":
				copy := *pins
				current = &copy
			case "wrong issuer":
				current = NewPGProfilePins(f.ds)
			case "wrong mode":
				wanted = ""
			case "wrong path":
				retry.URL.Path = "/account/api/users/target/kids/lists"
			}
			before := f.ds.Pool().Stat().AcquireCount()
			if intent, err := current.ClaimParentListRemoval(retry, "target", wanted, token); err == nil || intent != nil {
				t.Fatal("invalid claim rebound to a fresh operation")
			}
			if f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("invalid local claim performed database acquisition")
			}
			pins.parentModeMu.Lock()
			for key, entry := range pins.parentModes {
				entry.timer.Stop()
				entry.close()
				delete(pins.parentModes, key)
			}
			pins.parentModeBytes = 0
			pins.parentModeMu.Unlock()
		})
	}
}
func TestParentListRemovalFacadeBoundedMapAndAutomaticOriginalExpiry(t *testing.T) {
	t.Run("per actor maximum", func(t *testing.T) {
		f := issuerFacadePG(t)
		pins := NewPGProfilePins(f.ds)
		r, stop := removalFacadeRequest(t, f, pins, t.Context())
		defer stop()
		for range parentModeMaximumActorEntries {
			if _, err := pins.ParentListRemovalChallenge(r, "target", "SYNTHETIC_LIST"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pins.ParentListRemovalChallenge(r, "target", "SYNTHETIC_LIST"); !errors.Is(err, datastore.ErrParentListRemovalUnavailable) {
			t.Fatal("unbounded actor challenge map")
		}
		pins.parentModeMu.Lock()
		if len(pins.parentModes) != 8 || pins.parentModeBytes > parentModeMaximumBytes {
			t.Fatal("challenge memory budget exceeded")
		}
		for _, entry := range pins.parentModes {
			entry.timer.Stop()
			entry.close()
		}
		pins.parentModes = nil
		pins.parentModeBytes = 0
		pins.parentModeMu.Unlock()
	})
	t.Run("actual original deadline removes entries", func(t *testing.T) {
		f := issuerFacadePG(t)
		pins := NewPGProfilePins(f.ds)
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancel()
		r, stop := removalFacadeRequest(t, f, pins, ctx)
		defer stop()
		token, err := pins.ParentListRemovalChallenge(r, "target", "SYNTHETIC_LIST")
		if err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		until := time.Now().Add(time.Second)
		for {
			pins.parentModeMu.Lock()
			empty := len(pins.parentModes) == 0 && pins.parentModeBytes == 0
			pins.parentModeMu.Unlock()
			if empty {
				break
			}
			if time.Now().After(until) {
				t.Fatal("expired challenge retained private snapshot")
			}
			time.Sleep(time.Millisecond)
		}
		retry, release := removalFacadeRequest(t, f, pins, t.Context())
		defer release()
		if intent, err := pins.ClaimParentListRemoval(retry, "target", "SYNTHETIC_LIST", token); intent != nil || err == nil {
			t.Fatal("receipt arrival renewed original deadline")
		}
	})
}

func TestParentListRemovalAndModeActionsRejectEachOtherBeforeIO(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	mode, stopMode := modeFacadeRequest(t, f, pins, t.Context())
	defer stopMode()
	removal, stopRemoval := removalFacadeRequest(t, f, pins, t.Context())
	defer stopRemoval()
	modeToken, err := pins.ParentModeChallenge(mode, "target", "rating")
	if err != nil {
		t.Fatal(err)
	}
	removalToken, err := pins.ParentListRemovalChallenge(removal, "target", "SYNTHETIC_LIST")
	if err != nil {
		t.Fatal(err)
	}
	before := f.ds.Pool().Stat().AcquireCount()
	if i, e := pins.ClaimParentListRemoval(removal, "target", "SYNTHETIC_LIST", modeToken); i != nil || e == nil {
		t.Fatal("mode token became removal")
	}
	if i, e := pins.ClaimParentMode(mode, "target", "rating", removalToken); i != nil || e == nil {
		t.Fatal("removal token became mode")
	}
	// The action tag is checked independently of external token shape.
	modeKey, _ := parentModeTokenKey(modeToken)
	if pins.claimParentConfirmation(modeKey, parentConfirmationListRemoval) != nil {
		t.Fatal("private action tag reinterpreted")
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("cross action claim performed IO")
	}
	a, err := pins.ClaimParentMode(mode, "target", "rating", modeToken)
	if err != nil {
		t.Fatal("wrong action consumed original mode", err)
	}
	defer a.Close()
	b, err := pins.ClaimParentListRemoval(removal, "target", "SYNTHETIC_LIST", removalToken)
	if err != nil {
		t.Fatal("wrong action consumed original removal", err)
	}
	defer b.Close()
}
func TestParentListRemovalAndModeShareOneActorBudget(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	mode, stopMode := modeFacadeRequest(t, f, pins, t.Context())
	defer stopMode()
	removal, stopRemoval := removalFacadeRequest(t, f, pins, t.Context())
	defer stopRemoval()
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
	for n := 0; n < parentModeMaximumActorEntries; n++ {
		var err error
		if n%2 == 0 {
			_, err = pins.ParentModeChallenge(mode, "target", "rating")
		} else {
			_, err = pins.ParentListRemovalChallenge(removal, "target", "SYNTHETIC_LIST")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pins.ParentModeChallenge(mode, "target", "rating"); err == nil {
		t.Fatal("mode bypassed shared actor budget")
	}
	if _, err := pins.ParentListRemovalChallenge(removal, "target", "SYNTHETIC_LIST"); err == nil {
		t.Fatal("removal doubled actor budget")
	}
	pins.parentModeMu.Lock()
	defer pins.parentModeMu.Unlock()
	if len(pins.parentModes) != parentModeMaximumActorEntries || pins.parentModeBytes > parentModeMaximumBytes {
		t.Fatal("shared registry bound exceeded")
	}
}
