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

func modeFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(ctx)
	r.Method = http.MethodPut
	r.URL.Path = "/api/users/target/kids/mode"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, stop, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, stop
}
func TestParentModeFacadeActualCanceledFirstHTTPAndTargetedWrite(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	first, stop := modeFacadeRequest(t, f, pins, t.Context())
	token, err := pins.ParentModeChallenge(first, "target", "rating")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	if _, valid := parentModeTokenKey(token); !valid {
		t.Fatal("challenge does not match the agreed opaque shape")
	}
	retry, release := modeFacadeRequest(t, f, pins, t.Context())
	defer release()
	intent, err := pins.ClaimParentMode(retry, "target", "rating", token)
	if err != nil {
		t.Fatal("HTTP completion destroyed bounded challenge", err)
	}
	defer intent.Close()
	if raw, _ := json.Marshal(intent); string(raw) != "null" || fmt.Sprintf("%+v", intent) != "ParentModeIntent(opaque)" {
		t.Fatal("intent exposes private state")
	}
	retry, err = pins.BoundParentModeRequest(retry, intent)
	if err != nil {
		t.Fatal(err)
	}
	retry.Header.Set("X-Account-Password", "synthetic actor account password")
	if err = pins.VerifyParentMode(retry, intent); err != nil {
		t.Fatal(err)
	}
	saved, err := f.users.CommitParentMode(retry.Context(), "target", intent)
	if err != nil || saved.KidsMode != "rating" {
		t.Fatal("actual users mode commit failed", err)
	}
	if _, err = pins.ClaimParentMode(retry, "target", "rating", token); !errors.Is(err, datastore.ErrParentModeChanged) {
		t.Fatal("claimed operation replayed")
	}
}
func TestParentModeFacadeInvalidClaimsNeverReplaceOriginalOperation(t *testing.T) {
	for _, mode := range []string{"bad token", "copy issuer", "wrong issuer", "wrong mode", "wrong path"} {
		t.Run(mode, func(t *testing.T) {
			f := issuerFacadePG(t)
			pins := NewPGProfilePins(f.ds)
			first, stop := modeFacadeRequest(t, f, pins, t.Context())
			defer stop()
			token, err := pins.ParentModeChallenge(first, "target", "rating")
			if err != nil {
				t.Fatal(err)
			}
			retry, release := modeFacadeRequest(t, f, pins, t.Context())
			defer release()
			current := pins
			wanted := "rating"
			switch mode {
			case "bad token":
				token = "pmi_bad"
			case "copy issuer":
				copy := *pins
				current = &copy
			case "wrong issuer":
				current = NewPGProfilePins(f.ds)
			case "wrong mode":
				wanted = ""
			case "wrong path":
				retry.URL.Path = "/account/api/users/target/kids/mode"
			}
			before := f.ds.Pool().Stat().AcquireCount()
			if intent, err := current.ClaimParentMode(retry, "target", wanted, token); err == nil || intent != nil {
				t.Fatal("invalid claim rebound to a fresh operation")
			}
			if f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("invalid local claim performed database acquisition")
			}
			pins.parentModeMu.Lock()
			for key, entry := range pins.parentModes {
				entry.timer.Stop()
				entry.intent.Close()
				delete(pins.parentModes, key)
			}
			pins.parentModeBytes = 0
			pins.parentModeMu.Unlock()
		})
	}
}
func TestParentModeFacadeBoundedMapAndAutomaticOriginalExpiry(t *testing.T) {
	t.Run("per actor maximum", func(t *testing.T) {
		f := issuerFacadePG(t)
		pins := NewPGProfilePins(f.ds)
		r, stop := modeFacadeRequest(t, f, pins, t.Context())
		defer stop()
		for range parentModeMaximumActorEntries {
			if _, err := pins.ParentModeChallenge(r, "target", "rating"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pins.ParentModeChallenge(r, "target", "rating"); !errors.Is(err, datastore.ErrParentModeUnavailable) {
			t.Fatal("unbounded actor challenge map")
		}
		pins.parentModeMu.Lock()
		if len(pins.parentModes) != 8 || pins.parentModeBytes > parentModeMaximumBytes {
			t.Fatal("challenge memory budget exceeded")
		}
		for _, entry := range pins.parentModes {
			entry.timer.Stop()
			entry.intent.Close()
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
		r, stop := modeFacadeRequest(t, f, pins, ctx)
		defer stop()
		token, err := pins.ParentModeChallenge(r, "target", "rating")
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
		retry, release := modeFacadeRequest(t, f, pins, t.Context())
		defer release()
		if intent, err := pins.ClaimParentMode(retry, "target", "rating", token); intent != nil || err == nil {
			t.Fatal("receipt arrival renewed original deadline")
		}
	})
}
