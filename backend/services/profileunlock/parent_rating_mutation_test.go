package profileunlock

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"

	"novastream/internal/datastore"
)

func ratingFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins, kind string) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(t.Context())
	r.Method = http.MethodPut
	r.URL.Path = "/api/users/target/kids/rating"
	if kind == "movie" {
		r.URL.Path += "/movie"
	}
	if kind == "tv" {
		r.URL.Path += "/tv"
	}
	if kind == "pair" {
		r.URL.Path = "/api/users/target/kids/ratings"
	}
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func TestParentRatingFacadeActualPairHandoffAndAck(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	first, release := ratingFacadeRequest(t, f, pins, "pair")
	body := []byte(`{"movieRating":"movie-x","tvRating":"tv-y"}`)
	original := bytes.Clone(body)
	token, err := pins.ParentRatingPairChallenge(first, "target", body)
	release()
	if err != nil {
		t.Fatal(err)
	}
	body[3] = 'X'
	retry, stop := ratingFacadeRequest(t, f, pins, "pair")
	defer stop()
	copyFacade := *pins
	before := f.ds.Pool().Stat().AcquireCount()
	if i, e := copyFacade.ClaimParentRatingPair(retry, "target", original, token); i != nil || e == nil {
		t.Fatal("copied issuer adopted rating")
	}
	if i, e := NewPGProfilePins(f.ds).ClaimParentRatingPair(retry, "target", original, token); i != nil || e == nil {
		t.Fatal("foreign issuer adopted rating")
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("invalid issuer performed SQL")
	}
	i, err := pins.ClaimParentRatingPair(retry, "target", original, token)
	if err != nil {
		t.Fatal("completed first HTTP or caller body mutation destroyed original", err)
	}
	defer i.Close()
	retry, err = pins.BoundParentRatingRequest(retry, i)
	if err != nil {
		t.Fatal(err)
	}
	retry.Header.Set("X-Account-Password", "synthetic actor account password")
	if err = pins.VerifyParentRating(retry, i); err != nil {
		t.Fatal(err)
	}
	saved, err := f.users.CommitParentRatingPair(retry.Context(), "target", i)
	if err != nil || saved.KidsMaxMovieRating != "movie-x" || saved.KidsMaxTVRating != "tv-y" {
		t.Fatal("actual pair failed", err)
	}
	if _, err = pins.ClaimParentRatingPair(retry, "target", original, token); err == nil {
		t.Fatal("rating challenge replayed")
	}
	if _, err = pins.ParentRatingPairChallenge(first, "target", bytes.Repeat([]byte(" "), datastore.ParentRatingBodyLimit+1)); !errors.Is(err, datastore.ErrParentRatingBodyTooLarge) {
		t.Fatal("rating raw cap missing", err)
	}
}

type ratingFacadeAction struct {
	request   *http.Request
	challenge func() (string, error)
	claim     func(string) error
	tag       parentConfirmationAction
}

func ratingAllActions(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins) []ratingFacadeAction {
	t.Helper()
	mode, sm := modeFacadeRequest(t, f, pins, t.Context())
	t.Cleanup(sm)
	removal, sr := removalFacadeRequest(t, f, pins, t.Context())
	t.Cleanup(sr)
	add, sa := writesFacadeRequest(t, f, pins, false, nil)
	t.Cleanup(sa)
	replace, sp := writesFacadeRequest(t, f, pins, true, nil)
	t.Cleanup(sp)
	scalar := []byte(`{"rating":"x"}`)
	pairBody := []byte(`{"movieRating":"x","tvRating":"y"}`)
	actions := []ratingFacadeAction{
		{mode, func() (string, error) { return pins.ParentModeChallenge(mode, "target", "rating") }, func(token string) error {
			i, e := pins.ClaimParentMode(mode, "target", "rating", token)
			if i != nil {
				i.Close()
			}
			return e
		}, parentConfirmationMode},
		{removal, func() (string, error) { return pins.ParentListRemovalChallenge(removal, "target", "SYNTHETIC_LIST") }, func(token string) error {
			i, e := pins.ClaimParentListRemoval(removal, "target", "SYNTHETIC_LIST", token)
			if i != nil {
				i.Close()
			}
			return e
		}, parentConfirmationListRemoval},
		{add, func() (string, error) { return pins.ParentListAddChallenge(add, "target", []byte(`{"url":"x"}`)) }, func(token string) error {
			i, e := pins.ClaimParentListAdd(add, "target", []byte(`{"url":"x"}`), token)
			if i != nil {
				i.Close()
			}
			return e
		}, parentConfirmationListAdd},
		{replace, func() (string, error) {
			return pins.ParentListReplaceChallenge(replace, "target", []byte(`{"lists":["x"]}`))
		}, func(token string) error {
			i, e := pins.ClaimParentListReplace(replace, "target", []byte(`{"lists":["x"]}`), token)
			if i != nil {
				i.Close()
			}
			return e
		}, parentConfirmationListReplace},
	}
	for _, kind := range []string{"generic", "movie", "tv", "pair"} {
		r, stop := ratingFacadeRequest(t, f, pins, kind)
		t.Cleanup(stop)
		var challenge func() (string, error)
		var claim func(string) error
		var tag parentConfirmationAction
		switch kind {
		case "generic":
			tag = parentConfirmationRatingGeneric
			challenge = func() (string, error) { return pins.ParentRatingGenericChallenge(r, "target", scalar) }
			claim = func(token string) error {
				i, e := pins.ClaimParentRatingGeneric(r, "target", scalar, token)
				if i != nil {
					i.Close()
				}
				return e
			}
		case "movie":
			tag = parentConfirmationRatingMovie
			challenge = func() (string, error) { return pins.ParentRatingMovieChallenge(r, "target", scalar) }
			claim = func(token string) error {
				i, e := pins.ClaimParentRatingMovie(r, "target", scalar, token)
				if i != nil {
					i.Close()
				}
				return e
			}
		case "tv":
			tag = parentConfirmationRatingTV
			challenge = func() (string, error) { return pins.ParentRatingTVChallenge(r, "target", scalar) }
			claim = func(token string) error {
				i, e := pins.ClaimParentRatingTV(r, "target", scalar, token)
				if i != nil {
					i.Close()
				}
				return e
			}
		case "pair":
			tag = parentConfirmationRatingPair
			challenge = func() (string, error) { return pins.ParentRatingPairChallenge(r, "target", pairBody) }
			claim = func(token string) error {
				i, e := pins.ClaimParentRatingPair(r, "target", pairBody, token)
				if i != nil {
					i.Close()
				}
				return e
			}
		}
		actions = append(actions, ratingFacadeAction{r, challenge, claim, tag})
	}
	return actions
}
func TestParentRatingAllEightActionsIsolatedBeforeClaimIO(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	actions := ratingAllActions(t, f, pins)
	tokens := make([]string, len(actions))
	for n, action := range actions {
		token, err := action.challenge()
		if err != nil {
			t.Fatal(err)
		}
		tokens[n] = token
	}
	before := f.ds.Pool().Stat().AcquireCount()
	for source, token := range tokens {
		for target, action := range actions {
			if source != target && action.claim(token) == nil {
				t.Fatal("cross-action rating token adopted")
			}
		}
	}
	// The private tag is checked independently of the token's public prefix.
	for n, token := range tokens {
		prefix := token[:4]
		key, ok := parentConfirmationTokenKey(token, prefix)
		if !ok {
			t.Fatal("invalid generated token")
		}
		wrong := parentConfirmationMode
		if actions[n].tag == wrong {
			wrong = parentConfirmationRatingPair
		}
		if pins.claimParentConfirmation(key, wrong) != nil {
			t.Fatal("private action tag was ignored")
		}
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("cross-action claim stages performed SQL")
	}
	for n, action := range actions {
		if err := action.claim(tokens[n]); err != nil {
			t.Fatal("wrong action consumed original slot", n, err)
		}
	}
}
func TestParentRatingEightActionsShareOneActorBudget(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	actions := ratingAllActions(t, f, pins)
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
	for _, action := range actions {
		if _, err := action.challenge(); err != nil {
			t.Fatal(err)
		}
	}
	for n, action := range actions {
		if _, err := action.challenge(); err == nil {
			t.Fatal("action has an independent actor budget", n)
		}
	}
	pins.parentModeMu.Lock()
	defer pins.parentModeMu.Unlock()
	if len(pins.parentModes) != 8 || pins.parentModeBytes > parentModeMaximumBytes {
		t.Fatal("shared parent memory/count budget escaped")
	}
}
func TestParentRatingFacadeBadRawBodyDoesNotRebind(t *testing.T) {
	f := issuerFacadePG(t)
	pins := NewPGProfilePins(f.ds)
	r, stop := ratingFacadeRequest(t, f, pins, "pair")
	defer stop()
	body := []byte(`{"movieRating":"x","tvRating":"y"}`)
	token, err := pins.ParentRatingPairChallenge(r, "target", body)
	if err != nil {
		t.Fatal(err)
	}
	before := f.ds.Pool().Stat().AcquireCount()
	if i, e := pins.ClaimParentRatingPair(r, "target", append(bytes.Clone(body), ' '), token); i != nil || e == nil {
		t.Fatal("semantic-equivalent changed raw body adopted")
	}
	if f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("changed raw body reached SQL")
	}
}
