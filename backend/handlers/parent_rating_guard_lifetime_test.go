package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/services/accounts"
)

func TestParentRatingOriginalChallengeDeadlineBoundsActualSharedGuard(t *testing.T) {
	for _, kind := range []parentRatingAction{parentRatingGeneric, parentRatingMovie, parentRatingTV, parentRatingPair} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			method, body := http.MethodPut, `{"rating":"new-value"}`
			if kind == parentRatingPair {
				body = `{"movieRating":"movie-x","tvRating":"tv-y"}`
			}

			f := newPublicPinCarrierFixture(t)
			actual, err := accounts.NewServiceWithStore(f.store)
			if err != nil {
				t.Fatal(err)
			}
			verifier := &modeGateAccounts{Service: actual, entered: make(chan struct{}), release: make(chan struct{})}
			verifier.block.Store(true)
			handler := NewUsersHandler(f.users, verifier)
			handler.SetPGProfilePins(f.pins)
			path := "/api/users/" + f.profile.ID + "/kids/rating"
			if kind == parentRatingMovie {
				path += "/movie"
			}
			if kind == parentRatingTV {
				path += "/tv"
			}
			if kind == parentRatingPair {
				path = "/api/users/" + f.profile.ID + "/kids/ratings"
			}
			apply := func(w http.ResponseWriter, r *http.Request) {
				switch kind {
				case parentRatingGeneric:
					handler.SetKidsMaxRating(w, r)
				case parentRatingMovie:
					handler.SetKidsMaxMovieRating(w, r)
				case parentRatingTV:
					handler.SetKidsMaxTVRating(w, r)
				case parentRatingPair:
					handler.SetKidsRatings(w, r)
				}
			}
			// Prepare a challenge with a short genuine original HTTP deadline.
			parent, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
			defer cancel()
			first, release, err := f.pins.Authenticate(httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(parent), f.session.Token, "synthetic-client")
			if err != nil {
				t.Fatal(err)
			}
			first = mux.SetURLVars(first, map[string]string{"userID": f.profile.ID})
			response := httptest.NewRecorder()
			apply(response, first)
			release()
			var challenge struct {
				Intent string `json:"parentIntent"`
			}
			if err = json.Unmarshal(response.Body.Bytes(), &challenge); err != nil || response.Code != 403 || challenge.Intent == "" {
				t.Fatal("first challenge unavailable")
			}
			held, stop := f.request(t, method, path)
			defer stop()
			held.Header.Set("X-Account-Password", "synthetic-parent-password")
			if auth.GetAccountID(held) != f.session.AccountID {
				t.Fatal("guard holder lost original account")
			}
			heldDone := make(chan bool, 1)
			go func() { heldDone <- handler.parentAuthorization.RequireFresh(httptest.NewRecorder(), held) }()
			<-verifier.entered
			retry, finish, err := f.pins.Authenticate(httptest.NewRequest(method, path, strings.NewReader(body)), f.session.Token, "synthetic-client")
			if err != nil {
				close(verifier.release)
				t.Fatal(err)
			}
			defer finish()
			retry = mux.SetURLVars(retry, map[string]string{"userID": f.profile.ID})
			retry.Header.Set("X-Parent-Intent", challenge.Intent)
			retry.Header.Set("X-Account-Password", "synthetic-parent-password")
			result := httptest.NewRecorder()
			done := make(chan struct{})
			go func() {
				apply(result, retry)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				close(verifier.release)
				t.Fatal("password gate waited beyond original challenge lifetime")
			}
			if verifier.calls.Load() != 1 {
				close(verifier.release)
				t.Fatal("expired waiter reached credential bcrypt")
			}
			var denial struct {
				Code string `json:"code"`
			}
			if err = json.Unmarshal(result.Body.Bytes(), &denial); err != nil || result.Code != http.StatusForbidden || denial.Code != "parent_authorization_required" {
				close(verifier.release)
				t.Fatal("request did not reach the actual bounded password guard", result.Code)
			}
			if retry.Context().Err() != nil {
				close(verifier.release)
				t.Fatal("outer HTTP request expired instead of original challenge guard")
			}
			profile, err := f.store.Users().Get(t.Context(), f.profile.ID)
			if err != nil || profile.KidsMaxRating != f.profile.KidsMaxRating || profile.KidsMaxMovieRating != f.profile.KidsMaxMovieRating || profile.KidsMaxTVRating != f.profile.KidsMaxTVRating {
				close(verifier.release)
				t.Fatal("expired gate waiter changed ratings")
			}
			close(verifier.release)
			if !<-heldDone {
				t.Fatal("independent valid guard holder failed")
			}

		})
	}
}
