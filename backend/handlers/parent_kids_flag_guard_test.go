package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/services/accounts"
	"novastream/services/user_settings"
)

func TestParentKidsFlagOriginalDeadlineBoundsActualSharedGuard(t *testing.T) {
	for _, management := range []bool{false, true} {
		t.Run(fmt.Sprint(management), func(t *testing.T) {
			f := newPublicPinCarrierFixture(t)
			actual, e := accounts.NewServiceWithStore(f.store)
			if e != nil {
				t.Fatal(e)
			}
			verifier := &modeGateAccounts{Service: actual, entered: make(chan struct{}), release: make(chan struct{})}
			verifier.block.Store(true)
			h := NewUsersHandler(f.users, verifier)
			h.SetPGProfilePins(f.pins)
			settings, e := user_settings.NewServiceWithStore(f.store)
			if e != nil {
				t.Fatal(e)
			}
			body := `{"isKidsProfile":true}`
			path := "/api/users/" + f.profile.ID + "/kids-profile"
			if management {
				path = "/account/api/profiles/kids?profileId=" + f.profile.ID
			}
			apply := func(w http.ResponseWriter, r *http.Request) {
				if management {
					h.SetKidsProfileManagement(w, r, f.profile.ID, settings)
				} else {
					h.SetKidsProfile(w, r)
				}
			}
			parent, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
			defer cancel()
			first, release, e := f.pins.Authenticate(httptest.NewRequest(http.MethodPut, path, strings.NewReader(body)).WithContext(parent), f.session.Token, "synthetic-client")
			if e != nil {
				t.Fatal(e)
			}
			first = mux.SetURLVars(first, map[string]string{"userID": f.profile.ID})
			response := httptest.NewRecorder()
			apply(response, first)
			release()
			var challenge struct {
				Intent string `json:"parentIntent"`
			}
			if json.Unmarshal(response.Body.Bytes(), &challenge) != nil || response.Code != 403 || challenge.Intent == "" {
				t.Fatal("first challenge unavailable", response.Code)
			}
			held, stop := f.request(t, http.MethodPut, path)
			defer stop()
			held.Header.Set("X-Account-Password", "synthetic-parent-password")
			heldDone := make(chan bool, 1)
			go func() { heldDone <- h.parentAuthorization.RequireFresh(httptest.NewRecorder(), held) }()

			var once sync.Once
			holderJoined := false
			unblock := func() { once.Do(func() { close(verifier.release) }) }
			t.Cleanup(func() {
				unblock()
				if holderJoined {
					return
				}
				select {
				case <-heldDone:
					holderJoined = true
				case <-time.After(3 * time.Second):
					t.Error("held guard did not finish")
				}
			})
			select {
			case <-verifier.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("actual shared verifier not entered")
			}

			retry, finish, e := f.pins.Authenticate(httptest.NewRequest(http.MethodPut, path, strings.NewReader(body)), f.session.Token, "synthetic-client")
			if e != nil {
				t.Fatal(e)
			}
			defer finish()
			retry = mux.SetURLVars(retry, map[string]string{"userID": f.profile.ID})
			retry.Header.Set("X-Parent-Intent", challenge.Intent)
			retry.Header.Set("X-Account-Password", "synthetic-parent-password")
			result := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { apply(result, retry); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				unblock()
				<-done
				t.Fatal("actual shared guard exceeded original challenge deadline")
			}
			if verifier.calls.Load() != 1 {
				t.Fatal("expired waiter reached bcrypt")
			}
			var denial struct {
				Code string `json:"code"`
			}
			if json.Unmarshal(result.Body.Bytes(), &denial) != nil || result.Code != 403 || denial.Code != "parent_authorization_required" {
				t.Fatal("waiter did not reach original bounded guard", result.Code)
			}
			if retry.Context().Err() != nil {
				t.Fatal("outer retry expired instead of original confirmation budget")
			}
			profile, e := f.store.Users().Get(t.Context(), f.profile.ID)
			if e != nil || profile.IsKidsProfile != f.profile.IsKidsProfile {
				t.Fatal("expired waiter wrote profile")
			}
			value, e := settings.Get(f.profile.ID)
			if e != nil || value != nil {
				t.Fatal("expired waiter wrote defaults")
			}
			unblock()
			select {
			case ok := <-heldDone:
				holderJoined = true
				if !ok {
					t.Fatal("independent original guard holder failed")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("valid guard holder did not finish before its context release")
			}
		})
	}
}
