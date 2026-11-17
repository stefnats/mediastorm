package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/accounts"
)

type modeGateAccounts struct {
	*accounts.Service
	entered, release chan struct{}
	block            atomic.Bool
	calls            atomic.Int64
}

func (a *modeGateAccounts) AuthenticateContext(ctx context.Context, user, password string) (account models.Account, err error) {
	a.calls.Add(1)
	if a.block.CompareAndSwap(true, false) {
		close(a.entered)
		select {
		case <-a.release:
		case <-ctx.Done():
			return account, ctx.Err()
		}
	}
	return a.Service.AuthenticateContext(ctx, user, password)
}
func TestParentModeOriginalChallengeDeadlineBoundsActualSharedGuard(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	actual, err := accounts.NewServiceWithStore(f.store)
	if err != nil {
		t.Fatal(err)
	}
	verifier := &modeGateAccounts{Service: actual, entered: make(chan struct{}), release: make(chan struct{})}
	verifier.block.Store(true)
	handler := NewUsersHandler(f.users, verifier)
	handler.SetPGProfilePins(f.pins)
	path := "/api/users/" + f.profile.ID + "/kids/mode"
	// Prepare a challenge with a short genuine original HTTP deadline.
	parent, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
	defer cancel()
	first, release, err := f.pins.Authenticate(httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"mode":"rating"}`)).WithContext(parent), f.session.Token, "synthetic-client")
	if err != nil {
		t.Fatal(err)
	}
	first = mux.SetURLVars(first, map[string]string{"userID": f.profile.ID})
	response := httptest.NewRecorder()
	handler.SetKidsMode(response, first)
	release()
	var challenge struct {
		Intent string `json:"parentIntent"`
	}
	if err = json.Unmarshal(response.Body.Bytes(), &challenge); err != nil || response.Code != 403 || challenge.Intent == "" {
		t.Fatal("first challenge unavailable")
	}
	held, stop := f.request(t, http.MethodPut, path)
	defer stop()
	held.Header.Set("X-Account-Password", "synthetic-parent-password")
	if auth.GetAccountID(held) != f.session.AccountID {
		t.Fatal("guard holder lost original account")
	}
	heldDone := make(chan bool, 1)
	go func() { heldDone <- handler.parentAuthorization.RequireFresh(httptest.NewRecorder(), held) }()
	<-verifier.entered
	retry, finish, err := f.pins.Authenticate(httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"mode":"rating"}`)), f.session.Token, "synthetic-client")
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
	go func() { handler.SetKidsMode(result, retry); close(done) }()
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
	if err != nil || profile.KidsMode != "" {
		close(verifier.release)
		t.Fatal("expired gate waiter wrote mode")
	}
	close(verifier.release)
	if !<-heldDone {
		t.Fatal("independent valid guard holder failed")
	}
}
