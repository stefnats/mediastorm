package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/users"
)

// Register every policy operation through actual handler request/response
// validation. With the synthetic PG opt-in this also exercises current original
// frames and successful fresh-parent writes; normal offline runs verify that the
// new operation cannot fall back to legacy mutable profile storage without PG.
func regionalChildPolicyOpenAPIContract(t *testing.T, validator *openapi.Validator) {
	t.Helper()
	policy := models.RegionalChildPolicy{Version: 1, Country: "DE", MaxMovieAge: 6, MaxEpisodeAge: 12}
	if os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL") == "" {
		service, err := users.NewService(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		handler := NewUsersHandler(service)
		for _, prefix := range []string{"/api", "/admin/api", "/account/api"} {
			for _, method := range []string{http.MethodGet, http.MethodPut} {
				call := contractCall{method: method, target: prefix + "/users/synthetic-profile/kids/regional-policy", vars: map[string]string{"userID": "synthetic-profile"}, master: true}
				if method == http.MethodPut {
					call.body = policy
				}
				exercise(t, validator, call, handler.RegionalChildPolicy, http.StatusServiceUnavailable)
			}
		}
		return
	}
	f := newPublicPinCarrierFixture(t)
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-contract-master-password")
	accountService, err := accounts.NewServiceWithStore(f.store)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewUsersHandler(f.users, accountService)
	handler.SetPGProfilePins(f.pins)
	call := func(method, path string, desired *models.RegionalChildPolicy) *httptest.ResponseRecorder {
		var raw []byte
		if desired != nil {
			var err error
			raw, err = json.Marshal(desired)
			if err != nil {
				t.Fatal(err)
			}
		}
		request := httptest.NewRequest(method, "http://synthetic.test"+path, bytes.NewReader(raw))
		request = mux.SetURLVars(request, map[string]string{"userID": f.profile.ID})
		request.Header.Set("Authorization", "Bearer "+f.session.Token)
		request.Header.Set("X-Client-ID", "synthetic-client")
		if desired != nil {
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Account-Password", "synthetic-parent-password")
		}
		if strings.HasPrefix(path, "/admin/") || strings.HasPrefix(path, "/account/") {
			request.Header.Set("Origin", "http://synthetic.test")
			request.Header.Set("Sec-Fetch-Site", "same-origin")
		}
		if err := validator.ValidateRequest(request, raw); err != nil {
			t.Fatal(err)
		}
		checked, release, err := f.pins.Authenticate(request, f.session.Token, "synthetic-client")
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		reply := httptest.NewRecorder()
		handler.RegionalChildPolicy(reply, checked)
		if reply.Code != 200 {
			t.Fatalf("current policy contract failed: method=%s HTTP=%d", method, reply.Code)
		}
		if err = validator.ValidateResponse(request, reply.Code, reply.Header(), reply.Body.Bytes()); err != nil {
			t.Fatal(err)
		}
		return reply
	}
	for index, prefix := range []string{"/api", "/admin/api", "/account/api"} {
		path := prefix + "/users/" + f.profile.ID + "/kids/regional-policy"
		before := call(http.MethodGet, path, nil)
		if index == 0 && strings.TrimSpace(before.Body.String()) != "null" {
			t.Fatal("unconfigured policy was invented")
		}
		saved := call(http.MethodPut, path, &policy)
		read := call(http.MethodGet, path, nil)
		var current, committed models.RegionalChildPolicy
		if json.Unmarshal(saved.Body.Bytes(), &committed) != nil || json.Unmarshal(read.Body.Bytes(), &current) != nil || current != policy || committed != policy {
			t.Fatal("current PG policy response differs from exact committed configuration")
		}
	}
}

func TestRegionalChildPolicyOpenAPI(t *testing.T) {
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	regionalChildPolicyOpenAPIContract(t, validator)
}
