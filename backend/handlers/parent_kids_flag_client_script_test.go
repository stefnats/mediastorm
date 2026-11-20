package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/users"
)

const parentLegacyFixturePassword = "synthetic-legacy-parent-password"

// The schema fixture's explicit acct-1 context has a real JSON account and
// password verifier. This creates no PG authority and never adds request proof.
func parentLegacyContractAccounts(t *testing.T) *accounts.Service {
	t.Helper()
	directory := t.TempDir()
	hash, err := bcrypt.GenerateFromPassword([]byte(parentLegacyFixturePassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal("hash synthetic legacy password")
	}
	stored := []models.AccountStorage{{ID: contractAccountID, Username: "synthetic-legacy-parent", PasswordHash: string(hash), IsMaster: true}}
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatal("encode synthetic account storage")
	}
	if err := os.WriteFile(filepath.Join(directory, "accounts.json"), raw, 0600); err != nil {
		t.Fatal("write own synthetic account storage")
	}
	service, err := accounts.NewService(directory)
	if err != nil {
		t.Fatal("load actual legacy account service")
	}
	return service
}

func TestParentKidsFlagClientActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	if output, err := exec.CommandContext(t.Context(), node, "--test", "testdata/parent_kids_flag_client.cjs").CombinedOutput(); err != nil {
		t.Fatalf("profile flag client: %v\n%s", err, output)
	}
}

// This explicit test session is the AdminUI carrier consumed by getPageRoleInfo.
// It is separate from contractCall's API auth context, not a cookie login proof.
func parentKidsFlagClientLegacySession(r *http.Request) *http.Request {
	session := &models.Session{AccountID: contractAccountID, IsMaster: true}
	return r.WithContext(context.WithValue(r.Context(), adminSessionContextKey{}, session))
}

// Canonical schema requests use actual JSON services and handlers. Fresh PG
// authority, compound defaults and cookie middleware are separate tests.
func parentKidsFlagClientOpenAPIContract(t *testing.T, v *openapi.Validator) {
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount(contractAccountID, "Synthetic flag schema")
	if err != nil {
		t.Fatal(err)
	}
	actualAccounts := parentLegacyContractAccounts(t)
	handler := NewUsersHandler(service, actualAccounts)
	admin := NewAdminUIHandler("", "", nil, service, nil, nil)
	admin.SetAccountsService(actualAccounts)
	for _, desired := range []bool{true, false} {
		exercise(t, v, contractCall{method: "PUT", target: "/api/users/" + profile.ID + "/kids-profile", vars: map[string]string{"userID": profile.ID}, master: true, headers: map[string]string{"X-Account-Password": parentLegacyFixturePassword}, body: map[string]bool{"isKidsProfile": desired}}, handler.SetKidsProfile, 200)
		for _, prefix := range []string{"/admin", "/account"} {
			exercise(t, v, contractCall{method: "PUT", target: prefix + "/api/profiles/kids?profileId=" + profile.ID, master: true, headers: map[string]string{"X-Account-Password": parentLegacyFixturePassword}, body: map[string]bool{"isKidsProfile": desired}}, func(w http.ResponseWriter, r *http.Request) {
				admin.SetKidsProfile(w, parentKidsFlagClientLegacySession(r))
			}, 200)
		}
	}
}

// The old JSON serializer's zero booleans are not a fresh rights observation.
// This checks the actual handler before testing the client provenance rule.
func TestParentKidsFlagClientLegacyReplyAndProfilesRights(t *testing.T) {
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	user, err := service.CreateForAccount(contractAccountID, "Synthetic legacy rights")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.SetLivePermissions(user.ID, true, true); err != nil {
		t.Fatal(err)
	}
	admin := NewAdminUIHandler("", "", nil, service, nil, nil)
	admin.SetAccountsService(parentLegacyContractAccounts(t))
	call := contractCall{method: "PUT", target: "/account/api/profiles/kids?profileId=" + user.ID, master: true, body: map[string]bool{"isKidsProfile": false}}
	before, _ := service.Get(user.ID)
	for _, password := range []string{"", "synthetic-wrong-parent-password"} {
		denied := call
		denied.headers = map[string]string{"X-Account-Password": password}
		r, _ := denied.request(t)
		w := httptest.NewRecorder()
		admin.SetKidsProfile(w, parentKidsFlagClientLegacySession(r))
		after, _ := service.Get(user.ID)
		if w.Code != http.StatusForbidden || !reflect.DeepEqual(before, after) {
			t.Fatal("unconfirmed master changed legacy rights")
		}
	}
	call.headers = map[string]string{"X-Account-Password": parentLegacyFixturePassword}
	r, _ := call.request(t)
	w := httptest.NewRecorder()
	admin.SetKidsProfile(w, parentKidsFlagClientLegacySession(r))
	var partial ProfileWithPinStatus
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &partial) != nil || partial.AllowLiveTakeover || partial.AllowRecordingInterruption {
		t.Fatalf("legacy projection changed: status=%d", w.Code)
	}
	kept, _ := service.Get(user.ID)
	if !kept.AllowLiveTakeover || !kept.AllowRecordingInterruption {
		t.Fatal("legacy disable changed stored rights")
	}
	call = contractCall{method: "GET", target: "/account/api/profiles", master: true}
	r, _ = call.request(t)
	w = httptest.NewRecorder()
	admin.GetProfiles(w, parentKidsFlagClientLegacySession(r))
	var profiles []ProfileWithPinStatus
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &profiles) != nil {
		t.Fatalf("actual profiles read failed: status=%d", w.Code)
	}
	for _, actual := range profiles {
		if actual.ID == user.ID {
			if !actual.AllowLiveTakeover || !actual.AllowRecordingInterruption {
				t.Fatal("GET omitted actual rights")
			}
			return
		}
	}
	t.Fatal("stored profile absent from GET")
}
