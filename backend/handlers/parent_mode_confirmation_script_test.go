package handlers

import (
	"os/exec"
	"testing"

	"novastream/api/openapi"
	"novastream/services/users"
)

func TestParentModeConfirmationActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	if output, err := exec.Command(node, "--test", "testdata/parent_mode_confirmation.cjs").CombinedOutput(); err != nil {
		t.Fatalf("bound parent confirmation behavior: %v\n%s", err, output)
	}
}

// This schema-only legacy control needs no PG opt-in. Actual current PG
// authority and confirmation are exercised separately by API/cookie tests.
func parentModeOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount(contractAccountID, "Synthetic mode contract")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.SetKidsProfile(profile.ID, true); err != nil {
		t.Fatal(err)
	}
	handler := NewUsersHandler(service, parentLegacyContractAccounts(t))
	for _, prefix := range []string{"/api", "/admin/api", "/account/api"} {
		for _, mode := range []string{"", "rating", "content_list"} {
			exercise(t, v, contractCall{
				method: "PUT", target: prefix + "/users/" + profile.ID + "/kids/mode",
				vars: map[string]string{"userID": profile.ID}, master: true,
				headers: map[string]string{"X-Account-Password": parentLegacyFixturePassword},
				body:    map[string]string{"mode": mode},
			}, handler.SetKidsMode, 200)
		}
	}
}
