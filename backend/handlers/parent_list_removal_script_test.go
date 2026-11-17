package handlers

import (
	"net/url"
	"os/exec"
	"testing"

	"novastream/api/openapi"
	"novastream/services/users"
)

func TestParentListRemovalActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	if output, err := exec.Command(node, "--test", "testdata/parent_list_removal.cjs").CombinedOutput(); err != nil {
		t.Fatalf("bound list-removal script: %v\n%s", err, output)
	}
}

// Schema coverage uses the actual JSON handler; current PG authority is
// separately covered by registered API and real cookie/PG tests.
func parentListRemovalOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount(contractAccountID, "Synthetic list removal")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewUsersHandler(service)
	identity := "https://mdblist.com/lists/synthetic/quote-'\"-雪?encoded=%2F&literal=+"
	for _, prefix := range []string{"/api", "/admin/api", "/account/api"} {
		if _, err = service.SetKidsAllowedLists(profile.ID, []string{identity, "keep", identity}); err != nil {
			t.Fatal(err)
		}
		call := contractCall{method: "DELETE", target: prefix + "/users/" + profile.ID + "/kids/lists?url=" + url.QueryEscape(identity), vars: map[string]string{"userID": profile.ID}, master: true}
		exercise(t, v, call, handler.RemoveKidsAllowedList, 200)
		// Preserve the existing successful originally-absent identity contract.
		exercise(t, v, call, handler.RemoveKidsAllowedList, 200)
	}
}
