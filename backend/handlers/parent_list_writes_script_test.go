package handlers

import (
	"os/exec"
	"testing"

	"novastream/api/openapi"
	"novastream/services/users"
)

func TestParentListWritesActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	if output, err := exec.Command(node, "--test", "testdata/parent_list_writes.cjs").CombinedOutput(); err != nil {
		t.Fatalf("bound list-write script: %v\n%s", err, output)
	}
}

// This is schema/legacy compatibility coverage, not PG authority. Actual
// registered API and cookie/PG probes cover the new private intents separately.
func parentListWritesOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount(contractAccountID, "Synthetic list writes")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewUsersHandler(service)
	for _, prefix := range []string{"/api", "/admin/api", "/account/api"} {
		path := prefix + "/users/" + profile.ID + "/kids/lists"
		call := contractCall{method: "POST", target: path, vars: map[string]string{"userID": profile.ID}, master: true, body: map[string]string{"url": " https://mdblist.com/lists/synthetic/雪?literal=%2F+ "}}
		exercise(t, v, call, handler.AddKidsAllowedList, 200)
		before, _ := service.Get(profile.ID)
		exercise(t, v, call, handler.AddKidsAllowedList, 200)
		after, _ := service.Get(profile.ID)
		if !before.UpdatedAt.Equal(after.UpdatedAt) {
			t.Fatal("legacy duplicate Add changed timestamp")
		}
		call.method = "PUT"
		call.body = map[string]any{"lists": []any{" first ", nil, "first", ""}}
		exercise(t, v, call, handler.SetKidsAllowedLists, 200)
		for _, body := range []any{map[string]any{}, map[string]any{"lists": nil}, "null"} {
			call.body = body
			exercise(t, v, call, handler.SetKidsAllowedLists, 200)
		}
	}
}
