package handlers

import (
	"os/exec"
	"testing"

	"novastream/api/openapi"
)

func TestProfileRecoveryActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	if output, err := exec.CommandContext(t.Context(), node, "--test", "testdata/profile_recovery.cjs").CombinedOutput(); err != nil {
		t.Fatalf("explicit profile recovery script: %v\n%s", err, output)
	}
}

// No PG opt-in is required to exercise the real closed/unavailable handler.
// Actual successful read/adopt, cookie identity and PIN authority are separate
// own-schema tests. A nil port never invents a successful JSON recovery.
func profileRecoveryOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	handler := NewProfileRecoveryHandler(nil, nil)
	for _, prefix := range []string{"/api", "/admin/api", "/account/api"} {
		call := contractCall{method: "POST", target: prefix + "/users/profile-a/state/reload", vars: map[string]string{"userID": "profile-a"}}
		method := handler.API
		if prefix != "/api" {
			method = handler.Cookie
		}
		exercise(t, v, call, method, 503)
	}
}
