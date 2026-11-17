package handlers

import (
	"context"
	"os/exec"
	"testing"
	"time"
)

// The harness loads the actual shared script and actual page functions. Its
// synthetic fetch transport denies every request not expected by each case.
func TestParentProfileCreationClientActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, node, "--test", "testdata/parent_profile_creation_client.cjs").CombinedOutput(); err != nil {
		t.Fatalf("profile creation client: %v\n%s", err, output)
	}
}
