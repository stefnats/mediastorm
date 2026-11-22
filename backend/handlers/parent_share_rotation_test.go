package handlers

import (
	"bytes"
	"encoding/json"
	"regexp"
	"testing"
)

var parentCookieShareUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Migration 088 rotates the stored share identity for these already expected
// parent-setter changes. Every other column remains in the whole-row check.
func parentExpectedCookieShareRotation(t *testing.T, before, after, expected map[string]json.RawMessage) {
	t.Helper()
	var oldID, newID string
	if json.Unmarshal(before["share_incarnation"], &oldID) != nil || json.Unmarshal(after["share_incarnation"], &newID) != nil ||
		!parentCookieShareUUID.MatchString(oldID) || !parentCookieShareUUID.MatchString(newID) ||
		oldID == "00000000-0000-0000-0000-000000000000" || newID == "00000000-0000-0000-0000-000000000000" {
		t.Fatal("parent mutation did not retain canonical nonzero stored share identities")
	}

	rotateExpected := false
	for _, field := range []string{"is_kids_profile", "kids_mode", "kids_max_rating", "kids_max_movie_rating", "kids_max_tv_rating", "kids_allowed_lists"} {
		if !bytes.Equal(before[field], expected[field]) {
			rotateExpected = true
		}
	}
	if rotateExpected && oldID == newID {
		t.Fatal("changed parent restriction did not rotate the stored share identity")
	}
	if !rotateExpected && oldID != newID {
		t.Fatal("unchanged parent restrictions unexpectedly rotated the stored share identity")
	}
	if rotateExpected {
		expected["share_incarnation"] = after["share_incarnation"]
	}
}
