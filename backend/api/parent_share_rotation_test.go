package api

import (
	"encoding/json"
	"regexp"
	"sort"
	"testing"
)

var parentShareUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Migration 088 also rotates the stored share identity when these existing
// parent-setter values change. Every other column stays in the whole-row check.
func parentExpectedFieldsWithShareRotation(t *testing.T, before, after map[string]json.RawMessage, expected []string) []string {
	t.Helper()
	var oldID, newID string
	if json.Unmarshal(before["share_incarnation"], &oldID) != nil || json.Unmarshal(after["share_incarnation"], &newID) != nil ||
		!parentShareUUID.MatchString(oldID) || !parentShareUUID.MatchString(newID) ||
		oldID == "00000000-0000-0000-0000-000000000000" || newID == "00000000-0000-0000-0000-000000000000" {
		t.Fatal("parent mutation did not retain canonical nonzero stored share identities")
	}

	rotateExpected := false
	for _, field := range expected {
		switch field {
		case "is_kids_profile", "kids_mode", "kids_max_rating", "kids_max_movie_rating", "kids_max_tv_rating", "kids_allowed_lists":
			rotateExpected = true
		case "share_incarnation":
			t.Fatal("share rotation must follow the already expected setter fields")
		}
	}
	if rotateExpected && oldID == newID {
		t.Fatal("changed parent restriction did not rotate the stored share identity")
	}
	if !rotateExpected && oldID != newID {
		t.Fatal("unchanged parent restrictions unexpectedly rotated the stored share identity")
	}

	wanted := append([]string(nil), expected...)
	if rotateExpected {
		wanted = append(wanted, "share_incarnation")
	}
	sort.Strings(wanted)
	return wanted
}
