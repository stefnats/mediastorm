package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const parentListAPIURL = "https://lists.example.test/café/a'b?term=Köln+100%25&literal=%41"
const parentListAPIKeep = "https://lists.example.test/keep?first=1&second=2"

func parentListAPIFixture(t *testing.T) *publicPGPinFixture {
	t.Helper()
	f := parentModeAPIFixture(t)
	if _, err := f.users.SetKidsAllowedLists(f.open, []string{parentListAPIURL, parentListAPIKeep, parentListAPIURL}); err != nil {
		t.Fatal(err)
	}
	return f
}

func parentListAPIPath(profile, list string) string {
	return "/api/users/" + profile + "/kids/lists?url=" + url.QueryEscape(list)
}

func parentListAPIRequest(path, token, intent, password string) *http.Request {
	r := httptest.NewRequest("DELETE", path, nil)
	r.Header.Set("X-Client-ID", "synthetic-pg-api-client")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if intent != "" {
		r.Header.Set("X-Parent-Intent", intent)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}

func parentListAPIChallenge(t *testing.T, f *publicPGPinFixture, path, token string) string {
	t.Helper()
	before := parentModeAPIRow(t, f, f.open)
	w := parentModeAPIServe(f, parentListAPIRequest(path, token, "", ""))
	var value struct {
		Code         string `json:"code"`
		ParentIntent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || !strings.HasPrefix(value.ParentIntent, "pli_") || len(value.ParentIntent) != 68 {
		t.Fatalf("registered list removal did not challenge: HTTP %d", w.Code)
	}
	decoded, err := hex.DecodeString(value.ParentIntent[4:])
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value.ParentIntent[4:] {
		t.Fatal("list removal challenge is not canonical")
	}
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("unconfirmed removal changed the original profile")
	}
	return value.ParentIntent
}

func TestParentListRemovalRegisteredExactIdentityAndNarrowCommit(t *testing.T) {
	for _, name := range []string{"regular duplicate URL", "master duplicate URL", "first decoded query value", "originally absent URL"} {
		t.Run(name, func(t *testing.T) {
			f := parentListAPIFixture(t)
			if name == "master duplicate URL" {
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			}
			token := f.session(false)
			path := parentListAPIPath(f.open, parentListAPIURL)
			want := []string{parentListAPIKeep}
			fields := []string{"kids_allowed_lists", "updated_at"}
			if name == "first decoded query value" {
				path += "&url=" + url.QueryEscape(parentListAPIKeep)
			}
			if name == "originally absent URL" {
				path = parentListAPIPath(f.open, "https://lists.example.test/absent")
				want = []string{parentListAPIURL, parentListAPIKeep, parentListAPIURL}
				fields = []string{"updated_at"}
			}
			before := parentModeAPIRow(t, f, f.open)
			other := parentModeAPIRow(t, f, f.foreign)
			intent := parentListAPIChallenge(t, f, path, token)
			w := parentModeAPIServe(f, parentListAPIRequest(path, token, intent, "synthetic-api-password"))
			if w.Code != 200 {
				t.Fatalf("original confirmed removal failed: HTTP %d", w.Code)
			}
			after := parentModeAPIRow(t, f, f.open)
			var lists []string
			if json.Unmarshal(after["kids_allowed_lists"], &lists) != nil || !reflect.DeepEqual(lists, want) {
				t.Fatal("removal changed URL identity, duplicate semantics or retained order")
			}
			var changed []string
			for key, value := range before {
				if !bytes.Equal(value, after[key]) {
					changed = append(changed, key)
				}
			}
			sort.Strings(changed)
			if !reflect.DeepEqual(changed, fields) || !reflect.DeepEqual(other, parentModeAPIRow(t, f, f.foreign)) {
				t.Fatal("list removal changed an unrelated field or profile")
			}
			profile, ok := f.users.Get(f.open)
			if !ok || !reflect.DeepEqual(profile.KidsAllowedLists, want) {
				t.Fatal("known own list revision was not acknowledged")
			}
			replay := parentModeAPIServe(f, parentListAPIRequest(path, token, intent, "synthetic-api-password"))
			if replay.Code != 409 || !reflect.DeepEqual(after, parentModeAPIRow(t, f, f.open)) {
				t.Fatal("consumed list removal was replayed")
			}
		})
	}
}

func TestParentListRemovalRegisteredRetryNeverAdoptsChangedContext(t *testing.T) {
	for _, name := range []string{"different URL", "equivalent encoding", "query append", "session", "stored client", "actor role", "actual refresh", "concurrent list addition", "absent URL added later", "duplicate header", "unknown token"} {
		t.Run(name, func(t *testing.T) {
			f := parentListAPIFixture(t)
			token := f.session(false)
			path := parentListAPIPath(f.open, parentListAPIURL)
			if name == "absent URL added later" {
				path = parentListAPIPath(f.open, "https://lists.example.test/later")
			}
			intent := parentListAPIChallenge(t, f, path, token)
			switch name {
			case "different URL":
				path = parentListAPIPath(f.open, parentListAPIKeep)
			case "equivalent encoding":
				path = strings.Replace(path, "%3A", "%3a", 1)
			case "query append":
				path += "&url=" + url.QueryEscape(parentListAPIKeep)
			case "session":
				token = f.session(false)
			case "stored client":
				f.exec(`UPDATE sessions SET client_id='synthetic-replacement-client' WHERE token=$1`, token)
			case "actor role":
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			case "actual refresh":
				response := f.do("POST", "/api/auth/refresh", token, "", nil)
				var value struct {
					Token string `json:"token"`
				}
				if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &value) != nil || value.Token == "" || value.Token == token {
					t.Fatal("registered refresh did not replace the original session")
				}
				token = value.Token
			case "concurrent list addition":
				if _, err := f.users.AddKidsAllowedList(f.open, "https://lists.example.test/newer"); err != nil {
					t.Fatal(err)
				}
			case "absent URL added later":
				if _, err := f.users.AddKidsAllowedList(f.open, "https://lists.example.test/later"); err != nil {
					t.Fatal(err)
				}
			case "unknown token":
				intent = "pli_" + strings.Repeat("0", 64)
			}
			r := parentListAPIRequest(path, token, intent, "synthetic-api-password")
			if name == "duplicate header" {
				r.Header.Add("X-Parent-Intent", intent)
			}
			before := parentModeAPIRow(t, f, f.open)
			w := parentModeAPIServe(f, r)
			var response struct {
				Code string `json:"code"`
			}
			if w.Code != 409 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Code != "parent_confirmation_changed" || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
				t.Fatalf("removal adopted changed %s: HTTP %d", name, w.Code)
			}
		})
	}
}

func TestParentListRemovalRegisteredActionTypesAreIsolated(t *testing.T) {
	f := parentListAPIFixture(t)
	token := f.session(false)
	path := parentListAPIPath(f.open, parentListAPIURL)
	modeIntent := parentModeAPIChallenge(t, f, token, "content_list")
	listIntent := parentListAPIChallenge(t, f, path, token)
	before := parentModeAPIRow(t, f, f.open)
	for _, request := range []*http.Request{
		parentListAPIRequest(path, token, modeIntent, "synthetic-api-password"),
		parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, "content_list", listIntent, "synthetic-api-password"),
	} {
		w := parentModeAPIServe(f, request)
		if w.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
			t.Fatal("a different parent action consumed a mutation proof")
		}
	}
}

func TestParentListRemovalRegisteredWrongPasswordAndCanceledRequest(t *testing.T) {
	f := parentListAPIFixture(t)
	token := f.session(false)
	path := parentListAPIPath(f.open, parentListAPIURL)
	intent := parentListAPIChallenge(t, f, path, token)
	before := parentModeAPIRow(t, f, f.open)
	w := parentModeAPIServe(f, parentListAPIRequest(path, token, intent, "synthetic-wrong-password"))
	if w.Code != 403 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("wrong password removed an allowed list")
	}
	w = parentModeAPIServe(f, parentListAPIRequest(path, token, intent, "synthetic-api-password"))
	if w.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("failed proof allowed a second password attempt")
	}
	intent = parentListAPIChallenge(t, f, path, token)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	parentModeAPIServe(f, parentListAPIRequest(path, token, intent, "synthetic-api-password").WithContext(ctx))
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
		t.Fatal("already canceled removal changed the target")
	}
}

func TestParentListRemovalRegisteredInputAndOwnershipBoundaries(t *testing.T) {
	f := parentListAPIFixture(t)
	token := f.session(false)
	before := parentModeAPIRow(t, f, f.open)
	for _, item := range []struct {
		path string
		want int
	}{
		{parentListAPIPath(f.foreign, parentListAPIURL), 404},
		{parentListAPIPath("synthetic-missing-profile", parentListAPIURL), 404},
		{parentListAPIPath(f.open, "   "), 400},
	} {
		w := parentModeAPIServe(f, parentListAPIRequest(item.path, token, "", ""))
		if w.Code != item.want || strings.Contains(w.Body.String(), "parentIntent") || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) {
			t.Fatalf("invalid or foreign removal acquired an intent or changed the target: HTTP %d", w.Code)
		}
	}
}
