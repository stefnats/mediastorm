package api

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
)

const parentListWritesNewURL = "https://lists.example.test/new/café?literal=+%41&quote='"

func parentListWritesJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func parentListWritesPath(profile string) string {
	return "/api/users/" + profile + "/kids/lists"
}

func parentListWritesRequest(method, path, token, body, intent, password string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
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

func parentListWritesVersion(t *testing.T, f *publicPGPinFixture) string {
	t.Helper()
	var version string
	if err := f.ds.Pool().QueryRow(t.Context(), `SELECT xmin::text FROM users WHERE id=$1`, f.open).Scan(&version); err != nil {
		t.Fatal("read own list target revision")
	}
	return version
}

func parentListWritesChallenge(t *testing.T, f *publicPGPinFixture, method, token, body string) string {
	t.Helper()
	before := parentModeAPIRow(t, f, f.open)
	version := parentListWritesVersion(t, f)
	w := parentModeAPIServe(f, parentListWritesRequest(method, parentListWritesPath(f.open), token, body, "", ""))
	var value struct {
		Code         string `json:"code"`
		ParentIntent string `json:"parentIntent"`
	}
	prefix := "pla_"
	if method == "PUT" {
		prefix = "plw_"
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Code != "parent_authorization_required" || !strings.HasPrefix(value.ParentIntent, prefix) || len(value.ParentIntent) != 68 {
		t.Fatalf("registered %s list write did not issue its action challenge: HTTP %d", method, w.Code)
	}
	decoded, err := hex.DecodeString(value.ParentIntent[4:])
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != value.ParentIntent[4:] {
		t.Fatal("list write challenge is not canonical lowercase hex")
	}
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
		t.Fatal("first challenge changed a target row or its revision")
	}
	return value.ParentIntent
}

func TestParentListWritesRegisteredDecoderSemanticsAndTrueNoop(t *testing.T) {
	initial := []string{parentListAPIURL, parentListAPIKeep, parentListAPIURL}
	appended := append(slices.Clone(initial), parentListWritesNewURL)
	qNew := parentListWritesJSON(t, parentListWritesNewURL)
	exactAdd := `{"url":` + qNew + `}`
	exactAdd += strings.Repeat(" ", (16<<10)-len(exactAdd))
	exactReplace := parentListWritesJSON(t, map[string]any{"lists": initial})
	exactReplace += strings.Repeat(" ", (16<<10)-len(exactReplace))
	for _, item := range []struct {
		name, method, body string
		want               []string
		fields             []string
		master, noop       bool
	}{
		{"regular add", "POST", `{"url":` + qNew + `}`, appended, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"add exact body limit", "POST", exactAdd, appended, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"master exact duplicate", "POST", parentListWritesJSON(t, map[string]string{"url": " \t" + parentListAPIURL + "\n "}), initial, nil, true, true},
		{"add repeated null retains preceding string", "POST", `{"url":` + qNew + `,"URL":null}`, appended, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"add trailing value remains ignored", "POST", `{"url":` + qNew + `} {"unread":true}`, appended, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"replace trim null and duplicates", "PUT", parentListWritesJSON(t, map[string]any{"lists": []any{parentListAPIKeep, " " + parentListAPIURL + " ", " ", nil, parentListAPIKeep}}), []string{parentListAPIKeep, parentListAPIURL, parentListAPIKeep}, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"master replace identical still writes", "PUT", parentListWritesJSON(t, map[string]any{"lists": initial}), initial, []string{"updated_at"}, true, false},
		{"replace exact body limit", "PUT", exactReplace, initial, []string{"updated_at"}, false, false},
		{"replace missing lists clears", "PUT", `{}`, []string{}, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"replace root null clears", "PUT", `null`, []string{}, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"replace lists null clears", "PUT", `{"lists":null}`, []string{}, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"replace repeated null clears earlier array", "PUT", `{"lists":[` + qNew + `],"LISTS":null}`, []string{}, []string{"kids_allowed_lists", "updated_at"}, false, false},
		{"replace trailing value remains ignored", "PUT", parentListWritesJSON(t, map[string]any{"lists": []string{parentListAPIKeep}}) + ` {"unread":true}`, []string{parentListAPIKeep}, []string{"kids_allowed_lists", "updated_at"}, false, false},
	} {
		t.Run(item.name, func(t *testing.T) {
			f := parentListAPIFixture(t)
			if item.master {
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
			}
			token := f.session(false)
			before, other := parentModeAPIRow(t, f, f.open), parentModeAPIRow(t, f, f.foreign)
			version := parentListWritesVersion(t, f)
			intent := parentListWritesChallenge(t, f, item.method, token, item.body)
			w := parentModeAPIServe(f, parentListWritesRequest(item.method, parentListWritesPath(f.open), token, item.body, intent, "synthetic-api-password"))
			if w.Code != 200 {
				t.Fatalf("original confirmed list write failed: HTTP %d", w.Code)
			}
			after := parentModeAPIRow(t, f, f.open)
			var wanted []string
			if json.Unmarshal(after["kids_allowed_lists"], &wanted) != nil || !slices.Equal(wanted, item.want) {
				t.Fatal("list write changed established decoder, trimming, order or duplicate semantics")
			}
			var changed []string
			for key, value := range before {
				if !bytes.Equal(value, after[key]) {
					changed = append(changed, key)
				}
			}
			sort.Strings(changed)
			wantedFields := parentExpectedFieldsWithShareRotation(t, before, after, item.fields)
			if !reflect.DeepEqual(changed, wantedFields) || !reflect.DeepEqual(other, parentModeAPIRow(t, f, f.foreign)) {
				t.Fatal("list write changed an unrelated target field or profile")
			}
			if (version == parentListWritesVersion(t, f)) != item.noop {
				t.Fatal("POST duplicate performed a row write or PUT identical failed to write")
			}
			profile, ok := f.users.Get(f.open)
			if !ok || !slices.Equal(profile.KidsAllowedLists, item.want) {
				t.Fatal("the exact known target result was not acknowledged in users")
			}
			replay := parentModeAPIServe(f, parentListWritesRequest(item.method, parentListWritesPath(f.open), token, item.body, intent, "synthetic-api-password"))
			if replay.Code != 409 || !reflect.DeepEqual(after, parentModeAPIRow(t, f, f.open)) {
				t.Fatal("a consumed add or replace confirmation was replayed")
			}
		})
	}
}

func TestParentListWritesRegisteredOriginalBytesAndContext(t *testing.T) {
	for _, method := range []string{"POST", "PUT"} {
		for _, name := range []string{"body whitespace", "equivalent field case", "ignored trailing bytes", "query", "new session", "stored client", "actor role", "actual refresh", "concurrent array", "duplicate header"} {
			t.Run(method+"/"+name, func(t *testing.T) {
				f := parentListAPIFixture(t)
				token := f.session(false)
				body := parentListWritesJSON(t, map[string]string{"url": parentListWritesNewURL})
				if method == "PUT" {
					body = parentListWritesJSON(t, map[string]any{"lists": []string{parentListWritesNewURL}})
				}
				intent := parentListWritesChallenge(t, f, method, token, body)
				path := parentListWritesPath(f.open)
				switch name {
				case "body whitespace":
					body += " "
				case "equivalent field case":
					body = strings.ReplaceAll(strings.ReplaceAll(body, `"url"`, `"URL"`), `"lists"`, `"LISTS"`)
				case "ignored trailing bytes":
					body += ` {"ignored":true}`
				case "query":
					path += "?same-selected-body=1"
				case "new session":
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
						t.Fatal("registered refresh did not rotate the original session")
					}
					token = value.Token
				case "concurrent array":
					if _, err := f.users.AddKidsAllowedList(f.open, parentListWritesNewURL); err != nil {
						t.Fatal(err)
					}
				}
				r := parentListWritesRequest(method, path, token, body, intent, "synthetic-api-password")
				if name == "duplicate header" {
					r.Header.Add("X-Parent-Intent", intent)
				}
				before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
				w := parentModeAPIServe(f, r)
				var response struct {
					Code string `json:"code"`
				}
				if w.Code != 409 || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Code != "parent_confirmation_changed" || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
					t.Fatalf("list write adopted changed %s: HTTP %d", name, w.Code)
				}
			})
		}
	}
}

func TestParentListWritesRegisteredDistinctActionsAndCanceledRequests(t *testing.T) {
	f := parentListAPIFixture(t)
	token := f.session(false)
	path := parentListWritesPath(f.open)
	addBody := parentListWritesJSON(t, map[string]string{"url": parentListWritesNewURL})
	replaceBody := parentListWritesJSON(t, map[string]any{"lists": []string{parentListWritesNewURL}})
	add := parentListWritesChallenge(t, f, "POST", token, addBody)
	replace := parentListWritesChallenge(t, f, "PUT", token, replaceBody)
	mode := parentModeAPIChallenge(t, f, token, "content_list")
	removal := parentListAPIChallenge(t, f, parentListAPIPath(f.open, parentListAPIURL), token)
	before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
	for _, request := range []*http.Request{
		parentListWritesRequest("POST", path, token, addBody, replace, "synthetic-api-password"),
		parentListWritesRequest("PUT", path, token, replaceBody, add, "synthetic-api-password"),
		parentListWritesRequest("POST", path, token, addBody, mode, "synthetic-api-password"),
		parentListWritesRequest("POST", path, token, addBody, removal, "synthetic-api-password"),
		parentModeAPIRequest(t, "PUT", parentModeAPIPath(f.open), token, "content_list", add, "synthetic-api-password"),
		parentListAPIRequest(parentListAPIPath(f.open, parentListAPIURL), token, replace, "synthetic-api-password"),
	} {
		w := parentModeAPIServe(f, request)
		if w.Code != 409 || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
			t.Fatal("list write proof was accepted by another parent action")
		}
	}
	cancelIntent := parentListWritesChallenge(t, f, "POST", token, addBody)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	parentModeAPIServe(f, parentListWritesRequest("POST", path, token, addBody, cancelIntent, "synthetic-api-password").WithContext(ctx))
	if !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
		t.Fatal("already canceled add changed the target or revision")
	}
}

func TestParentListWritesRegisteredInvalidAndOversizedBodies(t *testing.T) {
	f := parentListAPIFixture(t)
	token := f.session(false)
	before, version := parentModeAPIRow(t, f, f.open), parentListWritesVersion(t, f)
	for _, item := range []struct {
		method, body string
		want         int
	}{
		{"POST", `{}`, 400}, {"POST", `null`, 400}, {"POST", `{"url":null}`, 400},
		{"POST", `{"url":42}`, 400}, {"POST", `{"unknown":true}`, 400},
		{"PUT", `{"lists":42}`, 400}, {"PUT", `{"unknown":true}`, 400},
		{"POST", `{"url":"x"}` + strings.Repeat(" ", 16<<10), 413},
		{"PUT", `{"lists":[]}` + strings.Repeat(" ", 16<<10), 413},
	} {
		w := parentModeAPIServe(f, parentListWritesRequest(item.method, parentListWritesPath(f.open), token, item.body, "", ""))
		if w.Code != item.want || strings.Contains(w.Body.String(), "parentIntent") || !reflect.DeepEqual(before, parentModeAPIRow(t, f, f.open)) || version != parentListWritesVersion(t, f) {
			t.Fatalf("invalid list body obtained confirmation or changed data: HTTP %d", w.Code)
		}
	}
}
