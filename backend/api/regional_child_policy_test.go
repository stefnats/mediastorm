package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"novastream/models"
	"reflect"
	"strings"
	"testing"
)

const regionalAPIBody = `{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12}`

func regionalAPIRequest(profile, token, body, intent, password string) *http.Request {
	return parentListWritesRequest("PUT", "/api/users/"+profile+"/kids/regional-policy", token, body, intent, password)
}
func TestRegionalPolicyActualRegisteredFreshParentRoundtripAndReplay(t *testing.T) {
	f := parentModeAPIFixture(t)
	token := f.session(false)
	first := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, "", ""))
	var challenge struct {
		Code   string `json:"code"`
		Intent string `json:"parentIntent"`
	}
	if first.Code != 403 || json.Unmarshal(first.Body.Bytes(), &challenge) != nil || !strings.HasPrefix(challenge.Intent, "prf_") {
		t.Fatal("missing original regional password challenge")
	}
	before := parentModeAPIRow(t, f, f.open)
	saved := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, challenge.Intent, "synthetic-api-password"))
	var policy models.RegionalChildPolicy
	if saved.Code != 200 || json.Unmarshal(saved.Body.Bytes(), &policy) != nil || policy.MaxMovieAge != 6 || policy.MaxEpisodeAge != 12 {
		t.Fatalf("regional commit failed: HTTP %d", saved.Code)
	}
	after := parentModeAPIRow(t, f, f.open)
	for key, value := range before {
		if key != "updated_at" && !reflect.DeepEqual(value, after[key]) {
			t.Fatal("regional save changed legacy profile field")
		}
	}
	read := httptest.NewRequest("GET", "/api/users/"+f.open+"/kids/regional-policy", nil)
	read.Header.Set("Authorization", "Bearer "+token)
	read.Header.Set("X-Client-ID", "synthetic-pg-api-client")
	got := parentModeAPIServe(f, read)
	var current models.RegionalChildPolicy
	decodeErr := json.Unmarshal(got.Body.Bytes(), &current)
	if got.Code != 200 || decodeErr != nil || current != policy {
		safeBody := got.Body.String()
		if len(safeBody) > 512 {
			safeBody = safeBody[:512]
		}
		t.Fatalf("GET current policy: HTTP%d decode=%v syntheticBody=%q", got.Code, decodeErr, safeBody)
	}
	replay := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, challenge.Intent, "synthetic-api-password"))
	if replay.Code != 409 {
		t.Fatal("regional confirmation replayed")
	}
}
func TestRegionalPolicyRegisteredStrictBodyBeforeChallengeAndMasterNeedsPassword(t *testing.T) {
	f := parentModeAPIFixture(t)
	f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
	token := f.session(true)
	for _, body := range []string{`{"version":1,"country":"DE","maxMovieAge":6,"maxMovieAge":18,"maxEpisodeAge":12}`, `{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12,"enabled":false}`, regionalAPIBody + ` {}`, `{"version":1,"country":"DE","maxMovieAge":null,"maxEpisodeAge":12}`} {
		got := parentModeAPIServe(f, regionalAPIRequest(f.open, token, body, "", ""))
		if got.Code != 400 || strings.Contains(got.Body.String(), "parentIntent") {
			t.Fatal("invalid caps consumed confirmation or changed policy")
		}
	}
	got := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, "", ""))
	if got.Code != 403 {
		t.Fatal("master bypassed fresh password")
	}
	for _, cap := range []int{0, 6, 12, 16, 18} {
		body := fmt.Sprintf(`{"version":1,"country":"DE","maxMovieAge":%d,"maxEpisodeAge":18}`, cap)
		got := parentModeAPIServe(f, regionalAPIRequest(f.open, token, body, "", "synthetic-api-password"))
		if got.Code != 200 {
			t.Fatalf("supported cap %d rejected", cap)
		}
	}
}
func TestRegionalPolicyRegisteredWrongPasswordAndLegacyActionIsolation(t *testing.T) {
	f := parentModeAPIFixture(t)
	token := f.session(false)
	first := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, "", ""))
	var challenge struct {
		Intent string `json:"parentIntent"`
	}
	if json.Unmarshal(first.Body.Bytes(), &challenge) != nil {
		t.Fatal("challenge")
	}
	wrong := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, challenge.Intent, "wrong-synthetic-password"))
	if wrong.Code != 403 {
		t.Fatal("wrong password accepted")
	}
	retry := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, challenge.Intent, "synthetic-api-password"))
	if retry.Code != 409 {
		t.Fatal("wrong password did not consume operation")
	}
	legacy := ratingAPIChallenge(t, f, "pair", token, ratingAPIBody("pair"))
	cross := parentModeAPIServe(f, regionalAPIRequest(f.open, token, regionalAPIBody, legacy, "synthetic-api-password"))
	if cross.Code != 409 {
		t.Fatal("legacy pair intent became regional proof")
	}
	foreign := parentModeAPIServe(f, regionalAPIRequest(f.foreign, token, regionalAPIBody, "", "synthetic-api-password"))
	if foreign.Code != 404 && foreign.Code != 403 {
		t.Fatal("foreign profile regional caps changed")
	}
}
