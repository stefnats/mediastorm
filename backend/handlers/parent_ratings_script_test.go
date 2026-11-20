package handlers

import (
	"net/http"
	"os/exec"
	"testing"

	"novastream/api/openapi"
	"novastream/services/users"
)

func TestParentRatingsActualScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node.js is not installed")
	}
	if output, err := exec.Command(node, "--test", "testdata/parent_ratings.cjs").CombinedOutput(); err != nil {
		t.Fatalf("bound rating-pair script: %v\n%s", err, output)
	}
}

// Canonical request/response schema controls use real JSON users and handlers;
// PG original identity/credential/CAS authority is covered separately.
func parentRatingsOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount(contractAccountID, "Synthetic rating contract")
	if err != nil {
		t.Fatal(err)
	}
	handler := NewUsersHandler(service, parentLegacyContractAccounts(t))
	for _, prefix := range []string{"/api", "/admin/api", "/account/api"} {
		path := prefix + "/users/" + profile.ID + "/kids/"
		for _, single := range []struct {
			suffix  string
			handler http.HandlerFunc
		}{
			{"rating", handler.SetKidsMaxRating},
			{"rating/movie", handler.SetKidsMaxMovieRating},
			{"rating/tv", handler.SetKidsMaxTVRating},
		} {
			exercise(t, v, contractCall{method: "PUT", target: path + single.suffix, vars: map[string]string{"userID": profile.ID}, master: true, headers: map[string]string{"X-Account-Password": parentLegacyFixturePassword}, body: map[string]string{"rating": " saved arbitrary scalar 雪 "}}, single.handler, 200)
		}
		pair := contractCall{method: "PUT", target: path + "ratings", vars: map[string]string{"userID": profile.ID}, master: true, headers: map[string]string{"X-Account-Password": parentLegacyFixturePassword}, body: map[string]string{"movieRating": " saved movie ", "tvRating": " saved TV "}}
		exercise(t, v, pair, handler.SetKidsRatings, 200)
		pair.body = map[string]string{"movieRating": "", "tvRating": ""}
		exercise(t, v, pair, handler.SetKidsRatings, 200)
		kept, _ := service.Get(profile.ID)
		if kept.KidsMaxRating != "saved arbitrary scalar 雪" || kept.KidsMaxMovieRating != "" || kept.KidsMaxTVRating != "" {
			t.Fatal("pair changed legacy fallback")
		}
		pair.body = `{"movieRating":null,"movieRating":"G","tvRating":"TV-Y"}`
		exercise(t, v, pair, handler.SetKidsRatings, 200)
		pair.body = `{"movieRating":"G","movieRating":null,"tvRating":"TV-Y"}`
		pair.invalid = true
		exercise(t, v, pair, handler.SetKidsRatings, 400)
	}
}
