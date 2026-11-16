package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/debrid"
	"novastream/services/mediaevidence"
	"novastream/services/sourcefilters"
)

func sourceFilterWireContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	h, _, cfg := sourceFilterFixture(t, 6)
	fixtures := map[string]json.RawMessage{}
	call := func(name, method, target string, body any, handler http.HandlerFunc, status int) *httptest.ResponseRecorder {
		t.Helper()
		var data []byte
		if body != nil {
			data, _ = json.Marshal(body)
		}
		r := filterRequest(method, target, data)
		if method == "POST" && handler != nil {
			r = mux.SetURLVars(r, map[string]string{"userID": "profile"})
		}
		if err := v.ValidateRequest(r, data); err != nil {
			t.Fatalf("%s request: %v", name, err)
		}
		rec := httptest.NewRecorder()
		handler(rec, r)
		if rec.Code != status {
			t.Fatalf("%s %d: %s", name, rec.Code, rec.Body.String())
		}
		if err := v.ValidateResponse(r, rec.Code, rec.Header(), rec.Body.Bytes()); err != nil {
			t.Fatalf("%s response: %v\n%s", name, err, rec.Body.String())
		}
		fixtures[name] = append(json.RawMessage(nil), rec.Body.Bytes()...)
		return rec
	}
	initial := call("initial-page.json", "GET", "/api/indexers/search?q=Synthetic&titleId=tmdb:movie:41&mediaType=movie&userId=profile&sourceFiltersVersion=1&limit=2", nil, h.Search, 200)
	var first sourcefilters.Page
	_ = json.Unmarshal(initial.Body.Bytes(), &first)
	page := func(name string, filters sourcefilters.Filters, cursor string, status int) *httptest.ResponseRecorder {
		t.Helper()
		target := "/api/users/profile/source-searches/" + first.SnapshotID + "/page"
		body := map[string]any{"version": 1, "filters": filters, "limit": 50}
		if cursor != "" {
			body["cursor"] = cursor
		}
		return call(name, "POST", target, body, func(w http.ResponseWriter, r *http.Request) {
			r = mux.SetURLVars(r, map[string]string{"userID": "profile", "snapshotID": first.SnapshotID})
			h.SourceSearchPage(w, r)
		}, status)
	}
	page("initial-continuation.json", first.Filters, first.NextCursor, 200)
	page("claimed-language-page.json", sourcefilters.Filters{Basis: "candidate", Unknown: "exclude", AudioLanguage: "fra"}, "", 200)
	page("empty-file-page.json", sourcefilters.Filters{Basis: "probed", Unknown: "exclude", AudioLanguage: "fra"}, "", 200)
	health := &scopedDebridProbe{}
	health.before = func(row models.NZBResult) {
		health.response = &debrid.DebridHealthCheck{Status: "cached", Healthy: true, Cached: true, MediaEvidence: mediaevidence.Probed(row, "synthetic-input-file", "", 100, evidenceFacts(time.Now()), "", nil, time.Now())}
	}
	probe := NewDebridHandler(nil, health)
	probe.SourceFilters = h
	call("explicit-file-probe.json", "POST", "/api/debrid/cached", map[string]any{"result": models.NZBResult{GUID: "ignored-client-result"}, "profileId": "profile", "snapshotId": first.SnapshotID, "sourceIndex": 5, "verifyUncached": true}, probe.CheckCached, 200)
	page("view-changed-error.json", first.Filters, first.NextCursor, 409)
	page("file-movie-page.json", sourcefilters.Filters{Basis: "probed", Unknown: "include", AudioLanguage: "fra"}, "", 200)
	call("legacy-array.json", "GET", "/api/indexers/search?q=Synthetic&titleId=tmdb:movie:41&mediaType=movie&userId=profile", nil, h.Search, 200)
	cfg.change()
	page("context-changed-error.json", first.Filters, "", 409)
	series, _, _ := sourceFilterFixture(t, 2)
	seriesInitial := call("series-initial-page.json", "GET", "/api/indexers/search?q=Synthetic+S01E02&titleId=tvdb:series:74852&mediaType=series&episodeId=tvdb:episode:123&seasonNumber=1&episodeNumber=2&episodeNumberingId=tvdb:series:74852&episodeOrdering=official&userId=profile&sourceFiltersVersion=1&limit=50", nil, series.Search, 200)
	var seriesPage sourcefilters.Page
	_ = json.Unmarshal(seriesInitial.Body.Bytes(), &seriesPage)
	seriesHealth := &scopedDebridProbe{}
	seriesHealth.before = func(row models.NZBResult) {
		seriesHealth.response = &debrid.DebridHealthCheck{Status: "cached", Healthy: true, MediaEvidence: mediaevidence.Probed(row, "synthetic-series-file", "", 100, evidenceFacts(time.Now()), "", nil, time.Now())}
	}
	seriesProbe := NewDebridHandler(nil, seriesHealth)
	seriesProbe.SourceFilters = series
	call("series-source-only-probe.json", "POST", "/api/debrid/cached", map[string]any{"result": models.NZBResult{}, "profileId": "profile", "snapshotId": seriesPage.SnapshotID, "sourceIndex": 0, "verifyUncached": true}, seriesProbe.CheckCached, 200)
	call("series-source-only-page.json", "POST", "/api/users/profile/source-searches/"+seriesPage.SnapshotID+"/page", map[string]any{"version": 1, "filters": sourcefilters.Filters{Basis: "probed", Unknown: "include", AudioLanguage: "fra"}, "limit": 50}, func(w http.ResponseWriter, r *http.Request) {
		r = mux.SetURLVars(r, map[string]string{"userID": "profile", "snapshotID": seriesPage.SnapshotID})
		series.SourceSearchPage(w, r)
	}, 200)
	expiring, expirySearch, _ := sourceFilterFixture(t, 2)
	clockNow := time.Now().UTC()
	expiryStart := clockNow
	expiring.SourceFilters = sourcefilters.NewStoreWithClock(func() time.Time { return clockNow })
	for i := range expirySearch.results {
		expirySearch.results[i].EvidenceExpiresAt = expiryStart.Add(time.Minute)
	}
	expiryInitial := httptest.NewRecorder()
	expiring.Search(expiryInitial, filterRequest("GET", "/api/indexers/search?q=Synthetic&titleId=tmdb:movie:41&mediaType=movie&userId=profile&sourceFiltersVersion=1&limit=1", nil))
	if expiryInitial.Code != 200 {
		t.Fatal(expiryInitial.Body.String())
	}
	var expiryPage sourcefilters.Page
	_ = json.Unmarshal(expiryInitial.Body.Bytes(), &expiryPage)
	expiryCall := func(name, cursor string, status int) {
		t.Helper()
		body := map[string]any{"version": 1, "filters": expiryPage.Filters, "limit": 50}
		if cursor != "" {
			body["cursor"] = cursor
		}
		call(name, "POST", "/api/users/profile/source-searches/"+expiryPage.SnapshotID+"/page", body, func(w http.ResponseWriter, r *http.Request) {
			r = mux.SetURLVars(r, map[string]string{"userID": "profile", "snapshotID": expiryPage.SnapshotID})
			expiring.SourceSearchPage(w, r)
		}, status)
	}
	clockNow = expiryStart.Add(2 * time.Minute)
	expiryCall("expired-view-error.json", expiryPage.NextCursor, 409)
	clockNow = expiryStart.Add(16 * time.Minute)
	expiryCall("expired-snapshot-error.json", "", 410)
	if directory := os.Getenv("SOURCE_FILTER_CONTRACT_DIR"); directory != "" {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		hashes := map[string]string{}
		for name, data := range fixtures {
			if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			hashes[name] = hex.EncodeToString(sum[:])
		}
		data, _ := json.MarshalIndent(hashes, "", "  ")
		if err := os.WriteFile(filepath.Join(directory, "sha256.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		data, _ = json.MarshalIndent(map[string]any{"evaluatedAt": time.Now().UTC(), "origin": "actual Go HTTP handlers and OpenAPI validator", "responses": hashes}, "", "  ")
		if err := os.WriteFile(filepath.Join(directory, "manifest.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSourceFilterWireContract(t *testing.T) {
	v, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sourceFilterWireContract(t, v)
}
