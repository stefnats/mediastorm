package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/debrid"
	"novastream/services/sourcefilters"
)

// These are serialized production native helper outputs, not captured network
// traffic. Only opaque runtime snapshot/cursor references are rebound below.
//
//go:embed testdata/sourcefilters-native/*.json
var nativeSourceFilterRequests embed.FS

func nativeSourceFilterBody(t *testing.T, name string) []byte {
	t.Helper()
	data, err := nativeSourceFilterRequests.ReadFile("testdata/sourcefilters-native/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestNativeSourceFilterFixtureBytes(t *testing.T) {
	data := nativeSourceFilterBody(t, "sha256")
	var hashes map[string]string
	if err := json.Unmarshal(data, &hashes); err != nil || len(hashes) != 7 {
		t.Fatal("missing production-native fixture manifest")
	}
	for name, want := range hashes {
		t.Run(name, func(t *testing.T) {
			body, err := nativeSourceFilterRequests.ReadFile("testdata/sourcefilters-native/" + name)
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(body)
			if hex.EncodeToString(hash[:]) != want {
				t.Fatal("production native request bytes changed")
			}
		})
	}
}

func nativeFilterFixture(t *testing.T, count int) (*IndexerHandler, *filterSearch) {
	t.Helper()
	h, search, _ := sourceFilterFixture(t, count)
	h.SourceFilterProfile = func(id string) (models.User, bool) { return models.User{ID: id, AccountID: "account"}, id == "adult" }
	return h, search
}

func nativeFilterCall(t *testing.T, v *openapi.Validator, method, target string, body []byte, handler http.HandlerFunc, status int, vars map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := filterRequest(method, target, body)
	if vars != nil {
		r = mux.SetURLVars(r, vars)
	}
	if err := v.ValidateRequest(r, body); err != nil {
		t.Fatalf("native request contract: %v", err)
	}
	reply := httptest.NewRecorder()
	handler(reply, r)
	if reply.Code != status {
		t.Fatalf("native request status=%d want=%d body=%s", reply.Code, status, reply.Body.String())
	}
	if err := v.ValidateResponse(r, reply.Code, reply.Header(), reply.Body.Bytes()); err != nil {
		t.Fatalf("native response contract: %v", err)
	}
	return reply
}

func nativeInitialFilterPage(t *testing.T, h *IndexerHandler, v *openapi.Validator) sourcefilters.Page {
	t.Helper()
	var query map[string]string
	if err := json.Unmarshal(nativeSourceFilterBody(t, "native-initial-query"), &query); err != nil {
		t.Fatal(err)
	}
	values := url.Values{}
	for key, value := range query {
		values.Set(key, value)
	}
	r := nativeFilterCall(t, v, "GET", "/api/indexers/search?"+values.Encode(), nil, h.Search, 200, nil)
	var page sourcefilters.Page
	if err := json.Unmarshal(r.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if page.Target.TitleID != "tmdb:series:41" || page.Target.MediaType != "series" || page.Target.Episode == nil || page.Target.Episode.EpisodeID != "tvdb:episode:123" || page.Target.Episode.SeasonNumber != 2 || page.Target.Episode.EpisodeNumber != 3 || page.Target.Episode.Numbering == nil || page.Target.Episode.Numbering.SeriesID != "tvdb:series:74852" || page.Target.Episode.Numbering.Ordering != "official" {
		t.Fatal("native query lost its exact request correlation")
	}
	return page
}

func TestNativeSourceFilterPageRequestsUseOnlyOwnedSnapshot(t *testing.T) {
	v, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	h, search := nativeFilterFixture(t, 120)
	first := nativeInitialFilterPage(t, h, v)
	if search.calls != 1 || len(first.Rows) != 50 || first.Total != 120 || !search.lastOpts.RetainForSourceFilters {
		t.Fatal("native initial search did not retain the full bounded pool")
	}
	target := "/api/users/adult/source-searches/" + first.SnapshotID + "/page"
	vars := map[string]string{"userID": "adult", "snapshotID": first.SnapshotID}
	var clear sourcefilters.Page
	for _, name := range []string{"native-claims-filter", "native-file-audio-conjunction", "native-clear-filter"} {
		t.Run(name, func(t *testing.T) {
			body := nativeSourceFilterBody(t, name)
			reply := nativeFilterCall(t, v, "POST", target, body, h.SourceSearchPage, 200, vars)
			var page sourcefilters.Page
			if err := json.Unmarshal(reply.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			var request struct {
				Filters sourcefilters.Filters `json:"filters"`
			}
			if err := json.Unmarshal(body, &request); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(page.Filters, request.Filters) || page.SnapshotID != first.SnapshotID || search.calls != 1 {
				t.Fatal("native filter payload changed or performed another search")
			}
			if name == "native-file-audio-conjunction" && (page.Total != 0 || page.UnknownTotal != 120) {
				t.Fatal("unprobed exact episode traits became known file matches")
			}
			if name == "native-clear-filter" {
				clear = page
				if page.Total != 120 || page.MatchedTotal != 0 || page.UnknownTotal != 0 {
					t.Fatal("clear lost retained unfiltered rows")
				}
			}
		})
	}
	// The literal production example uses cursor-a. It must fail closed;
	// only the server-returned opaque cursor can continue this owned view.
	nativeFilterCall(t, v, "POST", target, nativeSourceFilterBody(t, "native-next-page"), h.SourceSearchPage, 409, vars)
	var bound map[string]json.RawMessage
	if err := json.Unmarshal(nativeSourceFilterBody(t, "native-next-page"), &bound); err != nil {
		t.Fatal(err)
	}
	bound["cursor"], _ = json.Marshal(clear.NextCursor)
	body, err := json.Marshal(bound)
	if err != nil {
		t.Fatal(err)
	}
	reply := nativeFilterCall(t, v, "POST", target, body, h.SourceSearchPage, 200, vars)
	var next sourcefilters.Page
	if err := json.Unmarshal(reply.Body.Bytes(), &next); err != nil {
		t.Fatal(err)
	}
	if next.ViewID != clear.ViewID || next.Total != clear.Total || len(next.Rows) != 50 || next.Rows[0].SourceIndex != 50 || next.Rows[49].SourceIndex != 99 || search.calls != 1 {
		t.Fatal("native continuation changed the frozen view or fetched providers")
	}
}

type nativeFilterUsenetRecorder struct {
	calls int
	last  models.NZBResult
}

func (s *nativeFilterUsenetRecorder) CheckHealth(_ context.Context, row models.NZBResult) (*models.NZBHealthCheck, error) {
	s.calls++
	s.last = row
	return &models.NZBHealthCheck{Status: "healthy", Healthy: true}, nil
}

func TestNativeSourceFilterScopedProbeRequestsUseServerCandidate(t *testing.T) {
	for _, mode := range []string{"usenet", "debrid"} {
		t.Run(mode, func(t *testing.T) {
			v, err := openapi.NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			h, search := nativeFilterFixture(t, 20)
			if mode == "usenet" {
				search.results[7].ServiceType = models.ServiceTypeUsenet
			}
			first := nativeInitialFilterPage(t, h, v)
			usenet := &nativeFilterUsenetRecorder{}
			debridService := &scopedDebridProbe{response: &debrid.DebridHealthCheck{Status: "cached", Healthy: true, Cached: true}}
			var handler http.HandlerFunc
			path := "/api/usenet/health"
			if mode == "usenet" {
				probe := NewUsenetHandler(usenet)
				probe.SourceFilters = h
				handler = probe.CheckHealth
			} else {
				probe := NewDebridHandler(nil, debridService)
				probe.SourceFilters = h
				handler = probe.CheckCached
				path = "/api/debrid/cached"
			}
			fixture := nativeSourceFilterBody(t, "native-scoped-"+mode+"-probe")
			nativeFilterCall(t, v, "POST", path, fixture, handler, 404, nil)
			if usenet.calls != 0 || debridService.calls != 0 {
				t.Fatal("placeholder snapshot reached provider work")
			}
			var bound map[string]json.RawMessage
			if err := json.Unmarshal(fixture, &bound); err != nil {
				t.Fatal(err)
			}
			originalResult := bytes.Clone(bound["result"])
			bound["snapshotId"], _ = json.Marshal(first.SnapshotID)
			body, err := json.Marshal(bound)
			if err != nil {
				t.Fatal(err)
			}
			var after map[string]json.RawMessage
			if err := json.Unmarshal(body, &after); err != nil || !bytes.Equal(after["result"], originalResult) {
				t.Fatal("runtime binding rewrote the native result payload")
			}
			nativeFilterCall(t, v, "POST", path, body, handler, 200, nil)
			last, calls := usenet.last, usenet.calls
			if mode == "debrid" {
				last, calls = debridService.last, debridService.calls
			}
			if calls != 1 || last.GUID != "synthetic-7" || last.Attributes["profileId"] != "adult" || search.calls != 1 {
				t.Fatal("scoped probe trusted the forged native example row or searched again")
			}
			if mode == "debrid" && last.ServiceType != models.ServiceTypeDebrid {
				t.Fatal("client usenet type overrode the server's debrid candidate")
			}
		})
	}
}
