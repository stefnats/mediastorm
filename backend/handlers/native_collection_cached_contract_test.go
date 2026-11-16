package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
)

// Real native builder URLs consume a real loopback producer, cached observation,
// language evidence reader and browse handler. The owner is synthetic and
// injected; this is not account login, SQL or complete Android transport proof.
func TestNativeCollectionCachedRequests(t *testing.T) {
	dir := os.Getenv("NATIVE_COLLECTION_CACHED_CONTRACT_DIR")
	if dir == "" {
		t.Skip("native collection request exports not configured")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "native-collection-queries.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 32*1024 {
		t.Fatal("oversized native export")
	}
	var requests map[string]string
	if err := json.Unmarshal(raw, &requests); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		total int
		ids   []string
	}{
		{"unfiltered", 701, []string{"tmdb:movie:1", "tmdb:movie:2"}},
		{"continuation", 701, []string{"tmdb:movie:3", "tmdb:movie:4"}},
		{"french", 1, []string{"tmdb:movie:651"}},
		{"unknown", 697, []string{"tmdb:movie:1", "tmdb:movie:2"}},
		{"or-unknown", 698, []string{"tmdb:movie:1", "tmdb:movie:2"}},
		{"random-pool", 2, []string{"tmdb:movie:651", "tmdb:movie:653"}},
	}
	if len(requests) != len(cases) {
		t.Fatal("exactly six native requests required")
	}
	f, service, guard := collectionCachedWireFixture(t)
	// Change only fixture identity. The captured native URLs are never rewritten.
	f.base.meta.UsersService = fallbackProfiles{models.User{ID: "profile-a", AccountID: "account"}}
	if _, err := f.base.hidden.Hide("profile-a", models.HiddenItemUpsert{ID: "tmdb:movie:652", MediaType: "movie", Name: "Observed 0652"}); err != nil {
		t.Fatal(err)
	}
	beforeRatings := append([]byte(nil), f.base.repo.snapshot()...)
	normal := NewMetadataHandler(service, f.base.meta.CfgManager)
	normal.UsersService = f.base.meta.UsersService
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/display-list", f.base.display.Get).Methods(http.MethodGet)
	router.HandleFunc("/api/metadata/collection", normal.CollectionDetails).Methods(http.MethodGet)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(profile, account string) bool { return profile == "profile-a" && account == "account" })(router).ServeHTTP(w, r)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	local := collectionLoopbackTransport(server.Listener.Addr().String())
	t.Cleanup(local.CloseIdleConnections)
	client := &http.Client{Transport: local, Timeout: 5 * time.Second}
	get := func(path string) (*http.Request, *http.Response, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Client-ID", "fixture-client")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return req, resp, body
	}
	// One explicit ordinary load is the producer; cache browsing never invokes it.
	_, produced, _ := get("/api/metadata/collection?id=77&userId=profile-a")
	if produced.StatusCode != http.StatusOK || guard.calls.Load() != 1 || guard.producer.Load() != 1 || guard.denied.Load() != 0 {
		t.Fatal("explicit local producer did not run exactly once")
	}
	observation, err := service.GetCachedCollectionSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	beforeObservation, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			value, found := requests[c.name]
			if !found {
				t.Fatal("missing native request")
			}
			u, err := url.ParseRequestURI(value)
			if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Path != "/api/users/profile-a/display-list" || u.Query().Get("source") != "collection-cached" || u.Query().Get("collectionId") != "77" {
				t.Fatal("native export is not its original relative collection route")
			}
			reads := f.port.snapshots.Load()
			req, resp, body := get(value)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("native query status=%d", resp.StatusCode)
			}
			var page struct {
				Source                          string                            `json:"source"`
				Items                           []models.Title                    `json:"items"`
				Total                           int                               `json:"total"`
				OriginalLanguageFilterSupported bool                              `json:"originalLanguageFilterSupported"`
				Collection                      cachedCollectionBrowseObservation `json:"collection"`
			}
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 0, len(page.Items))
			for _, title := range page.Items {
				if title.MediaType != "movie" {
					t.Fatal("collection member kind changed")
				}
				ids = append(ids, title.ID)
			}
			if page.Source != "collection-cached" || !page.OriginalLanguageFilterSupported || page.Total != c.total || !reflect.DeepEqual(ids, c.ids) || f.port.snapshots.Load()-reads != 1 {
				t.Fatalf("complete page total=%d ids=%v", page.Total, ids)
			}
			if page.Collection.ID != 77 || page.Collection.MetadataLanguage != observation.MetadataLanguage || !page.Collection.ObservedAt.Equal(observation.ObservedAt) || !page.Collection.ExpiresAt.Equal(observation.ExpiresAt) {
				t.Fatal("native browse renewed or replaced its observed pool")
			}
			if resp.Header.Get("Cache-Control") != "private, no-store" || strings.Contains(string(body), "PRIVATE-NOT-A-SNAPSHOT-FIELD") {
				t.Fatal("private projection changed")
			}
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, body); err != nil {
				t.Fatal(err)
			}
			if guard.calls.Load() != 1 || guard.producer.Load() != 1 || guard.denied.Load() != 0 || f.base.repo.calls.Load() != 0 || f.base.repo.writes.Load() != 0 || f.base.ratings.observes.Load() != 0 {
				t.Fatal("native cache query started excluded provider or preference work")
			}
		})
	}
	after, err := service.GetCachedCollectionSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	afterObservation, err := json.Marshal(after)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeObservation, afterObservation) || !bytes.Equal(beforeRatings, f.base.repo.snapshot()) {
		t.Fatal("native queries changed snapshot or saved preferences")
	}
}
