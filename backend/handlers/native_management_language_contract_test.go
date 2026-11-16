package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
)

// Requests are exported by the native request builders. These handler-level
// checks complement the separate real API ownership and PostgreSQL tests; they
// do not claim Android transport, login, repository grouping or device execution.
func TestNativeManagementLanguageRequests(t *testing.T) {
	dir := os.Getenv("NATIVE_MANAGEMENT_LANGUAGE_CONTRACT_DIR")
	if dir == "" {
		t.Skip("native management request exports not configured")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "native-management-queries.json"))
	if err != nil {
		t.Fatal("read native request export:", err)
	}
	if len(raw) > 32*1024 {
		t.Fatal("unexpectedly large native request export")
	}
	var requests map[string]string
	if err := json.Unmarshal(raw, &requests); err != nil {
		t.Fatal("decode native request export:", err)
	}
	cases := []struct {
		name  string
		total int
		ids   []string
	}{
		{"hidden-selected", 2, []string{"tmdb:movie:2"}},
		{"hidden-continuation", 2, []string{"tmdb:movie:3"}},
		{"hidden-unknown", 6, []string{"manual-unbound", "tmdb:movie:7", "tmdb:movie:8", "tmdb:movie:9", "tmdb:movie:10", "tmdb:movie:1"}},
		{"local-cards-selected", 2, []string{"group-02"}},
		{"local-cards-continuation", 2, []string{"group-03"}},
		{"local-cards-unknown", 5, []string{"group-07", "group-08", "group-09", "group-10", "unbound"}},
	}
	if len(requests) != len(cases) {
		t.Fatal("native request set must contain exactly the six required cases")
	}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	hidden, hiddenStore, hiddenReader, hiddenTransport, hiddenHashes := hiddenCatalogueWireFixture(t)
	hiddenBefore, err := json.Marshal(hiddenStore.items)
	if err != nil {
		t.Fatal(err)
	}
	reader, localHashes := localLibraryLanguageCache(t)
	localStore := &localLibraryLanguageGroups{references: map[string]string{}}
	for n := 1; n <= 10; n++ {
		id := fmt.Sprintf("group-%02d", n)
		localStore.references[id] = fmt.Sprintf("tmdb:movie:%d", n)
		localStore.groups = append(localStore.groups, models.LocalMediaItemGroup{ID: id, GroupType: "title", LibraryType: models.LocalMediaLibraryTypeMovie, Title: fmt.Sprintf("Saved %02d", n), ItemCount: 3, Year: 2020})
	}
	localStore.groups = append(localStore.groups, models.LocalMediaItemGroup{ID: "unbound", GroupType: "title", LibraryType: models.LocalMediaLibraryTypeMovie, Title: "Unbound", ItemCount: 2, TMDBID: 2})
	localBefore, err := json.Marshal(localStore.groups)
	if err != nil {
		t.Fatal(err)
	}
	library := NewLocalMediaHandler(localStore, nil, false)
	library.SetMetadataLanguageProviders(reader, nil, nil)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			value, found := requests[c.name]
			if !found {
				t.Fatal("missing required native request")
			}
			u, err := url.ParseRequestURI(value)
			if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" {
				t.Fatal("request must be a relative API URL")
			}
			isHidden := strings.HasPrefix(c.name, "hidden-")
			wantPath := "/api/library/libraries/local/groups"
			if isHidden {
				wantPath = "/api/users/profile-a/display-list"
			}
			if u.Path != wantPath {
				t.Fatal("request path changed its synthetic original owner or library")
			}
			request := httptest.NewRequest(http.MethodGet, value, nil)
			if isHidden {
				request = mux.SetURLVars(request, map[string]string{"userID": "profile-a"})
			} else {
				request = mux.SetURLVars(request, map[string]string{"libraryID": "local"})
				query := request.URL.Query()
				profile, user := query.Get("profileId"), query.Get("userId")
				if query.Get("include") != "cards" || profile == "" && user == "" || profile != "" && profile != "profile-a" || user != "" && user != "profile-a" {
					t.Fatal("library request lost the cards projection or original profile")
				}
			}
			if err := validator.ValidateRequest(request, nil); err != nil {
				t.Fatal("native request violates the actual API contract:", err)
			}
			response := httptest.NewRecorder()
			var total int
			var ids []string
			if isHidden {
				beforeReads := hiddenStore.reads.Load()
				hidden.Get(response, request)
				payload, items := hiddenManagementDecode(t, response)
				if hiddenStore.reads.Load()-beforeReads != 1 || payload.Source != "hidden" || !payload.OriginalLanguageFilterSupported {
					t.Fatal("hidden request lost its separate management contract")
				}
				total = payload.Total
				for _, item := range items {
					ids = append(ids, item.ID)
				}
			} else {
				beforeReads := localStore.reads
				library.ListGroups(response, request)
				payload := localLibraryLanguageDecode(t, response)
				if localStore.reads-beforeReads != 1 || !payload.OriginalLanguageFilterSupported {
					t.Fatal("library capability was not returned by the single cards response")
				}
				total = payload.Total
				for _, item := range payload.Groups {
					ids = append(ids, item.ID)
					wantCount := 3
					if item.ID == "unbound" {
						wantCount = 2
					}
					if item.ItemCount != wantCount {
						t.Fatal("language selection changed the stored version count")
					}
				}
			}
			if response.Code != http.StatusOK || total != c.total || !reflect.DeepEqual(ids, c.ids) {
				t.Fatalf("selection changed: status=%d total=%d ids=%v", response.Code, total, ids)
			}
			if err := validator.ValidateResponse(request, response.Code, response.Header(), response.Body.Bytes()); err != nil {
				t.Fatal("response violates the actual API contract:", err)
			}
		})
	}
	hiddenAfter, _ := json.Marshal(hiddenStore.items)
	localAfter, _ := json.Marshal(localStore.groups)
	if string(hiddenBefore) != string(hiddenAfter) || string(localBefore) != string(localAfter) {
		t.Fatal("request selection changed the full stored index")
	}
	if hiddenStore.selfFilters.Load() != 0 || hiddenTransport.calls.Load() != 0 || hiddenReader.reads.Load() == 0 {
		t.Fatal("management requests escaped the cache-only evidence boundary")
	}
	for _, hashes := range []map[string]string{hiddenHashes, localHashes} {
		for path, expected := range hashes {
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(body)
			if hex.EncodeToString(sum[:]) != expected {
				t.Fatal("request renewed an existing catalogue observation")
			}
		}
	}
}
