package handlers

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/metadata"
)

func hiddenCatalogueWireFixture(t *testing.T) (*DisplayListHandler, *hiddenManagementStore, *hiddenManagementMetadata, *catalogueWireTransport, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	service := metadata.NewService("", "", "eng", dir, 24, false, metadata.MDBListConfig{})
	h, stored, _, boundary := hiddenManagementFixture(t)
	boundary.reader = service
	now := time.Now().UTC()
	hiddenAt := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	hashes := map[string]string{}
	for i, language := range []string{"en", "fr", "nob", "nno", "nor", "haw", "", "expired", "wrong", "malformed"} {
		id := fmt.Sprintf("tmdb:movie:%d", i+1)
		stored.items = append(stored.items, models.HiddenItem{ID: id, MediaType: "movie", Name: fmt.Sprintf("Saved %02d", i+1), HiddenAt: hiddenAt.Add(time.Duration(i) * time.Minute), Year: 2020, ExternalIDs: map[string]string{"tmdb": fmt.Sprint(i + 1)}})
		if language == "" {
			continue
		}
		fact := models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: id, ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
		if language == "expired" {
			fact.Language = "fra"
			fact.ObservedAt = now.Add(-3 * time.Hour)
			fact.ExpiresAt = now.Add(-time.Hour)
		}
		if language == "wrong" {
			fact.Language = "fra"
			fact.SourceTitleID = "tmdb:movie:999"
		}
		raw, _ := json.Marshal(fact)
		if language == "malformed" {
			raw = []byte("{ malformed provider sidecar")
		}
		key := sha1.Sum([]byte(fmt.Sprintf("tmdb:movie:original_language_evidence:v1:%d", i+1)))
		path := filepath.Join(dir, "metadata", hex.EncodeToString(key[:])+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if i < 6 {
			sum := sha256.Sum256(raw)
			hashes[path] = hex.EncodeToString(sum[:])
		}
	}
	stored.items = append(stored.items, models.HiddenItem{ID: "tmdb:movie:1", MediaType: "series", Name: "Wrong kind", HiddenAt: hiddenAt}, models.HiddenItem{ID: "manual-unbound", MediaType: "movie", Name: "Manual French", ExternalIDs: map[string]string{"tmdb": "2", "originalLanguage": "fra"}, HiddenAt: hiddenAt})
	transport := &catalogueWireTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = old })
	return h, stored, boundary, transport, hashes
}
func TestHiddenCatalogueLanguageActualCacheAndWireContract(t *testing.T) {
	h, stored, reader, transport, hashes := hiddenCatalogueWireFixture(t)
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	for _, c := range []struct {
		name, query  string
		total, count int
	}{
		{"hidden-unfiltered.json", "limit=2&sortBy=name", 12, 2},
		{"hidden-selected.json", "originalLanguages=fra,nob&limit=1&sortBy=name", 2, 1},
		{"hidden-continuation.json", "originalLanguages=fra,nob&limit=1&offset=1&sortBy=name", 2, 1},
		{"hidden-norwegian.json", "originalLanguages=nob,nno,nor&limit=0&sortBy=name", 3, 3},
		{"hidden-rare.json", "originalLanguages=haw", 1, 1},
		{"hidden-unknown.json", "includeUnknownOriginalLanguage=true&limit=0&sortBy=name", 6, 6},
		{"hidden-code-or-unknown.json", "originalLanguages=fra&includeUnknownOriginalLanguage=true&limit=0&sortBy=name", 7, 7},
		{"hidden-wrongkind.json", "originalLanguages=eng&limit=0", 1, 1},
		{"hidden-empty.json", "originalLanguages=isl", 0, 0},
		{"hidden-random-pool.json", "originalLanguages=fra,nob&limit=0&sortBy=name", 2, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			request := httptest.NewRequest("GET", "/api/users/profile-a/display-list?source=hidden&"+c.query, nil)
			request = mux.SetURLVars(request, map[string]string{"userID": "profile-a"})
			if err := validator.ValidateRequest(request, nil); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			h.Get(response, request)
			payload, items := hiddenManagementDecode(t, response)
			if payload.Total != c.total || len(items) != c.count {
				t.Fatalf("cache-backed total%d count%d expected%d/%d", payload.Total, len(items), c.total, c.count)
			}
			if err := validator.ValidateResponse(request, response.Code, response.Header(), response.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			var object map[string]json.RawMessage
			_ = json.Unmarshal(response.Body.Bytes(), &object)
			if strings.Contains(string(object["items"]), "addedAt") || strings.Contains(string(object["items"]), "originalLanguageEvidence") {
				t.Fatal("original HiddenItem shape replaced by enriched DTO")
			}
			for _, item := range items {
				found := false
				for _, original := range stored.items {
					if reflect.DeepEqual(item, original) {
						found = true
						break
					}
				}
				if !found {
					t.Fatal("hidden fields/ID/timestamp changed")
				}
			}
			fixtures[c.name] = append([]byte(nil), response.Body.Bytes()...)
		})
	}
	for _, q := range []string{"watchStatus=watched", "sortBy=rating", "includeUnwatchedCounts=true"} {
		response := hiddenManagementGet(t, h, q)
		request := httptest.NewRequest("GET", "/api/users/profile-a/display-list?source=hidden&"+q, nil)
		if err := validator.ValidateResponse(request, response.Code, response.Header(), response.Body.Bytes()); err != nil {
			t.Fatal("unsupported requirement schema", err)
		}
	}
	for path, expected := range hashes {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != expected {
			t.Fatal("cache observation renewed")
		}
	}
	if transport.calls.Load() != 0 || stored.selfFilters.Load() != 0 || reader.reads.Load() == 0 {
		t.Fatal("cache-only boundary escaped")
	}
	if dir := os.Getenv("HIDDEN_CATALOGUE_CONTRACT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		manifest := map[string]string{}
		for name, body := range fixtures {
			if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(body)
			manifest[name] = hex.EncodeToString(sum[:])
		}
		raw, _ := json.MarshalIndent(manifest, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
func TestHiddenCatalogueLanguageConcurrentCacheOnlyQueries(t *testing.T) {
	h, _, _, transport, _ := hiddenCatalogueWireFixture(t)
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Go(func() {
			for j := 0; j < 3; j++ {
				w := hiddenManagementGet(t, h, "originalLanguages=fra,nob&limit=1&offset=1&sortBy=name")
				_, items := hiddenManagementDecode(t, w)
				if len(items) != 1 || items[0].ID != "tmdb:movie:3" {
					t.Error("concurrent selection changed")
				}
			}
		})
	}
	group.Wait()
	if transport.calls.Load() != 0 {
		t.Fatal("concurrent query caused provider work")
	}
}
