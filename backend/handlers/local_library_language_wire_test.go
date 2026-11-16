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
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/metadata"
)

// The actual cache getter consumes synthetic previously-observed sidecars. No
// producer/network call is implied; normal raw-provider provenance is covered
// by metadata's existing observation tests.
func localLibraryLanguageCache(t *testing.T) (*hiddenManagementMetadata, map[string]string) {
	t.Helper()
	dir := t.TempDir()
	svc := metadata.NewService("", "", "eng", dir, 24, false, metadata.MDBListConfig{})
	reader := &hiddenManagementMetadata{reader: svc}
	hashes := map[string]string{}
	now := time.Now().UTC()
	for i, language := range []string{"en", "fr", "haw", "nob", "nno", "nor", "expired", "wrong", "malformed", ""} {
		if language == "" {
			continue
		}
		id := fmt.Sprintf("tmdb:movie:%d", i+1)
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
			raw = []byte("{ invalid synthetic sidecar")
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
	return reader, hashes
}
func TestLocalLibraryLanguageActualCacheWireAndOpenAPI(t *testing.T) {
	reader, hashes := localLibraryLanguageCache(t)
	stored := &localLibraryLanguageGroups{references: map[string]string{}}
	for n := 1; n <= 10; n++ {
		id := fmt.Sprintf("group-%02d", n)
		stored.references[id] = fmt.Sprintf("tmdb:movie:%d", n)
		stored.groups = append(stored.groups, models.LocalMediaItemGroup{ID: id, GroupType: "title", LibraryType: models.LocalMediaLibraryTypeMovie, Title: fmt.Sprintf("Saved %02d", n), ItemCount: 3, Year: 2020})
	}
	stored.groups = append(stored.groups, models.LocalMediaItemGroup{ID: "unbound", GroupType: "title", LibraryType: models.LocalMediaLibraryTypeMovie, Title: "Unbound", ItemCount: 2, TMDBID: 2})
	h := NewLocalMediaHandler(stored, nil, false)
	h.SetMetadataLanguageProviders(reader, nil, nil)
	old := http.DefaultTransport
	transport := &catalogueWireTransport{}
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = old })
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	for _, tc := range []struct {
		name, q      string
		total, count int
	}{
		{"local-cards-unfiltered.json", "limit=2", 11, 2},
		{"local-cards-selected.json", "originalLanguages=fra,haw&limit=1", 2, 1},
		{"local-cards-continuation.json", "originalLanguages=fra,haw&limit=1&offset=1", 2, 1},
		{"local-cards-norwegian.json", "originalLanguages=nob,nno,nor", 3, 3},
		{"local-cards-rare.json", "originalLanguages=haw", 1, 1},
		{"local-cards-unknown.json", "includeUnknownOriginalLanguage=true", 5, 5},
		{"local-cards-code-or-unknown.json", "originalLanguages=fra&includeUnknownOriginalLanguage=true", 6, 6},
		{"local-cards-empty.json", "originalLanguages=isl", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/api/library/libraries/local/groups?include=cards&profileId=profile-a&"+tc.q, nil)
			req = mux.SetURLVars(req, map[string]string{"libraryID": "local"})
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ListGroups(w, req)
			out := localLibraryLanguageDecode(t, w)
			if out.Total != tc.total || len(out.Groups) != tc.count || !out.OriginalLanguageFilterSupported {
				t.Fatalf("total/count %+v", out)
			}
			if err := validator.ValidateResponse(req, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(w.Body.String(), "originalLanguageEvidence") {
				t.Fatal("evidence leaked into original card shape")
			}
			fixtures[tc.name] = append([]byte(nil), w.Body.Bytes()...)
		})
	}
	for path, want := range hashes {
		raw, err := os.ReadFile(path)
		sum := sha256.Sum256(raw)
		if err != nil || hex.EncodeToString(sum[:]) != want {
			t.Fatal("observation timestamp/content changed")
		}
	}
	if transport.calls.Load() != 0 || reader.reads.Load() == 0 {
		t.Fatal("cache boundary escaped")
	}
	if dir := os.Getenv("LOCAL_LIBRARY_LANGUAGE_CONTRACT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		manifest := map[string]string{}
		for name, raw := range fixtures {
			if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			manifest[name] = hex.EncodeToString(sum[:])
		}
		raw, _ := json.MarshalIndent(manifest, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func localLibraryLanguageOpenAPIContract(t *testing.T, v *openapi.Validator) {
	h, _, _ := localLibraryLanguageFixture(t)
	positive := exercise(t, v, contractCall{method: "GET", target: "/api/library/libraries/local/groups?include=cards&profileId=profile-a&originalLanguages=fra", vars: map[string]string{"libraryID": "local"}}, h.ListGroups, 200)
	var page models.LocalMediaGroupListResult
	if err := json.Unmarshal(positive.Body.Bytes(), &page); err != nil || !page.OriginalLanguageFilterSupported || page.Total != 1 || len(page.Groups) != 1 || page.Groups[0].ID != "two" {
		t.Fatal("effective library page contract")
	}
	exercise(t, v, contractCall{method: "GET", target: "/api/library/libraries/local/groups?originalLanguages=fra", vars: map[string]string{"libraryID": "local"}}, h.ListGroups, 400)
}
