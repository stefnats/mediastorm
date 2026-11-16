package handlers

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	"novastream/services/metadata"
)

func lovedStoredWireFixture(t *testing.T) (*lovedStoredFixture, *catalogueWireTransport, map[string]string) {
	t.Helper()
	f := newLovedStoredFixture(t)
	dir := t.TempDir()
	f.cache.reader = metadata.NewService("", "", "eng", dir, 24, false, metadata.MDBListConfig{})
	saved := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC()
	hashes := map[string]string{}
	for i, lang := range []string{"en", "fr", "nob", "nno", "nor", "haw", "", "expired", "wrong", "malformed"} {
		id := fmt.Sprintf("tmdb:movie:%d", i+1)
		f.seed(id, "movie", fmt.Sprintf("Saved %02d", i+1), "", saved.Add(time.Duration(i)*time.Minute))
		if lang == "" {
			continue
		}
		fact := models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
		if lang == "expired" {
			fact.Language = "fra"
			fact.ObservedAt = now.Add(-3 * time.Hour)
			fact.ExpiresAt = now.Add(-time.Hour)
		}
		if lang == "wrong" {
			fact.Language = "fra"
			fact.SourceTitleID = "tmdb:movie:999"
		}
		raw, _ := json.Marshal(fact)
		if lang == "malformed" {
			raw = []byte("{ invalid provider observation")
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
	f.seed("manual-unbound", "movie", "Saved scalar spoof", "", saved)
	last := len(f.repo.items["profile"]) - 1
	f.repo.items["profile"][last].Title.TMDBID = 2
	f.repo.items["profile"][last].Title.Language = "fra"
	f.seed("tmdb:movie:1", "series", "Wrong kind", "", saved)
	transport := &catalogueWireTransport{}
	old := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = old })
	return f, transport, hashes
}
func TestLovedStoredActualHTTPAndCacheWireContract(t *testing.T) {
	assertLovedStoredActualHTTPAndCacheWireContract(t)
}
func assertLovedStoredActualHTTPAndCacheWireContract(t *testing.T) {
	t.Helper()
	f, transport, hashes := lovedStoredWireFixture(t)
	before := string(f.repo.snapshot())
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/display-list", f.display.Get)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" })(router).ServeHTTP(w, r)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	actualTransport := &http.Transport{Proxy: nil}
	t.Cleanup(actualTransport.CloseIdleConnections)
	client := &http.Client{Transport: actualTransport, Timeout: 5 * time.Second}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	for _, tc := range []struct {
		name, q      string
		total, count int
	}{
		{"loved-stored-unfiltered.json", "limit=2&sortBy=name", 12, 2},
		{"loved-stored-selected.json", "originalLanguages=fra,nob&limit=1&sortBy=name", 2, 1},
		{"loved-stored-continuation.json", "originalLanguages=fra,nob&limit=1&offset=1&sortBy=name", 2, 1},
		{"loved-stored-norwegian.json", "originalLanguages=nob,nno,nor&limit=0&sortBy=name", 3, 3},
		{"loved-stored-rare.json", "originalLanguages=haw", 1, 1},
		{"loved-stored-unknown.json", "includeUnknownOriginalLanguage=true&limit=0&sortBy=name", 6, 6},
		{"loved-stored-or-unknown.json", "originalLanguages=fra&includeUnknownOriginalLanguage=true&limit=0&sortBy=name", 7, 7},
		{"loved-stored-empty.json", "originalLanguages=isl", 0, 0},
		{"loved-stored-random-pool.json", "originalLanguages=fra,nob&limit=0&sortBy=name", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", server.URL+"/api/users/profile/display-list?source=loved-stored&"+tc.q, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Client-ID", "fixture-client")
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 {
				t.Fatalf("actual HTTP status%d: %s", resp.StatusCode, body)
			}
			var v lovedStoredReply
			if err := json.Unmarshal(body, &v); err != nil {
				t.Fatal(err)
			}
			if v.Total != tc.total || len(v.Items) != tc.count || !v.Supported || resp.Header.Get("Cache-Control") != "private, no-store" {
				t.Fatalf("wrong wire page: %+v", v)
			}
			if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, body); err != nil {
				t.Fatal(err)
			}
			for _, item := range v.Items {
				found := false
				for _, stored := range f.repo.items["profile"] {
					if stored.Title.ID == item.Title.ID && stored.Title.MediaType == item.Title.MediaType && stored.Title.Name == item.Title.Name && stored.Title.Year == item.Title.Year && stored.UpdatedAt.Equal(item.UpdatedAt) {
						found = true
						break
					}
				}
				if !found {
					t.Fatal("original saved fields changed")
				}
				if item.Title.PersonalRating != "love" {
					t.Fatal("missing actual saved love")
				}
			}
			for _, canary := range []string{"PRIVATE_KEY", "aliases", "originalLanguageEvidence", "sourceAvailability", "textPoster", "watchState", "unwatchedCount", "cardSubtitle", "sourceTotal"} {
				if strings.Contains(string(body), canary) {
					t.Fatal("unexpected annotation:", canary)
				}
			}
			fixtures[tc.name] = append([]byte(nil), body...)
		})
	}
	for _, q := range []string{"watchStatus=complete", "includeFacets=true", "sortBy=rating"} {
		req, _ := http.NewRequest("GET", server.URL+"/api/users/profile/display-list?source=loved-stored&"+q, nil)
		req.Header.Set("X-Client-ID", "fixture-client")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatal("unsupported request accepted")
		}
		if err := validator.ValidateResponse(req, 400, resp.Header, body); err != nil {
			t.Fatal(err)
		}
	}
	for path, expected := range hashes {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != expected {
			t.Fatal("provider observation timestamps refreshed")
		}
	}
	if string(f.repo.snapshot()) != before || f.repo.writes.Load() != 0 || transport.calls.Load() != 0 || f.cache.calls.Load() == 0 {
		t.Fatal("complete browse escaped cache-only saved boundary")
	}
	if dir := os.Getenv("LOVED_STORED_CONTRACT_DIR"); dir != "" {
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
func TestLovedStoredLegacyShelfContractRemainsSeparate(t *testing.T) {
	h, _, repo, title := ratingFixture(t)
	meta := &lovedCacheOnlyMetadata{}
	h.Service = meta
	for i := 0; i < 653; i++ {
		copy := title
		copy.ID = fmt.Sprintf("tmdb:movie:%d", i+1)
		copy.TMDBID = int64(i + 1)
		copy.Name = fmt.Sprintf("Saved %03d", i)
		repo.items["profile"] = append(repo.items["profile"], models.TitleRating{Title: copy, Aliases: []string{fmt.Sprintf("movie:tmdb:%d", i+1)}, Value: "love", UpdatedAt: time.Unix(int64(i), 0)})
	}
	h.HistoryService = &fakeMetadataHistoryService{history: []models.WatchHistoryItem{{MediaType: "movie", ItemID: "tmdb:movie:653", Watched: true}}}
	d := NewDisplayListHandler(nil, nil, nil)
	d.SetMetadataHandler(h)
	d.SetHiddenItemsService(newLovedStoredFixture(t).hidden)
	w := httptest.NewRecorder()
	d.Get(w, lovedRequest("limit=1"))
	var v lovedReply
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || v.Total != 1 || len(v.Items) != 1 || v.Items[0].Title.WatchState != "complete" || v.Items[0].Title.TextPoster == nil || v.Items[0].Title.PersonalRating != "love" || strings.Contains(w.Body.String(), "originalLanguageFilterSupported") {
		t.Fatal("legacy shelf altered", w.Code, w.Body.String())
	}
	var legacyTotals struct {
		SourceTotal int `json:"sourceTotal"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &legacyTotals)
	if legacyTotals.SourceTotal != 500 {
		t.Fatal("legacy discovery/hidden paging contract changed")
	}
	if _, ok := h.TitleRatings.Resolve("profile", "fixture-client", "movie", v.Items[0].Title.ID); !ok {
		t.Fatal("legacy rating receipt lost")
	}
	if !reflect.DeepEqual(v.Items[0].Title.Name, "Saved 652") {
		t.Fatal("legacy order changed")
	}
}

func lovedStoredLanguageOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	f, _, _ := lovedStoredWireFixture(t)
	for _, q := range []string{"limit=1&sortBy=name", "originalLanguages=fra,nob&limit=1&offset=1", "includeUnknownOriginalLanguage=true", "watchStatus=complete"} {
		r := lovedStoredRequest(q)
		w := httptest.NewRecorder()
		f.display.Get(w, r)
		if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
}
