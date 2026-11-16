package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"net/http/httptest"
	"net/url"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/playback"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCatalogueLanguageActualNativeQueries(t *testing.T) {
	root := filepath.Join("testdata", "catalogue-language-native")
	raw, err := os.ReadFile(filepath.Join(root, "sha256.json"))
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]string{}
	if err := json.Unmarshal(raw, &hashes); err != nil {
		t.Fatal(err)
	}
	meta := &catalogueLanguageMetadata{facts: map[string]*models.OriginalLanguageEvidence{}}
	now := time.Now()
	rows := make([]models.WatchlistItem, 0, 77)
	for i := 0; i < 77; i++ {
		lang := "fr"
		if i >= 70 {
			lang = "de"
		}
		if i >= 75 {
			lang = ""
		}
		id := fmt.Sprintf("tmdb:movie:%d", 100+i)
		rows = append(rows, models.WatchlistItem{ID: id, MediaType: "movie", Name: fmt.Sprintf("Saved %03d", i), Year: 2020})
		if lang != "" {
			meta.facts[id] = &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
		}
	}
	custom := []models.WatchlistItem{{ID: "tmdb:movie:500", MediaType: "movie", Name: "Spanish", Year: 2020}, {ID: "tmdb:movie:501", MediaType: "movie", Name: "Unknown", Year: 2020}, {ID: "tmdb:movie:502", MediaType: "movie", Name: "French", Year: 2020}}
	for id, lang := range map[string]string{"tmdb:movie:500": "es", "tmdb:movie:502": "fr"} {
		meta.facts[id] = &models.OriginalLanguageEvidence{Language: lang, Source: "tmdb", SourceTitleID: id, ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
	}
	h := NewDisplayListHandler(catalogueWireSaved{items: rows}, catalogueWireCustom{items: custom}, nil)
	h.MetadataService = meta
	entries := []*playback.PrequeueEntry{}
	for _, item := range custom {
		entries = append(entries, &playback.PrequeueEntry{TitleID: item.ID, MediaType: item.MediaType, TitleName: item.Name, Year: item.Year, Persistent: true, UserID: "default"})
	}
	entries = append(entries, &playback.PrequeueEntry{TitleID: "tmdb:movie:999", MediaType: "movie", TitleName: "Foreign unknown", Persistent: true, UserID: "another-profile"})
	h.PrequeueStore = catalogueLanguagePrequeue{entries}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		total, count int
		first        string
	}{
		{"watchlist-languages.json", 75, 60, "Saved 000"},
		{"watchlist-next-page.json", 70, 10, "Saved 060"},
		{"watchlist-random.json", 72, 1, "Saved 002"},
		{"custom-language-or-unknown.json", 2, 2, "Spanish"},
		{"custom-unfiltered.json", 3, 3, "Spanish"},
		{"permanent-unknown-only.json", 1, 1, "Unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(root, tc.name))
			if err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(data)
			if hashes[tc.name] != hex.EncodeToString(sum[:]) {
				t.Fatal("production native fixture hash changed")
			}
			var wire struct {
				Method, Path string
				Query        map[string]string
			}
			if err := json.Unmarshal(data, &wire); err != nil {
				t.Fatal(err)
			}
			if wire.Method != "GET" || wire.Path != "/api/users/default/display-list" {
				t.Fatalf("unexpected native route %+v", wire)
			}
			query := url.Values{}
			for k, v := range wire.Query {
				query.Set(k, v)
			}
			r := httptest.NewRequest(wire.Method, wire.Path+"?"+query.Encode(), nil)
			r = mux.SetURLVars(r, map[string]string{"userID": "default"})
			if err := validator.ValidateRequest(r, nil); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.Get(w, r)
			if w.Code != 200 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if err := validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Total     int               `json:"total"`
				Items     []json.RawMessage `json:"items"`
				Supported bool              `json:"originalLanguageFilterSupported"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Total != tc.total || len(payload.Items) != tc.count || !payload.Supported || !strings.Contains(string(payload.Items[0]), tc.first) {
				t.Fatalf("unexpected filtered native response: %s", w.Body.String())
			}
		})
	}
}
