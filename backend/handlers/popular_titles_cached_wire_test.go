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

func TestPopularTitlesCachedActualHTTPProviderEvidenceAndReadOnlyInputs(t *testing.T) {
	now := time.Now().UTC()
	rows := map[string][]models.WatchHistoryItem{"alice": {}, "anonymous": {}}
	for i := 1; i <= 10; i++ {
		for user := range rows {
			id := fmt.Sprintf("tmdb:movie:%d", i)
			at := now.Add(-time.Duration(i) * time.Minute)
			if user == "alice" && i == 2 {
				id = "tmdb:2"
				at = at.Add(time.Second)
			}
			rows[user] = append(rows[user], recentCachedMovie(id, fmt.Sprintf("Stored %02d", i), at))
		}
	}
	rows["private"] = []models.WatchHistoryItem{recentCachedMovie("tmdb:movie:999", "PRIVATE_NONSHARED_FILM", now)}
	f := newPopularCachedFixture(t, rows)
	f.profiles.change("profile", func(u *models.User) { u.PinHash = "synthetic-private-pin-fingerprint" })
	cacheDir := t.TempDir()
	f.cache.reader = metadata.NewService("", "", "eng", cacheDir, 24, false, metadata.MDBListConfig{})
	validHashes := map[string][32]byte{}
	for i, lang := range []string{"en", "fr", "nob", "nno", "nor", "haw", "", "expired", "wrong", "malformed"} {
		if lang == "" {
			continue
		}
		id := fmt.Sprintf("tmdb:movie:%d", i+1)
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
		raw, err := json.Marshal(fact)
		if err != nil {
			t.Fatal(err)
		}
		if lang == "malformed" {
			raw = []byte("{ invalid cached provider observation")
		}
		key := sha1.Sum([]byte(fmt.Sprintf("tmdb:movie:original_language_evidence:v1:%d", i+1)))
		path := filepath.Join(cacheDir, "metadata", hex.EncodeToString(key[:])+".json")
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if i < 6 {
			validHashes[path] = sha256.Sum256(raw)
		}
	}
	settingsPath := f.meta.CfgManager.ConfigPath()
	settingsRaw := []byte("{\n  \"homeShelves\": {\"popularOnServerWindowDays\": 120},\n  \"sentinel\": \"preserve exact bytes\"\n}\n")
	if err := os.WriteFile(settingsPath, settingsRaw, 0600); err != nil {
		t.Fatal(err)
	}
	unchanged := map[string][32]byte{}
	for _, path := range []string{settingsPath, filepath.Join(f.dir, "watched_items.json")} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		unchanged[path] = sha256.Sum256(raw)
	}
	beforeProfiles := f.profiles.ListAll()
	beforeRatings := string(f.repo.snapshot())
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/display-list", f.display.Get)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" })(router).ServeHTTP(w, r)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := &http.Transport{Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		q     string
		total int
		ids   []string
	}{
		{"sortBy=name&limit=2", 10, []string{"tmdb:movie:1", "tmdb:2"}},
		{"originalLanguages=fra", 1, []string{"tmdb:2"}},
		{"originalLanguages=nob,nno,nor&sortBy=name", 3, []string{"tmdb:movie:3", "tmdb:movie:4", "tmdb:movie:5"}},
		{"originalLanguages=haw", 1, []string{"tmdb:movie:6"}},
		{"includeUnknownOriginalLanguage=true&sortBy=name", 4, []string{"tmdb:movie:7", "tmdb:movie:8", "tmdb:movie:9", "tmdb:movie:10"}},
		{"originalLanguages=fra&includeUnknownOriginalLanguage=true&sortBy=name&limit=2&offset=1", 5, []string{"tmdb:movie:7", "tmdb:movie:8"}},
		{"originalLanguages=isl", 0, []string{}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			req, err := http.NewRequest("GET", server.URL+"/api/users/profile/display-list?source=popular-on-server-cached&"+tc.q, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Client-ID", "synthetic-client")
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 200 || resp.Header.Get("Cache-Control") != "private, no-store" {
				t.Fatalf("HTTP status=%d body=%s", resp.StatusCode, raw)
			}
			if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, raw); err != nil {
				t.Fatal(err)
			}
			var v recentCachedReply
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Fatal(err)
			}
			if v.Source != "popular-on-server-cached" || v.Total != tc.total || !reflect.DeepEqual(recentCachedIDs(v), tc.ids) || !v.OriginalLanguageFilterSupported {
				t.Fatalf("wrong cache-only popular page: %+v", v)
			}
			for _, item := range v.Items {
				if item.Rank != 2 || item.Title.CardSubtitle != "2 views" || item.Title.OriginalLanguageEvidence != nil || item.Title.Language != "" || item.Title.SourceAvailability != nil || item.Title.Poster != nil || len(item.Title.Ratings) > 0 {
					t.Fatalf("browse enriched a count card: %+v", item)
				}
			}
			for _, secret := range []string{"PRIVATE_", "synthetic-private-pin-fingerprint", `"group"`, `"userId"`, `"revisions"`, `"watch"`, "Alice", "Fellow user"} {
				if strings.Contains(string(raw), secret) {
					t.Fatalf("contributor or private field escaped: %s", secret)
				}
			}
		})
	}
	for path, want := range validHashes {
		raw, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(raw) != want {
			t.Fatal("browse renewed valid evidence")
		}
	}
	for path, want := range unchanged {
		raw, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(raw) != want {
			t.Fatal("browse changed persisted settings/history")
		}
	}
	beforeByID := map[string]models.User{}
	for _, u := range beforeProfiles {
		beforeByID[u.ID] = u
	}
	afterProfiles := f.profiles.ListAll()
	if len(afterProfiles) != len(beforeProfiles) {
		t.Fatal("browse changed profiles")
	}
	for _, u := range afterProfiles {
		if !reflect.DeepEqual(u, beforeByID[u.ID]) {
			t.Fatal("browse changed a profile/private PIN fingerprint")
		}
	}
	if string(f.repo.snapshot()) != beforeRatings || f.repo.calls.Load() != 0 || f.repo.writes.Load() != 0 || f.ratings.observes.Load() != 0 {
		t.Fatal("browse invoked ratings or observation writes")
	}
}
