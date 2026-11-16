package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
)

// Native production builders export these URLs. The loopback handler uses the
// actual cached-evidence reader and real bounded rating receipt service over a
// synthetic saved-rating repository. It is not login, PostgreSQL or Android proof.
func TestNativeLovedStoredRequests(t *testing.T) {
	dir := os.Getenv("NATIVE_LOVED_STORED_CONTRACT_DIR")
	if dir == "" {
		t.Skip("native stored-Love request exports not configured")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "native-loved-stored-queries.json"))
	if err != nil {
		t.Fatal("read native request exports:", err)
	}
	if len(raw) > 32*1024 {
		t.Fatal("unexpectedly large request export")
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
		{"selected", 2, []string{"movie/tmdb:movie:2"}},
		{"continuation", 2, []string{"movie/tmdb:movie:3"}},
		{"unknown-only", 6, []string{"movie/tmdb:movie:7", "movie/tmdb:movie:8", "movie/tmdb:movie:9", "movie/tmdb:movie:10", "movie/manual-unbound", "series/tmdb:movie:1"}},
		{"rare", 1, []string{"movie/tmdb:movie:6"}},
		{"random-pool", 2, []string{"movie/tmdb:movie:2", "movie/tmdb:movie:3"}},
		{"explicit-rate-refresh", 2, []string{"movie/tmdb:movie:3"}},
	}
	if len(requests) != len(cases) {
		t.Fatal("exactly six native requests required")
	}
	f, transport, hashes := lovedStoredWireFixture(t)
	// Rebind only synthetic fixture identity, never rewrite the captured URLs.
	f.repo.items["profile-a"] = f.repo.items["profile"]
	delete(f.repo.items, "profile")
	f.meta.UsersService = fallbackProfiles{models.User{ID: "profile-a", AccountID: "account"}}
	before := append([]byte(nil), f.repo.snapshot()...)
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/display-list", f.display.Get).Methods(http.MethodGet)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(profile, account string) bool { return profile == "profile-a" && account == "account" })(router).ServeHTTP(w, r)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	local := &http.Transport{Proxy: nil}
	t.Cleanup(local.CloseIdleConnections)
	client := &http.Client{Transport: local, Timeout: 5 * time.Second}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			value, found := requests[c.name]
			if !found {
				t.Fatal("missing required native request")
			}
			u, err := url.ParseRequestURI(value)
			if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Path != "/api/users/profile-a/display-list" || u.Query().Get("source") != "loved-stored" {
				t.Fatal("request is not the original relative stored-Love profile route")
			}
			req, err := http.NewRequest(http.MethodGet, server.URL+value, nil)
			if err != nil {
				t.Fatal(err)
			}
			clientID := "fixture-client"
			if c.name == "explicit-rate-refresh" {
				clientID = "fresh-refresh-client"
				if _, editable := f.ratings.Resolve("profile-a", clientID, "movie", "tmdb:movie:3"); editable {
					t.Fatal("refresh client already has a delivery receipt")
				}
			}
			req.Header.Set("X-Client-ID", clientID)
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			reads := f.repo.calls.Load()
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("native request status=%d", resp.StatusCode)
			}
			var page lovedStoredReply
			if err := json.Unmarshal(body, &page); err != nil {
				t.Fatal(err)
			}
			var ids []string
			for _, item := range page.Items {
				ids = append(ids, item.Title.MediaType+"/"+item.Title.ID)
				if item.Title.PersonalRating != "love" || item.Rank <= 0 || item.UpdatedAt.IsZero() {
					t.Fatal("stored row identity/state/time changed")
				}
			}
			if page.Source != "loved-stored" || !page.Supported || page.Total != c.total || !reflect.DeepEqual(ids, c.ids) || f.repo.calls.Load()-reads != 1 {
				t.Fatalf("wrong complete page: total=%d ids=%v", page.Total, ids)
			}
			if resp.Header.Get("Cache-Control") != "private, no-store" {
				t.Fatal("private response became cacheable")
			}
			if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, body); err != nil {
				t.Fatal(err)
			}
			if c.name == "explicit-rate-refresh" {
				item := page.Items[0]
				if item.Rank != 2 {
					t.Fatal("row refresh lost filtered global rank")
				}
				actual, editable := f.ratings.Resolve("profile-a", clientID, item.Title.MediaType, item.Title.ID)
				if !editable || actual.ID != item.Title.ID {
					t.Fatal("actual server delivery receipt not refreshed")
				}
				if _, ok := f.ratings.Resolve("profile-a", "different-client", item.Title.MediaType, item.Title.ID); ok {
					t.Fatal("receipt leaked to another client")
				}
				if _, ok := f.ratings.Resolve("different-profile", clientID, item.Title.MediaType, item.Title.ID); ok {
					t.Fatal("receipt leaked to another profile")
				}
			}
		})
	}
	if !reflect.DeepEqual(before, f.repo.snapshot()) || f.repo.writes.Load() != 0 || transport.calls.Load() != 0 || f.cache.calls.Load() == 0 {
		t.Fatal("native queries mutated preferences or escaped cache-only evidence")
	}
	for path, expected := range hashes {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != expected {
			t.Fatal("native query renewed original-language evidence")
		}
	}
}
