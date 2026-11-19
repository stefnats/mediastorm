package handlers

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/metadata"
)

type tmdbPersonCreditsCachedProfilePreferences struct {
	value     *models.UserSettings
	err       error
	reads     int
	afterRead func(int)
}

func (p *tmdbPersonCreditsCachedProfilePreferences) Get(string) (*models.UserSettings, error) {
	p.reads++
	var copy *models.UserSettings
	raw, _ := json.Marshal(p.value)
	_ = json.Unmarshal(raw, &copy)
	if p.afterRead != nil {
		p.afterRead(p.reads)
	}
	return copy, p.err
}

type tmdbPersonCreditsCachedRealFixture struct {
	*lovedStoredFixture
	profiles     *recentCachedProfiles
	preferences  *tmdbPersonCreditsCachedProfilePreferences
	service      *metadata.Service
	settingsPath string
	settingsRaw  []byte
	cachePaths   []string
}

func tmdbPersonCreditsCachedCacheFile(dir string, parts ...string) string {
	key := sha1.Sum([]byte(strings.Join(parts, ":")))
	return filepath.Join(dir, "metadata", hex.EncodeToString(key[:])+".json")
}
func writeTMDBPersonCreditsCachedJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func newTMDBPersonCreditsCachedRealFixture(t *testing.T) *tmdbPersonCreditsCachedRealFixture {
	t.Helper()
	watchingTransportGuard(t)
	base := newLovedStoredFixture(t)
	profiles := &recentCachedProfiles{items: map[string]models.User{"profile": {ID: "profile", AccountID: "account", PinHash: "synthetic-private-pin"}}}
	preferences := &tmdbPersonCreditsCachedProfilePreferences{value: &models.UserSettings{}}
	preferences.value.Metadata.PrimaryLanguage = "fra"
	base.meta.UsersService = profiles
	base.meta.UserSettings = preferences
	dir := t.TempDir()
	service := metadata.NewService("", "", "eng", dir, 24, false, metadata.MDBListConfig{})
	base.meta.Service = service
	at := time.Now().UTC().Add(-time.Hour)
	cachePaths := []string{}
	for _, locale := range []string{"en-US", "fr-US"} {
		value := metadata.CachedTMDBPersonCreditsSnapshot{Version: 1, Provider: "tmdb", SourceType: "person-credits", PersonID: 77, MetadataLanguage: locale, ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour), Candidates: make([]metadata.CachedTMDBPersonCreditsCandidate, 0, 10)}
		for i := 1; i <= 10; i++ {
			kind, primary := "movie", "movie"
			if i%2 == 0 {
				kind, primary = "series", "tv"
			}
			value.Candidates = append(value.Candidates, metadata.CachedTMDBPersonCreditsCandidate{ID: fmt.Sprintf("tmdb:%s:%d", primary, i), MediaType: kind, TMDBID: int64(i), Name: fmt.Sprintf("Stored %02d", i)})
		}
		path := tmdbPersonCreditsCachedCacheFile(dir, "tmdb", "person_credits_snapshot", "v1", "77", locale)
		writeTMDBPersonCreditsCachedJSON(t, path, value)
		cachePaths = append(cachePaths, path)
	}
	for i, language := range []string{"eng", "fra", "nob", "nno", "nor", "haw", "", "expired", "wrong", "malformed"} {
		if language == "" {
			continue
		}
		kind := "movie"
		if (i+1)%2 == 0 {
			kind = "series"
		}
		fact := models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: fmt.Sprintf("tmdb:%s:%d", kind, i+1), ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour)}
		if language == "expired" {
			fact.Language = "fra"
			fact.ExpiresAt = time.Now().Add(-time.Second)
		}
		if language == "wrong" {
			fact.Language = "fra"
			fact.SourceTitleID = "tmdb:movie:999"
		}
		path := tmdbPersonCreditsCachedCacheFile(dir, "tmdb", kind, "original_language_evidence", "v1", fmt.Sprint(i+1))
		writeTMDBPersonCreditsCachedJSON(t, path, fact)
		if language == "malformed" {
			if err := os.WriteFile(path, []byte("{ malformed fact"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if i < 6 {
			cachePaths = append(cachePaths, path)
		} // Expired cache housekeeping is allowed.
	}
	settingsPath := base.meta.CfgManager.ConfigPath()
	settingsRaw := []byte("{\n  \"metadata\": {\"language\": [\"eng\", \"fra\"], \"primaryLanguage\": \"eng\"},\n  \"syntheticSentinel\": \"preserve exact settings bytes\"\n}\n")
	if err := os.WriteFile(settingsPath, settingsRaw, 0600); err != nil {
		t.Fatal(err)
	}
	return &tmdbPersonCreditsCachedRealFixture{base, profiles, preferences, service, settingsPath, settingsRaw, cachePaths}
}

func TestTMDBPersonCreditsCachedActualHTTPUsesProfileLocaleAndCurrentProviderFacts(t *testing.T) {
	f := newTMDBPersonCreditsCachedRealFixture(t)
	hashes := map[string][32]byte{}
	for _, path := range append([]string{f.settingsPath}, f.cachePaths...) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		hashes[path] = sha256.Sum256(raw)
	}
	profileBefore, _ := cachedRecentViewerStamp(f.profiles.items["profile"])
	preferencesBefore, _ := json.Marshal(f.preferences.value)
	ratingsBefore := string(f.repo.snapshot())
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/display-list", f.display.Get)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" })(router).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	transport := &http.Transport{Proxy: nil}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query string
		total int
		ids   []string
	}{
		{"sortBy=name&limit=2", 10, []string{"tmdb:movie:1", "tmdb:tv:2"}},
		{"originalLanguages=fra", 1, []string{"tmdb:tv:2"}},
		{"originalLanguages=nob,nno,nor&sortBy=name", 3, []string{"tmdb:movie:3", "tmdb:tv:4", "tmdb:movie:5"}},
		{"originalLanguages=haw", 1, []string{"tmdb:tv:6"}},
		{"includeUnknownOriginalLanguage=true&sortBy=name", 4, []string{"tmdb:movie:7", "tmdb:tv:8", "tmdb:movie:9", "tmdb:tv:10"}},
		{"originalLanguages=fra&includeUnknownOriginalLanguage=true&sortBy=name&limit=2&offset=1", 5, []string{"tmdb:movie:7", "tmdb:tv:8"}},
		{"originalLanguages=isl", 0, []string{}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			req, err := http.NewRequest("GET", server.URL+"/api/users/profile/display-list?source=tmdb-person-credits-cached&personId=77&"+tc.query, nil)
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
			if resp.StatusCode != 200 {
				t.Fatalf("status=%d body=%s", resp.StatusCode, raw)
			}
			if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, raw); err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			recorder.Code = resp.StatusCode
			recorder.Header().Set("Cache-Control", resp.Header.Get("Cache-Control"))
			recorder.Body.Write(raw)
			observation, items, total, _ := decodeTMDBPersonCreditsCached(t, recorder)
			if total != tc.total || !reflect.DeepEqual(tmdbPersonCreditsCachedIDs(items), tc.ids) || observation.MetadataLanguage != "fr-US" {
				t.Fatalf("total%d ids%v locale%s", total, tmdbPersonCreditsCachedIDs(items), observation.MetadataLanguage)
			}
			for _, title := range items {
				if title.SourceAvailability != nil || title.OriginalLanguageEvidence != nil || title.Overview != "" || title.Year != 0 || title.Language != "" {
					t.Fatal("unobserved title data adopted")
				}
			}
		})
	}
	for path, expected := range hashes {
		raw, err := os.ReadFile(path)
		if err != nil || sha256.Sum256(raw) != expected {
			t.Fatalf("input changed: %s", filepath.Base(path))
		}
	}
	profileAfter, _ := cachedRecentViewerStamp(f.profiles.items["profile"])
	preferencesAfter, _ := json.Marshal(f.preferences.value)
	if profileBefore != profileAfter || string(preferencesBefore) != string(preferencesAfter) || ratingsBefore != string(f.repo.snapshot()) {
		t.Fatal("cached browse mutated profile, settings or ratings")
	}
}

func TestTMDBPersonCreditsCachedPureSettingsMissingMalformedAndUnavailablePaths(t *testing.T) {
	for _, state := range []string{"missing", "malformed", "trailing", "unreadable", "profile unavailable"} {
		t.Run(state, func(t *testing.T) {
			f := newTMDBPersonCreditsCachedRealFixture(t)
			path := filepath.Join(t.TempDir(), "settings.json")
			f.meta.CfgManager = config.NewManager(path)
			var original []byte
			switch state {
			case "malformed":
				original = []byte("{ malformed")
			case "trailing":
				original = []byte("{} {}")
			case "unreadable":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "profile unavailable":
				f.preferences.err = errors.New("settings unavailable")
			}
			if original != nil {
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			w := httptest.NewRecorder()
			f.display.Get(w, tmdbPersonCreditsCachedRequest(""))
			want := 503
			if state == "missing" {
				want = 200
			}
			if w.Code != want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if state == "missing" || state == "profile unavailable" {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("settings defaults materialized")
				}
			}
			if original != nil {
				raw, err := os.ReadFile(path)
				if err != nil || string(raw) != string(original) {
					t.Fatal("existing settings bytes changed")
				}
			}
		})
	}
}

func TestTMDBPersonCreditsCachedPureLocalePreferenceChangesFailClosed(t *testing.T) {
	for _, change := range []string{"profile", "global"} {
		t.Run(change, func(t *testing.T) {
			f := newTMDBPersonCreditsCachedRealFixture(t)
			f.preferences.afterRead = func(read int) {
				if read != 1 {
					return
				}
				if change == "profile" {
					f.preferences.value.Metadata.PrimaryLanguage = "eng"
				} else {
					if err := os.WriteFile(f.settingsPath, []byte("{\"metadata\":{\"language\":[\"eng\"],\"primaryLanguage\":\"eng\"}}"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			w := httptest.NewRecorder()
			f.display.Get(w, tmdbPersonCreditsCachedRequest("originalLanguages=fra"))
			if w.Code != 409 || strings.Contains(w.Body.String(), "Stored") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}
