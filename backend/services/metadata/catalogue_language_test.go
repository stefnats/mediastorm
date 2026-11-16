package metadata

import (
	"encoding/json"
	"net/http"
	"novastream/models"
	"reflect"
	"testing"
	"time"
)

func TestCachedCatalogueLanguageExactAnchorAndFreshness(t *testing.T) {
	s, _ := shelfTestService(t)
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("cache-only getter contacted provider")
		return nil, nil
	})}
	now := time.Now().UTC()
	good := models.OriginalLanguageEvidence{Language: "is", Source: "tmdb", SourceTitleID: "tmdb:movie:41", ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
	seed := func(f models.OriginalLanguageEvidence) {
		t.Helper()
		if err := s.cache.set(originalLanguageCacheKey("movie", 41), f); err != nil {
			t.Fatal(err)
		}
	}
	seed(good)
	for _, tc := range []struct {
		id, kind string
		want     bool
	}{
		{"tmdb:movie:41", "movie", true}, {"tmdb:series:41", "movie", false}, {"tmdb:movie:041", "movie", false}, {"tmdb:movie:41:episode:1", "movie", false},
		{"manual:41", "movie", false}, {"tvdb:movie:41", "movie", false}, {"imdb:tt0041", "movie", false}, {"tmdb:movie:41", "series", false}, {"tmdb:movie:41", "episode", false},
	} {
		t.Run(tc.id+"/"+tc.kind, func(t *testing.T) {
			got := s.GetCachedOriginalLanguageEvidence(tc.id, tc.kind, now)
			if (got != nil) != tc.want {
				t.Fatalf("fact=%+v", got)
			}
			if got != nil {
				if !reflect.DeepEqual(*got, good) {
					t.Fatal("timestamps or values refreshed")
				}
				got.Language = "fr"
			}
		})
	}
	for name, mutate := range map[string]func(*models.OriginalLanguageEvidence){
		"expired":       func(f *models.OriginalLanguageEvidence) { f.ExpiresAt = now.Add(-time.Second) },
		"future":        func(f *models.OriginalLanguageEvidence) { f.ObservedAt = now.Add(time.Second) },
		"wrongID":       func(f *models.OriginalLanguageEvidence) { f.SourceTitleID = "tmdb:movie:42" },
		"wrongKind":     func(f *models.OriginalLanguageEvidence) { f.SourceTitleID = "tmdb:series:41" },
		"wrongProvider": func(f *models.OriginalLanguageEvidence) { f.Source = "manual" },
		"overTTL":       func(f *models.OriginalLanguageEvidence) { f.ExpiresAt = now.Add(48 * time.Hour) },
		"scalarUnknown": func(f *models.OriginalLanguageEvidence) { f.Language = "und" },
	} {
		t.Run(name, func(t *testing.T) {
			f := good
			mutate(&f)
			seed(f)
			if got := s.GetCachedOriginalLanguageEvidence("tmdb:movie:41", "movie", now); got != nil {
				t.Fatalf("invalid fact: %+v", got)
			}
		})
	}
	seed(good)
	if got := s.GetCachedOriginalLanguageEvidence("tmdb:movie:41", "movie", time.Time{}); got != nil {
		t.Fatal("zero time accepted")
	}
	if got := s.GetCachedOriginalLanguageEvidence("tmdb:movie:41", "movie", good.ExpiresAt); got != nil {
		t.Fatal("expiry boundary accepted")
	}
}

func TestCachedCatalogueLanguageProviderFindAliasesOnly(t *testing.T) {
	s, _ := shelfTestService(t)
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { t.Fatal("mapping miss started demand"); return nil, nil })}
	now := time.Now().UTC()
	f := models.OriginalLanguageEvidence{Language: "ja", Source: "tmdb", SourceTitleID: "tmdb:series:73", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
	if err := s.cache.set(originalLanguageCacheKey("series", 73), f); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"tvdb:series:4", "tvdb:4", "imdb:tt0004", "tt0004", "tmdb:tv:73", "tmdb:series:73"} {
		if got := s.GetCachedOriginalLanguageEvidence(id, "series", now); (got != nil) != (id == "tmdb:tv:73" || id == "tmdb:series:73") {
			t.Fatalf("cold mapping %s: %+v", id, got)
		}
	}
	if err := s.cache.set(cacheKey("tmdb", "resolve", "series", "tvdb", "4"), int64(73)); err != nil {
		t.Fatal(err)
	}
	if err := s.idCache.set(cacheKey("id", "imdb-to-tmdb", "tv", "tt0004"), int64(73)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"tvdb:series:4", "tvdb:4", "imdb:tt0004", "tt0004"} {
		if got := s.GetCachedOriginalLanguageEvidence(id, "series", now); got == nil || !reflect.DeepEqual(*got, f) {
			t.Fatalf("cached provider mapping %s: %+v", id, got)
		}
	}
	// An assembled cached Title may contain caller-derived side IDs; only the
	// provider find mapping establishes an alias for this cache-only projection.
	title := models.Title{ID: "tvdb:series:99", MediaType: "series", TVDBID: 99, TMDBID: 73, Language: "ja", OriginalLanguageEvidence: &f}
	if err := s.cache.set(cacheKey("tvdb", "series", "info", "v1", s.client.language, "99"), title); err != nil {
		t.Fatal(err)
	}
	if got := s.GetCachedOriginalLanguageEvidence(title.ID, "series", now); got != nil {
		t.Fatal("assembled side ID granted alias")
	}
}

func TestCachedCatalogueLanguageActualObservedResponseAndNoExtraHTTP(t *testing.T) {
	for _, kind := range []string{"movie", "series"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := shelfTestService(t)
			calls := 0
			s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				calls++
				return jsonResponse(200, `{"id":41,"title":"Synthetic","name":"Synthetic","original_language":"haw"}`, nil), nil
			})}
			var title *models.Title
			var err error
			if kind == "movie" {
				title, err = s.tmdb.movieDetails(t.Context(), 41)
			} else {
				title, err = s.tmdb.seriesDetails(t.Context(), 41)
			}
			if err != nil || title == nil || title.OriginalLanguageEvidence == nil {
				t.Fatalf("producer %v %+v", err, title)
			}
			before := *title.OriginalLanguageEvidence
			for i := 0; i < 5; i++ {
				fact := s.GetCachedOriginalLanguageEvidence("tmdb:"+kind+":41", kind, time.Now())
				if fact == nil || !reflect.DeepEqual(*fact, before) {
					t.Fatal("actual observed fact changed")
				}
				fact.Language = "fr"
			}
			if calls != 1 {
				t.Fatalf("cache reads made %d total HTTP calls", calls)
			}
		})
	}
}

func TestCachedCatalogueLanguageRawTVDBMovieResponseAlias(t *testing.T) {
	s, _ := shelfTestService(t)
	calls := 0
	s.client.apiKey = "synthetic-key"
	s.client.token = "synthetic-token"
	s.client.tokenExpiry = time.Now().Add(time.Hour)
	s.client.minInterval = 0
	s.client.httpc = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Path != "/v4/movies/4/extended" {
			t.Fatalf("unexpected route %s", r.URL.Path)
		}
		return jsonResponse(200, `{"data":{"id":4,"name":"Synthetic","remoteIds":[{"id":"41","sourceName":"TheMovieDB.com"}]}}`, nil), nil
	})}
	s.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return jsonResponse(200, `{"id":41,"title":"Synthetic","original_language":"es"}`, nil), nil
	})}
	raw, err := s.cachedMovieExtended(4, nil)
	if err != nil {
		t.Fatal(err)
	}
	title, err := s.tmdb.movieDetails(t.Context(), 41)
	if err != nil || title == nil {
		t.Fatalf("producer %v", err)
	}
	at := time.Now()
	for i := 0; i < 3; i++ {
		got := s.GetCachedOriginalLanguageEvidence("tvdb:movie:4", "movie", at)
		if got == nil || !reflect.DeepEqual(got, title.OriginalLanguageEvidence) {
			t.Fatalf("actual raw alias %+v", got)
		}
	}
	if calls != 2 {
		t.Fatalf("getter caused HTTP: calls=%d", calls)
	}
	for name, body := range map[string]string{
		"wrongID":                 `{"id":5,"remoteIds":[{"id":"41","sourceName":"TheMovieDB.com"}]}`,
		"conflicting":             `{"id":4,"remoteIds":[{"id":"41","sourceName":"TheMovieDB.com"},{"id":"42","sourceName":"TMDB"}]}`,
		"malformed":               `{"id":4,"remoteIds":[{"id":"041","sourceName":"TheMovieDB.com"}]}`,
		"negative":                `{"id":4,"remoteIds":[{"id":"-41","sourceName":"TheMovieDB.com"}]}`,
		"missing":                 `{"id":4,"remoteIds":[{"id":"41","sourceName":"IMDB"}]}`,
		"wrongSource":             `{"id":4,"remoteIds":[{"id":"41","sourceName":"not-tmdb"}]}`,
		"malformedAlongsideValid": `{"id":4,"remoteIds":[{"id":"41","sourceName":"TheMovieDB.com"},{"id":"","sourceName":"TMDB"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			var bad tvdbMovieExtendedData
			if err := json.Unmarshal([]byte(body), &bad); err != nil {
				t.Fatal(err)
			}
			if err := s.cache.set(movieExtendedCacheID(4), bad); err != nil {
				t.Fatal(err)
			}
			if got := s.GetCachedOriginalLanguageEvidence("tvdb:movie:4", "movie", at); got != nil {
				t.Fatalf("invalid raw alias granted %+v", got)
			}
		})
	}
	if calls != 2 {
		t.Fatalf("invalid aliases caused HTTP: %d", calls)
	}
	if err := s.cache.set(movieExtendedCacheID(4), raw); err != nil {
		t.Fatal(err)
	}
	// Mutating a returned raw snapshot cannot change the persisted alias.
	raw.ID = 999
	if got := s.GetCachedOriginalLanguageEvidence("tvdb:movie:4", "movie", at); got == nil {
		t.Fatal("raw cache shared mutable identity")
	}
}
