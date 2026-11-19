package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"novastream/models"
)

type similarSnapshotFixture struct {
	service        *Service
	cache          *fileCache
	kind           string
	calls          atomic.Int32
	mode           atomic.Value
	results        atomic.Value
	closeResults   func()
	resultStatus   atomic.Int32
	seedWithGenres bool
}

type similarSnapshotClosingBody struct {
	io.ReadCloser
	after func()
}

func (b similarSnapshotClosingBody) Close() error {
	err := b.ReadCloser.Close()
	b.after()
	return err
}

func similarSnapshotPayload(n int) string {
	results := []map[string]any{}
	for i := 1; i <= n; i++ {
		results = append(results, map[string]any{"id": i, "title": fmt.Sprintf("Result %04d", i), "name": fmt.Sprintf("Result %04d", i), "original_language": "en", "overview": "PRIVATE_UNPROJECTED", "poster_path": "/synthetic.jpg", "adult": true})
	}
	raw, _ := json.Marshal(map[string]any{"results": results})
	return string(raw)
}

func newSimilarSnapshotFixture(t *testing.T, kind string, n int, genres bool) *similarSnapshotFixture {
	t.Helper()
	// Install the global deny transport BEFORE constructing provider clients.
	old := http.DefaultTransport
	var denied atomic.Int32
	http.DefaultTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		denied.Add(1)
		return nil, errors.New("external HTTP denied by synthetic similar fixture")
	})
	t.Cleanup(func() {
		http.DefaultTransport = old
		if denied.Load() != 0 {
			t.Errorf("unexpected default-transport demand: %d", denied.Load())
		}
	})
	f := &similarSnapshotFixture{kind: kind, seedWithGenres: genres}
	f.mode.Store("normal")
	f.results.Store(similarSnapshotPayload(n))
	f.resultStatus.Store(200)
	apiKind := "movie"
	if kind == "series" {
		apiKind = "tv"
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/3/" + apiKind + "/77":
			if f.mode.Load().(string) == "seed-failure" {
				w.WriteHeader(400)
				io.WriteString(w, `{"error":"synthetic seed failure"}`)
			} else if f.seedWithGenres {
				io.WriteString(w, `{"genres":[{"id":1}],"keywords":{"keywords":[]},"release_date":"2000-01-01"}`)
			} else {
				io.WriteString(w, `{"genres":[],"keywords":{"keywords":[]}}`)
			}
		case "/3/discover/" + apiKind, "/3/" + apiKind + "/77/similar":
			w.WriteHeader(int(f.resultStatus.Load()))
			io.WriteString(w, f.results.Load().(string))
		default:
			t.Errorf("unexpected local provider path %q", r.URL.Path)
			w.WriteHeader(400)
		}
	}))
	t.Cleanup(server.Close)
	target, _ := url.Parse(server.URL)
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != server.Listener.Addr().String() {
			return nil, errors.New("nonlocal dial denied")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.themoviedb.org" {
			return nil, errors.New("unexpected provider origin")
		}
		copy := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = target.Scheme, target.Host
		copy.URL = &u
		response, err := transport.RoundTrip(copy)
		if err == nil && f.closeResults != nil && (strings.Contains(r.URL.Path, "/discover/") || strings.HasSuffix(r.URL.Path, "/similar")) {
			response.Body = similarSnapshotClosingBody{response.Body, f.closeResults}
		}
		return response, err
	})}
	f.cache = newFileCache(t.TempDir(), 48)
	f.service = &Service{client: newTVDBClient("", "eng", &http.Client{}, 48), tmdb: newTMDBClient("synthetic-similar-key", "eng", client, f.cache), cache: f.cache}
	return f
}

func (f *similarSnapshotFixture) path() string {
	return filepath.Join(f.cache.dir, similarSnapshotKey(f.kind, 77, f.service.CachedSimilarMetadataLanguage())+".json")
}

func (f *similarSnapshotFixture) removeLegacy(t *testing.T) {
	t.Helper()
	key := cacheKey("discover", "similar", "v4", f.kind, "77", strings.TrimSpace(f.service.client.language))
	if err := os.Remove(filepath.Join(f.cache.dir, key+".json")); err != nil {
		t.Fatal(err)
	}
}

func TestSimilarSnapshotActualNormalProducerAndCacheHitRemainIndependent(t *testing.T) {
	for _, kind := range []string{"movie", "series"} {
		t.Run(kind, func(t *testing.T) {
			f := newSimilarSnapshotFixture(t, kind, 8, true)
			if _, err := f.service.GetCachedSimilarSnapshot(kind, 77, time.Now()); err != ErrSimilarSnapshotUnavailable || f.calls.Load() != 0 {
				t.Fatal("missing snapshot caused provider demand", err)
			}
			legacy, err := f.service.Similar(t.Context(), kind, 77)
			if err != nil || len(legacy) != 8 || f.calls.Load() != 2 {
				t.Fatal("normal real HTTP producer changed", err, f.calls.Load())
			}
			snapshot, err := f.service.GetCachedSimilarSnapshot(kind, 77, time.Now())
			if err != nil || len(snapshot.Candidates) != 8 || snapshot.ExpiresAt.Sub(snapshot.ObservedAt) != 24*time.Hour || snapshot.MetadataLanguage != "en-US" {
				t.Fatal("finite normal observation unavailable", err)
			}
			projection := snapshot.Titles()
			if projection[0].ID != legacy[0].ID || projection[0].TMDBID != legacy[0].TMDBID || projection[0].Name != legacy[0].Name || projection[0].MediaType != kind || projection[0].Language != "" || projection[0].Poster != nil || projection[0].Overview != "" || projection[0].OriginalLanguageEvidence != nil || projection[0].Adult {
				t.Fatal("snapshot projected unrelated metadata")
			}
			before, _ := os.ReadFile(f.path())
			stat, _ := os.Stat(f.path())
			if stat.Mode().Perm() != 0600 || strings.Contains(string(before), "PRIVATE_") || strings.Contains(string(before), "poster") || strings.Contains(string(before), "original_language") {
				t.Fatal("snapshot retained private fields or unsafe file mode")
			}
			cached, err := f.service.Similar(t.Context(), kind, 77)
			left, _ := json.Marshal(legacy)
			right, _ := json.Marshal(cached)
			if err != nil || !bytes.Equal(left, right) || f.calls.Load() != 2 {
				t.Fatal("ordinary v4 cache result/provider behavior changed", err)
			}
			snapshot.Candidates[0].Name = "caller mutation"
			projection[0].Name = "projection mutation"
			again, err := f.service.GetCachedSimilarSnapshot(kind, 77, time.Now())
			after, _ := os.ReadFile(f.path())
			afterStat, _ := os.Stat(f.path())
			if err != nil || again.Candidates[0].Name != legacy[0].Name || !bytes.Equal(before, after) || !stat.ModTime().Equal(afterStat.ModTime()) || f.calls.Load() != 2 {
				t.Fatal("cached getter mutated data or renewed observation", err)
			}
			if err := os.Remove(f.path()); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.Similar(t.Context(), kind, 77); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(f.path()); !os.IsNotExist(err) {
				t.Fatal("legacy cache hit promoted a new observation")
			}
		})
	}
}

func TestSimilarSnapshotFinalFallbackEmptyAndSwallowedSeedFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		mode string
	}{{"valid empty", 0, "normal"}, {"finite result larger than twenty", 702, "normal"}, {"seed failure then successful fallback", 8, "seed-failure"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSimilarSnapshotFixture(t, "movie", tc.n, false)
			f.mode.Store(tc.mode)
			legacy, err := f.service.Similar(t.Context(), "movie", 77)
			snapshot, readErr := f.service.GetCachedSimilarSnapshot("movie", 77, time.Now())
			if err != nil || readErr != nil || len(legacy) != tc.n || snapshot.Candidates == nil || len(snapshot.Candidates) != tc.n || f.calls.Load() != 2 {
				t.Fatal("final normal fallback set changed", err, readErr)
			}
		})
	}
}

func TestSimilarSnapshotActualNormalProducerPreservesBlankStoredLabels(t *testing.T) {
	for _, label := range []string{"", "   "} {
		t.Run(fmt.Sprintf("bytes-%d", len(label)), func(t *testing.T) {
			f := newSimilarSnapshotFixture(t, "movie", 8, false)
			f.results.Store(strings.ReplaceAll(similarSnapshotPayload(8), "Result 0001", label))
			legacy, err := f.service.Similar(t.Context(), "movie", 77)
			if err != nil || len(legacy) != 8 || legacy[0].Name != label {
				t.Fatal("normal blank stored label changed", err)
			}
			snapshot, err := f.service.GetCachedSimilarSnapshot("movie", 77, time.Now())
			if err != nil || len(snapshot.Candidates) != 8 || snapshot.Candidates[0].Name != label || snapshot.Titles()[0].Name != label || f.calls.Load() != 2 {
				t.Fatal("valid primary-bound blank label lost its finite observation", err)
			}
		})
	}
}

func TestSimilarSnapshotWriterBoundsAndIndependentProjection(t *testing.T) {
	f := newSimilarSnapshotFixture(t, "movie", 0, false)
	for _, size := range []int{0, similarSnapshotMaxCandidates} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			titles := make([]models.Title, 0, size)
			for i := 1; i <= size; i++ {
				titles = append(titles, models.Title{ID: "tmdb:movie:" + strconv.Itoa(i), TMDBID: int64(i), MediaType: "movie", Name: "Boundary", Language: "fr", Adult: true})
			}
			f.service.recordSimilarSnapshot(t.Context(), "movie", 77, titles, time.Now())
			snapshot, err := f.service.GetCachedSimilarSnapshot("movie", 77, time.Now())
			if err != nil || snapshot.Candidates == nil || len(snapshot.Candidates) != size {
				t.Fatal("declared finite observation bound changed", err)
			}
			if size > 0 {
				projected := snapshot.Titles()
				if projected[0].Adult || projected[0].Language != "" {
					t.Fatal("scalar age/language claim was projected")
				}
				projected[0].Name = "mutated projection"
				if snapshot.Titles()[0].Name != "Boundary" {
					t.Fatal("projection shares mutable storage")
				}
			}
		})
	}
	before, _ := os.ReadFile(f.path())
	for _, name := range []string{"zero ttl", "cancelled", "wrong kind", "scalar mismatch", "invalid utf8", "oversized encoding"} {
		t.Run(name, func(t *testing.T) {
			title := models.Title{ID: "tmdb:movie:1", TMDBID: 1, MediaType: "movie", Name: "Boundary"}
			titles := []models.Title{title}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			previousTTL := f.cache.ttl
			defer func() { f.cache.ttl = previousTTL }()
			switch name {
			case "zero ttl":
				f.cache.ttl = 0
			case "cancelled":
				cancel()
			case "wrong kind":
				titles[0].MediaType = "series"
			case "scalar mismatch":
				titles[0].TMDBID = 2
			case "invalid utf8":
				titles[0].Name = string([]byte{'B', 0xff, 'd'})
			case "oversized encoding":
				titles = make([]models.Title, 0, similarSnapshotMaxCandidates)
				for i := 1; i <= similarSnapshotMaxCandidates; i++ {
					titles = append(titles, models.Title{ID: "tmdb:movie:" + strconv.Itoa(i), TMDBID: int64(i), MediaType: "movie", Name: strings.Repeat("x", 1024)})
				}
			}
			f.service.recordSimilarSnapshot(ctx, "movie", 77, titles, time.Now())
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(before, after) || f.calls.Load() != 0 {
				t.Fatal("invalid record renewed bytes or caused provider demand")
			}
		})
	}
}

func TestSimilarSnapshotFailedCancelledOrInvalidNormalResultDoesNotOverwrite(t *testing.T) {
	for _, name := range []string{"final failure", "cancelled after decoded result", "invalid primary", "duplicate primary", "too many candidates"} {
		t.Run(name, func(t *testing.T) {
			f := newSimilarSnapshotFixture(t, "movie", 8, false)
			if _, err := f.service.Similar(t.Context(), "movie", 77); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(f.path())
			f.removeLegacy(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch name {
			case "final failure":
				f.resultStatus.Store(400)
			case "cancelled after decoded result":
				f.closeResults = cancel
			case "invalid primary":
				f.results.Store(strings.Replace(similarSnapshotPayload(8), `"id":1`, `"id":0`, 1))
			case "duplicate primary":
				f.results.Store(strings.Replace(similarSnapshotPayload(8), `"id":2`, `"id":1`, 1))
			case "too many candidates":
				f.results.Store(similarSnapshotPayload(similarSnapshotMaxCandidates + 1))
			}
			legacy, err := f.service.Similar(ctx, "movie", 77)
			if name == "final failure" && err == nil || name != "final failure" && (err != nil || legacy == nil) {
				t.Fatal("normal return behavior changed", err)
			}
			if name == "cancelled after decoded result" && ctx.Err() != context.Canceled {
				t.Fatal("actual result-close cancellation hook did not run")
			}
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(before, after) {
				t.Fatal("failed/cancelled/invalid generation overwrote or renewed prior observation")
			}
		})
	}
}

func TestSimilarSnapshotGetterRejectsUntrustedCompleteJSON(t *testing.T) {
	f := newSimilarSnapshotFixture(t, "movie", 8, false)
	if _, err := f.service.Similar(t.Context(), "movie", 77); err != nil {
		t.Fatal(err)
	}
	original, err := f.service.GetCachedSimilarSnapshot("movie", 77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.service.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("getter attempted provider access")
		return nil, errors.New("provider read trap")
	})}
	for _, name := range []string{"version", "provider", "kind", "seed", "locale", "future", "expired", "extended ttl", "null candidates", "duplicate identity", "wrong scalar", "wrong primary kind", "opaque primary", "null name", "control name", "invalid utf8", "unknown field", "duplicate envelope key", "duplicate candidate key", "trailing object", "trailing garbage", "missing field", "too many candidates", "too many bytes", "truncated"} {
		t.Run(name, func(t *testing.T) {
			value := *original
			value.Candidates = append([]CachedSimilarCandidate{}, original.Candidates...)
			switch name {
			case "version":
				value.Version++
			case "provider":
				value.Provider = "foreign"
			case "kind":
				value.MediaType = "series"
			case "seed":
				value.TMDBID++
			case "locale":
				value.MetadataLanguage = "fr-US"
			case "future":
				value.ObservedAt = time.Now().Add(time.Hour)
				value.ExpiresAt = value.ObservedAt.Add(time.Hour)
			case "expired":
				value.ObservedAt = time.Now().Add(-2 * time.Hour)
				value.ExpiresAt = time.Now().Add(-time.Hour)
			case "extended ttl":
				value.ExpiresAt = value.ObservedAt.Add(25 * time.Hour)
			case "null candidates":
				value.Candidates = nil
			case "duplicate identity":
				value.Candidates = append(value.Candidates, value.Candidates[0])
			case "wrong scalar":
				value.Candidates[0].TMDBID++
			case "wrong primary kind":
				value.Candidates[0].ID = "tmdb:tv:1"
			case "opaque primary":
				value.Candidates[0].ID = "opaque:1"
			case "control name":
				value.Candidates[0].Name = "Unsafe\nName"
			case "too many candidates":
				value.Candidates = []CachedSimilarCandidate{}
				for i := 1; i <= similarSnapshotMaxCandidates+1; i++ {
					value.Candidates = append(value.Candidates, CachedSimilarCandidate{ID: "tmdb:movie:" + strconv.Itoa(i), TMDBID: int64(i), Name: "Bounded"})
				}
			}
			raw, _ := json.Marshal(value)
			switch name {
			case "unknown field":
				raw = append([]byte(`{"unexpected":true,`), raw[1:]...)
			case "duplicate envelope key":
				raw = append([]byte(`{"version":1,`), raw[1:]...)
			case "duplicate candidate key":
				raw = bytes.Replace(raw, []byte(`"id":"tmdb:movie:1"`), []byte(`"id":"tmdb:movie:9","id":"tmdb:movie:1"`), 1)
			case "trailing object":
				raw = append(raw, []byte(` {}`)...)
			case "trailing garbage":
				raw = append(raw, []byte(` invalid`)...)
			case "missing field":
				raw = bytes.Replace(raw, []byte(`"name":"Result 0001",`), nil, 1)
				// Name is the final candidate field, so remove its leading comma.
				raw = bytes.Replace(raw, []byte(`,"name":"Result 0001"`), nil, 1)
			case "null name":
				raw = bytes.Replace(raw, []byte(`"name":"Result 0001"`), []byte(`"name":null`), 1)
			case "too many bytes":
				raw = append(bytes.Repeat([]byte(" "), similarSnapshotMaxBytes), raw...)
			case "truncated":
				raw = raw[:len(raw)-2]
			case "invalid utf8":
				raw = bytes.Replace(raw, []byte("Result 0001"), []byte{'B', 0xff, 'd'}, 1)
			}
			if err := os.WriteFile(f.path(), raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.GetCachedSimilarSnapshot("movie", 77, time.Now()); err != ErrSimilarSnapshotUnavailable {
				t.Fatal("invalid complete JSON accepted", err)
			}
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(raw, after) {
				t.Fatal("invalid read rewrote or removed source bytes")
			}
		})
	}
}

func TestSimilarSnapshotTTLReadSafetyAndNilInputs(t *testing.T) {
	f := newSimilarSnapshotFixture(t, "movie", 8, false)
	f.cache.ttl = time.Hour
	if _, err := f.service.Similar(t.Context(), "movie", 77); err != nil {
		t.Fatal(err)
	}
	value, err := f.service.GetCachedSimilarSnapshot("movie", 77, time.Now())
	if err != nil || value.ExpiresAt.Sub(value.ObservedAt) != time.Hour {
		t.Fatal("existing positive shorter TTL was ignored", err)
	}
	raw, _ := os.ReadFile(f.path())
	for _, name := range []string{"touched expired", "copied expired", "symlink", "directory", "fifo", "reduced base ttl", "disabled base ttl", "missing parent"} {
		t.Run(name, func(t *testing.T) {
			cache := newFileCache(t.TempDir(), 1)
			service := &Service{cache: cache, tmdb: f.service.tmdb}
			path := filepath.Join(cache.dir, similarSnapshotKey("movie", 77, value.MetadataLanguage)+".json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			at := time.Now()
			switch name {
			case "touched expired", "copied expired":
				at = value.ExpiresAt
				if name == "touched expired" {
					if err := os.Chtimes(path, at.Add(time.Hour), at.Add(time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
			case "symlink", "directory", "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				var err error
				if name == "symlink" {
					err = os.Symlink(f.path(), path)
				} else if name == "directory" {
					err = os.Mkdir(path, 0700)
				} else {
					err = syscall.Mkfifo(path, 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "reduced base ttl":
				cache.ttl = 30 * time.Minute
			case "disabled base ttl":
				cache.ttl = 0
			case "missing parent":
				cache.dir = filepath.Join(cache.dir, "not-created")
			}
			if _, err := service.GetCachedSimilarSnapshot("movie", 77, at); err != ErrSimilarSnapshotUnavailable {
				t.Fatal("unsafe file/expiry accepted", err)
			}
			if name == "missing parent" {
				if _, err := os.Stat(cache.dir); !os.IsNotExist(err) {
					t.Fatal("getter created cache directories")
				}
			}
		})
	}
	for _, service := range []*Service{nil, {}, {cache: f.cache}, {tmdb: f.service.tmdb}} {
		if _, err := service.GetCachedSimilarSnapshot("movie", 77, time.Now()); err != ErrSimilarSnapshotUnavailable {
			t.Fatal("unavailable service accepted", err)
		}
	}
	var absent *Service
	if absent.CachedSimilarMetadataLanguage() != "" || (*CachedSimilarSnapshot)(nil).Titles() != nil {
		t.Fatal("nil-safe projection/locale contract changed")
	}
	localized := f.service.WithLanguage("fra")
	if localized.CachedSimilarMetadataLanguage() != "fr-US" {
		t.Fatal("effective locale no longer uses the existing provider normalizer")
	}
	if _, err := localized.GetCachedSimilarSnapshot("movie", 77, time.Now()); err != ErrSimilarSnapshotUnavailable {
		t.Fatal("different locale borrowed an existing observation")
	}
	for _, kind := range []string{"", "tv", "MOVIE", " movie", "episode"} {
		if _, err := f.service.GetCachedSimilarSnapshot(kind, 77, time.Now()); err != ErrSimilarSnapshotUnavailable {
			t.Fatal("noncanonical media selector accepted")
		}
	}
	if _, err := f.service.GetCachedSimilarSnapshot("movie", 0, time.Now()); err != ErrSimilarSnapshotUnavailable {
		t.Fatal("invalid seed accepted")
	}
	if _, err := f.service.GetCachedSimilarSnapshot("movie", 77, time.Time{}); err != ErrSimilarSnapshotUnavailable {
		t.Fatal("missing observation time accepted")
	}
	if !reflect.DeepEqual(value.Titles()[0], models.Title{ID: "tmdb:movie:1", Name: "Result 0001", TMDBID: 1, MediaType: "movie"}) {
		t.Fatal("allowlist projection expanded")
	}
}
