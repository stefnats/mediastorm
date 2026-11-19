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

type personCreditsSnapshotFixture struct {
	service    *Service
	cache      *fileCache
	calls      atomic.Int32
	body       atomic.Value
	status     atomic.Int32
	closeBody  func()
	failDetail atomic.Bool
}

func personCreditsSnapshotPayload(n int) string {
	items := []map[string]any{}
	for id := 1; id <= n; id++ {
		kind, title, name := "movie", fmt.Sprintf("Movie %04d", id), "SECONDARY_MOVIE_LABEL"
		if id%2 == 0 {
			kind, title, name = "tv", "SECONDARY_SERIES_LABEL", fmt.Sprintf("Series %04d", id)
		}
		items = append(items, map[string]any{"id": id, "media_type": kind, "title": title, "name": name, "original_language": "fr", "overview": "PRIVATE_UNUSED", "poster_path": "/synthetic.jpg", "adult": true})
	}
	raw, _ := json.Marshal(map[string]any{"cast": items, "crew": []map[string]any{{"id": 999999, "media_type": "movie", "title": "CREW_ONLY_NOT_PROJECTED", "job": "Director"}}, "owner": "PRIVATE_OWNER_NOT_PROJECTED"})
	return string(raw)
}

func newPersonCreditsSnapshotFixture(t *testing.T, body string) *personCreditsSnapshotFixture {
	t.Helper()
	old := http.DefaultTransport
	var denied atomic.Int32
	http.DefaultTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		denied.Add(1)
		return nil, errors.New("external HTTP denied by synthetic person-credits fixture")
	})
	t.Cleanup(func() {
		http.DefaultTransport = old
		if denied.Load() != 0 {
			t.Errorf("unexpected default transport demand: %d", denied.Load())
		}
	})
	f := &personCreditsSnapshotFixture{}
	f.body.Store(body)
	f.status.Store(200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.Header.Get("Authorization") != "" || r.URL.Query().Get("session_id") != "" || r.URL.Query().Get("api_key") != "synthetic-global-key" {
			t.Error("normal global API-key-only cast operation changed")
			w.WriteHeader(400)
			return
		}
		if r.URL.Path == "/3/movie/1" && f.failDetail.Load() {
			w.WriteHeader(400)
			io.WriteString(w, `{"error":"synthetic later filter error"}`)
			return
		}
		if r.URL.Path != "/3/person/77/combined_credits" {
			t.Error("unexpected synthetic provider path")
			w.WriteHeader(400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(int(f.status.Load()))
		io.WriteString(w, f.body.Load().(string))
	}))
	t.Cleanup(server.Close)
	target, _ := url.Parse(server.URL)
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != server.Listener.Addr().String() {
			return nil, errors.New("nonlocal fixture dial denied")
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
		if err == nil && f.closeBody != nil {
			response.Body = similarSnapshotClosingBody{response.Body, f.closeBody}
		}
		return response, err
	})}
	f.cache = newFileCache(t.TempDir(), 48)
	f.service = &Service{client: newTVDBClient("", "eng", client, 48), tmdb: newTMDBClient("synthetic-global-key", "eng", client, f.cache), cache: f.cache, idCache: newFileCache(t.TempDir(), 48), inflightRequests: make(map[string]*inflightRequest)}
	return f
}

func (f *personCreditsSnapshotFixture) path() string {
	return filepath.Join(f.cache.dir, personCreditsSnapshotKey(77, f.service.CachedTMDBPersonCreditsMetadataLanguage())+".json")
}

func TestTMDBPersonCreditsSnapshotActualProducerBeforeLegacyFiveHundredWindow(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, personCreditsSnapshotPayload(704))
	if _, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now()); err != ErrTMDBPersonCreditsSnapshotUnavailable || f.calls.Load() != 0 {
		t.Fatal("missing observation caused provider demand", err)
	}
	opts := TMDBListOptions{SourceType: TMDBSourcePersonCredits, SourceID: "77", Limit: 1000, DeferArtwork: true}
	legacy, total, err := f.service.GetTMDBList(t.Context(), opts)
	if err != nil || len(legacy) != 500 || total != 704 || f.calls.Load() != 1 {
		t.Fatal("normal list mapping/cap/provider count changed", err, len(legacy), total, f.calls.Load())
	}
	snapshot, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || len(snapshot.Candidates) != 704 || snapshot.SourceType != TMDBSourcePersonCredits || snapshot.MetadataLanguage != "en-US" || snapshot.ExpiresAt.Sub(snapshot.ObservedAt) != 24*time.Hour {
		t.Fatal("whole decoded finite observation unavailable", err)
	}
	if snapshot.Candidates[650].ID != "tmdb:movie:651" || snapshot.Candidates[651].ID != "tmdb:tv:652" || snapshot.Candidates[650].Name != "Movie 0651" || snapshot.Candidates[651].Name != "Series 0652" {
		t.Fatal("typed items beyond the old local window were changed")
	}
	for i := range legacy {
		if legacy[i].Title.ID != snapshot.Candidates[i].ID || legacy[i].Title.Name != snapshot.Candidates[i].Name || legacy[i].Rank != i+1 {
			t.Fatal("normal first page/order was altered")
		}
	}
	before, _ := os.ReadFile(f.path())
	stat, _ := os.Stat(f.path())
	if stat.Mode().Perm() != 0600 || strings.Contains(string(before), "PRIVATE_") || strings.Contains(string(before), "original_language") || strings.Contains(string(before), "SECONDARY_") || strings.Contains(string(before), "CREW_ONLY_") || strings.Contains(string(before), `"public":`) {
		t.Fatal("observation projected unrelated fields or claimed provider visibility")
	}
	cached, cachedTotal, err := f.service.GetTMDBList(t.Context(), opts)
	left, _ := json.Marshal(legacy)
	right, _ := json.Marshal(cached)
	after, _ := os.ReadFile(f.path())
	afterStat, _ := os.Stat(f.path())
	if err != nil || cachedTotal != total || !bytes.Equal(left, right) || !bytes.Equal(before, after) || !stat.ModTime().Equal(afterStat.ModTime()) || f.calls.Load() != 1 {
		t.Fatal("legacy cache hit renewed observation or changed ordinary result", err)
	}
	projection := snapshot.Titles()
	if projection[0].Language != "" || projection[0].OriginalLanguageEvidence != nil || projection[0].Poster != nil || projection[0].Adult || projection[0].Overview != "" || projection[0].Year != 0 {
		t.Fatal("snapshot projection invented scalar metadata claims")
	}
	projection[0].Name, snapshot.Candidates[0].Name = "caller mutation", "snapshot mutation"
	again, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || again.Candidates[0].Name != "Movie 0001" {
		t.Fatal("snapshot/read projection shares caller storage", err)
	}
	if err := os.Remove(f.path()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.service.GetTMDBList(t.Context(), opts); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.path()); !os.IsNotExist(err) {
		t.Fatal("bare v5 cache hit promoted a missing observation")
	}
}

func TestTMDBPersonCreditsSnapshotMixedSameIDAndExactPrimaryLabels(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, `{"cast":[{"id":42,"media_type":"movie","title":"","name":"Secondary movie fallback"},{"id":42,"media_type":"tv","name":"   ","title":"Secondary series fallback"}]}`)
	legacy, _, err := f.service.GetTMDBList(t.Context(), TMDBListOptions{SourceType: TMDBSourcePersonCredits, SourceID: "77", DeferArtwork: true})
	if err != nil || len(legacy) != 1 || legacy[0].Title.Name != "Secondary movie fallback" {
		t.Fatal("legacy fallback/drop-blank mapping changed", err)
	}
	snapshot, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || len(snapshot.Candidates) != 2 {
		t.Fatal("mixed primary identities with same number were lost", err)
	}
	want := []CachedTMDBPersonCreditsCandidate{{ID: "tmdb:movie:42", MediaType: "movie", TMDBID: 42, Name: ""}, {ID: "tmdb:tv:42", MediaType: "series", TMDBID: 42, Name: "   "}}
	if !reflect.DeepEqual(snapshot.Candidates, want) || snapshot.Titles()[0].Name != "" || snapshot.Titles()[1].Name != "   " {
		t.Fatal("primary provider labels were trimmed, guessed or replaced")
	}
}

func TestTMDBPersonCreditsSnapshotSuccessfulOperationSurvivesLaterLocalFilterError(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, personCreditsSnapshotPayload(1))
	f.failDetail.Store(true)
	_, _, err := f.service.GetTMDBList(t.Context(), TMDBListOptions{SourceType: TMDBSourcePersonCredits, SourceID: "77", DiscoverQuery: "companies=5", DeferArtwork: true})
	if err == nil {
		t.Fatal("ordinary later filter error disappeared")
	}
	// The observed operation already succeeded. This is not a claim that all
	// later filtering, mapping or enrichment for GetTMDBList also succeeded.
	snapshot, readErr := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if readErr != nil || len(snapshot.Candidates) != 1 || f.calls.Load() != 2 {
		t.Fatal("successful raw operation was incorrectly tied to later filter success", readErr)
	}
}

func TestTMDBPersonCreditsSnapshotExplicitEmptyAndInvalidProducerDoesNotRenew(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		cancel     bool
	}{
		{"missing cast", `{}`, 200, false}, {"null cast", `{"cast":null}`, 200, false},
		{"missing explicit kind", `{"cast":[{"id":1,"title":"Normal mapper can infer this"}]}`, 200, false},
		{"noncanonical kind", `{"cast":[{"id":1,"media_type":"series","name":"Normal mapper can infer this"}]}`, 200, false},
		{"invalid id", `{"cast":[{"id":0,"media_type":"movie","title":"Invalid"}]}`, 200, false},
		{"invalid duplicate label", `{"cast":[{"id":1,"media_type":"movie","title":"First"},{"id":1,"media_type":"movie","title":"Bad\nLabel"}]}`, 200, false},
		{"control label", `{"cast":[{"id":1,"media_type":"movie","title":"Unsafe\nLabel"}]}`, 200, false},
		{"provider error", `{"error":"synthetic failure"}`, 400, false},
		{"cancelled after decode", personCreditsSnapshotPayload(2), 200, true},
		{"too many candidates", personCreditsSnapshotPayload(4097), 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPersonCreditsSnapshotFixture(t, `{"cast":[]}`)
			if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePersonCredits}, 77); err != nil {
				t.Fatal(err)
			}
			empty, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
			if err != nil || empty.Candidates == nil || len(empty.Candidates) != 0 {
				t.Fatal("explicit empty observation lost", err)
			}
			before, _ := os.ReadFile(f.path())
			f.body.Store(tc.body)
			f.status.Store(int32(tc.status))
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.cancel {
				f.closeBody = cancel
			}
			_, err = f.service.tmdb.staticShelfTitles(ctx, TMDBListOptions{SourceType: TMDBSourcePersonCredits}, 77)
			if (err != nil) != (tc.status >= 400) {
				t.Fatal("ordinary decoder/error behavior changed", err)
			}
			if tc.cancel && ctx.Err() != context.Canceled {
				t.Fatal("decoded-response cancellation did not execute")
			}
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(before, after) || f.calls.Load() != 2 {
				t.Fatal("invalid/cancelled/failed response renewed or fetched extra")
			}
		})
	}
}

func TestTMDBPersonCreditsSnapshotStrictStoredObservation(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, personCreditsSnapshotPayload(2))
	if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePersonCredits}, 77); err != nil {
		t.Fatal(err)
	}
	original, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.service.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("getter attempted provider work")
		return nil, errors.New("provider trap")
	})}
	for _, name := range []string{"version", "provider", "source type", "person id", "locale", "future", "expired", "extended ttl", "nil candidates", "wrong scalar", "wrong kind", "short identity", "duplicate same kind alias", "unknown field", "missing name", "null name", "duplicate top key", "duplicate candidate key", "trailing object", "trailing garbage", "invalid utf8", "too many candidates", "too many bytes"} {
		t.Run(name, func(t *testing.T) {
			value := *original
			value.Candidates = append([]CachedTMDBPersonCreditsCandidate{}, original.Candidates...)
			switch name {
			case "version":
				value.Version++
			case "provider":
				value.Provider = "other"
			case "source type":
				value.SourceType = "private-account-list"
			case "person id":
				value.PersonID++
			case "locale":
				value.MetadataLanguage = "fr-US"
			case "future":
				value.ObservedAt, value.ExpiresAt = time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
			case "expired":
				value.ObservedAt, value.ExpiresAt = time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
			case "extended ttl":
				value.ExpiresAt = value.ObservedAt.Add(25 * time.Hour)
			case "nil candidates":
				value.Candidates = nil
			case "wrong scalar":
				value.Candidates[0].TMDBID++
			case "wrong kind":
				value.Candidates[0].MediaType = "series"
			case "short identity":
				value.Candidates[0].ID = "tmdb:1"
			case "duplicate same kind alias":
				duplicate := value.Candidates[1]
				duplicate.ID = "tmdb:series:2"
				value.Candidates = append(value.Candidates, duplicate)
			case "too many candidates":
				value.Candidates = []CachedTMDBPersonCreditsCandidate{}
				for i := 1; i <= 4097; i++ {
					value.Candidates = append(value.Candidates, CachedTMDBPersonCreditsCandidate{ID: "tmdb:movie:" + strconv.Itoa(i), MediaType: "movie", TMDBID: int64(i), Name: "Bounded"})
				}
			}
			raw, _ := json.Marshal(value)
			switch name {
			case "unknown field":
				raw = append([]byte(`{"public":true,`), raw[1:]...)
			case "missing name":
				raw = bytes.Replace(raw, []byte(`,"name":"Movie 0001"`), nil, 1)
			case "null name":
				raw = bytes.Replace(raw, []byte(`"name":"Movie 0001"`), []byte(`"name":null`), 1)
			case "duplicate top key":
				raw = append([]byte(`{"version":1,`), raw[1:]...)
			case "duplicate candidate key":
				raw = bytes.Replace(raw, []byte(`"id":"tmdb:movie:1"`), []byte(`"id":"other","id":"tmdb:movie:1"`), 1)
			case "trailing object":
				raw = append(raw, []byte(` {}`)...)
			case "trailing garbage":
				raw = append(raw, []byte(` invalid`)...)
			case "invalid utf8":
				raw = bytes.Replace(raw, []byte("Movie 0001"), []byte{0xff}, 1)
			case "too many bytes":
				raw = append(bytes.Repeat([]byte(" "), personCreditsSnapshotMaxBytes), raw...)
			}
			if err := os.WriteFile(f.path(), raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now()); err != ErrTMDBPersonCreditsSnapshotUnavailable {
				t.Fatal("untrusted snapshot accepted", err)
			}
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(raw, after) || f.calls.Load() != 1 {
				t.Fatal("getter wrote invalid bytes or caused provider demand")
			}
		})
	}
}

func TestTMDBPersonCreditsSnapshotExpiryNoFollowAndReadOnlyMisses(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, personCreditsSnapshotPayload(2))
	f.cache.ttl = time.Hour
	if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePersonCredits}, 77); err != nil {
		t.Fatal(err)
	}
	value, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || value.ExpiresAt.Sub(value.ObservedAt) != time.Hour {
		t.Fatal("positive base TTL was not honored", err)
	}
	raw, _ := os.ReadFile(f.path())
	stat, _ := os.Stat(f.path())
	if stat.Mode().Perm() != 0600 {
		t.Fatal("unsafe snapshot file mode")
	}
	for _, name := range []string{"copied expired", "touched expired", "reduced ttl", "disabled ttl", "symlink", "directory", "fifo", "missing directory"} {
		t.Run(name, func(t *testing.T) {
			cache := newFileCache(t.TempDir(), 1)
			service := &Service{cache: cache, tmdb: f.service.tmdb}
			path := filepath.Join(cache.dir, personCreditsSnapshotKey(77, value.MetadataLanguage)+".json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			at := time.Now()
			switch name {
			case "copied expired", "touched expired":
				at = value.ExpiresAt
				if name == "touched expired" {
					if err := os.Chtimes(path, at.Add(time.Hour), at.Add(time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
			case "reduced ttl":
				cache.ttl = time.Minute
			case "disabled ttl":
				cache.ttl = 0
			case "missing directory":
				cache.dir = filepath.Join(cache.dir, "not-created")
			default:
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
			}
			if _, err := service.GetCachedTMDBPersonCreditsSnapshot(77, at); err != ErrTMDBPersonCreditsSnapshotUnavailable {
				t.Fatal("unsafe/expired observation accepted", err)
			}
			if name == "missing directory" {
				if _, err := os.Stat(cache.dir); !os.IsNotExist(err) {
					t.Fatal("getter created missing cache directories")
				}
			}
		})
	}
	for _, service := range []*Service{nil, {}, {cache: f.cache}, {tmdb: f.service.tmdb}} {
		if _, err := service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now()); err != ErrTMDBPersonCreditsSnapshotUnavailable {
			t.Fatal("missing reader accepted")
		}
	}
	var absent *Service
	if absent.CachedTMDBPersonCreditsMetadataLanguage() != "" || (*CachedTMDBPersonCreditsSnapshot)(nil).Titles() != nil {
		t.Fatal("nil-safe API changed")
	}
	if _, err := f.service.GetCachedTMDBPersonCreditsSnapshot(0, time.Now()); err != ErrTMDBPersonCreditsSnapshotUnavailable {
		t.Fatal("invalid list ID accepted")
	}
	if _, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Time{}); err != ErrTMDBPersonCreditsSnapshotUnavailable {
		t.Fatal("zero observation time accepted")
	}
	localized := f.service.WithLanguage("fra")
	if localized.CachedTMDBPersonCreditsMetadataLanguage() != "fr-US" {
		t.Fatal("canonical effective locale changed")
	}
	if _, err := localized.GetCachedTMDBPersonCreditsSnapshot(77, time.Now()); err != ErrTMDBPersonCreditsSnapshotUnavailable {
		t.Fatal("different locale borrowed a snapshot")
	}
}

func TestTMDBPersonCreditsSnapshotWriterBoundsAndNoScalarPromotion(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, `{"cast":[]}`)
	items := make([]tmdbShelfTitle, 0, 4096)
	for id := 1; id <= 4096; id++ {
		items = append(items, tmdbShelfTitle{ID: int64(id), MediaType: "movie", Title: "Boundary", Adult: true, OriginalLanguage: "fr"})
	}
	f.service.tmdb.recordTMDBPersonCreditsSnapshot(t.Context(), 77, items, time.Now())
	snapshot, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || len(snapshot.Candidates) != 4096 {
		t.Fatal("declared finite bound changed", err)
	}
	if !reflect.DeepEqual(snapshot.Titles()[0], models.Title{ID: "tmdb:movie:1", MediaType: "movie", TMDBID: 1, Name: "Boundary"}) {
		t.Fatal("projection expanded beyond primary fields")
	}
	before, _ := os.ReadFile(f.path())
	for i := range items {
		items[i].Title = strings.Repeat("x", 1024)
	}
	f.service.tmdb.recordTMDBPersonCreditsSnapshot(t.Context(), 77, items, time.Now())
	after, _ := os.ReadFile(f.path())
	if !bytes.Equal(before, after) || f.calls.Load() != 0 {
		t.Fatal("oversized encoded observation renewed bytes or fetched")
	}
}

func TestTMDBPersonCreditsSnapshotNormalDedupAndSortRemainIndependentFromObservedMembership(t *testing.T) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal([]byte(personCreditsSnapshotPayload(704)), &payload); err != nil {
		t.Fatal(err)
	}
	var cast []map[string]any
	if err := json.Unmarshal(payload["cast"], &cast); err != nil {
		t.Fatal(err)
	}
	cast = append(cast,
		map[string]any{"id": 1, "media_type": "movie", "title": "ZZZ later duplicate label"},
		map[string]any{"id": 650, "media_type": "tv", "name": "AAA later duplicate label"},
	)
	payload["cast"], _ = json.Marshal(cast)
	raw, _ := json.Marshal(payload)
	f := newPersonCreditsSnapshotFixture(t, string(raw))
	opts := TMDBListOptions{SourceType: TMDBSourcePersonCredits, SourceID: "77", Sort: "title.desc", Limit: 1000, DeferArtwork: true}
	legacy, total, err := f.service.GetTMDBList(t.Context(), opts)
	if err != nil || len(legacy) != 500 || total != 706 || f.calls.Load() != 1 {
		t.Fatal("ordinary pre-dedup total, cap or request behavior changed", err, len(legacy), total)
	}
	if legacy[0].Title.ID != "tmdb:movie:1" || legacy[0].Title.Name != "ZZZ later duplicate label" {
		t.Fatal("normal sort-before-dedup label choice changed")
	}
	observation, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || len(observation.Candidates) != 704 || observation.Candidates[0].Name != "Movie 0001" || observation.Candidates[649].Name != "Series 0650" {
		t.Fatal("raw first-appearance title membership was replaced by legacy sorted projection", err)
	}
	seen := map[string]bool{}
	for _, candidate := range observation.Candidates {
		key := candidate.MediaType + ":" + strconv.FormatInt(candidate.TMDBID, 10)
		if seen[key] || candidate.TMDBID == 999999 {
			t.Fatal("duplicate title or crew row entered observed cast membership")
		}
		seen[key] = true
	}
	first, _ := json.Marshal(legacy)
	before, _ := os.ReadFile(f.path())
	cached, cachedTotal, err := f.service.GetTMDBList(t.Context(), opts)
	second, _ := json.Marshal(cached)
	after, _ := os.ReadFile(f.path())
	if err != nil || cachedTotal != total || !bytes.Equal(first, second) || !bytes.Equal(before, after) || f.calls.Load() != 1 {
		t.Fatal("existing paged cache or ordinary output changed", err)
	}
}

func TestTMDBPersonCreditsSnapshotCapturesBeforeMediaFilterAndExcludesCrewMode(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, personCreditsSnapshotPayload(704))
	items, total, err := f.service.GetTMDBList(t.Context(), TMDBListOptions{
		SourceType: TMDBSourcePersonCredits, SourceID: "77", MediaType: "movie", DiscoverQuery: "language=fr", Limit: 1, Offset: 350, DeferArtwork: true,
	})
	if err != nil || len(items) != 1 || total != 352 || items[0].Title.ID != "tmdb:movie:701" || f.calls.Load() != 1 {
		t.Fatal("ordinary media/discover/window result changed", err, total)
	}
	snapshot, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || len(snapshot.Candidates) != 704 || snapshot.Candidates[1].MediaType != "series" {
		t.Fatal("observed cast was narrowed to requested media, filter or window", err)
	}
	before, _ := os.ReadFile(f.path())
	f.body.Store(`{"cast":[{"id":12345,"media_type":"movie","title":"Different cast"}],"crew":[{"id":42,"media_type":"movie","title":"Director title","job":" Director "},{"id":43,"media_type":"movie","title":"Department is not job","department":"Directing"}]}`)
	crew, crewTotal, err := f.service.GetTMDBList(t.Context(), TMDBListOptions{SourceType: TMDBSourceDirectorCredits, SourceID: "77", DeferArtwork: true})
	if err != nil || len(crew) != 1 || crewTotal != 1 || crew[0].Title.ID != "tmdb:movie:42" || f.calls.Load() != 2 {
		t.Fatal("normal director job selection changed", err, crewTotal)
	}
	after, _ := os.ReadFile(f.path())
	if !bytes.Equal(before, after) {
		t.Fatal("director operation overwrote person cast observation")
	}
	if err := os.Remove(f.path()); err != nil {
		t.Fatal(err)
	}
	// A new director-only normal read remains unable to establish cast provenance.
	if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourceDirectorCredits}, 77); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now()); err != ErrTMDBPersonCreditsSnapshotUnavailable || f.calls.Load() != 3 {
		t.Fatal("director operation promoted a missing person observation", err)
	}
}

func TestTMDBPersonCreditsSnapshotDuplicatesCannotRepairPrimaryLabels(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, `{"cast":[{"id":42,"media_type":"movie","title":"","name":"Secondary movie fallback"},{"id":42,"media_type":"tv","name":"   ","title":"Secondary series fallback"},{"id":42,"media_type":"movie","title":"Later usable movie label"},{"id":42,"media_type":"tv","name":"Later usable series label"}],"crew":[{"id":99,"media_type":"movie","title":"CREW_ONLY_NOT_PROJECTED","job":"Director"}]}`)
	if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePersonCredits}, 77); err != nil {
		t.Fatal(err)
	}
	value, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	want := []CachedTMDBPersonCreditsCandidate{{ID: "tmdb:movie:42", MediaType: "movie", TMDBID: 42, Name: ""}, {ID: "tmdb:tv:42", MediaType: "series", TMDBID: 42, Name: "   "}}
	if err != nil || !reflect.DeepEqual(value.Candidates, want) {
		t.Fatal("later appearances repaired or replaced the first primary label", err)
	}
}

func TestTMDBPersonCreditsSnapshotRawBudgetAndInvalidDuplicateRowsPreservePriorObservation(t *testing.T) {
	f := newPersonCreditsSnapshotFixture(t, `{"cast":[]}`)
	rows := make([]tmdbShelfTitle, 4096)
	for i := range rows {
		rows[i] = tmdbShelfTitle{ID: 1, MediaType: "movie", Title: "First occurrence"}
	}
	f.service.tmdb.recordTMDBPersonCreditsSnapshot(t.Context(), 77, rows, time.Now())
	value, err := f.service.GetCachedTMDBPersonCreditsSnapshot(77, time.Now())
	if err != nil || len(value.Candidates) != 1 || value.Candidates[0].Name != "First occurrence" {
		t.Fatal("valid raw bound was treated as a role count", err)
	}
	before, _ := os.ReadFile(f.path())
	for _, tc := range []struct {
		name   string
		mutate func([]tmdbShelfTitle) []tmdbShelfTitle
	}{
		{"too-many-raw-duplicates", func(v []tmdbShelfTitle) []tmdbShelfTitle { return append(v, v[0]) }},
		{"late-duplicate-invalid-label", func(v []tmdbShelfTitle) []tmdbShelfTitle { v[len(v)-1].Title = "Invalid\nlabel"; return v }},
		{"late-duplicate-oversized-label", func(v []tmdbShelfTitle) []tmdbShelfTitle { v[len(v)-1].Title = strings.Repeat("x", 1025); return v }},
		{"late-duplicate-invalid-primary", func(v []tmdbShelfTitle) []tmdbShelfTitle { v[len(v)-1].ID = 0; return v }},
		{"late-duplicate-ambiguous-kind", func(v []tmdbShelfTitle) []tmdbShelfTitle { v[len(v)-1].MediaType = ""; return v }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := append([]tmdbShelfTitle{}, rows...)
			f.service.tmdb.recordTMDBPersonCreditsSnapshot(t.Context(), 77, tc.mutate(copy), time.Now())
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(before, after) || f.calls.Load() != 0 {
				t.Fatal("bad raw appearance was silently discarded, renewed or fetched")
			}
		})
	}
}
