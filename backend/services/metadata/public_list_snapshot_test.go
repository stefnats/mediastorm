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

type publicListSnapshotFixture struct {
	service    *Service
	cache      *fileCache
	calls      atomic.Int32
	body       atomic.Value
	status     atomic.Int32
	closeBody  func()
	failDetail atomic.Bool
}

func publicListSnapshotPayload(n int) string {
	items := []map[string]any{}
	for id := 1; id <= n; id++ {
		kind, title, name := "movie", fmt.Sprintf("Movie %04d", id), "SECONDARY_MOVIE_LABEL"
		if id%2 == 0 {
			kind, title, name = "tv", "SECONDARY_SERIES_LABEL", fmt.Sprintf("Series %04d", id)
		}
		items = append(items, map[string]any{"id": id, "media_type": kind, "title": title, "name": name, "original_language": "fr", "overview": "PRIVATE_UNUSED", "poster_path": "/synthetic.jpg", "adult": true})
	}
	raw, _ := json.Marshal(map[string]any{"items": items, "owner": "PRIVATE_OWNER_NOT_PROJECTED"})
	return string(raw)
}

func newPublicListSnapshotFixture(t *testing.T, body string) *publicListSnapshotFixture {
	t.Helper()
	old := http.DefaultTransport
	var denied atomic.Int32
	http.DefaultTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		denied.Add(1)
		return nil, errors.New("external HTTP denied by synthetic public-list fixture")
	})
	t.Cleanup(func() {
		http.DefaultTransport = old
		if denied.Load() != 0 {
			t.Errorf("unexpected default transport demand: %d", denied.Load())
		}
	})
	f := &publicListSnapshotFixture{}
	f.body.Store(body)
	f.status.Store(200)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.Header.Get("Authorization") != "" || r.URL.Query().Get("session_id") != "" || r.URL.Query().Get("api_key") != "synthetic-global-key" {
			t.Error("normal global API-key-only list operation changed")
			w.WriteHeader(400)
			return
		}
		if r.URL.Path == "/3/movie/1" && f.failDetail.Load() {
			w.WriteHeader(400)
			io.WriteString(w, `{"error":"synthetic later filter error"}`)
			return
		}
		if r.URL.Path != "/3/list/77" {
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

func (f *publicListSnapshotFixture) path() string {
	return filepath.Join(f.cache.dir, publicListSnapshotKey(77, f.service.CachedTMDBPublicListMetadataLanguage())+".json")
}

func TestTMDBPublicListSnapshotActualProducerBeforeLegacyFiveHundredWindow(t *testing.T) {
	f := newPublicListSnapshotFixture(t, publicListSnapshotPayload(704))
	if _, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now()); err != ErrTMDBPublicListSnapshotUnavailable || f.calls.Load() != 0 {
		t.Fatal("missing observation caused provider demand", err)
	}
	opts := TMDBListOptions{SourceType: TMDBSourcePublicList, SourceID: "77", Limit: 1000, DeferArtwork: true}
	legacy, total, err := f.service.GetTMDBList(t.Context(), opts)
	if err != nil || len(legacy) != 500 || total != 704 || f.calls.Load() != 1 {
		t.Fatal("normal list mapping/cap/provider count changed", err, len(legacy), total, f.calls.Load())
	}
	snapshot, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
	if err != nil || len(snapshot.Candidates) != 704 || snapshot.SourceType != TMDBSourcePublicList || snapshot.MetadataLanguage != "en-US" || snapshot.ExpiresAt.Sub(snapshot.ObservedAt) != 24*time.Hour {
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
	if stat.Mode().Perm() != 0600 || strings.Contains(string(before), "PRIVATE_") || strings.Contains(string(before), "original_language") || strings.Contains(string(before), "SECONDARY_") || strings.Contains(string(before), `"public":`) {
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
	again, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
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

func TestTMDBPublicListSnapshotMixedSameIDAndExactPrimaryLabels(t *testing.T) {
	f := newPublicListSnapshotFixture(t, `{"items":[{"id":42,"media_type":"movie","title":"","name":"Secondary movie fallback"},{"id":42,"media_type":"tv","name":"   ","title":"Secondary series fallback"}]}`)
	legacy, _, err := f.service.GetTMDBList(t.Context(), TMDBListOptions{SourceType: TMDBSourcePublicList, SourceID: "77", DeferArtwork: true})
	if err != nil || len(legacy) != 1 || legacy[0].Title.Name != "Secondary movie fallback" {
		t.Fatal("legacy fallback/drop-blank mapping changed", err)
	}
	snapshot, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
	if err != nil || len(snapshot.Candidates) != 2 {
		t.Fatal("mixed primary identities with same number were lost", err)
	}
	want := []CachedTMDBPublicListCandidate{{ID: "tmdb:movie:42", MediaType: "movie", TMDBID: 42, Name: ""}, {ID: "tmdb:tv:42", MediaType: "series", TMDBID: 42, Name: "   "}}
	if !reflect.DeepEqual(snapshot.Candidates, want) || snapshot.Titles()[0].Name != "" || snapshot.Titles()[1].Name != "   " {
		t.Fatal("primary provider labels were trimmed, guessed or replaced")
	}
}

func TestTMDBPublicListSnapshotSuccessfulOperationSurvivesLaterLocalFilterError(t *testing.T) {
	f := newPublicListSnapshotFixture(t, publicListSnapshotPayload(1))
	f.failDetail.Store(true)
	_, _, err := f.service.GetTMDBList(t.Context(), TMDBListOptions{SourceType: TMDBSourcePublicList, SourceID: "77", DiscoverQuery: "companies=5", DeferArtwork: true})
	if err == nil {
		t.Fatal("ordinary later filter error disappeared")
	}
	// The observed operation already succeeded. This is not a claim that all
	// later filtering, mapping or enrichment for GetTMDBList also succeeded.
	snapshot, readErr := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
	if readErr != nil || len(snapshot.Candidates) != 1 || f.calls.Load() != 2 {
		t.Fatal("successful raw operation was incorrectly tied to later filter success", readErr)
	}
}

func TestTMDBPublicListSnapshotExplicitEmptyAndInvalidProducerDoesNotRenew(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		cancel     bool
	}{
		{"missing items", `{}`, 200, false}, {"null items", `{"items":null}`, 200, false},
		{"missing explicit kind", `{"items":[{"id":1,"title":"Normal mapper can infer this"}]}`, 200, false},
		{"noncanonical kind", `{"items":[{"id":1,"media_type":"series","name":"Normal mapper can infer this"}]}`, 200, false},
		{"invalid id", `{"items":[{"id":0,"media_type":"movie","title":"Invalid"}]}`, 200, false},
		{"duplicate kind id", `{"items":[{"id":1,"media_type":"movie","title":"First"},{"id":1,"media_type":"movie","title":"Second"}]}`, 200, false},
		{"control label", `{"items":[{"id":1,"media_type":"movie","title":"Unsafe\nLabel"}]}`, 200, false},
		{"provider error", `{"error":"synthetic failure"}`, 400, false},
		{"cancelled after decode", publicListSnapshotPayload(2), 200, true},
		{"too many candidates", publicListSnapshotPayload(4097), 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPublicListSnapshotFixture(t, `{"items":[]}`)
			if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePublicList}, 77); err != nil {
				t.Fatal(err)
			}
			empty, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
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
			_, err = f.service.tmdb.staticShelfTitles(ctx, TMDBListOptions{SourceType: TMDBSourcePublicList}, 77)
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

func TestTMDBPublicListSnapshotStrictStoredObservation(t *testing.T) {
	f := newPublicListSnapshotFixture(t, publicListSnapshotPayload(2))
	if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePublicList}, 77); err != nil {
		t.Fatal(err)
	}
	original, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	f.service.tmdb.httpc = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("getter attempted provider work")
		return nil, errors.New("provider trap")
	})}
	for _, name := range []string{"version", "provider", "source type", "list id", "locale", "future", "expired", "extended ttl", "nil candidates", "wrong scalar", "wrong kind", "short identity", "duplicate same kind alias", "unknown field", "missing name", "null name", "duplicate top key", "duplicate candidate key", "trailing object", "trailing garbage", "invalid utf8", "too many candidates", "too many bytes"} {
		t.Run(name, func(t *testing.T) {
			value := *original
			value.Candidates = append([]CachedTMDBPublicListCandidate{}, original.Candidates...)
			switch name {
			case "version":
				value.Version++
			case "provider":
				value.Provider = "other"
			case "source type":
				value.SourceType = "private-account-list"
			case "list id":
				value.ListID++
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
				value.Candidates = []CachedTMDBPublicListCandidate{}
				for i := 1; i <= 4097; i++ {
					value.Candidates = append(value.Candidates, CachedTMDBPublicListCandidate{ID: "tmdb:movie:" + strconv.Itoa(i), MediaType: "movie", TMDBID: int64(i), Name: "Bounded"})
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
				raw = append(bytes.Repeat([]byte(" "), publicListSnapshotMaxBytes), raw...)
			}
			if err := os.WriteFile(f.path(), raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now()); err != ErrTMDBPublicListSnapshotUnavailable {
				t.Fatal("untrusted snapshot accepted", err)
			}
			after, _ := os.ReadFile(f.path())
			if !bytes.Equal(raw, after) || f.calls.Load() != 1 {
				t.Fatal("getter wrote invalid bytes or caused provider demand")
			}
		})
	}
}

func TestTMDBPublicListSnapshotExpiryNoFollowAndReadOnlyMisses(t *testing.T) {
	f := newPublicListSnapshotFixture(t, publicListSnapshotPayload(2))
	f.cache.ttl = time.Hour
	if _, err := f.service.tmdb.staticShelfTitles(t.Context(), TMDBListOptions{SourceType: TMDBSourcePublicList}, 77); err != nil {
		t.Fatal(err)
	}
	value, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
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
			path := filepath.Join(cache.dir, publicListSnapshotKey(77, value.MetadataLanguage)+".json")
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
			if _, err := service.GetCachedTMDBPublicListSnapshot(77, at); err != ErrTMDBPublicListSnapshotUnavailable {
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
		if _, err := service.GetCachedTMDBPublicListSnapshot(77, time.Now()); err != ErrTMDBPublicListSnapshotUnavailable {
			t.Fatal("missing reader accepted")
		}
	}
	var absent *Service
	if absent.CachedTMDBPublicListMetadataLanguage() != "" || (*CachedTMDBPublicListSnapshot)(nil).Titles() != nil {
		t.Fatal("nil-safe API changed")
	}
	if _, err := f.service.GetCachedTMDBPublicListSnapshot(0, time.Now()); err != ErrTMDBPublicListSnapshotUnavailable {
		t.Fatal("invalid list ID accepted")
	}
	if _, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Time{}); err != ErrTMDBPublicListSnapshotUnavailable {
		t.Fatal("zero observation time accepted")
	}
	localized := f.service.WithLanguage("fra")
	if localized.CachedTMDBPublicListMetadataLanguage() != "fr-US" {
		t.Fatal("canonical effective locale changed")
	}
	if _, err := localized.GetCachedTMDBPublicListSnapshot(77, time.Now()); err != ErrTMDBPublicListSnapshotUnavailable {
		t.Fatal("different locale borrowed a snapshot")
	}
}

func TestTMDBPublicListSnapshotWriterBoundsAndNoScalarPromotion(t *testing.T) {
	f := newPublicListSnapshotFixture(t, `{"items":[]}`)
	items := make([]tmdbShelfTitle, 0, 4096)
	for id := 1; id <= 4096; id++ {
		items = append(items, tmdbShelfTitle{ID: int64(id), MediaType: "movie", Title: "Boundary", Adult: true, OriginalLanguage: "fr"})
	}
	f.service.tmdb.recordTMDBPublicListSnapshot(t.Context(), 77, items, time.Now())
	snapshot, err := f.service.GetCachedTMDBPublicListSnapshot(77, time.Now())
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
	f.service.tmdb.recordTMDBPublicListSnapshot(t.Context(), 77, items, time.Now())
	after, _ := os.ReadFile(f.path())
	if !bytes.Equal(before, after) || f.calls.Load() != 0 {
		t.Fatal("oversized encoded observation renewed bytes or fetched")
	}
}
