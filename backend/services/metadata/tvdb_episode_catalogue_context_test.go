package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
)

// This transport changes only the wire destination to an owned loopback server.
// The actual client still constructs and validates the fixed official URL.
type catalogueLoopbackTransport struct {
	target   *url.URL
	actual   *http.Transport
	calls    atomic.Int64
	decorate func(*http.Request, io.ReadCloser) io.ReadCloser
	origin   string
	before   func(*http.Request) error
}

func (p *catalogueLoopbackTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	p.calls.Add(1)
	origin := p.origin
	if origin == "" {
		origin = "api4.thetvdb.com"
	}
	if request.URL.Scheme != "https" || request.URL.Host != origin {
		return nil, fmt.Errorf("unexpected provider origin")
	}
	if p.before != nil {
		if err := p.before(request); err != nil {
			return nil, err
		}
	}
	local := request.Clone(request.Context())
	copyURL := *request.URL
	local.URL = &copyURL
	local.URL.Scheme = p.target.Scheme
	local.URL.Host = p.target.Host
	local.Host = p.target.Host
	response, err := p.actual.RoundTrip(local)
	if err == nil && response.Body != nil && p.decorate != nil {
		response.Body = p.decorate(request, response.Body)
	}
	return response, err
}

type catalogueFixture struct {
	service   *Service
	transport *catalogueLoopbackTransport
	pages     atomic.Int64
	server    *httptest.Server
	handle    func(http.ResponseWriter, *http.Request) bool
}

func newCatalogueFixture(t *testing.T, handle func(http.ResponseWriter, *http.Request) bool) *catalogueFixture {
	t.Helper()
	f := &catalogueFixture{handle: handle}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/episodes/") {
			f.pages.Add(1)
		}
		if f.handle != nil && f.handle(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v4/login":
			io.WriteString(w, `{"status":"success","data":{"token":"synthetic-catalogue-token"}}`)
		case strings.Contains(r.URL.Path, "/episodes/"):
			parts := strings.Split(r.URL.Path, "/")
			id, _ := strconv.ParseInt(parts[3], 10, 64)
			writeCataloguePage(w, []tvdbEpisode{catalogueEpisode(id, id*100+1, 1, 1, "2024-01-02")}, nil)
		case strings.HasSuffix(r.URL.Path, "/extended"):
			io.WriteString(w, `{"data":{"id":42,"name":"Synthetic programme","year":"2024","seasons":[{"id":420,"number":1,"type":{"type":"official","name":"Official"}}],"episodes":[{"id":4201,"seriesId":42,"seasonNumber":1,"number":1,"name":"Synthetic episode","aired":"2024-01-02"}]}}`)
		case strings.Contains(r.URL.Path, "/translations/"):
			io.WriteString(w, `{"data":{"name":"Synthetic programme","overview":"Synthetic description"}}`)
		case r.URL.Path == "/v4/series/42":
			io.WriteString(w, `{"data":{"id":42,"name":"Synthetic programme","year":"2024"}}`)
		default:
			http.Error(w, "unexpected fixture path", http.StatusNotFound)
		}
	}))
	target, _ := url.Parse(f.server.URL)
	f.transport = &catalogueLoopbackTransport{target: target, actual: http.DefaultTransport.(*http.Transport).Clone()}
	f.service = NewService("synthetic-catalogue-key", "", "eng", t.TempDir(), 24, false, MDBListConfig{})
	f.service.client.httpc = &http.Client{Transport: f.transport}
	f.service.client.minInterval = 0
	t.Cleanup(func() {
		f.transport.actual.CloseIdleConnections()
		f.server.Close()
		if f.service.trailerPrequeue != nil {
			f.service.trailerPrequeue.Stop()
		}
	})
	return f
}
func catalogueEpisode(series, id int64, season, number int, date string) tvdbEpisode {
	return tvdbEpisode{SeriesID: series, ID: id, SeasonNumber: season, Number: number, Aired: date, Name: "Synthetic episode", Translations: []tvdbEpisodeTranslation{{Language: "eng", Name: "Synthetic title"}}}
}
func writeCataloguePage(w http.ResponseWriter, rows []tvdbEpisode, next any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"episodes": rows}, "links": map[string]any{"next": next}})
}
func (f *catalogueFixture) fetch(ctx context.Context, series int64) error {
	_, err := f.service.cachedSeriesEpisodesBySeasonTypeContext(ctx, series, "official", "eng")
	return err
}
func catalogueMatched(s *Service, series int64, date string) bool {
	_, ok := s.GetCachedDateEpisodeMatch(context.Background(), models.EpisodeNumbering{SeriesID: fmt.Sprintf("tvdb:series:%d", series), Ordering: "official"}, date)
	return ok
}
func awaitCatalogue(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("actual %s did not finish", label)
	}
}

type catalogueHeldClose struct {
	io.ReadCloser
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (b *catalogueHeldClose) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return err
}

func TestTVDBDateCatalogueOriginalProducer(t *testing.T) {
	cases := []string{
		"complete-two-page-original-series",
		"caller-cancel-held-login",
		"caller-cancel-held-page-body",
		"held-body-close-delays-publication",
		"original-deadline-before-late-page",
		"page-cap-never-complete",
		"byte-cap-never-complete",
		"episode-cap-never-complete",
		"next-origin-path-and-sequence-denied",
		"foreign-series-row-never-matches",
		"duplicate-id-or-coordinate-unknown",
		"token-mutex-wait-cancel-no-request",
		"redirect-cancel-and-origin-boundary",
		"normal-series-details-produces-without-extra-lookup",
		"full-fallback-joins-original-catalogue-close",
		"lite-fallback-joins-original-catalogue-close",
		"full-fresh-multiple-orderings-observed-once",
		"lite-fresh-multiple-orderings-observed-once",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			switch name {
			case "complete-two-page-original-series":
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					if r.Header.Get("Authorization") != "Bearer synthetic-catalogue-token" {
						t.Error("missing actual login token")
					}
					page, _ := strconv.Atoi(r.URL.Query().Get("page"))
					var next any
					if page == 0 {
						next = "/v4/series/42/episodes/official/eng?page=1"
					}
					writeCataloguePage(w, []tvdbEpisode{catalogueEpisode(42, int64(4201+page), 1, page+1, fmt.Sprintf("2024-01-%02d", page+2))}, next)
					return true
				})
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				if f.pages.Load() != 2 || !catalogueMatched(f.service, 42, "2024-01-02") || !catalogueMatched(f.service, 42, "2024-01-03") {
					t.Fatal("genuine complete two-page observation missing")
				}
				if catalogueMatched(f.service, 43, "2024-01-02") {
					t.Fatal("foreign series matched")
				}
			case "caller-cancel-held-login", "caller-cancel-held-page-body", "original-deadline-before-late-page":
				entered := make(chan struct{})
				returned := make(chan struct{})
				release := make(chan struct{})
				var releaseOnce sync.Once
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					login := r.URL.Path == "/v4/login"
					if (name == "caller-cancel-held-login") != login {
						return false
					}
					defer close(returned)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(200)
					if login {
						io.WriteString(w, `{"status":"success","data":{"token":"`)
					} else {
						io.WriteString(w, `{"status":"success","data":{"episodes":[`)
					}
					w.(http.Flusher).Flush()
					close(entered)
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return true
				})
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if name == "original-deadline-before-late-page" {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
				}
				done := make(chan struct{})
				var err error
				t.Cleanup(func() {
					cancel()
					releaseOnce.Do(func() { close(release) })
					awaitCatalogue(t, done, "canceled fetch")
					awaitCatalogue(t, returned, "server read")
				})
				go func() { defer close(done); err = f.fetch(ctx, 42) }()
				awaitCatalogue(t, entered, "real response headers")
				if name != "original-deadline-before-late-page" {
					cancel()
				}
				awaitCatalogue(t, done, "original request cancellation")
				awaitCatalogue(t, returned, "actual server cancellation")
				if err == nil || catalogueMatched(f.service, 42, "2024-01-02") || len(f.service.episodeDates.slots) != 0 {
					t.Fatal("cancelled producer published or leaked a slot")
				}
			case "held-body-close-delays-publication":
				f := newCatalogueFixture(t, nil)
				entered := make(chan struct{})
				release := make(chan struct{})
				var once sync.Once
				f.transport.decorate = func(r *http.Request, body io.ReadCloser) io.ReadCloser {
					if strings.Contains(r.URL.Path, "/episodes/") {
						return &catalogueHeldClose{ReadCloser: body, entered: entered, release: release}
					}
					return body
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				var err error
				t.Cleanup(func() { cancel(); once.Do(func() { close(release) }); awaitCatalogue(t, done, "held physical Close") })
				go func() { defer close(done); err = f.fetch(ctx, 42) }()
				awaitCatalogue(t, entered, "delegated body Close")
				if catalogueMatched(f.service, 42, "2024-01-02") || len(f.service.episodeDates.slots) != 1 {
					t.Fatal("physical Close treated as finished")
				}
				select {
				case <-done:
					t.Fatal("held actual body Close returned early")
				default:
				}
				once.Do(func() { close(release) })
				awaitCatalogue(t, done, "unblocked physical Close")
				if err != nil || !catalogueMatched(f.service, 42, "2024-01-02") || len(f.service.episodeDates.slots) != 0 {
					t.Fatalf("complete Close did not publish: %v", err)
				}
			case "page-cap-never-complete":
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					page, _ := strconv.Atoi(r.URL.Query().Get("page"))
					writeCataloguePage(w, []tvdbEpisode{}, fmt.Sprintf("/v4/series/42/episodes/official/eng?page=%d", page+1))
					return true
				})
				if err := f.fetch(context.Background(), 42); err == nil || f.pages.Load() != 256 || catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("nonterminal page cap became a complete list")
				}
			case "byte-cap-never-complete":
				for _, aggregate := range []bool{false, true} {
					f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
						if !strings.Contains(r.URL.Path, "/episodes/") {
							return false
						}
						if !aggregate {
							io.WriteString(w, strings.Repeat("x", episodeCataloguePageBytes+1))
							return true
						}
						page, _ := strconv.Atoi(r.URL.Query().Get("page"))
						row := catalogueEpisode(42, int64(4201+page), 1, page+1, "2024-01-02")
						row.Overview = strings.Repeat("x", 900000)
						writeCataloguePage(w, []tvdbEpisode{row}, fmt.Sprintf("/v4/series/42/episodes/official/eng?page=%d", page+1))
						return true
					})
					if err := f.fetch(context.Background(), 42); err == nil || catalogueMatched(f.service, 42, "2024-01-02") {
						t.Fatal("overbound body accepted")
					}
					if aggregate && f.pages.Load() != 19 {
						t.Fatalf("aggregate byte cap not witnessed: %d", f.pages.Load())
					}
				}
			case "episode-cap-never-complete":
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					page, _ := strconv.Atoi(r.URL.Query().Get("page"))
					rows := make([]tvdbEpisode, 2000)
					for i := range rows {
						rows[i] = tvdbEpisode{ID: int64(page*2000 + i + 1), SeriesID: 42, SeasonNumber: 1, Number: page*2000 + i + 1, Aired: "2024-01-02"}
					}
					writeCataloguePage(w, rows, fmt.Sprintf("/v4/series/42/episodes/official/eng?page=%d", page+1))
					return true
				})
				if err := f.fetch(context.Background(), 42); err == nil || f.pages.Load() != 11 || catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("episode cap trimmed a complete list")
				}
			case "next-origin-path-and-sequence-denied":
				invalid := []any{"https://example.invalid/v4/series/42/episodes/official/eng?page=1", "//example.invalid/v4/series/42/episodes/official/eng?page=1", "/v4/series/43/episodes/official/eng?page=1", "/v4/series/42/episodes/official/fra?page=1", "/v4/series/42/episodes/official/eng?page=2", "/v4/series/42/episodes/official/eng?page=1&extra=x", "", "missing", "malformed"}
				for _, next := range invalid {
					f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
						if !strings.Contains(r.URL.Path, "/episodes/") {
							return false
						}
						if next == "missing" {
							io.WriteString(w, `{"status":"success","data":{"episodes":[]},"links":{}}`)
						} else if next == "malformed" {
							io.WriteString(w, `{"status":"success","data":{"episodes":[]},"links":{"next":false}}`)
						} else {
							writeCataloguePage(w, []tvdbEpisode{catalogueEpisode(42, 4201, 1, 1, "2024-01-02")}, next)
						}
						return true
					})
					if err := f.fetch(context.Background(), 42); err == nil || f.pages.Load() != 1 || catalogueMatched(f.service, 42, "2024-01-02") {
						t.Fatalf("invalid Next accepted: %v", next)
					}
				}
			case "foreign-series-row-never-matches", "duplicate-id-or-coordinate-unknown":
				for variant := 0; variant < 2; variant++ {
					f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
						if !strings.Contains(r.URL.Path, "/episodes/") {
							return false
						}
						rows := []tvdbEpisode{catalogueEpisode(42, 4201, 1, 1, "2024-01-02"), catalogueEpisode(42, 4202, 1, 2, "2024-01-03")}
						if name == "foreign-series-row-never-matches" {
							rows[variant].SeriesID = 43
						} else if variant == 0 {
							rows[1].ID = 4201
						} else {
							rows[1].Number = 1
						}
						writeCataloguePage(w, rows, nil)
						return true
					})
					if err := f.fetch(context.Background(), 42); err != nil {
						t.Fatal(err)
					}
					if catalogueMatched(f.service, 42, "2024-01-02") || catalogueMatched(f.service, 42, "2024-01-03") {
						t.Fatal("uncorrelated rows minted date observation")
					}
				}
			case "token-mutex-wait-cancel-no-request":
				f := newCatalogueFixture(t, nil)
				f.service.client.mu.Lock()
				var once sync.Once
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				var err error
				t.Cleanup(func() { cancel(); once.Do(f.service.client.mu.Unlock); awaitCatalogue(t, done, "mutex cancellation") })
				go func() { defer close(done); err = f.fetch(ctx, 42) }()
				// Observe the actual bounded wait behind the original client's
				// mutex, not an unrelated successful-acquire counter or a sleep.
				deadline := time.Now().Add(5 * time.Second)
				for {
					stack := make([]byte, 1<<20)
					n := runtime.Stack(stack, true)
					if strings.Contains(string(stack[:n]), "metadata.lockEpisodeCatalogue(") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("actual original token mutex wait not observed")
					}
					runtime.Gosched()
				}
				cancel()
				awaitCatalogue(t, done, "held token mutex cancellation")
				if err == nil || f.transport.calls.Load() != 0 || len(f.service.episodeDates.slots) != 0 {
					t.Fatal("token wait escaped original cancellation")
				}
			case "redirect-cancel-and-origin-boundary":
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					http.Redirect(w, r, "https://example.invalid/foreign", http.StatusFound)
					return true
				})
				if err := f.fetch(context.Background(), 42); err == nil || f.pages.Load() != 1 || f.transport.calls.Load() != 2 {
					t.Fatal("foreign redirect attempted or accepted")
				}
				if catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("redirect published catalogue")
				}
			case "full-fallback-joins-original-catalogue-close", "lite-fallback-joins-original-catalogue-close":
				entered := make(chan struct{})
				release := make(chan struct{})
				capturedContext := make(chan context.Context, 1)
				var once sync.Once
				var fallbackRequests atomic.Int64
				var heldCatalogue atomic.Bool
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if strings.HasPrefix(r.URL.Path, "/3/") {
						fallbackRequests.Add(1)
						io.WriteString(w, `{"id":77,"name":"Actual fallback catalogue title","original_name":"Actual fallback catalogue title","first_air_date":"2024-01-01","seasons":[],"external_ids":{},"cast":[],"crew":[],"results":[],"logos":[],"posters":[],"backdrops":[]}`)
						return true
					}
					if strings.HasSuffix(r.URL.Path, "/extended") {
						io.WriteString(w, `{"data":{"id":42,"name":"Synthetic programme","year":"2024","remoteIds":[{"id":"88","sourceName":"TheMovieDB.com"}],"seasons":[{"id":420,"number":1,"type":{"type":"official","name":"Official"}}],"episodes":[{"id":4201,"seriesId":42,"seasonNumber":1,"number":1,"aired":"2024-01-02"}]}}`)
						return true
					}
					return false
				})
				f.transport.decorate = func(r *http.Request, body io.ReadCloser) io.ReadCloser {
					if strings.Contains(r.URL.Path, "/episodes/") {
						if !heldCatalogue.CompareAndSwap(false, true) {
							t.Error("unexpected duplicate localized catalogue response")
							return body
						}
						capturedContext <- r.Context()
						return &catalogueHeldClose{ReadCloser: body, entered: entered, release: release}
					}
					return body
				}
				tmdbWire := &catalogueLoopbackTransport{target: f.transport.target, actual: http.DefaultTransport.(*http.Transport).Clone(), origin: "api.themoviedb.org", before: func(r *http.Request) error {
					// This only delays a real delegated request until the actual
					// catalogue Close is held; it supplies no provider result.
					select {
					case <-entered:
						return nil
					case <-r.Context().Done():
						return r.Context().Err()
					}
				}}
				f.service.tmdb = newTMDBClient("synthetic-tmdb-key", "eng", &http.Client{Transport: tmdbWire}, f.service.cache)
				t.Cleanup(tmdbWire.actual.CloseIdleConnections)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				done := make(chan struct{})
				var result *models.SeriesDetails
				var err error
				t.Cleanup(func() {
					cancel()
					once.Do(func() { close(release) })
					awaitCatalogue(t, done, "fallback and original catalogue Close")
				})
				go func() {
					defer close(done)
					request := models.SeriesDetailsQuery{TVDBID: 42, TMDBID: 77, Name: "Synthetic programme", Year: 2024}
					if name == "full-fallback-joins-original-catalogue-close" {
						result, err = f.service.SeriesDetails(ctx, request)
					} else {
						result, err = f.service.SeriesDetailsLite(ctx, request)
					}
				}()
				awaitCatalogue(t, entered, "real localized catalogue Body.Close")
				actualContext := <-capturedContext
				awaitCatalogue(t, actualContext.Done(), "method fallback retirement of original catalogue child")
				if fallbackRequests.Load() == 0 || len(f.service.episodeDates.slots) != 1 || catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("fallback skipped actual HTTP or refunded held original Close")
				}
				select {
				case <-done:
					t.Fatal("actual fallback returned before its localized child physically ended")
				default:
				}
				once.Do(func() { close(release) })
				awaitCatalogue(t, done, "joined real fallback return")
				if err != nil || result == nil || result.Title.TMDBID != 77 || result.Title.Name != "Actual fallback catalogue title" || f.pages.Load() != 1 || len(f.service.episodeDates.slots) != 0 || catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatalf("genuine fallback/original child cleanup: %v", err)
				}
			case "full-fresh-multiple-orderings-observed-once", "lite-fresh-multiple-orderings-observed-once":
				var official, alternative atomic.Int64
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if strings.HasSuffix(r.URL.Path, "/extended") {
						io.WriteString(w, `{"data":{"id":42,"name":"Synthetic programme","year":"2024","seasons":[{"id":420,"number":1,"type":{"type":"official","name":"Official"}},{"id":421,"number":1,"type":{"type":"dvd","name":"DVD"}}],"episodes":[{"id":4201,"seriesId":42,"seasonNumber":1,"number":1,"aired":"2024-01-02"}]}}`)
						return true
					}
					if strings.Contains(r.URL.Path, "/episodes/official/") {
						official.Add(1)
					}
					if strings.Contains(r.URL.Path, "/episodes/dvd/") {
						alternative.Add(1)
					}
					return false
				})
				request := models.SeriesDetailsQuery{TVDBID: 42, Name: "Synthetic programme", Year: 2024}
				var result *models.SeriesDetails
				var err error
				if name == "full-fresh-multiple-orderings-observed-once" {
					result, err = f.service.SeriesDetails(context.Background(), request)
				} else {
					result, err = f.service.SeriesDetailsLite(context.Background(), request)
				}
				if err != nil || result == nil || len(result.AvailableOrderings) != 2 || official.Load() != 1 || alternative.Load() != 1 || !catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatalf("normal multi-ordering observation/reuse: %v official=%d alternative=%d", err, official.Load(), alternative.Load())
				}
				before := f.transport.calls.Load()
				tooMany := make([]models.SeriesOrdering, 33)
				for i := range tooMany {
					tooMany[i].Type = fmt.Sprintf("synthetic-order-%d", i)
				}
				if result := f.service.filterOrderingsWithEpisodesContext(context.Background(), 43, tooMany, "eng"); result != nil || f.transport.calls.Load() != before || catalogueMatched(f.service, 43, "2024-01-02") {
					t.Fatal("overbound ordering validation truncated/fetched/minted DATA")
				}
			case "normal-series-details-produces-without-extra-lookup":
				f := newCatalogueFixture(t, nil)
				details, err := f.service.SeriesDetails(context.Background(), models.SeriesDetailsQuery{TVDBID: 42, Name: "Synthetic programme", Year: 2024})
				if err != nil || details == nil || len(details.Seasons) != 1 {
					t.Fatalf("actual SeriesDetails: %v", err)
				}
				before := f.transport.calls.Load()
				if !catalogueMatched(f.service, 42, "2024-01-02") || f.pages.Load() != 1 || f.transport.calls.Load() != before {
					t.Fatal("normal producer observation absent or getter fetched")
				}
			}
		})
	}
}
