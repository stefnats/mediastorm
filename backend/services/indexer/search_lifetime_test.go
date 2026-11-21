package indexer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/providerbreaker"
	"novastream/models"
	"novastream/services/debrid"
)

func indexerLifeWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		t.Fatal("actual indexer source did not end")
	}
}
func indexerLifePoll(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual indexer source witness missing")
}

type indexerHTTPGate struct {
	client           *http.Client
	server           *httptest.Server
	reading, closing atomic.Int32
	release          chan struct{}
	once             sync.Once
}
type indexerGateTransport struct {
	actual http.RoundTripper
	gate   *indexerHTTPGate
}

func (t indexerGateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, e := t.actual.RoundTrip(r)
	if response != nil && response.Body != nil {
		response.Body = &indexerGateBody{ReadCloser: response.Body, gate: t.gate}
	}
	return response, e
}

type indexerGateBody struct {
	io.ReadCloser
	gate *indexerHTTPGate
	once sync.Once
}

func (b *indexerGateBody) Read(p []byte) (int, error) {
	b.once.Do(func() { b.gate.reading.Add(1) })
	return b.ReadCloser.Read(p)
}
func (b *indexerGateBody) Close() error {
	b.gate.closing.Add(1)
	<-b.gate.release
	return b.ReadCloser.Close()
}
func (g *indexerHTTPGate) unblock() { g.once.Do(func() { close(g.release) }) }
func newIndexerHTTPGate(t *testing.T, status int) *indexerHTTPGate {
	t.Helper()
	g := &indexerHTTPGate{release: make(chan struct{})}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte("<rss><channel>"))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	tr := &http.Transport{}
	g.client = &http.Client{Transport: indexerGateTransport{tr, g}}
	t.Cleanup(func() { g.unblock(); tr.CloseIdleConnections(); g.server.Close() })
	return g
}
func (g *indexerHTTPGate) Search(ctx context.Context, _ debrid.SearchOptions) ([]models.NZBResult, error) {
	request, e := http.NewRequestWithContext(ctx, "GET", g.server.URL+"/debrid", nil)
	if e != nil {
		return nil, e
	}
	response, e := g.client.Do(request)
	if e != nil {
		return nil, e
	}
	defer response.Body.Close()
	_, e = io.ReadAll(response.Body)
	return nil, e
}
func TestIndexerSourcePhysicalLifetime(t *testing.T) {
	for _, kind := range []string{"split-cancel-joins-held-http-body", "split-cancel-joins-both-source-workers", "parallel-query-cancel-joins-all-indexers", "combined-filter-cancel-joins-queries", "error-response-close-before-channel-end", "caller-expiry-no-result-cache"} {
		t.Run(kind, func(t *testing.T) {
			status := 200
			if kind == "error-response-close-before-channel-end" {
				status = 500
			}
			gate := newIndexerHTTPGate(t, status)
			manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
			settings := config.DefaultSettings()
			settings.Streaming.ServiceMode = config.StreamingServiceModeUsenet
			settings.Indexers = []config.IndexerConfig{{Name: "Synthetic lifecycle one", URL: gate.server.URL, APIKey: "synthetic-unused", Type: "newznab", Enabled: true}}
			want := int32(1)
			if kind == "parallel-query-cancel-joins-all-indexers" || kind == "combined-filter-cancel-joins-queries" {
				second := settings.Indexers[0]
				second.Name = "Synthetic lifecycle two"
				settings.Indexers = append(settings.Indexers, second)
				want = 4
			}
			if kind == "split-cancel-joins-both-source-workers" {
				want = 2
			}
			if e := manager.Save(settings); e != nil {
				t.Fatal(e)
			}
			service := &Service{cfg: manager, httpc: gate.client, providerBreaker: providerbreaker.New()}

			ctx, cancel := context.WithCancel(t.Context())
			if kind == "caller-expiry-no-result-cache" {
				var end context.CancelFunc
				ctx, end = context.WithTimeout(ctx, 300*time.Millisecond)
				defer end()
			}
			ctx = joinedIndexerSourceContext(ctx)
			done := make(chan struct{})
			t.Cleanup(func() { cancel(); gate.unblock(); indexerLifeWait(t, done) })
			go func() {
				defer close(done)
				opts := SearchOptions{Query: "Synthetic movie", MediaType: "movie", SkipFilter: true}
				switch kind {
				case "parallel-query-cancel-joins-all-indexers":
					_, _ = service.fetchUsenetResultsAllQueries(ctx, settings, opts, []string{"Synthetic one", "Synthetic two"})
				case "combined-filter-cancel-joins-queries":
					_, _ = service.searchUsenetWithFilter(ctx, settings, opts, debrid.ParseQuery(opts.Query), nil, nil, []string{"Synthetic one", "Synthetic two"}, models.FilterSettings{})
				case "error-response-close-before-channel-end":
					_, _ = service.fetchTorznab(ctx, settings.Indexers[0], opts)
				default:
					count := 1
					if kind == "split-cancel-joins-both-source-workers" {
						count = 2
					}
					var callers sync.WaitGroup
					for i := 0; i < count; i++ {
						callers.Add(1)
						go func() {
							defer callers.Done()
							u, d := service.SearchWithScoringSplitJoined(ctx, opts)
							for range u {
							}
							for range d {
							}
						}()
					}
					callers.Wait()
				}
			}()
			indexerLifePoll(t, func() bool { return gate.reading.Load() == want })
			if kind != "caller-expiry-no-result-cache" {
				cancel()
			}
			indexerLifePoll(t, func() bool { return gate.closing.Load() == want })
			select {
			case <-done:
				t.Fatal("result completion preceded actual source Body.Close")
			case <-time.After(20 * time.Millisecond):
			}
			gate.unblock()
			indexerLifeWait(t, done)
			if gate.closing.Load() != want {
				t.Fatal("actual source count changed", gate.closing.Load(), want)
			}
			// The cancelled split must not populate a reusable aggregate, even if all
			// source goroutines finally return after their held closes are released.
			if kind == "caller-expiry-no-result-cache" || kind == "split-cancel-joins-held-http-body" {
				service.searchCacheMu.RLock()
				count := len(service.searchCache)
				service.searchCacheMu.RUnlock()
				if count != 0 {
					t.Fatal("cancelled source populated search cache", count)
				}
			}
		})
	}
}
