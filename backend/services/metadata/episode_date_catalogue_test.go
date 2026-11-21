package metadata

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"novastream/models"
)

func TestCachedEpisodeDateCatalogue(t *testing.T) {
	cases := []string{
		"exact-original-provider-numbering-and-id",
		"duplicate-date-including-special",
		"missing-or-invalid-date-unknown",
		"future-date-and-expired-observation-unknown",
		"old-disk-cache-and-imported-flags-unknown",
		"getter-zero-http-file-and-no-renewal",
		"result-defensive-data-copy",
		"language-view-shares-original-observation",
		"config-rotation-retires-old-inflight",
		"no-tmdb-name-or-filename-alias",
		"bounded-whole-entry-eviction",
		"legacy-api-and-current-search-hints-unchanged",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			n := models.EpisodeNumbering{SeriesID: "tvdb:series:42", Ordering: "official"}
			switch name {
			case "exact-original-provider-numbering-and-id":
				f := newCatalogueFixture(t, nil)
				before := time.Now()
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				result, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				if !ok || result.Provider != "tvdb" || result.EpisodeID != "tvdb:episode:4201" || result.Numbering != n || result.SeasonNumber != 1 || result.EpisodeNumber != 1 || result.AiredDate != "2024-01-02" || result.ObservedAt.Before(before) || !result.ExpiresAt.Equal(result.ObservedAt.Add(24*time.Hour)) {
					t.Fatalf("wrong observed provider DATA: %+v %v", result, ok)
				}
				for _, bad := range []models.EpisodeNumbering{{SeriesID: "tvdb:series:43", Ordering: "official"}, {SeriesID: n.SeriesID, Ordering: "dvd"}, {SeriesID: n.SeriesID, Ordering: "absolute"}} {
					if _, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), bad, "2024-01-02"); ok {
						t.Fatal("foreign numbering matched")
					}
				}
			case "duplicate-date-including-special":
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					writeCataloguePage(w, []tvdbEpisode{catalogueEpisode(42, 4201, 1, 1, "2024-01-02"), catalogueEpisode(42, 4202, 0, 1, "2024-01-02"), catalogueEpisode(42, 4203, 1, 2, "2024-01-03"), catalogueEpisode(42, 4204, 0, 2, "2024-01-04")}, nil)
					return true
				})
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				if catalogueMatched(f.service, 42, "2024-01-02") || catalogueMatched(f.service, 42, "2024-01-04") || !catalogueMatched(f.service, 42, "2024-01-03") {
					t.Fatal("special/duplicate date was selected or unrelated unique date lost")
				}
			case "missing-or-invalid-date-unknown":
				for _, date := range []string{"", "2024-02-30", "2024-1-02", "2024-01-02T00:00:00Z", "0000-01-01"} {
					f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
						if !strings.Contains(r.URL.Path, "/episodes/") {
							return false
						}
						writeCataloguePage(w, []tvdbEpisode{catalogueEpisode(42, 4201, 1, 1, "2024-01-02"), catalogueEpisode(42, 4202, 1, 2, date)}, nil)
						return true
					})
					if err := f.fetch(context.Background(), 42); err != nil {
						t.Fatal(err)
					}
					if catalogueMatched(f.service, 42, "2024-01-02") || catalogueMatched(f.service, 42, date) {
						t.Fatalf("incomplete date list accepted: %q", date)
					}
				}
			case "future-date-and-expired-observation-unknown":
				future := time.Now().UTC().AddDate(0, 0, 2).Format("2006-01-02")
				f := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					writeCataloguePage(w, []tvdbEpisode{catalogueEpisode(42, 4201, 1, 1, "2024-01-02"), catalogueEpisode(42, 4202, 1, 2, future)}, nil)
					return true
				})
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				captured, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				if !ok {
					t.Fatal("actual observation missing")
				}
				calls := f.transport.calls.Load()
				if catalogueMatched(f.service, 42, future) {
					t.Fatal("future date selected")
				}
				if _, ok := f.service.cachedDateEpisodeMatchAt(context.Background(), n, "2024-01-02", captured.ExpiresAt); ok {
					t.Fatal("original fixed expiry renewed")
				}
				if f.transport.calls.Load() != calls {
					t.Fatal("pure expiry check fetched")
				}
			case "old-disk-cache-and-imported-flags-unknown":
				f := newCatalogueFixture(t, nil)
				key := cacheKey("tvdb", "series", "episodes", "by-season-type", "v1", "42", "official", "eng")
				if err := f.service.cache.set(key, []tvdbEpisode{catalogueEpisode(42, 4201, 1, 1, "2024-01-02")}); err != nil {
					t.Fatal(err)
				}
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				if f.transport.calls.Load() != 0 || catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("legacy bare array promoted or getter warmed provider")
				}
				imported := models.SeriesDetails{Seasons: []models.SeriesSeason{{Number: 1, Episodes: []models.SeriesEpisode{{ID: "tvdb:episode:4201", EpisodeNumber: 1, AiredDate: "2024-01-02", SourceDateUnique: true}}}}}
				if err := f.service.cache.set(seriesDetailsCacheKey("eng", 42, ""), imported); err != nil {
					t.Fatal(err)
				}
				if catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("imported search hint created a provider observation")
				}
			case "getter-zero-http-file-and-no-renewal":
				f := newCatalogueFixture(t, nil)
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				original, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				if !ok {
					t.Fatal("missing original observation")
				}
				calls := f.transport.calls.Load()
				oldDirectory := f.service.cache.dir
				retired := filepath.Join(t.TempDir(), "retired-cache")
				if err := os.Rename(oldDirectory, retired); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 20; i++ {
					got, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
					if !ok || got != original {
						t.Fatal("getter changed original DATA or expiry")
					}
				}
				if _, err := os.Stat(oldDirectory); !os.IsNotExist(err) {
					t.Fatalf("getter accessed/recreated cache directory: %v", err)
				}
				if f.transport.calls.Load() != calls {
					t.Fatal("cache-only getter fetched")
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, ok := f.service.GetCachedDateEpisodeMatch(ctx, n, "2024-01-02"); ok {
					t.Fatal("cancelled getter returned match")
				}
			case "result-defensive-data-copy":
				f := newCatalogueFixture(t, nil)
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				original, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				if !ok {
					t.Fatal("missing actual DATA")
				}
				mutated := original
				mutated.Numbering.SeriesID = "tmdb:tv:1"
				mutated.EpisodeID = "caller-invented"
				mutated.ExpiresAt = time.Now().AddDate(1, 0, 0)
				rows, err := f.service.cachedSeriesEpisodesBySeasonTypeContext(context.Background(), 42, "official", "eng")
				if err != nil {
					t.Fatal(err)
				}
				rows[0].ID = 999
				rows[0].Translations[0].Name = "caller replacement"
				again, _ := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				second, err := f.service.cachedSeriesEpisodesBySeasonTypeContext(context.Background(), 42, "official", "eng")
				if err != nil || again != original || second[0].ID != 4201 || second[0].Translations[0].Name != "Synthetic title" || mutated == again {
					t.Fatal("caller mutated retained observation")
				}
			case "language-view-shares-original-observation":
				f := newCatalogueFixture(t, nil)
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				original, _ := f.service.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				french := f.service.WithLanguage("fr")
				calls := f.transport.calls.Load()
				result, ok := french.GetCachedDateEpisodeMatch(context.Background(), n, "2024-01-02")
				if !ok || result != original || french.episodeCatalogueLanguage() != "fra" || french.episodeDates != f.service.episodeDates || f.transport.calls.Load() != calls {
					t.Fatal("language view replaced/renewed genuine numbering observation")
				}
			case "config-rotation-retires-old-inflight":
				f := newCatalogueFixture(t, nil)
				entered := make(chan struct{}, 4)
				release := make(chan struct{})
				var once sync.Once
				f.transport.decorate = func(r *http.Request, body io.ReadCloser) io.ReadCloser {
					if strings.Contains(r.URL.Path, "/episodes/") {
						signal := make(chan struct{})
						wrapped := &catalogueHeldClose{ReadCloser: body, entered: signal, release: release}
						return &catalogueCloseWitness{catalogueHeldClose: wrapped, signal: entered}
					}
					return body
				}
				ctx, cancel := context.WithCancel(context.Background())
				var wg sync.WaitGroup
				errors := make(chan error, 4)
				t.Cleanup(func() { cancel(); once.Do(func() { close(release) }); wg.Wait() })
				for series := int64(42); series < 46; series++ {
					wg.Add(1)
					go func(id int64) { defer wg.Done(); errors <- f.fetch(ctx, id) }(series)
				}
				for i := 0; i < 4; i++ {
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("four real body Close calls did not enter")
					}
				}
				calls := f.transport.calls.Load()
				if err := f.fetch(ctx, 46); err == nil || f.transport.calls.Load() != calls || len(f.service.episodeDates.slots) != 4 {
					t.Fatal("fifth producer queued or exceeded cap")
				}
				f.service.UpdateAPIKeys("synthetic-new-key", "", "eng")
				if len(f.service.episodeDates.slots) != 4 || catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("rotation refunded unfinished old IO")
				}
				once.Do(func() { close(release) })
				wg.Wait()
				close(errors)
				for err := range errors {
					if err == nil {
						t.Fatal("retired original producer succeeded")
					}
				}
				if len(f.service.episodeDates.slots) != 0 {
					t.Fatal("actual finished producers leaked slots")
				}
				f.service.client.httpc = &http.Client{Transport: f.transport}
				f.service.client.minInterval = 0
				if err := f.fetch(context.Background(), 42); err != nil || !catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatalf("fresh genuine configuration could not produce: %v", err)
				}
			case "no-tmdb-name-or-filename-alias":
				f := newCatalogueFixture(t, nil)
				if err := f.fetch(context.Background(), 42); err != nil {
					t.Fatal(err)
				}
				for _, identity := range []string{"tmdb:tv:42", "42", "Synthetic programme", "Synthetic.2024.01.02.mkv", "tvdb:series:0042", "tvdb:series:-42"} {
					if _, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), models.EpisodeNumbering{SeriesID: identity, Ordering: "official"}, "2024-01-02"); ok {
						t.Fatalf("unobserved alias selected: %q", identity)
					}
				}
			case "bounded-whole-entry-eviction":
				f := newCatalogueFixture(t, nil)
				for series := int64(1); series <= 65; series++ {
					if err := f.fetch(context.Background(), series); err != nil {
						t.Fatal(err)
					}
				}
				c := f.service.episodeDates
				c.mu.RLock()
				entries, rows, bytes := len(c.entries), c.rows, c.bytes
				c.mu.RUnlock()
				if entries != 64 || rows != 64 || bytes > episodeCatalogueMaxBytes || catalogueMatched(f.service, 1, "2024-01-02") || !catalogueMatched(f.service, 65, "2024-01-02") {
					t.Fatal("bounded cache trimmed/adopted wrong entry")
				}
				if _, ok := f.service.GetCachedDateEpisodeMatch(context.Background(), models.EpisodeNumbering{SeriesID: "tvdb:series:65", Ordering: "official"}, "2024-01-02"); !ok {
					t.Fatal("newest whole entry missing")
				}
				// Exercise the owned-byte bound independently of the entry cap:
				// forty genuine one-row observations fit below 64 entries/32768
				// rows but exceed 32 MiB of retained decoded catalogue DATA.
				large := newCatalogueFixture(t, func(w http.ResponseWriter, r *http.Request) bool {
					if !strings.Contains(r.URL.Path, "/episodes/") {
						return false
					}
					parts := strings.Split(r.URL.Path, "/")
					id, _ := strconv.ParseInt(parts[3], 10, 64)
					row := catalogueEpisode(id, id*100+1, 1, 1, "2024-01-02")
					row.Overview = strings.Repeat("x", 900000)
					writeCataloguePage(w, []tvdbEpisode{row}, nil)
					return true
				})
				for series := int64(1); series <= 40; series++ {
					if err := large.fetch(context.Background(), series); err != nil {
						t.Fatal(err)
					}
				}
				large.service.episodeDates.mu.RLock()
				largeCount, largeRows, largeBytes := len(large.service.episodeDates.entries), large.service.episodeDates.rows, large.service.episodeDates.bytes
				large.service.episodeDates.mu.RUnlock()
				if largeCount >= 40 || largeCount == 0 || largeRows != largeCount || largeBytes > episodeCatalogueMaxBytes || catalogueMatched(large.service, 1, "2024-01-02") || !catalogueMatched(large.service, 40, "2024-01-02") {
					t.Fatal("actual byte-bound eviction lost whole-entry semantics")
				}
			case "legacy-api-and-current-search-hints-unchanged":
				f := newCatalogueFixture(t, nil)
				rows, err := f.service.client.seriesEpisodesBySeasonType(42, "official", "eng")
				if err != nil || len(rows) != 1 || rows[0].ID != 4201 {
					t.Fatalf("ordinary legacy API changed: %v", err)
				}
				if catalogueMatched(f.service, 42, "2024-01-02") {
					t.Fatal("legacy list promoted to genuine bounded observation")
				}
				episode := models.SeriesEpisode{SourceDateUnique: true, AiredDate: "2024-01-02"}
				if !episode.SourceDateUnique {
					t.Fatal("existing search-only flag changed")
				}
				// Search DATA is deliberately independent of the new cache. The
				// actual model still excludes that hint from its public wire form.
				data, err := json.Marshal(episode)
				if err != nil || strings.Contains(string(data), "SourceDateUnique") || strings.Contains(string(data), "sourceDateUnique") {
					t.Fatal("search-only hint leaked onto wire")
				}
			}
		})
	}
}

type catalogueCloseWitness struct {
	*catalogueHeldClose
	signal chan<- struct{}
}

func (b *catalogueCloseWitness) Close() error {
	err := b.ReadCloser.Close()
	b.signal <- struct{}{}
	<-b.release
	return err
}
