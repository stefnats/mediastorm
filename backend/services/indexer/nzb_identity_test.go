package indexer

import (
	"context"
	"fmt"
	"net/http"
	"novastream/models"
	"sync/atomic"
	"testing"
	"time"
)

func TestNZBIdentitySurvivesQueryVariantsAndSharedResponses(t *testing.T) {
	var hits atomic.Int32
	svc, idx := dedupTestService(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		fmt.Fprint(w, dedupFeed)
	})
	opts := withNZBIdentity(SearchOptions{TitleID: "tmdb:tv:42", MediaType: "series", Query: "Example S02E03"})
	opts.Query = "Localized Example S02" // season-pack fallback, same selected episode
	first, err := svc.searchTorznab(t.Context(), idx, opts)
	if err != nil || len(first) != 1 {
		t.Fatalf("first search: %v %v", first, err)
	}
	if key, err := first[0].NZBIdentity().Key(); err != nil || key != "tmdb:tv:42:s2e3" {
		t.Fatalf("key=%q err=%v", key, err)
	}
	other := opts
	other.acquisitionIdentity.CatalogID = "tmdb:tv:43"
	second, err := svc.searchTorznab(t.Context(), idx, other)
	if err != nil || len(second) != 1 {
		t.Fatalf("second search: %v %v", second, err)
	}
	if key, err := second[0].NZBIdentity().Key(); err != nil || key != "tmdb:tv:43:s2e3" {
		t.Fatalf("cached key=%q err=%v", key, err)
	}
	if hits.Load() != 1 || first[0].Attributes["nzbCatalogId"] != "tmdb:tv:42" {
		t.Fatal("shared search mutated caller identity or repeated the query")
	}
}

func TestNZBIdentityUsesTheCatalogOwningEpisodeCoordinates(t *testing.T) {
	opts := withNZBIdentity(SearchOptions{
		TitleID: "tmdb:tv:42", MediaType: "series", Query: "Example S02E03",
		Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:99", Ordering: "dvd"},
	})
	key, err := opts.acquisitionIdentity.Key()
	if err != nil || key != "tvdb:series:99:s2e3:ordering:dvd" {
		t.Fatalf("key=%q err=%v", key, err)
	}
}

func TestNZBIdentityKeepsJoinedSourceCacheIndependent(t *testing.T) {
	svc, idx := dedupTestService(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, dedupFeed) })
	opts := withNZBIdentity(SearchOptions{TitleID: "tmdb:tv:42", MediaType: "series", Query: "Example S02E03"})
	ctx := context.WithValue(t.Context(), joinedIndexerSourceKey{}, joinedIndexerSource{})
	first, err := svc.searchTorznab(ctx, idx, opts)
	if err != nil || len(first) != 1 {
		t.Fatalf("joined source=%v err=%v", first, err)
	}
	cached, ok := svc.indexerCalls.recent(indexerCallKey(idx, opts), time.Now())
	if !ok || len(cached) != 1 || cached[0].Attributes["nzbCatalogId"] != "" {
		t.Fatal("request identity mutated shared provider cache")
	}
	if key, err := first[0].NZBIdentity().Key(); err != nil || key != "tmdb:tv:42:s2e3" {
		t.Fatalf("first identity=%q err=%v", key, err)
	}
	other := opts
	other.acquisitionIdentity.CatalogID = "tmdb:tv:43"
	second, err := svc.searchTorznab(ctx, idx, other)
	if err != nil || len(second) != 1 {
		t.Fatalf("cached source=%v err=%v", second, err)
	}
	if key, err := second[0].NZBIdentity().Key(); err != nil || key != "tmdb:tv:43:s2e3" {
		t.Fatalf("second identity=%q err=%v", key, err)
	}
	if first[0].Attributes["nzbCatalogId"] != "tmdb:tv:42" || cached[0].Attributes["nzbCatalogId"] != "" {
		t.Fatal("cache reuse changed a prior caller or shared result")
	}
}
