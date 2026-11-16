package indexer

import (
	"fmt"
	"net/http"
	"novastream/services/mediaevidence"
	"sync/atomic"
	"testing"
	"time"
)

func TestActualSearchCandidateEvidenceDoesNotRenewOnCachedCopy(t *testing.T) {
	var calls atomic.Int32
	service, indexer := dedupTestService(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); fmt.Fprint(w, dedupFeed) })
	options := SearchOptions{Query: "Some Movie 2024", MediaType: "movie"}
	first, err := service.searchTorznab(t.Context(), indexer, options)
	if err != nil || len(first) != 1 {
		t.Fatalf("search: %v", err)
	}
	proof := mediaevidence.Candidate(first[0], time.Now())
	if proof == nil || first[0].EvidenceObservedAt.IsZero() {
		t.Fatal("actual provider observation missing")
	}
	again, err := service.searchTorznab(t.Context(), indexer, options)
	if err != nil || len(again) != 1 || calls.Load() != 1 || !again[0].EvidenceObservedAt.Equal(first[0].EvidenceObservedAt) || !again[0].EvidenceExpiresAt.Equal(first[0].EvidenceExpiresAt) || mediaevidence.CandidateVersion(again[0]) != proof.SourceVersion {
		t.Fatal("search cache refreshed provenance or made extra request")
	}
	if mediaevidence.Candidate(again[0], first[0].EvidenceExpiresAt) != nil {
		t.Fatal("cached result became a fresh candidate")
	}
}
