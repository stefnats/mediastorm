package models

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestOriginalLanguageEvidenceRequiresConsistentProviderAnchors(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, kind := range []string{"movie", "series"} {
		for _, mode := range []string{"valid", "legacy alias", "verified opaque", "conflicting anchor", "conflicting external ID", "opposite kind", "person", "scalar only", "expired", "future", "unbounded"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				fact := &OriginalLanguageEvidence{Language: "fr", Source: "tmdb", SourceTitleID: "tmdb:" + kind + ":8", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour)}
				title := Title{ID: fact.SourceTitleID, MediaType: kind, TMDBID: 8, Language: "de", OriginalLanguageEvidence: fact}
				switch mode {
				case "legacy alias":
					if kind == "series" {
						title.ID = "tmdb:tv:8"
					}
				case "verified opaque":
					title.ID = "opaque:provider-anchor"
				case "conflicting anchor":
					title.ID = "tmdb:" + kind + ":9"
				case "conflicting external ID":
					title.TMDBID = 9
				case "opposite kind":
					if kind == "movie" {
						title.ID = "tmdb:series:8"
					} else {
						title.ID = "tvdb:movie:8"
					}
				case "person":
					title.MediaType = "person"
				case "scalar only":
					title.OriginalLanguageEvidence = nil
				case "expired":
					fact.ExpiresAt = now
				case "future":
					fact.ObservedAt = now.Add(time.Minute)
				case "unbounded":
					fact.ExpiresAt = now.Add(48 * time.Hour)
				}
				want := mode == "valid" || mode == "legacy alias" || mode == "verified opaque"
				got := CurrentOriginalLanguageEvidence(title, now)
				if (got != nil) != want {
					t.Fatalf("current=%+v want=%v", got, want)
				}
				if got != nil && (got.Language != "fr" || title.Language != "de" || !got.ObservedAt.Equal(fact.ObservedAt)) {
					t.Fatal("legacy locale or provenance was rewritten")
				}
			})
		}
	}
}

func TestManualMetadataCannotImportOriginalLanguageProof(t *testing.T) {
	title := &Title{ID: "tmdb:movie:8", MediaType: "movie", Language: "de", OriginalLanguageEvidence: &OriginalLanguageEvidence{Language: "fr"}, CertificationEvidence: []CertificationEvidence{{Value: "FSK 6"}}}
	clean := WithoutProviderEvidence(title)
	if clean.OriginalLanguageEvidence != nil || clean.CertificationEvidence != nil || clean.Language != "de" || title.OriginalLanguageEvidence == nil {
		t.Fatal("manual proof survived or input was mutated")
	}
	data, err := json.Marshal(Title{ID: "tmdb:movie:8", MediaType: "movie", Name: "Synthetic", Language: "de"})
	if err != nil || strings.Contains(string(data), "originalLanguageEvidence") {
		t.Fatal("legacy scalar locale was promoted into evidence")
	}
}

func TestCurrentMediaEvidenceRejectsMalformedAndUnboundedBlocks(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, mode := range []string{"valid", "expired", "future", "unbounded", "raw URL", "bad hash", "wrong domain", "clipped incomplete"} {
		t.Run(mode, func(t *testing.T) {
			value := &MediaEvidence{Version: 1, SourceVersion: "file_v1_" + strings.Repeat("a", 64), Probed: &MediaProbeFacts{Source: "ffprobe", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute), AudioStreams: []MediaAudioFacts{}, SubtitleStreams: []MediaSubtitleFacts{}}}
			switch mode {
			case "expired":
				value.Probed.ExpiresAt = now
			case "future":
				value.Probed.ObservedAt = now.Add(time.Minute)
			case "unbounded":
				value.Probed.ExpiresAt = now.Add(time.Hour)
			case "raw URL":
				value.SourceVersion = "https://private.invalid/file?token=synthetic"
			case "bad hash":
				value.SourceVersion = "file_v1_" + strings.Repeat("g", 64)
			case "wrong domain":
				value.SourceVersion = "candidate_v1_" + strings.Repeat("a", 64)
			case "clipped incomplete":
				value.Probed.AudioStreams = make([]MediaAudioFacts, MaxMediaEvidenceStreams)
				for i := range value.Probed.AudioStreams {
					value.Probed.AudioStreams[i].Index = i
				}
				value.Probed.AudioStreamsComplete = false
			}
			got := CurrentMediaEvidence(value, now)
			want := mode == "valid" || mode == "clipped incomplete"
			if (got != nil) != want {
				t.Fatalf("evidence=%+v want=%v", got, want)
			}
			if got != nil && !got.Probed.ObservedAt.Equal(value.Probed.ObservedAt) {
				t.Fatal("read renewed observation")
			}
		})
	}
}
