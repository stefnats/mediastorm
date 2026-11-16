package sourcefilters

import (
	"strings"
	"testing"
	"time"

	"novastream/models"
)

func evidenceFixture(now time.Time) *models.MediaEvidence {
	return &models.MediaEvidence{Version: 1, SourceVersion: "file_v1_" + strings.Repeat("a", 64), CandidateVersion: "candidate_v1_" + strings.Repeat("b", 64), Candidate: &models.MediaCandidateFacts{Source: "release_metadata", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Minute), AudioLanguages: []string{"deu"}, Resolution: "1080p", VideoCodec: "hevc", HDR: "hdr10"}, Probed: &models.MediaProbeFacts{Source: "ffprobe", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(2 * time.Minute), AudioStreams: []models.MediaAudioFacts{{Index: 1, Language: "deu", Codec: "ac3"}, {Index: 2, Language: "eng", Codec: "truehd"}}, AudioStreamsComplete: true, SubtitleStreams: []models.MediaSubtitleFacts{{Index: 3, Language: "fra"}}, SubtitleStreamsComplete: true, Video: models.MediaVideoFacts{Codec: "hevc", Height: 1080, HDR: "hdr10"}}}
}
func boolValue(v bool) *bool { return &v }
func intValue(v int) *int    { return &v }

func TestFilterBasisAndThreeValuedTrackConjunction(t *testing.T) {
	now := time.Now()
	target := Target{TitleID: "tmdb:movie:41", MediaType: "movie"}
	tests := []struct {
		name    string
		filters Filters
		change  func(*models.MediaEvidence)
		want    string
	}{
		{"no predicates", Filters{Basis: "candidate", Unknown: "include"}, nil, "unfiltered"},
		{"claimed language present", Filters{Basis: "candidate", Unknown: "include", AudioLanguage: "deu"}, nil, "match"},
		{"claimed absent is unknown", Filters{Basis: "candidate", Unknown: "include", AudioLanguage: "fra"}, nil, "unknown"},
		{"claimed height threshold", Filters{Basis: "candidate", Unknown: "include", ClaimedResolutionAtLeast: "2160p"}, nil, "no_match"},
		{"claimed codec mismatch", Filters{Basis: "candidate", Unknown: "include", VideoCodec: "av1"}, nil, "no_match"},
		{"candidate cannot borrow probed language", Filters{Basis: "candidate", Unknown: "include", AudioLanguage: "eng"}, nil, "unknown"},
		{"audio same track rejects combined different rows", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "deu", AudioCodec: "truehd"}, nil, "no_match"},
		{"audio same track known", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "eng", AudioCodec: "truehd"}, nil, "match"},
		{"incomplete track inventory cannot reject", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "fra"}, func(e *models.MediaEvidence) { e.Probed.AudioStreamsComplete = false }, "unknown"},
		{"missing language cannot reject", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "fra"}, func(e *models.MediaEvidence) { e.Probed.AudioStreams[0].Language = "" }, "unknown"},
		{"complete known absent audio", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "fra"}, nil, "no_match"},
		{"complete empty audio", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "fra"}, func(e *models.MediaEvidence) { e.Probed.AudioStreams = []models.MediaAudioFacts{} }, "no_match"},
		{"actual AD only", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "deu", AudioDescription: boolValue(true)}, nil, "unknown"},
		{"AD other track cannot satisfy language", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "deu", AudioDescription: boolValue(true)}, func(e *models.MediaEvidence) {
			e.Probed.AudioStreams[0].AudioDescription = boolValue(false)
			e.Probed.AudioStreams[1].AudioDescription = boolValue(true)
		}, "no_match"},
		{"same track AD", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "deu", AudioDescription: boolValue(true)}, func(e *models.MediaEvidence) { e.Probed.AudioStreams[0].AudioDescription = boolValue(true) }, "match"},
		{"subtitle complete absence", Filters{Basis: "probed", Unknown: "include", SubtitleLanguage: "eng"}, nil, "no_match"},
		{"subtitle incomplete absence", Filters{Basis: "probed", Unknown: "include", SubtitleLanguage: "eng"}, func(e *models.MediaEvidence) { e.Probed.SubtitleStreamsComplete = false }, "unknown"},
		{"measured height", Filters{Basis: "probed", Unknown: "include", HeightAtLeast: intValue(1080)}, nil, "match"},
		{"height unknown", Filters{Basis: "probed", Unknown: "include", HeightAtLeast: intValue(1080)}, func(e *models.MediaEvidence) { e.Probed.Video.Height = 0 }, "unknown"},
		{"SDR not guessed", Filters{Basis: "probed", Unknown: "include", DynamicRange: "sdr"}, func(e *models.MediaEvidence) { e.Probed.Video.HDR = "" }, "unknown"},
		{"expired probe never borrows release", Filters{Basis: "probed", Unknown: "include", AudioLanguage: "deu"}, func(e *models.MediaEvidence) { e.Probed.ExpiresAt = now }, "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.filters.Validate(); err != nil {
				t.Fatal(err)
			}
			e := evidenceFixture(now)
			if test.change != nil {
				test.change(e)
			}
			if got := Evaluate(test.filters, e, target, now); got != test.want {
				t.Fatalf("got %s want %s", got, test.want)
			}
		})
	}
}

func TestSeriesFileFactsRequireExactTrustedLeafAndNumbering(t *testing.T) {
	now := time.Now()
	ep := &models.EpisodeReference{EpisodeID: "tvdb:episode:123", SeasonNumber: 0, EpisodeNumber: 2, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}}
	target := Target{TitleID: "tmdb:series:41", MediaType: "series", Episode: ep}
	filters := Filters{Basis: "probed", Unknown: "include", VideoCodec: "hevc"}
	tests := []struct {
		name   string
		change func(*models.MediaEvidence)
		want   string
	}{
		{"exact", nil, "match"},
		{"source only", func(e *models.MediaEvidence) { e.TitleID = ""; e.TargetEpisode = nil }, "unknown"},
		{"same coordinates other leaf", func(e *models.MediaEvidence) { e.TargetEpisode.EpisodeID = "tvdb:episode:456" }, "unknown"},
		{"same leaf other coordinates", func(e *models.MediaEvidence) { e.TargetEpisode.EpisodeNumber = 3 }, "unknown"},
		{"other numbering owner", func(e *models.MediaEvidence) { e.TargetEpisode.Numbering.SeriesID = "tmdb:series:41" }, "unknown"},
		{"other ordering", func(e *models.MediaEvidence) { e.TargetEpisode.Numbering.Ordering = "dvd" }, "unknown"},
		{"other title", func(e *models.MediaEvidence) { e.TitleID = "tmdb:series:42" }, "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := evidenceFixture(now)
			e.TitleID = target.TitleID
			e.TargetEpisode = copyTarget(target).Episode
			if test.change != nil {
				test.change(e)
			}
			if got := Evaluate(filters, e, target, now); got != test.want {
				t.Fatalf("got %s want %s", got, test.want)
			}
		})
	}
}

func TestFilterValidationPreservesRareCanonicalLanguagesAndBasis(t *testing.T) {
	for _, lang := range []string{"nob", "nno", "nor", "fra", "deu", "cym", "ksh"} {
		f := Filters{Basis: "candidate", Unknown: "include", AudioLanguage: lang}
		if err := f.Validate(); err != nil {
			t.Fatalf("%s: %v", lang, err)
		}
	}
	for _, lang := range []string{"zzz", "und", "mul", "zxx", "ger", "de", "fr-CA"} {
		f := Filters{Basis: "candidate", Unknown: "include", AudioLanguage: lang}
		if f.Validate() == nil {
			t.Fatalf("accepted %s", lang)
		}
	}
	for _, f := range []Filters{{Basis: "candidate", Unknown: "include", AudioCodec: "ac3"}, {Basis: "candidate", Unknown: "include", AudioDescription: boolValue(true)}, {Basis: "probed", Unknown: "include", ClaimedResolutionAtLeast: "1080p"}, {Basis: "probed", Unknown: "include", AudioDescription: boolValue(false)}, {Basis: "candidate", Unknown: "include", ClaimedResolutionAtLeast: "01080p"}, {Basis: "probed", Unknown: "include", HeightAtLeast: intValue(10000)}} {
		if f.Validate() == nil {
			t.Fatalf("accepted %+v", f)
		}
	}
}
