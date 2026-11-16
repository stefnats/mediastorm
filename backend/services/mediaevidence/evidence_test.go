package mediaevidence

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"novastream/models"
	"novastream/utils/filter"
)

func TestActualProbeDispositionIsDistinctFromTrackName(t *testing.T) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, mode := range []string{"missing", "other flag", "false", "true", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			flags := map[string]int{}
			switch mode {
			case "other flag":
				flags["default"] = 1
			case "false":
				flags["visual_impaired"] = 0
			case "true":
				flags["visual_impaired"] = 1
			case "invalid":
				flags["visual_impaired"] = -1
			}
			facts := ProbeFacts([]ProbeStream{{Index: 1, CodecType: "audio", CodecName: "aac", Tags: map[string]string{"language": "fr", "title": "Audio Description AD"}, Disposition: flags}}, at)
			got := facts.AudioStreams[0].AudioDescription
			if (got != nil) != (mode == "false" || mode == "true") || got != nil && *got != (mode == "true") {
				t.Fatalf("AD=%v for %s", got, mode)
			}
		})
	}
}

func TestRawSubtitleInventoryAndBoundsRemainHonest(t *testing.T) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	streams := []ProbeStream{{Index: 0, CodecType: "video", CodecName: "h264", Width: 1920, Height: 1080}, {Index: 1, CodecType: "subtitle", CodecName: "future_codec", Tags: map[string]string{"language": "cym"}}}
	facts := ProbeFacts(streams, at)
	if len(facts.SubtitleStreams) != 1 || facts.SubtitleStreams[0].Codec != "future_codec" || !facts.SubtitleStreamsComplete || facts.Video.HDR != "" {
		t.Fatalf("unknown codec/absence became false proof: %+v", facts)
	}
	for i := 2; i < 38; i++ {
		streams = append(streams, ProbeStream{Index: i, CodecType: "subtitle", CodecName: "subrip"})
	}
	facts = ProbeFacts(streams, at)
	if len(facts.SubtitleStreams) != 32 || facts.SubtitleStreamsComplete {
		t.Fatal("clipped list proved total inventory")
	}
	facts = ProbeFacts([]ProbeStream{{Index: 1, CodecType: "audio", Tags: map[string]string{"language": "fr"}}, {Index: 1, CodecType: "audio", Tags: map[string]string{"language": "en"}}}, at)
	if len(facts.AudioStreams) != 0 || facts.AudioStreamsComplete {
		t.Fatal("duplicate index selected one contradictory language")
	}
}

func TestObservedHDRRequiresActualSignal(t *testing.T) {
	for _, tc := range []struct{ transfer, primaries, expected string }{{"", "", ""}, {"unknown", "bt709", ""}, {"bt709", "bt709", "sdr"}, {"smpte2084", "bt2020", "hdr10"}, {"smpte2084", "", ""}, {"arib-std-b67", "bt2020", "hlg"}} {
		facts := ProbeFacts([]ProbeStream{{Index: 0, CodecType: "video", CodecName: "hevc", ColorTransfer: tc.transfer, ColorPrimaries: tc.primaries}}, time.Now())
		if facts.Video.HDR != tc.expected {
			t.Fatalf("%s/%s gave %s", tc.transfer, tc.primaries, facts.Video.HDR)
		}
	}
}

func TestRareLanguageTagsAreNotLimitedByDisplayNameTable(t *testing.T) {
	got := NormalizeLanguages("isl,cat,cym,yue,fr-CA,und,mul,zxx,nonsense")
	want := []string{"isl", "cat", "cym", "yue", "fra"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}

func TestReleaseAssociationSurvivesNormalResolverURLMutation(t *testing.T) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, guid := range []string{"stable-provider-guid", ""} {
		result := models.NZBResult{GUID: guid, Indexer: "Synthetic", Link: "https://private.invalid/original?token=synthetic", Attributes: map[string]string{"languages": "fra", "resolution": "1080p"}}
		ObserveCandidate(&result, at)
		candidate := Candidate(result, at)
		version := candidate.SourceVersion
		result.Link = "https://private.invalid/resolved?token=rotated"
		result.DownloadURL = result.Link
		if CandidateVersion(result) != version {
			t.Fatal("resolver URL mutation broke originating release association")
		}
		facts := ProbeFacts([]ProbeStream{{Index: 0, CodecType: "video", CodecName: "h264", Width: 3840, Height: 2160}, {Index: 1, CodecType: "audio", Tags: map[string]string{"language": "en"}}}, at)
		value := Probed(result, "/synthetic/selected-file.mkv", "part-1", 100, facts, "", nil, at)
		if value.SourceVersion == version || value.CandidateVersion != version || value.Probed.AudioStreams[0].Language != "eng" || value.Candidate.AudioLanguages[0] != "fra" {
			t.Fatal("candidate claim and actual file metadata were combined")
		}
		if strings.Contains(value.SourceVersion, "private") || strings.Contains(value.CandidateVersion, "synthetic") {
			t.Fatal("private identity escaped hashing")
		}
		if Candidate(result, at.Add(16*time.Minute)) != nil {
			t.Fatal("cache read renewed expired candidate")
		}
		if !result.EvidenceObservedAt.Equal(at) {
			t.Fatal("projection changed original observation time")
		}
	}
}

func TestProbeInputBindingNormalizesOnlyEquivalentInputs(t *testing.T) {
	for _, pair := range [][2]string{{"/webdav/folder/a.mkv", "/folder/a.mkv"}, {"webdav/folder/a.mkv", "/folder/a.mkv"}, {"https://example.invalid/a b.mkv?q=a b", "https://example.invalid/a%20b.mkv?q=a%20b"}, {"localmedia:item/a.mkv", "localmedia:item/a.mkv"}, {"plexmedia:item", "plexmedia:item"}, {"jellyfinmedia:item", "jellyfinmedia:item"}} {
		if InputVersion(pair[0]) != InputVersion(pair[1]) {
			t.Fatalf("equivalent logical inputs differ: %q %q", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{{"localmedia:item/a.mkv", "localmedia:item/b.mkv"}, {"plexmedia:part-1", "plexmedia:part-2"}, {"https://example.invalid/file?token=a", "https://example.invalid/file?token=b"}} {
		if InputVersion(pair[0]) == InputVersion(pair[1]) {
			t.Fatal("different input accepted")
		}
	}
}

func TestCandidateUsesCanonicalNormalReleaseParserAttributes(t *testing.T) {
	at := time.Now()
	results := filter.ResultsWithDetails([]models.NZBResult{{Title: "Synthetic.Movie.2024.1080p.WEB-DL.H265.AAC", GUID: "parsed-release"}}, filter.Options{ExpectedTitle: "Synthetic Movie", IsMovie: true})
	if len(results) != 1 {
		t.Fatal("normal release parser returned no result")
	}
	result := results[0].Result
	if result.Attributes["codec"] == "" || result.Attributes["audio"] == "" {
		t.Fatalf("normal parser keys absent: %+v", result.Attributes)
	}
	ObserveCandidate(&result, at)
	facts := Candidate(result, at).Candidate
	if facts.VideoCodec != "hevc" || len(facts.AudioCodecs) != 1 || facts.AudioCodecs[0] != "aac" {
		t.Fatalf("canonical codec/audio keys ignored: %+v attrs=%v", facts, result.Attributes)
	}
	result.Attributes["audio"] = "Atmos,unrecognized-container-name"
	if len(Candidate(result, at).Candidate.AudioCodecs) != 0 {
		t.Fatal("unknown audio label invented a codec")
	}
}
