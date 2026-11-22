package localmedia

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"novastream/models"
)

func TestLocalMediaStoredFilterFacts(t *testing.T) {
	p := &models.LocalMediaProbe{VideoCodec: "hevc", Height: 2160, HDRFormat: "hdr10", TracksScanned: true, AudioStreams: 2, SubtitleStreams: 1,
		AudioTracks:    []models.MediaAudioFacts{{Language: "fra", Codec: "ac3"}, {Language: "eng", Codec: "aac"}},
		SubtitleTracks: []models.MediaSubtitleFacts{{Language: "spa", Codec: "subrip"}}}
	for _, tc := range []struct {
		name   string
		probe  *models.LocalMediaProbe
		filter models.LocalMediaFilters
		match  bool
	}{
		{"same-track", p, models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "ac3", Unknown: "exclude"}, true},
		{"different-tracks", p, models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", Unknown: "exclude"}, false},
		{"video-height-hdr", p, models.LocalMediaFilters{VideoCodec: "hevc", MinHeight: 2160, HDR: "hdr10", Unknown: "exclude"}, true},
		{"subtitle-language", p, models.LocalMediaFilters{SubtitleLanguage: "spa", Subtitles: "present", Unknown: "exclude"}, true},
		{"known-language-absent", p, models.LocalMediaFilters{SubtitleLanguage: "fra", Unknown: "include"}, false},
		{"unscanned-included", nil, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "include"}, true},
		{"unscanned-not-known", nil, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "exclude"}, false},
		{"unscanned-only", nil, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "only"}, true},
		{"known-not-unknown", p, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "only"}, false},
		{"legacy-language-unknown", &models.LocalMediaProbe{AudioCodecs: []string{"aac"}}, models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "only"}, true},
		{"legacy-missing-codec-unknown", &models.LocalMediaProbe{AudioCodecs: []string{"aac"}, AudioStreams: 2}, models.LocalMediaFilters{AudioCodec: "dts", Unknown: "only"}, true},
		{"legacy-codec-contradiction-include", &models.LocalMediaProbe{AudioCodecs: []string{"ac3"}}, models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", Unknown: "include"}, false},
		{"legacy-codec-contradiction-only", &models.LocalMediaProbe{AudioCodecs: []string{"ac3"}}, models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", Unknown: "only"}, false},
		{"legacy-codec-known", &models.LocalMediaProbe{AudioCodecs: []string{"aac"}}, models.LocalMediaFilters{AudioCodec: "aac", Unknown: "exclude"}, true},
		{"legacy-zero-not-known-absent", &models.LocalMediaProbe{}, models.LocalMediaFilters{Subtitles: "absent", Unknown: "exclude"}, false},
		{"scanned-zero-known-absent", &models.LocalMediaProbe{TracksScanned: true}, models.LocalMediaFilters{Subtitles: "absent", Unknown: "exclude"}, true},
		{"missing-hdr-not-sdr", &models.LocalMediaProbe{VideoCodec: "h264"}, models.LocalMediaFilters{HDR: "sdr", Unknown: "exclude"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateMediaFilters(tc.filter); err != nil {
				t.Fatal(err)
			}
			if got := localMediaMatchesFilters(tc.probe, tc.filter); got != tc.match {
				t.Fatalf("match=%v want=%v", got, tc.match)
			}
		})
	}
}

func TestLocalMediaFiltersWholeIndexBeforePagination(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	rows, err := f.store.LocalMedia().ListAllItemsByLibrary(t.Context(), "catalogue")
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		var index int
		if _, err := fmt.Sscanf(rows[i].ID, "local-%02d", &index); err != nil {
			t.Fatal(err)
		}
		lang := "eng"
		if index%4 == 0 {
			lang = "fra"
		}
		rows[i].Probe = &models.LocalMediaProbe{TracksScanned: true, AudioTracks: []models.MediaAudioFacts{{Language: lang, Codec: "aac"}}}
		if err := f.store.LocalMedia().UpsertItem(t.Context(), &rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	q := models.LocalMediaItemListQuery{Sort: "name", Dir: "asc", Limit: 2, MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", Unknown: "exclude"}}
	for _, tc := range []struct {
		offset int
		ids    []string
	}{{0, []string{"local-04", "local-08"}}, {2, []string{"local-12", "local-16"}}, {99, []string{}}} {
		q.Offset = tc.offset
		page, err := f.service.ListItems(t.Context(), "catalogue", q)
		if err != nil || page.Total != 4 || page.Offset != tc.offset || len(page.Items) != len(tc.ids) {
			t.Fatalf("page=%+v error=%v", page, err)
		}
		for i := range tc.ids {
			if page.Items[i].ID != tc.ids[i] {
				t.Fatalf("got %s want %s", page.Items[i].ID, tc.ids[i])
			}
		}
	}
}

func TestLocalMediaFilterGroupsRequireOneMatchingVersion(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	f.service.metadata = &languagePoisonMetadata{}
	f.service.ffprobePath = "/must-not-run/ffprobe"
	rows, err := f.store.LocalMedia().ListAllItemsByLibrary(t.Context(), "catalogue")
	if err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		if err := f.store.LocalMedia().DeleteItem(t.Context(), rows[i].ID); err != nil {
			t.Fatal(err)
		}
	}
	for n, p := range []*models.LocalMediaProbe{
		{VideoCodec: "hevc", TracksScanned: true, AudioTracks: []models.MediaAudioFacts{{Language: "fra", Codec: "ac3"}}},
		{VideoCodec: "h264", TracksScanned: true, AudioTracks: []models.MediaAudioFacts{{Language: "eng", Codec: "aac"}}},
		{VideoCodec: "hevc", TracksScanned: true, AudioTracks: []models.MediaAudioFacts{{Language: "fra", Codec: "aac"}}},
		{VideoCodec: "h264", TracksScanned: true, AudioTracks: []models.MediaAudioFacts{{Language: "eng", Codec: "aac"}}},
	} {
		id := fmt.Sprintf("version-%d", n)
		row := &models.LocalMediaItem{ID: id, LibraryID: "catalogue", RelativePath: id + ".mkv", FilePath: "/not-opened/" + id, FileName: id + ".mkv", LibraryType: models.LocalMediaLibraryTypeMovie,
			MatchedTitleID: fmt.Sprintf("tmdb:movie:%d", n/2+1), MatchedName: fmt.Sprintf("Title %d", n/2+1), MatchStatus: models.LocalMediaMatchStatusMatched, Probe: p, SizeBytes: 1024}
		if err := f.store.LocalMedia().UpsertItem(t.Context(), row); err != nil {
			t.Fatal(err)
		}
	}
	q := models.LocalMediaItemListQuery{Limit: 1, Sort: "name", Dir: "asc", MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", VideoCodec: "hevc", Unknown: "exclude"}}
	page, err := f.service.ListGroups(t.Context(), "catalogue", q)
	if err != nil || page.Total != 1 || len(page.Groups) != 1 || page.Groups[0].ItemCount != 1 || len(page.Groups[0].Items) != 1 || page.Groups[0].Items[0].ID != "version-2" {
		t.Fatalf("versions combined or wrong count: %+v %v", page, err)
	}
	q.Offset = 1
	page, err = f.service.ListGroups(t.Context(), "catalogue", q)
	if err != nil || page.Total != 1 || len(page.Groups) != 0 {
		t.Fatalf("tail count: %+v %v", page, err)
	}
}

func TestLocalMediaRegularScanStoresTrackLanguages(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, err := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if err != nil {
		t.Fatal(err)
	}
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Fatal(err)
	}
	f.service.ffprobePath = ffprobe
	sub := filepath.Join(t.TempDir(), "subtitle.srt")
	if err := os.WriteFile(sub, []byte("1\n00:00:00,000 --> 00:00:01,000\nSynthetic caption\n"), 0600); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(library.RootPath, "Synthetic multilingual.mkv")
	cmd := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=blue:s=64x64:r=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-f", "lavfi", "-i", "sine=frequency=880:sample_rate=48000", "-i", sub, "-map", "0:v", "-map", "1:a", "-map", "2:a", "-map", "3:s", "-t", "1", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-c:a", "aac", "-c:s", "srt", "-metadata:s:a:0", "language=fra", "-metadata:s:a:1", "language=spa", "-metadata:s:s:0", "language=fra", name)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("synthetic media: %v %s", err, raw)
	}
	if _, err := f.service.RunScan(t.Context(), "catalogue"); err != nil {
		t.Fatal(err)
	}
	rows, err := f.store.LocalMedia().ListAllItemsByLibrary(t.Context(), "catalogue")
	if err != nil || len(rows) != 1 {
		t.Fatalf("scan: %+v %v", rows, err)
	}
	p := rows[0].Probe
	if p == nil || !p.TracksScanned || len(p.AudioTracks) != 2 || len(p.SubtitleTracks) != 1 || p.AudioTracks[0].Language != "fra" || p.AudioTracks[1].Language != "spa" || p.SubtitleTracks[0].Language != "fra" || p.VideoCodec != "h264" {
		t.Fatalf("stored scan facts: %+v", p)
	}
	// The following lists must use stored facts even when probing cannot run
	// and the file is no longer at its scanned path. This is not a live check.
	f.service.ffprobePath = "/must-not-run/ffprobe"
	if err := os.Rename(name, name+".moved"); err != nil {
		t.Fatal(err)
	}
	q := models.LocalMediaItemListQuery{MediaFilters: models.LocalMediaFilters{AudioLanguage: "fra", AudioCodec: "aac", SubtitleLanguage: "fra", VideoCodec: "h264", Unknown: "exclude"}}
	page, err := f.service.ListItems(t.Context(), "catalogue", q)
	if err != nil || page.Total != 1 || len(page.Items) != 1 || !strings.Contains(page.Items[0].FileName, "multilingual") {
		t.Fatalf("stored list: %+v %v", page, err)
	}
}
