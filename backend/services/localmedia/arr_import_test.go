package localmedia

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/arrclient"
)

func arrTestLocalVideo(t *testing.T, service *Service, name string) {
	t.Helper()
	ffmpeg, e := exec.LookPath("ffmpeg")
	if e != nil {
		t.Fatal("synthetic media fixture needs ffmpeg", e)
	}
	probe, e := exec.LookPath("ffprobe")
	if e != nil {
		t.Fatal(e)
	}
	service.ffprobePath = probe
	raw, e := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=blue:s=64x64:r=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.5", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-c:a", "aac", "-f", "matroska", name).CombinedOutput()
	if e != nil {
		t.Fatalf("create synthetic local file: %v %s", e, raw)
	}

}

func TestARRImportedFileBecomesNormalLocalVersion(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, e := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if e != nil {
		t.Fatal(e)
	}
	name := filepath.Join(library.RootPath, "Synthetic Movie (2024).mkv")
	arrTestLocalVideo(t, f.service, name)
	if e = f.service.ValidateARRLibrary(t.Context(), "catalogue", "radarr"); e != nil {
		t.Fatal(e)
	}
	result, e := f.service.ImportARRFiles(t.Context(), "catalogue", "/radarr/library", []arrclient.ImportedFile{{Path: "/radarr/library/Synthetic Movie (2024).mkv", Title: "Synthetic Movie", Year: 2024, MediaType: "movie", TMDBID: 303}})
	if e != nil || result.Missing || result.NeedsMatch || len(result.ItemIDs) != 1 {
		t.Fatal("ARR file did not finish in local library", result, e)
	}
	item, e := f.service.GetItem(t.Context(), result.ItemIDs[0])
	if e != nil || item.FilePath != name || item.Metadata == nil || item.Metadata.TMDBID != 303 || item.Probe == nil || item.Probe.VideoCodec != "h264" {
		t.Fatal("imported version lost actual file/identity/probe", item, e)
	}
	matches, e := f.service.FindMatches(t.Context(), models.LocalMediaMatchQuery{MediaType: "movie", TitleID: "tmdb:movie:303", TMDBID: "303"})
	if e != nil || len(matches) != 1 || len(matches[0].Group.Items) != 1 || matches[0].Group.Items[0].ID != item.ID {
		t.Fatal("existing playback local lookup cannot find imported version", matches, e)
	}
	// Rechecking does not create a second catalogue item for the same file.
	second, e := f.service.ImportARRFiles(t.Context(), "catalogue", "/radarr/library", []arrclient.ImportedFile{{Path: "/radarr/library/Synthetic Movie (2024).mkv", Title: "Synthetic Movie", MediaType: "movie", TMDBID: 303}})
	if e != nil || len(second.ItemIDs) != 1 || second.ItemIDs[0] != item.ID {
		t.Fatal("repeat import duplicated local version", second, e)
	}
}
func TestARRMissingFileAndUnmatchedTitleRemainVisible(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, e := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if e != nil {
		t.Fatal(e)
	}
	missing, e := f.service.ImportARRFiles(t.Context(), "catalogue", "", []arrclient.ImportedFile{{Path: filepath.Join(library.RootPath, "absent.mkv")}})
	if e != nil || !missing.Missing || len(missing.ItemIDs) != 0 {
		t.Fatal("missing file was advertised as available", missing, e)
	}
	name := filepath.Join(library.RootPath, "Unidentified.mkv")
	arrTestLocalVideo(t, f.service, name)
	unmatched, e := f.service.ImportARRFiles(t.Context(), "catalogue", "", []arrclient.ImportedFile{{Path: name, MediaType: "movie"}})
	if e != nil || !unmatched.NeedsMatch || len(unmatched.ItemIDs) != 1 {
		t.Fatal("missing title needs an actionable library item", unmatched, e)
	}
	if e = f.service.ValidateARRLibrary(t.Context(), "catalogue", "sonarr"); e == nil {
		t.Fatal("series import accepted a movie library")
	}
}

func TestARRCorrectsAutomaticIdentityAndPreservesManualSelection(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, e := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if e != nil {
		t.Fatal(e)
	}
	library.Type = models.LocalMediaLibraryTypeShow
	if e = f.store.LocalMedia().UpdateLibrary(t.Context(), library); e != nil {
		t.Fatal(e)
	}
	name := filepath.Join(library.RootPath, "Wrong.Series.S09E09.mkv")
	arrTestLocalVideo(t, f.service, name)
	if _, e = f.service.RunScan(t.Context(), library.ID); e != nil {
		t.Fatal(e)
	}
	items, e := f.store.LocalMedia().ListAllItemsByLibrary(t.Context(), library.ID)
	if e != nil {
		t.Fatal(e)
	}
	var item *models.LocalMediaItem
	for i := range items {
		if items[i].FilePath == name {
			item = &items[i]
			break
		}
	}
	if item == nil {
		t.Fatal("scanned file absent")
	}
	item.MatchStatus = models.LocalMediaMatchStatusMatched
	item.MatchedTitleID = "tvdb:series:111"
	item.MatchedMediaType = "series"
	item.Metadata = &models.Title{ID: item.MatchedTitleID, MediaType: "series", TVDBID: 111}
	item.SeasonNumber, item.EpisodeNumber = 9, 9
	if e = f.store.LocalMedia().UpsertItem(t.Context(), item); e != nil {
		t.Fatal(e)
	}
	files := []arrclient.ImportedFile{{Path: name, Title: "Correct Series", MediaType: "series", TVDBID: 505, SeasonNumber: 1, EpisodeNumber: 2}}
	result, e := f.service.ImportARRFiles(t.Context(), library.ID, "", files)
	if e != nil || result.NeedsMatch || result.InvalidMedia {
		t.Fatal(result, e)
	}
	current, e := f.service.GetItem(t.Context(), item.ID)
	if e != nil || current.MatchedTitleID != "tvdb:series:505" || current.Metadata.TVDBID != 505 || current.SeasonNumber != 1 || current.EpisodeNumber != 2 {
		t.Fatal("definitive imported identity did not correct automatic filename match", current, e)
	}
	// Same TVDB identity, initially without the secondary IDs. ARR supplies
	// those IDs while the existing artwork/details must survive enrichment.
	current.Metadata.Overview = "Keep existing series details"
	current.Metadata.Poster = &models.Image{URL: "/synthetic-existing-poster.jpg", Type: "poster"}
	current.Metadata.MediaType = ""
	if e = f.store.LocalMedia().UpsertItem(t.Context(), current); e != nil {
		t.Fatal(e)
	}
	files[0].TMDBID = 303
	files[0].IMDBID = "tt1234567"
	result, e = f.service.ImportARRFiles(t.Context(), library.ID, "", files)
	if e != nil || result.NeedsMatch || result.InvalidMedia {
		t.Fatal(result, e)
	}
	current, e = f.service.GetItem(t.Context(), item.ID)
	if e != nil || current.Metadata.TMDBID != 303 || current.Metadata.TVDBID != 505 || current.Metadata.IMDBID != "tt1234567" || current.Metadata.MediaType != "series" || current.Metadata.Overview != "Keep existing series details" || current.Metadata.Poster == nil || current.Metadata.Poster.URL != "/synthetic-existing-poster.jpg" {
		t.Fatal("same-identity enrichment lost added IDs or existing details", current, e)
	}
	matches, e := f.service.FindMatches(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TMDBID: "303"})
	if e != nil || len(matches) != 1 || matches[0].LibraryID != library.ID || matches[0].Group.ItemCount != 1 {
		t.Fatal("TMDB-only local lookup cannot find the enriched TVDB series", matches, e)
	}
	current.MatchStatus = models.LocalMediaMatchStatusManual
	current.MatchedTitleID = "tvdb:series:777"
	current.Metadata = &models.Title{ID: current.MatchedTitleID, MediaType: "series", TVDBID: 777}
	current.SeasonNumber, current.EpisodeNumber = 4, 5
	if e = f.store.LocalMedia().UpsertItem(t.Context(), current); e != nil {
		t.Fatal(e)
	}
	// Force the scanner's changed-file branch, not just its reuse branch.
	changedAt := time.Now().Add(2 * time.Second)
	if e = os.Chtimes(name, changedAt, changedAt); e != nil {
		t.Fatal(e)
	}
	result, e = f.service.ImportARRFiles(t.Context(), library.ID, "", files)
	if e != nil || !result.NeedsMatch || result.Reason != "manual_match_conflict" {
		t.Fatal("manual conflict was not actionable", result, e)
	}
	current, e = f.service.GetItem(t.Context(), item.ID)
	if e != nil || current.MatchedTitleID != "tvdb:series:777" || current.Metadata.TVDBID != 777 || current.SeasonNumber != 4 || current.EpisodeNumber != 5 || current.MatchStatus != models.LocalMediaMatchStatusManual {
		t.Fatal("manual selection overwritten", current, e)
	}
}
func TestARRDoubleEpisodeRequiresManualReview(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, e := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if e != nil {
		t.Fatal(e)
	}
	library.Type = models.LocalMediaLibraryTypeShow
	if e = f.store.LocalMedia().UpdateLibrary(t.Context(), library); e != nil {
		t.Fatal(e)
	}
	name := filepath.Join(library.RootPath, "Synthetic.Series.S01E01E02.mkv")
	arrTestLocalVideo(t, f.service, name)
	result, e := f.service.ImportARRFiles(t.Context(), library.ID, "", []arrclient.ImportedFile{{Path: name, MediaType: "series", TVDBID: 505, SeasonNumber: 1, EpisodeNumber: 1, AmbiguousEpisodes: true}})
	if e != nil || !result.NeedsMatch || result.Reason != "multiple_episodes" || len(result.ItemIDs) != 1 {
		t.Fatal("double episode reported complete", result, e)
	}
	item, e := f.service.GetItem(t.Context(), result.ItemIDs[0])
	if e != nil || item.MatchStatus != models.LocalMediaMatchStatusUnmatched || item.SeasonNumber != 0 || item.EpisodeNumber != 0 {
		t.Fatal("single episode invented for a double-episode file", item, e)
	}
	matches, e := f.service.FindMatches(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TVDBID: "505"})
	if e != nil || len(matches) != 0 {
		t.Fatal("ambiguous episode entered automatic local lookup", matches, e)
	}
}
func TestARRBrokenContainerIsNotAvailable(t *testing.T) {
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, e := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if e != nil {
		t.Fatal(e)
	}
	probe, e := exec.LookPath("ffprobe")
	if e != nil {
		t.Fatal(e)
	}
	f.service.ffprobePath = probe
	name := filepath.Join(library.RootPath, "Broken.Movie.mkv")
	if e = os.WriteFile(name, []byte("this is not a video container"), 0600); e != nil {
		t.Fatal(e)
	}
	result, e := f.service.ImportARRFiles(t.Context(), library.ID, "", []arrclient.ImportedFile{{Path: name, Title: "Broken Movie", MediaType: "movie", TMDBID: 303}})
	if e != nil || !result.InvalidMedia || len(result.ItemIDs) != 1 {
		t.Fatal("size-only probe advertised as video", result, e)
	}
}
