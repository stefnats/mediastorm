package localmedia

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"novastream/models"
)

func legacyMediaInfoFixture(t *testing.T) (*catalogueCountsPGFixture, *models.LocalMediaItem, string) {
	t.Helper()
	f := newCatalogueCountsPGFixture(t)
	t.Cleanup(func() { _ = f.service.Close() })
	library, err := f.store.LocalMedia().GetLibrary(t.Context(), "catalogue")
	if err != nil {
		t.Fatal(err)
	}
	library.Type = models.LocalMediaLibraryTypeShow
	if err = f.store.LocalMedia().UpdateLibrary(t.Context(), library); err != nil {
		t.Fatal(err)
	}
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Fatal(err)
	}
	probe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Fatal(err)
	}
	f.service.ffprobePath = probe
	file := filepath.Join(library.RootPath, "Synthetic.Show.S01E02.mkv")
	cmd := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=blue:s=64x64:r=10", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.5", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-c:a", "aac", "-metadata:s:a:0", "language=fra", file)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("synthetic file: %v %s", err, output)
	}
	if _, err := f.service.RunScan(t.Context(), library.ID); err != nil {
		t.Fatal(err)
	}
	items, err := f.store.LocalMedia().ListAllItemsByLibrary(t.Context(), library.ID)
	if err != nil || len(items) != 1 {
		t.Fatalf("initial scan: %+v %v", items, err)
	}
	item := items[0]
	item.MatchStatus = models.LocalMediaMatchStatusManual
	item.MatchedTitleID = "tvdb:series:505"
	item.MatchedMediaType = "series"
	item.MatchedName = "Manual series"
	item.Metadata = &models.Title{ID: item.MatchedTitleID, Name: item.MatchedName, TVDBID: 505, MediaType: "series"}
	item.SeasonNumber, item.EpisodeNumber = 4, 5
	item.Probe.TracksScanned = false
	item.Probe.AudioTracks, item.Probe.SubtitleTracks = nil, nil
	if err := f.store.LocalMedia().UpsertItem(t.Context(), &item); err != nil {
		t.Fatal(err)
	}
	// An information refresh must not turn into a new metadata-provider lookup.
	f.service.metadata = &languagePoisonMetadata{}
	return f, &item, probe
}

func TestExplicitMediaInfoRefreshReadsUnchangedFile(t *testing.T) {
	f, original, probe := legacyMediaInfoFixture(t)
	before, err := os.Stat(original.FilePath)
	if err != nil {
		t.Fatal(err)
	}
	f.service.ffprobePath = "/must-not-run/ffprobe"
	if _, err := f.service.RunScan(t.Context(), original.LibraryID); err != nil {
		t.Fatal(err)
	}
	unchanged, err := f.service.GetItem(t.Context(), original.ID)
	if err != nil || unchanged.Probe.TracksScanned || len(unchanged.Probe.AudioCodecs) != 1 {
		t.Fatalf("ordinary scan did not retain legacy facts: %+v %v", unchanged, err)
	}
	f.service.ffprobePath = probe
	if _, err := f.service.RunMediaInfoRefresh(t.Context(), original.LibraryID); err != nil {
		t.Fatal(err)
	}
	current, err := f.service.GetItem(t.Context(), original.ID)
	if err != nil || !current.Probe.TracksScanned || len(current.Probe.AudioTracks) != 1 || current.Probe.AudioTracks[0].Language != "fra" {
		t.Fatalf("explicit refresh did not store file languages: %+v %v", current, err)
	}
	if current.MatchStatus != models.LocalMediaMatchStatusManual || current.MatchedTitleID != original.MatchedTitleID || current.SeasonNumber != 4 || current.EpisodeNumber != 5 {
		t.Fatalf("information refresh changed the manual catalogue selection: %+v", current)
	}
	after, err := os.Stat(original.FilePath)
	if err != nil || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("refresh changed the actual file: %v", err)
	}
	// A failed explicit refresh leaves the file's facts unknown, rather than
	// continuing to advertise a stale known language or codec.
	f.service.ffprobePath = "/unavailable/ffprobe"
	if _, err := f.service.RunMediaInfoRefresh(t.Context(), original.LibraryID); err != nil {
		t.Fatal(err)
	}
	current, err = f.service.GetItem(t.Context(), original.ID)
	if err != nil || current.Probe.TracksScanned || len(current.Probe.AudioCodecs) != 0 || current.MatchedTitleID != original.MatchedTitleID {
		t.Fatalf("unreadable information did not remain unknown: %+v %v", current, err)
	}
}

func TestExplicitMediaInfoRefreshUsesExistingBackgroundScan(t *testing.T) {
	f, original, _ := legacyMediaInfoFixture(t)
	summary, err := f.service.StartMediaInfoRefresh(t.Context(), original.LibraryID)
	if err != nil || !summary.Async {
		t.Fatalf("refresh was not accepted as a normal background scan: %+v %v", summary, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		library, err := f.store.LocalMedia().GetLibrary(t.Context(), original.LibraryID)
		if err != nil {
			t.Fatal(err)
		}
		if library.LastScanStatus == models.LocalMediaScanStatusComplete {
			break
		}
		if library.LastScanStatus == models.LocalMediaScanStatusFailed || time.Now().After(deadline) {
			t.Fatalf("background refresh did not complete: %+v", library)
		}
		time.Sleep(10 * time.Millisecond)
	}
	current, err := f.service.GetItem(t.Context(), original.ID)
	if err != nil || !current.Probe.TracksScanned || current.MatchedTitleID != original.MatchedTitleID {
		t.Fatalf("background refresh lost actual facts or manual identity: %+v %v", current, err)
	}
}
