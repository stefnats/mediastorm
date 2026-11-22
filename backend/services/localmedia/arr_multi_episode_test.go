package localmedia

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/arrclient"
	"novastream/utils/parsett"
)

func TestARRMultiEpisodeImportPersistsOnePhysicalFileAcrossRescan(t *testing.T) {
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
	name := filepath.Join(library.RootPath, "Synthetic.Series.S01E01E02.mkv")
	arrTestLocalVideo(t, f.service, name)
	members := []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}
	// Duplicate Sonarr history rows still describe just one physical file.
	files := []arrclient.ImportedFile{
		{Path: name, Title: "Synthetic Series", MediaType: "series", TVDBID: 505, SeasonNumber: 1, EpisodeNumber: 2},
		{Path: name, Title: "Synthetic Series", MediaType: "series", TVDBID: 505, SeasonNumber: 1, EpisodeNumber: 1},
		{Path: name, Title: "Synthetic Series", MediaType: "series", TVDBID: 505, SeasonNumber: 1, EpisodeNumber: 2},
	}
	result, err := f.service.ImportARRFiles(t.Context(), library.ID, "", files)
	if err != nil || result.NeedsMatch || result.InvalidMedia || result.Missing || len(result.ItemIDs) != 1 {
		t.Fatal(result, err)
	}
	id := result.ItemIDs[0]
	assertCurrent := func() {
		t.Helper()
		item, e := f.service.GetItem(t.Context(), id)
		if e != nil || item == nil {
			t.Fatal(item, e)
		}
		if item.SeasonNumber != 0 || item.EpisodeNumber != 0 || !reflect.DeepEqual(item.Episodes, members) || item.MatchStatus != models.LocalMediaMatchStatusMatched || item.FilePath != name {
			t.Fatalf("lost combined file membership: %#v", item)
		}
		if item.Metadata == nil || item.Metadata.TVDBID != 505 {
			t.Fatal("lost series identity", item.Metadata)
		}
		titleJSON, e := json.Marshal(item.Metadata)
		if e != nil {
			t.Fatal(e)
		}
		var title map[string]any
		if e = json.Unmarshal(titleJSON, &title); e != nil {
			t.Fatal(e)
		}
		if _, leaked := title["localFileEpisodes"]; leaked {
			t.Fatal("file membership leaked into catalogue Title")
		}
		items, e := f.store.LocalMedia().ListAllItemsByLibrary(t.Context(), library.ID)
		if e != nil || len(items) != 1 || items[0].ID != id {
			t.Fatal("physical file duplicated", items, e)
		}
		matches, e := f.service.FindMatches(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TVDBID: "505"})
		if e != nil || len(matches) != 1 {
			t.Fatal(matches, e)
		}
		group := matches[0].Group
		if group.ItemCount != 1 || group.TotalSizeBytes != item.SizeBytes || len(group.Seasons) != 1 || len(group.Seasons[0].Episodes) != 2 || group.Seasons[0].ItemCount != 1 || group.Seasons[0].TotalSizeBytes != item.SizeBytes {
			t.Fatalf("shared file counted twice: %#v", group)
		}
		for i, episode := range group.Seasons[0].Episodes {
			if episode.EpisodeNumber != i+1 || len(episode.Items) != 1 || episode.Items[0].ID != id || episode.Items[0].EpisodeNumber != 0 {
				t.Fatal("group invented a primary episode", episode)
			}
		}
		for _, member := range members {
			candidates, e := f.service.FindPlaybackCandidates(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TVDBID: "505"}, &models.EpisodeReference{SeasonNumber: member.SeasonNumber, EpisodeNumber: member.EpisodeNumber}, func(string) bool { return true })
			if e != nil || len(candidates) != 0 {
				t.Fatal("combined file automatically selected without verified offsets", candidates, e)
			}
		}
	}
	assertCurrent()
	if _, err = f.service.RunScan(t.Context(), library.ID); err != nil {
		t.Fatal(err)
	}
	assertCurrent()
	again, err := f.service.ImportARRFiles(t.Context(), library.ID, "", files)
	if err != nil || len(again.ItemIDs) != 1 || again.ItemIDs[0] != id {
		t.Fatal("repeat import duplicated file", again, err)
	}
	assertCurrent()
	manual, err := f.service.GetItem(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	manual.MatchStatus = models.LocalMediaMatchStatusManual
	if err = f.store.LocalMedia().UpsertItem(t.Context(), manual); err != nil {
		t.Fatal(err)
	}
	conflict := []arrclient.ImportedFile{{Path: name, Title: "Synthetic Series", MediaType: "series", TVDBID: 505, Episodes: []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 3}}}}
	denied, err := f.service.ImportARRFiles(t.Context(), library.ID, "", conflict)
	if err != nil || !denied.NeedsMatch || denied.Reason != "manual_match_conflict" {
		t.Fatal("manual membership conflict was not preserved", denied, err)
	}
	manual, err = f.service.GetItem(t.Context(), id)
	if err != nil || !reflect.DeepEqual(manual.Episodes, members) || manual.MatchStatus != models.LocalMediaMatchStatusManual {
		t.Fatal("ARR overwrote manual membership", manual, err)
	}
	manual.MatchStatus = models.LocalMediaMatchStatusMatched
	if err = f.store.LocalMedia().UpsertItem(t.Context(), manual); err != nil {
		t.Fatal(err)
	}
	// Same path with a new version cannot inherit historical membership.
	at := time.Now().Add(5 * time.Second)
	if err = os.Chtimes(name, at, at); err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.RunScan(t.Context(), library.ID); err != nil {
		t.Fatal(err)
	}
	changed, err := f.service.GetItem(t.Context(), id)
	if err != nil || len(changed.Episodes) != 0 || changed.EpisodeNumber != 0 || changed.MatchStatus != models.LocalMediaMatchStatusUnmatched {
		t.Fatal("replacement inherited old membership", changed, err)
	}
}

func TestARRMultiEpisodeCannotOverwriteConflictingManualMembership(t *testing.T) {
	item := models.LocalMediaItem{SeasonNumber: 1, EpisodeNumber: 2}
	members := []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}
	if arrEpisodeMembershipMatches(item, members, true) {
		t.Fatal("a manually selected single episode equals a combined file")
	}
	item.Episodes = append([]models.LocalMediaEpisode(nil), members...)
	if !arrEpisodeMembershipMatches(item, members, true) {
		t.Fatal("exact manual membership did not compare equal")
	}
	file := arrclient.ImportedFile{MediaType: "series", AmbiguousEpisodes: true, SeasonNumber: 1, EpisodeNumber: 1}
	if _, ok := arrImportedEpisodes(file); ok {
		t.Fatal("unresolved ambiguity became explicit membership")
	}
	file.Episodes = members
	file.EpisodeMappingIncomplete = true
	if _, ok := arrImportedEpisodes(file); ok {
		t.Fatal("incomplete history became explicit membership")
	}
}

func TestLocalMultiEpisodeGroupsCountPhysicalVersionsOnce(t *testing.T) {
	item := models.LocalMediaItem{ID: "one", LibraryType: models.LocalMediaLibraryTypeShow, MatchedTitleID: "tvdb:series:505", SizeBytes: 100, Episodes: []models.LocalMediaEpisode{{SeasonNumber: 0, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}}
	other := item
	other.ID = "two"
	other.SizeBytes = 50
	groups := buildLocalMediaGroups([]models.LocalMediaItem{item, other})
	if len(groups) != 1 || groups[0].ItemCount != 2 || groups[0].TotalSizeBytes != 150 || len(groups[0].Seasons) != 2 {
		t.Fatal(groups)
	}
	for _, season := range groups[0].Seasons {
		if season.ItemCount != 2 || season.TotalSizeBytes != 150 {
			t.Fatal("duplicate physical totals", season)
		}
	}
	if flat := collectItemsFromGroups(groups); len(flat) != 2 {
		t.Fatal("flattening duplicated shared versions", flat)
	}
}

func TestLocalMultiEpisodeFilenameNeverSelectsFirstParsedEpisode(t *testing.T) {
	got := detectTitle(models.LocalMediaLibraryTypeShow, "ignored.mkv", &parsett.ParsedTitle{Title: "Synthetic Series", Seasons: []int{1}, Episodes: []int{1, 2}})
	if got.episode != 0 {
		t.Fatal("scanner selected arbitrary first episode", got)
	}
	for _, name := range []string{"Synthetic.Series.S01E01E02.mkv", "Synthetic.Series.S01E01-E02.mkv", "Synthetic.Series.1x01x02.mkv"} {
		got = detectTitle(models.LocalMediaLibraryTypeShow, name, nil)
		if got.episode != 0 {
			t.Fatal("filename fallback selected arbitrary first episode", name, got)
		}
	}
}
