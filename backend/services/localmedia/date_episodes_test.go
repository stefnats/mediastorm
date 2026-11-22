package localmedia

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"novastream/models"
	"novastream/services/metadata"
	"novastream/utils/parsett"
)

// The catalogue is already stored: any search/details call made by a read-only
// candidate lookup is observable independently of the date lookup.
type dateEpisodeMetadata struct {
	title             models.Title
	matches           map[string][]metadata.EpisodeDateCatalogueMatch
	results           []models.SearchResult
	searches, details int
}

func newDateEpisodeMetadata() *dateEpisodeMetadata {
	m := &dateEpisodeMetadata{title: models.Title{ID: "tvdb:series:77", Name: "Synthetic Show", MediaType: "series", TVDBID: 77}, matches: make(map[string][]metadata.EpisodeDateCatalogueMatch)}
	m.matches["2024-01-02"] = []metadata.EpisodeDateCatalogueMatch{{Provider: "tvdb", Numbering: models.EpisodeNumbering{SeriesID: "tvdb:series:77", Ordering: "official"}, EpisodeID: "tvdb:episode:7010", AiredDate: "2024-01-02", SeasonNumber: 1, EpisodeNumber: 10}}
	return m
}
func (m *dateEpisodeMetadata) Search(context.Context, string, string) ([]models.SearchResult, error) {
	m.searches++
	if m.results != nil {
		return m.results, nil
	}
	return []models.SearchResult{{Title: m.title}}, nil
}
func (m *dateEpisodeMetadata) MovieDetails(context.Context, models.MovieDetailsQuery) (*models.Title, error) {
	m.details++
	return nil, nil
}
func (m *dateEpisodeMetadata) SeriesDetails(context.Context, models.SeriesDetailsQuery) (*models.SeriesDetails, error) {
	m.details++
	return &models.SeriesDetails{Title: m.title}, nil
}
func (m *dateEpisodeMetadata) GetCachedDateEpisodeMatch(ctx context.Context, n models.EpisodeNumbering, date string) (metadata.EpisodeDateCatalogueMatch, bool) {
	rows := m.matches[date]
	if ctx.Err() != nil || len(rows) != 1 || !models.SameEpisodeNumbering(&n, &rows[0].Numbering) {
		return metadata.EpisodeDateCatalogueMatch{}, false
	}
	return rows[0], true
}
func dateEpisodeItem() models.LocalMediaItem {
	return models.LocalMediaItem{ID: "dated", LibraryID: "library", LibraryType: models.LocalMediaLibraryTypeShow, FileName: "Synthetic.Show.2024.01.02.mkv", MatchedTitleID: "tvdb:series:77", MatchedMediaType: "series", MatchStatus: models.LocalMediaMatchStatusMatched}
}
func dateEpisodeReference() *models.EpisodeReference {
	return &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 10, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:77", Ordering: "official"}}
}

func TestDateEpisodeFilenameAndNormalNumbering(t *testing.T) {
	for _, name := range []string{"Synthetic.Show.2024.01.02.mkv", "Synthetic Show - 2024-01-02 - HD.mkv", "Synthetic_Show_2024_01_02.mkv", "Synthetic.Show.02.01.2024.mkv"} {
		t.Run(name, func(t *testing.T) {
			parsed, err := parsett.ParseTitle(name)
			if err != nil {
				t.Fatal(err)
			}
			got := detectTitle(models.LocalMediaLibraryTypeShow, name, parsed)
			if got.title != "Synthetic Show" || got.airedDate != "2024-01-02" || got.year != 0 || got.season != 0 || got.episode != 0 {
				t.Fatalf("date detection = %+v", got)
			}
		})
	}
	for _, name := range []string{"Synthetic.Show.S02E03.2024.01.02.mkv", "Synthetic.Show.2x03.2024-01-02.mkv"} {
		got := detectTitle(models.LocalMediaLibraryTypeShow, name, nil)
		if got.season != 2 || got.episode != 3 || got.airedDate != "" {
			t.Fatalf("ordinary numbering changed: %+v", got)
		}
	}
	for _, name := range []string{"Synthetic.Show.2024.02.30.mkv", "Synthetic.Show.2024.01.02.2024.01.03.mkv", "Synthetic.Show.2024.01.mkv"} {
		if _, _, ok := localEpisodeAirDate(name); ok {
			t.Fatalf("ambiguous/incomplete filename accepted: %s", name)
		}
	}
}

func TestDateEpisodesScanAndAutomaticPlayback(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "Synthetic Show {tvdb-77}")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	name := "Synthetic.Show.2024.01.02.mkv"
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("synthetic stored video"), 0600); err != nil {
		t.Fatal(err)
	}
	repo := &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "library", Name: "Synthetic series", Type: models.LocalMediaLibraryTypeShow, RootPath: root}, items: make(map[string]*models.LocalMediaItem)}
	md := newDateEpisodeMetadata()
	s := &Service{repo: repo, metadata: md, ffprobePath: filepath.Join(root, "unconfigured-probe"), scans: make(map[string]scanState)}
	if _, err := s.RunScan(t.Context(), "library"); err != nil {
		t.Fatal(err)
	}
	saved := repo.items[filepath.Join("Synthetic Show {tvdb-77}", name)]
	if saved == nil || saved.SeasonNumber != 1 || saved.EpisodeNumber != 10 || saved.MatchedTitleID != "tvdb:series:77" {
		t.Fatalf("scan did not persist dated episode: %+v", saved)
	}
	searches, details := md.searches, md.details
	candidates, err := s.FindPlaybackCandidates(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TitleID: "tvdb:series:77"}, dateEpisodeReference(), func(string) bool { return true })
	if err != nil || len(candidates) != 1 {
		t.Fatalf("playback candidates=%+v err=%v", candidates, err)
	}
	current, err := s.GetFallbackItem(t.Context(), candidates[0].ID)
	if err != nil || current.SeasonNumber != 1 || current.EpisodeNumber != 10 {
		t.Fatalf("fallback recheck lost dated match: %+v %v", current, err)
	}
	if md.searches != searches || md.details != details {
		t.Fatal("playback lookup fetched provider metadata")
	}
	// An unchanged rescan must also fill a formerly date-only catalogue row.
	saved.SeasonNumber, saved.EpisodeNumber = 0, 0
	if _, err = s.RunScan(t.Context(), "library"); err != nil {
		t.Fatal(err)
	}
	saved = repo.items[filepath.Join("Synthetic Show {tvdb-77}", name)]
	if saved.SeasonNumber != 1 || saved.EpisodeNumber != 10 {
		t.Fatal("reused scan row stayed unresolved")
	}
}

func TestPreviouslyScannedDateEpisodeUsesOnlyStoredData(t *testing.T) {
	root := t.TempDir()
	item := dateEpisodeItem()
	item.FilePath = filepath.Join(root, item.FileName)
	item.RelativePath = item.FileName
	if err := os.WriteFile(item.FilePath, []byte("synthetic video"), 0600); err != nil {
		t.Fatal(err)
	}
	repo := &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "library", Type: models.LocalMediaLibraryTypeShow, RootPath: root}, items: map[string]*models.LocalMediaItem{item.FileName: &item}}
	md := newDateEpisodeMetadata()
	s := &Service{repo: repo, metadata: md}
	candidates, err := s.FindPlaybackCandidates(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TitleID: "tvdb:series:77"}, dateEpisodeReference(), func(string) bool { return true })
	if err != nil || len(candidates) != 1 {
		t.Fatalf("existing dated catalogue row not resolved: %+v %v", candidates, err)
	}
	current, err := s.GetItem(t.Context(), "dated")
	if err != nil || current.SeasonNumber != candidates[0].SeasonNumber || current.EpisodeNumber != candidates[0].EpisodeNumber {
		t.Fatalf("single-item lookup differs: %+v %v", current, err)
	}
	s.enrichEpisodeMetadata(t.Context(), candidates)
	if md.searches != 0 || md.details != 0 {
		t.Fatal("display/play lookup initiated metadata enrichment")
	}
	if item.SeasonNumber != 0 || item.EpisodeNumber != 0 {
		t.Fatal("read-only lookup wrote catalogue data")
	}
}

func TestDateEpisodesLeaveUncertainAndManualMatchesAlone(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		change                  func(*models.LocalMediaItem, *dateEpisodeMetadata)
		wantSeason, wantEpisode int
	}{
		{"ambiguous date", func(_ *models.LocalMediaItem, m *dateEpisodeMetadata) {
			m.matches["2024-01-02"] = append(m.matches["2024-01-02"], m.matches["2024-01-02"][0])
		}, 0, 0},
		{"missing catalogue", func(_ *models.LocalMediaItem, m *dateEpisodeMetadata) { delete(m.matches, "2024-01-02") }, 0, 0},
		{"conflicting series", func(i *models.LocalMediaItem, _ *dateEpisodeMetadata) { i.Metadata = &models.Title{TVDBID: 99} }, 0, 0},
		{"no series anchor", func(i *models.LocalMediaItem, _ *dateEpisodeMetadata) { i.MatchedTitleID = "" }, 0, 0},
		{"low confidence", func(i *models.LocalMediaItem, _ *dateEpisodeMetadata) {
			i.MatchStatus = models.LocalMediaMatchStatusLowConfidence
		}, 0, 0},
		{"manual", func(i *models.LocalMediaItem, _ *dateEpisodeMetadata) {
			i.MatchStatus = models.LocalMediaMatchStatusManual
			i.SeasonNumber = 2
			i.EpisodeNumber = 3
		}, 2, 3},
		{"ordinary numbered", func(i *models.LocalMediaItem, _ *dateEpisodeMetadata) {
			i.FileName = "Synthetic.Show.S02E03.mkv"
			i.SeasonNumber = 2
			i.EpisodeNumber = 3
		}, 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i := dateEpisodeItem()
			m := newDateEpisodeMetadata()
			tc.change(&i, m)
			s := &Service{metadata: m}
			s.matchDateEpisode(t.Context(), &i)
			if i.SeasonNumber != tc.wantSeason || i.EpisodeNumber != tc.wantEpisode {
				t.Fatalf("unexpected mapping %+v", i)
			}
			if m.searches != 0 || m.details != 0 {
				t.Fatal("uncertain mapping fetched more metadata")
			}
		})
	}
	item := dateEpisodeItem()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	(&Service{metadata: newDateEpisodeMetadata()}).matchDateEpisode(ctx, &item)
	if item.SeasonNumber != 0 || item.EpisodeNumber != 0 {
		t.Fatal("cancelled lookup mapped an episode")
	}
}

func TestDateEpisodesDoNotMixAlternateNumbering(t *testing.T) {
	item := dateEpisodeItem()
	md := newDateEpisodeMetadata()
	s := &Service{repo: &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "library", Type: models.LocalMediaLibraryTypeShow}, items: map[string]*models.LocalMediaItem{item.ID: &item}}, metadata: md}
	episode := dateEpisodeReference()
	episode.Numbering.Ordering = "dvd"
	found, err := s.FindPlaybackCandidates(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TitleID: "tvdb:series:77"}, episode, func(string) bool { return true })
	if err != nil || len(found) != 0 {
		t.Fatalf("alternate ordering used official date mapping: %+v %v", found, err)
	}
	episode = dateEpisodeReference()
	episode.Numbering.SeriesID = "tvdb:series:99"
	found, err = s.FindPlaybackCandidates(t.Context(), models.LocalMediaMatchQuery{MediaType: "series", TitleID: "tvdb:series:77"}, episode, func(string) bool { return true })
	if err != nil || len(found) != 0 {
		t.Fatal("foreign series numbering used date mapping")
	}
}

func TestDateEpisodeScanDoesNotChooseBetweenEqualSeriesNames(t *testing.T) {
	md := newDateEpisodeMetadata()
	other := md.title
	other.ID = "tvdb:series:99"
	other.TVDBID = 99
	md.results = []models.SearchResult{{Title: md.title}, {Title: other}}
	s := &Service{metadata: md}
	detected := detectedTitle{title: "Synthetic Show", airedDate: "2024-01-02"}
	if got := s.matchMetadata(t.Context(), models.LocalMediaLibraryTypeShow, detected, nil); got.status != models.LocalMediaMatchStatusLowConfidence {
		t.Fatalf("ambiguous series selected: %+v", got)
	}
}
