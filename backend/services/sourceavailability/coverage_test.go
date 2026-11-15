package sourceavailability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/badstreams"
)

func cacheEpisode(t *testing.T, s *Service, profile, client string, title models.Title, ep models.SeriesEpisode, names ...string) {
	t.Helper()
	resolve, err := s.scope(profile, client)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := resolve(title.ID)
	if err != nil {
		t.Fatal(err)
	}
	selected = episodeSelection(selected, &ep, title)
	item := request{ProfileID: profile, ClientID: client, Title: title, Episode: &ep, Scope: selected.Key, AudioLanguage: selected.AudioLanguage}
	evidence := &observation{Version: 1, Kind: "search", Complete: true}
	for _, name := range names {
		candidate, ok := projectCandidate(release(name))
		if !ok {
			t.Fatal("invalid synthetic release")
		}
		evidence.Candidates = append(evidence.Candidates, candidate)
	}
	checked, expires := s.now().UTC(), s.now().Add(time.Hour).UTC()
	value := models.SourceAvailability{State: "unknown", CheckedAt: &checked, ExpiresAt: &expires, Providers: []string{"Fast"}}
	value = s.evaluateEpisode(value, evidence, item, selected)
	if err := s.Store.putObservation(profile, item.cacheID(), selected.Key, value, evidence, projectEpisode(item)); err != nil {
		t.Fatal(err)
	}
}

func seriesStatus(s *Service, title models.Title) models.SourceAvailability {
	return *s.AnnotateTitles("profile", "tv", []models.Title{title})[0].SourceAvailability
}

func assertCoverage(t *testing.T, value models.SourceAvailability, state string, found, missing, unknown int) {
	t.Helper()
	c := value.EpisodeCoverage
	if c == nil || c.State != state || c.FoundEpisodes != found || c.NotFoundEpisodes != missing || c.UnknownEpisodes != unknown || c.CatalogueComplete {
		t.Fatalf("unexpected episode coverage: %+v, legacy=%+v", c, value)
	}
}

func TestSeriesCoverageUsesObservedCoordinatesWithoutAdditionalWork(t *testing.T) {
	s, search, _, _ := reportFixture(t)
	searchReleases(search, release("Synthetic.Series.S01E01.1080p"))
	view(t, s, "tv", "row", 1, episode(1))
	s.checkOne(t.Context())
	view(t, s, "tv", "row", 2, episode(2))
	s.checkOne(t.Context())
	before := len(s.pending)
	v := seriesStatus(s, episodeTitle())
	assertCoverage(t, v, "partial", 1, 1, 0)
	if v.State != "found" || v.Complete || v.Reason != "partial_episode_candidates" || v.CheckedAt == nil || v.ExpiresAt == nil || search.calls != 2 || len(s.pending) != before {
		t.Fatalf("aggregation created work or claimed completeness: %+v", v)
	}
	response := view(t, s, "tv", "row", 3, episode(1), episode(2), episode(3))
	if c := response.EpisodeCoverage; c == nil || c.State != "partial" || c.FoundEpisodes != 1 || c.NotFoundEpisodes != 1 || c.UnknownEpisodes != 1 {
		t.Fatalf("visible unchecked episode disappeared: %+v", c)
	}
	if search.calls != 2 {
		t.Fatal("coverage performed another search")
	}
}

func TestSeriesCoverageReducerNeverInventsCatalogueCompleteness(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		found, missing, uncertain bool
		state, legacy             string
	}{
		{"found-only", true, false, false, "unknown", "found"},
		{"missing-only", false, true, false, "unknown", "unknown"},
		{"unknown-only", false, false, true, "unknown", "unknown"},
		{"found-and-unknown", true, false, true, "unknown", "found"},
		{"mixed", true, true, false, "partial", "found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, _ := reportFixture(t)
			if tc.found {
				cacheEpisode(t, s, "profile", "tv", episodeTitle(), episode(1), "Synthetic.Series.S01E01.1080p")
			}
			if tc.missing {
				cacheEpisode(t, s, "profile", "tv", episodeTitle(), episode(2))
			}
			if tc.uncertain {
				cacheEpisode(t, s, "profile", "tv", episodeTitle(), episode(3), "Synthetic.Series.S01.Complete.1080p")
			}
			v := seriesStatus(s, episodeTitle())
			f, m, u := 0, 0, 0
			if tc.found {
				f = 1
			}
			if tc.missing {
				m = 1
			}
			if tc.uncertain {
				u = 1
			}
			assertCoverage(t, v, tc.state, f, m, u)
			if v.State != tc.legacy || v.Complete {
				t.Fatalf("legacy state=%+v", v)
			}
		})
	}
}

func TestSeriesCoverageGeneralCandidateAndNegativeDoNotDescribeEveryEpisode(t *testing.T) {
	s, search, _, _ := reportFixture(t)
	title := episodeTitle()
	searchReleases(search, release("Synthetic.Series.S01.Complete.1080p"))
	visibleTitlesForTest(t, s, "profile", "tv", []models.Title{title})
	s.checkOne(t.Context())
	v := seriesStatus(s, title)
	if v.State != "found" || v.EpisodeCoverage != nil {
		t.Fatal("general series candidate invented episode coverage")
	}
	cacheEpisode(t, s, "profile", "tv", title, episode(2))
	v = seriesStatus(s, title)
	assertCoverage(t, v, "unknown", 0, 1, 0)
	if v.State != "found" {
		t.Fatal("a missing episode overrode an independent general candidate")
	}
	resolve, _ := s.scope("profile", "tv")
	selection, _ := resolve(title.ID)
	now, expires := s.now(), s.now().Add(time.Hour)
	if err := s.Store.putObservation("profile", title.ID, selection.Key, models.SourceAvailability{State: "not_found", Complete: true, CheckedAt: &now, ExpiresAt: &expires}, &observation{Version: 1, Kind: "search", Complete: true}); err != nil {
		t.Fatal(err)
	}
	v = seriesStatus(s, title)
	if v.State != "unknown" || v.Complete {
		t.Fatal("bounded observations grayed the whole series")
	}
}

func TestSeriesCoverageFiltersCurrentProfileLanguageQualityAndProviderScope(t *testing.T) {
	for _, mutation := range []string{"profile", "language", "quality", "provider", "title-metadata"} {
		t.Run(mutation, func(t *testing.T) {
			s, _, _, profiles := reportFixture(t)
			title := episodeTitle()
			cacheEpisode(t, s, "profile", "tv", title, episode(1), "Synthetic.Series.S01E01.1080p")
			switch mutation {
			case "profile":
				v := *s.AnnotateTitles("another-profile", "tv", []models.Title{title})[0].SourceAvailability
				if v.State != "unknown" || v.EpisodeCoverage != nil {
					t.Fatal("another profile's observations leaked")
				}
				return
			case "language":
				profiles["profile"].Playback.PreferredAudioLanguage = "spa"
			case "quality":
				profiles["profile"].Filtering.MaxResolution = models.StringPtr("720p")
			case "provider":
				cfg, _ := s.manager.Load()
				cfg.Indexers[0].Enabled = false
				if err := s.manager.Save(cfg); err != nil {
					t.Fatal(err)
				}
			case "title-metadata":
				title.Name = "Changed series identity"
			}
			v := seriesStatus(s, title)
			if v.State != "unknown" || v.EpisodeCoverage != nil {
				t.Fatalf("old scope reused: %+v", v)
			}
		})
	}
}

func TestSeriesCoverageReevaluatesEpisodeReportsUndoAndExpiryWithoutSearch(t *testing.T) {
	s, search, reports, _ := reportFixture(t)
	title, ep := episodeTitle(), episode(1)
	cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
	cacheEpisode(t, s, "profile", "tv", title, episode(2))
	assertCoverage(t, seriesStatus(s, title), "partial", 1, 1, 0)
	before := s.Revision("profile", "tv")
	report, err := reports.Report(badstreams.ReportRequest{Result: release("Synthetic.Series.S01E01.1080p"), Context: badstreams.PlaybackContext{ProfileID: "profile", ClientID: "tv", TitleID: title.ID, AudioLanguage: "fra", Episode: &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 1, Numbering: ep.Numbering}}, Reason: "missing_language", ReporterID: "profile", Shared: true})
	if err != nil {
		t.Fatal(err)
	}
	v := seriesStatus(s, title)
	assertCoverage(t, v, "unknown", 0, 2, 0)
	if v.State != "unknown" {
		t.Fatal("episode reports declared a whole series missing")
	}
	if s.Revision("profile", "tv") == before {
		t.Fatal("episode report did not invalidate the profile homepage revision")
	}
	if !reports.Delete(report.ID) {
		t.Fatal("undo failed")
	}
	assertCoverage(t, seriesStatus(s, title), "partial", 1, 1, 0)
	if s.Revision("profile", "tv") != before {
		t.Fatal("episode report undo did not restore the profile homepage revision")
	}
	now := s.now().Add(2 * time.Hour)
	s.now = func() time.Time { return now }
	s.Store.now = s.now
	v = seriesStatus(s, title)
	assertCoverage(t, v, "unknown", 0, 0, 2)
	if v.State != "unknown" || search.calls != 0 {
		t.Fatal("expiry searched or retained an old positive")
	}
}

func TestSeriesCoverageDeduplicatesAndRejectsAmbiguousEpisodeAliases(t *testing.T) {
	s, _, _, _ := reportFixture(t)
	title, ep := episodeTitle(), episode(1)
	cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
	cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
	assertCoverage(t, seriesStatus(s, title), "unknown", 1, 0, 0)
	alias := ep
	alias.ID = "unresolved-provider-alias"
	cacheEpisode(t, s, "profile", "tv", title, alias)
	assertCoverage(t, seriesStatus(s, title), "unknown", 0, 0, 1)
	alias.Numbering = &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}
	cacheEpisode(t, s, "profile", "tv", title, alias)
	assertCoverage(t, seriesStatus(s, title), "unknown", 0, 0, 1)
	// A visible subset cannot prove that another previously observed alias has
	// disappeared from the catalogue, even when only this ID is requested.
	v := view(t, s, "tv", "row", 1, ep)
	if v.EpisodeCoverage.FoundEpisodes != 0 || v.EpisodeCoverage.UnknownEpisodes != 1 {
		t.Fatal("visible subset incorrectly resolved an episode alias")
	}
}

func TestSeriesCoverageCurrentEpisodeMetadataInvalidatesPriorProjection(t *testing.T) {
	s, _, _, _ := reportFixture(t)
	ep := episode(1)
	cacheEpisode(t, s, "profile", "tv", episodeTitle(), ep, "Synthetic.Series.S01E01.1080p")
	ep.AiredDate = "2020-01-02"
	v := view(t, s, "tv", "row", 1, ep)
	if v.Items[0].Availability.State != "unknown" || v.EpisodeCoverage.FoundEpisodes != 0 || v.EpisodeCoverage.UnknownEpisodes != 1 {
		t.Fatal("old episode metadata retained positivity")
	}
}

func TestSeriesCoveragePersistsMinimalDailyProjectionAndSeasonZero(t *testing.T) {
	for _, id := range []string{"tmdb:series:123", "tvdb:series:74852"} {
		t.Run(id, func(t *testing.T) {
			s, _, _, _ := reportFixture(t)
			title := episodeTitle()
			title.ID = id
			title.IsDaily = true
			ep := episode(1)
			ep.SeasonNumber = 0
			ep.Numbering = &models.EpisodeNumbering{SeriesID: id, Ordering: "official"}
			ep.Overview = "private overview"
			ep.Image = &models.Image{URL: "https://synthetic.invalid/private-artwork"}
			cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.2020.01.01.1080p")
			store, err := NewStore(s.Store.directory)
			if err != nil {
				t.Fatal(err)
			}
			s.Store = store
			v := seriesStatus(s, title)
			assertCoverage(t, v, "unknown", 1, 0, 0)
			if v.EpisodeCoverage.Seasons[0].SeasonNumber != 0 {
				t.Fatal("season zero lost")
			}
			files, _ := os.ReadDir(store.directory)
			for _, file := range files {
				if strings.HasSuffix(file.Name(), ".json") {
					data, _ := os.ReadFile(filepath.Join(store.directory, file.Name()))
					for _, private := range []string{"private overview", "private-artwork", "resume", "watched"} {
						if strings.Contains(string(data), private) {
							t.Fatal("projection retained unrelated metadata")
						}
					}
				}
			}
		})
	}
}

func TestSeriesCoverageUsesOnlyExistingLocalProofAndLosesRevokedAccess(t *testing.T) {
	s, search, _, _ := reportFixture(t)
	checker := &fakeLocalHints{proof: LocalProof{ItemID: "private-item", Fingerprint: "private-proof"}, valid: true}
	s.SetLocalChecker(checker)
	view(t, s, "tv", "row", 1, episode(1))
	s.checkOne(t.Context())
	checks := checker.checks
	v := seriesStatus(s, episodeTitle())
	assertCoverage(t, v, "unknown", 1, 0, 0)
	if v.State != "found" || v.SourceType != "local" || checker.checks != checks || search.calls != 0 {
		t.Fatal("cached aggregation probed or lost local evidence")
	}
	encoded, _ := json.Marshal(v)
	if strings.Contains(string(encoded), "private-") {
		t.Fatal("local proof leaked")
	}
	checker.valid = false
	v = seriesStatus(s, episodeTitle())
	if v.State != "unknown" || v.EpisodeCoverage.FoundEpisodes != 0 || checker.checks != checks {
		t.Fatal("revoked local source retained positive or was probed")
	}
}

func TestSeriesCoverageLegacyRecordsRemainUnknownAndProjectionIsDefensive(t *testing.T) {
	s, _, _, _ := reportFixture(t)
	title, ep := episodeTitle(), episode(1)
	resolve, _ := s.scope("profile", "tv")
	selected, _ := resolve(title.ID)
	selected = episodeSelection(selected, &ep, title)
	now, expires := s.now(), s.now().Add(time.Hour)
	if err := s.Store.Put("profile", episodeCacheID(title.ID, &ep), selected.Key, models.SourceAvailability{State: "found", CheckedAt: &now, ExpiresAt: &expires}); err != nil {
		t.Fatal(err)
	}
	if v := seriesStatus(s, title); v.EpisodeCoverage != nil || v.State != "unknown" {
		t.Fatal("legacy unprojected episode asserted coverage")
	}
	cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
	entries := s.Store.episodeRecords("profile", title.ID)
	entries[0].Episode.ID = "mutated"
	assertCoverage(t, seriesStatus(s, title), "unknown", 1, 0, 0)
	value := seriesStatus(s, title)
	copy := copyValue(value)
	copy.EpisodeCoverage.Seasons[0].FoundEpisodes = 99
	if value.EpisodeCoverage.Seasons[0].FoundEpisodes != 1 {
		t.Fatal("coverage seasons share mutable storage")
	}
	// The added object is optional; clients decoding only the old state survive.
	data, _ := json.Marshal(value)
	var old struct {
		State    string `json:"state"`
		Complete bool   `json:"complete"`
	}
	if json.Unmarshal(data, &old) != nil || old.State != "found" || old.Complete {
		t.Fatal("legacy state contract changed")
	}
}

func TestSeriesCoverageSeasonsAreOrderedAndIndependentlyPartial(t *testing.T) {
	s, _, _, _ := reportFixture(t)
	for _, season := range []int{2, 0, 1} {
		for _, number := range []int{1, 2} {
			ep := episode(number)
			ep.ID = fmt.Sprintf("s%d-e%d", season, number)
			ep.SeasonNumber = season
			names := []string{}
			if number == 1 {
				names = append(names, fmt.Sprintf("Synthetic.Series.S%02dE%02d.1080p", season, number))
			}
			cacheEpisode(t, s, "profile", "tv", episodeTitle(), ep, names...)
		}
	}
	v := seriesStatus(s, episodeTitle())
	assertCoverage(t, v, "partial", 3, 3, 0)
	for i, season := range v.EpisodeCoverage.Seasons {
		if season.SeasonNumber != i || season.State != "partial" || season.FoundEpisodes != 1 || season.NotFoundEpisodes != 1 {
			t.Fatalf("wrong season summary: %+v", season)
		}
	}
}

type changingCoverageLocal struct {
	fakeLocalHints
	action func(context.Context)
}

func (c *changingCoverageLocal) Validate(ctx context.Context, _ LocalRequest, _ LocalProof) bool {
	if c.action != nil {
		action := c.action
		c.action = nil
		action(ctx)
	}
	return c.valid
}

func TestSeriesCoverageDiscardsReportsOrScopeChangedDuringCachedValidation(t *testing.T) {
	for _, change := range []string{"report", "selection"} {
		t.Run(change, func(t *testing.T) {
			s, _, reports, profiles := reportFixture(t)
			checker := &changingCoverageLocal{fakeLocalHints: fakeLocalHints{proof: LocalProof{ItemID: "synthetic-item"}, valid: true}}
			s.SetLocalChecker(checker)
			view(t, s, "tv", "row", 1, episode(1))
			s.checkOne(t.Context())
			checker.action = func(context.Context) {
				if change == "selection" {
					profiles["profile"].Playback.PreferredAudioLanguage = "eng"
					return
				}
				other := episode(99)
				_, err := reports.Report(badstreams.ReportRequest{Result: release("Synthetic.Series.S01E99.1080p"), Context: badstreams.PlaybackContext{ProfileID: "profile", ClientID: "tv", TitleID: episodeTitle().ID, AudioLanguage: "fra", Episode: &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 99, Numbering: other.Numbering}}, Reason: "wrong_episode", ReporterID: "profile", Shared: true})
				if err != nil {
					t.Fatal(err)
				}
			}
			v := seriesStatus(s, episodeTitle())
			if v.State != "unknown" || v.EpisodeCoverage != nil && v.EpisodeCoverage.FoundEpisodes != 0 {
				t.Fatalf("context changed while reducing: %+v", v)
			}
		})
	}
}

func TestSeriesCoverageTitleBatchSharesOneCachedValidationBudget(t *testing.T) {
	s, _, _, _ := reportFixture(t)
	checker := &changingCoverageLocal{fakeLocalHints: fakeLocalHints{proof: LocalProof{ItemID: "synthetic-item"}, valid: true}}
	s.SetLocalChecker(checker)
	titles := []models.Title{episodeTitle(), episodeTitle()}
	titles[1].ID = "tvdb:series:74852"
	for i, title := range titles {
		ep := episode(1)
		ep.Numbering = &models.EpisodeNumbering{SeriesID: title.ID, Ordering: "official"}
		if _, err := s.ReplaceEpisodeView("profile", "tv", fmt.Sprintf("row-%d", i), 1, title, []models.SeriesEpisode{ep}); err != nil {
			t.Fatal(err)
		}
		s.checkOne(t.Context())
	}
	checker.action = func(ctx context.Context) { <-ctx.Done() }
	started := time.Now()
	response := s.AnnotateTitles("profile", "tv", titles)
	if time.Since(started) > 1500*time.Millisecond {
		t.Fatal("each title received a separate local validation budget")
	}
	for _, title := range response {
		if title.SourceAvailability.State == "found" {
			t.Fatal("exhausted validation budget retained local positivity")
		}
	}
}
