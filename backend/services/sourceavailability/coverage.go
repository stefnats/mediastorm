package sourceavailability

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"novastream/models"
	"novastream/services/badstreams"
)

// episodeProjection retains only previously verified coordinates and search
// inputs. In particular it contains no artwork, progress, URLs or local paths.
// SourceDateUnique is explicit because SeriesEpisode deliberately omits it.
type episodeProjection struct {
	SeriesID              string                  `json:"seriesId"`
	ID                    string                  `json:"episodeId"`
	Numbering             models.EpisodeNumbering `json:"numbering"`
	Season                int                     `json:"seasonNumber"`
	Episode               int                     `json:"episodeNumber"`
	AbsoluteEpisodeNumber int                     `json:"absoluteEpisodeNumber,omitempty"`
	AiredDate             string                  `json:"airedDate,omitempty"`
	AiredDateTimeUTC      string                  `json:"airedDateTimeUTC,omitempty"`
	SourceDateUnique      bool                    `json:"sourceDateUnique,omitempty"`
}

func projectEpisode(item request) *episodeProjection {
	if item.Episode == nil || item.Episode.Numbering == nil {
		return nil
	}
	ep := item.Episode
	return &episodeProjection{SeriesID: item.Title.ID, ID: ep.ID, Numbering: *ep.Numbering,
		Season: ep.SeasonNumber, Episode: ep.EpisodeNumber, AbsoluteEpisodeNumber: ep.AbsoluteEpisodeNumber,
		AiredDate: ep.AiredDate, AiredDateTimeUTC: ep.AiredDateTimeUTC, SourceDateUnique: ep.SourceDateUnique}
}

func (p *episodeProjection) episode() models.SeriesEpisode {
	n := p.Numbering
	return models.SeriesEpisode{ID: p.ID, Numbering: &n, SeasonNumber: p.Season, EpisodeNumber: p.Episode,
		AbsoluteEpisodeNumber: p.AbsoluteEpisodeNumber, AiredDate: p.AiredDate, AiredDateTimeUTC: p.AiredDateTimeUTC,
		SourceDateUnique: p.SourceDateUnique}
}

func validEpisodeProjection(p *episodeProjection) bool {
	return p != nil && p.SeriesID != "" && len(p.SeriesID) <= 128 && p.ID != "" && len(p.ID) <= 256 &&
		p.Numbering.SeriesID != "" && len(p.Numbering.SeriesID) <= 128 && len(p.Numbering.Ordering) <= 64 &&
		p.Season >= 0 && p.Season <= 10000 && p.Episode > 0 && p.Episode <= 100000 &&
		p.AbsoluteEpisodeNumber >= 0 && p.AbsoluteEpisodeNumber <= 1000000 &&
		len(p.AiredDate) <= 10 && len(p.AiredDateTimeUTC) <= 40
}

func copyEpisodeProjection(p *episodeProjection) *episodeProjection {
	if p == nil {
		return nil
	}
	copy := *p
	return &copy
}

func (s *Store) episodeRecords(profile, series string) []record {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := []record{}
	for _, entry := range s.records {
		if entry.ProfileID != profile || !validEpisodeProjection(entry.Episode) || entry.Episode.SeriesID != series {
			continue
		}
		entry.Episode = copyEpisodeProjection(entry.Episode)
		result = append(result, entry)
	}
	return result
}

type coverageObservation struct {
	projection *episodeProjection
	scope      string
}

type coverageResult struct {
	coverage *models.EpisodeSourceCoverage
	positive models.SourceAvailability
}

// cachedEpisodeCoverage performs bounded cache reads and validates existing
// local proofs only. It never requests metadata, probes files or schedules work.
// Supplied current coordinates can invalidate older cached metadata for the
// same episode. Without a current catalogue, conflicting aliases stay unknown.
func (s *Service) cachedEpisodeCoverage(ctx context.Context, profile, client string, title models.Title, selection Selection, current []models.SeriesEpisode) coverageResult {
	reportContext := badstreams.PlaybackContext{ProfileID: profile, ClientID: client}
	reportsBefore := s.reports.RevisionFor(reportContext)
	observations := []coverageObservation{}
	for _, entry := range s.Store.episodeRecords(profile, title.ID) {
		observations = append(observations, coverageObservation{entry.Episode, entry.Scope})
	}
	s.mu.Lock()
	for id, entry := range s.localRecords {
		if !validEpisodeProjection(entry.Episode) || entry.Episode.SeriesID != title.ID {
			continue
		}
		ep := entry.Episode.episode()
		selected := episodeSelection(selection, &ep, title)
		item := request{ProfileID: profile, ClientID: client, Title: title, Episode: &ep, Scope: selected.Key}
		if requestID(item) == id {
			observations = append(observations, coverageObservation{copyEpisodeProjection(entry.Episode), selected.Key})
		}
	}
	s.mu.Unlock()

	// Group by visible coordinates before considering positivity. Different IDs
	// or numbering owners at one coordinate cannot establish the same episode.
	groups := map[string]map[string]*episodeProjection{}
	currentIDs := map[string]*episodeProjection{}
	for i := range current {
		p := projectEpisode(request{Title: title, Episode: &current[i]})
		if validEpisodeProjection(p) {
			currentIDs[p.ID] = p
		}
	}
	add := func(p *episodeProjection) {
		coord := fmt.Sprintf("%d:%d", p.Season, p.Episode)
		encoded, _ := json.Marshal(p)
		if groups[coord] == nil {
			groups[coord] = map[string]*episodeProjection{}
		}
		groups[coord][string(encoded)] = p
	}
	for _, observation := range observations {
		p := observation.projection
		ep := p.episode()
		if episodeSelection(selection, &ep, title).Key != observation.scope {
			continue // Old language, quality, source rights or title metadata.
		}
		if fresh := currentIDs[p.ID]; fresh != nil && *fresh != *p {
			continue
		}
		add(p)
	}
	// Currently delivered but unchecked coordinates are explicitly unknown;
	// they are observations, not an invented catalogue-wide denominator.
	for _, p := range currentIDs {
		add(p)
	}
	if len(groups) == 0 {
		return coverageResult{}
	}
	coverage := &models.EpisodeSourceCoverage{State: "unknown", Seasons: []models.SeasonSourceCoverage{}}
	seasons := map[int]*models.SeasonSourceCoverage{}
	positive := unknown("episode_candidates")
	allLocal := true
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	for _, aliases := range groups {
		var p *episodeProjection
		for _, item := range aliases {
			p = item
			break
		}
		season := seasons[p.Season]
		if season == nil {
			season = &models.SeasonSourceCoverage{SeasonNumber: p.Season, State: "unknown"}
			seasons[p.Season] = season
		}
		value := unknown("episode_identity_unverified")
		if len(aliases) == 1 {
			ep := p.episode()
			selected := episodeSelection(selection, &ep, title)
			item := request{ProfileID: profile, ClientID: client, Title: title, Episode: &ep, Scope: selected.Key, AudioLanguage: selected.AudioLanguage}
			value = s.localHint(bounded, item, selected)
			value = s.recheckPresentation(value, profile, item.cacheID(), selected.Key)
		}
		switch value.State {
		case "found":
			coverage.FoundEpisodes++
			season.FoundEpisodes++
			positive.State = "found"
			allLocal = allLocal && value.SourceType == "local"
			if value.CheckedAt != nil && (positive.CheckedAt == nil || value.CheckedAt.Before(*positive.CheckedAt)) {
				positive.CheckedAt = value.CheckedAt
			}
			if value.ExpiresAt != nil && (positive.ExpiresAt == nil || value.ExpiresAt.Before(*positive.ExpiresAt)) {
				positive.ExpiresAt = value.ExpiresAt
			}
		case "not_found":
			coverage.NotFoundEpisodes++
			season.NotFoundEpisodes++
		default:
			coverage.UnknownEpisodes++
			season.UnknownEpisodes++
		}
	}
	if coverage.FoundEpisodes > 0 && coverage.NotFoundEpisodes > 0 {
		coverage.State = "partial"
		positive.Reason = "partial_episode_candidates"
	}
	if positive.State == "found" && allLocal {
		positive.SourceType = "local"
	}
	if !s.scopeStillMatches(profile, client, title.ID, selection.Key) {
		return coverageResult{positive: unknown("selection_changed")}
	}
	if s.reports.RevisionFor(reportContext) != reportsBefore {
		coverage.State = "unknown"
		coverage.UnknownEpisodes += coverage.FoundEpisodes + coverage.NotFoundEpisodes
		coverage.FoundEpisodes, coverage.NotFoundEpisodes = 0, 0
		for _, season := range seasons {
			season.UnknownEpisodes += season.FoundEpisodes + season.NotFoundEpisodes
			season.FoundEpisodes, season.NotFoundEpisodes = 0, 0
		}
		positive = unknown("reports_changed")
	}
	for _, season := range seasons {
		if season.FoundEpisodes > 0 && season.NotFoundEpisodes > 0 {
			season.State = "partial"
		}
		coverage.Seasons = append(coverage.Seasons, *season)
	}
	sort.Slice(coverage.Seasons, func(i, j int) bool { return coverage.Seasons[i].SeasonNumber < coverage.Seasons[j].SeasonNumber })
	return coverageResult{coverage, positive}
}

func (s *Service) withEpisodeCoverage(ctx context.Context, profile, client string, title models.Title, selection Selection, value models.SourceAvailability) models.SourceAvailability {
	if title.MediaType != "series" && title.MediaType != "tv" {
		return value
	}
	result := s.cachedEpisodeCoverage(ctx, profile, client, title, selection, nil)
	if result.positive.Reason == "selection_changed" || result.positive.Reason == "reports_changed" {
		value = result.positive
	}
	if result.coverage == nil {
		return value
	}
	if result.coverage.FoundEpisodes > 0 {
		value = result.positive
	} else if value.State == "not_found" {
		// A bounded title query plus missing/unknown episode observations cannot
		// prove absence for every episode in a series we have not inventoried.
		value.State, value.Complete, value.SourceType, value.Reason = "unknown", false, "", "series_episodes_unconfirmed"
	}
	value.EpisodeCoverage = result.coverage
	return s.recheckPresentation(value, profile, title.ID, selection.Key)
}
