package sourceavailability

import (
	"context"
	"log"
	"time"

	"novastream/models"
)

type ReminderObserver func(context.Context, string, string, models.Title, *models.SeriesEpisode, models.SourceAvailability) error

func (s *Service) SetReminderObserver(observer ReminderObserver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reminderObserver = observer
}

// CurrentReminderEvidence consumes existing cache/proof only. It neither creates
// a view lease nor calls a searcher, metadata service, or LocalChecker.Check.
func (s *Service) CurrentReminderEvidence(ctx context.Context, profile, client string, title models.Title, episode *models.SeriesEpisode) models.SourceAvailability {
	if ctx.Err() != nil || !s.EpisodeHintsEnabled() {
		return unknown("not_checked")
	}
	resolve, err := s.scope(profile, client)
	if err != nil {
		return unknown("selection_changed")
	}
	selected, err := resolve(title.ID)
	if err != nil {
		return unknown("selection_changed")
	}
	if episode != nil {
		// Reconciliation passes identity only. Recover search inputs exclusively from
		// one unambiguous, nonexpired stored server projection, never a backup DTO.
		var matched *models.SeriesEpisode
		for _, entry := range s.Store.episodeRecords(profile, title.ID) {
			if entry.Value.ExpiresAt == nil || !entry.Value.ExpiresAt.After(s.now()) {
				continue
			}
			p := entry.Episode
			if p.Season != episode.SeasonNumber || p.Episode != episode.EpisodeNumber {
				continue
			}
			ep := p.episode()
			if episodeSelection(selected, &ep, title).Key != entry.Scope {
				continue
			}
			if p.ID != episode.ID || !models.SameEpisodeNumbering(&p.Numbering, episode.Numbering) {
				return unknown("episode_identity_unverified")
			}
			if matched != nil && episodeSelection(selected, matched, title).Key != entry.Scope {
				return unknown("episode_identity_unverified")
			}
			matched = &ep
		}
		// Local proof inputs are in-memory only and also require an exact identity.
		s.mu.Lock()
		for id, entry := range s.localRecords {
			p := entry.Episode
			if !validEpisodeProjection(p) || p.SeriesID != title.ID || p.Season != episode.SeasonNumber || p.Episode != episode.EpisodeNumber || !entry.Expires.After(s.now()) {
				continue
			}
			ep := p.episode()
			localSelection := episodeSelection(selected, &ep, title)
			localItem := request{ProfileID: profile, ClientID: client, Title: title, Episode: &ep, Scope: localSelection.Key, AudioLanguage: localSelection.AudioLanguage}
			if requestID(localItem) != id {
				continue
			}
			if p.ID != episode.ID || !models.SameEpisodeNumbering(&p.Numbering, episode.Numbering) {
				s.mu.Unlock()
				return unknown("episode_identity_unverified")
			}
			if matched != nil && episodeSelection(selected, matched, title).Key != localSelection.Key {
				s.mu.Unlock()
				return unknown("episode_identity_unverified")
			}
			if matched == nil {
				matched = &ep
			}
		}
		s.mu.Unlock()
		if matched == nil {
			return unknown("not_checked")
		}
		episode = matched
		selected = episodeSelection(selected, episode, title)
	}
	item := request{ProfileID: profile, ClientID: client, Title: title, Episode: episode, Scope: selected.Key, AudioLanguage: selected.AudioLanguage}
	revision := s.reports.RevisionFor(s.makeEpisodeReportContext(item, selected))
	value := s.localHint(ctx, item, selected)
	value = s.recheckPresentation(value, profile, item.cacheID(), selected.Key)
	// Ready/prepared only proves a stream existed; manual selection and soft
	// language fallback do not establish the desired search intent.
	_, evidence := s.Store.getObservation(profile, item.cacheID(), selected.Key)
	if value.State == "found" && value.SourceType != "local" && (evidence == nil || evidence.Kind != "search") {
		return unknown("candidate_intent_unverified")
	}

	if ctx.Err() != nil || !s.requestScopeStillMatches(item) || s.reports.RevisionFor(s.makeEpisodeReportContext(item, selected)) != revision {
		return unknown("selection_changed")
	}
	return value
}
func (s *Service) publishReminder(ctx context.Context, item request) {
	s.mu.Lock()
	observer := s.reminderObserver
	s.mu.Unlock()
	if observer == nil {
		return
	}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	value := s.CurrentReminderEvidence(bounded, item.ProfileID, item.ClientID, item.Title, item.Episode)
	if value.State != "found" {
		return
	}
	if err := observer(bounded, item.ProfileID, item.ClientID, item.Title, item.Episode, value); err != nil {
		log.Printf("[reminders] source observation was not persisted: %v", err)
	}
}
