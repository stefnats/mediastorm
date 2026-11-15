// Package reminders records profile-owned in-app milestones from existing
// observations. It owns no metadata, provider, search or media-probe client.
package reminders

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"novastream/models"
	"novastream/services/titleratings"
)

var (
	ErrInvalid   = errors.New("invalid reminder")
	ErrReload    = errors.New("reload this title before creating a reminder")
	ErrNotFound  = errors.New("reminder not found")
	ErrLimit     = errors.New("reminder limit reached")
	ErrForbidden = errors.New("reminders unavailable for this profile")
)

const MaxActive = 100
const MaxStored = 2000
const MaxMilestones = 10000

type CreateRequest struct {
	ContentID   string                  `json:"contentId"`
	ContentType string                  `json:"contentType"`
	EventKind   string                  `json:"eventKind"`
	ReleaseType string                  `json:"releaseType,omitempty"`
	Episode     *models.ReminderEpisode `json:"episode,omitempty"`
}
type SourceCheck func(context.Context, string, string, models.Title, *models.SeriesEpisode) models.SourceAvailability

type projectionKey struct{ profile, client, kind, id string }
type projection struct {
	episodes map[string]models.SeriesEpisode
	expires  time.Time
	observed time.Time
	title    models.Title
	releases map[string]string
}
type Service struct {
	repo        Repository
	allowed     func(string) bool
	source      SourceCheck
	now         func() time.Time
	mu          sync.Mutex
	projections map[projectionKey]projection
}

func New(repo Repository, allowed func(string) bool) *Service {
	return &Service{repo: repo, allowed: allowed, now: time.Now, projections: map[projectionKey]projection{}}
}
func (s *Service) SetSourceCheck(check SourceCheck) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.source = check
}
func (s *Service) permitted(profile string) bool {
	return profile != "" && len(profile) <= 128 && s.allowed != nil && s.allowed(profile)
}
func digest(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func aliasMatch(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}

var canonicalOwner = regexp.MustCompile(`^(?:tmdb:(?:series|tv):[1-9][0-9]*|tvdb:(?:series:)?[1-9][0-9]*)$`)

func canonicalEpisode(ep *models.ReminderEpisode) bool {
	return ep != nil && ep.EpisodeID != "" && len(ep.EpisodeID) <= 256 && canonicalOwner.MatchString(ep.Numbering.SeriesID) && (ep.Numbering.Ordering == "" || ep.Numbering.Ordering == "official") && ep.SeasonNumber >= 0 && ep.SeasonNumber <= 10000 && ep.EpisodeNumber > 0 && ep.EpisodeNumber <= 100000
}
func episodeRef(ep models.SeriesEpisode) *models.ReminderEpisode {
	if ep.Numbering == nil {
		return nil
	}
	n := *ep.Numbering
	n.Ordering = strings.ToLower(strings.TrimSpace(n.Ordering))
	if n.Ordering == "" {
		n.Ordering = "official"
	}
	return &models.ReminderEpisode{EpisodeID: ep.ID, Numbering: n, SeasonNumber: ep.SeasonNumber, EpisodeNumber: ep.EpisodeNumber}
}
func episodeKey(ep *models.ReminderEpisode) string {
	if !canonicalEpisode(ep) {
		return ""
	}
	return digest([]any{ep.Numbering.SeriesID, "official", ep.SeasonNumber, ep.EpisodeNumber, ep.EpisodeID})
}
func coordinateKey(ep *models.ReminderEpisode) string {
	if ep == nil {
		return ""
	}
	return fmt.Sprintf("%d:%d", ep.SeasonNumber, ep.EpisodeNumber)
}
func (s *Service) List(ctx context.Context, profile, client string) ([]models.Reminder, error) {
	if !s.permitted(profile) {
		return nil, ErrForbidden
	}
	s.reconcile(ctx, profile)
	state, err := s.repo.Get(ctx, profile)
	if err != nil {
		return nil, err
	}
	if !s.permitted(profile) {
		return nil, ErrForbidden
	}
	out := []models.Reminder{}
	for _, item := range state.Subscriptions {
		if item.RevokedAt == nil {
			value := item.Reminder
			value.CreatedOnCurrentDevice = value.EventKind != models.ReminderMatchingSource || item.ClientID == client
			out = append(out, value)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}
func (s *Service) Create(ctx context.Context, profile, client string, req CreateRequest, title models.Title) (models.Reminder, *models.SourceAvailability, error) {
	var zero models.Reminder
	if title.MediaType == "tv" {
		title.MediaType = "series"
	}
	if !s.permitted(profile) {
		return zero, nil, ErrForbidden
	}
	if client == "" || len(client) > 128 || req.ContentID == "" || len(req.ContentID) > 128 || req.ContentID != title.ID || req.ContentType != title.MediaType || title.Name == "" {
		return zero, nil, ErrInvalid
	}
	switch req.EventKind {
	case models.ReminderRelease:
		if req.ContentType != "movie" || req.Episode != nil {
			return zero, nil, ErrInvalid
		}
		if req.ReleaseType == "" {
			req.ReleaseType = "digital"
		}
		if req.ReleaseType != "digital" && req.ReleaseType != "physical" && req.ReleaseType != "theatrical" {
			return zero, nil, ErrInvalid
		}
	case models.ReminderNewEpisode:
		if req.ContentType != "series" || req.Episode != nil || req.ReleaseType != "" {
			return zero, nil, ErrInvalid
		}
	case models.ReminderMatchingSource:
		if req.ReleaseType != "" || req.ContentType != "movie" && req.ContentType != "series" || req.ContentType == "movie" && req.Episode != nil || req.ContentType == "series" && !canonicalEpisode(req.Episode) {
			return zero, nil, ErrInvalid
		}
	default:
		return zero, nil, ErrInvalid
	}
	s.mu.Lock()
	delivered, hasDelivered := s.projections[projectionKey{profile, client, req.ContentType, req.ContentID}]
	s.mu.Unlock()
	freshDelivered := hasDelivered && s.now().Before(delivered.expires) && !delivered.observed.After(s.now())
	if freshDelivered {
		title = delivered.title
	}
	var verified *models.SeriesEpisode
	if req.Episode != nil {
		s.mu.Lock()
		p, ok := s.projections[projectionKey{profile, client, req.ContentType, req.ContentID}]
		ep, found := p.episodes[episodeKey(req.Episode)]
		s.mu.Unlock()
		if !ok || !found || !s.now().Before(p.expires) {
			return zero, nil, ErrReload
		}
		verified = &ep
		req.Episode = episodeRef(ep)
	}
	searchTitle := models.Title{ID: title.ID, Name: title.Name, OriginalName: title.OriginalName, MediaType: title.MediaType, Year: title.Year, Language: title.Language, IMDBID: title.IMDBID, TMDBID: title.TMDBID, TVDBID: title.TVDBID, IsDaily: title.IsDaily}
	title = titleratings.BareTitle(title)
	aliases := titleratings.Aliases(title)
	if len(aliases) == 0 {
		return zero, nil, ErrInvalid
	}
	binding := ""
	if req.EventKind == models.ReminderMatchingSource {
		binding = client
	}
	key := digest([]any{req.EventKind, req.ReleaseType, episodeKey(req.Episode), binding})
	now := s.now().UTC()
	item := models.Reminder{ID: uuid.NewString(), ContentID: title.ID, ContentType: title.MediaType, Title: title, EventKind: req.EventKind, ReleaseType: req.ReleaseType, Episode: req.Episode, State: "waiting", CreatedAt: now, CreatedOnCurrentDevice: true}
	err := s.repo.Update(ctx, profile, func(state *models.ReminderProfileState) error {
		if !s.permitted(profile) {
			return ErrForbidden
		}
		if len(state.Subscriptions) >= MaxStored || len(state.Checkpoints) >= MaxMilestones || len(state.Events) >= MaxMilestones {
			pruneRevoked(state)
		}
		active := 0
		for i := range state.Subscriptions {
			existing := &state.Subscriptions[i]
			if existing.RevokedAt == nil {
				active++
				if existing.TargetKey == key && aliasMatch(existing.Aliases, aliases) && (req.EventKind != models.ReminderMatchingSource || existing.ClientID == client) {
					if existing.Reminder.ContentID == title.ID && existing.Reminder.ContentType == title.MediaType {
						existing.Aliases = aliases
						existing.SearchTitle = searchTitle
						existing.Reminder.Title = title
					}
					item = existing.Reminder
					item.CreatedOnCurrentDevice = item.EventKind != models.ReminderMatchingSource || existing.ClientID == client
					return nil
				}
			}
		}
		if active >= MaxActive || len(state.Subscriptions) >= MaxStored {
			return ErrLimit
		}
		state.Subscriptions = append(state.Subscriptions, models.StoredReminder{Reminder: item, ClientID: client, TargetKey: key, Aliases: aliases, SearchTitle: searchTitle, ObserveAfter: now})
		if freshDelivered {
			if req.EventKind == models.ReminderRelease && dateState(delivered.releases[req.ReleaseType], now) == "upcoming" {
				state.Checkpoints = append(state.Checkpoints, models.ReminderCheckpoint{ReminderID: item.ID, MilestoneKey: req.ReleaseType, State: "upcoming", ObservedAt: delivered.observed})
			}
			if req.EventKind == models.ReminderNewEpisode {
				for _, ep := range delivered.episodes {
					date := ep.AiredDateTimeUTC
					if date == "" {
						date = ep.AiredDate
					}
					if dateState(date, now) != "upcoming" {
						continue
					}
					if len(state.Checkpoints) >= MaxMilestones {
						return ErrLimit
					}
					ref := episodeRef(ep)
					state.Checkpoints = append(state.Checkpoints, models.ReminderCheckpoint{ReminderID: item.ID, MilestoneKey: coordinateKey(ref), State: "upcoming", ObservedAt: delivered.observed, Episode: ref})
				}
			}
		}
		return nil
	})
	if err != nil {
		return zero, nil, err
	}
	var candidate *models.SourceAvailability
	s.mu.Lock()
	check := s.source
	s.mu.Unlock()
	if req.EventKind == models.ReminderMatchingSource && check != nil {
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		value := check(bounded, profile, client, searchTitle, verified)
		if bounded.Err() != nil {
			value = models.SourceAvailability{State: "unknown", Reason: "not_checked"}
		}
		cancel()
		candidate = &value
	}
	return item, candidate, nil
}
func (s *Service) Revoke(ctx context.Context, profile, id string) error {
	if !s.permitted(profile) {
		return ErrForbidden
	}
	return s.repo.Update(ctx, profile, func(state *models.ReminderProfileState) error {
		if !s.permitted(profile) {
			return ErrForbidden
		}
		for i := range state.Subscriptions {
			if state.Subscriptions[i].Reminder.ID == id {
				now := s.now().UTC()
				state.Subscriptions[i].RevokedAt = &now
				return nil
			}
		}
		return ErrNotFound
	})
}
func (s *Service) Events(ctx context.Context, profile, cursor string, limit int, unread bool) ([]models.ReminderEvent, string, error) {
	if !s.permitted(profile) {
		return nil, "", ErrForbidden
	}
	if limit < 1 || limit > 100 {
		return nil, "", ErrInvalid
	}
	state, err := s.repo.Get(ctx, profile)
	if err != nil {
		return nil, "", err
	}
	if !s.permitted(profile) {
		return nil, "", ErrForbidden
	}
	items := append([]models.ReminderEvent(nil), state.Events...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].OccurredAt.Equal(items[j].OccurredAt) {
			return items[i].ID > items[j].ID
		}
		return items[i].OccurredAt.After(items[j].OccurredAt)
	})
	var boundaryTime time.Time
	boundaryID := ""
	if cursor != "" {
		if len(cursor) > 1024 {
			return nil, "", ErrInvalid
		}
		data, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
		parts := strings.Split(string(data), "\x00")
		if decodeErr != nil || len(parts) != 3 || parts[0] != profile || len(parts[2]) > 128 {
			return nil, "", ErrInvalid
		}
		boundaryTime, decodeErr = time.Parse(time.RFC3339Nano, parts[1])
		if decodeErr != nil {
			return nil, "", ErrInvalid
		}
		boundaryID = parts[2]
	}
	visible := []models.ReminderEvent{}
	for _, event := range items {
		if cursor != "" && (!event.OccurredAt.Before(boundaryTime) && (!event.OccurredAt.Equal(boundaryTime) || event.ID >= boundaryID)) {
			continue
		}
		if event.DismissedAt == nil && (!unread || event.ReadAt == nil) {
			visible = append(visible, event)
		}
	}
	next := ""
	end := limit
	if end < len(visible) {
		last := visible[end-1]
		next = base64.RawURLEncoding.EncodeToString([]byte(profile + "\x00" + last.OccurredAt.UTC().Format(time.RFC3339Nano) + "\x00" + last.ID))
	} else {
		end = len(visible)
	}
	return visible[:end], next, nil
}
func (s *Service) Mark(ctx context.Context, profile, id string, read, dismissed *bool) (models.ReminderEvent, error) {
	var result models.ReminderEvent
	if !s.permitted(profile) {
		return result, ErrForbidden
	}
	if read == nil && dismissed == nil {
		return result, ErrInvalid
	}
	err := s.repo.Update(ctx, profile, func(state *models.ReminderProfileState) error {
		if !s.permitted(profile) {
			return ErrForbidden
		}
		for i := range state.Events {
			event := &state.Events[i]
			if event.ID != id {
				continue
			}
			now := s.now().UTC()
			if read != nil {
				event.ReadAt = nil
				if *read {
					event.ReadAt = &now
				}
			}
			if dismissed != nil {
				event.DismissedAt = nil
				if *dismissed {
					event.DismissedAt = &now
				}
			}
			result = *event
			return nil
		}
		return ErrNotFound
	})
	return result, err
}

type observation struct {
	title                                       models.Title
	episode                                     *models.ReminderEpisode
	kind, releaseType, milestone, state, client string
	observed                                    time.Time
}

func (s *Service) apply(ctx context.Context, profile string, o observation) error {
	return s.applyBatch(ctx, profile, []observation{o})
}
func (s *Service) applyBatch(ctx context.Context, profile string, observations []observation) error {
	if !s.permitted(profile) {
		return nil
	}
	return s.repo.Update(ctx, profile, func(state *models.ReminderProfileState) error {
		if !s.permitted(profile) {
			return nil
		}
		if len(state.Checkpoints)+len(observations) >= MaxMilestones || len(state.Events)+len(observations) >= MaxMilestones {
			pruneRevoked(state)
		}
		for _, o := range observations {
			if err := ctx.Err(); err != nil {
				return err
			}
			aliases := titleratings.Aliases(o.title)
			for i := range state.Subscriptions {
				stored := &state.Subscriptions[i]
				sub := &stored.Reminder
				if stored.RevokedAt == nil && o.kind != models.ReminderMatchingSource && o.observed.After(sub.CreatedAt) && o.observed.After(stored.ObserveAfter) && o.title.ID == sub.ContentID && o.title.MediaType == sub.ContentType {
					stored.Aliases = aliases
					sub.Title = titleratings.BareTitle(o.title)
					t := o.title
					stored.SearchTitle = models.Title{ID: t.ID, Name: t.Name, OriginalName: t.OriginalName, MediaType: t.MediaType, Year: t.Year, Language: t.Language, TMDBID: t.TMDBID, TVDBID: t.TVDBID, IMDBID: t.IMDBID, IsDaily: t.IsDaily}
				}
				if stored.RevokedAt != nil || sub.EventKind != o.kind || !aliasMatch(stored.Aliases, aliases) || !o.observed.After(sub.CreatedAt) || !o.observed.After(stored.ObserveAfter) {
					continue
				}
				if o.kind == models.ReminderRelease && sub.ReleaseType != o.releaseType {
					continue
				}
				if o.kind == models.ReminderMatchingSource && (stored.ClientID != o.client || episodeKey(sub.Episode) != episodeKey(o.episode)) {
					continue
				}
				if sub.State == "triggered" && o.kind != models.ReminderNewEpisode {
					continue
				}
				index := -1
				for j := range state.Checkpoints {
					if state.Checkpoints[j].ReminderID == sub.ID && state.Checkpoints[j].MilestoneKey == o.milestone {
						index = j
						break
					}
				}
				if index < 0 {
					if len(state.Checkpoints) >= MaxMilestones {
						return ErrLimit
					}
					state.Checkpoints = append(state.Checkpoints, models.ReminderCheckpoint{ReminderID: sub.ID, MilestoneKey: o.milestone})
					index = len(state.Checkpoints) - 1
				}
				checkpoint := &state.Checkpoints[index]
				if o.kind == models.ReminderNewEpisode {
					conflict := o.episode == nil
					if checkpoint.Episode != nil && episodeKey(checkpoint.Episode) != episodeKey(o.episode) {
						conflict = true
					}
					for j := range state.Checkpoints {
						prior := &state.Checkpoints[j]
						if prior.ReminderID == sub.ID && prior.Episode != nil && o.episode != nil && prior.Episode.EpisodeID == o.episode.EpisodeID && prior.MilestoneKey != o.milestone {
							prior.Ambiguous = true
							prior.State = "unknown"
							conflict = true
						}
					}
					if conflict {
						checkpoint.Ambiguous = true
					}
					if checkpoint.Episode == nil {
						checkpoint.Episode = o.episode
					}
					if checkpoint.Ambiguous {
						checkpoint.State = "unknown"
						checkpoint.ObservedAt = o.observed
						observed := o.observed
						sub.LastObservedAt = &observed
						continue
					}
				}
				if checkpoint.Fired {
					continue
				}
				if !checkpoint.ObservedAt.IsZero() && o.observed.Before(checkpoint.ObservedAt) {
					continue
				}
				fire := !checkpoint.Fired && ((o.kind == models.ReminderMatchingSource && o.state == "found") || (checkpoint.State == "upcoming" && o.state == "released"))
				if o.state == "released" && checkpoint.State != "upcoming" && o.kind != models.ReminderMatchingSource {
					checkpoint.Fired = true
				}
				checkpoint.State = o.state
				checkpoint.ObservedAt = o.observed
				observed := o.observed
				sub.LastObservedAt = &observed
				if !fire {
					continue
				}
				if len(state.Events) >= MaxMilestones {
					return ErrLimit
				}
				// Canonical milestone keys exclude schedule dates, names and provider config.
				duplicate := false
				for _, event := range state.Events {
					if event.ReminderID == sub.ID && event.MilestoneKey == o.milestone {
						duplicate = true
						break
					}
				}
				checkpoint.Fired = true
				if duplicate {
					continue
				}
				state.Events = append(state.Events, models.ReminderEvent{ID: uuid.NewString(), ReminderID: sub.ID, EventKind: o.kind, ContentID: sub.ContentID, ContentType: sub.ContentType, Title: sub.Title, Episode: o.episode, ReleaseType: o.releaseType, OccurredAt: o.observed, CandidateOnly: o.kind == models.ReminderMatchingSource, MilestoneKey: o.milestone})
				if o.kind != models.ReminderNewEpisode {
					sub.State = "triggered"
				}
			}
		}
		return nil
	})
}
func dateState(raw string, now time.Time) string {
	var at time.Time
	var err error
	if len(raw) == 10 {
		at, err = time.Parse("2006-01-02", raw)
	} else {
		at, err = time.Parse(time.RFC3339Nano, raw)
	}
	if err != nil {
		return "unknown"
	}
	if at.After(now) {
		return "upcoming"
	}
	return "released"
}
func (s *Service) ObserveTitles(ctx context.Context, profile, client string, titles []models.Title) error {
	if !s.permitted(profile) {
		return nil
	}
	if len(titles) > 100 {
		return ErrInvalid
	}
	now := s.now().UTC()
	observations := []observation{}
	for _, title := range titles {
		if title.MediaType != "movie" {
			continue
		}
		releases := append([]models.Release(nil), title.Releases...)
		if title.HomeRelease != nil {
			releases = append(releases, *title.HomeRelease)
		}
		if title.Theatrical != nil {
			releases = append(releases, *title.Theatrical)
		}
		byType := map[string]string{}
		for _, release := range releases {
			if release.Type != "digital" && release.Type != "physical" && release.Type != "theatrical" {
				continue
			}
			if old, ok := byType[release.Type]; ok && old != release.Date {
				byType[release.Type] = ""
			} else if !ok {
				byType[release.Type] = release.Date
			}
		}
		if client != "" {
			s.rememberProjection(projectionKey{profile, client, "movie", title.ID}, projection{title: title, releases: byType, observed: now, expires: now.Add(30 * time.Minute)})
		}
		for kind, date := range byType {
			observations = append(observations, observation{title: title, kind: models.ReminderRelease, releaseType: kind, milestone: kind, state: dateState(date, now), observed: now})
		}
	}
	return s.applyBatch(ctx, profile, observations)
}
func (s *Service) ObserveSeries(ctx context.Context, profile, client string, details *models.SeriesDetails) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !s.permitted(profile) || details == nil {
		return nil
	}
	if details.ActiveOrdering != "" && details.ActiveOrdering != "official" {
		return nil
	}
	count := 0
	ids := map[string]int{}
	coordinates := map[string]int{}
	episodes := []models.SeriesEpisode{}
	for _, season := range details.Seasons {
		for _, ep := range season.Episodes {
			if err := ctx.Err(); err != nil {
				return err
			}
			count++
			if count > 10000 {
				return ErrInvalid
			}
			ref := episodeRef(ep)
			if !canonicalEpisode(ref) {
				continue
			}
			ids[ep.ID]++
			coord := fmt.Sprintf("%d:%d", ep.SeasonNumber, ep.EpisodeNumber)
			coordinates[coord]++
			episodes = append(episodes, ep)
		}
	}
	now := s.now().UTC()
	verified := map[string]models.SeriesEpisode{}
	observations := []observation{}
	for _, ep := range episodes {
		if ids[ep.ID] != 1 || coordinates[fmt.Sprintf("%d:%d", ep.SeasonNumber, ep.EpisodeNumber)] != 1 {
			observations = append(observations, observation{title: details.Title, kind: models.ReminderNewEpisode, milestone: fmt.Sprintf("%d:%d", ep.SeasonNumber, ep.EpisodeNumber), state: "unknown", observed: now})
			continue
		}
		ref := episodeRef(ep)
		verified[episodeKey(ref)] = models.SeriesEpisode{ID: ep.ID, Numbering: &ref.Numbering, SeasonNumber: ep.SeasonNumber, EpisodeNumber: ep.EpisodeNumber, AiredDate: ep.AiredDate, AiredDateTimeUTC: ep.AiredDateTimeUTC, AbsoluteEpisodeNumber: ep.AbsoluteEpisodeNumber, SourceDateUnique: ep.SourceDateUnique}
		date := ep.AiredDateTimeUTC
		if date == "" {
			date = ep.AiredDate
		}
		observations = append(observations, observation{title: details.Title, episode: ref, kind: models.ReminderNewEpisode, milestone: coordinateKey(ref), state: dateState(date, now), observed: now})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.rememberProjection(projectionKey{profile, client, "series", details.Title.ID}, projection{episodes: verified, title: details.Title, observed: now, expires: now.Add(30 * time.Minute)})
	return s.applyBatch(ctx, profile, observations)
}

func (s *Service) rememberProjection(key projectionKey, value projection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for key, p := range s.projections {
		if !now.Before(p.expires) {
			delete(s.projections, key)
		}
	}
	if len(s.projections) >= 2000 {
		if _, ok := s.projections[key]; !ok {
			for oldest := range s.projections {
				delete(s.projections, oldest)
				break
			}
		}
	}
	// Retain only the search inputs and dates needed for creation, never artwork,
	// progress, media URLs or a mutable metadata-owned slice.
	t := value.title
	value.title = models.Title{ID: t.ID, Name: t.Name, OriginalName: t.OriginalName, MediaType: t.MediaType, Year: t.Year, Language: t.Language, IMDBID: t.IMDBID, TMDBID: t.TMDBID, TVDBID: t.TVDBID, IsDaily: t.IsDaily}
	s.projections[key] = value
}

// ObserveSource is called with a currently revalidated cached candidate. Its
// timestamp comes from the original check, never the time a cache was restored.
func (s *Service) ObserveSource(ctx context.Context, profile, client string, title models.Title, episode *models.SeriesEpisode, value models.SourceAvailability) error {
	if !s.permitted(profile) || value.State != "found" || value.CheckedAt == nil || value.ExpiresAt == nil {
		return nil
	}
	now := s.now().UTC()
	if value.CheckedAt.After(now) || !value.ExpiresAt.After(now) || !value.ExpiresAt.After(*value.CheckedAt) || value.ExpiresAt.Sub(*value.CheckedAt) > 24*time.Hour {
		return nil
	}
	var ref *models.ReminderEpisode
	if title.MediaType == "series" || title.MediaType == "tv" {
		if episode == nil {
			return nil
		}
		ref = episodeRef(*episode)
		if !canonicalEpisode(ref) {
			return nil
		}
	}
	if value.SourceType == "local" && (value.Reason != "local_file_checked" || value.ExpiresAt.Sub(*value.CheckedAt) > 2*time.Minute) {
		return nil
	}
	if value.SourceType != "" && value.SourceType != "local" {
		return nil
	}
	return s.apply(ctx, profile, observation{title: title, episode: ref, kind: models.ReminderMatchingSource, milestone: "first_matching_candidate", state: "found", client: client, observed: value.CheckedAt.UTC()})
}

// Reconciliation reads only existing scoped evidence; it creates no demands or
// polling. Restored controls have a new ObserveAfter boundary, so an imported
// timestamp cannot turn old cached proof into a fresh candidate event.
func (s *Service) reconcile(ctx context.Context, profile string) {
	s.mu.Lock()
	check := s.source
	s.mu.Unlock()
	if check == nil {
		return
	}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	state, err := s.repo.Get(bounded, profile)
	if err != nil {
		return
	}
	for _, stored := range state.Subscriptions {
		if bounded.Err() != nil {
			return
		}
		if stored.RevokedAt != nil || stored.Reminder.State != "waiting" || stored.Reminder.EventKind != models.ReminderMatchingSource {
			continue
		}
		var episode *models.SeriesEpisode
		if ref := stored.Reminder.Episode; ref != nil {
			episode = &models.SeriesEpisode{ID: ref.EpisodeID, Numbering: &ref.Numbering, SeasonNumber: ref.SeasonNumber, EpisodeNumber: ref.EpisodeNumber}
		}
		value := check(bounded, profile, stored.ClientID, stored.SearchTitle, episode)
		_ = s.ObserveSource(bounded, profile, stored.ClientID, stored.SearchTitle, episode, value)
	}
}

// Only explicitly revoked generations with no visible inbox items can be
// pruned. New generations get a new ID/time boundary, so old proof cannot be
// replayed into them. Active dedup receipts are never removed by quota cleanup.
func pruneRevoked(state *models.ReminderProfileState) {
	visible := map[string]bool{}
	for _, event := range state.Events {
		if event.DismissedAt == nil {
			visible[event.ReminderID] = true
		}
	}
	removed := map[string]bool{}
	subs := state.Subscriptions[:0]
	for _, sub := range state.Subscriptions {
		if sub.RevokedAt != nil && !visible[sub.Reminder.ID] {
			removed[sub.Reminder.ID] = true
		} else {
			subs = append(subs, sub)
		}
	}
	state.Subscriptions = subs
	checkpoints := state.Checkpoints[:0]
	for _, item := range state.Checkpoints {
		if !removed[item.ReminderID] {
			checkpoints = append(checkpoints, item)
		}
	}
	state.Checkpoints = checkpoints
	events := state.Events[:0]
	for _, event := range state.Events {
		if removed[event.ReminderID] {
			delete(state.EventKeys, event.ID)
		} else {
			events = append(events, event)
		}
	}
	state.Events = events
}

// RebuildRestoredIntent treats imported derived hashes as untrusted. Only the
// explicit validated control fields determine idempotency and device binding.
func RebuildRestoredIntent(stored *models.StoredReminder) error {
	sub := &stored.Reminder
	if sub.ID == "" || len(sub.ID) > 128 || sub.ContentID == "" || len(sub.ContentID) > 128 || (sub.ContentType != "movie" && sub.ContentType != "series") || (sub.State != "waiting" && sub.State != "triggered") {
		return ErrInvalid
	}
	switch sub.EventKind {
	case models.ReminderRelease:
		if sub.ContentType != "movie" || sub.Episode != nil {
			return ErrInvalid
		}
		if sub.ReleaseType == "" {
			sub.ReleaseType = "digital"
		}
		if sub.ReleaseType != "digital" && sub.ReleaseType != "physical" && sub.ReleaseType != "theatrical" {
			return ErrInvalid
		}
	case models.ReminderNewEpisode:
		if sub.ContentType != "series" || sub.Episode != nil || sub.ReleaseType != "" {
			return ErrInvalid
		}
	case models.ReminderMatchingSource:
		if sub.ReleaseType != "" || stored.ClientID == "" || len(stored.ClientID) > 128 || sub.ContentType == "movie" && sub.Episode != nil || sub.ContentType == "series" && !canonicalEpisode(sub.Episode) {
			return ErrInvalid
		}
		if sub.Episode != nil && sub.Episode.Numbering.Ordering == "" {
			copy := *sub.Episode
			copy.Numbering.Ordering = "official"
			sub.Episode = &copy
		}
	default:
		return ErrInvalid
	}
	binding := ""
	if sub.EventKind == models.ReminderMatchingSource {
		binding = stored.ClientID
	}
	stored.TargetKey = digest([]any{sub.EventKind, sub.ReleaseType, episodeKey(sub.Episode), binding})
	sub.CreatedOnCurrentDevice = false
	return nil
}
