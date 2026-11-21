package metadata

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"novastream/models"
)

// EpisodeDateCatalogueMatch describes an observed provider catalogue only. It
// cannot identify the bytes of a file, authorize a source, or prove availability.
type EpisodeDateCatalogueMatch struct {
	Provider                    string
	Numbering                   models.EpisodeNumbering
	EpisodeID                   string
	SeasonNumber, EpisodeNumber int
	AiredDate                   string
	ObservedAt, ExpiresAt       time.Time
}

var errEpisodeDateCatalogueUnavailable = errors.New("episode date catalogue unavailable")

const (
	episodeCatalogueMaxEntries = 64
	episodeCatalogueMaxRows    = 32768
	episodeCatalogueMaxBytes   = 32 << 20
)

type episodeDateCoordinate struct {
	id                    int64
	season, number, count int
}
type episodeDateEntry struct {
	series            int64
	language          string
	rows              []tvdbEpisode
	dates             map[string]episodeDateCoordinate
	observed, expires time.Time
	bytes             int64
}
type episodeDateCache struct {
	mu         sync.RWMutex
	client     *tvdbClient
	generation uint64
	ttl        time.Duration
	entries    map[int64]*episodeDateEntry
	rows       int
	bytes      int64
	slots      chan struct{}
	active     map[*episodeDateWork]struct{}
}
type episodeDateWork struct {
	owner      *episodeDateCache
	client     *tvdbClient
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	once       sync.Once
}

// Only the actual complete HTTP producer returns this private observation.
// No exported setter accepts a caller's list, completeness flag, or timestamp.
type episodeCatalogueObservation struct {
	work               *episodeDateWork
	series             int64
	ordering, language string
	rows               []tvdbEpisode
	observed           time.Time
}

func newEpisodeDateCache(client *tvdbClient, ttlHours int) *episodeDateCache {
	if ttlHours <= 0 || ttlHours > 24 {
		ttlHours = 24
	}
	return &episodeDateCache{client: client, generation: 1, ttl: time.Duration(ttlHours) * time.Hour, entries: make(map[int64]*episodeDateEntry), slots: make(chan struct{}, 4), active: make(map[*episodeDateWork]struct{})}
}
func (c *episodeDateCache) reset(client *tvdbClient) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.client = client
	c.generation++
	c.entries = make(map[int64]*episodeDateEntry)
	c.rows = 0
	c.bytes = 0
	// Signaling is not completion. A retired operation keeps its producer slot
	// until its actual response/read/Close stack returns to finish.
	for work := range c.active {
		work.cancel()
	}
}
func (c *episodeDateCache) begin(original context.Context) (*episodeDateWork, error) {
	if original == nil || original.Err() != nil {
		return nil, errEpisodeDateCatalogueUnavailable
	}
	select {
	case c.slots <- struct{}{}:
	default:
		return nil, errEpisodeDateCatalogueUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if original.Err() != nil || c.client == nil || !c.client.isConfigured() {
		<-c.slots
		return nil, errEpisodeDateCatalogueUnavailable
	}
	ctx, cancel := context.WithCancel(original)
	work := &episodeDateWork{owner: c, client: c.client, generation: c.generation, ctx: ctx, cancel: cancel}
	c.active[work] = struct{}{}
	return work, nil
}
func (w *episodeDateWork) finish() {
	w.once.Do(func() { w.cancel(); w.owner.mu.Lock(); delete(w.owner.active, w); w.owner.mu.Unlock(); <-w.owner.slots })
}
func copyEpisodeDateRows(rows []tvdbEpisode) []tvdbEpisode {
	copied := append([]tvdbEpisode(nil), rows...)
	for i := range copied {
		copied[i].Translations = append([]tvdbEpisodeTranslation(nil), copied[i].Translations...)
	}
	return copied
}
func canonicalEpisodeDate(value string) (time.Time, bool) {
	date, err := time.Parse("2006-01-02", value)
	return date, err == nil && date.Year() > 0 && date.Format("2006-01-02") == value
}
func episodeDateSeries(numbering models.EpisodeNumbering) (int64, bool) {
	order := strings.ToLower(strings.TrimSpace(numbering.Ordering))
	if order != "" && order != "official" {
		return 0, false
	}
	series := models.CanonicalSeriesIdentity(numbering.SeriesID)
	if !strings.HasPrefix(series, "tvdb:series:") {
		return 0, false
	}
	id, err := strconv.ParseInt(strings.TrimPrefix(series, "tvdb:series:"), 10, 64)
	return id, err == nil && id > 0 && series == fmt.Sprintf("tvdb:series:%d", id)
}

// GetCachedDateEpisodeMatch never fetches, writes, warms or renews. Its copied
// DATA is not a permission or a file-to-episode association.
func (s *Service) GetCachedDateEpisodeMatch(ctx context.Context, numbering models.EpisodeNumbering, airedDate string) (EpisodeDateCatalogueMatch, bool) {
	return s.cachedDateEpisodeMatchAt(ctx, numbering, airedDate, time.Now())
}

// The private pure-time predicate also permits checking an already captured
// observation at its original fixed expiry without renewing or editing it.
func (s *Service) cachedDateEpisodeMatchAt(ctx context.Context, numbering models.EpisodeNumbering, airedDate string, now time.Time) (EpisodeDateCatalogueMatch, bool) {
	var empty EpisodeDateCatalogueMatch
	if s == nil || s.episodeDates == nil || ctx == nil || ctx.Err() != nil {
		return empty, false
	}
	series, ok := episodeDateSeries(numbering)
	date, valid := canonicalEpisodeDate(airedDate)
	today := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	if !ok || !valid || date.After(today) {
		return empty, false
	}
	c := s.episodeDates
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry := c.entries[series]
	if c.client == nil || !c.client.isConfigured() || entry == nil || entry.observed.After(now) || !entry.expires.After(now) || ctx.Err() != nil {
		return empty, false
	}
	episode := entry.dates[airedDate]
	if episode.count != 1 || episode.season <= 0 || episode.number <= 0 || episode.id <= 0 {
		return empty, false
	}
	return EpisodeDateCatalogueMatch{Provider: "tvdb", Numbering: models.EpisodeNumbering{SeriesID: fmt.Sprintf("tvdb:series:%d", series), Ordering: "official"}, EpisodeID: fmt.Sprintf("tvdb:episode:%d", episode.id), SeasonNumber: episode.season, EpisodeNumber: episode.number, AiredDate: airedDate, ObservedAt: entry.observed, ExpiresAt: entry.expires}, true
}
func (s *Service) episodeCatalogueLanguage() string {
	if s.episodeLanguage != "" {
		return s.episodeLanguage
	}
	if s.episodeDates != nil {
		s.episodeDates.mu.RLock()
		defer s.episodeDates.mu.RUnlock()
		if s.episodeDates.client != nil {
			return s.episodeDates.client.language
		}
	}
	if s.client != nil {
		return s.client.language
	}
	return "eng"
}
func (c *episodeDateCache) cachedRows(ctx context.Context, series int64, language string) ([]tvdbEpisode, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry := c.entries[series]
	now := time.Now()
	if ctx.Err() != nil || c.client == nil || !c.client.isConfigured() || entry == nil || entry.language != language || entry.observed.After(now) || !entry.expires.After(now) {
		return nil, false
	}
	return copyEpisodeDateRows(entry.rows), true
}
func (c *episodeDateCache) publish(work *episodeDateWork, observation *episodeCatalogueObservation) {
	if observation == nil || observation.work != work || observation.ordering != "official" || observation.series <= 0 || work.ctx.Err() != nil {
		return
	}
	entry := &episodeDateEntry{series: observation.series, language: observation.language, dates: make(map[string]episodeDateCoordinate), observed: observation.observed, expires: observation.observed.Add(c.ttl)}
	ids := make(map[int64]bool)
	coordinates := make(map[[2]int]bool)
	for _, row := range observation.rows {
		if work.ctx.Err() != nil {
			return
		}
		key := [2]int{row.SeasonNumber, row.Number}
		if row.ID <= 0 || row.SeriesID != observation.series || row.SeasonNumber < 0 || row.Number <= 0 || ids[row.ID] || coordinates[key] {
			return
		}
		if _, ok := canonicalEpisodeDate(row.Aired); !ok {
			return
		}
		ids[row.ID] = true
		coordinates[key] = true
		value := entry.dates[row.Aired]
		value.id = row.ID
		value.season = row.SeasonNumber
		value.number = row.Number
		value.count++
		entry.dates[row.Aired] = value
		// Conservative per-record/index/map charge plus retained string/slice
		// bytes. Whole entries are evicted; a complete list is never trimmed.
		entry.bytes += 1024 + int64(len(row.Name)+len(row.Abbreviation)+len(row.Overview)+len(row.Aired)+len(row.Image))
		for _, translation := range row.Translations {
			entry.bytes += 128 + int64(len(translation.Language)+len(translation.Name)+len(translation.Overview))
		}
		if entry.bytes > episodeCatalogueMaxBytes {
			return
		}
	}
	if len(observation.rows) > episodeCatalogueMaxRows {
		return
	}
	entry.rows = copyEpisodeDateRows(observation.rows)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, active := c.active[work]; !active || work.ctx.Err() != nil || c.client != work.client || c.generation != work.generation || entry.observed.After(time.Now()) || !entry.expires.After(time.Now()) {
		return
	}
	if old := c.entries[entry.series]; old != nil {
		delete(c.entries, entry.series)
		c.rows -= len(old.rows)
		c.bytes -= old.bytes
	}
	for len(c.entries) >= episodeCatalogueMaxEntries || c.rows+len(entry.rows) > episodeCatalogueMaxRows || c.bytes+entry.bytes > episodeCatalogueMaxBytes {
		var oldest *episodeDateEntry
		for _, candidate := range c.entries {
			if oldest == nil || candidate.observed.Before(oldest.observed) || candidate.observed.Equal(oldest.observed) && candidate.series < oldest.series {
				oldest = candidate
			}
		}
		if oldest == nil {
			return
		}
		delete(c.entries, oldest.series)
		c.rows -= len(oldest.rows)
		c.bytes -= oldest.bytes
	}
	c.entries[entry.series] = entry
	c.rows += len(entry.rows)
	c.bytes += entry.bytes
}
