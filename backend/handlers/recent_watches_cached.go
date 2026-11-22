package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/mediaidentity"
	"novastream/models"
	historypkg "novastream/services/history"
)

type cachedRecentWatchesReader interface {
	SnapshotRecentWatches(context.Context, string, map[string]models.User, int, time.Time) (*historypkg.RecentWatchesSnapshot, error)
	cachedSharedActivityHistory
}

func hasCachedRecentWatchesSource(r *http.Request) bool {
	for _, source := range r.URL.Query()["source"] {
		if strings.EqualFold(strings.TrimSpace(source), "recently-watched-cached") {
			return true
		}
	}
	return false
}

// This reads only the existing shelf cap. Manager.Load can persist defaults and
// normalize unrelated settings, so it must not be called by this read endpoint.
func (h *DisplayListHandler) cachedRecentProfileCap() (int, error) {
	if h.MetadataHandler.CfgManager == nil {
		return 3, nil
	}
	file, err := os.Open(h.MetadataHandler.CfgManager.ConfigPath())
	if os.IsNotExist(err) {
		return 3, nil
	}
	if err != nil {
		return 3, nil
	}
	defer file.Close()
	var settings struct {
		HomeShelves struct {
			Cap int `json:"recentlyWatchedCapPerProfile"`
		} `json:"homeShelves"`
	}
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&settings); err != nil {
		return 3, nil
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return 3, nil
	}
	if settings.HomeShelves.Cap >= 1 && settings.HomeShelves.Cap <= 20 {
		return settings.HomeShelves.Cap, nil
	}
	return 3, nil
}

// Saved side IDs can be imported or supplied by a caller. Only the primary
// stored movie/series identity may supply catalogue IDs to the public card.
func cachedRecentTitle(watch models.RecentWatch) (models.Title, bool) {
	id, name, kind := watch.ItemID, watch.Name, "movie"
	switch watch.MediaType {
	case "movie":
	case "series":
		kind = "series"
	case "episode":
		if watch.ItemID == "" || watch.ItemID != strings.TrimSpace(watch.ItemID) || watch.ItemID != mediaidentity.SanitizeID(watch.ItemID) || strings.ContainsAny(watch.ItemID, "\x00\r\n") || watch.SeasonNumber < 0 || watch.EpisodeNumber < 0 {
			return models.Title{}, false
		}
		if seriesID, season, episode, encoded := mediaidentity.ParseEpisodeID(watch.ItemID); encoded {
			if models.CanonicalSeriesIdentity(seriesID) != models.CanonicalSeriesIdentity(watch.SeriesID) || season != watch.SeasonNumber || episode != watch.EpisodeNumber {
				return models.Title{}, false
			}
		}
		id, name, kind = watch.SeriesID, watch.SeriesName, "series"
	default:
		return models.Title{}, false
	}
	if id == "" || id != strings.TrimSpace(id) || id != mediaidentity.SanitizeID(id) || strings.ContainsAny(id, "\x00\r\n") {
		return models.Title{}, false
	}
	title := models.Title{ID: id, Name: name, MediaType: kind, ForceTitleOverlay: true}
	parts := strings.Split(id, ":")
	positive := func(raw string) int64 {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value <= 0 || strconv.FormatInt(value, 10) != raw {
			return 0
		}
		return value
	}
	if len(parts) == 3 && (parts[0] == "tmdb" || parts[0] == "tvdb") {
		idKind := parts[1]
		if idKind == "tv" {
			idKind = "series"
		}
		value := positive(parts[2])
		if idKind != kind || value == 0 {
			return models.Title{}, false
		}
		if parts[0] == "tmdb" {
			title.TMDBID = value
		} else {
			title.TVDBID = value
		}
	} else if len(parts) == 2 && (parts[0] == "tmdb" || parts[0] == "tvdb") {
		value := positive(parts[1])
		if value == 0 {
			return models.Title{}, false
		}
		if parts[0] == "tmdb" {
			title.TMDBID = value
		} else {
			title.TVDBID = value
		}
	} else {
		imdb := id
		if len(parts) == 2 && parts[0] == "imdb" {
			imdb = parts[1]
		}
		if strings.HasPrefix(imdb, "tt") && len(imdb) >= 3 && len(imdb) <= 16 {
			valid := true
			for _, char := range imdb[2:] {
				valid = valid && char >= '0' && char <= '9'
			}
			if valid {
				title.IMDBID = imdb
			}
		}
		if title.IMDBID == "" {
			return models.Title{}, false
		}
	}
	if watch.IsAnonymous {
		watch.UserID, watch.UserName = "", "Fellow user"
	}
	title.CardSubtitle = recentWatchSubtitle(watch)
	return title, true
}

type cachedRecentCard struct {
	group int
	at    time.Time
	item  models.TrendingItem
	tie   string
}

func (h *DisplayListHandler) getCachedRecentWatches(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	viewer := strings.TrimSpace(mux.Vars(r)["userID"])
	query, err := validateCachedRecentWatchesQuery(r, viewer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	fail := func(status int, code string) { cachedRecentActivityCodes.fail(w, status, code) }
	var reader cachedRecentWatchesReader
	if h.HistoryHandler != nil {
		reader, _ = h.HistoryHandler.Service.(cachedRecentWatchesReader)
	}
	observation, ok := h.captureCachedSharedActivity(w, r, viewer, cachedRecentActivityCodes, reader != nil)
	if !ok {
		return
	}
	cap := query.perProfileCap
	if cap == 0 {
		cap, err = h.cachedRecentProfileCap()
		if err != nil {
			fail(503, "recent_watches_reader_unavailable")
			return
		}
	}
	at := time.Now()
	snapshot, err := reader.SnapshotRecentWatches(r.Context(), viewer, observation.eligible, query.windowDays, at)
	if r.Context().Err() != nil {
		return
	}
	if err != nil || snapshot == nil || !snapshot.ObservedAt.Equal(at) {
		fail(409, "recent_watches_snapshot_unavailable")
		return
	}
	if !observation.captureHidden(w) {
		return
	}
	media, err := h.requestStoredMediaMatcher(r, viewer)
	if err != nil {
		fail(503, "stored_media_unavailable")
		return
	}
	membership := map[cachedSharedLanguageIdentity]bool{}
	matches := originalLanguageMatcherAt(observation.languages, query.language, at)
	cards := make([]cachedRecentCard, 0, len(snapshot.Items))
	for _, row := range snapshot.Items {
		if r.Context().Err() != nil {
			return
		}
		if row.Group <= 0 {
			fail(409, "recent_watches_snapshot_unavailable")
			return
		}
		title, valid := cachedRecentTitle(row.Watch)
		if !valid || observation.hidden.hides(title) {
			continue
		}
		if query.filter.MediaType != "" && query.filter.MediaType != "all" && title.MediaType != query.filter.MediaType {
			continue
		}
		if query.filter.Title != "" && !strings.Contains(strings.ToLower(title.Name), strings.ToLower(query.filter.Title)) {
			continue
		}
		if query.hideWatched {
			watched, err := reader.GetWatchHistoryItem(viewer, title.MediaType, buildItemIDForHistory(models.TrendingItem{Title: title}))
			if err != nil {
				fail(503, "recent_watches_reader_unavailable")
				return
			}
			if watched != nil && watched.Watched {
				continue
			}
		}
		if !media.matches(title) {
			continue
		}
		if query.language.active() {
			key := cachedSharedLanguageIdentity{title.ID, title.MediaType}
			membership[key] = matches(key.id, key.kind)
			if !membership[key] {
				continue
			}
		}
		cards = append(cards, cachedRecentCard{row.Group, row.Watch.WatchedAt, models.TrendingItem{Title: title}, title.MediaType + "\x00" + title.ID + "\x00" + row.Watch.ItemID})
	}
	// Select each contributor's newest qualifying rows before response sorting.
	sort.SliceStable(cards, func(i, j int) bool {
		if !cards[i].at.Equal(cards[j].at) {
			return cards[i].at.After(cards[j].at)
		}
		if cards[i].tie != cards[j].tie {
			return cards[i].tie < cards[j].tie
		}
		return cards[i].group < cards[j].group
	})
	counts := make(map[int]int)
	facetItems := make([]models.TrendingItem, 0, len(cards))
	selected := make([]cachedRecentCard, 0, len(cards))
	for _, card := range cards {
		// Facets retain every eligible letter before the per-contributor cap.
		// Selection applies the letter first so an older matching row can
		// replace a newer row of another letter.
		facetItems = append(facetItems, card.item)
		if query.filter.Alphabet != "" && displayListAlphabetBucket(card.item.Title.Name) != query.filter.Alphabet {
			continue
		}
		if counts[card.group] < cap {
			counts[card.group]++
			selected = append(selected, card)
		}
	}
	if query.filter.SortBy == "name" {
		sort.SliceStable(selected, func(i, j int) bool {
			a, b := sortableDisplayListName(selected[i].item.Title.Name), sortableDisplayListName(selected[j].item.Title.Name)
			if a == b {
				return false
			}
			if query.filter.SortDirection == "desc" {
				return a > b
			}
			return a < b
		})
	} else if query.filter.SortDirection == "asc" {
		sort.SliceStable(selected, func(i, j int) bool { return selected[i].at.Before(selected[j].at) })
	}
	items := make([]models.TrendingItem, len(selected))
	for i, card := range selected {
		items[i] = card.item
		items[i].Rank = i + 1
	}
	alphabet := displayListAlphabetBuckets(facetItems, displayListQueryOptions{})
	total := len(items)
	if query.offset >= len(items) {
		items = []models.TrendingItem{}
	} else {
		items = items[query.offset:]
		if query.limit > 0 && query.limit < len(items) {
			items = items[:query.limit]
		}
	}
	configurationCurrent := func() bool {
		if query.perProfileCap != 0 {
			return true
		}
		currentCap, err := h.cachedRecentProfileCap()
		return err == nil && currentCap == cap
	}
	if !observation.recheck(w, r, snapshot, query.language, membership, configurationCurrent) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DisplayListResponse{Source: "recently-watched-cached", Items: items, Total: total, AlphabetBuckets: alphabet, OriginalLanguageFilterSupported: true, StoredMediaFilterSupported: h.storedMediaReady(), StoredMediaFilterBasis: storedListMediaBasis(media.filter.Active())})
}
