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
	"novastream/models"
	historypkg "novastream/services/history"
)

type cachedPopularTitlesReader interface {
	SnapshotPopularWatches(context.Context, string, map[string]models.User, int, time.Time) (*historypkg.RecentWatchesSnapshot, error)
	cachedSharedActivityHistory
}

func hasCachedPopularTitlesSource(r *http.Request) bool {
	for _, source := range r.URL.Query()["source"] {
		if strings.EqualFold(strings.TrimSpace(source), "popular-on-server-cached") {
			return true
		}
	}
	return false
}

// Read only the existing activity window. Manager.Load may save defaults.
func (h *DisplayListHandler) cachedPopularWindowDays() int {
	if h.MetadataHandler.CfgManager == nil {
		return 90
	}
	file, err := os.Open(h.MetadataHandler.CfgManager.ConfigPath())
	if err != nil {
		return 90
	}
	defer file.Close()
	var settings struct {
		HomeShelves struct {
			Window int `json:"popularOnServerWindowDays"`
		} `json:"homeShelves"`
	}
	decoder := json.NewDecoder(file)
	if decoder.Decode(&settings) != nil {
		return 90
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return 90
	}
	if settings.HomeShelves.Window >= 7 && settings.HomeShelves.Window <= 365 {
		return settings.HomeShelves.Window
	}
	return 90
}

type cachedPopularGroup struct {
	key                           string
	title                         models.Title
	languageID                    string
	views                         int
	lastWatched, representativeAt time.Time
	representativeTie             string
}

// The caller supplies the strict primary-only projection, never saved side IDs.
// The public representative keeps its exact stored ID; only the cache lookup
// folds known forms of the same proven provider identity.
func cachedPopularPrimaryIdentity(title models.Title) (string, string) {
	provider, value := "", ""
	switch {
	case title.TMDBID > 0:
		provider, value = "tmdb", strconv.FormatInt(title.TMDBID, 10)
	case title.TVDBID > 0:
		provider, value = "tvdb", strconv.FormatInt(title.TVDBID, 10)
	case title.IMDBID != "":
		provider, value = "imdb", title.IMDBID
	}
	if provider == "" {
		return "", ""
	}
	languageID := provider + ":" + title.MediaType + ":" + value
	if provider == "imdb" {
		languageID = "imdb:" + value
	}
	return title.MediaType + "\x00" + provider + "\x00" + value, languageID
}

func (h *DisplayListHandler) getCachedPopularTitles(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	viewer := strings.TrimSpace(mux.Vars(r)["userID"])
	query, err := validateCachedPopularTitlesQuery(r, viewer)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	codes := cachedPopularActivityCodes
	fail := func(status int, code string) { codes.fail(w, status, code) }
	var reader cachedPopularTitlesReader
	if h.HistoryHandler != nil {
		reader, _ = h.HistoryHandler.Service.(cachedPopularTitlesReader)
	}
	observation, ok := h.captureCachedSharedActivity(w, r, viewer, codes, reader != nil)
	if !ok {
		return
	}
	window := query.windowDays
	if window == 0 {
		window = h.cachedPopularWindowDays()
	}
	at := time.Now()
	snapshot, err := reader.SnapshotPopularWatches(r.Context(), viewer, observation.eligible, window, at)
	if r.Context().Err() != nil {
		return
	}
	if err != nil || snapshot == nil || !snapshot.ObservedAt.Equal(at) {
		fail(409, codes.changed)
		return
	}
	if !observation.captureHidden(w) {
		return
	}
	groups := make(map[string]*cachedPopularGroup)
	for _, row := range snapshot.Items {
		if r.Context().Err() != nil {
			return
		}
		if row.Group <= 0 {
			fail(409, codes.changed)
			return
		}
		title, valid := cachedRecentTitle(row.Watch)
		if !valid {
			continue
		}
		key, languageID := cachedPopularPrimaryIdentity(title)
		if key == "" {
			continue
		}
		group := groups[key]
		if group == nil {
			group = &cachedPopularGroup{key: key, languageID: languageID}
			groups[key] = group
		}
		group.views++
		if row.Watch.WatchedAt.After(group.lastWatched) {
			group.lastWatched = row.Watch.WatchedAt
		}
		tie := title.ID + "\x00" + title.Name + "\x00" + row.Watch.ItemID
		if strings.TrimSpace(title.Name) != "" && (group.title.ID == "" || row.Watch.WatchedAt.After(group.representativeAt) || row.Watch.WatchedAt.Equal(group.representativeAt) && tie < group.representativeTie) {
			group.title = title
			group.representativeAt = row.Watch.WatchedAt
			group.representativeTie = tie
		}
	}
	matches := originalLanguageMatcherAt(observation.languages, query.language, at)
	membership := map[cachedSharedLanguageIdentity]bool{}
	selected := make([]*cachedPopularGroup, 0, len(groups))
	facets := make([]models.TrendingItem, 0, len(groups))
	for _, group := range groups {
		if r.Context().Err() != nil {
			return
		}
		if group.views < query.minimumViews || group.title.ID == "" || observation.hidden.hides(group.title) {
			continue
		}
		if query.hideWatched {
			watched, err := observation.history.GetWatchHistoryItem(viewer, group.title.MediaType, buildItemIDForHistory(models.TrendingItem{Title: group.title}))
			if err != nil {
				fail(503, codes.unavailable)
				return
			}
			if watched != nil && watched.Watched {
				continue
			}
		}
		if query.language.active() {
			identity := cachedSharedLanguageIdentity{group.languageID, group.title.MediaType}
			membership[identity] = matches(identity.id, identity.kind)
			if !membership[identity] {
				continue
			}
		}
		if query.filter.MediaType != "" && query.filter.MediaType != "all" && query.filter.MediaType != group.title.MediaType {
			continue
		}
		if query.filter.Title != "" && !strings.Contains(strings.ToLower(group.title.Name), strings.ToLower(query.filter.Title)) {
			continue
		}
		group.title.CardSubtitle = strconv.Itoa(group.views) + " " + pluralizeView(group.views)
		group.title.ForceTitleOverlay = true
		facets = append(facets, models.TrendingItem{Rank: group.views, Title: group.title})
		if query.filter.Alphabet != "" && displayListAlphabetBucket(group.title.Name) != query.filter.Alphabet {
			continue
		}
		selected = append(selected, group)
	}
	defaultLess := func(a, b *cachedPopularGroup, ascending bool) bool {
		if a.views != b.views {
			if ascending {
				return a.views < b.views
			}
			return a.views > b.views
		}
		if !a.lastWatched.Equal(b.lastWatched) {
			return a.lastWatched.After(b.lastWatched)
		}
		an, bn := sortableDisplayListName(a.title.Name), sortableDisplayListName(b.title.Name)
		if an != bn {
			return an < bn
		}
		return a.key < b.key
	}
	sort.Slice(selected, func(i, j int) bool {
		a, b := selected[i], selected[j]
		if query.filter.SortBy == "name" {
			an, bn := sortableDisplayListName(a.title.Name), sortableDisplayListName(b.title.Name)
			if an != bn {
				if query.filter.SortDirection == "desc" {
					return an > bn
				}
				return an < bn
			}
			return defaultLess(a, b, false)
		}
		return defaultLess(a, b, query.filter.SortDirection == "asc")
	})
	total := len(selected)
	items := make([]models.TrendingItem, 0)
	if query.offset < len(selected) {
		page := selected[query.offset:]
		if query.limit > 0 && query.limit < len(page) {
			page = page[:query.limit]
		}
		for _, group := range page {
			items = append(items, models.TrendingItem{Rank: group.views, Title: group.title})
		}
	}
	configurationCurrent := func() bool { return query.windowDays != 0 || h.cachedPopularWindowDays() == window }
	if !observation.recheck(w, r, snapshot, query.language, membership, configurationCurrent) {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DisplayListResponse{Source: "popular-on-server-cached", Items: items, Total: total, AlphabetBuckets: displayListAlphabetBuckets(facets, displayListQueryOptions{}), OriginalLanguageFilterSupported: true})
}
