package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/mediaidentity"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/customlists"
	historypkg "novastream/services/history"
	metadatapkg "novastream/services/metadata"
	"novastream/services/playback"
	"novastream/services/watchlist"

	"github.com/gorilla/mux"
)

type DisplayListHandler struct {
	storedMediaLocal   storedListLocalMedia
	storedMediaRemote  storedListRemoteMedia
	storedMediaAccess  storedListLibraryAccess
	WatchlistService   watchlistService
	CustomListsService customListsService
	Users              userService
	HistoryService     historyService
	HistoryHandler     *HistoryHandler
	MetadataService    metadataService
	MetadataHandler    *MetadataHandler
	HiddenItemsService hiddenItemsService
	PrequeueStore      persistentPrequeueStore
}

type persistentPrequeueStore interface {
	ListAll() []*playback.PrequeueEntry
}

type DisplayListResponse struct {
	StoredMediaFilterSupported      bool        `json:"storedMediaFilterSupported,omitempty"`
	StoredMediaFilterBasis          string      `json:"storedMediaFilterBasis,omitempty"`
	Source                          string      `json:"source"`
	ListID                          string      `json:"listId,omitempty"`
	Items                           interface{} `json:"items"`
	Total                           int         `json:"total"`
	UnfilteredTotal                 int         `json:"unfilteredTotal,omitempty"`
	Genres                          []string    `json:"genres,omitempty"`
	AlphabetBuckets                 []string    `json:"alphabetBuckets,omitempty"`
	MetadataPending                 bool        `json:"metadataPending,omitempty"`
	OriginalLanguageFilterSupported bool        `json:"originalLanguageFilterSupported,omitempty"`
}

const (
	maxDiscoveryListItems   = 500
	watchTMDBShelfURLPrefix = "mediastorm:tmdb:"
)

func NewDisplayListHandler(watchlist watchlistService, customLists customListsService, users userService) *DisplayListHandler {
	return &DisplayListHandler{
		WatchlistService:   watchlist,
		CustomListsService: customLists,
		Users:              users,
	}
}

func (h *DisplayListHandler) SetHistoryService(service historyService) {
	h.HistoryService = service
}

func (h *DisplayListHandler) SetHistoryHandler(handler *HistoryHandler) {
	h.HistoryHandler = handler
}

func (h *DisplayListHandler) SetMetadataService(service metadataService) {
	h.MetadataService = service
}

func (h *DisplayListHandler) SetMetadataHandler(handler *MetadataHandler) {
	h.MetadataHandler = handler
}

func (h *DisplayListHandler) SetHiddenItemsService(service hiddenItemsService) {
	h.HiddenItemsService = service
}

func (h *DisplayListHandler) SetPrequeueStore(store persistentPrequeueStore) {
	h.PrequeueStore = store
}

func (h *DisplayListHandler) Get(w http.ResponseWriter, r *http.Request) {
	mediaFilter, mediaErr := parseLocalMediaFilters(r)
	if mediaErr != nil {
		http.Error(w, mediaErr.Error(), http.StatusBadRequest)
		return
	}
	if mediaFilter.Active() && !savedListMediaSupported(r) {
		http.Error(w, "stored media filters are supported for My List and saved personal lists only", http.StatusBadRequest)
		return
	}
	if hasCachedTMDBPersonCreditsSource(r) {
		h.getCachedTMDBPersonCredits(w, r)
		return
	}
	if hasCachedTMDBPublicListSource(r) {
		h.getCachedTMDBPublicList(w, r)
		return
	}
	if hasCachedSimilarSource(r) {
		h.getCachedSimilar(w, r)
		return
	}
	if hasCachedPopularTitlesSource(r) {
		h.getCachedPopularTitles(w, r)
		return
	}
	if hasCachedRecentWatchesSource(r) {
		h.getCachedRecentWatches(w, r)
		return
	}
	if _, err := parseDisplayListOriginalLanguageQuery(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	view := r.URL.Query().Get("homeView")
	filter := view
	if view != "movies" && view != "shows" && config.IsHomeViewID(view) {
		userID, ok := h.requireUser(w, r)
		if !ok {
			return
		}
		filter = config.HomeViewFilter(h.homeViewSettings(userID).Views, view)
	}
	if homeViewMediaType(filter) != "" {
		// Stored indexes already apply media filters before paging. In particular,
		// a custom-list shelf sentinel must not inherit the discovery window cap.
		source := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source")))
		if source == "" {
			source = "watchlist"
		}
		stored := displayListOriginalLanguageSupportedSource(source)
		if (source == "mdblist" || source == "mdblist-url" || source == "mdblist-shelf") && strings.HasPrefix(strings.TrimSpace(r.URL.Query().Get("url")), customListShelfURLPrefix) {
			stored = true
		}
		if stored {
			delegated := r.Clone(r.Context())
			u := *r.URL
			query := r.URL.Query()
			query.Set("filterMediaType", homeViewMediaType(filter))
			u.RawQuery = query.Encode()
			delegated.URL = &u
			h.get(w, delegated)
			return
		}
		h.getHomeView(w, r, filter)
		return
	}
	h.get(w, r)
}

func (h *DisplayListHandler) get(w http.ResponseWriter, r *http.Request) {
	userID, ok := h.requireUser(w, r)
	if !ok {
		return
	}

	source := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source")))
	if source == "" {
		source = "watchlist"
	}
	// Use the existing remote-shelf transport so current Watch clients can
	// render personal list shelves without a frontend update. Resolve locally;
	// this sentinel must never reach an external MDBList provider.
	customListShelf := false
	listID := strings.TrimSpace(r.URL.Query().Get("listId"))
	if source == "mdblist" || source == "mdblist-url" || source == "mdblist-shelf" {
		if listURL := strings.TrimSpace(r.URL.Query().Get("url")); strings.HasPrefix(listURL, customListShelfURLPrefix) {
			source = "custom-list"
			listID = strings.TrimSpace(strings.TrimPrefix(listURL, customListShelfURLPrefix))
			customListShelf = true
		}
	}
	mediaFilter, _ := parseLocalMediaFilters(r)
	languageQuery, queryErr := parseDisplayListOriginalLanguageQuery(r)
	if queryErr != nil {
		http.Error(w, queryErr.Error(), http.StatusBadRequest)
		return
	}
	languageSupported := displayListOriginalLanguageSupportedSource(source)
	if languageQuery.active() && !languageSupported {
		http.Error(w, "original-language filtering is unsupported for this source", http.StatusBadRequest)
		return
	}
	// Hidden management reads the complete saved suppression index. It must
	// never hide its own rows, warm metadata, or inherit discovery-window caps.
	if source == "hidden" {
		h.getHiddenManagement(w, r, userID, languageQuery)
		return
	}
	if source == "loved-stored" {
		h.getLovedStored(w, r, userID, languageQuery)
		return
	}
	if source == "continue-watching-cached" {
		h.getCachedContinueWatching(w, r, userID, languageQuery)
		return
	}
	if source == "collection-cached" {
		h.getCachedCollection(w, r, userID, languageQuery)
		return
	}
	metadataSource := source != "watchlist" &&
		source != "custom-list" &&
		source != "custom_user_list" &&
		source != "custom-user-list" &&
		source != "continue-watching" &&
		source != "continue_watching" &&
		source != "permanent-prequeue"
	if metadataSource && h.MetadataHandler == nil {
		http.Error(w, "metadata source is unavailable", http.StatusServiceUnavailable)
		return
	}

	var items []models.WatchlistItem
	var err error

	switch source {
	case "watchlist":
		if h.WatchlistService == nil {
			http.Error(w, "watchlist source is unavailable", http.StatusServiceUnavailable)
			return
		}
		items, err = h.WatchlistService.List(userID)
	case "custom-list", "custom_user_list", "custom-user-list":
		source = "custom-list"
		if h.CustomListsService == nil {
			http.Error(w, "custom list source is unavailable", http.StatusServiceUnavailable)
			return
		}
		if listID == "" {
			http.Error(w, "listId is required for custom-list source", http.StatusBadRequest)
			return
		}
		items, err = h.CustomListsService.ListItems(userID, listID)
	case "continue-watching", "continue_watching":
		source = "continue-watching"
		if h.HistoryHandler == nil {
			http.Error(w, "continue-watching source is unavailable", http.StatusServiceUnavailable)
			return
		}
		h.delegateMetadata(w, r, source, h.HistoryHandler.ListContinueWatching, displayListQuery(r, userID, nil))
		return
	case "permanent-prequeue":
		if h.PrequeueStore == nil {
			http.Error(w, "permanent prequeue source is unavailable", http.StatusServiceUnavailable)
			return
		}
		items = permanentPrequeueWatchlistItems(h.PrequeueStore.ListAll(), userID)
	case "top-ten":
		h.delegateMetadata(w, r, source, h.MetadataHandler.TopTen, displayListQuery(r, userID, map[string]string{
			"type": firstQueryValue(r, "mediaType", "type"),
		}))
		return
	case "popular-on-server":
		h.delegateMetadata(w, r, source, h.MetadataHandler.PopularOnServer, displayListQuery(r, userID, nil))
		return
	case "recently-watched":
		h.delegateMetadata(w, r, source, h.MetadataHandler.RecentlyWatched, displayListQuery(r, userID, nil))
		return
	case "trending":
		h.delegateMetadata(w, r, source, h.MetadataHandler.DiscoverNew, displayListQuery(r, userID, map[string]string{
			"type": firstQueryValue(r, "mediaType", "type"),
		}))
		return
	case "genre":
		h.delegateMetadata(w, r, source, h.MetadataHandler.DiscoverByGenre, displayListQuery(r, userID, map[string]string{
			"type": firstQueryValue(r, "mediaType", "type"),
		}))
		return
	case "decade":
		h.delegateMetadata(w, r, source, h.MetadataHandler.DiscoverByDecade, displayListQuery(r, userID, map[string]string{
			"type": firstQueryValue(r, "mediaType", "type"),
		}))
		return
	case "mdblist", "mdblist-url", "mdblist-shelf", "seasonal":
		if shelfID, ok := watchTMDBShelfID(r.URL.Query().Get("url")); ok {
			overrides, found := h.watchTMDBShelfOverrides(userID, shelfID, r.URL.Query().Get("homeView"))
			if !found {
				http.Error(w, "TMDB shelf is unavailable", http.StatusNotFound)
				return
			}
			source = "tmdb-list"
			h.delegateMetadata(w, r, source, h.MetadataHandler.TMDBList, displayListQuery(r, userID, overrides))
			return
		}
		source = "mdblist"
		h.delegateMetadata(w, r, source, h.MetadataHandler.CustomList, displayListQuery(r, userID, nil))
		return
	case "stremio", "stremio-catalog":
		source = "stremio"
		h.delegateMetadata(w, r, source, h.MetadataHandler.StremioList, displayListQuery(r, userID, nil))
		return
	case "tmdb-list":
		h.delegateMetadata(w, r, source, h.MetadataHandler.TMDBList, displayListQuery(r, userID, nil))
		return
	case "trakt-list":
		h.delegateMetadata(w, r, source, h.MetadataHandler.TraktList, displayListQuery(r, userID, nil))
		return
	case "publicmetadb-list":
		h.delegateMetadata(w, r, source, h.MetadataHandler.PublicMetaDBList, displayListQuery(r, userID, nil))
		return
	case "simkl-list":
		h.delegateMetadata(w, r, source, h.MetadataHandler.SimklList, displayListQuery(r, userID, nil))
		return
	case "letterboxd-list":
		h.delegateMetadata(w, r, source, h.MetadataHandler.LetterboxdList, displayListQuery(r, userID, nil))
		return
	case "loved":
		h.delegateMetadata(w, r, source, h.MetadataHandler.LovedTitles, displayListQuery(r, userID, nil))
		return
	case "personalized", "my-recommended":
		source = "personalized"
		h.delegateMetadata(w, r, source, h.MetadataHandler.GetPersonalizedRecommendations, displayListQuery(r, userID, nil))
		return
	case "custom-ai":
		h.delegateMetadata(w, r, source, h.MetadataHandler.GetAICustomRecommendations, displayListQuery(r, userID, nil))
		return
	case "similar":
		h.delegateMetadata(w, r, source, h.MetadataHandler.Similar, displayListQuery(r, userID, map[string]string{
			"type": firstQueryValue(r, "mediaType", "type"),
		}))
		return
	case "collection":
		h.delegateMetadata(w, r, source, h.MetadataHandler.CollectionDetails, displayListQuery(r, userID, map[string]string{
			"id": firstQueryValue(r, "collectionId", "id"),
		}))
		return
	default:
		http.Error(w, "unsupported display list source", http.StatusBadRequest)
		return
	}

	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, customlists.ErrUserIDRequired), errors.Is(err, customlists.ErrListIDRequired):
			status = http.StatusBadRequest
		case errors.Is(err, os.ErrNotExist):
			status = http.StatusNotFound
		}
		http.Error(w, err.Error(), status)
		return
	}

	if h.HiddenItemsService != nil {
		items = h.HiddenItemsService.FilterHiddenWatchlistItems(userID, items)
	}
	unfilteredTotal := 0
	if customListShelf {
		unfilteredTotal = len(items)
	}
	if mediaFilter.Active() {
		h.enrichStoredMediaWatchState(userID, items)
		items, err = h.filterSavedListMedia(r, userID, items, mediaFilter)
		if err != nil {
			http.Error(w, "stored library media information unavailable", http.StatusServiceUnavailable)
			return
		}
	} else {
		h.enrich(userID, items, r)
	}

	// Personal watchlists retain explicitly saved titles regardless of release
	// status. Discovery and other lists still follow release visibility settings.
	if source != "watchlist" && h.MetadataHandler != nil {
		policy := resolveUnreleasedVisibilityPolicy(
			h.MetadataHandler.CfgManager,
			h.MetadataHandler.UserSettings,
			h.MetadataHandler.ClientSettings,
			userID,
			requestClientID(r),
			unreleasedVisibilityLists,
		)
		items = filterWatchlistItemsByUnreleasedVisibility(items, policy)
	}
	if customListShelf {
		if strings.EqualFold(r.URL.Query().Get("hideUnreleased"), "true") {
			items = filterWatchlistItemsByUnreleasedVisibility(items, unreleasedVisibilityPolicy{})
		}
		if strings.EqualFold(r.URL.Query().Get("hideWatched"), "true") {
			visible := make([]models.WatchlistItem, 0, len(items))
			for _, item := range items {
				if item.WatchState != "complete" {
					visible = append(visible, item)
				}
			}
			items = visible
		}
	}
	items = h.filterOriginalLanguages(userID, items, languageQuery)
	items, genres, alphabet := queryWatchlistItems(items, parseDisplayListQuery(r))
	total := len(items)
	limit, offset := parseLimitOffset(r)
	if offset >= len(items) {
		items = []models.WatchlistItem{}
	} else {
		if offset > 0 {
			items = items[offset:]
		}
		if limit > 0 && limit < len(items) {
			items = items[:limit]
		}
	}

	if items == nil {
		items = []models.WatchlistItem{}
	}
	logDisplayListWatchlistArtworkTrace(userID, source, items)
	if h.MetadataHandler != nil && !mediaFilter.Active() {
		items = availabilityWatchlist(h.MetadataHandler.SourceAvailability, userID, requestClientID(r), items)
		if h.MetadataHandler.TitleRatings != nil {
			titles := make([]models.Title, len(items))
			for i, item := range items {
				titles[i] = titleFromSharedShelfItem(item.ID, item.Name, item.MediaType, item.Year, item.ExternalIDs)
			}
			h.MetadataHandler.TitleRatings.Observe(userID, requestClientID(r), titles)
		}
	}
	responseItems := interface{}(items)
	metadataPending := false
	if source == "permanent-prequeue" || customListShelf {
		trendingItems := watchlistItemsToTrending(items)
		if customListShelf && !mediaFilter.Active() {
			deferArtwork := strings.EqualFold(r.URL.Query().Get("shelfPhase"), "cards") ||
				strings.EqualFold(r.URL.Query().Get("deferArtwork"), "true")
			metadataSvc := h.metadataForUser(userID)
			postersPending := enrichCustomListShelfTextPosters(r.Context(), trendingItems, metadataSvc, deferArtwork)
			logosPending := fillMissingTitleLogos(r.Context(), trendingItems, metadataSvc, deferArtwork)
			metadataPending = postersPending || logosPending
		}
		responseItems = trendingItems
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DisplayListResponse{
		Source:                          source,
		ListID:                          listID,
		Items:                           responseItems,
		Total:                           total,
		UnfilteredTotal:                 unfilteredTotal,
		Genres:                          genres,
		AlphabetBuckets:                 alphabet,
		MetadataPending:                 metadataPending,
		OriginalLanguageFilterSupported: languageSupported,
		StoredMediaFilterSupported:      h.storedMediaReady() && savedListMediaSupported(r),
		StoredMediaFilterBasis:          storedListMediaBasis(mediaFilter.Active()),
	})
}

func (h *DisplayListHandler) getHiddenManagement(w http.ResponseWriter, r *http.Request, userID string, language displayListOriginalLanguageQuery) {
	query := parseDisplayListQuery(r)
	if err := validateHiddenManagementQuery(query); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if h.HiddenItemsService == nil {
		http.Error(w, "hidden items source unavailable", http.StatusServiceUnavailable)
		return
	}
	items, err := h.HiddenItemsService.List(userID)
	if err != nil {
		http.Error(w, "hidden items source unavailable", http.StatusServiceUnavailable)
		return
	}
	if language.active() {
		matches := h.originalLanguageMatcher(userID, language)
		filtered := make([]models.HiddenItem, 0, len(items))
		for _, item := range items {
			if matches(item.ID, item.MediaType) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	items, alphabet := queryHiddenManagementItems(items, query)
	total := len(items)
	limit, offset := parseLimitOffset(r)
	if offset >= len(items) {
		items = []models.HiddenItem{}
	} else {
		items = items[offset:]
		if limit > 0 && limit < len(items) {
			items = items[:limit]
		}
	}
	if items == nil {
		items = []models.HiddenItem{}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(DisplayListResponse{Source: "hidden", Items: items, Total: total, AlphabetBuckets: alphabet, OriginalLanguageFilterSupported: true})
}

// lovedStoredItem exposes the saved preference time without adding private
// rating keys or aliases to the catalogue title contract.
type cachedCollectionReader interface {
	GetCachedCollectionSnapshot(int64, time.Time) (*metadatapkg.CachedCollectionSnapshot, error)
}
type cachedCollectionBrowseResponse struct {
	DisplayListResponse
	Collection cachedCollectionBrowseObservation `json:"collection"`
}
type cachedCollectionBrowseObservation struct {
	ID               int64     `json:"id"`
	Name             string    `json:"name"`
	MetadataLanguage string    `json:"metadataLanguage"`
	ObservedAt       time.Time `json:"observedAt"`
	ExpiresAt        time.Time `json:"expiresAt"`
}

func cachedCollectionError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": "Cached collection unavailable"})
}

type cachedContinueWatchingReader interface {
	GetCachedContinueWatchingPool(string, time.Time) (*historypkg.CachedContinueWatchingPool, error)
	CachedContinueWatchingPoolCurrent(string, string, time.Time) bool
}
type cachedWatchingSuppression struct {
	hidden []models.HiddenItem
	aside  []models.WatchlistItem
	stamp  [32]byte
	tokens map[string]struct{}
}

func (h *DisplayListHandler) cachedWatchingSuppression(userID string) (cachedWatchingSuppression, error) {
	var result cachedWatchingSuppression
	if h.HiddenItemsService == nil || h.CustomListsService == nil {
		return result, errors.New("suppression reader unavailable")
	}
	hidden, err := h.HiddenItemsService.List(userID)
	if err != nil {
		return result, err
	}
	result.hidden = hidden
	lists, err := h.CustomListsService.ListLists(userID)
	if err != nil {
		return result, err
	}
	for _, list := range lists {
		if !strings.EqualFold(strings.TrimSpace(list.Name), "Set Aside") {
			continue
		}
		items, err := h.CustomListsService.ListItems(userID, list.ID)
		if err != nil {
			return result, err
		}
		result.aside = append(result.aside, items...)
	}
	// Private suppression records are never response fields or diagnostics.
	raw, err := json.Marshal(struct {
		Hidden []models.HiddenItem
		Aside  []models.WatchlistItem
	}{result.hidden, result.aside})
	if err != nil {
		return result, err
	}
	result.stamp = sha256.Sum256(raw)
	result.tokens = make(map[string]struct{})
	add := func(kind, id string, aliases map[string]string) {
		identity := mediaidentity.Resolve(mediaidentity.Input{MediaType: kind, ID: id, ExternalIDs: aliases})
		for token := range identity.Tokens {
			result.tokens[identity.MediaType+"\x00"+token] = struct{}{}
		}
	}
	for _, item := range result.hidden {
		add(item.MediaType, item.ID, item.ExternalIDs)
	}
	for _, item := range result.aside {
		add(item.MediaType, item.ID, item.ExternalIDs)
	}
	return result, nil
}
func (s cachedWatchingSuppression) hides(row historypkg.CachedContinueWatchingItem) bool {
	identity := mediaidentity.Resolve(mediaidentity.Input{MediaType: row.MediaType, ID: row.SeriesID, ExternalIDs: row.ExternalIDs})
	for token := range identity.Tokens {
		if _, ok := s.tokens[identity.MediaType+"\x00"+token]; ok {
			return true
		}
	}
	return false
}
func (h *DisplayListHandler) getCachedContinueWatching(w http.ResponseWriter, r *http.Request, userID string, language displayListOriginalLanguageQuery) {
	w.Header().Set("Cache-Control", "private, no-store")
	fail := func(status int, code string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": "Cached Continue Watching unavailable"})
	}
	if auth.IsShareLinkRequest(r) || auth.GetSessionScope(r) != "" {
		fail(403, "profile_session_required")
		return
	}
	if err := validateCachedWatchingQuery(r, parseDisplayListQuery(r)); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if h.MetadataHandler == nil || h.MetadataHandler.UsersService == nil || h.HistoryHandler == nil {
		fail(503, "continue_watching_reader_unavailable")
		return
	}
	profile, found := h.MetadataHandler.UsersService.Get(userID)
	if !found || profile.ID != userID || !auth.IsMaster(r) && (auth.GetAccountID(r) == "" || profile.AccountID != auth.GetAccountID(r)) {
		fail(404, "profile_not_found")
		return
	}
	if !profileaccess.CheckResource(w, r, userID, "") {
		return
	}
	if profile.IsKidsProfile {
		fail(403, "kids_continue_watching_unavailable")
		return
	}
	if _, ok := h.metadataForUser(userID).(cachedOriginalLanguageReader); !ok {
		fail(503, "continue_watching_reader_unavailable")
		return
	}
	reader, ok := h.HistoryHandler.Service.(cachedContinueWatchingReader)
	if !ok {
		fail(503, "continue_watching_reader_unavailable")
		return
	}
	pool, err := reader.GetCachedContinueWatchingPool(userID, time.Now())
	if err != nil || pool == nil || !pool.Complete || pool.ID == "" || pool.ObservedAt.After(time.Now()) || !pool.ExpiresAt.After(time.Now()) || pool.ExpiresAt.Sub(pool.ObservedAt) > 10*time.Minute {
		fail(409, "continue_watching_snapshot_unavailable")
		return
	}
	suppression, err := h.cachedWatchingSuppression(userID)
	if err != nil {
		fail(503, "continue_watching_reader_unavailable")
		return
	}
	matches := h.originalLanguageMatcher(userID, language)
	rows := make([]historypkg.CachedContinueWatchingItem, 0, len(pool.Items))
	for _, row := range pool.Items {
		if suppression.hides(row) {
			continue
		}
		if language.active() && !matches(row.SeriesID, row.MediaType) {
			continue
		}
		rows = append(rows, row)
	}
	rows, alphabet := queryCachedWatchingItems(rows, parseDisplayListQuery(r))
	total := len(rows)
	limit, offset := parseLimitOffset(r)
	if offset >= len(rows) {
		rows = []historypkg.CachedContinueWatchingItem{}
	} else {
		rows = rows[offset:]
		if limit > 0 && limit < len(rows) {
			rows = rows[:limit]
		}
	}
	if r.Context().Err() != nil {
		return
	}
	// Recheck the original profile, mutable suppressions and observation before
	// publication, without rebuilding a pool or extending its original expiry.
	currentSuppression, err := h.cachedWatchingSuppression(userID)
	if err != nil {
		fail(503, "continue_watching_reader_unavailable")
		return
	}
	latest, found := h.MetadataHandler.UsersService.Get(userID)
	if !found || latest.ID != profile.ID || latest.AccountID != profile.AccountID || latest.IsKidsProfile != profile.IsKidsProfile || !latest.UpdatedAt.Equal(profile.UpdatedAt) {
		fail(409, "continue_watching_snapshot_unavailable")
		return
	}
	if !profileaccess.CheckResource(w, r, userID, "") {
		return
	}
	if currentSuppression.stamp != suppression.stamp || !reader.CachedContinueWatchingPoolCurrent(userID, pool.ID, time.Now()) {
		fail(409, "continue_watching_snapshot_unavailable")
		return
	}
	observation := struct {
		ID         string    `json:"id"`
		ObservedAt time.Time `json:"observedAt"`
		ExpiresAt  time.Time `json:"expiresAt"`
		Complete   bool      `json:"complete"`
	}{pool.ID, pool.ObservedAt, pool.ExpiresAt, true}
	response := struct {
		Source          string                                  `json:"source"`
		Items           []historypkg.CachedContinueWatchingItem `json:"items"`
		Total           int                                     `json:"total"`
		AlphabetBuckets []string                                `json:"alphabetBuckets,omitempty"`
		Supported       bool                                    `json:"originalLanguageFilterSupported"`
		Pool            any                                     `json:"pool"`
	}{"continue-watching-cached", rows, total, alphabet, true, observation}
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (h *DisplayListHandler) getCachedCollection(w http.ResponseWriter, r *http.Request, userID string, language displayListOriginalLanguageQuery) {
	w.Header().Set("Cache-Control", "private, no-store")
	if h.MetadataHandler == nil || h.MetadataHandler.UsersService == nil {
		cachedCollectionError(w, 503, "collection_reader_unavailable")
		return
	}
	profile, found := h.MetadataHandler.UsersService.Get(userID)
	if !found || profile.ID != userID {
		cachedCollectionError(w, 404, "profile_not_found")
		return
	}
	if profile.IsKidsProfile {
		cachedCollectionError(w, 403, "kids_collection_unavailable")
		return
	}
	id, err := validateCachedCollectionQuery(r, parseDisplayListQuery(r))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if h.HiddenItemsService == nil {
		cachedCollectionError(w, 503, "collection_reader_unavailable")
		return
	}
	reader, ok := h.metadataForUser(userID).(cachedCollectionReader)
	if !ok {
		cachedCollectionError(w, 503, "collection_reader_unavailable")
		return
	}
	at := time.Now()
	snapshot, err := reader.GetCachedCollectionSnapshot(id, at)
	if err != nil || snapshot == nil || snapshot.CollectionID != id || !snapshot.ExpiresAt.After(at) {
		cachedCollectionError(w, 409, "collection_snapshot_unavailable")
		return
	}
	// The concrete getter validates its own complete provider observation. No
	// Title/client input may fill a missing collection or original-language fact.
	titles := snapshot.Titles()
	policy := resolveUnreleasedVisibilityPolicy(h.MetadataHandler.CfgManager, h.MetadataHandler.UserSettings, h.MetadataHandler.ClientSettings, userID, requestClientID(r), unreleasedVisibilityLists)
	titles = filterTitlesByUnreleasedVisibility(titles, policy)
	visible := make([]models.Title, 0, len(titles))
	for _, title := range titles {
		if !h.HiddenItemsService.ShouldHideTitleMap(userID, map[string]interface{}{"id": title.ID, "name": title.Name, "mediaType": title.MediaType, "tmdbId": strconv.FormatInt(title.TMDBID, 10)}) {
			visible = append(visible, title)
		}
	}
	if language.active() {
		matches := h.originalLanguageMatcher(userID, language)
		filtered := make([]models.Title, 0, len(visible))
		for _, title := range visible {
			if matches(title.ID, title.MediaType) {
				filtered = append(filtered, title)
			}
		}
		visible = filtered
	}
	items, alphabet := queryCachedCollectionTitles(visible, parseDisplayListQuery(r))
	total := len(items)
	limit, offset := parseLimitOffset(r)
	if offset >= len(items) {
		items = []models.Title{}
	} else {
		items = items[offset:]
		if limit > 0 && limit < len(items) {
			items = items[:limit]
		}
	}
	if r.Context().Err() != nil {
		return
	}
	if !snapshot.ExpiresAt.After(time.Now()) {
		cachedCollectionError(w, 409, "collection_snapshot_unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(cachedCollectionBrowseResponse{DisplayListResponse: DisplayListResponse{Source: "collection-cached", Items: items, Total: total, AlphabetBuckets: alphabet, OriginalLanguageFilterSupported: true}, Collection: cachedCollectionBrowseObservation{ID: snapshot.CollectionID, Name: snapshot.Name, MetadataLanguage: snapshot.MetadataLanguage, ObservedAt: snapshot.ObservedAt, ExpiresAt: snapshot.ExpiresAt}})
}

type lovedStoredItem struct {
	Rank      int          `json:"rank"`
	Title     models.Title `json:"title"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

func (h *DisplayListHandler) getLovedStored(w http.ResponseWriter, r *http.Request, userID string, language displayListOriginalLanguageQuery) {
	w.Header().Set("Cache-Control", "private, no-store")
	if h.MetadataHandler == nil || h.MetadataHandler.UsersService == nil {
		http.Error(w, "personal favorites unavailable", http.StatusServiceUnavailable)
		return
	}
	profile, client, ok := h.MetadataHandler.requireLovedProfile(w, r)
	if !ok {
		return
	}
	if profile != userID {
		http.NotFound(w, r)
		return
	}
	query := parseDisplayListQuery(r)
	if err := validateLovedStoredQuery(r, query); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if h.HiddenItemsService == nil {
		http.Error(w, "hidden items source unavailable", http.StatusServiceUnavailable)
		return
	}
	ratings, err := h.MetadataHandler.TitleRatings.List(r.Context(), profile)
	if err != nil {
		http.Error(w, "could not load personal favorites", http.StatusServiceUnavailable)
		return
	}
	items := make([]lovedStoredItem, 0, len(ratings))
	for _, rating := range ratings {
		stored := rating.Title
		if rating.Value != "love" || stored.ID == "" || len(stored.ID) > 128 || stored.MediaType != "movie" && stored.MediaType != "series" || !h.MetadataHandler.ratingTitleAllowed(profile, stored) {
			continue
		}
		// Explicitly copy saved presentation/identity fields. In particular, no
		// scalar language, imported evidence or playback/source context survives.
		title := models.Title{ID: stored.ID, Name: stored.Name, MediaType: stored.MediaType, Year: stored.Year, TMDBID: stored.TMDBID, IMDBID: stored.IMDBID, TVDBID: stored.TVDBID, Adult: stored.Adult, Certification: stored.Certification, PersonalRating: "love"}
		if h.HiddenItemsService.ShouldHideTitleMap(profile, map[string]interface{}{"id": title.ID, "name": title.Name, "mediaType": title.MediaType, "tmdbId": strconv.FormatInt(title.TMDBID, 10), "tvdbId": strconv.FormatInt(title.TVDBID, 10), "imdbId": title.IMDBID}) {
			continue
		}
		items = append(items, lovedStoredItem{Title: title, UpdatedAt: rating.UpdatedAt})
	}
	if language.active() {
		matches := h.originalLanguageMatcher(profile, language)
		filtered := make([]lovedStoredItem, 0, len(items))
		for _, item := range items {
			if matches(item.Title.ID, item.Title.MediaType) {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	items, alphabet := queryLovedStoredItems(items, query)
	total := len(items)
	for i := range items {
		items[i].Rank = i + 1
	}
	limit, offset := parseLimitOffset(r)
	if offset >= len(items) {
		items = []lovedStoredItem{}
	} else {
		items = items[offset:]
		if limit > 0 && limit < len(items) {
			items = items[:limit]
		}
	}
	// Preserve normal rating editing only for this delivered page, using the
	// existing bounded, expiring receipt store. A full Random pool does not
	// extend its 2000-receipt capacity or TTL and never grants playback access.
	titles := make([]models.Title, len(items))
	for i := range items {
		titles[i] = items[i].Title
	}
	h.MetadataHandler.TitleRatings.Observe(profile, client, titles)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DisplayListResponse{Source: "loved-stored", Items: items, Total: total, AlphabetBuckets: alphabet, OriginalLanguageFilterSupported: true})
}

func permanentPrequeueWatchlistItems(entries []*playback.PrequeueEntry, userID string) []models.WatchlistItem {
	filtered := make([]*playback.PrequeueEntry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || !entry.Persistent || entry.UserID != userID {
			continue
		}
		filtered = append(filtered, entry)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})

	items := make([]models.WatchlistItem, 0, len(filtered))
	for _, entry := range filtered {
		items = append(items, models.WatchlistItem{
			ID:          entry.TitleID,
			MediaType:   entry.MediaType,
			Name:        entry.TitleName,
			Year:        entry.Year,
			AddedAt:     entry.CreatedAt,
			ExternalIDs: canonicalTitleExternalIDs(entry.TitleID),
		})
	}
	return items
}

func canonicalTitleExternalIDs(titleID string) map[string]string {
	parts := strings.Split(strings.TrimSpace(titleID), ":")
	ids := make(map[string]string)
	if len(parts) >= 2 {
		switch strings.ToLower(parts[0]) {
		case "tmdb":
			ids["tmdb"] = parts[len(parts)-1]
		case "tvdb":
			ids["tvdb"] = parts[len(parts)-1]
		case "imdb":
			ids["imdb"] = parts[len(parts)-1]
		}
	} else if strings.HasPrefix(strings.ToLower(titleID), "tt") {
		ids["imdb"] = titleID
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

func watchlistItemsToTrending(items []models.WatchlistItem) []models.TrendingItem {
	result := make([]models.TrendingItem, 0, len(items))
	for i, item := range items {
		title := models.Title{
			SourceAvailability: item.SourceAvailability,
			CardSubtitle:       item.CardSubtitle,
			ForceTitleOverlay:  item.ForceTitleOverlay,
			ID:                 item.ID,
			Name:               item.Name,
			Overview:           item.Overview,
			Year:               item.Year,
			MediaType:          item.MediaType,
			Genres:             item.Genres,
			RuntimeMinutes:     item.RuntimeMinutes,
			WatchState:         item.WatchState,
			UnwatchedCount:     item.UnwatchedCount,
			Ratings:            item.Ratings,
			Status:             item.Status,
			LifecycleStatus:    item.LifecycleStatus,
			Theatrical:         item.Theatrical,
			HomeRelease:        item.HomeRelease,
			ReleaseDate:        item.ReleaseDate,
		}
		if value := item.ExternalIDs["tmdb"]; value != "" {
			title.TMDBID, _ = strconv.ParseInt(value, 10, 64)
		}
		if value := item.ExternalIDs["tvdb"]; value != "" {
			title.TVDBID, _ = strconv.ParseInt(value, 10, 64)
		}
		title.IMDBID = item.ExternalIDs["imdb"]
		if item.PosterURL != "" {
			title.Poster = &models.Image{URL: item.PosterURL, Type: "poster"}
		}
		if item.TextPosterURL != "" {
			title.TextPoster = &models.Image{URL: item.TextPosterURL, Type: "poster"}
		}
		if item.BackdropURL != "" {
			title.Backdrop = &models.Image{URL: item.BackdropURL, Type: "backdrop"}
		}
		if item.TextBackdropURL != "" {
			title.TextBackdrop = &models.Image{URL: item.TextBackdropURL, Type: "backdrop"}
		}
		if item.LogoURL != "" {
			title.Logo = &models.Image{URL: item.LogoURL, Type: "logo"}
		}
		for _, url := range item.BackdropURLs {
			if url != "" {
				title.Backdrops = append(title.Backdrops, models.Image{URL: url, Type: "backdrop"})
			}
		}
		result = append(result, models.TrendingItem{Rank: i + 1, Title: title})
	}
	return result
}

func (h *DisplayListHandler) Options(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *DisplayListHandler) Post(w http.ResponseWriter, r *http.Request) {
	userID, ok := h.requireUser(w, r)
	if !ok {
		return
	}
	source := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("source")))
	if source != "curated" {
		http.Error(w, "unsupported display list source", http.StatusBadRequest)
		return
	}
	if h.MetadataHandler == nil {
		http.Error(w, "metadata source is unavailable", http.StatusServiceUnavailable)
		return
	}
	h.delegateMetadata(w, r, source, h.MetadataHandler.CuratedList, displayListQuery(r, userID, nil))
}

func displayListQuery(r *http.Request, userID string, overrides map[string]string) url.Values {
	query := r.URL.Query()
	query.Del("source")
	if query.Get("userId") == "" {
		query.Set("userId", userID)
	}
	for key, value := range overrides {
		if strings.TrimSpace(value) == "" {
			query.Del(key)
			continue
		}
		query.Set(key, value)
	}
	return query
}

func firstQueryValue(r *http.Request, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(r.URL.Query().Get(name)); value != "" {
			return value
		}
	}
	return ""
}
func watchTMDBShelfID(listURL string) (string, bool) {
	listURL = strings.TrimSpace(listURL)
	if !strings.HasPrefix(listURL, watchTMDBShelfURLPrefix) {
		return "", false
	}
	shelfID := strings.TrimSpace(strings.TrimPrefix(listURL, watchTMDBShelfURLPrefix))
	return shelfID, shelfID != ""
}

func (h *DisplayListHandler) watchTMDBShelfOverrides(userID, shelfID string, views ...string) (map[string]string, bool) {
	if h.MetadataHandler == nil {
		return nil, false
	}
	view := ""
	if len(views) > 0 {
		view = views[0]
	}
	home := h.homeViewSettings(userID)
	home = models.ResolveHomeView(home, view)
	for _, shelf := range home.Shelves {
		if shelf.ID == shelfID && strings.EqualFold(strings.TrimSpace(shelf.Type), "tmdb") {
			return tmdbShelfQueryOverrides(shelf.TMDBSourceType, shelf.TMDBSourceID, shelf.TMDBMediaType, shelf.Sort, shelf.TMDBDiscoverQuery), true
		}
	}
	return nil, false
}

func tmdbShelfQueryOverrides(sourceType, sourceID, mediaType, sortBy, discoverQuery string) map[string]string {
	return map[string]string{
		"url":           "",
		"sourceType":    sourceType,
		"sourceId":      sourceID,
		"mediaType":     mediaType,
		"sort":          sortBy,
		"discoverQuery": discoverQuery,
	}
}

func (h *DisplayListHandler) delegateMetadata(
	w http.ResponseWriter,
	r *http.Request,
	source string,
	handler func(http.ResponseWriter, *http.Request),
	query url.Values,
) {
	query = cappedDisplayListQuery(query)
	filters := &progressiveShelfFilters{}
	ctx := r.Context()
	if progressiveShelfRequest(r) && progressiveShelfSource(source) {
		filters.hidden = h.HiddenItemsService
		if filters.hidden != nil {
			items, err := filters.hidden.List(query.Get("userId"))
			filters.hasHidden = err != nil || len(items) > 0
		}
		ctx = context.WithValue(ctx, progressiveShelfFiltersKey{}, filters)
	}
	delegated := r.Clone(ctx)
	delegatedURL := *r.URL
	delegatedURL.RawQuery = query.Encode()
	delegated.URL = &delegatedURL

	rec := httptest.NewRecorder()
	handler(rec, delegated)
	if rec.Code >= http.StatusBadRequest {
		for key, values := range rec.Header() {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
		return
	}

	var payload interface{}
	if err := json.NewDecoder(rec.Body).Decode(&payload); err != nil {
		http.Error(w, "failed to decode display list source", http.StatusBadGateway)
		return
	}

	if control := rec.Header().Get("Cache-Control"); control != "" {
		w.Header().Set("Cache-Control", control)
	}
	normalised := normaliseDisplayListPayload(source, payload)
	if h.HiddenItemsService != nil && !filters.applied {
		normalised = h.filterHiddenPayload(query.Get("userId"), normalised)
	}
	capDisplayListPayload(normalised, query)
	logDisplayListPayloadArtworkTrace(query.Get("userId"), source, normalised)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(normalised)
}

func cappedDisplayListQuery(query url.Values) url.Values {
	capped := make(url.Values, len(query))
	for key, values := range query {
		capped[key] = append([]string(nil), values...)
	}
	offset := 0
	if parsed, err := strconv.Atoi(capped.Get("offset")); err == nil && parsed > 0 {
		offset = parsed
	}
	remaining := maxDiscoveryListItems - offset
	if remaining < 0 {
		remaining = 0
	}
	limit, err := strconv.Atoi(capped.Get("limit"))
	if err != nil || limit <= 0 || limit > remaining {
		forwardedLimit := remaining
		if forwardedLimit == 0 {
			// Metadata handlers treat zero as unlimited. Ask for the smallest
			// possible page and discard it below when the cap is exhausted.
			forwardedLimit = 1
		}
		capped.Set("limit", strconv.Itoa(forwardedLimit))
	}
	return capped
}

func capDisplayListPayload(payload map[string]interface{}, query url.Values) {
	offset := 0
	if parsed, err := strconv.Atoi(query.Get("offset")); err == nil && parsed > 0 {
		offset = parsed
	}
	remaining := maxDiscoveryListItems - offset
	if remaining < 0 {
		remaining = 0
	}

	for _, key := range []string{"items", "movies", "series"} {
		if items, ok := payload[key].([]interface{}); ok && len(items) > remaining {
			payload[key] = items[:remaining]
		}
	}
	for _, key := range []string{"total", "sourceTotal", "unfilteredTotal"} {
		switch value := payload[key].(type) {
		case float64:
			if value > maxDiscoveryListItems {
				payload[key] = float64(maxDiscoveryListItems)
			}
		case int:
			if value > maxDiscoveryListItems {
				payload[key] = maxDiscoveryListItems
			}
		}
	}
}

func (h *DisplayListHandler) filterHiddenPayload(userID string, payload map[string]interface{}) map[string]interface{} {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return payload
	}
	// Preserve the source's pre-pagination total before replacing `total` with
	// the visible count from this response page. Clients need this value to know
	// that another page exists even when the current page has hidden items (or
	// simply contains fewer records than the full source).
	if total, ok := payload["total"]; ok {
		payload["sourceTotal"] = total
	}
	for _, key := range []string{"items", "movies", "series"} {
		rawItems, ok := payload[key].([]interface{})
		if !ok {
			continue
		}
		filtered := rawItems[:0]
		for _, raw := range rawItems {
			itemMap, ok := raw.(map[string]interface{})
			if !ok {
				filtered = append(filtered, raw)
				continue
			}
			titleMap, ok := itemMap["title"].(map[string]interface{})
			if !ok {
				titleMap = itemMap
			}
			if h.HiddenItemsService.ShouldHideTitleMap(userID, titleMap) {
				continue
			}
			filtered = append(filtered, raw)
		}
		payload[key] = filtered
		if key == "items" {
			payload["total"] = len(filtered)
		}
	}
	return payload
}

func normaliseDisplayListPayload(source string, payload interface{}) map[string]interface{} {
	switch typed := payload.(type) {
	case []interface{}:
		return map[string]interface{}{
			"source": source,
			"items":  typed,
			"total":  len(typed),
		}
	case map[string]interface{}:
		typed["source"] = source
		if _, ok := typed["items"]; !ok {
			if movies, ok := typed["movies"]; ok {
				typed["items"] = movies
			}
		}
		if _, ok := typed["total"]; !ok {
			if items, ok := typed["items"].([]interface{}); ok {
				typed["total"] = len(items)
			}
		}
		return typed
	default:
		return map[string]interface{}{
			"source": source,
			"items":  []interface{}{},
			"total":  0,
		}
	}
}

func (h *DisplayListHandler) requireUser(w http.ResponseWriter, r *http.Request) (string, bool) {
	userID := strings.TrimSpace(mux.Vars(r)["userID"])
	if userID == "" {
		http.Error(w, "user id is required", http.StatusBadRequest)
		return "", false
	}
	if h.Users != nil && !h.Users.Exists(userID) {
		http.Error(w, "user not found", http.StatusNotFound)
		return "", false
	}
	return userID, true
}

func (h *DisplayListHandler) metadataForUser(userID string) metadataService {
	metadataSvc := h.MetadataService
	if h.MetadataHandler != nil {
		metadataSvc = h.MetadataHandler.serviceForUser(userID)
	}
	return metadataSvc
}

func (h *DisplayListHandler) enrich(userID string, items []models.WatchlistItem, r *http.Request) {
	metadataSvc := h.metadataForUser(userID)

	if h.HistoryService != nil {
		wh, whErr := h.HistoryService.ListWatchHistory(userID)
		cw, _ := h.HistoryService.ListSeriesStates(userID)
		pp, _ := h.HistoryService.ListPlaybackProgress(userID)
		if whErr == nil {
			idx := buildWatchStateIndex(wh, cw, pp)
			warmEpisodeCounts := strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("includeUnwatchedCounts")), "true")
			enrichWatchlistItems(items, idx, metadataSvc, warmEpisodeCounts)
		}
	}

	enrichWatchlistRatings(r.Context(), items, metadataSvc)
	enrichWatchlistArtwork(items, metadataSvc)
	enrichDisplayListReleases(r, items, metadataSvc)
}

func enrichDisplayListReleases(r *http.Request, items []models.WatchlistItem, meta metadataService) {
	if meta == nil || len(items) == 0 {
		return
	}

	enrichDisplayListSeriesReleases(items, meta)

	queries := make([]models.BatchMovieReleasesQuery, 0)
	indexes := make([]int, 0)
	for i := range items {
		if strings.ToLower(strings.TrimSpace(items[i].MediaType)) != "movie" {
			continue
		}
		tmdbID, _ := watchlist.NumericIDs(items[i].ExternalIDs)
		imdbID := strings.TrimSpace(items[i].ExternalIDs["imdb"])
		if tmdbID <= 0 && imdbID == "" {
			continue
		}
		queries = append(queries, models.BatchMovieReleasesQuery{
			TitleID: items[i].ID,
			TMDBID:  tmdbID,
			IMDBID:  imdbID,
		})
		indexes = append(indexes, i)
	}
	if len(queries) == 0 {
		return
	}

	results := meta.BatchMovieReleases(r.Context(), queries)
	for i, result := range results {
		if i >= len(indexes) {
			break
		}
		idx := indexes[i]
		items[idx].Status = result.Status
		items[idx].Theatrical = result.Theatrical
		items[idx].HomeRelease = result.HomeRelease
		if items[idx].Status == "" {
			items[idx].Status = models.MovieReleaseStatusFromWindows(items[idx].Theatrical, items[idx].HomeRelease)
		}
	}
}

// cachedSeriesReleaseProvider is the optional cache-only series release lookup.
type cachedSeriesReleaseProvider interface {
	GetCachedSeriesRelease(tmdbID int64, tvdbID int64) (status string, releaseDate string)
}

// enrichDisplayListSeriesReleases refreshes series release status and premiere
// date from cached metadata: saved items keep the status from when they were
// added (often none), so an unreleased show would otherwise carry no badge.
func enrichDisplayListSeriesReleases(items []models.WatchlistItem, meta metadataService) {
	cached, ok := meta.(cachedSeriesReleaseProvider)
	if !ok {
		return
	}
	for i := range items {
		switch strings.ToLower(strings.TrimSpace(items[i].MediaType)) {
		case "series", "tv", "show":
		default:
			continue
		}
		tmdbID, tvdbID := watchlist.NumericIDs(items[i].ExternalIDs)
		if tmdbID <= 0 && tvdbID <= 0 {
			continue
		}
		status, releaseDate := cached.GetCachedSeriesRelease(tmdbID, tvdbID)
		if status != "" {
			items[i].Status = status
		}
		if releaseDate != "" {
			items[i].ReleaseDate = releaseDate
		}
	}
}

func logDisplayListWatchlistArtworkTrace(userID, source string, items []models.WatchlistItem) {
	if source != "watchlist" && source != "custom-list" {
		return
	}
	for _, item := range items {
		name := strings.ToLower(strings.TrimSpace(item.Name))
		isTarget := strings.Contains(name, "bang my box") || strings.Contains(name, "robyn bird") || strings.Contains(name, "robin byrd")
		hasAnyArtwork := item.PosterURL != "" || item.TextPosterURL != "" || item.BackdropURL != "" ||
			item.TextBackdropURL != "" || len(item.BackdropURLs) > 0
		if !isTarget && hasAnyArtwork {
			continue
		}
		log.Printf(
			"[display-list][artwork] user=%s id=%s mediaType=%s name=%q year=%d target=%t poster=%t textPoster=%t backdrop=%t textBackdrop=%t backdropCount=%d externalIds=%v",
			userID,
			item.ID,
			item.MediaType,
			item.Name,
			item.Year,
			isTarget,
			item.PosterURL != "",
			item.TextPosterURL != "",
			item.BackdropURL != "",
			item.TextBackdropURL != "",
			len(item.BackdropURLs),
			item.ExternalIDs,
		)
	}
}

func logDisplayListPayloadArtworkTrace(userID, source string, payload map[string]interface{}) {
	rawItems, ok := payload["items"].([]interface{})
	if !ok {
		return
	}
	for _, raw := range rawItems {
		itemMap, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		titleMap, ok := itemMap["title"].(map[string]interface{})
		if !ok {
			titleMap = itemMap
		}
		name := displayListString(titleMap["name"])
		if name == "" {
			name = displayListString(titleMap["title"])
		}
		normalizedName := strings.ToLower(strings.TrimSpace(name))
		isTarget := strings.Contains(normalizedName, "bang my box") || strings.Contains(normalizedName, "robyn bird") || strings.Contains(normalizedName, "robin byrd")
		if !isTarget {
			continue
		}
		posterURL := displayListNestedURL(titleMap["poster"])
		textPosterURL := displayListNestedURL(titleMap["textPoster"])
		backdropURL := displayListNestedURL(titleMap["backdrop"])
		textBackdropURL := displayListNestedURL(titleMap["textBackdrop"])
		log.Printf(
			"[display-list][artwork] user=%s source=%s id=%s mediaType=%s name=%q year=%v target=%t poster=%t textPoster=%t backdrop=%t textBackdrop=%t tmdbId=%v tvdbId=%v imdbId=%v",
			userID,
			source,
			displayListString(titleMap["id"]),
			displayListString(titleMap["mediaType"]),
			name,
			titleMap["year"],
			isTarget,
			posterURL != "",
			textPosterURL != "",
			backdropURL != "",
			textBackdropURL != "",
			titleMap["tmdbId"],
			titleMap["tvdbId"],
			titleMap["imdbId"],
		)
	}
}

func displayListString(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func displayListNestedURL(value interface{}) string {
	m, ok := value.(map[string]interface{})
	if !ok {
		return ""
	}
	return displayListString(m["url"])
}
