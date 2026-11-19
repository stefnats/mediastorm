package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"novastream/models"
	historypkg "novastream/services/history"
)

func queryWatchlistItems(items []models.WatchlistItem, query displayListQueryOptions) ([]models.WatchlistItem, []string, []string) {
	genreItems := make([]models.TrendingItem, len(items))
	for i := range items {
		genreItems[i].Title.Genres = items[i].Genres
	}
	genres := displayListGenres(genreItems)
	alphabet := displayListWatchlistAlphabetBuckets(items, query)
	if !query.Active() {
		return items, genres, alphabet
	}
	result := make([]models.WatchlistItem, 0, len(items))
	for _, item := range items {
		if query.MediaType != "" && query.MediaType != "all" && item.MediaType != query.MediaType {
			continue
		}
		if query.Title != "" && !strings.Contains(strings.ToLower(item.Name), strings.ToLower(query.Title)) {
			continue
		}
		if query.WatchStatus != "" && query.WatchStatus != "all" && item.WatchState != query.WatchStatus {
			continue
		}
		if len(query.Genres) > 0 && !hasAnyDisplayListGenre(item.Genres, query.Genres) {
			continue
		}
		if query.Alphabet != "" && displayListAlphabetBucket(item.Name) != query.Alphabet {
			continue
		}
		result = append(result, item)
	}
	if query.SortBy == "" || query.SortBy == "default" {
		return result, genres, alphabet
	}
	direction := 1
	if query.SortDirection == "desc" {
		direction = -1
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := result[i], result[j]
		cmp := 0
		switch query.SortBy {
		case "added":
			if a.AddedAt.Before(b.AddedAt) {
				cmp = -1
			} else if a.AddedAt.After(b.AddedAt) {
				cmp = 1
			}
		case "name":
			cmp = strings.Compare(sortableDisplayListName(a.Name), sortableDisplayListName(b.Name))
		case "year":
			cmp = compareDisplayListInt(a.Year, b.Year)
		case "duration":
			cmp = compareDisplayListInt(a.RuntimeMinutes, b.RuntimeMinutes)
		case "rating":
			ar, aok := watchlistRating(a, query.RatingSource)
			br, bok := watchlistRating(b, query.RatingSource)
			if aok != bok {
				return aok
			}
			if ar < br {
				cmp = -1
			} else if ar > br {
				cmp = 1
			}
		}
		return cmp*direction < 0
	})
	return result, genres, alphabet
}

func watchlistRating(item models.WatchlistItem, source string) (float64, bool) {
	for _, rating := range item.Ratings {
		if (source == "" || strings.EqualFold(rating.Source, source)) && rating.Max > 0 {
			return rating.Value / rating.Max, true
		}
	}
	return 0, false
}

type displayListQueryOptions struct {
	Title                  string
	MediaType              string
	WatchStatus            string
	Genres                 []string
	SortBy                 string
	SortDirection          string
	RatingSource           string
	IncludeFacets          bool
	IncludeUnwatchedCounts bool
	Alphabet               string
}

func parseDisplayListQuery(r *http.Request) displayListQueryOptions {
	q := r.URL.Query()
	genres := make([]string, 0)
	for _, genre := range strings.Split(q.Get("genres"), ",") {
		if genre = strings.TrimSpace(genre); genre != "" {
			genres = append(genres, genre)
		}
	}
	return displayListQueryOptions{
		Title:                  strings.TrimSpace(q.Get("titleFilter")),
		MediaType:              strings.ToLower(strings.TrimSpace(q.Get("filterMediaType"))),
		WatchStatus:            normalizeDisplayListWatchStatus(q.Get("watchStatus")),
		Genres:                 genres,
		SortBy:                 strings.ToLower(strings.TrimSpace(q.Get("sortBy"))),
		SortDirection:          strings.ToLower(strings.TrimSpace(q.Get("sortDirection"))),
		RatingSource:           strings.ToLower(strings.TrimSpace(q.Get("ratingSource"))),
		IncludeFacets:          strings.EqualFold(strings.TrimSpace(q.Get("includeFacets")), "true"),
		IncludeUnwatchedCounts: strings.EqualFold(strings.TrimSpace(q.Get("includeUnwatchedCounts")), "true"),
		Alphabet:               strings.ToUpper(strings.TrimSpace(q.Get("alphabet"))),
	}
}

// normalizeDisplayListWatchStatus maps the watch-status filter onto the
// canonical WatchState values (none, partial, complete). Descriptive aliases
// are accepted so clients that do not share the frontend's vocabulary (for
// example the native tvOS client) can use the same contract.
func normalizeDisplayListWatchStatus(value string) string {
	status := strings.ToLower(strings.TrimSpace(value))
	switch status {
	case "unwatched", "not_watched", "not-watched":
		return "none"
	case "in_progress", "in-progress", "inprogress", "watching", "started":
		return "partial"
	case "watched", "completed":
		return "complete"
	}
	return status
}

func (q displayListQueryOptions) RequiresIndex() bool {
	return q.Active() || q.IncludeFacets
}

// RequiresFullList reports whether filtering or sorting needs to see the whole
// list before pagination. Facets and per-item watch counts can be computed for
// the requested page without blocking the first page on every list item.
func (q displayListQueryOptions) RequiresFullList() bool {
	return q.Title != "" || (q.MediaType != "" && q.MediaType != "all") ||
		(q.WatchStatus != "" && q.WatchStatus != "all") || len(q.Genres) > 0 ||
		(q.SortBy != "" && q.SortBy != "default") || q.Alphabet != ""
}

func (q displayListQueryOptions) Active() bool {
	return q.RequiresFullList() || q.IncludeUnwatchedCounts
}

func (q displayListQueryOptions) Apply(items []models.TrendingItem) []models.TrendingItem {
	result := make([]models.TrendingItem, 0, len(items))
	for _, item := range items {
		title := item.Title
		if q.MediaType != "" && q.MediaType != "all" && title.MediaType != q.MediaType {
			continue
		}
		if q.Title != "" && !strings.Contains(strings.ToLower(title.Name), strings.ToLower(q.Title)) {
			continue
		}
		if q.WatchStatus != "" && q.WatchStatus != "all" && title.WatchState != q.WatchStatus {
			continue
		}
		if len(q.Genres) > 0 && !hasAnyDisplayListGenre(title.Genres, q.Genres) {
			continue
		}
		if q.Alphabet != "" && displayListAlphabetBucket(title.Name) != q.Alphabet {
			continue
		}
		result = append(result, item)
	}
	if q.SortBy == "" || q.SortBy == "default" {
		return result
	}
	direction := 1
	if q.SortDirection == "desc" {
		direction = -1
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := result[i].Title, result[j].Title
		cmp := 0
		switch q.SortBy {
		case "added":
			// Remote lists do not expose an added timestamp; retain source order.
			cmp = 0
		case "name":
			cmp = strings.Compare(sortableDisplayListName(a.Name), sortableDisplayListName(b.Name))
		case "year":
			cmp = compareDisplayListInt(a.Year, b.Year)
		case "duration":
			cmp = compareDisplayListInt(a.RuntimeMinutes, b.RuntimeMinutes)
		case "rating":
			ar, aok := displayListRating(a, q.RatingSource)
			br, bok := displayListRating(b, q.RatingSource)
			if aok != bok {
				return aok
			}
			if ar < br {
				cmp = -1
			} else if ar > br {
				cmp = 1
			}
		default:
			cmp = 0
		}
		return cmp*direction < 0
	})
	return result
}

func paginateTrendingItems(items []models.TrendingItem, offset, limit int) []models.TrendingItem {
	if offset >= len(items) {
		return []models.TrendingItem{}
	}
	if offset > 0 {
		items = items[offset:]
	}
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return items
}

func displayListGenres(items []models.TrendingItem) []string {
	set := make(map[string]string)
	for _, item := range items {
		for _, genre := range item.Title.Genres {
			trimmed := strings.TrimSpace(genre)
			if trimmed != "" {
				set[strings.ToLower(trimmed)] = trimmed
			}
		}
	}
	genres := make([]string, 0, len(set))
	for _, genre := range set {
		genres = append(genres, genre)
	}
	sort.Strings(genres)
	return genres
}

func displayListAlphabetBuckets(items []models.TrendingItem, query displayListQueryOptions) []string {
	withoutAlphabet := query
	withoutAlphabet.Alphabet = ""
	if withoutAlphabet.Active() {
		items = withoutAlphabet.Apply(items)
	}
	set := make(map[string]struct{})
	for _, item := range items {
		set[displayListAlphabetBucket(item.Title.Name)] = struct{}{}
	}
	return sortedDisplayListAlphabetBuckets(set)
}

func displayListWatchlistAlphabetBuckets(items []models.WatchlistItem, query displayListQueryOptions) []string {
	withoutAlphabet := query
	withoutAlphabet.Alphabet = ""
	filtered, _, _ := queryWatchlistItemsWithoutAlphabet(items, withoutAlphabet)
	set := make(map[string]struct{})
	for _, item := range filtered {
		set[displayListAlphabetBucket(item.Name)] = struct{}{}
	}
	return sortedDisplayListAlphabetBuckets(set)
}

func queryWatchlistItemsWithoutAlphabet(items []models.WatchlistItem, query displayListQueryOptions) ([]models.WatchlistItem, []string, []string) {
	query.Alphabet = ""
	// Avoid recursively computing buckets.
	result := make([]models.WatchlistItem, 0, len(items))
	for _, item := range items {
		if query.MediaType != "" && query.MediaType != "all" && item.MediaType != query.MediaType {
			continue
		}
		if query.Title != "" && !strings.Contains(strings.ToLower(item.Name), strings.ToLower(query.Title)) {
			continue
		}
		if query.WatchStatus != "" && query.WatchStatus != "all" && item.WatchState != query.WatchStatus {
			continue
		}
		if len(query.Genres) > 0 && !hasAnyDisplayListGenre(item.Genres, query.Genres) {
			continue
		}
		result = append(result, item)
	}
	return result, nil, nil
}

func displayListAlphabetBucket(name string) string {
	sortable := strings.ToUpper(sortableDisplayListName(name))
	if sortable == "" || sortable[0] < 'A' || sortable[0] > 'Z' {
		return "#"
	}
	return sortable[:1]
}

func sortedDisplayListAlphabetBuckets(set map[string]struct{}) []string {
	buckets := make([]string, 0, len(set))
	for bucket := range set {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i] == "#" {
			return true
		}
		if buckets[j] == "#" {
			return false
		}
		return buckets[i] < buckets[j]
	})
	return buckets
}

func hasAnyDisplayListGenre(actual, selected []string) bool {
	for _, a := range actual {
		for _, s := range selected {
			if strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(s)) {
				return true
			}
		}
	}
	return false
}

func sortableDisplayListName(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	for _, article := range []string{"the ", "an ", "a "} {
		if strings.HasPrefix(lower, article) {
			return strings.TrimSpace(lower[len(article):])
		}
	}
	return lower
}

func displayListRating(title models.Title, source string) (float64, bool) {
	for _, rating := range title.Ratings {
		if (source == "" || strings.EqualFold(rating.Source, source)) && rating.Max > 0 {
			return rating.Value / rating.Max, true
		}
	}
	return 0, false
}

func compareDisplayListInt(a, b int) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// queryTrendingList applies the shared shelf-page query contract to a complete
// source dataset. Callers remain responsible for visibility and kids rules.
func (h *MetadataHandler) queryTrendingList(
	userID string,
	service metadataService,
	items []models.TrendingItem,
	query displayListQueryOptions,
) ([]models.TrendingItem, []string, []string) {
	// Every page carries watch state, not only sorted or filtered ones: clients
	// draw watched / in-progress badges from it. Clients showing the unwatched
	// count ask for it, which warms series totals the cache is missing.
	if userID != "" && h.HistoryService != nil {
		wh, whErr := h.HistoryService.ListWatchHistory(userID)
		if whErr == nil {
			cw, _ := h.HistoryService.ListSeriesStates(userID)
			pp, _ := h.HistoryService.ListPlaybackProgress(userID)
			enrichTrendingItems(items, buildWatchStateIndex(wh, cw, pp), service, query.IncludeUnwatchedCounts)
		}
	}
	enrichTrendingRatings(items, service)
	genres := displayListGenres(items)
	alphabet := displayListAlphabetBuckets(items, query)
	if query.Active() {
		items = query.Apply(items)
	}
	return items, genres, alphabet
}

// Original-language filtering is supported only by complete stored-list indexes.
// It is deliberately separate from discovery queries and source/audio filters.
const maxDisplayListOriginalLanguages = 32

type displayListOriginalLanguageQuery struct {
	Languages      map[string]bool
	IncludeUnknown bool
}

func (q displayListOriginalLanguageQuery) active() bool {
	return len(q.Languages) > 0 || q.IncludeUnknown
}

func parseDisplayListOriginalLanguageQuery(r *http.Request) (displayListOriginalLanguageQuery, error) {
	q := displayListOriginalLanguageQuery{Languages: map[string]bool{}}
	invalid := errors.New("invalid original-language filter")
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return q, invalid
	}
	for _, raw := range values["originalLanguages"] {
		if len(raw) > maxDisplayListOriginalLanguages*4 {
			return q, invalid
		}
		if raw == "" {
			continue
		}
		for _, code := range strings.Split(raw, ",") {
			if len(code) != 3 || models.NormalizeEvidenceLanguage(code) != code {
				return q, invalid
			}
			q.Languages[code] = true
			if len(q.Languages) > maxDisplayListOriginalLanguages {
				return q, invalid
			}
		}
	}
	var unknown string
	for i, value := range values["includeUnknownOriginalLanguage"] {
		if value != "true" && value != "false" {
			return q, invalid
		}
		if i > 0 && value != unknown {
			return q, invalid
		}
		unknown = value
	}
	q.IncludeUnknown = unknown == "true"
	return q, nil
}

type cachedOriginalLanguageReader interface {
	GetCachedOriginalLanguageEvidence(titleID, mediaType string, at time.Time) *models.OriginalLanguageEvidence
}

// originalLanguageMatcher shares one current observation time and one verified
// cache lookup per exact ID/kind within this response. Saved aliases/scalars
// never substitute for the existing provider-bound cache validation.
func (h *DisplayListHandler) originalLanguageMatcher(userID string, q displayListOriginalLanguageQuery) func(string, string) bool {
	reader, _ := h.metadataForUser(userID).(cachedOriginalLanguageReader)
	return originalLanguageMatcherAt(reader, q, time.Now())
}

// originalLanguageMatcherAt lets cache-only sources bind their own observation
// without invoking per-user metadata configuration or constructing a provider.
func originalLanguageMatcherAt(reader cachedOriginalLanguageReader, q displayListOriginalLanguageQuery, at time.Time) func(string, string) bool {
	type identity struct{ id, kind string }
	facts := map[identity]string{}
	return func(id, kind string) bool {
		key := identity{id, kind}
		language, found := facts[key]
		if !found {
			if reader != nil {
				if fact := reader.GetCachedOriginalLanguageEvidence(id, kind, at); fact != nil {
					language = models.NormalizeEvidenceLanguage(fact.Language)
				}
			}
			facts[key] = language
		}
		return language == "" && q.IncludeUnknown || language != "" && q.Languages[language]
	}
}
func (h *DisplayListHandler) filterOriginalLanguages(userID string, items []models.WatchlistItem, q displayListOriginalLanguageQuery) []models.WatchlistItem {
	if !q.active() {
		return items
	}
	matches := h.originalLanguageMatcher(userID, q)
	result := make([]models.WatchlistItem, 0, len(items))
	for _, item := range items {
		if matches(item.ID, item.MediaType) {
			result = append(result, item)
		}
	}
	return result
}

func validateHiddenManagementQuery(q displayListQueryOptions) error {
	if q.WatchStatus != "" && q.WatchStatus != "all" || len(q.Genres) > 0 || q.IncludeUnwatchedCounts || q.IncludeFacets || q.RatingSource != "" {
		return fmt.Errorf("hidden items do not support watched, genre, rating, general facets or episode-count requirements")
	}
	switch q.SortBy {
	case "", "default", "added", "name", "year":
	default:
		return fmt.Errorf("hidden items support only default, added, name and year sorting")
	}
	if q.MediaType != "" && q.MediaType != "all" && q.MediaType != "movie" && q.MediaType != "series" {
		return fmt.Errorf("unsupported hidden media type")
	}
	return nil
}
func queryHiddenManagementItems(items []models.HiddenItem, q displayListQueryOptions) ([]models.HiddenItem, []string) {
	// Private ordinals keep original HiddenItem fields/IDs intact through the
	// existing stored-field query/sort, even if an adapter returns duplicate IDs.
	index := make([]models.WatchlistItem, len(items))
	for i, item := range items {
		index[i] = models.WatchlistItem{ID: strconv.Itoa(i), Name: item.Name, MediaType: item.MediaType, Year: item.Year, AddedAt: item.HiddenAt}
	}
	ordered, _, alphabet := queryWatchlistItems(index, q)
	result := make([]models.HiddenItem, 0, len(ordered))
	for _, item := range ordered {
		i, _ := strconv.Atoi(item.ID)
		result = append(result, items[i])
	}
	return result, alphabet
}

func validateLovedStoredQuery(r *http.Request, q displayListQueryOptions) error {
	if q.WatchStatus != "" && q.WatchStatus != "all" || len(q.Genres) > 0 || q.IncludeUnwatchedCounts || q.IncludeFacets || q.RatingSource != "" {
		return fmt.Errorf("stored favorites do not support watched, genre, rating, general facets or episode-count requirements")
	}
	// Check every occurrence of unsupported requirements rather than silently
	// ignoring a later active value after a neutral first occurrence.
	for _, value := range r.URL.Query()["watchStatus"] {
		if status := normalizeDisplayListWatchStatus(value); status != "" && status != "all" {
			return fmt.Errorf("stored favorites do not support watched requirements")
		}
	}
	for _, key := range []string{"genres", "ratingSource"} {
		for _, value := range r.URL.Query()[key] {
			if strings.TrimSpace(value) != "" {
				return fmt.Errorf("stored favorites do not support %s", key)
			}
		}
	}
	for _, key := range []string{"hideWatched", "hideUnreleased", "includeUnwatchedCounts", "includeFacets"} {
		for _, value := range r.URL.Query()[key] {
			if value != "" && !strings.EqualFold(strings.TrimSpace(value), "false") {
				return fmt.Errorf("stored favorites do not support %s", key)
			}
		}
	}
	switch q.SortBy {
	case "", "default", "added", "name", "year":
	default:
		return fmt.Errorf("stored favorites support only default, added, name and year sorting")
	}
	if q.SortDirection != "" && q.SortDirection != "asc" && q.SortDirection != "desc" {
		return fmt.Errorf("unsupported stored favorites sort direction")
	}
	if q.MediaType != "" && q.MediaType != "all" && q.MediaType != "movie" && q.MediaType != "series" {
		return fmt.Errorf("unsupported stored favorites media type")
	}
	return nil
}

func queryLovedStoredItems(items []lovedStoredItem, q displayListQueryOptions) ([]lovedStoredItem, []string) {
	// Seed a deterministic default order; stable field sorts retain this tie
	// order. Copy the slice because adapters may share their stored records.
	items = append([]lovedStoredItem(nil), items...)
	sort.SliceStable(items, func(i, j int) bool {
		if !items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].UpdatedAt.After(items[j].UpdatedAt)
		}
		a, b := items[i].Title, items[j].Title
		return a.MediaType+":"+a.ID < b.MediaType+":"+b.ID
	})
	index := make([]models.WatchlistItem, len(items))
	for i, item := range items {
		index[i] = models.WatchlistItem{ID: strconv.Itoa(i), Name: item.Title.Name, MediaType: item.Title.MediaType, Year: item.Title.Year, AddedAt: item.UpdatedAt}
	}
	ordered, _, alphabet := queryWatchlistItems(index, q)
	result := make([]lovedStoredItem, 0, len(ordered))
	for _, row := range ordered {
		i, _ := strconv.Atoi(row.ID)
		result = append(result, items[i])
	}
	return result, alphabet
}

func displayListOriginalLanguageSupportedSource(source string) bool {
	switch source {
	case "watchlist", "custom-list", "custom_user_list", "custom-user-list", "permanent-prequeue", "hidden", "loved-stored", "collection-cached", "continue-watching-cached", "recently-watched-cached", "popular-on-server-cached", "similar-cached", "tmdb-public-list-cached":
		return true
	default:
		return false
	}
}

func validateCachedCollectionQuery(r *http.Request, q displayListQueryOptions) (int64, error) {
	if err := validateLovedStoredQuery(r, q); err != nil {
		return 0, err
	}
	if q.SortBy == "added" {
		return 0, fmt.Errorf("cached collections do not contain added timestamps")
	}
	allowed := map[string]bool{}
	for _, key := range []string{"source", "collectionId", "id", "limit", "offset", "homeView", "titleFilter", "filterMediaType", "sortBy", "sortDirection", "alphabet", "originalLanguages", "includeUnknownOriginalLanguage", "watchStatus", "genres", "ratingSource", "hideWatched", "hideUnreleased", "includeUnwatchedCounts", "includeFacets", "token", "profileUnlock", "profileId", "userId", "clientId"} {
		allowed[key] = true
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return 0, fmt.Errorf("invalid cached collection query")
	}
	for key, items := range values {
		if !allowed[key] {
			return 0, fmt.Errorf("unsupported cached collection requirement: %s", key)
		}
		if key == "originalLanguages" || key == "includeUnknownOriginalLanguage" {
			continue
		}
		for _, item := range items {
			if len(items) > 0 && item != items[0] {
				return 0, fmt.Errorf("conflicting cached collection query")
			}
		}
	}
	for _, key := range []string{"limit", "offset"} {
		if raw, exists := values[key]; exists {
			v, err := strconv.Atoi(raw[0])
			if err != nil || v < 0 || v > 1000000 || strconv.Itoa(v) != raw[0] {
				return 0, fmt.Errorf("invalid cached collection pagination")
			}
		}
	}
	var id int64
	for _, key := range []string{"collectionId", "id"} {
		for _, raw := range values[key] {
			v, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || v <= 0 || strconv.FormatInt(v, 10) != raw || id != 0 && id != v {
				return 0, fmt.Errorf("invalid collection identity")
			}
			id = v
		}
	}
	if id == 0 {
		return 0, fmt.Errorf("collectionId is required")
	}
	return id, nil
}

func queryCachedCollectionTitles(items []models.Title, q displayListQueryOptions) ([]models.Title, []string) {
	index := make([]models.WatchlistItem, len(items))
	for i, title := range items {
		index[i] = models.WatchlistItem{ID: strconv.Itoa(i), Name: title.Name, MediaType: title.MediaType, Year: title.Year}
	}
	filter := q
	filter.SortBy = "default"
	filtered, _, alphabet := queryWatchlistItems(index, filter)
	result := make([]models.Title, 0, len(filtered))
	for _, row := range filtered {
		i, _ := strconv.Atoi(row.ID)
		result = append(result, items[i])
	}
	direction := 1
	if q.SortDirection == "desc" {
		direction = -1
	}
	sort.SliceStable(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if q.SortBy != "name" {
			if (a.Year == 0) != (b.Year == 0) {
				return a.Year != 0
			}
			if a.Year != b.Year {
				return compareDisplayListInt(a.Year, b.Year)*direction < 0
			}
		}
		if cmp := strings.Compare(sortableDisplayListName(a.Name), sortableDisplayListName(b.Name)); cmp != 0 {
			if q.SortBy == "name" {
				return cmp*direction < 0
			}
			return cmp < 0
		}
		return a.ID < b.ID
	})
	return result, alphabet
}

func validateCachedWatchingQuery(r *http.Request, q displayListQueryOptions) error {
	if err := validateLovedStoredQuery(r, q); err != nil {
		return err
	}
	if q.SortBy == "added" {
		return fmt.Errorf("cached continue watching supports only default, name and year sorting")
	}
	allowed := map[string]bool{}
	for _, key := range []string{"source", "limit", "offset", "homeView", "titleFilter", "filterMediaType", "sortBy", "sortDirection", "alphabet", "originalLanguages", "includeUnknownOriginalLanguage", "watchStatus", "genres", "ratingSource", "hideWatched", "hideUnreleased", "includeUnwatchedCounts", "includeFacets", "token", "profileUnlock", "profileId", "userId", "clientId"} {
		allowed[key] = true
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return fmt.Errorf("invalid cached continue watching query")
	}
	for key, items := range values {
		if !allowed[key] {
			return fmt.Errorf("unsupported cached continue watching requirement: %s", key)
		}
		if key == "originalLanguages" || key == "includeUnknownOriginalLanguage" {
			continue
		}
		for _, item := range items {
			if len(items) > 0 && item != items[0] {
				return fmt.Errorf("conflicting cached continue watching query")
			}
		}
	}
	for _, key := range []string{"limit", "offset"} {
		if raw, exists := values[key]; exists {
			value, err := strconv.Atoi(raw[0])
			if err != nil || value < 0 || value > 1000000 || strconv.Itoa(value) != raw[0] {
				return fmt.Errorf("invalid cached continue watching pagination")
			}
		}
	}
	return nil
}

func queryCachedWatchingItems(items []historypkg.CachedContinueWatchingItem, q displayListQueryOptions) ([]historypkg.CachedContinueWatchingItem, []string) {
	index := make([]models.WatchlistItem, len(items))
	for i, item := range items {
		index[i] = models.WatchlistItem{ID: strconv.Itoa(i), Name: item.SeriesTitle, MediaType: item.MediaType, Year: item.Year}
	}
	ordered, _, alphabet := queryWatchlistItems(index, q)
	result := make([]historypkg.CachedContinueWatchingItem, 0, len(ordered))
	for _, row := range ordered {
		i, _ := strconv.Atoi(row.ID)
		result = append(result, items[i])
	}
	return result, alphabet
}

type cachedRecentWatchesQuery struct {
	filter        displayListQueryOptions
	language      displayListOriginalLanguageQuery
	windowDays    int
	perProfileCap int
	limit         int
	offset        int
	hideWatched   bool
}

// Validate every selector before consulting users, history or settings. The path
// remains the viewer even when an existing client sends matching query aliases.
func validateCachedRecentWatchesQuery(r *http.Request, viewerID string) (cachedRecentWatchesQuery, error) {
	q := cachedRecentWatchesQuery{filter: parseDisplayListQuery(r), windowDays: 14}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return q, fmt.Errorf("invalid cached recent watches query")
	}
	allowed := map[string]bool{}
	for _, key := range []string{"source", "limit", "offset", "homeView", "titleFilter", "filterMediaType", "sortBy", "sortDirection", "alphabet", "originalLanguages", "includeUnknownOriginalLanguage", "activityWindowDays", "maxItemsPerProfile", "hideWatched", "watchStatus", "genres", "ratingSource", "hideUnreleased", "includeUnwatchedCounts", "includeFacets", "token", "profileUnlock", "profileId", "userId", "clientId"} {
		allowed[key] = true
	}
	for key, items := range values {
		if !allowed[key] || len(items) != 1 {
			return q, fmt.Errorf("unsupported or duplicate cached recent watches selector")
		}
	}
	if values.Get("source") != "recently-watched-cached" {
		return q, fmt.Errorf("invalid cached recent watches source")
	}
	for _, key := range []string{"profileId", "userId"} {
		if value := strings.TrimSpace(values.Get(key)); value != "" && value != viewerID {
			return q, fmt.Errorf("cached recent watches cannot select another profile")
		}
	}
	for _, key := range []string{"limit", "offset", "activityWindowDays", "maxItemsPerProfile"} {
		items, exists := values[key]
		if !exists {
			continue
		}
		n, parseErr := strconv.Atoi(items[0])
		if parseErr != nil || strconv.Itoa(n) != items[0] || n < 0 {
			return q, fmt.Errorf("invalid cached recent watches pagination or window")
		}
		switch key {
		case "limit":
			q.limit = n
		case "offset":
			q.offset = n
		case "activityWindowDays":
			if n < 1 || n > 90 {
				return q, fmt.Errorf("cached recent watches window must be between 1 and 90 days")
			}
			q.windowDays = n
		case "maxItemsPerProfile":
			if n < 1 || n > 20 {
				return q, fmt.Errorf("cached recent watches cap must be between 1 and 20")
			}
			q.perProfileCap = n
		}
	}
	for _, key := range []string{"hideWatched", "hideUnreleased", "includeUnwatchedCounts", "includeFacets"} {
		items, exists := values[key]
		if !exists {
			continue
		}
		if items[0] != "true" && items[0] != "false" {
			return q, fmt.Errorf("invalid cached recent watches boolean")
		}
		if key == "hideWatched" {
			q.hideWatched = items[0] == "true"
		} else if items[0] != "false" {
			return q, fmt.Errorf("cached recent watches cannot satisfy metadata-dependent requirements")
		}
	}
	if q.filter.WatchStatus != "" && q.filter.WatchStatus != "all" || len(q.filter.Genres) != 0 || q.filter.RatingSource != "" {
		return q, fmt.Errorf("cached recent watches cannot satisfy metadata-dependent requirements")
	}
	if q.filter.SortBy != "" && q.filter.SortBy != "default" && q.filter.SortBy != "name" {
		return q, fmt.Errorf("cached recent watches support only default and name sorting")
	}
	if q.filter.SortDirection != "" && q.filter.SortDirection != "asc" && q.filter.SortDirection != "desc" {
		return q, fmt.Errorf("invalid cached recent watches sort direction")
	}
	if q.filter.MediaType != "" && q.filter.MediaType != "all" && q.filter.MediaType != "movie" && q.filter.MediaType != "series" {
		return q, fmt.Errorf("invalid cached recent watches media type")
	}
	if q.filter.Alphabet != "" && q.filter.Alphabet != "#" && (len(q.filter.Alphabet) != 1 || q.filter.Alphabet[0] < 'A' || q.filter.Alphabet[0] > 'Z') {
		return q, fmt.Errorf("invalid cached recent watches alphabet")
	}
	viewKind := ""
	switch values.Get("homeView") {
	case "", "all":
	case "movies":
		viewKind = "movie"
	case "shows":
		viewKind = "series"
	default:
		return q, fmt.Errorf("cached recent watches support only builtin home views")
	}
	if viewKind != "" {
		if q.filter.MediaType != "" && q.filter.MediaType != "all" && q.filter.MediaType != viewKind {
			return q, fmt.Errorf("conflicting cached recent watches media selectors")
		}
		q.filter.MediaType = viewKind
	}
	q.language, err = parseDisplayListOriginalLanguageQuery(r)
	return q, err
}

type cachedPopularTitlesQuery struct {
	filter       displayListQueryOptions
	language     displayListOriginalLanguageQuery
	windowDays   int
	minimumViews int
	limit        int
	offset       int
	hideWatched  bool
}

// Validate every selector before consulting users, history or settings. The path
// remains the viewer even when an existing client sends matching query aliases.
func validateCachedPopularTitlesQuery(r *http.Request, viewerID string) (cachedPopularTitlesQuery, error) {
	q := cachedPopularTitlesQuery{filter: parseDisplayListQuery(r), minimumViews: 2}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return q, fmt.Errorf("invalid cached popular titles query")
	}
	allowed := map[string]bool{}
	for _, key := range []string{"source", "limit", "offset", "homeView", "titleFilter", "filterMediaType", "sortBy", "sortDirection", "alphabet", "originalLanguages", "includeUnknownOriginalLanguage", "activityWindowDays", "minimumViews", "minimumProfiles", "hideWatched", "watchStatus", "genres", "ratingSource", "hideUnreleased", "includeUnwatchedCounts", "includeFacets", "token", "profileUnlock", "profileId", "userId", "clientId"} {
		allowed[key] = true
	}
	for key, items := range values {
		if !allowed[key] || len(items) != 1 {
			return q, fmt.Errorf("unsupported or duplicate cached popular titles selector")
		}
	}
	if values.Get("source") != "popular-on-server-cached" {
		return q, fmt.Errorf("invalid cached popular titles source")
	}
	for _, key := range []string{"profileId", "userId"} {
		if value := strings.TrimSpace(values.Get(key)); value != "" && value != viewerID {
			return q, fmt.Errorf("cached popular titles cannot select another profile")
		}
	}
	for _, key := range []string{"limit", "offset", "activityWindowDays", "minimumViews", "minimumProfiles"} {
		items, exists := values[key]
		if !exists {
			continue
		}
		n, parseErr := strconv.Atoi(items[0])
		if parseErr != nil || strconv.Itoa(n) != items[0] || n < 0 {
			return q, fmt.Errorf("invalid cached popular titles pagination or window")
		}
		switch key {
		case "limit":
			q.limit = n
		case "offset":
			q.offset = n
		case "activityWindowDays":
			if n < 7 || n > 365 {
				return q, fmt.Errorf("cached popular titles window must be between 7 and 365 days")
			}
			q.windowDays = n
		case "minimumViews", "minimumProfiles":
			if n < 1 || n > 100 {
				return q, fmt.Errorf("cached popular titles minimum views must be between 1 and 100")
			}
			q.minimumViews = n
		}
	}
	if values.Has("minimumViews") && values.Has("minimumProfiles") && values.Get("minimumViews") != values.Get("minimumProfiles") {
		return q, fmt.Errorf("conflicting cached popular titles minimum views")
	}
	for _, key := range []string{"hideWatched", "hideUnreleased", "includeUnwatchedCounts", "includeFacets"} {
		items, exists := values[key]
		if !exists {
			continue
		}
		if items[0] != "true" && items[0] != "false" {
			return q, fmt.Errorf("invalid cached popular titles boolean")
		}
		if key == "hideWatched" {
			q.hideWatched = items[0] == "true"
		} else if items[0] != "false" {
			return q, fmt.Errorf("cached popular titles cannot satisfy metadata-dependent requirements")
		}
	}
	if q.filter.WatchStatus != "" && q.filter.WatchStatus != "all" || len(q.filter.Genres) != 0 || q.filter.RatingSource != "" {
		return q, fmt.Errorf("cached popular titles cannot satisfy metadata-dependent requirements")
	}
	if q.filter.SortBy != "" && q.filter.SortBy != "default" && q.filter.SortBy != "name" {
		return q, fmt.Errorf("cached popular titles support only default and name sorting")
	}
	if q.filter.SortDirection != "" && q.filter.SortDirection != "asc" && q.filter.SortDirection != "desc" {
		return q, fmt.Errorf("invalid cached popular titles sort direction")
	}
	if q.filter.MediaType != "" && q.filter.MediaType != "all" && q.filter.MediaType != "movie" && q.filter.MediaType != "series" {
		return q, fmt.Errorf("invalid cached popular titles media type")
	}
	if q.filter.Alphabet != "" && q.filter.Alphabet != "#" && (len(q.filter.Alphabet) != 1 || q.filter.Alphabet[0] < 'A' || q.filter.Alphabet[0] > 'Z') {
		return q, fmt.Errorf("invalid cached popular titles alphabet")
	}
	viewKind := ""
	switch values.Get("homeView") {
	case "", "all":
	case "movies":
		viewKind = "movie"
	case "shows":
		viewKind = "series"
	default:
		return q, fmt.Errorf("cached popular titles support only builtin home views")
	}
	if viewKind != "" {
		if q.filter.MediaType != "" && q.filter.MediaType != "all" && q.filter.MediaType != viewKind {
			return q, fmt.Errorf("conflicting cached popular titles media selectors")
		}
		q.filter.MediaType = viewKind
	}
	q.language, err = parseDisplayListOriginalLanguageQuery(r)
	return q, err
}

type cachedSimilarQuery struct {
	filter        displayListQueryOptions
	language      displayListOriginalLanguageQuery
	kind          string
	seed          int64
	limit, offset int
}

// Reject unsupported selectors before any profile, settings or cache access.
func validateCachedSimilarQuery(r *http.Request, viewer string) (cachedSimilarQuery, error) {
	q := cachedSimilarQuery{filter: parseDisplayListQuery(r)}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return q, fmt.Errorf("invalid cached similar query")
	}
	allowed := map[string]bool{}
	for _, key := range []string{"source", "mediaType", "tmdbId", "filterMediaType", "titleFilter", "sortBy", "sortDirection", "alphabet", "originalLanguages", "includeUnknownOriginalLanguage", "limit", "offset", "token", "profileUnlock", "profileId", "userId", "clientId"} {
		allowed[key] = true
	}
	for key, values := range values {
		if !allowed[key] || len(values) != 1 {
			return q, fmt.Errorf("unsupported or duplicate cached similar selector")
		}
	}
	if values.Get("source") != "similar-cached" || viewer == "" || strings.TrimSpace(viewer) != viewer {
		return q, fmt.Errorf("invalid cached similar source")
	}
	q.kind = values.Get("mediaType")
	if q.kind != "movie" && q.kind != "series" {
		return q, fmt.Errorf("cached similar requires movie or series seed kind")
	}
	q.seed, err = strconv.ParseInt(values.Get("tmdbId"), 10, 64)
	if err != nil || q.seed <= 0 || strconv.FormatInt(q.seed, 10) != values.Get("tmdbId") {
		return q, fmt.Errorf("invalid cached similar seed")
	}
	for _, key := range []string{"profileId", "userId"} {
		if value := strings.TrimSpace(values.Get(key)); value != "" && value != viewer {
			return q, fmt.Errorf("cached similar cannot select another profile")
		}
	}
	for _, key := range []string{"limit", "offset"} {
		if !values.Has(key) {
			continue
		}
		n, err := strconv.Atoi(values.Get(key))
		if err != nil || n < 0 || strconv.Itoa(n) != values.Get(key) {
			return q, fmt.Errorf("invalid cached similar pagination")
		}
		if key == "limit" {
			q.limit = n
		} else {
			q.offset = n
		}
	}
	if values.Has("filterMediaType") && values.Get("filterMediaType") != "all" && values.Get("filterMediaType") != "movie" && values.Get("filterMediaType") != "series" {
		return q, fmt.Errorf("invalid cached similar media filter")
	}
	if values.Has("sortBy") && values.Get("sortBy") != "default" && values.Get("sortBy") != "name" {
		return q, fmt.Errorf("unsupported cached similar sort")
	}
	if values.Has("sortDirection") && values.Get("sortDirection") != "asc" && values.Get("sortDirection") != "desc" {
		return q, fmt.Errorf("invalid cached similar sort direction")
	}
	if values.Has("alphabet") {
		a := values.Get("alphabet")
		if a != "#" && (len(a) != 1 || a[0] < 'A' || a[0] > 'Z') {
			return q, fmt.Errorf("invalid cached similar alphabet")
		}
	}
	q.language, err = parseDisplayListOriginalLanguageQuery(r)
	return q, err
}

type cachedTMDBPublicListQuery struct {
	filter        displayListQueryOptions
	language      displayListOriginalLanguageQuery
	listID        int64
	limit, offset int
}

// Reject unsupported selectors before any profile, settings or cache access.
func validateCachedTMDBPublicListQuery(r *http.Request, viewer string) (cachedTMDBPublicListQuery, error) {
	q := cachedTMDBPublicListQuery{filter: parseDisplayListQuery(r)}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return q, fmt.Errorf("invalid cached TMDB public list query")
	}
	allowed := map[string]bool{}
	for _, key := range []string{"source", "listId", "filterMediaType", "titleFilter", "sortBy", "sortDirection", "alphabet", "originalLanguages", "includeUnknownOriginalLanguage", "limit", "offset", "token", "profileUnlock", "profileId", "userId", "clientId"} {
		allowed[key] = true
	}
	for key, values := range values {
		if !allowed[key] || len(values) != 1 {
			return q, fmt.Errorf("unsupported or duplicate cached TMDB public list selector")
		}
	}
	if values.Get("source") != "tmdb-public-list-cached" || viewer == "" || strings.TrimSpace(viewer) != viewer {
		return q, fmt.Errorf("invalid cached TMDB public list source")
	}
	q.listID, err = strconv.ParseInt(values.Get("listId"), 10, 64)
	if err != nil || q.listID <= 0 || strconv.FormatInt(q.listID, 10) != values.Get("listId") {
		return q, fmt.Errorf("invalid cached TMDB public list identity")
	}
	for _, key := range []string{"profileId", "userId"} {
		if value := strings.TrimSpace(values.Get(key)); value != "" && value != viewer {
			return q, fmt.Errorf("cached TMDB public list cannot select another profile")
		}
	}
	for _, key := range []string{"limit", "offset"} {
		if !values.Has(key) {
			continue
		}
		n, err := strconv.Atoi(values.Get(key))
		if err != nil || n < 0 || strconv.Itoa(n) != values.Get(key) {
			return q, fmt.Errorf("invalid cached TMDB public list pagination")
		}
		if key == "limit" {
			q.limit = n
		} else {
			q.offset = n
		}
	}
	if values.Has("filterMediaType") && values.Get("filterMediaType") != "all" && values.Get("filterMediaType") != "movie" && values.Get("filterMediaType") != "series" {
		return q, fmt.Errorf("invalid cached TMDB public list media filter")
	}
	if values.Has("sortBy") && values.Get("sortBy") != "default" && values.Get("sortBy") != "name" {
		return q, fmt.Errorf("unsupported cached TMDB public list sort")
	}
	if values.Has("sortDirection") && values.Get("sortDirection") != "asc" && values.Get("sortDirection") != "desc" {
		return q, fmt.Errorf("invalid cached TMDB public list sort direction")
	}
	if values.Has("alphabet") {
		a := values.Get("alphabet")
		if a != "#" && (len(a) != 1 || a[0] < 'A' || a[0] > 'Z') {
			return q, fmt.Errorf("invalid cached TMDB public list alphabet")
		}
	}
	q.language, err = parseDisplayListOriginalLanguageQuery(r)
	return q, err
}
