package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/mediaidentity"
	"novastream/internal/profileaccess"
	"novastream/models"
	historypkg "novastream/services/history"
)

type cachedRecentWatchesReader interface {
	SnapshotRecentWatches(context.Context, string, map[string]models.User, int, time.Time) (*historypkg.RecentWatchesSnapshot, error)
	RecentWatchesSnapshotCurrent(*historypkg.RecentWatchesSnapshot) bool
	GetWatchHistoryItem(string, string, string) (*models.WatchHistoryItem, error)
}

func hasCachedRecentWatchesSource(r *http.Request) bool {
	for _, source := range r.URL.Query()["source"] {
		if strings.EqualFold(strings.TrimSpace(source), "recently-watched-cached") {
			return true
		}
	}
	return false
}

func cachedRecentWatchesError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": "Cached recent watches unavailable"})
}

func cachedRecentViewerStamp(user models.User) ([32]byte, error) {
	// User.MarshalJSON omits PinHash. Bind it privately as well, without putting
	// either the profile or its credential stamp in the response or diagnostics.
	raw, err := json.Marshal(struct {
		Profile models.User
		PinHash string
	}{user, user.PinHash})
	return sha256.Sum256(raw), err
}

func cachedRecentContributors(users []models.User) (map[string]models.User, [32]byte, error) {
	eligible, _ := eligibleSharedShelfUsers(users)
	type contributor struct {
		ID, AccountID, Name, Privacy string
		Kids                         bool
		CreatedAt, UpdatedAt         time.Time
	}
	facts := make([]contributor, 0, len(eligible))
	for id, user := range eligible {
		if id == "" || id != strings.TrimSpace(id) || user.ID != id {
			return nil, [32]byte{}, errors.New("invalid shared contributor")
		}
		facts = append(facts, contributor{id, user.AccountID, user.Name, user.ActivityPrivacy, user.IsKidsProfile, user.CreatedAt, user.UpdatedAt})
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].ID < facts[j].ID })
	raw, err := json.Marshal(facts)
	return eligible, sha256.Sum256(raw), err
}

type cachedRecentHidden struct {
	stamp  [32]byte
	tokens map[string]struct{}
}

// Only the exact primary ID can establish equivalence between legacy and
// canonical provider forms. Saved side IDs and card scalar fields are not proof.
func cachedRecentPrimaryTokens(id, kind string) map[string]struct{} {
	title, valid := cachedRecentTitle(models.RecentWatch{ItemID: id, MediaType: mediaidentity.NormalizeMediaType(kind)})
	if !valid {
		return nil
	}
	providers := make(map[string]string)
	if title.TMDBID > 0 {
		providers["tmdb"] = strconv.FormatInt(title.TMDBID, 10)
	}
	if title.TVDBID > 0 {
		providers["tvdb"] = strconv.FormatInt(title.TVDBID, 10)
	}
	if title.IMDBID != "" {
		providers["imdb"] = title.IMDBID
	}
	return mediaidentity.IdentityTokens(title.MediaType, title.ID, "", 0, 0, providers)
}

func (h *DisplayListHandler) cachedRecentHiddenItems(viewer string) (cachedRecentHidden, error) {
	result := cachedRecentHidden{tokens: make(map[string]struct{})}
	if h.HiddenItemsService == nil {
		return result, errors.New("hidden reader unavailable")
	}
	items, err := h.HiddenItemsService.List(viewer)
	if err != nil {
		return result, err
	}
	owned := make([]models.HiddenItem, len(items))
	for i, item := range items {
		owned[i] = item
		if item.ExternalIDs != nil {
			owned[i].ExternalIDs = make(map[string]string, len(item.ExternalIDs))
			for key, value := range item.ExternalIDs {
				owned[i].ExternalIDs[key] = value
			}
		}
		identity := mediaidentity.Resolve(mediaidentity.Input{MediaType: item.MediaType, ID: item.ID, ExternalIDs: owned[i].ExternalIDs})
		for token := range identity.Tokens {
			result.tokens[identity.MediaType+"\x00"+token] = struct{}{}
		}
		for token := range cachedRecentPrimaryTokens(item.ID, item.MediaType) {
			result.tokens[identity.MediaType+"\x00"+token] = struct{}{}
		}
	}
	raw, err := json.Marshal(owned)
	result.stamp = sha256.Sum256(raw)
	return result, err
}

func (hidden cachedRecentHidden) hides(title models.Title) bool {
	identity := mediaidentity.Resolve(mediaidentity.Input{MediaType: title.MediaType, ID: title.ID})
	for token := range cachedRecentPrimaryTokens(title.ID, title.MediaType) {
		identity.Tokens[token] = struct{}{}
	}
	for token := range identity.Tokens {
		if _, found := hidden.tokens[identity.MediaType+"\x00"+token]; found {
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
	fail := func(status int, code string) { cachedRecentWatchesError(w, status, code) }
	if auth.IsShareLinkRequest(r) || auth.GetSessionScope(r) != "" {
		fail(403, "profile_session_required")
		return
	}
	if _, ok := h.requireUser(w, r); !ok {
		return
	}
	if r.Context().Err() != nil {
		return
	}
	if h.MetadataHandler == nil || h.MetadataHandler.UsersService == nil || h.HistoryHandler == nil {
		fail(503, "recent_watches_reader_unavailable")
		return
	}
	profile, found := h.MetadataHandler.UsersService.Get(viewer)
	if !found || profile.ID != viewer || !auth.IsMaster(r) && (auth.GetAccountID(r) == "" || profile.AccountID != auth.GetAccountID(r)) {
		fail(404, "profile_not_found")
		return
	}
	if !profileaccess.CheckResource(w, r, viewer, "") {
		return
	}
	if profile.IsKidsProfile {
		fail(403, "kids_recent_watches_unavailable")
		return
	}
	profileStamp, err := cachedRecentViewerStamp(profile)
	if err != nil {
		fail(503, "recent_watches_reader_unavailable")
		return
	}
	users, usersOK := h.MetadataHandler.UsersService.(sharedShelfUsersService)
	reader, historyOK := h.HistoryHandler.Service.(cachedRecentWatchesReader)
	languages, languageOK := h.MetadataHandler.Service.(cachedOriginalLanguageReader)
	if !usersOK || !historyOK || !languageOK {
		fail(503, "recent_watches_reader_unavailable")
		return
	}
	eligible, privacyStamp, err := cachedRecentContributors(users.ListAll())
	if err != nil {
		fail(503, "recent_watches_reader_unavailable")
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
	snapshot, err := reader.SnapshotRecentWatches(r.Context(), viewer, eligible, query.windowDays, at)
	if r.Context().Err() != nil {
		return
	}
	if err != nil || snapshot == nil || !snapshot.ObservedAt.Equal(at) {
		fail(409, "recent_watches_snapshot_unavailable")
		return
	}
	hidden, err := h.cachedRecentHiddenItems(viewer)
	if err != nil {
		fail(503, "recent_watches_reader_unavailable")
		return
	}
	type identity struct{ id, kind string }
	membership := map[identity]bool{}
	matches := originalLanguageMatcherAt(languages, query.language, at)
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
		if !valid || hidden.hides(title) {
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
		if query.language.active() {
			key := identity{title.ID, title.MediaType}
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
	// Cache membership is checked again before authority/privacy checks. These
	// independent observations do not claim one transaction across all services.
	currentMatches := originalLanguageMatcherAt(languages, query.language, time.Now())
	for key, accepted := range membership {
		if currentMatches(key.id, key.kind) != accepted {
			fail(409, "recent_watches_snapshot_unavailable")
			return
		}
	}
	latest, found := h.MetadataHandler.UsersService.Get(viewer)
	latestStamp, stampErr := cachedRecentViewerStamp(latest)
	if !profileaccess.CheckResource(w, r, viewer, "") {
		return
	}
	if !found || stampErr != nil || latestStamp != profileStamp {
		fail(409, "recent_watches_snapshot_unavailable")
		return
	}
	_, currentPrivacy, err := cachedRecentContributors(users.ListAll())
	if err != nil || currentPrivacy != privacyStamp {
		fail(409, "recent_watches_snapshot_unavailable")
		return
	}
	currentHidden, err := h.cachedRecentHiddenItems(viewer)
	if err != nil {
		fail(503, "recent_watches_reader_unavailable")
		return
	}
	if query.perProfileCap == 0 {
		currentCap, err := h.cachedRecentProfileCap()
		if err != nil || currentCap != cap {
			fail(409, "recent_watches_snapshot_unavailable")
			return
		}
	}
	if currentHidden.stamp != hidden.stamp || !reader.RecentWatchesSnapshotCurrent(snapshot) {
		fail(409, "recent_watches_snapshot_unavailable")
		return
	}
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(DisplayListResponse{Source: "recently-watched-cached", Items: items, Total: total, AlphabetBuckets: alphabet, OriginalLanguageFilterSupported: true})
}
