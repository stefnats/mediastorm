package handlers

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	metadatapkg "novastream/services/metadata"
)

type cachedSimilarReader interface {
	GetCachedSimilarSnapshot(string, int64, time.Time) (*metadatapkg.CachedSimilarSnapshot, error)
	CachedSimilarMetadataLanguage() string
}

var cachedSimilarCodes = cachedSharedActivityCodes{"similar_reader_unavailable", "similar_snapshot_unavailable", "kids_similar_unavailable", "Cached similar titles unavailable"}

type cachedSimilarObservation struct {
	TMDBID           int64     `json:"tmdbId"`
	MediaType        string    `json:"mediaType"`
	MetadataLanguage string    `json:"metadataLanguage"`
	ObservedAt       time.Time `json:"observedAt"`
	ExpiresAt        time.Time `json:"expiresAt"`
	Scope            string    `json:"scope"`
}

type cachedSimilarResponse struct {
	DisplayListResponse
	Similar cachedSimilarObservation `json:"similar"`
}

func hasCachedSimilarSource(r *http.Request) bool {
	for _, source := range r.URL.Query()["source"] {
		if strings.EqualFold(strings.TrimSpace(source), "similar-cached") {
			return true
		}
	}
	return false
}

func (h *DisplayListHandler) cachedSimilarViewer(w http.ResponseWriter, r *http.Request, viewer string) ([32]byte, bool) {
	if auth.IsShareLinkRequest(r) || auth.GetSessionScope(r) != "" {
		cachedSimilarCodes.fail(w, 403, "profile_session_required")
		return [32]byte{}, false
	}
	if _, ok := h.requireUser(w, r); !ok {
		return [32]byte{}, false
	}
	if h.MetadataHandler == nil || h.MetadataHandler.UsersService == nil {
		cachedSimilarCodes.fail(w, 503, cachedSimilarCodes.unavailable)
		return [32]byte{}, false
	}
	profile, found := h.MetadataHandler.UsersService.Get(viewer)
	if !found || profile.ID != viewer || !auth.IsMaster(r) && (auth.GetAccountID(r) == "" || profile.AccountID != auth.GetAccountID(r)) {
		cachedSimilarCodes.fail(w, 404, "profile_not_found")
		return [32]byte{}, false
	}
	if !profileaccess.CheckResource(w, r, viewer, "") {
		return [32]byte{}, false
	}
	if profile.IsKidsProfile {
		cachedSimilarCodes.fail(w, 403, cachedSimilarCodes.kids)
		return [32]byte{}, false
	}
	stamp, err := cachedRecentViewerStamp(profile)
	if err != nil {
		cachedSimilarCodes.fail(w, 503, cachedSimilarCodes.unavailable)
		return [32]byte{}, false
	}
	return stamp, true
}

// This independently checks the interface contract even when a test/alternate
// cache reader is supplied. The stamp remains private and grants no authority.
func cachedSimilarSnapshotStamp(value *metadatapkg.CachedSimilarSnapshot, q cachedSimilarQuery, locale string, at time.Time) ([32]byte, error) {
	unavailable := fmt.Errorf("cached similar observation unavailable")
	if value == nil || value.Version != 1 || value.Provider != "tmdb" || value.MediaType != q.kind || value.TMDBID != q.seed ||
		value.MetadataLanguage != locale || locale == "" || value.ObservedAt.IsZero() || value.ObservedAt.After(at) || !value.ExpiresAt.After(at) ||
		!value.ExpiresAt.After(value.ObservedAt) || value.ExpiresAt.Sub(value.ObservedAt) > 24*time.Hour || value.Candidates == nil || len(value.Candidates) > 4096 {
		return [32]byte{}, unavailable
	}
	seen := make(map[int64]bool)
	for _, candidate := range value.Candidates {
		if candidate.TMDBID <= 0 || seen[candidate.TMDBID] || len(candidate.Name) > 1024 || !utf8.ValidString(candidate.Name) {
			return [32]byte{}, unavailable
		}
		for _, character := range candidate.Name {
			if character < 0x20 || character == 0x7f {
				return [32]byte{}, unavailable
			}
		}
		seen[candidate.TMDBID] = true
		primary := "tmdb:movie:" + strconv.FormatInt(candidate.TMDBID, 10)
		if q.kind == "series" {
			primary = "tmdb:tv:" + strconv.FormatInt(candidate.TMDBID, 10)
		}
		if candidate.ID != primary && !(q.kind == "series" && candidate.ID == "tmdb:series:"+strconv.FormatInt(candidate.TMDBID, 10)) {
			return [32]byte{}, unavailable
		}
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 4<<20 {
		return [32]byte{}, unavailable
	}
	return sha256.Sum256(raw), nil
}

func (h *DisplayListHandler) getCachedSimilar(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	viewer := mux.Vars(r)["userID"]
	query, err := validateCachedSimilarQuery(r, viewer)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	fail := func(status int, code string) { cachedSimilarCodes.fail(w, status, code) }
	profileStamp, ok := h.cachedSimilarViewer(w, r, viewer)
	if !ok || r.Context().Err() != nil {
		return
	}
	context, err := h.MetadataHandler.captureCachedMetadata(viewer)
	if err != nil {
		fail(503, cachedSimilarCodes.unavailable)
		return
	}
	hidden, err := h.cachedRecentHiddenItems(viewer)
	if err != nil {
		fail(503, cachedSimilarCodes.unavailable)
		return
	}
	at := time.Now()
	snapshot, err := context.reader.GetCachedSimilarSnapshot(query.kind, query.seed, at)
	if r.Context().Err() != nil {
		return
	}
	if err != nil {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	stamp, err := cachedSimilarSnapshotStamp(snapshot, query, context.locale, at)
	if err != nil {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	// Copy the validated finite pool before independent current observations.
	media, err := h.requestStoredMediaMatcher(r, viewer)
	if err != nil {
		fail(503, "stored_media_unavailable")
		return
	}
	titles := snapshot.Titles()
	observation := cachedSimilarObservation{snapshot.TMDBID, snapshot.MediaType, snapshot.MetadataLanguage, snapshot.ObservedAt, snapshot.ExpiresAt, "returned-results"}
	matches := originalLanguageMatcherAt(context.languages, query.language, at)
	membership := make(map[cachedSharedLanguageIdentity]bool)
	selected := make([]models.Title, 0, len(titles))
	facets := make([]models.TrendingItem, 0, len(titles))
	for _, title := range titles {
		if r.Context().Err() != nil {
			return
		}
		if hidden.hides(title) {
			continue
		}
		if !media.matches(title) {
			continue
		}
		if query.language.active() {
			identity := cachedSharedLanguageIdentity{title.ID, title.MediaType}
			membership[identity] = matches(identity.id, identity.kind)
			if !membership[identity] {
				continue
			}
		}
		if query.filter.MediaType != "" && query.filter.MediaType != "all" && query.filter.MediaType != title.MediaType {
			continue
		}
		if query.filter.Title != "" && !strings.Contains(strings.ToLower(title.Name), strings.ToLower(query.filter.Title)) {
			continue
		}
		facets = append(facets, models.TrendingItem{Title: title})
		if query.filter.Alphabet != "" && query.filter.Alphabet != displayListAlphabetBucket(title.Name) {
			continue
		}
		selected = append(selected, title)
	}
	if query.filter.SortBy == "name" {
		sort.SliceStable(selected, func(i, j int) bool {
			a, b := sortableDisplayListName(selected[i].Name), sortableDisplayListName(selected[j].Name)
			if query.filter.SortDirection == "desc" {
				return a > b
			}
			return a < b
		})
	}
	total := len(selected)
	items := make([]models.Title, 0)
	if query.offset < len(selected) {
		items = selected[query.offset:]
		if query.limit > 0 && query.limit < len(items) {
			items = items[:query.limit]
		}
	}
	// These independent rechecks do not form a global immutable transaction.
	now := time.Now()
	latestSnapshot, err := context.reader.GetCachedSimilarSnapshot(query.kind, query.seed, now)
	if r.Context().Err() != nil {
		return
	}
	if err != nil {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	latestStamp, err := cachedSimilarSnapshotStamp(latestSnapshot, query, context.locale, now)
	if err != nil || latestStamp != stamp || !observation.ExpiresAt.After(time.Now()) {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	latestHidden, err := h.cachedRecentHiddenItems(viewer)
	if err != nil {
		fail(503, cachedSimilarCodes.unavailable)
		return
	}
	if latestHidden.stamp != hidden.stamp {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	currentMatches := originalLanguageMatcherAt(context.languages, query.language, time.Now())
	for identity, accepted := range membership {
		if currentMatches(identity.id, identity.kind) != accepted {
			fail(409, cachedSimilarCodes.changed)
			return
		}
	}
	current, err := h.MetadataHandler.cachedMetadataCurrent(viewer, context)
	if err != nil {
		fail(503, cachedSimilarCodes.unavailable)
		return
	}
	if !current {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	latestProfileStamp, ok := h.cachedSimilarViewer(w, r, viewer)
	if !ok || r.Context().Err() != nil {
		return
	}
	if latestProfileStamp != profileStamp {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	if !observation.ExpiresAt.After(time.Now()) {
		fail(409, cachedSimilarCodes.changed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(cachedSimilarResponse{DisplayListResponse{Source: "similar-cached", Items: items, Total: total, AlphabetBuckets: displayListAlphabetBuckets(facets, displayListQueryOptions{}), OriginalLanguageFilterSupported: true, StoredMediaFilterSupported: h.storedMediaReady(), StoredMediaFilterBasis: storedListMediaBasis(media.filter.Active())}, observation})
}
