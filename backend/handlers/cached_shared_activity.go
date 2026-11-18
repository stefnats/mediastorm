package handlers

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"novastream/internal/auth"
	"novastream/internal/mediaidentity"
	"novastream/internal/profileaccess"
	"novastream/models"
	historypkg "novastream/services/history"
)

type cachedSharedActivityHistory interface {
	RecentWatchesSnapshotCurrent(*historypkg.RecentWatchesSnapshot) bool
	GetWatchHistoryItem(string, string, string) (*models.WatchHistoryItem, error)
}

type cachedSharedActivityCodes struct{ unavailable, changed, kids, message string }

var cachedRecentActivityCodes = cachedSharedActivityCodes{"recent_watches_reader_unavailable", "recent_watches_snapshot_unavailable", "kids_recent_watches_unavailable", "Cached recent watches unavailable"}
var cachedPopularActivityCodes = cachedSharedActivityCodes{"popular_titles_reader_unavailable", "popular_titles_snapshot_unavailable", "kids_popular_titles_unavailable", "Cached popular titles unavailable"}

func (codes cachedSharedActivityCodes) fail(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": code, "error": codes.message})
}

type cachedSharedLanguageIdentity struct{ id, kind string }
type cachedSharedActivityObservation struct {
	handler                    *DisplayListHandler
	viewer                     string
	profileStamp, privacyStamp [32]byte
	eligible                   map[string]models.User
	users                      sharedShelfUsersService
	history                    cachedSharedActivityHistory
	languages                  cachedOriginalLanguageReader
	hidden                     cachedRecentHidden
	codes                      cachedSharedActivityCodes
}

// The source checks its own optional snapshot method before capture. Requiring
// both methods here would break services that support only one explicit view.
func (h *DisplayListHandler) captureCachedSharedActivity(w http.ResponseWriter, r *http.Request, viewer string, codes cachedSharedActivityCodes, snapshotAvailable bool) (*cachedSharedActivityObservation, bool) {
	fail := func(status int, code string) { codes.fail(w, status, code) }
	if auth.IsShareLinkRequest(r) || auth.GetSessionScope(r) != "" {
		fail(403, "profile_session_required")
		return nil, false
	}
	if _, ok := h.requireUser(w, r); !ok {
		return nil, false
	}
	if r.Context().Err() != nil {
		return nil, false
	}
	if h.MetadataHandler == nil || h.MetadataHandler.UsersService == nil || h.HistoryHandler == nil {
		fail(503, codes.unavailable)
		return nil, false
	}
	profile, found := h.MetadataHandler.UsersService.Get(viewer)
	if !found || profile.ID != viewer || !auth.IsMaster(r) && (auth.GetAccountID(r) == "" || profile.AccountID != auth.GetAccountID(r)) {
		fail(404, "profile_not_found")
		return nil, false
	}
	if !profileaccess.CheckResource(w, r, viewer, "") {
		return nil, false
	}
	if profile.IsKidsProfile {
		fail(403, codes.kids)
		return nil, false
	}
	profileStamp, err := cachedRecentViewerStamp(profile)
	if err != nil {
		fail(503, codes.unavailable)
		return nil, false
	}
	users, usersOK := h.MetadataHandler.UsersService.(sharedShelfUsersService)
	history, historyOK := h.HistoryHandler.Service.(cachedSharedActivityHistory)
	languages, languageOK := h.MetadataHandler.Service.(cachedOriginalLanguageReader)
	if !snapshotAvailable || !usersOK || !historyOK || !languageOK {
		fail(503, codes.unavailable)
		return nil, false
	}
	eligible, privacyStamp, err := cachedRecentContributors(users.ListAll())
	if err != nil {
		fail(503, codes.unavailable)
		return nil, false
	}
	return &cachedSharedActivityObservation{handler: h, viewer: viewer, profileStamp: profileStamp, privacyStamp: privacyStamp, eligible: eligible, users: users, history: history, languages: languages, codes: codes}, true
}

func (o *cachedSharedActivityObservation) captureHidden(w http.ResponseWriter) bool {
	hidden, err := o.handler.cachedRecentHiddenItems(o.viewer)
	if err != nil {
		o.codes.fail(w, 503, o.codes.unavailable)
		return false
	}
	o.hidden = hidden
	return true
}

// These are independent current observations, not a transaction across services.
func (o *cachedSharedActivityObservation) recheck(w http.ResponseWriter, r *http.Request, snapshot *historypkg.RecentWatchesSnapshot, query displayListOriginalLanguageQuery, membership map[cachedSharedLanguageIdentity]bool, configurationCurrent func() bool) bool {
	changed := func() bool { o.codes.fail(w, 409, o.codes.changed); return false }
	currentMatches := originalLanguageMatcherAt(o.languages, query, time.Now())
	for key, accepted := range membership {
		if currentMatches(key.id, key.kind) != accepted {
			return changed()
		}
	}
	latest, found := o.handler.MetadataHandler.UsersService.Get(o.viewer)
	latestStamp, stampErr := cachedRecentViewerStamp(latest)
	if !profileaccess.CheckResource(w, r, o.viewer, "") {
		return false
	}
	if !found || stampErr != nil || latestStamp != o.profileStamp {
		return changed()
	}
	_, privacyStamp, err := cachedRecentContributors(o.users.ListAll())
	if err != nil || privacyStamp != o.privacyStamp {
		return changed()
	}
	hidden, err := o.handler.cachedRecentHiddenItems(o.viewer)
	if err != nil {
		o.codes.fail(w, 503, o.codes.unavailable)
		return false
	}
	if configurationCurrent != nil && !configurationCurrent() {
		return changed()
	}
	if hidden.stamp != o.hidden.stamp || !o.history.RecentWatchesSnapshotCurrent(snapshot) {
		return changed()
	}
	return r.Context().Err() == nil
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
