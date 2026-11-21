package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/requestsecurity"
	"novastream/models"
	"novastream/services/libraryaccess"
)

// SharePlaybackSessionTTL is how long the minted stream-scoped session stays valid
// after a share link is opened — long enough to watch a movie with pauses.
const SharePlaybackSessionTTL = 6 * time.Hour

// shareAllowedParams is the whitelist of playback parameters a share link may carry
// through to the web player. Anything not listed is dropped when creating a share.
var shareAllowedParams = map[string]bool{
	"sourcePath": true, "movie": true,
	"profileId": true, "profileName": true, "userId": true,
	"mediaType": true, "title": true, "seriesTitle": true, "displayName": true,
	"year": true, "seasonNumber": true, "episodeNumber": true, "episodeName": true,
	"imdbId": true, "tvdbId": true, "tmdbId": true, "titleId": true,
	"headerImage": true, "posterUrl": true,
	"dv": true, "hdr10": true, "dvProfile": true, "forceAAC": true,
	"preselectedAudioTrack": true, "preselectedSubtitleTrack": true,
	"tvgId": true, "liveSourceUrl": true,
}

// ShareSessionService mints the short-lived stream-scoped session granted to a
// share-link recipient. Satisfied by *sessions.Service.
type ShareSessionService interface {
	CreateScoped(accountID string, isMaster bool, userAgent, ipAddress string, duration time.Duration, scope string) (models.Session, error)
	CreateScopedWithResource(accountID string, isMaster bool, userAgent, ipAddress string, duration time.Duration, scope string, scopeResource string) (models.Session, error)
}

// ShareProfileService resolves the profile a share link is created for so the
// handler can enforce the per-profile share permission and profile ownership.
// Satisfied by *users.Service.
type ShareProfileService interface {
	Get(id string) (models.User, bool)
	BelongsToAccount(profileID, accountID string) bool
}

// ShareHandler creates and consumes one-time shareable playback links.
type ShareHandler struct {
	store           *ShareStore
	sessions        ShareSessionService
	users           ShareProfileService
	serverBasePath  string
	libraryAccess   *libraryaccess.Service
	managedCreation bool
	creator         *datastore.ShareLinkCreator
}

// Selecting PG creation remains fail-closed even when its original managed
// creator is unavailable. The existing constructor isolates file compatibility.
func (h *ShareHandler) SetManagedShareCreation(creator *datastore.ShareLinkCreator) {
	h.managedCreation = true
	h.creator = creator
}

func (h *ShareHandler) SetLibraryAccessService(service *libraryaccess.Service) {
	h.libraryAccess = service
}

// NewShareHandler creates a ShareHandler.
func NewShareHandler(store *ShareStore, sessions ShareSessionService, users ShareProfileService, serverBasePath string) *ShareHandler {
	serverBasePath = "/" + strings.Trim(serverBasePath, "/")
	if serverBasePath == "/" {
		serverBasePath = ""
	}
	return &ShareHandler{store: store, sessions: sessions, users: users, serverBasePath: serverBasePath}
}

// MaxShareLinkTTLDays caps the validity period a share link may request.
const MaxShareLinkTTLDays = 7

// shareCreateRequest is the body posted to create a share link. Params carries the
// captured playback parameters; ttlDays and maxUses control validity and reuse.
type shareCreateRequest struct {
	Params  map[string]string `json:"params"`
	TTLDays int               `json:"ttlDays"`
	MaxUses int               `json:"maxUses"`
	Label   string            `json:"label"`
}

type shareLinkView struct {
	URL        string  `json:"url"`
	Token      string  `json:"token"`
	Label      string  `json:"label,omitempty"`
	Title      string  `json:"title,omitempty"`
	MaxUses    int     `json:"maxUses"`
	UseCount   int     `json:"useCount"`
	Active     bool    `json:"active"`
	Expired    bool    `json:"expired"`
	Usable     bool    `json:"usable"`
	CreatedAt  string  `json:"createdAt"`
	ExpiresAt  string  `json:"expiresAt"`
	LastUsedAt *string `json:"lastUsedAt,omitempty"`
}

// Create generates a share link from captured playback parameters. The
// authenticated account is read from the request context, so this handler works
// behind both the session-token middleware (web player) and the admin/account
// cookie auth.
//
// It accepts the structured {params, ttlDays, maxUses, label} body, and also
// tolerates a bare param map for backward compatibility with older clients.
func (h *ShareHandler) Create(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if h.managedCreation && r.Context().Value(shareCreateEntryKey{}) != h {
		h.serveBoundedCreate(w, r, http.HandlerFunc(h.create))
		return
	}
	h.create(w, r)
}

type shareCreateEntryKey struct{}

// BoundedCreateEntry starts the actual managed lifetime before the registered
// authentication/ownership middleware can read a request body. File mode and
// unrelated endpoints retain their original middleware behavior.
func (h *ShareHandler) BoundedCreateEntry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if !h.managedCreation || r.Method != http.MethodPost || (path != "/api/share/create" && path != "/admin/api/share/create" && path != "/account/api/share/create") {
			next.ServeHTTP(w, r)
			return
		}
		h.serveBoundedCreate(w, r, next)
	})
}

func (h *ShareHandler) serveBoundedCreate(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if h.creator == nil || h.creator.Context().Err() != nil {
		writeShareJSONError(w, http.StatusServiceUnavailable, "share creation unavailable")
		return
	}
	if r.Context().Value(shareCreateEntryKey{}) == h {
		next.ServeHTTP(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, shareCreateEntryKey{}, h)
	r = r.WithContext(ctx)
	control := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = control.SetReadDeadline(deadline)
	_ = control.SetWriteDeadline(deadline)
	interrupted := make(chan struct{})
	stopRead := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		_ = control.SetReadDeadline(time.Now())
		_ = control.SetWriteDeadline(time.Now())
	})
	runtimeDone := make(chan struct{})
	stopRuntime := context.AfterFunc(h.creator.Context(), func() { defer close(runtimeDone); cancel() })
	defer func() {
		if !stopRuntime() {
			<-runtimeDone
		}
		if !stopRead() {
			<-interrupted
		}
	}()
	next.ServeHTTP(w, r)
}

func (h *ShareHandler) create(w http.ResponseWriter, r *http.Request) {

	accountID := auth.GetAccountID(r)
	if accountID == "" {
		writeShareJSONError(w, http.StatusUnauthorized, "authentication required")
		return
	}

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		if h.managedCreation && r.Context().Err() != nil {
			writeShareJSONError(w, http.StatusServiceUnavailable, "share creation unavailable")
			return
		}
		writeShareJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var req shareCreateRequest
	if err := json.Unmarshal(raw, &req); err != nil || req.Params == nil {
		// Backward compatibility: older clients posted a bare param map.
		var bare map[string]string
		if err := json.Unmarshal(raw, &bare); err != nil {
			writeShareJSONError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		req.Params = bare
	}

	params := make(map[string]string, len(req.Params))
	for key, value := range req.Params {
		value = strings.TrimSpace(value)
		if value == "" || !shareAllowedParams[key] {
			continue
		}
		params[key] = value
	}

	if params["sourcePath"] == "" && params["movie"] == "" {
		writeShareJSONError(w, http.StatusBadRequest, "no playback source to share")
		return
	}

	// Enforce the per-profile share permission. The link is created for the
	// profile identified by profileId; that profile must belong to the caller's
	// account (master may act across accounts) and must have the grant the master
	// sets. There is no master bypass — the flag is always required.
	if !h.managedCreation && h.users != nil {
		profileID := params["profileId"]
		profile, ok := h.users.Get(profileID)
		if profileID == "" || !ok ||
			(!auth.IsMaster(r) && !h.users.BelongsToAccount(profileID, accountID)) ||
			!profile.AllowShareLinks {
			writeShareJSONError(w, http.StatusForbidden, "share links are not enabled for this profile")
			return
		}
	}
	if h.libraryAccess != nil {
		streamPath := params["sourcePath"]
		if streamPath == "" {
			streamPath = params["movie"]
		}
		profileID := strings.TrimSpace(params["profileId"])
		recognized, allowed, accessErr := h.libraryAccess.CanAccessStream(r.Context(), streamPath, accountID, profileID, auth.IsMaster(r))
		if accessErr != nil {
			writeShareJSONError(w, http.StatusInternalServerError, "failed to verify library access")
			return
		}
		if recognized && !allowed {
			writeShareJSONError(w, http.StatusNotFound, "playback source not found")
			return
		}
	}

	ttl := DefaultShareLinkTTL
	if req.TTLDays > 0 {
		days := req.TTLDays
		if days > MaxShareLinkTTLDays {
			days = MaxShareLinkTTLDays
		}
		ttl = time.Duration(days) * 24 * time.Hour
	}

	var link *models.ShareLink
	if h.managedCreation {
		link, err = h.creator.Create(r, params["profileId"], params, ttl, req.MaxUses, strings.TrimSpace(req.Label))
	} else {
		link, err = h.store.Create(r.Context(), accountID, auth.IsMaster(r), params, ttl, req.MaxUses, strings.TrimSpace(req.Label))
	}
	if err != nil {
		status := http.StatusInternalServerError
		if h.managedCreation {
			status = http.StatusServiceUnavailable
			if errors.Is(err, datastore.ErrShareProvenanceForbidden) {
				status = http.StatusForbidden
			}
		}
		writeShareJSONError(w, status, "failed to create share link")
		return
	}

	if h.managedCreation {
		h.writeManagedCreatedShare(w, r, link)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.viewOf(link))
}

// A committed link remains committed if delivery fails. This bounded managed
// publication never retries, logs the token or calls a failed output rollback.
func (h *ShareHandler) writeManagedCreatedShare(w http.ResponseWriter, r *http.Request, link *models.ShareLink) {
	deadline, ok := r.Context().Deadline()
	if !ok || link == nil {
		writeShareJSONError(w, http.StatusServiceUnavailable, "share creation unavailable")
		return
	}
	if link.ExpiresAt.Before(deadline) {
		deadline = link.ExpiresAt
	}
	valid := func() bool {
		return r.Context().Err() == nil && deadline.After(time.Now()) && h.creator != nil && h.creator.Context().Err() == nil
	}
	if !valid() {
		writeShareJSONError(w, http.StatusServiceUnavailable, "share creation unavailable")
		return
	}
	control := http.NewResponseController(w)
	_ = control.SetWriteDeadline(deadline)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(interrupted); _ = control.SetWriteDeadline(time.Now()) })
	defer func() {
		if !stop() {
			<-interrupted
		}
	}()
	payload, err := json.Marshal(h.viewOf(link))
	if err != nil || len(payload) > 2<<20 || ctx.Err() != nil || !valid() {
		writeShareJSONError(w, http.StatusServiceUnavailable, "share creation unavailable")
		return
	}
	payload = append(payload, '\n')
	if ctx.Err() != nil || !valid() {
		writeShareJSONError(w, http.StatusServiceUnavailable, "share creation unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	if _, err = w.Write(payload); err != nil {
		return
	}
}

// List returns the share links the caller may manage (own links, or all for the
// master account).
func (h *ShareHandler) List(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	accountID := auth.GetAccountID(r)
	if accountID == "" {
		writeShareJSONError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	links, err := h.store.List(r.Context(), accountID, auth.IsMaster(r))
	if err != nil {
		writeShareJSONError(w, http.StatusInternalServerError, "failed to list share links")
		return
	}
	views := make([]shareLinkView, 0, len(links))
	for i := range links {
		views = append(views, h.viewOf(&links[i]))
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"links": views})
}

// SetActive activates or deactivates a share link (?token=...&active=true|false).
func (h *ShareHandler) SetActive(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	accountID := auth.GetAccountID(r)
	if accountID == "" {
		writeShareJSONError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		writeShareJSONError(w, http.StatusBadRequest, "missing token")
		return
	}
	active := r.URL.Query().Get("active") != "false"
	ok, err := h.store.SetActive(r.Context(), accountID, auth.IsMaster(r), token, active)
	if err != nil {
		writeShareJSONError(w, http.StatusInternalServerError, "failed to update share link")
		return
	}
	if !ok {
		writeShareJSONError(w, http.StatusNotFound, "share link not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// Delete removes a share link (?token=...).
func (h *ShareHandler) Delete(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	accountID := auth.GetAccountID(r)
	if accountID == "" {
		writeShareJSONError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	token := strings.TrimSpace(r.URL.Query().Get("token"))
	if token == "" {
		writeShareJSONError(w, http.StatusBadRequest, "missing token")
		return
	}
	ok, err := h.store.Delete(r.Context(), accountID, auth.IsMaster(r), token)
	if err != nil {
		writeShareJSONError(w, http.StatusInternalServerError, "failed to delete share link")
		return
	}
	if !ok {
		writeShareJSONError(w, http.StatusNotFound, "share link not found")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

// viewOf builds the JSON-facing representation of a share link, including its
// absolute-relative URL and a human title derived from the captured params.
func (h *ShareHandler) viewOf(link *models.ShareLink) shareLinkView {
	v := shareLinkView{
		URL:       h.serverBasePath + "/share/" + link.Token,
		Token:     link.Token,
		Label:     link.Label,
		Title:     shareTitle(link.Params),
		MaxUses:   link.EffectiveMaxUses(),
		UseCount:  link.UseCount,
		Active:    link.Active,
		Expired:   link.Expired(),
		Usable:    link.Usable(),
		CreatedAt: link.CreatedAt.UTC().Format(time.RFC3339),
		ExpiresAt: link.ExpiresAt.UTC().Format(time.RFC3339),
	}
	if link.LastUsedAt != nil {
		s := link.LastUsedAt.UTC().Format(time.RFC3339)
		v.LastUsedAt = &s
	}
	return v
}

// shareTitle derives a display title from captured playback params.
func shareTitle(params map[string]string) string {
	if t := params["seriesTitle"]; t != "" {
		if s, e := params["seasonNumber"], params["episodeNumber"]; s != "" && e != "" {
			return t + " S" + s + "E" + e
		}
		return t
	}
	if t := params["title"]; t != "" {
		if y := params["year"]; y != "" {
			return t + " (" + y + ")"
		}
		return t
	}
	return params["displayName"]
}

// Open consumes a one-time share link (GET /share/{token}). It mints a short-lived
// stream-scoped session and redirects into the player-only web view. Already-used,
// expired, or unknown links render a friendly 410 page.
func (h *ShareHandler) Open(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimSpace(shareTokenFromPath(r.URL.Path))

	rec, ok := h.store.Consume(r.Context(), token)
	if !ok {
		renderShareUnavailable(w, h.serverBasePath)
		return
	}

	session, err := h.sessions.CreateScopedWithResource(
		rec.AccountID, rec.IsMaster,
		r.UserAgent(), clientIPForShare(r),
		SharePlaybackSessionTTL, models.SessionScopeStream, shareScopeResource(rec.Params),
	)
	if err != nil {
		http.Error(w, "failed to open shared playback", http.StatusInternalServerError)
		return
	}

	values := url.Values{}
	for key, value := range rec.Params {
		if !shareAllowedParams[key] || shareRedirectSensitiveParams[key] {
			continue
		}
		values.Set(key, value)
	}
	values.Set("token", session.Token)
	values.Set("shareMode", "1")
	values.Set("sharedSource", "1")

	target := h.serverBasePath + "/watch/playback.html?" + values.Encode()
	http.Redirect(w, r, target, http.StatusSeeOther)
}

// shareTokenFromPath extracts the token from a /share/{token} path without
// depending on the mux vars (so it also works in lightweight tests).
func shareTokenFromPath(path string) string {
	idx := strings.LastIndex(path, "/share/")
	if idx < 0 {
		return ""
	}
	return strings.Trim(path[idx+len("/share/"):], "/")
}

func shareScopeResource(params map[string]string) string {
	if params == nil {
		return ""
	}
	for _, key := range []string{"sourcePath", "movie", "liveSourceUrl"} {
		if value := strings.TrimSpace(params[key]); value != "" {
			return value
		}
	}
	return ""
}

var shareRedirectSensitiveParams = map[string]bool{
	"sourcePath":    true,
	"movie":         true,
	"liveSourceUrl": true,
	"displayName":   true,
}

func clientIPForShare(r *http.Request) string {
	return requestsecurity.ClientIP(r)
}

func writeShareJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func renderShareUnavailable(w http.ResponseWriter, basePath string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusGone)
	watchURL := html.EscapeString(basePath + "/watch")
	w.Write([]byte(`<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">` +
		`<meta name="viewport" content="width=device-width, initial-scale=1">` +
		`<title>Link unavailable</title><style>` +
		`html,body{height:100%;margin:0}body{display:flex;align-items:center;justify-content:center;` +
		`background:#0b0d12;color:#e7eaf0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}` +
		`.card{max-width:420px;padding:32px;text-align:center}h1{font-size:20px;margin:0 0 12px}` +
		`p{opacity:.7;line-height:1.5;margin:0 0 20px}a{color:#7aa2ff;text-decoration:none}` +
		`</style></head><body><div class="card"><h1>This share link is no longer available</h1>` +
		`<p>It may have expired, been used up, or been deactivated. ` +
		`Ask whoever shared it to send a new link.</p>` +
		`<a href="` + watchURL + `">Go to the app</a></div></body></html>`))
}
