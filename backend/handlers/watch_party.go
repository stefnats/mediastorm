package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/sessions"
	"novastream/services/watchrooms"
)

const watchPartyCookieName = "strmr_watch_party"

type WatchPartyHandler struct {
	service          *watchrooms.Service
	sessions         *sessions.Service
	serverBasePath   string
	libraryAccess    *libraryaccess.Service
	settings         watchPartySettingsProvider
	managedRoom      *managedWatchRoomEntry
	publicPartyEntry *managedWatchRoomEntry
}

type watchPartySettingsProvider interface {
	Load() (config.Settings, error)
}

func (h *WatchPartyHandler) SetLibraryAccessService(service *libraryaccess.Service) {
	h.libraryAccess = service
}

func (h *WatchPartyHandler) SetSettingsProvider(provider watchPartySettingsProvider) {
	h.settings = provider
}

func NewWatchPartyHandler(service *watchrooms.Service, sessionsService *sessions.Service, serverBasePath string) *WatchPartyHandler {
	serverBasePath = "/" + strings.Trim(serverBasePath, "/")
	if serverBasePath == "/" {
		serverBasePath = ""
	}
	return &WatchPartyHandler{service: service, sessions: sessionsService, serverBasePath: serverBasePath}
}

type watchPartyInviteView struct {
	URL        string    `json:"url"`
	LandingURL string    `json:"landingUrl"`
	ShortCode  string    `json:"shortCode"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

func (h *WatchPartyHandler) CreateInvitation(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.managedRoom.serve(w, r, http.HandlerFunc(h.createManagedInvitation))
		return
	}
	externalBaseURL, err := h.externalBaseURL()
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusPreconditionRequired)
		return
	}
	invite, err := h.service.CreateExternalInvitation(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], mux.Vars(r)["roomID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusCreated, watchPartyInviteView{
		URL: externalBaseURL + "/watch-party/" + invite.Token, LandingURL: externalBaseURL + "/watch-party",
		ShortCode: invite.ShortCode, ExpiresAt: invite.ExpiresAt,
	})
}

func (h *WatchPartyHandler) externalBaseURL() (string, error) {
	if h.settings == nil {
		return "", errors.New("external backend URL is required for external Watch Together access")
	}
	settings, err := h.settings.Load()
	if err != nil {
		return "", errors.New("unable to load the external backend URL")
	}
	if err := settings.Server.NormalizeExternalBackendURL(); err != nil {
		return "", err
	}
	baseURL := strings.TrimRight(settings.Server.ExternalBackendURL, "/")
	if baseURL == "" {
		return "", errors.New("external backend URL is required for external Watch Together access")
	}
	return baseURL, nil
}

func (h *WatchPartyHandler) RevokeInvitation(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.managedRoom.serve(w, r, http.HandlerFunc(h.revokeManagedInvitation))
		return
	}
	err := h.service.RevokeExternalInvitation(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], mux.Vars(r)["roomID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var watchPartySourceAllowedParams = map[string]bool{
	"mediaType": true, "title": true, "seriesTitle": true, "displayName": true,
	"year": true, "seasonNumber": true, "episodeNumber": true, "episodeName": true,
	"imdbId": true, "tvdbId": true, "tmdbId": true, "titleId": true,
	"headerImage": true, "posterUrl": true, "dv": true, "hdr10": true,
	"dvProfile": true, "forceAAC": true, "preselectedAudioTrack": true,
	"preselectedSubtitleTrack": true,
}

func (h *WatchPartyHandler) BindSource(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.managedRoom.serve(w, r, http.HandlerFunc(h.bindManagedSource))
		return
	}
	var body struct {
		Source string            `json:"source"`
		Params map[string]string `json:"params"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	source := strings.TrimSpace(body.Source)
	if source == "" {
		writeJSONError(w, "playback source is required", http.StatusBadRequest)
		return
	}
	profileID := mux.Vars(r)["userID"]
	if h.libraryAccess != nil {
		recognized, allowed, err := h.libraryAccess.CanAccessStream(r.Context(), source, auth.GetAccountID(r), profileID, auth.IsMaster(r))
		if err != nil {
			writeJSONError(w, "failed to verify library access", http.StatusInternalServerError)
			return
		}
		if recognized && !allowed {
			writeJSONError(w, "playback source not found", http.StatusNotFound)
			return
		}
	}
	params := make(map[string]string, len(body.Params)+1)
	for key, value := range body.Params {
		value = strings.TrimSpace(value)
		if value != "" && watchPartySourceAllowedParams[key] {
			params[key] = value
		}
	}
	params["profileId"] = profileID
	_, err := h.service.BindExternalSource(r.Context(), auth.GetAccountID(r), profileID, mux.Vars(r)["roomID"], source, params)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, map[string]bool{"ready": true})
}

func (h *WatchPartyHandler) Landing(w http.ResponseWriter, _ *http.Request) {
	h.renderPage(w, "Join a Watch Together room", `<p>Enter the code shown on the host's TV.</p>
		<form method="post" action="`+html.EscapeString(h.serverBasePath)+`/watch-party/code"><input id="watch-party-code" name="code" maxlength="9" autocomplete="one-time-code" autocapitalize="characters" spellcheck="false" placeholder="ABCD-EFGH" required><button type="submit">Continue</button></form>
		<script>const codeInput=document.getElementById('watch-party-code');codeInput.addEventListener('input',()=>{const compact=codeInput.value.toUpperCase().replace(/[^A-Z2-7]/g,'').slice(0,8);codeInput.value=compact.length>4?compact.slice(0,4)+'-'+compact.slice(4):compact;});</script>`)
}

func (h *WatchPartyHandler) ResolveCode(w http.ResponseWriter, r *http.Request) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(h.resolveManagedPartyCode))
		return
	}
	invite, err := h.service.ResolveExternalInvitation(r.Context(), r.FormValue("code"), true)
	if err != nil {
		h.renderUnavailable(w)
		return
	}
	http.Redirect(w, r, h.serverBasePath+"/watch-party/code/"+invite.ShortCode, http.StatusSeeOther)
}

func (h *WatchPartyHandler) OpenToken(w http.ResponseWriter, r *http.Request) {
	h.openInvitation(w, r, mux.Vars(r)["token"], false)
}

func (h *WatchPartyHandler) OpenCode(w http.ResponseWriter, r *http.Request) {
	h.openInvitation(w, r, mux.Vars(r)["code"], true)
}

func (h *WatchPartyHandler) openInvitation(w http.ResponseWriter, r *http.Request, secret string, byCode bool) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.openManagedPartyInvitation(w, r, secret, byCode) }))
		return
	}
	invite, err := h.service.ResolveExternalInvitation(r.Context(), secret, byCode)
	if err != nil {
		h.renderUnavailable(w)
		return
	}
	room, err := h.service.GetForExternalInvite(r.Context(), invite.RoomID)
	if err != nil {
		h.renderUnavailable(w)
		return
	}
	action := h.serverBasePath + "/watch-party/"
	if byCode {
		action += "code/" + invite.ShortCode
	} else {
		action += secret
	}
	h.renderPage(w, "Join "+room.Title, `<p><strong>`+html.EscapeString(room.CreatorName)+`</strong> invited you to watch <strong>`+html.EscapeString(room.Title)+`</strong>.</p>
		<form method="post" action="`+html.EscapeString(action)+`"><label for="name">Display name</label><input id="name" name="name" maxlength="40" autocomplete="nickname" required><button type="submit">Join room</button></form>`)
}

func (h *WatchPartyHandler) JoinToken(w http.ResponseWriter, r *http.Request) {
	h.joinInvitation(w, r, mux.Vars(r)["token"], false)
}

func (h *WatchPartyHandler) JoinCode(w http.ResponseWriter, r *http.Request) {
	h.joinInvitation(w, r, mux.Vars(r)["code"], true)
}

func (h *WatchPartyHandler) joinInvitation(w http.ResponseWriter, r *http.Request, secret string, byCode bool) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.joinManagedPartyInvitation(w, r, secret, byCode) }))
		return
	}
	invite, err := h.service.ResolveExternalInvitation(r.Context(), secret, byCode)
	if err != nil {
		h.renderUnavailable(w)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" || len([]rune(name)) > 40 {
		h.renderPage(w, "Display name required", `<p>Enter a display name of 40 characters or fewer.</p><p><a href="javascript:history.back()">Go back</a></p>`)
		return
	}
	capabilities := models.WatchRoomClientCapabilities{StateSync: true, ProtocolVersion: 1}
	if session, guestID, ok := h.guestSession(r, invite.RoomID); ok {
		if _, err := h.service.JoinExternalGuest(r.Context(), invite, guestID, name, r.UserAgent(), capabilities); err != nil {
			h.writeError(w, err)
			return
		}
		// Refresh the browser cookie while preserving the room-scoped token. A
		// returning guest must keep the same derived member ID so browser back /
		// rejoin cannot leave an orphaned Away member in the room.
		h.setWatchPartyCookie(w, r, session)
		h.redirectToRoom(w, r, invite.RoomID)
		return
	}
	duration := time.Until(invite.ExpiresAt)
	if duration <= 0 {
		h.renderUnavailable(w)
		return
	}
	session, err := h.sessions.CreateScopedWithResource(invite.AccountID, false, r.UserAgent(), clientIPForShare(r), duration, models.SessionScopeWatchParty, invite.RoomID)
	if err != nil {
		http.Error(w, "failed to join watch party", http.StatusInternalServerError)
		return
	}
	guestID := watchPartyGuestID(session.Token)
	if _, err := h.service.JoinExternalGuest(r.Context(), invite, guestID, name, r.UserAgent(), capabilities); err != nil {
		_ = h.sessions.Revoke(session.Token)
		h.writeError(w, err)
		return
	}
	h.setWatchPartyCookie(w, r, session)
	h.redirectToRoom(w, r, invite.RoomID)
}

func (h *WatchPartyHandler) setWatchPartyCookie(w http.ResponseWriter, r *http.Request, session models.Session) {
	http.SetCookie(w, &http.Cookie{Name: watchPartyCookieName, Value: session.Token, Path: "/", HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode, Expires: session.ExpiresAt})
}

func (h *WatchPartyHandler) redirectToRoom(w http.ResponseWriter, r *http.Request, roomID string) {
	http.Redirect(w, r, h.serverBasePath+"/watch-party/room/"+roomID, http.StatusSeeOther)
}

func watchPartyGuestID(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:16])
}

func (h *WatchPartyHandler) guestSession(r *http.Request, roomID string) (models.Session, string, bool) {
	session, guestID, err := h.lookupGuestSession(r, roomID)
	return session, guestID, err == nil
}

// errGuestSessionMissing means the request carries no usable guest session for
// the room. Any other lookupGuestSession error is a failure to load the session
// (for example a brief database outage) and must not be reported as 401: the
// web player treats 401 as the end of the room and stops playback for good.
var errGuestSessionMissing = errors.New("watch party session required")

func (h *WatchPartyHandler) lookupGuestSession(r *http.Request, roomID string) (models.Session, string, error) {
	if h.publicPartyEntry != nil {
		cookie, e := r.Cookie(watchPartyCookieName)
		if e != nil {
			return models.Session{}, "", errGuestSessionMissing
		}
		authority := h.publicPartyEntry.authority
		if authority == nil {
			return models.Session{}, "", datastore.ErrWatchRoomUnavailable
		}
		actual, release, e := authority.AuthenticateGuestRequest(r, cookie.Value, roomID)
		if e != nil {
			if errors.Is(e, datastore.ErrWatchRoomForbidden) || errors.Is(e, datastore.ErrWatchRoomChanged) || errors.Is(e, datastore.ErrWatchRoomRecreate) {
				return models.Session{}, "", errGuestSessionMissing
			}
			return models.Session{}, "", e
		}
		defer release()
		session, ok := auth.GetSession(actual)
		if !ok || session.Scope != models.SessionScopeWatchParty || session.ScopeResource != roomID {
			return models.Session{}, "", errGuestSessionMissing
		}
		return session, watchPartyGuestID(session.Token), nil
	}

	if session, guestID, ok := verifiedGuestFromContext(r, roomID); ok {
		return session, guestID, nil
	}
	cookie, err := r.Cookie(watchPartyCookieName)
	if err != nil || h.sessions == nil {
		return models.Session{}, "", errGuestSessionMissing
	}
	session, err := h.sessions.Validate(cookie.Value)
	if err != nil {
		if errors.Is(err, sessions.ErrSessionNotFound) || errors.Is(err, sessions.ErrSessionExpired) || errors.Is(err, sessions.ErrInvalidToken) {
			return models.Session{}, "", errGuestSessionMissing
		}
		return models.Session{}, "", err
	}
	if session.Scope != models.SessionScopeWatchParty || session.ScopeResource != roomID {
		return models.Session{}, "", errGuestSessionMissing
	}
	return session, watchPartyGuestID(session.Token), nil
}

// requireGuestSession resolves the guest session for a JSON endpoint, writing
// 401 when there is none and 503 when it could not be loaded.
func (h *WatchPartyHandler) requireGuestSession(w http.ResponseWriter, r *http.Request, roomID string) (models.Session, string, bool) {
	session, guestID, err := h.lookupGuestSession(r, roomID)
	switch {
	case err == nil:
		return session, guestID, true
	case errors.Is(err, errGuestSessionMissing):
		writeJSONError(w, "watch party session required", http.StatusUnauthorized)
	default:
		writeJSONError(w, "watch party session temporarily unavailable", http.StatusServiceUnavailable)
	}
	return models.Session{}, "", false
}

type watchPartyRoomView struct {
	ID                string                   `json:"id"`
	Title             string                   `json:"title"`
	CreatorName       string                   `json:"creatorName"`
	Status            string                   `json:"status"`
	WaitingForReady   bool                     `json:"waitingForReady"`
	Position          float64                  `json:"position"`
	Revision          int64                    `json:"revision"`
	Members           []models.WatchRoomMember `json:"members"`
	CurrentGuestReady bool                     `json:"currentGuestReady"`
	PlaybackReady     bool                     `json:"playbackReady"`
}

func externalRoomView(room *models.WatchRoom, guestID string) watchPartyRoomView {
	members := make([]models.WatchRoomMember, 0, len(room.Members))
	currentGuestReady := false
	for _, member := range room.Members {
		if member.Joined {
			if member.IsGuest && member.ProfileID == "guest:"+guestID {
				currentGuestReady = member.Ready
			}
			member.ProfileID = ""
			member.IconURL = ""
			member.ClientID = ""
			members = append(members, member)
		}
	}
	return watchPartyRoomView{ID: room.ID, Title: room.Title, CreatorName: room.CreatorName, Status: room.Status,
		WaitingForReady: room.WaitingForReady, Position: room.Position, Revision: room.Revision, Members: members,
		CurrentGuestReady: currentGuestReady}
}

func (h *WatchPartyHandler) RoomPage(w http.ResponseWriter, r *http.Request) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(h.managedPartyRoomPage))
		return
	}
	roomID := mux.Vars(r)["roomID"]
	_, guestID, ok := h.guestSession(r, roomID)
	if !ok {
		h.renderUnavailable(w)
		return
	}
	room, err := h.service.GetForExternalGuest(r.Context(), roomID, guestID)
	if err != nil {
		h.renderUnavailable(w)
		return
	}
	h.renderPartyRoomPage(w, roomID, room)
}

func (h *WatchPartyHandler) renderPartyRoomPage(w http.ResponseWriter, roomID string, room *models.WatchRoom) {
	content := `<p>Connected to <strong>` + html.EscapeString(room.Title) + `</strong>.</p>
	<div id="status">Waiting for the host…</div>
	<div id="members"></div>
	<button id="readyButton" class="ready-button" type="button" onclick="toggleReady()">I’m ready</button>
	<div id="readyError" class="inline-error" role="status"></div>
	<script>
	const roomId=` + string(mustJSON(roomID)) + `;
	const base=` + string(mustJSON(h.serverBasePath)) + `;
	let grantRequested=false;
	let currentGuestReady=false;
	let readyBusy=false;
	function renderReady(room){
		currentGuestReady=Boolean(room.currentGuestReady);
		const button=document.getElementById('readyButton');
		button.hidden=room.status!=='lobby';
		button.disabled=readyBusy||room.status!=='lobby';
		button.classList.toggle('is-ready',currentGuestReady);
		button.textContent=currentGuestReady?'Mark not ready':'I’m ready';
	}
	async function toggleReady(){
		if(readyBusy)return;
		readyBusy=true;
		const button=document.getElementById('readyButton');
		const error=document.getElementById('readyError');
		button.disabled=true;
		error.textContent='';
		try{
			const response=await fetch(base+'/api/watch-party/room/'+encodeURIComponent(roomId)+'/ready',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({ready:!currentGuestReady})});
			if(!response.ok)throw new Error('Unable to update readiness.');
			currentGuestReady=!currentGuestReady;
			button.classList.toggle('is-ready',currentGuestReady);
			button.textContent=currentGuestReady?'Mark not ready':'I’m ready';
		}catch(err){error.textContent=err&&err.message?err.message:'Unable to update readiness.'}
		finally{readyBusy=false;button.disabled=false}
	}
	async function refresh(){
		try{
			const response=await fetch(base+'/api/watch-party/room/'+encodeURIComponent(roomId),{credentials:'same-origin',cache:'no-store'});
			if(!response.ok)throw new Error();
			const room=await response.json();
			renderReady(room);
			document.getElementById('status').textContent=room.status==='lobby'?'Waiting for the host to start…':room.status==='ended'?'This room has ended.':room.waitingForReady?'Preparing synchronized playback…':room.playbackReady?'Joining playback…':'The host is preparing playback…';
			document.getElementById('members').innerHTML='<h2>In the room</h2>'+room.members.map(m=>{const state=!m.present?'Away':m.buffering?'Buffering':m.ready?'Ready':'Not ready';return '<div class="member"><span>'+escapeHTML(m.name)+(m.isCreator?' · Host':'')+'</span><span class="member-state">'+state+'</span></div>'}).join('');
			if(room.status==='playing'&&room.playbackReady&&!grantRequested){
				grantRequested=true;
				const grant=await fetch(base+'/api/watch-party/room/'+encodeURIComponent(roomId)+'/playback-grant',{method:'POST',credentials:'same-origin'});
				if(!grant.ok)throw new Error();
				const payload=await grant.json();
				window.location.replace(payload.url);
			}
		}catch{grantRequested=false;document.getElementById('status').textContent='Unable to reach the room.'}
	}
	function escapeHTML(v){const d=document.createElement('div');d.textContent=v||'';return d.innerHTML}
	refresh();setInterval(refresh,2000);
	</script>`
	h.renderPage(w, "Watch Together", content)
}

func mustJSON(value string) []byte {
	b, _ := json.Marshal(value)
	return b
}

func (h *WatchPartyHandler) RoomSnapshot(w http.ResponseWriter, r *http.Request) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(h.managedPartySnapshot))
		return
	}
	roomID := mux.Vars(r)["roomID"]
	_, guestID, ok := h.requireGuestSession(w, r, roomID)
	if !ok {
		return
	}
	buffering, _ := strconv.ParseBool(r.URL.Query().Get("buffering"))
	if err := h.service.HeartbeatExternalGuest(r.Context(), roomID, guestID, r.UserAgent(), buffering); err != nil {
		h.writeError(w, err)
		return
	}
	room, err := h.service.GetForExternalGuest(r.Context(), roomID, guestID)
	if err != nil {
		h.writeError(w, err)
		return
	}
	view := externalRoomView(room, guestID)
	if _, err := h.service.ExternalSourceForGuest(r.Context(), roomID, guestID); err == nil {
		view.PlaybackReady = true
	}
	writeWatchRoomJSON(w, http.StatusOK, view)
}

func (h *WatchPartyHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(h.managedPartyReady))
		return
	}
	roomID := mux.Vars(r)["roomID"]
	_, guestID, ok := h.requireGuestSession(w, r, roomID)
	if !ok {
		return
	}
	var body struct {
		Ready bool `json:"ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.service.SetExternalGuestReady(r.Context(), roomID, guestID, body.Ready); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchPartyHandler) PlaybackGrant(w http.ResponseWriter, r *http.Request) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(h.managedPartyGrant))
		return
	}
	roomID := mux.Vars(r)["roomID"]
	guestSession, guestID, ok := h.requireGuestSession(w, r, roomID)
	if !ok {
		return
	}
	room, err := h.service.GetForExternalGuest(r.Context(), roomID, guestID)
	if err != nil {
		h.writeError(w, err)
		return
	}
	if room.Status != models.WatchRoomStatusPlaying {
		writeJSONError(w, "watch party has not started", http.StatusConflict)
		return
	}
	source, err := h.service.ExternalSourceForGuest(r.Context(), roomID, guestID)
	if err != nil {
		h.writeError(w, err)
		return
	}
	duration := time.Until(guestSession.ExpiresAt)
	if duration > SharePlaybackSessionTTL {
		duration = SharePlaybackSessionTTL
	}
	streamSession, err := h.sessions.CreateWatchPartyPlayback(guestSession.AccountID, r.UserAgent(), clientIPForShare(r), duration, roomID, guestID, source.Resource)
	if err != nil {
		writeJSONError(w, "failed to grant watch party playback", http.StatusInternalServerError)
		return
	}
	values := url.Values{}
	for key, value := range source.Params {
		if watchPartySourceAllowedParams[key] || key == "profileId" {
			values.Set(key, value)
		}
	}
	values.Set("token", streamSession.Token)
	values.Set("shareMode", "1")
	values.Set("sharedSource", "1")
	values.Set("watchPartyRoomId", roomID)
	values.Set("startOffset", strconv.FormatFloat(room.Position, 'f', 2, 64))
	writeWatchRoomJSON(w, http.StatusOK, map[string]string{"url": h.serverBasePath + "/watch/playback.html?" + values.Encode()})
}

func (h *WatchPartyHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if h.publicPartyEntry != nil {
		h.serveManagedParty(w, r, http.HandlerFunc(h.managedPartyHeartbeat))
		return
	}
	roomID := mux.Vars(r)["roomID"]
	_, guestID, ok := h.requireGuestSession(w, r, roomID)
	if !ok {
		return
	}
	if err := h.service.HeartbeatExternalGuest(r.Context(), roomID, guestID, r.UserAgent(), false); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchPartyHandler) writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, watchrooms.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, watchrooms.ErrNotCreator), errors.Is(err, watchrooms.ErrNotMember):
		status = http.StatusForbidden
	case errors.Is(err, watchrooms.ErrInviteUnavailable), errors.Is(err, watchrooms.ErrRoomEnded):
		status = http.StatusGone
	case errors.Is(err, watchrooms.ErrIncompatibleClient):
		status = http.StatusBadRequest
	case errors.Is(err, watchrooms.ErrShareLinksDisabled):
		status = http.StatusForbidden
	case errors.Is(err, watchrooms.ErrSourceUnavailable), errors.Is(err, watchrooms.ErrSourceConflict):
		status = http.StatusConflict
	}
	writeJSONError(w, strings.TrimSpace(err.Error()), status)
}

func (h *WatchPartyHandler) renderUnavailable(w http.ResponseWriter) {
	h.renderPage(w, "Invitation unavailable", `<p>This Watch Together invitation has expired, was replaced, or the room has ended.</p>`)
}

func (h *WatchPartyHandler) renderPage(w http.ResponseWriter, title, content string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	_, _ = w.Write([]byte(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>` + html.EscapeString(title) + `</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#070b14;color:#e2e8f0;font:16px system-ui;padding:24px;box-sizing:border-box}.card{width:min(480px,100%);background:#0f172a;border:1px solid #334155;border-radius:20px;padding:28px;box-sizing:border-box}h1{color:#fff;margin-top:0}h2{font-size:16px;margin-top:24px}p,#status{line-height:1.5;color:#cbd5e1}label{display:block;margin:18px 0 8px}input{width:100%;box-sizing:border-box;padding:14px;border-radius:10px;border:1px solid #475569;background:#020617;color:#fff;font-size:18px;text-transform:none}button{width:100%;margin-top:16px;padding:14px;border:0;border-radius:10px;background:#2563eb;color:#fff;font-size:17px;font-weight:700;cursor:pointer}button:disabled{opacity:.5;cursor:not-allowed}.ready-button.is-ready{background:#334155}.member{display:flex;justify-content:space-between;gap:16px;padding:10px 0;border-bottom:1px solid #1e293b}.member-state{color:#94a3b8;font-size:14px;white-space:nowrap}.inline-error{min-height:20px;margin-top:8px;color:#fca5a5;font-size:14px}a{color:#93c5fd}</style></head><body><main class="card"><h1>` + html.EscapeString(title) + `</h1>` + content + `</main></body></html>`))
}

func (h *WatchPartyHandler) createManagedInvitation(w http.ResponseWriter, r *http.Request) {
	external, e := h.externalBaseURL()
	if e != nil {
		writeJSONError(w, "external backend URL is required", http.StatusPreconditionRequired)
		return
	}
	invite, e := h.managedRoom.authority.ReplaceExternalInvite(r, mux.Vars(r)["userID"], mux.Vars(r)["roomID"])
	if e != nil {
		writeManagedRoomError(w, e)
		return
	}
	view := watchPartyInviteView{URL: external + "/watch-party/" + invite.Token, LandingURL: external + "/watch-party", ShortCode: invite.ShortCode, ExpiresAt: invite.ExpiresAt}
	writeManagedRoomResult(w, r, h.managedRoom, http.StatusCreated, view, invite.ExpiresAt)
}
func (h *WatchPartyHandler) revokeManagedInvitation(w http.ResponseWriter, r *http.Request) {
	if e := h.managedRoom.authority.RevokeExternalInvite(r, mux.Vars(r)["userID"], mux.Vars(r)["roomID"]); e != nil {
		writeManagedRoomError(w, e)
		return
	}
	writeManagedRoomResult(w, r, h.managedRoom, http.StatusNoContent, nil, time.Time{})
}
func (h *WatchPartyHandler) bindManagedSource(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Source string            `json:"source"`
		Params map[string]string `json:"params"`
	}
	if e := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); e != nil {
		if r.Context().Err() != nil {
			writeManagedRoomError(w, datastore.ErrWatchRoomUnavailable)
		} else {
			writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		}
		return
	}
	profile := mux.Vars(r)["userID"]
	params := make(map[string]string, len(body.Params)+1)
	for key, value := range body.Params {
		value = strings.TrimSpace(value)
		if value != "" && watchPartySourceAllowedParams[key] {
			params[key] = value
		}
	}
	params["profileId"] = profile
	result, e := h.managedRoom.authority.BindExternalSource(r, profile, mux.Vars(r)["roomID"], body.Source, params)
	if e != nil {
		writeManagedRoomError(w, e)
		return
	}
	// Only the existing public ready view is delivered, never the internal
	// publication deadline or trusted source/nonce provenance.
	writeManagedRoomResult(w, r, h.managedRoom, http.StatusOK, map[string]bool{"ready": true}, result.PublicationUntil)
}

func (h *WatchPartyHandler) SetManagedPublicParty(authority *datastore.WatchRoomAuthority) {
	h.publicPartyEntry = &managedWatchRoomEntry{authority: authority, basePath: h.serverBasePath}
}

type managedPartyEntryKey struct{}

// This marker only avoids duplicate ingress plumbing. Every guest/media core
// still consumes the actual original frame and current private SQL proof.
func (h *WatchPartyHandler) serveManagedParty(w http.ResponseWriter, r *http.Request, next http.Handler) {
	entry := h.publicPartyEntry
	if producer, _ := r.Context().Value(managedPartyEntryKey{}).(*managedWatchRoomEntry); producer != nil && producer == entry {
		next.ServeHTTP(w, r)
		return
	}
	entry.serve(w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		marked := r.WithContext(context.WithValue(r.Context(), managedPartyEntryKey{}, entry))
		complete := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Body.Close may drain a real incomplete request. Keep actual deadline
			// interruption and entry ownership armed through that physical read too.
			defer func() {
				if r.Body != nil {
					_ = r.Body.Close()
				}
			}()
			next.ServeHTTP(w, r)
		})
		roomID := mux.Vars(marked)["roomID"]
		if roomID == "" {
			complete.ServeHTTP(w, marked)
			return
		}
		cookie, e := marked.Cookie(watchPartyCookieName)
		if e != nil {
			writeJSONError(w, "watch party session required", http.StatusUnauthorized)
			if marked.Body != nil {
				_ = marked.Body.Close()
			}
			return
		}
		actual, release, e := entry.authority.AuthenticateGuestRequest(marked, cookie.Value, roomID)
		if e != nil {
			writeManagedPartyGuestError(w, e)
			if marked.Body != nil {
				_ = marked.Body.Close()
			}
			return
		}
		defer release()
		// Re-arm the genuine earlier guest/account expiry while preserving the SAME
		// authenticated request. Physical read/write/Flush ends before release.
		watchRoomIO(w, actual, complete, entry.authority)
	}))
}
func (h *WatchPartyHandler) BoundedManagedPartyEntry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.publicPartyEntry == nil {
			next.ServeHTTP(w, r)
			return
		}
		h.serveManagedParty(w, r, next)
	})
}
func writeManagedPartyGuestError(w http.ResponseWriter, e error) {
	if errors.Is(e, datastore.ErrWatchRoomForbidden) || errors.Is(e, datastore.ErrWatchRoomChanged) || errors.Is(e, datastore.ErrWatchRoomRecreate) {
		writeJSONError(w, "watch party session required", http.StatusUnauthorized)
		return
	}
	writeManagedRoomError(w, e)
}
func withManagedPartyOutput(w http.ResponseWriter, r *http.Request, entry *managedWatchRoomEntry, until time.Time, publish func()) {
	valid := func() bool {
		return entry != nil && entry.authority != nil && entry.authority.Context().Err() == nil && r.Context().Err() == nil && until.After(time.Now())
	}
	if at, ok := r.Context().Deadline(); ok && at.Before(until) {
		until = at
	}
	if !valid() {
		writeJSONError(w, "watch party unavailable", 503)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), until)
	transport := http.NewResponseController(w)
	_ = transport.SetWriteDeadline(until)
	ended := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(ended); _ = transport.SetWriteDeadline(time.Now()) })
	defer func() {
		if !stop() {
			<-ended
		}
		cancel()
	}()
	if ctx.Err() != nil || !valid() {
		writeJSONError(w, "watch party unavailable", 503)
		return
	}
	publish()
	_ = transport.Flush()
}
func (h *WatchPartyHandler) resolveManagedPartyCode(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	if e := r.ParseForm(); e != nil {
		writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		return
	}
	view, e := h.publicPartyEntry.authority.ResolveInvitation(r, r.Form.Get("code"), true)
	if e != nil {
		writeManagedRoomError(w, e)
		return
	}
	withManagedPartyOutput(w, r, h.publicPartyEntry, view.PublicationUntil, func() {
		http.Redirect(w, r, h.serverBasePath+"/watch-party/code/"+url.PathEscape(view.ShortCode), http.StatusSeeOther)
	})
}
func (h *WatchPartyHandler) openManagedPartyInvitation(w http.ResponseWriter, r *http.Request, secret string, byCode bool) {
	view, e := h.publicPartyEntry.authority.ResolveInvitation(r, secret, byCode)
	if e != nil {
		writeManagedRoomError(w, e)
		return
	}
	action := h.serverBasePath + "/watch-party/"
	if byCode {
		action += "code/" + url.PathEscape(view.ShortCode)
	} else {
		action += url.PathEscape(secret)
	}
	withManagedPartyOutput(w, r, h.publicPartyEntry, view.PublicationUntil, func() {
		h.renderPage(w, "Join "+view.Title, `<p><strong>`+html.EscapeString(view.CreatorName)+`</strong> invited you to watch <strong>`+html.EscapeString(view.Title)+`</strong>.</p><form method="post" action="`+html.EscapeString(action)+`"><label for="name">Display name</label><input id="name" name="name" maxlength="40" autocomplete="nickname" required><button type="submit">Join room</button></form>`)
	})
}
func (h *WatchPartyHandler) joinManagedPartyInvitation(w http.ResponseWriter, r *http.Request, secret string, byCode bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 32<<10)
	if e := r.ParseForm(); e != nil {
		writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		return
	}
	previous := ""
	if cookie, e := r.Cookie(watchPartyCookieName); e == nil {
		previous = cookie.Value
	}
	result, e := h.publicPartyEntry.authority.JoinInvitation(r, secret, byCode, r.Form.Get("name"), previous)
	if e != nil {
		writeManagedRoomError(w, e)
		return
	}
	withManagedPartyOutput(w, r, h.publicPartyEntry, result.PublicationUntil, func() {
		h.setWatchPartyCookie(w, r, result.Session)
		http.Redirect(w, r, h.serverBasePath+"/watch-party/room/"+url.PathEscape(result.RoomID), http.StatusSeeOther)
	})
}
func (h *WatchPartyHandler) managedPartyRoomPage(w http.ResponseWriter, r *http.Request) {
	result, e := h.publicPartyEntry.authority.ReadGuestRoom(r, mux.Vars(r)["roomID"])
	if e != nil {
		writeManagedPartyGuestError(w, e)
		return
	}
	withManagedPartyOutput(w, r, h.publicPartyEntry, result.PublicationUntil, func() { h.renderPartyRoomPage(w, result.Room.ID, result.Room) })
}
func (h *WatchPartyHandler) managedPartySnapshot(w http.ResponseWriter, r *http.Request) {
	result, e := h.publicPartyEntry.authority.ReadGuestRoom(r, mux.Vars(r)["roomID"])
	if e != nil {
		writeManagedPartyGuestError(w, e)
		return
	}
	session, ok := auth.GetSession(r)
	if !ok {
		writeManagedPartyGuestError(w, datastore.ErrWatchRoomForbidden)
		return
	}
	view := externalRoomView(result.Room, watchPartyGuestID(session.Token))
	view.PlaybackReady = result.PlaybackReady
	writeManagedRoomResult(w, r, h.publicPartyEntry, http.StatusOK, view, result.PublicationUntil)
}
func (h *WatchPartyHandler) managedPartyReady(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Ready bool `json:"ready"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	decoder.DisallowUnknownFields()
	if e := decoder.Decode(&body); e != nil {
		writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		return
	}
	// An additional value/trailing incomplete body must finish under the same
	// owned read deadline before the actual ready mutation can commit.
	var extra any
	if e := decoder.Decode(&extra); e != io.EOF {
		writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		return
	}
	if e := h.publicPartyEntry.authority.SetGuestReady(r, mux.Vars(r)["roomID"], body.Ready); e != nil {
		writeManagedPartyGuestError(w, e)
		return
	}
	writeManagedRoomResult(w, r, h.publicPartyEntry, http.StatusNoContent, nil, time.Time{})
}
func (h *WatchPartyHandler) managedPartyGrant(w http.ResponseWriter, r *http.Request) {
	if _, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024)); e != nil {
		writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		return
	}
	result, e := h.publicPartyEntry.authority.GrantPartyPlayback(r, mux.Vars(r)["roomID"])
	if e != nil {
		writeManagedPartyGuestError(w, e)
		return
	}
	values := url.Values{}
	for key, value := range result.Params {
		if watchPartySourceAllowedParams[key] || key == "profileId" {
			values.Set(key, value)
		}
	}
	values.Set("token", result.Session.Token)
	values.Set("shareMode", "1")
	values.Set("sharedSource", "1")
	values.Set("watchPartyRoomId", result.RoomID)
	values.Set("startOffset", strconv.FormatFloat(result.Position, 'f', 2, 64))
	writeManagedRoomResult(w, r, h.publicPartyEntry, http.StatusOK, map[string]string{"url": h.serverBasePath + "/watch/playback.html?" + values.Encode()}, result.PublicationUntil)
}
func (h *WatchPartyHandler) managedPartyHeartbeat(w http.ResponseWriter, r *http.Request) {
	if _, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024)); e != nil {
		writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		return
	}
	buffering, _ := strconv.ParseBool(r.URL.Query().Get("buffering"))
	if e := h.publicPartyEntry.authority.HeartbeatGuest(r, mux.Vars(r)["roomID"], buffering); e != nil {
		writeManagedPartyGuestError(w, e)
		return
	}
	writeManagedRoomResult(w, r, h.publicPartyEntry, http.StatusNoContent, nil, time.Time{})
}
