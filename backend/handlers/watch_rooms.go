package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/watchrooms"
)

type WatchRoomsHandler struct {
	service     *watchrooms.Service
	managedRoom *managedWatchRoomEntry
}

func NewWatchRoomsHandler(service *watchrooms.Service) *WatchRoomsHandler {
	return &WatchRoomsHandler{service: service}
}

func (h *WatchRoomsHandler) Create(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.managedRoom.serve(w, r, http.HandlerFunc(h.createManagedRoom))
		return
	}
	var in models.WatchRoomCreate
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	room, err := h.service.Create(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], in)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusCreated, room)
}

func (h *WatchRoomsHandler) InviteAccount(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeInviteAccount)
		return
	}
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	invite, err := h.service.InviteAccount(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], mux.Vars(r)["roomID"], body.Username)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusCreated, invite)
}

func (h *WatchRoomsHandler) AccountInvitations(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeAccountInvitations)
		return
	}
	invites, err := h.service.AccountInvitations(r.Context(), auth.GetAccountID(r))
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, invites)
}

func (h *WatchRoomsHandler) RoomAccountInvitations(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeRoomAccountInvitations)
		return
	}
	invites, err := h.service.RoomAccountInvitations(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], mux.Vars(r)["roomID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, invites)
}

func (h *WatchRoomsHandler) AcceptAccountInvitation(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeAcceptAccountInvitation)
		return
	}
	room, err := h.service.AcceptAccountInvitation(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], mux.Vars(r)["inviteID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, room)
}

func (h *WatchRoomsHandler) DeclineAccountInvitation(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeDeclineAccountInvitation)
		return
	}
	if err := h.service.DeclineAccountInvitation(r.Context(), auth.GetAccountID(r), mux.Vars(r)["inviteID"]); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchRoomsHandler) RevokeAccountInvitation(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeRevokeAccountInvitation)
		return
	}
	err := h.service.RevokeAccountInvitation(r.Context(), auth.GetAccountID(r), mux.Vars(r)["userID"], mux.Vars(r)["roomID"], mux.Vars(r)["inviteID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchRoomsHandler) Invitations(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeInvitations)
		return
	}
	rooms, err := h.service.Invitations(r.Context(), mux.Vars(r)["userID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, rooms)
}

func (h *WatchRoomsHandler) Get(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeGet)
		return
	}
	room, err := h.service.Get(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"])
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, room)
}

func (h *WatchRoomsHandler) Join(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeJoin)
		return
	}
	var body struct {
		ClientID     string                             `json:"clientId"`
		Capabilities models.WatchRoomClientCapabilities `json:"capabilities"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	room, err := h.service.Join(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"], body.ClientID, body.Capabilities)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, room)
}

func (h *WatchRoomsHandler) Ready(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeReady)
		return
	}
	var body struct {
		Ready bool `json:"ready"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	room, err := h.service.SetReady(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"], body.Ready)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, room)
}

func (h *WatchRoomsHandler) State(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeState)
		return
	}
	var body models.WatchRoomStateUpdate
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	room, err := h.service.UpdateState(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"], body)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeWatchRoomJSON(w, http.StatusOK, room)
}

func (h *WatchRoomsHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeHeartbeat)
		return
	}
	var body struct {
		ClientID  string `json:"clientId"`
		Buffering bool   `json:"buffering"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if err := h.service.Heartbeat(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"], body.ClientID, body.Buffering); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchRoomsHandler) Leave(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeLeave)
		return
	}
	if err := h.service.Leave(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"]); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchRoomsHandler) End(w http.ResponseWriter, r *http.Request) {
	if h.managedRoom != nil {
		h.serveManagedNative(w, r, nativeEnd)
		return
	}
	if err := h.service.End(r.Context(), mux.Vars(r)["roomID"], mux.Vars(r)["userID"]); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *WatchRoomsHandler) Options(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *WatchRoomsHandler) writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, watchrooms.ErrNotFound), errors.Is(err, watchrooms.ErrNotInvited):
		status = http.StatusNotFound
	case errors.Is(err, watchrooms.ErrNotMember), errors.Is(err, watchrooms.ErrNotCreator), errors.Is(err, watchrooms.ErrForeignProfile):
		status = http.StatusForbidden
	case errors.Is(err, watchrooms.ErrInvalidMedia), errors.Is(err, watchrooms.ErrInvalidState), errors.Is(err, watchrooms.ErrIncompatibleClient), errors.Is(err, watchrooms.ErrSameAccount):
		status = http.StatusBadRequest
	case errors.Is(err, watchrooms.ErrAccountNotFound):
		status = http.StatusNotFound
	case errors.Is(err, watchrooms.ErrInviteUnavailable):
		status = http.StatusGone
	case errors.Is(err, watchrooms.ErrAlreadyInvited):
		status = http.StatusConflict
	case errors.Is(err, watchrooms.ErrRevisionConflict):
		status = http.StatusConflict
	case errors.Is(err, watchrooms.ErrRoomEnded):
		status = http.StatusGone
	}
	writeJSONError(w, strings.TrimSpace(err.Error()), status)
}

func writeWatchRoomJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type managedWatchRoomEntry struct {
	authority *datastore.WatchRoomAuthority
	basePath  string
}
type watchRoomEntryKey struct{}
type watchRoomBoundEntry struct {
	entry   *managedWatchRoomEntry
	lease   *datastore.WatchRoomHTTPEntry
	initial *http.Request
}

func (h *WatchRoomsHandler) SetManagedWatchRoomAuthority(authority *datastore.WatchRoomAuthority, basePath string) {
	h.managedRoom = &managedWatchRoomEntry{authority: authority, basePath: "/" + strings.Trim(basePath, "/")}
	if h.managedRoom.basePath == "/" {
		h.managedRoom.basePath = ""
	}
}
func (h *WatchPartyHandler) SetManagedWatchRoomAuthority(authority *datastore.WatchRoomAuthority, basePath string) {
	h.managedRoom = &managedWatchRoomEntry{authority: authority, basePath: "/" + strings.Trim(basePath, "/")}
	if h.managedRoom.basePath == "/" {
		h.managedRoom.basePath = ""
	}
}
func (e *managedWatchRoomEntry) matches(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	p := r.URL.Path
	if e.basePath != "" && strings.HasPrefix(p, e.basePath+"/") {
		p = strings.TrimPrefix(p, e.basePath)
	}
	if p == "/api/watch-room-account-invitations" {
		return r.Method == http.MethodGet
	}
	if strings.HasPrefix(p, "/api/watch-room-account-invitations/") {
		parts := strings.Split(strings.TrimPrefix(p, "/api/watch-room-account-invitations/"), "/")
		return len(parts) == 2 && parts[0] != "" && parts[1] == "decline" && r.Method == http.MethodPost
	}
	const prefix = "/api/users/"
	if !strings.HasPrefix(p, prefix) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(p, prefix), "/")
	if len(parts) < 2 || parts[0] == "" {
		return false
	}
	if len(parts) == 4 && parts[1] == "watch-room-account-invitations" && parts[2] != "" && parts[3] == "accept" {
		return r.Method == http.MethodPost
	}
	if parts[1] != "watch-rooms" {
		return false
	}
	if len(parts) == 2 {
		return r.Method == http.MethodPost
	}
	if len(parts) == 3 && parts[2] != "" {
		return r.Method == http.MethodGet
	}
	if len(parts) == 4 && parts[2] != "" {
		switch parts[3] {
		case "external-invite":
			return r.Method == http.MethodPost || r.Method == http.MethodDelete
		case "external-source", "join", "ready", "state", "heartbeat", "leave", "end":
			return r.Method == http.MethodPost
		case "account-invites":
			return r.Method == http.MethodGet || r.Method == http.MethodPost
		}
	}
	return len(parts) == 5 && parts[2] != "" && parts[3] == "account-invites" && parts[4] != "" && r.Method == http.MethodDelete
}

func (h *WatchRoomsHandler) BoundedManagedEntry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.managedRoom != nil && h.managedRoom.matches(r) {
			h.managedRoom.serve(w, r, next)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (h *WatchPartyHandler) BoundedManagedEntry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.managedRoom != nil && h.managedRoom.matches(r) {
			h.managedRoom.serve(w, r, next)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func watchRoomStopSignal(ctx context.Context, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); cancel() })
	return func() {
		if !stop() {
			<-done
		}
	}
}
func watchRoomIO(w http.ResponseWriter, r *http.Request, next http.Handler, authority *datastore.WatchRoomAuthority) {
	transport := http.NewResponseController(w)
	deadline := time.Now().Add(30 * time.Second)
	if at, ok := r.Context().Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	_ = transport.SetReadDeadline(deadline)
	_ = transport.SetWriteDeadline(deadline)
	ended := make(chan struct{})
	stop := context.AfterFunc(r.Context(), func() {
		defer close(ended)
		_ = transport.SetReadDeadline(time.Now())
		_ = transport.SetWriteDeadline(time.Now())
	})
	writer := &watchRoomHTTPWriter{ResponseWriter: w, request: r, authority: authority}
	defer func() {
		// A real Body.Close may drain unread input. Keep the original read deadline
		// and cancellation signal armed until that physical body work also ends.
		if r.Body != nil {
			_ = r.Body.Close()
		}
		if !writer.started && writer.unavailable() {
			writer.deny()
		}
		// Keep real interruption hooks armed until the actual buffered transport
		// flush ends; retire/join them before any intentional local cancellation.
		_ = transport.Flush()
		if !stop() {
			<-ended
		}
	}()
	next.ServeHTTP(writer, r)
}
func (e *managedWatchRoomEntry) serve(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if e == nil || e.authority == nil || e.authority.Context().Err() != nil {
		_ = http.NewResponseController(w).SetReadDeadline(time.Now())
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(2 * time.Second))
		writeJSONError(w, "watch room operation unavailable", http.StatusServiceUnavailable)
		return
	}
	if bound, _ := r.Context().Value(watchRoomEntryKey{}).(*watchRoomBoundEntry); bound != nil && bound.entry.authority == e.authority {
		if bound.lease.Context().Err() != nil {
			writeJSONError(w, "watch room operation unavailable", http.StatusServiceUnavailable)
			return
		}
		if r == bound.initial {
			next.ServeHTTP(w, r)
			return
		}
		// The post-auth wrapper retains the SAME genuine framed request. Its actual
		// earlier auth deadline must interrupt upstream ownership/body readers, and
		// its physical Flush ends before intentional Authenticate release.
		watchRoomIO(w, r, next, e.authority)
		return
	}
	lease, err := e.authority.OpenHTTPEntry(r)
	if err != nil {
		writeManagedRoomError(w, err)
		return
	}
	ctx, cancel := context.WithDeadline(r.Context(), lease.ExpiresAt())
	stopSource := watchRoomStopSignal(lease.Context(), cancel)
	bounded := r.WithContext(ctx)
	state := &watchRoomBoundEntry{entry: e, lease: lease, initial: bounded}
	bounded = bounded.WithContext(context.WithValue(bounded.Context(), watchRoomEntryKey{}, state))
	state.initial = bounded
	defer func() { stopSource(); cancel(); lease.Close() }()
	watchRoomIO(w, bounded, next, e.authority)
}

type watchRoomHTTPWriter struct {
	http.ResponseWriter
	request         *http.Request
	authority       *datastore.WatchRoomAuthority
	started, denied bool
}

func (w *watchRoomHTTPWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *watchRoomHTTPWriter) unavailable() bool {
	return w.request.Context().Err() != nil || w.authority == nil || w.authority.Context().Err() != nil
}
func (w *watchRoomHTTPWriter) deny() {
	if w.denied {
		return
	}
	w.denied = true
	if w.started {
		return
	}
	w.started = true
	for _, h := range []string{"Content-Length", "Content-Range", "Content-Encoding", "Location", "Set-Cookie"} {
		w.Header().Del(h)
	}
	writeJSONError(w.ResponseWriter, "watch room operation unavailable", http.StatusServiceUnavailable)
}
func (w *watchRoomHTTPWriter) WriteHeader(status int) {
	if w.denied {
		return
	}
	if w.unavailable() {
		w.deny()
		return
	}
	w.started = true
	w.ResponseWriter.WriteHeader(status)
}
func (w *watchRoomHTTPWriter) Write(data []byte) (int, error) {
	if w.denied {
		return 0, datastore.ErrWatchRoomUnavailable
	}
	if w.unavailable() {
		w.deny()
		return 0, datastore.ErrWatchRoomUnavailable
	}
	w.started = true
	return w.ResponseWriter.Write(data)
}
func (w *watchRoomHTTPWriter) Flush() {
	if w.denied {
		return
	}
	if w.unavailable() {
		w.deny()
		return
	}
	w.started = true
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func writeManagedRoomError(w http.ResponseWriter, err error) {
	status := http.StatusForbidden
	switch {
	case errors.Is(err, datastore.ErrWatchRoomUnavailable), errors.Is(err, datastore.ErrWatchRoomBusy), errors.Is(err, datastore.ErrWatchRoomUnknown):
		status = http.StatusServiceUnavailable
	case errors.Is(err, datastore.ErrWatchRoomRecreate), errors.Is(err, datastore.ErrWatchRoomChanged):
		status = http.StatusGone
	case errors.Is(err, datastore.ErrWatchRoomConflict):
		status = http.StatusConflict
	case errors.Is(err, datastore.ErrWatchRoomInvalid):
		status = http.StatusBadRequest
	}
	writeJSONError(w, "watch room operation unavailable", status)
}
func writeManagedRoomResult(w http.ResponseWriter, r *http.Request, entry *managedWatchRoomEntry, status int, value any, until time.Time) {
	valid := func() bool {
		return r.Context().Err() == nil && entry != nil && entry.authority != nil && entry.authority.Context().Err() == nil && until.After(time.Now())
	}
	deadline := time.Now().Add(30 * time.Second)
	if at, ok := r.Context().Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	if until.IsZero() || deadline.Before(until) {
		until = deadline
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 1<<20 || !valid() {
		writeJSONError(w, "watch room operation unavailable", 503)
		return
	}
	output, cancel := context.WithDeadline(r.Context(), until)
	transport := http.NewResponseController(w)
	_ = transport.SetWriteDeadline(until)
	ended := make(chan struct{})
	stop := context.AfterFunc(output, func() { defer close(ended); _ = transport.SetWriteDeadline(time.Now()) })
	defer func() {
		if !stop() {
			<-ended
		}
		cancel()
	}()
	if output.Err() != nil || !valid() {
		writeJSONError(w, "watch room operation unavailable", 503)
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status != http.StatusNoContent {
		if _, err = w.Write(raw); err != nil {
			return
		}
	}
	_ = transport.Flush()
}
func (h *WatchRoomsHandler) createManagedRoom(w http.ResponseWriter, r *http.Request) {
	var in models.WatchRoomCreate
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if e := decoder.Decode(&in); e != nil {
		if r.Context().Err() != nil {
			writeManagedRoomError(w, datastore.ErrWatchRoomUnavailable)
		} else {
			writeManagedRoomError(w, datastore.ErrWatchRoomInvalid)
		}
		return
	}
	room, e := h.managedRoom.authority.CreateRoom(r, mux.Vars(r)["userID"], in)
	if e != nil {
		writeManagedRoomError(w, e)
		return
	}
	writeManagedRoomResult(w, r, h.managedRoom, http.StatusCreated, room, room.ExpiresAt)
}

// This private dispatch selects a concrete original operation; no enum value is
// a user/profile/source grant. Every branch consumes the actual original frame.
type nativeRoomHTTPMethod uint8

const (
	nativeInviteAccount nativeRoomHTTPMethod = iota
	nativeAccountInvitations
	nativeRoomAccountInvitations
	nativeAcceptAccountInvitation
	nativeDeclineAccountInvitation
	nativeRevokeAccountInvitation
	nativeInvitations
	nativeGet
	nativeJoin
	nativeReady
	nativeState
	nativeHeartbeat
	nativeLeave
	nativeEnd
)

func nativeRoomDecode(w http.ResponseWriter, r *http.Request, value any, empty bool) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	decoder.DisallowUnknownFields()
	e := decoder.Decode(value)
	if e == io.EOF && empty {
		return nil
	}
	if e != nil {
		if r.Context().Err() != nil {
			return datastore.ErrWatchRoomUnavailable
		}
		return datastore.ErrWatchRoomInvalid
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return datastore.ErrWatchRoomInvalid
	}
	return nil
}
func nativeRoomEmptyBody(w http.ResponseWriter, r *http.Request) error {
	raw, e := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if e != nil {
		if r.Context().Err() != nil {
			return datastore.ErrWatchRoomUnavailable
		}
		return datastore.ErrWatchRoomInvalid
	}
	if value := strings.TrimSpace(string(raw)); value != "" && value != "{}" {
		return datastore.ErrWatchRoomInvalid
	}
	return nil
}
func (h *WatchRoomsHandler) serveManagedNative(w http.ResponseWriter, r *http.Request, method nativeRoomHTTPMethod) {
	h.managedRoom.serve(w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entry := h.managedRoom
		if entry == nil || entry.authority == nil || entry.authority.Context().Err() != nil {
			writeManagedRoomError(w, datastore.ErrWatchRoomUnavailable)
			return
		}
		vars := mux.Vars(r)
		profile, room, id := vars["userID"], vars["roomID"], vars["inviteID"]
		var value any
		var until time.Time
		var e error
		status := http.StatusOK
		switch method {
		case nativeInviteAccount:
			var in struct {
				Username string `json:"username"`
			}
			if e = nativeRoomDecode(w, r, &in, false); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeInviteResult
			result, e = entry.authority.InviteNativeAccount(r, profile, room, in.Username)
			if e == nil && result != nil {
				value = result.Invite
				until = result.PublicationUntil
				status = http.StatusCreated
			}
		case nativeAccountInvitations, nativeRoomAccountInvitations:
			var result *datastore.WatchRoomNativeInvitesResult
			if method == nativeAccountInvitations {
				result, e = entry.authority.ListNativeAccountInvitations(r)
			} else {
				result, e = entry.authority.ListNativeRoomAccountInvitations(r, profile, room)
			}
			if e == nil && result != nil {
				value = result.Invites
				until = result.PublicationUntil
			}
		case nativeAcceptAccountInvitation:
			if e = nativeRoomEmptyBody(w, r); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeResult
			result, e = entry.authority.AcceptNativeAccountInvitation(r, profile, id)
			if e == nil && result != nil {
				value = result.Room
				until = result.PublicationUntil
			}
		case nativeInvitations:
			var result *datastore.WatchRoomNativeRoomsResult
			result, e = entry.authority.ListNativeInvitations(r, profile)
			if e == nil && result != nil {
				value = result.Rooms
				until = result.PublicationUntil
			}
		case nativeGet:
			var result *datastore.WatchRoomNativeResult
			result, e = entry.authority.ReadNativeRoom(r, profile, room)
			if e == nil && result != nil {
				value = result.Room
				until = result.PublicationUntil
			}
		case nativeJoin:
			var in struct {
				ClientID     string                             `json:"clientId"`
				Capabilities models.WatchRoomClientCapabilities `json:"capabilities"`
			}
			if e = nativeRoomDecode(w, r, &in, false); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeResult
			result, e = entry.authority.JoinNativeRoom(r, profile, room, in.ClientID, in.Capabilities)
			if e == nil && result != nil {
				value = result.Room
				until = result.PublicationUntil
			}
		case nativeReady:
			var in struct {
				Ready bool `json:"ready"`
			}
			if e = nativeRoomDecode(w, r, &in, false); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeResult
			result, e = entry.authority.SetNativeReady(r, profile, room, in.Ready)
			if e == nil && result != nil {
				value = result.Room
				until = result.PublicationUntil
			}
		case nativeState:
			var in models.WatchRoomStateUpdate
			if e = nativeRoomDecode(w, r, &in, false); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeResult
			result, e = entry.authority.UpdateNativeState(r, profile, room, in)
			if e == nil && result != nil {
				value = result.Room
				until = result.PublicationUntil
			}
		case nativeHeartbeat:
			var in struct {
				ClientID  string `json:"clientId"`
				Buffering bool   `json:"buffering"`
			}
			if e = nativeRoomDecode(w, r, &in, true); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeMutationResult
			result, e = entry.authority.HeartbeatNativeRoom(r, profile, room, in.ClientID, in.Buffering)
			if e == nil && result != nil {
				until = result.PublicationUntil
				status = http.StatusNoContent
			}
		case nativeLeave, nativeEnd, nativeDeclineAccountInvitation, nativeRevokeAccountInvitation:
			if e = nativeRoomEmptyBody(w, r); e != nil {
				break
			}
			var result *datastore.WatchRoomNativeMutationResult
			switch method {
			case nativeLeave:
				result, e = entry.authority.LeaveNativeRoom(r, profile, room)
			case nativeEnd:
				result, e = entry.authority.EndNativeRoom(r, profile, room)
			case nativeDeclineAccountInvitation:
				result, e = entry.authority.DeclineNativeAccountInvitation(r, id)
			case nativeRevokeAccountInvitation:
				result, e = entry.authority.RevokeNativeAccountInvitation(r, profile, room, id)
			}
			if e == nil && result != nil {
				until = result.PublicationUntil
				status = http.StatusNoContent
			}
		default:
			e = datastore.ErrWatchRoomInvalid
		}
		if e != nil {
			writeManagedRoomError(w, e)
			return
		}
		if until.IsZero() {
			writeManagedRoomError(w, datastore.ErrWatchRoomUnavailable)
			return
		}
		writeManagedRoomResult(w, r, entry, status, value, until)
	}))
}
