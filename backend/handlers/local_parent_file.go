package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/kidsvod"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
)

// ParentLocalFileHandler is an explicitly configured raw-local-only vertical.
// Main does not install it yet; unsupported media routes are a separate closure.
type ParentLocalFileHandler struct {
	store       *datastore.DataStore
	pins        *profileunlock.PGProfilePins
	parent      *parentauth.Guard
	local       *localmedia.Service
	core        *kidsvod.Service
	httpRuntime *parentFileHTTPRuntime
	hlsMu       sync.Mutex
	hls         *parentFileHLSManager
}
type parentFileProfiles struct{ store *datastore.DataStore }

func (p parentFileProfiles) CurrentProfile(ctx context.Context, id string) (kidsvod.Profile, error) {
	v, err := p.store.CurrentRegionalProfile(ctx, id)
	return kidsvod.Profile{ID: v.ProfileID, AccountID: v.AccountID, Revision: v.Revision, IsKids: v.IsKids}, err
}
func NewParentLocalFileHandler(store *datastore.DataStore, pins *profileunlock.PGProfilePins, local *localmedia.Service, parent *parentauth.Guard) (*ParentLocalFileHandler, error) {
	if store == nil || pins == nil || local == nil || parent == nil {
		return nil, kidsvod.ErrParentFileChanged
	}
	identity := &kidsVODIdentity{accounts: store.Accounts(), sessions: store.Sessions(), now: time.Now}
	core, err := kidsvod.New(kidsvod.Dependencies{Profiles: parentFileProfiles{store}, Policies: &kidsVODRegionalPolicy{current: store}, Sessions: identity})
	if err != nil {
		return nil, err
	}
	return &ParentLocalFileHandler{store: store, pins: pins, parent: parent, local: local, core: core}, nil
}
func (h *ParentLocalFileHandler) available(w http.ResponseWriter) bool {
	w.Header().Set("Cache-Control", "private, no-store")
	if h == nil || h.store == nil || h.pins == nil || h.parent == nil || h.local == nil || h.core == nil || !h.managedAvailable() {
		apierror.Write(w, 503, "kids_local_file_unavailable", "Parent-approved local files are unavailable.")
		return false
	}
	return true
}
func (h *ParentLocalFileHandler) current(r *http.Request, profile string) error {
	if r == nil || r.URL == nil || profile == "" || strings.TrimSpace(profile) != profile || h.pins.CheckOriginal(r) != nil {
		return kidsvod.ErrParentFileChanged
	}
	if len(r.Header.Values("X-Client-ID")) > 1 {
		return kidsvod.ErrParentFileChanged
	}
	for _, key := range []string{"profileId", "userId"} {
		for _, value := range r.URL.Query()[key] {
			if value != profile {
				return kidsvod.ErrParentFileChanged
			}
		}
	}
	if r.URL.Query().Has("profileName") {
		return kidsvod.ErrParentFileChanged
	}
	observation, err := h.pins.Current(r, profile)
	if err != nil || observation.Outcome() != profileunlock.PGPinNoPin && observation.Outcome() != profileunlock.PGPinUnlocked {
		return kidsvod.ErrParentFileChanged
	}
	kids, err := observation.IsKids()
	if err != nil || !kids || h.pins.CheckOriginal(r) != nil {
		return kidsvod.ErrParentFileChanged
	}
	return nil
}

// Approve requires an explicit initial challenge even when the first request
// supplies a password. No file read, probe, or player setup occurs at the prompt.
func (h *ParentLocalFileHandler) Approve(w http.ResponseWriter, r *http.Request) {
	if !h.available(w) {
		return
	}
	if r.Method != http.MethodPost {
		apierror.Write(w, 405, "method_not_allowed", "POST is required.")
		return
	}
	profile := mux.Vars(r)["userID"]
	if h.current(r, profile) != nil {
		h.deny(w, r)
		return
	}
	if r.URL.RawQuery != "" {
		apierror.Write(w, 400, "invalid_request", "Unexpected query.")
		return
	}
	body, err := readParentRatingBody(r)
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	item, err := datastore.ParentLocalFileItem(body)
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	values := r.Header.Values(profileunlock.ParentModeIntentHeader)
	if len(values) > 1 {
		writeParentRatingError(w, r, datastore.ErrParentRatingChanged)
		return
	}
	if len(values) == 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		prepared, err := h.core.PrepareParentFileRequest(r.WithContext(ctx), profile, item, h.local.OpenParentFile)
		if err != nil {
			h.deny(w, r)
			return
		}
		if h.current(r.WithContext(ctx), profile) != nil {
			prepared.Close()
			h.deny(w, r)
			return
		}
		token, err := h.pins.ParentLocalFileChallenge(r.WithContext(ctx), profile, body, prepared)
		if err != nil {
			writeParentRatingError(w, r, err)
			return
		}
		apierror.Write(w, 403, parentauth.RequiredCode, "Confirm the account password to approve this local file.", map[string]string{"parentIntent": token})
		h.flushParentOutput(w, r)
		return
	}
	intent, attachment, err := h.pins.ClaimParentLocalFile(r, profile, body, values[0])
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	defer intent.Close()
	defer attachment.Close()
	defer h.flushParentOnReturn(w, r)
	file, ok := attachment.(*kidsvod.ParentFileIntent)
	if !ok {
		h.deny(w, r)
		return
	}
	bound, err := h.pins.BoundParentRatingRequest(r, intent)
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	if !h.parent.RequireFresh(w, bound) {
		return
	}
	if err = h.pins.VerifyParentRating(bound, intent); err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	permit, err := h.pins.CommitParentLocalFile(intent)
	if err != nil {
		writeParentRatingError(w, r, err)
		return
	}
	if h.current(bound, profile) != nil {
		h.deny(w, r)
		return
	}
	id, expires, err := h.core.ApproveParentFile(bound, file, permit)
	if err != nil {
		h.deny(w, r)
		return
	}
	published := false
	defer func() {
		if !published {
			h.core.CancelParentFile(file)
		}
	}()
	if h.current(bound, profile) != nil || bound.Context().Err() != nil {
		h.deny(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err = h.writeParentSuccess(w, r, struct {
		ApprovalID string    `json:"approvalId"`
		ExpiresAt  time.Time `json:"expiresAt"`
		StreamURL  string    `json:"streamUrl"`
		Outcome    string    `json:"outcome"`
	}{id, expires, parentFileURL(profile, item, id), "parent-file-exception"})
	published = err == nil && bound.Context().Err() == nil
}

// Exact success framing completes on the wire before original source/worker
// cleanup may begin. Generic denials retain their existing bounded envelope.
func (h *ParentLocalFileHandler) writeParentSuccess(w http.ResponseWriter, r *http.Request, view any) error {
	if h.httpRuntime == nil {
		return json.NewEncoder(w).Encode(view)
	}
	raw, err := json.Marshal(view)
	if err != nil || len(raw) >= 4096 {
		clear(raw)
		apierror.Write(w, 503, "kids_local_file_unavailable", "Parent-approved local files are unavailable.")
		return kidsvod.ErrParentFileChanged
	}
	raw = append(raw, '\n')
	defer clear(raw)
	if r.Context().Err() != nil || !h.managedAvailable() {
		panic(http.ErrAbortHandler)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	n, err := w.Write(raw)
	if err != nil || n != len(raw) {
		panic(http.ErrAbortHandler)
	}
	h.flushParentOutput(w, r)
	return nil
}

func parentFileURL(profile, item, id string) string {
	return "/api/users/" + url.PathEscape(profile) + "/library/items/" + url.PathEscape(item) + "/approved-file?approvalId=" + url.QueryEscape(id)
}
func (h *ParentLocalFileHandler) deny(w http.ResponseWriter, r *http.Request) {
	if r.Context().Err() == nil {
		apierror.Write(w, 403, kidsvod.CodeAccessDenied, "Parent-approved local file access denied.")
	}
}
func parentFileSelectors(r *http.Request) (string, string, string, bool) {
	profile, item := mux.Vars(r)["userID"], mux.Vars(r)["itemID"]
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || len(query) != 1 || len(query["approvalId"]) != 1 || query.Get("approvalId") == "" {
		return "", "", "", false
	}
	return profile, item, query.Get("approvalId"), profile != "" && item != ""
}
func (h *ParentLocalFileHandler) Revoke(w http.ResponseWriter, r *http.Request) {
	if !h.available(w) {
		return
	}
	if r.Method != http.MethodDelete {
		apierror.Write(w, 405, "method_not_allowed", "DELETE is required.")
		return
	}
	profile, item, id, ok := parentFileSelectors(r)
	if !ok || h.current(r, profile) != nil || h.core.RevokeParentFileRequest(r, profile, item, id) != nil {
		h.deny(w, r)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parentFileRange(raw string, size int64) (int64, int64, error) {
	if raw == "" {
		return 0, size, nil
	}
	if !strings.HasPrefix(raw, "bytes=") || strings.Contains(raw, ",") {
		return 0, 0, kidsvod.ErrParentFileChanged
	}
	parts := strings.Split(strings.TrimPrefix(raw, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, kidsvod.ErrParentFileChanged
	}
	parse := func(s string) (int64, error) {
		if s == "" {
			return 0, errors.New("empty")
		}
		for _, r := range s {
			if r < '0' || r > '9' {
				return 0, errors.New("invalid")
			}
		}
		return strconv.ParseInt(s, 10, 64)
	}
	if parts[0] == "" {
		n, err := parse(parts[1])
		if err != nil || n <= 0 {
			return 0, 0, kidsvod.ErrParentFileChanged
		}
		if n > size {
			n = size
		}
		return size - n, n, nil
	}
	start, err := parse(parts[0])
	if err != nil || start >= size {
		return 0, 0, kidsvod.ErrParentFileChanged
	}
	end := size - 1
	if parts[1] != "" {
		end, err = parse(parts[1])
		if err != nil || end < start {
			return 0, 0, kidsvod.ErrParentFileChanged
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end - start + 1, nil
}

// Stream publishes only checked ReadAt chunks. Each request owns a duplicate
// actual FD and independent position. Original expiry is installed before the
// cancellation callback, whose retirement is joined before returning.
func (h *ParentLocalFileHandler) Stream(w http.ResponseWriter, r *http.Request) {
	if !h.available(w) {
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		apierror.Write(w, 405, "method_not_allowed", "GET or HEAD is required.")
		return
	}
	profile, item, id, ok := parentFileSelectors(r)
	if !ok || h.current(r, profile) != nil {
		h.deny(w, r)
		return
	}
	if len(r.Header.Values("Range")) > 1 || r.Header.Get("If-Range") != "" {
		apierror.Write(w, 400, "invalid_request", "Unsupported range.")
		return
	}
	reader, err := h.core.OpenParentFileRequest(r, profile, item, id)
	if err != nil {
		h.deny(w, r)
		return
	}
	defer reader.Close()
	controller := http.NewResponseController(w)
	deadline := reader.ExpiresAt()
	if original, ok := r.Context().Deadline(); ok && original.Before(deadline) {
		deadline = original
	}
	// Unsupported ResponseController deadlines are denied rather than silently
	// permitting a blocked write beyond revocation. Test recorders provide one.
	if controller.SetWriteDeadline(deadline) != nil {
		apierror.Write(w, 503, "kids_local_file_unavailable", "Cancelable local file streaming is unavailable.")
		return
	}
	done := make(chan struct{})
	stop := context.AfterFunc(reader.Context(), func() { defer close(done); _ = controller.SetWriteDeadline(time.Now()); _ = reader.Close() })
	defer func() {
		if !stop() {
			<-done
		}
	}()
	check := func(ctx context.Context) bool {
		return h.current(r.WithContext(ctx), profile) == nil && reader.Check(ctx) == nil && ctx.Err() == nil && reader.Context().Err() == nil
	}
	// A bounded joined watcher cancels blocked writes/reads when current PG PIN,
	// policy, session or source/ACL observations stop agreeing with the original.
	watchCtx, stopWatch := context.WithCancel(reader.Context())
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				if !check(watchCtx) {
					if watchCtx.Err() == nil {
						reader.CancelAccess()
					}
					return
				}
			}
		}
	}()
	defer func() { stopWatch(); <-watchDone }()
	defer h.flushParentOnReturn(w, r)
	if !check(r.Context()) {
		h.deny(w, r)
		return
	}
	offset, length, err := parentFileRange(r.Header.Get("Range"), reader.Size())
	if err != nil {
		if !check(r.Context()) {
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", reader.Size()))
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	status := http.StatusOK
	if r.Header.Get("Range") != "" {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, reader.Size()))
	}
	if !check(r.Context()) {
		w.Header().Del("Content-Length")
		w.Header().Del("Content-Range")
		w.Header().Del("Accept-Ranges")
		w.Header().Del("Content-Type")
		h.deny(w, r)
		return
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	for length > 0 {
		amount := int64(len(buffer))
		if length < amount {
			amount = length
		}
		n, readErr := reader.ReadAt(buffer[:amount], offset)
		if readErr != nil && readErr != io.EOF || n == 0 || !check(r.Context()) {
			clear(buffer)
			return
		}
		written, writeErr := w.Write(buffer[:n])
		clear(buffer)
		if writeErr != nil || written != n || !check(r.Context()) {
			return
		}
		offset += int64(n)
		length -= int64(n)
	}
}

// childPlayback is before GetItem/Probe/remote fallback/directURL. A configured
// child branch returns only the exact approved raw route, never generic video.
func (h *ParentLocalFileHandler) childPlayback(w http.ResponseWriter, r *http.Request, item string) bool {
	if !h.available(w) {
		return true
	}
	profile := r.URL.Query().Get("profileId")
	if len(r.URL.Query()["profileId"]) != 1 || profile == "" || r.URL.Query().Has("profileName") {
		h.deny(w, r)
		return true
	}
	snapshot, err := h.store.CurrentRegionalProfile(r.Context(), profile)
	if err != nil || snapshot.AccountID != auth.GetAccountID(r) && !auth.IsMaster(r) {
		h.deny(w, r)
		return true
	}
	if !snapshot.IsKids {
		return false
	}
	if h.current(r, profile) != nil {
		h.deny(w, r)
		return true
	}
	query := r.URL.Query()
	for key := range query {
		if key != "profileId" && key != "approvalId" {
			h.deny(w, r)
			return true
		}
	}
	id := query.Get("approvalId")
	if id == "" {
		apierror.Write(w, 403, kidsvod.CodeParentApprovalRequired, "This local file requires fresh parent approval.")
		return true
	}
	if len(query["approvalId"]) != 1 {
		h.deny(w, r)
		return true
	}
	reader, err := h.core.OpenParentFileRequest(r, profile, item, id)
	if err != nil {
		h.deny(w, r)
		return true
	}
	defer reader.Close()
	if h.current(r, profile) != nil || reader.Check(r.Context()) != nil {
		h.deny(w, r)
		return true
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(models.LocalMediaPlaybackResponse{ItemID: item, FileName: reader.Name(), StreamURL: parentFileURL(profile, item, id), DirectStream: true, HLSAvailable: false, SourceType: "local", SourceName: "Local"})
	return true
}
