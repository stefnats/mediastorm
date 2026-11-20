package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

var recordingTimelineSlots = make(chan struct{}, 16)

// RecordingCurrentTimelineHandler is deliberately unregistered. It preserves
// existing DVR policy; ordinary FamilyMedia or parent exceptions are not used.
type RecordingCurrentTimelineHandler struct {
	store     *datastore.DataStore
	service   *recordings.Service
	guard     *RecordingsHandler
	mu        sync.Mutex
	closed    bool
	active    map[*recordingTimelineOperation]struct{}
	closeDone chan struct{}
}
type recordingTimelineOperation struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func NewRecordingCurrentTimelineHandler(store *datastore.DataStore, service *recordings.Service, pins *profileunlock.PGProfilePins, policy func() (config.SecuritySettings, error)) (*RecordingCurrentTimelineHandler, error) {
	if store == nil || service == nil || pins == nil || policy == nil || !pins.FamilyMediaReady(store) || !service.DerivedTimelineReady(store) {
		return nil, errors.New("recording timeline storage unavailable")
	}
	access := NewRecordingStreamAccess(service, nil, nil, nil, nil, policy)
	access.SetPGProfilePins(pins)
	return &RecordingCurrentTimelineHandler{store: store, service: service, guard: &RecordingsHandler{service: service, recordingAccess: access}, active: make(map[*recordingTimelineOperation]struct{}), closeDone: make(chan struct{})}, nil
}

func (h *RecordingCurrentTimelineHandler) admit(ctx context.Context) *recordingTimelineOperation {
	if h == nil || h.store == nil || h.service == nil || h.guard == nil || h.active == nil || h.closeDone == nil || ctx == nil || ctx.Err() != nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || len(h.active) >= 2 {
		return nil
	}
	select {
	case recordingTimelineSlots <- struct{}{}:
	default:
		return nil
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	op := &recordingTimelineOperation{ctx: bounded, cancel: cancel, done: make(chan struct{})}
	h.active[op] = struct{}{}
	return op
}
func (h *RecordingCurrentTimelineHandler) finish(op *recordingTimelineOperation) {
	op.cancel()
	h.mu.Lock()
	delete(h.active, op)
	<-recordingTimelineSlots
	close(op.done)
	h.mu.Unlock()
}

// Close signals first, then waits outside the registry lock. A cancellation
// callback never calls Close or returns an admission charge on its own.
func (h *RecordingCurrentTimelineHandler) Close() {
	if h == nil || h.closeDone == nil {
		return
	}
	h.mu.Lock()
	if h.closed {
		done := h.closeDone
		h.mu.Unlock()
		<-done
		return
	}
	h.closed = true
	ops := make([]*recordingTimelineOperation, 0, len(h.active))
	for op := range h.active {
		ops = append(ops, op)
	}
	h.mu.Unlock()
	for _, op := range ops {
		op.cancel()
	}
	for _, op := range ops {
		<-op.done
	}
	close(h.closeDone)
}

func (h *RecordingCurrentTimelineHandler) Get(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.Header().Set("Allow", "GET, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := mux.Vars(r)["recordingID"]
	if id == "" || len(id) > 128 || strings.TrimSpace(id) != id || strings.ContainsAny(id, "/\\\x00") || r.URL == nil || r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || r.Body != nil && r.Body != http.NoBody || len(r.TransferEncoding) != 0 {
		http.Error(w, "invalid recording timeline request", http.StatusBadRequest)
		return
	}
	for _, header := range []string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		if len(r.Header.Values(header)) != 0 {
			http.Error(w, "recording timeline conditions unsupported", http.StatusBadRequest)
			return
		}
	}
	caller := r.Context()
	op := h.admit(caller)
	if op == nil {
		http.Error(w, "recording timeline busy", http.StatusTooManyRequests)
		return
	}
	defer h.finish(op)
	r = r.WithContext(op.ctx)
	path := "recording:" + id
	before, ok := h.guard.recordingAccess.current(r, path)
	if !ok {
		_, _ = h.guard.recordingAccess.Authorize(w, r, path)
		return
	}
	ctx, cancel, authorize, ok := h.guard.recordingDerivedAuthority(w, r, false)
	if !ok {
		return
	}
	defer cancel()
	after, ok := h.guard.recordingAccess.current(r.WithContext(ctx), path)
	if !ok || before.fingerprint != after.fingerprint {
		http.Error(w, "recording timeline changed", http.StatusForbidden)
		return
	}
	first, err := h.store.ReadRecordingTimelineIdentity(ctx, id)
	if err != nil {
		http.Error(w, "recording timeline identity unavailable", http.StatusServiceUnavailable)
		return
	}
	if first.OwnerProfileID() != before.owner.ProfileID || !first.CreatedAt().Equal(before.owner.CreatedAt) {
		http.Error(w, "recording timeline changed", http.StatusConflict)
		return
	}
	descriptor, err := h.service.GetDerived(ctx, id, authorize)
	if err != nil {
		writeRecordingDerivedError(w, err)
		return
	}
	if err = authorize(); err != nil {
		writeRecordingDerivedError(w, err)
		return
	}
	latest, err := h.store.ReadRecordingTimelineIdentity(ctx, id)
	if err != nil {
		http.Error(w, "recording timeline identity unavailable", http.StatusServiceUnavailable)
		return
	}
	if !first.Same(latest) {
		http.Error(w, "recording timeline changed", http.StatusConflict)
		return
	}
	value, err := recordings.ProjectDerivedTimelineData(first.Incarnation(), descriptor, time.Now())
	if err != nil {
		http.Error(w, "recording timeline unavailable", http.StatusConflict)
		return
	}
	writeRecordingCurrentTimeline(w, ctx, caller, op.ctx, value, authorize)
}

// This publication helper receives only the enclosing handler's original check.
// It does not make a caller-created data DTO authoritative.
func writeRecordingCurrentTimeline(w http.ResponseWriter, ctx, caller, work context.Context, value recordings.DerivedTimelineData, authorize func() error) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 64*1024 {
		http.Error(w, "recording timeline unavailable", http.StatusServiceUnavailable)
		return
	}
	defer clear(raw)
	if err = authorize(); err != nil || ctx.Err() != nil || work.Err() != nil || caller.Err() != nil || value.ExpiresAt != nil && !time.Now().Before(*value.ExpiresAt) {
		http.Error(w, "recording timeline access unavailable", http.StatusForbidden)
		return
	}
	controller := http.NewResponseController(w)
	deadline, exists := ctx.Deadline()
	if value.ExpiresAt != nil && value.ExpiresAt.Before(deadline) {
		deadline = *value.ExpiresAt
	}
	if !exists || controller.SetWriteDeadline(deadline) != nil {
		http.Error(w, "recording timeline transport unavailable", http.StatusServiceUnavailable)
		return
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = controller.SetWriteDeadline(time.Now()); close(callbackDone) })
	defer func() {
		if !stop() {
			<-callbackDone
		}
	}()
	if err = authorize(); err != nil || ctx.Err() != nil || work.Err() != nil || caller.Err() != nil || value.ExpiresAt != nil && !time.Now().Before(*value.ExpiresAt) {
		http.Error(w, "recording timeline access unavailable", http.StatusForbidden)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(raw)
}
