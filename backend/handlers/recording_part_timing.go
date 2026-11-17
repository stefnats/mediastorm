package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/recordings"
)

type recordingPartTimer interface {
	GetPartTiming(context.Context, string, int, func() error) (recordings.PartTiming, error)
	RefreshPartTiming(context.Context, string, int, func() error) (recordings.PartTiming, error)
}

func (h *RecordingsHandler) GetPartTiming(w http.ResponseWriter, r *http.Request) {
	h.partTiming(w, r, false)
}
func (h *RecordingsHandler) RefreshPartTiming(w http.ResponseWriter, r *http.Request) {
	h.partTiming(w, r, true)
}
func (h *RecordingsHandler) partTiming(w http.ResponseWriter, r *http.Request, refresh bool) {
	ctx, cancel, authorize, ok := h.partTimingAuthority(w, r)
	if !ok {
		return
	}
	defer cancel()
	raw := mux.Vars(r)["partIndex"]
	index, err := strconv.Atoi(raw)
	if err != nil || strconv.Itoa(index) != raw || index < 1 || index > models.MaxRecordingParts {
		http.Error(w, "invalid recording part", http.StatusBadRequest)
		return
	}
	timer, ok := h.service.(recordingPartTimer)
	if !ok {
		http.Error(w, "recording timing unavailable", http.StatusServiceUnavailable)
		return
	}
	// POST is an explicit empty-body operation. No caller path or probe arguments.
	if refresh && (r.ContentLength != 0 || len(r.TransferEncoding) != 0) {
		http.Error(w, "request body is unsupported", http.StatusBadRequest)
		return
	}
	id := mux.Vars(r)["recordingID"]
	var value recordings.PartTiming
	if refresh {
		value, err = timer.RefreshPartTiming(ctx, id, index, authorize)
	} else {
		value, err = timer.GetPartTiming(ctx, id, index, authorize)
	}
	if err != nil {
		status := http.StatusConflict
		if errors.Is(err, recordings.ErrPartTimingBusy) {
			status = http.StatusTooManyRequests
		}
		if errors.Is(err, recordings.ErrRecordingExportAccess) || ctx.Err() != nil {
			status = http.StatusForbidden
		}
		http.Error(w, "recording timing unavailable", status)
		return
	}
	rawJSON, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "recording timing unavailable", http.StatusServiceUnavailable)
		return
	}
	if err = authorize(); err != nil {
		http.Error(w, "recording timing unavailable", http.StatusForbidden)
		return
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_, _ = w.Write(rawJSON)
}

// This private capture deliberately keeps the same original authority as the
// export endpoint, without recapturing a different owner during a probe.
func (h *RecordingsHandler) partTimingAuthority(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc, func() error, bool) {
	id := mux.Vars(r)["recordingID"]
	path := "recording:" + id
	access := h.recordingAccess
	before, ok := access.current(r, path)
	if !ok {
		_, _ = access.Authorize(w, r, path)
		return nil, nil, nil, false
	}
	policy, err := access.policy()
	if err != nil {
		http.Error(w, "recording export unavailable", http.StatusNotFound)
		return nil, nil, nil, false
	}
	checked, ok := access.Authorize(w, r, path)
	if !ok {
		return nil, nil, nil, false
	}
	after, ok := access.current(checked, path)
	latestPolicy, err := access.policy()
	if !ok || err != nil || before.fingerprint != after.fingerprint || latestPolicy.EnforceProfilePins != policy.EnforceProfilePins {
		http.Error(w, "recording export unavailable", http.StatusNotFound)
		return nil, nil, nil, false
	}
	if r.Header.Get("Range") != "" {
		http.Error(w, "recording exports do not support byte ranges", http.StatusRequestedRangeNotSatisfiable)
		return nil, nil, nil, false
	}
	// Even a current master with no selector remains bound to this exact stored
	// owner. Never recapture a changed owner or extend the initial authority.
	cloned := *checked.URL
	query := cloned.Query()
	query.Set("profileId", before.owner.ProfileID)
	cloned.RawQuery = query.Encode()
	checked = checked.Clone(checked.Context())
	checked.URL = &cloned
	expiry, exists := access.originalExpiry(before)
	if !exists {
		http.Error(w, "recording export unavailable", http.StatusNotFound)
		return nil, nil, nil, false
	}
	if deadline, exists := checked.Context().Deadline(); exists && deadline.Before(expiry) {
		expiry = deadline
	}
	ctx, cancel := context.WithDeadline(checked.Context(), expiry)
	checked = checked.WithContext(ctx)
	authorize := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		sink := &exportAuthorizationSink{header: make(http.Header)}
		if _, allowed := access.Authorize(sink, checked, path); !allowed {
			return recordings.ErrRecordingExportAccess
		}
		current, valid := access.current(checked, path)
		p, err := access.policy()
		if !valid || err != nil || current.fingerprint != before.fingerprint || p.EnforceProfilePins != policy.EnforceProfilePins {
			return recordings.ErrRecordingExportAccess
		}
		return ctx.Err()
	}
	if err = authorize(); err != nil {
		cancel()
		http.Error(w, "recording export unavailable", http.StatusForbidden)
		return nil, nil, nil, false
	}
	return ctx, cancel, authorize, true
}
