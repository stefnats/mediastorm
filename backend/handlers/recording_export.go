package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"novastream/services/recordings"
)

type recordingPartsExporter interface {
	OpenPartsExport(context.Context, string, func() error) (*recordings.PartsExport, error)
}

// ExportParts downloads independent closed attempt files. This API does not
// concatenate parts, fill gaps or declare a partial recording complete.
func (h *RecordingsHandler) ExportParts(w http.ResponseWriter, r *http.Request) {
	exporter, ok := h.service.(recordingPartsExporter)
	if !ok {
		http.Error(w, "recording export unavailable", http.StatusServiceUnavailable)
		return
	}
	id := mux.Vars(r)["recordingID"]
	path := "recording:" + id
	access := h.recordingAccess
	before, ok := access.current(r, path)
	if !ok {
		_, _ = access.Authorize(w, r, path)
		return
	}
	policy, err := access.policy()
	if err != nil {
		http.Error(w, "recording export unavailable", http.StatusNotFound)
		return
	}
	checked, ok := access.Authorize(w, r, path)
	if !ok {
		return
	}
	after, ok := access.current(checked, path)
	latestPolicy, err := access.policy()
	if !ok || err != nil || before.fingerprint != after.fingerprint || latestPolicy.EnforceProfilePins != policy.EnforceProfilePins {
		http.Error(w, "recording export unavailable", http.StatusNotFound)
		return
	}
	if r.Header.Get("Range") != "" {
		http.Error(w, "recording exports do not support byte ranges", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	// Even a current master with no selector remains bound to this exact stored
	// owner. Never recapture a changed owner or extend the initial authority.
	cloned := *checked.URL
	query := cloned.Query()
	query.Set("profileId", before.owner.ProfileID)
	cloned.RawQuery = query.Encode()
	checked = checked.Clone(checked.Context())
	checked.URL = &cloned
	expiry := before.session.ExpiresAt
	for _, accountID := range []string{before.session.AccountID, before.profile.AccountID} {
		account, exists := access.accounts.Get(accountID)
		if !exists {
			http.Error(w, "recording export unavailable", http.StatusNotFound)
			return
		}
		if account.ExpiresAt != nil && account.ExpiresAt.Before(expiry) {
			expiry = *account.ExpiresAt
		}
	}
	if deadline, exists := checked.Context().Deadline(); exists && deadline.Before(expiry) {
		expiry = deadline
	}
	ctx, cancel := context.WithDeadline(checked.Context(), expiry)
	defer cancel()
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
		http.Error(w, "recording export unavailable", http.StatusForbidden)
		return
	}
	archive, err := exporter.OpenPartsExport(ctx, id, authorize)
	if err != nil {
		writeRecordingExportError(w, err)
		return
	}
	defer archive.Close()
	if err = authorize(); err != nil {
		writeRecordingExportError(w, err)
		return
	}
	// Bound a slow HTTP writer by the same original session/account deadline.
	// Unsupported in-memory response writers still have all chunk checks.
	_ = http.NewResponseController(w).SetWriteDeadline(expiry)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+archive.Filename()+`"`)
	w.Header().Set("Cache-Control", "private, no-store")
	output := &exportResponseWriter{ResponseWriter: w}
	if err = archive.WriteZIP(output); err != nil {
		if output.started {
			panic(http.ErrAbortHandler)
		}
		w.Header().Del("Content-Disposition")
		w.Header().Del("Content-Type")
		writeRecordingExportError(w, err)
	}
}

type exportAuthorizationSink struct{ header http.Header }

func (s *exportAuthorizationSink) Header() http.Header       { return s.header }
func (*exportAuthorizationSink) WriteHeader(int)             {}
func (*exportAuthorizationSink) Write(p []byte) (int, error) { return len(p), nil }

type exportResponseWriter struct {
	http.ResponseWriter
	started bool
}

func (w *exportResponseWriter) Write(p []byte) (int, error) {
	w.started = true
	return w.ResponseWriter.Write(p)
}
func writeRecordingExportError(w http.ResponseWriter, err error) {
	status := http.StatusConflict
	if errors.Is(err, recordings.ErrRecordingExportBusy) {
		status = http.StatusTooManyRequests
	}
	if errors.Is(err, recordings.ErrRecordingExportAccess) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusForbidden
	}
	http.Error(w, "recording parts export unavailable", status)
}

// Compile-time interface checks keep the exporter and streaming writer narrow.
var _ io.Writer = (*exportResponseWriter)(nil)
