package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/services/recordings"
)

type recordingDerivedService interface {
	PrepareDerived(context.Context, string, func() error) (recordings.DerivedRecording, error)
	GetDerived(context.Context, string, func() error) (recordings.DerivedRecording, error)
	OpenDerived(context.Context, string, string, func() error) (*recordings.DerivedReader, error)
}

type recordingDerivedReader interface {
	io.ReadSeekCloser
	Descriptor() recordings.DerivedRecording
}

func (h *RecordingsHandler) PrepareDerived(w http.ResponseWriter, r *http.Request) {
	h.derivedDescriptor(w, r, true)
}

func (h *RecordingsHandler) GetDerived(w http.ResponseWriter, r *http.Request) {
	h.derivedDescriptor(w, r, false)
}

func (h *RecordingsHandler) derivedDescriptor(w http.ResponseWriter, r *http.Request, prepare bool) {
	ctx, cancel, authorize, ok := h.recordingDerivedAuthority(w, r, false)
	if !ok {
		return
	}
	defer cancel()
	// Both bearer operations and the authenticated cookie adapter supply no body.
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		http.Error(w, "request body is unsupported", http.StatusBadRequest)
		return
	}
	service, ok := h.service.(recordingDerivedService)
	if !ok {
		http.Error(w, "joined recording unavailable", http.StatusServiceUnavailable)
		return
	}
	id := mux.Vars(r)["recordingID"]
	var value recordings.DerivedRecording
	var err error
	if prepare {
		value, err = service.PrepareDerived(ctx, id, authorize)
	} else {
		value, err = service.GetDerived(ctx, id, authorize)
	}
	if err != nil {
		writeRecordingDerivedError(w, err)
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "joined recording unavailable", http.StatusServiceUnavailable)
		return
	}
	if err = authorize(); err != nil {
		writeRecordingDerivedError(w, err)
		return
	}
	if deadline, exists := ctx.Deadline(); exists {
		_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(raw)
}

// DownloadDerived retains the exact stream authority and only changes successful web delivery.
func (h *RecordingsHandler) DownloadDerived(w http.ResponseWriter, r *http.Request) {
	h.StreamDerived(&recordingDerivedDownloadWriter{ResponseWriter: w}, r)
}

type recordingDerivedDownloadWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

// ResponseController must reach the original deadline/abort-capable transport.
func (w *recordingDerivedDownloadWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *recordingDerivedDownloadWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	if status == http.StatusOK || status == http.StatusPartialContent {
		w.Header().Set("Content-Disposition", `attachment; filename="recording-joined.mkv"`)
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *recordingDerivedDownloadWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(data)
}

// A derived ID identifies a registered output; it never supplies playback authority.
func (h *RecordingsHandler) StreamDerived(w http.ResponseWriter, r *http.Request) {
	ctx, cancel, authorize, ok := h.recordingDerivedAuthority(w, r, true)
	if !ok {
		return
	}
	defer cancel()
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || len(r.Header.Values("Range")) > 1 {
		http.Error(w, "invalid joined recording request", http.StatusBadRequest)
		return
	}
	service, ok := h.service.(recordingDerivedService)
	if !ok {
		http.Error(w, "joined recording unavailable", http.StatusServiceUnavailable)
		return
	}
	vars := mux.Vars(r)
	reader, err := service.OpenDerived(ctx, vars["recordingID"], vars["derivedID"], authorize)
	if err != nil {
		writeRecordingDerivedError(w, err)
		return
	}
	defer reader.Close()
	serveRecordingDerived(w, r, reader, ctx, authorize)
}

func serveRecordingDerived(w http.ResponseWriter, r *http.Request, reader recordingDerivedReader, ctx context.Context, authorize func() error) {
	// Set the outer deadline first. A source cancellation may only shorten it;
	// no later expiry assignment can revive a canceled blocked writer.
	if deadline, exists := ctx.Deadline(); exists {
		_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	}
	// A source/restore watcher can cancel the checked reader while the original
	// account request remains valid. Interrupt a blocked write and join this callback
	// before returning, so a retired stream cannot affect a subsequent response.
	if lifetime, ok := reader.(interface{ Context() context.Context }); ok {
		inner := lifetime.Context()
		if inner == nil {
			writeRecordingDerivedError(w, recordings.ErrDerivedUnavailable)
			return
		}
		originalCheck := authorize
		authorize = func() error {
			if err := inner.Err(); err != nil {
				return err
			}
			if err := originalCheck(); err != nil {
				return err
			}
			return inner.Err()
		}
		done := make(chan struct{})
		stop := context.AfterFunc(inner, func() {
			defer close(done)
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now())
		})
		defer func() {
			if !stop() {
				<-done
			}
		}()
	}
	value := reader.Descriptor()
	if value.Bytes <= 0 || value.State != "ready" {
		writeRecordingDerivedError(w, recordings.ErrDerivedUnavailable)
		return
	}
	rawRange := r.Header.Get("Range")
	// No validator is advertised. An If-Range request therefore gets the full representation.
	if r.Header.Get("If-Range") != "" {
		rawRange = ""
	}
	start, end, ranged, err := recordingDerivedRange(rawRange, value.Bytes)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", value.Bytes))
		http.Error(w, "unsupported recording range", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	position, err := reader.Seek(start, io.SeekStart)
	if err != nil || position != start {
		if err == nil {
			err = recordings.ErrDerivedUnavailable
		}
		writeRecordingDerivedError(w, err)
		return
	}
	if err = authorize(); err != nil {
		writeRecordingDerivedError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Content-Type", "video/x-matroska")
	w.Header().Set("Content-Disposition", `inline; filename="recording-joined.mkv"`)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	status := http.StatusOK
	if ranged {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, value.Bytes))
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	// The service reader rechecks the complete source/output bindings around each
	// read. Check original HTTP authority again immediately before/after each write.
	buffer := make([]byte, 32<<10)
	remaining := end - start + 1
	for remaining > 0 {
		if err = authorize(); err != nil {
			panic(http.ErrAbortHandler)
		}
		want := min(remaining, int64(len(buffer)))
		n, readErr := reader.Read(buffer[:int(want)])
		if n < 0 || int64(n) > want || n == 0 || readErr != nil && readErr != io.EOF || authorize() != nil {
			clear(buffer)
			panic(http.ErrAbortHandler)
		}
		written, writeErr := w.Write(buffer[:n])
		clear(buffer[:n])
		if writeErr != nil || written != n || authorize() != nil {
			panic(http.ErrAbortHandler)
		}
		remaining -= int64(n)
		if readErr == io.EOF && remaining != 0 {
			panic(http.ErrAbortHandler)
		}
	}
}

func recordingDerivedRange(value string, size int64) (start, end int64, ranged bool, err error) {
	bad := func() (int64, int64, bool, error) { return 0, 0, false, errors.New("invalid recording range") }
	if size <= 0 {
		return bad()
	}
	if value == "" {
		return 0, size - 1, false, nil
	}
	if len(value) > 128 || !strings.HasPrefix(value, "bytes=") || strings.ContainsAny(value, ", \t\r\n") {
		return bad()
	}
	parts := strings.Split(strings.TrimPrefix(value, "bytes="), "-")
	if len(parts) != 2 {
		return bad()
	}
	parse := func(s string) (int64, bool) {
		n, e := strconv.ParseInt(s, 10, 64)
		return n, e == nil && n >= 0 && strconv.FormatInt(n, 10) == s
	}
	if parts[0] == "" {
		n, ok := parse(parts[1])
		if !ok || n == 0 {
			return bad()
		}
		return max(0, size-n), size - 1, true, nil
	}
	var ok bool
	start, ok = parse(parts[0])
	if !ok || start >= size {
		return bad()
	}
	end = size - 1
	if parts[1] != "" {
		var valid bool
		end, valid = parse(parts[1])
		if !valid || end < start {
			return bad()
		}
		end = min(end, size-1)
	}
	return start, end, true, nil
}

func writeRecordingDerivedError(w http.ResponseWriter, err error) {
	status := http.StatusConflict
	switch {
	case errors.Is(err, recordings.ErrDerivedAccess), errors.Is(err, recordings.ErrRecordingExportAccess), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		status = http.StatusForbidden
	case errors.Is(err, recordings.ErrDerivedBusy):
		status = http.StatusTooManyRequests
	case errors.Is(err, recordings.ErrDerivedTooLarge):
		status = http.StatusRequestEntityTooLarge
	}
	http.Error(w, "joined recording unavailable", status)
}
