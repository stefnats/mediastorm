package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"novastream/internal/datastore"
	"novastream/services/localmedia"
)

func (h *SABLibraryPublicationHandler) Preview(w http.ResponseWriter, r *http.Request) {
	entry, err := h.entry(r)
	if err != nil {
		publicationHTTPError(w, err)
		return
	}
	id, err := publicationQueueID(r)
	if err != nil || id <= 0 || r.Method != http.MethodPost || r.URL.RawQuery != "" {
		http.Error(w, "400 Bad Request", http.StatusBadRequest)
		return
	}
	selectors, err := io.ReadAll(r.Body)
	defer clear(selectors)
	if err != nil || len(selectors) > 16384 || entry.ctx.Err() != nil || !entry.bodyExpiry.After(time.Now()) || datastore.ValidateSABLibraryPublicationSelection(selectors) != nil {
		http.Error(w, "400 Bad Request", http.StatusBadRequest)
		return
	}
	entry.ioMu.Lock()
	if entry.ctx.Err() == nil {
		entry.bodyDone = true
		err = entry.controller.SetReadDeadline(entry.expiry)
	} else {
		err = entry.ctx.Err()
	}
	entry.ioMu.Unlock()
	if err != nil || entry.ctx.Err() != nil {
		publicationHTTPError(w, datastore.ErrSABLibraryPublicationChanged)
		return
	}
	preview, err := h.publisher.Preview(r, id, selectors)
	if err != nil {
		publicationHTTPError(w, err)
		return
	}
	defer preview.Close()
	h.outputPreview(w, r, entry, preview)
}

func (h *SABLibraryPublicationHandler) outputPreview(w http.ResponseWriter, r *http.Request, entry *sabPublicationHTTPEntry, preview *localmedia.SABLibraryPublicationPreview) {
	wire, err := json.Marshal(preview.View())
	if err != nil || len(wire) > 256<<10 {
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	defer clear(wire)
	watch, cancel := context.WithCancel(entry.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-watch.Done():
				return
			case <-tick.C:
				if preview.Check(r) != nil {
					entry.cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	deadline := minTime(entry.expiry, preview.ExpiresAt())
	entry.ioMu.Lock()
	if entry.ctx.Err() == nil && deadline.After(time.Now()) {
		err = entry.controller.SetWriteDeadline(deadline)
	} else {
		err = datastore.ErrSABLibraryPublicationChanged
	}
	entry.ioMu.Unlock()
	if err != nil || preview.Check(r) != nil || entry.ctx.Err() != nil || !deadline.After(time.Now()) {
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if preview.Check(r) != nil || entry.ctx.Err() != nil || !deadline.After(time.Now()) {
		http.Error(w, "503 Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(wire); err != nil || n != len(wire) {
		return
	}
	if preview.Check(r) != nil || entry.ctx.Err() != nil || !deadline.After(time.Now()) {
		return
	}
	_ = entry.controller.Flush()
}
