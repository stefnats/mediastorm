package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/importer"
)

func (h *UsenetFallbackHandler) SetARRLibrary(library importer.ARRLibraryImporter) {
	h.arrLibrary = library
}
func (h *UsenetFallbackHandler) ARRStatus(w http.ResponseWriter, r *http.Request) {
	h.arrImport(w, r, "status")
}
func (h *UsenetFallbackHandler) ARRSubmit(w http.ResponseWriter, r *http.Request) {
	h.arrImport(w, r, "submit")
}
func (h *UsenetFallbackHandler) ARRRefresh(w http.ResponseWriter, r *http.Request) {
	h.arrImport(w, r, "refresh")
}
func (h *UsenetFallbackHandler) arrImport(w http.ResponseWriter, r *http.Request, action string) {
	original, ok := h.current(r)
	if !ok {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	raw := mux.Vars(r)["queueID"]
	id, e := strconv.ParseInt(raw, 10, 64)
	if e != nil || id <= 0 || strconv.FormatInt(id, 10) != raw || r.URL.RawQuery != "" {
		fallbackHTTPError(w, 400, "invalid_fallback_request")
		return
	}
	var input struct {
		Backend string `json:"backend"`
		Retry   bool   `json:"retry"`
	}
	if action == "submit" {
		decoder := json.NewDecoder(io.LimitReader(r.Body, 1025))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || (input.Backend != "sonarr" && input.Backend != "radarr") {
			fallbackHTTPError(w, 400, "invalid_import_request")
			return
		}
	} else if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody {
		fallbackHTTPError(w, 400, "invalid_import_request")
		return
	}
	service, ok := h.service.(interface {
		GetARRImportStatus(context.Context, int64) (importer.ARRImportStatus, error)
		SubmitARRImport(context.Context, int64, string, bool, importer.SABImportPreviewSettings, importer.ARRLibraryImporter, func(context.Context) error) (importer.ARRImportStatus, error)
		RefreshARRImport(context.Context, int64, importer.SABImportPreviewSettings, importer.ARRLibraryImporter, func(context.Context) error) (importer.ARRImportStatus, error)
	})
	if !ok || h.previewSettings == nil {
		fallbackHTTPError(w, 503, "arr_import_unavailable")
		return
	}
	deadline := time.Now().Add(5 * time.Minute)
	if action == "status" {
		deadline = time.Now().Add(5 * time.Second)
	}
	for _, at := range []time.Time{original.session.ExpiresAt, original.claimExpiry} {
		if at.Before(deadline) {
			deadline = at
		}
	}
	if original.accountExpiry != nil && original.accountExpiry.Before(deadline) {
		deadline = *original.accountExpiry
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	r = r.WithContext(ctx)
	check := func(work context.Context) error {
		current, valid := h.current(r.WithContext(work))
		if !valid || !reflect.DeepEqual(original, current) {
			return importer.ErrARRImportUnavailable
		}
		return work.Err()
	}
	var result importer.ARRImportStatus
	switch action {
	case "submit":
		result, e = service.SubmitARRImport(ctx, id, input.Backend, input.Retry, h.previewSettings, h.arrLibrary, check)
	case "refresh":
		result, e = service.RefreshARRImport(ctx, id, h.previewSettings, h.arrLibrary, check)
	default:
		result, e = service.GetARRImportStatus(ctx, id)
	}
	if check(ctx) != nil {
		fallbackHTTPError(w, 403, "master_required")
		return
	}
	if e != nil {
		status, code := 503, "arr_import_unavailable"
		if errors.Is(e, importer.ErrARRImportBusy) {
			status, code = 409, "arr_import_busy"
		}
		if errors.Is(e, importer.ErrSABFallbackNotFound) {
			status, code = 404, "fallback_not_found"
		}
		fallbackHTTPError(w, status, code)
		return
	}
	if cfg, err := h.previewSettings(ctx); err == nil {
		for _, kind := range []string{"sonarr", "radarr"} {
			if _, err := importer.ARRBackend(cfg, kind); err == nil {
				result.ConfiguredBackends = append(result.ConfiguredBackends, kind)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	_ = json.NewEncoder(w).Encode(result)
}
