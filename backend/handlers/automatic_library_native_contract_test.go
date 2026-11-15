package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/playback"
)

// These payloads are exported by the native client's request serializer tests.
// Validating them against real handlers prevents each side testing only its own
// independently invented DTO, especially progress item IDs versus metadata IDs.
func TestAutomaticLibraryNativeSerializerContracts(t *testing.T) {
	paths, err := filepath.Glob("testdata/automatic_library_native/*.json")
	if err != nil || len(paths) != 7 {
		t.Fatal("seven native serializer fixtures are required")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var body playback.PrequeueRequest
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&body); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(raw, []byte(`"itemId"`)) {
				t.Fatal("progress identity was serialized as a metadata input")
			}
			if body.EpisodeID != "" && !strings.HasPrefix(body.EpisodeID, "tvdb:episode:") {
				t.Fatal("unexpected metadata leaf namespace")
			}
			h, _, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
			h.users = fallbackProfiles{models.User{ID: body.UserID, AccountID: "account"}}
			title := canonicalPrequeueTitleID(body.MediaType, body.TitleID, body.ImdbID, body.TmdbID, body.TvdbID)
			var ep *models.EpisodeReference
			if body.MediaType == "series" {
				ep = &models.EpisodeReference{Numbering: body.Numbering, SeasonNumber: body.SeasonNumber, EpisodeNumber: body.EpisodeNumber, EpisodeID: body.EpisodeID}
			}
			scope := automaticLibraryScope(h.prequeueSettingsScopeKey(body.UserID, body.ClientID, title), body.AutomaticLibrarySelection)
			entry, _ := h.store.CreateScoped(title, body.TitleName, body.UserID, body.MediaType, body.Year, ep, "details", scope, body.AutomaticLibrarySelection)
			h.store.Update(entry.ID, func(e *playback.PrequeueEntry) { e.Status = playback.PrequeueStatusSearching })
			validator, err := openapi.NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue", bytes.NewReader(raw))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer synthetic-contract")
			r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
			if err := validator.ValidateRequest(r, raw); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.Prequeue(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("native POST %d %s", w.Code, w.Body.String())
			}
			if err := validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			var response playback.PrequeueResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.PrequeueID != entry.ID {
				t.Fatal("native contract did not reuse matching context")
			}
			statusRequest := httptest.NewRequest(http.MethodGet, "/api/playback/prequeue/"+entry.ID, nil)
			statusRequest.Header.Set("Authorization", "Bearer synthetic-contract")
			statusRequest = mux.SetURLVars(statusRequest, map[string]string{"prequeueID": entry.ID})
			statusRequest = statusRequest.WithContext(context.WithValue(statusRequest.Context(), auth.ContextKeyAccountID, "account"))
			status := httptest.NewRecorder()
			h.GetStatus(status, statusRequest)
			if status.Code != http.StatusOK {
				t.Fatalf("native status %d %s", status.Code, status.Body.String())
			}
			if err := validator.ValidateResponse(statusRequest, status.Code, status.Header(), status.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			var state playback.PrequeueStatusResponse
			if err := json.Unmarshal(status.Body.Bytes(), &state); err != nil || state.AutomaticLibrarySelection != body.AutomaticLibrarySelection {
				t.Fatal("queued/searching status lost source mode")
			}
			if ep != nil && (state.TargetEpisode == nil || state.TargetEpisode.EpisodeID != body.EpisodeID || !models.SameEpisodeNumbering(state.TargetEpisode.Numbering, body.Numbering)) {
				t.Fatal("native metadata identity changed at prequeue boundary")
			}
		})
	}
}
