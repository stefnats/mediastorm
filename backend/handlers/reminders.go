package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/reminders"
)

func (h *MetadataHandler) reminderIdentity(r *http.Request) (string, string, bool) {
	if h.Reminders == nil || h.UsersService == nil || auth.IsShareLinkRequest(r) {
		return "", "", false
	}
	if auth.GetSessionScope(r) != "" {
		return "", "", false
	}
	profile := strings.TrimSpace(mux.Vars(r)["userID"])
	if profile == "" {
		profile = strings.TrimSpace(r.URL.Query().Get("userId"))
	}
	client := requestClientID(r)
	user, ok := h.UsersService.Get(profile)
	if !ok || user.IsKidsProfile || profile == "" || len(profile) > 128 || client == "" || len(client) > 128 || !auth.IsMaster(r) && (auth.GetAccountID(r) == "" || user.AccountID != auth.GetAccountID(r)) {
		return "", "", false
	}
	return profile, client, true
}
func (h *MetadataHandler) reminderAccess(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	w.Header().Set("Cache-Control", "private, no-store")
	if h.Reminders == nil {
		http.Error(w, "reminders unavailable", 501)
		return "", "", false
	}
	profile, client, ok := h.reminderIdentity(r)
	if !ok {
		http.Error(w, "reminders unavailable for this profile", 403)
		return "", "", false
	}
	if !profileaccess.CheckResource(w, r, profile, "") {
		return "", "", false
	}
	return profile, client, true
}
func reminderError(w http.ResponseWriter, err error) {
	status := 500
	code := "reminder_storage_unavailable"
	message := "reminder storage unavailable"
	switch {
	case errors.Is(err, reminders.ErrInvalid):
		code = "reminder_invalid_request"
		status = 400
		message = "invalid reminder request"
	case errors.Is(err, reminders.ErrReload):
		code = "reminder_metadata_reload_required"
		status = 409
		message = "reload this title before creating a reminder"
	case errors.Is(err, reminders.ErrNotFound):
		code = "reminder_not_found"
		status = 404
		message = "reminder not found"
	case errors.Is(err, reminders.ErrLimit):
		code = "reminder_interest_limit"
		status = 409
		message = "reminder limit reached"
	case errors.Is(err, reminders.ErrForbidden):
		code = "reminder_profile_unavailable"
		status = 403
		message = "reminders unavailable for this profile"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message, "code": code})
}
func reminderBody(w http.ResponseWriter, r *http.Request, value any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil {
		http.Error(w, "invalid reminder request", 400)
		return false
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		http.Error(w, "one JSON object is required", 400)
		return false
	}
	return true
}
func reminderJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
func (h *MetadataHandler) RemindersListCreate(w http.ResponseWriter, r *http.Request) {
	profile, client, ok := h.reminderAccess(w, r)
	if !ok {
		return
	}
	if r.Method == http.MethodGet {
		items, err := h.Reminders.List(r.Context(), profile, client)
		if err != nil {
			reminderError(w, err)
			return
		}
		reminderJSON(w, map[string]any{"remindersSupported": true, "monitoring": "observations_only", "items": items})
		return
	}
	var body reminders.CreateRequest
	if !reminderBody(w, r, &body) {
		return
	}
	if h.TitleRatings == nil {
		reminderError(w, reminders.ErrReload)
		return
	}
	title, found := h.TitleRatings.Resolve(profile, client, body.ContentType, body.ContentID)
	if !found {
		reminderError(w, reminders.ErrReload)
		return
	}
	item, candidate, err := h.Reminders.Create(r.Context(), profile, client, body, title)
	if err != nil {
		reminderError(w, err)
		return
	}
	reply := map[string]any{"remindersSupported": true, "monitoring": "observations_only", "item": item, "requestedContentId": body.ContentID}
	if candidate != nil {
		reply["currentCandidate"] = candidate
	}
	reminderJSON(w, reply)
}
func (h *MetadataHandler) RevokeReminder(w http.ResponseWriter, r *http.Request) {
	profile, _, ok := h.reminderAccess(w, r)
	if !ok {
		return
	}
	if err := h.Reminders.Revoke(r.Context(), profile, mux.Vars(r)["reminderID"]); err != nil {
		reminderError(w, err)
		return
	}
	w.WriteHeader(204)
}
func (h *MetadataHandler) ReminderEvents(w http.ResponseWriter, r *http.Request) {
	profile, _, ok := h.reminderAccess(w, r)
	if !ok {
		return
	}
	limit := 50
	var err error
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			reminderError(w, reminders.ErrInvalid)
			return
		}
	}
	unread := false
	if raw := r.URL.Query().Get("unread"); raw != "" {
		unread, err = strconv.ParseBool(raw)
		if err != nil {
			reminderError(w, reminders.ErrInvalid)
			return
		}
	}
	items, next, err := h.Reminders.Events(r.Context(), profile, r.URL.Query().Get("cursor"), limit, unread)
	if err != nil {
		reminderError(w, err)
		return
	}
	reply := map[string]any{"remindersSupported": true, "monitoring": "observations_only", "items": items}
	if next != "" {
		reply["nextCursor"] = next
	}
	reminderJSON(w, reply)
}
func (h *MetadataHandler) MarkReminderEvent(w http.ResponseWriter, r *http.Request) {
	profile, _, ok := h.reminderAccess(w, r)
	if !ok {
		return
	}
	var body struct {
		Read      *bool `json:"read"`
		Dismissed *bool `json:"dismissed"`
	}
	if !reminderBody(w, r, &body) {
		return
	}
	item, err := h.Reminders.Mark(r.Context(), profile, mux.Vars(r)["eventID"], body.Read, body.Dismissed)
	if err != nil {
		reminderError(w, err)
		return
	}
	reminderJSON(w, map[string]any{"remindersSupported": true, "item": item})
}

// These hooks observe already-produced payloads. They never refresh metadata.
func (h *MetadataHandler) ObserveReminderDetails(r *http.Request, movie *models.Title, series *models.SeriesDetails) {
	profile, client, ok := h.reminderIdentity(r)
	if !ok {
		return
	}
	sink := reminderObservationSink{header: make(http.Header)}
	if !profileaccess.CheckResource(&sink, r, profile, "") {
		return
	}
	bounded, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()
	if movie != nil {
		h.logReminderObservation(h.Reminders.ObserveTitles(bounded, profile, client, []models.Title{*movie}))
	}
	if series != nil {
		h.logReminderObservation(h.Reminders.ObserveSeries(bounded, profile, client, series))
	}
}
func (h *MetadataHandler) observeReminderPayload(r *http.Request, payload any) {
	switch value := payload.(type) {
	case models.Title:
		h.ObserveReminderDetails(r, &value, nil)
	case *models.Title:
		h.ObserveReminderDetails(r, value, nil)
	case models.SeriesDetails:
		h.ObserveReminderDetails(r, nil, &value)
	case *models.SeriesDetails:
		h.ObserveReminderDetails(r, nil, value)
	}
}

func (h *MetadataHandler) logReminderObservation(err error) {
	if err != nil {
		log.Printf("[reminders] observation was not persisted: %v", err)
	}
}

// A passive observer must respect installed ownership/PIN resource guards
// without modifying the metadata response when observation is unavailable.
type reminderObservationSink struct{ header http.Header }

func (w *reminderObservationSink) Header() http.Header            { return w.header }
func (w *reminderObservationSink) Write(data []byte) (int, error) { return len(data), nil }
func (w *reminderObservationSink) WriteHeader(int)                {}
