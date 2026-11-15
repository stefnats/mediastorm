package handlers

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/reminders"
)

// Optional real native serializer fixtures remain synthetic and contain no
// server URLs, private catalogue or client credentials.
func TestReminderNativeRequestFixturesAgainstRealHandlersAndOpenAPI(t *testing.T) {
	directory := os.Getenv("NATIVE_REMINDER_CONTRACT_DIR")
	if directory == "" {
		t.Skip("native serializer fixture directory required")
	}
	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		count++
		t.Run(file.Name(), func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(directory, file.Name()))
			if err != nil {
				t.Fatal(err)
			}
			h, s, _ := reminderFixture(t)
			method, target, handler := "POST", "/api/users/profile/reminders", h.RemindersListCreate
			request := reminderRequest(method, target, string(data))
			if strings.HasPrefix(file.Name(), "event-") {
				title := models.Title{ID: "tmdb:movie:123", Name: "Synthetic Reminder", MediaType: "movie", TMDBID: 123}
				h.TitleRatings.Observe("profile", "fixture-client", []models.Title{title})
				reminderCreateHTTP(t, h, title, models.ReminderMatchingSource)
				checked, expires := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
				if err := s.ObserveSource(t.Context(), "profile", "fixture-client", title, nil, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires}); err != nil {
					t.Fatal(err)
				}
				events, _, _ := s.Events(t.Context(), "profile", "", 100, false)
				if len(events) != 1 {
					t.Fatal("synthetic inbox event missing")
				}
				method = "PATCH"
				target = "/api/users/profile/reminder-events/" + events[0].ID
				handler = h.MarkReminderEvent
				request = reminderRequest(method, target, string(data))
				vars := mux.Vars(request)
				vars["eventID"] = events[0].ID
				request = mux.SetURLVars(request, vars)
			} else {
				var body reminders.CreateRequest
				if err := json.Unmarshal(data, &body); err != nil {
					t.Fatal(err)
				}
				if body.ContentID != "tmdb:movie:123" && body.ContentID != "tmdb:series:41" && body.ContentID != "tvdb:series:74852" {
					t.Fatal("unexpected nonsynthetic native fixture identity")
				}
				title := models.Title{ID: body.ContentID, Name: "Synthetic Reminder", MediaType: body.ContentType}
				h.TitleRatings.Observe("profile", "fixture-client", []models.Title{title})
				if body.ContentType == "series" {
					owner := body.ContentID
					if strings.HasPrefix(file.Name(), "cross-provider") {
						owner = "tvdb:series:74852"
					}
					episode := models.SeriesEpisode{ID: "episode-3", Numbering: &models.EpisodeNumbering{SeriesID: owner, Ordering: "official"}, SeasonNumber: 2, EpisodeNumber: 3, AiredDate: "2099-01-01"}
					details := models.SeriesDetails{Title: title, ActiveOrdering: "official", Seasons: []models.SeriesSeason{{Number: 2, Episodes: []models.SeriesEpisode{episode}}}}
					h.ObserveReminderDetails(request, nil, &details)
				}
			}
			if err := validator.ValidateRequest(request, data); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			handler(response, request)
			if response.Code != 200 {
				t.Fatalf("native request failed %d: %s", response.Code, response.Body.String())
			}
			if err := validator.ValidateResponse(request, response.Code, response.Header(), response.Body.Bytes()); err != nil {
				t.Fatalf("native response contract: %v", err)
			}
			if out := os.Getenv("REMINDER_BACKEND_CONTRACT_DIR"); out != "" {
				if err := os.MkdirAll(out, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(out, "backend-"+file.Name()), response.Body.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	if count != 13 {
		t.Fatalf("got %d native fixture files, want13", count)
	}
}
