package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"novastream/handlers"
	"novastream/models"
	"novastream/services/reminders"
	"novastream/services/titleratings"
)

func TestReminderRegisteredRoutesEnforceOwnedFullProfileAndPIN(t *testing.T) {
	f := newKidsRatingRouteFixture(t)
	if _, err := f.users.SetKidsProfile(f.profile, false); err != nil {
		t.Fatal(err)
	}
	repo, err := reminders.NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f.metadata.UsersService = f.users
	service := reminders.New(repo, func(profile string) bool { u, ok := f.users.Get(profile); return ok && !u.IsKidsProfile })
	f.metadata.Reminders = service
	ratings := titleratings.New(nil)
	f.metadata.TitleRatings = ratings
	title := models.Title{ID: "tmdb:movie:7", Name: "Synthetic", MediaType: "movie"}
	ratings.Observe(f.profile, "synthetic-reminder-client", []models.Title{title})
	request := func(method, profile, suffix, token, unlock, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/users/"+profile+suffix, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Client-ID", "synthetic-reminder-client")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		if unlock != "" {
			r.Header.Set(ProfileUnlockHeader, unlock)
		}
		w := httptest.NewRecorder()
		f.router.ServeHTTP(w, r)
		return w
	}
	unlock := request("POST", f.profile, "/pin/verify", f.token, "", `{"pin":"1234"}`)
	var grant handlers.VerifyPinResponse
	if err := json.Unmarshal(unlock.Body.Bytes(), &grant); err != nil || grant.UnlockToken == "" {
		t.Fatal("fixture unlock failed")
	}
	for _, test := range []struct{ name, profile, token, unlock string }{{"anonymous", f.profile, "", ""}, {"scoped", f.profile, f.scoped, grant.UnlockToken}, {"foreign", f.foreign, f.token, grant.UnlockToken}, {"locked", f.profile, f.token, ""}} {
		t.Run(test.name, func(t *testing.T) {
			w := request("GET", test.profile, "/reminders", test.token, test.unlock, "")
			if w.Code == 200 || w.Code == 404 && test.name != "foreign" {
				t.Fatalf("route missing or bypassed gate: %d", w.Code)
			}
		})
	}
	created := request("POST", f.profile, "/reminders", f.token, grant.UnlockToken, `{"contentId":"tmdb:movie:7","contentType":"movie","eventKind":"matching_source"}`)
	if created.Code != 200 {
		t.Fatalf("registered create failed: %d %s", created.Code, created.Body.String())
	}
	var reply struct {
		Item models.Reminder `json:"item"`
	}
	json.Unmarshal(created.Body.Bytes(), &reply)
	checked, expires := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
	if err := service.ObserveSource(t.Context(), f.profile, "synthetic-reminder-client", title, nil, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires}); err != nil {
		t.Fatal(err)
	}
	inbox := request("GET", f.profile, "/reminder-events", f.token, grant.UnlockToken, "")
	if inbox.Code != 200 {
		t.Fatal(inbox.Body.String())
	}
	var events struct {
		Items []models.ReminderEvent `json:"items"`
	}
	json.Unmarshal(inbox.Body.Bytes(), &events)
	if len(events.Items) != 1 {
		t.Fatal("registered inbox missing event")
	}
	marked := request(http.MethodPatch, f.profile, "/reminder-events/"+events.Items[0].ID, f.token, grant.UnlockToken, `{"read":true}`)
	if marked.Code != 200 {
		t.Fatal(marked.Body.String())
	}
	revoked := request("DELETE", f.profile, "/reminders/"+reply.Item.ID, f.token, grant.UnlockToken, "")
	if revoked.Code != 204 {
		t.Fatal(revoked.Body.String())
	}
	if _, err := f.users.SetKidsProfile(f.profile, true); err != nil {
		t.Fatal(err)
	}
	blocked := request("GET", f.profile, "/reminder-events", f.token, grant.UnlockToken, "")
	if blocked.Code == 200 {
		t.Fatal("converted child profile exposed old reminders")
	}
}
