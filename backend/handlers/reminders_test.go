package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/reminders"
)

type reminderProfiles map[string]models.User

func (p reminderProfiles) Get(id string) (models.User, bool) { value, ok := p[id]; return value, ok }
func reminderFixture(t *testing.T) (*MetadataHandler, *reminders.Service, models.Title) {
	t.Helper()
	h, ratings, _, title := ratingFixture(t)
	h.UsersService = reminderProfiles{"profile": {ID: "profile", AccountID: "account"}, "other": {ID: "other", AccountID: "account"}, "child": {ID: "child", AccountID: "account", IsKidsProfile: true}}
	repo, err := reminders.NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := reminders.New(repo, func(profile string) bool { u, ok := h.UsersService.Get(profile); return ok && !u.IsKidsProfile })
	h.Reminders = s
	ratings.Observe("profile", "fixture-client", []models.Title{title})
	return h, s, title
}
func reminderRequest(method, path, body string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("X-Client-ID", "fixture-client")
	r.Header.Set("Content-Type", "application/json")
	r = mux.SetURLVars(r, map[string]string{"userID": "profile"})
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
}
func reminderCall(t *testing.T, h *MetadataHandler, method, path, body string, handler http.HandlerFunc, vars map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	r := reminderRequest(method, path, body)
	for key, value := range vars {
		v := mux.Vars(r)
		v[key] = value
		r = mux.SetURLVars(r, v)
	}
	w := httptest.NewRecorder()
	handler(w, r)
	if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatal("private reminder response may be cached")
	}
	return w
}
func reminderCreateHTTP(t *testing.T, h *MetadataHandler, title models.Title, kind string) models.Reminder {
	t.Helper()
	body := `{"contentId":"` + title.ID + `","contentType":"` + title.MediaType + `","eventKind":"` + kind + `"}`
	w := reminderCall(t, h, "POST", "/api/users/profile/reminders", body, h.RemindersListCreate, nil)
	if w.Code != 200 {
		t.Fatalf("create failed: %d %s", w.Code, w.Body.String())
	}
	var reply struct {
		Supported bool            `json:"remindersSupported"`
		Requested string          `json:"requestedContentId"`
		Item      models.Reminder `json:"item"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil || !reply.Supported || reply.Requested != title.ID || reply.Item.Title.ID != reply.Item.ContentID || reply.Item.Title.MediaType != reply.Item.ContentType || !reply.Item.CreatedOnCurrentDevice {
		t.Fatal("invalid native correlation contract")
	}
	return reply.Item
}
func TestRemindersHTTPPrivateLifecycleAndCanonicalAliasCorrelation(t *testing.T) {
	h, s, title := reminderFixture(t)
	sub := reminderCreateHTTP(t, h, title, models.ReminderMatchingSource)
	title.IMDBID = "tt0000001"
	h.TitleRatings.Observe("profile", "fixture-client", []models.Title{title})
	alias := title
	alias.ID = "tt0000001"
	h.TitleRatings.Observe("profile", "fixture-client", []models.Title{alias})
	same := reminderCreateHTTP(t, h, alias, models.ReminderMatchingSource)
	if same.ID != sub.ID {
		t.Fatal("verified alias duplicated an interest")
	}
	checked, expires := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
	if err := s.ObserveSource(t.Context(), "profile", "fixture-client", title, nil, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires}); err != nil {
		t.Fatal(err)
	}
	list := reminderCall(t, h, "GET", "/api/users/profile/reminder-events?unread=true", "", h.ReminderEvents, nil)
	if list.Code != 200 {
		t.Fatal(list.Body.String())
	}
	var reply struct {
		Items []models.ReminderEvent `json:"items"`
	}
	json.Unmarshal(list.Body.Bytes(), &reply)
	if len(reply.Items) != 1 || !reply.Items[0].CandidateOnly {
		t.Fatal("candidate event missing")
	}
	id := reply.Items[0].ID
	marked := reminderCall(t, h, "PATCH", "/api/users/profile/reminder-events/"+id, `{"read":true,"dismissed":true}`, h.MarkReminderEvent, map[string]string{"eventID": id})
	if marked.Code != 200 {
		t.Fatal(marked.Body.String())
	}
	revoked := reminderCall(t, h, "DELETE", "/api/users/profile/reminders/"+sub.ID, "", h.RevokeReminder, map[string]string{"reminderID": sub.ID})
	if revoked.Code != 204 {
		t.Fatal(revoked.Body.String())
	}
	other := reminderCall(t, h, "PATCH", "/api/users/other/reminder-events/"+id, `{"read":false}`, h.MarkReminderEvent, map[string]string{"eventID": id, "userID": "other"})
	if other.Code != 404 {
		t.Fatal("inbox mutation crossed own profiles")
	}
}
func TestRemindersDenyScopedMasterForeignChildAndPINContexts(t *testing.T) {
	for _, mode := range []string{"stream", "watch_party", "jellyfin", "future_scope", "foreign", "child", "PIN"} {
		for _, master := range []bool{false, true} {
			if mode == "foreign" && master {
				continue
			}
			t.Run(mode+map[bool]string{true: "-master", false: ""}[master], func(t *testing.T) {
				h, _, title := reminderFixture(t)
				r := reminderRequest("POST", "/api/users/profile/reminders", `{"contentId":"`+title.ID+`","contentType":"movie","eventKind":"matching_source"}`)
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, master))
				switch mode {
				case "foreign":
					r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "foreign"))
				case "child":
					r = mux.SetURLVars(r, map[string]string{"userID": "child"})
				case "PIN":
					r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, r *http.Request, p string) bool {
						http.Error(w, "locked", 403)
						return false
					})
				default:
					r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: mode}))
				}
				w := httptest.NewRecorder()
				profileaccess.Middleware(func(p, a string) bool { return a == "account" })(http.HandlerFunc(h.RemindersListCreate)).ServeHTTP(w, r)
				if w.Code != 403 && !(mode == "foreign" && w.Code == 404) {
					t.Fatalf("guard failed: %d %s", w.Code, w.Body.String())
				}
				r.Method = "GET"
				w = httptest.NewRecorder()
				profileaccess.Middleware(func(p, a string) bool { return a == "account" })(http.HandlerFunc(h.ReminderEvents)).ServeHTTP(w, r)
				if w.Code != 403 && !(mode == "foreign" && w.Code == 404) {
					t.Fatal("restricted inbox exposed")
				}
			})
		}
	}
}
func TestReminderMetadataHookIndependentOfSourceHintsAndNoRetroactiveFlood(t *testing.T) {
	h, s, title := reminderFixture(t)
	h.SourceAvailability = nil
	title.Releases = []models.Release{{Type: "digital", Date: "2099-01-01"}}
	r := reminderRequest("GET", "/api/users/profile/details-bundle", "")
	w := httptest.NewRecorder()
	h.ObserveReminderDetails(r, &title, nil)
	h.encodeMetadata(w, r, title)
	sub := reminderCreateHTTP(t, h, title, models.ReminderRelease)
	title.Releases[0].Date = "2000-01-01"
	h.ObserveReminderDetails(r, &title, nil)
	items, _, err := s.Events(t.Context(), "profile", "", 100, false)
	if err != nil || len(items) != 1 || items[0].ReminderID != sub.ID || items[0].CandidateOnly {
		t.Fatal("metadata-only release milestone failed")
	}
	h.ObserveReminderDetails(r, &title, nil)
	items, _, _ = s.Events(t.Context(), "profile", "", 100, false)
	if len(items) != 1 {
		t.Fatal("repeated metadata duplicated milestone")
	}
}
func TestReminderCreateRejectsClientMetadataUnknownEpisodeAndReloadHasStableCode(t *testing.T) {
	h, _, title := reminderFixture(t)
	for _, extra := range []string{`,"title":"forged"`, `,"date":"2099-01-01"`, `,"url":"https://synthetic.invalid/private"`, `,"profileId":"other"`} {
		body := `{"contentId":"` + title.ID + `","contentType":"movie","eventKind":"release"` + extra + `}`
		w := reminderCall(t, h, "POST", "/api/users/profile/reminders", body, h.RemindersListCreate, nil)
		if w.Code != 400 {
			t.Fatal("untrusted field accepted")
		}
	}
	w := reminderCall(t, h, "POST", "/api/users/profile/reminders", `{"contentId":"tmdb:movie:999","contentType":"movie","eventKind":"release"}`, h.RemindersListCreate, nil)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "reminder_metadata_reload_required") {
		t.Fatal("missing reload recovery code")
	}
}
func TestReminderOpenAPIRealHandlerContract(t *testing.T) {
	v, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	reminderOpenAPIContract(t, v)
}
func reminderOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	h, s, title := reminderFixture(t)
	run := func(method, target, body string, handler http.HandlerFunc, vars map[string]string, status int) *httptest.ResponseRecorder {
		t.Helper()
		r := reminderRequest(method, target, body)
		if vars != nil {
			vars["userID"] = "profile"
			r = mux.SetURLVars(r, vars)
		}
		if err := v.ValidateRequest(r, []byte(body)); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		handler(w, r)
		if w.Code != status {
			t.Fatalf("status %d: %s", w.Code, w.Body.String())
		}
		if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
			t.Fatalf("response contract: %v %s", err, w.Body.String())
		}
		return w
	}
	run("GET", "/api/users/profile/reminders", "", h.RemindersListCreate, nil, 200)
	created := run("POST", "/api/users/profile/reminders", `{"contentId":"`+title.ID+`","contentType":"movie","eventKind":"matching_source"}`, h.RemindersListCreate, nil, 200)
	var reply struct {
		Item models.Reminder `json:"item"`
	}
	json.Unmarshal(created.Body.Bytes(), &reply)
	checked, expires := time.Now().UTC(), time.Now().UTC().Add(time.Hour)
	s.ObserveSource(t.Context(), "profile", "fixture-client", title, nil, models.SourceAvailability{State: "found", CheckedAt: &checked, ExpiresAt: &expires})
	events := run("GET", "/api/users/profile/reminder-events?unread=true&limit=1", "", h.ReminderEvents, nil, 200)
	var inbox struct {
		Items []models.ReminderEvent `json:"items"`
	}
	json.Unmarshal(events.Body.Bytes(), &inbox)
	run("PATCH", "/api/users/profile/reminder-events/"+inbox.Items[0].ID, `{"read":true}`, h.MarkReminderEvent, map[string]string{"eventID": inbox.Items[0].ID}, 200)
	run("DELETE", "/api/users/profile/reminders/"+reply.Item.ID, "", h.RevokeReminder, map[string]string{"reminderID": reply.Item.ID}, 204)
}

func TestReminderPassiveMetadataHookAlsoHonorsInstalledPINAndScopedSessionGuards(t *testing.T) {
	h, s, title := reminderFixture(t)
	title.Releases = []models.Release{{Type: "digital", Date: "2099-01-01"}}
	r := reminderRequest("GET", "/api/users/profile/details-bundle", "")
	h.ObserveReminderDetails(r, &title, nil)
	reminderCreateHTTP(t, h, title, models.ReminderRelease)
	title.Releases[0].Date = "2000-01-01"
	for _, mode := range []string{"PIN", "watch_party", "jellyfin"} {
		blocked := r.Clone(r.Context())
		if mode == "PIN" {
			blocked = profileaccess.WithResourceGuard(blocked, func(w http.ResponseWriter, r *http.Request, p string) bool { return false })
		} else {
			blocked = blocked.WithContext(context.WithValue(blocked.Context(), auth.ContextKeySession, models.Session{Scope: mode}))
		}
		profileaccess.Middleware(func(p, a string) bool { return a == "account" })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ObserveReminderDetails(r, &title, nil) })).ServeHTTP(httptest.NewRecorder(), blocked)
	}
	items, _, err := s.Events(t.Context(), "profile", "", 100, false)
	if err != nil || len(items) != 0 {
		t.Fatal("denied passive observation recorded an event")
	}
}

func TestReminderInterestLimitHasStableRecoveryCode(t *testing.T) {
	h, _, title := reminderFixture(t)
	repo, err := reminders.NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.Reminders = reminders.New(repo, func(string) bool { return true })
	if err := repo.Update(t.Context(), "profile", func(state *models.ReminderProfileState) error {
		for i := 0; i < reminders.MaxActive; i++ {
			state.Subscriptions = append(state.Subscriptions, models.StoredReminder{Reminder: models.Reminder{ID: fmt.Sprintf("existing-%d", i)}})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	response := reminderCall(t, h, "POST", "/api/users/profile/reminders", `{"contentId":"`+title.ID+`","contentType":"movie","eventKind":"release"}`, h.RemindersListCreate, nil)
	if response.Code != 409 || !strings.Contains(response.Body.String(), "reminder_interest_limit") {
		t.Fatal("quota response cannot lead to useful native recovery")
	}
}
