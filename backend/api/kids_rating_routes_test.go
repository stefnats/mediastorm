package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/users"
)

type kidsRatingRouteFixture struct {
	metadata                                        *handlers.MetadataHandler
	router                                          *mux.Router
	users                                           *users.Service
	directory                                       string
	profile, foreign, token, scoped, master, unlock string
}

func newKidsRatingRouteFixture(t *testing.T, configureSettings ...func(*users.Service) *handlers.UserSettingsHandler) *kidsRatingRouteFixture {
	t.Helper()
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-master-password")
	directory := t.TempDir()
	accountsSvc, err := accounts.NewService(directory)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := accountsSvc.Create("synthetic-parent", "synthetic-parent-password")
	if err != nil {
		t.Fatal(err)
	}
	neighbour, err := accountsSvc.Create("synthetic-neighbour", "synthetic-other-password")
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := users.NewService(directory)
	if err != nil {
		t.Fatal(err)
	}
	child, err := profiles.CreateForAccount(parent.ID, "Synthetic child")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := profiles.CreateForAccount(neighbour.ID, "Synthetic other child")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{child.ID, foreign.ID} {
		if _, err := profiles.SetKidsProfile(id, true); err != nil {
			t.Fatal(err)
		}
		if _, err := profiles.SetKidsMode(id, "rating"); err != nil {
			t.Fatal(err)
		}
		if _, err := profiles.SetKidsMaxRating(id, "PG"); err != nil {
			t.Fatal(err)
		}
		if _, err := profiles.SetKidsMaxMovieRating(id, "G"); err != nil {
			t.Fatal(err)
		}
		if _, err := profiles.SetKidsMaxTVRating(id, "TV-G"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := profiles.SetPin(child.ID, "1234"); err != nil {
		t.Fatal(err)
	}
	sessionsSvc, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := sessionsSvc.Create(parent.ID, false, "synthetic", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := sessionsSvc.CreateScoped(parent.ID, false, "synthetic", "127.0.0.1", time.Hour, models.SessionScopeStream)
	if err != nil {
		t.Fatal(err)
	}
	master, ok := accountsSvc.GetMasterAccount()
	if !ok {
		t.Fatal("synthetic master account missing")
	}
	masterSession, err := sessionsSvc.Create(master.ID, true, "synthetic", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	usersHandler := handlers.NewUsersHandler(profiles, accountsSvc)
	unlocks := profileunlock.NewService(nil)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 1}
	}
	metadataHandler := &handlers.MetadataHandler{}
	var profileSettings *handlers.UserSettingsHandler
	if len(configureSettings) > 0 {
		profileSettings = configureSettings[0](profiles)
	}
	router := mux.NewRouter()
	// Exercise the production registration and middleware rather than copying
	// the two routes into a test-only router. Unused handlers are never invoked.
	Register(
		router,                              // r
		nil,                                 // settingsHandler
		metadataHandler,                     // metadataHandler
		nil,                                 // indexerHandler
		nil,                                 // playbackHandler
		nil,                                 // badStreamsHandler
		nil,                                 // prequeueHandler
		nil,                                 // usenetHandler
		nil,                                 // debridHandler
		nil,                                 // videoHandler
		usersHandler,                        // usersHandler
		nil,                                 // watchlistHandler
		nil,                                 // customListsHandler
		nil,                                 // displayListHandler
		&handlers.HistoryHandler{},          // historyHandler
		nil,                                 // debugHandler
		nil,                                 // logsHandler
		nil,                                 // liveHandler
		nil,                                 // recordingsHandler
		nil,                                 // localMediaHandler
		nil,                                 // epgHandler
		nil,                                 // sportsHandler
		nil,                                 // sportsLinksHandler
		profileSettings,                     // userSettingsHandler
		nil,                                 // subtitlesHandler
		nil,                                 // clientsHandler
		nil,                                 // contentPreferencesHandler
		nil,                                 // hiddenItemsHandler
		nil,                                 // imageHandler
		nil,                                 // startupHandler
		nil,                                 // detailsBundleHandler
		nil,                                 // calendarHandler
		nil,                                 // remoteAccessHandler
		nil,                                 // prerollHandler
		accountsSvc,                         // accountsSvc
		sessionsSvc,                         // sessionsSvc
		profiles,                            // usersSvc
		nil,                                 // shareHandler
		nil,                                 // watchRoomsHandler
		nil,                                 // watchPartyHandler
		"",                                  // homepageAPIKey
		nil,                                 // latencyAdmin
		WithProfileUnlocks(unlocks, policy), // opts
	)
	f := &kidsRatingRouteFixture{metadata: metadataHandler, router: router, users: profiles, directory: directory, profile: child.ID, foreign: foreign.ID, token: session.Token, scoped: scoped.Token, master: masterSession.Token}
	response := f.request(http.MethodPost, child.ID, "/pin/verify", session.Token, "", "", `{"pin":"1234"}`)
	if response.Code != http.StatusOK {
		t.Fatalf("unlock failed: %d %s", response.Code, response.Body.String())
	}
	var grant handlers.VerifyPinResponse
	if err := json.Unmarshal(response.Body.Bytes(), &grant); err != nil || grant.UnlockToken == "" {
		t.Fatal("unlock token missing")
	}
	f.unlock = grant.UnlockToken
	return f
}

func (f *kidsRatingRouteFixture) request(method, profile, suffix, token, unlock, password, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/api/users/"+profile+suffix, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if unlock != "" {
		r.Header.Set(ProfileUnlockHeader, unlock)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func TestKidsSplitRatingRegisteredRoutesSaveAndPersistIndependently(t *testing.T) {
	f := newKidsRatingRouteFixture(t)
	save := func(suffix, rating string) models.User {
		t.Helper()
		raw, _ := json.Marshal(map[string]string{"rating": rating})
		w := f.request(http.MethodPut, f.profile, suffix, f.token, f.unlock, "synthetic-parent-password", string(raw))
		if w.Code != http.StatusOK {
			t.Fatalf("save %s: %d %s", suffix, w.Code, w.Body.String())
		}
		var result models.User
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	movie := save("/kids/rating/movie", "PG-13")
	if movie.KidsMaxMovieRating != "PG-13" || movie.KidsMaxTVRating != "TV-G" || movie.KidsMaxRating != "PG" {
		t.Fatal("movie save changed another rating cap")
	}
	television := save("/kids/rating/tv", "TV-14")
	if television.KidsMaxTVRating != "TV-14" || television.KidsMaxMovieRating != "PG-13" || television.KidsMaxRating != "PG" {
		t.Fatal("TV save changed another rating cap")
	}
	cleared := save("/kids/rating/movie", "")
	if cleared.KidsMaxMovieRating != "" || cleared.KidsMaxTVRating != "TV-14" || cleared.KidsMaxRating != "PG" {
		t.Fatal("clearing movie override erased another cap or legacy fallback")
	}
	legacy := save("/kids/rating", "R")
	if legacy.KidsMaxRating != "R" || legacy.KidsMaxMovieRating != "" || legacy.KidsMaxTVRating != "TV-14" || !legacy.IsKidsProfile || legacy.KidsMode != "rating" {
		t.Fatal("legacy combined flow changed split caps or restriction mode")
	}
	restored, err := users.NewService(f.directory)
	if err != nil {
		t.Fatal(err)
	}
	stored, ok := restored.Get(f.profile)
	if !ok || stored.KidsMaxRating != "R" || stored.KidsMaxMovieRating != "" || stored.KidsMaxTVRating != "TV-14" {
		t.Fatal("router saves did not persist independently")
	}
	// Parent proof is fresh per request, even after an immediately successful save.
	before, _ := f.users.Get(f.profile)
	w := f.request(http.MethodPut, f.profile, "/kids/rating/tv", f.token, f.unlock, "", `{"rating":"TV-MA"}`)
	after, _ := f.users.Get(f.profile)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "parent_authorization_required") || !reflect.DeepEqual(before, after) {
		t.Fatal("a previous save retained parental authorization")
	}
}

func TestKidsSplitRatingRegisteredRoutesRejectUnauthorizedOrWrongProfile(t *testing.T) {
	f := newKidsRatingRouteFixture(t)
	for _, suffix := range []string{"/kids/rating/movie", "/kids/rating/tv"} {
		for _, tc := range []struct {
			name, profile, token, unlock, password, body, code string
			status                                             int
		}{
			{name: "no session", profile: f.profile, unlock: f.unlock, password: "synthetic-parent-password", status: 401, code: "unauthorized"},
			{name: "invalid session", profile: f.profile, token: "invalid-synthetic-token", unlock: f.unlock, password: "synthetic-parent-password", status: 401, code: "session_expired"},
			{name: "stream session", profile: f.profile, token: f.scoped, unlock: f.unlock, password: "synthetic-parent-password", status: 403, code: "forbidden"},
			{name: "foreign profile", profile: f.foreign, token: f.token, unlock: f.unlock, password: "synthetic-parent-password", status: 404, code: "profile_not_found"},
			{name: "unknown profile", profile: "synthetic-missing-profile", token: f.token, unlock: f.unlock, password: "synthetic-parent-password", status: 404, code: "profile_not_found"},
			{name: "locked profile", profile: f.profile, token: f.token, password: "synthetic-parent-password", status: 403, code: "profile_locked"},
			{name: "child PIN is not parent proof", profile: f.profile, token: f.token, unlock: f.unlock, password: "1234", status: 403, code: "parent_authorization_required"},
			{name: "missing parent proof", profile: f.profile, token: f.token, unlock: f.unlock, status: 403, code: "parent_authorization_required"},
			{name: "unknown mutation field", profile: f.profile, token: f.token, unlock: f.unlock, password: "synthetic-parent-password", body: `{"rating":"R","isKidsProfile":false}`, status: 400},
		} {
			t.Run(strings.TrimPrefix(suffix, "/kids/rating/")+"/"+tc.name, func(t *testing.T) {
				before, _ := f.users.Get(f.profile)
				otherBefore, _ := f.users.Get(f.foreign)
				body := tc.body
				if body == "" {
					body = `{"rating":"R"}`
				}
				w := f.request(http.MethodPut, tc.profile, suffix, tc.token, tc.unlock, tc.password, body)
				if w.Code != tc.status || tc.code != "" && !strings.Contains(w.Body.String(), tc.code) {
					t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
				}
				after, _ := f.users.Get(f.profile)
				otherAfter, _ := f.users.Get(f.foreign)
				if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(otherBefore, otherAfter) {
					t.Fatal("denied request mutated profile settings")
				}
			})
		}
	}
}

func TestKidsSplitRatingRegisteredRoutesPreserveMasterAndPreflightPolicy(t *testing.T) {
	f := newKidsRatingRouteFixture(t)
	for _, suffix := range []string{"/kids/rating/movie", "/kids/rating/tv"} {
		before, _ := f.users.Get(f.profile)
		w := f.request(http.MethodOptions, f.profile, suffix, "", "", "", "")
		after, _ := f.users.Get(f.profile)
		if w.Code != http.StatusOK || !reflect.DeepEqual(before, after) {
			t.Fatal("preflight required credentials or mutated a profile")
		}
		rating := "PG"
		if strings.HasSuffix(suffix, "/tv") {
			rating = "TV-PG"
		}
		body, _ := json.Marshal(map[string]string{"rating": rating})
		foreignBefore, _ := f.users.Get(f.foreign)
		for _, password := range []string{"", "synthetic-wrong-master-password"} {
			w = f.request(http.MethodPut, f.foreign, suffix, f.master, "", password, string(body))
			foreignAfter, _ := f.users.Get(f.foreign)
			if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "parent_authorization_required") || !reflect.DeepEqual(foreignBefore, foreignAfter) {
				t.Fatal("master without current password changed parental controls")
			}
		}
		w = f.request(http.MethodPut, f.foreign, suffix, f.master, "", "synthetic-master-password", string(body))
		if w.Code != http.StatusOK {
			t.Fatalf("confirmed master management failed: %d %s", w.Code, w.Body.String())
		}
		foreignAfter, _ := f.users.Get(f.foreign)
		if strings.HasSuffix(suffix, "/movie") && foreignAfter.KidsMaxMovieRating != rating || strings.HasSuffix(suffix, "/tv") && foreignAfter.KidsMaxTVRating != rating {
			t.Fatal("confirmed master did not update the selected rating")
		}
	}
}
