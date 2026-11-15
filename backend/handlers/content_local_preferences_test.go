package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	content_preferences "novastream/services/content_preferences"
	"novastream/services/playback"
)

func contentLocalHandler(t *testing.T) (*ContentPreferencesHandler, *content_preferences.Service) {
	t.Helper()
	service, err := content_preferences.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	users := &fakeRecordingUsersProvider{users: []models.User{{ID: "viewer", AccountID: "account"}, {ID: "foreign", AccountID: "another-account"}}}
	return NewContentPreferencesHandler(service, users), service
}

func contentLocalRequest(method, user, body string) *http.Request {
	r := httptest.NewRequest(method, "/api/users/"+user+"/preferences/content", strings.NewReader(body))
	r = mux.SetURLVars(r, map[string]string{"userID": user, "contentID": "tmdb:tv:123:season:2"})
	return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
}

func TestContentLocalPreferencePartialUpdatesPreserveTracksAndOverrides(t *testing.T) {
	h, s := contentLocalHandler(t)
	put := func(body string) models.ContentPreference {
		t.Helper()
		w := httptest.NewRecorder()
		h.SetPreference(w, contentLocalRequest("PUT", "viewer", body))
		if w.Code != 200 {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		var pref models.ContentPreference
		if err := json.Unmarshal(w.Body.Bytes(), &pref); err != nil {
			t.Fatal(err)
		}
		return pref
	}
	pref := put(`{"contentId":"tmdb:tv:123:season:2","contentType":"series","audioLanguage":"fra","subtitleLanguage":"spa","subtitleMode":"forced-only"}`)
	pref = put(`{"contentId":"TMDB:TV:123:SEASON:2","preferLocalFiles":true}`)
	if pref.AudioLanguage != "fra" || pref.SubtitleLanguage != "spa" || pref.SubtitleMode != "forced-only" || pref.PreferLocalFiles == nil || !*pref.PreferLocalFiles {
		t.Fatalf("source-only update lost tracks: %+v", pref)
	}
	pref = put(`{"contentId":"tmdb:tv:123:season:2","audioLanguage":"eng"}`)
	if pref.PreferLocalFiles == nil || !*pref.PreferLocalFiles || pref.SubtitleLanguage != "spa" {
		t.Fatal("old audio client erased source override or subtitles")
	}
	pref = put(`{"contentId":"tmdb:tv:123:season:2","preferLocalFiles":false}`)
	if pref.PreferLocalFiles == nil || *pref.PreferLocalFiles {
		t.Fatal("explicit normal ordering became inherit")
	}
	pref = put(`{"contentId":"tmdb:tv:123:season:2","preferLocalFiles":null}`)
	if pref.PreferLocalFiles != nil || pref.AudioLanguage != "eng" || pref.SubtitleLanguage != "spa" {
		t.Fatal("inherit erased tracks or retained override")
	}
	stored, _ := s.Get("viewer", pref.ContentID)
	if !reflect.DeepEqual(stored, &pref) {
		t.Fatal("response is not the stored preference")
	}
}

func TestContentLocalPreferenceCapabilityAndDedicatedSourceRoute(t *testing.T) {
	h, s := contentLocalHandler(t)
	r := contentLocalRequest("GET", "viewer", "")
	w := httptest.NewRecorder()
	h.GetPreference(w, r)
	var missing map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &missing); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || missing["localSourcePreferenceSupported"] != true || len(missing) != 1 {
		t.Fatalf("missing preference did not report capability safely: %d %s", w.Code, w.Body.String())
	}
	id := "tmdb:tv:123:season:2"
	if err := s.Set("viewer", models.ContentPreference{ContentID: id, ContentType: "series", AudioLanguage: "fra", SubtitleLanguage: "spa", SubtitleMode: "forced-only"}); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"true", "false", "null"} {
		w = httptest.NewRecorder()
		h.SetLocalSourcePreference(w, contentLocalRequest("PUT", "viewer", `{"contentId":"`+id+`","contentType":"series","preferLocalFiles":`+flag+`}`))
		if w.Code != 200 {
			t.Fatalf("source route rejected %s: %d %s", flag, w.Code, w.Body.String())
		}
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["localSourcePreferenceSupported"] != true || body["audioLanguage"] != "fra" || body["subtitleLanguage"] != "spa" || body["subtitleMode"] != "forced-only" {
			t.Fatal("source route erased tracks or capability")
		}
		if flag == "null" {
			if _, present := body["preferLocalFiles"]; present {
				t.Fatal("inherit persisted a source override")
			}
		} else if body["preferLocalFiles"] != (flag == "true") {
			t.Fatal("dedicated route lost explicit true/false")
		}
	}
	before, _ := s.Get("viewer", id)
	for _, body := range []string{`{"contentId":"` + id + `"}`, `{"contentId":"` + id + `","preferLocalFiles":true,"audioLanguage":"eng"}`, `{"contentId":"` + id + `","preferLocalFiles":true,"subtitleMode":"off"}`, `{"contentId":"` + id + `","preferLocalFiles":true,"unexpected":1}`} {
		w = httptest.NewRecorder()
		h.SetLocalSourcePreference(w, contentLocalRequest("PUT", "viewer", body))
		if w.Code != 400 {
			t.Fatalf("source route accepted extra/missing fields: %d", w.Code)
		}
	}
	after, _ := s.Get("viewer", id)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("rejected source-only request mutated saved preferences")
	}
}

func TestContentLocalPreferenceWritesInvalidateOnlyOwnedProfilePreparations(t *testing.T) {
	h, s := contentLocalHandler(t)
	store := playback.NewPrequeueStore(time.Minute)
	h.SetPrequeueStore(store)
	current, _ := store.Create("tmdb:tv:123", "Synthetic show", "viewer", "series", 2026, &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 1}, "manual")
	other, _ := store.Create("tmdb:tv:123", "Synthetic show", "foreign", "series", 2026, nil, "manual")
	w := httptest.NewRecorder()
	h.SetPreference(w, contentLocalRequest("PUT", "viewer", `{"contentId":"tmdb:tv:123:season:2","preferLocalFiles":true}`))
	if w.Code != 200 {
		t.Fatalf("save: %d %s", w.Code, w.Body.String())
	}
	if _, ok := store.Get(current.ID); ok {
		t.Fatal("pending source remained reusable")
	}
	if _, ok := store.Get(other.ID); !ok {
		t.Fatal("another profile preparation was removed")
	}
	current, _ = store.Create("tmdb:tv:123", "Synthetic show", "viewer", "series", 2026, nil, "manual")
	w = httptest.NewRecorder()
	h.DeletePreference(w, contentLocalRequest("DELETE", "viewer", ""))
	if w.Code != 204 {
		t.Fatalf("delete: %d", w.Code)
	}
	if _, ok := store.Get(current.ID); ok {
		t.Fatal("deletion kept old preparation")
	}
	if pref, _ := s.Get("viewer", "tmdb:tv:123:season:2"); pref != nil {
		t.Fatal("DELETE no longer removes the complete targeted row")
	}
}

func TestContentLocalPreferenceOwnershipPinAndScopedSessions(t *testing.T) {
	h, s := contentLocalHandler(t)
	for _, user := range []string{"foreign", "missing"} {
		w := httptest.NewRecorder()
		h.SetPreference(w, contentLocalRequest("PUT", user, `{"contentId":"movie","preferLocalFiles":true}`))
		if w.Code != 404 {
			t.Fatalf("foreign selector %s: %d", user, w.Code)
		}
	}
	r := contentLocalRequest("PUT", "viewer", `{"contentId":"movie","preferLocalFiles":true}`)
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
	w := httptest.NewRecorder()
	h.SetPreference(w, r)
	if w.Code != 403 {
		t.Fatal("share session edited preference")
	}
	r = contentLocalRequest("PUT", "viewer", `{"contentId":"movie","preferLocalFiles":true}`)
	r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, r *http.Request, _ string) bool {
		http.Error(w, "profile locked", 403)
		return false
	})
	guarded := profileaccess.Middleware(func(profile, account string) bool { return profile == "viewer" && account == "account" })(http.HandlerFunc(h.SetPreference))
	w = httptest.NewRecorder()
	guarded.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("PIN guard not retained: %d", w.Code)
	}
	if prefs, _ := s.List("viewer"); len(prefs) != 0 {
		t.Fatal("rejected request changed preferences")
	}
}

func TestContentLocalPreferenceRejectsInvalidBodiesWithoutInvalidation(t *testing.T) {
	h, _ := contentLocalHandler(t)
	store := playback.NewPrequeueStore(time.Minute)
	h.SetPrequeueStore(store)
	entry, _ := store.Create("movie", "Synthetic", "viewer", "movie", 2026, nil, "manual")
	for _, body := range []string{`null`, `[]`, `{}`, `{"contentId":" "}`, `{"contentId":"movie","preferLocalFiles":"inherit"}`, `{"contentId":"movie","preferLocalFiles":true} {}`, `{"contentId":"series:1:season:02","preferLocalFiles":true}`, `{"contentId":"` + strings.Repeat("x", 17000) + `"}`} {
		w := httptest.NewRecorder()
		h.SetPreference(w, contentLocalRequest("PUT", "viewer", body))
		if w.Code != 400 {
			t.Fatalf("invalid body: status=%d", w.Code)
		}
	}
	if _, ok := store.Get(entry.ID); !ok {
		t.Fatal("rejected update invalidated existing preparation")
	}
}

func TestContentLocalPreferenceConcurrentPartialUpdatesMerge(t *testing.T) {
	h, s := contentLocalHandler(t)
	var wg sync.WaitGroup
	for _, body := range []string{`{"contentId":"movie","audioLanguage":"fra"}`, `{"contentId":"movie","subtitleLanguage":"spa"}`, `{"contentId":"movie","preferLocalFiles":false}`} {
		wg.Add(1)
		go func(body string) {
			defer wg.Done()
			w := httptest.NewRecorder()
			h.SetPreference(w, contentLocalRequest("PUT", "viewer", body))
			if w.Code != 200 {
				t.Errorf("status=%d", w.Code)
			}
		}(body)
	}
	wg.Wait()
	pref, _ := s.Get("viewer", "movie")
	if pref.AudioLanguage != "fra" || pref.SubtitleLanguage != "spa" || pref.PreferLocalFiles == nil || *pref.PreferLocalFiles {
		t.Fatalf("concurrent partial update lost fields: %+v", pref)
	}
}

func TestContentLocalPreferenceControlsEligibleSourceAndPreparationFingerprint(t *testing.T) {
	for _, mode := range []string{"prefer-local", "normal-selection", "missing-local", "denied-library", "unplayable-local"} {
		t.Run(mode, func(t *testing.T) {
			search := &fallbackSearch{results: []models.NZBResult{{Title: "Synthetic.Movie.1080p.WEB-DL", Link: "https://example.invalid/synthetic", ServiceType: models.ServiceTypeDebrid, Indexer: "AIOStreams", Attributes: map[string]string{"resolution": "1080p"}}}}
			h, catalog, access := newFallbackWorker(t, search)
			prefs, err := content_preferences.NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			h.SetContentPreferencesService(prefs)
			before := h.prequeueSettingsScopeKey("profile", "", "tmdb:movie:123")
			if err := prefs.Set("profile", models.ContentPreference{ContentID: "tmdb:movie:123", ContentType: "movie", PreferLocalFiles: models.BoolPtr(mode != "normal-selection")}); err != nil {
				t.Fatal(err)
			}
			if before == h.prequeueSettingsScopeKey("profile", "", "tmdb:movie:123") {
				t.Fatal("content override did not rotate preparation fingerprint")
			}
			catalog.unavailable = mode == "missing-local"
			access.deny.Store(mode == "denied-library")
			if mode == "unplayable-local" {
				h.fullProber = fallbackFullProber{run: func(_ context.Context, path string) (*VideoFullResult, error) {
					if strings.HasPrefix(path, "localmedia:") {
						return nil, context.DeadlineExceeded
					}
					return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1920, VideoHeight: 1080, Duration: 100}, nil
				}}
			}
			got := runFallbackWorker(t, h)
			wantLocal := mode == "prefer-local"
			if got.Status != playback.PrequeueStatusReady || strings.HasPrefix(got.StreamPath, "localmedia:") != wantLocal {
				t.Fatalf("selection status=%s path=%s error=%s", got.Status, got.StreamPath, got.Error)
			}
			if wantLocal && search.calls.Load() != 0 {
				t.Fatal("content-local choice still started remote search")
			}
		})
	}
}

func TestContentLocalPreferenceSeasonFingerprintAndRemoteReadyInvalidation(t *testing.T) {
	h, _, _ := newFallbackWorker(t, &fallbackSearch{})
	prefs, err := content_preferences.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.SetContentPreferencesService(prefs)
	title := "tmdb:tv:123"
	ep := &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 4}
	before := h.prequeueSettingsScopeKey("profile", "", title)
	if err := prefs.Set("profile", models.ContentPreference{ContentID: title + ":season:2", ContentType: "series", PreferLocalFiles: models.BoolPtr(true)}); err != nil {
		t.Fatal(err)
	}
	if before == h.prequeueSettingsScopeKey("profile", "", title) {
		t.Fatal("season override did not rotate title-scoped prewarm")
	}
	if !h.localPlaybackPreferredFor("profile", title, ep) || h.localPlaybackPreferredFor("profile", title, &models.EpisodeReference{SeasonNumber: 1, EpisodeNumber: 4}) {
		t.Fatal("season preference was ignored or leaked to another season")
	}
	entry := &playback.PrequeueEntry{ID: "prepared", UserID: "profile", TitleID: title, MediaType: "series", StreamPath: "/webdav/synthetic.mkv", TargetEpisode: ep}
	r := httptest.NewRequest("GET", "/", nil)
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
	if err := h.validateReadyEntryForRequest(r, entry); err == nil {
		t.Fatal("old search preparation bypassed season-local override")
	}
	if err := prefs.Set("profile", models.ContentPreference{ContentID: title + ":season:2:episode:4", ContentType: "series", PreferLocalFiles: models.BoolPtr(false)}); err != nil {
		t.Fatal(err)
	}
	if h.localPlaybackPreferredFor("profile", title, ep) {
		t.Fatal("episode false did not override season true")
	}
}

func TestContentLocalPreferenceKeepsActiveReleaseReportsAndRetryReachable(t *testing.T) {
	for _, mode := range []string{"audio-only", "source-unchanged", "source-changed", "source-cleared"} {
		t.Run(mode, func(t *testing.T) {
			h, entry, _ := releaseReportFixture(t)
			prefs, err := content_preferences.NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := prefs.Set("profile", models.ContentPreference{ContentID: entry.TitleID, ContentType: "movie", AudioLanguage: "deu", PreferLocalFiles: models.BoolPtr(true)}); err != nil {
				t.Fatal(err)
			}
			h.SetContentPreferencesService(prefs)
			users := &fakeRecordingUsersProvider{users: []models.User{{ID: "profile", AccountID: "account"}}}
			preferences := NewContentPreferencesHandler(prefs, users)
			preferences.SetPrequeueStore(h.store)
			body := `{"contentId":"tmdb:movie:123","audioLanguage":"eng"}`
			switch mode {
			case "source-unchanged":
				body = `{"contentId":"tmdb:movie:123","preferLocalFiles":true}`
			case "source-changed":
				body = `{"contentId":"tmdb:movie:123","preferLocalFiles":false}`
			case "source-cleared":
				body = `{"contentId":"tmdb:movie:123","preferLocalFiles":null}`
			}
			w := httptest.NewRecorder()
			preferences.SetPreference(w, contentLocalRequest("PUT", "profile", body))
			if w.Code != 200 {
				t.Fatalf("save: %d %s", w.Code, w.Body.String())
			}
			kept, ok := h.store.Get(entry.ID)
			if !ok || kept.SelectedResult == nil || kept.StreamPath != entry.StreamPath {
				t.Fatal("preference save removed active release context")
			}
			if kept.ReuseDisabled != (mode == "source-changed" || mode == "source-cleared") {
				t.Fatal("audio-only or unchanged source preference invalidated an active selection")
			}
			w = httptest.NewRecorder()
			h.ReleaseReportOptions(w, reportRequest(t, "GET", entry.ID, nil, false))
			if w.Code != 200 {
				t.Fatalf("active source report options disappeared: %d %s", w.Code, w.Body.String())
			}
			w = httptest.NewRecorder()
			h.RetryRelease(w, reportRequest(t, "POST", entry.ID, releaseReportRequest{SourceVersion: sourceReportVersion(kept), SkipHLS: true}, false))
			if w.Code != 200 {
				t.Fatalf("retry original release stopped working: %d %s", w.Code, w.Body.String())
			}
			var response struct {
				Replacement playback.PrequeueResponse `json:"replacement"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			replacement := waitReportedReplacement(t, h, response.Replacement.PrequeueID)
			if replacement.Status != playback.PrequeueStatusReady {
				t.Fatalf("replacement failed: %s", replacement.Error)
			}
			// Reopening the report dialog obtains its current source version;
			// the retry may have enriched the original release metadata.
			w = httptest.NewRecorder()
			h.ReleaseReportOptions(w, reportRequest(t, "GET", entry.ID, nil, false))
			if w.Code != 200 {
				t.Fatalf("report options after retry: %d", w.Code)
			}
			var options struct {
				SourceVersion string `json:"sourceVersion"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &options); err != nil {
				t.Fatal(err)
			}
			w = httptest.NewRecorder()
			h.ReportRelease(w, reportRequest(t, "POST", entry.ID, releaseReportRequest{SourceVersion: options.SourceVersion, Reason: "audio_problem", SkipHLS: true}, false))
			if w.Code != 200 {
				t.Fatalf("report original release stopped working: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestContentLocalPreferenceReplayKeepsEffectivePreparationScope(t *testing.T) {
	writer, prefs := contentLocalHandler(t)
	h, _, _ := newFallbackWorker(t, &fallbackSearch{})
	h.users = fallbackProfiles{models.User{ID: "viewer", AccountID: "account"}}
	h.SetContentPreferencesService(prefs)
	store := playback.NewPrequeueStore(time.Hour)
	writer.SetPrequeueStore(store)
	title := "tmdb:movie:123"
	scope := func() string { return h.prequeueSettingsScopeKey("viewer", "", title) }
	put := func(flag string) {
		t.Helper()
		w := httptest.NewRecorder()
		writer.SetLocalSourcePreference(w, contentLocalRequest("PUT", "viewer", `{"contentId":"`+title+`","contentType":"movie","preferLocalFiles":`+flag+`}`))
		if w.Code != http.StatusOK {
			t.Fatalf("source-only write failed: %d %s", w.Code, w.Body.String())
		}
	}
	inherited := scope()
	put("null")
	if scope() != inherited {
		t.Fatal("saving inheritance without a prior preference changed effective scope")
	}
	put("true")
	preferred := scope()
	if preferred == inherited {
		t.Fatal("changing effective source ordering did not change scope")
	}
	ready, _ := store.CreateScoped(title, "Synthetic title", "viewer", "movie", 2026, nil, "prewarm", preferred)
	store.Update(ready.ID, func(e *playback.PrequeueEntry) {
		e.Status = playback.PrequeueStatusReady
		e.StreamPath = "/webdav/synthetic.mkv"
	})
	generation := store.Generation()
	put("true")
	if scope() != preferred || store.Generation() != generation {
		t.Fatal("replaying unchanged source preference invalidated preparation")
	}
	if entry, ok := store.GetByTitleUserScope(title, "viewer", scope()); !ok || entry.ID != ready.ID {
		t.Fatal("replayed preference lost the existing reusable preparation")
	}
	w := httptest.NewRecorder()
	writer.SetPreference(w, contentLocalRequest("PUT", "viewer", `{"contentId":"`+title+`","audioLanguage":"fra"}`))
	if w.Code != http.StatusOK || scope() == preferred {
		t.Fatal("effective track preference change failed to rotate source scope")
	}
}
