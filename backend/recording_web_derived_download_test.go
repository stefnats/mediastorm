package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/recordings"
)

// No process or media is opened. This port counts only requests that pass the actual
// current PG recording/PIN authority and rechecks its original captured callback.
type derivedPGDownloadService struct {
	*recordings.Service
	mediaCalls int
}

func (s *derivedPGDownloadService) OpenDerived(ctx context.Context, id, derivedID string, authorize func() error) (*recordings.DerivedReader, error) {
	s.mediaCalls++
	if err := authorize(); err != nil {
		return nil, err
	}
	return nil, recordings.ErrDerivedUnavailable
}

func TestRecordingDerivedDownloadWebOriginalPostgresPINFrame(t *testing.T) {
	for _, mode := range []string{"positive", "foreign origin", "missing PIN", "query token", "changed PIN", "changed owner"} {
		t.Run(mode, func(t *testing.T) {
			f := newPublicPinWebFixture(t)
			if _, err := f.users.SetPin(f.profile.ID, "1357"); err != nil {
				t.Fatal(err)
			}
			users := handlers.NewUsersHandler(f.users)
			users.SetPGProfilePins(f.pins, func() time.Duration { return time.Hour })
			repo := &recordingWebRepo{jobs: map[string]models.Recording{
				"synthetic": {ID: "synthetic", UserID: f.profile.ID, CreatedAt: time.Now().UTC()},
			}}
			service := &derivedPGDownloadService{Service: recordings.NewService(repo, "must-not-run", t.TempDir())}
			handler := handlers.NewRecordingsHandler(service, f.users)
			access := handlers.NewRecordingStreamAccess(service.Service, f.sessions, nil, f.users, nil,
				func() (config.SecuritySettings, error) { return config.SecuritySettings{EnforceProfilePins: true}, nil })
			// Match Main: current PG snapshots replace the legacy account/unlock cache path.
			access.SetPGProfilePins(f.pins)
			handler.SetRecordingStreamAccess(access)
			calls, originalStatus := 0, 0
			var originalBody, expectedUnlock string
			export := func(w http.ResponseWriter, r *http.Request) {
				calls++
				claim, ok := auth.GetSession(r)
				if !ok || claim.Token != f.session.Token || claim.Scope != "" || f.pins.CheckOriginal(r) != nil || r.Body != http.NoBody || r.Header.Get("X-Profile-Unlock") != expectedUnlock || r.URL.Query().Get("profileId") != f.profile.ID {
					t.Fatal("actual original PG cookie/PIN frame or internal body-token transport changed")
				}
				// Compare the actual original entry and download adapter under the same
				// configured authority, including locked PIN and unavailable-media errors.
				original := httptest.NewRecorder()
				handler.StreamDerived(original, r.Clone(r.Context()))
				originalStatus, originalBody = original.Code, original.Body.String()
				handler.DownloadDerived(w, r)
			}
			pin, download := newRecordingWebPOSTHandlers(f.sessions, nil, f.users, users.VerifyPin, export, f.pins)
			verify := f.cookieRequest(http.MethodPost, "/account/api/users/"+f.profile.ID+"/pin/verify", `{"pin":"1357"}`, "application/json")
			verify = mux.SetURLVars(verify, map[string]string{"userID": f.profile.ID})
			verified := httptest.NewRecorder()
			f.admin(t).RequireAuth(pin)(verified, verify)
			var result handlers.VerifyPinResponse
			if verified.Code != http.StatusOK || json.Unmarshal(verified.Body.Bytes(), &result) != nil || !result.Valid || result.UnlockToken == "" {
				t.Fatal("actual PG verification failed")
			}
			expectedUnlock = result.UnlockToken
			fields := url.Values{"profileId": {f.profile.ID}, "profileUnlock": {result.UnlockToken}}
			if mode == "missing PIN" {
				fields.Del("profileUnlock")
				expectedUnlock = ""
			}
			if mode == "changed PIN" {
				if _, err := f.users.SetPin(f.profile.ID, "9753"); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "changed owner" {
				if _, err := f.store.Pool().Exec(t.Context(), "INSERT INTO accounts(id,username,password_hash,created_at,updated_at) SELECT 'replacement','synthetic-replacement',password_hash,clock_timestamp(),clock_timestamp() FROM accounts WHERE id='actor'"); err != nil {
					t.Fatal("create own synthetic replacement account")
				}
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE users SET account_id='replacement',updated_at=clock_timestamp() WHERE id=$1", f.profile.ID); err != nil {
					t.Fatal("change own synthetic profile owner")
				}
			}
			r := f.cookieRequest(http.MethodPost, "/account/api/live/recordings/synthetic/derived/0123456789abcdef0123456789abcdef/stream", fields.Encode(), "application/x-www-form-urlencoded")
			r = mux.SetURLVars(r, map[string]string{"recordingID": "synthetic", "derivedID": "0123456789abcdef0123456789abcdef"})
			if mode == "foreign origin" {
				r.Header.Set("Origin", "https://foreign.invalid")
			}
			if mode == "query token" {
				r.URL.RawQuery = fields.Encode()
			}
			w := httptest.NewRecorder()
			recordingWebOrigin(f.admin(t).RequireAuth(download))(w, r)
			wantCalls, wantMedia := 0, 0
			if mode == "positive" || mode == "missing PIN" || mode == "changed PIN" {
				wantCalls = 1
			}
			if mode == "positive" {
				wantMedia = 2 // Original StreamDerived and DownloadDerived, no file/process activity.
				if w.Code != http.StatusConflict {
					t.Fatal("current verified PIN did not reach the unavailable derived-media port")
				}
			}
			if mode == "missing PIN" || mode == "changed PIN" {
				var denied struct {
					Code string `json:"code"`
				}
				if w.Code != http.StatusForbidden || json.Unmarshal(w.Body.Bytes(), &denied) != nil || denied.Code != "profile_locked" {
					t.Fatal("configured recording access did not enforce the actual current PG PIN")
				}
			}
			if calls != wantCalls || service.mediaCalls != wantMedia || w.Code < 400 || w.Code >= 500 || w.Header().Get("Content-Disposition") != "" {
				t.Fatalf("mode=%s status=%d callbacks=%d media=%d; download changed original authority", mode, w.Code, calls, service.mediaCalls)
			}
			if calls == 1 && (originalStatus != w.Code || originalBody != w.Body.String()) {
				t.Fatal("download adapter changed actual original stream error status or body")
			}
		})
	}
}

func TestRecordingDerivedDownloadCookieGETAndHEADRejectBeforeFallback(t *testing.T) {
	admin := handlers.NewAdminUIHandler("", "", nil, nil, nil, nil)
	recordingHandler := handlers.NewRecordingsHandler(nil, nil)
	router := mux.NewRouter()
	registerRecordingDerivedWebRoutes(router, admin, nil, nil, nil, nil, recordingHandler, nil)
	fallback := 0
	router.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallback++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("[]"))
	})
	for _, prefix := range []string{"/admin", "/account"} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			for _, query := range []string{"", "?profileId=synthetic&profileUnlock=synthetic-not-authority"} {
				r := httptest.NewRequest(method, "http://synthetic.test"+prefix+"/api/live/recordings/synthetic/derived/0123456789abcdef0123456789abcdef/stream"+query, nil)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost || w.Body.Len() != 0 || w.Header().Get("Content-Disposition") != "" || w.Header().Get("Location") != "" || w.Header().Get("Cache-Control") != "private, no-store" || fallback != 0 {
					t.Fatalf("cookie stream method=%s prefix=%s status=%d fallback=%d", method, prefix, w.Code, fallback)
				}
			}
		}
	}
}
