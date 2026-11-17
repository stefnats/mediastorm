package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/services/profile_recovery"
	"novastream/services/user_settings"
)

func recoveryHandlerFixture(t *testing.T) (*publicPinCarrierFixture, *ProfileRecoveryHandler) {
	t.Helper()
	f := newPublicPinCarrierFixture(t)
	settings, e := user_settings.NewServiceWithStore(f.store)
	if e != nil {
		t.Fatal(e)
	}
	return f, NewProfileRecoveryHandler(profile_recovery.NewService(f.store, f.users, settings, f.pins), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} })
}
func TestProfileRecoveryHandlerStrictBodyAndSafeDTO(t *testing.T) {
	for _, kind := range []string{"valid", "body", "intent", "password", "query"} {
		t.Run(kind, func(t *testing.T) {
			f, h := recoveryHandlerFixture(t)
			r, release := f.request(t, "POST", "/api/users/"+f.profile.ID+"/state/reload")
			defer release()
			r = mux.SetURLVars(r, map[string]string{"userID": f.profile.ID})
			switch kind {
			case "body":
				r.Body = http.NoBody
				r = httptest.NewRequest("POST", r.URL.String(), strings.NewReader("{}")).WithContext(r.Context())
				r = mux.SetURLVars(r, map[string]string{"userID": f.profile.ID})
			case "intent":
				r.Header.Set("X-Parent-Intent", "pmi_"+strings.Repeat("a", 64))
			case "password":
				r.Header.Set("X-Account-Password", "synthetic forbidden transport")
			case "query":
				r.URL.RawQuery = "profileId=other"
			}
			w := httptest.NewRecorder()
			h.API(w, r)
			if kind != "valid" {
				if w.Code != 400 {
					t.Fatal("mutation payload accepted", kind, w.Code)
				}
				return
			}
			if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("valid reload failed", w.Code, w.Body.String())
			}
			var body map[string]json.RawMessage
			if json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body) != 2 {
				t.Fatal("invalid response envelope")
			}
			var profile map[string]json.RawMessage
			if json.Unmarshal(body["profile"], &profile) != nil || len(profile) != 8 {
				t.Fatal("unexpected profile fields exposed")
			}
			for _, key := range []string{"id", "name", "isKidsProfile", "kidsMode", "kidsMaxRating", "kidsMaxMovieRating", "kidsMaxTVRating", "kidsAllowedLists"} {
				if _, ok := profile[key]; !ok {
					t.Fatal("required DTO field missing", key)
				}
			}
			var list []string
			if json.Unmarshal(profile["kidsAllowedLists"], &list) != nil || list == nil {
				t.Fatal("safe list not an array")
			}
		})
	}
}
func TestProfileRecoveryHandlerUnavailableHasNoLegacySuccess(t *testing.T) {
	h := NewProfileRecoveryHandler(nil, nil)
	for _, method := range []http.HandlerFunc{h.API, h.Cookie} {
		w := httptest.NewRecorder()
		method(w, httptest.NewRequest("POST", "/api/users/a/state/reload", nil))
		if w.Code != 503 || !strings.Contains(w.Body.String(), "profile_recovery_unavailable") {
			t.Fatal("missing PG recovered through a legacy path")
		}
	}
}
