package main

import (
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/services/profile_recovery"
	"novastream/services/user_settings"
)

func TestProfileRecoveryMainCookieAliasesUseActualSharedHelper(t *testing.T) {
	for _, prefix := range []string{"/admin/api", "/account/api"} {
		t.Run(prefix, func(t *testing.T) {
			t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-profile-recovery")
			f := newPublicPinWebFixture(t)
			settings, e := user_settings.NewServiceWithStore(f.store)
			if e != nil {
				t.Fatal(e)
			}
			h := handlers.NewProfileRecoveryHandler(profile_recovery.NewService(f.store, f.users, settings, f.pins), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} })
			router := mux.NewRouter()
			registerProfileRecoveryCookieRoutes(router, f.admin(t), h)
			path := prefix + "/users/" + f.profile.ID + "/state/reload"
			r := f.cookieRequest("POST", path, "", "")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != 200 {
				t.Fatal("actual cookie alias failed", w.Code, w.Body.String())
			}
			r = f.cookieRequest("POST", path, "", "")
			r.Header.Set("Origin", "https://foreign.invalid")
			w = httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatal("actual cookie alias omitted Origin guard", w.Code)
			}
			r = f.cookieRequest("POST", path, "", "")
			r.Header.Del("Cookie")
			w = httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code == 200 {
				t.Fatal("actual cookie alias omitted authentication")
			}
		})
	}
}
