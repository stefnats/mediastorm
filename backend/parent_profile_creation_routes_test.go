package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"novastream/handlers"
	"novastream/models"
	"novastream/services/accounts"
)

// The existing synthetic fixture supplies real PostgreSQL authentication and
// the actual shared UsersHandler. Both route helpers are consumed by Main;
// these tests do not launch Main or claim a complete browser creation flow.
func mainProfileCreationFixture(t *testing.T, master, budget bool) (*mainKidsFlagFixture, *mainKidsFlagBudgetAccounts) {
	t.Helper()
	f, verifier := newMainKidsFlagFixture(t, master, budget)
	actual, err := accounts.NewServiceWithStore(f.base.store)
	if err != nil {
		t.Fatal("create synthetic account facade", err)
	}
	admin := handlers.NewAdminUIHandler("", "", nil, f.base.users, f.settings, nil)
	admin.SetSessionsService(f.base.sessions)
	admin.SetAccountsService(actual)
	admin.SetPGProfilePins(f.base.pins)
	wireParentKidsFlagHandler(admin, f.shared)
	registerParentProfileCreationCookieRoutes(f.router, admin, f.shared, f.settings)
	return f, verifier
}

func mainProfileCreationPath(kind int) string {
	if kind == 0 {
		return "/admin/api/profiles"
	}
	return "/account/api/profiles"
}

type mainProfileCreationBundle struct {
	ParentIntent      string `json:"parentIntent"`
	CreationOperation string `json:"creationOperation"`
}

func mainProfileCreationRequest(f *mainKidsFlagFixture, kind int, body string, intent mainProfileCreationBundle, password string) *http.Request {
	r := f.base.cookieRequest(http.MethodPost, mainProfileCreationPath(kind), body, "application/json")
	if intent.ParentIntent != "" {
		r.Header.Set("X-Parent-Intent", intent.ParentIntent)
	}
	if intent.CreationOperation != "" {
		r.Header.Set("X-Profile-Creation-Operation", intent.CreationOperation)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}

func mainProfileCreationCall(f *mainKidsFlagFixture, kind int, body string, intent mainProfileCreationBundle, password string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, mainProfileCreationRequest(f, kind, body, intent, password))
	return w
}

func mainProfileCreationChallenge(t *testing.T, f *mainKidsFlagFixture, kind int, body string) mainProfileCreationBundle {
	t.Helper()
	before := f.physical(t)
	w := mainProfileCreationCall(f, kind, body, mainProfileCreationBundle{}, "")
	var result struct {
		Code string `json:"code"`
		mainProfileCreationBundle
	}
	if w.Code != http.StatusForbidden || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Code != "parent_authorization_required" || !strings.HasPrefix(result.ParentIntent, "pcc_") || len(result.ParentIntent) != 68 || !strings.HasPrefix(result.CreationOperation, "pcr_") || len(result.CreationOperation) != 68 {
		t.Fatal("actual Main creation route did not issue a private creation challenge", w.Code)
	}
	if !bytes.Equal(before, f.physical(t)) {
		t.Fatal("challenge changed User, Settings or durable Settings revisions")
	}
	return result.mainProfileCreationBundle
}

func TestParentProfileCreationMainActualAliasesAndAtomicProjection(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f, _ := mainProfileCreationFixture(t, master, false)
			seen := map[string]bool{f.base.profile.ID: true}
			for _, kind := range []int{0, 1} {
				// Empty color is valid. Existing password verification trims the
				// supplied password; neither behavior becomes a new policy here.
				body := `{"name":"  Main child  ","color":"   ","accountId":"actor","isKidsProfile":true}`
				intent := mainProfileCreationChallenge(t, f, kind, body)
				w := mainProfileCreationCall(f, kind, body, intent, "  synthetic-web-parent  ")
				var result handlers.ProfileWithPinStatus
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.ID == "" || seen[result.ID] {
					t.Fatal("actual Main creation alias did not return one new profile", w.Code)
				}
				seen[result.ID] = true
				if result.AccountID != "actor" || result.Name != "Main child" || result.Color != "" || !result.IsKidsProfile || result.KidsMode != "rating" || result.KidsMaxRating != "G" || result.HasPin || result.HasIcon || result.AllowShareLinks || result.AllowLiveTakeover || result.AllowRecordingInterruption || result.ActivityPrivacy != models.ActivityPrivacySharedAnonymous {
					t.Fatal("known creation response lost safe profile facts")
				}
				stored, err := f.base.store.Users().Get(t.Context(), result.ID)
				cached, ok := f.base.users.Get(result.ID)
				settings, settingsErr := f.settings.Get(result.ID)
				if err != nil || !ok || settingsErr != nil || settings == nil || settings.Display.SimpleMode == nil || !*settings.Display.SimpleMode || stored.ID != cached.ID || stored.Name != cached.Name || !stored.IsKidsProfile || !cached.IsKidsProfile || stored.AllowShareLinks || stored.AllowLiveTakeover || stored.AllowRecordingInterruption || stored.ActivityPrivacy != models.ActivityPrivacySharedAnonymous {
					t.Fatal("known creation failed to publish the paired committed User and Settings")
				}
				var simple bool
				var generation int64
				if err = f.base.store.Pool().QueryRow(t.Context(), `SELECT (s.settings->'display'->>'simpleMode')::boolean,v.generation FROM user_settings s JOIN user_settings_versions v USING(user_id) WHERE s.user_id=$1`, result.ID).Scan(&simple, &generation); err != nil || !simple || generation <= 0 {
					t.Fatal("created profile defaults are not present in durable paired storage", err)
				}
				before := f.physical(t)
				if retry := mainProfileCreationCall(f, kind, body, intent, "synthetic-web-parent"); retry.Code != http.StatusConflict || !bytes.Equal(before, f.physical(t)) {
					t.Fatal("completed creation intent was replayed", retry.Code)
				}
			}
		})
	}
}

func TestParentProfileCreationMainOriginalCookieBodyAndAliasBinding(t *testing.T) {
	f, _ := mainProfileCreationFixture(t, false, false)
	body := `{"name":"Main child","accountId":"actor","isKidsProfile":true}`
	before := f.physical(t)
	r := mainProfileCreationRequest(f, 0, body, mainProfileCreationBundle{}, "")
	r.Header.Del("Cookie")
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	if w.Code == http.StatusOK || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("actual consumed route omitted Cookie authentication")
	}
	intent := mainProfileCreationChallenge(t, f, 0, body)
	if w = mainProfileCreationCall(f, 1, body, intent, "synthetic-web-parent"); w.Code != http.StatusConflict || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("creation intent crossed original cookie alias", w.Code)
	}
	intent = mainProfileCreationChallenge(t, f, 0, body)
	changed := strings.Replace(body, `"isKidsProfile":true`, `"isKidsProfile":false`, 1)
	if w = mainProfileCreationCall(f, 0, changed, intent, "synthetic-web-parent"); w.Code != http.StatusConflict || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("present creation intent downgraded into the adult legacy path", w.Code)
	}
	flagBody := `{"isKidsProfile":true}`
	flagIntent := f.challenge(t, 2, flagBody)
	if w = mainProfileCreationCall(f, 0, body, mainProfileCreationBundle{ParentIntent: flagIntent, CreationOperation: intent.CreationOperation}, "synthetic-web-parent"); w.Code != http.StatusConflict || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("flag challenge became a creation capability", w.Code)
	}
	intent = mainProfileCreationChallenge(t, f, 0, body)
	if w = f.call(2, flagBody, intent.ParentIntent, "synthetic-web-parent"); w.Code != http.StatusConflict || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("creation challenge became an existing flag capability", w.Code)
	}
}

func TestParentProfileCreationMainSharesExistingAPICredentialBudget(t *testing.T) {
	f, verifier := mainProfileCreationFixture(t, true, true)
	before := f.physical(t)
	flagBody := `{"isKidsProfile":true}`
	// Spend nine attempts through the existing API. This isolates the shared
	// credential bucket from the separate eight-active creation ledger quota.
	for i := 0; i < 9; i++ {
		intent := f.challenge(t, 2, flagBody)
		if w := f.call(2, flagBody, intent, "synthetic-wrong-password"); w.Code != http.StatusForbidden {
			t.Fatal("existing API did not consume its actual credential budget", w.Code)
		}
	}
	body := `{"name":"Main child","accountId":"actor","isKidsProfile":true}`
	intent := mainProfileCreationChallenge(t, f, 0, body)
	if w := mainProfileCreationCall(f, 0, body, intent, "synthetic-wrong-password"); w.Code != http.StatusForbidden {
		t.Fatal("creation did not share the tenth actual credential attempt", w.Code)
	}
	for _, kind := range []int{0, 1} {
		intent := mainProfileCreationChallenge(t, f, kind, body)
		if w := mainProfileCreationCall(f, kind, body, intent, "synthetic-web-parent"); w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" {
			t.Fatal("creation cookie alias acquired a second guessing allowance", w.Code)
		}
	}
	if verifier.calls.Load() != 10 || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("exhausted shared budget checked another password or changed profile storage")
	}
}

func TestParentProfileCreationMainAdultWithoutIntentKeepsLegacyPath(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprintf("master=%t", master), func(t *testing.T) {
			f, _ := mainProfileCreationFixture(t, master, false)
			var before int
			if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []int{0, 1} {
				body := `{"name":"Main adult","accountId":"actor","isKidsProfile":false}`
				w := mainProfileCreationCall(f, kind, body, mainProfileCreationBundle{}, "synthetic-web-parent")
				var result handlers.ProfileWithPinStatus
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.ID == "" || result.IsKidsProfile {
					t.Fatal("adult no-intent carrier changed its existing path", w.Code)
				}
			}
			var after int
			if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&after); err != nil || after != before {
				t.Fatal("adult legacy path entered the new creation ledger", err)
			}
		})
	}
}
