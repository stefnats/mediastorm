package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"novastream/api/openapi"
	"novastream/handlers"
)

// These tests use the consumed Main registration helper, real Cookie auth and
// synthetic PostgreSQL. They do not launch Main or assert browser acceptance.
type mainProfileCreationReadResult struct {
	Outcome                    string                         `json:"outcome"`
	Profile                    *handlers.ProfileWithPinStatus `json:"profile"`
	UsersWriteBlocked          bool                           `json:"usersWriteBlocked"`
	SettingsPendingUncertainty bool                           `json:"settingsPendingUncertainty"`
}

func mainProfileCreationReadPath(kind int, locator string) string {
	prefix := "/admin/api"
	if kind == 1 {
		prefix = "/account/api"
	}
	return prefix + "/profiles/creation-operations/" + locator + "/read"
}

func mainProfileCreationRead(f *mainKidsFlagFixture, kind int, locator string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, f.base.cookieRequest(http.MethodPost, mainProfileCreationReadPath(kind, locator), "", ""))
	return w
}

func mainProfileCreationDecodeRead(t *testing.T, w *httptest.ResponseRecorder, status int, outcome string) mainProfileCreationReadResult {
	t.Helper()
	var value mainProfileCreationReadResult
	if w.Code != status || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.Outcome != outcome || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("unexpected qualified creation read result", w.Code)
	}
	for _, secret := range []string{"synthetic-web-parent", "password_hash", "pin_hash", "reference_hash", "marker_id", "session_token"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("creation result exposed private storage or credentials")
		}
	}
	return value
}

func TestProfileCreationResultsMainAcknowledgesLocatorBeforePasswordAttempt(t *testing.T) {
	for _, master := range []bool{false, true} {
		for _, kind := range []int{0, 1} {
			t.Run(fmt.Sprintf("master=%t/alias=%d", master, kind), func(t *testing.T) {
				f, verifier := mainProfileCreationFixture(t, master, true)
				before := f.physical(t)
				body := `{"name":"Result child","accountId":"actor","isKidsProfile":true}`
				w := mainProfileCreationCall(f, kind, body, mainProfileCreationBundle{}, "synthetic-web-parent")
				var bundle mainProfileCreationBundle
				if w.Code != http.StatusForbidden || json.Unmarshal(w.Body.Bytes(), &bundle) != nil || !strings.HasPrefix(bundle.ParentIntent, "pcc_") || len(bundle.ParentIntent) != 68 || !strings.HasPrefix(bundle.CreationOperation, "pcr_") || len(bundle.CreationOperation) != 68 || !bytes.Equal(before, f.physical(t)) || verifier.calls.Load() != 0 {
					t.Fatal("first request with password attempted creation before acknowledging locator", w.Code)
				}
				pending := mainProfileCreationDecodeRead(t, mainProfileCreationRead(f, 1-kind, bundle.CreationOperation), http.StatusOK, "pending")
				if pending.Profile != nil || !bytes.Equal(before, f.physical(t)) {
					t.Fatal("pending read created or adopted a profile")
				}
				w = mainProfileCreationCall(f, kind, body, bundle, "synthetic-web-parent")
				var profile handlers.ProfileWithPinStatus
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &profile) != nil || profile.ID == "" || verifier.calls.Load() != 1 {
					t.Fatal("explicit acknowledged creation failed", w.Code)
				}
				var response map[string]json.RawMessage
				if json.Unmarshal(w.Body.Bytes(), &response) != nil {
					t.Fatal("creation response is not JSON")
				}
				var echoed string
				if json.Unmarshal(response["creationOperation"], &echoed) != nil || echoed != bundle.CreationOperation {
					t.Fatal("known creation response lost original operation locator")
				}
				committed := f.physical(t)
				for _, readAlias := range []int{0, 1} {
					result := mainProfileCreationDecodeRead(t, mainProfileCreationRead(f, readAlias, bundle.CreationOperation), http.StatusOK, "created")
					if result.Profile == nil || result.Profile.ID != profile.ID || result.Profile.AccountID != "actor" || !result.Profile.IsKidsProfile || result.UsersWriteBlocked || result.SettingsPendingUncertainty || !bytes.Equal(committed, f.physical(t)) {
						t.Fatal("fresh read did not preserve the current committed profile without writes")
					}
				}
			})
		}
	}
}

func TestProfileCreationResultsMainIncompleteRetryNeverDowngrades(t *testing.T) {
	for _, master := range []bool{false, true} {
		for _, kind := range []int{0, 1} {
			t.Run(fmt.Sprintf("master=%t/alias=%d", master, kind), func(t *testing.T) {
				f, _ := mainProfileCreationFixture(t, master, false)
				original := `{"name":"Header child","accountId":"actor","isKidsProfile":true}`
				bundle := mainProfileCreationChallenge(t, f, kind, original)
				for _, mode := range []string{"pcr-only-true", "pcr-only-false", "pcr-only-missing", "pcc-only", "empty-pcr", "empty-pcc", "duplicate-pcr", "duplicate-pcc", "wrong-pcr", "cross-pcr"} {
					t.Run(mode, func(t *testing.T) {
						selected := bundle
						if mode == "cross-pcr" {
							selected = mainProfileCreationChallenge(t, f, kind, original)
							selected.CreationOperation = bundle.CreationOperation
						}
						body := original
						if mode == "pcr-only-false" {
							body = strings.Replace(original, `"isKidsProfile":true`, `"isKidsProfile":false`, 1)
						}
						if mode == "pcr-only-missing" {
							body = `{"name":"Header adult","accountId":"actor"}`
						}
						r := mainProfileCreationRequest(f, kind, body, selected, "synthetic-web-parent")
						switch mode {
						case "pcr-only-true", "pcr-only-false", "pcr-only-missing":
							r.Header.Del("X-Parent-Intent")
						case "pcc-only":
							r.Header.Del("X-Profile-Creation-Operation")
						case "empty-pcr":
							r.Header.Set("X-Profile-Creation-Operation", "")
						case "empty-pcc":
							r.Header.Set("X-Parent-Intent", "")
						case "duplicate-pcr":
							r.Header.Add("X-Profile-Creation-Operation", selected.CreationOperation)
						case "duplicate-pcc":
							r.Header.Add("X-Parent-Intent", selected.ParentIntent)
						case "wrong-pcr":
							r.Header.Set("X-Profile-Creation-Operation", "pcr_"+strings.Repeat("0", 64))
						}
						before := f.physical(t)
						var countBefore, countAfter int
						if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&countBefore); err != nil {
							t.Fatal(err)
						}
						w := httptest.NewRecorder()
						f.router.ServeHTTP(w, r)
						if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&countAfter); err != nil {
							t.Fatal(err)
						}
						if w.Code != http.StatusConflict || !bytes.Equal(before, f.physical(t)) || countBefore != countAfter {
							t.Fatal("malformed retry mutated a profile or allocated a replacement challenge", w.Code)
						}
					})
				}
			})
		}
	}
}

func TestProfileCreationResultsMainReadAuthenticatesAndRejectsWriteInputs(t *testing.T) {
	for _, kind := range []int{0, 1} {
		t.Run(fmt.Sprintf("alias=%d", kind), func(t *testing.T) {
			f, _ := mainProfileCreationFixture(t, false, false)
			bundle := mainProfileCreationChallenge(t, f, kind, `{"name":"Read child","accountId":"actor","isKidsProfile":true}`)
			before := f.physical(t)
			for _, mode := range []string{"body", "password", "intent", "locator-header", "owner-query", "target-query", "action-query", "foreign-origin", "no-cookie", "get", "public-api"} {
				t.Run(mode, func(t *testing.T) {
					path := mainProfileCreationReadPath(kind, bundle.CreationOperation)
					body := ""
					if mode == "body" {
						body = `{}`
					}
					if mode == "owner-query" {
						path += "?accountId=actor"
					}
					if mode == "target-query" {
						path += "?profileId=" + f.base.profile.ID
					}
					if mode == "action-query" {
						path += "?action=create"
					}
					if mode == "public-api" {
						path = "/api/profiles/creation-operations/" + bundle.CreationOperation + "/read"
					}
					r := f.base.cookieRequest(http.MethodPost, path, body, "")
					switch mode {
					case "password":
						r.Header.Set("X-Account-Password", "synthetic-web-parent")
					case "intent":
						r.Header.Set("X-Parent-Intent", bundle.ParentIntent)
					case "locator-header":
						r.Header.Set("X-Profile-Creation-Operation", bundle.CreationOperation)
					case "foreign-origin":
						r.Header.Set("Origin", "https://foreign.invalid")
					case "no-cookie":
						r.Header.Del("Cookie")
					case "get":
						r.Method = http.MethodGet
					}
					w := httptest.NewRecorder()
					f.router.ServeHTTP(w, r)
					expected := http.StatusBadRequest
					switch mode {
					case "foreign-origin":
						expected = http.StatusForbidden
					case "get":
						expected = http.StatusMethodNotAllowed
					case "public-api":
						expected = http.StatusNotFound
					case "no-cookie":
						if w.Code == http.StatusOK || !bytes.Equal(before, f.physical(t)) {
							t.Fatal("read omitted Cookie authentication or mutated profiles", w.Code)
						}
						return
					}
					if w.Code != expected || !bytes.Equal(before, f.physical(t)) {
						t.Fatal("read accepted write input or skipped actual routing guard", w.Code)
					}
					if mode == "foreign-origin" {
						validator, err := openapi.NewValidator(t.Context())
						if err != nil {
							t.Fatal("load actual origin-denial contract", err)
						}
						if err = validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
							t.Fatal("actual Main origin denial outside documented contract", err)
						}
					}
				})
			}
			if _, err := f.base.store.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=now()-interval '1 second' WHERE token=$1`, f.base.session.Token); err != nil {
				t.Fatal(err)
			}
			w := mainProfileCreationRead(f, kind, bundle.CreationOperation)
			if w.Code == http.StatusOK || !bytes.Equal(before, f.physical(t)) {
				t.Fatal("expired Cookie authorized operation read", w.Code)
			}
		})
	}
}

func TestProfileCreationResultsMainMissingLocatorIsUniformNonProof(t *testing.T) {
	f, _ := mainProfileCreationFixture(t, false, false)
	before := f.physical(t)
	for _, kind := range []int{0, 1} {
		result := mainProfileCreationDecodeRead(t, mainProfileCreationRead(f, kind, "pcr_"+strings.Repeat("0", 64)), http.StatusNotFound, "unqualified_or_missing")
		if result.Profile != nil || !result.UsersWriteBlocked || !result.SettingsPendingUncertainty || !bytes.Equal(before, f.physical(t)) {
			t.Fatal("missing locator became success, absence or cache authority")
		}
	}
}

func TestProfileCreationResultsMainReadAdoptsCurrentSameBirthWithoutReplayingCreate(t *testing.T) {
	f, _ := mainProfileCreationFixture(t, false, false)
	body := `{"name":"Initial child","accountId":"actor","isKidsProfile":true}`
	bundle := mainProfileCreationChallenge(t, f, 0, body)
	w := mainProfileCreationCall(f, 0, body, bundle, "synthetic-web-parent")
	var created handlers.ProfileWithPinStatus
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &created) != nil || created.ID == "" {
		t.Fatal("synthetic qualified creation failed", w.Code)
	}
	// A subsequent legitimate edit is the current same-birth state. Recovery
	// must not reinstall the original operation's Create projection over it.
	if _, err := f.base.store.Pool().Exec(t.Context(), `UPDATE users SET name='Current child',color='blue' WHERE id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.base.store.Pool().Exec(t.Context(), `UPDATE user_settings SET settings=jsonb_set(jsonb_set(settings,'{display,simpleMode}','false'),'{future}', '{"precise":9007199254740993}'::jsonb) WHERE user_id=$1`, created.ID); err != nil {
		t.Fatal(err)
	}
	before := f.physical(t)
	result := mainProfileCreationDecodeRead(t, mainProfileCreationRead(f, 1, bundle.CreationOperation), http.StatusOK, "created")
	cached, ok := f.base.users.Get(created.ID)
	settings, err := f.settings.Get(created.ID)
	if result.Profile == nil || result.Profile.ID != created.ID || result.Profile.Name != "Current child" || result.Profile.Color != "blue" || !ok || cached.Name != "Current child" || settings == nil || err != nil || settings.Display.SimpleMode == nil || *settings.Display.SimpleMode || !bytes.Equal(before, f.physical(t)) {
		t.Fatal("operation read restored stale initial values or wrote during current-state adoption")
	}
}
