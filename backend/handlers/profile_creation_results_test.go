package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"novastream/api/openapi"
	"novastream/services/profileunlock"
)

func creationResultsCookieFixture(t *testing.T, master bool) *kidsCookieFixture {
	t.Helper()
	f := creationCookieFixture(t, master)
	for _, prefix := range []string{"/admin", "/account"} {
		f.router.HandleFunc(prefix+"/api/profiles/creation-operations/{operationRef}/read", f.admin.RequireAuth(func(w http.ResponseWriter, r *http.Request) { f.shared.ReadProfileCreationManagement(w, r, f.settings) })).Methods(http.MethodPost)
	}
	return f
}
func creationResultsCookieRead(f *kidsCookieFixture, prefix, ref string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.test"+prefix+"/api/profiles/creation-operations/"+ref+"/read", nil)
	r.Header.Set("Origin", "http://synthetic.test")
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.base.session.Token})
	return r
}
func TestProfileCreationResultsCookieAcknowledgesLocatorBeforePasswordAttempt(t *testing.T) {
	for _, master := range []bool{false, true} {
		for _, prefix := range []string{"/admin", "/account"} {
			t.Run(fmt.Sprintf("%s/master=%t", prefix, master), func(t *testing.T) {
				f := creationResultsCookieFixture(t, master)
				body := `{"name":"Acknowledged child","accountId":"actor","isKidsProfile":true}`
				before := creationCookieRows(t, f)
				first := creationCookieCall(f, prefix, body, profileunlock.ParentProfileCreationChallengeBundle{}, "synthetic-parent-password")
				var bundle profileunlock.ParentProfileCreationChallengeBundle
				if first.Code != 403 || json.Unmarshal(first.Body.Bytes(), &bundle) != nil || !strings.HasPrefix(bundle.ParentIntent, "pcc_") || !strings.HasPrefix(bundle.CreationOperation, "pcr_") || !bytes.Equal(before, creationCookieRows(t, f)) {
					t.Fatal("first request attempted before locator handoff", first.Code)
				}
				read := httptest.NewRecorder()
				f.router.ServeHTTP(read, creationResultsCookieRead(f, prefix, bundle.CreationOperation))
				var pending profileCreationReadResponse
				if read.Code != 200 || json.Unmarshal(read.Body.Bytes(), &pending) != nil || pending.Outcome != "pending" || pending.Profile != nil || !bytes.Equal(before, creationCookieRows(t, f)) {
					t.Fatal("pending became created/absence", read.Code)
				}
				retry := creationCookieCall(f, prefix, body, bundle, "synthetic-parent-password")
				if retry.Code != 200 {
					t.Fatal("explicit original retry failed", retry.Code, retry.Body.String())
				}
				var result struct {
					ProfileWithPinStatus
					CreationOperation string
				}
				if json.Unmarshal(retry.Body.Bytes(), &result) != nil || result.ID == "" || result.CreationOperation != bundle.CreationOperation {
					t.Fatal("success lost original locator")
				}
				after := creationCookieRows(t, f)
				read = httptest.NewRecorder()
				f.router.ServeHTTP(read, creationResultsCookieRead(f, prefix, bundle.CreationOperation))
				var got profileCreationReadResponse
				if read.Code != 200 || json.Unmarshal(read.Body.Bytes(), &got) != nil || got.Outcome != "created" || got.Profile == nil || got.Profile.ID != result.ID || got.UsersWriteBlocked || got.SettingsPendingUncertainty || !bytes.Equal(after, creationCookieRows(t, f)) {
					t.Fatal("fresh current result not safely read", read.Code, read.Body.String())
				}
			})
		}
	}
}
func TestProfileCreationResultsCookieBothHeadersAlwaysEnterGuardedCarrier(t *testing.T) {
	for _, kind := range []string{"pcr_false", "pcr_missing", "pcc_only", "blank_pcr", "duplicate_pcr", "wrong_pcr", "cross_operation"} {
		t.Run(kind, func(t *testing.T) {
			f := creationResultsCookieFixture(t, true)
			body := `{"name":"Child","accountId":"actor","isKidsProfile":true}`
			bundle := creationCookieChallenge(t, f, "/admin", body)
			r := creationCookieRequest(f, "/admin", body, bundle, "synthetic-parent-password")
			switch kind {
			case "pcr_false":
				r.Header.Del("X-Parent-Intent")
				r.Body = io.NopCloser(strings.NewReader(`{"name":"Adult","accountId":"actor","isKidsProfile":false}`))
				r.ContentLength = -1
			case "pcr_missing":
				r.Header.Del("X-Parent-Intent")
				r.Body = io.NopCloser(strings.NewReader(`{"name":"Adult","accountId":"actor"}`))
				r.ContentLength = -1
			case "pcc_only":
				r.Header.Del("X-Profile-Creation-Operation")
			case "blank_pcr":
				r.Header.Set("X-Profile-Creation-Operation", "")
			case "duplicate_pcr":
				r.Header.Add("X-Profile-Creation-Operation", bundle.CreationOperation)
			case "wrong_pcr":
				r.Header.Set("X-Profile-Creation-Operation", "pcr_"+strings.Repeat("0", 64))
			case "cross_operation":
				other := creationCookieChallenge(t, f, "/admin", body)
				r.Header.Set("X-Profile-Creation-Operation", other.CreationOperation)
			}
			before := creationCookieRows(t, f)
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, r)
			if w.Code != 409 || !bytes.Equal(before, creationCookieRows(t, f)) {
				t.Fatal("malformed retry fell back or mutated", kind, w.Code)
			}
		})
	}
}
func TestProfileCreationResultsCookieReadRejectsWriteInputs(t *testing.T) {
	for _, kind := range []string{"password", "pcc", "pcr", "body", "query"} {
		t.Run(kind, func(t *testing.T) {
			f := creationResultsCookieFixture(t, false)
			bundle := creationCookieChallenge(t, f, "/account", `{"name":"Child","isKidsProfile":true}`)
			r := creationResultsCookieRead(f, "/account", bundle.CreationOperation)
			switch kind {
			case "password":
				r.Header.Set("X-Account-Password", "")
			case "pcc":
				r.Header.Set("X-Parent-Intent", bundle.ParentIntent)
			case "pcr":
				r.Header.Set("X-Profile-Creation-Operation", bundle.CreationOperation)
			case "body":
				r.Body = io.NopCloser(strings.NewReader(`{"accountId":"actor"}`))
				r.ContentLength = -1
			case "query":
				r.URL.RawQuery = "target=other"
			}
			before := creationCookieRows(t, f)
			w := httptest.NewRecorder()
			f.router.ServeHTTP(w, r)
			if w.Code != 400 || !bytes.Equal(before, creationCookieRows(t, f)) {
				t.Fatal("read accepted write inputs", kind, w.Code)
			}
		})
	}
}
func TestProfileCreationResultsCookieUniformMissingAndSafeDTO(t *testing.T) {
	f := creationResultsCookieFixture(t, false)
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, creationResultsCookieRead(f, "/account", "pcr_"+strings.Repeat("0", 64)))
	var missing profileCreationReadResponse
	if w.Code != 404 || json.Unmarshal(w.Body.Bytes(), &missing) != nil || missing.Outcome != "unqualified_or_missing" || missing.Profile != nil {
		t.Fatal("missing locator leaked evidence", w.Code)
	}
	body := `{"name":"Private child","isKidsProfile":true}`
	bundle := creationCookieChallenge(t, f, "/account", body)
	if w = creationCookieCall(f, "/account", body, bundle, "synthetic-parent-password"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	f.router.ServeHTTP(w, creationResultsCookieRead(f, "/admin", bundle.CreationOperation))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, secret := range []string{"password", "pinHash", "pin_hash", "reference_hash", "result_version", "SYNTHETIC_", "parentIntent"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("unsafe result DTO", secret)
		}
	}
}

// Deterministic contract coverage uses the real closed/unavailable handler
// without a PG opt-in. It does not manufacture successful PG authority, and it
// is separate from the actual Cookie/PG successful handshake below.
func profileCreationResultsOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	handler := NewUsersHandler(nil)
	locator := "pcr_" + strings.Repeat("1", 64)
	for _, prefix := range []string{"/admin", "/account"} {
		create := contractCall{method: http.MethodPost, target: prefix + "/api/profiles", body: map[string]any{"name": "Synthetic unavailable contract child", "isKidsProfile": true}}
		response := exercise(t, v, create, func(w http.ResponseWriter, r *http.Request) { handler.CreateProfileManagement(w, r, nil) }, 503)
		if got := decodeContract[struct {
			Code string `json:"code"`
		}](t, response); got.Code != "parent_confirmation_unavailable" {
			t.Fatal("closed Create contract did not remain unavailable")
		}
		read := contractCall{method: http.MethodPost, target: prefix + "/api/profiles/creation-operations/" + locator + "/read", vars: map[string]string{"operationRef": locator}}
		response = exercise(t, v, read, func(w http.ResponseWriter, r *http.Request) { handler.ReadProfileCreationManagement(w, r, nil) }, 503)
		if got := decodeContract[struct {
			Code string `json:"code"`
		}](t, response); got.Code != "parent_confirmation_unavailable" {
			t.Fatal("closed Read contract invented a result")
		}
	}
}
func TestProfileCreationResultsOpenAPIUnavailableContract(t *testing.T) {
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	profileCreationResultsOpenAPIContract(t, validator)
	for _, id := range []string{"createAdminProfileWithAcknowledgedOperation", "readAdminProfileCreationResult", "createAccountProfileWithAcknowledgedOperation", "readAccountProfileCreationResult"} {
		if !validator.Covered()[id] {
			t.Fatal("unavailable contract operation missing", id)
		}
	}
}

func creationResultsValidatedCookie(t *testing.T, validator *openapi.Validator, f *kidsCookieFixture, r *http.Request, raw []byte, status int) *httptest.ResponseRecorder {
	t.Helper()
	if err := validator.ValidateRequest(r, raw); err != nil {
		t.Fatal("actual Cookie request outside contract", err)
	}
	response := httptest.NewRecorder()
	f.router.ServeHTTP(response, r)
	if response.Code != status {
		t.Fatal("actual Cookie contract status", response.Code, status, response.Body.String())
	}
	if err := validator.ValidateResponse(r, response.Code, response.Header(), response.Body.Bytes()); err != nil {
		t.Fatal("actual Cookie response outside contract", err)
	}
	return response
}
func TestProfileCreationResultsOpenAPIActualCookieReplies(t *testing.T) {
	for _, prefix := range []string{"/admin", "/account"} {
		t.Run(prefix, func(t *testing.T) {
			validator, err := openapi.NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			f := creationResultsCookieFixture(t, false)
			body := `{"name":"Synthetic actual contract child","isKidsProfile":true}`
			original := creationCookieRequest(f, prefix, body, profileunlock.ParentProfileCreationChallengeBundle{}, "synthetic-parent-password")
			first := creationResultsValidatedCookie(t, validator, f, original, []byte(body), 403)
			var bundle profileunlock.ParentProfileCreationChallengeBundle
			if err = json.Unmarshal(first.Body.Bytes(), &bundle); err != nil || !strings.HasPrefix(bundle.ParentIntent, "pcc_") || !strings.HasPrefix(bundle.CreationOperation, "pcr_") {
				t.Fatal("actual challenge schema omitted acknowledged pair", err)
			}
			pendingRequest := creationResultsCookieRead(f, prefix, bundle.CreationOperation)
			pending := creationResultsValidatedCookie(t, validator, f, pendingRequest, nil, 200)
			if got := decodeContract[profileCreationReadResponse](t, pending); got.Outcome != "pending" || got.Profile != nil {
				t.Fatal("actual pending contract fabricated profile")
			}
			retry := creationCookieRequest(f, prefix, body, bundle, "synthetic-parent-password")
			saved := creationResultsValidatedCookie(t, validator, f, retry, []byte(body), 200)
			var profile struct {
				ProfileWithPinStatus
				CreationOperation string `json:"creationOperation"`
			}
			if err = json.Unmarshal(saved.Body.Bytes(), &profile); err != nil || profile.ID == "" || profile.CreationOperation != bundle.CreationOperation {
				t.Fatal("actual saved contract lost original locator", err)
			}
			readRequest := creationResultsCookieRead(f, prefix, bundle.CreationOperation)
			current := creationResultsValidatedCookie(t, validator, f, readRequest, nil, 200)
			if got := decodeContract[profileCreationReadResponse](t, current); got.Outcome != "created" || got.Profile == nil || got.Profile.ID != profile.ID || got.UsersWriteBlocked || got.SettingsPendingUncertainty {
				t.Fatal("actual successful read contract mismatch")
			}
			// The response schema must reject credentials even if a future serializer
			// accidentally appends them; this mutation is only in test-owned JSON bytes.
			var unsafe map[string]json.RawMessage
			if err = json.Unmarshal(current.Body.Bytes(), &unsafe); err != nil {
				t.Fatal(err)
			}
			var unsafeProfile map[string]json.RawMessage
			if err = json.Unmarshal(unsafe["profile"], &unsafeProfile); err != nil {
				t.Fatal(err)
			}
			unsafeProfile["pinHash"] = json.RawMessage(`"synthetic-disallowed-hash"`)
			unsafe["profile"], err = json.Marshal(unsafeProfile)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(unsafe)
			if err != nil {
				t.Fatal(err)
			}
			if err = validator.ValidateResponse(readRequest, 200, current.Header(), raw); err == nil {
				t.Fatal("safe profile contract accepted an undocumented credential field")
			}
			missingRequest := creationResultsCookieRead(f, prefix, "pcr_"+strings.Repeat("0", 64))
			missing := creationResultsValidatedCookie(t, validator, f, missingRequest, nil, 404)
			if got := decodeContract[profileCreationReadResponse](t, missing); got.Outcome != "unqualified_or_missing" || got.Profile != nil {
				t.Fatal("missing result contract claimed evidence")
			}
		})
	}
}
