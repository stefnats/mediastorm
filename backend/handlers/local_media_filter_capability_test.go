package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
)

func TestLocalGroupsStoredMediaCapabilityAndRequestLocalBasis(t *testing.T) {
	service, _ := newLocalGroupsFixture()
	handler := NewLocalMediaHandler(service, nil, false)
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ query, basis string }{
		{"include=cards", ""},
		{"include=cards&audioLanguage=fr&mediaUnknown=exclude", "stored_library_versions"},
		{"include=cards&mediaUnknown=only", ""}, // Missing-fact policy alone is not a criterion.
		{"include=cards&hdr=hdr10&minHeight=2160", "stored_library_versions"},
		{"include=cards", ""},
	} {
		w := localLibraryLanguageRequest(handler, tc.query)
		request := httptest.NewRequest(http.MethodGet, "/api/library/libraries/local/groups?"+tc.query, nil)
		if err := validator.ValidateRequest(request, nil); err != nil {
			t.Fatal(err)
		}
		if err := validator.ValidateResponse(request, w.Code, w.Header(), w.Body.Bytes()); err != nil {
			t.Fatal(err)
		}
		result := localLibraryLanguageDecode(t, w)
		if !result.StoredMediaFilterSupported || result.StoredMediaFilterBasis != tc.basis {
			t.Fatalf("support/basis for %q: %+v", tc.query, result)
		}
		if service.groups.StoredMediaFilterSupported || service.groups.StoredMediaFilterBasis != "" {
			t.Fatal("response markers mutated the retained repository result")
		}
		if result.Total != service.groups.Total || len(result.Groups) != len(service.groups.Groups) {
			t.Fatal("capability metadata changed the reader's result or total")
		}
	}
}

func TestLocalGroupsStoredMediaCapabilityOnlyAfterSuccessfulRead(t *testing.T) {
	handler, stored, _ := localLibraryLanguageFixture(t)
	invalid := localLibraryLanguageRequest(handler, "audioLanguage=invalid!")
	if invalid.Code != http.StatusBadRequest || stored.reads != 0 || strings.Contains(invalid.Body.String(), "storedMediaFilterSupported") {
		t.Fatal("invalid filters advertised a successful stored reader")
	}
	service, _ := newLocalGroupsFixture()
	service.err = errors.New("synthetic stored read failed")
	for _, reader := range []localMediaService{nil, service} {
		h := NewLocalMediaHandler(reader, nil, false)
		r := httptest.NewRequest(http.MethodGet, "/api/library/libraries/local/groups?audioLanguage=fr", nil)
		r = mux.SetURLVars(r, map[string]string{"libraryID": "local"})
		w := httptest.NewRecorder()
		h.ListGroups(w, r)
		if w.Code < 400 || strings.Contains(w.Body.String(), "storedMediaFilterSupported") {
			t.Fatal("missing/failed reader advertised supported filtered output")
		}
	}
}
