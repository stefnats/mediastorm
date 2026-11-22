package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestLocalGroupsStoredMediaCapabilityAndRequestLocalBasis(t *testing.T) {
	service, _ := newLocalGroupsFixture()
	handler := NewLocalMediaHandler(service, nil, false)
	for _, tc := range []struct{ query, basis string }{
		{"include=cards", ""},
		{"include=cards&audioLanguage=fr&mediaUnknown=exclude", "stored_library_versions"},
		{"include=cards&mediaUnknown=only", ""}, // Missing-fact policy alone is not a criterion.
		{"include=cards&hdr=hdr10&minHeight=2160", "stored_library_versions"},
		{"include=cards", ""},
	} {
		result := localGroupsRequest(t, handler, "local", tc.query)
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
