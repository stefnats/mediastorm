package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/importer"
)

type arrHandlerService struct {
	*fallbackAccessService
	actions []string
}

func (s *arrHandlerService) GetARRImportStatus(context.Context, int64) (importer.ARRImportStatus, error) {
	s.actions = append(s.actions, "status")
	return importer.ARRImportStatus{QueueID: 1, State: "not_started", ItemIDs: []string{}}, nil
}
func (s *arrHandlerService) SubmitARRImport(ctx context.Context, id int64, backend string, retry bool, _ importer.SABImportPreviewSettings, _ importer.ARRLibraryImporter, check func(context.Context) error) (importer.ARRImportStatus, error) {
	if e := check(ctx); e != nil {
		return importer.ARRImportStatus{}, e
	}
	s.actions = append(s.actions, "submit:"+backend)
	return importer.ARRImportStatus{QueueID: id, State: "importing", Backend: backend, CommandID: 7, ItemIDs: []string{}}, nil
}
func (s *arrHandlerService) RefreshARRImport(ctx context.Context, id int64, _ importer.SABImportPreviewSettings, _ importer.ARRLibraryImporter, check func(context.Context) error) (importer.ARRImportStatus, error) {
	if e := check(ctx); e != nil {
		return importer.ARRImportStatus{}, e
	}
	s.actions = append(s.actions, "refresh")
	return importer.ARRImportStatus{QueueID: id, State: "available", Backend: "radarr", ItemIDs: []string{"local-item"}}, nil
}
func TestARRImportHandlerActions(t *testing.T) {
	f := newFallbackAccessFixture(t)
	service := &arrHandlerService{fallbackAccessService: f.service}
	h := NewUsenetFallbackHandler(service, f.sessions, f.accounts)
	h.SetImportPreviewSettings(func(context.Context) (config.SABnzbdSettings, error) {
		return config.SABnzbdSettings{Radarr: config.ARRImportSettings{BaseURL: "http://synthetic.invalid", APIKey: "synthetic-key", LibraryID: "movies"}}, nil
	})
	for _, action := range []string{"status", "submit", "refresh"} {
		r := fallbackListAccessRequest(f, "/admin/api/usenet/fallback/1/arr", t.Context(), true)
		r = mux.SetURLVars(r, map[string]string{"queueID": "1"})
		r.Method = http.MethodGet
		if action == "submit" {
			r.Method = http.MethodPost
			request := httptest.NewRequest(http.MethodPost, r.URL.String(), strings.NewReader(`{"backend":"radarr","retry":false}`))
			request = request.WithContext(r.Context())
			r = mux.SetURLVars(request, map[string]string{"queueID": "1"})
		} else if action == "refresh" {
			r.Method = http.MethodPost
		}
		w := httptest.NewRecorder()
		switch action {
		case "status":
			h.ARRStatus(w, r)
		case "submit":
			h.ARRSubmit(w, r)
		default:
			h.ARRRefresh(w, r)
		}
		if w.Code != 200 {
			t.Fatal("existing fallback handler action failed", action, w.Code, w.Body.String())
		}
		var result importer.ARRImportStatus
		if json.Unmarshal(w.Body.Bytes(), &result) != nil || result.QueueID != 1 || len(result.ConfiguredBackends) != 1 || result.ConfiguredBackends[0] != "radarr" {
			t.Fatal("missing import controls/status", w.Body.String())
		}
	}
	if strings.Join(service.actions, ",") != "status,submit:radarr,refresh" {
		t.Fatal("incorrect action dispatch", service.actions)
	}
}
func TestARRSettingsSavebackPreservesConfiguredAPIKeys(t *testing.T) {
	original := config.DefaultSettings()
	original.SABnzbd.Sonarr = config.ARRImportSettings{BaseURL: "http://sonarr.invalid", APIKey: "synthetic-sonarr-key", LibraryID: "shows"}
	original.SABnzbd.Radarr = config.ARRImportSettings{BaseURL: "http://radarr.invalid", APIKey: "synthetic-radarr-key", LibraryID: "movies"}
	incoming := original
	redactSettings(&incoming)
	if incoming.SABnzbd.Sonarr.APIKey != redactedPlaceholder || incoming.SABnzbd.Radarr.APIKey != redactedPlaceholder {
		t.Fatal("ARR keys did not follow normal settings masking")
	}
	preserveRedactedFields(&incoming, &original)
	if incoming.SABnzbd.Sonarr.APIKey != original.SABnzbd.Sonarr.APIKey || incoming.SABnzbd.Radarr.APIKey != original.SABnzbd.Radarr.APIKey {
		t.Fatal("ordinary settings save erased configured ARR keys")
	}
}
