package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http/httptest"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/clients"
	"novastream/services/user_settings"
	"novastream/services/users"
	"path/filepath"
	"strings"
	"testing"
)

func TestSABImportPreviewSettingsPrivacyAndExactSaveback(t *testing.T) {
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.SABnzbd.ImportPreviewRemoteRoot = "/PRIVATE_REMOTE_ROOT_CANARY"
	settings.SABnzbd.ImportPreviewLocalRoot = "/PRIVATE_LOCAL_ROOT_CANARY"
	if e := manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	checkPrivate := func(w *httptest.ResponseRecorder) {
		t.Helper()
		if w.Code != 200 || strings.Contains(w.Body.String(), "PRIVATE_REMOTE_ROOT_CANARY") || strings.Contains(w.Body.String(), "PRIVATE_LOCAL_ROOT_CANARY") {
			t.Fatal("settings projection leaked mapping or failed")
		}
	}
	h := NewSettingsHandler(manager)
	w := httptest.NewRecorder()
	h.GetSettings(w, httptest.NewRequest("GET", "/api/settings", nil))
	checkPrivate(w)
	var projected config.Settings
	if e := json.Unmarshal(w.Body.Bytes(), &projected); e != nil {
		t.Fatal(e)
	}
	if projected.SABnzbd.ImportPreviewRemoteRoot != redactedPlaceholder || projected.SABnzbd.ImportPreviewLocalRoot != redactedPlaceholder {
		t.Fatal("roots not masked")
	}
	live, _ := manager.Load()
	if live.SABnzbd.ImportPreviewRemoteRoot != settings.SABnzbd.ImportPreviewRemoteRoot || live.SABnzbd.ImportPreviewLocalRoot != settings.SABnzbd.ImportPreviewLocalRoot {
		t.Fatal("redaction mutated live settings")
	}
	raw, _ := json.Marshal(projected)
	w = httptest.NewRecorder()
	h.PutSettings(w, httptest.NewRequest("PUT", "/api/settings", bytes.NewReader(raw)))
	checkPrivate(w)
	persisted, e := config.NewManager(manager.ConfigPath()).Load()
	if e != nil || persisted.SABnzbd.ImportPreviewRemoteRoot != settings.SABnzbd.ImportPreviewRemoteRoot || persisted.SABnzbd.ImportPreviewLocalRoot != settings.SABnzbd.ImportPreviewLocalRoot {
		t.Fatal("mask saveback lost exact server roots")
	}
	// Both actual role templates seed their detached full settings projection.
	owners, e := users.NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if _, e := owners.CreateForAccount("synthetic", "Synthetic profile"); e != nil {
		t.Fatal(e)
	}
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, owners, nil, manager)
	for _, path := range []string{"/admin/settings", "/account/settings"} {
		r := httptest.NewRequest("GET", path, nil)
		r = r.WithContext(context.WithValue(r.Context(), adminSessionContextKey{}, &models.Session{AccountID: "synthetic", IsMaster: path == "/admin/settings"}))
		w = httptest.NewRecorder()
		admin.SettingsPage(w, r)
		checkPrivate(w)
		if !strings.Contains(w.Body.String(), "currentSettings") || !strings.Contains(w.Body.String(), "importPreviewLocalRoot") {
			t.Fatal("actual settings seed not exercised")
		}
	}
	// Profile settings have a separate DTO, not an embedded config.Settings.
	profiles, e := user_settings.NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	userHandler := NewUserSettingsHandler(profiles, nil, manager)
	r := mux.SetURLVars(httptest.NewRequest("GET", "/api/users/profile/settings", nil), map[string]string{"userID": "profile"})
	w = httptest.NewRecorder()
	userHandler.GetSettings(w, r)
	checkPrivate(w)
	if strings.Contains(w.Body.String(), "importPreview") {
		t.Fatal("private server fields entered profile model")
	}
	// Client settings likewise use their actual bounded model.
	devices, e := clients.NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if _, e := devices.Register("device", "profile", "test", "test", "1", "test", ""); e != nil {
		t.Fatal(e)
	}
	clientHandler := NewClientsHandler(devices, previewClientSettings{})
	r = mux.SetURLVars(httptest.NewRequest("GET", "/api/clients/device/settings?userId=profile", nil), map[string]string{"clientID": "device"})
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
	w = httptest.NewRecorder()
	clientHandler.GetSettings(w, r)
	checkPrivate(w)
	if strings.Contains(w.Body.String(), "importPreview") {
		t.Fatal("private fields entered client model")
	}
	live, _ = manager.Load()
	if live.SABnzbd.ImportPreviewLocalRoot != settings.SABnzbd.ImportPreviewLocalRoot {
		t.Fatal("template/profile/client projection mutated manager")
	}
}

type previewClientSettings struct{}

func (previewClientSettings) Get(string, string) (*models.ClientFilterSettings, error) {
	return &models.ClientFilterSettings{}, nil
}
func (previewClientSettings) Update(string, string, models.ClientFilterSettings) error { return nil }
func (previewClientSettings) Delete(string, string) error                              { return nil }
func (previewClientSettings) DeleteByClient(string) error                              { return nil }
func (previewClientSettings) Move(string, string, string) error                        { return nil }
