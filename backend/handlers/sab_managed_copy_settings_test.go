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

func TestSABManagedCopyPrivateTargetSettingsProjectionsAndSaveback(t *testing.T) {
	cfg := config.DefaultSettings()
	if cfg.SABnzbd.ImportStageTargetRoot != "" || cfg.SABnzbd.ImportStageMaxBytes != 0 {
		t.Fatal("copy did not default off")
	}
	cfg.SABnzbd.ImportStageTargetRoot = "/PRIVATE_MANAGED_TARGET_CANARY"
	cfg.SABnzbd.ImportStageMaxBytes = 123456
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if e := manager.Save(cfg); e != nil {
		t.Fatal(e)
	}
	h := NewSettingsHandler(manager)
	safe := func(w *httptest.ResponseRecorder) {
		t.Helper()
		if w.Code != 200 || strings.Contains(w.Body.String(), "PRIVATE_MANAGED_TARGET_CANARY") {
			t.Fatal("private target projection leak or failure")
		}
	}
	w := httptest.NewRecorder()
	h.GetSettings(w, httptest.NewRequest("GET", "/api/settings", nil))
	safe(w)
	var view config.Settings
	if e := json.Unmarshal(w.Body.Bytes(), &view); e != nil {
		t.Fatal(e)
	}
	if view.SABnzbd.ImportStageTargetRoot != redactedPlaceholder || view.SABnzbd.ImportStageMaxBytes != 123456 {
		t.Fatal("mask/quota projection")
	}
	b, _ := json.Marshal(view)
	w = httptest.NewRecorder()
	h.PutSettings(w, httptest.NewRequest("PUT", "/api/settings", bytes.NewReader(b)))
	safe(w)
	current, e := manager.Load()
	if e != nil || current.SABnzbd.ImportStageTargetRoot != cfg.SABnzbd.ImportStageTargetRoot || current.SABnzbd.ImportStageMaxBytes != 123456 {
		t.Fatal("masked saveback lost target or quota")
	}
	owner, e := users.NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if _, e := owner.CreateForAccount("synthetic", "Synthetic person"); e != nil {
		t.Fatal(e)
	}
	admin := NewAdminUIHandler(manager.ConfigPath(), "", nil, owner, nil, manager)
	for _, path := range []string{"/admin/settings", "/account/settings"} {
		r := httptest.NewRequest("GET", path, nil)
		r = r.WithContext(context.WithValue(r.Context(), adminSessionContextKey{}, &models.Session{AccountID: "synthetic", IsMaster: path == "/admin/settings"}))
		w = httptest.NewRecorder()
		admin.SettingsPage(w, r)
		safe(w)
		if !strings.Contains(w.Body.String(), "currentSettings") || !strings.Contains(w.Body.String(), "importStageTargetRoot") {
			t.Fatal("actual full settings seed not exercised")
		}
	}
	us, e := user_settings.NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	uh := NewUserSettingsHandler(us, nil, manager)
	r := mux.SetURLVars(httptest.NewRequest("GET", "/api/users/person/settings", nil), map[string]string{"userID": "person"})
	w = httptest.NewRecorder()
	uh.GetSettings(w, r)
	safe(w)
	devices, e := clients.NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if _, e := devices.Register("device", "person", "test", "test", "1", "test", ""); e != nil {
		t.Fatal(e)
	}
	ch := NewClientsHandler(devices, previewClientSettings{})
	r = mux.SetURLVars(httptest.NewRequest("GET", "/api/clients/device/settings", nil), map[string]string{"clientID": "device"})
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
	w = httptest.NewRecorder()
	ch.GetSettings(w, r)
	safe(w)
	persisted, e := config.NewManager(manager.ConfigPath()).Load()
	if e != nil || persisted.SABnzbd.ImportStageTargetRoot != cfg.SABnzbd.ImportStageTargetRoot {
		t.Fatal("read/template mutation reached live/persisted config")
	}
}
