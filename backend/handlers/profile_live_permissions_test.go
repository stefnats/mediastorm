package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"errors"
	"novastream/internal/auth"
	"novastream/internal/parentauth"
	"novastream/models"
	"novastream/services/users"
)

func TestProfileLivePermissionsRequireOwningAccountManagement(t *testing.T) {
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	owned, err := service.CreateForAccount("household", "Synthetic viewer")
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := service.CreateForAccount("other", "Synthetic other")
	if err != nil {
		t.Fatal(err)
	}
	handler := &AdminUIHandler{usersService: service, parentAuthorization: parentauth.New(livePermissionTestAccounts{})}
	for _, tc := range []struct {
		name, account, id, body string
		master                  bool
		status                  int
	}{
		{"owner", "household", owned.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":false}`, false, 200},
		{"separate-recording", "household", owned.ID, `{"allowLiveTakeover":false,"allowRecordingInterruption":true}`, false, 200},
		{"foreign", "household", foreign.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":true}`, false, 404},
		{"fresh-password-required", "household", owned.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":true}`, false, 403},
		{"no-session", "", owned.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":true}`, false, 403},
		{"unknown-field", "household", owned.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":true,"profileId":"other"}`, false, 400},
		{"missing-field", "household", owned.ID, `{"allowLiveTakeover":true}`, false, 400},
		{"trailing", "household", owned.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":true}{}`, false, 400},
		{"master", "admin", foreign.ID, `{"allowLiveTakeover":true,"allowRecordingInterruption":false}`, true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest("PUT", "/account/api/profiles/live-permissions?profileId="+tc.id, strings.NewReader(tc.body))
			ctx := context.WithValue(request.Context(), auth.ContextKeyAccountID, tc.account)
			ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, tc.master)
			if tc.account != "" {
				ctx = context.WithValue(ctx, adminSessionContextKey{}, &models.Session{AccountID: tc.account, IsMaster: tc.master})
			}
			if tc.name != "fresh-password-required" {
				request.Header.Set(parentauth.PasswordHeader, "synthetic-confirmation")
			}
			response := httptest.NewRecorder()
			handler.SetProfileLivePermissions(response, request.WithContext(ctx))
			if response.Code != tc.status {
				t.Fatalf("got status %d, want %d", response.Code, tc.status)
			}
			if response.Header().Get("Cache-Control") != "private, no-store" {
				t.Fatal("rights response was cacheable")
			}
			if tc.status != http.StatusOK {
				p, _ := service.Get(foreign.ID)
				if p.AllowLiveTakeover || p.AllowRecordingInterruption {
					t.Fatal("invalid request granted foreign rights")
				}
			}
		})
	}
}

type livePermissionTestAccounts struct{}

func (livePermissionTestAccounts) Get(id string) (models.Account, bool) {
	return models.Account{ID: id, Username: id}, id != ""
}
func (livePermissionTestAccounts) Authenticate(username, password string) (models.Account, error) {
	if password != "synthetic-confirmation" {
		return models.Account{}, errors.New("invalid synthetic password")
	}
	return models.Account{ID: username}, nil
}
