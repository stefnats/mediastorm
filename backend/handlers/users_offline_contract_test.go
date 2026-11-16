package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"novastream/handlers"
	"novastream/models"
)

// The existing real handler receives synthetic server-owned account context;
// these are handler response bytes, not a recording of account authentication.
type offlineProfileContractUsers struct {
	fakeUsersService
	profiles    []models.User
	seenAccount string
}

func (s *offlineProfileContractUsers) ListForAccount(accountID string) []models.User {
	s.seenAccount = accountID
	result := []models.User{}
	for _, profile := range s.profiles {
		if profile.AccountID == accountID {
			result = append(result, profile)
		}
	}
	return result
}

func TestUsersOfflineProfileContract(t *testing.T) {
	at := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	profiles := []models.User{
		{ID: "profile-adult", AccountID: "account-a", Name: "Synthetic adult", IsKidsProfile: false, CreatedAt: at, UpdatedAt: at},
		{ID: "profile-child", AccountID: "account-a", Name: "Synthetic child", IsKidsProfile: true, CreatedAt: at, UpdatedAt: at},
		{ID: "profile-foreign", AccountID: "account-b", Name: "Synthetic other account", IsKidsProfile: false, CreatedAt: at, UpdatedAt: at},
	}
	for _, tc := range []struct {
		account string
		count   int
	}{{"account-a", 2}, {"account-b", 1}, {"unknown-account", 0}} {
		t.Run(tc.account, func(t *testing.T) {
			svc := &offlineProfileContractUsers{profiles: profiles}
			handler := handlers.NewUsersHandler(svc)
			request := usersRequest(http.MethodGet, "/api/users?accountId=account-b&userId=profile-foreign", nil, nil, tc.account, false)
			response := httptest.NewRecorder()
			handler.List(response, request)
			if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" || svc.seenAccount != tc.account {
				t.Fatal("profile handler did not use the original server account context")
			}
			var rows []map[string]json.RawMessage
			if err := json.Unmarshal(response.Body.Bytes(), &rows); err != nil {
				t.Fatal(err)
			}
			if len(rows) != tc.count {
				t.Fatalf("profile rows = %d, want %d", len(rows), tc.count)
			}
			for _, row := range rows {
				var owner, id string
				if err := json.Unmarshal(row["accountId"], &owner); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(row["id"], &id); err != nil {
					t.Fatal(err)
				}
				if owner != tc.account {
					t.Fatal("foreign account profile was serialized")
				}
				want := []byte("false")
				if id == "profile-child" {
					want = []byte("true")
				}
				if !bytes.Equal(row["isKidsProfile"], want) {
					t.Fatal("profile type is missing, null, quoted or incorrect")
				}
			}
			if tc.account != "account-a" {
				return
			}
			directory := "testdata/offline_profiles"
			if override := os.Getenv("OFFLINE_PROFILE_CONTRACT_FIXTURES"); override != "" {
				directory = override
			}
			golden := filepath.Join(directory, "profiles.json")
			if os.Getenv("OFFLINE_PROFILE_CONTRACT_EXPORT") == "1" {
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(golden, response.Body.Bytes(), 0600); err != nil {
					t.Fatal(err)
				}
			}
			expected, err := os.ReadFile(golden)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(expected, response.Body.Bytes()) {
				t.Fatal("real profile handler changed the native raw-response fixture")
			}
		})
	}
}
