package accounts

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

func TestFamilyAccountsReadinessRejectsLegacyWithoutChangingAuthentication(t *testing.T) {
	account := contextFixtureAccount(t)
	calls := 0
	legacy := &Service{accounts: map[string]models.Account{account.ID: account}, accountReader: stubAccountReader{getByUsername: func(context.Context, string) (*models.Account, error) {
		calls++
		copy := account
		return &copy, nil
	}}}
	var unavailable *Service
	if unavailable.FamilyMediaReady(nil) || (&Service{}).FamilyMediaReady(&datastore.DataStore{}) || legacy.FamilyMediaReady(&datastore.DataStore{}) || calls != 0 {
		t.Fatal("legacy or unavailable account wiring became family authority")
	}
	verified, err := legacy.AuthenticateContext(t.Context(), account.Username, "synthetic parent password")
	if err != nil || verified.ID != account.ID || calls != 1 {
		t.Fatal("generic injected authentication changed outside family installer")
	}
}

func familyReadinessAccountStore(t *testing.T) *datastore.DataStore {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("dedicated synthetic Unix-socket database required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("connect own synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_account_readiness_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own schema")
	}
	var store *datastore.DataStore
	t.Cleanup(func() {
		if store != nil {
			store.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove own schema")
		}
		admin.Close()
	})
	query := u.Query()
	query.Set("search_path", name)
	u.RawQuery = query.Encode()
	store, err = datastore.New(t.Context(), u.String(), datastore.WithMaxConns(4))
	if err != nil {
		t.Fatal("migrate own synthetic store")
	}
	// Seed a real master before the production service constructor, avoiding
	// its unrelated bootstrap credential creation path.
	account := contextFixtureAccount(t)
	account.ID = models.MasterAccountID
	account.IsMaster = true
	if err = store.Accounts().Create(t.Context(), &account); err != nil {
		t.Fatal("seed own master account")
	}
	return store
}

func TestFamilyAccountsReadinessRequiresActualStoreAndAuthenticationRepository(t *testing.T) {
	first, second := familyReadinessAccountStore(t), familyReadinessAccountStore(t)
	service, err := NewServiceWithStore(first)
	if err != nil {
		t.Fatal("construct actual account service")
	}
	if !service.FamilyMediaReady(first) || service.FamilyMediaReady(second) || service.FamilyMediaReady(nil) {
		t.Fatal("actual store correlation failed")
	}
	calls := 0
	fake := stubAccountReader{getByUsername: func(context.Context, string) (*models.Account, error) { calls++; return nil, nil }}
	for _, test := range []struct {
		name   string
		reader accountReader
		store  *datastore.DataStore
		want   bool
	}{
		{"nil reader", nil, first, false},
		{"injected reader", fake, first, false},
		{"foreign pool", second.Accounts(), first, false},
		{"foreign private store", first.Accounts(), second, false},
		{"new genuine wrapper", first.Accounts(), first, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			service.mu.Lock()
			service.accountReader = test.reader
			service.store = test.store
			service.mu.Unlock()
			if service.FamilyMediaReady(first) != test.want {
				t.Fatal("non-genuine reader/store accepted")
			}
		})
	}
	if calls != 0 {
		t.Fatal("readiness queried injected verifier")
	}
}
