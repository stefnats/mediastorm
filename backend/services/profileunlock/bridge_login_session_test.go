//go:build linux

package profileunlock

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/sessions"
)

type bridgeSessionFacadeFixture struct {
	ds          *datastore.DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	pins        *PGProfilePins
	issuer      *BridgeLoginSessionIssuer
}

func bridgeSessionFacadePG(t *testing.T) *bridgeSessionFacadeFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("own synthetic Unix PG fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix PG required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	f := &bridgeSessionFacadeFixture{}
	name := fmt.Sprintf("synthetic_bmint_facade_%d", time.Now().UnixNano())
	ownedRole, ownedSchema := false, false
	var objects *settingscommit.LockObjects
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		if objects != nil {
			_ = objects.Close()
		}
		if f.ds != nil {
			f.ds.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if ownedSchema {
			if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
				t.Error(err)
			}
		}
		if ownedRole {
			if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); err != nil {
				t.Error(err)
			}
		}
		admin.Close()
	})
	if _, err = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	ownedRole = true
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	ownedSchema = true
	query := u.Query()
	query.Set("search_path", pgx.Identifier{name}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+name)
	query.Set("application_name", name)
	u.RawQuery = query.Encode()
	f.ds, err = datastore.New(t.Context(), u.String())
	if err != nil {
		t.Fatal("actual migrated facade store", err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic actor account password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Actor',$1);`, string(hash)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic Profile');INSERT INTO user_settings_versions(user_id,generation)VALUES('target',0) ON CONFLICT(user_id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = u.String()
	settings.JellyfinBridge.Enabled = true
	settings.Security.EnforceProfilePins = false
	if err = f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	objects, err = settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.ds.Pool(), objects)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.pins = NewPGProfilePins(f.ds)
	f.issuer, err = f.pins.NewBridgeLoginSessionIssuer(f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestBridgeSessionFacadeActualColdAndStaleCachesAreNotAuthority(t *testing.T) {
	f := bridgeSessionFacadePG(t)
	cold, err := sessions.NewServiceWithStore(f.ds, time.Hour)
	if err != nil || cold.Count() != 0 {
		t.Fatal("genuine cold session projection", err)
	}
	r := bridgeCredentialFacadeRequest(t.Context())
	intent, err := f.pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := f.pins.VerifyBridgeAccountPassword(r, intent, "synthetic actor account password")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := models.Session{Token: "synthetic-stale-projection", AccountID: "actor", CreatedAt: now, ExpiresAt: now.Add(time.Hour), Scope: models.SessionScopeJellyfin, ScopeResource: "target"}
	if err = f.ds.Sessions().Create(t.Context(), &old); err != nil {
		t.Fatal(err)
	}
	stale, err := sessions.NewServiceWithStore(f.ds, time.Hour)
	if err != nil || stale.Count() != 1 {
		t.Fatal(err)
	}
	if count, err := f.ds.RevokeAccountSessions(t.Context(), "actor"); err != nil || count != 1 {
		t.Fatal(err)
	}
	observation, err := f.security.CaptureBridgePolicy(r.Context())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.issuer.Commit(r, proof, observation)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	session, err := f.issuer.Release(r, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if cold.Count() != 0 || stale.Count() != 1 {
		t.Fatal("mint eagerly changed an unrelated projection")
	}
	if actual, err := cold.ValidateScoped(session.Token, models.SessionScopeJellyfin); err != nil || actual.ScopeResource != "target" || actual.AccountID != "actor" {
		t.Fatal("cold projection could not read actual committed scoped row", err)
	}
	if _, err := stale.ValidateScoped(old.Token, models.SessionScopeJellyfin); err == nil {
		t.Fatal("stale cached token revived a revoked PG row")
	}
}

func TestBridgeSessionFacadeExactIssuerCopiesAndOpaqueReceipt(t *testing.T) {
	f := bridgeSessionFacadePG(t)
	r := bridgeCredentialFacadeRequest(t.Context())
	intent, err := f.pins.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := f.pins.VerifyBridgeAccountPassword(r, intent, "synthetic actor account password")
	if err != nil {
		t.Fatal(err)
	}
	observation, err := f.security.CaptureBridgePolicy(r.Context())
	if err != nil {
		t.Fatal(err)
	}
	otherPins := NewPGProfilePins(f.ds)
	other, err := otherPins.NewBridgeLoginSessionIssuer(f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := other.Commit(r, proof, observation); err == nil || receipt != nil {
		t.Fatal("different genuine PIN issuer borrowed original credential proof")
	}
	receipt, err := f.issuer.Commit(r, proof, observation)
	if err != nil {
		t.Fatal("foreign caller consumed intact original proof", err)
	}
	defer receipt.Close()
	copyReceipt := *receipt
	copyIssuer := *f.issuer
	if _, err := f.issuer.Release(r, &copyReceipt); err == nil {
		t.Fatal("receipt copy accepted")
	}
	if _, err := copyIssuer.Release(r, receipt); err == nil {
		t.Fatal("issuer copy accepted")
	}
	if _, err := other.Release(r, receipt); err == nil {
		t.Fatal("foreign issuer borrowed receipt")
	}
	for _, value := range []any{receipt, *receipt, f.issuer, *f.issuer} {
		data, err := json.Marshal(value)
		if err != nil || string(data) != "null" || !strings.HasSuffix(fmt.Sprintf("%#v", value), "(opaque)") {
			t.Fatal("opaque facade disclosed private token/state")
		}
	}
	if json.Unmarshal([]byte(`{}`), &copyReceipt) == nil {
		t.Fatal("imported facade receipt accepted")
	}
	if _, err := f.issuer.Release(r, receipt); err != nil {
		t.Fatal("original facade receipt no longer usable", err)
	}
	receipt.Close()
	proof.Close()
}

func TestBridgeSessionFacadeZeroFactoryCannotAcquireOrRelease(t *testing.T) {
	var pins *PGProfilePins
	if value, err := pins.NewBridgeLoginSessionIssuer(nil, nil, nil); err == nil || value != nil {
		t.Fatal("zero factory acquired issuer")
	}
	var issuer *BridgeLoginSessionIssuer
	if value, err := issuer.Commit(nil, nil, nil); err == nil || value != nil {
		t.Fatal("zero issuer minted")
	}
	if value, err := issuer.Release(nil, nil); err == nil || value.Token != "" {
		t.Fatal("zero issuer released token")
	}
	var receipt BridgeLoginSessionReceipt
	receipt.Close()
}
