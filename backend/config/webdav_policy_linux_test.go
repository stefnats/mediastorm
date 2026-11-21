//go:build linux

package config_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	"novastream/internal/settingsbootstrap"
)

// The external test package exercises the real config -> Startup -> same-store
// observer composition; an enforce flag or caller-made profile is never enough.
func deploymentWebDAVNativeFixture(t *testing.T, enforce bool) (*datastore.DataStore, *settingsbootstrap.DeploymentWebDAVIssuer) {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic Unix PostgreSQL required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix PostgreSQL only")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("synthetic_webdav_native_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	u.Path = "/" + name
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = u.String()
	settings.Security.EnforceProfilePins = enforce
	settings.WebDAV = config.WebDAVSettings{Enabled: true, Prefix: "/webdav", Username: "synthetic-basic", Password: "synthetic-password"}
	if err = manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	startup, err := settingsbootstrap.Begin(t.Context(), manager, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := startup.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := startup.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = startup.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('native-account','Synthetic Basic account','synthetic-unused-hash');INSERT INTO users(id,account_id,name)VALUES('native-profile','native-account','Synthetic profile')`); err != nil {
		t.Fatal(err)
	}
	issuer, err := startup.NewDeploymentWebDAVIssuer(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(issuer.Close)
	return store, issuer
}
func deploymentWebDAVNativeRequest(t *testing.T) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodGet, "http://synthetic.test/webdav/movie", nil).WithContext(ctx)
	r.RemoteAddr = "127.0.0.1:23001"
	r.SetBasicAuth("synthetic-basic", "synthetic-password")
	return r
}
func TestDeploymentWebDAVPINPolicy(t *testing.T) {
	for _, name := range []string{"false-alone-never-authorizes", "true-all-unpinned", "true-one-protected", "inconsistent-empty-pin-length", "true-unknown-pin-state"} {
		t.Run(name, func(t *testing.T) {
			store, issuer := deploymentWebDAVNativeFixture(t, name != "false-alone-never-authorizes")
			r := deploymentWebDAVNativeRequest(t)
			switch name {
			case "false-alone-never-authorizes":
				r.SetBasicAuth("synthetic-basic", "wrong-synthetic-password")
				if proof, err := issuer.Capture(r); !errors.Is(err, config.ErrWebDAVBasicRejected) || proof != nil {
					if proof != nil {
						proof.Close()
					}
					t.Fatal("enforce=false became Basic authority", err)
				}
				r.SetBasicAuth("synthetic-basic", "synthetic-password")
				if _, err := store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='native-profile'`); err != nil {
					t.Fatal(err)
				}
			case "true-one-protected":
				hash, err := bcrypt.GenerateFromPassword([]byte("1357"), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = store.Pool().Exec(t.Context(), `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='native-profile'`, string(hash)); err != nil {
					t.Fatal(err)
				}
			case "inconsistent-empty-pin-length":
				if _, err := store.Pool().Exec(t.Context(), `UPDATE users SET pin_hash='',pin_length=4 WHERE id='native-profile'`); err != nil {
					t.Fatal(err)
				}
			case "true-unknown-pin-state":
				if _, err := store.Pool().Exec(t.Context(), `ALTER TABLE users ALTER COLUMN pin_hash DROP NOT NULL;UPDATE users SET pin_hash=NULL WHERE id='native-profile'`); err != nil {
					t.Fatal(err)
				}
			}
			proof, err := issuer.Capture(r)
			if name == "true-all-unpinned" {
				if err != nil || proof == nil {
					t.Fatal("actual complete unpinned deployment", err)
				}
				defer proof.Close()
				if err = proof.Check(r); err != nil {
					t.Fatal(err)
				}
				return
			}
			if proof != nil {
				proof.Close()
				t.Fatal("protected/unknown deployment returned applicability")
			}
			if name == "true-one-protected" || name == "false-alone-never-authorizes" {
				if !errors.Is(err, datastore.ErrDeploymentWebDAVUnsupported) {
					t.Fatalf("known unsupported deployment: %v", err)
				}
			} else if !errors.Is(err, datastore.ErrDeploymentWebDAVUnavailable) {
				t.Fatalf("unknown PIN facts were not fail-closed: %v", err)
			}
		})
	}
}
