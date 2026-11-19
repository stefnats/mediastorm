package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/url"
	"novastream/internal/datastore"
	"novastream/models"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func regionalBackupFixture(t *testing.T) (*Service, *datastore.DataStore) {
	t.Helper()
	raw := os.Getenv("REMINDER_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("disposable PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("a synthetic Unix-socket fixture database is required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect to synthetic fixture")
	}
	t.Cleanup(admin.Close)
	name := fmt.Sprintf("synthetic_regional_policy_backup_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
	})
	target := *u
	target.Path = "/" + name
	store, err := datastore.New(ctx, target.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `INSERT INTO accounts(id,username,password_hash)VALUES('fixture','synthetic-backup','fixture-hash');INSERT INTO users(id,account_id,name)VALUES('one','fixture','Synthetic')`); err != nil {
		t.Fatal(err)
	}

	backup, _ := setupTestService(t)
	backup.SetDataStore(store)
	// Close/drop cleanup must run after callers finish using this helper.
	t.Cleanup(store.Close)
	return backup, store
}
func TestRegionalPolicyBackupRoundtripRetainsOnlyConfigurationAndRegeneratesAuthority(t *testing.T) {
	service, store := regionalBackupFixture(t)
	entry := models.RegionalChildPolicyBackup{ProfileID: "one", Policy: models.RegionalChildPolicy{Version: 1, Country: "DE", MaxMovieAge: 6, MaxEpisodeAge: 12}}
	if err := store.WithTx(t.Context(), func(tx *datastore.Tx) error {
		return tx.RestoreRegionalPolicies(t.Context(), []models.RegionalChildPolicyBackup{entry})
	}); err != nil {
		t.Fatal(err)
	}
	before, err := store.CurrentRegionalProfile(t.Context(), "one")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := service.ExportDatabaseJSON()
	if err != nil {
		t.Fatal(err)
	}
	var exported databaseExport
	if err = json.Unmarshal(raw, &exported); err != nil || len(exported.RegionalChildPolicies) != 1 {
		t.Fatal("durable policy omitted from backup")
	}
	if strings.Contains(string(raw), "regional_policy_revision_seq") || strings.Contains(string(raw), "classificationFingerprint") {
		t.Fatal("runtime authority exported")
	}
	if err = service.ImportDatabaseJSON(raw); err != nil {
		t.Fatal(err)
	}
	after, err := store.CurrentRegionalProfile(t.Context(), "one")
	if err != nil || after.Policy == nil || *after.Policy != entry.Policy || after.Revision == before.Revision {
		t.Fatal("restore lost config or revived original authority")
	}
}
func TestRegionalPolicyRestoreDeniesCurrentOrImportedChildrenBeforeAnyDestructiveWrite(t *testing.T) {
	for _, mode := range []string{"current", "imported"} {
		t.Run(mode, func(t *testing.T) {
			service, store := regionalBackupFixture(t)
			raw, err := service.ExportDatabaseJSON()
			if err != nil {
				t.Fatal(err)
			}
			if mode == "current" {
				if _, err = store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true,name='Current protected child' WHERE id='one'`); err != nil {
					t.Fatal(err)
				}
			} else {
				var data databaseExport
				if err = json.Unmarshal(raw, &data); err != nil {
					t.Fatal(err)
				}
				data.Users[0].IsKidsProfile = true
				raw, err = json.Marshal(data)
				if err != nil {
					t.Fatal(err)
				}
			}
			before, err := store.Users().Get(t.Context(), "one")
			if err != nil {
				t.Fatal(err)
			}
			if err = service.ImportDatabaseJSON(raw); !errors.Is(err, datastore.ErrProtectedFamilyRestore) {
				t.Fatal("protected original restore transaction was not denied", err)
			}
			after, err := store.Users().Get(t.Context(), "one")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("denied restore changed current profile")
			}
		})
	}
}
func TestRegionalPolicyMalformedOrForeignBackupConfigurationRejectsBeforeRestore(t *testing.T) {
	service, store := regionalBackupFixture(t)
	raw, err := service.ExportDatabaseJSON()
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]json.RawMessage
	if err = json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	data["regionalChildPolicies"] = json.RawMessage(`[{"profileId":"missing","policy":{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12}}]`)
	bad, _ := json.Marshal(data)
	if err = service.ImportDatabaseJSON(bad); err == nil {
		t.Fatal("foreign policy restore accepted")
	}
	if current, err := store.Users().Get(t.Context(), "one"); err != nil || current == nil {
		t.Fatal("bad config cleared current data")
	}
}
