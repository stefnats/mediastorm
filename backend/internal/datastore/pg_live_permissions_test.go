package datastore

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
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"novastream/models"
)

func TestLivePermissionsPostgresFreshUpgradePersistenceAndRollback(t *testing.T) {
	raw := os.Getenv("LIVE_TAKEOVER_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("disposable PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || !strings.HasPrefix(u.Query().Get("host"), "/") || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || u.Path != "/postgres" && !strings.HasPrefix(strings.TrimPrefix(u.Path, "/"), "synthetic_") {
		t.Fatal("a synthetic Unix-socket fixture database is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect to fixture")
	}
	defer admin.Close()
	for _, upgrade := range []bool{false, true} {
		name := "fresh"
		if upgrade {
			name = "upgrade72"
		}
		t.Run(name, func(t *testing.T) {
			dbName := fmt.Sprintf("synthetic_live_rights_%d", time.Now().UnixNano())
			if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if _, err := admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{dbName}.Sanitize()+" WITH (FORCE)"); err != nil {
					t.Error("remove own fixture database")
				}
			}()
			target := *u
			target.Path = "/" + dbName
			pool, err := pgxpool.New(ctx, target.String())
			if err != nil {
				t.Fatal("connect to disposable fixture")
			}
			defer pool.Close()
			if upgrade {
				goose.SetBaseFS(embedMigrations)
				if err := goose.SetDialect("postgres"); err != nil {
					t.Fatal(err)
				}
				db := stdlib.OpenDBFromPool(pool)
				err := goose.UpToContext(ctx, db, "migrations", 72)
				db.Close()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('fixture','synthetic-rights','fixture-hash'); INSERT INTO users(id,account_id,name) VALUES('legacy','fixture','Synthetic legacy')`); err != nil {
					t.Fatal(err)
				}
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if !upgrade {
				if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('fixture','synthetic-rights','fixture-hash'); INSERT INTO users(id,account_id,name) VALUES('legacy','fixture','Synthetic legacy')`); err != nil {
					t.Fatal(err)
				}
			}
			repo := &pgUserRepo{pool: pool}
			legacy, err := repo.Get(ctx, "legacy")
			if err != nil || legacy == nil {
				t.Fatal("legacy profile did not load")
			}
			if legacy.AllowLiveTakeover || legacy.AllowRecordingInterruption {
				t.Fatal("migration granted legacy rights")
			}
			now := time.Now().UTC()
			created := &models.User{ID: "new", AccountID: "fixture", Name: "Synthetic new", AllowLiveTakeover: true, ActivityPrivacy: models.ActivityPrivacyNotShared, CreatedAt: now, UpdatedAt: now}
			if err := repo.Create(ctx, created); err != nil {
				t.Fatal(err)
			}
			loaded, err := repo.Get(ctx, "new")
			if err != nil || loaded == nil || !loaded.AllowLiveTakeover || loaded.AllowRecordingInterruption {
				t.Fatal("create did not preserve separate rights")
			}
			loaded.AllowLiveTakeover = false
			loaded.AllowRecordingInterruption = true
			if err := repo.Update(ctx, loaded); err != nil {
				t.Fatal(err)
			}
			for _, list := range []func(context.Context) ([]models.User, error){repo.List, func(ctx context.Context) ([]models.User, error) { return repo.ListByAccount(ctx, "fixture") }} {
				profiles, err := list(ctx)
				if err != nil || len(profiles) != 2 {
					t.Fatal("list did not load both profiles")
				}
				found := false
				for _, p := range profiles {
					if p.ID == "new" {
						found = true
						if p.AllowLiveTakeover || !p.AllowRecordingInterruption || p.ActivityPrivacy != models.ActivityPrivacyNotShared {
							t.Fatal("list lost rights or existing privacy")
						}
					}
				}
				if !found {
					t.Fatal("created profile missing")
				}
			}
			if _, err := pool.Exec(ctx, `UPDATE users SET allow_live_takeover=NULL WHERE id='new'`); err == nil {
				t.Fatal("nullable permission accepted")
			}
			db := stdlib.OpenDBFromPool(pool)
			err = goose.DownToContext(ctx, db, "migrations", 72)
			db.Close()
			if err != nil {
				t.Fatal(err)
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			reset, err := repo.Get(ctx, "new")
			if err != nil || reset == nil || reset.AllowLiveTakeover || reset.AllowRecordingInterruption {
				t.Fatal("rollback and re-upgrade did not default rights off")
			}
		})
	}
}
