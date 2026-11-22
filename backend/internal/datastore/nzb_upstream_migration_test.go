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
)

// The two published histories assign different tables to version 71. Exercise
// both real migration ledgers, retaining existing rows across upgrade/rerun.
func TestNZBUpstreamMigrationPreservesBothVersion71Histories(t *testing.T) {
	raw := os.Getenv("TITLE_RATINGS_TEST_DATABASE_URL")
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
		t.Fatal(err)
	}
	defer admin.Close()
	for _, history := range []string{"fork-profile-ratings", "upstream-nzb-budget"} {
		t.Run(history, func(t *testing.T) {
			name := fmt.Sprintf("synthetic_nzb_upgrade_%d", time.Now().UnixNano())
			if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
					t.Error(err)
				}
			}()
			target := *u
			target.Path = "/" + name
			pool, err := pgxpool.New(ctx, target.String())
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			goose.SetBaseFS(embedMigrations)
			if err := goose.SetDialect("postgres"); err != nil {
				t.Fatal(err)
			}
			db := stdlib.OpenDBFromPool(pool)
			defer db.Close()
			version := int64(70)
			if history == "fork-profile-ratings" {
				version = 71
			}
			if err := goose.UpToContext(ctx, db, "migrations", version); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('fixture','synthetic-upgrade','fixture-hash'); INSERT INTO users(id,account_id,name) VALUES('profile','fixture','Synthetic profile')`); err != nil {
				t.Fatal(err)
			}
			if history == "fork-profile-ratings" {
				if _, err := pool.Exec(ctx, `INSERT INTO profile_title_ratings(profile_id,title_key,aliases,title,rating,updated_at) VALUES('profile','tmdb:movie:7',ARRAY['tmdb:movie:7'],'{"id":"tmdb:movie:7"}','love',now())`); err != nil {
					t.Fatal(err)
				}
			} else {
				// Exact original upstream 071 schema, independently of the new 094.
				if _, err := pool.Exec(ctx, `CREATE TABLE nzb_download_attempts(title_key TEXT NOT NULL, provider TEXT NOT NULL, attempted_at TIMESTAMPTZ NOT NULL);
CREATE INDEX nzb_download_attempts_title_time ON nzb_download_attempts(title_key,attempted_at);
CREATE INDEX nzb_download_attempts_time ON nzb_download_attempts(attempted_at);
CREATE TABLE indexer_cooldowns(provider TEXT PRIMARY KEY,until_at TIMESTAMPTZ NOT NULL,failures INTEGER NOT NULL);
INSERT INTO goose_db_version(version_id,is_applied) VALUES(71,true);
INSERT INTO nzb_download_attempts VALUES('tmdb:movie:7','synthetic',now());
INSERT INTO indexer_cooldowns VALUES('synthetic',now()+interval '1 hour',1)`); err != nil {
					t.Fatal(err)
				}
			}
			for range 2 {
				if err := runMigrations(ctx, pool); err != nil {
					t.Fatal(err)
				}
			}
			version, err = goose.GetDBVersionContext(ctx, db)
			if err != nil || version != 94 {
				t.Fatalf("final migration version=%d err=%v", version, err)
			}
			var tables int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name IN ('nzb_download_attempts','indexer_cooldowns','profile_title_ratings')`).Scan(&tables); err != nil || tables != 3 {
				t.Fatalf("coexisting tables=%d err=%v", tables, err)
			}
			if history == "fork-profile-ratings" {
				var rating string
				if err := pool.QueryRow(ctx, `SELECT rating FROM profile_title_ratings WHERE profile_id='profile' AND title_key='tmdb:movie:7'`).Scan(&rating); err != nil || rating != "love" {
					t.Fatalf("original rating=%q err=%v", rating, err)
				}
			} else {
				var attempts, cooldowns int
				if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM nzb_download_attempts WHERE title_key='tmdb:movie:7'),(SELECT count(*) FROM indexer_cooldowns WHERE provider='synthetic' AND failures=1)`).Scan(&attempts, &cooldowns); err != nil || attempts != 1 || cooldowns != 1 {
					t.Fatalf("original budget data=%d/%d err=%v", attempts, cooldowns, err)
				}
				if _, err := pool.Exec(ctx, `INSERT INTO profile_title_ratings(profile_id,title_key,aliases,title,rating,updated_at) VALUES('profile','tmdb:movie:7',ARRAY['tmdb:movie:7'],'{}','like',now())`); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
