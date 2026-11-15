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

func TestContentLocalPreferencePostgresFreshUpgradeRoundtripAndRollback(t *testing.T) {
	raw := os.Getenv("CONTENT_LOCAL_PREFERENCE_TEST_DATABASE_URL")
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
			name = "upgrade73"
		}
		t.Run(name, func(t *testing.T) {
			dbName := fmt.Sprintf("synthetic_content_local_%d", time.Now().UnixNano())
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
			seed := func() {
				t.Helper()
				if _, err := pool.Exec(ctx, `INSERT INTO accounts(id,username,password_hash) VALUES('fixture','synthetic-content-local','fixture-hash'); INSERT INTO users(id,account_id,name) VALUES('first','fixture','Synthetic first'),('second','fixture','Synthetic second'); INSERT INTO content_preferences(user_id,content_id,content_type,audio_language,subtitle_language,subtitle_mode) VALUES('first','tmdb:tv:123','series','fra','spa','forced-only')`); err != nil {
					t.Fatal(err)
				}
			}
			if upgrade {
				goose.SetBaseFS(embedMigrations)
				if err := goose.SetDialect("postgres"); err != nil {
					t.Fatal(err)
				}
				db := stdlib.OpenDBFromPool(pool)
				err := goose.UpToContext(ctx, db, "migrations", 73)
				db.Close()
				if err != nil {
					t.Fatal(err)
				}
				seed()
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			if !upgrade {
				seed()
			}
			repo := &pgContentPrefsRepo{pool: pool}
			legacy, err := repo.Get(ctx, "first", "tmdb:tv:123")
			if err != nil || legacy == nil || legacy.PreferLocalFiles != nil || legacy.AudioLanguage != "fra" || legacy.SubtitleLanguage != "spa" || legacy.SubtitleMode != "forced-only" {
				t.Fatal("migration changed legacy tracks or enabled a source override")
			}
			for _, profile := range []string{"first", "second"} {
				for _, state := range []struct {
					id    string
					value *bool
				}{{"tmdb:tv:123:season:2", models.BoolPtr(true)}, {"tmdb:tv:123:season:2:episode:4", models.BoolPtr(false)}, {"tmdb:movie:1", nil}} {
					pref := models.ContentPreference{ContentID: state.id, ContentType: "series", AudioLanguage: "eng", SubtitleLanguage: "fra", SubtitleMode: "on", PreferLocalFiles: state.value, UpdatedAt: time.Now().UTC()}
					if err := repo.Upsert(ctx, profile, &pref); err != nil {
						t.Fatal(err)
					}
					got, err := repo.Get(ctx, profile, state.id)
					if err != nil || got == nil || (got.PreferLocalFiles == nil) != (state.value == nil) || state.value != nil && *got.PreferLocalFiles != *state.value || got.AudioLanguage != "eng" || got.SubtitleLanguage != "fra" {
						t.Fatal("roundtrip lost three-state override or tracks")
					}
				}
			}
			for _, profile := range []string{"first", "second"} {
				listed, err := repo.ListByUser(ctx, profile)
				if err != nil || len(listed) < 3 {
					t.Fatal("list omitted preferences")
				}
				var sawFalse bool
				for _, pref := range listed {
					if pref.PreferLocalFiles != nil && !*pref.PreferLocalFiles {
						sawFalse = true
					}
				}
				if !sawFalse {
					t.Fatal("list collapsed explicit false into inheritance")
				}
			}
			legacy.PreferLocalFiles = models.BoolPtr(true)
			if err := repo.Upsert(ctx, "first", legacy); err != nil {
				t.Fatal(err)
			}
			legacy.PreferLocalFiles = nil
			if err := repo.Upsert(ctx, "first", legacy); err != nil {
				t.Fatal(err)
			}
			cleared, err := repo.Get(ctx, "first", legacy.ContentID)
			if err != nil || cleared.PreferLocalFiles != nil || cleared.AudioLanguage != "fra" {
				t.Fatal("clearing override erased tracks")
			}
			if err := repo.Delete(ctx, "first", "tmdb:tv:123:season:2"); err != nil {
				t.Fatal(err)
			}
			if other, err := repo.Get(ctx, "second", "tmdb:tv:123:season:2"); err != nil || other == nil || other.PreferLocalFiles == nil || !*other.PreferLocalFiles {
				t.Fatal("deletion crossed profiles")
			}
			db := stdlib.OpenDBFromPool(pool)
			err = goose.DownToContext(ctx, db, "migrations", 73)
			db.Close()
			if err != nil {
				t.Fatal(err)
			}
			var audio, subtitle, mode string
			if err := pool.QueryRow(ctx, `SELECT audio_language,subtitle_language,subtitle_mode FROM content_preferences WHERE user_id='first' AND content_id='tmdb:tv:123'`).Scan(&audio, &subtitle, &mode); err != nil || audio != "fra" || subtitle != "spa" || mode != "forced-only" {
				t.Fatal("rollback damaged language preferences")
			}
			if err := runMigrations(ctx, pool); err != nil {
				t.Fatal(err)
			}
			reset, err := repo.Get(ctx, "second", "tmdb:tv:123:season:2")
			if err != nil || reset == nil || reset.PreferLocalFiles != nil || reset.AudioLanguage != "eng" {
				t.Fatal("re-upgrade should inherit while retaining old tracks")
			}
		})
	}
}
