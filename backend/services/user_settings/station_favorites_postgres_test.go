package user_settings

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

func TestStationFavoritesPostgresExistingJSONRoundtrip(t *testing.T) {
	raw := os.Getenv("STATION_FAVORITES_TEST_DATABASE_URL")
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
		t.Fatal("connect to synthetic fixture")
	}
	defer admin.Close()
	name := fmt.Sprintf("synthetic_station_favorites_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
	}()
	target := *u
	target.Path = "/" + name
	store, err := datastore.New(ctx, target.String())
	if err != nil {
		t.Fatal("create disposable migrated datastore")
	}
	defer store.Close()
	now := time.Now().UTC()
	account := models.Account{ID: "synthetic_station_account", Username: "synthetic_station_account", PasswordHash: "synthetic-unused", CreatedAt: now, UpdatedAt: now}
	if err = store.Accounts().Create(ctx, &account); err != nil {
		t.Fatal(err)
	}
	profile := models.User{ID: "synthetic_station_profile", AccountID: account.ID, Name: "Synthetic station profile", CreatedAt: now, UpdatedAt: now}
	if err = store.Users().Create(ctx, &profile); err != nil {
		t.Fatal(err)
	}
	s, err := NewServiceWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Update(profile.ID, models.UserSettings{LiveTV: models.LiveTVSettings{FavoriteChannels: []string{"legacy:stream"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetFavoriteStation(profile.ID, stationFixture(1), true); err != nil {
		t.Fatal(err)
	}
	if err = s.Update(profile.ID, models.UserSettings{LiveTV: models.LiveTVSettings{FavoriteChannels: []string{"legacy:changed"}, FavoriteStationIDs: []string{stationFixture(9)}}}); err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewServiceWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := reloaded.Get(profile.ID)
	if got == nil || !reflect.DeepEqual(got.LiveTV.FavoriteStationIDs, []string{stationFixture(1)}) || !reflect.DeepEqual(got.LiveTV.FavoriteChannels, []string{"legacy:changed"}) {
		t.Fatal("new station list or legacy source favorites lost in existing JSON column")
	}
	if _, err = reloaded.SetFavoriteStation(profile.ID, stationFixture(1), false); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.UserSettings().Get(ctx, profile.ID)
	if err != nil || persisted == nil || len(persisted.LiveTV.FavoriteStationIDs) != 0 || len(persisted.LiveTV.FavoriteChannels) != 1 {
		t.Fatal("station removal damaged legacy favorites or did not persist")
	}
}
