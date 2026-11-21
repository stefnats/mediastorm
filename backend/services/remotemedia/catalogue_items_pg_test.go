package remotemedia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
)

type cataloguePGFixture struct {
	store         *datastore.DataStore
	service       *Service
	providerCalls atomic.Int64
}

func newCataloguePGFixture(t *testing.T) *cataloguePGFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("own synthetic Unix-socket PostgreSQL required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("synthetic_catalogue_%d", time.Now().UnixNano())
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close()
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	store, err := datastore.New(t.Context(), u.String(), datastore.WithMaxConns(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	f := &cataloguePGFixture{store: store}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.providerCalls.Add(1)
		http.Error(w, "provider must not be called", 500)
	}))
	t.Cleanup(server.Close)
	f.service, err = NewService(store, config.NewManager(filepath.Join(t.TempDir(), "config.json")), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	library := &models.RemoteMediaLibrary{ID: "catalogue", Name: "Synthetic catalogue", Type: models.LocalMediaLibraryTypeMovie, Provider: "jellyfin", AccountID: "synthetic-provider", ExternalLibraryID: "synthetic-library", ServerURL: server.URL, CreatedAt: now, UpdatedAt: now}
	if err := store.RemoteMedia().CreateLibrary(t.Context(), library); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.providerCalls.Load() != 0 {
			t.Error("catalogue listing performed provider HTTP")
		}
	})
	return f
}

func (f *cataloguePGFixture) put(t *testing.T, id, title, file, group string, missing bool) models.RemoteMediaItem {
	t.Helper()
	at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	row := models.RemoteMediaItem{ID: id, LibraryID: "catalogue", ExternalItemID: id, ExternalMediaID: "version-" + id, GroupKey: group, LibraryType: models.LocalMediaLibraryTypeMovie, Title: title, FileName: file, SizeBytes: 32, Year: 2025, IsMissing: missing, CreatedAt: at, UpdatedAt: at}
	if err := f.store.RemoteMedia().UpsertItem(t.Context(), &row); err != nil {
		t.Fatal(err)
	}
	return row
}

func catalogueIDs(result *models.LocalMediaItemListResult) []string {
	ids := make([]string, 0, len(result.Items))
	for _, item := range result.Items {
		ids = append(ids, item.ID)
	}
	return ids
}

func TestRemoteCatalogueItemPages(t *testing.T) {
	for _, mode := range []string{"complete-index-over-200-groups", "show-episodes-and-versions-flat-count", "search-and-status-before-page", "supported-sort-and-stable-tie-before-page", "missing-rows-before-total", "offset-and-empty-tail", "original-library-and-repository-failure", "original-context-cancel-zero-provider"} {
		t.Run(mode, func(t *testing.T) {
			f := newCataloguePGFixture(t)
			list := func(q models.LocalMediaItemListQuery) *models.LocalMediaItemListResult {
				t.Helper()
				r, err := f.service.ListItems(t.Context(), "catalogue", q)
				if err != nil || r == nil {
					t.Fatalf("actual mirrored page: %v", err)
				}
				return r
			}
			switch mode {
			case "complete-index-over-200-groups":
				want := map[string]bool{}
				for i := 0; i < 225; i++ {
					id := fmt.Sprintf("item-%03d", i)
					f.put(t, id, fmt.Sprintf("Title %03d", i), id+".mkv", id, false)
					want[id] = true
				}
				for i := 0; i < 3; i++ {
					id := fmt.Sprintf("version-%d", i)
					f.put(t, id, "Title 000", id+".mkv", "item-000", false)
					want[id] = true
				}
				seen := map[string]bool{}
				for offset := 0; offset < 228; offset += 37 {
					r := list(models.LocalMediaItemListQuery{Limit: 37, Offset: offset})
					if r.Total != 228 || r.Offset != offset || r.Limit != 37 {
						t.Fatalf("full version total/page: %+v", r)
					}
					for _, item := range r.Items {
						if !want[item.ID] || seen[item.ID] {
							t.Fatalf("unexpected/repeated row %q", item.ID)
						}
						seen[item.ID] = true
					}
				}
				if !reflect.DeepEqual(seen, want) {
					t.Fatal("later groups or alternate versions lost")
				}
			case "show-episodes-and-versions-flat-count":
				for episode := 1; episode <= 4; episode++ {
					for version := 1; version <= 2; version++ {
						id := fmt.Sprintf("e%d-v%d", episode, version)
						row := f.put(t, id, "Same show", id+".mkv", "same-show", false)
						row.LibraryType = models.LocalMediaLibraryTypeShow
						row.SeasonNumber = 2
						row.EpisodeNumber = episode
						row.EpisodeTitle = fmt.Sprintf("Episode %d", episode)
						row.VersionLabel = fmt.Sprintf("Version %d", version)
						if err := f.store.RemoteMedia().UpsertItem(t.Context(), &row); err != nil {
							t.Fatal(err)
						}
					}
				}
				r := list(models.LocalMediaItemListQuery{Offset: 5, Limit: 2})
				if r.Total != 8 || !reflect.DeepEqual(catalogueIDs(r), []string{"e3-v2", "e4-v1"}) {
					t.Fatalf("episodes/versions flattened after paging: %+v", r)
				}
				for _, item := range r.Items {
					if item.LibraryType != models.LocalMediaLibraryTypeShow || item.SeasonNumber != 2 || item.EpisodeNumber == 0 || item.VersionLabel == "" {
						t.Fatal("episode/version metadata lost")
					}
				}
			case "search-and-status-before-page":
				for i := 0; i < 8; i++ {
					file := "ordinary.mkv"
					title := "Other"
					if i < 3 {
						file = "UNIQUE-file.mkv"
					}
					if i >= 3 && i < 6 {
						title = "Unique title"
					}
					f.put(t, fmt.Sprint(i), title, file, fmt.Sprint(i), false)
				}
				r := list(models.LocalMediaItemListQuery{Query: "  uNiQuE  ", Filter: "matched", Offset: 4, Limit: 1})
				if r.Total != 6 || len(r.Items) != 1 {
					t.Fatalf("filter before total/page: %+v", r)
				}
				for _, filter := range []string{"manual", " all ", ""} {
					if list(models.LocalMediaItemListQuery{Filter: filter}).Total != 8 {
						t.Fatal("existing remote filter changed")
					}
				}
				for _, filter := range []string{"unmatched", "low_confidence", "MATCHED", "unknown"} {
					if list(models.LocalMediaItemListQuery{Filter: filter}).Total != 0 {
						t.Fatal("unavailable match state invented")
					}
				}
			case "supported-sort-and-stable-tie-before-page":
				at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
				for i, id := range []string{"a", "b", "c"} {
					title := "Alpha"
					if id == "b" {
						title = "Beta"
					}
					row := f.put(t, id, title, id, "group", false)
					row.CreatedAt = at.Add(time.Duration(i) * time.Hour)
					row.UpdatedAt = at.Add(time.Duration(2-i) * time.Hour)
					row.SizeBytes = int64(i+1) * 10
					row.Year = 2020
					if id == "b" {
						row.Year = 2022
					}
					if err := f.store.RemoteMedia().UpsertItem(t.Context(), &row); err != nil {
						t.Fatal(err)
					}
				}
				// Upsert keeps the earliest creation time; set the actual stored
				// fixture timestamps explicitly so every genuine sort key differs.
				if _, err := f.store.Pool().Exec(t.Context(), `UPDATE remote_media_items SET created_at=$1::timestamptz + CASE id WHEN 'a' THEN interval '0 hour' WHEN 'b' THEN interval '1 hour' ELSE interval '2 hour' END`, at); err != nil {
					t.Fatal(err)
				}
				checks := []struct {
					sort, dir string
					want      []string
				}{{"", "desc", []string{"a", "c", "b"}}, {"name", "desc", []string{"b", "a", "c"}}, {"created", "asc", []string{"a", "b", "c"}}, {"updated", "asc", []string{"c", "b", "a"}}, {"year", "asc", []string{"a", "c", "b"}}, {"size", "desc", []string{"c", "b", "a"}}, {"created", "desc", []string{"c", "b", "a"}}, {"updated", "desc", []string{"a", "b", "c"}}, {"year", "desc", []string{"b", "a", "c"}}, {"size", "asc", []string{"a", "b", "c"}}, {"name", "unexpected", []string{"a", "c", "b"}}}
				for _, check := range checks {
					full := list(models.LocalMediaItemListQuery{Sort: check.sort, Dir: check.dir, Limit: 3})
					if !reflect.DeepEqual(catalogueIDs(full), check.want) {
						t.Fatalf("full actual sort %s/%s: %v", check.sort, check.dir, catalogueIDs(full))
					}
					r := list(models.LocalMediaItemListQuery{Sort: check.sort, Dir: check.dir, Limit: 1, Offset: 1})
					if r.Total != 3 || !reflect.DeepEqual(catalogueIDs(r), check.want[1:2]) {
						t.Fatalf("sort %s/%s before page: %v", check.sort, check.dir, catalogueIDs(r))
					}
				}
				for _, sort := range []string{"modified", "confidence", "status", "bogus"} {
					if r, err := f.service.ListItems(t.Context(), "catalogue", models.LocalMediaItemListQuery{Sort: sort}); r != nil || !errors.Is(err, ErrInvalidQuery) {
						t.Fatalf("unsupported sort %q accepted: %v", sort, err)
					}
				}
			case "missing-rows-before-total":
				f.put(t, "a", "A", "a", "a", false)
				f.put(t, "b", "B", "b", "b", true)
				f.put(t, "c", "C", "c", "c", false)
				r := list(models.LocalMediaItemListQuery{Offset: 1, Limit: 1})
				if r.Total != 2 || !reflect.DeepEqual(catalogueIDs(r), []string{"c"}) {
					t.Fatalf("missing filter after total/page: %+v", r)
				}
				r = list(models.LocalMediaItemListQuery{IncludeMissing: true, Offset: 1, Limit: 1})
				if r.Total != 3 || len(r.Items) != 1 || r.Items[0].ID != "b" || !r.Items[0].IsMissing {
					t.Fatal("actual missing row flag lost")
				}
			case "offset-and-empty-tail":
				f.put(t, "a", "A", "a", "a", false)
				for _, q := range []models.LocalMediaItemListQuery{{Limit: 0, Offset: -1}, {Limit: 201}, {Limit: -1}} {
					r := list(q)
					if r.Limit != 50 || r.Offset != 0 || r.Total != 1 || len(r.Items) != 1 {
						t.Fatalf("legacy page normalization changed: %+v", r)
					}
				}
				r := list(models.LocalMediaItemListQuery{Limit: 200, Offset: int(^uint(0) >> 1)})
				if r.Offset != 1 || r.Total != 1 || r.Items == nil || len(r.Items) != 0 {
					t.Fatalf("empty tail overflow/nonempty: %+v", r)
				}
				encoded, err := json.Marshal(r)
				if err != nil || !strings.Contains(string(encoded), `"items":[]`) {
					t.Fatal("empty page must remain JSON array")
				}
			case "original-library-and-repository-failure":
				f.put(t, "a", "A", "a", "a", false)
				if r, err := f.service.ListItems(t.Context(), "absent", models.LocalMediaItemListQuery{}); r != nil || !errors.Is(err, ErrNotFound) {
					t.Fatalf("missing original library: %v", err)
				}
				f.store.Close()
				if r, err := f.service.ListItems(t.Context(), "catalogue", models.LocalMediaItemListQuery{}); r != nil || err == nil || errors.Is(err, ErrNotFound) {
					t.Fatal("repository failure converted to empty/missing catalogue")
				}
			case "original-context-cancel-zero-provider":
				f.put(t, "a", "A", "a", "a", false)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if r, err := f.service.ListItems(ctx, "catalogue", models.LocalMediaItemListQuery{}); r != nil || !errors.Is(err, context.Canceled) {
					t.Fatal("original canceled request returned a page")
				}
				held, err := f.store.Pool().Acquire(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer held.Release()
				ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
				defer cancel()
				if r, err := f.service.ListItems(ctx, "catalogue", models.LocalMediaItemListQuery{}); r != nil || err == nil || ctx.Err() != context.DeadlineExceeded {
					t.Fatalf("original held SQL acquisition not canceled: %v", err)
				}
				if f.store.Pool().Stat().CanceledAcquireCount() != 1 || f.providerCalls.Load() != 0 {
					t.Fatal("original PG wait/provider cancellation witness missing")
				}
			}
		})
	}
}
