package localmedia

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

type catalogueCountsPGFixture struct {
	store   *datastore.DataStore
	service *Service
}

func newCatalogueCountsPGFixture(t *testing.T) *catalogueCountsPGFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "postgresql" || parsed.Host != "" || parsed.User == nil || !strings.HasPrefix(parsed.User.Username(), "synthetic_") || !strings.HasPrefix(parsed.Query().Get("host"), "/") {
		t.Fatal("own synthetic Unix-socket PostgreSQL required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("synthetic_local_catalogue_%d", time.Now().UnixNano())
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
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	store, err := datastore.New(t.Context(), parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	service, err := NewService(store, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	root := t.TempDir()
	library := &models.LocalMediaLibrary{ID: "catalogue", Name: "Synthetic local catalogue", Type: models.LocalMediaLibraryTypeMovie, RootPath: root, CreatedAt: at, UpdatedAt: at}
	if err := store.LocalMedia().CreateLibrary(t.Context(), library); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("local-%02d", i)
		title := "Ordinary title"
		if i%3 == 0 {
			title = "Needle title"
		}
		status := models.LocalMediaMatchStatusUnmatched
		if i%2 == 0 {
			status = models.LocalMediaMatchStatusMatched
		}
		row := &models.LocalMediaItem{ID: id, LibraryID: library.ID, RelativePath: id + ".mkv", FilePath: filepath.Join(root, id+".mkv"), FileName: id + ".mkv", LibraryType: library.Type, DetectedTitle: title, MatchedName: id, MatchStatus: status, IsMissing: i%5 == 0, CreatedAt: at, UpdatedAt: at, SizeBytes: int64(i + 1)}
		if err := store.LocalMedia().UpsertItem(t.Context(), row); err != nil {
			t.Fatal(err)
		}
	}
	return &catalogueCountsPGFixture{store: store, service: service}
}

func TestLocalCatalogueItemCounts(t *testing.T) {
	for _, mode := range []string{"first-page-full-filtered-total", "later-page-original-offset", "empty-tail-original-offset", "missing-filter-before-count-and-page", "search-and-status-full-total", "include-missing-original-result-unchanged"} {
		t.Run(mode, func(t *testing.T) {
			f := newCatalogueCountsPGFixture(t)
			q := models.LocalMediaItemListQuery{Sort: "name", Dir: "asc", Limit: 3}
			total := 16
			want := []string{"local-01", "local-02", "local-03"}
			switch mode {
			case "first-page-full-filtered-total":
			case "later-page-original-offset":
				q.Offset = 7
				want = []string{"local-09", "local-11", "local-12"}
			case "empty-tail-original-offset":
				q.Offset = 99
				want = []string{}
			case "missing-filter-before-count-and-page":
				q.Offset = 3
				q.Limit = 4
				want = []string{"local-04", "local-06", "local-07", "local-08"}
			case "search-and-status-full-total":
				q.Query = "nEeDlE"
				q.Filter = "matched"
				q.Limit = 1
				q.Offset = 1
				total = 3
				want = []string{"local-12"}
			case "include-missing-original-result-unchanged":
				q.IncludeMissing = true
				q.Offset = 4
				q.Limit = 4
				total = 20
				want = []string{"local-04", "local-05", "local-06", "local-07"}
			}
			// Compare two actual PG-backed paths. No substitute repository
			// supplies totals; the SQL count covers the complete stored set.
			repository, err := f.store.LocalMedia().ListItemsByLibrary(t.Context(), "catalogue", q)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := f.service.ListItems(t.Context(), "catalogue", q)
			if err != nil || actual == nil {
				t.Fatalf("actual local catalogue: %v", err)
			}
			if repository.Total != total || actual.Total != total || actual.Offset != q.Offset || actual.Limit != q.Limit || repository.Offset != actual.Offset || repository.Limit != actual.Limit {
				t.Fatalf("full PG total/original page changed: repo=%+v service=%+v", repository, actual)
			}
			ids := make([]string, 0, len(actual.Items))
			repoIDs := make([]string, 0, len(repository.Items))
			missing := 0
			for _, item := range actual.Items {
				ids = append(ids, item.ID)
				if item.IsMissing {
					missing++
				}
			}
			for _, item := range repository.Items {
				repoIDs = append(repoIDs, item.ID)
			}
			if !reflect.DeepEqual(ids, want) || !reflect.DeepEqual(ids, repoIDs) {
				t.Fatalf("actual matching row page: %v, want %v (repo %v)", ids, want, repoIDs)
			}
			if q.IncludeMissing {
				if missing != 1 || !reflect.DeepEqual(actual, repository) {
					t.Fatal("IncludeMissing original repository result changed")
				}
			} else if missing != 0 {
				t.Fatal("missing row escaped the original SQL predicate")
			}
		})
	}
}
