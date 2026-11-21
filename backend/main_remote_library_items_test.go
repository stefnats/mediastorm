package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"novastream/config"
	"novastream/models"
	"novastream/services/localmedia"
	"novastream/services/remotemedia"
	"novastream/utils"
)

type mainRemoteCatalogueFixture struct {
	base   *publicPinWebFixture
	server *httptest.Server
	local  *localmedia.Service
}

func mainRemoteCataloguePG(t *testing.T) *mainRemoteCatalogueFixture {
	t.Helper()
	base := newPublicPinWebFixture(t)
	if _, err := base.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor'`); err != nil {
		t.Fatal(err)
	}
	local, err := localmedia.NewService(base.store, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	remote, err := remotemedia.NewService(base.store, config.NewManager(filepath.Join(t.TempDir(), "config.json")), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	admin := base.admin(t)
	admin.SetLocalMediaService(local)
	admin.SetRemoteMediaService(remote)
	router := utils.NewRouter()
	// These are the actual Main aliases and the actual current PG cookie guard.
	for _, prefix := range []string{"/admin", "/account"} {
		router.HandleFunc(prefix+"/api/library/libraries/{libraryID}/items", admin.RequireAuth(admin.ListLocalMediaItems)).Methods(http.MethodGet)
	}
	server := httptest.NewServer(utils.BasePathHandler("/app", router))
	t.Cleanup(server.Close)
	at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	library := &models.RemoteMediaLibrary{ID: "remote", Name: "Synthetic remote", Type: models.LocalMediaLibraryTypeMovie, Provider: "jellyfin", AccountID: "synthetic-provider", ExternalLibraryID: "synthetic-library", CreatedAt: at, UpdatedAt: at}
	if err := base.store.RemoteMedia().CreateLibrary(t.Context(), library); err != nil {
		t.Fatal(err)
	}
	return &mainRemoteCatalogueFixture{base: base, server: server, local: local}
}

func (f *mainRemoteCatalogueFixture) put(t *testing.T, id, title, file, group string) {
	t.Helper()
	at := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	row := models.RemoteMediaItem{ID: id, LibraryID: "remote", ExternalItemID: id, ExternalMediaID: "version-" + id, GroupKey: group, LibraryType: models.LocalMediaLibraryTypeMovie, Title: title, FileName: file, SizeBytes: 64, CreatedAt: at, UpdatedAt: at}
	if err := f.base.store.RemoteMedia().UpsertItem(t.Context(), &row); err != nil {
		t.Fatal(err)
	}
}

func (f *mainRemoteCatalogueFixture) get(t *testing.T, path string, cookie bool) (int, []byte) {
	t.Helper()
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, f.server.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cookie {
		r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.base.session.Token})
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("actual HTTP body/close: %v %v", err, closeErr)
	}
	return response.StatusCode, body
}

func mainCataloguePage(t *testing.T, status int, body []byte) *models.LocalMediaItemListResult {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("actual catalogue status %d: %s", status, body)
	}
	var result models.LocalMediaItemListResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	return &result
}

func TestMainRemoteCatalogueItemPages(t *testing.T) {
	for _, mode := range []string{"actual-master-cookie-flat-version-page", "search-and-later-page-full-total", "missing-remote-library-no-local-fallback", "current-master-revoked-before-list", "missing-cookie-no-catalogue", "unchanged-local-pg-item-branch"} {
		t.Run(mode, func(t *testing.T) {
			f := mainRemoteCataloguePG(t)
			switch mode {
			case "actual-master-cookie-flat-version-page":
				for i := 0; i < 205; i++ {
					id := fmt.Sprintf("item-%03d", i)
					f.put(t, id, fmt.Sprintf("Title %03d", i), id+".mkv", id)
				}
				f.put(t, "extra-version", "Title 204", "alternate.mkv", "item-204")
				for _, prefix := range []string{"/app/admin", "/app/account"} {
					status, body := f.get(t, prefix+"/api/library/libraries/remote/items?sort=name&dir=asc&offset=204&limit=1", true)
					r := mainCataloguePage(t, status, body)
					if r.Total != 206 || r.Limit != 1 || r.Offset != 204 || len(r.Items) != 1 || r.Items[0].ID != "extra-version" || r.Items[0].SourceType != "jellyfin" {
						t.Fatalf("original alias lost later version page: %+v", r)
					}
				}
				status, body := f.get(t, "/app/admin/api/library/libraries/remote/items?sort=modified", true)
				if status != http.StatusBadRequest || !strings.Contains(string(body), "invalid remote catalogue query") {
					t.Fatalf("unsupported remote sort not explicit400: %d %s", status, body)
				}
			case "search-and-later-page-full-total":
				for i := 0; i < 12; i++ {
					file := "ordinary.mkv"
					if i%2 == 0 {
						file = "Needle-version.mkv"
					}
					f.put(t, fmt.Sprintf("item-%02d", i), fmt.Sprintf("Title %02d", i), file, fmt.Sprint(i))
				}
				status, body := f.get(t, "/app/admin/api/library/libraries/remote/items?query=NEEDLE&filter=matched&sort=name&dir=desc&limit=2&offset=2", true)
				r := mainCataloguePage(t, status, body)
				ids := []string{}
				for _, item := range r.Items {
					ids = append(ids, item.ID)
				}
				if r.Total != 6 || r.Offset != 2 || r.Limit != 2 || !reflect.DeepEqual(ids, []string{"item-06", "item-04"}) {
					t.Fatalf("original query/full total not before page: %+v", r)
				}
			case "missing-remote-library-no-local-fallback":
				f.put(t, "remote-row", "Remote title", "remote.mkv", "remote")
				status, body := f.get(t, "/app/admin/api/library/libraries/remote/items", true)
				if mainCataloguePage(t, status, body).Total != 1 {
					t.Fatal("original remote row missing")
				}
				if err := f.base.store.RemoteMedia().DeleteLibrary(t.Context(), "remote"); err != nil {
					t.Fatal(err)
				}
				status, body = f.get(t, "/app/admin/api/library/libraries/remote/items", true)
				if status != http.StatusBadRequest || strings.Contains(string(body), `"items"`) {
					t.Fatalf("missing library became successful local catalogue: %d %s", status, body)
				}
			case "current-master-revoked-before-list":
				f.put(t, "private-row", "Private catalogue title", "private.mkv", "private")
				status, body := f.get(t, "/app/admin/api/library/libraries/remote/items", true)
				if mainCataloguePage(t, status, body).Total != 1 {
					t.Fatal("genuine current master was denied")
				}
				if _, err := f.base.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=false WHERE id='actor'`); err != nil {
					t.Fatal(err)
				}
				status, body = f.get(t, "/app/admin/api/library/libraries/remote/items", true)
				if status != http.StatusForbidden || strings.Contains(string(body), "Private catalogue title") {
					t.Fatalf("cached master cookie exposed catalogue after current revocation: %d", status)
				}
			case "missing-cookie-no-catalogue":
				f.put(t, "private-row", "Private catalogue title", "private.mkv", "private")
				status, body := f.get(t, "/app/admin/api/library/libraries/remote/items", false)
				if status != http.StatusSeeOther || strings.Contains(string(body), "Private catalogue title") {
					t.Fatalf("missing cookie exposed catalogue: %d", status)
				}
			case "unchanged-local-pg-item-branch":
				at := time.Now().UTC()
				library := &models.LocalMediaLibrary{ID: "local", Name: "Actual local", Type: models.LocalMediaLibraryTypeMovie, RootPath: t.TempDir(), CreatedAt: at, UpdatedAt: at}
				if err := f.base.store.LocalMedia().CreateLibrary(t.Context(), library); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 5; i++ {
					id := fmt.Sprintf("local-%d", i)
					row := &models.LocalMediaItem{ID: id, LibraryID: "local", RelativePath: id + ".mkv", FilePath: filepath.Join(library.RootPath, id+".mkv"), FileName: id + ".mkv", LibraryType: library.Type, DetectedTitle: id, MatchedName: id, MatchStatus: models.LocalMediaMatchStatusMatched, SizeBytes: int64(i + 1), CreatedAt: at, UpdatedAt: at}
					if err := f.base.store.LocalMedia().UpsertItem(t.Context(), row); err != nil {
						t.Fatal(err)
					}
				}
				q := models.LocalMediaItemListQuery{Filter: "matched", Sort: "name", Dir: "asc", Limit: 2, Offset: 2, IncludeMissing: true}
				expected, err := f.local.ListItems(t.Context(), "local", q)
				if err != nil {
					t.Fatal(err)
				}
				status, body := f.get(t, "/app/admin/api/library/libraries/local/items?filter=matched&sort=name&dir=asc&limit=2&offset=2", true)
				actual := mainCataloguePage(t, status, body)
				// Compare actual serialized repo/service output, including total
				// and pagination, without deriving it from a duplicate selector.
				expectedJSON, err := json.Marshal(expected)
				if err != nil {
					t.Fatal(err)
				}
				var want map[string]any
				var got map[string]any
				if json.Unmarshal(expectedJSON, &want) != nil || json.Unmarshal(body, &got) != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("local branch changed: %+v", actual)
				}
				if actual.Total != 5 || len(actual.Items) != 2 {
					t.Fatal("local full total/page lost")
				}
			}
		})
	}
}
