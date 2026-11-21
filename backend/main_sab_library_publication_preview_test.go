//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"novastream/internal/datastore"
	"novastream/services/localmedia"
	"novastream/utils"
)

type publicationPreviewCountedBody struct {
	io.Reader
	reads int
}

func (b *publicationPreviewCountedBody) Read(p []byte) (int, error) {
	b.reads++
	return b.Reader.Read(p)
}
func (*publicationPreviewCountedBody) Close() error { return nil }

func assertPublicationPreviewUnchanged(t *testing.T, f *mainSABPublicationFixture) {
	t.Helper()
	var items, pg, sqlite int
	if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM local_media_items`).Scan(&items); err != nil {
		t.Fatal(err)
	}
	if err := f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM sab_library_publications`).Scan(&pg); err != nil {
		t.Fatal(err)
	}
	if err := f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&sqlite); err != nil {
		t.Fatal(err)
	}
	if items != 0 || pg != 0 || sqlite != 0 {
		t.Fatal("preview or rejected selection changed actual catalogue/reservation", items, pg, sqlite)
	}
	if _, err := os.Lstat(filepath.Join(f.root, ".mediastorm-publications")); !os.IsNotExist(err) {
		t.Fatal("preview created a destination namespace", err)
	}
}

func TestMainSABPublicationPreviewRoutes(t *testing.T) {
	for _, kind := range []string{"actual-basepath-preview", "origin-required", "cookie-required", "malformed-selection-zero-reserve", "missing-runtime-before-body", "status-recorded-selection-after-publish"} {
		t.Run(kind, func(t *testing.T) {
			f := mainSABPublicationPG(t)
			if kind == "missing-runtime-before-body" {
				// Run the actual Main route factory against an unread original body.
				// No auth/source runtime exists, so even a deliberately invalid cookie
				// cannot trigger body collection or a legacy route fallback.
				fallback := utils.NewRouter()
				registerSABLibraryPublicationRoutes(fallback, f.admin, nil)
				body := &publicationPreviewCountedBody{Reader: strings.NewReader("{")}
				r := httptest.NewRequest("POST", "/admin/api/usenet/fallback/1/publication/preview", body)
				r.ContentLength = 1000
				r.Header.Set("Cookie", "strmr_admin_session=synthetic-invalid")
				w := httptest.NewRecorder()
				fallback.ServeHTTP(w, r)
				if w.Code != 503 || body.reads != 0 {
					t.Fatal("missing Main runtime consumed body or fell through", w.Code, body.reads)
				}
				assertPublicationPreviewUnchanged(t, f)
				return
			}
			server := httptest.NewServer(f.entry)
			t.Cleanup(server.Close)
			body := `{"libraryId":"publication-library","ordinals":[1]}`
			if kind == "origin-required" {
				for _, origin := range []string{"", "https://foreign.invalid"} {
					code, _ := f.request(t, server, "POST", "/preview", body, origin, true)
					if code != 403 {
						t.Fatal("preview bypassed actual Main origin guard", code)
					}
				}
				assertPublicationPreviewUnchanged(t, f)
				return
			}
			if kind == "cookie-required" {
				code, _ := f.request(t, server, "POST", "/preview", body, server.URL, false)
				if code != 403 {
					t.Fatal("preview bypassed actual master cookie", code)
				}
				assertPublicationPreviewUnchanged(t, f)
				return
			}
			if kind == "malformed-selection-zero-reserve" {
				for _, malformed := range []string{`{"libraryId":"publication-library","ordinals":[1],"path":"/foreign"}`, `{"libraryId":"publication-library","ordinals":[1],"titleId":"999"}`, `{"libraryId":"publication-library","ordinals":[1,1]}`, `{"libraryId":"publication-library","libraryId":"other","ordinals":[1]}`} {
					code, _ := f.request(t, server, "POST", "/preview", malformed, server.URL, true)
					if code != 400 {
						t.Fatal("preview accepted malformed selectors", code)
					}
				}
				assertPublicationPreviewUnchanged(t, f)
				return
			}
			if kind == "status-recorded-selection-after-publish" {
				code, raw := f.request(t, server, "POST", "", body, server.URL, true)
				var view datastore.SABLibraryPublicationView
				if code != 200 || json.Unmarshal(raw, &view) != nil || view.State != "registered" {
					t.Fatal("actual publication failed before descriptive status", code)
				}
				code, raw = f.request(t, server, "GET", "", "", "", true)
				if code != 200 || json.Unmarshal(raw, &view) != nil || view.LibraryID != "publication-library" || len(view.Ordinals) != 1 || view.Ordinals[0] != 1 || len(view.Files) != 1 || view.Files[0].Ordinal != 1 {
					t.Fatal("fresh status lost exact recorded selection", code)
				}
				return
			}
			code, raw := f.request(t, server, "POST", "/preview", body, server.URL, true)
			var view localmedia.SABLibraryPublicationPreviewView
			if code != 200 || json.Unmarshal(raw, &view) != nil || view.QueueID != 1 || view.LibraryID != "publication-library" || len(view.Files) != 1 || !view.Files[0].Eligible || view.Files[0].Container != "matroska" || view.Files[0].Bytes != int64(len(f.payload)) {
				t.Fatal("actual prefixed Main preview did not inspect saved media", code)
			}
			if bytes.Contains(raw, []byte(f.root)) || bytes.Contains(raw, []byte("sha256")) || bytes.Contains(raw, []byte("token")) {
				t.Fatal("preview exposed private evidence")
			}
			assertPublicationPreviewUnchanged(t, f)
		})
	}
}
