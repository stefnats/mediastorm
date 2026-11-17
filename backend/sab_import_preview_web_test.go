package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"novastream/internal/importer"
)

// Extend the existing synthetic carrier port with the real preview operation.
// Its production counterpart is importer.Service, which already has this method.
func (s managedWebService) PreviewSABImport(ctx context.Context, id int64, settings importer.SABImportPreviewSettings, authorize func(context.Context) error) (importer.SABImportPreview, error) {
	return s.f.service.PreviewSABImport(ctx, id, settings, authorize)
}

func importPreviewWebFixture(t *testing.T) (*managedWebFixture, *atomic.Int32, http.Handler) {
	t.Helper()
	f := newManagedWebFixture(t)
	settings, err := f.manager.Load()
	if err != nil {
		t.Fatal(err)
	}
	// Inspection remains useful when managed copying is deliberately disabled.
	settings.SABnzbd.ImportStageTargetRoot = ""
	settings.SABnzbd.ImportStageMaxBytes = 0
	if err := f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	previews := &atomic.Int32{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fixture/preview-info" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int32{"previews": previews.Load(), "provider": f.provider.Load(), "copies": f.copies.Load(), "reads": f.reads.Load(), "denied": f.guard.denied.Load()})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/admin/api/usenet/fallback/") && strings.HasSuffix(r.URL.Path, "/import-preview") {
			previews.Add(1)
		}
		f.handler.ServeHTTP(w, r)
	})
	return f, previews, handler
}

func TestSABImportPreviewWebActualCookieMetadataWithoutCopyConfiguration(t *testing.T) {
	f, previews, handler := importPreviewWebFixture(t)
	server := f.start(handler)
	r, err := http.NewRequest(http.MethodPost, server.URL+"/admin/api/usenet/fallback/53/import-preview", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Origin", server.URL)
	r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.token})
	response, err := (&http.Client{Transport: f.guard}).Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var value importer.SABImportPreview
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&value) != nil || value.State != "local_inventory" || value.QueueID != 53 || value.EntryCount != 1 || len(value.Files) != 1 || value.Files[0].Extension != "mkv" || value.ObservedBytes != int64(len("synthetic managed copy bytes")) {
		t.Fatal("actual cookie preview did not expose the complete safe inventory")
	}
	if previews.Load() != 1 || f.provider.Load() != 2 || f.copies.Load() != 0 || f.reads.Load() != 0 || f.guard.denied.Load() != 0 {
		t.Fatal("metadata inspection copied, fetched a saved copy or made unexpected requests")
	}
	var stages int
	if err := f.db.Connection().QueryRow("SELECT count(*) FROM sab_import_stages").Scan(&stages); err != nil || stages != 0 {
		t.Fatal("preview created a managed-copy operation")
	}
	entries, err := os.ReadDir(f.target)
	if err != nil || len(entries) != 0 {
		t.Fatal("preview populated the managed target")
	}
}

func TestSABImportPreviewWebProductionBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("dedicated local browser fixture required")
	}
	f, _, handler := importPreviewWebFixture(t)
	server := f.start(handler)
	artifact := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifact == "" {
		artifact = t.TempDir()
	}
	artifact = filepath.Join(artifact, "import-preview-web")
	if err := os.MkdirAll(artifact, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := "/home/stefan/ops-runbook/tmp/ms-ipw-browser"
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), python, "testdata/sab_import_preview_web_browser.py", "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifact)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("synthetic file-inspection browser: %v\n%s", err, output)
	}
}
