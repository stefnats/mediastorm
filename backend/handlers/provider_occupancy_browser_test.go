package handlers

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestProviderOccupancyProductionSettingsBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("Playwright Python and Chromium required")
	}
	// Reuse the genuine cookie-authenticated settings/report fixture; its only
	// failure control expires its own synthetic cache and changes the provider
	// fixture to unavailable. No prototype response replaces the real handler.
	server := httptest.NewServer(newAutomaticProviderLimitsBrowserFixture(t))
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	artifacts = filepath.Join(artifacts, "provider-occupancy")
	if err := os.MkdirAll(artifacts, 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join("testdata", "provider_occupancy_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("provider occupancy browser: %v\n%s", err, output)
	}
}
