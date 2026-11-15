package handlers

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRecordingRuleForecastProductionTemplateBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("Playwright Python and Chromium required")
	}
	server := httptest.NewServer(recordingPlanningBrowserFixtureMode(t, true))
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	if err := os.MkdirAll(artifacts, 0755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(python, filepath.Join("testdata", "recording_rule_forecast_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rule forecast browser: %v\n%s", err, output)
	}
}
