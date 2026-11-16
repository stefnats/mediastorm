package main

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRecordingWebExportProductionBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("local Playwright Python and Chromium required")
	}
	f := newRecordingWebFixture(t)
	server := httptest.NewServer(f.handler)
	defer server.Close()
	artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if artifacts == "" {
		artifacts = t.TempDir()
	}
	artifacts = filepath.Join(artifacts, "web-recording-export")
	if err := os.MkdirAll(artifacts, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := "/home/stefan/ops-runbook/tmp/ms-wde-browser"
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), python, filepath.Join("testdata", "recording_web_export_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual recording export browser: %v\n%s", err, out)
	}
}
