package main

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRecordingWebTimingProductionBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("local Playwright Python and Chromium required")
	}
	f := newTimingWebFixture(t)
	server := httptest.NewServer(f.handler)
	defer server.Close()
	dir := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if dir == "" {
		dir = t.TempDir()
	}
	dir = filepath.Join(dir, "web-recording-timing")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := "/home/stefan/ops-runbook/tmp/ms-wdt-browser"
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), python, filepath.Join("testdata", "recording_web_timing_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", dir)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual recording timing browser: %v\n%s", err, output)
	}
}

func TestRecordingWebTimingDelayedResponseBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("local Playwright Python and Chromium required")
	}
	f := newTimingWebFixture(t)
	server := httptest.NewServer(f.handler)
	defer server.Close()
	dir := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if dir == "" {
		dir = t.TempDir()
	}
	dir = filepath.Join(dir, "web-recording-timing-delay")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := "/home/stefan/ops-runbook/tmp/ms-wdt-delay"
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), python, filepath.Join("testdata", "recording_web_timing_delay_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", dir)
	cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("actual delayed server response browser: %v\n%s", err, output)
	}
}
