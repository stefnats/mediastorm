package main

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRecordingWebDerivedProductionBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("local Playwright Python and Chromium required")
	}
	fixture := newDerivedWebFixture(t)
	server := httptest.NewServer(fixture.handler)
	defer server.Close()
	dir := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
	if dir == "" {
		dir = t.TempDir()
	}
	dir = filepath.Join(dir, "web-recording-derived")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	scratch := os.Getenv("MEDIASTORM_BROWSER_TMPDIR")
	if scratch == "" {
		scratch = t.TempDir()
	}
	if err := os.MkdirAll(scratch, 0700); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), python, filepath.Join("testdata", "recording_web_derived_browser.py"), "--base-url", server.URL, "--chromium", chromium, "--artifacts", dir)
	command.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual joined recording browser: %v\n%s", err, output)
	}
}
