package handlers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
)

// Both operations always reach the strict collector. Without the explicitly
// selected synthetic PG, only actual unconfigured-handler denial is exercised.
// Positive responses use the existing genuine fixture and actual loopback IO.
func recordingProgressOpenAPIContract(t *testing.T, validator *openapi.Validator) {
	if os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL") == "" || runtime.GOOS != "linux" {
		handler := &RecordingProgressHandler{}
		target := "/api/live/recordings/synthetic-contract-progress/progress"
		vars := map[string]string{"recordingID": "synthetic-contract-progress"}
		exercise(t, validator, contractCall{method: http.MethodGet, target: target, vars: vars}, handler.ServeHTTP, 503)
		body := map[string]any{"action": "from-start", "expectedRevision": "0", "writerGeneration": "0", "incarnation": "11111111-1111-4111-8111-111111111111", "artifactId": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "mappingSha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "durationMs": 1000, "positionMs": 0}
		exercise(t, validator, contractCall{method: http.MethodPost, target: target, vars: vars, body: body}, handler.ServeHTTP, 503)
		return
	}
	base := newCurrentTimelineFixture(t)
	base.generateAndPrepare(t)
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = base.pin.store.Pool().Config().ConnConfig.ConnString()
	settings.Security.EnforceProfilePins = true
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	objects, err := settingscommit.OpenLockObjects(t.Context(), manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()
	coordinator, err := settingscommit.NewForPool(t.Context(), base.pin.store.Pool(), objects)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	guard, err := coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	security, err := config.InstallManagedSecurity(manager, coordinator, "")
	_ = guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer security.Close()
	handler, err := NewRecordingProgressHandler(base.pin.store, base.service, base.pin.pins, manager, security, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	defer handler.Close()
	target := "/api/live/recordings/" + base.job.ID + "/progress"
	perform := func(method string, raw []byte) []byte {
		t.Helper()
		done := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			checked, release, err := base.pin.pins.Authenticate(r, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), r.Header.Get("X-Client-ID"))
			if err != nil {
				http.Error(w, "current synthetic frame unavailable", 503)
				return
			}
			defer release()
			handler.ServeHTTP(w, mux.SetURLVars(checked, map[string]string{"recordingID": base.job.ID}))
		}))
		defer server.Close()
		request, err := http.NewRequestWithContext(t.Context(), method, server.URL+target, bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+base.pin.session.Token)
		request.Header.Set("X-Client-ID", "synthetic-client")
		if method == http.MethodPost {
			request.Header.Set("Content-Type", "application/json")
		}
		if err := validator.ValidateRequest(request, raw); err != nil {
			t.Fatal("actual request outside documented contract", err)
		}
		transport := &http.Transport{Proxy: nil}
		defer transport.CloseIdleConnections()
		response, err := (&http.Client{Transport: transport, Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			t.Fatal("actual response did not complete", readErr, closeErr)
		}
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			t.Fatal("actual original HTTP/frame cleanup not joined")
		}
		if response.StatusCode != 200 {
			t.Fatalf("genuine current source denied: %d %s", response.StatusCode, body)
		}
		if err := validator.ValidateResponse(request, response.StatusCode, response.Header, body); err != nil {
			t.Fatal("actual response outside documented contract", err)
		}
		return body
	}
	var snapshot recordingProgressSnapshotData
	if err := json.Unmarshal(perform(http.MethodGet, nil), &snapshot); err != nil || snapshot.Progress.PositionState != "none" {
		t.Fatal("genuine absent snapshot", err)
	}
	command, _ := json.Marshal(map[string]any{"action": datastore.RecordingProgressFromStart, "expectedRevision": snapshot.Progress.Revision, "writerGeneration": snapshot.Progress.WriterGeneration, "incarnation": snapshot.Timeline.Incarnation, "artifactId": snapshot.Timeline.ArtifactID, "mappingSha256": snapshot.Timeline.MappingRevision, "durationMs": snapshot.Timeline.DurationMS, "positionMs": 0})
	var result recordingProgressWriteData
	if err := json.Unmarshal(perform(http.MethodPost, command), &result); err != nil || result.Result.PositionMS != 0 || result.Result.ValidUntil.IsZero() {
		t.Fatal("genuine known write result", err)
	}
}
