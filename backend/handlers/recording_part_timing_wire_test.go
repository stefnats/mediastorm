package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/services/recordings"
)

// This fixture proves actual local HTTP/handler/service/FFprobe FD serialization.
// Its server-injected auth is not password login; api.Register coverage is separate.
func TestRecordingPartTimingActualHTTPWire(t *testing.T) {
	f := newExportHTTPFixture(t)
	service := f.h.service.(*recordings.Service)
	service.SetPartTimingProber("/usr/bin/ffprobe")
	media, err := exec.Command("/usr/bin/ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-t", "2", "-c:v", "libx264", "-preset", "ultrafast", "-f", "mpegts", "pipe:1").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(f.root, "adult", "recording-one", "part-000001.ts"), media, 0600); err != nil {
		t.Fatal(err)
	}
	router := mux.NewRouter()
	base := "/api/live/recordings/{recordingID}/parts/{partIndex}/timing"
	router.HandleFunc(base, f.h.GetPartTiming).Methods("GET")
	router.HandleFunc(base+"/refresh", f.h.RefreshPartTiming).Methods("POST")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), auth.ContextKeySession, f.access.session)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.access.session.AccountID)
		router.ServeHTTP(w, r.WithContext(ctx))
	}))
	defer server.Close()
	address := server.Listener.Addr().String()
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != address {
			return nil, fmt.Errorf("non-fixture dial rejected")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	fixtures := map[string][]byte{}
	request := func(name, method, path string, want int) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != want {
			t.Fatalf("wire %s status %d %v", name, response.StatusCode, err)
		}
		if bytes.Contains(raw, []byte(f.root)) || bytes.Contains(raw, []byte("PRIVATE_")) {
			t.Fatal("private wire data")
		}
		fixtures[name] = raw
	}
	path := "/api/live/recordings/recording-one/parts/1/timing?profileId=adult"
	request("unknown", "GET", path, 200)
	request("container-estimate", "POST", "/api/live/recordings/recording-one/parts/1/timing/refresh?profileId=adult", 200)
	request("passive-same-observation", "GET", path, 200)
	if !bytes.Equal(fixtures["container-estimate"], fixtures["passive-same-observation"]) {
		t.Fatal("passive HTTP changed observed timestamp")
	}
	request("invalid-index", "GET", "/api/live/recordings/recording-one/parts/01/timing?profileId=adult", 400)
	f.repo.mu.Lock()
	f.repo.item.Status = "cancelled"
	f.repo.mu.Unlock()
	request("ledger-changed", "GET", path, 200)
	if dir := os.Getenv("DVR_PART_TIMING_CONTRACT_DIR"); dir != "" {
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		for name, raw := range fixtures {
			if err = os.WriteFile(filepath.Join(dir, name+".json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
		}
		manifest, _ := json.MarshalIndent(map[string]any{"producer": "TestRecordingPartTimingActualHTTPWire", "scope": "actual synthetic HTTP/guarded service/FFprobe FD; injected auth, no login/provider claim", "count": len(fixtures)}, "", "  ")
		if err = os.WriteFile(filepath.Join(dir, "manifest.json"), manifest, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
