package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/services/recordings"
)

// Consume native production builder bytes over actual local HTTP and FFprobe.
// Authentication is injected over controlled authority ports, not login or SQL.
func TestNativeRecordingTimingRequests(t *testing.T) {
	dir := os.Getenv("NATIVE_RECORDING_TIMING_CONTRACT_DIR")
	if dir == "" {
		t.Skip("native recording timing request exports not configured")
	}
	queryFile := filepath.Join(dir, "native-recording-timing-queries.json")
	raw, err := os.ReadFile(queryFile)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != "f68868546698ffaa2bb23351869ffc0969425b9175e715556394cf7f73f9ea50" {
		t.Fatal("native request export changed")
	}
	var requests map[string]string
	if err := json.Unmarshal(raw, &requests); err != nil || len(requests) != 2 {
		t.Fatal("invalid native request export")
	}
	for _, key := range []string{"passive", "inspect"} {
		u, parseErr := url.Parse(requests[key])
		if parseErr != nil || u.Scheme != "" || u.Host != "" || u.User != nil || u.Fragment != "" || u.Query().Get("profileId") != "profile-a" {
			t.Fatal("unexpected native request identity")
		}
	}
	f := newExportHTTPFixture(t)
	// Rebind only synthetic fixture identity. Never rewrite the native URLs.
	profile := f.access.profiles["adult"]
	profile.ID = "profile-a"
	f.access.profiles[profile.ID] = profile
	f.repo.item.UserID = profile.ID
	mediaDir := filepath.Join(f.root, profile.ID, "recording-one")
	if err := os.MkdirAll(mediaDir, 0700); err != nil {
		t.Fatal(err)
	}
	media, err := exec.Command("/usr/bin/ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-t", "2", "-c:v", "libx264", "-preset", "ultrafast", "-f", "mpegts", "pipe:1").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mediaDir, "part-000001.ts"), media, 0600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "probe-count")
	quoted, _ := json.Marshal(marker)
	prober := filepath.Join(t.TempDir(), "probe")
	script := "#!/usr/bin/python3\nimport os,sys\nwith open(" + string(quoted) + ",'ab') as marker:\n marker.write(b'1')\nos.execv('/usr/bin/ffprobe',['/usr/bin/ffprobe']+sys.argv[1:])\n"
	if err := os.WriteFile(prober, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	f.h.service.(*recordings.Service).SetPartTimingProber(prober)
	originalSession := f.access.session
	router := mux.NewRouter()
	base := "/api/live/recordings/{recordingID}/parts/{partIndex}/timing"
	router.HandleFunc(base, f.h.GetPartTiming).Methods(http.MethodGet)
	router.HandleFunc(base+"/refresh", f.h.RefreshPartTiming).Methods(http.MethodPost)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), auth.ContextKeySession, originalSession)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, originalSession.AccountID)
		router.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	address := server.Listener.Addr().String()
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != address {
			return nil, fmt.Errorf("non-fixture dial rejected")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var observation []byte
	cases := []struct {
		name, key, method, state string
		status, probes           int
	}{
		{"initial passive", "passive", "GET", "unknown", 200, 0},
		{"explicit empty inspection", "inspect", "POST", "container_estimate", 200, 1},
		{"same saved observation", "passive", "GET", "container_estimate", 200, 1},
		{"wrong method cannot inspect", "inspect", "GET", "", 405, 1},
		{"retired original cannot read", "passive", "GET", "", 404, 1},
		{"retired original cannot inspect", "inspect", "POST", "", 404, 1},
	}
	for index, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if index == 4 {
				f.access.session.Token = "retired-synthetic-session"
			}
			req, err := http.NewRequest(c.method, server.URL+requests[c.key], nil)
			if err != nil {
				t.Fatal(err)
			}
			if req.ContentLength != 0 {
				t.Fatal("unexpected request body")
			}
			if c.status == 200 {
				if err := validator.ValidateRequest(req, nil); err != nil {
					t.Fatal(err)
				}
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
			_ = resp.Body.Close()
			if err != nil || len(body) > 8192 || resp.StatusCode != c.status {
				t.Fatalf("HTTP status=%d expected=%d error=%v", resp.StatusCode, c.status, err)
			}
			if bytes.Contains(body, []byte(f.root)) || bytes.Contains(body, []byte(originalSession.Token)) {
				t.Fatal("private data in response")
			}
			if c.status == 200 {
				if err := validator.ValidateResponse(req, resp.StatusCode, resp.Header, body); err != nil {
					t.Fatal(err)
				}
				var dto struct {
					RecordingID string `json:"recordingId"`
					PartIndex   int    `json:"partIndex"`
					State       string `json:"state"`
					Duration    int64  `json:"containerDurationMs"`
				}
				if err := json.Unmarshal(body, &dto); err != nil {
					t.Fatal(err)
				}
				if dto.RecordingID != "recording-one" || dto.PartIndex != 1 || dto.State != c.state {
					t.Fatal("native request did not bind the original part")
				}
				if c.state == "container_estimate" && (dto.Duration < 1500 || dto.Duration > 3000) {
					t.Fatal("actual synthetic TS estimate missing")
				}
				if index == 1 {
					observation = append([]byte(nil), body...)
				}
				if index == 2 && !bytes.Equal(observation, body) {
					t.Fatal("passive read renewed the observation")
				}
			}
			count, err := os.ReadFile(marker)
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			if len(count) != c.probes {
				t.Fatalf("actual prober executions=%d expected=%d", len(count), c.probes)
			}
		})
	}
	current, err := os.ReadFile(queryFile)
	if err != nil || !bytes.Equal(raw, current) {
		t.Fatal("native query bytes changed")
	}
}
