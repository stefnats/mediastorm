package arrclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testDirectory = "/synthetic-owned-copy/Film ü"
const testDownload = "synthetic-download"

func commandACK(kind, status string, id int64) string {
	name := "DownloadedMoviesScan"
	if kind == "sonarr" {
		name = "DownloadedEpisodesScan"
	}
	result := "unknown"
	if status == "completed" {
		result = "successful"
	}
	raw, _ := json.Marshal(map[string]any{"id": id, "name": name, "status": status, "result": result, "body": map[string]any{"path": testDirectory, "downloadClientId": testDownload, "importMode": "copy"}})
	return string(raw)
}

func testClient(t *testing.T, kind string, server *httptest.Server, suffix string) *Client {
	t.Helper()
	c, err := New(t.Context(), Config{kind, server.URL + suffix, "synthetic-key"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("actual operation did not finish")
	}
}

func pending(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("physical operation ended before release")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestARRCopyCommandProtocol(t *testing.T) {
	for _, kind := range []string{"sonarr-copy-command", "radarr-copy-command", "configured-base-path", "exact-command-status", "ambiguous-submission-not-retried", "wrong-ack-identity", "unexpected-status-denied"} {
		t.Run(kind, func(t *testing.T) {
			backend := "radarr"
			if kind == "sonarr-copy-command" {
				backend = "sonarr"
			}
			var calls atomic.Int32
			var unsuccessful atomic.Bool
			var pathSeen atomic.Value
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				pathSeen.Store(r.URL.Path)
				if r.Header.Get("X-Api-Key") != "synthetic-key" || r.URL.RawQuery != "" || r.Header.Get("Cookie") != "" {
					t.Error("credential transport changed")
				}
				if kind == "exact-command-status" {
					if r.Method != "GET" || r.URL.Path != "/api/v3/command/7" {
						t.Error("status request changed")
					}
					payload := commandACK(backend, "completed", 7)
					if unsuccessful.Load() {
						payload = strings.Replace(payload, `"result":"successful"`, `"result":"unsuccessful"`, 1)
					}
					_, _ = io.WriteString(w, payload)
					return
				}
				var body struct{ Name, Path, DownloadClientID, ImportMode string }
				if json.NewDecoder(r.Body).Decode(&body) != nil || r.Method != "POST" || body.Path != testDirectory || body.DownloadClientID != testDownload || body.ImportMode != "copy" {
					t.Error("copy-only actual command changed")
				}
				expectedName := "DownloadedMoviesScan"
				if backend == "sonarr" {
					expectedName = "DownloadedEpisodesScan"
				}
				if body.Name != expectedName {
					t.Error("wrong actual backend command")
				}
				if kind == "ambiguous-submission-not-retried" {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				response := commandACK(backend, "queued", 7)
				if kind == "sonarr-copy-command" {
					response = strings.Replace(commandACK(backend, "completed", 7), `"result":"successful"`, `"result":"indeterminate"`, 1)
				}
				if kind == "wrong-ack-identity" {
					response = strings.Replace(response, testDownload, "another-download", 1)
				}
				if kind == "unexpected-status-denied" {
					response = commandACK(backend, "future-success", 7)
				}
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, response)
			}))
			t.Cleanup(server.Close)
			suffix := ""
			if kind == "configured-base-path" {
				suffix = "/arr/"
			}
			c := testClient(t, backend, server, suffix)
			var out Command
			var err error
			if kind == "exact-command-status" {
				out, err = c.Status(t.Context(), 7)
			} else {
				out, err = c.SubmitCopy(t.Context(), testDirectory, testDownload)
			}
			if kind == "ambiguous-submission-not-retried" || kind == "wrong-ack-identity" || kind == "unexpected-status-denied" {
				if !errors.Is(err, ErrUnknown) || out.ID != 0 || calls.Load() != 1 {
					t.Fatal("uncertain acknowledgement adopted or retried", err, out.ID, calls.Load())
				}
				return
			}
			if err != nil || out.ID != 7 || calls.Load() != 1 {
				t.Fatal("actual acknowledgement missing", err, out.ID)
			}
			if kind == "configured-base-path" && pathSeen.Load() != "/arr/api/v3/command" {
				t.Fatal("configured prefix lost")
			}
			if kind == "sonarr-copy-command" && (out.Status != "completed" || out.Result != "indeterminate") {
				t.Fatal("indeterminate Sonarr command result was promoted or lost")
			}
			if kind == "exact-command-status" {
				if out.Status != "completed" || out.Result != "successful" {
					t.Fatal("actual command status/result lost")
				}
				unsuccessful.Store(true)
				out, err = c.Status(t.Context(), 7)
				if err != nil || out.ID != 7 || out.Status != "completed" || out.Result != "unsuccessful" || calls.Load() != 2 {
					t.Fatal("completed command status concealed an unsuccessful result", err)
				}
			}
		})
	}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type heldClose struct {
	io.ReadCloser
	entered, release chan struct{}
}

func (b *heldClose) Close() error {
	err := b.ReadCloser.Close()
	close(b.entered)
	<-b.release
	return err
}

func TestARRClientTransportBounds(t *testing.T) {
	for _, kind := range []string{"redirect-no-key-forwarding", "caller-cancel-held-response", "client-close-joins-held-body", "closed-client-no-request", "response-byte-cap", "invalid-json-and-duplicate-key", "early-fixed-deadline", "four-request-admission"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "redirect-no-key-forwarding" {
				var forwarded atomic.Int32
				target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1) }))
				t.Cleanup(target.Close)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL+"/foreign", 307) }))
				t.Cleanup(server.Close)
				c := testClient(t, "radarr", server, "")
				if out, err := c.SubmitCopy(t.Context(), testDirectory, testDownload); !errors.Is(err, ErrUnknown) || out.ID != 0 || forwarded.Load() != 0 {
					t.Fatal("redirect followed or adopted")
				}
				return
			}
			if kind == "caller-cancel-held-response" || kind == "early-fixed-deadline" {
				entered, ended, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				fixtureEnd := make(chan struct{})
				var fixtureOnce sync.Once
				finishFixture := func() { fixtureOnce.Do(func() { close(fixtureEnd) }) }
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(ended)
					// Drain the actual finite request before waiting for transport
					// cancellation, so net/http can observe the connection ending.
					if _, err := io.Copy(io.Discard, r.Body); err != nil {
						return
					}
					w.WriteHeader(200)
					_ = http.NewResponseController(w).Flush()
					close(entered)
					select {
					case <-r.Context().Done():
					case <-fixtureEnd:
					}
				}))
				t.Cleanup(server.Close)
				c := testClient(t, "sonarr", server, "")
				ctx, cancel := context.WithCancel(t.Context())
				if kind == "early-fixed-deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 150*time.Millisecond)
				}
				t.Cleanup(cancel)
				var out Command
				var err error
				started := time.Now()
				go func() { defer close(done); out, err = c.SubmitCopy(ctx, testDirectory, testDownload) }()
				t.Cleanup(func() { cancel(); finishFixture(); c.Close(); wait(t, done) })
				wait(t, entered)
				if kind == "caller-cancel-held-response" {
					cancel()
				}
				wait(t, done)
				wait(t, ended)
				if !errors.Is(err, ErrUnknown) || out.ID != 0 || time.Since(started) > 2*time.Second {
					t.Fatal("original cancellation/deadline lost", err)
				}
				return
			}
			if kind == "client-close-joins-held-body" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.WriteString(w, commandACK("radarr", "queued", 7))
				}))
				t.Cleanup(server.Close)
				c := testClient(t, "radarr", server, "")
				entered, release, done, closed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				c.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
					response, err := c.transport.RoundTrip(r)
					if err == nil {
						response.Body = &heldClose{response.Body, entered, release}
					}
					return response, err
				})
				var out Command
				var err error
				closeStarted := false
				t.Cleanup(func() {
					unblock()
					c.Close()
					wait(t, done)
					if closeStarted {
						wait(t, closed)
					}
				})
				go func() { defer close(done); out, err = c.SubmitCopy(t.Context(), testDirectory, testDownload) }()
				wait(t, entered)
				closeStarted = true
				go func() { defer close(closed); c.Close() }()
				pending(t, closed)
				pending(t, done)
				unblock()
				wait(t, done)
				wait(t, closed)
				if out.ID != 0 || !errors.Is(err, ErrUnknown) {
					t.Fatal("retired physical close published acknowledgement", err)
				}
				return
			}
			if kind == "four-request-admission" {
				entered := make(chan struct{}, 4)
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					entered <- struct{}{}
					<-release
					_, _ = io.WriteString(w, commandACK("radarr", "started", 7))
				}))
				t.Cleanup(server.Close)
				c := testClient(t, "radarr", server, "")
				var workers sync.WaitGroup
				t.Cleanup(func() { unblock(); c.Close(); workers.Wait() })
				for i := 0; i < 4; i++ {
					workers.Add(1)
					go func() { defer workers.Done(); _, _ = c.Status(t.Context(), 7) }()
				}
				for i := 0; i < 4; i++ {
					wait(t, entered)
				}
				if _, err := c.Status(t.Context(), 7); !errors.Is(err, ErrBusy) || calls.Load() != 4 {
					t.Fatal("unbounded actual HTTP admission", err)
				}
				unblock()
				workers.Wait()
				return
			}
			payloads := []string{commandACK("radarr", "queued", 7)}
			if kind == "invalid-json-and-duplicate-key" {
				payloads = []string{`{"id":7,"id":8,"name":"DownloadedMoviesScan","status":"queued"}`, `{"id":`, commandACK("radarr", "queued", 7) + `{}`, strings.Replace(commandACK("radarr", "queued", 7), `"result":"unknown"`, `"result":"future-success"`, 1)}
			}
			for _, payload := range payloads {
				if kind == "response-byte-cap" {
					payload = strings.Repeat(" ", maxResponseBytes+1)
				}
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); _, _ = io.WriteString(w, payload) }))
				t.Cleanup(server.Close)
				c := testClient(t, "radarr", server, "")
				if kind == "closed-client-no-request" {
					c.Close()
				}
				out, err := c.Status(t.Context(), 7)
				if err == nil || out.ID != 0 {
					t.Fatal("invalid/closed response adopted")
				}
				if kind == "closed-client-no-request" && calls.Load() != 0 {
					t.Fatal("closed client started HTTP")
				}
			}
		})
	}
}

func TestARRClientConfiguration(t *testing.T) {
	for _, kind := range []string{"unsupported-kind", "credentialed-or-query-base-denied", "malformed-key-denied", "nonabsolute-copy-path-denied", "cancelled-caller-no-work"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "unsupported-kind" || kind == "credentialed-or-query-base-denied" || kind == "malformed-key-denied" {
				config := Config{"radarr", "http://127.0.0.1:1", "synthetic-key"}
				if kind == "unsupported-kind" {
					config.Kind = "other"
				}
				if kind == "credentialed-or-query-base-denied" {
					config.BaseURL = "http://synthetic-user:synthetic-pass@127.0.0.1:1/?apikey=synthetic"
				}
				if kind == "malformed-key-denied" {
					config.APIKey = "synthetic\nkey"
				}
				if c, err := New(t.Context(), config); !errors.Is(err, ErrConfiguration) || c != nil {
					if c != nil {
						c.Close()
					}
					t.Fatal("invalid configuration accepted")
				}
				if strings.Contains(fmt.Sprintf("%+v", config), "synthetic") {
					t.Fatal("configuration formatting exposed private values")
				}
				return
			}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
			t.Cleanup(server.Close)
			c := testClient(t, "radarr", server, "")
			ctx := t.Context()
			directory := "../foreign"
			if kind == "cancelled-caller-no-work" {
				original, cancel := context.WithCancel(ctx)
				cancel()
				ctx = original
				directory = testDirectory
			}
			if out, err := c.SubmitCopy(ctx, directory, testDownload); err == nil || out.ID != 0 || calls.Load() != 0 {
				t.Fatal("invalid original input started HTTP")
			}
		})
	}
}
