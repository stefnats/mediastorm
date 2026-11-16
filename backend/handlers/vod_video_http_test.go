package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type rootRawHTTPBody struct {
	io.ReadCloser
	closes   atomic.Int32
	reading  chan struct{}
	readOnce sync.Once
}

func (b *rootRawHTTPBody) Read(data []byte) (int, error) {
	b.readOnce.Do(func() { close(b.reading) })
	return b.ReadCloser.Read(data)
}
func (b *rootRawHTTPBody) Close() error { b.closes.Add(1); return b.ReadCloser.Close() }

type rootRawHTTPTransport struct {
	transport *http.Transport
	requests  atomic.Int32
	mu        sync.Mutex
	bodies    []*rootRawHTTPBody
}

func (t *rootRawHTTPTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.requests.Add(1)
	resp, err := t.transport.RoundTrip(r)
	if resp != nil && resp.Body != nil {
		body := &rootRawHTTPBody{ReadCloser: resp.Body, reading: make(chan struct{})}
		resp.Body = body
		t.mu.Lock()
		t.bodies = append(t.bodies, body)
		t.mu.Unlock()
	}
	return resp, err
}
func (t *rootRawHTTPTransport) singleBody(test *testing.T) *rootRawHTTPBody {
	test.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.bodies) != 1 {
		test.Fatalf("expected one actual HTTP body, got %d", len(t.bodies))
	}
	return t.bodies[0]
}
func rootRawHTTPClient(t *testing.T) (*http.Client, *rootRawHTTPTransport) {
	t.Helper()
	transport := &rootRawHTTPTransport{transport: http.DefaultTransport.(*http.Transport).Clone()}
	t.Cleanup(transport.transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}, transport
}
func rootRawHTTPContext(t *testing.T, h *VideoHandler, f *vodTestState, path string) (context.Context, context.CancelFunc) {
	t.Helper()
	r := f.request(http.MethodGet, "/synthetic?profileId=profile", "")
	parent, cancel := context.WithCancel(r.Context())
	r = r.WithContext(parent)
	_, r, finish, ok := h.beginVideoVOD(httptest.NewRecorder(), r, path)
	if !ok {
		cancel()
		t.Fatal("original current owner denied")
	}
	t.Cleanup(finish)
	t.Cleanup(cancel)
	return r.Context(), cancel
}
func rootRawHTTPWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("synthetic HTTP barrier timed out")
	}
}
func rootRawHTTPRetire(f *vodTestState) {
	f.change(func(f *vodTestState) { p := f.profiles["profile"]; p.IsKidsProfile = true; f.profiles[p.ID] = p })
}

func TestVODRawHTTPRedirectCannotOpenUnderRetiredOwner(t *testing.T) {
	h, _, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		io.WriteString(w, "SYNTHETIC-UNAUTHORIZED-REDIRECT")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rootRawHTTPRetire(f)
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer origin.Close()
	ctx, _ := rootRawHTTPContext(t, h, f, origin.URL)
	client, transport := rootRawHTTPClient(t)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin.URL, nil)
	resp, err := h.vodHTTPDo(client, r)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil {
		t.Fatal("retired redirect accepted")
	}
	if targetCalls.Load() != 0 || transport.requests.Load() != 1 {
		t.Fatalf("retired redirect caused further HTTP opens: target=%d transport=%d", targetCalls.Load(), transport.requests.Load())
	}
	if transport.singleBody(t).closes.Load() != 1 {
		t.Fatal("rejected redirect response must be closed exactly once by net/http")
	}
}
func TestVODRawHTTPPreservesAllowedRedirectAndActualBody(t *testing.T) {
	h, _, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "SYNTHETIC-OWNED-HTTP") }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	ctx, _ := rootRawHTTPContext(t, h, f, origin.URL)
	client, transport := rootRawHTTPClient(t)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin.URL, nil)
	resp, err := h.vodHTTPDo(client, r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != "SYNTHETIC-OWNED-HTTP" || transport.requests.Load() != 2 {
		t.Fatalf("owned HTTP control failed: body=%q err=%v opens=%d", data, err, transport.requests.Load())
	}
}

func TestVODRawHTTPRechecksAfterExistingRedirectPolicyCallback(t *testing.T) {
	h, _, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		io.WriteString(w, "SYNTHETIC-LATE-TARGET")
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer origin.Close()
	ctx, _ := rootRawHTTPContext(t, h, f, origin.URL)
	client, transport := rootRawHTTPClient(t)
	client.CheckRedirect = func(*http.Request, []*http.Request) error { rootRawHTTPRetire(f); return nil }
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin.URL, nil)
	resp, err := h.vodHTTPDo(client, r)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil || resp != nil || targetCalls.Load() != 0 || transport.requests.Load() != 1 {
		t.Fatalf("callback retirement reached target: response=%v error=%v target=%d opens=%d", resp != nil, err, targetCalls.Load(), transport.requests.Load())
	}
	if transport.singleBody(t).closes.Load() != 1 {
		t.Fatal("redirect callback denial did not close origin once")
	}
}
func TestVODRawHTTPLateHeadersDisposeActualReturnedBody(t *testing.T) {
	h, _, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	arrived, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		select {
		case <-release:
			io.WriteString(w, "SYNTHETIC-LATE-PRIVATE-HTTP")
		case <-r.Context().Done():
		}
	}))
	defer func() { once.Do(func() { close(release) }); server.Close() }()
	ctx, _ := rootRawHTTPContext(t, h, f, server.URL)
	client, transport := rootRawHTTPClient(t)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() { resp, err := h.vodHTTPDo(client, r); done <- result{resp, err} }()
	rootRawHTTPWait(t, arrived)
	rootRawHTTPRetire(f)
	once.Do(func() { close(release) })
	select {
	case got := <-done:
		if got.resp != nil {
			got.resp.Body.Close()
			t.Fatal("late HTTP response escaped")
		}
		if got.err == nil {
			t.Fatal("late HTTP headers accepted")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late HTTP did not complete")
	}
	if transport.singleBody(t).closes.Load() != 1 {
		t.Fatal("late actual body was not closed once")
	}
}
func TestVODRawHTTPActualBlockedBodyDropsLateBytes(t *testing.T) {
	h, _, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-release:
			io.WriteString(w, "SYNTHETIC-LATE-PRIVATE-BYTES")
		case <-r.Context().Done():
		}
	}))
	defer func() { once.Do(func() { close(release) }); server.Close() }()
	ctx, _ := rootRawHTTPContext(t, h, f, server.URL)
	client, transport := rootRawHTTPClient(t)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := h.vodHTTPDo(client, r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 128)
	started, done := make(chan struct{}), make(chan struct{})
	var n int
	var readErr error
	go func() { close(started); n, readErr = resp.Body.Read(buf); close(done) }()
	rootRawHTTPWait(t, started)
	rootRawHTTPWait(t, transport.singleBody(t).reading)
	rootRawHTTPRetire(f)
	once.Do(func() { close(release) })
	rootRawHTTPWait(t, done)
	if n != 0 || readErr == nil || strings.Contains(string(buf), "SYNTHETIC") {
		t.Fatalf("retired actual HTTP read exposed bytes: n=%d err=%v", n, readErr)
	}
	if transport.singleBody(t).closes.Load() != 1 {
		t.Fatal("retired actual HTTP body not closed once")
	}
}
func TestVODRawHTTPParentCancellationClosesBlockedActualRead(t *testing.T) {
	h, _, f, _ := rootRawVideoFixture(t, vodAccountActive, false)
	serverCancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(serverCancelled)
	}))
	defer server.Close()
	ctx, cancel := rootRawHTTPContext(t, h, f, server.URL)
	defer cancel()
	client, transport := rootRawHTTPClient(t)
	r, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := h.vodHTTPDo(client, r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	done := make(chan struct{})
	var n int
	var readErr error
	go func() { n, readErr = resp.Body.Read(make([]byte, 16)); close(done) }()
	rootRawHTTPWait(t, transport.singleBody(t).reading)
	cancel()
	rootRawHTTPWait(t, done)
	rootRawHTTPWait(t, serverCancelled)
	if n != 0 || readErr == nil {
		t.Fatalf("cancelled actual HTTP read=%d %v", n, readErr)
	}
	if transport.singleBody(t).closes.Load() != 1 {
		t.Fatal("cancelled actual HTTP body not closed once")
	}
}
