package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/services/streaming"
)

func TestHLSZeroRestartActualHTTPSourceAndControlTwoSequentialGETs(t *testing.T) {
	data := managedHLSMedia(t)
	var sourceCalls, active, maxActive atomic.Int32
	gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
	ended := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var old hlsSessionSnapshot
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(sourceCalls.Add(1)) - 1
		if i > 1 {
			http.Error(w, "unexpected input", 500)
			return
		}
		n := active.Add(1)
		defer func() { active.Add(-1); close(ended[i]) }()
		if n > maxActive.Load() {
			maxActive.Store(n)
		}
		if i == 1 {
			select {
			case <-ended[0]:
			default:
				t.Error("source GET2 preceded GET1 completion")
			}
			select {
			case <-old.input.done:
			default:
				t.Error("source GET2 preceded process/pump join")
			}
		}
		if r.Method != "GET" || r.Header.Get("Range") != "" || r.Header.Get("Authorization") != "" || r.Header.Get("X-Profile-Unlock") != "" {
			t.Error("wrong source method or forwarded account credentials")
		}
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(data[:1<<20])
		w.(http.Flusher).Flush()
		select {
		case <-gates[i]:
			_, _ = w.Write(data[1<<20:])
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(origin.Close)
	h, f, _ := zeroRestartFixture(t)
	previous := hlsRedirectHTTPClient
	transport := &rootRawHTTPTransport{transport: previous.Transport.(*http.Transport).Clone()}
	trackedClient := *previous
	trackedClient.Transport = transport
	hlsRedirectHTTPClient = &trackedClient
	t.Cleanup(func() { hlsRedirectHTTPClient = previous; transport.transport.CloseIdleConnections() })
	settings := config.DefaultSettings()
	settings.Server.AllowedPrivateMediaOrigins = []string{origin.URL}
	h.SetConfigManager(staticVideoConfigProvider{settings: settings})
	router := mux.NewRouter()
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			s, err := f.boundary.identity.CurrentSession(r.Context(), token)
			if err != nil {
				http.Error(w, "unauthorized", 401)
				return
			}
			ctx := context.WithValue(r.Context(), auth.ContextKeySession, s)
			ctx = context.WithValue(ctx, auth.ContextKeyAccountID, s.AccountID)
			ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, s.IsMaster)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
	router.Use(profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" }))
	router.HandleFunc("/api/video/hls/start", h.StartHLSSession)
	router.HandleFunc("/api/video/hls/{sessionID}/seek", h.SeekHLSSession)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	clientTransport := &http.Transport{Proxy: nil}
	t.Cleanup(clientTransport.CloseIdleConnections)
	client := &http.Client{Transport: clientTransport, Timeout: 10 * time.Second}
	request := func(method, path string, unlock bool) (int, []byte) {
		r, _ := http.NewRequest(method, server.URL+path, nil)
		r.Header.Set("Authorization", "Bearer "+f.session.Token)
		r.Header.Set("X-Client-ID", "device")
		if unlock {
			r.Header.Set("X-Profile-Unlock", f.token)
		}
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, b
	}
	status, body := request("GET", "/api/video/hls/start?"+url.Values{"path": {origin.URL + "/media.ts"}, "profileId": {"profile"}}.Encode(), true)
	if status != 200 {
		t.Fatalf("HTTP original start%d %s", status, body)
	}
	var v struct {
		ID string `json:"sessionId"`
	}
	_ = json.Unmarshal(body, &v)
	old, _ = h.hlsManager.snapshotHLSSession(v.ID)
	close(gates[0])
	assertZeroRestartMedia(t, old)
	<-ended[0]
	denied, _ := request("POST", "/api/video/hls/"+old.id+"/seek?time=0&profileId=profile", false)
	if denied != 403 || sourceCalls.Load() != 1 {
		t.Fatal("HTTP missing PIN opened source")
	}
	status, body = request("POST", "/api/video/hls/"+old.id+"/seek?time=0&profileId=profile", true)
	if status != 200 {
		t.Fatalf("HTTP restart%d %s", status, body)
	}
	fresh, ok := h.hlsManager.snapshotHLSSession(old.id)
	if !ok || fresh.input.owner != old.input.owner {
		t.Fatal("HTTP control changed original owner")
	}
	close(gates[1])
	assertZeroRestartMedia(t, fresh)
	<-ended[1]
	transport.mu.Lock()
	bodies := append([]*rootRawHTTPBody(nil), transport.bodies...)
	transport.mu.Unlock()
	if sourceCalls.Load() != 2 || transport.requests.Load() != 2 || len(bodies) != 2 || maxActive.Load() != 1 {
		t.Fatalf("actual source requests not two sequential GETs: %d/%d max%d", sourceCalls.Load(), transport.requests.Load(), maxActive.Load())
	}
	for _, b := range bodies {
		if b.closes.Load() != 1 {
			t.Fatalf("actual source body closes=%d", b.closes.Load())
		}
	}
}

func TestHLSZeroRestartOriginalDeadlineAndTrustedInvalidation(t *testing.T) {
	for _, mode := range []string{"deadline", "invalidation"} {
		t.Run(mode, func(t *testing.T) {
			h, f, p := zeroRestartFixture(t)
			req := managedHLSRequest(f)
			var cancel context.CancelFunc = func() {}
			if mode == "deadline" {
				ctx, c := context.WithTimeout(req.Context(), 2500*time.Millisecond)
				cancel = c
				req = req.WithContext(ctx)
			}
			defer cancel()
			w := httptest.NewRecorder()
			h.StartHLSSession(w, req)
			if w.Code != 200 {
				t.Fatal("original setup denied")
			}
			var v struct {
				ID string `json:"sessionId"`
			}
			_ = json.Unmarshal(w.Body.Bytes(), &v)
			old, _ := h.hlsManager.snapshotHLSSession(v.ID)
			deadline := old.input.owner.expires
			w = zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
			if w.Code != 200 {
				t.Fatalf("restart denied%d %s", w.Code, w.Body.String())
			}
			fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
			if !fresh.input.owner.expires.Equal(deadline) {
				t.Fatal("successor renewed authority")
			}
			if mode == "invalidation" {
				f.boundary.invalidateProfile("profile")
			}
			select {
			case <-fresh.input.ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("original retirement did not reach successor")
			}
			waitZeroRestartDone(t, fresh.input)
			if p.calls.Load() != 2 || p.bodies[1].closes.Load() != 1 {
				t.Fatal("retirement reopened or retained input")
			}
		})
	}
}
func TestHLSZeroRestartInvalidationDuringPostJoinRecheckCannotDisappear(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	prior := f.boundary.pin
	var once sync.Once
	f.boundary.pin = func(w http.ResponseWriter, r *http.Request, id string) bool {
		select {
		case <-old.input.done:
			once.Do(func() { f.boundary.invalidateProfile("profile") })
		default:
		}
		return prior(w, r, id)
	}
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 403 || p.calls.Load() != 1 {
		t.Fatal("post-join invalidation allowed a new GET")
	}
	select {
	case <-old.input.owner.ctx.Done():
	default:
		t.Fatal("original owner missing from invalidation registry")
	}
}
func TestHLSZeroRestartCancelBeforeSuccessorHandoffClosesOnlyNewInput(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	r := zeroRestartRequest(f, old.id)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	r = r.WithContext(ctx)
	p.bodies[1].onPrefix = cancel
	w := zeroRestartHTTP(h, r)
	if w.Code == 200 || p.calls.Load() != 2 || p.bodies[0].closes.Load() != 1 || p.bodies[1].closes.Load() != 1 {
		t.Fatalf("cancelled successor leaked status%d inputs%d closes%d/%d", w.Code, p.calls.Load(), p.bodies[0].closes.Load(), p.bodies[1].closes.Load())
	}
	if _, exists := h.hlsManager.snapshotHLSSession(old.id); exists {
		t.Fatal("cancelled unhanded successor remained published")
	}
}

type zeroRestartBlockingBody struct {
	data                             *bytes.Reader
	prefix                           int
	reading, closing, released       chan struct{}
	readOnce, closeOnce, releaseOnce sync.Once
	closes                           atomic.Int32
}

func (b *zeroRestartBlockingBody) Read(p []byte) (int, error) {
	if b.prefix > 0 {
		if len(p) > b.prefix {
			p = p[:b.prefix]
		}
		n, err := b.data.Read(p)
		b.prefix -= n
		return n, err
	}
	b.readOnce.Do(func() { close(b.reading) })
	<-b.released
	return 0, io.EOF
}
func (b *zeroRestartBlockingBody) Close() error {
	b.closeOnce.Do(func() { b.closes.Add(1); close(b.closing); <-b.released })
	return nil
}
func (b *zeroRestartBlockingBody) unblock() { b.releaseOnce.Do(func() { close(b.released) }) }

type zeroRestartBlockingProvider struct {
	body  *zeroRestartBlockingBody
	calls atomic.Int32
}

func (p *zeroRestartBlockingProvider) Stream(context.Context, streaming.Request) (*streaming.Response, error) {
	p.calls.Add(1)
	return &streaming.Response{Body: p.body, Status: 200, Headers: make(http.Header)}, nil
}
func TestHLSZeroRestartCancelledJoinRetainsSlotUntilRealSourceJoin(t *testing.T) {
	data := managedHLSMedia(t)
	b := &zeroRestartBlockingBody{data: bytes.NewReader(data), prefix: 1 << 20, reading: make(chan struct{}), closing: make(chan struct{}), released: make(chan struct{})}
	p := &zeroRestartBlockingProvider{body: b}
	h, f := managedHLSHandler(t, p)
	f.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v1"}
	})
	t.Cleanup(b.unblock)
	old := zeroRestartStart(t, h, f)
	select {
	case <-b.reading:
	case <-time.After(5 * time.Second):
		t.Fatal("old actual reader not blocked")
	}
	r := zeroRestartRequest(f, old.id)
	ctx, cancel := context.WithCancel(r.Context())
	r = r.WithContext(ctx)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- zeroRestartHTTP(h, r) }()
	select {
	case <-b.closing:
	case <-time.After(5 * time.Second):
		t.Fatal("restart did not close owned old input")
	}
	cancel()
	select {
	case w := <-result:
		if w.Code != 403 {
			t.Fatal("cancelled join reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("HTTP cancellation waited on uncooperative old input")
	}
	h.hlsManager.vodInputMu.Lock()
	held := h.hlsManager.vodInputs[old.input]
	h.hlsManager.vodInputMu.Unlock()
	if !held || old.input.owner.ctx.Err() != nil || p.calls.Load() != 1 {
		t.Fatal("slot/authority was released before actual join")
	}
	b.unblock()
	waitZeroRestartDone(t, old.input)
	select {
	case <-old.input.owner.ctx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("joined abandoned original authority leaked")
	}
	if b.closes.Load() != 1 || p.calls.Load() != 1 {
		t.Fatal("abandoned join reopened/double closed")
	}
}
func TestHLSZeroRestartOldGenerationCannotOverwriteReplacementFiles(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 200 {
		t.Fatal("replacement setup failed")
	}
	fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
	close(p.bodies[1].gate)
	assertZeroRestartMedia(t, fresh)
	before, err := os.ReadFile(filepath.Join(fresh.output, "segment0.ts"))
	if err != nil {
		t.Fatal(err)
	}
	old.input.releaseRetired()
	if h.hlsManager.cleanupHLSSessionSnapshot(old) {
		t.Fatal("stale snapshot cleanup succeeded")
	}
	h.hlsManager.removeDetachedHLSInputOutput(old)
	after, err := os.ReadFile(filepath.Join(fresh.output, "segment0.ts"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("late old cleanup damaged replacement")
	}
}
