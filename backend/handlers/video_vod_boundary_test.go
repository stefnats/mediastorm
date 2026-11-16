package handlers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/services/streaming"
)

type rootRawVideoBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *rootRawVideoBody) Close() error { b.closed.Add(1); return nil }

type rootRawVideoProvider struct {
	streams atomic.Int32
	directs atomic.Int32
	after   func()
	body    *rootRawVideoBody
}

func (p *rootRawVideoProvider) Stream(ctx context.Context, req streaming.Request) (*streaming.Response, error) {
	p.streams.Add(1)
	data := "SYNTHETIC-FRESH-OWNED-BYTES"
	status := http.StatusOK
	headers := make(http.Header)
	if start, end, ok := parseByteRange(req.RangeHeader); ok && start >= 0 && end < int64(len(data)) {
		headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
		data = data[start : end+1]
		status = http.StatusPartialContent
	}
	p.body = &rootRawVideoBody{Reader: strings.NewReader(data)}
	if p.after != nil {
		p.after()
	}
	return &streaming.Response{Status: status, Body: p.body, Headers: headers, ContentLength: int64(len(data))}, nil
}
func (p *rootRawVideoProvider) GetDirectURL(context.Context, string) (string, error) {
	p.directs.Add(1)
	if p.after != nil {
		p.after()
	}
	return "https://synthetic.invalid/PRIVATE-DIRECT-OUTPUT", nil
}
func rootRawVideoFixture(t *testing.T, mode vodApplicabilityMode, kid bool) (*VideoHandler, *rootRawVideoProvider, *vodTestState, string) {
	t.Helper()
	p := &rootRawVideoProvider{}
	h := NewVideoHandlerWithProvider(false, "", "", "", p)
	t.Cleanup(h.Shutdown)
	f := newVODTestState(t, mode, kid)
	h.vodBoundary = f.boundary
	path := "debrid/synthetic/fixture.mkv"
	h.rangeCache.put(path, 0, []byte("SYNTHETIC-PRIVATE-CACHED-BYTES"))
	h.setCachedMetadata(path, &videoMetadataResponse{Path: "SYNTHETIC-PRIVATE-CACHED-METADATA", DurationSeconds: 42})
	return h, p, f, path
}
func rootRawVideoInvoke(h *VideoHandler, action string, w http.ResponseWriter, r *http.Request) {
	switch action {
	case "stream":
		r.Header.Set("Range", "bytes=0-8")
		h.StreamVideo(w, r)
	case "metadata":
		h.ProbeVideo(w, r)
	case "direct":
		h.GetDirectURL(w, r)
	}
}
func TestVideoVODBoundaryDeniesBeforeCachedOrProviderOutput(t *testing.T) {
	for _, mode := range []string{"unknown", "policy error", "child", "missing profile", "wrong profile", "duplicate profile", "case profile", "missing PIN", "revoked session", "expired session", "spoofed master", "conflicting client"} {
		for _, action := range []string{"stream", "metadata", "direct"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				h, p, f, path := rootRawVideoFixture(t, vodAccountActive, mode == "child")
				r := f.request(http.MethodGet, "/api/video/stream?transmux=0&profileId=profile&path="+url.QueryEscape(path), "")
				switch mode {
				case "unknown":
					f.change(func(f *vodTestState) { f.states["account"] = vodAccountApplicability{} })
				case "policy error":
					f.change(func(f *vodTestState) { f.failure = errors.New("synthetic policy unavailable") })
				case "missing profile":
					q := r.URL.Query()
					q.Del("profileId")
					r.URL.RawQuery = q.Encode()
				case "wrong profile":
					q := r.URL.Query()
					q.Set("profileId", "foreign")
					r.URL.RawQuery = q.Encode()
				case "duplicate profile":
					q := r.URL.Query()
					q.Add("profileId", "foreign")
					r.URL.RawQuery = q.Encode()
				case "case profile":
					q := r.URL.Query()
					q.Add("ProfileID", "foreign")
					r.URL.RawQuery = q.Encode()
				case "missing PIN":
					r.Header.Del("X-Profile-Unlock")
				case "revoked session":
					f.change(func(f *vodTestState) { f.session.Token = "replacement-session" })
				case "expired session":
					f.change(func(f *vodTestState) { f.session.ExpiresAt = time.Now().Add(-time.Second) })
				case "spoofed master":
					r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
				case "conflicting client":
					q := r.URL.Query()
					q.Add("clientId", "device")
					q.Add("clientId", "other")
					r.URL.RawQuery = q.Encode()
				}
				w := httptest.NewRecorder()
				rootRawVideoInvoke(h, action, w, r)
				if w.Code != http.StatusForbidden {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}
				if p.streams.Load() != 0 || p.directs.Load() != 0 {
					t.Fatalf("denial reached provider stream=%d direct=%d", p.streams.Load(), p.directs.Load())
				}
				if strings.Contains(w.Body.String(), "SYNTHETIC-PRIVATE") || strings.Contains(w.Body.String(), "PRIVATE-DIRECT") {
					t.Fatal("denial exposed media/cache output")
				}
			})
		}
	}
}
func TestVideoVODBoundaryRetainsLegacyAndCheckedAdultControls(t *testing.T) {
	for _, mode := range []string{"nil legacy", "inactive", "active adult"} {
		for _, action := range []string{"stream", "direct"} {
			t.Run(mode+"/"+action, func(t *testing.T) {
				state := vodAccountActive
				if mode == "inactive" {
					state = vodExplicitInactive
				}
				h, p, f, path := rootRawVideoFixture(t, state, false)
				if mode == "nil legacy" {
					h.vodBoundary = nil
				}
				r := f.request(http.MethodGet, "/api/video/stream?transmux=0&profileId=profile&path="+url.QueryEscape(path), "")
				w := httptest.NewRecorder()
				rootRawVideoInvoke(h, action, w, r)
				want := http.StatusOK
				if action == "stream" {
					want = http.StatusPartialContent
				}
				if w.Code != want {
					t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
				}
				if action == "stream" {
					if mode == "nil legacy" {
						if p.streams.Load() != 0 || !strings.Contains(w.Body.String(), "SYNTHETIC") {
							t.Fatal("legacy cache control changed")
						}
					} else if p.streams.Load() != 1 || w.Body.String() != "SYNTHETIC" {
						t.Fatalf("configured stream did not use fresh owned range: calls=%d body=%q", p.streams.Load(), w.Body.String())
					}
				}
				if action == "direct" && (p.directs.Load() != 1 || !strings.Contains(w.Body.String(), "PRIVATE-DIRECT-OUTPUT")) {
					t.Fatal("current adult direct control missing")
				}
			})
		}
	}
}
func TestVideoVODBoundaryDiscardsLateActualProviderResults(t *testing.T) {
	for _, action := range []string{"stream", "direct"} {
		t.Run(action, func(t *testing.T) {
			h, p, f, path := rootRawVideoFixture(t, vodAccountActive, false)
			p.after = func() {
				f.change(func(f *vodTestState) {
					profile := f.profiles["profile"]
					profile.IsKidsProfile = true
					f.profiles[profile.ID] = profile
				})
			}
			r := f.request(http.MethodGet, "/api/video/stream?transmux=0&profileId=profile&path="+url.QueryEscape(path), "")
			w := httptest.NewRecorder()
			rootRawVideoInvoke(h, action, w, r)
			if w.Code != http.StatusForbidden || strings.Contains(w.Body.String(), "PRIVATE-DIRECT") || strings.Contains(w.Body.String(), "SYNTHETIC-FRESH") {
				t.Fatalf("late result escaped: %d %s", w.Code, w.Body.String())
			}
			if action == "stream" && (p.body == nil || p.body.closed.Load() != 1) {
				t.Fatal("late actual body was not closed exactly once")
			}
		})
	}
}

func TestVideoVODBoundaryLateDenialDropsPreparedPrivateHeaders(t *testing.T) {
	h, _, f, path := rootRawVideoFixture(t, vodAccountActive, false)
	r := f.request(http.MethodGet, "/api/video/stream?transmux=0&profileId=profile&path="+url.QueryEscape(path), "")
	response := httptest.NewRecorder()
	response.Header().Set("Access-Control-Allow-Origin", "https://synthetic-ui.invalid")
	writer, _, finish, ok := h.beginVideoVOD(response, r, path)
	if !ok {
		t.Fatal("owned control denied")
	}
	defer finish()
	writer.Header().Set("ETag", "synthetic-private-file-validator")
	writer.Header().Set("Last-Modified", "Mon, 02 Nov 2026 01:02:03 GMT")
	writer.Header().Set("Content-Disposition", "attachment; filename=synthetic-private-title.mkv")
	writer.Header().Set("X-Private-Source", "synthetic-private-provider-metadata")
	f.unlocks.RevokeProfile("profile")
	writer.WriteHeader(http.StatusOK)
	if response.Code != http.StatusForbidden {
		t.Fatalf("late denial=%d", response.Code)
	}
	for _, key := range []string{"ETag", "Last-Modified", "Content-Disposition", "X-Private-Source"} {
		if response.Header().Get(key) != "" {
			t.Errorf("late denial exposed prepared private %s header", key)
		}
	}
	if response.Header().Get("Access-Control-Allow-Origin") != "https://synthetic-ui.invalid" {
		t.Fatal("late denial removed the original CORS response header")
	}
}
