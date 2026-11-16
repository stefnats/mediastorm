package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/recordings"
	"novastream/services/streaming"
)

type rootRecordingAliasProvider struct {
	*recordings.StreamProvider
	streamCalls atomic.Int32
	directCalls atomic.Int32
}

func (p *rootRecordingAliasProvider) Stream(ctx context.Context, request streaming.Request) (*streaming.Response, error) {
	p.streamCalls.Add(1)
	return p.StreamProvider.Stream(ctx, request)
}

func (p *rootRecordingAliasProvider) GetDirectURL(ctx context.Context, path string) (string, error) {
	p.directCalls.Add(1)
	return p.StreamProvider.GetDirectURL(ctx, path)
}

type rootRecordingAliasFixture struct {
	handler  *VideoHandler
	provider *rootRecordingAliasProvider
	service  *recordings.Service
	path     string
	payload  string
	access   *recordingAccessFixture
}

func newRootRecordingAliasFixture(t *testing.T) *rootRecordingAliasFixture {
	t.Helper()
	root := t.TempDir()
	file := filepath.Join(root, "capture.mkv")
	payload := "SYNTHETIC-FOREIGN-RECORDING-BYTES"
	if err := os.WriteFile(file, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	item := models.Recording{ID: "foreign-recording", UserID: "profile-b", Status: models.RecordingStatusCompleted, OutputPath: file}
	repo := &capturePartHTTPRepo{item: item}
	service := recordings.NewService(repo, "", root)
	provider := &rootRecordingAliasProvider{StreamProvider: recordings.NewStreamProvider(service)}
	handler := NewVideoHandlerWithProvider(false, "", "", "", provider)
	access := newRecordingAccessFixture(t)
	access.session = models.Session{Token: "synthetic-account-a-session", AccountID: "account-a", ExpiresAt: time.Now().Add(time.Hour)}
	access.accounts = recordingAccessAccountMap{"account-a": {ID: "account-a"}, "account-b": {ID: "account-b"}}
	access.profiles = recordingAccessProfileMap{"profile-b": {ID: "profile-b", AccountID: "account-b"}}
	access.guard = NewRecordingStreamAccess(service, recordingSessionFunc(func(ctx context.Context, token string) (*models.Session, error) {
		if token != access.session.Token {
			return nil, errors.New("session unavailable")
		}
		session := access.session
		return &session, ctx.Err()
	}), access.accounts, access.profiles, access.unlocks, func() (config.SecuritySettings, error) {
		return config.SecuritySettings{EnforceProfilePins: access.enforce}, access.policyErr
	})
	handler.SetRecordingStreamAccess(access.guard)
	t.Cleanup(handler.Shutdown)
	return &rootRecordingAliasFixture{handler: handler, provider: provider, service: service, path: recordings.BuildStreamPath(item), payload: payload, access: access}
}

func TestRecordingAliasesPreserveOwnedMediaAndCurrentMaster(t *testing.T) {
	for _, ownerMode := range []string{"owned", "current master"} {
		for _, action := range []string{"stream", "HEAD", "Range", "cached metadata", "direct URL"} {
			t.Run(ownerMode+"/"+action, func(t *testing.T) {
				f := newRootRecordingAliasFixture(t)
				if ownerMode == "owned" {
					p := f.access.profiles["profile-b"]
					p.AccountID = "account-a"
					f.access.profiles[p.ID] = p
				} else {
					a := f.access.accounts["account-a"]
					a.IsMaster = true
					f.access.accounts[a.ID] = a
				}
				r := f.request()
				w := httptest.NewRecorder()
				want := http.StatusOK
				switch action {
				case "cached metadata":
					f.handler.setCachedMetadata(f.path, &videoMetadataResponse{Path: "SYNTHETIC-PRIVATE-METADATA", DurationSeconds: 42})
					f.handler.ProbeVideo(w, r)
				case "direct URL":
					f.handler.GetDirectURL(w, r)
				default:
					if action == "HEAD" {
						r.Method = http.MethodHead
					}
					if action == "Range" {
						r.Header.Set("Range", "bytes=2-7")
						want = http.StatusPartialContent
					}
					f.handler.StreamVideo(w, r)
				}
				if w.Code != want {
					t.Fatalf("status=%d want=%d body=%s", w.Code, want, w.Body.String())
				}
				switch action {
				case "stream":
					if w.Body.String() != f.payload {
						t.Fatal("owned bytes missing")
					}
				case "HEAD":
					if w.Body.Len() != 0 {
						t.Fatal("HEAD returned body")
					}
				case "Range":
					if w.Body.String() != f.payload[2:8] {
						t.Fatal("range changed")
					}
				case "cached metadata":
					if !strings.Contains(w.Body.String(), "SYNTHETIC-PRIVATE-METADATA") {
						t.Fatal("owned cache unavailable")
					}
				case "direct URL":
					if f.provider.directCalls.Load() != 1 {
						t.Fatal("owned provider not reached")
					}
				}
			})
		}
	}
}

func TestRecordingInternalFallbackRequiresActualPrivateEntry(t *testing.T) {
	for _, mode := range []string{"real loopback", "spoofed forwarded loopback", "public handler loopback", "public handler internal URL"} {
		t.Run(mode, func(t *testing.T) {
			f := newRootRecordingAliasFixture(t)
			f.handler.SetRecordingStreamAccess(nil)
			f.handler.SetLocalBaseURL("http://127.0.0.1:9999")
			raw := f.handler.buildLocalVideoStreamURL(f.path)
			if raw == "" {
				t.Fatal("thumbnail fallback URL missing")
			}
			r := httptest.NewRequest(http.MethodGet, raw, nil)
			q := r.URL.Query()
			q.Set("transmux", "0")
			r.URL.RawQuery = q.Encode()
			r.RemoteAddr = "127.0.0.1:43210"
			if mode == "spoofed forwarded loopback" {
				r.RemoteAddr = "192.0.2.10:43210"
				r.Header.Set("X-Forwarded-For", "127.0.0.1")
				r.Host = "localhost"
			}
			w := httptest.NewRecorder()
			if strings.HasPrefix(mode, "public handler") {
				if mode == "public handler loopback" {
					r.URL.Path = "/api/video/stream"
				}
				f.handler.StreamVideo(w, r)
			} else {
				f.handler.StreamInternalVideo(w, r)
			}
			if mode == "real loopback" {
				if w.Code != 200 || w.Body.String() != f.payload {
					t.Fatalf("internal fallback=%d %s", w.Code, w.Body.String())
				}
			} else if w.Code < 400 || f.provider.streamCalls.Load() != 0 {
				t.Fatalf("untrusted entry reached file status=%d", w.Code)
			}
		})
	}
}

func (f *rootRecordingAliasFixture) request() *http.Request {
	request := httptest.NewRequest(http.MethodGet, "/api/video/stream?transmux=0&path="+url.QueryEscape(f.path), nil)
	session := models.Session{Token: "synthetic-account-a-session", AccountID: "account-a", ExpiresAt: time.Now().Add(time.Hour)}
	ctx := context.WithValue(request.Context(), auth.ContextKeyAccountID, "account-a")
	ctx = context.WithValue(ctx, auth.ContextKeySession, session)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
	return request.WithContext(ctx)
}

func TestRecordingAliasesRejectForeignOwnerBeforeFileOrCachedMetadata(t *testing.T) {
	for _, action := range []string{"stream", "cached metadata", "direct URL"} {
		t.Run(action, func(t *testing.T) {
			f := newRootRecordingAliasFixture(t)
			request := f.request()
			response := httptest.NewRecorder()
			switch action {
			case "stream":
				f.handler.StreamVideo(response, request)
			case "cached metadata":
				f.handler.setCachedMetadata(f.path, &videoMetadataResponse{Path: "SYNTHETIC-PRIVATE-METADATA", DurationSeconds: 42})
				f.handler.ProbeVideo(response, request)
			case "direct URL":
				f.handler.GetDirectURL(response, request)
			}
			denied := response.Code == http.StatusNotFound || response.Code == http.StatusForbidden || response.Code == http.StatusUnauthorized
			if !denied {
				t.Errorf("foreign recording alias returned status %d instead of denying account A access to profile B", response.Code)
			}
			if f.provider.streamCalls.Load() != 0 || f.provider.directCalls.Load() != 0 {
				t.Errorf("foreign recording reached real file provider: stream=%d direct=%d", f.provider.streamCalls.Load(), f.provider.directCalls.Load())
			}
			if response.Body.String() == f.payload {
				t.Error("foreign recording bytes were returned")
			}
		})
	}
}
