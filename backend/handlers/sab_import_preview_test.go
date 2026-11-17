package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"net/http/httptest"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/importer"
	"novastream/models"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type previewAccessService struct {
	fallbackAccessService
	calls          atomic.Int32
	performPreview func(context.Context, func(context.Context) error) error
}

func (s *previewAccessService) PreviewSABImport(ctx context.Context, id int64, settings importer.SABImportPreviewSettings, check func(context.Context) error) (importer.SABImportPreview, error) {
	s.calls.Add(1)
	if _, e := settings(ctx); e != nil {
		return importer.SABImportPreview{}, e
	}
	if s.performPreview != nil {
		if e := s.performPreview(ctx, check); e != nil {
			return importer.SABImportPreview{}, e
		}
	}
	if e := check(ctx); e != nil {
		return importer.SABImportPreview{}, e
	}
	return importer.SABImportPreview{QueueID: id, State: "local_inventory", ObservedAt: time.Now().UTC(), EntryCount: 1, ObservedBytes: 4, Files: []importer.SABImportPreviewFile{{Ordinal: 1, Kind: "media_candidate", Extension: "ts", ObservedBytes: 4}}}, nil
}
func previewAccessRequest(f *fallbackAccessFixture) *http.Request {
	r := httptest.NewRequest("POST", "/admin/api/usenet/fallback/17/import-preview", nil)
	r = mux.SetURLVars(r, map[string]string{"queueID": "17"})
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, f.claim)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.claim.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
	return r.WithContext(ctx)
}
func TestSABImportPreviewOriginalHandlerAuthority(t *testing.T) {
	for _, mode := range []string{"positive", "no claim", "scoped", "demoted", "expired claim", "missing settings", "missing optional service", "bad id", "query", "body", "late role", "late revoke", "late expiry", "caller cancel", "settings failure", "service private error"} {
		t.Run(mode, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			s := &previewAccessService{}
			h := NewUsenetFallbackHandler(s, f.sessions, f.accounts)
			h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) {
				if mode == "settings failure" {
					return config.SABnzbdSettings{}, errors.New("PRIVATE_PATH_ERROR")
				}
				return config.SABnzbdSettings{}, ctx.Err()
			})
			switch mode {
			case "scoped":
				f.claim.Scope = "stream"
			case "demoted":
				f.accounts.change(func(a *models.Account) { a.IsMaster = false })
			case "expired claim":
				f.claim.ExpiresAt = time.Now().Add(-time.Second)
			case "missing settings":
				h.previewSettings = nil
			case "missing optional service":
				h.service = f.service
			}
			r := previewAccessRequest(f)
			switch mode {
			case "no claim":
				r = httptest.NewRequest("POST", r.URL.String(), nil)
			case "bad id":
				r = mux.SetURLVars(r, map[string]string{"queueID": "017"})
			case "query":
				r.URL.RawQuery = "path=PRIVATE"
			case "body":
				r.Body = io.NopCloser(strings.NewReader("{}"))
				r.ContentLength = 2
			}
			s.performPreview = func(ctx context.Context, check func(context.Context) error) error {
				switch mode {
				case "late role":
					f.accounts.change(func(a *models.Account) { a.IsMaster = false })
				case "late revoke":
					f.sessions.real.Revoke(f.claim.Token)
				case "late expiry":
					f.sessions.modify = func(s *models.Session) { s.ExpiresAt = time.Now().Add(-time.Second) }
				case "service private error":
					return errors.New("PRIVATE_PATH_ERROR")
				}
				return check(ctx)
			}
			if mode == "caller cancel" {
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			}
			w := httptest.NewRecorder()
			h.Preview(w, r)
			if mode == "positive" {
				var v importer.SABImportPreview
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &v) != nil || v.QueueID != 17 || v.EntryCount != 1 {
					t.Fatal("positive DTO")
				}
				if w.Header().Get("Cache-Control") != "private, no-store" {
					t.Fatal("cache header")
				}
			} else {
				if w.Code < 400 || strings.Contains(w.Body.String(), "PRIVATE") || strings.Contains(w.Body.String(), "local_inventory") {
					t.Fatal("denial published private data")
				}
			}
			expected := int32(0)
			switch mode {
			case "positive", "late role", "late revoke", "late expiry", "settings failure", "service private error":
				expected = 1
			}
			if s.calls.Load() != expected {
				t.Fatalf("preview calls%d want%d", s.calls.Load(), expected)
			}
		})
	}
}

func TestSABImportPreviewOriginalDeadlineNeverRenewed(t *testing.T) {
	f := newFallbackAccessFixture(t)
	f.claim.ExpiresAt = time.Now().Add(80 * time.Millisecond)
	s := &previewAccessService{}
	s.performPreview = func(ctx context.Context, check func(context.Context) error) error {
		deadline, ok := ctx.Deadline()
		if !ok || deadline.After(f.claim.ExpiresAt) {
			t.Error("original claim deadline renewed")
		}
		<-ctx.Done()
		return check(ctx)
	}
	h := NewUsenetFallbackHandler(s, f.sessions, f.accounts)
	h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { return config.SABnzbdSettings{}, ctx.Err() })
	w := httptest.NewRecorder()
	h.Preview(w, previewAccessRequest(f))
	if w.Code != 403 || s.calls.Load() != 1 || strings.Contains(w.Body.String(), "local_inventory") {
		t.Fatal("original deadline not enforced after blocked operation")
	}
}
