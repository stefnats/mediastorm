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
	"novastream/internal/database"
	"novastream/internal/importer"
	"novastream/models"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type managedCopyAccessService struct {
	fallbackAccessService
	reads, copies atomic.Int32
	afterCopy     func(context.Context)
	duringRead    func(context.Context)
	failure       error
}

func (s *managedCopyAccessService) StageSABImport(ctx context.Context, id int64, source importer.SABImportPreviewSettings, target importer.SABImportStageSettings, check func(context.Context) error) (importer.SABImportStageStatus, error) {
	s.copies.Add(1)
	if _, e := source(ctx); e != nil {
		return importer.SABImportStageStatus{}, e
	}
	if _, e := target(ctx); e != nil {
		return importer.SABImportStageStatus{}, e
	}
	if e := check(ctx); e != nil {
		return importer.SABImportStageStatus{}, e
	}
	if s.afterCopy != nil {
		s.afterCopy(ctx)
	}
	if s.failure != nil {
		return importer.SABImportStageStatus{}, s.failure
	}
	return importer.SABImportStageStatus{QueueID: id, State: "staged_copy", ObservedBytes: 4, Files: []database.SABStageFile{{Ordinal: 1, Bytes: 4, SHA256: strings.Repeat("a", 64)}}, ObservedAt: time.Now().UTC()}, nil
}
func (s *managedCopyAccessService) GetSABManagedCopy(ctx context.Context, id int64, source importer.SABImportPreviewSettings, target importer.SABImportStageSettings, check func(context.Context) error) (importer.SABManagedCopyView, error) {
	s.reads.Add(1)
	if _, e := source(ctx); e != nil {
		return importer.SABManagedCopyView{}, e
	}
	if _, e := target(ctx); e != nil {
		return importer.SABManagedCopyView{}, e
	}
	if e := check(ctx); e != nil {
		return importer.SABManagedCopyView{}, e
	}
	if s.duringRead != nil {
		s.duringRead(ctx)
	}
	if s.failure != nil {
		return importer.SABManagedCopyView{}, s.failure
	}
	zero := int64(0)
	count := 0
	return importer.SABManagedCopyView{QueueID: id, State: "not_recorded", Configured: true, ChargedBytes: &zero, FileCount: &count}, nil
}
func managedCopyRequest(f *fallbackAccessFixture, method string) *http.Request {
	r := httptest.NewRequest(method, "/admin/api/usenet/fallback/17/stage", nil)
	r = mux.SetURLVars(r, map[string]string{"queueID": "17"})
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, f.claim)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.claim.AccountID)
	return r.WithContext(ctx)
}
func managedCopyConfig() config.SABnzbdSettings {
	enabled := true
	return config.SABnzbdSettings{Enabled: &enabled, FallbackHost: "http://synthetic.invalid", FallbackAPIKey: "synthetic-key", ImportPreviewRemoteRoot: "/private-remote", ImportPreviewLocalRoot: "/private-local", ImportStageTargetRoot: "/private-target", ImportStageMaxBytes: 1000}
}
func TestSABManagedCopyOriginalAuthorityAndInputs(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		for _, mode := range []string{"positive", "missing claim", "scope", "role", "expired", "missing settings", "query", "body", "noncanonical", "late role", "late revoke", "unknown result"} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				f := newFallbackAccessFixture(t)
				s := &managedCopyAccessService{}
				h := NewUsenetFallbackHandler(s, f.sessions, f.accounts)
				cfg := managedCopyConfig()
				var configReads atomic.Int32
				h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { configReads.Add(1); return cfg, ctx.Err() })
				switch mode {
				case "scope":
					f.claim.Scope = "stream"
				case "role":
					f.accounts.change(func(a *models.Account) { a.IsMaster = false })
				case "expired":
					f.claim.ExpiresAt = time.Now().Add(-time.Second)
				case "missing settings":
					h.previewSettings = nil
				case "unknown result":
					s.failure = database.ErrSABStageOutcomeUnknown
				}
				r := managedCopyRequest(f, method)
				switch mode {
				case "missing claim":
					r = httptest.NewRequest(method, r.URL.String(), nil)
				case "query":
					r.URL.RawQuery = "path=PRIVATE"
				case "body":
					r.Body = io.NopCloser(strings.NewReader("{}"))
					r.ContentLength = 2
				case "noncanonical":
					r = mux.SetURLVars(r, map[string]string{"queueID": "017"})
				}
				change := func(context.Context) {
					if mode == "late role" {
						f.accounts.change(func(a *models.Account) { a.IsMaster = false })
					}
					if mode == "late revoke" {
						f.sessions.real.Revoke(f.claim.Token)
					}
				}
				s.afterCopy = change
				s.duringRead = change
				w := httptest.NewRecorder()
				if method == "GET" {
					h.GetManagedCopy(w, r)
				} else {
					h.CopyFiles(w, r)
				}
				if mode == "positive" {
					if w.Code != 200 {
						t.Fatal("positive", w.Code)
					}
					var v importer.SABManagedCopyView
					if json.Unmarshal(w.Body.Bytes(), &v) != nil || v.QueueID != 17 {
						t.Fatal("safe DTO")
					}
					if method == "POST" && (v.State != "copy_recorded" || s.reads.Load() != 0) {
						t.Fatal("POST adopted a later GET projection")
					}
				} else if w.Code < 400 || strings.Contains(w.Body.String(), "PRIVATE") || strings.Contains(w.Body.String(), "copy_recorded") {
					t.Fatal("denial published")
				}
				expected := int32(0)
				switch mode {
				case "positive", "late role", "late revoke", "unknown result":
					expected = 1
				}
				if s.reads.Load()+s.copies.Load() != expected {
					t.Fatal("denial/service count")
				}
				if expected == 0 && configReads.Load() != 0 {
					t.Fatal("invalid input/settings unavailable reached config reader")
				}
			})
		}
	}
}
func TestSABManagedCopyFrozenConfigurationAcrossStageAndFinalPublication(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		for _, mode := range []string{"target", "quota", "source", "enabled-pointer", "receipt-change-control"} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				f := newFallbackAccessFixture(t)
				s := &managedCopyAccessService{}
				h := NewUsenetFallbackHandler(s, f.sessions, f.accounts)
				cfg := managedCopyConfig()
				h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { return cfg, ctx.Err() })
				mutate := func(context.Context) {
					switch mode {
					case "target":
						cfg.ImportStageTargetRoot = "/changed"
					case "quota":
						cfg.ImportStageMaxBytes++
					case "source":
						cfg.FallbackAPIKey = "changed-synthetic"
					case "enabled-pointer":
						*cfg.Enabled = false
					}
				}
				s.afterCopy = mutate
				s.duringRead = mutate
				w := httptest.NewRecorder()
				r := managedCopyRequest(f, method)
				if method == "POST" {
					h.CopyFiles(w, r)
				} else {
					h.GetManagedCopy(w, r)
				}
				if mode == "receipt-change-control" {
					if w.Code != 200 {
						t.Fatal("original successful historical projection unavailable")
					}
					if method == "POST" && (s.reads.Load() != 0 || !strings.Contains(w.Body.String(), "copy_recorded")) {
						t.Fatal("crosscall receipt adoption")
					}
				} else {
					if w.Code < 400 || strings.Contains(w.Body.String(), "not_recorded") || strings.Contains(w.Body.String(), "copy_recorded") {
						t.Fatal("new configuration adopted")
					}
					if method == "POST" && (s.reads.Load() != 0 || !strings.Contains(w.Body.String(), "may already have been copied")) {
						t.Fatal("copy error promised rollback or adopted getter")
					}
				}
			})
		}
	}
}
func TestSABManagedCopyOriginalDeadlineAndSanitizedErrors(t *testing.T) {
	f := newFallbackAccessFixture(t)
	f.claim.ExpiresAt = time.Now().Add(70 * time.Millisecond)
	s := &managedCopyAccessService{}
	h := NewUsenetFallbackHandler(s, f.sessions, f.accounts)
	cfg := managedCopyConfig()
	h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { return cfg, ctx.Err() })
	s.afterCopy = func(ctx context.Context) {
		d, ok := ctx.Deadline()
		if !ok || d.After(f.claim.ExpiresAt) {
			t.Error("original deadline renewed")
		}
		<-ctx.Done()
	}
	s.failure = errors.New("PRIVATE_PATH_ERROR")
	w := httptest.NewRecorder()
	h.CopyFiles(w, managedCopyRequest(f, http.MethodPost))
	if w.Code != 403 || strings.Contains(w.Body.String(), "PRIVATE") || strings.Contains(w.Body.String(), "copy_recorded") {
		t.Fatal("expired original published")
	}
}
