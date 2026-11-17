package handlers

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/config"
	"novastream/internal/importer"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type managedCopyWireService struct {
	managedCopyAccessService
	view importer.SABManagedCopyView
}

func (s *managedCopyWireService) GetSABManagedCopy(ctx context.Context, id int64, source importer.SABImportPreviewSettings, target importer.SABImportStageSettings, check func(context.Context) error) (importer.SABManagedCopyView, error) {
	if e := check(ctx); e != nil {
		return importer.SABManagedCopyView{}, e
	}
	s.view.QueueID = id
	return s.view, nil
}
func managedCopyOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	at := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	bytes := int64(4)
	count := 1
	zero := int64(0)
	none := 0
	cases := []struct {
		name, method string
		view         importer.SABManagedCopyView
	}{
		{"unknown_namespace", "GET", importer.SABManagedCopyView{State: "unknown", Configured: true, Reason: "namespace_unobserved"}},
		{"not_configured", "GET", importer.SABManagedCopyView{State: "unknown", Configured: false, Reason: "not_configured"}},
		{"not_recorded", "GET", importer.SABManagedCopyView{State: "not_recorded", Configured: true, ChargedBytes: &zero, FileCount: &none}},
		{"copy_recorded", "GET", importer.SABManagedCopyView{State: "copy_recorded", Configured: true, ChargedBytes: &bytes, FileCount: &count, ObservedAt: &at}},
		{"needs_checking", "GET", importer.SABManagedCopyView{State: "reconciliation_required", Configured: true, Reason: "attempt_unconfirmed", ChargedBytes: &bytes, FileCount: &count, ObservedAt: &at}},
		{"copy_post", "POST", importer.SABManagedCopyView{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			service := &managedCopyWireService{view: tc.view}
			handler := NewUsenetFallbackHandler(service, f.sessions, f.accounts)
			cfg := managedCopyConfig()
			handler.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { return cfg, ctx.Err() })
			r := managedCopyRequest(f, tc.method)
			if e := v.ValidateRequest(r, nil); e != nil {
				t.Fatal(e)
			}
			w := httptest.NewRecorder()
			if tc.method == "POST" {
				handler.CopyFiles(w, r)
			} else {
				handler.GetManagedCopy(w, r)
			}
			if w.Code != 200 {
				t.Fatal(w.Code)
			}
			if e := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); e != nil {
				t.Fatal(e)
			}
			var parsed importer.SABManagedCopyView
			if e := json.Unmarshal(w.Body.Bytes(), &parsed); e != nil || parsed.QueueID != 17 {
				t.Fatal("actual handler JSON")
			}
			if dir := os.Getenv("SAB_MANAGED_COPY_CONTRACT_DIR"); dir != "" {
				if e := os.MkdirAll(dir, 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(dir, tc.name+".json"), w.Body.Bytes(), 0600); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
func TestSABManagedCopyActualHandlerOpenAPI(t *testing.T) {
	v, e := openapi.NewValidator(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	managedCopyOpenAPIContract(t, v)
}
