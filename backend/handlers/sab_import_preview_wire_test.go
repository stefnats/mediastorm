package handlers

import (
	"context"
	"errors"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/config"
	"novastream/internal/importer"
	"os"
	"path/filepath"
	"testing"
)

func sabImportPreviewOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	for _, mode := range []string{"positive", "unavailable", "busy", "source_changed", "missing"} {
		t.Run(mode, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			s := &previewAccessService{}
			switch mode {
			case "unavailable":
				s.performPreview = func(context.Context, func(context.Context) error) error { return errors.New("PRIVATE_PATH") }
			case "busy":
				s.performPreview = func(context.Context, func(context.Context) error) error { return importer.ErrSABFallbackBusy }
			case "source_changed":
				s.performPreview = func(context.Context, func(context.Context) error) error { return importer.ErrSABFallbackSourceChanged }
			case "missing":
				s.performPreview = func(context.Context, func(context.Context) error) error { return importer.ErrSABFallbackNotFound }
			}
			h := NewUsenetFallbackHandler(s, f.sessions, f.accounts)
			h.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) { return config.SABnzbdSettings{}, ctx.Err() })
			r := previewAccessRequest(f)
			if e := v.ValidateRequest(r, nil); e != nil {
				t.Fatal(e)
			}
			w := httptest.NewRecorder()
			h.Preview(w, r)
			if e := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); e != nil {
				t.Fatal(e)
			}
			if mode == "positive" && w.Code != 200 || mode != "positive" && w.Code < 400 {
				t.Fatal("contract control not reached")
			}
			if dir := os.Getenv("SAB_IMPORT_PREVIEW_CONTRACT_DIR"); dir != "" {
				if e := os.MkdirAll(dir, 0700); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(dir, mode+".json"), w.Body.Bytes(), 0600); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
func TestSABImportPreviewActualHandlerOpenAPI(t *testing.T) {
	v, e := openapi.NewValidator(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	sabImportPreviewOpenAPIContract(t, v)
}
