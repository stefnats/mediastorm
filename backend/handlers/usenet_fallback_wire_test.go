package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"novastream/models"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/importer"
)

func sabFallbackContractRequest(f *fallbackAccessFixture, method string) *http.Request {
	path := "/admin/api/usenet/fallback/17"
	if method == "POST" {
		path += "/refresh"
	}
	r := httptest.NewRequest(method, path, nil)
	r = mux.SetURLVars(r, map[string]string{"queueID": "17"})
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, f.claim)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.claim.AccountID)
	return r.WithContext(ctx)
}
func sabFallbackOpenAPIContract(t *testing.T, v *openapi.Validator) {
	f := newFallbackAccessFixture(t)
	h := NewUsenetFallbackHandler(f.service, f.sessions, f.accounts)
	for _, method := range []string{"GET", "POST"} {
		r := sabFallbackContractRequest(f, method)
		if err := v.ValidateRequest(r, nil); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		if method == "GET" {
			h.Get(w, r)
		} else {
			h.Refresh(w, r)
		}
		if w.Code != 200 {
			t.Fatal(w.Code)
		}
		if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
}

type sabWireState string

func (s sabWireState) GetSABFallbackStatus(_ context.Context, id int64) (importer.SABFallbackStatus, error) {
	at := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	status := importer.SABFallbackStatus{QueueID: id, State: string(s), SubmittedAt: &at}
	if s == "submission_unknown" {
		status.Reason = "acknowledgement_unavailable"
	}
	if s == "download_completed" || s == "downloading" {
		checked := at.Add(time.Minute)
		status.CheckedAt = &checked
	}
	if s == "not_submitted" {
		status.SubmittedAt = nil
	}
	return status, nil
}
func (s sabWireState) RefreshSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	return s.GetSABFallbackStatus(ctx, id)
}
func TestSABFallbackActualHandlerWireContract(t *testing.T) {
	v, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	for _, state := range []string{"not_submitted", "submission_unknown", "submitted", "downloading", "download_completed", "failed", "unknown"} {
		t.Run(state, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			h := NewUsenetFallbackHandler(sabWireState(state), f.sessions, f.accounts)
			r := sabFallbackContractRequest(f, "GET")
			w := httptest.NewRecorder()
			h.Get(w, r)
			if w.Code != 200 {
				t.Fatal(w.Code)
			}
			if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{"jobId", "source", "password", "apiKey", "outputPath", "PRIVATE", "imported", "stream_ready"} {
				if strings.Contains(w.Body.String(), private) {
					t.Fatal("unsafe wirefield", private)
				}
			}
			fixtures[state+".json"] = append([]byte(nil), w.Body.Bytes()...)
		})
	}
	if dir := os.Getenv("SAB_FALLBACK_CONTRACT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		manifest := map[string]string{}
		for name, raw := range fixtures {
			if err := os.WriteFile(filepath.Join(dir, name), raw, 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(raw)
			manifest[name] = hex.EncodeToString(sum[:])
		}
		raw, _ := json.MarshalIndent(manifest, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

type sabBoundedInitialReader struct{ deadline chan time.Time }

func (s sabBoundedInitialReader) SnapshotForAuthorization(ctx context.Context, _ string) (*models.Session, error) {
	at, ok := ctx.Deadline()
	if !ok {
		s.deadline <- time.Time{}
		return nil, errors.New("missing initial budget")
	}
	s.deadline <- at
	return nil, errors.New("synthetic initial reader stop")
}
func TestSABFallbackOriginalClaimBoundsInitialAuthorityRead(t *testing.T) {
	f := newFallbackAccessFixture(t)
	f.claim.ExpiresAt = time.Now().Add(time.Second)
	observed := make(chan time.Time, 1)
	h := NewUsenetFallbackHandler(f.service, sabBoundedInitialReader{observed}, f.accounts)
	w := httptest.NewRecorder()
	h.Get(w, sabFallbackContractRequest(f, "GET"))
	deadline := <-observed
	if deadline.IsZero() || deadline.After(f.claim.ExpiresAt) || f.service.gets.Load() != 0 || w.Code != 403 {
		t.Fatal("initial reader escaped original claim budget")
	}
}
