package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"novastream/api/openapi"
	"novastream/internal/importer"
)

type sabListWireService struct{ sabWireState }

func (s sabListWireService) ListSABFallbackStatuses(_ context.Context, cursor string, limit int) (importer.SABFallbackPage, error) {
	at := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	if cursor != "" {
		return importer.SABFallbackPage{Items: []importer.SABFallbackStatus{{QueueID: 1, State: "submission_unknown", Reason: "acknowledgement_unavailable", SubmittedAt: &at}}}, nil
	}
	return importer.SABFallbackPage{Items: []importer.SABFallbackStatus{{QueueID: 17, State: "submitted", SubmittedAt: &at}}, NextCursor: "v1.17.17"}, nil
}
func sabFallbackListOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	f := newFallbackAccessFixture(t)
	h := NewUsenetFallbackHandler(sabListWireService{sabWireState("submitted")}, f.sessions, f.accounts)
	for _, path := range []string{"/admin/api/usenet/fallback?limit=1", "/admin/api/usenet/fallback?limit=1&cursor=v1.17.17"} {
		r := sabFallbackContractRequest(f, "GET")
		r.URL.Path = "/admin/api/usenet/fallback"
		r.URL.RawQuery = "limit=1"
		if path != "/admin/api/usenet/fallback?limit=1" {
			r.URL.RawQuery += "&cursor=v1.17.17"
		}
		if err := v.ValidateRequest(r, nil); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		h.List(w, r)
		if w.Code != http.StatusOK {
			t.Fatal(w.Code, w.Body.String())
		}
		if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
			t.Fatal(err)
		}
		var page importer.SABFallbackPage
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || len(page.Items) != 1 {
			t.Fatal("actual serialized page", err)
		}
	}
}
func TestSABFallbackListActualHandlerOpenAPI(t *testing.T) {
	v, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	sabFallbackListOpenAPIContract(t, v)
}
