package handlers

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"strings"
	"testing"
)

func TestAutoProviderLimitsActualReportOpenAPI(t *testing.T) {
	h, cfg, calls, _, _ := autoProviderFixture(t, nil)
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		setAutoProviderOptIn(t, cfg, enabled)
		raw := []byte(`{"sourceId":"xtream"}`)
		r := httptest.NewRequest("POST", "/admin/api/live/provider-capacity", strings.NewReader(string(raw)))
		r.Header.Set("Content-Type", "application/json")
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
		if err := validator.ValidateRequest(r, raw); err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		h.CheckProviderCapacity(w, r)
		if w.Code != 200 {
			t.Fatalf("%d %s", w.Code, w.Body.String())
		}
		if err := validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
			t.Fatalf("schema: %v\n%s", err, w.Body.String())
		}
		var report ProviderCapacityReport
		if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
			t.Fatal(err)
		}
		if report.AutomaticAdoptionEnabled != enabled || enabled != (report.EnforcementOrigin == "provider_extra_cap") {
			t.Fatalf("wrong policy origin: %+v", report)
		}
		for _, forbidden := range []string{"synthetic-user", "synthetic-password", "synthetic-private", "player_api.php", "proxyUrl"} {
			if strings.Contains(w.Body.String(), forbidden) {
				t.Fatalf("private data returned: %s", forbidden)
			}
		}
	}
	if calls.Load() != 1 {
		t.Fatal("opt-in caused an unrequested second HTTP fetch")
	}
}
