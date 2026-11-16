package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func assertOccupancyUnknown(t *testing.T, report ProviderCapacityReport, reason string) {
	t.Helper()
	if report.ExternalUsage.State != "unknown" || report.ExternalUsage.Reason != reason {
		t.Fatalf("external attribution: %+v", report.ExternalUsage)
	}
	raw, err := json.Marshal(report.ExternalUsage)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 2 {
		t.Fatalf("external usage contains invented count/range: %s", raw)
	}
	if report.LocalSnapshotCompletedAt.IsZero() {
		t.Fatal("local completion time absent")
	}
}

func TestProviderOccupancyActualCountersNeverProveExternalZeroOrRange(t *testing.T) {
	cases := []struct {
		name, active string
		known        bool
	}{
		{"zero", "0", true}, {"zero-string", `"0"`, true}, {"positive", "4", true}, {"missing", "", false}, {"null", "null", false}, {"malformed", `"not-a-count"`, false}, {"negative", "-1", false}, {"decimal", "1.5", false}, {"too-large", "1000001", false}, {"boolean", "true", false}, {"object", `{}`, false},
	}
	for _, limit := range []string{"2", "0"} {
		for _, tc := range cases {
			t.Run("limit-"+limit+"/"+tc.name, func(t *testing.T) {
				body := `{"user_info":{"auth":1,"status":"Active","max_connections":` + limit
				if tc.active != "" {
					body += `,"active_cons":` + tc.active
				}
				body += `},"server_info":{"timestamp_now":1}}`
				h, cfg, calls, m, bucket := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) })
				setAutoProviderOptIn(t, cfg, true)
				before := time.Now()
				lease, err := m.Acquire(bucket, "", 3)
				if err != nil {
					t.Fatal(err)
				}
				defer lease.Release()
				w, report := capacityCheck(t, h, "xtream")
				after := time.Now()
				if w.Code != 200 {
					t.Fatalf("status=%d %s", w.Code, w.Body.String())
				}
				reason := "provider_count_unavailable"
				if tc.known {
					reason = "no_common_trusted_measurement"
				}
				assertOccupancyUnknown(t, report, reason)
				if (report.ProviderActiveConnections != nil) != tc.known || report.LocalConnections != 1 {
					t.Fatalf("distinct counts %+v", report)
				}
				if tc.known && tc.active == "0" && *report.ProviderActiveConnections != 0 {
					t.Fatal("valid provider zero lost")
				}
				if report.CheckedAt == nil || report.CheckedAt.Before(before) || report.CheckedAt.After(after) || report.LocalSnapshotCompletedAt.Before(*report.CheckedAt) || report.LocalSnapshotCompletedAt.After(after) {
					t.Fatal("server completion scopes invalid")
				}
				want := 3
				if limit == "2" {
					want = 2
				}
				if report.EnforcedLimit != want || (report.ReportedLimit != nil) != (limit == "2") {
					t.Fatal("count availability changed limit policy")
				}
				if calls.Load() != 1 {
					t.Fatal("report added provider demand")
				}
				// Even contradictory provider0/local1 is unknown. There is no clamp,
				// subtraction, external0, or inferred external range in the wire object.
			})
		}
	}
}

func TestProviderOccupancyUnauthenticatedBannedAndInvalidChecksExposeNoCount(t *testing.T) {
	for _, body := range []string{
		`{"user_info":{"auth":0,"status":"Active","max_connections":2,"active_cons":9}}`,
		`{"user_info":{"auth":1,"status":"Banned","max_connections":2,"active_cons":9}}`,
		`{"user_info":{"auth":1,"status":"Expired","max_connections":2,"active_cons":9}}`,
		`{"user_info":{"status":"Active","max_connections":2,"active_cons":9}}`,
		`{invalid`,
	} {
		t.Run(body, func(t *testing.T) {
			h, _, calls, _, _ := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) })
			w, report := capacityCheck(t, h, "xtream")
			if w.Code != 200 || report.ProviderActiveConnections != nil || report.State != "unavailable" || calls.Load() != 1 {
				t.Fatalf("untrusted count exposed: %+v", report)
			}
			assertOccupancyUnknown(t, report, "provider_count_unavailable")
		})
	}
	h, _, calls, _, _ := autoProviderFixture(t, nil)
	w, report := capacityCheck(t, h, "plain")
	if w.Code != 200 || report.State != "unsupported" || report.CheckedAt != nil || calls.Load() != 0 {
		t.Fatal("unsupported source produced provider observation")
	}
	assertOccupancyUnknown(t, report, "provider_count_unavailable")
	r := httptest.NewRequest("POST", "/admin/api/live/provider-capacity", strings.NewReader(`{"sourceId":"xtream"}`))
	w = httptest.NewRecorder()
	h.CheckProviderCapacity(w, r)
	if w.Code != 403 || calls.Load() != 0 || strings.Contains(w.Body.String(), "externalUsage") {
		t.Fatal("unauthorized caller received account observation")
	}
}

func TestProviderOccupancyCachedProviderTimeAndNewLocalTimeStaySeparate(t *testing.T) {
	h, _, calls, m, bucket := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_info":{"auth":1,"status":"Active","max_connections":0,"active_cons":0}}`))
	})
	_, first := capacityCheck(t, h, "xtream")
	lease, err := m.AcquireOwned(bucket, "", 3, livecapacity.Owner{Kind: livecapacity.KindRecording, RecordingID: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	_, second := capacityCheck(t, h, "export")
	if !second.Cached || first.CheckedAt == nil || second.CheckedAt == nil || !first.CheckedAt.Equal(*second.CheckedAt) || !first.CacheExpiresAt.Equal(*second.CacheExpiresAt) || !second.LocalSnapshotCompletedAt.After(first.LocalSnapshotCompletedAt) || first.LocalConnections != 0 || second.LocalConnections != 1 || calls.Load() != 1 {
		t.Fatalf("provider observation retimed or local sample stale: first=%+v second=%+v", first, second)
	}
	assertOccupancyUnknown(t, first, "no_common_trusted_measurement")
	assertOccupancyUnknown(t, second, "no_common_trusted_measurement")
}

func TestProviderOccupancyExpiredThenFailedCheckRemainsUnknownWithoutPoll(t *testing.T) {
	var unavailable atomic.Bool
	h, cfg, calls, m, bucket := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		if unavailable.Load() {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(capacityAccountResponse))
	})
	setAutoProviderOptIn(t, cfg, true)
	_, first := capacityCheck(t, h, "xtream")
	assertOccupancyUnknown(t, first, "no_common_trusted_measurement")
	h.providerCapacity.mu.Lock()
	past := time.Now().Add(-time.Minute)
	for _, entry := range h.providerCapacity.entries {
		entry.expires = past
		entry.result.CacheExpiresAt = &past
	}
	h.providerCapacity.mu.Unlock()
	unavailable.Store(true)
	for i := 0; i < 3; i++ {
		if _, limit := m.Usage(bucket, 3); limit != 3 {
			t.Fatal("expired cap did not fall back")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("expiry/current local reads polled provider")
	}
	// The next explicit check alone fetches again; no stale provider counter is
	// promoted into a current or external measurement.
	_, failed := capacityCheck(t, h, "xtream")
	assertOccupancyUnknown(t, failed, "provider_count_unavailable")
	if failed.ProviderActiveConnections != nil || failed.State != "unavailable" || failed.EnforcedLimit != 3 || calls.Load() != 2 {
		t.Fatal("failed explicit check reused old counter")
	}
}

func TestProviderOccupancyActualWireOpenAPI(t *testing.T) {
	h, _, calls, _, _ := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_info":{"auth":1,"status":"Active","max_connections":0,"active_cons":0}}`))
	})
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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
		t.Fatal(w.Body.String())
	}
	if err := validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
		t.Fatalf("schema: %v\n%s", err, w.Body.String())
	}
	var report ProviderCapacityReport
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	assertOccupancyUnknown(t, report, "no_common_trusted_measurement")
	if report.State != "unknown" || report.ProviderActiveConnections == nil || *report.ProviderActiveConnections != 0 || report.ReportedLimit != nil || calls.Load() != 1 {
		t.Fatal("limit/count availability conflated")
	}
}
