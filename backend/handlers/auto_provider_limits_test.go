package handlers

import (
	"context"
	"errors"
	"net/http"
	"novastream/config"
	"novastream/internal/livecapacity"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func autoProviderFixture(t *testing.T, respond http.HandlerFunc) (*LiveHandler, *config.Manager, *atomic.Int32, *livecapacity.Manager, string) {
	t.Helper()
	h, cfg, calls := capacityFixture(t, respond)
	settings, err := cfg.Load()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		settings.Live.Sources[i].MaxStreams = 3
	}
	if err := cfg.Save(settings); err != nil {
		t.Fatal(err)
	}
	source, ok := savedCapacitySource(settings, "xtream")
	if !ok {
		t.Fatal("source missing")
	}
	bucket, _ := deriveLiveSourceBucket(source.Mode, source)
	manager := livecapacity.NewManager()
	manager.SetAutomaticLimitReader(h.AutomaticProviderLimit)
	old := livecapacity.Default
	livecapacity.Default = manager
	t.Cleanup(func() { livecapacity.Default = old })
	return h, cfg, calls, manager, bucket
}
func setAutoProviderOptIn(t *testing.T, cfg *config.Manager, on bool) {
	t.Helper()
	s, err := cfg.Load()
	if err != nil {
		t.Fatal(err)
	}
	s.Live.AutoAdoptProviderLimits = on
	if err := cfg.Save(s); err != nil {
		t.Fatal(err)
	}
}

func TestAutoProviderLimitsActualReportAndNormalAdmission(t *testing.T) {
	h, cfg, calls, m, bucket := autoProviderFixture(t, nil)
	if h.AutomaticProviderLimit(bucket).Limit != 0 || calls.Load() != 0 {
		t.Fatal("default opt-out performed work")
	}
	if w, r := capacityCheck(t, h, "xtream"); w.Code != 200 || r.AutomaticAdoptionEnabled || r.EnforcementOrigin != "manual" {
		t.Fatalf("default report %+v", r)
	}
	setAutoProviderOptIn(t, cfg, true)
	w, report := capacityCheck(t, h, "export")
	if w.Code != 200 || !report.Cached || !report.AutomaticAdoptionEnabled || report.AutomaticLimit == nil || *report.AutomaticLimit != 2 || report.EnforcedLimit != 2 || report.EnforcementOrigin != "provider_extra_cap" {
		t.Fatalf("fresh checked report %+v body=%s", report, w.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatal("alias/check created extra provider calls")
	}
	a, err := m.Acquire(bucket, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	dvr, err := m.AcquireOwned(bucket, "", 3, livecapacity.Owner{Kind: livecapacity.KindRecording, RecordingID: "synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	defer dvr.Release()
	if _, err := m.Acquire(bucket, "", 3); !errors.Is(err, livecapacity.ErrLimit) {
		t.Fatal("normal admission bypassed fresh report", err)
	}
	if current, limit := m.Usage(bucket, 3); current != 2 || limit != 2 {
		t.Fatalf("usage %d/%d", current, limit)
	}
	// Expiry is a peek-only decision, even while live and DVR inputs stay active.
	h.providerCapacity.now = func() time.Time { return report.CacheExpiresAt.Add(time.Second) }
	if h.AutomaticProviderLimit(bucket).Limit != 0 {
		t.Fatal("expired observation used")
	}
	third, err := m.Acquire(bucket, "", 3)
	if err != nil {
		t.Fatal("expired overlay stuck until idle", err)
	}
	third.Release()
	if current, limit := m.Usage(bucket, 3); current != 2 || limit != 3 {
		t.Fatal("manual fallback lost active inputs")
	}
	h.providerCapacity.now = nil
	setAutoProviderOptIn(t, cfg, false)
	if _, limit := m.Usage(bucket, 3); limit != 3 {
		t.Fatal("opt-out did not remove active overlay")
	}
	if calls.Load() != 1 {
		t.Fatalf("admission/expiry/optout made HTTP requests: %d", calls.Load())
	}
}

func TestAutoProviderLimitsCurrentSourceFingerprintAndAliases(t *testing.T) {
	h, cfg, calls, m, bucket := autoProviderFixture(t, nil)
	setAutoProviderOptIn(t, cfg, true)
	if w, _ := capacityCheck(t, h, "xtream"); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	base, err := cfg.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []struct {
		name   string
		mutate func(*config.Settings)
	}{
		{"different-password-alias", func(s *config.Settings) { s.Live.Sources[0].XtreamPassword = "another-synthetic-password" }},
		{"different-panel-same-bucket", func(s *config.Settings) {
			s.Live.Sources[0].XtreamHost = strings.TrimSuffix(s.Live.Sources[0].XtreamHost, "/panel") + "/other-panel"
		}},
		{"different-proxy-alias", func(s *config.Settings) { s.Live.Sources[0].ProxyURL = "http://synthetic.invalid:9999" }},
		{"removed-observed-alias-and-changed-other", func(s *config.Settings) {
			s.Live.Sources = s.Live.Sources[1:]
			s.Live.Sources[0].PlaylistURL = strings.Replace(s.Live.Sources[0].PlaylistURL, "synthetic-password", "new-password", 1)
		}},
		{"disabled", func(s *config.Settings) {
			off := false
			s.Live.Sources[0].Enabled = &off
			s.Live.Sources[1].Enabled = &off
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			s, err := cfg.Load()
			if err != nil {
				t.Fatal(err)
			}
			s.Live.Sources = append([]config.LivePlaylistSource(nil), base.Live.Sources...)
			change.mutate(&s)
			if err := cfg.Save(s); err != nil {
				t.Fatal(err)
			}
			if h.AutomaticProviderLimit(bucket).Limit != 0 {
				t.Fatal("changed source retained old fact")
			}
			if _, limit := m.Usage(bucket, 3); limit != 3 {
				t.Fatal("changed source failed manual fallback")
			}
			if err := cfg.Save(base); err != nil {
				t.Fatal(err)
			}
		})
	}
	if h.AutomaticProviderLimit(bucket).Limit != 2 {
		t.Fatal("matching current saved aliases failed")
	}
	if calls.Load() != 1 {
		t.Fatal("cache-only fingerprint checks fetched provider")
	}
}

func TestAutoProviderLimitsUnknownAndErrorsDiscardPreviousFact(t *testing.T) {
	for _, body := range []string{`{"user_info":{"auth":1,"status":"Active","max_connections":0}}`, `{"user_info":{"auth":0,"status":"Active","max_connections":2}}`, `{malformed`} {
		t.Run(body, func(t *testing.T) {
			var failed atomic.Bool
			h, cfg, calls, m, bucket := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) {
				if failed.Load() {
					_, _ = w.Write([]byte(body))
				} else {
					_, _ = w.Write([]byte(capacityAccountResponse))
				}
			})
			setAutoProviderOptIn(t, cfg, true)
			if w, _ := capacityCheck(t, h, "xtream"); w.Code != 200 {
				t.Fatal(w.Body.String())
			}
			h.providerCapacity.mu.Lock()
			for _, entry := range h.providerCapacity.entries {
				entry.expires = time.Now().Add(-time.Second)
			}
			h.providerCapacity.mu.Unlock()
			failed.Store(true)
			if w, report := capacityCheck(t, h, "xtream"); w.Code != 200 || report.AutomaticLimit != nil || report.EnforcementOrigin != "manual" {
				t.Fatalf("unknown/error retained old report %+v", report)
			}
			if _, limit := m.Usage(bucket, 3); limit != 3 || calls.Load() != 2 {
				t.Fatal("unknown/error did not fall back without more HTTP")
			}
		})
	}
}

func TestAutoProviderLimitsFetchIsNotAuthorityBeforeSavedSourceRecheck(t *testing.T) {
	h, cfg, calls, _, bucket := autoProviderFixture(t, nil)
	setAutoProviderOptIn(t, cfg, true)
	settings, _ := cfg.Load()
	source, _ := savedCapacitySource(settings, "xtream")
	endpoint, _ := providerCapacityEndpoint(source)
	key := liveDigest(endpoint.String(), source.ProxyURL)
	report, _, err := h.providerCapacity.check(context.Background(), key, func(ctx context.Context) ProviderCapacityReport {
		return h.fetchProviderCapacity(ctx, endpoint, source.ProxyURL)
	})
	if err != nil || report.State != "available" || calls.Load() != 1 {
		t.Fatal("actual observation failed", err)
	}
	if h.AutomaticProviderLimit(bucket).Limit != 0 {
		t.Fatal("unconfirmed fetch bypassed saved-source publication check")
	}
	if w, confirmed := capacityCheck(t, h, "xtream"); w.Code != 200 || !confirmed.Cached || h.AutomaticProviderLimit(bucket).Limit != 2 || calls.Load() != 1 {
		t.Fatal("normal confirmed producer failed")
	}
}

func TestAutoProviderLimitsConcurrentAdmissionUsesOneCheckedObservation(t *testing.T) {
	h, cfg, calls, m, bucket := autoProviderFixture(t, nil)
	setAutoProviderOptIn(t, cfg, true)
	capacityCheck(t, h, "xtream")
	start := make(chan struct{})
	leases := make(chan *livecapacity.Lease, 48)
	var wg sync.WaitGroup
	for i := 0; i < 48; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			lease, err := m.Acquire(bucket, "", 3)
			if err == nil {
				leases <- lease
			} else if !errors.Is(err, livecapacity.ErrLimit) {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(leases)
	if len(leases) != 2 || calls.Load() != 1 {
		t.Fatalf("concurrent admission %d provider calls %d", len(leases), calls.Load())
	}
	for lease := range leases {
		lease.Release()
	}
}

func TestAutoProviderLimitsStickyStricterManualOriginRemainsManual(t *testing.T) {
	h, cfg, _, m, bucket := autoProviderFixture(t, nil)
	setAutoProviderOptIn(t, cfg, true)
	old, err := m.Acquire(bucket, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	w, report := capacityCheck(t, h, "xtream")
	if w.Code != 200 || report.EnforcedLimit != 1 || report.AutomaticLimit != nil || report.EnforcementOrigin != "manual" {
		t.Fatalf("sticky manual origin falsely attributed to report: %+v", report)
	}
}

func TestAutoProviderLimitsChangedSourceDuringActualCheckNotPublished(t *testing.T) {
	var cfg *config.Manager
	h, current, calls, m, bucket := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		s, err := cfg.Load()
		if err != nil {
			t.Error(err)
			return
		}
		s.Live.Sources[0].MaxStreams = 4
		if err := cfg.Save(s); err != nil {
			t.Error(err)
			return
		}
		_, _ = w.Write([]byte(capacityAccountResponse))
	})
	cfg = current
	setAutoProviderOptIn(t, cfg, true)
	w, _ := capacityCheck(t, h, "xtream")
	if w.Code != 409 || calls.Load() != 1 || h.AutomaticProviderLimit(bucket).Limit != 0 {
		t.Fatalf("rejected report became automatic authority: status=%d calls=%d", w.Code, calls.Load())
	}
	if _, limit := m.Usage(bucket, 3); limit != 3 {
		t.Fatal("unverified fetched fact changed admission")
	}
}

func TestAutoProviderLimitsNewProcessHasNoRestoredReportAuthority(t *testing.T) {
	h, cfg, calls, _, bucket := autoProviderFixture(t, nil)
	setAutoProviderOptIn(t, cfg, true)
	capacityCheck(t, h, "xtream")
	if h.AutomaticProviderLimit(bucket).Limit != 2 {
		t.Fatal("actual producer baseline absent")
	}
	reopened := NewLiveHandler(nil, false, "", 1, 0, 0, false, cfg, nil)
	if reopened.AutomaticProviderLimit(bucket).Limit != 0 || calls.Load() != 1 {
		t.Fatal("configuration rehydrated private report or caused a fetch")
	}
}

func TestAutoProviderLimitsEqualStickyManualAndReportIsManual(t *testing.T) {
	h, cfg, _, m, bucket := autoProviderFixture(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_info":{"auth":1,"status":"Active","max_connections":1}}`))
	})
	setAutoProviderOptIn(t, cfg, true)
	old, err := m.Acquire(bucket, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	w, report := capacityCheck(t, h, "xtream")
	if w.Code != 200 || report.EnforcedLimit != 1 || report.SharedConfiguredLimit != 3 || report.AutomaticLimit != nil || report.EnforcementOrigin != "manual" {
		t.Fatalf("equal sticky manual cap misclassified: %+v", report)
	}
}

func TestAutoProviderLimitsGlobalBudgetAlsoRestrictsCoarsePrivateAliases(t *testing.T) {
	h, cfg, _, m, bucket := autoProviderFixture(t, nil)
	setAutoProviderOptIn(t, cfg, true)
	capacityCheck(t, h, "xtream")
	settings, _ := cfg.Load()
	global, _ := savedCapacitySource(settings, "xtream")
	private := global
	private.ID = "private-profile-source"
	private.XtreamHost = strings.TrimSuffix(global.XtreamHost, "/panel") + "/private-panel"
	private.XtreamPassword = "different-private-fixture-password"
	private.ProxyURL = "http://synthetic-private-proxy.invalid:8888"
	privateBucket, _ := deriveLiveSourceBucket(private.Mode, private)
	globalEndpoint, _ := providerCapacityEndpoint(global)
	privateEndpoint, _ := providerCapacityEndpoint(private)
	if privateBucket != bucket || liveDigest(globalEndpoint.String(), global.ProxyURL) == liveDigest(privateEndpoint.String(), private.ProxyURL) {
		t.Fatal("fixture did not establish coarse shared bucket with distinct exact identities")
	}
	// This approved overlay is a restriction on the existing global account
	// bucket. It does not prove that the private selected transport was checked.
	if h.AutomaticProviderLimit(privateBucket).Limit != 2 {
		t.Fatal("global shared-budget overlay missing")
	}
	if _, limit, automatic := m.UsageWithAutomaticLimit(privateBucket, 1); limit != 1 || automatic.Limit != 0 {
		t.Fatal("global fact raised private manual cap")
	}
	if _, limit, automatic := m.UsageWithAutomaticLimit(privateBucket, 5); limit != 2 || automatic.Limit != 2 {
		t.Fatal("global extra cap did not restrict shared bucket")
	}
}
