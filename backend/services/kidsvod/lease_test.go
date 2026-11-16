package kidsvod

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"novastream/models"
)

func openFixtureLease(t *testing.T, s *Service, f *fixture) (*Intent, *Grant, *Lease) {
	t.Helper()
	intent, grant := issue(t, s, f)
	lease, decision := s.OpenLease(context.Background(), intent, grant)
	if lease == nil || !decision.Allowed() {
		t.Fatalf("lease failed: %s", decision.Code())
	}
	t.Cleanup(lease.Close)
	return intent, grant, lease
}
func assertClosed(t *testing.T, lease *Lease, code string) {
	t.Helper()
	select {
	case <-lease.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("lease remained open")
	}
	if lease.Reason() != code {
		t.Fatalf("reason %s want %s", lease.Reason(), code)
	}
}

func TestLeaseExpiresAtEarliestParentSourceOrSessionBoundary(t *testing.T) {
	for _, boundary := range []string{"parent", "source", "session"} {
		t.Run(boundary, func(t *testing.T) {
			s, f := newFixture(t)
			now := f.clock.Now()
			ttl := time.Minute
			if boundary == "source" {
				f.binding.ExpiresAt = now.Add(30 * time.Second)
			}
			if boundary == "session" {
				f.session.ExpiresAt = now.Add(20 * time.Second)
			}
			intent, _ := s.ResolveRequest(f.request(false), "child", f.ref)
			grant, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, ttl)
			if grant == nil || !decision.Allowed() {
				t.Fatal(decision.Code())
			}
			lease, decision := s.OpenLease(context.Background(), intent, grant)
			if lease == nil || !decision.Allowed() {
				t.Fatal(decision.Code())
			}
			want := now.Add(ttl)
			if boundary == "source" {
				want = f.binding.ExpiresAt
			}
			if boundary == "session" {
				want = f.session.ExpiresAt
			}
			if !lease.ExpiresAt().Equal(want) {
				t.Fatal("lease outlives authority")
			}
			f.clock.advance(want.Sub(now))
			assertClosed(t, lease, CodeApprovalExpired)
			if lease.Check(context.Background()).Allowed() {
				t.Fatal("expired lease allowed")
			}
		})
	}
}

func TestLeaseRechecksExactFileEpisodePolicyAndCurrentOwnership(t *testing.T) {
	for _, change := range []string{"file", "source revision", "policy", "ownership", "session", "client", "pin revision", "episode", "numbering", "coordinates"} {
		t.Run(change, func(t *testing.T) {
			s, f := newFixture(t)
			if change == "episode" || change == "numbering" || change == "coordinates" {
				f.binding.MediaType = "series"
				f.binding.CatalogID = "tmdb:series:41"
				f.binding.Episode = &models.EpisodeReference{EpisodeID: "tvdb:episode:123", SeasonNumber: 0, EpisodeNumber: 2, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}}
			}
			_, _, lease := openFixtureLease(t, s, f)
			f.mu.Lock()
			switch change {
			case "file":
				f.binding.SourceVersion = "file_v1_" + strings.Repeat("b", 64)
			case "source revision":
				f.binding.Revision = "source-v2"
			case "policy":
				f.policy.Revision = "policy-v2"
			case "ownership":
				f.profile.AccountID = "foreign"
			case "session":
				f.session.Token = "revoked-session"
			case "client":
				f.session.ClientID = "other-device"
			case "pin revision":
				f.profile.Revision = "pin-v2"
			case "numbering":
				ep := *f.binding.Episode
				n := *ep.Numbering
				n.Ordering = "dvd"
				ep.Numbering = &n
				f.binding.Episode = &ep
			case "coordinates":
				ep := *f.binding.Episode
				ep.EpisodeNumber++
				f.binding.Episode = &ep
			case "episode":
				ep := *f.binding.Episode
				ep.EpisodeID = "tvdb:episode:456"
				f.binding.Episode = &ep
			}
			f.mu.Unlock()
			assertDenied(t, lease.Check(context.Background()))
			select {
			case <-lease.Context().Done():
			default:
				t.Fatal("changed permission did not cancel ongoing lease")
			}
			if f.providerCalls.Load() != 0 {
				t.Fatal("recheck initiated provider work")
			}
		})
	}
}

func TestPublicRevocationNeedsFreshParentProofAndCancelsExistingLease(t *testing.T) {
	s, f := newFixture(t)
	intent, grant, lease := openFixtureLease(t, s, f)
	rec := httptest.NewRecorder()
	decision := s.RevokeParentException(rec, f.request(false), grant)
	if decision.Code() != CodeParentApprovalRequired || rec.Code != 403 {
		t.Fatal("account session revoked without parent confirmation")
	}
	if !s.EvaluateStored(context.Background(), intent, grant).Allowed() {
		t.Fatal("failed confirmation mutated exception")
	}
	decision = s.RevokeParentException(httptest.NewRecorder(), f.request(true), grant)
	if decision.Allowed() {
		t.Fatal("revocation returned an allow")
	}
	assertClosed(t, lease, CodeAccessDenied)
	assertDenied(t, s.EvaluateStored(context.Background(), intent, grant))
	if f.parentCalls.Load() != 2 {
		t.Fatal("fresh proof not required on issue and revoke")
	}
}

func TestTrustedInvalidationAndCallerCancellationCloseWithoutPolling(t *testing.T) {
	s, f := newFixture(t)
	_, _, lease := openFixtureLease(t, s, f)
	s.InvalidateProfile("unrelated")
	if !lease.Check(context.Background()).Allowed() {
		t.Fatal("unrelated profile revoked current approval")
	}
	s.InvalidateProfile("child")
	assertClosed(t, lease, CodeContextChanged)
	s, f = newFixture(t)
	intent, grant := issue(t, s, f)
	ctx, cancel := context.WithCancel(context.Background())
	lease, decision := s.OpenLease(ctx, intent, grant)
	if !decision.Allowed() {
		t.Fatal(decision.Code())
	}
	cancel()
	lease.Close()
	assertClosed(t, lease, CodeContextChanged)
	f.clock.mu.Lock()
	timers := len(f.clock.timers)
	f.clock.mu.Unlock()
	if timers != 0 {
		t.Fatal("closed lease retained expiry timer")
	}
	var zero Lease
	select {
	case <-zero.Context().Done():
	default:
		t.Fatal("zero lease is usable")
	}
	assertDenied(t, zero.Check(context.Background()))
}

func TestDisablingPolicyDoesNotTurnAnOldRegionalLeaseIntoLegacyPermission(t *testing.T) {
	s, f := newFixture(t)
	_, _, lease := openFixtureLease(t, s, f)
	f.mu.Lock()
	f.policy.Enabled = false
	f.policy.Revision = "explicitly-disabled-v2"
	f.mu.Unlock()
	decision := lease.Check(context.Background())
	if decision.Allowed() || decision.IsNotApplicable() {
		t.Fatal("old regional lease fell through to legacy access")
	}
	assertClosed(t, lease, CodeContextChanged)
}

func TestMemoryCapsKeepExistingLeasesAndExpiredApprovalsAreRecoverable(t *testing.T) {
	s, f := newFixture(t)
	intent, grant, lease := openFixtureLease(t, s, f)
	for i := 1; i < MaxGrants; i++ {
		g, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Hour)
		if g == nil || !decision.Allowed() {
			t.Fatal("early approval cap")
		}
	}
	if g, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Hour); g != nil || decision.Allowed() {
		t.Fatal("unbounded approvals")
	}
	if !lease.Check(context.Background()).Allowed() {
		t.Fatal("cap evicted ongoing authority")
	}
	for i := 1; i < MaxLeases; i++ {
		l, decision := s.OpenLease(context.Background(), intent, grant)
		if l == nil || !decision.Allowed() {
			t.Fatal("early lease cap")
		}
	}
	if l, decision := s.OpenLease(context.Background(), intent, grant); l != nil || decision.Allowed() {
		t.Fatal("unbounded leases")
	}
	f.clock.advance(time.Hour)
	assertClosed(t, lease, CodeApprovalExpired)
	if g, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Hour); g == nil || !decision.Allowed() {
		t.Fatal("expired approval quota was not recoverable")
	}
	s.mu.Lock()
	count, leases := len(s.grants), len(s.leases)
	s.mu.Unlock()
	if count != 1 || leases != 0 {
		t.Fatal("expired memory retained")
	}
}

func TestProfileEpochBudgetDoesNotEvictActiveOtherProfileAuthority(t *testing.T) {
	s, f := newFixture(t)
	_, _, lease := openFixtureLease(t, s, f)
	for i := 0; i < 2200; i++ {
		s.InvalidateProfile(strings.Repeat("x", i%127+1) + time.Duration(i).String())
	}
	s.mu.Lock()
	count := len(s.epochs)
	s.mu.Unlock()
	if count > 2048 {
		t.Fatal("unbounded revision cache")
	}
	if !lease.Check(context.Background()).Allowed() {
		t.Fatal("unrelated revision eviction lost active child lease")
	}
}

func TestConcurrentIssueRevokeCheckAndInvalidationNeverCreateUnownedAuthority(t *testing.T) {
	s, f := newFixture(t)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				intent, _ := s.ResolveRequest(f.request(false), "child", f.ref)
				grant, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Minute)
				if grant == nil {
					if decision.Allowed() {
						t.Error("nil grant allowed")
					}
					continue
				}
				lease, decision := s.OpenLease(context.Background(), intent, grant)
				if lease != nil {
					if !decision.Allowed() {
						t.Error("denied lease returned")
					}
					_ = lease.Check(context.Background())
					lease.Close()
				}
				_ = s.RevokeParentException(httptest.NewRecorder(), f.request(true), grant)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; n < 100; n++ {
			s.InvalidateProfile("child")
		}
	}()
	wg.Wait()
	s.InvalidateProfile("child")
	s.mu.Lock()
	grants, leases := len(s.grants), len(s.leases)
	s.mu.Unlock()
	if grants > MaxGrants || leases != 0 || f.providerCalls.Load() != 0 {
		t.Fatal("concurrent lifecycle leaked authority or external work")
	}
}
