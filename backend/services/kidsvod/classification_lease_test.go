package kidsvod

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func openClassificationLease(t *testing.T, s *Service, f *fixture) (*Intent, *Lease) {
	t.Helper()
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertClassified(t, decision)
	lease, decision := s.OpenLease(context.Background(), intent, nil)
	assertClassified(t, decision)
	if lease == nil || lease.grant != nil {
		t.Fatal("classification lease required a parent grant")
	}
	t.Cleanup(lease.Close)
	return intent, lease
}

func TestClassificationLeaseWithoutGrantExpiresAtEarliestAuthorityBoundary(t *testing.T) {
	for _, boundary := range []string{"classification", "source", "session"} {
		t.Run(boundary, func(t *testing.T) {
			s, f, reader := newVerifiedFixture(t, false)
			now := f.clock.Now()
			want := reader.proof.facts.ExpiresAt
			if boundary == "source" {
				f.binding.ExpiresAt = now.Add(20 * time.Second)
				want = f.binding.ExpiresAt
			}
			if boundary == "session" {
				f.session.ExpiresAt = now.Add(10 * time.Second)
				want = f.session.ExpiresAt
			}
			intent, lease := openClassificationLease(t, s, f)
			if !lease.ExpiresAt().Equal(want) || len(s.grants) != 0 {
				t.Fatal("grantless expiry or parent state incorrect")
			}
			assertClassified(t, lease.Check(context.Background()))
			f.clock.advance(want.Sub(now))
			assertClosed(t, lease, CodeApprovalExpired)
			assertDenied(t, s.EvaluateStored(context.Background(), intent, nil))
			assertDenied(t, lease.Check(context.Background()))
			if f.providerCalls.Load() != 0 || f.parentCalls.Load() != 0 {
				t.Fatal("lease initiated provider or parent work")
			}
		})
	}
}

func TestClassificationLeaseRechecksAllOriginalProvenanceAndAuthority(t *testing.T) {
	for _, change := range []string{"age cap", "country", "disabled child", "policy revision", "profile PIN", "ownership", "session", "client", "source", "rights", "binding revision", "edition", "authority revision", "mapping revision", "provenance revision", "observation", "expiry", "conflict", "missing reader"} {
		t.Run(change, func(t *testing.T) {
			s, f, reader := newVerifiedFixture(t, false)
			_, lease := openClassificationLease(t, s, f)
			switch change {
			case "age cap":
				f.policy.MaxMovieAge = age(0)
			case "country":
				f.policy.Country = "US"
			case "disabled child":
				f.policy.Enabled = false
			case "policy revision":
				f.policy.Revision = "policy-v2"
			case "profile PIN":
				f.profile.Revision = "pin-v2"
			case "ownership":
				f.profile.AccountID = "other-account"
			case "session":
				f.session.Token = "revoked-session"
			case "client":
				f.session.ClientID = "other-device"
			case "source":
				f.binding.SourceVersion = "file_v1_" + strings.Repeat("b", 64)
			case "rights":
				f.cacheMiss = true
			case "binding revision":
				f.binding.Revision = "binding-v2"
			case "edition":
				f.binding.EditionID = "another-cut"
			case "authority revision":
				reader.proof.facts.AuthorityRevision = "authority-v2"
			case "mapping revision":
				reader.proof.facts.MappingRevision = "mapping-v2"
			case "provenance revision":
				reader.proof.facts.ProvenanceRevision = "provenance-v2"
			case "observation":
				reader.proof.facts.ObservedAt = reader.proof.facts.ObservedAt.Add(time.Second)
			case "expiry":
				reader.proof.facts.ExpiresAt = reader.proof.facts.ExpiresAt.Add(time.Minute)
			case "conflict":
				reader.proof.facts.Conflicting = true
			case "missing reader":
				s.deps.Classifications = nil
			}
			assertDenied(t, lease.Check(context.Background()))
			select {
			case <-lease.Context().Done():
			default:
				t.Fatal("stale classification lease remained open")
			}
			if f.providerCalls.Load() != 0 {
				t.Fatal("recheck opened a provider")
			}
		})
	}
}

func TestClassificationLeaseCannotBeRetargetedToDifferentEpisodeOrdering(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, true)
	_, lease := openClassificationLease(t, s, f)
	ep := copyEpisode(f.binding.Episode)
	ep.Numbering.Ordering = "dvd"
	f.binding.Episode = ep
	facts := reader.proof.facts
	facts.Episode = copyEpisode(ep)
	facts.MappingRevision = "new-exact-dvd-map"
	replacement, err := NewVerifiedRegionalClassification(f.binding, facts)
	if err != nil {
		t.Fatal(err)
	}
	reader.proof = replacement
	assertDenied(t, lease.Check(context.Background()))
	_, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertClassified(t, decision) // A distinct current intent may be classified; the old lease cannot migrate.
}

func TestClassificationInvalidationCancellationAndEpochBudgetRetainCorrectLease(t *testing.T) {
	s, f, _ := newVerifiedFixture(t, false)
	_, lease := openClassificationLease(t, s, f)
	for i := 0; i < 2200; i++ {
		s.InvalidateProfile("unrelated-" + time.Duration(i).String())
	}
	assertClassified(t, lease.Check(context.Background()))
	s.mu.Lock()
	count := len(s.epochs)
	s.mu.Unlock()
	if count > 2048 {
		t.Fatal("unbounded profile epochs")
	}
	s.InvalidateProfile("child")
	assertClosed(t, lease, CodeContextChanged)
	s, f, _ = newVerifiedFixture(t, false)
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertClassified(t, decision)
	ctx, cancel := context.WithCancel(context.Background())
	lease, decision = s.OpenLease(ctx, intent, nil)
	assertClassified(t, decision)
	cancel()
	lease.Close()
	assertClosed(t, lease, CodeContextChanged)
	f.clock.mu.Lock()
	timers := len(f.clock.timers)
	f.clock.mu.Unlock()
	if timers != 0 {
		t.Fatal("cancelled lease retained timer")
	}
	data, err := json.Marshal(lease)
	if err != nil || string(data) != "null" {
		t.Fatal("lease authority exported")
	}
	var imported Lease
	if json.Unmarshal([]byte(`{"grant":null,"intent":{}}`), &imported) == nil {
		t.Fatal("lease imported")
	}
	assertDenied(t, imported.Check(context.Background()))
}

func TestParentAndClassificationLeasesHaveIndependentExpiryAndRevocation(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, false)
	reader.proof.facts.ExpiresAt = f.clock.Now().Add(time.Minute)
	_, classificationLease := openClassificationLease(t, s, f)
	intent, grant := issue(t, s, f)
	parentLease, decision := s.OpenLease(context.Background(), intent, grant)
	if parentLease == nil || decision.Outcome() != AllowedByParent {
		t.Fatal("parent branch lost")
	}
	t.Cleanup(parentLease.Close)
	if !parentLease.ExpiresAt().Equal(f.clock.Now().Add(time.Hour)) {
		t.Fatal("parent lease borrowed classification expiry")
	}
	s.RevokeParentException(httptest.NewRecorder(), f.request(true), grant)
	assertClosed(t, parentLease, CodeAccessDenied)
	assertClassified(t, classificationLease.Check(context.Background()))
	// A supplied revoked grant never falls back to another classification allowance.
	assertDenied(t, s.EvaluateStored(context.Background(), intent, grant))
	f.clock.advance(time.Minute)
	assertClosed(t, classificationLease, CodeApprovalExpired)
	_, newGrant := issue(t, s, f)
	next, decision := s.OpenLease(context.Background(), intent, newGrant)
	if next == nil || decision.Outcome() != AllowedByParent {
		t.Fatal("classification expiry blocked fresh explicit parent exception")
	}
	next.Close()
}

func TestBlockedClassificationCheckDiscardsLateProofAfterDependencyChangeOrCancellation(t *testing.T) {
	for _, mode := range []string{"policy", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, f, reader := newVerifiedFixture(t, false)
			_, lease := openClassificationLease(t, s, f)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			reader.hook = func() { once.Do(func() { close(entered); <-release }) }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan Decision, 1)
			go func() { result <- lease.Check(ctx) }()
			<-entered
			if mode == "policy" {
				f.mu.Lock()
				f.policy.Revision = "late-policy-v2"
				f.mu.Unlock()
			} else {
				cancel()
			}
			close(release)
			decision := <-result
			assertDenied(t, decision)
			select {
			case <-lease.Context().Done():
			default:
				t.Fatal("late read kept lease alive")
			}
			if f.providerCalls.Load() != 0 {
				t.Fatal("blocked check initiated provider work")
			}
		})
	}
}

func TestClassificationLeaseMemoryCapDoesNotEvictCurrentAuthority(t *testing.T) {
	s, f, _ := newVerifiedFixture(t, false)
	intent, first := openClassificationLease(t, s, f)
	for i := 1; i < MaxLeases; i++ {
		lease, decision := s.OpenLease(context.Background(), intent, nil)
		if lease == nil || decision.Outcome() != AllowedByClassification {
			t.Fatal("early classification lease cap")
		}
	}
	if lease, decision := s.OpenLease(context.Background(), intent, nil); lease != nil || decision.Allowed() {
		t.Fatal("unbounded classification leases")
	}
	assertClassified(t, first.Check(context.Background()))
	s.InvalidateProfile("child")
	assertClosed(t, first, CodeContextChanged)
	s.mu.Lock()
	count := len(s.leases)
	grants := len(s.grants)
	s.mu.Unlock()
	if count != 0 || grants != 0 {
		t.Fatal("grantless invalidation leaked state")
	}
}
