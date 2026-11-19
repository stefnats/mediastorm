package kidsvod

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"novastream/models"
)

type verifiedReader struct {
	mu      sync.Mutex
	proof   *VerifiedRegionalClassification
	err     error
	hook    func()
	calls   int
	subject Subject
	binding Binding
}

func (r *verifiedReader) ResolveVerifiedCacheOnly(_ context.Context, subject Subject, binding Binding) (*VerifiedRegionalClassification, error) {
	r.mu.Lock()
	r.calls++
	r.subject, r.binding = subject, copyBinding(binding)
	proof, err, hook := r.proof, r.err, r.hook
	r.mu.Unlock()
	if hook != nil {
		hook()
	}
	return proof, err
}
func newVerifiedFixture(t *testing.T, episode bool) (*Service, *fixture, *verifiedReader) {
	t.Helper()
	s, f := newFixture(t)
	f.binding.EditionID = "trusted-played-theatrical-edition-v1"
	if episode {
		f.binding.MediaType, f.binding.CatalogID, f.binding.EditionID = "series", "tmdb:series:41", ""
		f.binding.Episode = &models.EpisodeReference{EpisodeID: "tvdb:episode:123", SeasonNumber: 1, EpisodeNumber: 2, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}}
	}
	facts := RegionalClassificationFacts{Resource: f.ref, CatalogID: f.binding.CatalogID, MediaType: f.binding.MediaType, EditionID: f.binding.EditionID, Episode: copyEpisode(f.binding.Episode), SourceVersion: f.binding.SourceVersion, BindingRevision: f.binding.Revision, Country: "DE", Scheme: "FSK", Age: 6, AuthorityID: "synthetic-regional-authority", AuthorityRevision: "authority-v1", ProvenanceRevision: "provenance-v1", MappingRevision: "trusted-exact-mapping-v1", ObservedAt: f.clock.Now().Add(-30 * time.Second), ExpiresAt: f.clock.Now().Add(30 * time.Minute), Complete: true}
	proof, err := NewVerifiedRegionalClassification(f.binding, facts)
	if err != nil {
		t.Fatal(err)
	}
	reader := &verifiedReader{proof: proof}
	s.deps.Classifications = reader
	return s, f, reader
}
func assertClassified(t *testing.T, decision Decision) {
	t.Helper()
	if !decision.Allowed() || decision.Outcome() != AllowedByClassification || decision.IsNotApplicable() || decision.Code() != "" {
		t.Fatalf("missing classification allowance: %d %s", decision.Outcome(), decision.Code())
	}
}

func TestVerifiedMovieAndExactEpisodeAllowAllSupportedFSKCapEdges(t *testing.T) {
	for _, episode := range []bool{false, true} {
		for _, cap := range []int{0, 6, 12, 16, 18} {
			for _, rating := range []int{0, 6, 12, 16, 18} {
				s, f, reader := newVerifiedFixture(t, episode)
				f.policy.MaxMovieAge, f.policy.MaxEpisodeAge = age(cap), age(cap)
				reader.proof.facts.Age = rating // Synthetic server reader publication, not an HTTP input.
				intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
				if intent == nil {
					t.Fatal("exact current source lost")
				}
				if rating <= cap {
					assertClassified(t, decision)
					assertClassified(t, s.EvaluateStored(context.Background(), intent, nil))
				} else {
					if decision.Outcome() != Denied {
						t.Fatal("higher FSK age did not deny")
					}
					assertDenied(t, s.EvaluateStored(context.Background(), intent, nil))
				}
				if reader.subject.AccountID() != "household" || reader.subject.ProfileID() != "child" || reader.subject.ClientID() != "device" || reader.binding.Resource != f.ref || f.providerCalls.Load() != 0 || f.parentCalls.Load() != 0 {
					t.Fatal("classification escaped its actual server binding")
				}
			}
		}
	}
}

func TestVerifiedClassificationRejectsWrongEditionEpisodeSourceAuthorityAndLifetime(t *testing.T) {
	tests := []struct {
		name    string
		episode bool
		mutate  func(*RegionalClassificationFacts, *fixture)
	}{
		{"country", false, func(v *RegionalClassificationFacts, f *fixture) { v.Country = "FR" }},
		{"scheme", false, func(v *RegionalClassificationFacts, f *fixture) { v.Scheme = "MPAA" }},
		{"unsupported age", false, func(v *RegionalClassificationFacts, f *fixture) { v.Age = 7 }},
		{"same numeric series", false, func(v *RegionalClassificationFacts, f *fixture) {
			v.CatalogID = "tmdb:series:41"
			v.MediaType = "series"
		}},
		{"movie edition", false, func(v *RegionalClassificationFacts, f *fixture) { v.EditionID = "another-cut" }},
		{"unmapped played edition", false, func(v *RegionalClassificationFacts, f *fixture) { f.binding.EditionID = "" }},
		{"resource", false, func(v *RegionalClassificationFacts, f *fixture) { v.Resource.id = "other-file" }},
		{"source version", false, func(v *RegionalClassificationFacts, f *fixture) {
			v.SourceVersion = "file_v1_" + strings.Repeat("b", 64)
		}},
		{"binding revision", false, func(v *RegionalClassificationFacts, f *fixture) { v.BindingRevision = "stale-source" }},
		{"authority", false, func(v *RegionalClassificationFacts, f *fixture) { v.AuthorityID = "" }},
		{"authority revision", false, func(v *RegionalClassificationFacts, f *fixture) { v.AuthorityRevision = "" }},
		{"provenance revision", false, func(v *RegionalClassificationFacts, f *fixture) { v.ProvenanceRevision = "" }},
		{"mapping revision", false, func(v *RegionalClassificationFacts, f *fixture) { v.MappingRevision = "" }},
		{"incomplete", false, func(v *RegionalClassificationFacts, f *fixture) { v.Complete = false }},
		{"conflicting", false, func(v *RegionalClassificationFacts, f *fixture) { v.Conflicting = true }},
		{"expired", false, func(v *RegionalClassificationFacts, f *fixture) { v.ExpiresAt = f.clock.Now() }},
		{"future observation", false, func(v *RegionalClassificationFacts, f *fixture) { v.ObservedAt = f.clock.Now().Add(time.Second) }},
		{"unbounded lifetime", false, func(v *RegionalClassificationFacts, f *fixture) {
			v.ExpiresAt = v.ObservedAt.Add(MaxBindingTTL + time.Second)
		}},
		{"stale observation", false, func(v *RegionalClassificationFacts, f *fixture) {
			v.ObservedAt = f.clock.Now().Add(-MaxBindingTTL - time.Second)
		}},
		{"episode leaf", true, func(v *RegionalClassificationFacts, f *fixture) { v.Episode.EpisodeID = "tvdb:episode:456" }},
		{"episode coordinates", true, func(v *RegionalClassificationFacts, f *fixture) { v.Episode.EpisodeNumber++ }},
		{"episode season", true, func(v *RegionalClassificationFacts, f *fixture) { v.Episode.SeasonNumber++ }},
		{"episode ordering", true, func(v *RegionalClassificationFacts, f *fixture) { v.Episode.Numbering.Ordering = "dvd" }},
		{"episode series", true, func(v *RegionalClassificationFacts, f *fixture) { v.Episode.Numbering.SeriesID = "tvdb:series:999" }},
		{"series wide", true, func(v *RegionalClassificationFacts, f *fixture) { v.Episode = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, f, reader := newVerifiedFixture(t, test.episode)
			facts := reader.proof.facts
			facts.Episode = copyEpisode(facts.Episode)
			test.mutate(&facts, f)
			// Deliberately corrupt a synthetic configured cache; service validation
			// must remain strict even if a server adapter violates its constructor.
			reader.proof = &VerifiedRegionalClassification{facts: facts}
			intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
			assertDenied(t, decision)
			if intent != nil {
				assertDenied(t, s.EvaluateStored(context.Background(), intent, nil))
			}
			if f.providerCalls.Load() != 0 || f.parentCalls.Load() != 0 {
				t.Fatal("bad classification performed external work")
			}
		})
	}
}

func TestClassificationConstructionIsImmutableOpaqueAndCannotReplaceAnUnconfiguredReader(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, true)
	original := reader.proof.facts
	original.Episode = copyEpisode(original.Episode)
	frozen, err := NewVerifiedRegionalClassification(f.binding, original)
	if err != nil {
		t.Fatal(err)
	}
	original.Episode.EpisodeNumber = 999
	original.Episode.Numbering.Ordering = "dvd"
	reader.proof = frozen
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertClassified(t, decision)
	for _, capability := range []any{frozen, &original, Subject{account: "household", profile: "child", client: "device"}, decision, intent} {
		data, err := json.Marshal(capability)
		if err != nil || string(data) != "null" {
			t.Fatal("authority exported")
		}
	}
	for _, raw := range []string{`null`, `{"Country":"DE","Scheme":"FSK","Age":0}`, `{"facts":{"Complete":true}}`} {
		var imported VerifiedRegionalClassification
		var facts RegionalClassificationFacts
		var subject Subject
		if !errors.Is(json.Unmarshal([]byte(raw), &imported), ErrOpaqueCapability) || !errors.Is(json.Unmarshal([]byte(raw), &facts), ErrOpaqueCapability) || !errors.Is(json.Unmarshal([]byte(raw), &subject), ErrOpaqueCapability) {
			t.Fatal("authority imported")
		}
		reader.proof = &imported
		_, decision := s.ResolveRequest(f.request(false), "child", f.ref)
		assertDenied(t, decision)
	}
	reader.proof = frozen
	reader.err = errors.New("synthetic unknown authority")
	_, decision = s.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, decision)
	s.deps.Classifications = nil
	f.binding.LegacyCertification = "G"
	f.binding.CertificationEvidence = []models.CertificationEvidence{{Country: "DE", Value: "0", Source: "synthetic-copied-proof", Scope: "movie_release"}}
	_, decision = s.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, decision)
	if AllowedByClassification != 4 || AllowedByParent != 2 || Denied != 3 {
		t.Fatal("existing outcome values changed")
	}
}

func TestClassificationLateDependencyChangeOrCancellationCannotPublishAllowance(t *testing.T) {
	for _, change := range []string{"policy", "profile", "session", "rights", "source", "invalidation", "cancel"} {
		t.Run(change, func(t *testing.T) {
			s, f, reader := newVerifiedFixture(t, false)
			request := f.request(false)
			ctx, cancel := context.WithCancel(request.Context())
			defer cancel()
			reader.hook = func() {
				f.mu.Lock()
				switch change {
				case "policy":
					f.policy.Revision = "policy-v2"
					f.policy.MaxMovieAge = age(0)
				case "profile":
					f.profile.Revision = "pin-v2"
				case "session":
					f.session.Token = "revoked-session"
				case "rights":
					f.cacheMiss = true
				case "source":
					f.binding.SourceVersion = "file_v1_" + strings.Repeat("b", 64)
				}
				f.mu.Unlock()
				if change == "invalidation" {
					s.InvalidateProfile("child")
				}
				if change == "cancel" {
					cancel()
				}
			}
			intent, decision := s.ResolveRequest(request.WithContext(ctx), "child", f.ref)
			if intent != nil {
				t.Fatal("late reader published stale intent")
			}
			assertDenied(t, decision)
			if f.providerCalls.Load() != 0 {
				t.Fatal("late change opened provider")
			}
		})
	}
}

func TestDisabledChildIsUnknownWhileProvenInactiveAdultRetainsCompatibility(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, false)
	f.policy.Enabled = false
	_, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, decision)
	if reader.calls != 0 || f.cacheCalls.Load() != 0 {
		t.Fatal("disabled child reached media dependencies")
	}
	f.profile.IsKids = false
	_, decision = s.ResolveRequest(f.request(false), "child", f.ref)
	if !decision.IsNotApplicable() || decision.Allowed() {
		t.Fatal("healthy inactive adult compatibility lost")
	}
	f.policy = Policy{}
	_, decision = s.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, decision)
}

func TestExplicitParentExceptionIgnoresOptionalClassificationRefreshAndExpiry(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, false)
	reader.proof.facts.ExpiresAt = f.clock.Now().Add(time.Minute)
	intent, grant := issue(t, s, f)
	if !grant.expires.Equal(f.clock.Now().Add(time.Hour)) {
		t.Fatal("optional classification shortened parent exception")
	}
	reader.proof.facts.AuthorityRevision = "authority-v2"
	reader.proof.facts.Age = 18
	decision := s.EvaluateStored(context.Background(), intent, grant)
	if !decision.Allowed() || decision.Outcome() != AllowedByParent {
		t.Fatal("explicit exception became a classification decision")
	}
	f.clock.advance(time.Minute)
	reader.err = errors.New("synthetic classification expired")
	decision = s.EvaluateStored(context.Background(), intent, grant)
	if !decision.Allowed() || decision.Outcome() != AllowedByParent {
		t.Fatal("optional expired metadata invalidated parent permission")
	}
	decision = s.RevokeParentException(httptest.NewRecorder(), f.request(true), grant)
	assertDenied(t, decision)
	assertDenied(t, s.EvaluateStored(context.Background(), intent, grant))
}

// A configured adapter receives a copied binding, never authority it may rewrite.
type verifiedReaderFunc func(context.Context, Subject, Binding) (*VerifiedRegionalClassification, error)

func (f verifiedReaderFunc) ResolveVerifiedCacheOnly(ctx context.Context, subject Subject, binding Binding) (*VerifiedRegionalClassification, error) {
	return f(ctx, subject, binding)
}

func TestVerifiedReaderCannotMutateActualEpisodeBindingOrAcceptForeignOpaqueIntent(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, true)
	proof := reader.proof
	s.deps.Classifications = verifiedReaderFunc(func(_ context.Context, _ Subject, binding Binding) (*VerifiedRegionalClassification, error) {
		binding.Episode.EpisodeNumber = 999
		binding.Episode.Numbering.Ordering = "dvd"
		binding.CatalogID = "tmdb:series:999"
		return proof, nil
	})
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertClassified(t, decision)
	if f.binding.Episode.EpisodeNumber != 2 || f.binding.Episode.Numbering.Ordering != "official" {
		t.Fatal("classification adapter mutated exact source")
	}
	other, err := New(s.deps)
	if err != nil {
		t.Fatal(err)
	}
	assertDenied(t, other.EvaluateStored(context.Background(), intent, nil))
	var imported Intent
	if json.Unmarshal([]byte(`{"classificationOutcome":4,"classificationExpires":"2099-01-01"}`), &imported) == nil {
		t.Fatal("classification intent imported")
	}
	assertDenied(t, s.EvaluateStored(context.Background(), &imported, nil))
	var forged Decision
	if json.Unmarshal([]byte(`{"outcome":4}`), &forged) == nil {
		t.Fatal("classification decision imported")
	}
	assertDenied(t, forged)
}

func TestMovieAndEpisodeCapsRemainSeparateAndConstructorRejectsIncompleteMappings(t *testing.T) {
	for _, episode := range []bool{false, true} {
		s, f, reader := newVerifiedFixture(t, episode)
		f.policy.MaxMovieAge, f.policy.MaxEpisodeAge = age(6), age(12)
		reader.proof.facts.Age = 12
		_, decision := s.ResolveRequest(f.request(false), "child", f.ref)
		if episode {
			assertClassified(t, decision)
		} else {
			assertDenied(t, decision)
		}
		facts := reader.proof.facts
		facts.MappingRevision = ""
		if _, err := NewVerifiedRegionalClassification(f.binding, facts); err == nil {
			t.Fatal("constructor accepted unverified mapping")
		}
		facts = reader.proof.facts
		facts.Country = "US"
		if _, err := NewVerifiedRegionalClassification(f.binding, facts); err == nil {
			t.Fatal("constructor accepted wrong regional scheme")
		}
	}
}

func TestLateEditionOnlyChangeCannotPublishClassificationButRetainsDistinctParentGrant(t *testing.T) {
	s, f, reader := newVerifiedFixture(t, false)
	original, grant := issue(t, s, f)
	var once sync.Once
	reader.hook = func() {
		once.Do(func() {
			f.mu.Lock()
			f.binding.EditionID = "different-played-edition-with-same-source-revision"
			f.mu.Unlock()
		})
	}
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, decision)
	if intent == nil {
		t.Fatal("optional classification drift lost valid base parent correlation")
	}
	assertDenied(t, s.EvaluateStored(context.Background(), original, nil))
	decision = s.EvaluateStored(context.Background(), original, grant)
	if !decision.Allowed() || decision.Outcome() != AllowedByParent {
		t.Fatal("optional edition classification changed explicit parent authority")
	}
}

func TestInactiveAdultCannotPublishLegacyFallbackAfterBlockingPolicyCrossesSessionExpiry(t *testing.T) {
	s, f, _ := newVerifiedFixture(t, false)
	f.profile.IsKids = false
	f.policy.Enabled = false
	f.session.ExpiresAt = f.clock.Now().Add(time.Second)
	calls := 0
	s.deps.Policies = policyFunc(func(ctx context.Context, profile string) (Policy, error) {
		calls++
		if calls == 2 {
			f.clock.advance(time.Second)
		}
		return f.CurrentPolicy(ctx, profile)
	})
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	if intent != nil || decision.IsNotApplicable() || decision.Allowed() {
		t.Fatal("expired inactive adult escaped to legacy access")
	}
}
