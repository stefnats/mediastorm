package kidsvod

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/internal/parentauth"
	"novastream/models"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*fakeTimer]struct {
		at  time.Time
		run func()
	}
}
type fakeTimer struct {
	clock   *fakeClock
	stopped bool
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) AfterFunc(d time.Duration, run func()) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{clock: c}
	c.timers[timer] = struct {
		at  time.Time
		run func()
	}{c.now.Add(d), run}
	return timer
}
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	was := !t.stopped
	t.stopped = true
	delete(t.clock.timers, t)
	return was
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []func()
	for timer, event := range c.timers {
		if !event.at.After(c.now) {
			timer.stopped = true
			delete(c.timers, timer)
			due = append(due, event.run)
		}
	}
	c.mu.Unlock()
	for _, run := range due {
		run()
	}
}

type fixture struct {
	mu            sync.RWMutex
	clock         *fakeClock
	profile       Profile
	policy        Policy
	session       models.Session
	binding       Binding
	ref           ResourceRef
	cacheMiss     bool
	authorityHook func()
	parentHook    func()
	parentCalls   atomic.Int32
	cacheCalls    atomic.Int32
	providerCalls atomic.Int32
}

func age(value int) *int { return &value }
func newFixture(t *testing.T) (*Service, *fixture) {
	t.Helper()
	now := time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC)
	clock := &fakeClock{now: now, timers: map[*fakeTimer]struct {
		at  time.Time
		run func()
	}{}}
	ref, err := NewResourceRef(Prequeue, "owned-preparation", 0)
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{clock: clock, ref: ref, profile: Profile{ID: "child", AccountID: "household", Revision: "profile-v1", IsKids: true}, policy: Policy{Enabled: true, Country: "DE", MaxMovieAge: age(6), MaxEpisodeAge: age(6), Revision: "policy-v1"}, session: models.Session{Token: "synthetic-session", AccountID: "household", ClientID: "device", CreatedAt: now.Add(-time.Hour), ExpiresAt: now.Add(8 * time.Hour)}, binding: Binding{Resource: ref, AccountID: "household", ProfileID: "child", CatalogID: "tmdb:movie:41", MediaType: "movie", SourceVersion: "file_v1_" + strings.Repeat("a", 64), Revision: "source-v1", ObservedAt: now.Add(-time.Minute), ExpiresAt: now.Add(6 * time.Hour)}}
	s, err := New(Dependencies{Profiles: f, Policies: f, Sessions: f, Resources: f, Parent: parentauth.New(f), Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	return s, f
}
func (f *fixture) CurrentProfile(_ context.Context, id string) (Profile, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if id != f.profile.ID {
		return Profile{}, errors.New("unknown profile")
	}
	return f.profile, nil
}
func (f *fixture) CurrentPolicy(_ context.Context, _ string) (Policy, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p := f.policy
	if p.MaxMovieAge != nil {
		p.MaxMovieAge = age(*p.MaxMovieAge)
	}
	if p.MaxEpisodeAge != nil {
		p.MaxEpisodeAge = age(*p.MaxEpisodeAge)
	}
	return p, nil
}
func (f *fixture) CurrentSession(_ context.Context, token string) (models.Session, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if token != f.session.Token {
		return models.Session{}, errors.New("revoked session")
	}
	return f.session, nil
}
func (f *fixture) ResolveCacheOnly(_ context.Context, subject Subject, ref ResourceRef) (Binding, error) {
	f.cacheCalls.Add(1)
	f.mu.RLock()
	b, miss, hook := f.binding, f.cacheMiss, f.authorityHook
	b.CertificationEvidence = models.CopyCertificationEvidence(b.CertificationEvidence)
	if b.Episode != nil {
		ep := *b.Episode
		if ep.Numbering != nil {
			n := *ep.Numbering
			ep.Numbering = &n
		}
		b.Episode = &ep
	}
	f.mu.RUnlock()
	if hook != nil {
		hook()
	}
	if miss || ref != f.ref || subject.ProfileID() != b.ProfileID {
		return Binding{}, errors.New("no current cached binding; external work required")
	}
	return b, nil
}
func (f *fixture) OpenProvider() error {
	f.providerCalls.Add(1)
	return errors.New("external work forbidden")
}
func (f *fixture) Get(id string) (models.Account, bool) {
	return models.Account{ID: "household", Username: "parent"}, id == "household"
}
func (f *fixture) Authenticate(username, password string) (models.Account, error) {
	f.parentCalls.Add(1)
	if username != "parent" || password != "synthetic parent confirmation" {
		return models.Account{}, errors.New("invalid fixture confirmation")
	}
	f.mu.RLock()
	hook := f.parentHook
	f.mu.RUnlock()
	if hook != nil {
		hook()
	}
	return models.Account{ID: "household"}, nil
}
func (f *fixture) request(password bool) *http.Request {
	f.mu.RLock()
	session := f.session
	f.mu.RUnlock()
	r := httptest.NewRequest("POST", "/internal-fixture", nil)
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, session.IsMaster)
	r = r.WithContext(ctx)
	r.Header.Set("X-Client-ID", "device")
	if password {
		r.Header.Set(parentauth.PasswordHeader, "synthetic parent confirmation")
	}
	return r
}
func issue(t *testing.T, s *Service, f *fixture) (*Intent, *Grant) {
	t.Helper()
	intent, _ := s.ResolveRequest(f.request(false), "child", f.ref)
	grant, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Hour)
	if intent == nil || grant == nil || !decision.Allowed() {
		t.Fatalf("parent exception failed: %s", decision.Code())
	}
	return intent, grant
}
func assertDenied(t *testing.T, decision Decision) {
	t.Helper()
	if decision.Allowed() || decision.IsNotApplicable() {
		t.Fatalf("unexpected permission: outcome=%d code=%s", decision.Outcome(), decision.Code())
	}
}

func TestZeroImportedAndDisabledDecisionsCannotInventLegacyFallback(t *testing.T) {
	s, f := newFixture(t)
	var zero Decision
	assertDenied(t, zero)
	if zero.Outcome() != UnknownDenied {
		t.Fatal("zero decision not fail-closed")
	}
	for _, data := range []string{`null`, `{"outcome":1,"allowed":true}`, `{"outcome":2,"code":""}`} {
		if err := json.Unmarshal([]byte(data), &zero); !errors.Is(err, ErrOpaqueCapability) {
			t.Fatal("decision imported")
		}
		assertDenied(t, zero)
	}
	f.mu.Lock()
	f.policy.Enabled = false
	f.mu.Unlock()
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	if intent != nil || decision.IsNotApplicable() || decision.Allowed() || f.cacheCalls.Load() != 0 {
		t.Fatal("disabled child policy bypassed regional protection")
	}
	if err := json.Unmarshal([]byte(`{"outcome":1}`), &decision); err == nil {
		t.Fatal("decision imported")
	}
	assertDenied(t, decision)
	f.mu.Lock()
	f.policy = Policy{}
	f.mu.Unlock()
	_, decision = s.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, decision)
	_, decision = s.ResolveRequest(f.request(false), "", f.ref)
	if decision.Code() != CodeProfileRequired {
		t.Fatal("missing profile fell back to adult")
	}
}

func TestRawGermanMetadataAndUSScalarsNeverAutomaticallyAllow(t *testing.T) {
	for _, scope := range []string{"movie_release", "series", ""} {
		t.Run(scope, func(t *testing.T) {
			s, f := newFixture(t)
			expiry := f.clock.Now().Add(time.Hour)
			f.binding.LegacyCertification = "G"
			f.binding.CertificationEvidence = []models.CertificationEvidence{{Country: "DE", Value: "0", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:41", Scope: scope, RetrievedAt: f.clock.Now(), ExpiresAt: &expiry}}
			intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
			if intent == nil {
				t.Fatal("valid resource lost")
			}
			assertDenied(t, decision)
			assertDenied(t, s.EvaluateStored(context.Background(), intent, nil))
			if f.providerCalls.Load() != 0 {
				t.Fatal("classification requested provider work")
			}
		})
	}
}

func TestStrictContextAndExactResourceIdentityRemainUnknown(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fixture)
	}{
		{"unconfigured cap", func(f *fixture) { f.policy.MaxMovieAge = nil }},
		{"unsupported country", func(f *fixture) { f.policy.Country = "US" }},
		{"foreign profile", func(f *fixture) { f.profile.AccountID = "other" }},
		{"untrusted catalogue name", func(f *fixture) { f.binding.CatalogID = "A Movie Name" }},
		{"wrong catalogue namespace", func(f *fixture) { f.binding.CatalogID = "tmdb:series:41" }},
		{"release hash is not file proof", func(f *fixture) { f.binding.SourceVersion = "candidate_v1_" + strings.Repeat("a", 64) }},
		{"wrong resource echo", func(f *fixture) { f.binding.Resource.id = "other-preparation" }},
		{"unknown current source rights", func(f *fixture) { f.cacheMiss = true }},
		{"expired source", func(f *fixture) { f.binding.ExpiresAt = f.clock.Now() }},
		{"future observation", func(f *fixture) { f.binding.ObservedAt = f.clock.Now().Add(time.Minute) }},
		{"scoped session", func(f *fixture) { f.session.Scope = models.SessionScopeStream }},
		{"unknown source revision", func(f *fixture) { f.binding.Revision = "" }},
		{"series without leaf", func(f *fixture) { f.binding.MediaType = "series"; f.binding.CatalogID = "tvdb:series:74852" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, f := newFixture(t)
			test.change(f)
			grant, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Hour)
			if grant != nil {
				t.Fatal("invalid context gained parent exception")
			}
			assertDenied(t, decision)
			if f.parentCalls.Load() != 0 || f.providerCalls.Load() != 0 {
				t.Fatal("unresolved context requested confirmation or provider work")
			}
		})
	}
	for _, value := range []string{"/private/file.mkv", "https://example.invalid/video", "file\\name"} {
		if _, err := NewResourceRef(Local, value, 0); err == nil {
			t.Fatal("raw path accepted as opaque reference")
		}
	}
}

func TestParentGrantRequiresFreshProofAndIsNotImportableOrPortable(t *testing.T) {
	s, f := newFixture(t)
	rec := httptest.NewRecorder()
	if grant, decision := s.IssueParentException(rec, f.request(false), "child", f.ref, time.Hour); grant != nil || decision.Code() != CodeParentApprovalRequired || rec.Code != 403 {
		t.Fatal("ordinary account session acted as parent")
	}
	if len(s.grants) != 0 {
		t.Fatal("failed parent check changed approvals")
	}
	intent, grant := issue(t, s, f)
	if !s.EvaluateStored(context.Background(), intent, grant).Allowed() {
		t.Fatal("current exact parent grant rejected")
	}
	for _, raw := range []string{`null`, `{"id":"forged","expires":"2099-01-01"}`, `{"seal":true}`} {
		var imported Grant
		if json.Unmarshal([]byte(raw), &imported) == nil {
			t.Fatal("grant imported")
		}
		assertDenied(t, s.EvaluateStored(context.Background(), intent, &imported))
	}
	var importedIntent Intent
	if json.Unmarshal([]byte(`{"profile":"child"}`), &importedIntent) == nil {
		t.Fatal("intent imported")
	}
	assertDenied(t, s.EvaluateStored(context.Background(), &importedIntent, grant))
	other, err := New(s.deps)
	if err != nil {
		t.Fatal(err)
	}
	assertDenied(t, other.EvaluateStored(context.Background(), intent, grant))
	own, _ := other.ResolveRequest(f.request(false), "child", f.ref)
	assertDenied(t, other.EvaluateStored(context.Background(), own, grant))
	data, _ := json.Marshal(grant)
	if string(data) != "null" {
		t.Fatal("runtime authority exported")
	}
	if !s.EvaluateStored(context.Background(), intent, grant).Allowed() {
		t.Fatal("reading current authority changed it")
	}
}

func TestDeferredConfirmationCannotApproveChangedOrCancelledContext(t *testing.T) {
	for _, change := range []string{"source", "policy", "profile invalidation", "cancel"} {
		t.Run(change, func(t *testing.T) {
			s, f := newFixture(t)
			r := f.request(true)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			f.parentHook = func() {
				switch change {
				case "source":
					f.mu.Lock()
					f.binding.SourceVersion = "file_v1_" + strings.Repeat("b", 64)
					f.mu.Unlock()
				case "policy":
					f.mu.Lock()
					f.policy.Revision = "policy-v2"
					f.mu.Unlock()
				case "profile invalidation":
					s.InvalidateProfile("child")
				case "cancel":
					cancel()
				}
			}
			grant, decision := s.IssueParentException(httptest.NewRecorder(), r, "child", f.ref, time.Hour)
			if grant != nil || decision.Code() != CodeContextChanged || len(s.grants) != 0 {
				t.Fatal("late confirmation gained changed authority")
			}
		})
	}
	// A late cache-only lookup may not publish a pre-invalidation intent.
	s, f := newFixture(t)
	f.authorityHook = func() { s.InvalidateProfile("child") }
	intent, decision := s.ResolveRequest(f.request(false), "child", f.ref)
	if intent != nil || decision.Code() != CodeContextChanged {
		t.Fatal("late old resource acquired new epoch")
	}
}

type policyFunc func(context.Context, string) (Policy, error)

func (f policyFunc) CurrentPolicy(ctx context.Context, profile string) (Policy, error) {
	return f(ctx, profile)
}

func TestDisabledFallbackCannotPublishAcrossInvalidationOrCancellation(t *testing.T) {
	for _, mode := range []string{"invalidate", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			r := f.request(false)
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			s.deps.Policies = policyFunc(func(context.Context, string) (Policy, error) {
				policy := Policy{Enabled: false, Revision: "explicitly-disabled"}
				if mode == "invalidate" {
					s.InvalidateProfile("child")
				} else {
					cancel()
				}
				return policy, nil
			})
			intent, decision := s.ResolveRequest(r, "child", f.ref)
			if intent != nil || decision.IsNotApplicable() || decision.Code() != CodeContextChanged {
				t.Fatal("late disabled policy escaped strict unknown denial")
			}
		})
	}
}
