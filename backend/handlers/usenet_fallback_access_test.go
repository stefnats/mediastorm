package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/importer"
	"novastream/models"
	"novastream/services/sessions"
)

// This boundary uses the actual in-memory sessions service and its nonrenewing
// snapshot, a controlled fresh account reader and a counted fallback-service
// port. SQL role reads, Main's cookie/origin wiring and actual SAB HTTP are
// separately reviewed/tested; they are not simulated as completed here.
type fallbackAccessAccounts struct {
	mu    sync.Mutex
	value *models.Account
	err   error
	reads atomic.Int32
}

func (a *fallbackAccessAccounts) Get(ctx context.Context, _ string) (*models.Account, error) {
	a.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil || a.value == nil {
		return nil, a.err
	}
	copy := *a.value
	return &copy, nil
}

func (a *fallbackAccessAccounts) change(action func(*models.Account)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	action(a.value)
}

type fallbackAccessSessions struct {
	real   *sessions.Service
	reads  atomic.Int32
	err    error
	modify func(*models.Session)
}

func (s *fallbackAccessSessions) SnapshotForAuthorization(ctx context.Context, token string) (*models.Session, error) {
	s.reads.Add(1)
	if s.err != nil {
		return nil, s.err
	}
	value, err := s.real.SnapshotForAuthorization(ctx, token)
	if value != nil && s.modify != nil {
		s.modify(value)
	}
	return value, err
}

type fallbackAccessService struct {
	gets      atomic.Int32
	refreshes atomic.Int32
	perform   func(context.Context) error
}

func (s *fallbackAccessService) GetSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	s.gets.Add(1)
	if s.perform != nil {
		if err := s.perform(ctx); err != nil {
			return importer.SABFallbackStatus{}, err
		}
	}
	return importer.SABFallbackStatus{QueueID: id, State: "submitted"}, nil
}

func (s *fallbackAccessService) RefreshSABFallbackStatus(ctx context.Context, id int64) (importer.SABFallbackStatus, error) {
	s.refreshes.Add(1)
	if s.perform != nil {
		if err := s.perform(ctx); err != nil {
			return importer.SABFallbackStatus{}, err
		}
	}
	return importer.SABFallbackStatus{QueueID: id, State: "submitted"}, nil
}

type fallbackAccessFixture struct {
	claim    models.Session
	sessions *fallbackAccessSessions
	accounts *fallbackAccessAccounts
	service  *fallbackAccessService
}

func newFallbackAccessFixture(t *testing.T) *fallbackAccessFixture {
	t.Helper()
	svc, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := svc.Create("current-master", true, "synthetic-test", "loopback")
	if err != nil {
		t.Fatal(err)
	}
	return &fallbackAccessFixture{claim: claim, sessions: &fallbackAccessSessions{real: svc},
		accounts: &fallbackAccessAccounts{value: &models.Account{ID: claim.AccountID, IsMaster: true}},
		service:  &fallbackAccessService{}}
}

func (f *fallbackAccessFixture) call(method, id string, withClaim bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/admin/api/usenet/fallback/1", nil)
	r = mux.SetURLVars(r, map[string]string{"queueID": id})
	if withClaim {
		ctx := context.WithValue(r.Context(), adminSessionContextKey{}, &f.claim)
		ctx = context.WithValue(ctx, auth.ContextKeySession, f.claim)
		ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.claim.AccountID)
		r = r.WithContext(ctx)
	}
	h := NewUsenetFallbackHandler(f.service, f.sessions, f.accounts)
	w := httptest.NewRecorder()
	if method == http.MethodPost {
		h.Refresh(w, r)
	} else {
		h.Get(w, r)
	}
	return w
}

func assertFallbackAccessDenied(t *testing.T, f *fallbackAccessFixture, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code < 400 || w.Code >= 500 || f.service.gets.Load()+f.service.refreshes.Load() != 0 {
		t.Fatalf("denial status=%d get=%d refresh=%d", w.Code, f.service.gets.Load(), f.service.refreshes.Load())
	}
}

func TestUsenetFallbackAccessDenials(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *fallbackAccessFixture)
	}{
		{"revoked", func(t *testing.T, f *fallbackAccessFixture) {
			t.Helper()
			if err := f.sessions.real.Revoke(f.claim.Token); err != nil {
				t.Fatal(err)
			}
		}},
		{"unknown-token", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.Token = "unregistered-synthetic-token" }},
		{"claim-account-mismatch", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.AccountID = "different-account" }},
		{"current-role-revoked", func(_ *testing.T, f *fallbackAccessFixture) { f.accounts.value.IsMaster = false }},
		{"current-account-missing", func(_ *testing.T, f *fallbackAccessFixture) { f.accounts.value = nil }},
		{"current-account-mismatch", func(_ *testing.T, f *fallbackAccessFixture) { f.accounts.value.ID = "different-account" }},
		{"current-account-expired", func(_ *testing.T, f *fallbackAccessFixture) {
			past := time.Now().Add(-time.Hour)
			f.accounts.value.ExpiresAt = &past
		}},
		{"claim-expired", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.ExpiresAt = time.Now().Add(-time.Hour) }},
		{"claim-resource", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.ScopeResource = "synthetic-resource" }},
		{"stored-resource", func(_ *testing.T, f *fallbackAccessFixture) {
			f.sessions.modify = func(s *models.Session) { s.ScopeResource = "synthetic-resource" }
		}},
		{"stored-expired", func(_ *testing.T, f *fallbackAccessFixture) {
			f.sessions.modify = func(s *models.Session) { s.ExpiresAt = time.Now().Add(-time.Hour) }
		}},
		{"stored-account-mismatch", func(_ *testing.T, f *fallbackAccessFixture) {
			f.sessions.modify = func(s *models.Session) { s.AccountID = "different-account" }
		}},
		{"stored-token-mismatch", func(_ *testing.T, f *fallbackAccessFixture) {
			f.sessions.modify = func(s *models.Session) { s.Token = "different-token" }
		}},
	}
	for _, scope := range []string{models.SessionScopeStream, models.SessionScopeWatchParty, models.SessionScopeJellyfin, "unknown"} {
		cases = append(cases, struct {
			name   string
			change func(*testing.T, *fallbackAccessFixture)
		}{
			"stored-scope-" + scope, func(_ *testing.T, f *fallbackAccessFixture) {
				f.sessions.modify = func(s *models.Session) { s.Scope = scope }
			},
		})
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				f := newFallbackAccessFixture(t)
				c.change(t, f)
				assertFallbackAccessDenied(t, f, f.call(method, "1", true))
			}
		})
	}
	t.Run("no-session", func(t *testing.T) {
		f := newFallbackAccessFixture(t)
		assertFallbackAccessDenied(t, f, f.call(http.MethodGet, "1", false))
	})
}

func TestUsenetFallbackAccessFreshRoleAndNonrenewingSnapshot(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		t.Run(method, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			f.claim.IsMaster = false // Current authoritative role wins over the stale cached flag.
			before, err := f.sessions.real.SnapshotForAuthorization(context.Background(), f.claim.Token)
			if err != nil {
				t.Fatal(err)
			}
			w := f.call(method, "1", true)
			if w.Code != http.StatusOK {
				t.Fatalf("current master status=%d", w.Code)
			}
			if f.service.gets.Load()+f.service.refreshes.Load() != 1 || (method == http.MethodGet && f.service.refreshes.Load() != 0) {
				t.Fatal("status request selected the wrong fallback operation")
			}
			after, err := f.sessions.real.SnapshotForAuthorization(context.Background(), f.claim.Token)
			if err != nil || !after.ExpiresAt.Equal(before.ExpiresAt) || !after.CreatedAt.Equal(before.CreatedAt) {
				t.Fatal("authorization renewed or replaced the session")
			}
			if f.sessions.reads.Load() < 2 || f.accounts.reads.Load() < 2 {
				t.Fatal("missing pre/post authoritative checks")
			}
		})
	}
}

func TestUsenetFallbackAccessPostWorkRevocation(t *testing.T) {
	for _, change := range []string{"demoted", "expired-account", "revoked-session"} {
		t.Run(change, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			f.service.perform = func(context.Context) error {
				switch change {
				case "demoted":
					f.accounts.change(func(a *models.Account) { a.IsMaster = false })
				case "expired-account":
					f.accounts.change(func(a *models.Account) { past := time.Now().Add(-time.Hour); a.ExpiresAt = &past })
				case "revoked-session":
					return f.sessions.real.Revoke(f.claim.Token)
				}
				return nil
			}
			w := f.call(http.MethodPost, "1", true)
			if w.Code < 400 || w.Code >= 500 || f.service.refreshes.Load() != 1 {
				t.Fatalf("post-work revocation status=%d", w.Code)
			}
			if strings.Contains(w.Body.String(), "queueId") || strings.Contains(w.Body.String(), "download_completed") {
				t.Fatal("revoked reply disclosed fallback status")
			}
		})
	}
}

func TestUsenetFallbackAccessCanonicalQueueIDAndSafeErrors(t *testing.T) {
	for _, id := range []string{"0", "-1", "01", "1.0", " 1", "1 ", "9223372036854775808", "unknown"} {
		t.Run("id-"+id, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			assertFallbackAccessDenied(t, f, f.call(http.MethodPost, id, true))
		})
	}
	t.Run("private-service-error", func(t *testing.T) {
		f := newFallbackAccessFixture(t)
		f.service.perform = func(context.Context) error {
			return errors.New("SYNTHETIC_PRIVATE_CANARY /synthetic/private/file https://private.example/api?key=fixture")
		}
		w := f.call(http.MethodPost, "1", true)
		if w.Code < 400 || strings.Contains(w.Body.String(), "SYNTHETIC_PRIVATE_CANARY") || strings.Contains(w.Body.String(), "private.example") || strings.Contains(w.Body.String(), "/synthetic/private") {
			t.Fatal("service error was exposed or reported as successful")
		}
	})
}

func TestUsenetFallbackAccessOriginalDeadline(t *testing.T) {
	for _, name := range []string{"claim", "account"} {
		t.Run(name, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			bound := time.Now().Add(2 * time.Second)
			if name == "claim" {
				f.claim.ExpiresAt = bound
			} else {
				f.accounts.value.ExpiresAt = &bound
			}
			f.service.perform = func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				if !ok || deadline.After(bound) {
					t.Error("external status work escaped its original authorization deadline")
				}
				return nil
			}
			w := f.call(http.MethodPost, "1", true)
			if w.Code != http.StatusOK || f.service.refreshes.Load() != 1 {
				t.Fatalf("bounded positive request status=%d", w.Code)
			}
		})
	}
}

func TestUsenetFallbackAccessReaderFailure(t *testing.T) {
	for _, name := range []string{"sessions", "accounts"} {
		t.Run(name, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			if name == "sessions" {
				f.sessions.err = errors.New("SYNTHETIC_PRIVATE_READER_CANARY")
			} else {
				f.accounts.err = errors.New("SYNTHETIC_PRIVATE_READER_CANARY")
			}
			w := f.call(http.MethodPost, "1", true)
			assertFallbackAccessDenied(t, f, w)
			if strings.Contains(w.Body.String(), "SYNTHETIC_PRIVATE_READER_CANARY") {
				t.Fatal("authorization reader error was disclosed")
			}
		})
	}
}
