package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/internal/importer"
	"novastream/models"
)

// These tests exercise the real nonrenewing session snapshots with controlled
// account and list ports. SQL, cookie/Origin wiring and SAB HTTP have separate
// fixtures; a counted port is not evidence of executing those transports.
type fallbackListAccessService struct {
	*fallbackAccessService
	lists   atomic.Int32
	perform func(context.Context, string, int) (importer.SABFallbackPage, error)
}

func (s *fallbackListAccessService) ListSABFallbackStatuses(ctx context.Context, cursor string, limit int) (importer.SABFallbackPage, error) {
	s.lists.Add(1)
	if s.perform != nil {
		return s.perform(ctx, cursor, limit)
	}
	return importer.SABFallbackPage{Items: []importer.SABFallbackStatus{{QueueID: 17, State: "submitted"}}}, nil
}

func fallbackListAccessRequest(f *fallbackAccessFixture, path string, ctx context.Context, claimed bool) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	if claimed {
		ctx = context.WithValue(r.Context(), adminSessionContextKey{}, &f.claim)
		ctx = context.WithValue(ctx, auth.ContextKeySession, f.claim)
		ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.claim.AccountID)
		r = r.WithContext(ctx)
	}
	return r
}

func TestUsenetFallbackListAccessDenials(t *testing.T) {
	cases := []struct {
		name   string
		change func(*testing.T, *fallbackAccessFixture)
	}{
		{"revoked-session", func(t *testing.T, f *fallbackAccessFixture) {
			if err := f.sessions.real.Revoke(f.claim.Token); err != nil {
				t.Fatal(err)
			}
		}},
		{"unknown-session", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.Token = "unknown-synthetic-session" }},
		{"changed-client", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.ClientID = "another-client" }},
		{"claim-account", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.AccountID = "another-account" }},
		{"claim-expired", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.ExpiresAt = time.Now().Add(-time.Hour) }},
		{"claim-resource", func(_ *testing.T, f *fallbackAccessFixture) { f.claim.ScopeResource = "another-resource" }},
		{"current-demoted", func(_ *testing.T, f *fallbackAccessFixture) { f.accounts.value.IsMaster = false }},
		{"account-missing", func(_ *testing.T, f *fallbackAccessFixture) { f.accounts.value = nil }},
		{"account-reader-error", func(_ *testing.T, f *fallbackAccessFixture) {
			f.accounts.err = errors.New("synthetic-private-account-error")
		}},
		{"account-mismatch", func(_ *testing.T, f *fallbackAccessFixture) { f.accounts.value.ID = "another-account" }},
		{"account-expired", func(_ *testing.T, f *fallbackAccessFixture) {
			x := time.Now().Add(-time.Hour)
			f.accounts.value.ExpiresAt = &x
		}},
		{"stored-resource", func(_ *testing.T, f *fallbackAccessFixture) {
			f.sessions.modify = func(s *models.Session) { s.ScopeResource = "resource" }
		}},
		{"stored-expired", func(_ *testing.T, f *fallbackAccessFixture) {
			f.sessions.modify = func(s *models.Session) { s.ExpiresAt = time.Now().Add(-time.Hour) }
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
			f := newFallbackAccessFixture(t)
			c.change(t, f)
			s := &fallbackListAccessService{fallbackAccessService: f.service}
			w := httptest.NewRecorder()
			NewUsenetFallbackHandler(s, f.sessions, f.accounts).List(w, fallbackListAccessRequest(f, "/admin/api/usenet/fallback", context.Background(), true))
			if w.Code != http.StatusForbidden || s.lists.Load() != 0 || s.gets.Load()+s.refreshes.Load() != 0 {
				t.Fatalf("denied list: status=%d list=%d", w.Code, s.lists.Load())
			}
			if strings.Contains(w.Body.String(), "synthetic-private") {
				t.Fatal("private reader error exposed")
			}
		})
	}
	for _, claimed := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-claim", true: "canceled-before-entry"}[claimed], func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			s := &fallbackListAccessService{fallbackAccessService: f.service}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if claimed {
				cancel()
			}
			w := httptest.NewRecorder()
			NewUsenetFallbackHandler(s, f.sessions, f.accounts).List(w, fallbackListAccessRequest(f, "/admin/api/usenet/fallback", ctx, claimed))
			if w.Code != http.StatusForbidden || s.lists.Load() != 0 || f.accounts.reads.Load() != 0 {
				t.Fatal("unauthorized entry reached account or list work")
			}
		})
	}
}

func TestUsenetFallbackListAccessMalformedBeforeList(t *testing.T) {
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=-1", "?limit=01", "?limit=1&limit=2", "?limit=", "?unknown=1", "?cursor=", "?cursor=x&cursor=y", "?limit=%zz", "?cursor=" + strings.Repeat("x", 513)} {
		t.Run(query[:min(len(query), 45)], func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			s := &fallbackListAccessService{fallbackAccessService: f.service}
			w := httptest.NewRecorder()
			NewUsenetFallbackHandler(s, f.sessions, f.accounts).List(w, fallbackListAccessRequest(f, "/admin/api/usenet/fallback"+query, context.Background(), true))
			if w.Code != http.StatusBadRequest || s.lists.Load() != 0 {
				t.Fatalf("malformed request status=%d list=%d", w.Code, s.lists.Load())
			}
		})
	}
}

func TestUsenetFallbackListAccessFreshRoleAndNonrenewal(t *testing.T) {
	f := newFallbackAccessFixture(t)
	f.claim.IsMaster = false
	s := &fallbackListAccessService{fallbackAccessService: f.service}
	s.perform = func(ctx context.Context, cursor string, limit int) (importer.SABFallbackPage, error) {
		if cursor != "opaque-test-cursor" || limit != 7 {
			t.Fatal("request selection changed")
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second {
			t.Fatal("list lacks bounded authority context")
		}
		return importer.SABFallbackPage{Items: []importer.SABFallbackStatus{{QueueID: 17, State: "submitted"}}}, nil
	}
	before, err := f.sessions.real.SnapshotForAuthorization(context.Background(), f.claim.Token)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	NewUsenetFallbackHandler(s, f.sessions, f.accounts).List(w, fallbackListAccessRequest(f, "/admin/api/usenet/fallback?limit=7&cursor=opaque-test-cursor", context.Background(), true))
	if w.Code != 200 || s.lists.Load() != 1 || s.gets.Load()+s.refreshes.Load() != 0 || !strings.Contains(w.Body.String(), `"queueId":17`) {
		t.Fatalf("valid current role: status=%d body=%s", w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "private, no-store" || f.accounts.reads.Load() < 2 || f.sessions.reads.Load() < 2 {
		t.Fatal("missing privacy or actual pre/post authority checks")
	}
	after, err := f.sessions.real.SnapshotForAuthorization(context.Background(), f.claim.Token)
	if err != nil || !before.ExpiresAt.Equal(after.ExpiresAt) || !before.CreatedAt.Equal(after.CreatedAt) {
		t.Fatal("list renewed original session")
	}
}

func TestUsenetFallbackListAccessLateRevocation(t *testing.T) {
	for _, name := range []string{"demoted", "revoked", "account-expired", "account-revision", "request-canceled", "claim-deadline", "stored-deadline"} {
		t.Run(name, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if name == "claim-deadline" {
				f.claim.ExpiresAt = time.Now().Add(30 * time.Millisecond)
			}
			if name == "stored-deadline" {
				x := time.Now().Add(30 * time.Millisecond)
				f.sessions.modify = func(s *models.Session) { s.ExpiresAt = x }
			}
			s := &fallbackListAccessService{fallbackAccessService: f.service}
			s.perform = func(work context.Context, _ string, _ int) (importer.SABFallbackPage, error) {
				switch name {
				case "demoted":
					f.accounts.change(func(a *models.Account) { a.IsMaster = false })
				case "revoked":
					if err := f.sessions.real.Revoke(f.claim.Token); err != nil {
						t.Fatal(err)
					}
				case "account-expired":
					f.accounts.change(func(a *models.Account) { x := time.Now().Add(-time.Hour); a.ExpiresAt = &x })
				case "account-revision":
					f.accounts.change(func(a *models.Account) { a.UpdatedAt = time.Now() })
				case "request-canceled":
					cancel()
				case "claim-deadline", "stored-deadline":
					<-work.Done()
				}
				// A late successful port must not publish after its original authority ends.
				return importer.SABFallbackPage{Items: []importer.SABFallbackStatus{{QueueID: 987654321, State: "submitted"}}}, nil
			}
			w := httptest.NewRecorder()
			NewUsenetFallbackHandler(s, f.sessions, f.accounts).List(w, fallbackListAccessRequest(f, "/admin/api/usenet/fallback", ctx, true))
			if w.Code != http.StatusForbidden || s.lists.Load() != 1 || strings.Contains(w.Body.String(), "987654321") {
				t.Fatalf("late publication: status=%d list=%d", w.Code, s.lists.Load())
			}
		})
	}
}

func TestUsenetFallbackListAccessReaderErrorsStayUnavailable(t *testing.T) {
	for _, name := range []string{"missing-list-port", "list-error"} {
		t.Run(name, func(t *testing.T) {
			f := newFallbackAccessFixture(t)
			var service usenetFallbackService = f.service
			if name == "list-error" {
				s := &fallbackListAccessService{fallbackAccessService: f.service}
				s.perform = func(context.Context, string, int) (importer.SABFallbackPage, error) {
					return importer.SABFallbackPage{}, errors.New("synthetic-private-database-path-and-key")
				}
				service = s
			}
			w := httptest.NewRecorder()
			NewUsenetFallbackHandler(service, f.sessions, f.accounts).List(w, fallbackListAccessRequest(f, "/admin/api/usenet/fallback", context.Background(), true))
			if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "synthetic-private") || strings.Contains(w.Body.String(), `"items"`) {
				t.Fatal("unavailable reader became empty success or exposed private error")
			}
		})
	}
}
