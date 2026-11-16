package kidsvod

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/internal/parentauth"
	"novastream/models"
)

func TestParentGuardRejectsWrongPasswordChildPINAndBodyProof(t *testing.T) {
	for _, mode := range []string{"wrong_password", "child_pin_header", "profile_unlock", "json_confirmation", "json_password", "body_master"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			r := f.request(false)
			switch mode {
			case "wrong_password":
				r.Header.Set(parentauth.PasswordHeader, "incorrect synthetic confirmation")
			case "child_pin_header":
				r.Header.Set(parentauth.PasswordHeader, "1234")
			case "profile_unlock":
				r.Header.Set("X-Profile-Unlock", "synthetic-child-pin-unlock")
			default:
				body := map[string]string{"json_confirmation": `{"parentConfirmed":true}`, "json_password": `{"parentPassword":"synthetic parent confirmation"}`, "body_master": `{"isMaster":true,"parentConfirmed":true}`}[mode]
				next := httptest.NewRequest(http.MethodPost, "/internal-fixture", strings.NewReader(body)).WithContext(r.Context())
				next.Header = r.Header.Clone()
				next.Header.Set("Content-Type", "application/json")
				r = next
			}
			reply := httptest.NewRecorder()
			grant, decision := s.IssueParentException(reply, r, "child", f.ref, time.Hour)
			if grant != nil || reply.Code != 403 || decision.Code() != CodeParentApprovalRequired || len(s.grants) != 0 {
				t.Fatal("non-parent input created an exception")
			}
			if f.providerCalls.Load() != 0 {
				t.Fatal("failed parent proof opened provider work")
			}
		})
	}
}

func TestParentGuardRejectsScopedAndSpoofedMasterSessions(t *testing.T) {
	for _, scope := range []string{models.SessionScopeStream, models.SessionScopeWatchParty, models.SessionScopeJellyfin, "invitation"} {
		for _, master := range []bool{false, true} {
			name := scope
			if master {
				name += "_master"
			}
			t.Run(name, func(t *testing.T) {
				s, f := newFixture(t)
				f.session.Scope, f.session.IsMaster = scope, master
				grant, decision := s.IssueParentException(httptest.NewRecorder(), f.request(true), "child", f.ref, time.Hour)
				if grant != nil || decision.Outcome() != UnknownDenied || len(s.grants) != 0 || f.parentCalls.Load() != 0 || f.cacheCalls.Load() != 0 {
					t.Fatal("scoped session reached authority or parent confirmation")
				}
			})
		}
	}
	for _, mode := range []string{"master_context_only", "master_context_and_session", "foreign_account_context", "forged_session_token"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			r := f.request(true)
			ctx := r.Context()
			switch mode {
			case "master_context_only":
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
			case "master_context_and_session":
				session := f.session
				session.IsMaster = true
				ctx = context.WithValue(ctx, auth.ContextKeySession, session)
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
			case "foreign_account_context":
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "another-account")
			case "forged_session_token":
				session := f.session
				session.Token = "unregistered-synthetic-token"
				ctx = context.WithValue(ctx, auth.ContextKeySession, session)
			}
			grant, decision := s.IssueParentException(httptest.NewRecorder(), r.WithContext(ctx), "child", f.ref, time.Hour)
			if grant != nil || decision.Outcome() != UnknownDenied || len(s.grants) != 0 || f.parentCalls.Load() != 0 {
				t.Fatal("caller claims replaced authoritative session state")
			}
		})
	}
}

func TestParentGuardDoesNotReuseSuccessfulConfirmationAcrossMutations(t *testing.T) {
	s, f := newFixture(t)
	intent, grant := issue(t, s, f)
	if f.parentCalls.Load() != 1 {
		t.Fatal("initial parent confirmation missing")
	}
	reply := httptest.NewRecorder()
	next, decision := s.IssueParentException(reply, f.request(false), "child", f.ref, time.Hour)
	if next != nil || reply.Code != 403 || decision.Code() != CodeParentApprovalRequired || len(s.grants) != 1 {
		t.Fatal("new request reused an earlier parent confirmation")
	}
	reply = httptest.NewRecorder()
	decision = s.RevokeParentException(reply, f.request(false), grant)
	if reply.Code != 403 || decision.Code() != CodeParentApprovalRequired || !s.EvaluateStored(context.Background(), intent, grant).Allowed() {
		t.Fatal("revocation accepted session/PIN without fresh parent proof")
	}
	if f.parentCalls.Load() != 1 || f.providerCalls.Load() != 0 {
		t.Fatal("missing proof unexpectedly authenticated or opened a provider")
	}
	decision = s.RevokeParentException(httptest.NewRecorder(), f.request(true), grant)
	if decision.Outcome() != Denied || decision.Code() != CodeAccessDenied || s.EvaluateStored(context.Background(), intent, grant).Allowed() || f.parentCalls.Load() != 2 {
		t.Fatal("fresh explicit revocation did not take effect")
	}
}

func TestParentGuardRevokeRechecksOwnerAndSessionAfterConfirmation(t *testing.T) {
	for _, mode := range []string{"ownership_changed", "session_revoked", "session_became_scoped"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			intent, grant := issue(t, s, f)
			request := f.request(true)
			originalProfile, originalSession := f.profile, f.session
			f.parentHook = func() {
				f.mu.Lock()
				defer f.mu.Unlock()
				switch mode {
				case "ownership_changed":
					f.profile.AccountID, f.profile.Revision = "new-owner", "moved-profile"
				case "session_revoked":
					f.session.Token = "replacement-session"
				case "session_became_scoped":
					f.session.Scope = models.SessionScopeStream
				}
			}
			decision := s.RevokeParentException(httptest.NewRecorder(), request, grant)
			s.mu.Lock()
			entry := s.grants[grant.id]
			revoked := entry == nil || entry.revoked
			s.mu.Unlock()
			// Restore only the synthetic reader state to distinguish a rejected
			// mutation from authority that was already invalid while it changed.
			f.mu.Lock()
			f.profile, f.session, f.parentHook = originalProfile, originalSession, nil
			f.mu.Unlock()
			if decision.Outcome() != UnknownDenied || revoked || !s.EvaluateStored(context.Background(), intent, grant).Allowed() {
				t.Fatal("delayed confirmation mutated an exception after losing current ownership/session")
			}
			if f.providerCalls.Load() != 0 {
				t.Fatal("revocation performed external work")
			}
		})
	}
}
