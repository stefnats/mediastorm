package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/kidsvod"
	"novastream/services/playback"
	"novastream/services/profileunlock"
)

type rootKidsVODProfiles func(context.Context, string) (*models.User, error)

func (f rootKidsVODProfiles) Get(c context.Context, id string) (*models.User, error) { return f(c, id) }

type rootKidsVODAccounts func(context.Context, string) (*models.Account, error)

func (f rootKidsVODAccounts) Get(c context.Context, id string) (*models.Account, error) {
	return f(c, id)
}

type rootKidsVODSessions func(context.Context, string) (*models.Session, error)

func (f rootKidsVODSessions) Get(c context.Context, id string) (*models.Session, error) {
	return f(c, id)
}

type rootKidsVODPolicy func(context.Context, string) (kidsvod.Policy, error)

func (f rootKidsVODPolicy) CurrentPolicy(c context.Context, id string) (kidsvod.Policy, error) {
	return f(c, id)
}

type rootKidsVODIdentityFixture struct {
	identity *kidsVODIdentity
	profile  *models.User
	account  *models.Account
	session  *models.Session
	now      time.Time
	reads    int
	failure  error
}

func newRootKidsVODIdentityFixture() *rootKidsVODIdentityFixture {
	f := &rootKidsVODIdentityFixture{now: time.Now().UTC().Truncate(time.Second)}
	f.profile = &models.User{ID: "profile-a", AccountID: "account-a", IsKidsProfile: true, PinHash: "synthetic-current-fingerprint"}
	f.account = &models.Account{ID: "account-a"}
	f.session = &models.Session{Token: "synthetic-session", AccountID: "account-a", ExpiresAt: f.now.Add(time.Hour), ClientID: "client-a"}
	f.identity = &kidsVODIdentity{now: func() time.Time { return f.now },
		profiles: rootKidsVODProfiles(func(_ context.Context, id string) (*models.User, error) { f.reads++; return f.profile, f.failure }),
		accounts: rootKidsVODAccounts(func(_ context.Context, id string) (*models.Account, error) { f.reads++; return f.account, f.failure }),
		sessions: rootKidsVODSessions(func(_ context.Context, id string) (*models.Session, error) { f.reads++; return f.session, f.failure })}
	return f
}
func (f *rootKidsVODIdentityFixture) request() *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/synthetic-vod", nil)
	c := context.WithValue(r.Context(), auth.ContextKeyAccountID, "account-a")
	c = context.WithValue(c, auth.ContextKeySession, *f.session)
	c = context.WithValue(c, auth.ContextKeyIsMaster, f.account.IsMaster)
	r = r.WithContext(c)
	r.Header.Set("X-Client-ID", "client-a")
	return r
}
func (f *rootKidsVODIdentityFixture) bridge(t *testing.T) (*kidsVODHTTPBridge, kidsvod.ResourceRef, *int, *int) {
	t.Helper()
	pinCalls, policyCalls := new(int), new(int)
	core, err := kidsvod.New(kidsvod.Dependencies{Profiles: f.identity, Sessions: f.identity, Policies: rootKidsVODPolicy(func(context.Context, string) (kidsvod.Policy, error) {
		*policyCalls++
		return kidsvod.Policy{Enabled: false, Revision: "disabled-explicitly"}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	store := playback.NewPrequeueStore(time.Hour)
	entry, _ := store.Create("tmdb:movie:42", "Synthetic movie", "profile-a", "movie", 2026, nil, "details")
	store.Update(entry.ID, func(e *playback.PrequeueEntry) {
		e.StreamPath = "/synthetic/file.mkv"
		e.Status = playback.PrequeueStatusReady
	})
	ref, err := kidsvod.NewResourceRef(kidsvod.Prequeue, entry.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	return &kidsVODHTTPBridge{core: core, identity: f.identity, prequeue: &PrequeueHandler{store: store}, pin: func(http.ResponseWriter, *http.Request, string) bool { *pinCalls++; return true }}, ref, pinCalls, policyCalls
}

func TestKidsVODIdentityUsesCurrentEffectiveRoleAndEarliestExpiry(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(map[bool]string{false: "demoted", true: "promoted"}[master], func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			f.account.IsMaster = master
			f.session.IsMaster = !master
			expiry := f.now.Add(time.Minute)
			f.account.ExpiresAt = &expiry
			actual, err := f.identity.CurrentSession(context.Background(), f.session.Token)
			if err != nil || actual.IsMaster != master || !actual.ExpiresAt.Equal(expiry) {
				t.Fatal("cached role or session expiry replaced current account authority")
			}
			if f.session.IsMaster == master || !f.session.ExpiresAt.Equal(f.now.Add(time.Hour)) {
				t.Fatal("reader mutated persisted session")
			}
		})
	}
}

func TestKidsVODIdentityRejectsMissingExpiredForeignAndFailedState(t *testing.T) {
	cases := map[string]func(*rootKidsVODIdentityFixture){
		"profile missing":   func(f *rootKidsVODIdentityFixture) { f.profile = nil },
		"profile mismatch":  func(f *rootKidsVODIdentityFixture) { f.profile.ID = "foreign" },
		"account missing":   func(f *rootKidsVODIdentityFixture) { f.account = nil },
		"account mismatch":  func(f *rootKidsVODIdentityFixture) { f.account.ID = "foreign" },
		"account expired":   func(f *rootKidsVODIdentityFixture) { at := f.now; f.account.ExpiresAt = &at },
		"repository failed": func(f *rootKidsVODIdentityFixture) { f.failure = errors.New("synthetic repository unavailable") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			mutate(f)
			if _, err := f.identity.CurrentProfile(context.Background(), "profile-a"); err == nil {
				t.Fatal("unavailable profile/account became current authority")
			}
		})
	}
	for _, name := range []string{"revoked", "token mismatch", "expired", "foreign account"} {
		t.Run("session "+name, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			switch name {
			case "revoked":
				f.session = nil
			case "token mismatch":
				f.session.Token = "foreign"
			case "expired":
				f.session.ExpiresAt = f.now
			case "foreign account":
				f.session.AccountID = "foreign"
			}
			if _, err := f.identity.CurrentSession(context.Background(), "synthetic-session"); err == nil {
				t.Fatal("invalid session became current authority")
			}
		})
	}
	f := newRootKidsVODIdentityFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.identity.CurrentProfile(ctx, "profile-a"); err == nil {
		t.Fatal("cancelled profile query succeeded")
	}
	if _, err := f.identity.CurrentSession(ctx, "synthetic-session"); err == nil || f.reads != 0 {
		t.Fatal("cancelled identity query touched repository")
	}
}

func TestKidsVODIdentityRevisionTracksPINAndOwnershipPrerequisites(t *testing.T) {
	for name, mutate := range map[string]func(*rootKidsVODIdentityFixture){
		"PIN":            func(f *rootKidsVODIdentityFixture) { f.profile.PinHash = "synthetic-new-fingerprint" },
		"kids state":     func(f *rootKidsVODIdentityFixture) { f.profile.IsKidsProfile = false },
		"account role":   func(f *rootKidsVODIdentityFixture) { f.account.IsMaster = true },
		"account expiry": func(f *rootKidsVODIdentityFixture) { at := f.now.Add(time.Minute); f.account.ExpiresAt = &at },
		"ownership":      func(f *rootKidsVODIdentityFixture) { f.profile.AccountID = "account-b"; f.account.ID = "account-b" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			first, err := f.identity.CurrentProfile(context.Background(), "profile-a")
			if err != nil {
				t.Fatal(err)
			}
			mutate(f)
			second, err := f.identity.CurrentProfile(context.Background(), "profile-a")
			if err != nil || first.Revision == second.Revision {
				t.Fatal("security prerequisite changed without retiring profile revision")
			}
		})
	}
}

func TestKidsVODHTTPBridgeUsesStoredOwnerWithoutGrantingDisabledPolicy(t *testing.T) {
	f := newRootKidsVODIdentityFixture()
	bridge, ref, pinCalls, policyCalls := f.bridge(t)
	w := httptest.NewRecorder()
	intent, decision := bridge.resolve(w, f.request(), "", ref)
	if intent != nil || !decision.IsNotApplicable() || decision.Allowed() || *pinCalls != 1 || *policyCalls != 1 {
		t.Fatal("stored owner/PIN/disabled-policy result was not preserved without granting playback")
	}
}

func TestKidsVODHTTPBridgeRejectsSelectorsAndStaleAuthenticationBeforePIN(t *testing.T) {
	cases := map[string]func(*rootKidsVODIdentityFixture, *kidsVODHTTPBridge, *http.Request, *string){
		"missing PIN guard": func(_ *rootKidsVODIdentityFixture, b *kidsVODHTTPBridge, _ *http.Request, _ *string) { b.pin = nil },
		"explicit foreign owner": func(_ *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, _ *http.Request, p *string) {
			*p = "profile-b"
		},
		"query foreign owner": func(_ *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, r *http.Request, _ *string) {
			r.URL.RawQuery = "userId=profile-b"
		},
		"duplicate selectors": func(_ *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, r *http.Request, _ *string) {
			r.URL.RawQuery = "profileId=profile-a&profileId=profile-b"
		},
		"path foreign owner": func(_ *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, r *http.Request, _ *string) {
			*r = *mux.SetURLVars(r, map[string]string{"userID": "profile-b"})
		},
		"foreign profile ownership": func(f *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, _ *http.Request, _ *string) {
			f.profile.AccountID = "account-b"
			f.account.ID = "account-b"
		},
		"revoked session": func(f *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, _ *http.Request, _ *string) { f.session = nil },
		"expired account": func(f *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, _ *http.Request, _ *string) {
			at := f.now
			f.account.ExpiresAt = &at
		},
		"demoted master": func(f *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, r *http.Request, _ *string) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
		},
		"current share session": func(f *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, _ *http.Request, _ *string) {
			f.session.Scope = models.SessionScopeStream
		},
		"current party session": func(f *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, _ *http.Request, _ *string) {
			f.session.Scope = models.SessionScopeWatchParty
		},
		"spoofed request account": func(_ *rootKidsVODIdentityFixture, _ *kidsVODHTTPBridge, r *http.Request, _ *string) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account-b"))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			bridge, ref, pinCalls, policyCalls := f.bridge(t)
			r := f.request()
			profile := ""
			mutate(f, bridge, r, &profile)
			w := httptest.NewRecorder()
			intent, decision := bridge.resolve(w, r, profile, ref)
			if intent != nil || decision.IsNotApplicable() || decision.Allowed() || w.Code != http.StatusForbidden || *pinCalls != 0 || *policyCalls != 0 {
				t.Fatal("invalid selector/current authentication reached PIN or regional policy")
			}
		})
	}
}

func TestKidsVODHTTPBridgeRequiresConcreteOwnerForColdResources(t *testing.T) {
	for _, profile := range []string{"", " profile-a ", "\t"} {
		t.Run(profile, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			bridge, _, pinCalls, policyCalls := f.bridge(t)
			ref, _ := kidsvod.NewResourceRef(kidsvod.Local, "synthetic-item", 0)
			w := httptest.NewRecorder()
			intent, _ := bridge.resolve(w, f.request(), profile, ref)
			if intent != nil || w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), kidsvod.CodeProfileRequired) || *pinCalls != 0 || *policyCalls != 0 {
				t.Fatal("missing concrete profile selected a default authority")
			}
		})
	}
}

func TestKidsVODHTTPBridgeHonorsExistingResourceGuardBeforeMandatoryPIN(t *testing.T) {
	f := newRootKidsVODIdentityFixture()
	bridge, ref, pinCalls, policyCalls := f.bridge(t)
	guardCalls := 0
	r := profileaccess.WithResourceGuard(f.request(), func(w http.ResponseWriter, _ *http.Request, id string) bool {
		guardCalls++
		if id != "profile-a" {
			t.Fatal("resource guard received caller-selected owner")
		}
		apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked")
		return false
	})
	handler := profileaccess.Middleware(func(profile, account string) bool { return profile == "profile-a" && account == "account-a" })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { bridge.resolve(w, r, "", ref) }))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), apierror.CodeProfileLocked) || guardCalls != 1 || *pinCalls != 0 || *policyCalls != 0 {
		t.Fatal("stored resource bypassed existing ownership/PIN middleware")
	}
}

func TestKidsVODHTTPBridgeUsesRealSessionAndProfileBoundUnlockValidator(t *testing.T) {
	for _, mode := range []string{"valid", "missing", "foreign session", "foreign profile", "PIN replaced", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			bridge, ref, _, policyCalls := f.bridge(t)
			unlocks := profileunlock.NewService(nil)
			issue := profileunlock.IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: "account-a", ProfileID: "profile-a", PinFingerprint: f.profile.PinHash}
			if mode == "foreign session" {
				issue.SessionToken = "synthetic-other-session"
			}
			if mode == "foreign profile" {
				issue.ProfileID = "profile-b"
			}
			token, _, err := unlocks.Issue(issue)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "PIN replaced" {
				f.profile.PinHash = "synthetic-new-fingerprint"
			}
			if mode == "revoked" {
				unlocks.RevokeProfile("profile-a")
			}
			bridge.pin = func(w http.ResponseWriter, r *http.Request, id string) bool {
				session, _ := auth.GetSession(r)
				if unlocks.Validate(profileunlock.RequestTokens(r), session.Token, id, f.profile.PinHash) {
					return true
				}
				apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked")
				return false
			}
			r := f.request()
			if mode != "missing" {
				r.Header.Set(profileunlock.Header, token)
			}
			w := httptest.NewRecorder()
			intent, decision := bridge.resolve(w, r, "", ref)
			if mode == "valid" {
				if intent != nil || !decision.IsNotApplicable() || decision.Allowed() || *policyCalls != 1 {
					t.Fatal("real current unlock did not reach explicit disabled policy")
				}
				return
			}
			if intent != nil || decision.IsNotApplicable() || w.Code != http.StatusForbidden || *policyCalls != 0 {
				t.Fatal("invalid/revoked unlock reached regional policy")
			}
		})
	}
}

func TestKidsVODHTTPBridgeRechecksAuthorityAfterPINCallback(t *testing.T) {
	for _, change := range []string{"profile ownership", "session revoked", "account expires", "account demoted", "PIN replaced"} {
		t.Run(change, func(t *testing.T) {
			f := newRootKidsVODIdentityFixture()
			if change == "account demoted" {
				f.account.IsMaster = true
			}
			bridge, ref, _, policyCalls := f.bridge(t)
			r := f.request()
			bridge.pin = func(http.ResponseWriter, *http.Request, string) bool {
				switch change {
				case "profile ownership":
					f.profile.AccountID = "account-b"
					f.account.ID = "account-b"
				case "session revoked":
					f.session = nil
				case "account expires":
					at := f.now
					f.account.ExpiresAt = &at
				case "account demoted":
					f.account.IsMaster = false
				case "PIN replaced":
					f.profile.PinHash = "synthetic-new-fingerprint"
				}
				return true
			}
			intent, decision := bridge.resolve(httptest.NewRecorder(), r, "", ref)
			if intent != nil || decision.IsNotApplicable() || decision.Allowed() || *policyCalls != 0 {
				t.Fatal("authority changed during PIN check but stale context reached policy")
			}
		})
	}
}
