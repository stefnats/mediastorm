package handlers

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
	"novastream/services/profileunlock"
	"novastream/services/trackselect"
)

type vodTestState struct {
	mu       sync.Mutex
	profiles map[string]models.User
	accounts map[string]models.Account
	session  models.Session
	states   map[string]vodAccountApplicability
	failure  error
	pinCalls atomic.Int32
	unlocks  *profileunlock.Service
	token    string
	boundary *vodBoundary
}

func (f *vodTestState) CurrentVODApplicability(_ context.Context, account string) (vodAccountApplicability, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.states[account], f.failure
}
func newVODTestState(t *testing.T, mode vodApplicabilityMode, kid bool) *vodTestState {
	t.Helper()
	f := &vodTestState{profiles: map[string]models.User{"profile": {ID: "profile", AccountID: "account", IsKidsProfile: kid, PinHash: "pin-v1"}, "foreign": {ID: "foreign", AccountID: "other", PinHash: "other-pin"}}, accounts: map[string]models.Account{"account": {ID: "account"}, "other": {ID: "other"}}, session: models.Session{Token: "synthetic-vod-session", AccountID: "account", ClientID: "device", ExpiresAt: time.Now().Add(time.Hour)}, states: map[string]vodAccountApplicability{"account": {mode: mode, revision: "v1"}, "other": {mode: mode, revision: "v1"}}, unlocks: profileunlock.NewService(nil)}
	identity := &kidsVODIdentity{now: time.Now, profiles: rootKidsVODProfiles(func(_ context.Context, id string) (*models.User, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p, ok := f.profiles[id]
		if !ok {
			return nil, errors.New("missing profile")
		}
		return &p, nil
	}), accounts: rootKidsVODAccounts(func(_ context.Context, id string) (*models.Account, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		a, ok := f.accounts[id]
		if !ok {
			return nil, errors.New("missing account")
		}
		return &a, nil
	}), sessions: rootKidsVODSessions(func(_ context.Context, token string) (*models.Session, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if token != f.session.Token {
			return nil, errors.New("revoked")
		}
		s := f.session
		return &s, nil
	})}
	token, _, err := f.unlocks.Issue(profileunlock.IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: "account", ProfileID: "profile", PinFingerprint: "pin-v1"})
	if err != nil {
		t.Fatal(err)
	}
	f.token = token
	f.boundary = newVODBoundary(identity, f, func(w http.ResponseWriter, r *http.Request, id string) bool {
		f.pinCalls.Add(1)
		f.mu.Lock()
		p := f.profiles[id]
		f.mu.Unlock()
		session, _ := auth.GetSession(r)
		if f.unlocks.Validate(profileunlock.RequestTokens(r), session.Token, id, p.PinHash) {
			return true
		}
		w.WriteHeader(http.StatusForbidden)
		return false
	})
	t.Cleanup(func() {
		f.boundary.mu.Lock()
		defer f.boundary.mu.Unlock()
		for _, o := range f.boundary.workers {
			o.release()
		}
		for _, o := range f.boundary.active {
			o.release()
		}
	})
	return f
}
func (f *vodTestState) change(fn func(*vodTestState)) { f.mu.Lock(); defer f.mu.Unlock(); fn(f) }
func (f *vodTestState) request(method, target, body string) *http.Request {
	f.mu.Lock()
	session := f.session
	master := f.accounts[session.AccountID].IsMaster
	f.mu.Unlock()
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	c := context.WithValue(r.Context(), auth.ContextKeyAccountID, session.AccountID)
	c = context.WithValue(c, auth.ContextKeySession, session)
	c = context.WithValue(c, auth.ContextKeyIsMaster, master)
	r = r.WithContext(c)
	r.Header.Set("X-Client-ID", "device")
	r.Header.Set(profileunlock.Header, f.token)
	return r
}
func TestVODBoundaryUnknownInactiveAndActiveAreDistinct(t *testing.T) {
	for _, mode := range []vodApplicabilityMode{vodApplicabilityUnknown, vodExplicitInactive, vodAccountActive} {
		t.Run(string(rune('0'+mode)), func(t *testing.T) {
			f := newVODTestState(t, mode, true)
			r := f.request("GET", "/unregistered?profileId=profile", "")
			o, ok := f.boundary.requestOwner(httptest.NewRecorder(), r, vodDemand, "")
			defer o.release()
			if ok != (mode == vodExplicitInactive) {
				t.Fatal("unknown/active child became legacy fallback")
			}
			if ok && !o.notApplicable() {
				t.Fatal("explicit inactive did not retain distinct correlation")
			}
		})
	}
	f := newVODTestState(t, vodExplicitInactive, false)
	f.change(func(f *vodTestState) { f.failure = errors.New("synthetic unavailable") })
	if o, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("GET", "/", ""), vodDemand, ""); ok || o != nil {
		t.Fatal("failed applicability became inactive")
	}
	var zero vodOwnerContext
	if zero.notApplicable() || zero.recheck(context.Background(), vodDemand) == nil {
		t.Fatal("zero context granted legacy path")
	}
}
func TestVODBoundaryOpaqueCorrelationRetainsOriginalScopeAndExpiry(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, false)
	r := f.request("GET", "/?profileId=profile", "")
	r.Header.Set(parentauth.PasswordHeader, "synthetic-parent-password")
	r.Header.Set("Authorization", "Bearer synthetic-unrelated-token")
	o, ok := f.boundary.requestOwner(httptest.NewRecorder(), r, vodDemand, "")
	if !ok {
		t.Fatal("adult owner denied")
	}
	defer o.release()
	expiry := o.expires
	for i := 0; i < 5; i++ {
		if err := o.recheck(context.Background(), vodDemand); err != nil {
			t.Fatal(err)
		}
	}
	if !expiry.Equal(o.expires) {
		t.Fatal("recheck renewed original expiry")
	}
	if o.request.Header.Get(parentauth.PasswordHeader) != "" || o.request.Header.Get("Authorization") != "" || o.request.Body != nil || o.request.URL.RawQuery != "" {
		t.Fatal("private worker retained unrelated credentials/body/source claims")
	}
	data, _ := json.Marshal(o)
	if string(data) != "null" {
		t.Fatal("owner context serialized authority")
	}
	var imported vodOwnerContext
	if json.Unmarshal(data, &imported) == nil || imported.notApplicable() {
		t.Fatal("imported correlation became authority")
	}
	f.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v2"}
	})
	if o.recheck(context.Background(), vodDemand) == nil {
		t.Fatal("captured active intent replayed under new policy")
	}
}
func TestVODBoundaryInactiveCannotBecomeLateActiveFallback(t *testing.T) {
	f := newVODTestState(t, vodExplicitInactive, false)
	o, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("GET", "/?profileId=profile", ""), vodDemand, "")
	if !ok {
		t.Fatal("inactive rejected")
	}
	defer o.release()
	f.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "v2"}
	})
	if o.recheck(context.Background(), vodDemand) == nil {
		t.Fatal("inactive correlation bypassed newly active rule")
	}
}
func TestVODBoundaryRealPINAndContextChangesRejectReplay(t *testing.T) {
	for _, change := range []string{"PIN", "unlock revoked", "session revoked", "expiry", "child", "owner", "client"} {
		t.Run(change, func(t *testing.T) {
			f := newVODTestState(t, vodAccountActive, false)
			o, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("GET", "/?profileId=profile", ""), vodDemand, "")
			if !ok {
				t.Fatal("valid existing unlock denied")
			}
			defer o.release()
			if change == "unlock revoked" {
				f.unlocks.RevokeProfile("profile")
			} else {
				f.change(func(f *vodTestState) {
					p := f.profiles["profile"]
					switch change {
					case "PIN":
						p.PinHash = "pin-v2"
					case "session revoked":
						f.session.Token = "replacement"
					case "expiry":
						f.session.ExpiresAt = time.Now().Add(-time.Second)
					case "child":
						p.IsKidsProfile = true
					case "owner":
						p.AccountID = "other"
					case "client":
						f.session.ClientID = "other"
					}
					f.profiles["profile"] = p
				})
			}
			if o.recheck(context.Background(), vodDemand) == nil {
				t.Fatal("stale owner context remained usable")
			}
		})
	}
}
func TestVODBoundaryCleanupOnlySurvivesPolicyFailureWithoutBecomingDemand(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, true)
	f.change(func(f *vodTestState) { f.failure = errors.New("policy unavailable") })
	o, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("DELETE", "/?profileId=profile", ""), vodCleanup, "profile")
	if !ok {
		t.Fatal("policy outage prevented owned current-PIN cleanup")
	}
	defer o.release()
	if o.notApplicable() || o.recheck(context.Background(), vodDemand) == nil {
		t.Fatal("cleanup-only became media permission")
	}
	f.change(func(f *vodTestState) {
		f.failure = nil
		f.states["account"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v2"}
	})
	if o.recheck(context.Background(), vodCleanup) != nil || o.recheck(context.Background(), vodDemand) == nil {
		t.Fatal("cleanup context changed purpose after policy switch")
	}
	f = newVODTestState(t, vodExplicitInactive, false)
	old, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("DELETE", "/", ""), vodCleanup, "profile")
	if !ok {
		t.Fatal("legacy inactive stored-owner cleanup denied")
	}
	defer old.release()
	f.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "v2"}
	})
	if old.recheck(context.Background(), vodCleanup) == nil {
		t.Fatal("no-selector cleanup replayed into active account")
	}
}
func TestVODBoundaryMasterCannotHideProtectedStoredOwnerAccount(t *testing.T) {
	f := newVODTestState(t, vodExplicitInactive, false)
	f.change(func(f *vodTestState) {
		a := f.accounts["account"]
		a.IsMaster = true
		f.accounts["account"] = a
		f.states["other"] = vodAccountApplicability{mode: vodAccountActive, revision: "protected"}
		p := f.profiles["foreign"]
		p.IsKidsProfile = true
		f.profiles["foreign"] = p
	})
	for _, query := range []string{"", "?profileId=foreign"} {
		o, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("GET", "/"+query, ""), vodReady, "foreign")
		defer o.release()
		if ok {
			t.Fatal("master inactive account hid protected owner account")
		}
	}
}

func TestVODBoundaryRechecksForeignOwnerPolicyAfterFinalSessionRead(t *testing.T) {
	f := newVODTestState(t, vodExplicitInactive, false)
	f.change(func(f *vodTestState) { a := f.accounts["account"]; a.IsMaster = true; f.accounts["account"] = a })
	o, ok := f.boundary.requestOwner(httptest.NewRecorder(), f.request("GET", "/?profileId=foreign", ""), vodReady, "foreign")
	if !ok {
		t.Fatal("inactive owned master context unavailable")
	}
	defer o.release()
	original := f.boundary.identity.sessions
	var calls atomic.Int32
	f.boundary.identity.sessions = rootKidsVODSessions(func(ctx context.Context, token string) (*models.Session, error) {
		s, err := original.Get(ctx, token)
		if calls.Add(1) == 2 {
			f.change(func(f *vodTestState) {
				f.states["other"] = vodAccountApplicability{mode: vodAccountActive, revision: "new"}
			})
		}
		return s, err
	})
	if o.recheck(context.Background(), vodReady) == nil {
		t.Fatal("changed foreign-owner policy was omitted from final dependency check")
	}
}

type vodTrackClientSettings struct{ client, profile string }

func (s *vodTrackClientSettings) Get(client, profile string) (*models.ClientFilterSettings, error) {
	s.client, s.profile = client, profile
	language := "fra"
	return &models.ClientFilterSettings{PreferredAudioLanguage: &language}, nil
}
func TestVODResolveUsesAuthenticatedClientForServiceAndTracksWithoutHeader(t *testing.T) {
	f := newVODTestState(t, vodAccountActive, false)
	seenClient, seenProfile := "", ""
	h := NewPlaybackHandler(&mockPlaybackService{resolveFunc: func(_ context.Context, r models.NZBResult) (*models.PlaybackResolution, error) {
		seenClient, seenProfile = r.Attributes["clientId"], r.Attributes["profileId"]
		return &models.PlaybackResolution{WebDAVPath: "/synthetic/current.mkv"}, nil
	}})
	h.vodBoundary = f.boundary
	settings := &vodTrackClientSettings{}
	h.TrackPreferences = &trackselect.Resolver{ClientSettings: settings}
	h.VideoProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
		return &VideoFullResult{AudioStreams: []models.AudioStreamInfo{{Index: 1, Language: "eng", Codec: "aac"}, {Index: 4, Language: "fra", Codec: "aac"}}}, nil
	}}
	r := f.request(http.MethodPost, "/?profileId=profile", `{"result":{"title":"Synthetic"},"includeTrackSelection":true}`)
	r.Header.Del("X-Client-ID")
	w := httptest.NewRecorder()
	h.Resolve(w, r)
	var response models.PlaybackResolution
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || seenClient != "device" || seenProfile != "profile" || settings.client != "device" || settings.profile != "profile" || response.TrackSelection == nil || response.TrackSelection.AudioStreamIndex() != 4 {
		t.Fatal("manual resolution used a different client/profile than current authorization")
	}
}

func TestVODBoundaryRepeatedBodySelectorsAndTrailingDataAreNotCollapsed(t *testing.T) {
	for _, body := range []string{`{"profileId":"foreign","profileId":"profile","result":{"title":"Synthetic"}}`, `{"profileId":"foreign","result":{"title":"Synthetic"}} {}`} {
		t.Run(body, func(t *testing.T) {
			f := newVODTestState(t, vodAccountActive, false)
			var calls atomic.Int32
			h := NewPlaybackHandler(&mockPlaybackService{resolveFunc: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
				calls.Add(1)
				return &models.PlaybackResolution{WebDAVPath: "/synthetic/ready"}, nil
			}})
			h.vodBoundary = f.boundary
			w := httptest.NewRecorder()
			h.Resolve(w, f.request(http.MethodPost, "/?profileId=profile", body))
			if w.Code < 400 || calls.Load() != 0 {
				t.Fatal("contradictory repeated/trailing selector body reached provider demand")
			}
		})
	}
}
