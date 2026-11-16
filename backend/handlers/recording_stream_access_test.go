package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

type recordingOwnerFunc func(context.Context, string) (recordings.StreamOwner, error)

func (f recordingOwnerFunc) SnapshotStreamOwner(c context.Context, p string) (recordings.StreamOwner, error) {
	return f(c, p)
}

type recordingSessionFunc func(context.Context, string) (*models.Session, error)

func (f recordingSessionFunc) SnapshotForAuthorization(c context.Context, p string) (*models.Session, error) {
	return f(c, p)
}

type recordingAccessAccountMap map[string]models.Account

func (m recordingAccessAccountMap) Get(id string) (models.Account, bool) {
	a, ok := m[id]
	return a, ok
}

type recordingAccessProfileMap map[string]models.User

func (m recordingAccessProfileMap) Get(id string) (models.User, bool) { p, ok := m[id]; return p, ok }
func (m recordingAccessProfileMap) PinFingerprint(id string) string {
	if m[id].PinHash == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(m[id].PinHash))
	return hex.EncodeToString(sum[:8])
}

type recordingUnlockFunc func([]string, string, string, string) bool

func (f recordingUnlockFunc) Validate(ts []string, s, p, h string) bool { return f(ts, s, p, h) }

type recordingAccessFixture struct {
	guard     *RecordingStreamAccess
	profiles  recordingAccessProfileMap
	accounts  recordingAccessAccountMap
	session   models.Session
	owner     recordings.StreamOwner
	enforce   bool
	policyErr error
	unlocks   *profileunlock.Service
}

func newRecordingAccessFixture(t *testing.T) *recordingAccessFixture {
	t.Helper()
	f := &recordingAccessFixture{profiles: recordingAccessProfileMap{"adult": {ID: "adult", AccountID: "account-one"}}, accounts: recordingAccessAccountMap{"account-one": {ID: "account-one"}, "account-two": {ID: "account-two"}}, session: models.Session{Token: "synthetic-session", AccountID: "account-one", ExpiresAt: time.Now().Add(time.Hour)}, owner: recordings.StreamOwner{RecordingID: "recording-one", ProfileID: "adult"}, unlocks: profileunlock.NewService(nil)}
	f.guard = NewRecordingStreamAccess(recordingOwnerFunc(func(ctx context.Context, path string) (recordings.StreamOwner, error) { return f.owner, ctx.Err() }), recordingSessionFunc(func(ctx context.Context, token string) (*models.Session, error) {
		if token != f.session.Token {
			return nil, errors.New("missing")
		}
		s := f.session
		return &s, ctx.Err()
	}), f.accounts, f.profiles, f.unlocks, func() (config.SecuritySettings, error) {
		return config.SecuritySettings{EnforceProfilePins: f.enforce}, f.policyErr
	})
	return f
}
func (f *recordingAccessFixture) request(t *testing.T, query string) *http.Request {
	t.Helper()
	r := httptest.NewRequest("GET", "/api/video/stream?path=recording:recording-one"+query, nil)
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, f.session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, f.session.IsMaster)
	return r.WithContext(ctx)
}
func (f *recordingAccessFixture) issue(t *testing.T) string {
	t.Helper()
	token, _, err := f.unlocks.Issue(profileunlock.IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: f.session.AccountID, ProfileID: "adult", PinFingerprint: f.profiles.PinFingerprint("adult")})
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func TestRecordingStreamAccessOwnerRoleAndScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*recordingAccessFixture)
		query  string
		allow  bool
		code   string
	}{
		{"owned omitted selector", nil, "", true, ""},
		{"owned explicit selector", nil, "&profileId=adult", true, ""},
		{"wrong selector", nil, "&profileId=other", false, "resource_not_found"},
		{"conflicting legacy selector", nil, "&profileId=adult&userId=other", false, "resource_not_found"},
		{"case variant selector", nil, "&ProfileID=other", false, "resource_not_found"},
		{"foreign", func(f *recordingAccessFixture) {
			p := f.profiles["adult"]
			p.AccountID = "account-two"
			f.profiles["adult"] = p
		}, "", false, "resource_not_found"},
		{"real current master", func(f *recordingAccessFixture) {
			a := f.accounts["account-one"]
			a.IsMaster = true
			f.accounts[a.ID] = a
			p := f.profiles["adult"]
			p.AccountID = "account-two"
			f.profiles[p.ID] = p
		}, "", true, ""},
		{"cached master is not authority", func(f *recordingAccessFixture) {
			f.session.IsMaster = true
			p := f.profiles["adult"]
			p.AccountID = "account-two"
			f.profiles[p.ID] = p
		}, "", false, "resource_not_found"},
		{"stored child", func(f *recordingAccessFixture) {
			p := f.profiles["adult"]
			p.IsKidsProfile = true
			f.profiles[p.ID] = p
		}, "", false, "kids_live_tv_unavailable"},
		{"expired session", func(f *recordingAccessFixture) { f.session.ExpiresAt = time.Now().Add(-time.Second) }, "", false, "resource_not_found"},
		{"expired account", func(f *recordingAccessFixture) {
			a := f.accounts["account-one"]
			at := time.Now().Add(-time.Second)
			a.ExpiresAt = &at
			f.accounts[a.ID] = a
		}, "", false, "resource_not_found"},
		{"missing owner", func(f *recordingAccessFixture) { f.owner.ProfileID = "" }, "", false, "resource_not_found"},
		{"policy unavailable", func(f *recordingAccessFixture) { f.policyErr = errors.New("unavailable") }, "", false, "resource_not_found"},
		{"stream scoped", func(f *recordingAccessFixture) {
			f.session.Scope = models.SessionScopeStream
			f.session.ScopeResource = "recording:recording-one"
		}, "", false, "resource_not_found"},
		{"party scoped", func(f *recordingAccessFixture) { f.session.Scope = models.SessionScopeWatchParty }, "", false, "resource_not_found"},
		{"bridge scoped", func(f *recordingAccessFixture) { f.session.Scope = models.SessionScopeJellyfin }, "", false, "resource_not_found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRecordingAccessFixture(t)
			if tc.change != nil {
				tc.change(f)
			}
			w := httptest.NewRecorder()
			ok := f.guard.Check(w, f.request(t, tc.query), "recording:recording-one")
			if ok != tc.allow || !ok && !strings.Contains(w.Body.String(), tc.code) {
				t.Fatalf("allowed=%v status=%d body=%s", ok, w.Code, w.Body.String())
			}
		})
	}
}

func TestRecordingStreamAccessRealPINAndCurrentPolicy(t *testing.T) {
	for _, kind := range []string{"missing", "valid", "other session", "other profile", "replaced PIN", "revoked", "policy off"} {
		t.Run(kind, func(t *testing.T) {
			f := newRecordingAccessFixture(t)
			f.enforce = true
			p := f.profiles["adult"]
			p.PinHash = "synthetic-pin-hash"
			f.profiles[p.ID] = p
			r := f.request(t, "")
			if kind != "missing" && kind != "policy off" {
				token := f.issue(t)
				r.Header.Set("X-Profile-Unlock", token)
			}
			switch kind {
			case "other session":
				f.session.Token = "replacement-session"
				r = f.request(t, "")
			case "other profile":
				f.owner.ProfileID = "other"
				f.profiles["other"] = models.User{ID: "other", AccountID: "account-one", PinHash: "different"}
			case "replaced PIN":
				p.PinHash = "replacement"
				f.profiles[p.ID] = p
			case "revoked":
				f.unlocks.RevokeProfile("adult")
			case "policy off":
				f.enforce = false
			}
			want := kind == "valid" || kind == "policy off"
			w := httptest.NewRecorder()
			if got := f.guard.Check(w, r, "recording:recording-one"); got != want {
				t.Fatalf("allow=%v want=%v status=%d body=%s", got, want, w.Code, w.Body.String())
			}
		})
	}
}

func TestRecordingStreamAccessRechecksAuthorityAfterPIN(t *testing.T) {
	for _, change := range []string{"owner", "PIN", "role", "child", "session", "policy", "cancel"} {
		t.Run(change, func(t *testing.T) {
			f := newRecordingAccessFixture(t)
			f.enforce = true
			p := f.profiles["adult"]
			p.PinHash = "synthetic-pin-hash"
			f.profiles[p.ID] = p
			r := f.request(t, "")
			ctx, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(ctx)
			f.guard.unlocks = recordingUnlockFunc(func([]string, string, string, string) bool {
				switch change {
				case "owner":
					f.owner.ProfileID = "other"
				case "PIN":
					p.PinHash = "changed"
					f.profiles[p.ID] = p
				case "role":
					a := f.accounts["account-one"]
					a.IsMaster = true
					f.accounts[a.ID] = a
				case "child":
					p.IsKidsProfile = true
					f.profiles[p.ID] = p
				case "session":
					f.session.ExpiresAt = time.Now().Add(-time.Second)
				case "policy":
					f.enforce = false
				case "cancel":
					cancel()
				}
				return true
			})
			if f.guard.Check(httptest.NewRecorder(), r, "recording:recording-one") {
				t.Fatal("changed authorization allowed demand")
			}
		})
	}
}

type recordingProjectionCounter struct {
	recordingService
	gets int
}

func (s *recordingProjectionCounter) Get(string) (*models.Recording, error) {
	s.gets++
	return nil, errors.New("media projection must not run")
}

func TestRecordingsStreamDeniesBeforeMediaProjection(t *testing.T) {
	for _, mode := range []string{"nil guard", "foreign", "missing owner", "child", "scoped", "locked"} {
		t.Run(mode, func(t *testing.T) {
			f := newRecordingAccessFixture(t)
			s := &recordingProjectionCounter{}
			h := NewRecordingsHandler(s, nil)
			if mode != "nil guard" {
				h.SetRecordingStreamAccess(f.guard)
			}
			switch mode {
			case "foreign":
				p := f.profiles["adult"]
				p.AccountID = "account-two"
				f.profiles[p.ID] = p
			case "missing owner":
				f.owner.ProfileID = ""
			case "child":
				p := f.profiles["adult"]
				p.IsKidsProfile = true
				f.profiles[p.ID] = p
			case "scoped":
				f.session.Scope = models.SessionScopeStream
			case "locked":
				f.enforce = true
				p := f.profiles["adult"]
				p.PinHash = "synthetic-pin"
				f.profiles[p.ID] = p
			}
			r := mux.SetURLVars(f.request(t, ""), map[string]string{"recordingID": "recording-one"})
			w := httptest.NewRecorder()
			h.Stream(w, r)
			if w.Code < 400 || s.gets != 0 {
				t.Fatalf("status=%d media projections=%d", w.Code, s.gets)
			}
		})
	}
}

func TestRecordingsStreamUsesCurrentMasterForStoredOwnerMiddleware(t *testing.T) {
	f := newRecordingAccessFixture(t)
	a := f.accounts["account-one"]
	a.IsMaster = true
	f.accounts[a.ID] = a
	p := f.profiles["adult"]
	p.AccountID = "account-two"
	f.profiles[p.ID] = p
	file := filepath.Join(t.TempDir(), "synthetic.ts")
	if err := os.WriteFile(file, []byte("synthetic-owned-bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	s := &fakeRecordingService{recording: models.Recording{ID: "recording-one", UserID: "adult", Status: models.RecordingStatusCompleted, OutputPath: file}}
	users := &fakeRecordingUsersProvider{users: []models.User{p}}
	h := NewRecordingsHandler(s, users)
	h.SetRecordingStreamAccess(f.guard)
	r := mux.SetURLVars(f.request(t, ""), map[string]string{"recordingID": "recording-one"})
	w := httptest.NewRecorder()
	profileaccess.Middleware(users.BelongsToAccount)(http.HandlerFunc(h.Stream)).ServeHTTP(w, r)
	if w.Code != 200 || w.Body.String() != "synthetic-owned-bytes" {
		t.Fatalf("current master denied: %d %s", w.Code, w.Body.String())
	}
	if auth.IsMaster(r) {
		t.Fatal("original request role was mutated")
	}
}
