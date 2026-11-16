package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/libraryaccess"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
)

type zeroRestartProvider struct {
	mu       sync.Mutex
	bodies   []*managedHLSGateBody
	contexts []context.Context
	calls    atomic.Int32
	before   func(int) error
}

func (p *zeroRestartProvider) Stream(ctx context.Context, r streaming.Request) (*streaming.Response, error) {
	n := int(p.calls.Add(1))
	if r.Method != "GET" || r.RangeHeader != "" {
		return nil, errors.New("unexpected source request")
	}
	if p.before != nil {
		if err := p.before(n); err != nil {
			return nil, err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if n > len(p.bodies) {
		return nil, errors.New("extra source input")
	}
	p.contexts = append(p.contexts, ctx)
	b := p.bodies[n-1]
	return &streaming.Response{Body: b, Status: 200, ContentLength: int64(len(b.all)), Headers: make(http.Header)}, nil
}
func (*zeroRestartProvider) GetDirectURL(context.Context, string) (string, error) {
	panic("restart used a direct URL")
}
func (*zeroRestartProvider) GetDuration(context.Context, string) (float64, error) {
	panic("restart used cached duration")
}
func zeroRestartFixture(t *testing.T) (*VideoHandler, *vodTestState, *zeroRestartProvider) {
	t.Helper()
	data := managedHLSMedia(t)
	p := &zeroRestartProvider{bodies: []*managedHLSGateBody{newManagedHLSGate(data), newManagedHLSGate(data)}}
	h, state := managedHLSHandler(t, p)
	state.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v1"}
		f.states["other"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v1"}
	})
	return h, state, p
}
func zeroRestartStart(t *testing.T, h *VideoHandler, f *vodTestState) hlsSessionSnapshot {
	t.Helper()
	w := httptest.NewRecorder()
	h.StartHLSSession(w, managedHLSRequest(f))
	if w.Code != 200 {
		t.Fatalf("normal original start denied %d %s", w.Code, w.Body.String())
	}
	var v struct {
		ID string `json:"sessionId"`
	}
	if json.Unmarshal(w.Body.Bytes(), &v) != nil || v.ID == "" {
		t.Fatal("missing original publication")
	}
	s, ok := h.hlsManager.snapshotHLSSession(v.ID)
	if !ok || s.input == nil {
		t.Fatal("missing actual original input")
	}
	return s
}
func zeroRestartRequest(f *vodTestState, id string) *http.Request {
	r := f.request("POST", "/api/video/hls/"+id+"/seek?time=0&profileId=profile", "")
	return mux.SetURLVars(r, map[string]string{"sessionID": id})
}
func zeroRestartHTTP(h *VideoHandler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.SeekHLSSession(w, r)
	return w
}
func waitZeroRestartDone(t *testing.T, c *vodHLSInput) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(10 * time.Second):
		t.Fatal("owned process/body/pump not joined")
	}
}
func assertZeroRestartMedia(t *testing.T, s hlsSessionSnapshot) {
	t.Helper()
	waitZeroRestartDone(t, s.input)
	raw, err := os.ReadFile(filepath.Join(s.output, "stream.m3u8"))
	if err != nil || !bytes.Contains(raw, []byte("segment0.ts")) {
		t.Fatalf("actual playlist missing: %v", err)
	}
	out, err := exec.CommandContext(t.Context(), "/usr/bin/ffprobe", "-v", "error", "-show_entries", "stream=codec_name", "-of", "json", filepath.Join(s.output, "segment0.ts")).CombinedOutput()
	if err != nil || !bytes.Contains(out, []byte("h264")) || !bytes.Contains(out, []byte("aac")) {
		t.Fatalf("actual output not H264/AAC: %v %s", err, out)
	}
}
func TestHLSZeroRestartActualTwoAttemptsKeepOriginalAuthorityAndHandoff(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	close(p.bodies[0].gate)
	assertZeroRestartMedia(t, old)
	original := old.input.owner
	deadline := original.expires
	p.before = func(n int) error {
		if n != 2 {
			return nil
		}
		select {
		case <-old.input.done:
		default:
			return errors.New("second input opened before old join")
		}
		if p.bodies[0].closes.Load() != 1 {
			return errors.New("old actual body not closed exactly once")
		}
		return nil
	}
	r := zeroRestartRequest(f, old.id)
	setup, cancel := context.WithCancel(r.Context())
	r = r.WithContext(setup)
	w := zeroRestartHTTP(h, r)
	if w.Code != 200 {
		t.Fatalf("zero restart denied %d %s", w.Code, w.Body.String())
	}
	fresh, ok := h.hlsManager.snapshotHLSSession(old.id)
	if !ok || fresh.input == old.input || fresh.session == old.session || fresh.generation != old.generation+1 || fresh.directory == old.directory || fresh.output == old.output || fresh.input.owner != original || !fresh.input.owner.expires.Equal(deadline) {
		t.Fatal("replacement changed original authority or reused old attempt/claim")
	}
	if p.calls.Load() != 2 {
		t.Fatal("expected one actual GET per attempt")
	}
	cancel()
	if fresh.input.ctx.Err() != nil {
		t.Fatal("HTTP completion killed handed-off successor")
	}
	close(p.bodies[1].gate)
	assertZeroRestartMedia(t, fresh)
	if _, err := os.Stat(old.output); !os.IsNotExist(err) {
		t.Fatal("detached joined old directory retained")
	}
	if h.hlsManager.cleanupHLSSessionSnapshot(old) {
		t.Fatal("old snapshot removed successor")
	}
	if _, err := os.Stat(fresh.output); err != nil {
		t.Fatal("old cleanup removed new output")
	}
	denied := zeroRestartHTTP(h, zeroRestartRequest(f, fresh.id))
	if denied.Code != 403 || p.calls.Load() != 2 {
		t.Fatal("third attempt opened source")
	}
	// Existing HTTP/session scope can still read the actual successor playlist.
	read := httptest.NewRecorder()
	rr := f.request("GET", "/api/video/hls/"+fresh.id+"/stream.m3u8?profileId=profile", "")
	rr = mux.SetURLVars(rr, map[string]string{"sessionID": fresh.id})
	h.ServeHLSPlaylist(read, rr)
	if read.Code != 200 || !strings.Contains(read.Body.String(), "profileId=profile") {
		t.Fatal("normal successor playlist not readable")
	}
}
func TestHLSZeroRestartJoinsRunningOldBodyBeforeReopening(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	select {
	case <-p.bodies[0].reading:
	case <-time.After(5 * time.Second):
		t.Fatal("old process did not consume actual source")
	}
	p.before = func(n int) error {
		if n == 2 {
			select {
			case <-old.input.done:
			default:
				return errors.New("old pump still active")
			}
			if p.bodies[0].closes.Load() != 1 {
				return errors.New("old body retained")
			}
			old.session.mu.RLock()
			cmd := old.session.FFmpegCmd
			old.session.mu.RUnlock()
			if cmd == nil || cmd.ProcessState == nil {
				return errors.New("old process not waited")
			}
		}
		return nil
	}
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 200 || p.calls.Load() != 2 {
		t.Fatalf("running restart failed %d %s", w.Code, w.Body.String())
	}
	fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
	close(p.bodies[1].gate)
	assertZeroRestartMedia(t, fresh)
}
func TestHLSZeroRestartAuthorityAndRawQueriesDenyBeforeTouchOrNewInput(t *testing.T) {
	for _, mode := range []string{"missing_profile", "foreign_profile", "conflict", "empty_alias", "time_missing", "time_nonzero", "time_duplicate", "time_fraction", "path", "audio", "body", "missing_claim", "other_token", "scope", "client", "client_duplicate", "missing_pin", "pin_changed", "policy_active", "policy_unknown", "policy_revision", "profile_changed", "expired", "renewed", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			h, f, p := zeroRestartFixture(t)
			old := zeroRestartStart(t, h, f)
			// Ensure the original worker is inside the actual blocked source read,
			// rather than still observing the mutation during initial replay. This
			// targets the HTTP denial, not a background-retirement404 shortcut.
			select {
			case <-p.bodies[0].reading:
			case <-time.After(5 * time.Second):
				t.Fatal("original actual reader did not reach denial barrier")
			}
			r := zeroRestartRequest(f, old.id)
			q := r.URL.Query()
			switch mode {
			case "missing_profile":
				q.Del("profileId")
			case "foreign_profile":
				q.Set("profileId", "foreign")
			case "conflict":
				q.Add("USERID", "foreign")
			case "empty_alias":
				q.Add("userId", "")
			case "time_missing":
				q.Del("time")
			case "time_nonzero":
				q.Set("time", "1")
			case "time_duplicate":
				q.Add("time", "0")
			case "time_fraction":
				q.Set("time", "0.0")
			case "path":
				q.Set("path", "debrid/other.ts")
			case "audio":
				q.Set("audioTrack", "1")
			case "body":
				r.Body = io.NopCloser(strings.NewReader(`{"profileId":"profile"}`))
				r.ContentLength = 23
			case "missing_claim":
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{}))
			case "other_token":
				claim, _ := auth.GetSession(r)
				claim.Token = "other-session"
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, claim))
			case "scope":
				claim, _ := auth.GetSession(r)
				claim.Scope = models.SessionScopeStream
				claim.ScopeResource = old.path
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, claim))
			case "client":
				r.Header.Set("X-Client-ID", "other")
			case "client_duplicate":
				r.Header.Add("X-Client-ID", "other")
			case "missing_pin":
				r.Header.Del("X-Profile-Unlock")
			case "pin_changed":
				f.change(func(f *vodTestState) { v := f.profiles["profile"]; v.PinHash = "replaced"; f.profiles[v.ID] = v })
			case "policy_active":
				f.change(func(f *vodTestState) {
					f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "v1"}
				})
			case "policy_unknown":
				f.change(func(f *vodTestState) { f.states["account"] = vodAccountApplicability{} })
			case "policy_revision":
				f.change(func(f *vodTestState) {
					f.states["account"] = vodAccountApplicability{mode: vodExplicitInactive, revision: "v2"}
				})
			case "profile_changed":
				f.change(func(f *vodTestState) { v := f.profiles["profile"]; v.IsKidsProfile = true; f.profiles[v.ID] = v })
			case "expired":
				f.change(func(f *vodTestState) { f.session.ExpiresAt = time.Now().Add(-time.Second) })
			case "renewed":
				f.change(func(f *vodTestState) { f.session.ExpiresAt = f.session.ExpiresAt.Add(time.Hour) })
			case "cancelled":
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			}
			r.URL.RawQuery = q.Encode()
			old.session.mu.RLock()
			access := old.session.LastAccess
			old.session.mu.RUnlock()
			w := zeroRestartHTTP(h, r)
			old.session.mu.RLock()
			same := old.session.LastAccess.Equal(access)
			old.session.mu.RUnlock()
			if w.Code != 403 || p.calls.Load() != 1 || !same {
				t.Fatalf("denial touched/reopened %d calls%d touch%v", w.Code, p.calls.Load(), !same)
			}
		})
	}
}
func TestHLSZeroRestartConcurrentReservationOnlyOpensOneSuccessor(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	var wg sync.WaitGroup
	codes := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); codes <- zeroRestartHTTP(h, zeroRestartRequest(f, old.id)).Code }()
	}
	wg.Wait()
	close(codes)
	success := 0
	for code := range codes {
		if code == 200 {
			success++
		} else if code != 403 && code != 404 {
			t.Fatalf("unexpected concurrent status%d", code)
		}
	}
	if success != 1 || p.calls.Load() != 2 {
		t.Fatalf("successors=%d inputs%d", success, p.calls.Load())
	}
	fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
	close(p.bodies[1].gate)
	assertZeroRestartMedia(t, fresh)
}
func TestHLSZeroRestartCurrentSourceACLRevokedPreventsNewGET(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	repo := &fakeLibraryAccessRepo{policies: map[string]models.LibraryAccessPolicy{"library": {LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"profile"}}}}
	access := libraryaccess.New(repo, fakeLibraryAccessLocalItems{item: &models.LocalMediaItem{ID: "item", LibraryID: "library"}}, nil)
	h.SetLibraryAccessService(access)
	h.SetUsersService(fakeLocalMediaUsersProvider{user: models.User{ID: "profile", AccountID: "account"}, userOK: true})
	r := managedHLSRequest(f)
	q := r.URL.Query()
	q.Set("path", "localmedia:item/media.ts")
	r.URL.RawQuery = q.Encode()
	w := httptest.NewRecorder()
	h.StartHLSSession(w, r)
	if w.Code != 200 {
		t.Fatalf("initial source denied%d %s", w.Code, w.Body.String())
	}
	var v struct {
		ID string `json:"sessionId"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &v)
	if err := access.Set(t.Context(), models.LibraryAccessPolicy{LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"foreign"}}); err != nil {
		t.Fatal(err)
	}
	denied := zeroRestartHTTP(h, zeroRestartRequest(f, v.ID))
	if denied.Code != 403 || p.calls.Load() != 1 {
		t.Fatal("revoked source started another input")
	}
}

func hlsZeroRestartOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	h, f, _ := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	r := zeroRestartRequest(f, old.id)
	w := zeroRestartHTTP(h, r)
	if w.Code != 200 {
		t.Fatalf("actual zero-restart setup denied%d %s", w.Code, w.Body.String())
	}
	if err := v.ValidateRequest(r, nil); err != nil {
		t.Fatal(err)
	}
	if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
	r = zeroRestartRequest(f, old.id)
	w = zeroRestartHTTP(h, r)
	if w.Code != 403 {
		t.Fatal("repeat restart not denied")
	}
	if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func TestHLSZeroRestartOldCheckCallbackCannotCancelSuccessorOwner(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	prior := f.boundary.identity.sessions
	enabled := atomic.Bool{}
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f.boundary.identity.sessions = rootKidsVODSessions(func(ctx context.Context, token string) (*models.Session, error) {
		if enabled.CompareAndSwap(true, false) {
			entered <- ctx
			<-release
		}
		return prior.Get(ctx, token)
	})
	old := zeroRestartStart(t, h, f)
	enabled.Store(true)
	checked := make(chan error, 1)
	go func() { checked <- old.input.check(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("old actual owner callback not reached")
	}
	opening := make(chan struct{})
	p.before = func(n int) error {
		if n == 2 {
			close(opening)
		}
		return nil
	}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- zeroRestartHTTP(h, zeroRestartRequest(f, old.id)) }()
	select {
	case <-old.input.ctx.Done():
	case <-time.After(2 * time.Second):
		unblock()
		t.Fatal("actual old input was not retired before callback return")
	}
	select {
	case <-opening:
		unblock()
	case <-time.After(250 * time.Millisecond):
		unblock()
	}
	w := <-result
	<-checked
	if w.Code != 200 || p.calls.Load() != 2 || old.input.owner.ctx.Err() != nil {
		t.Fatalf("stale old callback canceled shared successor: status%d inputs%d owner%v", w.Code, p.calls.Load(), old.input.owner.ctx.Err())
	}
	fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
	close(p.bodies[1].gate)
	assertZeroRestartMedia(t, fresh)
}

func TestHLSZeroRestartStaleRawRegistrationIsLocalButRawWrapperStillRetires(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 200 {
		t.Fatal("restart failed")
	}
	fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
	stale := ctxWithHLSRaw(old.input.owner.ctx, old.input.raw)
	if !errors.Is(h.checkVODRawOwner(stale), errVODRawStaleRegistration) || fresh.input.owner.ctx.Err() != nil {
		t.Fatal("non-destructive stale check retired successor")
	}
	if h.vodRawCheck(stale) == nil {
		t.Fatal("legacy raw wrapper allowed stale registration")
	}
	select {
	case <-fresh.input.owner.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("legacy raw wrapper lost release semantics")
	}
	waitZeroRestartDone(t, fresh.input)
	if p.calls.Load() != 2 {
		t.Fatal("stale raw check opened input")
	}
}

func TestHLSZeroRestartRevokedOriginalUnlockAfterJoinDeniesBeforeNewGET(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	prior := f.boundary.pin
	var once sync.Once
	f.boundary.pin = func(w http.ResponseWriter, r *http.Request, id string) bool {
		allowed := prior(w, r, id)
		if allowed {
			select {
			case <-old.input.done:
				once.Do(func() { f.unlocks.RevokeProfile("profile") })
			default:
			}
		}
		return allowed
	}
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 403 || p.calls.Load() != 1 {
		t.Fatalf("revoked original PIN authorization opened successor: status%d inputs%d", w.Code, p.calls.Load())
	}
}

func TestHLSZeroRestartFreshUnlockCannotReplaceRevokedOrExpiredOriginalBeforeCancel(t *testing.T) {
	for _, mode := range []string{"revoked", "expired"} {
		t.Run(mode, func(t *testing.T) {
			h, f, p := zeroRestartFixture(t)
			f.unlocks.RevokeProfile("profile")
			ttl := time.Duration(0)
			if mode == "expired" {
				ttl = time.Second
			}
			token, expires, err := f.unlocks.Issue(profileunlock.IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: "account", ProfileID: "profile", PinFingerprint: "pin-v1", TTL: ttl})
			if err != nil {
				t.Fatal(err)
			}
			f.token = token
			old := zeroRestartStart(t, h, f)
			if mode == "revoked" {
				f.unlocks.RevokeProfile("profile")
			} else {
				timer := time.NewTimer(time.Until(expires) + 10*time.Millisecond)
				defer timer.Stop()
				<-timer.C
			}
			fresh, _, err := f.unlocks.Issue(profileunlock.IssueRequest{SessionToken: f.session.Token, SessionExpiresAt: f.session.ExpiresAt, AccountID: "account", ProfileID: "profile", PinFingerprint: "pin-v1"})
			if err != nil {
				t.Fatal(err)
			}
			r := zeroRestartRequest(f, old.id)
			r.Header.Set(profileunlock.Header, fresh)
			if !f.boundary.pin(&vodDiscardWriter{header: make(http.Header)}, r, "profile") {
				t.Fatal("fresh caller unlock control is invalid")
			}
			old.session.mu.RLock()
			access := old.session.LastAccess
			old.session.mu.RUnlock()
			w := zeroRestartHTTP(h, r)
			old.session.mu.RLock()
			same := old.session.LastAccess.Equal(access)
			old.session.mu.RUnlock()
			if w.Code != 403 || p.calls.Load() != 1 || !same || old.input.ctx.Err() != nil || p.bodies[0].closes.Load() != 0 {
				t.Fatalf("renewed caller replaced original PIN or canceled original: %d inputs%d child%v closes%d", w.Code, p.calls.Load(), old.input.ctx.Err(), p.bodies[0].closes.Load())
			}
		})
	}
}

func TestHLSZeroRestartRetainsActualPrivateSelectionInsteadOfLaterSessionMetadata(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	old.input.mu.Lock()
	actual := old.input.audioIndex
	old.input.mu.Unlock()
	old.session.mu.Lock()
	old.session.AudioTrackIndex = 9999
	old.session.mu.Unlock()
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 200 {
		t.Fatalf("private original selection replaced by metadata: %d %s", w.Code, w.Body.String())
	}
	fresh, _ := h.hlsManager.snapshotHLSSession(old.id)
	fresh.input.mu.Lock()
	chosen := fresh.input.audioIndex
	fresh.input.mu.Unlock()
	if chosen != actual || p.calls.Load() != 2 {
		t.Fatal("restart changed actual captured track")
	}
	close(p.bodies[1].gate)
	assertZeroRestartMedia(t, fresh)
}

func TestHLSZeroRestartOriginalInvalidationAfterReservationCleansHeldJoinedInput(t *testing.T) {
	h, f, p := zeroRestartFixture(t)
	old := zeroRestartStart(t, h, f)
	prior := f.boundary.pin
	var pins atomic.Int32
	f.boundary.pin = func(w http.ResponseWriter, r *http.Request, id string) bool {
		allowed := prior(w, r, id)
		if pins.Add(1) == 3 {
			f.boundary.invalidateProfile("profile")
			waitZeroRestartDone(t, old.input)
			old.input.releaseRetired()
		}
		return allowed
	}
	w := zeroRestartHTTP(h, zeroRestartRequest(f, old.id))
	if w.Code != 403 || p.calls.Load() != 1 {
		t.Fatal("invalidated reserved original opened input")
	}
	f.boundary.mu.Lock()
	_, leaked := f.boundary.active[old.input.raw.key]
	f.boundary.mu.Unlock()
	if leaked {
		t.Fatal("already-joined held original left active registration")
	}
	if _, ok := h.hlsManager.snapshotHLSSession(old.id); ok {
		t.Fatal("already-joined invalid original remained mapped")
	}
	if _, err := os.Stat(old.output); !os.IsNotExist(err) {
		t.Fatal("already-joined invalid original output leaked")
	}
}
