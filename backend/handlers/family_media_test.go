package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/profileunlock"
)

// Internal controlled IO owner only. Public allowance is tested separately
// through the actual PG facade; this fake cannot enter the public installer.
type familyHTTPTestOwner struct {
	ctx     context.Context
	cancel  context.CancelFunc
	expires time.Time
	mode    profileunlock.FamilyMediaMode
	changed atomic.Bool
	closed  atomic.Int32
	checks  atomic.Int32
	once    sync.Once
	handed  atomic.Bool
}

func newFamilyHTTPTestOwner() *familyHTTPTestOwner {
	expires := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), expires)
	return &familyHTTPTestOwner{ctx: ctx, cancel: cancel, expires: expires, mode: profileunlock.FamilyMediaInactive}
}
func (o *familyHTTPTestOwner) Check(ctx context.Context) error {
	o.checks.Add(1)
	if ctx == nil || ctx.Err() != nil || o.ctx.Err() != nil || o.changed.Load() {
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}
func (o *familyHTTPTestOwner) CheckRequest(r *http.Request, _ string) error {
	if r == nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	return o.Check(r.Context())
}
func (o *familyHTTPTestOwner) Handoff(r *http.Request) error {
	if o.CheckRequest(r, "") != nil || !o.handed.CompareAndSwap(false, true) {
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}
func (o *familyHTTPTestOwner) Context() context.Context            { return o.ctx }
func (o *familyHTTPTestOwner) ExpiresAt() time.Time                { return o.expires }
func (o *familyHTTPTestOwner) Mode() profileunlock.FamilyMediaMode { return o.mode }
func (o *familyHTTPTestOwner) Close()                              { o.once.Do(func() { o.cancel(); o.closed.Add(1) }) }
func familyHTTPControlled(t *testing.T) (*vodTestState, *FamilyMediaBoundary, *familyHTTPTestOwner) {
	t.Helper()
	state := newVODTestState(t, vodExplicitInactive, false)
	owner := newFamilyHTTPTestOwner()
	t.Cleanup(owner.Close)
	b := newFamilyMediaBoundary(state.boundary.identity, state, func(*http.Request, string) (vodOriginalOwner, error) { return owner, nil })
	return state, b, owner
}
func familyHTTPRequest(t *testing.T) (*FamilyMediaRequest, *familyHTTPTestOwner) {
	t.Helper()
	state, b, owner := familyHTTPControlled(t)
	request, ok := b.Begin(httptest.NewRecorder(), state.request("GET", "/unregistered?profileId=profile", ""), "")
	if !ok {
		t.Fatal("controlled internal owner not adopted")
	}
	t.Cleanup(request.Close)
	return request, owner
}

func TestFamilyHTTPRejectsAmbiguousSelectorsBeforeAnyCapture(t *testing.T) {
	state, b, _ := familyHTTPControlled(t)
	calls := atomic.Int32{}
	b.capture = func(*http.Request, string) (vodOriginalOwner, error) {
		calls.Add(1)
		return nil, errors.New("must not capture")
	}
	for _, tc := range []struct{ url, body, target string }{
		{"/?profileId=profile&profileId=profile", "", ""}, {"/?ProfileId=profile", "", ""}, {"/?profileId=profile&userId=other", "", ""}, {"/?profileId=", "", ""},
		{"/", `{"profileId":"profile","profileId":"profile"}`, ""}, {"/", `{"ProfileId":"profile"}`, ""}, {"/", `{"profileId":null}`, ""}, {"/", `{"profileId":"profile","userId":"other"}`, ""},
		{"/", `{"profileId":"profile"} {}`, ""}, {"/", strings.Repeat(" ", 1<<20) + "{}", ""}, {"/?profileId=profile", "", "other"},
		{"/?profileId=profile&profileName=other", "", ""}, {"/?ProfileName=other", "", ""},
		{"/", `{"profileId":"profile","profileName":"other"}`, ""}, {"/", `{"profileId":"profile","PROFILEname":"other"}`, ""},
		{"/", `{"profileId":"profile","\u0070rofileName":"other"}`, ""},
	} {
		if q, ok := b.Begin(httptest.NewRecorder(), state.request("POST", tc.url, tc.body), tc.target); ok || q != nil {
			if q != nil {
				q.Close()
			}
			t.Fatal("ambiguous selector entered owner capture")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid request read PG capture")
	}
}
func TestFamilyHTTPPreservesOriginalBodyAndCapturesBeforeSemanticReaders(t *testing.T) {
	state, b, owner := familyHTTPControlled(t)
	original := state.request("POST", "/?profileId=profile", `{"profileId":"profile","source":"synthetic","caption":"profileName remains ordinary text"}`)
	expected := original.Context()
	captured := false
	reader := state.boundary.identity.sessions
	state.boundary.identity.sessions = rootKidsVODSessions(func(ctx context.Context, token string) (*models.Session, error) {
		if !captured {
			t.Error("semantic reader preceded admitted original capture")
		}
		return reader.Get(ctx, token)
	})
	b.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
		if r.Context() != expected || target != "profile" {
			t.Error("original request replaced with expiring semantic check context")
		}
		captured = true
		privateBody, err := io.ReadAll(r.Body)
		if err != nil || string(privateBody) != `{"profileId":"profile","source":"synthetic","caption":"profileName remains ordinary text"}` {
			t.Error("private selector cursor changed the captured body")
		}
		return owner, nil
	}
	q, ok := b.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("valid original capture denied")
	}
	defer q.Close()
	raw, err := io.ReadAll(original.Body)
	if err != nil || string(raw) != `{"profileId":"profile","source":"synthetic","caption":"profileName remains ordinary text"}` {
		t.Fatal("selector peek consumed original production decoder")
	}
	if q.Check(q.Context()) != nil {
		t.Fatal("semantic500ms cancel retired original owner")
	}
	if background, err := b.boundary.backgroundOwner(context.Background(), "profile", "device"); err == nil || background != nil {
		if background != nil {
			background.release()
		}
		t.Fatal("original-owner factory admitted an unbound background fallback")
	}
}
func TestFamilyHTTPUnknownAndFailedCaptureNeverEnterSemanticOrSourceCallbacks(t *testing.T) {
	for _, mode := range []string{"unknown", "error", "nil"} {
		state, b, owner := familyHTTPControlled(t)
		state.boundary.identity.sessions = rootKidsVODSessions(func(context.Context, string) (*models.Session, error) {
			t.Error("unknown capture entered semantic reader")
			return nil, errors.New("unexpected")
		})
		b.capture = func(*http.Request, string) (vodOriginalOwner, error) {
			switch mode {
			case "unknown":
				owner.mode = profileunlock.FamilyMediaUnknown
				return owner, nil
			case "error":
				return owner, errors.New("synthetic private source detail")
			default:
				return nil, nil
			}
		}
		w := httptest.NewRecorder()
		if q, ok := b.Begin(w, state.request("GET", "/?profileId=profile", ""), ""); ok || q != nil {
			t.Fatal("unknown original owner became fallback")
		}
		if strings.Contains(w.Body.String(), "private source detail") {
			t.Fatal("private error exposed")
		}
	}
}
func TestFamilyHTTPUsesOriginalPortWithoutSyntheticPINOrModePromotion(t *testing.T) {
	state, b, owner := familyHTTPControlled(t)
	state.change(func(s *vodTestState) {
		s.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "active"}
	})
	owner.mode = profileunlock.FamilyMediaActiveAdult
	claim := b.boundary.original
	var originalRequest *http.Request
	b.boundary.original = func(r *http.Request, target string) (vodOriginalOwner, error) {
		originalRequest = r
		return claim(r, target)
	}
	q, ok := b.Begin(httptest.NewRecorder(), state.request("GET", "/?profileId=profile", ""), "")
	if !ok {
		t.Fatal("original active owner not adopted")
	}
	defer q.Close()
	if state.pinCalls.Load() != 0 {
		t.Fatal("original proof replaced with legacy PIN callback")
	}
	if q.owner.request != nil {
		t.Fatal("original PG owner fabricated an authenticated background request")
	}
	if duplicate, e := claim(originalRequest, "profile"); duplicate != nil || e == nil {
		t.Fatal("original owner acquired two cleanup owners")
	}
	if owner.closed.Load() != 0 || q.Check(q.Context()) != nil {
		t.Fatal("duplicate private claim retired valid original owner")
	}
	owner.changed.Store(true)
	if q.Check(context.Background()) == nil {
		t.Fatal("same public DTO fields promoted changed original proof")
	}
}
func TestFamilyHTTPImportedAndUnavailableObjectsCannotOpenOrAdopt(t *testing.T) {
	var imported FamilyMediaRequest
	if json.Unmarshal([]byte(`{}`), &imported) == nil {
		t.Fatal("imported HTTP authority")
	}
	if raw, err := json.Marshal(&imported); err != nil || string(raw) != "null" || fmt.Sprintf("%+v", &imported) != "FamilyMediaRequest(opaque)" {
		t.Fatal("opaque HTTP owner exposed")
	}
	calls := 0
	if body, err := imported.OpenBody(func(context.Context) (io.ReadCloser, error) { calls++; return nil, nil }); err == nil || body != nil || calls != 0 {
		t.Fatal("imported owner invoked opener")
	}
	if imported.Handoff(httptest.NewRequest("GET", "/", nil)) == nil || imported.Check(context.Background()) == nil || imported.Context().Err() == nil {
		t.Fatal("imported owner became live")
	}
	var b *FamilyMediaBoundary
	w := httptest.NewRecorder()
	if q, ok := b.Begin(w, httptest.NewRequest("GET", "/", nil), ""); q != nil || ok || w.Code != 503 {
		t.Fatal("missing boundary became legacy allow")
	}
	q, owner := familyHTTPRequest(t)
	copied := *q
	for _, value := range []any{q, *q, copied, &copied} {
		if fmt.Sprintf("%+v", value) != "FamilyMediaRequest(opaque)" || fmt.Sprintf("%#v", value) != "FamilyMediaRequest(opaque)" {
			t.Fatal("value formatting exposed original HTTP authority")
		}
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("value JSON exposed original HTTP authority")
		}
	}
	if copied.Check(context.Background()) == nil {
		t.Fatal("value copy borrowed original HTTP owner")
	}
	copied.Close()
	if owner.closed.Load() != 0 || q.Check(q.Context()) != nil {
		t.Fatal("copy retirement changed original HTTP owner")
	}
}
func TestFamilyHTTPFreshConsumerRechecksAllSelectorsWithoutRetiringOriginal(t *testing.T) {
	state, b, _ := familyHTTPControlled(t)
	q, ok := b.Begin(httptest.NewRecorder(), state.request("GET", "/?profileId=profile", ""), "")
	if !ok {
		t.Fatal("original owner denied")
	}
	defer q.Close()
	for _, r := range []*http.Request{
		state.request("GET", "/?profileId=other", ""),
		state.request("GET", "/?profileId=profile&profileId=profile", ""),
		mux.SetURLVars(state.request("GET", "/?profileId=profile", ""), map[string]string{"userID": "other"}),
		state.request("POST", "/", `{"profileId":"other"}`),
		state.request("GET", "/?profileId=profile&profileName=other", ""),
		state.request("POST", "/", `{"profileId":"profile","ProfileName":"other"}`),
	} {
		if q.CheckRequest(r, "profile") == nil {
			t.Fatal("fresh consumer selected another profile while borrowing original owner")
		}
	}
	if q.Check(q.Context()) != nil {
		t.Fatal("foreign consumer retired valid original owner")
	}
}
