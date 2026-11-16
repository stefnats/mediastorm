package handlers

import (
	"context"
	"errors"
	"io"
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
	"novastream/services/kidsvod"
	"novastream/services/playback"
)

type kidsIOProfiles struct{}

func (kidsIOProfiles) CurrentProfile(context.Context, string) (kidsvod.Profile, error) {
	return kidsvod.Profile{ID: "child", AccountID: "account", IsKids: true, Revision: "profile-v1"}, nil
}

type kidsIOPolicies struct{}

func (kidsIOPolicies) CurrentPolicy(context.Context, string) (kidsvod.Policy, error) {
	age := 12
	return kidsvod.Policy{Enabled: true, Country: "DE", MaxMovieAge: &age, MaxEpisodeAge: &age, Revision: "policy-v1"}, nil
}

type kidsIOSessions struct{ session models.Session }

func (s kidsIOSessions) CurrentSession(context.Context, string) (models.Session, error) {
	return s.session, nil
}

type kidsIOResources struct{ binding kidsvod.Binding }

func (r kidsIOResources) ResolveCacheOnly(context.Context, kidsvod.Subject, kidsvod.ResourceRef) (kidsvod.Binding, error) {
	return r.binding, nil
}

type kidsIOAccounts struct{}

func (kidsIOAccounts) Get(id string) (models.Account, bool) {
	return models.Account{ID: id, Username: "synthetic-parent"}, id == "account"
}
func (kidsIOAccounts) Authenticate(user, password string) (models.Account, error) {
	if user != "synthetic-parent" || password != "synthetic-password" {
		return models.Account{}, errors.New("invalid synthetic credentials")
	}
	return models.Account{ID: "account"}, nil
}

// This fixture represents an independently trusted already-open server input.
// It deliberately does not claim that the current adult-only remote-library
// producer can issue a child binding, or install a public provider-open hook.
func kidsVODIOFixture(t *testing.T, ttl time.Duration) (*kidsvod.Service, *kidsVODBoundLease, *kidsVODInputAuthority) {
	t.Helper()
	store := playback.NewPrequeueStore(time.Hour)
	entry, _ := store.CreateScoped("tmdb:movie:41", "Synthetic", "child", "movie", 2024, nil, "details", "")
	stamp, _ := store.BeginAuthorizationAttempt(entry.ID)
	ref, _ := kidsvod.NewResourceRef(kidsvod.Prequeue, entry.ID, 0)
	now := time.Now()
	binding := kidsvod.Binding{Resource: ref, AccountID: "account", ProfileID: "child", CatalogID: "tmdb:movie:41", MediaType: "movie", SourceVersion: "file_v1_" + strings.Repeat("a", 64), Revision: "input-v1", ObservedAt: now, ExpiresAt: now.Add(ttl)}
	session := models.Session{Token: "synthetic-token", AccountID: "account", ExpiresAt: now.Add(time.Hour)}
	core, err := kidsvod.New(kidsvod.Dependencies{Profiles: kidsIOProfiles{}, Policies: kidsIOPolicies{}, Sessions: kidsIOSessions{session}, Resources: kidsIOResources{binding}, Parent: parentauth.New(kidsIOAccounts{})})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodGet, "/unregistered", nil)
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "account")
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
	r = r.WithContext(ctx)
	r.Header.Set(parentauth.PasswordHeader, "synthetic-password")
	grant, d := core.IssueParentException(httptest.NewRecorder(), r, "child", ref, time.Minute)
	if !d.Allowed() {
		t.Fatalf("real parent guard fixture denied: %s", d.Code())
	}
	intent, d := core.ResolveRequest(r, "child", ref)
	if intent == nil {
		t.Fatalf("fixture intent denied: %s", d.Code())
	}
	lease, d := core.OpenLease(context.Background(), intent, grant)
	if !d.Allowed() {
		t.Fatalf("fixture lease denied: %s", d.Code())
	}
	t.Cleanup(lease.Close)
	key := kidsVODInputKey{ref: ref, attempt: stamp, inputVersion: "server-handle-v1", sourceVersion: binding.SourceVersion}
	bound := &kidsVODBoundLease{lease: lease, key: key, current: func(context.Context) (kidsVODInputKey, error) { return key, nil }}
	authority, err := newKidsVODInputAuthority()
	if err != nil {
		t.Fatal(err)
	}
	return core, bound, authority
}

type kidsIOBody struct {
	reads, closes atomic.Int32
	read          func([]byte) (int, error)
	once          sync.Once
	closed        chan struct{}
}

func (b *kidsIOBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	if b.read != nil {
		return b.read(p)
	}
	return copy(p, "synthetic bytes"), io.EOF
}
func (b *kidsIOBody) Close() error {
	b.once.Do(func() {
		b.closes.Add(1)
		if b.closed != nil {
			close(b.closed)
		}
	})
	return nil
}

type kidsIOServerInput struct {
	key  kidsVODInputKey
	body io.ReadCloser
	used bool
}

func (s *kidsIOServerInput) consumeExistingInput() (kidsVODInputKey, io.ReadCloser, error) {
	if s.used {
		return kidsVODInputKey{}, nil, errKidsVODInput
	}
	s.used = true
	return s.key, s.body, nil
}
func kidsIOCapture(t *testing.T, a *kidsVODInputAuthority, b *kidsVODBoundLease, body io.ReadCloser) *kidsVODCapturedInput {
	t.Helper()
	input, err := a.capture(&kidsIOServerInput{key: b.key, body: body})
	if err != nil {
		t.Fatal(err)
	}
	return input
}

func TestKidsVODBoundReaderChecksBeforeAndAfterRead(t *testing.T) {
	for _, mode := range []string{"allowed", "revoked before", "revoked during", "input changed"} {
		t.Run(mode, func(t *testing.T) {
			core, bound, a := kidsVODIOFixture(t, time.Minute)
			body := &kidsIOBody{}
			input := kidsIOCapture(t, a, bound, body)
			reader, err := a.wrap(context.Background(), bound, input)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			switch mode {
			case "revoked before":
				core.InvalidateProfile("child")
			case "revoked during":
				body.read = func(p []byte) (int, error) {
					n := copy(p, "late secret bytes")
					core.InvalidateProfile("child")
					return n, nil
				}
			case "input changed":
				bound.current = func(context.Context) (kidsVODInputKey, error) {
					key := bound.key
					key.inputVersion = "replacement-handle"
					return key, nil
				}
			}
			buffer := make([]byte, 64)
			n, err := reader.Read(buffer)
			if mode == "allowed" {
				if n == 0 || err != io.EOF {
					t.Fatal("valid existing bytes unavailable")
				}
			} else {
				if n != 0 || err == nil || strings.Trim(string(buffer), "\x00") != "" {
					t.Fatalf("late bytes escaped: n=%d err=%v", n, err)
				}
				if mode != "revoked during" && body.reads.Load() != 0 {
					t.Fatal("denied reader touched input")
				}
			}
		})
	}
}
func TestKidsVODRevocationAndRequestCancellationUnblockOnlyOwnedReader(t *testing.T) {
	for _, cause := range []string{"revoke", "request cancelled", "binding expires"} {
		t.Run(cause, func(t *testing.T) {
			ttl := time.Minute
			if cause == "binding expires" {
				ttl = 80 * time.Millisecond
			}
			core, bound, a := kidsVODIOFixture(t, ttl)
			body := &kidsIOBody{closed: make(chan struct{})}
			entered := make(chan struct{})
			body.read = func(p []byte) (int, error) { close(entered); <-body.closed; return copy(p, "bytes after close"), nil }
			other := &kidsIOBody{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader, err := a.wrap(ctx, bound, kidsIOCapture(t, a, bound, body))
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				p := make([]byte, 32)
				n, err := reader.Read(p)
				if n != 0 || err == nil || strings.Trim(string(p), "\x00") != "" {
					t.Errorf("late read emitted bytes: %d %v", n, err)
				}
			}()
			<-entered
			if cause == "revoke" {
				core.InvalidateProfile("child")
			} else if cause == "request cancelled" {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("owned blocked reader was not cancelled")
			}
			if body.closes.Load() != 1 || other.closes.Load() != 0 {
				t.Fatal("wrong body ownership cleanup")
			}
		})
	}
}
func TestKidsVODCapturedHandleCannotBeRelabelledOrReplayed(t *testing.T) {
	_, bound, a := kidsVODIOFixture(t, time.Minute)
	other, _ := newKidsVODInputAuthority()
	body := &kidsIOBody{}
	input := kidsIOCapture(t, a, bound, body)
	if _, err := other.wrap(context.Background(), bound, input); err == nil {
		t.Fatal("foreign authority accepted handle")
	}
	wrong := *bound
	wrong.key.inputVersion = "other actual input"
	wrong.current = func(context.Context) (kidsVODInputKey, error) { return wrong.key, nil }
	if _, err := a.wrap(context.Background(), &wrong, input); err == nil {
		t.Fatal("expected receipt relabelled different actual input")
	}
	reader, err := a.wrap(context.Background(), bound, input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.wrap(context.Background(), bound, input); err == nil {
		t.Fatal("actual input claimed twice")
	}
	reader.Close()
	if _, err = reader.Read(make([]byte, 4)); err == nil || body.reads.Load() != 0 {
		t.Fatal("closed handle read")
	}
	if _, err = a.wrap(context.Background(), bound, &kidsVODCapturedInput{}); err == nil {
		t.Fatal("zero/imported input became authority")
	}
}
