package parentauth

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/accounts"
)

type freshAccounts struct {
	get    func(string) (models.Account, bool)
	verify func(context.Context, string, string) (models.Account, error)
	legacy func(string, string) (models.Account, error)
}

func (a *freshAccounts) Get(id string) (models.Account, bool) {
	if a.get != nil {
		return a.get(id)
	}
	return models.Account{ID: id, Username: "parent"}, true
}
func (a *freshAccounts) Authenticate(user, password string) (models.Account, error) {
	if a.legacy != nil {
		return a.legacy(user, password)
	}
	return models.Account{}, errors.New("legacy authentication must not run")
}
func (a *freshAccounts) AuthenticateContext(ctx context.Context, user, password string) (models.Account, error) {
	return a.verify(ctx, user, password)
}
func freshRequest(ctx context.Context, master bool, scope, password string) *http.Request {
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "account-a")
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, master)
	ctx = context.WithValue(ctx, auth.ContextKeySession, models.Session{Scope: scope})
	r := httptest.NewRequest("PUT", "/synthetic-parent-operation", nil).WithContext(ctx)
	r.Header.Set(PasswordHeader, password)
	return r
}

func TestFreshParentPasswordRequiresNewProofForMaster(t *testing.T) {
	for _, tc := range []struct {
		name, password, scope    string
		master, foreign, expired bool
		want                     bool
	}{
		{name: "parent", password: "synthetic password", want: true},
		{name: "master", master: true, password: "synthetic password", want: true},
		{name: "master without proof", master: true},
		{name: "master wrong proof", master: true, password: "wrong"},
		{name: "scoped master", master: true, scope: models.SessionScopeStream, password: "synthetic password"},
		{name: "other account", password: "synthetic password", foreign: true},
		{name: "expired proof", password: "synthetic password", expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			a := &freshAccounts{verify: func(ctx context.Context, user, password string) (models.Account, error) {
				calls++
				if user != "parent" || password != "synthetic password" {
					return models.Account{}, errors.New("wrong password")
				}
				verified := models.Account{ID: "account-a"}
				if tc.foreign {
					verified.ID = "account-b"
				}
				if tc.expired {
					past := time.Now().Add(-time.Hour)
					verified.ExpiresAt = &past
				}
				return verified, nil
			}}
			w := httptest.NewRecorder()
			allowed := New(a).RequireFresh(w, freshRequest(context.Background(), tc.master, tc.scope, tc.password))
			if allowed != tc.want || (w.Code == 200) != tc.want {
				t.Fatalf("allowed=%v status=%d", allowed, w.Code)
			}
			if tc.want && calls != 1 {
				t.Fatal("fresh proof was not verified exactly once")
			}
			if strings.Contains(w.Body.String(), "synthetic password") {
				t.Fatal("password leaked")
			}
		})
	}
}

func TestFreshParentFailsClosedWithoutContextVerifier(t *testing.T) {
	guard := New(&testAccounts{})
	if guard.RequireFresh(httptest.NewRecorder(), freshRequest(context.Background(), true, "", "fixture parent password")) {
		t.Fatal("legacy-only verifier accepted")
	}
	missing := httptest.NewRecorder()
	if guard.Require(missing, freshRequest(context.Background(), true, "", "")) || missing.Code != http.StatusForbidden {
		t.Fatal("master session bypassed the current account password rule")
	}
	confirmed := httptest.NewRecorder()
	if !guard.Require(confirmed, freshRequest(context.Background(), true, "", "fixture parent password")) || confirmed.Code != http.StatusOK {
		t.Fatal("ordinary guard rejected the master's current account password")
	}
	var unavailable *Guard
	if unavailable.RequireFresh(httptest.NewRecorder(), freshRequest(context.Background(), true, "", "synthetic password")) {
		t.Fatal("nil fresh guard accepted")
	}
}

func TestFreshParentOriginalContextAndLateSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := freshRequest(ctx, true, "", "base64:"+base64.StdEncoding.EncodeToString([]byte("synthetic パスワード")))
	calls := 0
	a := &freshAccounts{verify: func(got context.Context, user, password string) (models.Account, error) {
		calls++
		if got != r.Context() || password != "synthetic パスワード" {
			t.Error("request or password changed")
		}
		cancel()
		return models.Account{ID: "account-a"}, nil
	}}
	if New(a).RequireFresh(httptest.NewRecorder(), r) || calls != 1 {
		t.Fatal("cancelled late verification succeeded")
	}
}

func TestFreshParentCancelledWaitDoesNotBlockOrSpendBudget(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	a := &freshAccounts{legacy: func(user, password string) (models.Account, error) {
		close(entered)
		<-release
		return models.Account{ID: "account-a"}, nil
	}, verify: func(ctx context.Context, user, password string) (models.Account, error) {
		return models.Account{}, errors.New("wrong")
	}}
	g := New(a)
	first := make(chan bool, 1)
	go func() {
		first <- g.Require(httptest.NewRecorder(), freshRequest(context.Background(), false, "", "synthetic password"))
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("original verification did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		done <- g.RequireFresh(httptest.NewRecorder(), freshRequest(ctx, true, "", "synthetic password"))
	}()
	cancel()
	select {
	case allowed := <-done:
		if allowed {
			t.Fatal("cancelled waiter accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("fresh waiter blocked behind legacy verification")
	}
	once.Do(func() { close(release) })
	if !<-first {
		t.Fatal("original legacy verification failed")
	}
	for n := 0; n < 10; n++ {
		w := httptest.NewRecorder()
		g.RequireFresh(w, freshRequest(context.Background(), true, "", "wrong"))
		if w.Code != 403 {
			t.Fatalf("attempt%d status%d", n, w.Code)
		}
	}
	w := httptest.NewRecorder()
	g.RequireFresh(w, freshRequest(context.Background(), true, "", "wrong"))
	if w.Code != 429 {
		t.Fatalf("shared budget not enforced: %d", w.Code)
	}
}

func TestFreshParentActualAccountServiceAndNoRememberedProof(t *testing.T) {
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic parent password")
	service, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	master, ok := service.GetMasterAccount()
	if !ok {
		t.Fatal("master missing")
	}
	g := New(service)
	request := func(password string) *http.Request {
		r := freshRequest(context.Background(), true, "", password)
		return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, master.ID))
	}
	if g.RequireFresh(httptest.NewRecorder(), request("wrong")) {
		t.Fatal("wrong actual password accepted")
	}
	if !g.RequireFresh(httptest.NewRecorder(), request("synthetic parent password")) {
		t.Fatal("correct actual password rejected")
	}
	if g.RequireFresh(httptest.NewRecorder(), request("")) {
		t.Fatal("previous proof reused")
	}
}

func TestFreshAndLegacyGuessesSpendTheSameBudget(t *testing.T) {
	denied := func(string, string) (models.Account, error) { return models.Account{}, errors.New("wrong") }
	a := &freshAccounts{legacy: denied, verify: func(context.Context, string, string) (models.Account, error) {
		return denied("parent", "wrong")
	}}
	g := New(a)
	for attempt := 0; attempt < 10; attempt++ {
		w := httptest.NewRecorder()
		r := freshRequest(context.Background(), false, "", "wrong")
		if attempt%2 == 0 {
			g.Require(w, r)
		} else {
			g.RequireFresh(w, r)
		}
		if w.Code != 403 {
			t.Fatalf("attempt %d returned %d", attempt, w.Code)
		}
	}
	for _, fresh := range []bool{false, true} {
		w := httptest.NewRecorder()
		r := freshRequest(context.Background(), false, "", "wrong")
		if fresh {
			g.RequireFresh(w, r)
		} else {
			g.Require(w, r)
		}
		if w.Code != 429 || w.Header().Get("Retry-After") != "12" {
			t.Fatal("mixed guesses did not share one limit")
		}
	}
}
