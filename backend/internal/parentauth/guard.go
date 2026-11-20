// Package parentauth requires fresh account credentials for parental controls.
package parentauth

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/models"
)

const PasswordHeader = "X-Account-Password"
const RequiredCode = "parent_authorization_required"

type Accounts interface {
	Get(id string) (models.Account, bool)
	Authenticate(username, password string) (models.Account, error)
}

type Guard struct {
	accounts Accounts
	mu       sync.Mutex
	attempts map[string]*attemptBudget
}

// attemptBudget limits wrong passwords per account. Checks for one account run
// one at a time so concurrent guesses cannot overspend the budget.
type attemptBudget struct {
	gate    chan struct{}
	limiter *rate.Limiter
}

func New(accounts Accounts) *Guard {
	return &Guard{accounts: accounts, attempts: make(map[string]*attemptBudget)}
}

// Require accepts the current account's password for this mutation. The
// master (host) account is held to the same rule as every other account: a
// signed-in session, master or not, is not parental proof, and neither is a
// child's own PIN. No password or successful authorization is retained after
// the request.
func (g *Guard) Require(w http.ResponseWriter, r *http.Request) bool {
	return g.require(w, r, false)
}

// RequireFresh always confirms the current account password, including admins.
// It requires a context-aware verifier and does not install a route or policy.
// Callers remain responsible for current session, profile and operation checks.
func (g *Guard) RequireFresh(w http.ResponseWriter, r *http.Request) bool {
	return g.require(w, r, true)
}

type contextAccounts interface {
	AuthenticateContext(context.Context, string, string) (models.Account, error)
}

func (g *Guard) require(w http.ResponseWriter, r *http.Request, fresh bool) bool {
	if auth.GetSessionScope(r) != "" {
		apierror.Write(w, http.StatusForbidden, apierror.CodeForbidden, "session not permitted for this endpoint")
		return false
	}
	accountID := auth.GetAccountID(r)
	password := r.Header.Get(PasswordHeader)
	deny := func() bool {
		apierror.Write(w, http.StatusForbidden, RequiredCode, "confirm the account password to manage parental controls")
		return false
	}
	if g == nil || g.accounts == nil || accountID == "" || password == "" {
		return deny()
	}
	ctx := context.Background()
	var verifier contextAccounts
	if fresh {
		ctx = r.Context()
		verifier, _ = g.accounts.(contextAccounts)
		if verifier == nil || ctx.Err() != nil {
			return deny()
		}
	}
	// Browser headers cannot carry arbitrary UTF-8 or surrounding whitespace.
	if strings.HasPrefix(password, "base64:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(password, "base64:"))
		if err != nil {
			return deny()
		}
		password = string(decoded)
	}
	g.mu.Lock()
	budget := g.attempts[accountID]
	if budget == nil {
		budget = &attemptBudget{gate: make(chan struct{}, 1), limiter: rate.NewLimiter(rate.Every(12*time.Second), 10)}
		g.attempts[accountID] = budget
	}
	g.mu.Unlock()
	select {
	case budget.gate <- struct{}{}:
		defer func() { <-budget.gate }()
	case <-ctx.Done():
		return deny()
	}
	if ctx.Err() != nil {
		return deny()
	}
	// Only wrong passwords spend the budget, so a parent editing several
	// settings in a row is never throttled; guessing is still limited.
	if budget.limiter.Tokens() < 1 {
		w.Header().Set("Retry-After", "12")
		apierror.Write(w, http.StatusTooManyRequests, apierror.CodeRateLimited, "too many password attempts")
		return false
	}
	account, ok := g.accounts.Get(accountID)
	if !ok || fresh && (ctx.Err() != nil || account.ID != accountID || account.IsExpired()) {
		return deny()
	}
	var verified models.Account
	var err error
	if fresh {
		verified, err = verifier.AuthenticateContext(ctx, account.Username, password)
	} else {
		verified, err = g.accounts.Authenticate(account.Username, password)
	}
	if ctx.Err() != nil {
		return deny()
	}
	if err != nil || verified.ID != accountID {
		budget.limiter.Allow()
		return deny()
	}
	if fresh && verified.IsExpired() {
		return deny()
	}
	return true
}
