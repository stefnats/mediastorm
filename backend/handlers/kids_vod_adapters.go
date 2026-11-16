package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/kidsvod"
)

// These adapters are deliberately not installed by Main or any HTTP route.
// Their repositories read current server state; no session validator, provider,
// metadata hydrator or input opener is a dependency.
type kidsVODProfileRepository interface {
	Get(context.Context, string) (*models.User, error)
}
type kidsVODAccountRepository interface {
	Get(context.Context, string) (*models.Account, error)
}
type kidsVODSessionRepository interface {
	Get(context.Context, string) (*models.Session, error)
}
type kidsVODIdentity struct {
	profiles kidsVODProfileRepository
	accounts kidsVODAccountRepository
	sessions kidsVODSessionRepository
	now      func() time.Time
}

var errKidsVODCurrentState = errors.New("current VOD authorization state unavailable")

func (i *kidsVODIdentity) CurrentProfile(ctx context.Context, id string) (kidsvod.Profile, error) {
	if i == nil || i.profiles == nil || i.accounts == nil || i.now == nil || ctx.Err() != nil {
		return kidsvod.Profile{}, errKidsVODCurrentState
	}
	p, err := i.profiles.Get(ctx, id)
	if err != nil || p == nil || p.ID != id {
		return kidsvod.Profile{}, errKidsVODCurrentState
	}
	a, err := i.accounts.Get(ctx, p.AccountID)
	if err != nil || a == nil || a.ID != p.AccountID || a.ExpiresAt != nil && !a.ExpiresAt.After(i.now()) || ctx.Err() != nil {
		return kidsvod.Profile{}, errKidsVODCurrentState
	}
	return kidsvod.Profile{ID: p.ID, AccountID: p.AccountID, IsKids: p.IsKidsProfile, Revision: kidsVODHash([]any{p, p.PinHash, a.ID, a.IsMaster, a.ExpiresAt, a.UpdatedAt})}, nil
}
func (i *kidsVODIdentity) CurrentSession(ctx context.Context, token string) (models.Session, error) {
	if i == nil || i.accounts == nil || i.sessions == nil || i.now == nil || token == "" || ctx.Err() != nil {
		return models.Session{}, errKidsVODCurrentState
	}
	s, err := i.sessions.Get(ctx, token)
	if err != nil || s == nil || s.Token != token || !s.ExpiresAt.After(i.now()) {
		return models.Session{}, errKidsVODCurrentState
	}
	a, err := i.accounts.Get(ctx, s.AccountID)
	if err != nil || a == nil || a.ID != s.AccountID || a.ExpiresAt != nil && !a.ExpiresAt.After(i.now()) || ctx.Err() != nil {
		return models.Session{}, errKidsVODCurrentState
	}
	copy := *s
	copy.IsMaster = a.IsMaster
	if a.ExpiresAt != nil && a.ExpiresAt.Before(copy.ExpiresAt) {
		copy.ExpiresAt = *a.ExpiresAt
	}
	return copy, nil
}

// A future public boundary must configure the existing real PIN checker here.
// CheckResource alone can intentionally be permissive without its middleware;
// this bridge therefore requires a separate mandatory server guard as well.
type kidsVODHTTPBridge struct {
	core     *kidsvod.Service
	identity *kidsVODIdentity
	prequeue *PrequeueHandler
	pin      func(http.ResponseWriter, *http.Request, string) bool
}

func (b *kidsVODHTTPBridge) resolve(w http.ResponseWriter, r *http.Request, profile string, ref kidsvod.ResourceRef) (*kidsvod.Intent, kidsvod.Decision) {
	deny := func(code string) (*kidsvod.Intent, kidsvod.Decision) {
		apierror.Write(w, http.StatusForbidden, code, "VOD authorization unavailable")
		return nil, kidsvod.Decision{}
	}
	if b == nil || b.core == nil || b.identity == nil || b.pin == nil || r == nil {
		return deny(kidsvod.CodeAccessDenied)
	}
	resource := ""
	if ref.Kind() == kidsvod.Prequeue {
		if b.prequeue == nil || b.prequeue.store == nil {
			return deny(kidsvod.CodeClassificationUnknown)
		}
		entry, _, ok := b.prequeue.store.SnapshotForAuthorization(ref.ID())
		if !ok || entry.UserID == "" {
			return deny(kidsvod.CodeClassificationUnknown)
		}
		if profile != "" && profile != entry.UserID {
			return deny(kidsvod.CodeAccessDenied)
		}
		profile = entry.UserID
		resource = entry.StreamPath
	}
	if profile == "" || strings.TrimSpace(profile) != profile {
		return deny(kidsvod.CodeProfileRequired)
	}
	// All selectors supplied by the caller must agree with the stored/explicit
	// owner. The caller decoding a body passes its profile as the argument above.
	for _, key := range []string{"userId", "profileId"} {
		for _, value := range r.URL.Query()[key] {
			if value != "" && value != profile {
				return deny(kidsvod.CodeAccessDenied)
			}
		}
	}
	for _, key := range []string{"userID", "profileID"} {
		if value := mux.Vars(r)[key]; value != "" && value != profile {
			return deny(kidsvod.CodeAccessDenied)
		}
	}
	session, ok := auth.GetSession(r)
	if !ok || auth.GetAccountID(r) != session.AccountID || session.Scope != "" {
		return deny(kidsvod.CodeAccessDenied)
	}
	current, err := b.identity.CurrentSession(r.Context(), session.Token)
	if err != nil || current.AccountID != session.AccountID || current.Scope != "" || current.IsMaster != auth.IsMaster(r) {
		return deny(kidsvod.CodeAccessDenied)
	}
	owner, err := b.identity.CurrentProfile(r.Context(), profile)
	if err != nil || owner.AccountID != current.AccountID && !current.IsMaster {
		return deny(kidsvod.CodeAccessDenied)
	}
	if !profileaccess.CheckResource(w, r, profile, resource) || !b.pin(w, r, profile) {
		return nil, kidsvod.Decision{}
	}
	unchanged := func() bool {
		latestOwner, err := b.identity.CurrentProfile(r.Context(), profile)
		if err != nil || latestOwner != owner {
			return false
		}
		latestSession, err := b.identity.CurrentSession(r.Context(), session.Token)
		return err == nil && latestSession == current && r.Context().Err() == nil
	}
	// Do not attach a newly read PIN/profile revision to an older successful
	// unlock check, including a change while the core reads its dependencies.
	if !unchanged() {
		return deny(kidsvod.CodeContextChanged)
	}
	intent, decision := b.core.ResolveRequest(r, profile, ref)
	if !unchanged() {
		return deny(kidsvod.CodeContextChanged)
	}
	return intent, decision
}

func (b *kidsVODHTTPBridge) openLease(w http.ResponseWriter, r *http.Request, profile string, ref kidsvod.ResourceRef, grant *kidsvod.Grant) (*kidsvod.Lease, kidsvod.Decision) {
	intent, decision := b.resolve(w, r, profile, ref)
	if intent == nil {
		return nil, decision
	}
	return b.core.OpenLease(r.Context(), intent, grant)
}

func (b *kidsVODHTTPBridge) openBoundLease(w http.ResponseWriter, r *http.Request, profile string, ref kidsvod.ResourceRef, grant *kidsvod.Grant) (*kidsVODBoundLease, kidsvod.Decision) {
	lease, decision := b.openLease(w, r, profile, ref, grant)
	if lease == nil {
		return nil, decision
	}
	if b.prequeue == nil || b.prequeue.kidsVODBindings == nil {
		lease.Close()
		return nil, kidsvod.Decision{}
	}
	resources := b.prequeue.kidsVODBindings
	entry, _, ok := b.prequeue.store.SnapshotForAuthorization(ref.ID())
	if !ok {
		lease.Close()
		return nil, kidsvod.Decision{}
	}
	account := auth.GetAccountID(r)
	profile = entry.UserID
	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	session, err := b.identity.CurrentSession(r.Context(), mustKidsVODToken(r))
	if err != nil {
		lease.Close()
		return nil, kidsvod.Decision{}
	}
	if session.ClientID != "" {
		client = session.ClientID
	}
	current := func(ctx context.Context) (kidsVODInputKey, error) {
		binding, err := resources.resolveOwner(ctx, account, profile, client, ref)
		if err != nil {
			return kidsVODInputKey{}, err
		}
		resources.mu.Lock()
		receipt, ok := resources.receipts[ref.ID()]
		resources.mu.Unlock()
		if !ok {
			return kidsVODInputKey{}, errKidsVODInput
		}
		return kidsVODInputKey{ref: ref, attempt: receipt.stamp, inputVersion: receipt.proof.inputVersion, sourceVersion: binding.SourceVersion}, nil
	}
	key, err := current(r.Context())
	if err != nil {
		lease.Close()
		return nil, kidsvod.Decision{}
	}
	return &kidsVODBoundLease{lease: lease, key: key, current: current}, decision
}
func mustKidsVODToken(r *http.Request) string { session, _ := auth.GetSession(r); return session.Token }
