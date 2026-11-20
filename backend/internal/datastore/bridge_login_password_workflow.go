package datastore

import (
	"context"
	"errors"
	"net/http"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/config"
)

// VerifyPasswordWorkflow selects credential work from one genuine policy and
// current private PG snapshot. It returns credential evidence, never a session.
// The caller must retain this exact already-bounded request for Commit/Release.
func (i *BridgeLoginSessionIssuer) VerifyPasswordWorkflow(r *http.Request, account, profile, client, raw string, original *config.BridgeLoginPolicyObservation) (proof *BridgeLoginCredentialProof, err error) {
	entered := time.Now()
	if !i.valid() {
		return nil, ErrBridgeSessionUnavailable
	}
	if r == nil || r.Context().Err() != nil {
		return nil, ErrBridgeCredentialChanged
	}
	requestContext := r.Context()
	fixedExpiry, bounded := requestContext.Deadline()
	if !bounded || !fixedExpiry.After(entered) || fixedExpiry.After(entered.Add(30*time.Second)) {
		return nil, ErrBridgeCredentialChanged
	}
	binding, err := bridgeCredentialRequestBinding(r, client)
	if err != nil || !parentMutationID(account) || !parentMutationID(profile) {
		return nil, ErrBridgeCredentialChanged
	}
	if len(raw) == 0 || len(raw) > 1024 {
		return nil, ErrBridgeCredentialRejected
	}
	hold, err := i.coordinator.TryProgressFileHold(requestContext)
	if err != nil {
		return nil, ErrBridgeSessionUnavailable
	}
	var intent *BridgeLoginCredentialIntent
	joinHold := func() {}
	defer func() {
		// Only signal callbacks call cancel. All joins are owned here, outside
		// credential operations, and precede native selection-hold retirement.
		if joinHold != nil {
			joinHold()
		}
		if err != nil && intent != nil {
			intent.Close()
		}
		if closeErr := hold.Close(); closeErr != nil && err == nil {
			err = ErrBridgeSessionChanged
		}
		// Closing the hold deliberately cancels its context. Check the original
		// request and effective proof lifetime after cleanup, not that dead hold.
		if proof != nil && (err != nil || !i.valid() || r.Context() != requestContext || requestContext.Err() != nil || !proof.state.matchesRequest(r) || proof.Context().Err() != nil || !proof.ExpiresAt().After(time.Now())) {
			proof.Close()
			proof = nil
			if err == nil {
				err = ErrBridgeCredentialChanged
			}
		}
	}()
	fence, err := i.security.BindBridgePolicy(requestContext, original, hold)
	if err != nil {
		return nil, ErrBridgeSessionChanged
	}
	values, err := fence.Values()
	if err != nil || !values.BridgeEnabled {
		return nil, ErrBridgeSessionUnavailable
	}
	intent, joinHold, err = i.store.prepareBridgePasswordWorkflow(r, requestContext, account, profile, client, binding, fixedExpiry, hold.Context())
	if err != nil {
		return nil, err
	}
	intent.state.mu.Lock()
	first := intent.state.snapshot // Immutable string/scalar/time DATA, not aliased mutable byte slices.
	evidenceExpiry := intent.state.expiry
	intent.state.mu.Unlock()
	if first.pinHash != "" && values.EnforceProfilePins {
		proof, err = i.store.VerifyBridgePasswordWithPIN(r, intent, raw)
	} else {
		proof, err = i.store.VerifyBridgeAccountPassword(r, intent, raw)
	}
	// The underlying comparator reports rejection after cancellation too. A
	// retired native/caller lifetime is terminal, not an invalid-password result.
	if requestContext.Err() != nil || r.Context() != requestContext || hold.Context().Err() != nil || !i.valid() {
		return nil, ErrBridgeCredentialChanged
	}
	if errors.Is(err, ErrBridgeCredentialRejected) && values.AllowPinLogin && first.pinHash != "" {
		currentBinding, bindingErr := bridgeCredentialRequestBinding(r, client)
		if !i.valid() || requestContext.Err() != nil || r.Context() != requestContext || hold.Context().Err() != nil || !evidenceExpiry.After(time.Now()) || bindingErr != nil || currentBinding != binding {
			return nil, ErrBridgeCredentialChanged
		}
		// The failed original is terminal. A fresh capture may only establish
		// equality to its immutable snapshot, never adopt changed authority.
		joinHold()
		intent.Close()
		intent, joinHold, err = i.store.prepareBridgePasswordWorkflow(r, requestContext, account, profile, client, binding, evidenceExpiry, hold.Context())
		if err != nil {
			return nil, err
		}
		intent.state.mu.Lock()
		unchanged := sameBridgeCredentialSnapshot(first, intent.state.snapshot)
		intent.state.mu.Unlock()
		if !unchanged {
			return nil, ErrBridgeCredentialChanged
		}
		proof, err = i.store.VerifyBridgeProfilePIN(r, intent, raw)
	}
	if err != nil {
		return nil, err
	}
	if err = proof.Check(requestContext); err != nil {
		return nil, err
	}
	// A managed write attempt may retire the original observation even when
	// the SH hold made that writer Busy. Never capture a replacement policy.
	finalFence, err := i.security.BindBridgePolicy(requestContext, original, hold)
	if err != nil {
		return nil, ErrBridgeSessionChanged
	}
	current, err := finalFence.Values()
	if err != nil || current != values {
		return nil, ErrBridgeSessionChanged
	}
	joinHold()
	if hold.Context().Err() != nil || requestContext.Err() != nil || r.Context() != requestContext || !proof.state.matchesRequest(r) || proof.Context().Err() != nil || !proof.ExpiresAt().After(time.Now()) {
		return nil, ErrBridgeCredentialChanged
	}
	return proof, nil
}

// The existing public Prepare cannot receive a separate work context without
// changing its exact request binding. This private preparation preserves the
// original pointer/context and reuses its actual state/admission/snapshot/check/
// verifier/Close machinery, linking the genuine hold before its first PG read.
func (ds *DataStore) prepareBridgePasswordWorkflow(r *http.Request, requestContext context.Context, account, profile, client string, binding [32]byte, fixedExpiry time.Time, holdContext context.Context) (intent *BridgeLoginCredentialIntent, releaseHold func(), err error) {
	if ds == nil || ds.pool == nil || r == nil || r.Context() != requestContext || requestContext.Err() != nil || holdContext == nil || holdContext.Err() != nil || !fixedExpiry.After(time.Now()) {
		return nil, nil, ErrBridgeCredentialChanged
	}
	current, err := bridgeCredentialRequestBinding(r, client)
	if err != nil || current != binding {
		return nil, nil, ErrBridgeCredentialChanged
	}
	life, cancel := context.WithDeadline(context.Background(), fixedExpiry)
	value := &BridgeLoginCredentialIntent{state: &bridgeCredentialState{store: ds, request: r, requestContext: requestContext, account: account, profile: profile, client: client, binding: binding, ctx: life, cancel: cancel, expiry: fixedExpiry, phase: bridgeCredentialPrepared, setupDone: make(chan struct{}), compare: bcrypt.CompareHashAndPassword}}
	value.state.self = value
	bridgeCredentialAdmission.Lock()
	if len(bridgeCredentialAdmission.intents) >= 64 || bridgeCredentialAdmission.accounts[account] >= 8 {
		bridgeCredentialAdmission.Unlock()
		cancel()
		return nil, nil, ErrBridgeCredentialBusy
	}
	bridgeCredentialAdmission.intents[value] = account
	bridgeCredentialAdmission.accounts[account]++
	bridgeCredentialAdmission.Unlock()
	value.state.setupStop = context.AfterFunc(requestContext, func() { defer close(value.state.setupDone); cancel() })
	joinHold := familyMediaAfterFunc(holdContext, cancel)
	defer func() {
		if intent == nil {
			joinHold()
			value.Close()
		}
	}()
	ctx, done, err := value.state.begin(requestContext)
	if err != nil {
		return nil, nil, err
	}
	err = func() error {
		defer done()
		snapshot, err := ds.readBridgeCredentialSnapshot(ctx, account, profile)
		if err != nil {
			return err
		}
		expiry := fixedExpiry
		if snapshot.hasAccountExpiry && snapshot.accountExpiry.Before(expiry) {
			expiry = snapshot.accountExpiry
		}
		bounded, finish := context.WithDeadline(life, expiry)
		if ctx.Err() != nil || bounded.Err() != nil || requestContext.Err() != nil || r.Context() != requestContext || holdContext.Err() != nil || !expiry.After(time.Now()) {
			finish()
			return ErrBridgeCredentialChanged
		}
		current, err := bridgeCredentialRequestBinding(r, client)
		if err != nil || current != binding {
			finish()
			return ErrBridgeCredentialChanged
		}
		value.state.snapshot = snapshot
		value.state.ctx = bounded
		value.state.expiry = expiry
		value.state.cancel = func() { finish(); cancel() }
		return nil
	}()
	if err != nil {
		return nil, nil, err
	}
	return value, joinHold, nil
}
