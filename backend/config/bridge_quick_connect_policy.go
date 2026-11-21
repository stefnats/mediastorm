package config

import (
	"context"
	"fmt"
	"time"

	"novastream/internal/settingscommit"
)

type BridgeQuickConnectPolicyValues struct {
	BridgeEnabled, QuickConnectEnabled, EnforceProfilePins bool
}

// These original file receipts contain policy DATA only, never a session,
// delegation permission, caller role, or proof of an SQL transaction.
type BridgeQuickConnectPolicyObservation struct {
	self    *BridgeQuickConnectPolicyObservation
	owner   *ManagedSecurity
	ctx     context.Context
	attempt *securityAttempt
	read    bridgeLoginPolicyRead
}
type BridgeQuickConnectPolicyReceipt struct {
	self    *BridgeQuickConnectPolicyReceipt
	owner   *ManagedSecurity
	attempt *securityAttempt
	read    bridgeLoginPolicyRead
	expiry  time.Time
}
type BridgeQuickConnectPolicyFence struct {
	self     *BridgeQuickConnectPolicyFence
	owner    *ManagedSecurity
	ctx      context.Context
	hold     *settingscommit.ProgressFileHold
	original *BridgeQuickConnectPolicyObservation
	receipt  *BridgeQuickConnectPolicyReceipt
	read     bridgeLoginPolicyRead
}

func (BridgeQuickConnectPolicyObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectPolicyObservation(opaque)"))
}
func (BridgeQuickConnectPolicyObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectPolicyObservation) UnmarshalJSON([]byte) error {
	return ErrSecurityCoordinator
}
func (BridgeQuickConnectPolicyReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectPolicyReceipt(opaque)"))
}
func (BridgeQuickConnectPolicyReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectPolicyReceipt) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (BridgeQuickConnectPolicyFence) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectPolicyFence(opaque)"))
}
func (BridgeQuickConnectPolicyFence) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectPolicyFence) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }

func (s *ManagedSecurity) CaptureBridgeQuickConnectPolicy(ctx context.Context) (*BridgeQuickConnectPolicyObservation, error) {
	if !bridgeLoginPolicySupported() || ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	attempt := settingsAttemptNow()
	read, err := readOriginalBridgeLoginPolicy(ctx, s.manager)
	if err != nil || !read.quickConnectValid || !s.bridgePolicyDomain(read) || !settingsAttemptCurrent(attempt) || !s.bridgePolicyCurrent(ctx) {
		return nil, ErrSecurityCoordinator
	}
	v := &BridgeQuickConnectPolicyObservation{owner: s, ctx: ctx, attempt: attempt, read: read}
	v.self = v
	return v, nil
}

func sameQuickConnectPolicy(a, b bridgeLoginPolicyRead) bool {
	return a.version == b.version && a.digest == b.digest && a.values == b.values && a.quickConnectValid && b.quickConnectValid && a.quickConnect == b.quickConnect && a.databaseURL == b.databaseURL
}
func (s *ManagedSecurity) BindBridgeQuickConnectPolicy(ctx context.Context, original *BridgeQuickConnectPolicyObservation, hold *settingscommit.ProgressFileHold) (*BridgeQuickConnectPolicyFence, error) {
	if original == nil || original.self != original || original.owner != s || original.ctx.Err() != nil {
		return nil, ErrSecurityCoordinator
	}
	return s.bindQuickConnectPolicy(ctx, original, nil, hold)
}
func (s *ManagedSecurity) BindBridgeQuickConnectPolicyReceipt(ctx context.Context, receipt *BridgeQuickConnectPolicyReceipt, hold *settingscommit.ProgressFileHold) (*BridgeQuickConnectPolicyFence, error) {
	if receipt == nil || receipt.self != receipt || receipt.owner != s || !receipt.expiry.After(time.Now()) {
		return nil, ErrSecurityCoordinator
	}
	return s.bindQuickConnectPolicy(ctx, nil, receipt, hold)
}
func (s *ManagedSecurity) bindQuickConnectPolicy(ctx context.Context, original *BridgeQuickConnectPolicyObservation, receipt *BridgeQuickConnectPolicyReceipt, hold *settingscommit.ProgressFileHold) (*BridgeQuickConnectPolicyFence, error) {
	if ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	read := bridgeLoginPolicyRead{}
	var attempt *securityAttempt
	if original != nil {
		read = original.read
		attempt = original.attempt
	} else {
		read = receipt.read
		attempt = receipt.attempt
	}
	if !settingsAttemptCurrent(attempt) || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	current, err := readOriginalBridgeLoginPolicy(ctx, s.manager)
	if err != nil || !s.bridgePolicyDomain(current) || !sameQuickConnectPolicy(current, read) || !settingsAttemptCurrent(attempt) || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil || original != nil && original.ctx.Err() != nil || receipt != nil && !receipt.expiry.After(time.Now()) {
		return nil, ErrSecurityCoordinator
	}
	f := &BridgeQuickConnectPolicyFence{owner: s, ctx: ctx, hold: hold, original: original, receipt: receipt, read: read}
	f.self = f
	return f, nil
}
func (f *BridgeQuickConnectPolicyFence) Values() (BridgeQuickConnectPolicyValues, error) {
	if f == nil || f.self != f || f.ctx.Err() != nil || f.owner == nil || !f.owner.enter() {
		return BridgeQuickConnectPolicyValues{}, ErrSecurityCoordinator
	}
	defer f.owner.operations.Done()
	if !f.owner.bridgePolicyCurrent(f.ctx) || f.hold.Current(f.owner.coordinator) != nil || f.original != nil && f.original.ctx.Err() != nil || f.receipt != nil && !f.receipt.expiry.After(time.Now()) {
		return BridgeQuickConnectPolicyValues{}, ErrSecurityCoordinator
	}
	return BridgeQuickConnectPolicyValues{f.read.values.BridgeEnabled, f.read.quickConnect, f.read.values.EnforceProfilePins}, nil
}

// Handoff mints a distinct bounded policy receipt while the original request
// and genuine hold survive. It never extends their contexts or renews a receipt.
func (f *BridgeQuickConnectPolicyFence) Handoff(expiry time.Time) (*BridgeQuickConnectPolicyReceipt, error) {
	if f == nil || f.original == nil || f.receipt != nil || !expiry.After(time.Now()) || expiry.After(time.Now().Add(10*time.Minute)) {
		return nil, ErrSecurityCoordinator
	}
	if _, err := f.Values(); err != nil || !settingsAttemptCurrent(f.original.attempt) {
		return nil, ErrSecurityCoordinator
	}
	r := &BridgeQuickConnectPolicyReceipt{owner: f.owner, attempt: f.original.attempt, read: f.read, expiry: expiry}
	r.self = r
	return r, nil
}
