package config

import (
	"context"
	"fmt"

	"novastream/internal/settingscommit"
)

// BridgeLoginPolicyValues is observed DATA only. No issuer accepts caller-made
// values as authorization, a transaction, a credential proof, or a mint permit.
type BridgeLoginPolicyValues struct {
	EnforceProfilePins bool
	AllowPinLogin      bool
	BridgeEnabled      bool
}

type bridgeLoginPolicyVersion struct {
	device, inode, links           uint64
	mode                           uint32
	size                           int64
	modifiedSeconds, modifiedNanos int64
	changedSeconds, changedNanos   int64
}

type bridgeLoginPolicyRead struct {
	values                          BridgeLoginPolicyValues
	version                         bridgeLoginPolicyVersion
	digest                          [32]byte
	databaseURL                     string
	quickConnect, quickConnectValid bool
}

type BridgeLoginPolicyObservation struct {
	self    *BridgeLoginPolicyObservation
	owner   *ManagedSecurity
	ctx     context.Context
	attempt *securityAttempt
	values  BridgeLoginPolicyValues
	version bridgeLoginPolicyVersion
	digest  [32]byte
}

type BridgeLoginPolicyFence struct {
	self     *BridgeLoginPolicyFence
	owner    *ManagedSecurity
	original context.Context
	ctx      context.Context
	hold     *settingscommit.ProgressFileHold
	values   BridgeLoginPolicyValues
}

func (BridgeLoginPolicyObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginPolicyObservation) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (BridgeLoginPolicyObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginPolicyObservation(opaque)"))
}
func (BridgeLoginPolicyFence) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginPolicyFence) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (BridgeLoginPolicyFence) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginPolicyFence(opaque)"))
}

func (s *ManagedSecurity) bridgePolicyCurrent(ctx context.Context) bool {
	return ctx != nil && ctx.Err() == nil && s.ctx.Err() == nil && s.coordinator.MatchesConfigPath(s.manager.path) && s.coordinator.Armed()
}
func (s *ManagedSecurity) bridgePolicyDomain(read bridgeLoginPolicyRead) bool {
	effective := read.databaseURL
	if s.override != "" {
		effective = s.override
	}
	return effective != "" && s.coordinator.MatchesSourceURL(effective)
}

// CaptureBridgePolicy never loads defaults, migrates settings, opens PG or
// verifies credentials. One original file supplies all three observed flags.
func (s *ManagedSecurity) CaptureBridgePolicy(ctx context.Context) (*BridgeLoginPolicyObservation, error) {
	if !bridgeLoginPolicySupported() || ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	attempt := settingsAttemptNow()
	if !s.bridgePolicyCurrent(ctx) {
		return nil, ErrSecurityCoordinator
	}
	read, err := readOriginalBridgeLoginPolicy(ctx, s.manager)
	if err != nil || !s.bridgePolicyDomain(read) || !settingsAttemptCurrent(attempt) || !s.bridgePolicyCurrent(ctx) {
		return nil, ErrSecurityCoordinator
	}
	value := &BridgeLoginPolicyObservation{owner: s, ctx: ctx, attempt: attempt, values: read.values, version: read.version, digest: read.digest}
	value.self = value
	return value, nil
}

// BindBridgePolicy checks the genuine native hold of this exact coordinator.
// It does NOT prove that the publicly obtainable hold belongs to a PG TX. A
// later concrete datastore mint must use only its own private settings TX hold.
func (s *ManagedSecurity) BindBridgePolicy(ctx context.Context, original *BridgeLoginPolicyObservation, hold *settingscommit.ProgressFileHold) (*BridgeLoginPolicyFence, error) {
	if !bridgeLoginPolicySupported() || original == nil || original.self != original || original.owner != s || ctx == nil || ctx.Err() != nil || original.ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	if !settingsAttemptCurrent(original.attempt) || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	read, err := readOriginalBridgeLoginPolicy(ctx, s.manager)
	if err != nil || !s.bridgePolicyDomain(read) || read.values != original.values || read.version != original.version || read.digest != original.digest || !settingsAttemptCurrent(original.attempt) || original.ctx.Err() != nil || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	value := &BridgeLoginPolicyFence{owner: s, original: original.ctx, ctx: ctx, hold: hold, values: read.values}
	value.self = value
	return value, nil
}

// Values remains the original three-value snapshot while its own hold and
// lifetimes survive. A Busy writer attempt prevents new binds but does not
// change an already-bound snapshot or borrow another attempt's policy.
func (f *BridgeLoginPolicyFence) Values() (BridgeLoginPolicyValues, error) {
	var zero BridgeLoginPolicyValues
	if f == nil || f.self != f || f.ctx.Err() != nil || f.original.Err() != nil || !f.owner.enter() {
		return zero, ErrSecurityCoordinator
	}
	defer f.owner.operations.Done()
	if f.owner.ctx.Err() != nil || f.hold.Current(f.owner.coordinator) != nil || f.ctx.Err() != nil || f.original.Err() != nil || f.owner.ctx.Err() != nil {
		return zero, ErrSecurityCoordinator
	}
	return f.values, nil
}
