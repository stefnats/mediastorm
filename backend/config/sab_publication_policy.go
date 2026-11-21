package config

import (
	"context"
	"fmt"
	"novastream/internal/settingscommit"
)

// SABPublicationPolicyValues describes one native configuration. It carries no
// actor, source permission, title evidence or authority to register an item.
type SABPublicationPolicyValues struct {
	StageRoot          string
	MaxBytes           int64
	SourceFingerprint  string
	EnforceProfilePins bool
}
type sabPublicationPolicyRead struct {
	values      SABPublicationPolicyValues
	version     bridgeLoginPolicyVersion
	digest      [32]byte
	databaseURL string
}
type SABPublicationPolicyObservation struct {
	self    *SABPublicationPolicyObservation
	owner   *ManagedSecurity
	ctx     context.Context
	attempt *securityAttempt
	read    sabPublicationPolicyRead
}
type SABPublicationPolicyFence struct {
	self     *SABPublicationPolicyFence
	owner    *ManagedSecurity
	original *SABPublicationPolicyObservation
	ctx      context.Context
	hold     *settingscommit.ProgressFileHold
	values   SABPublicationPolicyValues
}

func (SABPublicationPolicyObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABPublicationPolicyObservation) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (SABPublicationPolicyObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABPublicationPolicyObservation(opaque)"))
}
func (SABPublicationPolicyFence) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABPublicationPolicyFence) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (SABPublicationPolicyFence) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABPublicationPolicyFence(opaque)"))
}
func (s *ManagedSecurity) CaptureSABPublicationPolicy(ctx context.Context) (*SABPublicationPolicyObservation, error) {
	if !sabPublicationPolicySupported() || ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	attempt := settingsAttemptNow()
	read, err := readOriginalSABPublicationPolicy(ctx, s.manager)
	if err != nil || !s.sabPublicationDomain(read) || !settingsAttemptCurrent(attempt) || !s.bridgePolicyCurrent(ctx) {
		return nil, ErrSecurityCoordinator
	}
	value := &SABPublicationPolicyObservation{owner: s, ctx: ctx, attempt: attempt, read: read}
	value.self = value
	return value, nil
}
func (s *ManagedSecurity) sabPublicationDomain(read sabPublicationPolicyRead) bool {
	value := read.databaseURL
	if s.override != "" {
		value = s.override
	}
	return value != "" && s.coordinator.MatchesSourceURL(value)
}
func (s *ManagedSecurity) BindSABPublicationPolicy(ctx context.Context, original *SABPublicationPolicyObservation, hold *settingscommit.ProgressFileHold) (*SABPublicationPolicyFence, error) {
	if !sabPublicationPolicySupported() || original == nil || original.self != original || original.owner != s || ctx == nil || ctx.Err() != nil || original.ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	if !settingsAttemptCurrent(original.attempt) || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	current, err := readOriginalSABPublicationPolicy(ctx, s.manager)
	if err != nil || current != original.read || !s.sabPublicationDomain(current) || !settingsAttemptCurrent(original.attempt) || original.ctx.Err() != nil || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	value := &SABPublicationPolicyFence{owner: s, original: original, ctx: ctx, hold: hold, values: current.values}
	value.self = value
	return value, nil
}
func (f *SABPublicationPolicyFence) Values() (SABPublicationPolicyValues, error) {
	if f == nil || f.self != f || f.original == nil || f.ctx.Err() != nil || f.original.ctx.Err() != nil || !f.owner.enter() {
		return SABPublicationPolicyValues{}, ErrSecurityCoordinator
	}
	defer f.owner.operations.Done()
	if !settingsAttemptCurrent(f.original.attempt) || !f.owner.bridgePolicyCurrent(f.ctx) || f.hold.Current(f.owner.coordinator) != nil {
		return SABPublicationPolicyValues{}, ErrSecurityCoordinator
	}
	return f.values, nil
}
