//go:build !linux

package config

import "context"

// Native Save/bootstrap support is unchanged. Only this new advanced policy
// observation is denied when its actual regular-file implementation is absent.
func bridgeLoginPolicySupported() bool { return false }
func readOriginalBridgeLoginPolicy(context.Context, *Manager) (bridgeLoginPolicyRead, error) {
	return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
}
