//go:build !linux

package config

import "context"

func webDAVPolicySupported() bool { return false }
func readOriginalWebDAVPolicy(context.Context, *Manager) (webDAVPolicyRead, error) {
	return webDAVPolicyRead{}, ErrSecurityCoordinator
}
