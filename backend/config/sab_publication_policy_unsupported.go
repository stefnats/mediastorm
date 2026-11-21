//go:build !linux

package config

import "context"

func sabPublicationPolicySupported() bool { return false }
func readOriginalSABPublicationPolicy(context.Context, *Manager) (sabPublicationPolicyRead, error) {
	return sabPublicationPolicyRead{}, ErrSecurityCoordinator
}
