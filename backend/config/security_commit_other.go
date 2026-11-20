//go:build !linux

package config

import (
	"context"
	"os"
)

// Ordinary bootstrap and Save use native WRITE coordination in common code.
// This port denies only the new advanced policy/progress capability.
func recordingPolicySupported() bool { return false }
func readOriginalSecurityPolicy(context.Context, *Manager) (bool, os.FileInfo, error) {
	return false, nil, ErrSecurityCoordinator
}
