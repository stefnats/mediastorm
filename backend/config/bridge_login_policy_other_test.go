//go:build !linux

package config

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBridgePolicyUnsupportedPlatformDeniesOnlyAdvancedPort(t *testing.T) {
	var security *ManagedSecurity
	if value, err := security.CaptureBridgePolicy(context.Background()); err == nil || value != nil {
		t.Fatal("unsupported advanced observation accepted")
	}
	if value, err := security.BindBridgePolicy(context.Background(), nil, nil); err == nil || value != nil {
		t.Fatal("unsupported advanced binding accepted")
	}
	if _, err := readOriginalBridgeLoginPolicy(context.Background(), NewManager(filepath.Join(t.TempDir(), "settings.json"))); err == nil {
		t.Fatal("unsupported original-file reader accepted")
	}
	securityWritePlatform(t)
	manager := NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if err := manager.Save(DefaultSettings()); err != nil {
		t.Fatal("ordinary native Save changed", err)
	}
	if _, err := manager.Load(); err != nil {
		t.Fatal("ordinary native bootstrap changed", err)
	}
}
