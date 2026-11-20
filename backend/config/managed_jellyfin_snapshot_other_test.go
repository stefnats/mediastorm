//go:build !linux

package config

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestManagedJellyfinSnapshotUnsupportedPlatformDenies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	lease, err := NewManager(filepath.Join(t.TempDir(), "settings.json")).CaptureManagedJellyfin(ctx, "integration")
	if lease != nil || !errors.Is(err, ErrManagedConfigurationUnavailable) {
		t.Fatal("unsupported snapshot platform was promoted")
	}
}
