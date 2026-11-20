//go:build !linux

package config

import "context"

func (*Manager) CaptureManagedJellyfin(context.Context, string) (*ManagedJellyfinLease, error) {
	return nil, ErrManagedConfigurationUnavailable
}
