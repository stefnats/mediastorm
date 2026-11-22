//go:build !linux

package config

import "context"

func CaptureFamilyNZBWorkingDirectory(context.Context, *Manager) (*FamilyNZBRoot, error) {
	return nil, ErrSecurityCoordinator
}
func checkFamilyNZBDirectory(context.Context, *FamilyNZBRoot) error { return ErrSecurityCoordinator }
func familyNZBMetadataRoot(string, string) (string, error)          { return "", ErrSecurityCoordinator }
func readOriginalFamilyNZBPolicy(context.Context, *FamilyNZBRoot) (familyNZBPolicyRead, error) {
	return familyNZBPolicyRead{}, ErrSecurityCoordinator
}
