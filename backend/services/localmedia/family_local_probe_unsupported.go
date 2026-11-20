//go:build !linux || !amd64

package localmedia

import (
	"context"
	"novastream/services/profileunlock"
)

func newFamilyLocalProbeTools(FamilyLocalProbeConfig) (*FamilyLocalProbeTools, error) {
	return nil, ErrFamilyLocalProbeUnavailable
}
func (*Service) ProbeFamilyLocalFile(context.Context, profileunlock.FamilyLocalFile, *FamilyLocalProbeTools) (FamilyLocalProbeResult, error) {
	return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
}
