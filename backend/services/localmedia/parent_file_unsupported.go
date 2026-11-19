//go:build !linux

package localmedia

import (
	"context"
	"novastream/services/kidsvod"
)

// Platforms without the audited FD/stat/local-filesystem checks fail closed.
func (*Service) OpenParentFile(context.Context, kidsvod.Subject, string) (kidsvod.ParentFileSource, error) {
	return nil, kidsvod.ErrParentFileChanged
}
