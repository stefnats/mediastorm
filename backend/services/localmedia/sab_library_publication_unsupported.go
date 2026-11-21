//go:build !linux

package localmedia

import (
	"context"
	"net/http"
	"novastream/internal/datastore"
)

func sabPublicationPlatformReady() bool { return false }
func (*SABLibraryPublisher) runPublication(*http.Request, int64, []byte, bool) (*SABLibraryPublicationResult, error) {
	return nil, datastore.ErrSABLibraryPublicationUnavailable
}
func (*Service) publicationScanNamespace(context.Context, string, string) bool { return false }
