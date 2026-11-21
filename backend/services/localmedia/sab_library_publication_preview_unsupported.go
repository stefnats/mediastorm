//go:build !linux

package localmedia

import (
	"net/http"
	"novastream/internal/datastore"
)

func (*SABLibraryPublisher) previewSavedStage(*http.Request, int64, []byte) (*SABLibraryPublicationPreview, error) {
	return nil, datastore.ErrSABLibraryPublicationUnavailable
}
