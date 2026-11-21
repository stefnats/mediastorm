package localmedia

import (
	"fmt"
	"net/http"
	"time"

	"novastream/internal/datastore"
)

// Preview describes the bytes of the current original saved stage. It never
// reserves a publication, copies a file or grants catalogue/media permission.
type SABLibraryPublicationPreviewFile struct {
	Ordinal   int    `json:"ordinal"`
	Bytes     int64  `json:"bytes"`
	Container string `json:"container,omitempty"`
	Eligible  bool   `json:"eligible"`
}
type SABLibraryPublicationPreviewView struct {
	QueueID   int64                              `json:"queueId"`
	LibraryID string                             `json:"libraryId"`
	Files     []SABLibraryPublicationPreviewFile `json:"files"`
}
type SABLibraryPublicationPreview struct {
	self   *SABLibraryPublicationPreview
	job    *sabPublicationJob
	view   SABLibraryPublicationPreviewView
	expiry time.Time
}

func (SABLibraryPublicationPreview) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABLibraryPublicationPreview) UnmarshalJSON([]byte) error {
	return datastore.ErrSABLibraryPublicationChanged
}
func (SABLibraryPublicationPreview) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABLibraryPublicationPreview(opaque)"))
}
func (p *SABLibraryPublisher) Preview(r *http.Request, queueID int64, selectors []byte) (*SABLibraryPublicationPreview, error) {
	return p.previewSavedStage(r, queueID, selectors)
}
func (v *SABLibraryPublicationPreview) valid() bool {
	return v != nil && v.self == v && v.job != nil && v.job.owner != nil && v.job.owner.self == v.job.owner
}
func (v *SABLibraryPublicationPreview) Check(r *http.Request) error {
	if !v.valid() || r != v.job.request || r.Context().Err() != nil || v.job.owner.ctx.Err() != nil || !v.expiry.After(time.Now()) {
		return datastore.ErrSABLibraryPublicationChanged
	}
	v.job.mu.Lock()
	intent := v.job.intent
	v.job.mu.Unlock()
	if intent == nil {
		return datastore.ErrSABLibraryPublicationChanged
	}
	return intent.Check(r)
}
func (v *SABLibraryPublicationPreview) View() SABLibraryPublicationPreviewView {
	if !v.valid() {
		return SABLibraryPublicationPreviewView{}
	}
	view := v.view
	view.Files = append([]SABLibraryPublicationPreviewFile(nil), view.Files...)
	return view
}
func (v *SABLibraryPublicationPreview) ExpiresAt() time.Time {
	if v.valid() {
		return v.expiry
	}
	return time.Time{}
}
func (v *SABLibraryPublicationPreview) Close() {
	if v.valid() {
		v.job.close()
	}
}
