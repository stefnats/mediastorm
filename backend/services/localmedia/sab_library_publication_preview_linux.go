//go:build linux

package localmedia

import (
	"encoding/json"
	"net/http"

	"novastream/internal/datastore"
)

func (p *SABLibraryPublisher) previewSavedStage(r *http.Request, queueID int64, selectors []byte) (result *SABLibraryPublicationPreview, err error) {
	job, err := p.begin(r)
	if err != nil {
		return nil, err
	}
	defer close(job.prepared)
	defer func() {
		if result == nil {
			job.close()
		}
	}()
	intent, err := p.issuer.Capture(r, queueID, selectors)
	if err != nil {
		return nil, err
	}
	job.mu.Lock()
	job.intent = intent
	job.mu.Unlock()
	// Capture already validates the complete original selector bytes. The
	// decoded library name below is descriptive output, never a new authority.
	var selection struct {
		LibraryID string `json:"libraryId"`
		Ordinals  []int  `json:"ordinals"`
	}
	if json.Unmarshal(selectors, &selection) != nil {
		return nil, datastore.ErrSABLibraryPublicationChanged
	}
	files := intent.Files()
	if len(files) == 0 || len(files) > 128 || len(files) != len(selection.Ordinals) {
		return nil, datastore.ErrSABLibraryPublicationChanged
	}
	view := SABLibraryPublicationPreviewView{QueueID: queueID, LibraryID: selection.LibraryID}
	for _, file := range files {
		container, formatErr := detectFamilyLocalProbeFormat(sabPublicationFormatReader{r, intent, file.Ordinal}, file.Bytes)
		if intent.Check(r) != nil || p.ctx.Err() != nil {
			return nil, datastore.ErrSABLibraryPublicationChanged
		}
		entry := SABLibraryPublicationPreviewFile{Ordinal: file.Ordinal, Bytes: file.Bytes}
		if formatErr == nil {
			entry.Container = container
			entry.Eligible = true
		}
		view.Files = append(view.Files, entry)
	}
	value := &SABLibraryPublicationPreview{job: job, view: view, expiry: intent.ExpiresAt()}
	value.self = value
	if value.Check(r) != nil {
		return nil, datastore.ErrSABLibraryPublicationChanged
	}
	return value, nil
}
