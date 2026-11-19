package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"novastream/services/recordings"
)

func TestRecordingDerivedDownloadActualTransferAttachmentAndAPIInline(t *testing.T) {
	for _, ranged := range []bool{false, true} {
		for _, download := range []bool{false, true} {
			reader := newDerivedHTTPReader([]byte("0123456789abcdef"))
			r := httptest.NewRequest(http.MethodPost, "/synthetic-derived", nil)
			if ranged {
				r.Header.Set("Range", "bytes=2-5")
			}
			response := httptest.NewRecorder()
			var writer http.ResponseWriter = response
			if download {
				writer = &recordingDerivedDownloadWriter{ResponseWriter: response}
			}
			serveRecordingDerived(writer, r, reader, r.Context(), func() error { return nil })
			status, body := http.StatusOK, "0123456789abcdef"
			if ranged {
				status, body = http.StatusPartialContent, "2345"
			}
			disposition := `inline; filename="recording-joined.mkv"`
			if download {
				disposition = `attachment; filename="recording-joined.mkv"`
			}
			if response.Code != status || response.Body.String() != body || response.Header().Get("Content-Disposition") != disposition || response.Header().Get("Cache-Control") != "private, no-store" || response.Header().Get("Content-Type") != "video/x-matroska" {
				t.Fatalf("transfer attachment=%t range=%t status=%d", download, ranged, response.Code)
			}
		}
	}
}

func TestRecordingDerivedDownloadErrorsRemainUnchanged(t *testing.T) {
	for _, err := range []error{recordings.ErrDerivedAccess, recordings.ErrDerivedBusy, recordings.ErrDerivedTooLarge, recordings.ErrDerivedUnavailable} {
		original, wrapped := httptest.NewRecorder(), httptest.NewRecorder()
		writeRecordingDerivedError(original, err)
		writeRecordingDerivedError(&recordingDerivedDownloadWriter{ResponseWriter: wrapped}, err)
		if original.Code != wrapped.Code || original.Body.String() != wrapped.Body.String() || wrapped.Header().Get("Content-Disposition") != "" || original.Header().Get("Content-Type") != wrapped.Header().Get("Content-Type") {
			t.Fatal("download wrapper changed error response")
		}
	}
	f := newExportHTTPFixture(t)
	f.h.SetRecordingStreamAccess(nil)
	original, wrapped := httptest.NewRecorder(), httptest.NewRecorder()
	f.h.StreamDerived(original, f.request(t))
	f.h.DownloadDerived(wrapped, f.request(t))
	if original.Code != wrapped.Code || original.Body.String() != wrapped.Body.String() || wrapped.Code != http.StatusNotFound || wrapped.Header().Get("Content-Disposition") != "" {
		t.Fatal("actual DownloadDerived weakened original authority or attached an error")
	}
}

type derivedDownloadDeadlineWriter struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *derivedDownloadDeadlineWriter) SetWriteDeadline(at time.Time) error {
	w.deadline = at
	return nil
}

func TestRecordingDerivedDownloadUnwrapPreservesOriginalWriteDeadline(t *testing.T) {
	original := &derivedDownloadDeadlineWriter{ResponseRecorder: httptest.NewRecorder()}
	wrapped := &recordingDerivedDownloadWriter{ResponseWriter: original}
	deadline := time.Now().Add(time.Minute)
	if wrapped.Unwrap() != original || http.NewResponseController(wrapped).SetWriteDeadline(deadline) != nil || !original.deadline.Equal(deadline) {
		t.Fatal("download wrapper hid original response controller")
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	r := httptest.NewRequest(http.MethodPost, "/synthetic-derived", nil)
	serveRecordingDerived(wrapped, r, newDerivedHTTPReader([]byte("0123")), ctx, func() error { return nil })
	if !original.deadline.Equal(deadline) || original.Code != http.StatusOK || original.Body.String() != "0123" {
		t.Fatal("actual transfer lost original deadline or bytes")
	}
}
