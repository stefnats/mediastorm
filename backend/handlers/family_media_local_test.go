package handlers

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/mux"
	"novastream/services/profileunlock"
)

// These additive tests target the private handler, not a registered public URL.
func TestFamilyLocalRawSingleRangesUseExactHeldSizeAndNeverInventValidators(t *testing.T) {
	for _, tc := range []struct {
		rangeHeader, ifRange string
		size                 int64
		head                 bool
		start, length        int64
		partial, bad         bool
	}{
		{"", "", 100, false, 0, 100, false, false},
		{"bytes=0-9", "", 100, false, 0, 10, true, false},
		{"bytes=90-", "", 100, false, 90, 10, true, false},
		{"bytes=-10", "", 100, false, 90, 10, true, false},
		{"bytes=-999", "", 100, false, 0, 100, true, false},
		{"bytes=90-999", "", 100, false, 90, 10, true, false},
		{"bytes=00-09", "", 100, false, 0, 10, true, false},
		{"bytes=0-9", `"unproven-size-label"`, 100, false, 0, 100, false, false},
		{"bytes=0-9", "Wed, 01 Jan 2020 00:00:00 GMT", 100, false, 0, 100, false, false},
		{"bytes=0-9", "", 100, true, 0, 100, false, false},
		{"", "", 0, false, 0, 0, false, false},
		{"bytes=0-0", "", 0, false, 0, 0, false, true},
		{"bytes=100-", "", 100, false, 0, 0, false, true},
		{"bytes=9-1", "", 100, false, 0, 0, false, true},
		{"bytes=-0", "", 100, false, 0, 0, false, true},
		{"bytes=+1-9", "", 100, false, 0, 0, false, true},
		{"bytes=1--1", "", 100, false, 0, 0, false, true},
		{"bytes=0-1,3-4", "", 100, false, 0, 0, false, true},
		{"bytes=9223372036854775808-", "", 100, false, 0, 0, false, true},
		{"bytes=-9223372036854775808", "", 100, false, 0, 0, false, true},
		{"bytes=9223372036854775806-9223372036854775807", "", 9223372036854775807, false, 9223372036854775806, 1, true, false},
		{"items=0-9", "", 100, false, 0, 0, false, true},
		{"bytes= 0-9", "", 100, false, 0, 0, false, true},
		{"bytes=-", "", 100, false, 0, 0, false, true},
	} {
		start, length, partial, err := familyLocalRawRange(tc.rangeHeader, tc.ifRange, tc.size, tc.head)
		if (err != nil) != tc.bad || err == nil && (start != tc.start || length != tc.length || partial != tc.partial) {
			t.Fatalf("range %q size=%d head=%v: got %d,%d,%v,error=%v", tc.rangeHeader, tc.size, tc.head, start, length, partial, err)
		}
	}
}

// Internal staged-copy observer only; it never enters the real constructor,
// PG source capture or filesystem opener used by the actual integration tests.
type familyLocalCopyFile struct{ changed atomic.Bool }

func (f *familyLocalCopyFile) Check(ctx context.Context) error {
	if f.changed.Load() || ctx.Err() != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}
func (*familyLocalCopyFile) Context() context.Context { return context.Background() }
func (*familyLocalCopyFile) Info() profileunlock.FamilyLocalFileInfo {
	return profileunlock.FamilyLocalFileInfo{}
}
func (*familyLocalCopyFile) OpenReader(context.Context, int64, int64) (io.ReadCloser, error) {
	return nil, profileunlock.ErrFamilyMediaChanged
}
func (*familyLocalCopyFile) Close() error { return nil }

type familyLocalLateChunk struct {
	file   *familyLocalCopyFile
	buffer []byte
}

func (r *familyLocalLateChunk) Read(p []byte) (int, error) {
	r.buffer = p
	r.file.changed.Store(true)
	return copy(p, []byte("Synthetic stale source chunk.")), nil
}
func TestFamilyLocalRawStagedCopyRechecksActualSourceBeforePublishingLateChunk(t *testing.T) {
	_, _, runtime, _ := familyConsumerRuntime(t)
	c, err := runtime.beginSubread(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	file := &familyLocalCopyFile{}
	reader := &familyLocalLateChunk{file: file}
	w := httptest.NewRecorder()
	if familyLocalRawCopy(c, file, w, reader, 1<<20) == nil || w.Body.Len() != 0 {
		t.Fatal("late source chunk reached actual output helper")
	}
	if len(reader.buffer) != 32<<10 || !bytes.Equal(reader.buffer, make([]byte, len(reader.buffer))) {
		t.Fatal("late source staging escaped bound or was not cleared")
	}
}

func TestFamilyLocalRawRequestAcceptsOnlyExactItemAndProfileSelectors(t *testing.T) {
	valid := mux.SetURLVars(httptest.NewRequest("GET", "/?profileId=profile", nil), map[string]string{"itemID": "synthetic-local-item"})
	item, err := familyLocalRawItem(valid)
	if err != nil || item != "synthetic-local-item" {
		t.Fatal("actual row item selector rejected", err)
	}
	for _, tc := range []struct{ query, item string }{
		{"?path=/private/source", "synthetic-local-item"},
		{"?url=https%3A%2F%2Fexample.invalid", "synthetic-local-item"},
		{"?source=localmedia%3Aforged", "synthetic-local-item"},
		{"?profileName=other", "synthetic-local-item"},
		{"?ProfileName=other", "synthetic-local-item"},
		{"?profileId=profile&profileId=profile", "synthetic-local-item"},
		{"?ProfileId=profile", "synthetic-local-item"},
		{"?profileId=profile&userId=other", "synthetic-local-item"},
		{"?profileId=", "synthetic-local-item"},
		{"", ""}, {"", " synthetic-local-item"}, {"", "../synthetic"}, {"", "localmedia:item"}, {"", strings.Repeat("x", 129)},
	} {
		r := mux.SetURLVars(httptest.NewRequest("GET", "/"+tc.query, nil), map[string]string{"itemID": tc.item})
		if _, err := familyLocalRawItem(r); err == nil {
			t.Fatalf("source/ambiguous selector accepted: %q %q", tc.query, tc.item)
		}
	}
	withBody := mux.SetURLVars(httptest.NewRequest("GET", "/", strings.NewReader(`{"itemID":"synthetic"}`)), map[string]string{"itemID": "synthetic-local-item"})
	if _, err := familyLocalRawItem(withBody); err == nil {
		t.Fatal("GET body became an alternative source selector")
	}
	withTransfer := mux.SetURLVars(httptest.NewRequest("HEAD", "/", nil), map[string]string{"itemID": "synthetic-local-item"})
	withTransfer.TransferEncoding = []string{"chunked"}
	if _, err := familyLocalRawItem(withTransfer); err == nil {
		t.Fatal("transfer-encoded HEAD body accepted")
	}
	duplicateRange := mux.SetURLVars(httptest.NewRequest("GET", "/", nil), map[string]string{"itemID": "synthetic-local-item"})
	duplicateRange.Header.Add("Range", "bytes=0-1")
	duplicateRange.Header.Add("Range", "bytes=2-3")
	if _, err := familyLocalRawItem(duplicateRange); err == nil {
		t.Fatal("two Range fields became one implicit selection")
	}
}

func TestFamilyLocalRawUnavailableAndUnsupportedMethodsNeverDelegate(t *testing.T) {
	var h *familyLocalRawHandler
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/", nil))
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, HEAD, OPTIONS" || w.Body.Len() > 256 {
			t.Fatal("unsupported method delegated or unbounded response", method, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("OPTIONS", "/", nil))
	if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
		t.Fatal("OPTIONS entered unavailable/source handler")
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, mux.SetURLVars(httptest.NewRequest("GET", "/", nil), map[string]string{"itemID": "synthetic-local-item"}))
	if w.Code != http.StatusServiceUnavailable || w.Body.Len() > 256 {
		t.Fatal("missing private dependencies became legacy access")
	}
	if raw, err := io.ReadAll(w.Result().Body); err != nil || strings.Contains(string(raw), "/private/") {
		t.Fatal("unavailable handler exposed a source path")
	}
}
