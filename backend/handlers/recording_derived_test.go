package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/api/openapi"
	"novastream/models"
	"novastream/services/recordings"
)

func TestRecordingDerivedCanonicalSingleRangeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		header     string
		start, end int64
		ranged     bool
	}{
		{"", 0, 9, false}, {"bytes=0-0", 0, 0, true}, {"bytes=2-7", 2, 7, true},
		{"bytes=2-", 2, 9, true}, {"bytes=-3", 7, 9, true}, {"bytes=-99", 0, 9, true},
		{"bytes=9-99", 9, 9, true}, {"bytes=0-9223372036854775807", 0, 9, true},
	} {
		t.Run(tc.header, func(t *testing.T) {
			start, end, ranged, err := recordingDerivedRange(tc.header, 10)
			if err != nil || start != tc.start || end != tc.end || ranged != tc.ranged {
				t.Fatalf("unexpected single range %d..%d/%t: %v", start, end, ranged, err)
			}
		})
	}
	start, end, _, err := recordingDerivedRange("bytes=-9223372036854775807", math.MaxInt64)
	if err != nil || start != 0 || end != math.MaxInt64-1 {
		t.Fatal("suffix arithmetic overflowed")
	}
}

func TestRecordingDerivedRejectsAmbiguousOverflowAndMultipartRanges(t *testing.T) {
	for _, header := range []string{"bytes=", "bytes=-", "bytes=-0", "bytes=10-", "bytes=7-2", "bytes=00-2", "bytes=+1-2", "bytes=1-+2", "bytes=1-02", "bytes=1--2", "bytes=1-2,4-5", "bytes=1 -2", "bytes=1-2\r\n", "Bytes=1-2", "items=1-2", "bytes=9223372036854775808-", "bytes=-9223372036854775808", strings.Repeat("0", 129)} {
		if _, _, _, err := recordingDerivedRange(header, 10); err == nil {
			t.Fatalf("ambiguous range accepted: %q", header)
		}
	}
	for _, size := range []int64{0, -1, math.MinInt64} {
		if _, _, _, err := recordingDerivedRange("", size); err == nil {
			t.Fatal("invalid source size accepted")
		}
	}
}

// This fake isolates the production HTTP transfer adapter. Real codec/container
// generation and actual DerivedReader source binding are separate media tests.
type derivedHTTPReader struct {
	*bytes.Reader
	desc       recordings.DerivedRecording
	afterRead  func()
	afterSeek  func()
	readError  error
	lastBuffer []byte
	reads      int
}

func (r *derivedHTTPReader) Descriptor() recordings.DerivedRecording { return r.desc }
func (*derivedHTTPReader) Close() error                              { return nil }
func (r *derivedHTTPReader) Seek(offset int64, whence int) (int64, error) {
	n, err := r.Reader.Seek(offset, whence)
	if r.afterSeek != nil {
		r.afterSeek()
	}
	return n, err
}
func (r *derivedHTTPReader) Read(p []byte) (int, error) {
	r.reads++
	r.lastBuffer = p
	n, err := r.Reader.Read(p)
	if r.afterRead != nil {
		r.afterRead()
	}
	if r.readError != nil {
		return n, r.readError
	}
	return n, err
}
func newDerivedHTTPReader(body []byte) *derivedHTTPReader {
	return &derivedHTTPReader{Reader: bytes.NewReader(body), desc: recordings.DerivedRecording{ID: "0123456789abcdef0123456789abcdef", RecordingID: "recording-one", State: "ready", Bytes: int64(len(body))}}
}

func TestRecordingDerivedProductionTransferFullHeadRangeAndIfRange(t *testing.T) {
	body := []byte("0123456789abcdef")
	for _, tc := range []struct {
		method, header, ifRange string
		status                  int
		wanted                  string
		length                  int
	}{
		{"GET", "", "", 200, string(body), 16},
		{"HEAD", "", "", 200, "", 16},
		{"GET", "bytes=2-5", "", 206, "2345", 4},
		{"HEAD", "bytes=-3", "", 206, "", 3},
		{"GET", "bytes=-3", "", 206, "def", 3},
		{"GET", "bytes=2-", "", 206, "23456789abcdef", 14},
		{"GET", "bytes=2-5", "unknown-validator", 200, string(body), 16},
	} {
		t.Run(tc.method+tc.header+tc.ifRange, func(t *testing.T) {
			reader := newDerivedHTTPReader(body)
			r := httptest.NewRequest(tc.method, "/synthetic-derived", nil)
			r.Header.Set("Range", tc.header)
			r.Header.Set("If-Range", tc.ifRange)
			w := httptest.NewRecorder()
			serveRecordingDerived(w, r, reader, r.Context(), func() error { return nil })
			if w.Code != tc.status || w.Body.String() != tc.wanted || w.Header().Get("Content-Length") != strconv.Itoa(tc.length) || w.Header().Get("Content-Type") != "video/x-matroska" || w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get("Accept-Ranges") != "bytes" {
				t.Fatalf("unexpected transfer: status=%d headers=%v body=%q", w.Code, w.Header(), w.Body.String())
			}
			if tc.method == "HEAD" && reader.reads != 0 {
				t.Fatal("HEAD consumed media bytes")
			}
		})
	}
}

func TestRecordingDerivedDiscardsLateDeniedOrErroredReadBuffer(t *testing.T) {
	for _, mode := range []string{"late authority", "reader failure", "caller cancellation"} {
		t.Run(mode, func(t *testing.T) {
			var allowed atomic.Bool
			allowed.Store(true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reader := newDerivedHTTPReader(bytes.Repeat([]byte("PRIVATE_MEDIA_"), 8000))
			switch mode {
			case "late authority":
				reader.afterRead = func() { allowed.Store(false) }
			case "reader failure":
				reader.readError = io.ErrUnexpectedEOF
			case "caller cancellation":
				reader.afterRead = cancel
			}
			authorize := func() error {
				if !allowed.Load() {
					return recordings.ErrDerivedAccess
				}
				return ctx.Err()
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "/synthetic-derived", nil)
			var caught any
			func() { defer func() { caught = recover() }(); serveRecordingDerived(w, r, reader, ctx, authorize) }()
			if caught != http.ErrAbortHandler || w.Body.Len() != 0 || reader.reads != 1 {
				t.Fatal("late media bytes escaped the checked read")
			}
			if !bytes.Equal(reader.lastBuffer, make([]byte, len(reader.lastBuffer))) {
				t.Fatal("discarded buffer retained late bytes")
			}
		})
	}
}

func TestRecordingDerivedStopsAfterMidWriteRevocationAndKeepsOriginalDeadline(t *testing.T) {
	var allowed atomic.Bool
	allowed.Store(true)
	expiry := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), expiry)
	defer cancel()
	reader := newDerivedHTTPReader(bytes.Repeat([]byte("PRIVATE_MEDIA_"), 8000))
	w := &exportMutationResponse{ResponseRecorder: httptest.NewRecorder(), change: func() { allowed.Store(false) }}
	authorize := func() error {
		if !allowed.Load() {
			return recordings.ErrDerivedAccess
		}
		return ctx.Err()
	}
	var caught any
	func() {
		defer func() { caught = recover() }()
		serveRecordingDerived(w, httptest.NewRequest("GET", "/synthetic-derived", nil), reader, ctx, authorize)
	}()
	if caught != http.ErrAbortHandler || w.Body.Len() != 32<<10 || !w.deadline.Equal(expiry) || strings.Contains(w.Body.String(), "unavailable") {
		t.Fatal("revoked media transfer continued or appended an error")
	}
}

func TestRecordingDerivedLateSeekDenialCannotCommitMediaHeaders(t *testing.T) {
	allowed := true
	reader := newDerivedHTTPReader([]byte("PRIVATE_MEDIA"))
	reader.afterSeek = func() { allowed = false }
	w := httptest.NewRecorder()
	serveRecordingDerived(w, httptest.NewRequest("GET", "/synthetic-derived", nil), reader, context.Background(), func() error {
		if !allowed {
			return recordings.ErrDerivedAccess
		}
		return nil
	})
	if w.Code != 403 || w.Header().Get("Content-Type") == "video/x-matroska" || strings.Contains(w.Body.String(), "PRIVATE_MEDIA") {
		t.Fatal("late seek published media")
	}
}

func TestRecordingDerivedActualStoredOwnerGateBeforeUnconfiguredMediaService(t *testing.T) {
	for _, mode := range []string{"child", "foreign profile", "scoped session", "revoked session", "missing guard"} {
		t.Run(mode, func(t *testing.T) {
			f := newExportHTTPFixture(t)
			r := f.request(t)
			switch mode {
			case "child":
				p := f.access.profiles["adult"]
				p.IsKidsProfile = true
				f.access.profiles["adult"] = p
			case "foreign profile":
				r.URL.RawQuery = "profileId=foreign"
			case "scoped session":
				f.access.session.Scope = models.SessionScopeStream
			case "revoked session":
				f.access.session.Token = "revoked"
			case "missing guard":
				f.h.SetRecordingStreamAccess(nil)
			}
			w := httptest.NewRecorder()
			f.h.PrepareDerived(w, r)
			if w.Code < 400 || w.Code >= 500 || w.Code == 409 || w.Header().Get("Content-Type") == "video/x-matroska" {
				t.Fatalf("invalid actor reached unconfigured media service: %d", w.Code)
			}
		})
	}
}

func TestRecordingDerivedErrorResponsesNeverExposeInternalDetails(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{
		{recordings.ErrDerivedBusy, 429}, {recordings.ErrDerivedTooLarge, 413}, {recordings.ErrDerivedAccess, 403},
		{recordings.ErrDerivedIncompatible, 409}, {context.DeadlineExceeded, 403}, {errors.New("private path and process details"), 409},
	} {
		w := httptest.NewRecorder()
		writeRecordingDerivedError(w, tc.err)
		if w.Code != tc.status || strings.Contains(w.Body.String(), "private path") {
			t.Fatal("internal error details disclosed")
		}
	}
}

func TestRecordingDerivedOpenAPIActualUnknownAndReadyShape(t *testing.T) {
	validator, err := openapi.NewValidator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	recordingDerivedOpenAPIContract(t, validator)
}

func recordingDerivedOpenAPIContract(t *testing.T, validator *openapi.Validator) {
	t.Helper()
	f := newExportHTTPFixture(t)
	r := f.request(t)
	r.URL.Path = "/api/live/recordings/recording-one/derived"
	w := httptest.NewRecorder()
	f.h.GetDerived(w, r)
	if w.Code != 200 {
		t.Fatalf("unconfigured current source did not expose honest unknown state: %d", w.Code)
	}
	if err := validator.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	expiry := now.Add(time.Hour)
	value := recordings.DerivedRecording{RecordingID: "recording-one", ID: "0123456789abcdef0123456789abcdef", State: "ready", DurationMS: 2000, Bytes: 1000, SourceStatus: models.RecordingStatusFailed, SourceIncomplete: true, GapsOmitted: true, ObservedAt: &now, ExpiresAt: &expiry, PartOffsets: []recordings.DerivedPartOffset{{PartIndex: 1, StartMS: 0, DurationMS: 1000}, {PartIndex: 2, StartMS: 1000, DurationMS: 1000}}}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err = validator.ValidateResponse(r, 200, http.Header{"Content-Type": []string{"application/json"}}, raw); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/live/recordings/recording-one/derived/join", "/api/live/recordings/recording-one/derived/0123456789abcdef0123456789abcdef/stream"} {
		request := httptest.NewRequest("POST", path, nil)
		if strings.HasSuffix(path, "/stream") {
			request.Method = "GET"
		}
		if err = validator.ValidateResponse(request, 403, http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"error":"access denied"}`)); err != nil {
			t.Fatal(err)
		}
	}
	head := httptest.NewRequest(http.MethodHead, "/api/live/recordings/recording-one/derived/0123456789abcdef0123456789abcdef/stream", nil)
	headReply := httptest.NewRecorder()
	serveRecordingDerived(headReply, head, newDerivedHTTPReader([]byte("SYNTHETIC_MEDIA")), head.Context(), func() error { return nil })
	if headReply.Code != 200 || headReply.Body.Len() != 0 {
		t.Fatal("HEAD contract consumed or published media bytes")
	}
	if err := validator.ValidateResponse(head, headReply.Code, headReply.Header(), headReply.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
}

type derivedContextHTTPReader struct {
	*derivedHTTPReader
	ctx context.Context
}

func (r *derivedContextHTTPReader) Context() context.Context { return r.ctx }

type derivedBlockedWriter struct {
	*httptest.ResponseRecorder
	entered     chan struct{}
	interrupted chan struct{}
	once        sync.Once
	mu          sync.Mutex
	deadlines   []time.Time
}

func (w *derivedBlockedWriter) SetWriteDeadline(at time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, at)
	w.mu.Unlock()
	if !at.After(time.Now()) {
		w.once.Do(func() { close(w.interrupted) })
	}
	return nil
}
func (w *derivedBlockedWriter) Write([]byte) (int, error) {
	close(w.entered)
	<-w.interrupted
	return 0, context.DeadlineExceeded
}

func TestRecordingDerivedInnerSourceCancellationInterruptsAndJoinsBlockedWriter(t *testing.T) {
	outer, finish := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer finish()
	inner, cancel := context.WithCancel(outer)
	defer cancel()
	reader := &derivedContextHTTPReader{derivedHTTPReader: newDerivedHTTPReader(bytes.Repeat([]byte("SOURCE_MEDIA_"), 8000)), ctx: inner}
	w := &derivedBlockedWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), interrupted: make(chan struct{})}
	done := make(chan any, 1)
	go func() {
		var caught any
		defer func() { done <- caught }()
		func() {
			defer func() { caught = recover() }()
			serveRecordingDerived(w, httptest.NewRequest("GET", "/synthetic-derived", nil), reader, outer, func() error { return nil })
		}()
	}()
	select {
	case <-w.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("test did not reach the blocked write")
	}
	cancel()
	select {
	case caught := <-done:
		if caught != http.ErrAbortHandler || outer.Err() != nil {
			t.Fatal("source cancellation did not abort independently of original HTTP lifetime")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("source-canceled writer or its callback did not finish")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.deadlines) != 2 || !w.deadlines[0].After(w.deadlines[1]) || w.deadlines[1].After(time.Now()) {
		t.Fatal("original deadline was extended after source cancellation")
	}
	if w.Body.Len() != 0 {
		t.Fatal("blocked canceled writer published media")
	}
}

func TestRecordingDerivedInnerCancellationDuringPostReadAuthorityDiscardsBuffer(t *testing.T) {
	inner, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &derivedContextHTTPReader{derivedHTTPReader: newDerivedHTTPReader(bytes.Repeat([]byte("SOURCE_MEDIA_"), 8000)), ctx: inner}
	w := httptest.NewRecorder()
	calls := 0
	check := func() error {
		calls++
		if calls == 3 {
			cancel()
		} // Original auth succeeds after the source lifetime ends.
		return nil
	}
	var caught any
	func() {
		defer func() { caught = recover() }()
		serveRecordingDerived(w, httptest.NewRequest("GET", "/synthetic-derived", nil), reader, context.Background(), check)
	}()
	if caught != http.ErrAbortHandler || calls != 3 || reader.reads != 1 || w.Body.Len() != 0 {
		t.Fatal("source cancellation during current auth published a late read")
	}
	if !bytes.Equal(reader.lastBuffer, make([]byte, len(reader.lastBuffer))) {
		t.Fatal("late source-canceled read was not cleared")
	}
}
