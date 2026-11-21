package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

var recordingProgressHTTPSlots = make(chan struct{}, 16)

// RecordingProgressHandler is intentionally unregistered. It reuses existing
// adult DVR ownership/PIN policy; neither FamilyMedia nor a client DTO grants it.
type RecordingProgressHandler struct{ *recordingProgressHTTPState }
type recordingProgressHTTPState struct {
	self        *RecordingProgressHandler
	store       *datastore.DataStore
	service     *recordings.Service
	pins        *profileunlock.PGProfilePins
	core        *datastore.RecordingProgressCore
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	mu          sync.Mutex
	closed      bool
	active      map[*recordingProgressHTTPOperation]struct{}
	closeDone   chan struct{}
}
type recordingProgressHTTPOperation struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}
type recordingProgressSnapshotData struct {
	SchemaVersion int                                 `json:"schemaVersion"`
	Timeline      recordings.DerivedTimelineData      `json:"timeline"`
	Progress      datastore.RecordingProgressSnapshot `json:"progress"`
}
type recordingProgressWriteData struct {
	SchemaVersion int                               `json:"schemaVersion"`
	Result        datastore.RecordingProgressResult `json:"result"`
}
type recordingProgressHTTPCommand struct {
	Incarnation, ArtifactID, MappingSHA256 string
	DurationMS                             int64
	Command                                datastore.RecordingProgressCommand
}

func NewRecordingProgressHandler(store *datastore.DataStore, service *recordings.Service, pins *profileunlock.PGProfilePins, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*RecordingProgressHandler, error) {
	if store == nil || service == nil || pins == nil || security == nil || !pins.FamilyMediaReady(store) || !service.DerivedTimelineReady(store) || !store.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, datastore.ErrRecordingProgressUnavailable
	}
	core, err := datastore.NewRecordingProgressCore(store, manager, security, coordinator)
	if err != nil {
		return nil, err
	}
	value := &RecordingProgressHandler{recordingProgressHTTPState: &recordingProgressHTTPState{store: store, service: service, pins: pins, core: core, security: security, coordinator: coordinator, active: map[*recordingProgressHTTPOperation]struct{}{}, closeDone: make(chan struct{})}}
	value.self = value
	return value, nil
}
func (RecordingProgressHandler) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("RecordingProgressHandler(opaque)"))
}
func (RecordingProgressHandler) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*RecordingProgressHandler) UnmarshalJSON([]byte) error {
	return datastore.ErrRecordingProgressUnavailable
}
func (h *RecordingProgressHandler) valid() bool {
	return h != nil && h.recordingProgressHTTPState != nil && h.self == h && h.store != nil && h.service != nil && h.pins != nil && h.core != nil && h.active != nil && h.closeDone != nil
}
func (h *RecordingProgressHandler) admit(ctx context.Context) *recordingProgressHTTPOperation {
	if !h.valid() || ctx == nil || ctx.Err() != nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || len(h.active) >= 2 {
		return nil
	}
	select {
	case recordingProgressHTTPSlots <- struct{}{}:
	default:
		return nil
	}
	work, cancel := context.WithTimeout(ctx, 30*time.Second)
	op := &recordingProgressHTTPOperation{work, cancel, make(chan struct{})}
	h.active[op] = struct{}{}
	return op
}
func (h *RecordingProgressHandler) finish(op *recordingProgressHTTPOperation) {
	op.cancel()
	h.mu.Lock()
	delete(h.active, op)
	<-recordingProgressHTTPSlots
	close(op.done)
	h.mu.Unlock()
}
func (h *RecordingProgressHandler) Close() {
	if !h.valid() {
		return
	}
	h.mu.Lock()
	if h.closed {
		done := h.closeDone
		h.mu.Unlock()
		<-done
		return
	}
	h.closed = true
	ops := make([]*recordingProgressHTTPOperation, 0, len(h.active))
	for op := range h.active {
		ops = append(ops, op)
	}
	h.mu.Unlock()
	for _, op := range ops {
		op.cancel()
	}
	for _, op := range ops {
		<-op.done
	}
	close(h.closeDone)
}

func (h *RecordingProgressHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.valid() {
		http.Error(w, "recording progress unavailable", 503)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", 405)
		return
	}
	id := mux.Vars(r)["recordingID"]
	if r.URL == nil || r.URL.RawQuery != "" || r.URL.ForceQuery || len(mux.Vars(r)) != 1 || id == "" || id == "." || id == ".." || len(id) > 128 || strings.TrimSpace(id) != id || strings.ContainsAny(id, "/\\\x00") || len(r.TransferEncoding) != 0 {
		http.Error(w, "invalid recording progress request", 400)
		return
	}
	for key := range r.Header {
		switch strings.ToLower(key) {
		case "range", "if-range", "if-match", "if-none-match", "if-modified-since", "if-unmodified-since":
			http.Error(w, "recording progress conditions unsupported", 400)
			return
		}
	}
	if r.Method == http.MethodGet && (r.ContentLength != 0 || r.Body != nil && r.Body != http.NoBody) {
		http.Error(w, "body unsupported", 400)
		return
	}
	if r.Method == http.MethodPost && !recordingProgressJSONFraming(r) {
		http.Error(w, "invalid recording progress body", 400)
		return
	}
	op := h.admit(r.Context())
	if op == nil {
		http.Error(w, "recording progress busy", 429)
		return
	}
	var hold *settingscommit.ProgressFileHold
	var unlink func()
	var cancelSource context.CancelFunc
	var intent *datastore.RecordingProgressIntent
	var response *recordingProgressHTTPResponse
	defer func() {
		if response != nil {
			response.close()
		} else {
			op.cancel()
		}
		if cancelSource != nil {
			cancelSource()
		}
		if intent != nil {
			intent.Close()
		}
		if unlink != nil {
			unlink()
		}
		if hold != nil {
			_ = hold.Close()
		}
		h.finish(op)
	}()
	var err error
	var observation *config.SecurityPolicyObservation
	if r.Method == http.MethodGet {
		observation, err = h.security.CapturePolicy(op.ctx)
		if err != nil {
			writeRecordingProgressHTTPError(w, err)
			return
		}
	}
	response, err = newRecordingProgressHTTPResponse(w, op)
	if err != nil {
		http.Error(w, "recording progress transport unavailable", 503)
		return
	}
	request := r.WithContext(op.ctx)
	var command recordingProgressHTTPCommand
	if r.Method == http.MethodPost {
		if err = response.readDeadline(); err != nil {
			http.Error(w, "recording progress transport unavailable", 503)
			return
		}
		raw, readErr := io.ReadAll(io.LimitReader(r.Body, 4097))
		closeErr := r.Body.Close()
		if readErr != nil || closeErr != nil || len(raw) > 4096 || int64(len(raw)) != r.ContentLength || op.ctx.Err() != nil {
			clear(raw)
			http.Error(w, "invalid recording progress body", 400)
			return
		}
		command, err = decodeRecordingProgressCommand(raw)
		clear(raw)
		if err != nil {
			http.Error(w, "invalid recording progress command", 400)
			return
		}
	}
	if observation == nil {
		observation, err = h.security.CapturePolicy(op.ctx)
		if err != nil {
			writeRecordingProgressHTTPError(w, err)
			return
		}
	}
	hold, err = h.coordinator.TryProgressFileHold(op.ctx)
	if err != nil {
		writeRecordingProgressHTTPError(w, err)
		return
	}
	// Native cancellation reaches real source/core/response work, not just a
	// later Boolean. Its callback is joined before the same hold is refunded.
	unlink = recordingProgressHTTPCancelLink(hold.Context(), op.cancel)
	fence, err := h.security.BindPolicy(op.ctx, observation, hold)
	if err != nil {
		writeRecordingProgressHTTPError(w, err)
		return
	}
	access := NewRecordingStreamAccess(h.service, nil, nil, nil, nil, func() (config.SecuritySettings, error) {
		flag, e := fence.EnforceProfilePins()
		return config.SecuritySettings{EnforceProfilePins: flag}, e
	})
	access.SetPGProfilePins(h.pins)
	guard := &RecordingsHandler{service: h.service, recordingAccess: access}
	before, ok := access.current(request, "recording:"+id)
	if !ok {
		_, _ = access.Authorize(w, request, "recording:"+id)
		return
	}
	ctx, cancel, authorize, ok := guard.recordingDerivedAuthority(w, request, false)
	if !ok {
		return
	}
	cancelSource = cancel
	first, err := h.store.ReadRecordingTimelineIdentity(ctx, id)
	if err != nil {
		writeRecordingProgressHTTPError(w, err)
		return
	}
	if first.OwnerProfileID() != before.owner.ProfileID || !first.CreatedAt().Equal(before.owner.CreatedAt) {
		writeRecordingProgressHTTPError(w, datastore.ErrRecordingProgressChanged)
		return
	}
	var descriptor recordings.DerivedRecording
	var history recordings.DerivedHistoryData
	if r.Method == http.MethodPost {
		descriptor, history, err = h.service.GetDerivedWithHistoryData(ctx, id, authorize)
	} else {
		descriptor, err = h.service.GetDerived(ctx, id, authorize)
	}
	if err != nil {
		writeRecordingProgressHTTPError(w, err)
		return
	}
	timeline, err := recordings.ProjectDerivedTimelineData(first.Incarnation(), descriptor, time.Now())
	if err != nil || timeline.State != "known" || timeline.ObservedAt == nil || timeline.ExpiresAt == nil {
		writeRecordingProgressHTTPError(w, datastore.ErrRecordingProgressChanged)
		return
	}
	source := &recordingProgressHTTPSource{handler: h, ctx: ctx, id: id, identity: first, timeline: timeline, authorize: authorize}
	if err = source.check(); err != nil {
		writeRecordingProgressHTTPError(w, err)
		return
	}
	actual := datastore.RecordingProgressObservation{ArtifactID: timeline.ArtifactID, MappingSHA256: timeline.MappingRevision, DurationMS: timeline.DurationMS, ObservedAt: *timeline.ObservedAt, ExpiresAt: *timeline.ExpiresAt}
	// Authorization's private profile query remains inside its closure. Core
	// receives the actual original method/URL with its query-free current context.
	currentRequest := request.WithContext(ctx)
	var value any
	var validUntil time.Time
	if r.Method == http.MethodGet {
		snapshot, e := h.core.ReadSnapshot(currentRequest, first, actual)
		if e != nil {
			writeRecordingProgressHTTPError(w, e)
			return
		}
		validUntil = snapshot.ValidUntil
		value = recordingProgressSnapshotData{1, timeline, snapshot}
	} else {
		if command.Incarnation != timeline.Incarnation || command.ArtifactID != timeline.ArtifactID || command.MappingSHA256 != timeline.MappingRevision || command.DurationMS != timeline.DurationMS {
			writeRecordingProgressHTTPError(w, datastore.ErrRecordingProgressChanged)
			return
		}
		if history.RecordingID != id || history.ArtifactID != descriptor.ID {
			writeRecordingProgressHTTPError(w, datastore.ErrRecordingProgressChanged)
			return
		}
		historical := datastore.RecordingProgressHistoryObservation{SourceStatus: string(descriptor.SourceStatus), SourceIncomplete: descriptor.SourceIncomplete, GapsOmitted: descriptor.GapsOmitted, OutputBytes: descriptor.Bytes, LedgerSHA256: history.LedgerSHA256, ToolSHA256: history.ToolSHA256, TransformSHA256: history.TransformSHA256, OutputSHA256: history.OutputSHA256}
		for _, span := range descriptor.PartOffsets {
			historical.Spans = append(historical.Spans, [3]int64{int64(span.PartIndex), span.StartMS, span.DurationMS})
		}
		for _, input := range history.Inputs {
			historical.Inputs = append(historical.Inputs, datastore.RecordingProgressHistoryInput{PartIndex: int64(input.PartIndex), ByteLength: input.ByteLength, SHA256: input.SHA256})
		}
		intent, err = h.core.PrepareWithHistory(currentRequest, first, actual, command.Command, historical)
		if err != nil {
			writeRecordingProgressHTTPError(w, err)
			return
		}
		result, e := h.core.Commit(intent)
		if e != nil {
			writeRecordingProgressHTTPError(w, e)
			return
		}
		validUntil = result.ValidUntil
		value = recordingProgressWriteData{1, result}
	}
	// ValidUntil is output DATA from the actual locked transaction, never input
	// permission or a substitute deadline for absent/expired source evidence.
	response.publish(source, value, validUntil)
}

func recordingProgressJSONFraming(r *http.Request) bool {
	if r.ContentLength < 1 || r.ContentLength > 4096 || r.Body == nil || r.Body == http.NoBody {
		return false
	}
	var values []string
	for key, v := range r.Header {
		if strings.EqualFold(key, "Content-Type") {
			values = append(values, v...)
		}
	}
	if len(values) != 1 {
		return false
	}
	kind, params, err := mime.ParseMediaType(values[0])
	if err != nil || kind != "application/json" {
		return false
	}
	for k, v := range params {
		if k != "charset" || !strings.EqualFold(v, "utf-8") {
			return false
		}
	}
	return true
}
func decodeRecordingProgressCommand(raw []byte) (value recordingProgressHTTPCommand, err error) {
	fail := func() (recordingProgressHTTPCommand, error) {
		return recordingProgressHTTPCommand{}, errors.New("invalid recording progress command")
	}
	if len(raw) == 0 || len(raw) > 4096 || !utf8.Valid(raw) {
		return fail()
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return fail()
	}
	fields := map[string]json.RawMessage{}
	allowed := map[string]bool{"action": true, "expectedRevision": true, "writerGeneration": true, "incarnation": true, "artifactId": true, "mappingSha256": true, "durationMs": true, "positionMs": true}
	for decoder.More() {
		token, err = decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || !allowed[key] || fields[key] != nil {
			return fail()
		}
		var field json.RawMessage
		if decoder.Decode(&field) != nil {
			return fail()
		}
		fields[key] = field
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || len(fields) != len(allowed) {
		return fail()
	}
	if _, err = decoder.Token(); err != io.EOF {
		return fail()
	}
	text := func(key string) (string, bool) {
		var v *string
		if json.Unmarshal(fields[key], &v) != nil || v == nil {
			return "", false
		}
		return *v, true
	}
	action, ok := text("action")
	if !ok {
		return fail()
	}
	value.Command.Action = datastore.RecordingProgressAction(action)
	for key, destination := range map[string]*string{"incarnation": &value.Incarnation, "artifactId": &value.ArtifactID, "mappingSha256": &value.MappingSHA256} {
		v, ok := text(key)
		if !ok || v == "" {
			return fail()
		}
		*destination = v
	}
	for key, destination := range map[string]*int64{"expectedRevision": &value.Command.ExpectedRevision, "writerGeneration": &value.Command.WriterGeneration} {
		v, ok := text(key)
		if !ok {
			return fail()
		}
		n, e := strconv.ParseInt(v, 10, 64)
		if e != nil || n < 0 || strconv.FormatInt(n, 10) != v {
			return fail()
		}
		*destination = n
	}
	var duration, position *int64
	if json.Unmarshal(fields["durationMs"], &duration) != nil || duration == nil || json.Unmarshal(fields["positionMs"], &position) != nil || position == nil || *duration <= 0 || *duration > 43200000 || *position < 0 || *position > *duration {
		return fail()
	}
	value.DurationMS = *duration
	value.Command.PositionMS = *position
	switch value.Command.Action {
	case datastore.RecordingProgressFromStart, datastore.RecordingProgressResume:
		if *position != 0 {
			return fail()
		}
	case datastore.RecordingProgressTouch:
	default:
		return fail()
	}
	return value, nil
}

func recordingProgressHTTPCancelLink(ctx context.Context, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { cancel(); close(done) })
	return func() {
		if stop() {
			close(done)
		}
		<-done
	}
}
func writeRecordingProgressHTTPError(w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, datastore.ErrRecordingProgressChanged), errors.Is(err, recordings.ErrDerivedUnavailable), errors.Is(err, recordings.ErrDerivedTimelineData):
		code = 409
	case errors.Is(err, datastore.ErrRecordingProgressLocked), errors.Is(err, recordings.ErrDerivedAccess), errors.Is(err, recordings.ErrRecordingExportAccess), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code = 403
	case errors.Is(err, datastore.ErrRecordingProgressBusy), errors.Is(err, settingscommit.ErrBusy), errors.Is(err, recordings.ErrDerivedBusy):
		code = 429
	}
	w.Header().Set("Cache-Control", "private, no-store")
	http.Error(w, "recording progress unavailable", code)
}

type recordingProgressHTTPSource struct {
	mu        sync.Mutex
	handler   *RecordingProgressHandler
	ctx       context.Context
	id        string
	identity  datastore.RecordingTimelineIdentity
	timeline  recordings.DerivedTimelineData
	authorize func() error
}

func (s *recordingProgressHTTPSource) check() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.authorize() != nil {
		return datastore.ErrRecordingProgressChanged
	}
	current, err := s.handler.store.ReadRecordingTimelineIdentity(s.ctx, s.id)
	if err != nil || !s.identity.Same(current) {
		return datastore.ErrRecordingProgressChanged
	}
	descriptor, err := s.handler.service.GetDerived(s.ctx, s.id, s.authorize)
	if err != nil {
		return err
	}
	if s.authorize() != nil {
		return datastore.ErrRecordingProgressChanged
	}
	latest, err := s.handler.store.ReadRecordingTimelineIdentity(s.ctx, s.id)
	if err != nil || !s.identity.Same(latest) {
		return datastore.ErrRecordingProgressChanged
	}
	timeline, err := recordings.ProjectDerivedTimelineData(s.identity.Incarnation(), descriptor, time.Now())
	if err != nil {
		return datastore.ErrRecordingProgressChanged
	}
	a, e := json.Marshal(s.timeline)
	if e != nil {
		return e
	}
	b, e := json.Marshal(timeline)
	if e != nil || !bytes.Equal(a, b) || s.ctx.Err() != nil || s.authorize() != nil {
		return datastore.ErrRecordingProgressChanged
	}
	return nil
}

type recordingProgressHTTPResponse struct {
	writer       http.ResponseWriter
	controller   *http.ResponseController
	op           *recordingProgressHTTPOperation
	mu           sync.Mutex
	canceled     bool
	stop         func() bool
	callbackDone chan struct{}
	watchDone    chan struct{}
}

func newRecordingProgressHTTPResponse(w http.ResponseWriter, op *recordingProgressHTTPOperation) (*recordingProgressHTTPResponse, error) {
	controller := http.NewResponseController(w)
	deadline, ok := op.ctx.Deadline()
	if !ok || controller.SetWriteDeadline(deadline) != nil || !recordingProgressFlushSupported(w) {
		return nil, errors.New("response deadlines unavailable")
	}
	value := &recordingProgressHTTPResponse{writer: w, controller: controller, op: op, callbackDone: make(chan struct{})}
	value.stop = context.AfterFunc(op.ctx, func() {
		value.mu.Lock()
		value.canceled = true
		_ = controller.SetReadDeadline(time.Now())
		_ = controller.SetWriteDeadline(time.Now())
		value.mu.Unlock()
		close(value.callbackDone)
	})
	return value, nil
}
func recordingProgressFlushSupported(w http.ResponseWriter) bool {
	for i := 0; i < 16; i++ {
		if _, ok := w.(interface{ FlushError() error }); ok {
			return true
		}
		if _, ok := w.(http.Flusher); ok {
			return true
		}
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		w = unwrapper.Unwrap()
		if w == nil {
			return false
		}
	}
	return false
}
func (r *recordingProgressHTTPResponse) deadline(at time.Time, read bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.canceled || r.op.ctx.Err() != nil || at.IsZero() || !at.After(time.Now()) {
		return context.Canceled
	}
	var err error
	if read {
		err = r.controller.SetReadDeadline(at)
	} else {
		err = r.controller.SetWriteDeadline(at)
	}
	if !at.After(time.Now()) {
		return context.DeadlineExceeded
	}
	if r.op.ctx.Err() != nil {
		r.canceled = true
		_ = r.controller.SetReadDeadline(time.Now())
		_ = r.controller.SetWriteDeadline(time.Now())
		return context.Canceled
	}
	return err
}
func (r *recordingProgressHTTPResponse) readDeadline() error {
	at, ok := r.op.ctx.Deadline()
	if !ok {
		return context.Canceled
	}
	return r.deadline(at, true)
}
func (r *recordingProgressHTTPResponse) publish(source *recordingProgressHTTPSource, value any, validUntil time.Time) {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 64<<10 || validUntil.IsZero() || !validUntil.After(time.Now()) || source.timeline.ExpiresAt == nil {
		writeRecordingProgressHTTPError(r.writer, datastore.ErrRecordingProgressChanged)
		return
	}
	defer clear(raw)
	deadline, ok := source.ctx.Deadline()
	if !ok {
		writeRecordingProgressHTTPError(r.writer, datastore.ErrRecordingProgressChanged)
		return
	}
	for _, at := range []time.Time{validUntil, *source.timeline.ExpiresAt} {
		if at.Before(deadline) {
			deadline = at
		}
	}
	if err = r.deadline(deadline, false); err != nil || source.check() != nil {
		writeRecordingProgressHTTPError(r.writer, datastore.ErrRecordingProgressChanged)
		return
	}
	// The fixed real deadline handles known expiry exactly. This joined watcher
	// additionally observes early revocation/source changes; no TX remains active.
	r.watchDone = make(chan struct{})
	go func() {
		defer close(r.watchDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.op.ctx.Done():
				return
			case <-ticker.C:
				if source.check() != nil {
					r.op.cancel()
					return
				}
			}
		}
	}()
	r.writer.Header().Set("Content-Type", "application/json")
	r.writer.Header().Set("Cache-Control", "private, no-store")
	r.writer.Header().Set("X-Content-Type-Options", "nosniff")
	r.writer.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	for len(raw) > 0 {
		if !validUntil.After(time.Now()) || source.check() != nil {
			r.op.cancel()
			return
		}
		n := min(len(raw), 4096)
		written, e := r.writer.Write(raw[:n])
		if e != nil || written != n {
			r.op.cancel()
			return
		}
		if e = r.controller.Flush(); e != nil {
			r.op.cancel()
			return
		}
		if !validUntil.After(time.Now()) || source.check() != nil {
			r.op.cancel()
			return
		}
		raw = raw[n:]
	}
}
func (r *recordingProgressHTTPResponse) close() {
	// Detach successful-response cancellation before ending our work context; a
	// completed Content-Length response must not be broken by our own cleanup.
	if r.stop() {
		close(r.callbackDone)
	}
	<-r.callbackDone
	r.op.cancel()
	if r.watchDone != nil {
		<-r.watchDone
	}
}
