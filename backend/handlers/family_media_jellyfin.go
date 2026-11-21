package handlers

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/services/profileunlock"
	"novastream/services/remotemedia"
)

// This private handler installs no route or legacy provider delegation.
type familyJellyfinRawHandler struct {
	installation *FamilyMediaInstallation
	sources      *remotemedia.FamilyJellyfinSources
	store        *datastore.DataStore
	manager      *config.Manager
	bridge       func(context.Context, func()) func()
	completion   func(<-chan struct{})
	original     context.Context
	mu           sync.Mutex
	closed       bool
	active       map[*familyJellyfinRawEntry]struct{}
	once         sync.Once
	done         chan struct{}
}
type familyJellyfinRawEntry struct {
	handler     *familyJellyfinRawHandler
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	once        sync.Once
	stopRuntime func()
}

func newFamilyJellyfinRawHandler(deps FamilyMediaDependencies, manager *config.Manager, remote *remotemedia.Service) (*familyJellyfinRawHandler, error) {
	installed, err := InstallFamilyMedia(deps)
	if err != nil {
		return nil, err
	}
	return newFamilyJellyfinRawHandlerForInstallation(installed, manager, remote)
}
func newFamilyJellyfinRawHandlerForInstallation(installed *FamilyMediaInstallation, manager *config.Manager, remote *remotemedia.Service) (*familyJellyfinRawHandler, error) {
	if !installed.valid() {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	sources, err := remotemedia.NewFamilyJellyfinSources(installed.deps.Store, manager, remote)
	if err != nil || !sources.ReadyFor(installed.deps.Store, manager) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	return &familyJellyfinRawHandler{installation: installed, sources: sources, store: installed.deps.Store, manager: manager, original: installed.original, bridge: familyMediaAfterFunc, active: make(map[*familyJellyfinRawEntry]struct{}), done: make(chan struct{})}, nil
}

func (h *familyJellyfinRawHandler) admit(ctx context.Context) (*familyJellyfinRawEntry, error) {
	if h == nil || ctx == nil || ctx.Err() != nil {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || h.active == nil || h.original != nil && h.original.Err() != nil {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	if len(h.active) >= 8 {
		return nil, profileunlock.ErrFamilyMediaBusy
	}
	life, cancel := context.WithCancel(ctx)
	entry := &familyJellyfinRawEntry{handler: h, ctx: life, cancel: cancel, done: make(chan struct{})}
	if h.original != nil {
		entry.stopRuntime = familyMediaAfterFunc(h.original, cancel)
	}
	h.active[entry] = struct{}{}
	return entry, nil
}
func (entry *familyJellyfinRawEntry) finish() {
	entry.once.Do(func() {
		if entry.stopRuntime != nil {
			entry.stopRuntime()
		}
		entry.cancel()
		entry.handler.mu.Lock()
		delete(entry.handler.active, entry)
		close(entry.done)
		entry.handler.mu.Unlock()
	})
}

func (h *familyJellyfinRawHandler) signalClose() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.closed = true
	entries := make([]*familyJellyfinRawEntry, 0, len(h.active))
	for entry := range h.active {
		entries = append(entries, entry)
	}
	h.mu.Unlock()
	for _, entry := range entries {
		entry.cancel()
	}
}

// Close is external and joins actual requests after signalling every entry.
func (h *familyJellyfinRawHandler) Close() {
	if h == nil || h.done == nil {
		return
	}
	h.once.Do(func() {
		h.signalClose()
		h.mu.Lock()
		entries := make([]*familyJellyfinRawEntry, 0, len(h.active))
		for entry := range h.active {
			entries = append(entries, entry)
		}
		h.mu.Unlock()
		for _, entry := range entries {
			<-entry.done
		}
		close(h.done)
	})
	<-h.done
}

func familyJellyfinRawItem(r *http.Request) (string, error) {
	if r == nil || r.URL == nil || r.Context().Err() != nil || len(r.URL.RawQuery) > 4096 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody {
		return "", profileunlock.ErrFamilyMediaChanged
	}
	item := mux.Vars(r)["itemID"]
	if item == "" || len(item) > 128 {
		return "", profileunlock.ErrFamilyMediaChanged
	}
	for _, character := range item {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_') {
			return "", profileunlock.ErrFamilyMediaChanged
		}
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return "", profileunlock.ErrFamilyMediaChanged
	}
	selected := ""
	for key, values := range query {
		if key != "profileId" && key != "userId" || len(values) != 1 || values[0] == "" || strings.TrimSpace(values[0]) != values[0] || len(values[0]) > 256 || selected != "" && selected != values[0] {
			return "", profileunlock.ErrFamilyMediaChanged
		}
		selected = values[0]
	}
	for _, header := range []string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		if len(r.Header.Values(header)) != 0 {
			return "", profileunlock.ErrFamilyMediaChanged
		}
	}
	return item, nil
}

func familyJellyfinRawDeny(w http.ResponseWriter, err error) {
	for _, header := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "Content-Disposition", "ETag", "Last-Modified", "Location"} {
		w.Header().Del(header)
	}
	familyMediaError(w, err)
}
func familyJellyfinRawCurrent(c *familyMediaConsumer, stream *remotemedia.FamilyJellyfinStream) error {
	if c == nil || stream == nil || c.Check(c.Context()) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	if err := stream.Check(c.Context()); err != nil {
		if !errors.Is(err, profileunlock.ErrFamilyMediaBusy) && c.Context().Err() == nil {
			c.runtime.request.cancel()
		}
		return err
	}
	return c.Check(c.Context())
}

type familyJellyfinRawProducer struct {
	source *profileunlock.FamilyJellyfinSource
	stream *remotemedia.FamilyJellyfinStream
	bridge func()
	once   sync.Once
	done   chan struct{}
	err    error
}

func (p *familyJellyfinRawProducer) stop() {
	p.once.Do(func() {
		if p.stream != nil {
			p.err = p.stream.Close()
		}
		if p.bridge != nil {
			p.bridge()
		}
		if p.source != nil {
			p.source.Close()
		}
		close(p.done)
	})
	<-p.done
}

// This already-open wrapper creates no HTTP operation or source authority.
// Its Close shares the exact producer completion used by runtime.openCleanup,
// so consumer admission cannot retire before both producer and writer finish.
type familyJellyfinRawBody struct{ producer *familyJellyfinRawProducer }

func (body *familyJellyfinRawBody) Read(p []byte) (int, error) {
	return body.producer.stream.Read(p)
}
func (body *familyJellyfinRawBody) Close() error {
	body.producer.stop()
	return body.producer.err
}

func (h *familyJellyfinRawHandler) open(runtime *familyMediaRuntime, setup context.Context, item string) (*remotemedia.FamilyJellyfinStream, io.ReadCloser, error) {
	if setup == nil {
		return nil, nil, profileunlock.ErrFamilyMediaChanged
	}
	// Cover openCleanup's preliminary PG work before source acquisition too.
	joinSetup := familyMediaAfterFunc(setup, runtime.request.cancel)
	defer joinSetup()
	owner, ok := runtime.request.owner.original.(interface {
		CaptureJellyfinSource(context.Context, string) (*profileunlock.FamilyJellyfinSource, error)
	})
	if !ok {
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	producer := &familyJellyfinRawProducer{done: make(chan struct{})}
	if h.completion != nil {
		// Passive completion observation exports no source or permission.
		h.completion(producer.done)
	}
	_, err := runtime.openCleanup(producer.done, func(context.Context) (func(), error) {
		var err error
		producer.source, err = owner.CaptureJellyfinSource(setup, item)
		if err != nil || producer.source == nil {
			producer.stop()
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		producer.stream, err = h.sources.Open(setup, producer.source)
		if err != nil || producer.stream == nil {
			producer.stop()
			if err == nil {
				err = profileunlock.ErrFamilyMediaChanged
			}
			return nil, err
		}
		// Source/config/upstream invalidation must reach a blocked downstream
		// writer even while the original PG household remains otherwise valid.
		producer.bridge = h.bridge(producer.stream.Context(), runtime.request.cancel)
		return producer.stop, nil
	})
	if err != nil {
		producer.stop()
		return nil, nil, err
	}
	return producer.stream, &familyJellyfinRawBody{producer: producer}, nil
}

func (h *familyJellyfinRawHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.serveHTTP(w, r)
}

func (h *familyJellyfinRawHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Allow", "GET, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if h == nil || h.installation == nil || h.installation.Boundary == nil || h.sources == nil || h.bridge == nil || !h.sources.ReadyFor(h.store, h.manager) {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	entry, err := h.admit(r.Context())
	if err != nil {
		familyJellyfinRawDeny(w, err)
		return
	}
	defer entry.finish() // q.Close and physical final Flush precede refund.
	if h.original != nil {
		output, stop, e := familyKnownRawOutput(w, entry.ctx)
		if e != nil {
			familyJellyfinRawDeny(w, e)
			return
		}
		defer stop()
		completed := false
		defer func() {
			if completed && output.FlushError() != nil {
				panic(http.ErrAbortHandler)
			}
		}()
		h.serveEntry(output, r.WithContext(entry.ctx), entry)
		completed = true
		return
	}
	h.serveEntry(w, r.WithContext(entry.ctx), entry)
}
func (h *familyJellyfinRawHandler) serveEntry(w http.ResponseWriter, r *http.Request, entry *familyJellyfinRawEntry) {
	item, err := familyJellyfinRawItem(r)
	if err != nil {
		http.Error(w, "Invalid media download request.", http.StatusBadRequest)
		return
	}
	q, ok := h.installation.Boundary.Begin(w, r, "")
	if !ok {
		return
	}
	defer q.Close()
	h.serveCaptured(w, r, q, item)
	if err := http.NewResponseController(w).Flush(); err != nil {
		panic(http.ErrAbortHandler)
	}
}
func (h *familyJellyfinRawHandler) serveCaptured(w http.ResponseWriter, r *http.Request, q *FamilyMediaRequest, item string) {
	runtime, err := q.adoptRuntime(r)
	if err != nil {
		familyJellyfinRawDeny(w, err)
		return
	}
	consumer, _, err := runtime.beginConsumer(w, r, q.target)
	if err != nil {
		familyJellyfinRawDeny(w, err)
		return
	}
	stream, readyBody, err := h.open(runtime, consumer.Context(), item)
	if err != nil {
		familyJellyfinRawDeny(w, err)
		return
	}
	if familyJellyfinRawCurrent(consumer, stream) != nil {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	body, err := consumer.OpenBody(func(context.Context) (io.ReadCloser, error) {
		if familyJellyfinRawCurrent(consumer, stream) != nil {
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		return readyBody, nil
	})
	if err != nil {
		familyJellyfinRawDeny(w, err)
		return
	}

	info := stream.Info()
	if info.Length <= 0 || info.Length > 8<<30 || stream.ExpiresAt().IsZero() || familyJellyfinRawCurrent(consumer, stream) != nil {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	writer, err := consumer.ResponseWriter(w)
	if err != nil {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	deadline := consumer.ExpiresAt()
	if stream.ExpiresAt().Before(deadline) {
		deadline = stream.ExpiresAt()
	}
	if !deadline.After(time.Now()) || http.NewResponseController(writer).SetWriteDeadline(deadline) != nil || familyJellyfinRawCurrent(consumer, stream) != nil {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.FormatInt(info.Length, 10))
	writer.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": "media-download.bin"}))
	for _, header := range []string{"Accept-Ranges", "Content-Range", "ETag", "Last-Modified", "Location"} {
		writer.Header().Del(header)
	}
	if familyJellyfinRawCurrent(consumer, stream) != nil {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	writer.WriteHeader(http.StatusOK)
	if familyJellyfinRawCurrent(consumer, stream) != nil || familyJellyfinRawCopy(consumer, stream, writer, body, info.Length) != nil {
		panic(http.ErrAbortHandler)
	}
	if e := http.NewResponseController(writer).Flush(); e != nil || familyJellyfinRawCurrent(consumer, stream) != nil {
		panic(http.ErrAbortHandler)
	}
}

func familyJellyfinRawCopy(c *familyMediaConsumer, stream *remotemedia.FamilyJellyfinStream, w http.ResponseWriter, body io.Reader, length int64) error {
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	for remaining := length; remaining > 0; {
		if familyJellyfinRawCurrent(c, stream) != nil {
			return profileunlock.ErrFamilyMediaChanged
		}
		amount := min(int64(len(buffer)), remaining)
		n, err := body.Read(buffer[:amount])
		if n < 0 || int64(n) > amount || err != nil && err != io.EOF || familyJellyfinRawCurrent(c, stream) != nil {
			clear(buffer)
			return profileunlock.ErrFamilyMediaChanged
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		written, writeErr := w.Write(buffer[:n])
		clear(buffer)
		if written != n || writeErr != nil || familyJellyfinRawCurrent(c, stream) != nil {
			return profileunlock.ErrFamilyMediaChanged
		}
		remaining -= int64(n)
		if err != nil && remaining > 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}
