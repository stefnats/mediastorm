package handlers

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/datastore"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
)

// This private handler is deliberately absent from Main and all public routes.
// Only an actual same-store LOCAL opener can satisfy the platform port.
type familyLocalRawOpener interface {
	FamilyLocalSourceReady(*datastore.DataStore) bool
	OpenFamilyLocalSource(context.Context, *profileunlock.FamilyLocalSource) (profileunlock.FamilyLocalFile, error)
}
type familyLocalRawHandler struct {
	installation *FamilyMediaInstallation
	opener       familyLocalRawOpener
	mu           sync.Mutex
	life         context.Context
	cancel       context.CancelFunc
	active       sync.WaitGroup
	closed       bool
	once         sync.Once
	done         chan struct{}
}

func familyLocalRawDeny(w http.ResponseWriter, err error) {
	for _, key := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "Content-Disposition"} {
		w.Header().Del(key)
	}
	familyMediaError(w, err)
}

func newFamilyLocalRawHandler(deps FamilyMediaDependencies) (*familyLocalRawHandler, error) {
	installed, err := InstallFamilyMedia(deps)
	if err != nil {
		return nil, err
	}
	return newFamilyLocalRawHandlerForInstallation(installed, deps.Local)
}
func newFamilyLocalRawHandlerForInstallation(installed *FamilyMediaInstallation, local *localmedia.Service) (*familyLocalRawHandler, error) {
	if !installed.valid() || local == nil || installed.deps.Local != local {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	opener, ok := any(local).(familyLocalRawOpener)
	if !ok || !opener.FamilyLocalSourceReady(installed.deps.Store) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	original := installed.original
	if original == nil {
		original = context.Background()
	} // Isolated legacy constructor only.
	life, cancel := context.WithCancel(original)
	return &familyLocalRawHandler{installation: installed, opener: opener, life: life, cancel: cancel, done: make(chan struct{})}, nil
}
func (h *familyLocalRawHandler) begin(caller context.Context) (context.Context, func(), error) {
	if h == nil || caller == nil || caller.Err() != nil {
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	h.mu.Lock()
	if h.closed || h.life == nil || h.life.Err() != nil {
		h.mu.Unlock()
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	h.active.Add(1)
	h.mu.Unlock()
	ctx, cancel := context.WithCancel(caller)
	stop := familyMediaAfterFunc(h.life, cancel)
	var once sync.Once
	finish := func() { once.Do(func() { stop(); cancel(); h.active.Done() }) }
	return ctx, finish, nil
}
func (h *familyLocalRawHandler) signalClose() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.closed = true
	if h.cancel != nil {
		h.cancel()
	}
	h.mu.Unlock()
}
func (h *familyLocalRawHandler) Close() {
	if h == nil || h.done == nil {
		return
	}
	h.once.Do(func() { h.signalClose(); h.active.Wait(); close(h.done) })
	<-h.done
}
func (h *familyLocalRawHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r == nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("Allow", "GET, HEAD, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "GET or HEAD is required.", http.StatusMethodNotAllowed)
		return
	}
	ctx, finish, err := h.begin(r.Context())
	if err != nil {
		familyLocalRawDeny(w, err)
		return
	}
	defer finish()
	// Existing isolated fixtures keep their original response semantics.
	if h.installation.original == nil {
		h.serveHTTP(w, r.WithContext(ctx))
		return
	}
	output, stop, err := familyKnownRawOutput(w, ctx)
	if err != nil {
		familyLocalRawDeny(w, err)
		return
	}
	defer stop()
	completed := false
	defer func() {
		if completed && output.FlushError() != nil {
			panic(http.ErrAbortHandler)
		}
	}()
	h.serveHTTP(output, r.WithContext(ctx))
	completed = true
}

func familyLocalRawItem(r *http.Request) (string, error) {
	if r == nil || r.URL == nil || r.Context().Err() != nil || len(r.URL.RawQuery) > 4096 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody {
		return "", profileunlock.ErrFamilyMediaChanged
	}
	item := mux.Vars(r)["itemID"]
	if item == "" || len(item) > 128 {
		return "", profileunlock.ErrFamilyMediaChanged
	}
	for _, c := range item {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
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
	if len(r.Header.Values("Range")) > 1 || len(r.Header.Values("If-Range")) > 1 || len(r.Header.Get("Range")) > 128 || len(r.Header.Get("If-Range")) > 512 {
		return "", profileunlock.ErrFamilyMediaChanged
	}
	return item, nil
}

func familyLocalRawRange(raw, ifRange string, size int64, head bool) (start, length int64, partial bool, err error) {
	if size < 0 {
		return 0, 0, false, profileunlock.ErrFamilyMediaChanged
	}
	// No current file validator is exposed by this port. An If-Range cannot
	// match one, so the ordinary full response is required; never invent one.
	if raw == "" || ifRange != "" || head {
		return 0, size, false, nil
	}
	if size == 0 || !strings.HasPrefix(raw, "bytes=") || strings.Contains(raw, ",") {
		return 0, 0, false, profileunlock.ErrFamilyMediaChanged
	}
	parts := strings.Split(strings.TrimPrefix(raw, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false, profileunlock.ErrFamilyMediaChanged
	}
	parse := func(value string) (int64, error) {
		if value == "" {
			return 0, profileunlock.ErrFamilyMediaChanged
		}
		for _, c := range value {
			if c < '0' || c > '9' {
				return 0, profileunlock.ErrFamilyMediaChanged
			}
		}
		return strconv.ParseInt(value, 10, 64)
	}
	if parts[0] == "" {
		n, e := parse(parts[1])
		if e != nil || n <= 0 {
			return 0, 0, false, profileunlock.ErrFamilyMediaChanged
		}
		if n > size {
			n = size
		}
		return size - n, n, true, nil
	}
	start, err = parse(parts[0])
	if err != nil || start >= size {
		return 0, 0, false, profileunlock.ErrFamilyMediaChanged
	}
	end := size - 1
	if parts[1] != "" {
		end, err = parse(parts[1])
		if err != nil || end < start {
			return 0, 0, false, profileunlock.ErrFamilyMediaChanged
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end - start + 1, true, nil
}

func familyLocalRawCurrent(c *familyMediaConsumer, file profileunlock.FamilyLocalFile) error {
	if c == nil || file == nil || c.Check(c.Context()) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	if file.Check(c.Context()) != nil {
		if c.Context().Err() == nil {
			c.runtime.request.cancel()
		}
		return profileunlock.ErrFamilyMediaChanged
	}
	return c.Check(c.Context())
}

// The completion channel belongs to actual source/FD/watch retirement, rather
// than a cancellation signal. No copy of safe metadata can enter this opener.
func (h *familyLocalRawHandler) open(r *familyMediaRuntime, setup context.Context, item string) (profileunlock.FamilyLocalFile, error) {
	if setup == nil {
		return nil, profileunlock.ErrFamilyMediaChanged
	}
	// This private runtime belongs solely to the incoming synchronous raw
	// request. Cover openCleanup's preliminary original PG check too, then
	// detach/join this setup link before handing back the actual opened file.
	stopSetup := familyMediaAfterFunc(setup, r.request.cancel)
	defer stopSetup()
	owner, ok := r.request.owner.original.(interface {
		CaptureLocalSource(context.Context, string) (*profileunlock.FamilyLocalSource, error)
	})
	if !ok {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	actualDone := make(chan struct{})
	var file profileunlock.FamilyLocalFile
	_, err := r.openCleanup(actualDone, func(ctx context.Context) (func(), error) {
		// The original runtime owns cleanup, but the actual incoming consumer
		// must be able to cancel blocked PG/FS acquisition after handoff.
		source, e := owner.CaptureLocalSource(setup, item)
		if e != nil {
			if source != nil {
				source.Close()
			}
			close(actualDone)
			return nil, e
		}
		file, e = h.opener.OpenFamilyLocalSource(setup, source)
		if e != nil || file == nil {
			if file != nil {
				_ = file.Close()
			}
			source.Close()
			close(actualDone)
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		watchCtx, cancelWatch := context.WithCancel(ctx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			ticker := time.NewTicker(250 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-watchCtx.Done():
					return
				case <-file.Context().Done():
					r.request.cancel()
					return
				case <-ticker.C:
					if r.Check(watchCtx) != nil || file.Check(watchCtx) != nil {
						if watchCtx.Err() == nil {
							r.request.cancel()
						}
						return
					}
				}
			}
		}()
		var stopOnce sync.Once
		return func() {
			stopOnce.Do(func() { cancelWatch(); _ = file.Close(); source.Close(); <-watchDone; close(actualDone) })
		}, nil
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (h *familyLocalRawHandler) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.installation == nil || h.installation.Boundary == nil || h.opener == nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	item, err := familyLocalRawItem(r)
	if err != nil {
		http.Error(w, "Invalid local media request.", http.StatusBadRequest)
		return
	}
	q, ok := h.installation.Boundary.Begin(w, r, "")
	if !ok {
		return
	}
	defer q.Close()
	h.serveCaptured(w, r, q, item)
	// A normal handled response, including a denial, must leave the actual
	// transport while the original writer/source cleanup still remains owned.
	if err := http.NewResponseController(w).Flush(); err != nil {
		panic(http.ErrAbortHandler)
	}
}
func (h *familyLocalRawHandler) serveCaptured(w http.ResponseWriter, r *http.Request, q *FamilyMediaRequest, item string) {
	runtime, err := q.adoptRuntime(r)
	if err != nil {
		familyLocalRawDeny(w, err)
		return
	}
	consumer, _, err := runtime.beginConsumer(w, r, q.target)
	if err != nil {
		familyLocalRawDeny(w, err)
		return
	}
	file, err := h.open(runtime, consumer.Context(), item)
	if err != nil {
		familyLocalRawDeny(w, err)
		return
	}
	if familyLocalRawCurrent(consumer, file) != nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	info := file.Info()
	if info.ItemID != item || info.LibraryID == "" || info.Size < 0 || familyLocalRawCurrent(consumer, file) != nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	start, length, partial, rangeErr := familyLocalRawRange(r.Header.Get("Range"), r.Header.Get("If-Range"), info.Size, r.Method == http.MethodHead)
	writer, err := consumer.ResponseWriter(w)
	if err != nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	if familyLocalRawCurrent(consumer, file) != nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	if rangeErr != nil {
		writer.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(info.Size, 10))
		writer.Header().Set("Content-Length", "0")
		writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		if familyLocalRawCurrent(consumer, file) != nil {
			panic(http.ErrAbortHandler)
		}
		if err := http.NewResponseController(writer).Flush(); err != nil || familyLocalRawCurrent(consumer, file) != nil {
			panic(http.ErrAbortHandler)
		}
		return
	}
	var body io.ReadCloser
	if r.Method == http.MethodGet && length > 0 {
		body, err = consumer.OpenBody(func(ctx context.Context) (io.ReadCloser, error) { return file.OpenReader(ctx, start, length) })
		if err != nil {
			familyLocalRawDeny(w, err)
			return
		}

	}
	if familyLocalRawCurrent(consumer, file) != nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Accept-Ranges", "bytes")
	writer.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	name := path.Base(strings.ReplaceAll(info.Name, "\\", "/"))
	if name == "." || name == "/" || len(name) > 512 {
		name = "local-media"
	}
	if disposition := mime.FormatMediaType("inline", map[string]string{"filename": name}); disposition != "" {
		writer.Header().Set("Content-Disposition", disposition)
	}
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		writer.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(start+length-1, 10)+"/"+strconv.FormatInt(info.Size, 10))
	}
	if familyLocalRawCurrent(consumer, file) != nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaChanged)
		return
	}
	writer.WriteHeader(status)
	if familyLocalRawCurrent(consumer, file) != nil {
		panic(http.ErrAbortHandler)
	}
	if body != nil && familyLocalRawCopy(consumer, file, writer, body, length) != nil {
		panic(http.ErrAbortHandler)
	}
	if err := http.NewResponseController(writer).Flush(); err != nil || familyLocalRawCurrent(consumer, file) != nil {
		panic(http.ErrAbortHandler)
	}
}

func familyLocalRawCopy(c *familyMediaConsumer, file profileunlock.FamilyLocalFile, w http.ResponseWriter, body io.Reader, length int64) error {
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	for remaining := length; remaining > 0; {
		if familyLocalRawCurrent(c, file) != nil {
			return profileunlock.ErrFamilyMediaChanged
		}
		amount := min(int64(len(buffer)), remaining)
		n, err := body.Read(buffer[:amount])
		if n < 0 || int64(n) > amount || err != nil && !errors.Is(err, io.EOF) || familyLocalRawCurrent(c, file) != nil {
			clear(buffer)
			return profileunlock.ErrFamilyMediaChanged
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		written, writeErr := w.Write(buffer[:n])
		clear(buffer)
		if writeErr != nil || written != n || familyLocalRawCurrent(c, file) != nil {
			return profileunlock.ErrFamilyMediaChanged
		}
		remaining -= int64(n)
		if err != nil && remaining > 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}
