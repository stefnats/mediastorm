package handlers

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/services/profileunlock"
)

// This actor owns ingress and physical cleanup, never authentication or a file
// grant. Only the actual original installation binds it to the parent handler.
type parentFileHTTPRuntime struct {
	handler     *ParentLocalFileHandler
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	entries     int
	work        sync.WaitGroup
	cleanupDone chan struct{}
	once        sync.Once
	done        chan struct{}
}
type parentFileHTTPKey struct{}

// This retains the identical incoming transport body, even if ownership
// middleware later replaces r.Body with a replay reader. Completion is the
// real underlying Close plus all delegated Reads, never a caller flag.
type parentFileIngressBody struct {
	raw         io.ReadCloser
	controller  *http.ResponseController
	mu          sync.Mutex
	closed, eof bool
	reads       sync.WaitGroup
	once        sync.Once
	err         error
}

func (b *parentFileIngressBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, http.ErrBodyReadAfterClose
	}
	b.reads.Add(1)
	b.mu.Unlock()
	defer b.reads.Done()
	n, err := b.raw.Read(p)
	if err == io.EOF {
		b.mu.Lock()
		b.eof = true
		b.mu.Unlock()
	}
	return n, err
}
func (b *parentFileIngressBody) Close() error {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		unread := !b.eof
		b.mu.Unlock()
		if unread {
			_ = b.controller.SetReadDeadline(time.Now())
		}
		b.err = b.raw.Close()
		b.reads.Wait()
	})
	return b.err
}
func (b *parentFileIngressBody) unread() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.eof
}
func parentIncomingBody(r *http.Request) bool {
	return r != nil && (r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.Body != nil && r.Body != http.NoBody)
}
func parentEntryUnavailable(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var body *parentFileIngressBody
	if r.Body != nil && r.Body != http.NoBody {
		body = &parentFileIngressBody{raw: r.Body, controller: http.NewResponseController(w)}
	}
	var stop func()
	// Register real body cleanup before the first fallible output deadline.
	// Abort cleanup keeps any installed interruption armed through body join.
	defer func() {
		if body != nil {
			_ = body.Close()
		}
		if stop != nil {
			stop()
		}
	}()
	output, stopOutput, err := familyKnownRawOutput(w, ctx)
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	stop = stopOutput
	if body != nil {
		output.Header().Set("Connection", "close")
	}
	apierror.Write(output, 503, "family_media_unavailable", "Family media is unavailable.")
	if output.FlushError() != nil {
		panic(http.ErrAbortHandler)
	}
	stop()
	if body != nil {
		_ = body.Close()
	}
}

func (h *ParentLocalFileHandler) bindOriginalLifetime(original context.Context) error {
	if h == nil || h.core == nil || original == nil || original.Err() != nil || h.httpRuntime != nil {
		return profileunlock.ErrFamilyMediaUnavailable
	}
	ctx, cancel := context.WithCancel(original)
	if err := h.core.BindParentFileRuntime(ctx); err != nil {
		cancel()
		return err
	}
	life := &parentFileHTTPRuntime{handler: h, ctx: ctx, cancel: cancel, cleanupDone: make(chan struct{}), done: make(chan struct{})}
	h.httpRuntime = life
	// One original cleanup owner, joined by external Close. Its callbacks only
	// signal; no admitted HTTP/reader/child stack joins its own completion.
	go func() {
		defer close(life.cleanupDone)
		<-ctx.Done()
		h.core.SignalParentFilesClose()
		h.signalParentHLS()
		h.CloseHLS()
		h.core.CloseParentFiles()
	}()
	return nil
}
func (h *ParentLocalFileHandler) signalParentHLS() {
	h.hlsMu.Lock()
	m := h.hls
	h.hlsMu.Unlock()
	if m != nil {
		m.signalClose()
	}
}
func (h *ParentLocalFileHandler) signalClose() {
	if h == nil || h.httpRuntime == nil {
		return
	}
	life := h.httpRuntime
	life.mu.Lock()
	life.closed = true
	life.mu.Unlock()
	life.cancel()
	h.core.SignalParentFilesClose()
	h.signalParentHLS()
}

// Close is external to every admitted HTTP, Check, callback and child stack.
func (h *ParentLocalFileHandler) Close() {
	if h == nil {
		return
	}
	if h.httpRuntime == nil {
		h.CloseHLS()
		return
	} // Isolated legacy handler.
	life := h.httpRuntime
	life.once.Do(func() {
		h.signalClose()
		<-life.cleanupDone
		life.work.Wait()
		close(life.done)
	})
	<-life.done
}
func (h *ParentLocalFileHandler) managedAvailable() bool {
	if h == nil || h.httpRuntime == nil {
		return h != nil
	}
	life := h.httpRuntime
	life.mu.Lock()
	defer life.mu.Unlock()
	return !life.closed && life.ctx.Err() == nil
}
func (life *parentFileHTTPRuntime) begin(caller context.Context, setup bool) (context.Context, func(), error) {
	if caller == nil || caller.Err() != nil {
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	life.mu.Lock()
	if life.closed || life.ctx.Err() != nil || life.entries >= 32 {
		life.mu.Unlock()
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	life.entries++
	life.work.Add(1)
	life.mu.Unlock()
	ctx, cancel := context.WithCancel(caller)
	if setup {
		cancel()
		ctx, cancel = context.WithTimeout(caller, 30*time.Second)
	}
	stop := familyMediaAfterFunc(life.ctx, cancel)
	var once sync.Once
	finish := func() {
		once.Do(func() {
			stop()
			cancel()
			life.mu.Lock()
			life.entries--
			life.mu.Unlock()
			life.work.Done()
		})
	}
	if ctx.Err() != nil {
		finish()
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	return ctx, finish, nil
}

// managedFamilyRoute is an exact route selector only. All current original PG
// frame, profile, parent password and concrete source checks remain downstream.
func managedFamilyRoute(r *http.Request, base string) (parent, setup, match bool) {
	if r == nil || r.URL == nil {
		return
	}
	path := r.URL.Path
	base = strings.TrimRight(base, "/")
	if base != "" && base != "/" && strings.HasPrefix(path, base+"/") {
		path = strings.TrimPrefix(path, base)
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 5 && parts[0] == "api" && parts[1] == "library" && parts[2] == "items" && parts[3] != "" && (parts[4] == "raw-local" || parts[4] == "raw-jellyfin") {
		return false, false, true
	}
	if len(parts) < 5 || parts[0] != "api" || parts[1] != "users" || parts[2] == "" {
		return
	}
	if len(parts) == 5 && parts[3] == "kids" && parts[4] == "local-file-approvals" {
		return true, true, true
	}
	if len(parts) < 6 || parts[3] != "library" || parts[4] != "items" || parts[5] == "" {
		return
	}
	if len(parts) == 7 && parts[6] == "approved-file" {
		return true, r.Method == http.MethodDelete, true
	}
	if len(parts) == 7 && parts[6] == "approved-hls" {
		return true, true, true
	}
	if len(parts) == 8 && parts[6] == "approved-hls" && parts[7] != "" {
		return true, true, true
	}
	if len(parts) == 9 && parts[6] == "approved-hls" && parts[7] != "" && parts[8] != "" {
		return true, false, true
	}
	return
}

// ManagedKnownSourceEntry must be registered before authentication/body readers.
// A selected nil/foreign/closed installation cannot silently choose legacy IO.
func (i *FamilyMediaInstallation) ManagedKnownSourceEntry(base string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, setup, matched := managedFamilyRoute(r, base)
			if !matched {
				next.ServeHTTP(w, r)
				return
			}
			if !i.managedParentReady() {
				parentEntryUnavailable(w, r)
				return
			}
			h := i.originalParentFiles
			if existing, _ := r.Context().Value(parentFileHTTPKey{}).(*parentFileHTTPRuntime); existing == h.httpRuntime {
				next.ServeHTTP(w, r)
				return
			}
			invalidBody := (r.Method == http.MethodGet || r.Method == http.MethodHead) && parentIncomingBody(r)
			ctx, finish, err := h.httpRuntime.begin(r.Context(), setup || invalidBody)
			if err != nil {
				parentEntryUnavailable(w, r)
				return
			}
			defer finish()
			controller := http.NewResponseController(w)
			var body *parentFileIngressBody
			if r.Body != nil && r.Body != http.NoBody {
				body = &parentFileIngressBody{raw: r.Body, controller: controller}
				defer body.Close()
			}
			if setup || invalidBody {
				deadline, _ := ctx.Deadline()
				if controller.SetReadDeadline(deadline) != nil || controller.SetWriteDeadline(deadline) != nil {
					if body != nil {
						_ = body.Close()
					}
					apierror.Write(w, 503, "family_media_unavailable", "Cancelable media is unavailable.")
					return
				}
			}
			stopInterrupt := familyMediaAfterFunc(ctx, func() {
				_ = controller.SetReadDeadline(time.Now())
				_ = controller.SetWriteDeadline(time.Now())
			})
			defer stopInterrupt()
			output, stopOutput, err := familyKnownRawOutput(w, ctx)
			if err != nil {
				if body != nil {
					_ = body.Close()
				}
				apierror.Write(w, 503, "family_media_unavailable", "Cancelable media is unavailable.")
				return
			}
			defer stopOutput()
			if body != nil {
				defer body.Close()
			}
			request := r.WithContext(context.WithValue(ctx, parentFileHTTPKey{}, h.httpRuntime))
			if body != nil {
				request.Body = body
			}
			finishOutput := func() {
				// Go must not drain an unread body before it sends the denial.
				// The intentional read interruption below may cancel its original
				// connection context, so it follows completed response framing and
				// actual callback disarm, never a context/deadline revival.
				if body != nil && body.unread() {
					output.Header().Set("Connection", "close")
				}
				if output.FlushError() != nil {
					panic(http.ErrAbortHandler)
				}
				stopOutput()
				stopInterrupt()
				if body != nil {
					_ = body.Close()
				}
			}
			if invalidBody {
				apierror.Write(output, 400, "invalid_request", "This media operation does not accept a request body.")
				finishOutput()
				return
			}
			next.ServeHTTP(output, request)
			finishOutput()
		})
	}
}
func (i *FamilyMediaInstallation) managedParentReady() bool {
	if !i.valid() || i.originalParentFiles == nil || i.originalParentFiles != i.ParentFiles || i.originalBoundary != i.Boundary {
		return false
	}
	i.mu.Lock()
	ready := i.selected && !i.closed && i.original != nil && i.original.Err() == nil
	i.mu.Unlock()
	return ready && i.originalParentFiles.httpRuntime != nil && i.originalParentFiles.managedAvailable()
}
func (i *FamilyMediaInstallation) ReadyForRegisteredFamily(users *UsersHandler, pins *profileunlock.PGProfilePins, local *LocalMediaHandler) bool {
	return i.managedParentReady() && users != nil && users.parentAuthorization == i.deps.Parent && users.pgPins == pins && pins == i.deps.Pins && pins.FamilyMediaReady(i.deps.Store) && local != nil && local.service == i.deps.Local
}
func (i *FamilyMediaInstallation) InstallRegisteredParent(local *LocalMediaHandler) {
	if i.managedParentReady() && local != nil && local.service == i.deps.Local {
		local.SetParentLocalFiles(i.originalParentFiles)
	}
}
func (i *FamilyMediaInstallation) serveParent(w http.ResponseWriter, r *http.Request, serve func(*ParentLocalFileHandler, http.ResponseWriter, *http.Request)) {
	if !i.managedParentReady() {
		apierror.Write(w, 503, "kids_local_file_unavailable", "Parent-approved local files are unavailable.")
		return
	}
	serve(i.originalParentFiles, w, r)
}
func (i *FamilyMediaInstallation) ServeParentApproval(w http.ResponseWriter, r *http.Request) {
	i.serveParent(w, r, (*ParentLocalFileHandler).Approve)
}
func (i *FamilyMediaInstallation) ServeParentRaw(w http.ResponseWriter, r *http.Request) {
	i.serveParent(w, r, (*ParentLocalFileHandler).Stream)
}
func (i *FamilyMediaInstallation) ServeParentRevoke(w http.ResponseWriter, r *http.Request) {
	i.serveParent(w, r, (*ParentLocalFileHandler).Revoke)
}
func (i *FamilyMediaInstallation) ServeParentHLSStart(w http.ResponseWriter, r *http.Request) {
	i.serveParent(w, r, (*ParentLocalFileHandler).StartHLS)
}
func (i *FamilyMediaInstallation) ServeParentHLSAsset(w http.ResponseWriter, r *http.Request) {
	i.serveParent(w, r, (*ParentLocalFileHandler).ServeHLS)
}
func (i *FamilyMediaInstallation) ServeParentHLSStop(w http.ResponseWriter, r *http.Request) {
	i.serveParent(w, r, (*ParentLocalFileHandler).StopHLS)
}

// This runs before reader/output ref defers. It cannot revive a cancelled
// original transport. Legacy isolated fixture response semantics stay intact.
func (h *ParentLocalFileHandler) flushParentOutput(w http.ResponseWriter, r *http.Request) {
	if h.httpRuntime == nil {
		return
	}
	if r.Context().Err() != nil || !h.managedAvailable() || http.NewResponseController(w).Flush() != nil {
		panic(http.ErrAbortHandler)
	}
}
func (h *ParentLocalFileHandler) flushParentOnReturn(w http.ResponseWriter, r *http.Request) {
	if failure := recover(); failure != nil {
		panic(failure)
	}
	h.flushParentOutput(w, r)
}
