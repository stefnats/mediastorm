package handlers

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
	"novastream/services/remotemedia"
)

// This constructor installs no HTTP handler, protocol or legacy HLS mode.
// Public Main/route/delegation closure and scoped compatibility remain C2 work.
type FamilyMediaDependencies struct {
	Store  *datastore.DataStore
	Pins   *profileunlock.PGProfilePins
	Local  *localmedia.Service
	Parent *parentauth.Guard
}
type FamilyMediaInstallation struct {
	Boundary    *FamilyMediaBoundary
	ParentFiles *ParentLocalFileHandler

	self                *FamilyMediaInstallation
	deps                FamilyMediaDependencies
	original            context.Context
	cancel              context.CancelFunc
	localRaw            *familyLocalRawHandler
	jellyfinRaw         *familyJellyfinRawHandler
	mu                  sync.Mutex
	selected, closed    bool
	once                sync.Once
	done                chan struct{}
	originalParentFiles *ParentLocalFileHandler
}

func InstallFamilyMedia(deps FamilyMediaDependencies) (*FamilyMediaInstallation, error) {
	if deps.Store == nil || deps.Store.Pool() == nil || deps.Pins == nil || !deps.Pins.FamilyMediaReady(deps.Store) || deps.Local == nil || deps.Parent == nil {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	// The audited LOCAL port is platform-specific. Its absence, including on
	// unsupported platforms, cannot publish a partial family installation.
	local, ok := any(deps.Local).(interface {
		FamilyLocalSourceReady(*datastore.DataStore) bool
	})
	if !ok || !local.FamilyLocalSourceReady(deps.Store) || !deps.Parent.FamilyMediaReady(deps.Store) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	files, err := NewParentLocalFileHandler(deps.Store, deps.Pins, deps.Local, deps.Parent)
	if err != nil {
		return nil, err
	}
	identity := &kidsVODIdentity{profiles: deps.Store.Users(), accounts: deps.Store.Accounts(), sessions: deps.Store.Sessions(), now: time.Now}
	boundary := newFamilyMediaBoundary(identity, &kidsVODRegionalPolicy{current: deps.Store}, func(r *http.Request, target string) (vodOriginalOwner, error) {
		return deps.Pins.CaptureFamilyMediaOwner(r, target)
	})
	installed := &FamilyMediaInstallation{Boundary: boundary, ParentFiles: files, deps: deps, done: make(chan struct{})}
	installed.self = installed
	installed.originalParentFiles = files
	return installed, nil
}

// PrepareFamilyMediaConsumers reuses this actual UsersHandler's original guard.
// It constructs inert known-source consumers; it activates no route or service.
func (h *UsersHandler) PrepareFamilyMediaConsumers(original context.Context, store *datastore.DataStore, pins *profileunlock.PGProfilePins, local *localmedia.Service, manager *config.Manager, remote *remotemedia.Service) (*FamilyMediaInstallation, error) {
	if original == nil || original.Err() != nil || h == nil || h.pgPins != pins || h.parentAuthorization == nil || !h.parentAuthorization.FamilyMediaReady(store) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	installed, err := InstallFamilyMedia(FamilyMediaDependencies{Store: store, Pins: pins, Local: local, Parent: h.parentAuthorization})
	if err != nil {
		return nil, err
	}
	installed.original, installed.cancel = context.WithCancel(original)
	keep := false
	defer func() {
		if !keep {
			installed.Close()
		}
	}()
	installed.localRaw, err = newFamilyLocalRawHandlerForInstallation(installed, local)
	if err != nil {
		return nil, err
	}
	installed.jellyfinRaw, err = newFamilyJellyfinRawHandlerForInstallation(installed, manager, remote)
	if err != nil || installed.original.Err() != nil {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	installed.selected = true
	keep = true
	return installed, nil
}
func (i *FamilyMediaInstallation) valid() bool {
	return i != nil && i.self == i && i.Boundary != nil && i.ParentFiles != nil
}
func (i *FamilyMediaInstallation) ServeLocalRaw(w http.ResponseWriter, r *http.Request) {
	if !i.valid() {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	i.mu.Lock()
	ready := i.selected && !i.closed && i.original != nil && i.original.Err() == nil
	h := i.localRaw
	i.mu.Unlock()
	if !ready || h == nil {
		familyLocalRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	h.ServeHTTP(w, r)
}
func (i *FamilyMediaInstallation) ServeJellyfinRaw(w http.ResponseWriter, r *http.Request) {
	if !i.valid() {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	i.mu.Lock()
	ready := i.selected && !i.closed && i.original != nil && i.original.Err() == nil
	h := i.jellyfinRaw
	i.mu.Unlock()
	if !ready || h == nil {
		familyJellyfinRawDeny(w, profileunlock.ErrFamilyMediaUnavailable)
		return
	}
	h.ServeHTTP(w, r)
}

// Close is external: no admitted handler/cancel callback may join itself here.
func (i *FamilyMediaInstallation) Close() {
	if i == nil || i.self != i {
		return
	}
	i.once.Do(func() {
		i.mu.Lock()
		i.closed = true
		local, remote := i.localRaw, i.jellyfinRaw
		i.mu.Unlock()
		if i.cancel != nil {
			i.cancel()
		}
		if local != nil {
			local.signalClose()
		}
		if remote != nil {
			remote.signalClose()
		}
		var joined sync.WaitGroup
		if local != nil {
			joined.Add(1)
			go func() { defer joined.Done(); local.Close() }()
		}
		if remote != nil {
			joined.Add(1)
			go func() { defer joined.Done(); remote.Close() }()
		}
		joined.Wait()
		i.originalParentFiles.CloseHLS()
		close(i.done)
	})
	<-i.done
}

// This wrapper owns only physical response lifetime, never source permission.
// Successful source bytes must still pass the existing familyMediaWriter.
type familyKnownRawResponse struct {
	target          http.ResponseWriter
	ctx             context.Context
	started, denied bool
	pendingStatus   int
	errorBody       []byte
	writeErr        error
}

func (w *familyKnownRawResponse) Header() http.Header { return w.target.Header() }
func (w *familyKnownRawResponse) deny() {
	if w.started {
		w.denied = true
		return
	}
	clear(w.errorBody)
	w.errorBody = nil
	w.pendingStatus = 0
	w.started, w.denied = true, true
	for _, key := range []string{"Content-Length", "Content-Range", "Accept-Ranges", "Content-Disposition", "ETag", "Last-Modified", "Location", "Set-Cookie", "Transfer-Encoding"} {
		w.target.Header().Del(key)
	}
	// The cancelled transport is never revived. Record an explicit unavailable
	// status with no body; the caller's terminal Abort prevents implicit200.
	w.target.Header().Set("Content-Length", "0")
	w.target.WriteHeader(http.StatusServiceUnavailable)
}
func (w *familyKnownRawResponse) WriteHeader(status int) {
	if w.denied || w.started || w.pendingStatus != 0 {
		return
	}
	if w.ctx.Err() != nil {
		w.deny()
		return
	}
	if status >= 400 {
		w.pendingStatus = status
		return
	}
	w.started = true
	w.target.WriteHeader(status)
}
func (w *familyKnownRawResponse) Write(p []byte) (int, error) {
	if w.denied || w.writeErr != nil {
		return 0, profileunlock.ErrFamilyMediaUnavailable
	}
	if w.ctx.Err() != nil {
		w.deny()
		return 0, profileunlock.ErrFamilyMediaUnavailable
	}
	if w.pendingStatus >= 400 && !w.started {
		// Only explicit generic error responses are staged. The actual guarded
		// source200/206 path always writes directly to the original transport.
		if len(p) > 4096-len(w.errorBody) {
			clear(w.errorBody)
			w.errorBody = nil
			w.writeErr = profileunlock.ErrFamilyMediaUnavailable
			return 0, w.writeErr
		}
		w.errorBody = append(w.errorBody, p...)
		return len(p), nil
	}
	w.started = true
	return w.target.Write(p)
}
func (w *familyKnownRawResponse) SetWriteDeadline(deadline time.Time) error {
	if fixed, ok := w.ctx.Deadline(); ok && (deadline.IsZero() || fixed.Before(deadline)) {
		deadline = fixed
	}
	if w.ctx.Err() != nil {
		deadline = time.Now()
	}
	return http.NewResponseController(w.target).SetWriteDeadline(deadline)
}
func (w *familyKnownRawResponse) FlushError() error {
	if w.ctx.Err() != nil {
		w.deny()
		return profileunlock.ErrFamilyMediaUnavailable
	}
	if w.denied || w.writeErr != nil {
		return profileunlock.ErrFamilyMediaUnavailable
	}
	if w.pendingStatus >= 400 && !w.started {
		status := w.pendingStatus
		w.pendingStatus = 0
		w.target.Header().Del("Transfer-Encoding")
		w.target.Header().Set("Content-Length", strconv.Itoa(len(w.errorBody)))
		w.started = true
		w.target.WriteHeader(status)
		if len(w.errorBody) > 0 {
			count := len(w.errorBody)
			n, err := w.target.Write(w.errorBody)
			clear(w.errorBody)
			w.errorBody = nil
			if err != nil || n != count {
				w.writeErr = profileunlock.ErrFamilyMediaUnavailable
				return w.writeErr
			}
		}
	}
	return http.NewResponseController(w.target).Flush()
}

// The caller keeps this signal-only callback armed through actual final Flush.
// It must stop/join before intentional local cancellation or entry refund.
func familyKnownRawOutput(w http.ResponseWriter, ctx context.Context) (*familyKnownRawResponse, func(), error) {
	guarded := &familyKnownRawResponse{target: w, ctx: ctx}
	deadline := time.Now().Add(30 * time.Second)
	if err := guarded.SetWriteDeadline(deadline); err != nil {
		return nil, nil, profileunlock.ErrFamilyMediaUnavailable
	}
	stop := familyMediaAfterFunc(ctx, func() { _ = guarded.SetWriteDeadline(time.Now()) })
	return guarded, stop, nil
}
