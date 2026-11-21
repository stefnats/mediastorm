package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/database"
	"novastream/internal/datastore"
	"novastream/services/localmedia"
)

type SABLibraryPublicationHandler struct {
	self      *SABLibraryPublicationHandler
	publisher *localmedia.SABLibraryPublisher
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	entries   map[*sabPublicationHTTPEntry]struct{}
	peers     map[netip.Addr]int
	pending   sync.WaitGroup
	once      sync.Once
}
type sabPublicationHTTPEntry struct {
	owner              *SABLibraryPublicationHandler
	peer               netip.Addr
	ctx                context.Context
	cancel             context.CancelFunc
	controller         *http.ResponseController
	ioMu               sync.Mutex
	stop               func()
	finished           chan struct{}
	expiry, bodyExpiry time.Time
	bodyDone           bool
}
type sabPublicationHTTPKey struct{}
type sabPublicationBoundedWriter struct {
	http.ResponseWriter
	entry *sabPublicationHTTPEntry
}

func (w *sabPublicationBoundedWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *sabPublicationBoundedWriter) SetReadDeadline(deadline time.Time) error {
	e := w.entry
	e.ioMu.Lock()
	defer e.ioMu.Unlock()
	limit := e.bodyExpiry
	if e.bodyDone {
		limit = e.expiry
	}
	if deadline.IsZero() || limit.Before(deadline) {
		deadline = limit
	}
	if e.ctx.Err() != nil {
		deadline = time.Now().Add(-time.Second)
	}
	return e.controller.SetReadDeadline(deadline)
}
func (w *sabPublicationBoundedWriter) SetWriteDeadline(deadline time.Time) error {
	e := w.entry
	e.ioMu.Lock()
	defer e.ioMu.Unlock()
	if deadline.IsZero() || e.expiry.Before(deadline) {
		deadline = e.expiry
	}
	if e.ctx.Err() != nil {
		deadline = time.Now().Add(-time.Second)
	}
	return e.controller.SetWriteDeadline(deadline)
}
func (w *sabPublicationBoundedWriter) FlushError() error { return w.entry.controller.Flush() }

func NewSABLibraryPublicationHandler(publisher *localmedia.SABLibraryPublisher) (*SABLibraryPublicationHandler, error) {
	if publisher == nil || publisher.Context().Err() != nil {
		return nil, datastore.ErrSABLibraryPublicationUnavailable
	}
	life, cancel := context.WithCancel(publisher.Context())
	value := &SABLibraryPublicationHandler{publisher: publisher, ctx: life, cancel: cancel, entries: map[*sabPublicationHTTPEntry]struct{}{}, peers: map[netip.Addr]int{}}
	value.self = value
	return value, nil
}
func publicationPeer(raw string) (netip.Addr, error) {
	if addr, e := netip.ParseAddr(raw); e == nil && addr.Zone() == "" {
		return addr.Unmap(), nil
	}
	host, _, e := net.SplitHostPort(raw)
	if e != nil {
		return netip.Addr{}, e
	}
	addr, e := netip.ParseAddr(host)
	if e != nil || addr.Zone() != "" {
		return netip.Addr{}, datastore.ErrSABLibraryPublicationChanged
	}
	return addr.Unmap(), nil
}
func publicationHTTPLink(ctx context.Context, signal func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); signal() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if stop() {
				close(done)
			}
		})
		<-done
	}
}
func (h *SABLibraryPublicationHandler) ready() bool {
	if h == nil || h.self != h || h.ctx == nil || h.ctx.Err() != nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.closed
}
func (h *SABLibraryPublicationHandler) BoundedEntry(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.ready() || next == nil {
			http.Error(w, "503 Service Unavailable", 503)
			return
		}
		peer, e := publicationPeer(r.RemoteAddr)
		if e != nil {
			http.Error(w, "503 Service Unavailable", 503)
			return
		}
		expiry := time.Now().Add(5 * time.Minute)
		if deadline, ok := r.Context().Deadline(); ok && deadline.Before(expiry) {
			expiry = deadline
		}
		if !expiry.After(time.Now()) {
			http.Error(w, "503 Service Unavailable", 503)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), expiry)
		entry := &sabPublicationHTTPEntry{owner: h, peer: peer, ctx: ctx, cancel: cancel, controller: http.NewResponseController(w), expiry: expiry, bodyExpiry: minTime(expiry, time.Now().Add(30*time.Second)), finished: make(chan struct{})}
		h.mu.Lock()
		if h.closed || len(h.entries) >= 64 || h.peers[peer] >= 8 {
			h.mu.Unlock()
			cancel()
			http.Error(w, "503 Service Unavailable", 503)
			return
		}
		h.entries[entry] = struct{}{}
		h.peers[peer]++
		h.pending.Add(1)
		h.mu.Unlock()
		stopOwner := publicationHTTPLink(h.ctx, cancel)
		entry.stop = publicationHTTPLink(ctx, func() {
			entry.ioMu.Lock()
			defer entry.ioMu.Unlock()
			_ = entry.controller.SetReadDeadline(time.Now().Add(-time.Second))
			_ = entry.controller.SetWriteDeadline(time.Now().Add(-time.Second))
		})
		defer func() {
			// Flush actual buffered output while all original IO links remain armed.
			_ = entry.controller.Flush()
			entry.ioMu.Lock()
			_ = entry.controller.SetReadDeadline(time.Now().Add(-time.Second))
			entry.ioMu.Unlock()
			if r.Body != nil {
				_ = r.Body.Close()
			}
			entry.stop()
			stopOwner()
			cancel()
			h.mu.Lock()
			delete(h.entries, entry)
			h.peers[peer]--
			if h.peers[peer] == 0 {
				delete(h.peers, peer)
			}
			h.mu.Unlock()
			close(entry.finished)
			h.pending.Done()
		}()
		entry.ioMu.Lock()
		var readErr, writeErr error
		if ctx.Err() == nil {
			readErr = entry.controller.SetReadDeadline(entry.bodyExpiry)
			writeErr = entry.controller.SetWriteDeadline(expiry)
		} else {
			readErr = ctx.Err()
			writeErr = ctx.Err()
		}
		entry.ioMu.Unlock()
		if readErr != nil || writeErr != nil || ctx.Err() != nil {
			http.Error(w, "503 Service Unavailable", 503)
			return
		}
		if r.ContentLength > 16384 {
			http.Error(w, "413 Request Entity Too Large", 413)
			return
		}
		r = r.WithContext(context.WithValue(ctx, sabPublicationHTTPKey{}, entry))
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, 16384)
		}
		next.ServeHTTP(&sabPublicationBoundedWriter{ResponseWriter: w, entry: entry}, r)
	})
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (h *SABLibraryPublicationHandler) Close() {
	if h == nil || h.self != h {
		return
	}
	h.once.Do(func() {
		h.mu.Lock()
		h.closed = true
		h.cancel()
		h.mu.Unlock()
		h.publisher.Close()
		h.pending.Wait()
	})
}
func (h *SABLibraryPublicationHandler) entry(r *http.Request) (*sabPublicationHTTPEntry, error) {
	if !h.ready() || r == nil {
		return nil, datastore.ErrSABLibraryPublicationUnavailable
	}
	entry, _ := r.Context().Value(sabPublicationHTTPKey{}).(*sabPublicationHTTPEntry)
	if entry == nil || entry.owner != h || entry.ctx.Err() != nil || !entry.expiry.After(time.Now()) {
		return nil, datastore.ErrSABLibraryPublicationUnavailable
	}
	return entry, nil
}
func publicationQueueID(r *http.Request) (int64, error) {
	return strconv.ParseInt(mux.Vars(r)["queueID"], 10, 64)
}
func publicationHTTPError(w http.ResponseWriter, e error) {
	code := 503
	if errors.Is(e, datastore.ErrSABLibraryPublicationNotFound) {
		code = 404
	} else if errors.Is(e, database.ErrSABPublicationConflict) || errors.Is(e, database.ErrSABPublicationQuota) {
		code = 409
	} else if errors.Is(e, datastore.ErrSABLibraryPublicationChanged) {
		code = 403
	}
	http.Error(w, http.StatusText(code), code)
}
func (h *SABLibraryPublicationHandler) Publish(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, false)
}
func (h *SABLibraryPublicationHandler) Reconcile(w http.ResponseWriter, r *http.Request) {
	h.mutate(w, r, true)
}
func (h *SABLibraryPublicationHandler) mutate(w http.ResponseWriter, r *http.Request, reconcile bool) {
	entry, e := h.entry(r)
	if e != nil {
		publicationHTTPError(w, e)
		return
	}
	if r.Method != http.MethodPost || r.URL.RawQuery != "" {
		http.Error(w, "400 Bad Request", 400)
		return
	}
	id, e := publicationQueueID(r)
	if e != nil || id <= 0 {
		http.Error(w, "400 Bad Request", 400)
		return
	}
	raw, e := io.ReadAll(r.Body)
	if e != nil || len(raw) > 16384 || entry.ctx.Err() != nil || !entry.bodyExpiry.After(time.Now()) {
		http.Error(w, "400 Bad Request", 400)
		return
	}
	if datastore.ValidateSABLibraryPublicationSelection(raw) != nil {
		clear(raw)
		http.Error(w, "400 Bad Request", 400)
		return
	}
	entry.ioMu.Lock()
	if entry.ctx.Err() == nil {
		entry.bodyDone = true
		e = entry.controller.SetReadDeadline(entry.expiry)
	} else {
		e = entry.ctx.Err()
	}
	entry.ioMu.Unlock()
	if e != nil {
		publicationHTTPError(w, e)
		return
	}
	var result *localmedia.SABLibraryPublicationResult
	if reconcile {
		result, e = h.publisher.Reconcile(r, id, raw)
	} else {
		result, e = h.publisher.Publish(r, id, raw)
	}
	clear(raw)
	if e != nil {
		publicationHTTPError(w, e)
		return
	}
	defer result.Close()
	h.output(w, r, entry, result)
}
func (h *SABLibraryPublicationHandler) Status(w http.ResponseWriter, r *http.Request) {
	entry, e := h.entry(r)
	if e != nil {
		publicationHTTPError(w, e)
		return
	}
	id, e := publicationQueueID(r)
	if e != nil || id <= 0 || r.Method != http.MethodGet || r.URL.RawQuery != "" {
		http.Error(w, "400 Bad Request", 400)
		return
	}
	result, e := h.publisher.ReadStatus(r, id)
	if e != nil {
		publicationHTTPError(w, e)
		return
	}
	defer result.Close()
	h.output(w, r, entry, result)
}
func (h *SABLibraryPublicationHandler) output(w http.ResponseWriter, r *http.Request, entry *sabPublicationHTTPEntry, result *localmedia.SABLibraryPublicationResult) {
	wire, e := json.Marshal(result.View())
	if e != nil || len(wire) > 256<<10 {
		http.Error(w, "503 Service Unavailable", 503)
		return
	}
	defer clear(wire)
	watch, cancel := context.WithCancel(entry.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-watch.Done():
				return
			case <-ticker.C:
				if result.Check(r) != nil {
					entry.cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	deadline := minTime(entry.expiry, result.ExpiresAt())
	entry.ioMu.Lock()
	if entry.ctx.Err() == nil && deadline.After(time.Now()) {
		e = entry.controller.SetWriteDeadline(deadline)
	} else {
		e = datastore.ErrSABLibraryPublicationChanged
	}
	entry.ioMu.Unlock()
	if e != nil || result.Check(r) != nil || entry.ctx.Err() != nil || !deadline.After(time.Now()) {
		http.Error(w, "503 Service Unavailable", 503)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if entry.ctx.Err() != nil || result.Check(r) != nil || !deadline.After(time.Now()) {
		return
	}
	if n, e := w.Write(wire); e != nil || n != len(wire) {
		return
	}
	if result.Check(r) != nil || entry.ctx.Err() != nil || !deadline.After(time.Now()) {
		return
	}
	_ = entry.controller.Flush()
}
