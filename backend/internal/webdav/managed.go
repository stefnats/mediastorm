package webdav

import (
	"context"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/webdav"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/nzbfilesystem"
	"novastream/internal/settingsbootstrap"
	"novastream/internal/webdav/propfind"
)

const managedBodyLimit = 1 << 20

var errManagedUnavailable = errors.New("managed WebDAV unavailable")

// ManagedHandler owns the original HTTP/source lifetime. Its deployment
// observation is deliberately not an actor, a PIN proof or a media grant.
type ManagedHandler struct {
	self    *ManagedHandler
	prefix  string
	source  *nzbfilesystem.NzbFilesystem
	issuer  *settingsbootstrap.DeploymentWebDAVIssuer
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
	active  map[*managedWebDAVRequest]string
	peers   map[string]int
	pending sync.WaitGroup
	once    sync.Once
	done    chan struct{}
	locks   webdav.LockSystem
}

func NewManagedHandler(prefix string, source *nzbfilesystem.NzbFilesystem, issuer *settingsbootstrap.DeploymentWebDAVIssuer) (*ManagedHandler, error) {
	if source == nil || source.GetRemoteFile() == nil || issuer == nil || prefix == "" || len(prefix) > 256 || path.Clean(prefix) != prefix || !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "?#\x00\r\n\\") {
		return nil, errManagedUnavailable
	}
	ctx, cancel := context.WithCancel(context.Background())
	h := &ManagedHandler{prefix: prefix, source: source, issuer: issuer, ctx: ctx, cancel: cancel, active: make(map[*managedWebDAVRequest]string), peers: make(map[string]int), done: make(chan struct{}), locks: webdav.NewMemLS()}
	h.self = h
	return h, nil
}
func (h *ManagedHandler) GetHTTPHandler() http.Handler { return h }

// Closed is local lifecycle DATA only; an open runtime does not authenticate.
func (h *ManagedHandler) Closed() bool {
	if h == nil || h.self != h || h.ctx == nil || h.ctx.Err() != nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

func (h *ManagedHandler) Close() {
	if h == nil || h.self != h {
		return
	}
	h.once.Do(func() {
		h.mu.Lock()
		h.closed = true
		h.cancel()
		h.mu.Unlock()
		h.pending.Wait()
		h.issuer.Close()
		close(h.done)
	})
	<-h.done
}
func managedPeer(raw string) (string, error) {
	if len(raw) > 256 {
		return "", errManagedUnavailable
	}
	if a, e := netip.ParseAddr(raw); e == nil && a.Zone() == "" {
		return a.Unmap().String(), nil
	}
	host, port, e := net.SplitHostPort(raw)
	if e != nil || port == "" {
		return "", errManagedUnavailable
	}
	a, e := netip.ParseAddr(host)
	if e != nil || a.Zone() != "" {
		return "", errManagedUnavailable
	}
	if _, e = netip.ParseAddrPort(raw); e != nil {
		return "", errManagedUnavailable
	}
	return a.Unmap().String(), nil
}
func managedLink(ctx context.Context, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); cancel() })
	return func() {
		if !stop() {
			<-done
		}
	}
}

type managedWebDAVRequest struct {
	handler                *ManagedHandler
	request                *http.Request
	ctx                    context.Context
	cancel                 context.CancelFunc
	expiry                 time.Time
	transport              *http.ResponseController
	proof                  *settingsbootstrap.DeploymentWebDAVApplicability
	source                 *nzbfilesystem.RequestSource
	body                   *managedWebDAVBody
	stopRuntime, stopProof func()
	stopIO                 func() bool
	ioDone                 chan struct{}
	workerStop, workerDone chan struct{}
	writer                 *managedWebDAVWriter
}

func (h *ManagedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Rejected admissions cannot read input or open any source. Bound their tiny
	// generic response as well; they never borrow one of the source slots.
	reject := func() {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(time.Second))
		http.Error(w, "WebDAV unavailable", http.StatusServiceUnavailable)
	}
	if h == nil || h.self != h || r == nil || r.URL == nil {
		reject()
		return
	}
	peer, err := managedPeer(r.RemoteAddr)
	if err != nil {
		reject()
		return
	}
	entered := time.Now()
	expiry := entered.Add(24 * time.Hour)
	if old, ok := r.Context().Deadline(); ok && old.Before(expiry) {
		expiry = old
	}
	ctx, cancel := context.WithDeadline(r.Context(), expiry)
	op := &managedWebDAVRequest{handler: h, ctx: ctx, cancel: cancel, expiry: expiry, transport: http.NewResponseController(w), ioDone: make(chan struct{})}
	h.mu.Lock()
	if h.closed || len(h.active) >= 64 || h.peers[peer] >= 8 {
		h.mu.Unlock()
		cancel()
		reject()
		return
	}
	h.active[op] = peer
	h.peers[peer]++
	h.pending.Add(1)
	h.mu.Unlock()
	op.stopRuntime = managedLink(h.ctx, cancel)
	// Every value and body identity is finalized before the one native capture.
	r = r.WithContext(ctx)
	r = r.WithContext(webDAVSourceContext(r))
	op.ctx = r.Context()
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	op.body = &managedWebDAVBody{raw: body, reader: http.MaxBytesReader(w, body, managedBodyLimit), op: op}
	r.Body = op.body
	op.request = r
	inputExpiry := entered.Add(30 * time.Second)
	if expiry.Before(inputExpiry) {
		inputExpiry = expiry
	}
	_ = op.transport.SetReadDeadline(inputExpiry)
	_ = op.transport.SetWriteDeadline(expiry)
	op.stopIO = context.AfterFunc(op.ctx, func() {
		defer close(op.ioDone)
		_ = op.transport.SetReadDeadline(time.Now())
		_ = op.transport.SetWriteDeadline(time.Now())
	})
	op.writer = &managedWebDAVWriter{ResponseWriter: w, op: op}
	defer op.finish()
	op.proof, err = h.issuer.Capture(r)
	if err != nil {
		op.writer.fail(err)
		return
	}
	op.stopProof = managedLink(op.proof.Context(), cancel)
	if op.proof.Prefix() != h.prefix {
		op.writer.fail(config.ErrWebDAVPolicyUnsupported)
		return
	}
	if err = op.current(); err != nil {
		op.writer.fail(err)
		return
	}
	if r.ContentLength > managedBodyLimit {
		op.writer.failStatus(http.StatusRequestEntityTooLarge)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, "PROPFIND", "MOVE", http.MethodDelete:
	default:
		op.writer.Header().Set("Allow", "GET, HEAD, OPTIONS, PROPFIND, MOVE, DELETE")
		op.writer.failStatus(http.StatusMethodNotAllowed)
		return
	}
	op.source, err = h.source.NewRequestSource(op.ctx, nzbfilesystem.RequestSourceBudgets{MetadataBytes: 16 << 20, Entries: 4096, Depth: 32, PendingReaders: 64})
	if err != nil {
		op.writer.fail(err)
		return
	}
	op.workerStop = make(chan struct{})
	op.workerDone = make(chan struct{})
	go op.watch()
	if r.Method == http.MethodOptions {
		op.writer.Header().Set("Allow", "GET, HEAD, OPTIONS, PROPFIND, MOVE, DELETE")
		op.writer.Header().Set("DAV", "1")
		op.writer.WriteHeader(http.StatusOK)
		return
	}
	if h.prefix != "/" && r.URL.Path == h.prefix {
		location, e := managedRedirectLocation(r, h.prefix)
		if e != nil {
			op.writer.failStatus(http.StatusBadRequest)
			return
		}
		http.Redirect(op.writer, r, location, http.StatusMovedPermanently)
		return
	}
	filesystem := &managedWebDAVFS{FileSystem: requestSourceToWebdavFS(op.source), op: op}
	handler := &webdav.Handler{Prefix: h.prefix, FileSystem: filesystem, LockSystem: h.locks}
	if ext := filepath.Ext(r.URL.Path); ext != "" {
		kind := mime.TypeByExtension(ext)
		if kind == "" {
			kind = "application/octet-stream"
		}
		op.writer.Header().Set("Content-Type", kind)
	}
	op.writer.Header().Set("Accept-Ranges", "bytes")
	if r.Method == "PROPFIND" {
		hrefPrefix, e := managedPresentationPrefix(r, h.prefix)
		if e != nil {
			op.writer.failStatus(http.StatusBadRequest)
			return
		}
		status, e := propfind.HandleBoundedPropfind(filesystem, h.locks, op.writer, r, h.prefix, hrefPrefix)
		if e != nil || status != 0 {
			if status == 0 {
				status = http.StatusServiceUnavailable
			}
			op.writer.failStatus(status)
		}
		return
	}
	handler.ServeHTTP(op.writer, r)
}
func (op *managedWebDAVRequest) current() error {
	if op == nil || op.ctx.Err() != nil || !op.expiry.After(time.Now()) || op.proof == nil {
		return errManagedUnavailable
	}
	return op.proof.Check(op.request)
}
func (op *managedWebDAVRequest) watch() {
	defer close(op.workerDone)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-op.workerStop:
			return
		case <-op.ctx.Done():
			return
		case <-tick.C:
			if op.current() != nil {
				op.cancel()
				return
			}
		}
	}
}
func (op *managedWebDAVRequest) finish() {
	// External interruption remains armed through physical output and cleanup.
	op.writer.finishHeaders()
	if !op.writer.failed {
		if err := op.current(); err != nil {
			op.writer.fail(err)
			op.cancel()
		}
	}
	_ = op.transport.Flush()
	if op.workerStop != nil {
		close(op.workerStop)
		<-op.workerDone
	}
	if op.source != nil {
		_ = op.source.Close()
		_ = op.source.CloseAndJoin()
	}
	// Do not drain an unread request past its actual ingress lifetime. This is
	// read-only interruption and does not expire the healthy response's write.
	_ = op.transport.SetReadDeadline(time.Now())
	_ = op.body.Close()
	if op.stopProof != nil {
		op.stopProof()
	}
	if !op.stopIO() {
		<-op.ioDone
	}
	op.stopRuntime()
	if op.proof != nil {
		op.proof.Close()
	}
	op.cancel()
	h := op.handler
	h.mu.Lock()
	peer := h.active[op]
	delete(h.active, op)
	h.peers[peer]--
	if h.peers[peer] == 0 {
		delete(h.peers, peer)
	}
	h.mu.Unlock()
	h.pending.Done()
}

type managedWebDAVBody struct {
	raw    io.ReadCloser
	reader io.ReadCloser
	op     *managedWebDAVRequest
	once   sync.Once
	err    error
}

func (b *managedWebDAVBody) Read(p []byte) (int, error) {
	if err := b.op.current(); err != nil {
		return 0, err
	}
	n, err := b.reader.Read(p)
	if e := b.op.current(); e != nil {
		return 0, e
	}
	return n, err
}
func (b *managedWebDAVBody) Close() error { b.once.Do(func() { b.err = b.raw.Close() }); return b.err }

type managedWebDAVWriter struct {
	http.ResponseWriter
	op              *managedWebDAVRequest
	started, failed bool
}

func (w *managedWebDAVWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func managedStatus(err error) int {
	if errors.Is(err, config.ErrWebDAVBasicRejected) {
		return http.StatusUnauthorized
	}
	if errors.Is(err, config.ErrWebDAVPolicyUnsupported) || errors.Is(err, datastore.ErrDeploymentWebDAVUnsupported) {
		return http.StatusForbidden
	}
	var large *http.MaxBytesError
	if errors.As(err, &large) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusServiceUnavailable
}
func (w *managedWebDAVWriter) fail(err error) { w.failStatus(managedStatus(err)) }
func (w *managedWebDAVWriter) failStatus(status int) {
	if w.failed {
		return
	}
	w.failed = true
	if w.started {
		return
	}
	w.started = true
	for _, key := range []string{"Content-Length", "Content-Range", "Content-Encoding", "Location", "Accept-Ranges"} {
		w.Header().Del(key)
	}
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Basic realm="BASIC WebDAV REALM"`)
	}
	http.Error(w.ResponseWriter, http.StatusText(status), status)
}
func (w *managedWebDAVWriter) finishHeaders() {
	if !w.started {
		if err := w.op.current(); err != nil {
			w.fail(err)
		}
	}
}
func (w *managedWebDAVWriter) WriteHeader(status int) {
	if w.failed {
		return
	}
	if err := w.op.current(); err != nil {
		w.fail(err)
		return
	}
	w.started = true
	w.ResponseWriter.WriteHeader(status)
}
func (w *managedWebDAVWriter) Write(p []byte) (int, error) {
	if w.failed {
		return 0, errManagedUnavailable
	}
	if err := w.op.current(); err != nil {
		w.fail(err)
		return 0, err
	}
	w.started = true
	n, err := w.ResponseWriter.Write(p)
	if e := w.op.current(); e != nil {
		return n, e
	}
	return n, err
}
func (w *managedWebDAVWriter) Flush() {
	if w.failed {
		return
	}
	if err := w.op.current(); err != nil {
		w.fail(err)
		return
	}
	w.started = true
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

type managedWebDAVFS struct {
	webdav.FileSystem
	op *managedWebDAVRequest
}

func (f *managedWebDAVFS) Mkdir(ctx context.Context, n string, p os.FileMode) error {
	if e := f.op.current(); e != nil {
		return e
	}
	return os.ErrPermission
}
func (f *managedWebDAVFS) OpenFile(ctx context.Context, n string, flag int, p os.FileMode) (webdav.File, error) {
	if e := f.op.current(); e != nil {
		return nil, e
	}
	file, err := f.FileSystem.OpenFile(ctx, n, flag, p)
	if err != nil {
		return nil, err
	}
	if e := f.op.current(); e != nil {
		_ = file.Close()
		return nil, e
	}
	if e := nzbfilesystem.RequireSourceCleanup(file); e != nil {
		_ = file.Close()
		return nil, e
	}
	return &managedWebDAVFile{File: file, op: f.op}, nil
}
func (f *managedWebDAVFS) Stat(ctx context.Context, n string) (os.FileInfo, error) {
	if e := f.op.current(); e != nil {
		return nil, e
	}
	v, e := f.FileSystem.Stat(ctx, n)
	if x := f.op.current(); x != nil {
		return nil, x
	}
	return v, e
}
func (f *managedWebDAVFS) RemoveAll(ctx context.Context, n string) error {
	if e := f.op.current(); e != nil {
		return e
	}
	e := f.FileSystem.RemoveAll(ctx, n)
	if x := f.op.current(); x != nil {
		return x
	}
	return e
}
func (f *managedWebDAVFS) Rename(ctx context.Context, a, b string) error {
	if e := f.op.current(); e != nil {
		return e
	}
	e := f.FileSystem.Rename(ctx, a, b)
	if x := f.op.current(); x != nil {
		return x
	}
	return e
}

type managedWebDAVFile struct {
	webdav.File
	op *managedWebDAVRequest
}

func (f *managedWebDAVFile) Read(p []byte) (int, error) {
	if e := f.op.current(); e != nil {
		return 0, e
	}
	n, e := f.File.Read(p)
	if x := f.op.current(); x != nil {
		return 0, x
	}
	return n, e
}
func (f *managedWebDAVFile) Seek(n int64, w int) (int64, error) {
	if e := f.op.current(); e != nil {
		return 0, e
	}
	v, e := f.File.Seek(n, w)
	if x := f.op.current(); x != nil {
		return 0, x
	}
	return v, e
}
func (f *managedWebDAVFile) Stat() (os.FileInfo, error) {
	if e := f.op.current(); e != nil {
		return nil, e
	}
	v, e := f.File.Stat()
	if x := f.op.current(); x != nil {
		return nil, x
	}
	return v, e
}
func (f *managedWebDAVFile) Readdir(n int) ([]os.FileInfo, error) {
	if e := f.op.current(); e != nil {
		return nil, e
	}
	v, e := f.File.Readdir(n)
	if x := f.op.current(); x != nil {
		return nil, x
	}
	return v, e
}
func (f *managedWebDAVFile) Write([]byte) (int, error) { return 0, os.ErrPermission }

// The original URI supplies a local presentation path only. Source policy and
// identity always use the already captured original request, never this value.

func managedOriginalURI(r *http.Request) (*url.URL, error) {
	raw := r.RequestURI
	if !strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "//") {
		return nil, errManagedUnavailable
	}
	u, e := url.ParseRequestURI(raw)
	if e != nil || u.Scheme != "" || u.Host != "" || u.User != nil || strings.HasPrefix(u.Path, "//") || !strings.HasSuffix(u.Path, r.URL.Path) || strings.ContainsAny(u.Path, "\\\x00\r\n") {
		return nil, errManagedUnavailable
	}
	return u, nil
}
func managedRedirectLocation(r *http.Request, prefix string) (string, error) {
	u, e := managedOriginalURI(r)
	if e != nil || r.URL.Path != prefix {
		return "", errManagedUnavailable
	}
	u.Path += "/"
	if u.RawPath != "" {
		u.RawPath += "/"
	}
	return u.String(), nil
}
func managedPresentationPrefix(r *http.Request, prefix string) (string, error) {
	u, e := managedOriginalURI(r)
	if e != nil {
		return "", e
	}
	base := strings.TrimSuffix(u.Path, r.URL.Path)
	return path.Join(base, prefix), nil
}
