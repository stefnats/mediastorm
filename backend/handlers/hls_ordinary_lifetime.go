package handlers

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"novastream/internal/nzbfilesystem"
	"novastream/services/streaming"
	"os"
	"os/exec"
	"sync"
	"time"
)

// These private objects own only real work and descriptors. They cannot grant
// source/profile/PIN access or attest an upstream actor through a status flag.
var errOrdinaryHLSClosed = errors.New("ordinary HLS work unavailable")

const ordinaryHLSSessionLimit = 32
const ordinaryHLSChildLimit = 16
const ordinaryHLSOutputLimit = 32

type ordinaryHLSManager struct {
	manager    *HLSManager
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	active     int
	operations sync.WaitGroup
	pending    map[*ordinaryHLSSession]struct{} // unpublished setup reservations only
	retired    map[*ordinaryHLSSession]error    // exact ended outputs whose removal failed
}

type ordinaryHLSSession struct {
	control       *ordinaryHLSManager
	session       *HLSSession
	output        string
	outputOnce    sync.Once
	operationOnce sync.Once
	setupOnce     sync.Once
	removalMu     sync.Mutex
	setupInputs   *ordinaryHLSInputs
	ctx           context.Context
	cancel        context.CancelFunc
	setup         context.Context
	stopSetup     func()
	cancelSetup   context.CancelFunc
	mu            sync.Mutex
	changing      bool
	closed        bool
	generation    *ordinaryHLSGeneration
	once          sync.Once
	done          chan struct{}
}

type ordinaryHLSGeneration struct {
	owner       *ordinaryHLSSession
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	children    int
	workers     sync.WaitGroup
	outputs     sync.WaitGroup
	outputCount int
	inputs      *ordinaryHLSInputs
	done        chan struct{}
	once        sync.Once
}

type ordinaryHLSGenerationKey struct{}
type ordinaryHLSCommandKey struct{}

type ordinaryHLSCommand struct {
	generation *ordinaryHLSGeneration
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	cmd        *exec.Cmd
	sources    []io.Closer
	stderr     io.Closer
	children   sync.WaitGroup
	once       sync.Once
	stopSignal func()
}

func newOrdinaryHLSManager(manager *HLSManager) *ordinaryHLSManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &ordinaryHLSManager{manager: manager, ctx: ctx, cancel: cancel}
}
func (m *HLSManager) ordinaryControl() *ordinaryHLSManager {
	m.sessionLifecycleMu.Lock()
	defer m.sessionLifecycleMu.Unlock()
	if m.ordinary == nil {
		m.ordinary = newOrdinaryHLSManager(m)
	}
	return m.ordinary
}
func (c *ordinaryHLSManager) reserve(caller context.Context) (*ordinaryHLSSession, error) {
	if caller == nil || caller.Err() != nil {
		return nil, errOrdinaryHLSClosed
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.ctx.Err() != nil || c.active >= ordinaryHLSSessionLimit {
		return nil, errOrdinaryHLSClosed
	}
	ctx, cancel := context.WithCancel(c.ctx)
	setup, cancelSetup := context.WithTimeout(caller, 30*time.Second)
	o := &ordinaryHLSSession{control: c, ctx: ctx, cancel: cancel, setup: setup, cancelSetup: cancelSetup, done: make(chan struct{})}
	stopManager := familyMediaAfterFunc(ctx, cancelSetup)
	stopCaller := familyMediaAfterFunc(setup, cancel)
	o.stopSetup = func() { stopCaller(); stopManager() }
	o.setupInputs = newOrdinaryHLSInputs(c.manager)
	o.setup = context.WithValue(setup, ordinaryHLSInputsKey{}, o.setupInputs)
	if c.pending == nil {
		c.pending = make(map[*ordinaryHLSSession]struct{})
	}
	c.pending[o] = struct{}{}
	c.active++
	c.operations.Add(2)
	return o, nil
}
func (o *ordinaryHLSSession) attach(s *HLSSession) context.Context {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.session = s
	o.generation = o.newGeneration()
	return o.generation.context()
}
func (o *ordinaryHLSSession) finishSetup(published bool) {
	o.setupOnce.Do(func() {
		defer o.control.operations.Done()
		defer func() { o.control.mu.Lock(); delete(o.control.pending, o); o.control.mu.Unlock() }()
		defer o.setupInputs.closeAndJoin()
		if !published {
			o.closeAndJoin()
			return
		}
		// This transfers only real process lifetime, never media authority.
		o.stopSetup()
		o.cancelSetup()
	})
}

func (o *ordinaryHLSSession) newGeneration() *ordinaryHLSGeneration {
	ctx, cancel := context.WithCancel(o.ctx)
	return &ordinaryHLSGeneration{owner: o, ctx: ctx, cancel: cancel, done: make(chan struct{}), inputs: newOrdinaryHLSInputs(o.control.manager)}
}
func (g *ordinaryHLSGeneration) context() context.Context {
	return context.WithValue(context.WithValue(g.ctx, ordinaryHLSGenerationKey{}, g), ordinaryHLSInputsKey{}, g.inputs)
}
func ordinaryGeneration(ctx context.Context, s *HLSSession) *ordinaryHLSGeneration {
	if ctx == nil || s == nil || s.ordinary == nil {
		return nil
	}
	g, _ := ctx.Value(ordinaryHLSGenerationKey{}).(*ordinaryHLSGeneration)
	if g == nil {
		ref, _ := ctx.Value(ordinaryHLSHTTPKey{}).(*ordinaryHLSHTTPRef)
		if ref != nil && ref.owner == s.ordinary && ref.active && ref.ctx.Err() == nil {
			g = ref.generation
		}
	}
	if g == nil || g.owner != s.ordinary || g.owner.session != s {
		return nil
	}
	return g
}
func (o *ordinaryHLSSession) originalRuntime() bool {
	if o == nil || o.session == nil || o.control == nil {
		return false
	}
	m := o.control.manager
	s := o.session
	m.mu.RLock()
	current := m.sessions[s.ID] == s
	m.mu.RUnlock()
	s.mu.RLock()
	identity := s.vodIdentity
	valid := identity != nil && identity.manager == m && identity.session == s
	s.mu.RUnlock()
	return current && valid
}
func (o *ordinaryHLSSession) current() *ordinaryHLSGeneration {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.generation
}
func (g *ordinaryHLSGeneration) spawn(ctx context.Context, work func()) bool {
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil || !g.owner.originalRuntime() || ctx == nil || ctx.Err() != nil || g.children >= ordinaryHLSChildLimit {
		if g.children >= ordinaryHLSChildLimit {
			g.cancel()
		}
		g.mu.Unlock()
		return false
	}
	run, _ := ctx.Value(ordinaryHLSCommandKey{}).(*ordinaryHLSCommand)
	if run != nil && run.generation != g {
		g.mu.Unlock()
		return false
	}
	if run != nil {
		run.mu.Lock()
		if run.closed {
			run.mu.Unlock()
			g.mu.Unlock()
			return false
		}
		run.children.Add(1)
		run.mu.Unlock()
	}
	g.children++
	g.workers.Add(1)
	g.mu.Unlock()
	go func() {
		defer func() {
			if run != nil {
				run.children.Done()
			}
			g.mu.Lock()
			g.children--
			g.mu.Unlock()
			g.workers.Done()
		}()
		work()
	}()
	return true
}
func (g *ordinaryHLSGeneration) closeAndJoin() {
	g.once.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.cancel()
		g.mu.Unlock()
		// Registration gates are already closed. Every actual child and output
		// descriptor must end; cancellation and Kill are not completion receipts.
		g.inputs.closeAndJoin()
		g.workers.Wait()
		g.outputs.Wait()
		close(g.done)
	})
	<-g.done
}
func (o *ordinaryHLSSession) closeAndJoin() {
	if o == nil {
		return
	}
	o.once.Do(func() {
		o.mu.Lock()
		o.closed = true
		o.cancel()
		g := o.generation
		o.mu.Unlock()
		if o.stopSetup != nil {
			o.stopSetup()
		}
		o.cancelSetup()
		if g != nil {
			g.closeAndJoin()
		}
		close(o.done)
	})
	<-o.done
}

func (o *ordinaryHLSSession) releaseOutput() {
	if o == nil {
		return
	}
	<-o.done
	o.outputOnce.Do(func() {
		o.control.mu.Lock()
		delete(o.control.pending, o)
		o.control.active--
		delete(o.control.retired, o)
		o.control.mu.Unlock()
	})
}

// Only an external HTTP mutation calls this. The running encoder never waits
// for its own generation when it performs an internal recovery.
func (o *ordinaryHLSSession) beginChange(ctx context.Context) (context.Context, func(), error) {
	o.mu.Lock()
	if o.closed || o.changing || ctx == nil || ctx.Err() != nil || o.ctx.Err() != nil {
		o.mu.Unlock()
		return nil, nil, errOrdinaryHLSClosed
	}
	o.changing = true
	old := o.generation
	o.mu.Unlock()
	if old != nil {
		old.closeAndJoin()
	}
	o.mu.Lock()
	if o.closed || ctx.Err() != nil || o.ctx.Err() != nil {
		o.changing = false
		o.mu.Unlock()
		return nil, nil, errOrdinaryHLSClosed
	}
	next := o.newGeneration()
	o.generation = next
	o.mu.Unlock()
	var ended sync.Once
	return next.context(), func() { ended.Do(func() { o.mu.Lock(); o.changing = false; o.mu.Unlock() }) }, nil
}
func newOrdinaryHLSCommand(ctx context.Context, s *HLSSession) (context.Context, *ordinaryHLSCommand) {
	g := ordinaryGeneration(ctx, s)
	if g == nil {
		return ctx, nil
	}
	next, cancel := context.WithCancel(ctx)
	run := &ordinaryHLSCommand{generation: g, ctx: next, cancel: cancel}
	run.stopSignal = familyMediaAfterFunc(next, run.signal)
	return context.WithValue(next, ordinaryHLSCommandKey{}, run), run
}

type ordinaryHLSInputBody struct {
	io.ReadCloser
	once         sync.Once
	err          error
	genuine      bool
	cancelSource context.CancelFunc
	inputs       *ordinaryHLSInputs
}

func ordinaryInputBody(body io.ReadCloser) io.ReadCloser {
	if body == nil {
		return nil
	}
	ready := nzbfilesystem.RequireSourceCleanup(body)
	genuine := ready == nil || !errors.Is(ready, nzbfilesystem.ErrSourceCleanupUnsupported)
	switch body.(type) {
	case *nzbfilesystem.MetadataVirtualFile, *nzbfilesystem.MetadataVirtualDirectory:
		genuine = true
	}
	return &ordinaryHLSInputBody{ReadCloser: body, genuine: genuine}
}
func (b *ordinaryHLSInputBody) Close() error {
	b.once.Do(func() {
		if b.inputs != nil {
			defer b.inputs.release(b)
		}
		if b.cancelSource != nil {
			b.cancelSource()
		}
		if b.genuine {
			signal := nzbfilesystem.SignalSourceClose(b.ReadCloser)
			closed := b.ReadCloser.Close()
			joined := nzbfilesystem.JoinSourceCleanup(b.ReadCloser)
			b.err = errors.Join(signal, closed, joined)
		} else {
			b.err = b.ReadCloser.Close()
		}
	})
	return b.err
}
func (m *HLSManager) ordinaryProviderStream(ctx context.Context, request streaming.Request) (*streaming.Response, error) {
	inputs, _ := ctx.Value(ordinaryHLSInputsKey{}).(*ordinaryHLSInputs)
	if inputs == nil {
		return m.streamer.Stream(ctx, request)
	}
	if inputs.manager != m {
		return nil, errOrdinaryHLSClosed
	}
	response, err := inputs.open(ctx, m.streamer, request)
	run, _ := ctx.Value(ordinaryHLSCommandKey{}).(*ordinaryHLSCommand)
	if response != nil && response.Body != nil && run != nil {
		if e := run.setInput(response.Body); e != nil {
			_ = response.Close()
			return nil, e
		}
	}
	return response, err
}

func (r *ordinaryHLSCommand) setInput(source io.Closer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil {
		return errOrdinaryHLSClosed
	}
	if source != nil {
		if len(r.sources) >= ordinaryHLSOutputLimit {
			return errOrdinaryHLSClosed
		}
		r.sources = append(r.sources, source)
	}
	return nil
}

func (r *ordinaryHLSCommand) setCommand(cmd *exec.Cmd, source, stderr io.Closer) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil {
		return errOrdinaryHLSClosed
	}
	r.cmd = cmd
	if source != nil {
		if len(r.sources) >= ordinaryHLSOutputLimit {
			return errOrdinaryHLSClosed
		}
		r.sources = append(r.sources, source)
	}
	r.stderr = stderr
	return nil
}
func (r *ordinaryHLSCommand) startCommand() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.ctx.Err() != nil || r.cmd == nil {
		return errOrdinaryHLSClosed
	}
	// Process is published under the same mutex used by cancellation, so a
	// concurrent stop cannot race the real Cmd.Start writing that field.
	return r.cmd.Start()
}

func (r *ordinaryHLSCommand) signal() {
	r.mu.Lock()
	var process *os.Process
	if r.cmd != nil {
		process = r.cmd.Process
	}
	sources, stderr := append([]io.Closer(nil), r.sources...), r.stderr
	r.mu.Unlock()
	// Signal every original physical input before waiting for any one of them.
	if process != nil {
		_ = process.Kill()
	}
	for _, source := range sources {
		if body, ok := source.(*ordinaryHLSInputBody); ok {
			if body.cancelSource != nil {
				body.cancelSource()
			}
			if body.genuine {
				_ = nzbfilesystem.SignalSourceClose(body.ReadCloser)
			}
		}
	}
	if stderr != nil {
		_ = stderr.Close()
	}
	var closing sync.WaitGroup
	closing.Add(len(sources))
	for _, source := range sources {
		go func(source io.Closer) { defer closing.Done(); _ = source.Close() }(source)
	}
	closing.Wait()
}
func (r *ordinaryHLSCommand) finish() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		r.cancel()
		r.stopSignal()
		r.children.Wait()
	})
}
func ordinaryAfterCommand(ctx context.Context, r *ordinaryHLSCommand) context.Context {
	if r == nil {
		return ctx
	}
	r.finish()
	return r.generation.context()
}
func (c *ordinaryHLSManager) closeAdmission() {
	c.mu.Lock()
	c.closed = true
	pending := make([]context.CancelFunc, 0, len(c.pending))
	for original := range c.pending {
		pending = append(pending, original.cancelSetup)
	}
	c.mu.Unlock()
	c.cancel()
	// Signal the actual original caller-derived setup contexts synchronously;
	// do not depend on scheduling an AfterFunc before beginning shutdown joins.
	for _, cancel := range pending {
		cancel()
	}
}
func ordinaryHLSGo(ctx context.Context, s *HLSSession, work func()) bool {
	if s.ordinary == nil {
		go work()
		return true
	}
	g := ordinaryGeneration(ctx, s)
	if g == nil {
		return false
	}
	return g.spawn(ctx, work)
}

// HTTP references below are physical I/O ownership only. The existing caller
// still performs all original session/profile/source authorization separately.
type ordinaryHLSHTTPKey struct{}
type ordinaryHLSHTTPRef struct {
	owner          *ordinaryHLSSession
	generation     *ordinaryHLSGeneration
	request        *http.Request
	writer         *ordinaryHLSHTTPWriter
	ctx            context.Context
	cancel         context.CancelFunc
	stopGeneration func()
	stopOwner      func()
	stopCaller     func()
	transport      *http.ResponseController
	active         bool
	once           sync.Once
}
type ordinaryHLSHTTPWriter struct {
	http.ResponseWriter
	ref             *ordinaryHLSHTTPRef
	started, denied bool
}

func (w *ordinaryHLSHTTPWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (r *ordinaryHLSHTTPRef) check() error {
	if r == nil || r.ctx.Err() != nil || !r.active || r.generation.ctx.Err() != nil {
		return errOrdinaryHLSClosed
	}
	o := r.owner
	o.mu.Lock()
	same := !o.closed && !o.changing && o.generation == r.generation
	o.mu.Unlock()
	if !same || !o.originalRuntime() {
		return errOrdinaryHLSClosed
	}
	return nil
}
func (w *ordinaryHLSHTTPWriter) deny() {
	if w.denied {
		return
	}
	w.denied = true
	if w.started {
		return
	}
	w.started = true
	for _, key := range []string{"Content-Length", "Content-Range", "Content-Encoding", "Location", "Set-Cookie"} {
		w.Header().Del(key)
	}
	http.Error(w.ResponseWriter, "HLS operation unavailable", http.StatusServiceUnavailable)
}
func (w *ordinaryHLSHTTPWriter) WriteHeader(status int) {
	if w.ref.check() != nil {
		w.deny()
		return
	}
	w.started = true
	w.ResponseWriter.WriteHeader(status)
}
func (w *ordinaryHLSHTTPWriter) Write(data []byte) (int, error) {
	if w.ref.check() != nil {
		w.deny()
		return 0, errOrdinaryHLSClosed
	}
	w.started = true
	return w.ResponseWriter.Write(data)
}
func (w *ordinaryHLSHTTPWriter) Flush() {
	if w.ref.check() != nil {
		w.deny()
		return
	}
	w.started = true
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (r *ordinaryHLSHTTPRef) leaveGeneration() {
	if !r.active {
		return
	}
	r.stopGeneration()
	r.active = false
	g := r.generation
	g.mu.Lock()
	g.outputCount--
	g.mu.Unlock()
	g.outputs.Done()
}
func (r *ordinaryHLSHTTPRef) enterGeneration(g *ordinaryHLSGeneration) error {
	g.mu.Lock()
	if g.closed || g.ctx.Err() != nil || g.outputCount >= ordinaryHLSOutputLimit {
		g.mu.Unlock()
		return errOrdinaryHLSClosed
	}
	g.outputCount++
	g.outputs.Add(1)
	g.mu.Unlock()
	r.generation = g
	r.active = true
	r.stopGeneration = familyMediaAfterFunc(g.ctx, func() { r.cancel() })
	return nil
}
func (r *ordinaryHLSHTTPRef) suspend() error {
	if r == nil || r.writer.started || r.ctx.Err() != nil {
		return errOrdinaryHLSClosed
	}
	r.leaveGeneration()
	return nil
}
func (r *ordinaryHLSHTTPRef) resume(g *ordinaryHLSGeneration) error {
	if r.ctx.Err() != nil || g == nil || g.owner != r.owner {
		return errOrdinaryHLSClosed
	}
	return r.enterGeneration(g)
}
func (r *ordinaryHLSHTTPRef) finish() {
	r.once.Do(func() {
		if !r.writer.started && r.check() != nil {
			r.writer.deny()
		}
		_ = r.transport.Flush()
		r.stopCaller()
		r.stopGeneration()
		r.stopOwner()
		r.leaveGeneration()
		r.cancel()
	})
}
func (m *HLSManager) ordinaryHTTP(w http.ResponseWriter, r *http.Request, id string) (http.ResponseWriter, *http.Request, func(), bool) {
	m.mu.RLock()
	s := m.sessions[id]
	m.mu.RUnlock()
	if s == nil || s.ordinary == nil {
		return w, r, func() {}, true
	}
	if existing, _ := r.Context().Value(ordinaryHLSHTTPKey{}).(*ordinaryHLSHTTPRef); existing != nil {
		if existing.owner != s.ordinary || existing.check() != nil {
			return w, r, func() {}, false
		}
		return w, r, func() {}, true
	}
	o := s.ordinary
	o.mu.Lock()
	g := o.generation
	closed := o.closed || o.changing
	o.mu.Unlock()
	if closed || g == nil {
		return w, r, func() {}, false
	}
	ctx, cancel := context.WithCancel(r.Context())
	ref := &ordinaryHLSHTTPRef{owner: o, request: r, ctx: ctx, cancel: cancel, transport: http.NewResponseController(w)}
	ref.writer = &ordinaryHLSHTTPWriter{ResponseWriter: w, ref: ref}
	if ref.enterGeneration(g) != nil {
		cancel()
		return w, r, func() {}, false
	}
	deadline := time.Now().Add(30 * time.Second)
	if at, ok := r.Context().Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	_ = ref.transport.SetWriteDeadline(deadline)
	interrupt := func() { _ = ref.transport.SetWriteDeadline(time.Now()) }
	ref.stopOwner = familyMediaAfterFunc(ctx, interrupt)
	ref.stopCaller = familyMediaAfterFunc(o.ctx, ref.cancel)
	bounded := r.WithContext(context.WithValue(ctx, ordinaryHLSHTTPKey{}, ref))
	return ref.writer, bounded, ref.finish, true
}

func ordinaryHTTPRef(r *http.Request) *ordinaryHLSHTTPRef {
	if r == nil {
		return nil
	}
	ref, _ := r.Context().Value(ordinaryHLSHTTPKey{}).(*ordinaryHLSHTTPRef)
	return ref
}

// Runtime work completion is separate from disk quota: an actual EACCES leaves
// its exact ended owner/path charged, but cannot prevent physical shutdown.
func (o *ordinaryHLSSession) finishOperation() {
	o.operationOnce.Do(func() { o.control.operations.Done() })
}
func (o *ordinaryHLSSession) recordRemovalFailure(err error) {
	if err == nil {
		return
	}
	c := o.control
	c.mu.Lock()
	if c.retired == nil {
		c.retired = make(map[*ordinaryHLSSession]error)
	}
	c.retired[o] = err
	c.mu.Unlock()
}
func (c *ordinaryHLSManager) retryRemovedOutputs(id string) {
	c.mu.Lock()
	owners := make([]*ordinaryHLSSession, 0, len(c.retired))
	for o := range c.retired {
		if id == "" || o.session != nil && o.session.ID == id {
			owners = append(owners, o)
		}
	}
	c.mu.Unlock()
	for _, o := range owners {
		o.removalMu.Lock()
		if err := os.RemoveAll(o.output); err != nil {
			o.recordRemovalFailure(err)
			log.Printf("[hls] ended output remains charged after removal failure: %v", err)
		} else {
			o.releaseOutput()
		}
		o.removalMu.Unlock()
	}
}

type ordinaryHLSInputsKey struct{}
type ordinaryHLSPendingInput struct{ cancel context.CancelFunc }

// Only actual private setup/generation scopes create this bounded lifecycle
// collector. It grants no media authority and never attests indirect children.
type ordinaryHLSInputs struct {
	manager  *HLSManager
	mu       sync.Mutex
	closed   bool
	pending  map[*ordinaryHLSPendingInput]struct{}
	bodies   map[*ordinaryHLSInputBody]struct{}
	openings sync.WaitGroup
	once     sync.Once
	done     chan struct{}
}

func newOrdinaryHLSInputs(m *HLSManager) *ordinaryHLSInputs {
	return &ordinaryHLSInputs{manager: m, pending: make(map[*ordinaryHLSPendingInput]struct{}), bodies: make(map[*ordinaryHLSInputBody]struct{}), done: make(chan struct{})}
}
func (s *ordinaryHLSInputs) open(ctx context.Context, provider streaming.Provider, request streaming.Request) (*streaming.Response, error) {
	s.mu.Lock()
	if s.closed || ctx.Err() != nil || len(s.pending)+len(s.bodies) >= ordinaryHLSOutputLimit {
		s.mu.Unlock()
		return nil, errOrdinaryHLSClosed
	}
	child, cancel := context.WithCancel(ctx)
	pending := &ordinaryHLSPendingInput{cancel: cancel}
	s.pending[pending] = struct{}{}
	s.openings.Add(1)
	s.mu.Unlock()
	defer s.openings.Done()
	response, err := provider.Stream(child, request)
	if response == nil && err == nil {
		err = errOrdinaryHLSClosed
	}
	var body *ordinaryHLSInputBody
	if response != nil && response.Body != nil {
		body = ordinaryInputBody(response.Body).(*ordinaryHLSInputBody)
		body.cancelSource = cancel
		body.inputs = s
		response.Body = body
	}
	s.mu.Lock()
	delete(s.pending, pending)
	denied := s.closed || ctx.Err() != nil || err != nil
	if body != nil {
		s.bodies[body] = struct{}{}
	}
	s.mu.Unlock()
	if denied {
		cancel()
		if body != nil {
			_ = body.Close()
		}
		if err == nil {
			err = errOrdinaryHLSClosed
		}
		return nil, err
	}
	if body == nil {
		cancel()
	}
	return response, err
}
func (s *ordinaryHLSInputs) release(body *ordinaryHLSInputBody) {
	s.mu.Lock()
	delete(s.bodies, body)
	s.mu.Unlock()
}
func (s *ordinaryHLSInputs) closeAndJoin() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		pending := make([]*ordinaryHLSPendingInput, 0, len(s.pending))
		for p := range s.pending {
			pending = append(pending, p)
		}
		bodies := make([]*ordinaryHLSInputBody, 0, len(s.bodies))
		for b := range s.bodies {
			bodies = append(bodies, b)
		}
		s.mu.Unlock()
		for _, p := range pending {
			p.cancel()
		}
		for _, b := range bodies {
			if b.cancelSource != nil {
				b.cancelSource()
			}
			if b.genuine {
				_ = nzbfilesystem.SignalSourceClose(b.ReadCloser)
			}
		}
		var closing sync.WaitGroup
		closing.Add(len(bodies))
		for _, b := range bodies {
			go func(b *ordinaryHLSInputBody) { defer closing.Done(); _ = b.Close() }(b)
		}
		closing.Wait()
		s.openings.Wait()
		close(s.done)
	})
	<-s.done
}

// The actual synchronous probe caller owns both Cmd.Output and this copier.
// Existing ordinary deadlines interrupt both pipe ends and the retained body
// before either physical child/copy completion is joined. This is lifetime
// ownership only; an indirect URL or unretained upstream is not certified.
func ordinaryProbeCopier(ctx context.Context, body io.ReadCloser) (*io.PipeReader, *io.PipeWriter, func()) {
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pw.Close()
		buffer := make([]byte, 128*1024)
		_, _ = io.CopyBuffer(pw, body, buffer)
	}()
	if inputs, _ := ctx.Value(ordinaryHLSInputsKey{}).(*ordinaryHLSInputs); inputs == nil {
		return pr, pw, func() {}
	}
	interrupt := func() { _ = pr.CloseWithError(ctx.Err()); _ = pw.CloseWithError(ctx.Err()); _ = body.Close() }
	stop := familyMediaAfterFunc(ctx, interrupt)
	var once sync.Once
	finish := func() { once.Do(func() { _ = pr.Close(); _ = pw.Close(); _ = body.Close(); stop(); <-done }) }
	return pr, pw, finish
}

// The caller owns both actual stderr endpoints even if lifetime admission
// rejects before exec.Cmd.Start. StderrPipe would hide its child writer.
func ordinaryHLSStderr(cmd *exec.Cmd) (*os.File, *os.File, error) {
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, nil, err
	}
	cmd.Stderr = writer
	return reader, writer, nil
}
