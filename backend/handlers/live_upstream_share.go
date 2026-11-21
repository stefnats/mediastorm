package handlers

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"novastream/internal/livecapacity"
	"novastream/internal/netproxy"
	"novastream/internal/requestsecurity"
)

// Live upstream sharing.
//
// Every live HLS session runs its own FFmpeg, and each FFmpeg used to open its own connection to
// the provider. Two devices on the same channel therefore meant two connections on one IPTV
// account. Most accounts allow exactly one, so the provider kept dropping the older connection,
// FFmpeg reconnected and dropped the other one, and both viewers saw their picture jump back a few
// seconds every ~5s ("Stream ends prematurely ... Will reconnect", timestamp discontinuity).
//
// The hub keeps one upstream connection per channel and fans its MPEG-TS bytes out to the stdin
// of every session's FFmpeg. Sessions stay independent (their own playlist, segment numbering,
// target-specific output and restart supervision); only the provider connection is shared. The
// connection closes as soon as the last session lets go, so a channel switch frees the account
// slot immediately.

const (
	liveUpstreamReadChunk          = 32 * 1024
	liveUpstreamPrimeBytes         = 4 * 1024 * 1024 // a few seconds a late joiner can start from at once
	liveUpstreamSubscriberLag      = 1024            // chunks queued per subscriber before it is dropped
	liveUpstreamReadTimeout        = 20 * time.Second
	liveUpstreamMaxReconnects      = 3
	liveUpstreamHealthyAfter       = 10 * time.Second // a connection that lasted this long resets the reconnect budget
	mpegTSPacketSize               = 188
	mpegTSSyncByte            byte = 0x47
)

var (
	// errLiveUpstreamNotTS means the source answered with an HLS playlist rather than a
	// transport stream. FFmpeg has to read those itself, so the caller falls back to that.
	errLiveUpstreamNotTS = errors.New("live upstream is not an MPEG-TS stream")
	errLiveUpstreamLag   = errors.New("live upstream subscriber fell behind")
)

type liveUpstreamOpener func(ctx context.Context) (io.ReadCloser, error)

// liveUpstreamSource says how to open a channel's provider connection. Reconnect mirrors FFmpeg's
// -reconnect_streamed for IPTV: a dropped connection is reopened in place so viewers only see a
// discontinuity. Without it (HDHomeRun tuners) the end of the stream ends every subscriber and
// each session's supervisor decides what happens next, as before sharing.
type liveUpstreamSource struct {
	open      liveUpstreamOpener
	reconnect bool
}

const liveUpstreamSourceLimit = 16
const liveUpstreamSubscriberLimit = 16
const liveUpstreamManagerReferenceLimit = 64

// The existing hub table owns pending, active and physically retiring sources.
// It is a lifetime registry, never a profile/source/PIN permission registry.
type liveUpstreamHub struct {
	mu      sync.Mutex
	streams map[string]*liveUpstream
}

var sharedLiveUpstreams = &liveUpstreamHub{streams: make(map[string]*liveUpstream)}

type liveUpstream struct {
	hub                  *liveUpstreamHub
	key, label           string
	source               liveUpstreamSource
	control              *ordinaryHLSManager
	ctx                  context.Context
	cancel               context.CancelFunc
	stopOwner            func()
	ready, done, removed chan struct{}
	openErr              error
	mu                   sync.Mutex
	body                 *liveUpstreamOwnedBody
	subscribers          map[*liveUpstreamSubscriber]struct{}
	prime                [][]byte
	primeBytes           int
	retiring, ended      bool
	reason               error
	closeJobs            sync.WaitGroup
}
type liveUpstreamOwnedBody struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (b *liveUpstreamOwnedBody) Close() error {
	b.once.Do(func() { b.err = b.ReadCloser.Close() })
	return b.err
}

type liveUpstreamSubscriber struct {
	upstream                        *liveUpstream
	control                         *ordinaryHLSManager
	chunks                          chan []byte
	done, closed, armed             chan struct{}
	pending                         []byte
	readMu                          sync.Mutex
	mu                              sync.Mutex
	err                             error
	failed                          atomic.Bool
	closing                         atomic.Bool
	lifeMu                          sync.Mutex
	readClosed                      bool
	readers                         sync.WaitGroup
	stopCaller, stopManager         func()
	doneOnce, closeOnce, queueClose sync.Once
}

// liveUpstreamShareKey identifies the provider connection a live session would open, or returns
// "" when the input cannot be relayed as raw bytes (HLS playlists, non-HTTP transports).
func liveUpstreamShareKey(path string, tuning LiveTuningSettings) string {
	if tuning.ForceHLSInput || inputLooksLikeHLS(path) {
		return ""
	}
	parsed, err := url.Parse(strings.TrimSpace(path))
	if err != nil {
		return ""
	}
	if scheme := strings.ToLower(parsed.Scheme); scheme != "http" && scheme != "https" {
		return ""
	}
	headerKeys := make([]string, 0, len(tuning.RequestHeaders))
	for key := range tuning.RequestHeaders {
		headerKeys = append(headerKeys, key)
	}
	sort.Strings(headerKeys)
	var b strings.Builder
	b.WriteString(strings.TrimSpace(path))
	b.WriteString("\x00")
	b.WriteString(strings.TrimSpace(tuning.ProxyURL))
	if tuning.HDHomeRunInput {
		b.WriteString("\x00hdhomerun")
	}
	for _, key := range headerKeys {
		b.WriteString("\x00")
		b.WriteString(strings.ToLower(key))
		b.WriteString("=")
		b.WriteString(tuning.RequestHeaders[key])
	}
	return b.String()
}

// Subscribe is the isolated legacy/test wrapper. Actual HLS calls the original
// manager/session port below; this function does not authorize managed media.
func (h *liveUpstreamHub) Subscribe(key, label string, source liveUpstreamSource) (*liveUpstreamSubscriber, bool, error) {
	return h.subscribe(context.Background(), nil, nil, key, label, source)
}

// This concrete reference identifies original published work only; the
// unchanged configured media boundary remains the source/PIN authority.
type liveOriginalSource struct {
	self     *liveOriginalSource
	manager  *HLSManager
	session  *HLSSession
	identity *hlsSessionIdentity
}

func (m *HLSManager) captureOriginalLiveSource(s *HLSSession) (*liveOriginalSource, error) {
	if m == nil || s == nil || m.hlsInputConfigured() {
		return nil, errVODOwner
	}
	control := m.ordinaryControl()
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.closed || control.ctx.Err() != nil {
		return nil, errOrdinaryHLSClosed
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m.sessions[s.ID] != s || s.Stopped || !s.IsLive || s.vodIdentity == nil || s.vodIdentity.manager != m || s.vodIdentity.session != s {
		return nil, errVODOwner
	}
	original := &liveOriginalSource{manager: m, session: s, identity: s.vodIdentity}
	original.self = original
	return original, nil
}
func (original *liveOriginalSource) subscribe(ctx context.Context, m *HLSManager, s *HLSSession) (*liveUpstreamSubscriber, bool, error) {
	if original == nil || original.self != original || original.manager != m || original.session != s {
		return nil, false, errVODOwner
	}
	s.mu.RLock()
	same := s.vodIdentity == original.identity
	s.mu.RUnlock()
	if !same {
		return nil, false, errVODOwner
	}
	return sharedLiveUpstreams.subscribe(ctx, m, s, "", "", liveUpstreamSource{})
}
func (m *HLSManager) subscribeOriginalLiveUpstream(ctx context.Context, s *HLSSession) (*liveUpstreamSubscriber, bool, error) {
	original, err := m.captureOriginalLiveSource(s)
	if err != nil {
		return nil, false, err
	}
	return original.subscribe(ctx, m, s)
}
func reserveLiveUpstreamReference(ctx context.Context, c *ordinaryHLSManager) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.ctx.Err() != nil || ctx.Err() != nil {
		return errOrdinaryHLSClosed
	}
	if c.liveReferences >= liveUpstreamManagerReferenceLimit {
		return livecapacity.ErrLimit
	}
	c.liveReferences++
	c.operations.Add(1)
	return nil
}
func releaseLiveUpstreamReference(c *ordinaryHLSManager) {
	c.mu.Lock()
	c.liveReferences--
	c.mu.Unlock()
	c.operations.Done()
}
func (h *liveUpstreamHub) subscribe(ctx context.Context, m *HLSManager, s *HLSSession, key, label string, source liveUpstreamSource) (*liveUpstreamSubscriber, bool, error) {
	if ctx == nil {
		return nil, false, context.Canceled
	}
	var control *ordinaryHLSManager
	if m != nil {
		control = m.ordinaryControl()
	}
	heldReference := false
	defer func() {
		if heldReference {
			releaseLiveUpstreamReference(control)
		}
	}()
	for {
		if control != nil && !heldReference {
			if err := reserveLiveUpstreamReference(ctx, control); err != nil {
				return nil, false, err
			}
			heldReference = true
		}
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if m != nil {
			control.mu.Lock()
			if control.closed || control.ctx.Err() != nil {
				control.mu.Unlock()
				return nil, false, errOrdinaryHLSClosed
			}
			m.mu.RLock()
			s.mu.RLock()
			if m.sessions[s.ID] != s || s.Stopped || !s.IsLive || s.vodIdentity == nil || s.vodIdentity.manager != m || s.vodIdentity.session != s {
				s.mu.RUnlock()
				m.mu.RUnlock()
				control.mu.Unlock()
				return nil, false, errVODOwner
			}
			// Freeze actual server-held source DATA while the exact original publication
			// is held. This copy does not become a source/media permission.
			tuning := s.LiveTuning
			tuning.RequestHeaders = cloneLiveUpstreamHeaders(tuning.RequestHeaders)
			frozen := &HLSSession{Path: s.Path, LiveTuning: tuning}
			key = liveUpstreamShareKey(frozen.Path, tuning)
			label = requestsecurity.URLForLog(frozen.Path)
			source = openLiveUpstream(frozen)
		}
		h.mu.Lock()
		if h.streams == nil {
			h.streams = make(map[string]*liveUpstream)
		}
		unlock := func() {
			h.mu.Unlock()
			if m != nil {
				s.mu.RUnlock()
				m.mu.RUnlock()
				control.mu.Unlock()
			}
		}
		if key == "" || source.open == nil {
			unlock()
			return nil, false, errLiveUpstreamNotTS
		}
		u := h.streams[key]
		if u != nil {
			u.mu.Lock()
			retiring := u.retiring
			removed := u.removed
			u.mu.Unlock()
			if retiring {
				unlock()
				select {
				case <-ctx.Done():
					return nil, false, ctx.Err()
				case <-liveUpstreamManagerDone(control):
					return nil, false, errOrdinaryHLSClosed
				case <-removed:
					continue
				}
			}
		}
		created := u == nil
		if created {
			if len(h.streams) >= liveUpstreamSourceLimit {
				unlock()
				return nil, false, livecapacity.ErrLimit
			}
			original := context.Background()
			if control != nil {
				original = control.ctx
			}
			producer, cancel := context.WithCancel(original)
			u = &liveUpstream{hub: h, key: key, label: label, source: source, control: control, ctx: producer, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), removed: make(chan struct{}), subscribers: make(map[*liveUpstreamSubscriber]struct{})}
			h.streams[key] = u
			if control != nil {
				control.operations.Add(1)
			}
		}
		u.mu.Lock()
		if u.retiring {
			removed := u.removed
			u.mu.Unlock()
			unlock()
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-liveUpstreamManagerDone(control):
				return nil, false, errOrdinaryHLSClosed
			case <-removed:
				continue
			}
		}
		if len(u.subscribers) >= liveUpstreamSubscriberLimit {
			u.mu.Unlock()
			unlock()
			return nil, false, livecapacity.ErrLimit
		}
		sub := &liveUpstreamSubscriber{upstream: u, control: control, chunks: make(chan []byte, liveUpstreamSubscriberLag+len(u.prime)), done: make(chan struct{}), closed: make(chan struct{}), armed: make(chan struct{})}
		for i, chunk := range u.prime {
			if i == 0 {
				chunk = alignToTransportStream(chunk)
			}
			if len(chunk) > 0 {
				sub.chunks <- chunk
			}
		}
		u.subscribers[sub] = struct{}{}
		u.mu.Unlock()
		// Transfer this exact already-reserved pending reference. No new grant
		// or capacity refund occurs while the original subscriber is closing.
		heldReference = false
		unlock()
		sub.arm(ctx)
		if created {
			if control != nil {
				u.stopOwner = familyMediaAfterFunc(control.ctx, func() { u.signalRetirement(context.Canceled) })
			} else {
				u.stopOwner = func() {}
			}
			go u.runOriginal()
		}
		select {
		case <-ctx.Done():
			_ = sub.Close()
			return nil, false, ctx.Err()
		case <-liveUpstreamManagerDone(control):
			_ = sub.Close()
			return nil, false, errOrdinaryHLSClosed
		case <-u.ready:
			if u.openErr != nil {
				_ = sub.Close()
				if !created && ctx.Err() == nil {
					select {
					case <-ctx.Done():
						return nil, false, ctx.Err()
					case <-u.removed:
						continue
					}
				}
				return nil, false, u.openErr
			}
			if ctx.Err() != nil {
				_ = sub.Close()
				return nil, false, ctx.Err()
			}
			if control != nil && (control.ctx.Err() != nil || u.control != nil && u.control.ctx.Err() != nil) {
				_ = sub.Close()
				return nil, false, errOrdinaryHLSClosed
			}
			return sub, !created, nil
		}
	}
}
func cloneLiveUpstreamHeaders(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
func liveUpstreamManagerDone(c *ordinaryHLSManager) <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.ctx.Done()
}
func (h *liveUpstreamHub) Subscribers(key string) int {
	h.mu.Lock()
	u := h.streams[key]
	h.mu.Unlock()
	if u == nil {
		return 0
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for sub := range u.subscribers {
		if !sub.failed.Load() {
			n++
		}
	}
	return n
}

func (u *liveUpstream) runOriginal() {
	if u.control != nil {
		defer u.control.operations.Done()
	}
	var terminal error
	defer func() { u.complete(terminal) }()
	body, err := u.openOriginal()
	if err != nil {
		terminal = err
		u.openErr = err
		close(u.ready)
		return
	}
	close(u.ready)
	reconnects := 0
	var openErr error
	for {
		started := time.Now()
		received, readErr := u.pump(body)
		u.mu.Lock()
		if u.body == body {
			u.body = nil
		}
		u.mu.Unlock()
		if u.ctx.Err() != nil {
			terminal = u.ctx.Err()
			return
		}
		if openErr != nil {
			readErr, openErr = openErr, nil
		}
		if !u.source.reconnect {
			terminal = readErr
			return
		}
		if received && time.Since(started) >= liveUpstreamHealthyAfter {
			reconnects = 0
		}
		if reconnects >= liveUpstreamMaxReconnects {
			terminal = readErr
			return
		}
		reconnects++
		delay := time.Duration(reconnects) * 500 * time.Millisecond
		log.Printf("[live-upstream] %s: upstream ended (%v), reconnecting in %v (attempt %d, %d viewer(s))", u.label, readErr, delay, reconnects, u.subscriberCount())
		select {
		case <-u.ctx.Done():
			terminal = u.ctx.Err()
			return
		case <-time.After(delay):
		}
		body, err = u.openOriginal()
		if err != nil {
			if u.ctx.Err() != nil {
				terminal = u.ctx.Err()
				return
			}
			openErr = err
			body = &liveUpstreamOwnedBody{ReadCloser: io.NopCloser(bytes.NewReader(nil))}
		}
	}
}
func (u *liveUpstream) openOriginal() (*liveUpstreamOwnedBody, error) {
	if err := u.ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := u.source.open(u.ctx)
	var body *liveUpstreamOwnedBody
	if raw != nil {
		body = &liveUpstreamOwnedBody{ReadCloser: raw}
	}
	u.mu.Lock()
	denied := u.retiring || u.ctx.Err() != nil
	if body != nil {
		u.body = body
	}
	u.mu.Unlock()
	if denied || err != nil || body == nil {
		if body != nil {
			_ = body.Close()
		}
		if err == nil {
			err = u.ctx.Err()
			if err == nil {
				err = errors.New("live upstream body unavailable")
			}
		}
		return nil, err
	}
	return body, nil
}
func (u *liveUpstream) pump(body *liveUpstreamOwnedBody) (received bool, err error) {
	watchCtx, stop := context.WithCancel(u.ctx)
	watchDone := make(chan struct{})
	activity := make(chan struct{}, 1)
	go func() {
		defer close(watchDone)
		timer := time.NewTimer(liveUpstreamReadTimeout)
		defer timer.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-activity:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(liveUpstreamReadTimeout)
			case <-timer.C:
				_ = body.Close()
				return
			}
		}
	}()
	defer func() { stop(); _ = body.Close(); <-watchDone }()
	for {
		chunk := make([]byte, liveUpstreamReadChunk)
		n, readErr := body.Read(chunk)
		if n > 0 {
			select {
			case activity <- struct{}{}:
			default:
			}
			received = true
			u.broadcast(chunk[:n])
		}
		if readErr != nil {
			return received, readErr
		}
	}
}
func (u *liveUpstream) broadcast(chunk []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.retiring {
		return
	}
	u.prime = append(u.prime, chunk)
	u.primeBytes += len(chunk)
	for u.primeBytes-len(u.prime[0]) >= liveUpstreamPrimeBytes {
		u.primeBytes -= len(u.prime[0])
		u.prime = u.prime[1:]
	}
	for sub := range u.subscribers {
		if sub.failed.Load() {
			continue
		}
		select {
		case sub.chunks <- chunk:
		default:
			sub.fail(errLiveUpstreamLag)
		}
	}
}
func (u *liveUpstream) subscriberCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	n := 0
	for sub := range u.subscribers {
		if !sub.failed.Load() {
			n++
		}
	}
	return n
}

// Signals are non-joining. Cancellation callbacks never wait on their own
// producer/pump/watchdog or body Close; all actual jobs precede the first join.
func (u *liveUpstream) prepareRetirementLocked(reason error) *liveUpstreamOwnedBody {
	if u.retiring {
		return nil
	}
	u.retiring = true
	u.reason = reason
	u.cancel()
	body := u.body
	if body != nil {
		u.closeJobs.Add(1)
	}
	return body
}
func (u *liveUpstream) closeRetainedBody(body *liveUpstreamOwnedBody) {
	if body != nil {
		go func() { defer u.closeJobs.Done(); _ = body.Close() }()
	}
}
func (u *liveUpstream) signalRetirement(reason error) {
	u.mu.Lock()
	body := u.prepareRetirementLocked(reason)
	u.mu.Unlock()
	u.closeRetainedBody(body)
}
func (u *liveUpstream) signalIfLast(sub *liveUpstreamSubscriber) {
	u.mu.Lock()
	remaining := 0
	for current := range u.subscribers {
		if !current.closing.Load() {
			remaining++
		}
	}
	var body *liveUpstreamOwnedBody
	if _, ok := u.subscribers[sub]; ok && remaining == 0 {
		body = u.prepareRetirementLocked(io.EOF)
	}
	u.mu.Unlock()
	u.closeRetainedBody(body)
}

func (u *liveUpstream) complete(reason error) {
	externallyCancelled := u.ctx.Err() != nil
	u.signalRetirement(reason)
	u.stopOwner()
	u.closeJobs.Wait()
	u.mu.Lock()
	u.ended = true
	u.body = nil
	u.prime = nil
	u.primeBytes = 0
	subs := make([]*liveUpstreamSubscriber, 0, len(u.subscribers))
	for sub := range u.subscribers {
		subs = append(subs, sub)
	}
	u.mu.Unlock()
	for _, sub := range subs {
		sub.fail(reason)
		if externallyCancelled {
			sub.requestClose()
		}
	}
	close(u.done)
	u.tryRemove()
}
func (u *liveUpstream) tryRemove() {
	u.hub.mu.Lock()
	u.mu.Lock()
	remove := u.ended && len(u.subscribers) == 0 && u.hub.streams[u.key] == u
	if remove {
		delete(u.hub.streams, u.key)
		close(u.removed)
	}
	u.mu.Unlock()
	u.hub.mu.Unlock()
}
func (u *liveUpstream) allClosing() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	for sub := range u.subscribers {
		if !sub.closing.Load() {
			return false
		}
	}
	return true
}
func (u *liveUpstream) removeReference(sub *liveUpstreamSubscriber) {
	u.mu.Lock()
	delete(u.subscribers, sub)
	u.mu.Unlock()
	u.tryRemove()
}

func (s *liveUpstreamSubscriber) arm(ctx context.Context) {
	s.lifeMu.Lock()
	s.stopCaller = familyMediaAfterFunc(ctx, s.parentCancelled)
	if s.control != nil {
		s.stopManager = familyMediaAfterFunc(s.control.ctx, s.parentCancelled)
	} else {
		s.stopManager = func() {}
	}
	s.lifeMu.Unlock()
	close(s.armed)
}
func (s *liveUpstreamSubscriber) parentCancelled() {
	s.signalRelease(context.Canceled)
	s.requestClose()
}
func (s *liveUpstreamSubscriber) requestClose() {
	s.queueClose.Do(func() { go func() { _ = s.Close() }() })
}
func (s *liveUpstreamSubscriber) fail(err error) {
	s.failed.Store(true)
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
	s.doneOnce.Do(func() { close(s.done) })
}
func (s *liveUpstreamSubscriber) signalRelease(err error) {
	s.lifeMu.Lock()
	s.readClosed = true
	s.lifeMu.Unlock()
	s.fail(err)
}
func (s *liveUpstreamSubscriber) readError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		return io.EOF
	}
	return s.err
}
func (s *liveUpstreamSubscriber) Read(p []byte) (int, error) {
	s.lifeMu.Lock()
	if s.readClosed {
		s.lifeMu.Unlock()
		return 0, s.readError()
	}
	s.readers.Add(1)
	s.lifeMu.Unlock()
	defer s.readers.Done()
	s.readMu.Lock()
	defer s.readMu.Unlock()
	s.lifeMu.Lock()
	closed := s.readClosed
	s.lifeMu.Unlock()
	if closed {
		return 0, s.readError()
	}
	if len(s.pending) == 0 {
		select {
		case chunk := <-s.chunks:
			s.pending = chunk
		case <-s.done:
			select {
			case chunk := <-s.chunks:
				s.pending = chunk
			default:
				return 0, s.readError()
			}
		}
	}
	s.lifeMu.Lock()
	closed = s.readClosed
	s.lifeMu.Unlock()
	if closed {
		return 0, s.readError()
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}
func (s *liveUpstreamSubscriber) Close() error {
	<-s.armed
	s.closeOnce.Do(func() {
		s.closing.Store(true)
		s.signalRelease(io.EOF)
		s.upstream.signalIfLast(s)
		s.stopCaller()
		s.stopManager()
		s.readers.Wait()
		if s.upstream.allClosing() || s.upstream.ctx.Err() != nil {
			<-s.upstream.done
		}
		s.upstream.removeReference(s)
		if s.control != nil {
			releaseLiveUpstreamReference(s.control)
		}
		close(s.closed)
	})
	<-s.closed
	return nil
}

// alignToTransportStream drops a partial leading packet so a late joiner starts on a TS packet
// boundary. FFmpeg would resync anyway; aligning just keeps the first probe clean.
func alignToTransportStream(chunk []byte) []byte {
	for i := 0; i < len(chunk) && i < mpegTSPacketSize; i++ {
		if chunk[i] != mpegTSSyncByte {
			continue
		}
		if next := i + mpegTSPacketSize; next >= len(chunk) || chunk[next] == mpegTSSyncByte {
			return chunk[i:]
		}
	}
	return chunk
}

// openLiveUpstream describes the provider connection for a session's channel.
func openLiveUpstream(session *HLSSession) liveUpstreamSource {
	path := session.Path
	tuning := session.LiveTuning
	if tuning.HDHomeRunInput {
		return liveUpstreamSource{open: func(ctx context.Context) (io.ReadCloser, error) {
			return openHDHomeRunStream(ctx, path, tuning.ProxyURL)
		}}
	}
	return liveUpstreamSource{reconnect: true, open: func(ctx context.Context) (io.ReadCloser, error) {
		requestCtx, cancelInput := context.WithCancel(ctx)
		// A failover-enabled tune has one startup budget across dial, headers
		// and the initial body. Stop this timer after sniffing data so a healthy
		// long-running provider connection keeps its normal lifetime.
		var startup func()
		if tuning.StationFailover != nil {
			startup = liveUpstreamAfterDelay(liveStationStartupTimeout, cancelInput)
		}
		opened := false
		defer func() {
			if startup != nil {
				startup()
			}
			if !opened {
				cancelInput()
			}
		}()
		client, err := netproxy.NewHTTPClientWithOptions(netproxy.HTTPClientOptions{ResponseHeaderTimeout: 15 * time.Second}, tuning.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("create live client: %w", err)
		}
		req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, path, nil)
		if err != nil {
			return nil, fmt.Errorf("prepare live request: %w", err)
		}
		req.Header.Set("User-Agent", liveStreamUserAgent)
		applyRequestHeaders(req.Header, tuning.RequestHeaders)
		resp, err := client.Do(req)
		if err != nil {
			client.CloseIdleConnections()
			return nil, fmt.Errorf("open live stream %s: %w", requestsecurity.URLForLog(path), err)
		}
		if resp.StatusCode >= http.StatusBadRequest {
			resp.Body.Close()
			client.CloseIdleConnections()
			return nil, fmt.Errorf("live stream returned status %d", resp.StatusCode)
		}
		contentType := strings.ToLower(resp.Header.Get("Content-Type"))
		reader := bufio.NewReaderSize(resp.Body, liveUpstreamReadChunk)
		// Header timeouts do not cover a provider that sends headers but no
		// body. Bound the initial sniff too, so a stopped startup cannot hold
		// its capacity reservation indefinitely.
		stopSniff := liveUpstreamAfterDelay(liveUpstreamReadTimeout, func() { _ = resp.Body.Close() })
		head, sniffErr := reader.Peek(7)
		stopSniff()
		if sniffErr != nil && len(head) == 0 {
			resp.Body.Close()
			client.CloseIdleConnections()
			return nil, fmt.Errorf("read live stream header: %w", sniffErr)
		}
		if strings.Contains(contentType, "mpegurl") || bytes.HasPrefix(head, []byte("#EXTM3U")) {
			resp.Body.Close()
			client.CloseIdleConnections()
			return nil, errLiveUpstreamNotTS
		}
		opened = true
		return liveUpstreamBody{Reader: reader, body: resp.Body, client: client, cancel: cancelInput}, nil
	}}
}

type liveUpstreamBody struct {
	io.Reader
	body   io.Closer
	client *http.Client
	cancel context.CancelFunc
}

func (b liveUpstreamBody) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	err := b.body.Close()
	b.client.CloseIdleConnections()
	return err
}

// The original bounded timer owns its real callback through completion, even
// when Stop races an already-running body Close. No deadline is renewed here.
func liveUpstreamAfterDelay(delay time.Duration, action func()) func() {
	done := make(chan struct{})
	timer := time.AfterFunc(delay, func() { defer close(done); action() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if timer.Stop() {
				close(done)
			}
		})
		<-done
	}
}
