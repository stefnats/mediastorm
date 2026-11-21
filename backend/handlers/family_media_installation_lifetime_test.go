//go:build linux

package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/services/profileunlock"
)

type familyKnownLifetime struct {
	t     *testing.T
	f     *familyKnownFixture
	joins []<-chan struct{}
	frees []func()
}

func newFamilyKnownLifetime(t *testing.T) *familyKnownLifetime {
	t.Helper()
	h := &familyKnownLifetime{t: t, f: newFamilyKnownFixture(t, true)}
	t.Cleanup(func() {
		h.f.cancel()
		h.f.wire.unblock()
		for i := len(h.frees) - 1; i >= 0; i-- {
			h.frees[i]()
		}
		for _, done := range h.joins {
			<-done
		}
		h.f.installed.Close()
	})
	return h
}
func (h *familyKnownLifetime) start(fn func()) <-chan struct{} {
	done := make(chan struct{})
	h.joins = append(h.joins, done)
	go func() { defer close(done); fn() }()
	return done
}
func familyKnownAwait(t *testing.T, c <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(15 * time.Second):
		t.Fatal(reason)
	}
}
func familyKnownPending(t *testing.T, c <-chan struct{}, reason string) {
	t.Helper()
	select {
	case <-c:
		t.Fatal(reason)
	default:
	}
}
func familyKnownUntil(t *testing.T, reason string, fn func() bool) {
	t.Helper()
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(reason)
}

type familyKnownHeldOpener struct {
	familyLocalRawOpener
	entered, release, returned chan struct{}
	held                       atomic.Bool
	once                       sync.Once
}

func (o *familyKnownHeldOpener) unblock() { o.once.Do(func() { close(o.release) }) }
func (o *familyKnownHeldOpener) OpenFamilyLocalSource(ctx context.Context, s *profileunlock.FamilyLocalSource) (profileunlock.FamilyLocalFile, error) {
	file, e := o.familyLocalRawOpener.OpenFamilyLocalSource(ctx, s)
	if e != nil || file == nil || !o.held.CompareAndSwap(false, true) {
		return file, e
	}
	close(o.entered)
	<-o.release
	close(o.returned)
	return file, e
}
func (h *familyKnownLifetime) holdOpen() *familyKnownHeldOpener {
	o := &familyKnownHeldOpener{familyLocalRawOpener: h.f.installed.localRaw.opener, entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	h.f.installed.localRaw.opener = o
	h.frees = append(h.frees, o.unblock)
	return o
}
func familyKnownCurrentID() string {
	var b [256]byte
	n := runtime.Stack(b[:], false)
	parts := strings.Fields(string(b[:n]))
	if len(parts) < 2 {
		return ""
	}
	return parts[1]
}
func familyKnownReadStack(id string) bool {
	b := make([]byte, 2<<20)
	n := runtime.Stack(b, true)
	for _, g := range strings.Split(string(b[:n]), "\n\n") {
		if strings.HasPrefix(g, "goroutine "+id+" ") && strings.Contains(g, "(*FamilyJellyfinStream).Read(") && strings.Contains(g, "(*familyJellyfinRawBody).Read(") {
			return true
		}
	}
	return false
}
func familyKnownWriteStack(id string) bool {
	b := make([]byte, 2<<20)
	n := runtime.Stack(b, true)
	for _, g := range strings.Split(string(b[:n]), "\n\n") {
		if strings.HasPrefix(g, "goroutine "+id+" ") && strings.Contains(g, "(*shareEntryConn).Write(") && strings.Contains(g, "internal/poll.(*FD).Write(") && strings.Contains(g, "runtime_pollWait(") {
			return true
		}
	}
	return false
}
func (h *familyKnownLifetime) heldRemote() (<-chan struct{}, *familyKnownRecorder) {
	h.f.wire.hold.Store(true)
	r, release := h.f.rawRequest(h.t, "GET", 0)
	h.frees = append(h.frees, release)
	w := newFamilyKnownRecorder()
	id := make(chan string, 1)
	done := h.start(func() { id <- familyKnownCurrentID(); familyKnownServe(h.f.installed.ServeJellyfinRaw, w, r) })
	actualID := <-id
	familyKnownAwait(h.t, h.f.wire.entered, "actual upstream HTTP download headers not observed")
	familyKnownUntil(h.t, "actual original HTTP body Read stack was not active", func() bool { return familyKnownReadStack(actualID) })
	return done, w
}
func (h *familyKnownLifetime) closedGate() {
	familyKnownUntil(h.t, "original installation gate not closed", func() bool { h.f.installed.mu.Lock(); defer h.f.installed.mu.Unlock(); return h.f.installed.closed })
}
func (h *familyKnownLifetime) socketServer(probe func(http.ResponseWriter) http.ResponseWriter) (*httptest.Server, *shareEntryListener, <-chan struct{}, <-chan string) {
	done := make(chan struct{})
	id := make(chan string, 1)
	var once sync.Once
	router := mux.NewRouter()
	router.HandleFunc("/known/items/{itemID}/raw", func(w http.ResponseWriter, r *http.Request) {
		defer once.Do(func() { close(done) })
		id <- familyKnownCurrentID()
		checked, release, e := h.f.pins.Authenticate(r, h.f.session.Token, "synthetic-client")
		if e != nil {
			http.Error(w, "actual fixture authentication unavailable", 503)
			return
		}
		defer release()
		if probe != nil {
			w = probe(w)
		}
		h.f.installed.ServeLocalRaw(w, checked)
	}).Methods(http.MethodGet, http.MethodHead)
	server := httptest.NewUnstartedServer(router)
	listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	h.frees = append(h.frees, server.Close)
	return server, listener, done, id
}
func (h *familyKnownLifetime) dial(server *httptest.Server) net.Conn {
	h.t.Helper()
	conn, e := net.Dial("tcp", server.Listener.Addr().String())
	if e != nil {
		h.t.Fatal(e)
	}
	// The real connection is owned before buffer/deadline setup can fail.
	h.frees = append(h.frees, func() { _ = conn.Close() })
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		h.t.Fatal("actual TCP client required")
	}
	if e = tcp.SetReadBuffer(1024); e != nil {
		h.t.Fatal(e)
	}
	if e = conn.SetDeadline(time.Now().Add(20 * time.Second)); e != nil {
		h.t.Fatal(e)
	}
	return conn
}
func (h *familyKnownLifetime) send(conn net.Conn) {
	h.t.Helper()
	if _, e := fmt.Fprintf(conn, "GET /known/items/known-local-item/raw?profileId=%s HTTP/1.1\r\nHost: synthetic\r\nX-Profile-Unlock: %s\r\nConnection: close\r\n\r\n", h.f.profile.ID, h.f.unlock); e != nil {
		h.t.Fatal(e)
	}
}

type familyKnownFlushProbe struct {
	http.ResponseWriter
	first func()
	count atomic.Int32
}

func (w *familyKnownFlushProbe) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *familyKnownFlushProbe) FlushError() error {
	if w.count.Add(1) == 1 && w.first != nil {
		w.first()
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

type familyKnownPostWriterOpener struct {
	familyLocalRawOpener
	beforeReader func()
}

func (o *familyKnownPostWriterOpener) OpenFamilyLocalSource(ctx context.Context, source *profileunlock.FamilyLocalSource) (profileunlock.FamilyLocalFile, error) {
	file, e := o.familyLocalRawOpener.OpenFamilyLocalSource(ctx, source)
	if e != nil || file == nil {
		return file, e
	}
	return &familyKnownPostWriterFile{FamilyLocalFile: file, beforeReader: o.beforeReader}, nil
}

type familyKnownPostWriterFile struct {
	profileunlock.FamilyLocalFile
	beforeReader func()
}

func (f *familyKnownPostWriterFile) OpenReader(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	f.beforeReader()
	return f.FamilyLocalFile.OpenReader(ctx, start, length)
}

// All eight readers come from the genuine checked descriptor. The ninth
// actual OpenReader produces Busy without retiring original source authority.
type familyKnownBusyReaderOpener struct {
	familyLocalRawOpener
	opened atomic.Int32
	busy   atomic.Bool
}

func (o *familyKnownBusyReaderOpener) OpenFamilyLocalSource(ctx context.Context, source *profileunlock.FamilyLocalSource) (profileunlock.FamilyLocalFile, error) {
	file, e := o.familyLocalRawOpener.OpenFamilyLocalSource(ctx, source)
	if e != nil || file == nil {
		return file, e
	}
	return &familyKnownBusyReaderFile{FamilyLocalFile: file, observer: o}, nil
}

type familyKnownBusyReaderFile struct {
	profileunlock.FamilyLocalFile
	observer *familyKnownBusyReaderOpener
}

func (f *familyKnownBusyReaderFile) OpenReader(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	for n := 0; n < 8; n++ {
		// The concrete file owns and closes these registered readers on the same
		// original source retirement; no independent closer grants permission.
		_, e := f.FamilyLocalFile.OpenReader(ctx, start, length)
		if e != nil {
			return nil, e
		}
		f.observer.opened.Add(1)
	}
	reader, e := f.FamilyLocalFile.OpenReader(ctx, start, length)
	if errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		f.observer.busy.Store(true)
	}
	return reader, e
}

func TestFamilyInstallationActualOutputAndClose(t *testing.T) {
	t.Run("local-close-joins-held-descriptor-work", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		r, release := h.f.localRequest(t, "GET")
		defer release()
		w := newFamilyControlledWriter()
		var writeOnce, callbackOnce sync.Once
		h.frees = append(h.frees, func() { writeOnce.Do(func() { close(w.writeGate) }); callbackOnce.Do(func() { close(w.callbackGate) }) })
		served := h.start(func() { familyKnownServe(h.f.installed.ServeLocalRaw, w, r) })
		familyKnownAwait(t, w.entered, "actual local file write not entered")
		if familyLocalRawFDs(t, h.f.path) < 2 {
			t.Fatal("held write did not retain actual original and reader descriptors")
		}
		closed := h.start(h.f.installed.Close)
		familyKnownAwait(t, w.interrupted, "original cancellation did not demand physical write deadline")
		familyKnownPending(t, closed, "installation Close skipped actual local output end")
		writeOnce.Do(func() { close(w.writeGate) })
		familyKnownPending(t, closed, "deadline callback completion was not joined")
		callbackOnce.Do(func() { close(w.callbackGate) })
		familyKnownAwait(t, served, "local handler did not join")
		familyKnownAwait(t, closed, "installation Close did not join local source")
		if familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("closed installation retained actual source descriptors")
		}
	})
	t.Run("jellyfin-close-joins-held-http-read", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		served, _ := h.heldRemote()
		closed := h.start(h.f.installed.Close)
		familyKnownAwait(t, served, "real original HTTP Read did not join on Close")
		familyKnownAwait(t, closed, "original stream/lease Close not joined")
		if familyJellyfinRawActive(h.f.installed.jellyfinRaw) != 0 {
			t.Fatal("actual HTTP entry refunded incompletely")
		}
	})
	t.Run("runtime-cancel-interrupts-held-actual-write", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		data := bytes.Repeat([]byte("actual-socket-source"), 128<<10)
		if e := os.WriteFile(h.f.path, data, 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := h.f.store.Pool().Exec(t.Context(), "UPDATE local_media_items SET size_bytes=$1 WHERE id='known-local-item'", len(data)); e != nil {
			t.Fatal(e)
		}
		server, listener, done, handlerID := h.socketServer(nil)
		conn := h.dial(server)
		h.send(conn)
		actual := <-listener.accepted
		actualID := <-handlerID
		familyKnownUntil(t, "genuine TCP Conn.Write was not physically active", func() bool { return actual.writing.Load() > 0 && familyKnownWriteStack(actualID) })
		h.f.cancel()
		familyKnownUntil(t, "original runtime did not demand actual socket deadline", func() bool { return actual.writeDemanded.Load() })
		familyKnownAwait(t, done, "actual held TCP handler did not join original runtime cancellation")
		h.f.installed.Close()
		if actual.writing.Load() != 0 || familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("actual network/source work survived Close")
		}
	})
	t.Run("healthy-final-physical-flush-before-owner-close", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		for _, span := range []string{"", "bytes=999999999-"} {
			var original context.Context
			old := h.f.installed.Boundary.capture
			h.f.installed.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
				o, e := old(r, target)
				if o != nil {
					original = o.Context()
				}
				return o, e
			}
			var early atomic.Bool
			var observed *familyKnownFlushProbe
			server, _, done, _ := h.socketServer(func(w http.ResponseWriter) http.ResponseWriter {
				observed = &familyKnownFlushProbe{ResponseWriter: w, first: func() {
					if original != nil && original.Err() == nil {
						early.Store(true)
					}
				}}
				return observed
			})
			conn := h.dial(server)
			if _, e := fmt.Fprintf(conn, "GET /known/items/known-local-item/raw?profileId=%s HTTP/1.1\r\nHost: synthetic\r\nX-Profile-Unlock: %s\r\nRange: %s\r\nConnection: close\r\n\r\n", h.f.profile.ID, h.f.unlock, span); e != nil {
				t.Fatal(e)
			}
			response, e := http.ReadResponse(bufio.NewReader(conn), nil)
			if e != nil {
				t.Fatal("actual complete response headers", e)
			}
			body, e := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if e != nil {
				t.Fatal("actual final response framing incomplete", e)
			}
			if span == "" {
				if response.StatusCode != 200 || !bytes.Equal(body, h.f.data) {
					t.Fatal("actual healthy final wire body incomplete")
				}
			} else {
				if response.StatusCode != 416 || response.ContentLength != 0 || len(body) != 0 || response.Header.Get("Content-Range") != fmt.Sprintf("bytes */%d", len(h.f.data)) {
					t.Fatal("actual guarded416 framing incomplete", response.StatusCode, response.ContentLength)
				}
			}
			familyKnownAwait(t, done, "actual raw TCP handler did not end")
			if !early.Load() || observed == nil || observed.count.Load() < 1 || original == nil || original.Err() == nil {
				t.Fatal("actual first physical Flush did not precede original owner retirement")
			}
			h.f.installed.Boundary.capture = old
		}
	})
	t.Run("preheader-denial-never-publishes-empty-200", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		// This nonterminal actual capacity denial must have complete HTTP
		// framing before original q.Close invalidates its guarded deadline.
		originalOpenerForBusy := h.f.installed.localRaw.opener
		busy := &familyKnownBusyReaderOpener{familyLocalRawOpener: originalOpenerForBusy}
		h.f.installed.localRaw.opener = busy
		var originalBusy context.Context
		captureForBusy := h.f.installed.Boundary.capture
		h.f.installed.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
			o, e := captureForBusy(r, target)
			if o != nil {
				originalBusy = o.Context()
			}
			return o, e
		}
		var flushedBusy atomic.Bool
		server, _, busyDone, _ := h.socketServer(func(w http.ResponseWriter) http.ResponseWriter {
			return &familyKnownFlushProbe{ResponseWriter: w, first: func() {
				if originalBusy != nil && originalBusy.Err() == nil {
					flushedBusy.Store(true)
				}
			}}
		})
		conn := h.dial(server)
		h.send(conn)
		response, e := http.ReadResponse(bufio.NewReader(conn), nil)
		if e != nil {
			t.Fatal("actual post-writer Busy response headers", e)
		}
		generic, e := io.ReadAll(response.Body)
		_ = response.Body.Close()
		var envelope map[string]string
		if e != nil || response.StatusCode != 403 || response.ContentLength != int64(len(generic)) || json.Unmarshal(generic, &envelope) != nil || envelope["code"] != "family_media_unavailable" || envelope["error"] == "" {
			t.Fatal("actual post-writer generic denial framing incomplete", response.StatusCode, e)
		}
		familyKnownAwait(t, busyDone, "actual Busy handler/FD cleanup did not join")
		if !busy.busy.Load() || busy.opened.Load() != 8 || !flushedBusy.Load() || originalBusy == nil || originalBusy.Err() == nil || familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("actual reader bound/original-live Flush/FD retirement not witnessed")
		}
		h.f.installed.localRaw.opener = originalOpenerForBusy
		h.f.installed.Boundary.capture = captureForBusy

		// The descriptor is genuinely open/current first. Mutate only its
		// actual name at OpenReader, after the real guarded writer exists.
		originalOpener := h.f.installed.localRaw.opener
		actualWriter := newFamilyKnownRecorder()
		var originalAtFailure context.Context
		captureBeforeFailure := h.f.installed.Boundary.capture
		h.f.installed.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
			o, e := captureBeforeFailure(r, target)
			if o != nil {
				originalAtFailure = o.Context()
			}
			return o, e
		}
		var postWriter atomic.Bool
		h.f.installed.localRaw.opener = &familyKnownPostWriterOpener{familyLocalRawOpener: originalOpener, beforeReader: func() {
			if actualWriter.deadlineCalls.Load() < 2 {
				t.Error("actual response writer was not installed before OpenReader")
			}
			postWriter.Store(true)
			if e := os.Rename(h.f.path, h.f.path+".retired"); e != nil {
				t.Error(e)
			}
		}}
		before, releaseBefore := h.f.localRequest(t, "GET")
		abortedBefore := familyKnownServe(h.f.installed.ServeLocalRaw, actualWriter, before)
		releaseBefore()
		if !postWriter.Load() || !abortedBefore && actualWriter.Code < 400 || bytes.Equal(actualWriter.Body.Bytes(), h.f.data) || familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("actual post-writer failure became success or leaked source")
		}
		// Real invalidation may already have cancelled q/its physical write
		// deadline. Complete framing is guaranteed by the nonterminal Busy
		// case above; this terminal case must only deny and physically retire.
		if originalAtFailure == nil || originalAtFailure.Err() == nil || familyLocalRawFDs(t, h.f.path+".retired") != 0 {
			t.Fatal("actual invalidated source/owner did not physically retire")
		}
		h.f.installed.Boundary.capture = captureBeforeFailure

		if e := os.Rename(h.f.path+".retired", h.f.path); e != nil {
			t.Fatal(e)
		}
		h.f.installed.localRaw.opener = originalOpener

		old := h.f.installed.Boundary.capture
		h.f.installed.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
			o, e := old(r, target)
			if e == nil {
				h.f.cancel()
				<-r.Context().Done()
			}
			return o, e
		}
		r, release := h.f.localRequest(t, "GET")
		defer release()
		w := newFamilyKnownRecorder()
		aborted := familyKnownServe(h.f.installed.ServeLocalRaw, w, r)
		if w.Code == 200 || w.Body.Len() == len(h.f.data) || !aborted && w.Code != 503 || familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("pre-header cancellation produced empty/success response", w.Code)
		}
	})
	t.Run("late-opener-closes-before-lifetime-refund", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		hold := h.holdOpen()
		r, release := h.f.localRequest(t, "GET")
		defer release()
		w := newFamilyKnownRecorder()
		served := h.start(func() { familyKnownServe(h.f.installed.ServeLocalRaw, w, r) })
		familyKnownAwait(t, hold.entered, "genuine checked descriptor open did not enter late-return boundary")
		if familyLocalRawFDs(t, h.f.path) == 0 {
			t.Fatal("late return boundary did not own actual descriptor")
		}
		closed := h.start(h.f.installed.Close)
		h.closedGate()
		familyKnownPending(t, closed, "installation refunded actual pending opener")
		hold.unblock()
		familyKnownAwait(t, hold.returned, "actual late opener not released")
		familyKnownAwait(t, served, "late source cleanup not joined")
		familyKnownAwait(t, closed, "Close did not join actual late source")
		if familyLocalRawFDs(t, h.f.path) != 0 || w.Body.Len() == len(h.f.data) {
			t.Fatal("late source escaped cleanup or published body")
		}
	})
	t.Run("fixed-original-source-expiry", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		expiry := time.Now().UTC().Add(3 * time.Second).Truncate(time.Microsecond)
		if _, e := h.f.store.Pool().Exec(t.Context(), "UPDATE accounts SET expires_at=$1 WHERE id='actor'", expiry); e != nil {
			t.Fatal(e)
		}
		fresh, release := h.f.request(t, "GET", "/?profileId="+h.f.profile.ID)
		defer release()
		h.f.unlock = h.f.issue(t, fresh)
		fresh.Header.Set(profileunlock.Header, h.f.unlock)
		q, ok := h.f.installed.Boundary.Begin(newFamilyKnownRecorder(), fresh, "")
		if !ok {
			t.Fatal("actual renewed frame/minimum expiry capture failed")
		}
		defer q.Close()
		if !q.ExpiresAt().Equal(expiry) || q.Handoff(fresh) != nil {
			t.Fatal("source owner did not retain actual account minimum")
		}
		select {
		case <-q.Context().Done():
		case <-time.After(6 * time.Second):
			t.Fatal("actual original expiry not observed")
		}
		if q.Check(t.Context()) == nil || !q.ExpiresAt().Equal(expiry) {
			t.Fatal("expired original was renewed by runtime or check")
		}
	})
	t.Run("caller-cancel-keeps-original-lifetime-rule", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		hold := h.holdOpen()
		r, release := h.f.localRequest(t, "GET")
		defer release()
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		r = r.WithContext(ctx)
		var oldOwner vodOriginalOwner
		original := h.f.installed.Boundary.capture
		h.f.installed.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
			o, e := original(r, target)
			oldOwner = o
			return o, e
		}
		served := h.start(func() { familyKnownServe(h.f.installed.ServeLocalRaw, newFamilyKnownRecorder(), r) })
		familyKnownAwait(t, hold.entered, "original consumer never reached actual source")
		cancel()
		hold.unblock()
		familyKnownAwait(t, served, "caller-cancelled actual source did not end")
		if oldOwner == nil || oldOwner.Check(t.Context()) == nil || familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("cancelled consumer retained or renewed original source")
		}
		h.f.installed.Boundary.capture = original
		fresh, stop := h.f.localRequest(t, "GET")
		defer stop()
		w := newFamilyKnownRecorder()
		if familyKnownServe(h.f.installed.ServeLocalRaw, w, fresh) || w.Code != 200 {
			t.Fatal("departed caller retired whole installation")
		}
	})
	t.Run("all-raw-consumers-signalled-before-first-join", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		hold := h.holdOpen()
		r, release := h.f.localRequest(t, "GET")
		defer release()
		local := h.start(func() { familyKnownServe(h.f.installed.ServeLocalRaw, newFamilyKnownRecorder(), r) })
		familyKnownAwait(t, hold.entered, "held actual local opener not active")
		remote, _ := h.heldRemote()
		closed := h.start(h.f.installed.Close)
		h.closedGate()
		familyKnownAwait(t, remote, "remote actual body not cancelled while Local join remained held")
		familyKnownPending(t, closed, "Close skipped held Local setup")
		familyKnownPending(t, local, "held Local setup ended without physical release")
		hold.unblock()
		familyKnownAwait(t, local, "local source not joined")
		familyKnownAwait(t, closed, "both known consumers not joined")
	})
	t.Run("repeated-concurrent-close-has-one-end", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		hold := h.holdOpen()
		r, release := h.f.localRequest(t, "GET")
		defer release()
		served := h.start(func() { familyKnownServe(h.f.installed.ServeLocalRaw, newFamilyKnownRecorder(), r) })
		familyKnownAwait(t, hold.entered, "actual late opener not entered")
		// Public composition DATA cannot disable the private original cleanup.
		h.f.installed.Boundary = nil
		h.f.installed.ParentFiles = nil
		var closes []<-chan struct{}
		for n := 0; n < 4; n++ {
			closes = append(closes, h.start(h.f.installed.Close))
		}
		h.closedGate()
		for _, done := range closes {
			familyKnownPending(t, done, "a concurrent Close skipped the same actual end")
		}
		hold.unblock()
		familyKnownAwait(t, served, "original raw operation not joined")
		for _, done := range closes {
			familyKnownAwait(t, done, "concurrent Close did not join same completion")
		}
	})
	t.Run("closed-installation-denies-before-capture", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		var captures atomic.Int32
		original := h.f.installed.Boundary.capture
		h.f.installed.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
			captures.Add(1)
			return original(r, target)
		}
		h.f.installed.Close()
		local, release := h.f.localRequest(t, "GET")
		defer release()
		remote, stop := h.f.rawRequest(t, "GET", 0)
		defer stop()
		for _, entry := range []struct {
			fn http.HandlerFunc
			r  *http.Request
		}{{h.f.installed.ServeLocalRaw, local}, {h.f.installed.ServeJellyfinRaw, remote}} {
			w := newFamilyKnownRecorder()
			if familyKnownServe(entry.fn, w, entry.r) || w.Code != 503 {
				t.Fatal("closed selected consumer did not fail unavailable", w.Code)
			}
		}
		if captures.Load() != 0 || h.f.wire.requests.Load() != 0 || familyLocalRawFDs(t, h.f.path) != 0 {
			t.Fatal("closed consumer reached actual capture/source")
		}
	})
	t.Run("genuine-original-never-needs-legacy-request", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		r, release := h.f.localRequest(t, "GET")
		defer release()
		q, ok := h.f.installed.Boundary.Begin(newFamilyKnownRecorder(), r, "")
		if !ok {
			t.Fatal("actual original capture failed")
		}
		if q.owner.original == nil || q.owner.request != nil {
			q.Close()
			t.Fatal("actual full original was replaced by legacy fake request")
		}
		q.Close()
		fresh, stop := h.f.localRequest(t, "GET")
		defer stop()
		w := newFamilyKnownRecorder()
		if familyKnownServe(h.f.installed.ServeLocalRaw, w, fresh) || w.Code != 200 {
			t.Fatal("actual nil-legacy-request local flow failed")
		}
		remote, end := h.f.rawRequest(t, "GET", 0)
		defer end()
		w2 := newFamilyKnownRecorder()
		if familyKnownServe(h.f.installed.ServeJellyfinRaw, w2, remote) || w2.Code != 200 {
			t.Fatal("actual nil-legacy-request remote flow failed")
		}
	})
}
