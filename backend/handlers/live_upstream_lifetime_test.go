//go:build linux

package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These isolated legacy-hub inputs own actual pipe/file descriptors. They are
// physical-lifetime fixtures, never a positive media/PIN/source permission.
type liveLifeBody struct {
	reader, writer         *os.File
	active                 atomic.Int32
	entered, release       chan struct{}
	closeOnce, releaseOnce sync.Once
}

func liveLifePipe(t *testing.T, held bool) *liveLifeBody {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	b := &liveLifeBody{reader: reader, writer: writer, entered: make(chan struct{}), release: make(chan struct{})}
	if !held {
		b.unblock()
	}
	t.Cleanup(func() { b.unblock(); _ = b.Close() })
	if _, err = writer.Write(tsPacket(1)); err != nil {
		t.Fatal(err)
	}
	return b
}
func (b *liveLifeBody) Read(p []byte) (int, error) {
	b.active.Add(1)
	defer b.active.Add(-1)
	return b.reader.Read(p)
}
func (b *liveLifeBody) Close() error {
	var err error
	b.closeOnce.Do(func() { _ = b.writer.Close(); err = b.reader.Close(); close(b.entered); <-b.release })
	return err
}
func (b *liveLifeBody) unblock() { b.releaseOnce.Do(func() { close(b.release) }) }
func liveLifeAwait(t *testing.T, ch <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatal(why)
	}
}
func liveLifePending(t *testing.T, ch <-chan struct{}, why string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(why)
	default:
	}
}
func liveLifeSubscribe(t *testing.T, h *liveUpstreamHub, key string, b *liveLifeBody) *liveUpstreamSubscriber {
	t.Helper()
	sub, _, err := h.Subscribe(key, "synthetic", liveUpstreamSource{open: func(context.Context) (io.ReadCloser, error) { return b, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.unblock(); _ = sub.Close() })
	return sub
}
func liveLifeCount(h *liveUpstreamHub) int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.streams) }
func liveLifeRead(t *testing.T, sub *liveUpstreamSubscriber) {
	t.Helper()
	var packet [mpegTSPacketSize]byte
	if _, err := io.ReadFull(sub, packet[:]); err != nil || packet[0] != mpegTSSyncByte {
		t.Fatal("actual TS bytes not delivered", err)
	}
}

func TestLiveUpstreamActualProducerLifetime(t *testing.T) {
	t.Run("cancel-original-pending-open", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		path := filepath.Join(t.TempDir(), "original.ts")
		if err := os.WriteFile(path, tsPacket(1), 0600); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		entered := make(chan context.Context, 1)
		gate := make(chan struct{})
		var once sync.Once
		done := make(chan struct{})
		launched := false
		var actual *os.File
		t.Cleanup(func() {
			cancel()
			once.Do(func() { close(gate) })
			if launched {
				<-done
			}
		})
		go func() {
			defer close(done)
			sub, _, err := h.subscribe(ctx, nil, nil, "original", "synthetic", liveUpstreamSource{open: func(original context.Context) (io.ReadCloser, error) {
				var err error
				actual, err = os.Open(path)
				entered <- original
				<-gate
				return actual, err
			}})
			if sub != nil || err == nil {
				t.Error("cancelled original pending source returned a subscriber")
			}
		}()
		launched = true
		var original context.Context
		select {
		case original = <-entered:
		case <-time.After(15 * time.Second):
			t.Fatal("actual file opener not entered")
		}
		cancel()
		liveLifeAwait(t, original.Done(), "original producer was not signalled")
		liveLifePending(t, done, "pending source refunded before actual late body end")
		if _, err := actual.Stat(); err != nil {
			t.Fatal("actual pending descriptor disappeared before provider returned", err)
		}
		once.Do(func() { close(gate) })
		liveLifeAwait(t, done, "late original body was not joined")
		if _, err := actual.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("late actual file body not closed", err)
		}
		if liveLifeCount(h) != 0 {
			t.Fatal("retired original table entry remains")
		}
	})
	t.Run("cancel-waiter-without-retiring-other-subscriber", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		body := liveLifePipe(t, false)
		first := liveLifeSubscribe(t, h, "shared", body)
		liveLifeRead(t, first)
		ctx, cancel := context.WithCancel(t.Context())
		follower, shared, err := h.subscribe(ctx, nil, nil, "shared", "synthetic", liveUpstreamSource{open: func(context.Context) (io.ReadCloser, error) {
			return nil, errors.New("a follower must not replace original source")
		}})
		if err != nil || !shared {
			t.Fatal("actual follower denied", err)
		}
		t.Cleanup(func() { cancel(); _ = follower.Close() })
		cancel()
		liveLifeAwait(t, follower.closed, "cancelled follower did not join own reference")
		if first.upstream.ctx.Err() != nil || h.Subscribers("shared") != 1 {
			t.Fatal("cancelled viewer retired the other original viewer")
		}
		if _, err = body.writer.Write(tsPacket(2)); err != nil {
			t.Fatal(err)
		}
		liveLifeRead(t, first)
	})
	t.Run("late-open-body-physically-closed", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		body := liveLifePipe(t, true)
		ctx, cancel := context.WithCancel(t.Context())
		entered := make(chan struct{})
		gate := make(chan struct{})
		done := make(chan struct{})
		var once sync.Once
		started := false
		t.Cleanup(func() {
			cancel()
			once.Do(func() { close(gate) })
			body.unblock()
			if started {
				<-done
			}
		})
		go func() {
			defer close(done)
			sub, _, err := h.subscribe(ctx, nil, nil, "late", "synthetic", liveUpstreamSource{open: func(context.Context) (io.ReadCloser, error) { close(entered); <-gate; return body, nil }})
			if sub != nil || err == nil {
				t.Error("late actual body was adopted")
			}
		}()
		started = true
		liveLifeAwait(t, entered, "original pending opener missing")
		cancel()
		once.Do(func() { close(gate) })
		liveLifeAwait(t, body.entered, "late retained body Close not invoked")
		liveLifePending(t, done, "returned before actual late Close finished")
		if _, err := body.reader.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("late actual descriptor not signalled", err)
		}
		body.unblock()
		liveLifeAwait(t, done, "late body Close was not joined")
	})
	t.Run("last-subscriber-joins-original-read-loop", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		body := liveLifePipe(t, true)
		sub := liveLifeSubscribe(t, h, "read", body)
		ordinaryLifeWait(t, "actual original pipe Read not held", func() bool { return body.active.Load() == 1 })
		done := make(chan struct{})
		started := false
		t.Cleanup(func() {
			body.unblock()
			if started {
				<-done
			}
		})
		go func() { _ = sub.Close(); close(done) }()
		started = true
		liveLifeAwait(t, body.entered, "last reference did not close actual source")
		liveLifePending(t, sub.upstream.done, "producer completed before retained Close ended")
		liveLifePending(t, done, "last Close returned before original producer ended")
		if liveLifeCount(h) != 1 {
			t.Fatal("source table/refund preceded physical end")
		}
		body.unblock()
		liveLifeAwait(t, done, "original read/Close/producer not joined")
		if body.active.Load() != 0 || liveLifeCount(h) != 0 {
			t.Fatal("actual original reader/table not retired")
		}
	})
	t.Run("all-body-close-jobs-before-join", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		firstBody, secondBody := liveLifePipe(t, true), liveLifePipe(t, true)
		first := liveLifeSubscribe(t, h, "first", firstBody)
		second := liveLifeSubscribe(t, h, "second", secondBody)
		var wg sync.WaitGroup
		started := false
		t.Cleanup(func() {
			firstBody.unblock()
			secondBody.unblock()
			if started {
				wg.Wait()
			}
		})
		wg.Add(2)
		go func() { defer wg.Done(); _ = first.Close() }()
		go func() { defer wg.Done(); _ = second.Close() }()
		started = true
		liveLifeAwait(t, firstBody.entered, "first actual Close missing")
		liveLifeAwait(t, secondBody.entered, "second actual Close blocked behind first join")
		liveLifePending(t, first.upstream.done, "first producer completed early")
		liveLifePending(t, second.upstream.done, "second producer completed early")
		firstBody.unblock()
		secondBody.unblock()
		wg.Wait()
	})
	t.Run("original-watchdog-callback-joined", func(t *testing.T) {
		// A real timer callback is held inside actual file/pipe Close. Stop
		// must join that actual callback, not merely report timer cancellation.
		held := liveLifePipe(t, true)
		stop := liveUpstreamAfterDelay(time.Millisecond, func() { _ = held.Close() })
		stopDone := make(chan struct{})
		stopStarted := false
		t.Cleanup(func() {
			held.unblock()
			if stopStarted {
				<-stopDone
			} else {
				stop()
			}
		})
		liveLifeAwait(t, held.entered, "actual timer body Close did not enter")
		go func() { stop(); close(stopDone) }()
		stopStarted = true
		liveLifePending(t, stopDone, "timer Stop returned before original invoked Close ended")
		if _, err := held.reader.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Fatal("original timer did not close actual descriptor", err)
		}
		held.unblock()
		liveLifeAwait(t, stopDone, "actual timer Close callback not joined")
		serverEntered, serverDone := make(chan struct{}), make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(serverDone)
			w.Header().Set("Content-Type", "video/mp2t")
			_, _ = w.Write(tsPacket(1))
			w.(http.Flusher).Flush()
			close(serverEntered)
			<-r.Context().Done()
		}))
		defer server.Close()
		h := newTestLiveUpstreamHub()
		source := openLiveUpstream(liveTestSession(server.URL + "/silent.ts"))
		source.reconnect = false
		sub, _, err := h.Subscribe("silent", "synthetic", source)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Close()
		liveLifeAwait(t, serverEntered, "actual TCP source not opened")
		liveLifeRead(t, sub)
		liveLifeAwait(t, sub.upstream.done, "original 20s read watchdog did not join actual HTTP/body/pump")
		liveLifeAwait(t, serverDone, "actual source TCP request remains")
		if sub.upstream.body != nil {
			t.Fatal("actual body retained after completed original pump")
		}
		if err = sub.Close(); err != nil {
			t.Fatal(err)
		}
		if liveLifeCount(h) != 0 {
			t.Fatal("watchdog producer/ref table not removed after physical close")
		}
	})
	t.Run("new-opener-waits-old-physical-end", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		oldBody, newBody := liveLifePipe(t, true), liveLifePipe(t, false)
		old := liveLifeSubscribe(t, h, "same", oldBody)
		closeDone := make(chan struct{})
		newDone := make(chan struct{})
		secondID := make(chan string, 1)
		var opens atomic.Int32
		closedStarted, newStarted := false, false
		var next *liveUpstreamSubscriber
		t.Cleanup(func() {
			oldBody.unblock()
			if closedStarted {
				<-closeDone
			}
			if newStarted {
				<-newDone
			}
			if next != nil {
				_ = next.Close()
			}
		})
		go func() { _ = old.Close(); close(closeDone) }()
		closedStarted = true
		liveLifeAwait(t, oldBody.entered, "old actual source Close missing")
		go func() {
			defer close(newDone)
			secondID <- liveLifeCurrentGoID()
			var err error
			next, _, err = h.Subscribe("same", "synthetic", liveUpstreamSource{open: func(context.Context) (io.ReadCloser, error) { opens.Add(1); return newBody, nil }})
			if err != nil {
				t.Error(err)
			}
		}()
		newStarted = true
		var actualSecondID string
		select {
		case actualSecondID = <-secondID:
		case <-time.After(15 * time.Second):
			t.Fatal("second actual subscriber goroutine did not enter")
		}
		ordinaryLifeWait(t, "second actual Subscribe was not observed at original removed-channel wait", func() bool {
			if opens.Load() != 0 {
				t.Fatal("fresh provider opened before actual retiring-wait witness")
			}
			select {
			case <-newDone:
				t.Fatal("fresh source bypassed actual retiring wait")
			default:
			}
			return liveLifeRemovedWaitStack(actualSecondID)
		})
		liveLifePending(t, newDone, "fresh source passed old retained owner")
		if opens.Load() != 0 || liveLifeCount(h) != 1 {
			t.Fatal("fresh provider opened before old physical end")
		}
		oldBody.unblock()
		liveLifeAwait(t, closeDone, "old owner not joined")
		liveLifeAwait(t, newDone, "fresh source not admitted after real retirement")
		if next == nil || next.upstream == old.upstream || opens.Load() != 1 {
			t.Fatal("same identifier adopted old producer")
		}
	})
	t.Run("no-hub-mutex-held-across-close", func(t *testing.T) {
		h := newTestLiveUpstreamHub()
		held := liveLifePipe(t, true)
		otherBody := liveLifePipe(t, false)
		sub := liveLifeSubscribe(t, h, "held", held)
		done, otherDone := make(chan struct{}), make(chan struct{})
		started, otherStarted := false, false
		var other *liveUpstreamSubscriber
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(func() {
			cancel()
			held.unblock()
			if started {
				<-done
			}
			if otherStarted {
				<-otherDone
			}
			if other != nil {
				_ = other.Close()
			}
		})
		go func() { _ = sub.Close(); close(done) }()
		started = true
		liveLifeAwait(t, held.entered, "held actual body Close missing")
		go func() {
			defer close(otherDone)
			var err error
			other, _, err = h.subscribe(ctx, nil, nil, "other", "synthetic", liveUpstreamSource{open: func(context.Context) (io.ReadCloser, error) { return otherBody, nil }})
			if err != nil && ctx.Err() == nil {
				t.Error(err)
			}
		}()
		otherStarted = true
		// Failure cleanup first releases the real held Close, then joins the
		// actual competing operation; a mutex regression cannot strand it.
		liveLifeAwait(t, otherDone, "different source was blocked by a hub mutex held across Close")
		if other == nil {
			t.Fatal("independent current source not admitted")
		}
		liveLifeRead(t, other)
		if h.Subscribers("other") != 1 {
			t.Fatal("different current source was lost")
		}
		held.unblock()
		liveLifeAwait(t, done, "held original close not joined")
	})
}

// These observations inspect only the actual test goroutine/program counter.
// They confer no permission and do not substitute a sleep for real waiting.
func liveLifeCurrentGoID() string {
	var data [256]byte
	n := runtime.Stack(data[:], false)
	fields := strings.Fields(string(data[:n]))
	if len(fields) < 2 {
		return ""
	}
	return fields[1]
}
func liveLifeRemovedWaitStack(id string) bool {
	if id == "" {
		return false
	}
	data := make([]byte, 2<<20)
	n := runtime.Stack(data, true)
	for _, stack := range strings.Split(string(data[:n]), "\n\n") {
		lines := strings.Split(stack, "\n")
		if len(lines) == 0 || !strings.HasPrefix(lines[0], "goroutine "+id+" [select") {
			continue
		}
		for i, line := range lines {
			if !strings.Contains(line, "(*liveUpstreamHub).subscribe(") || i+1 >= len(lines) {
				continue
			}
			location := strings.Fields(lines[i+1])
			if len(location) == 0 {
				continue
			}
			colon := strings.LastIndex(location[0], ":")
			if colon < 0 {
				continue
			}
			file := location[0][:colon]
			at, err := strconv.Atoi(location[0][colon+1:])
			if err != nil {
				continue
			}
			source, err := os.ReadFile(file)
			if err != nil {
				source, err = os.ReadFile(filepath.Base(file))
			}
			if err != nil {
				continue
			}
			original := strings.Split(string(source), "\n")
			if at < 1 || at > len(original) {
				continue
			}
			end := at + 10
			if end > len(original) {
				end = len(original)
			}
			// Only the actual retiring selects wait on the local 'removed' channel.
			// The distinct initial-ready/error selects use u.ready/u.removed instead.
			excerpt := strings.Join(original[at-1:end], "\n")
			if strings.Contains(excerpt, "select {") && strings.Contains(excerpt, "case <-removed:") {
				return true
			}
		}
	}
	return false
}
