package datastore

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The barrier delegates actual pgx queries and holds their real return stack.
// It does not stand in for a transaction acknowledgement or source authority.
type familyOwnerLifetimeTrace struct {
	fragment    string
	armed       atomic.Bool
	remaining   atomic.Int32
	starts      atomic.Int32
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	unblockOnce sync.Once
}
type familyOwnerLifetimeTraceKey struct{}

func newFamilyOwnerLifetimeTrace() *familyOwnerLifetimeTrace {
	return &familyOwnerLifetimeTrace{fragment: "FROM accounts a WHERE a.id=", entered: make(chan struct{}), release: make(chan struct{})}
}
func (b *familyOwnerLifetimeTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	b.starts.Add(1)
	selected := false
	if b.armed.Load() && strings.Contains(data.SQL, b.fragment) && b.remaining.Add(-1) == 0 {
		selected = b.armed.CompareAndSwap(true, false)
	}
	return context.WithValue(ctx, familyOwnerLifetimeTraceKey{}, selected)
}
func (b *familyOwnerLifetimeTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if selected, _ := ctx.Value(familyOwnerLifetimeTraceKey{}).(bool); selected {
		b.once.Do(func() { close(b.entered); <-b.release })
	}
}
func (b *familyOwnerLifetimeTrace) arm(fragment string, nth int32) {
	b.fragment = fragment
	b.remaining.Store(nth)
	b.armed.Store(true)
}
func (b *familyOwnerLifetimeTrace) unblock() { b.unblockOnce.Do(func() { close(b.release) }) }

type familyOwnerLifetimeCase struct {
	t       *testing.T
	f       *ratingPGFixture
	trace   *familyOwnerLifetimeTrace
	request *http.Request
	cancel  context.CancelFunc
	release context.CancelFunc
	cleanup []func()
	joins   []<-chan struct{}
}

func newFamilyOwnerLifetimeCase(t *testing.T) *familyOwnerLifetimeCase {
	t.Helper()
	b := newFamilyOwnerLifetimeTrace()
	f := familyJellyfinTracedPG(t, b)
	ctx, cancel := context.WithCancel(t.Context())
	request, release := familyMediaRequest(t, f, ctx)
	h := &familyOwnerLifetimeCase{t: t, f: f, trace: b, request: request, cancel: cancel, release: release}
	t.Cleanup(func() {
		cancel()
		b.unblock()
		for i := len(h.cleanup) - 1; i >= 0; i-- {
			h.cleanup[i]()
		}
		for _, done := range h.joins {
			<-done
		}
		f.ds.Close()
		release()
	})
	return h
}
func (h *familyOwnerLifetimeCase) start(fn func()) <-chan struct{} {
	done := make(chan struct{})
	h.joins = append(h.joins, done)
	go func() { defer close(done); fn() }()
	return done
}
func familyOwnerAwait(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal(message)
	}
}
func familyOwnerPending(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(message)
	default:
	}
}
func (h *familyOwnerLifetimeCase) charged() int {
	familyMediaAdmission.Lock()
	defer familyMediaAdmission.Unlock()
	n := 0
	for o := range familyMediaAdmission.owners {
		if o.store == h.f.ds {
			n++
		}
	}
	return n
}
func (h *familyOwnerLifetimeCase) owner() *FamilyMediaOwner {
	h.t.Helper()
	o, e := h.f.ds.CaptureFamilyMediaOwner(h.request, "target", nil)
	if e != nil || o == nil {
		h.t.Fatal("actual original capture", e)
	}
	return o
}
func (h *familyOwnerLifetimeCase) lock(table string) (uint32, func()) {
	h.t.Helper()
	tx, e := h.f.admin.Begin(h.t.Context())
	if e != nil {
		h.t.Fatal(e)
	}
	schema := h.f.cfg.ConnConfig.RuntimeParams["application_name"]
	if _, e = tx.Exec(h.t.Context(), "LOCK TABLE "+pgx.Identifier{schema, table}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); e != nil {
		_ = tx.Rollback(context.Background())
		h.t.Fatal(e)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tx.Rollback(ctx)
		})
	}
	h.cleanup = append(h.cleanup, release)
	return tx.Conn().PgConn().PID(), release
}
func (h *familyOwnerLifetimeCase) waitSQL(holder uint32, fragment string) {
	h.t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		var seen bool
		e := h.f.admin.QueryRow(h.t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity a WHERE a.application_name=$1 AND a.wait_event_type='Lock' AND position($2 in a.query)>0 AND $3::integer=ANY(pg_blocking_pids(a.pid)))`, h.f.cfg.ConnConfig.RuntimeParams["application_name"], fragment, int32(holder)).Scan(&seen)
		if e != nil {
			h.t.Fatal("observe actual holder-linked SQL", e)
		}
		if seen {
			return
		}
		time.Sleep(time.Millisecond)
	}
	h.t.Fatal("actual holder-linked SQL waiter missing")
}
func (h *familyOwnerLifetimeCase) capture() (<-chan struct{}, *familyOwnerLifetimeOutcome) {
	out := &familyOwnerLifetimeOutcome{}
	done := h.start(func() { out.owner, out.err = h.f.ds.CaptureFamilyMediaOwner(h.request, "target", nil) })
	return done, out
}

type familyOwnerLifetimeOutcome struct {
	owner *FamilyMediaOwner
	err   error
}

func (h *familyOwnerLifetimeCase) closeStore() <-chan struct{} { return h.start(h.f.ds.Close) }
func (h *familyOwnerLifetimeCase) denyCapture(out *familyOwnerLifetimeOutcome) {
	h.t.Helper()
	if out.owner != nil {
		out.owner.Close()
		h.t.Fatal("retired capture published original")
	}
	if out.err == nil {
		h.t.Fatal("retired capture succeeded")
	}
}
func (h *familyOwnerLifetimeCase) heldCapture(closeCount int, callerOnly bool) {
	h.t.Helper()
	pid, unlock := h.lock("accounts")
	h.trace.arm("FROM sessions s JOIN accounts a", 1)
	work, out := h.capture()
	h.waitSQL(pid, "accounts")
	if h.charged() != 1 {
		h.t.Fatal("actual pending capture admission missing")
	}
	var closes []<-chan struct{}
	if callerOnly {
		h.cancel()
	} else {
		for i := 0; i < closeCount; i++ {
			closes = append(closes, h.closeStore())
		}
	}
	familyOwnerAwait(h.t, h.trace.entered, "actual cancelled capture stack did not reach query-end barrier")
	familyOwnerPending(h.t, work, "capture returned before actual PG stack ended")
	if h.charged() != 1 {
		h.t.Fatal("pending original refunded before actual capture end")
	}
	for _, done := range closes {
		familyOwnerPending(h.t, done, "store Close returned before actual capture end")
	}
	h.trace.unblock()
	unlock()
	familyOwnerAwait(h.t, work, "capture cleanup self-joined")
	h.denyCapture(out)
	for _, done := range closes {
		familyOwnerAwait(h.t, done, "actual store close not joined")
	}
	if h.charged() != 0 {
		h.t.Fatal("finished failed capture retained admission")
	}
	if callerOnly {
		if e := h.f.ds.pool.Ping(h.t.Context()); e != nil {
			h.t.Fatal("caller cancellation retired original store", e)
		}
	}
}

func TestFamilyOriginalOwnerCaptureAndStoreRetirement(t *testing.T) {
	t.Run("store-close-joins-blocked-original-capture", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldCapture(1, false) })
	t.Run("cancelled-capture-refunds-after-real-query-end", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldCapture(0, true) })
	t.Run("closed-store-denies-before-any-new-sql", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		h.f.ds.Close()
		before := h.trace.starts.Load()
		o, e := h.f.ds.CaptureFamilyMediaOwner(h.request, "target", nil)
		if o != nil || !errors.Is(e, ErrFamilyMediaUnavailable) || h.trace.starts.Load() != before || h.charged() != 0 {
			t.Fatal("closed admission reached PG or published owner", e)
		}
	})
	t.Run("close-versus-final-snapshot-publication", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		h.trace.arm("FROM accounts a WHERE a.id=", 2)
		work, out := h.capture()
		familyOwnerAwait(t, h.trace.entered, "actual final snapshot query barrier missing")
		closed := h.closeStore()
		h.waitClosedGate()
		familyOwnerPending(t, closed, "close skipped actual capture completion")
		h.trace.unblock()
		familyOwnerAwait(t, work, "late capture did not terminate")
		h.denyCapture(out)
		familyOwnerAwait(t, closed, "store did not join late capture")
	})
	t.Run("failed-capture-has-no-close-self-join", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		h.f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second'")
		// Acquire a fresh genuine frame after the real household change.
		r, release := familyMediaRequest(t, h.f, t.Context())
		defer release()
		h.request = r
		done, out := h.capture()
		familyOwnerAwait(t, done, "failed acquisition called Close before capture completion")
		if out.owner != nil || !errors.Is(out.err, ErrFamilyMediaPINRequired) || h.charged() != 0 {
			t.Fatal("actual active-household denial lost cleanup", out.err)
		}
	})
	t.Run("concurrent-store-close-has-one-physical-join", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldCapture(4, false) })
	t.Run("foreign-original-store-remains-live", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		other := newFamilyOwnerLifetimeCase(t)
		first, second := h.owner(), other.owner()
		h.f.ds.Close()
		if first.Context().Err() == nil || second.Check(other.t.Context()) != nil || other.charged() != 1 {
			t.Fatal("original store retirement crossed actual store identity")
		}
		source, e := second.CaptureLocalSource(t.Context(), "local-item")
		if e != nil {
			t.Fatal("foreign healthy actual source denied", e)
		}
		source.Close()
	})
	t.Run("handed-off-owner-is-retired-by-original-store", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		if o.Handoff(h.request) != nil {
			t.Fatal("actual handoff denied")
		}
		h.cancel()
		if o.Check(t.Context()) != nil {
			t.Fatal("successful handoff retained setup cancellation")
		}
		h.f.ds.Close()
		if o.Context().Err() == nil || h.charged() != 0 || o.Check(t.Context()) == nil {
			t.Fatal("handed-off original escaped actual store retirement")
		}
	})
	t.Run("standalone-owner-check-joins-before-refund", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldOwnerCheck(false) })
	t.Run("cancelled-consumer-does-not-retire-original", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldOwnerCheck(true) })
	t.Run("original-expiry-is-never-extended", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		expiry := time.Now().UTC().Add(2 * time.Second).Truncate(time.Microsecond)
		h.f.exec(t, "UPDATE accounts SET expires_at=$1 WHERE id='actor'", expiry)
		r, release := familyMediaRequest(t, h.f, t.Context())
		defer release()
		h.request = r
		o := h.owner()
		if !o.ExpiresAt().Equal(expiry) || o.Handoff(r) != nil {
			t.Fatal("actual original account minimum not retained")
		}
		deadline, ok := o.Context().Deadline()
		if !ok || !deadline.Equal(expiry) {
			t.Fatal("original context expiry extended")
		}
		select {
		case <-o.Context().Done():
		case <-time.After(4 * time.Second):
			t.Fatal("actual original deadline not observed")
		}
		if o.Check(t.Context()) == nil || !o.ExpiresAt().Equal(expiry) {
			t.Fatal("expired original revived")
		}
		o.Close()
	})
	t.Run("closed-gate-prevents-check-add-after-wait", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		pid, unlock := h.lock("accounts")
		h.trace.arm("FROM accounts a WHERE a.id=", 1)
		var e error
		checked := h.start(func() { e = o.Check(t.Context()) })
		h.waitSQL(pid, "accounts")
		closed := h.start(o.Close)
		familyOwnerAwait(t, h.trace.entered, "actual first Check did not reach cancelled return barrier")
		before := h.trace.starts.Load()
		if o.Check(t.Context()) == nil || o.Handoff(h.request) == nil || h.trace.starts.Load() != before {
			t.Fatal("closed owner admitted new work")
		}
		familyOwnerPending(t, closed, "closed gate did not retain earlier actual Check")
		if h.charged() != 1 {
			t.Fatal("earlier Check refunded")
		}
		h.trace.unblock()
		unlock()
		familyOwnerAwait(t, checked, "actual Check not joined")
		familyOwnerAwait(t, closed, "owner close not joined")
		if e == nil {
			t.Fatal("cancelled check succeeded")
		}
	})
}
func (h *familyOwnerLifetimeCase) waitClosedGate() {
	h.t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		familyMediaAdmission.Lock()
		closed := h.f.ds.familyMediaRetired
		familyMediaAdmission.Unlock()
		if closed {
			return
		}
		time.Sleep(time.Millisecond)
	}
	h.t.Fatal("original store admission gate did not close")
}
func (h *familyOwnerLifetimeCase) heldOwnerCheck(callerOnly bool) {
	h.t.Helper()
	o := h.owner()
	if o.Handoff(h.request) != nil {
		h.t.Fatal("actual original Handoff failed")
	}
	pid, unlock := h.lock("accounts")
	h.trace.arm("FROM accounts a WHERE a.id=", 1)
	ctx, cancel := context.WithCancel(h.t.Context())
	h.cleanup = append(h.cleanup, cancel)
	var err error
	work := h.start(func() { err = o.Check(ctx) })
	h.waitSQL(pid, "accounts")
	var closed <-chan struct{}
	if callerOnly {
		cancel()
	} else {
		closed = h.start(o.Close)
	}
	familyOwnerAwait(h.t, h.trace.entered, "actual owner Check cancelled return barrier missing")
	if h.charged() != 1 {
		h.t.Fatal("standalone Check refunded original before actual return")
	}
	if closed != nil {
		familyOwnerPending(h.t, closed, "owner Close skipped standalone Check join")
	}
	h.trace.unblock()
	unlock()
	familyOwnerAwait(h.t, work, "actual Check did not end")
	if err == nil {
		h.t.Fatal("cancelled Check succeeded")
	}
	if closed != nil {
		familyOwnerAwait(h.t, closed, "Close did not finish after actual Check")
		if h.charged() != 0 {
			h.t.Fatal("owner admission not refunded")
		}
	} else {
		if o.Check(h.t.Context()) != nil || o.Context().Err() != nil {
			h.t.Fatal("departed consumer retired original")
		}
	}
}

// This producer owns an actual pipe/read/FD stack. BindRetirement observes only
// its physical lifetime; the genuine PG source above remains the authority.
type familyOwnerPhysical struct {
	reader, writer                      *os.File
	signalled, readEnded, done, release chan struct{}
	signalOnce, releaseOnce             sync.Once
}

func (h *familyOwnerLifetimeCase) physical(bind func(func(), <-chan struct{}) error, begin func(context.Context) (func(), error)) *familyOwnerPhysical {
	h.t.Helper()
	finish, e := begin(h.t.Context())
	if e != nil {
		h.t.Fatal("actual source BeginOpen", e)
	}
	r, w, e := os.Pipe()
	if e != nil {
		finish()
		h.t.Fatal(e)
	}
	p := &familyOwnerPhysical{reader: r, writer: w, signalled: make(chan struct{}), readEnded: make(chan struct{}), done: make(chan struct{}), release: make(chan struct{})}
	h.cleanup = append(h.cleanup, func() { p.signal(); p.unblock() })
	if e = bind(p.signal, p.done); e != nil {
		finish()
		_ = r.Close()
		_ = w.Close()
		h.t.Fatal("actual source retirement binding", e)
	}
	finish()
	h.start(func() {
		defer close(p.done)
		var data [1]byte
		_, _ = r.Read(data[:])
		close(p.readEnded)
		<-p.release
		_ = r.Close()
		_ = w.Close()
	})
	return p
}
func (p *familyOwnerPhysical) signal() {
	p.signalOnce.Do(func() { close(p.signalled); _ = p.reader.Close(); _ = p.writer.Close() })
}
func (p *familyOwnerPhysical) unblock() { p.releaseOnce.Do(func() { close(p.release) }) }
func (h *familyOwnerLifetimeCase) local(o *FamilyMediaOwner) *FamilyLocalSource {
	h.t.Helper()
	s, e := o.CaptureLocalSource(h.t.Context(), "local-item")
	if e != nil || s == nil {
		h.t.Fatal("actual LOCAL PG source", e)
	}
	return s
}
func (h *familyOwnerLifetimeCase) remote(o *FamilyMediaOwner) *FamilyJellyfinSource {
	h.t.Helper()
	s, e := o.CaptureJellyfinSource(h.t.Context(), "remote-item")
	if e != nil || s == nil {
		h.t.Fatal("actual Jellyfin PG source", e)
	}
	return s
}
func (h *familyOwnerLifetimeCase) heldSourceCapture(remote bool) {
	h.t.Helper()
	o := h.owner()
	table := "local_media_items"
	item := "local-item"
	fragment := "FROM local_media_items i"
	if remote {
		table = "remote_media_items"
		item = "remote-item"
		fragment = "FROM remote_media_items i"
	}
	pid, unlock := h.lock(table)
	h.trace.arm(fragment, 1)
	var local *FamilyLocalSource
	var jellyfin *FamilyJellyfinSource
	var err error
	work := h.start(func() {
		if remote {
			jellyfin, err = o.CaptureJellyfinSource(h.t.Context(), item)
		} else {
			local, err = o.CaptureLocalSource(h.t.Context(), item)
		}
	})
	h.waitSQL(pid, table)
	closed := h.closeStore()
	familyOwnerAwait(h.t, h.trace.entered, "actual source capture cancellation return missing")
	familyOwnerPending(h.t, work, "source capture returned before its actual query stack")
	familyOwnerPending(h.t, closed, "store Close skipped actual source.ready")
	if h.charged() != 1 {
		h.t.Fatal("source acquisition refunded original owner")
	}
	h.trace.unblock()
	unlock()
	familyOwnerAwait(h.t, work, "source ready failure cleanup self-joined")
	familyOwnerAwait(h.t, closed, "source capture was not joined")
	if local != nil || jellyfin != nil || err == nil || h.charged() != 0 {
		h.t.Fatal("late capture published a source", err)
	}
}
func (h *familyOwnerLifetimeCase) heldLocalOperation(info bool) {
	h.t.Helper()
	o := h.owner()
	s := h.local(o)
	pid, unlock := h.lock("local_media_items")
	h.trace.arm("FROM local_media_items i", 1)
	var err error
	work := h.start(func() {
		if info {
			_, err = s.Info(h.t.Context())
		} else {
			err = s.Check(h.t.Context())
		}
	})
	h.waitSQL(pid, "local_media_items")
	closed := h.start(o.Close)
	familyOwnerAwait(h.t, h.trace.entered, "actual standalone LOCAL ACL cancellation return barrier missing")
	familyOwnerPending(h.t, closed, "owner Close skipped actual standalone LOCAL SQL")
	familyOwnerPending(h.t, work, "LOCAL operation returned before its PG stack")
	if h.charged() != 1 {
		h.t.Fatal("LOCAL standalone ACL work refunded original admission")
	}
	h.trace.unblock()
	unlock()
	familyOwnerAwait(h.t, work, "LOCAL check stack was not joined")
	familyOwnerAwait(h.t, closed, "LOCAL source Close did not join Check/Info")
	if err == nil || h.charged() != 0 {
		h.t.Fatal("closed LOCAL operation authorized or leaked admission", err)
	}
}
func (h *familyOwnerLifetimeCase) physicalStoreClose(remote bool) {
	h.t.Helper()
	o := h.owner()
	var p *familyOwnerPhysical
	if remote {
		s := h.remote(o)
		p = h.physical(s.BindRetirement, s.BeginOpen)
	} else {
		s := h.local(o)
		p = h.physical(s.BindRetirement, s.BeginOpen)
	}
	closed := h.closeStore()
	familyOwnerAwait(h.t, p.signalled, "actual source retirement was not signalled")
	familyOwnerAwait(h.t, p.readEnded, "actual pipe Read not interrupted")
	familyOwnerPending(h.t, closed, "store closed before real source producer ended")
	if h.charged() != 1 {
		h.t.Fatal("actual source producer refunded early")
	}
	p.unblock()
	familyOwnerAwait(h.t, p.done, "actual source producer did not end")
	familyOwnerAwait(h.t, closed, "actual store did not join producer")
	if h.charged() != 0 || h.f.ds.pool.Ping(h.t.Context()) == nil {
		h.t.Fatal("source join/pool teardown order incomplete")
	}
}
func TestFamilyOriginalSourceRetirementUnderStoreClose(t *testing.T) {
	t.Run("local-capture-ready-before-owner-refund", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldSourceCapture(false) })
	t.Run("jellyfin-capture-ready-before-owner-refund", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldSourceCapture(true) })
	t.Run("local-standalone-acl-check-is-joined", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldLocalOperation(false) })
	t.Run("local-info-current-sql-is-joined", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).heldLocalOperation(true) })
	t.Run("local-physical-retirement-before-store-close", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).physicalStoreClose(false) })
	t.Run("jellyfin-physical-retirement-before-store-close", func(t *testing.T) { newFamilyOwnerLifetimeCase(t).physicalStoreClose(true) })
	t.Run("all-original-sources-signalled-before-any-held-join", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		first, second := h.owner(), h.owner()
		a, b := h.local(first), h.remote(second)
		pa, pb := h.physical(a.BindRetirement, a.BeginOpen), h.physical(b.BindRetirement, b.BeginOpen)
		closed := h.closeStore()
		familyOwnerAwait(t, pa.signalled, "first source not signalled")
		familyOwnerAwait(t, pb.signalled, "second source not signalled before first join")
		familyOwnerAwait(t, pa.readEnded, "first real reader survived close")
		familyOwnerAwait(t, pb.readEnded, "second real reader survived close")
		if h.charged() != 2 {
			t.Fatal("one of the held source producers refunded early")
		}
		familyOwnerPending(t, closed, "store did not join both actual producers")
		pa.unblock()
		pb.unblock()
		familyOwnerAwait(t, closed, "all actual original sources not joined")
		if h.charged() != 0 {
			t.Fatal("completed source admissions leaked")
		}
	})
	t.Run("cross-provider-slot-stays-consumed-after-failure", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		local, e := o.CaptureLocalSource(t.Context(), "actual-missing-item")
		if local != nil || e == nil {
			t.Fatal("missing source authorized")
		}
		remote, e := o.CaptureJellyfinSource(t.Context(), "remote-item")
		if remote != nil || !errors.Is(e, ErrFamilyMediaBusy) {
			t.Fatal("failed original slot was renewed across providers", e)
		}
		o.Close()
		if o.source == nil {
			t.Fatal("original permanent source claim discarded")
		}
	})
	t.Run("source-capture-versus-store-closed-gate", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		h.trace.arm("FROM local_media_items i", 1)
		var source *FamilyLocalSource
		var err error
		work := h.start(func() { source, err = o.CaptureLocalSource(t.Context(), "local-item") })
		familyOwnerAwait(t, h.trace.entered, "actual ready-boundary source capture missing")
		closed := h.closeStore()
		h.waitClosedGate()
		familyOwnerPending(t, closed, "store closed while source ready stack held")
		if h.charged() != 1 {
			t.Fatal("source capture slot refunded before actual end")
		}
		h.trace.unblock()
		familyOwnerAwait(t, work, "source capture did not end")
		familyOwnerAwait(t, closed, "store did not join source capture")
		if source != nil || err == nil || o.source == nil {
			t.Fatal("source capture replaced or published a retired original", err)
		}
	})
	t.Run("concurrent-owner-and-store-close-share-real-end", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		s := h.local(o)
		p := h.physical(s.BindRetirement, s.BeginOpen)
		ownerClosed := h.start(o.Close)
		familyOwnerAwait(t, p.signalled, "owner close did not signal source")
		storeClosed := h.closeStore()
		h.waitClosedGate()
		familyOwnerPending(t, ownerClosed, "owner skipped actual end")
		familyOwnerPending(t, storeClosed, "store skipped same original end")
		if h.charged() != 1 {
			t.Fatal("shared close refunded early")
		}
		p.unblock()
		familyOwnerAwait(t, ownerClosed, "owner close not joined")
		familyOwnerAwait(t, storeClosed, "store close not joined")
		if h.charged() != 0 {
			t.Fatal("original admission did not refund exactly once")
		}
	})
	t.Run("source-cancel-callback-joins-before-admission-refund", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		s := h.local(o)
		finish, e := s.BeginOpen(t.Context())
		if e != nil {
			t.Fatal(e)
		}
		reader, writer, e := os.Pipe()
		if e != nil {
			finish()
			t.Fatal(e)
		}
		callbackEntered, callbackRelease, callbackDone, actualDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
		var unblockOnce sync.Once
		unblock := func() { unblockOnce.Do(func() { close(callbackRelease) }) }
		h.cleanup = append(h.cleanup, func() { unblock(); _ = reader.Close(); _ = writer.Close() })
		stop := context.AfterFunc(s.Context(), func() { defer close(callbackDone); close(callbackEntered); _ = reader.Close(); <-callbackRelease })
		if e = s.BindRetirement(func() { _ = reader.Close(); _ = writer.Close() }, actualDone); e != nil {
			finish()
			if stop() {
				close(callbackDone)
			}
			unblock()
			t.Fatal(e)
		}
		h.start(func() {
			defer close(actualDone)
			_, _ = io.Copy(io.Discard, reader)
			<-callbackEntered
			if stop() {
				close(callbackDone)
			}
			<-callbackDone
			_ = reader.Close()
			_ = writer.Close()
		})
		finish()
		closed := h.closeStore()
		familyOwnerAwait(t, callbackEntered, "actual source cancellation callback not entered")
		familyOwnerPending(t, closed, "signal callback mistaken for actual producer completion")
		if h.charged() != 1 {
			t.Fatal("original refunded before callback/producer end")
		}
		unblock()
		familyOwnerAwait(t, callbackDone, "actual callback not joined")
		familyOwnerAwait(t, actualDone, "producer not joined")
		familyOwnerAwait(t, closed, "store did not join callback and producer")
	})
	t.Run("copied-owner-cannot-retire-original-source", func(t *testing.T) {
		h := newFamilyOwnerLifetimeCase(t)
		o := h.owner()
		s := h.local(o)
		p := h.physical(s.BindRetirement, s.BeginOpen)
		copy := *o
		copy.Close()
		if copy.Check(t.Context()) == nil || o.Check(t.Context()) != nil || h.charged() != 1 {
			t.Fatal("copied owner mutated genuine original")
		}
		familyOwnerPending(t, p.signalled, "copied handle cancelled original source")
		closed := h.start(o.Close)
		familyOwnerAwait(t, p.signalled, "actual original close not signalled")
		familyOwnerPending(t, closed, "actual close skipped producer end")
		p.unblock()
		familyOwnerAwait(t, closed, "actual original source close not joined")
	})
}
