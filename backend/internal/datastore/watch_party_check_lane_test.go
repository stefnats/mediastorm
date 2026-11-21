//go:build linux

package datastore

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func partyCheckLanePoll(t *testing.T, what string, ready func() bool) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		if ready() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual " + what + " not observed")
}

func TestWatchPartyRetainedCheckAdmission(t *testing.T) {
	for _, kind := range []string{"same-account-real-check-serializes", "queued-caller-cancel-keeps-original", "bounded-sixteen-before-sql", "runtime-close-joins-active-and-queued"} {
		t.Run(kind, func(t *testing.T) {
			var counted atomic.Bool
			var queryCount atomic.Int32
			var actualGate atomic.Pointer[queryLifecycleGatedConn]
			var actualPhysical atomic.Pointer[pgconn.PgConn]
			var gatesMu sync.Mutex
			var gates []*queryLifecycleGatedConn
			trace := &bridgeSessionTransactionTrace{start: func(c *pgx.Conn, q string) {
				if !counted.Load() {
					return
				}
				queryCount.Add(1)
				if strings.Contains(q, "watch_party_playback_bindings") && strings.Contains(q, "FOR UPDATE") {
					actualPhysical.Store(c.PgConn())
					if gate, ok := c.PgConn().Conn().(*queryLifecycleGatedConn); ok {
						actualGate.Store(gate)
					}
				}
			}}
			var dial func(context.Context, string, string) (net.Conn, error)
			if kind == "runtime-close-joins-active-and-queued" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					gate := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
					gatesMu.Lock()
					gates = append(gates, gate)
					gatesMu.Unlock()
					return gate, nil
				}
			}
			f := newPartyPlaybackDS(t, "retained-check-lane", trace, dial, t.Context())
			first, second := f.owner(t), f.owner(t)
			var overflow *WatchPartyPlaybackOwner
			if kind == "bounded-sixteen-before-sql" {
				overflow = f.owner(t)
			}
			expiry := second.ExpiresAt()
			holder, e := f.f.room.base.admin.Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			work, cancelWork := context.WithCancel(t.Context())
			queued, cancelQueued := context.WithCancel(t.Context())
			var done []chan struct{}
			var closeDone chan struct{}
			// Every real child/late transport is owned even if any later witness
			// fails. Register this before the first goroutine is launched.
			t.Cleanup(func() {
				cancelQueued()
				cancelWork()
				recordingStateRollback(holder)
				gatesMu.Lock()
				all := append([]*queryLifecycleGatedConn(nil), gates...)
				gatesMu.Unlock()
				for _, gate := range all {
					gate.release()
				}
				for _, child := range done {
					bridgeSessionAwait(t, child)
				}
				if closeDone != nil {
					bridgeSessionAwait(t, closeDone)
				}
			})
			if _, e = holder.Exec(t.Context(), `SELECT token FROM `+pgx.Identifier{f.f.room.base.schema, "sessions"}.Sanitize()+` WHERE token=$1 FOR UPDATE`, f.grant.Session.Token); e != nil {
				t.Fatal(e)
			}
			var firstErr, secondErr error
			firstDone := make(chan struct{})
			done = append(done, firstDone)
			counted.Store(true)
			go func() { defer close(firstDone); firstErr = first.Check(work) }()
			partyCheckLanePoll(t, "held original SQL check", func() bool {
				var waiting bool
				if e := f.f.room.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%watch_party_playback_bindings%' AND query LIKE '%FOR UPDATE%')`, f.f.room.base.schema).Scan(&waiting); e != nil {
					t.Error(e)
					return false
				}
				return waiting
			})
			beforeQueries := queryCount.Load()
			secondDone := make(chan struct{})
			done = append(done, secondDone)
			go func() { defer close(secondDone); secondErr = second.Check(queued) }()
			a := first.runtime
			account := first.original.AccountID
			partyCheckLanePoll(t, "same-account queued original check", func() bool {
				a.mu.Lock()
				defer a.mu.Unlock()
				lane := a.partyCheckLanes[account]
				return a.partyCheckCount == 2 && lane != nil && lane.count == 2 && lane.active
			})
			if queryCount.Load() != beforeQueries {
				t.Fatal("queued retained check opened SQL before its account lane")
			}
			a.mu.Lock()
			active := a.actors[account]
			a.mu.Unlock()
			if active != 1 {
				t.Fatal("retained reads consumed both physical account slots", active)
			}
			switch kind {
			case "same-account-real-check-serializes":
				recordingStateRollback(holder)
				bridgeSessionAwait(t, firstDone)
				bridgeSessionAwait(t, secondDone)
				if firstErr != nil || secondErr != nil || first.Context().Err() != nil || second.Context().Err() != nil {
					t.Fatal("actual healthy original checks did not serialize", firstErr, secondErr)
				}
				if queryCount.Load() <= beforeQueries {
					t.Fatal("second check reused cached allow instead of fresh actual SQL")
				}
			case "queued-caller-cancel-keeps-original":
				cancelQueued()
				bridgeSessionAwait(t, secondDone)
				if secondErr == nil || second.Context().Err() != nil || !second.ExpiresAt().Equal(expiry) {
					t.Fatal("queued caller cancellation retired/renewed original owner", secondErr)
				}
				select {
				case <-firstDone:
					t.Fatal("queued cancel ended held active SQL")
				default:
				}
				recordingStateRollback(holder)
				bridgeSessionAwait(t, firstDone)
				if firstErr != nil || second.Check(t.Context()) != nil {
					t.Fatal("healthy original source could not perform a later genuine SQL check", firstErr)
				}
			case "bounded-sixteen-before-sql":
				queuedErrors := make([]error, 14)
				for i := range queuedErrors {
					child := make(chan struct{})
					done = append(done, child)
					go func(index int) { defer close(child); queuedErrors[index] = second.Check(queued) }(i)
				}
				partyCheckLanePoll(t, "sixteen original account waiters", func() bool {
					a.mu.Lock()
					defer a.mu.Unlock()
					lane := a.partyCheckLanes[account]
					return a.partyCheckCount == 16 && lane != nil && lane.count == 16 && lane.active
				})
				if queryCount.Load() != beforeQueries {
					t.Fatal("queued waiters consumed SQL connections/settings locks")
				}
				if e := overflow.Check(t.Context()); !errors.Is(e, ErrWatchRoomBusy) {
					t.Fatal("seventeenth original account operation did not deny before SQL", e)
				}
				if queryCount.Load() != beforeQueries {
					t.Fatal("overflow acquired SQL before denial")
				}
				cancelQueued()
				for _, child := range done[1:] {
					bridgeSessionAwait(t, child)
				}
				for _, e := range queuedErrors {
					if e == nil {
						t.Fatal("cancelled queued check claimed current source")
					}
				}
				if secondErr == nil || second.Context().Err() != nil || first.Context().Err() != nil {
					t.Fatal("queue refund retired unrelated healthy originals", secondErr)
				}
				recordingStateRollback(holder)
				bridgeSessionAwait(t, firstDone)
				if firstErr != nil {
					t.Fatal(firstErr)
				}
			case "runtime-close-joins-active-and-queued":
				gate, physical := actualGate.Load(), actualPhysical.Load()
				if gate == nil || physical == nil || physical.Conn() != gate {
					t.Fatal("actual blocked query transport was not retained")
				}
				gate.armed.Store(true)
				closeDone = make(chan struct{})
				go func() { defer close(closeDone); a.Close() }()
				bridgeSessionAwait(t, secondDone)
				bridgeSessionAwait(t, gate.closeEntered)
				if secondErr == nil {
					t.Fatal("queued runtime cancellation claimed source permission")
				}
				select {
				case <-firstDone:
					t.Fatal("active check returned before physical cleanup")
				default:
				}
				select {
				case <-closeDone:
					t.Fatal("runtime Close returned before physical SQL end")
				default:
				}
				select {
				case <-physical.CleanupDone():
					t.Fatal("actual transport cleanup falsely ended while Close held")
				default:
				}
				a.mu.Lock()
				charged := a.partyCheckCount
				lane := a.partyCheckLanes[account]
				held := lane != nil && lane.active
				a.mu.Unlock()
				if charged != 1 || !held {
					t.Fatal("active account lane refunded before physical cleanup", charged)
				}
				gate.release()
				recordingStateRollback(holder)
				bridgeSessionAwait(t, physical.CleanupDone())
				bridgeSessionAwait(t, firstDone)
				bridgeSessionAwait(t, closeDone)
				if firstErr == nil {
					t.Fatal("cancelled broken source SQL certified current authority")
				}
			}
			a.mu.Lock()
			count, lanes := a.partyCheckCount, len(a.partyCheckLanes)
			a.mu.Unlock()
			if count != 0 || lanes != 0 {
				t.Fatal("original lane admission not refunded after actual end", count, lanes)
			}
		})
	}
}
