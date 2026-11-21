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
)

func nativeMaintenanceSeed(t *testing.T, f *nativeRoomFixture, prefix string, count int, expired bool) {
	t.Helper()
	expiry := time.Now().UTC().Add(time.Hour)
	if expired {
		expiry = time.Now().UTC().Add(-time.Minute)
	}
	f.f.base.exec(t, `INSERT INTO watch_rooms(id,creator_profile_id,title,media_type,item_id,created_at,expires_at)SELECT $1||lpad(i::text,4,'0'),'target','Synthetic maintenance DATA','movie','synthetic-maintenance',now(),$3 FROM generate_series(1,$2::integer)i`, prefix, count, expiry)
}
func nativeMaintenanceStatus(t *testing.T, f *nativeRoomFixture, id string) string {
	t.Helper()
	var status string
	if e := f.f.base.admin.QueryRow(t.Context(), `SELECT status FROM `+pgx.Identifier{f.f.base.schema, "watch_rooms"}.Sanitize()+` WHERE id=$1`, id).Scan(&status); e != nil {
		t.Fatal(e)
	}
	return status
}
func nativeMaintenancePoll(t *testing.T, what string, ready func() bool) {
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

func TestNativeRoomBoundedMaintenance(t *testing.T) {
	for _, kind := range []string{"actual-expiry-end", "actual-host-disconnect-end", "actual-all-left-end", "healthy-host-preserved", "ended-audit-window-delete", "current-room-nonce-before-update", "unattested-expiry-data-only", "unattested-active-not-adopted", "bounded-room-batches", "fair-cursor-no-starvation", "unknown-ack-no-false-count", "row-security-no-partial-sweep"} {
		t.Run(kind, func(t *testing.T) {
			var enabled, dropped atomic.Bool
			var dial func(context.Context, string, string) (net.Conn, error)
			if kind == "unknown-ack-no-false-count" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					return &bridgeSessionDropACK{Conn: conn, enabled: &enabled, actual: &parentDropCommit{Conn: conn, dropped: &dropped}}, nil
				}
			}
			fixtureKind := kind
			if kind == "actual-expiry-end" {
				fixtureKind = "expired-read-same-tx-terminal-data"
			}
			f := newNativeRoomFixture(t, fixtureKind, nil, dial, t.Context())
			a, room := f.f.authority, f.f.room.ID
			if kind == "actual-expiry-end" {
				timer := time.NewTimer(time.Until(f.f.room.ExpiresAt))
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-t.Context().Done():
					t.Fatal("natural original expiry cancelled")
				}
			}
			if kind == "actual-host-disconnect-end" {
				f.join(t, "household", false)
			}
			if kind == "actual-host-disconnect-end" || kind == "actual-all-left-end" || kind == "current-room-nonce-before-update" {
				f.f.base.exec(t, `UPDATE watch_room_members SET last_seen_at=$2 WHERE room_id=$1 AND profile_id='target'`, room, time.Now().UTC().Add(-3*time.Minute))
			}
			if kind == "ended-audit-window-delete" {
				if _, e := a.EndNativeRoom(f.request(t, "POST", false, t.Context()), "target", room); e != nil {
					t.Fatal(e)
				}
				f.f.base.exec(t, `UPDATE watch_rooms SET ended_at=$2 WHERE id=$1`, room, time.Now().UTC().Add(-25*time.Hour))
			}
			if kind == "current-room-nonce-before-update" {
				f.f.base.exec(t, `UPDATE watch_rooms SET room_incarnation=pg_catalog.gen_random_uuid() WHERE id=$1`, room)
			}
			if kind == "unattested-expiry-data-only" || kind == "unknown-ack-no-false-count" || kind == "row-security-no-partial-sweep" {
				nativeMaintenanceSeed(t, f, "synthetic-maint-expired-", 1, true)
			}
			if kind == "unattested-active-not-adopted" {
				nativeMaintenanceSeed(t, f, "synthetic-maint-active-", 1, false)
				f.f.base.exec(t, `UPDATE watch_rooms SET created_at=$2 WHERE id=$1`, "synthetic-maint-active-0001", time.Now().UTC().Add(-3*time.Minute))
			}
			if kind == "bounded-room-batches" {
				nativeMaintenanceSeed(t, f, "synthetic-maint-batch-", 130, true)
				cursor, visited, ended, _, e := a.maintenanceBatch("")
				if e != nil || visited != 64 || cursor == "" || ended <= 0 || ended > 64 {
					t.Fatal("first actual maintenance batch not bounded", cursor, visited, ended, e)
				}
				var remaining int
				if e = f.f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_rooms WHERE id LIKE 'synthetic-maint-batch-%' AND status<>'ended'`).Scan(&remaining); e != nil || remaining <= 0 {
					t.Fatal("one batch crossed actual64-row bound", remaining, e)
				}
				for pass := 0; pass < 4 && cursor != ""; pass++ {
					cursor, _, _, _, e = a.maintenanceBatch(cursor)
					if e != nil {
						t.Fatal(e)
					}
				}
				if e = f.f.base.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM watch_rooms WHERE id LIKE 'synthetic-maint-batch-%' AND status<>'ended'`).Scan(&remaining); e != nil || remaining != 0 {
					t.Fatal("bounded actual batches lost terminal work", remaining, e)
				}
				return
			}
			if kind == "fair-cursor-no-starvation" {
				nativeMaintenanceSeed(t, f, "0-healthy-maint-", 300, false)
				nativeMaintenanceSeed(t, f, "zz-expired-maint-", 1, true)
				a.maintenanceTick()
				if nativeMaintenanceStatus(t, f, "zz-expired-maint-0001") != "lobby" {
					t.Fatal("first tick ignored its actual256-room work bound")
				}
				a.maintenanceTick()
				if nativeMaintenanceStatus(t, f, "zz-expired-maint-0001") != "ended" {
					t.Fatal("healthy earlier rooms starved later expiry across ticks")
				}
				return
			}
			if kind == "row-security-no-partial-sweep" {
				f.f.base.exec(t, `ALTER TABLE watch_room_members ENABLE ROW LEVEL SECURITY;ALTER TABLE watch_room_members FORCE ROW LEVEL SECURITY`)
			}
			enabled.Store(true)
			_, _, ended, deleted, e := a.maintenanceBatch("")
			if kind == "unknown-ack-no-false-count" {
				if !errors.Is(e, ErrWatchRoomUnknown) || !dropped.Load() || ended != 0 || deleted != 0 {
					t.Fatal("unknown real maintenance ACK claimed counters", e, ended, deleted)
				}
				if nativeMaintenanceStatus(t, f, "synthetic-maint-expired-0001") != "ended" {
					t.Fatal("unknown ACK falsely implied rollback")
				}
				return
			}
			if kind == "row-security-no-partial-sweep" {
				if e == nil || ended != 0 || deleted != 0 || nativeMaintenanceStatus(t, f, "synthetic-maint-expired-0001") != "lobby" {
					t.Fatal("unknown RLS performed partial housekeeping", e, ended, deleted)
				}
				return
			}
			if e != nil {
				t.Fatal("actual original maintenance batch", e)
			}
			switch kind {
			case "actual-expiry-end", "actual-host-disconnect-end", "actual-all-left-end":
				want := "expired"
				if kind == "actual-host-disconnect-end" {
					want = "host_disconnected"
				}
				if kind == "actual-all-left-end" {
					want = "all_left"
				}
				var status, reason string
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT status,end_reason FROM watch_rooms WHERE id=$1`, room).Scan(&status, &reason); e != nil || status != "ended" || reason != want || ended != 1 {
					t.Fatal("original lifecycle reason/terminal count wrong", status, reason, ended, e)
				}
			case "healthy-host-preserved", "current-room-nonce-before-update":
				if ended != 0 || deleted != 0 || nativeMaintenanceStatus(t, f, room) != "lobby" {
					t.Fatal("healthy/changed original room was adopted or ended", ended, deleted)
				}
			case "ended-audit-window-delete":
				var count int
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_rooms WHERE id=$1)+(SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1)`, room).Scan(&count); e != nil || count != 0 || deleted != 1 {
					t.Fatal("old actual audit DATA or private admissions survived deletion", count, deleted, e)
				}
			case "unattested-expiry-data-only", "unattested-active-not-adopted":
				id, want := "synthetic-maint-expired-0001", "ended"
				if kind == "unattested-active-not-adopted" {
					id, want = "synthetic-maint-active-0001", "lobby"
				}
				if nativeMaintenanceStatus(t, f, id) != want {
					t.Fatal("legacy DATA housekeeping outcome wrong")
				}
				var private int
				if e := f.f.base.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_room_provenance WHERE room_id=$1)+(SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1)`, id).Scan(&private); e != nil || private != 0 {
					t.Fatal("maintenance attested legacy DATA", e)
				}
				if result, e := a.ReadNativeRoom(f.request(t, "GET", false, t.Context()), "target", id); e == nil || result != nil {
					t.Fatal("legacy housekeeping produced native permission")
				}
			}
		})
	}
}

func TestNativeRoomMaintenanceLifetime(t *testing.T) {
	for _, kind := range []string{"actual-periodic-owner-once", "immediate-original-pass", "caller-runtime-cancel-sql", "actual-physical-cleanup-joins", "close-during-held-transaction", "no-add-after-close", "repeated-concurrent-close", "source-retired-after-room-end"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "source-retired-after-room-end" {
				f := newPartyPlaybackDS(t, kind, nil, nil, t.Context())
				owner := f.owner(t)
				lease, e := owner.Handoff(f.raw)
				if e != nil {
					t.Fatal(e)
				}
				defer lease.Close()
				f.f.room.base.exec(t, `UPDATE watch_room_members SET last_seen_at=$2 WHERE room_id=$1 AND profile_id='target'`, f.f.room.room.ID, time.Now().UTC().Add(-3*time.Minute))
				if _, _, ended, _, e := f.f.room.authority.maintenanceBatch(""); e != nil || ended != 1 {
					t.Fatal("actual original room not ended by owned maintenance", e)
				}
				if owner.Check(t.Context()) == nil || owner.Context().Err() == nil {
					t.Fatal("terminal maintenance DATA did not retire original source owner")
				}
				return
			}
			life, cancelLife := context.WithCancel(t.Context())
			defer cancelLife()
			var gateMu sync.Mutex
			var gates []*queryLifecycleGatedConn
			var dial func(context.Context, string, string) (net.Conn, error)
			if kind == "actual-physical-cleanup-joins" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					conn, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					gate := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
					gateMu.Lock()
					gates = append(gates, gate)
					gateMu.Unlock()
					return gate, nil
				}
			}
			var watchCommits atomic.Bool
			var commits atomic.Int32
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, q string, data pgx.TraceQueryEndData) {
				if data.Err == nil && watchCommits.Load() && strings.EqualFold(strings.TrimSpace(q), "COMMIT") {
					commits.Add(1)
				}
			}}
			f := newNativeRoomFixture(t, kind, trace, dial, life)
			a := f.f.authority
			if kind == "no-add-after-close" {
				a.Close()
				if a.StartMaintenance() == nil {
					t.Fatal("closed runtime launched maintenance")
				}
				if _, _, _, _, e := a.maintenanceBatch(""); e == nil {
					t.Fatal("closed runtime acquired maintenance SQL")
				}
				a.mu.Lock()
				active := a.active
				started := a.maintenanceStarted
				a.mu.Unlock()
				if active != 0 || started {
					t.Fatal("actual maintenance registered work after Close")
				}
				return
			}
			if kind == "actual-periodic-owner-once" || kind == "immediate-original-pass" {
				nativeMaintenanceSeed(t, f, "synthetic-owned-immediate-", 1, true)
				watchCommits.Store(true)
				if e := a.StartMaintenance(); e != nil {
					t.Fatal(e)
				}
				a.mu.Lock()
				original := a.maintenanceDone
				a.mu.Unlock()
				if e := a.StartMaintenance(); e != nil {
					t.Fatal(e)
				}
				a.mu.Lock()
				same := a.maintenanceDone == original
				a.mu.Unlock()
				if !same {
					t.Fatal("second Start replaced original worker")
				}
				nativeMaintenancePoll(t, "actual immediate cleanup", func() bool {
					return nativeMaintenanceStatus(t, f, "synthetic-owned-immediate-0001") == "ended" && commits.Load() >= 1
				})
				a.Close()
				if commits.Load() != 1 {
					t.Fatal("repeated Start performed duplicate actual immediate transaction", commits.Load())
				}
				bridgeSessionAwait(t, original)
				return
			}
			if kind == "repeated-concurrent-close" {
				if e := a.StartMaintenance(); e != nil {
					t.Fatal(e)
				}
				done := make(chan struct{})
				var wg sync.WaitGroup
				t.Cleanup(func() { cancelLife(); bridgeSessionAwait(t, done) })
				for i := 0; i < 8; i++ {
					wg.Add(1)
					go func() { defer wg.Done(); a.Close() }()
				}
				go func() { wg.Wait(); close(done) }()
				bridgeSessionAwait(t, done)
				a.mu.Lock()
				worker, active := a.maintenanceDone, a.active
				a.mu.Unlock()
				bridgeSessionAwait(t, worker)
				if active != 0 {
					t.Fatal("concurrent Close left actual active SQL", active)
				}
				return
			}
			if kind == "actual-physical-cleanup-joins" {
				conn, e := f.f.base.ds.pool.Acquire(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				physical := conn.Conn().PgConn()
				gate, ok := physical.Conn().(*queryLifecycleGatedConn)
				conn.Release()
				if !ok {
					t.Fatal("actual owned maintenance transport absent")
				}
				done, closed := make(chan struct{}), make(chan struct{})
				var batchErr error
				closeStarted := false
				t.Cleanup(func() {
					cancelLife()
					gateMu.Lock()
					all := append([]*queryLifecycleGatedConn(nil), gates...)
					gateMu.Unlock()
					for _, gate := range all {
						gate.release()
					}
					bridgeSessionAwait(t, done)
					if closeStarted {
						bridgeSessionAwait(t, closed)
					}
				})
				gate.armed.Store(true)
				go func() { defer close(done); _, _, _, _, batchErr = a.maintenanceBatch("") }()
				bridgeSessionAwait(t, gate.closeEntered)
				closeStarted = true
				go func() { defer close(closed); a.Close() }()
				select {
				case <-done:
					t.Fatal("maintenance refunded before actual broken transport end")
				default:
				}
				select {
				case <-closed:
					t.Fatal("runtime returned before real maintenance cleanup")
				default:
				}
				select {
				case <-physical.CleanupDone():
					t.Fatal("maintenance physical cleanup falsely acknowledged")
				default:
				}
				gate.release()
				bridgeSessionAwait(t, physical.CleanupDone())
				bridgeSessionAwait(t, done)
				bridgeSessionAwait(t, closed)
				if batchErr == nil {
					t.Fatal("broken actual maintenance query certified counters")
				}
				return
			}
			// Both cancellation cases hold an actual room row, not a callback flag.
			holder, e := f.f.base.admin.Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			done := make(chan struct{})
			var closed chan struct{}
			var batchErr error
			var ended, deleted int64
			started := false
			t.Cleanup(func() {
				cancelLife()
				recordingStateRollback(holder)
				if started {
					bridgeSessionAwait(t, done)
				}
				if closed != nil {
					bridgeSessionAwait(t, closed)
				}
			})
			if _, e = holder.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.f.base.schema, "watch_rooms"}.Sanitize()+` WHERE id=$1 FOR UPDATE`, f.f.room.ID); e != nil {
				t.Fatal(e)
			}
			started = true
			go func() { defer close(done); _, _, ended, deleted, batchErr = a.maintenanceBatch("") }()
			nativeSQLWait(t, f, "watch_rooms", holder.Conn().PgConn().PID())
			if kind == "close-during-held-transaction" {
				closed = make(chan struct{})
				go func() { defer close(closed); a.Close() }()
			} else {
				cancelLife()
			}
			bridgeSessionAwait(t, done)
			if closed != nil {
				bridgeSessionAwait(t, closed)
			}
			if batchErr == nil || ended != 0 || deleted != 0 {
				t.Fatal("cancelled owned maintenance claimed committed counters", batchErr, ended, deleted)
			}
		})
	}
}
