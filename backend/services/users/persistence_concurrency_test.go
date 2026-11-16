package users

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
)

// A test-only literal-loopback to Unix socket proxy drops exactly one actual COMMIT response AFTER
// PostgreSQL has sent its command result. A separate direct pool verifies the
// durable write. No production transaction hook or mocked commit is used.
type usersCommitProxy struct {
	listener    net.Listener
	drop        atomic.Bool
	dropped     atomic.Bool
	wg          sync.WaitGroup
	mu          sync.Mutex
	connections []net.Conn
	upstream    string
}

func usersLostCommitProxy(t *testing.T, f *usersPGFixture) (string, *usersCommitProxy) {
	t.Helper()
	u, _ := url.Parse(f.uri)
	q := u.Query()
	port := q.Get("port")
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("own literal-loopback proxy")
	}
	p := &usersCommitProxy{listener: listener, upstream: filepath.Join(q.Get("host"), ".s.PGSQL."+port)}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, e := listener.Accept()
			if e != nil {
				return
			}
			server, e := net.Dial("unix", p.upstream)
			if e != nil {
				_ = client.Close()
				continue
			}
			p.mu.Lock()
			p.connections = append(p.connections, client, server)
			p.mu.Unlock()
			p.wg.Add(1)
			go p.pump(client, server)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		p.mu.Lock()
		for _, c := range p.connections {
			_ = c.Close()
		}
		p.mu.Unlock()
		p.wg.Wait()
	})
	q.Del("host")
	q.Del("port")
	u.Host = listener.Addr().String()
	u.RawQuery = q.Encode()
	return u.String(), p
}
func (p *usersCommitProxy) pump(client, server net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	defer server.Close()
	var commit atomic.Bool
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		var header [4]byte
		if _, e := io.ReadFull(client, header[:]); e != nil {
			return
		}
		n := binary.BigEndian.Uint32(header[:])
		if n < 8 || n > 1<<20 {
			return
		}
		body := make([]byte, n-4)
		if _, e := io.ReadFull(client, body); e != nil {
			return
		}
		if _, e := server.Write(append(header[:], body...)); e != nil {
			return
		}
		for {
			var h [5]byte
			if _, e := io.ReadFull(client, h[:]); e != nil {
				return
			}
			n = binary.BigEndian.Uint32(h[1:])
			if n < 4 || n > 64<<20 {
				return
			}
			body = make([]byte, n-4)
			if _, e := io.ReadFull(client, body); e != nil {
				return
			}
			if h[0] == 'Q' && strings.EqualFold(strings.TrimSuffix(string(body), "\x00"), "commit") && p.drop.CompareAndSwap(true, false) {
				commit.Store(true)
			}
			if _, e := server.Write(append(h[:], body...)); e != nil {
				return
			}
		}
	}()
	for {
		var h [5]byte
		if _, e := io.ReadFull(server, h[:]); e != nil {
			break
		}
		n := binary.BigEndian.Uint32(h[1:])
		if n < 4 || n > 64<<20 {
			break
		}
		body := make([]byte, n-4)
		if _, e := io.ReadFull(server, body); e != nil {
			break
		}
		if commit.Load() && h[0] == 'C' {
			p.dropped.Store(true)
			break
		}
		if _, e := client.Write(append(h[:], body...)); e != nil {
			break
		}
	}
	_ = client.Close()
	_ = server.Close()
	<-done
}
func TestUsersPersistenceLostCommitAckQuarantinesEvenEmptySave(t *testing.T) {
	f := usersPG(t)
	original := f.service(t, f.ds)
	id := original.ListAll()[0].ID
	uri, proxy := usersLostCommitProxy(t, f)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, e := datastore.New(ctx, uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if e != nil {
		t.Fatal("proxy datastore")
	}
	defer ds.Close()
	s := f.service(t, ds)
	before, _ := s.Get(id)
	proxy.drop.Store(true)
	if _, e = s.Rename(id, "Committed without acknowledgement"); !errors.Is(e, ErrPersistenceUncertain) || !proxy.dropped.Load() {
		t.Fatalf("unknown commit not quarantined: %v", e)
	}
	if dbUser(t, f, id).Name != "Committed without acknowledgement" {
		t.Fatal("lost ack control did not really commit")
	}
	after, _ := s.Get(id)
	if after.Name != before.Name {
		t.Fatal("unacknowledged cache published")
	}
	v := usersVersion(t, f, id)
	s.mu.Lock()
	e = s.saveLocked()
	s.mu.Unlock()
	if !errors.Is(e, ErrPersistenceUncertain) {
		t.Fatal("empty save escaped quarantine")
	}
	if _, e = s.Rename(id, "Blind retry"); !errors.Is(e, ErrPersistenceUncertain) {
		t.Fatal("unknown writer replayed")
	}
	if usersVersion(t, f, id) != v {
		t.Fatal("quarantine wrote database")
	}
	cancelled, stop := context.WithCancel(t.Context())
	stop()
	if e = s.ResyncStorage(cancelled); e == nil {
		t.Fatal("cancelled resync accepted")
	}
	s.mu.Lock()
	uncertain := s.persistence.uncertain
	s.mu.Unlock()
	if !uncertain {
		t.Fatal("failed resync cleared quarantine")
	}
	if e = s.ResyncStorage(t.Context()); e != nil {
		t.Fatal(e)
	}
	after, _ = s.Get(id)
	if after.Name != "Committed without acknowledgement" {
		t.Fatal("resync did not read actual outcome")
	}
	if _, e = s.Rename(id, "Explicit new mutation"); e != nil {
		t.Fatal(e)
	}
}
func userRecords(t *testing.T, f *usersPGFixture) map[string]datastore.UserWriteRecord {
	t.Helper()
	rows, e := f.ds.LoadUserWriteSnapshot(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	m := map[string]datastore.UserWriteRecord{}
	for _, r := range rows {
		m[r.User.ID] = r
	}
	return m
}
func TestUsersPersistenceExactBatchInsertAndDeleteRevisions(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	a := mustProfile(t, s, "A")
	b := mustProfile(t, s, "B")
	records := userRecords(t, f)
	for _, mode := range []string{"duplicate", "zero revision", "foreign store", "insert existing", "unknown kind", "invalid time", "delete recreated"} {
		t.Run(mode, func(t *testing.T) {
			r := records[a.ID]
			u := r.User
			u.Name = "must not persist"
			change := datastore.UserChange{Kind: datastore.UserReplace, ID: a.ID, User: u, Expected: r.Revision}
			ds := f.ds
			switch mode {
			case "duplicate":
			case "zero revision":
				change.Expected = datastore.UserRevision{}
			case "foreign store":
				ds = f.open(t)
				defer ds.Close()
			case "insert existing":
				change.Kind = datastore.UserInsert
				change.Expected = datastore.UserRevision{}
			case "unknown kind":
				change.Kind = 99
			case "invalid time":
				change.User.UpdatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "delete recreated":
				r = records[b.ID]
				f.exec(t, `DELETE FROM users WHERE id=$1`, b.ID)
				if e := f.ds.Users().Create(t.Context(), &r.User); e != nil {
					t.Fatal(e)
				}
				change = datastore.UserChange{Kind: datastore.UserDelete, ID: b.ID, Expected: r.Revision}
			}
			changes := []datastore.UserChange{change}
			if mode == "duplicate" {
				changes = append(changes, change)
			}
			if _, e := ds.CommitUserChanges(t.Context(), changes); e == nil {
				t.Fatal("invalid batch accepted")
			}
			if dbUser(t, f, a.ID).Name != a.Name {
				t.Fatal("failed batch mutated target")
			}
			if dbUser(t, f, b.ID) == nil {
				t.Fatal("recreated profile deleted by old revision")
			}
		})
	}
	// Insert is explicit and ordered with a known deletion in one transaction.
	r := records[a.ID]
	now := time.Now().UTC()
	u := models.User{ID: "explicit-insert", AccountID: models.DefaultAccountID, Name: "New", CreatedAt: now, UpdatedAt: now}
	result, e := f.ds.CommitUserChanges(t.Context(), []datastore.UserChange{{Kind: datastore.UserDelete, ID: a.ID, Expected: r.Revision}, {Kind: datastore.UserInsert, ID: u.ID, User: u}})
	if e != nil || len(result.Records) != 1 || len(result.Deleted) != 1 || dbUser(t, f, a.ID) != nil || dbUser(t, f, u.ID) == nil {
		t.Fatalf("exact insert/delete batch: %v", e)
	}
}
func waitUsersLock(t *testing.T, f *usersPGFixture, queryPrefix string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		e := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2`, f.schema, queryPrefix+"%").Scan(&n)
		if e != nil {
			t.Fatal("own lock telemetry")
		}
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("actual own SQL did not block")
}
func TestUsersPersistenceRelationBeforeRowAndCancelledWait(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	a := mustProfile(t, s, "A")
	b := mustProfile(t, s, "B")
	records := userRecords(t, f)
	conn, e := f.ds.Pool().Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Release()
	tx, e := conn.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(t.Context(), `SELECT id FROM users WHERE id=$1 FOR UPDATE`, a.ID); e != nil {
		t.Fatal(e)
	}
	r := records[a.ID]
	r.User.Name = "Updated after wait"
	update := make(chan error, 1)
	go func() {
		_, e := f.ds.CommitUserChanges(t.Context(), []datastore.UserChange{{Kind: datastore.UserReplace, ID: a.ID, User: r.User, Expected: r.Revision}})
		update <- e
	}()
	waitUsersLock(t, f, "SELECT concat")
	// Topology now waits on the first writer's pre-acquired relation lock, rather
	// than taking relation then waiting for its row while update upgrades later.
	deleteResult := make(chan error, 1)
	go func() {
		_, e := f.ds.CommitUserChanges(t.Context(), []datastore.UserChange{{Kind: datastore.UserDelete, ID: b.ID, Expected: records[b.ID].Revision}})
		deleteResult <- e
	}()
	waitUsersLock(t, f, "LOCK TABLE users IN SHARE ROW EXCLUSIVE")
	if e = tx.Commit(t.Context()); e != nil {
		t.Fatal(e)
	}
	for _, ch := range []chan error{update, deleteResult} {
		select {
		case e := <-ch:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("relation/tuple lock cycle")
		}
	}
	// A cancelled relation-lock wait must not publish/write anything.
	tx, e = conn.Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(t.Context(), `LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE`); e != nil {
		t.Fatal(e)
	}
	records = userRecords(t, f)
	r = records[a.ID]
	r.User.Name = "Cancelled update"
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, e := f.ds.CommitUserChanges(ctx, []datastore.UserChange{{Kind: datastore.UserReplace, ID: a.ID, User: r.User, Expected: r.Revision}})
		done <- e
	}()
	waitUsersLock(t, f, "LOCK TABLE users IN ROW EXCLUSIVE")
	cancel()
	select {
	case e := <-done:
		if e == nil || errors.Is(e, datastore.ErrUserWriteOutcomeUnknown) {
			t.Fatal("pre-commit cancellation misclassified")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel did not release wait")
	}
	if e = tx.Rollback(t.Context()); e != nil {
		t.Fatal(e)
	}
	if dbUser(t, f, a.ID).Name != "Updated after wait" {
		t.Fatal("cancelled update wrote")
	}
}

func TestUsersPersistenceConcurrentServicesDisjointAndSameTarget(t *testing.T) {
	f := usersPG(t)
	a := f.service(t, f.ds)
	first := mustProfile(t, a, "First")
	second := mustProfile(t, a, "Second")
	ds2 := f.open(t)
	defer ds2.Close()
	b := f.service(t, ds2)
	start := make(chan struct{})
	results := make(chan error, 2)
	go func() { <-start; _, e := a.Rename(first.ID, "First changed"); results <- e }()
	go func() { <-start; _, e := b.Rename(second.ID, "Second changed"); results <- e }()
	close(start)
	for n := 0; n < 2; n++ {
		if e := <-results; e != nil {
			t.Fatal(e)
		}
	}
	if dbUser(t, f, first.ID).Name != "First changed" || dbUser(t, f, second.ID).Name != "Second changed" {
		t.Fatal("disjoint service writers overwrote each other")
	}
	if e := a.ResyncStorage(t.Context()); e != nil {
		t.Fatal(e)
	}
	if e := b.ResyncStorage(t.Context()); e != nil {
		t.Fatal(e)
	}
	start = make(chan struct{})
	go func() { <-start; _, e := a.Rename(first.ID, "Winner A"); results <- e }()
	go func() { <-start; _, e := b.Rename(first.ID, "Winner B"); results <- e }()
	close(start)
	success, conflict := 0, 0
	for n := 0; n < 2; n++ {
		e := <-results
		if e == nil {
			success++
		} else if errors.Is(e, datastore.ErrUserWriteConflict) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("same-target stale writer silently merged")
	}
}
