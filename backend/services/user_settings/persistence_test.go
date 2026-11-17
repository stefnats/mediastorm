package user_settings

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

// Each fixture owns its schema and NOLOGIN role. The existing production
// constructor runs real schema/data migrations, never on the public schema.
type settingsPGFixture struct {
	ds          *datastore.DataStore
	admin       *pgxpool.Pool
	uri, schema string
}

func newSettingsPG(t *testing.T) *settingsPGFixture {
	t.Helper()
	raw := os.Getenv("USERS_PERSISTENCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix socket fixture required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_us_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatalf("own role: %v", err)
	}
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("own schema")
	}
	q := u.Query()
	q.Set("search_path", name+",pg_catalog")
	q.Set("role", name)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	f := &settingsPGFixture{admin: admin, uri: u.String(), schema: name}
	t.Cleanup(func() {
		if f.ds != nil {
			f.ds.Close()
		}
		c, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, e := admin.Exec(c, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); e != nil {
			t.Error("drop own schema")
		}
		if _, e := admin.Exec(c, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Error("drop own role")
		}
		admin.Close()
	})
	f.ds = f.open(t)
	f.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master) VALUES('master','Synthetic Master','synthetic unused hash',true),('default','Synthetic Default','synthetic unused hash',false),('other','Synthetic Other','synthetic unused hash',false)`)
	return f
}
func (f *settingsPGFixture) open(t *testing.T) *datastore.DataStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, err := datastore.New(ctx, f.uri, datastore.WithMaxConns(4), datastore.WithMinConns(1))
	if err != nil {
		t.Fatalf("own-schema datastore: %v", err)
	}
	var current, search string
	if err = ds.Pool().QueryRow(ctx, `SELECT current_user,current_schema()`).Scan(&current, &search); err != nil || current != f.schema || search != f.schema {
		ds.Close()
		t.Fatal("fixture scope not exact")
	}
	return ds
}
func (f *settingsPGFixture) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, e := f.ds.Pool().Exec(t.Context(), q, args...); e != nil {
		t.Fatalf("synthetic statement: %v", e)
	}
}
func (f *settingsPGFixture) service(t *testing.T, ds *datastore.DataStore) *Service {
	t.Helper()
	s, e := NewServiceWithStore(ds)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func (f *settingsPGFixture) user(t *testing.T, id string) {
	t.Helper()
	f.exec(t, `INSERT INTO users(id,name,account_id) VALUES($1,$1,'default')`, id)
}
func settingsText(t *testing.T, f *settingsPGFixture, id, path string) string {
	t.Helper()
	var value string
	if e := f.ds.Pool().QueryRow(t.Context(), `SELECT COALESCE(settings #>> string_to_array($2,'.'),'') FROM user_settings WHERE user_id=$1`, id, path).Scan(&value); e != nil {
		t.Fatal("read own settings field")
	}
	return value
}

// A test-only literal-loopback to Unix socket proxy drops exactly one actual COMMIT response AFTER
// PostgreSQL has sent its command result. A separate direct pool verifies the
// durable write. No production transaction hook or mocked commit is used.
type settingsCommitProxy struct {
	listener    net.Listener
	drop        atomic.Bool
	dropped     atomic.Bool
	wg          sync.WaitGroup
	mu          sync.Mutex
	connections []net.Conn
	upstream    string
}

func settingsLostCommitProxy(t *testing.T, f *settingsPGFixture) (string, *settingsCommitProxy) {
	t.Helper()
	u, _ := url.Parse(f.uri)
	q := u.Query()
	port := q.Get("port")
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("own literal-loopback proxy")
	}
	p := &settingsCommitProxy{listener: listener, upstream: filepath.Join(q.Get("host"), ".s.PGSQL."+port)}
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
func (p *settingsCommitProxy) pump(client, server net.Conn) {
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

func TestUserSettingsCASOnlyChangedTarget(t *testing.T) {
	f := newSettingsPG(t)
	f.user(t, "a")
	a := f.service(t, f.ds)
	f.user(t, "b")
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('b','{"future":{"precise":9007199254740993}}')`)
	value := models.UserSettings{}
	value.Display.SimpleMode = models.BoolPtr(false)
	if err := a.Update("a", value); err != nil {
		t.Fatal(err)
	}
	if settingsText(t, f, "b", "future.precise") != "9007199254740993" {
		t.Fatal("unrelated external row was lost")
	}
}

func TestUserSettingsCASTwoServicesExplicitResync(t *testing.T) {
	f := newSettingsPG(t)
	f.user(t, "a")
	a := f.service(t, f.ds)
	b := f.service(t, f.ds)
	value := models.UserSettings{}
	value.Display.SimpleMode = models.BoolPtr(false)
	if e := b.Update("a", value); e != nil {
		t.Fatal(e)
	}
	desired := models.UserSettings{}
	desired.Display.SimpleMode = models.BoolPtr(true)
	if e := a.Update("a", desired); !errors.Is(e, datastore.ErrUserSettingsConflict) {
		t.Fatal("stale target was rebased", e)
	}
	if got, e := a.Get("a"); e != nil || got != nil {
		t.Fatal("conflict adopted a replacement projection")
	}
	if settingsText(t, f, "a", "display.simpleMode") != "false" {
		t.Fatal("stale update replaced newer false")
	}
	if e := a.ResyncUserSettings(t.Context()); e != nil {
		t.Fatal(e)
	}
	if e := a.Update("a", desired); e != nil {
		t.Fatal(e)
	}
	if settingsText(t, f, "a", "display.simpleMode") != "true" {
		t.Fatal("explicitly reloaded mutation did not persist")
	}
}

func TestUserSettingsCASDeepCopies(t *testing.T) {
	f := newSettingsPG(t)
	f.user(t, "a")
	s := f.service(t, f.ds)
	value := models.UserSettings{}
	value.Display.SimpleMode = models.BoolPtr(false)
	value.Display.NavigationTabVisibility = []string{"home"}
	value.Calendar.MDBListShelves = map[string]bool{"one": false}
	if err := s.Update("a", value); err != nil {
		t.Fatal(err)
	}
	*value.Display.SimpleMode = true
	value.Display.NavigationTabVisibility[0] = "external"
	value.Calendar.MDBListShelves["one"] = true
	got, err := s.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if *got.Display.SimpleMode || got.Display.NavigationTabVisibility[0] != "home" || got.Calendar.MDBListShelves["one"] {
		t.Fatal("input references entered confirmed state")
	}
	*got.Display.SimpleMode = true
	got.Display.NavigationTabVisibility[0] = "returned"
	got.Calendar.MDBListShelves["one"] = true
	merged, err := s.GetWithDefaults("a", models.UserSettings{})
	if err != nil {
		t.Fatal(err)
	}
	if *merged.Display.SimpleMode || merged.Display.NavigationTabVisibility[0] != "home" || merged.Calendar.MDBListShelves["one"] {
		t.Fatal("getter reference changed canonical state")
	}
	*merged.Display.SimpleMode = true
	again, _ := s.Get("a")
	if *again.Display.SimpleMode {
		t.Fatal("merged reference escaped")
	}
}

func TestUserSettingsCASBatchFailureRestoresDeepProjection(t *testing.T) {
	f := newSettingsPG(t)
	for _, id := range []string{"a", "b"} {
		f.user(t, id)
	}
	s := f.service(t, f.ds)
	for _, id := range []string{"a", "b"} {
		value := models.UserSettings{}
		value.Display.SimpleMode = models.BoolPtr(false)
		if e := s.Update(id, value); e != nil {
			t.Fatal(e)
		}
	}
	f.exec(t, `CREATE FUNCTION reject_b_settings() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.user_id='b' THEN RAISE EXCEPTION 'synthetic settings rejection'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_b BEFORE UPDATE ON user_settings FOR EACH ROW EXECUTE FUNCTION reject_b_settings()`)
	s.mu.Lock()
	for _, id := range []string{"a", "b"} {
		v := s.settings[id]
		v.Display.SimpleMode = models.BoolPtr(true)
		s.settings[id] = v
	}
	e := s.saveLocked()
	s.mu.Unlock()
	if e == nil {
		t.Fatal("failing second target reported success")
	}
	for _, id := range []string{"a", "b"} {
		got, e := s.Get(id)
		if e != nil || got == nil || *got.Display.SimpleMode {
			t.Fatal("failed batch published staged target")
		}
		if settingsText(t, f, id, "display.simpleMode") != "false" {
			t.Fatal("partial settings batch persisted")
		}
	}
}

func TestUserSettingsCASLostCommitQuarantineAndExplicitResync(t *testing.T) {
	f := newSettingsPG(t)
	f.user(t, "a")
	f.user(t, "b")
	uri, proxy := settingsLostCommitProxy(t, f)
	ds, e := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
	if e != nil {
		t.Fatal("open own proxy datastore")
	}
	t.Cleanup(ds.Close)
	s := f.service(t, ds)
	proxy.drop.Store(true)
	value := models.UserSettings{}
	value.Display.SimpleMode = models.BoolPtr(true)
	if e = s.Update("a", value); !errors.Is(e, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("lost acknowledgement was not unknown", e)
	}
	if !proxy.dropped.Load() || settingsText(t, f, "a", "display.simpleMode") != "true" {
		t.Fatal("real committed write was not observed independently")
	}
	if _, e = s.Get("a"); !errors.Is(e, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("unknown target remained readable as confirmed")
	}
	if e = s.Update("a", models.UserSettings{}); !errors.Is(e, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("unknown target silently rebased")
	}
	if e = s.Update("b", value); e != nil {
		t.Fatal("unrelated confirmed target was quarantined", e)
	}
	if e = s.ResyncUserSettings(t.Context()); e != nil {
		t.Fatal(e)
	}
	value.Display.SimpleMode = models.BoolPtr(false)
	if e = s.Update("a", value); e != nil {
		t.Fatal(e)
	}
	if settingsText(t, f, "a", "display.simpleMode") != "false" {
		t.Fatal("explicit reconciliation failed")
	}
}
