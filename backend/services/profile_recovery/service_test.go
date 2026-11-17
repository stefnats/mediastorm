package profile_recovery

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/user_settings"
	"novastream/services/users"
)

// Each fixture owns its schema and NOLOGIN role. The existing production
// constructor runs real schema/data migrations, never on the public schema.
type recoveryPGFixture struct {
	ds          *datastore.DataStore
	admin       *pgxpool.Pool
	uri, schema string
}

func newRecoveryPG(t *testing.T) *recoveryPGFixture {
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
	name := fmt.Sprintf("synthetic_rui_%d", time.Now().UnixNano())
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
	f := &recoveryPGFixture{admin: admin, uri: u.String(), schema: name}
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
func (f *recoveryPGFixture) open(t *testing.T) *datastore.DataStore {
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
func (f *recoveryPGFixture) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, e := f.ds.Pool().Exec(t.Context(), q, args...); e != nil {
		t.Fatalf("synthetic statement: %v", e)
	}
}

type coordinatorFixture struct {
	pg       *recoveryPGFixture
	users    *users.Service
	settings *user_settings.Service
	pins     *profileunlock.PGProfilePins
	session  models.Session
	target   string
	service  *Service
}

func recoveryCoordinator(t *testing.T) *coordinatorFixture {
	t.Helper()
	pg := newRecoveryPG(t)
	profiles, e := users.NewServiceWithStore(pg.ds, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	target, e := profiles.CreateForAccount("default", "Synthetic target")
	if e != nil {
		t.Fatal(e)
	}
	settings, e := user_settings.NewServiceWithStore(pg.ds)
	if e != nil {
		t.Fatal(e)
	}
	ss, e := sessions.NewServiceWithStore(pg.ds, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	session, e := ss.Create("default", false, "synthetic-recovery", "127.0.0.1")
	if e != nil {
		t.Fatal(e)
	}
	pins := profileunlock.NewPGProfilePins(pg.ds, ss)
	return &coordinatorFixture{pg: pg, users: profiles, settings: settings, pins: pins, session: session, target: target.ID, service: NewService(pg.ds, profiles, settings, pins)}
}
func (f *coordinatorFixture) request(t *testing.T, ctx context.Context) *http.Request {
	t.Helper()
	r := httptest.NewRequest("POST", "/api/users/"+f.target+"/state/reload", nil).WithContext(ctx)
	checked, release, e := f.pins.Authenticate(r, f.session.Token, "synthetic-recovery-client")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	return checked
}

const coordinatorRawSQL = `SELECT jsonb_build_object('users',(SELECT jsonb_agg(to_jsonb(u)||jsonb_build_object('physical',concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY u.id) FROM users u),'settings',(SELECT jsonb_agg(to_jsonb(s)||jsonb_build_object('physical',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY s.user_id) FROM user_settings s),'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY v.user_id) FROM user_settings_versions v))::text`

func (f *coordinatorFixture) raw(t *testing.T) string {
	t.Helper()
	var raw string
	if e := f.pg.ds.Pool().QueryRow(t.Context(), coordinatorRawSQL).Scan(&raw); e != nil {
		t.Fatal("read own recovery state")
	}
	return raw
}

func TestProfileRecoveryCoordinatorActualSharedReadNoWrite(t *testing.T) {
	f := recoveryCoordinator(t)
	f.pg.exec(t, `UPDATE users SET name='Saved externally',kids_mode='rating',kids_max_movie_rating='PG' WHERE id=$1`, f.target)
	f.pg.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES($1,'{"future":{"n":9007199254740993},"display":{"simpleMode":false}}')`, f.target)
	r := f.request(t, t.Context())
	before := f.raw(t)
	result, e := f.service.RecoverAPI(r, f.target, func() config.SecuritySettings { return config.SecuritySettings{} })
	if e != nil || result.Profile.Name != "Saved externally" || result.Profile.KidsMaxMovieRating != "PG" || result.Profile.KidsAllowedLists == nil {
		t.Fatal("explicit saved snapshot missing", e)
	}
	value, ok := f.users.Get(f.target)
	settings, e := f.settings.Get(f.target)
	if !ok || value.Name != result.Profile.Name || e != nil || settings == nil || settings.Display.SimpleMode == nil || *settings.Display.SimpleMode {
		t.Fatal("shared adoption did not update both actual caches")
	}
	if before != f.raw(t) {
		t.Fatal("read-only recovery changed physical user/settings state")
	}
}
func TestProfileRecoveryCoordinatorActualAPIPINAndManagementBoundary(t *testing.T) {
	f := recoveryCoordinator(t)
	if _, e := f.users.SetPin(f.target, "1357"); e != nil {
		t.Fatal(e)
	}
	r := f.request(t, t.Context())
	if _, e := f.service.RecoverAPI(r, f.target, func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: true} }); !errors.Is(e, ErrLocked) {
		t.Fatal("API PIN enforcement bypassed", e)
	}
	if _, e := f.service.RecoverManagement(r, f.target); e != nil {
		t.Fatal("existing owned locked management boundary changed", e)
	}
	if _, e := f.service.RecoverAPI(r, f.target, func() config.SecuritySettings { return config.SecuritySettings{} }); e != nil {
		t.Fatal("existing enforcement-off changed", e)
	}
}
func TestProfileRecoveryCoordinatorRejectsOriginalChangesAndWrongStore(t *testing.T) {
	for _, kind := range []string{"session", "actor", "foreign", "missing", "wrong-store"} {
		t.Run(kind, func(t *testing.T) {
			f := recoveryCoordinator(t)
			r := f.request(t, t.Context())
			beforeName, _ := f.users.Get(f.target)
			switch kind {
			case "session":
				f.pg.exec(t, `UPDATE sessions SET client_id='changed' WHERE token=$1`, f.session.Token)
			case "actor":
				f.pg.exec(t, `UPDATE accounts SET is_master=true WHERE id='default'`)
			case "foreign":
				f.pg.exec(t, `UPDATE users SET account_id='other' WHERE id=$1`, f.target)
			case "missing":
				f.pg.exec(t, `DELETE FROM users WHERE id=$1`, f.target)
			case "wrong-store":
				other := f.pg.open(t)
				t.Cleanup(other.Close)
				f.service = NewService(other, f.users, f.settings, f.pins)
			}
			before := f.raw(t)
			if _, e := f.service.RecoverManagement(r, f.target); e == nil {
				t.Fatal("changed original frame or owner was accepted")
			}
			after, _ := f.users.Get(f.target)
			if after.Name != beforeName.Name || before != f.raw(t) {
				t.Fatal("denied recovery adopted or wrote data")
			}
		})
	}
}
func TestProfileRecoveryCoordinatorHeldLeaseCancellation(t *testing.T) {
	f := recoveryCoordinator(t)
	holdCtx := t.Context()
	hold, e := f.pg.ds.PrepareProfileRecovery(holdCtx, f.target)
	if e != nil {
		t.Fatal(e)
	}
	defer hold.Close()
	ticket, e := f.users.PrepareStorageRecovery(holdCtx, hold)
	if e != nil {
		t.Fatal(e)
	}
	defer ticket.Close()
	snapshot, e := f.pg.ds.ReadProfileRecovery(hold)
	if e != nil {
		t.Fatal(e)
	}
	lease, e := f.users.AcquireStorageRecoveryLease(holdCtx, ticket, snapshot)
	if e != nil {
		t.Fatal(e)
	}
	defer lease.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := f.request(t, ctx)
	started := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		var b [128]byte
		n := runtime.Stack(b[:], false)
		started <- strings.Fields(string(b[:n]))[1]
		_, e := f.service.RecoverManagement(r, f.target)
		done <- e
	}()
	worker := <-started
	if _, e = strconv.ParseUint(worker, 10, 64); e != nil {
		t.Fatal("invalid own worker identifier")
	}
	wait, cancelWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWait()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	observed := false
	for !observed {
		b := make([]byte, 1<<20)
		n := runtime.Stack(b, true)
		for _, stack := range strings.Split(string(b[:n]), "\n\n") {
			if strings.HasPrefix(stack, "goroutine "+worker+" ") && strings.Contains(stack, "PrepareStorageRecovery") && strings.Contains(stack, "Mutex") {
				observed = true
				break
			}
		}
		if !observed {
			select {
			case <-wait.Done():
				cancel()
				lease.Close()
				<-done
				t.Fatal("own recovery did not wait at actual held Users mutex")
			case <-ticker.C:
			}
		}
	}
	// Cancellation cannot actively interrupt a held sync.Mutex. Release it before
	// joining; the original canceled context must prevent further read/adoption.
	cancel()
	lease.Close()
	select {
	case e = <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal("canceled lock wait continued", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery did not end after unlock")
	}
}

func TestProfileRecoveryCoordinatorPairCompletesAfterLateCancel(t *testing.T) {
	for _, closeOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit-completion", true: "close-completion"}[closeOnly], func(t *testing.T) {
			f := recoveryCoordinator(t)
			f.pg.exec(t, `UPDATE users SET name='Paired saved value' WHERE id=$1`, f.target)
			f.pg.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES($1,'{"display":{"simpleMode":false}}')`, f.target)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			read, e := f.pg.ds.PrepareProfileRecovery(ctx, f.target)
			if e != nil {
				t.Fatal(e)
			}
			defer read.Close()
			ut, e := f.users.PrepareStorageRecovery(ctx, read)
			if e != nil {
				t.Fatal(e)
			}
			defer ut.Close()
			st, e := f.settings.PrepareUserSettingsRecovery(ctx, read)
			if e != nil {
				t.Fatal(e)
			}
			defer st.Close()
			snapshot, e := f.pg.ds.ReadProfileRecovery(read)
			if e != nil {
				t.Fatal(e)
			}
			ul, e := f.users.AcquireStorageRecoveryLease(ctx, ut, snapshot)
			if e != nil {
				t.Fatal(e)
			}
			defer ul.Close()
			sl, e := f.settings.AcquireUserSettingsRecoveryLease(ctx, st, snapshot)
			if e != nil {
				t.Fatal(e)
			}
			defer sl.Close()
			before := f.raw(t)
			if e = f.pg.ds.TakeProfileRecoveryPair(ctx, ul.PairLease(), sl.PairLease(), snapshot); e != nil {
				t.Fatal(e)
			}
			if !closeOnly {
				ul.Complete()
			}
			cancel()
			read.Close()
			if !closeOnly {
				sl.Complete()
			}
			// Exact production leases unwind in reverse order. Close also completes both
			// when cancellation occurs after PairTake but before either explicit ACK.
			sl.Close()
			ul.Close()
			user, ok := f.users.Get(f.target)
			settings, e := f.settings.Get(f.target)
			if !ok || user.Name != "Paired saved value" || e != nil || settings == nil || settings.Display.SimpleMode == nil || *settings.Display.SimpleMode {
				t.Fatal("late cancellation left half of the prepared bookkeeping")
			}
			if before != f.raw(t) {
				t.Fatal("cache completion wrote SQL state")
			}
		})
	}
}

// A test-only literal-loopback to Unix socket proxy drops exactly one actual COMMIT response AFTER
// PostgreSQL has sent its command result. A separate direct pool verifies the
// durable write. No production transaction hook or mocked commit is used.
type recoveryCommitProxy struct {
	listener    net.Listener
	drop        atomic.Bool
	dropped     atomic.Bool
	wg          sync.WaitGroup
	mu          sync.Mutex
	connections []net.Conn
	upstream    string
}

func recoveryLostCommitProxy(t *testing.T, f *recoveryPGFixture) (string, *recoveryCommitProxy) {
	t.Helper()
	u, _ := url.Parse(f.uri)
	q := u.Query()
	port := q.Get("port")
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("own literal-loopback proxy")
	}
	p := &recoveryCommitProxy{listener: listener, upstream: filepath.Join(q.Get("host"), ".s.PGSQL."+port)}
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
func (p *recoveryCommitProxy) pump(client, server net.Conn) {
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

func TestProfileRecoveryCoordinatorKeepsForeignUncertaintySeparate(t *testing.T) {
	for _, userUnknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "settings-only", true: "users-and-settings"}[userUnknown], func(t *testing.T) {
			f := recoveryCoordinator(t)
			b, e := f.users.CreateForAccount("default", "Foreign pending target")
			if e != nil {
				t.Fatal(e)
			}
			uri, proxy := recoveryLostCommitProxy(t, f.pg)
			store, e := datastore.New(t.Context(), uri, datastore.WithMaxConns(4), datastore.WithMinConns(1))
			if e != nil {
				t.Fatal("own proxy datastore")
			}
			t.Cleanup(store.Close)
			profiles, e := users.NewServiceWithStore(store, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			settings, e := user_settings.NewServiceWithStore(store)
			if e != nil {
				t.Fatal(e)
			}
			pins := profileunlock.NewPGProfilePins(store)
			f.users = profiles
			f.settings = settings
			f.pins = pins
			f.service = NewService(store, profiles, settings, pins)
			r := f.request(t, t.Context())
			if userUnknown {
				proxy.drop.Store(true)
				if _, e = profiles.Rename(b.ID, "Foreign accepted unknown"); !errors.Is(e, users.ErrPersistenceUncertain) || !proxy.dropped.Load() {
					t.Fatal("actual foreign user commit did not become uncertain", e)
				}
			}
			proxy.dropped.Store(false)
			proxy.drop.Store(true)
			if e = settings.Update(b.ID, models.UserSettings{Display: models.DisplaySettings{SimpleMode: models.BoolPtr(false)}}); !errors.Is(e, datastore.ErrUserSettingsOutcomeUnknown) || !proxy.dropped.Load() {
				t.Fatal("actual foreign settings commit did not become uncertain", e)
			}
			var stored string
			if e = f.pg.ds.Pool().QueryRow(t.Context(), `SELECT settings#>>'{display,simpleMode}' FROM user_settings WHERE user_id=$1`, b.ID).Scan(&stored); e != nil || stored != "false" {
				t.Fatal("independent foreign settings commit not observed")
			}
			before := f.raw(t)
			result, e := f.service.RecoverManagement(r, f.target)
			if e != nil || result.Recovery.UsersWriteBlocked != userUnknown || !result.Recovery.SettingsPendingUncertainty {
				t.Fatal("foreign uncertainty was cleared or globally ORed", e)
			}
			if _, e = settings.Get(b.ID); !errors.Is(e, datastore.ErrUserSettingsOutcomeUnknown) {
				t.Fatal("foreign settings marker cleared")
			}
			if before != f.raw(t) {
				t.Fatal("reconciliation changed database state")
			}
			_, e = profiles.Rename(f.target, "New explicit profile write")
			if userUnknown {
				if !errors.Is(e, users.ErrPersistenceUncertain) {
					t.Fatal("global Users stop was relaxed")
				}
			} else if e != nil {
				t.Fatal("foreign settings uncertainty became new global Users policy", e)
			}
		})
	}
}

func TestProfileRecoveryCoordinatorInvalidationDuringSharedRead(t *testing.T) {
	for _, kind := range []string{"session", "pin", "owner"} {
		t.Run(kind, func(t *testing.T) {
			f := recoveryCoordinator(t)
			beforeUser, _ := f.users.Get(f.target)
			f.pg.exec(t, `UPDATE users SET name='Would be adopted' WHERE id=$1`, f.target)
			f.pg.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES($1,'{"display":{"simpleMode":false}}')`, f.target)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := f.request(t, ctx)
			lock, e := f.pg.ds.Pool().Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			defer func() { _ = lock.Rollback(context.Background()) }()
			if _, e = lock.Exec(t.Context(), `LOCK TABLE user_settings IN ACCESS EXCLUSIVE MODE`); e != nil {
				t.Fatal(e)
			}
			result := make(chan error, 1)
			joined := make(chan struct{})
			go func() { defer close(joined); _, e := f.service.RecoverManagement(r, f.target); result <- e }()
			t.Cleanup(func() {
				cancel()
				_ = lock.Rollback(context.Background())
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("shared-read worker did not join")
				}
			})
			wait, stop := context.WithTimeout(t.Context(), 5*time.Second)
			defer stop()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			blocked := false
			for !blocked {
				if e = f.pg.admin.QueryRow(wait, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%LEFT JOIN user_settings s%')`, f.pg.schema).Scan(&blocked); e != nil {
					t.Fatal("observe actual own shared-read wait")
				}
				if !blocked {
					select {
					case <-wait.Done():
						t.Fatal("actual shared SELECT did not wait")
					case <-ticker.C:
					}
				}
			}
			if ctx.Err() != nil {
				t.Fatal("original request ended before invalidation")
			}
			switch kind {
			case "session":
				f.pg.exec(t, `UPDATE sessions SET client_id='invalidated-during-read' WHERE token=$1`, f.session.Token)
			case "pin":
				hash, e := bcrypt.GenerateFromPassword([]byte("1357"), bcrypt.MinCost)
				if e != nil {
					t.Fatal(e)
				}
				f.pg.exec(t, `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id=$2`, string(hash), f.target)
			case "owner":
				f.pg.exec(t, `UPDATE users SET account_id='other' WHERE id=$1`, f.target)
			}
			var before string
			if e = lock.QueryRow(t.Context(), coordinatorRawSQL).Scan(&before); e != nil {
				t.Fatal("read own state through the table-locking transaction")
			}
			if e = lock.Commit(t.Context()); e != nil {
				t.Fatal(e)
			}
			select {
			case e = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("released shared read did not end")
			}
			<-joined
			if e == nil || errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
				t.Fatal("current authority change was not the rejection boundary", e)
			}
			afterUser, _ := f.users.Get(f.target)
			afterSettings, e := f.settings.Get(f.target)
			if afterUser.Name != beforeUser.Name || e != nil || afterSettings != nil || before != f.raw(t) {
				t.Fatal("invalidated shared read adopted a cache or wrote data")
			}
		})
	}
}
