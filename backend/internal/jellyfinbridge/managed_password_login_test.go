//go:build linux

package jellyfinbridge

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/jellyfinbridge/dto"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
)

const managedPasswordTestSecret = "synthetic public bridge password"

type managedPasswordFixture struct {
	*bridgeLogoutFixture
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	adapter     *ManagedPasswordLogin
}

func managedPasswordPG(t *testing.T, oneSlot, proxied bool) (*managedPasswordFixture, *bridgeLogoutProxy) {
	t.Helper()
	base, proxy := bridgeLogoutPG(t, proxied)
	f := &managedPasswordFixture{bridgeLogoutFixture: base}
	if oneSlot {
		raw := f.ds.Pool().Config().ConnString()
		f.ds.Close()
		var err error
		f.ds, err = datastore.New(t.Context(), raw, datastore.WithMaxConns(1), datastore.WithMinConns(0))
		if err != nil {
			t.Fatal(err)
		}
		f.checked.Service, err = sessions.NewServiceWithStore(f.ds, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(managedPasswordTestSecret), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `DELETE FROM sessions;INSERT INTO user_settings_versions(user_id,generation)VALUES('target',0) ON CONFLICT(user_id) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET password_hash=$1 WHERE id='actor'`, string(hash)); err != nil {
		t.Fatal(err)
	}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = f.ds.Pool().Config().ConnString()
	settings.JellyfinBridge.Enabled = true
	settings.Security.EnforceProfilePins = false
	if err = f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	objects, err := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		_ = objects.Close()
	})
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.ds.Pool(), objects)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.adapter, err = NewManagedPasswordLogin(t.Context(), f.ds, f.manager, f.security, f.coordinator, profileunlock.NewPGProfilePins(f.ds))
	if err != nil {
		t.Fatal(err)
	}
	f.server.deps.ManagedPassword = f.adapter
	return f, proxy
}

func (f *managedPasswordFixture) callPassword(t *testing.T, ctx context.Context, name, password, address string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(authenticateByNameRequest{Username: name, Password: password})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.invalid/Users/AuthenticateByName", strings.NewReader(string(body)))
	r.RemoteAddr = address
	r.Header.Set("Authorization", `MediaBrowser Client="Synthetic%20TV", Version="2.0", Device="Living%20Room", DeviceId="synthetic-public-device"`)
	w := httptest.NewRecorder()
	f.server.Handler().ServeHTTP(w, r)
	return w
}

func (f *managedPasswordFixture) sessionCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+pgx.Identifier{f.schema, "sessions"}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestManagedPasswordPublicKnownACKWireAndPoolOne(t *testing.T) {
	for _, kind := range []string{"ordinary", "pool-one", "blank-account-name", "duplicate-blank-name"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := managedPasswordPG(t, kind == "pool-one", false)
			wantName := "Synthetic actor/Synthetic profile"
			if strings.Contains(kind, "blank") {
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET username='' WHERE id='actor'`); err != nil {
					t.Fatal(err)
				}
				wantName = "Synthetic profile"
			}
			if kind == "duplicate-blank-name" {
				if _, err := f.ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('other','Synthetic other','unused');INSERT INTO users(id,account_id,name)VALUES('other-profile','other','Synthetic profile')`); err != nil {
					t.Fatal(err)
				}
				wantName += " (" + deriveID(refUser, "target")[:4] + ")"
			}
			name := "Synthetic profile"
			if kind == "duplicate-blank-name" {
				name = wantName
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			w := f.callPassword(t, ctx, name, managedPasswordTestSecret, "198.51.100.8:1234")
			var response dto.AuthenticationResult
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.AccessToken == "" || response.User.Name != wantName || response.ServerID != f.server.serverID {
				t.Fatal("known managed response/wire contract", w.Code)
			}
			var stored models.Session
			if err := f.ds.Pool().QueryRow(t.Context(), `SELECT token,account_id,client_id,scope,scope_resource,user_agent,ip_address FROM sessions WHERE token=$1`, response.AccessToken).Scan(&stored.Token, &stored.AccountID, &stored.ClientID, &stored.Scope, &stored.ScopeResource, &stored.UserAgent, &stored.IPAddress); err != nil {
				t.Fatal(err)
			}
			if stored.AccountID != "actor" || stored.ScopeResource != "target" || stored.Scope != models.SessionScopeJellyfin || stored.ClientID != "synthetic-public-device" || stored.UserAgent != "Jellyfin: Synthetic TV 2.0 Living Room" || stored.IPAddress != "198.51.100.8" || f.sessionCount(t) != 1 || f.ds.Pool().Stat().AcquiredConns() != 0 {
				t.Fatal("actual scoped session/client metadata or cleanup differs")
			}
		})
	}
}

func TestManagedPasswordPublicTerminalFailures(t *testing.T) {
	for _, kind := range []string{"wrong-password", "ambiguous", "missing-adapter", "copied-adapter", "closed-security", "disabled-policy", "unavailable", "unknown-ack", "cancelled", "child", "household"} {
		t.Run(kind, func(t *testing.T) {
			f, proxy := managedPasswordPG(t, false, kind == "unknown-ack")
			password := managedPasswordTestSecret
			want := http.StatusServiceUnavailable
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			switch kind {
			case "wrong-password":
				password = "synthetic wrong"
				want = http.StatusUnauthorized
			case "ambiguous":
				hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('other','Synthetic other',$1)`, string(hash)); err != nil {
					t.Fatal(err)
				}
				if _, err = f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('other-profile','other','Synthetic profile');INSERT INTO user_settings_versions(user_id,generation)VALUES('other-profile',0) ON CONFLICT(user_id) DO NOTHING`); err != nil {
					t.Fatal(err)
				}
				want = http.StatusUnauthorized
			case "missing-adapter":
				f.server.deps.ManagedPassword = nil
			case "copied-adapter":
				copyOfAdapter := *f.adapter
				f.server.deps.ManagedPassword = &copyOfAdapter
			case "closed-security":
				_ = f.security.Close()
			case "disabled-policy":
				settings, err := f.manager.Load()
				if err != nil {
					t.Fatal(err)
				}
				settings.JellyfinBridge.Enabled = false
				if err = f.manager.Save(settings); err != nil {
					t.Fatal(err)
				}
			case "unavailable":
				f.ds.Close()
			case "unknown-ack":
				proxy.armed.Store(true)
			case "cancelled":
				cancel()
			case "child":
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id='target'`); err != nil {
					t.Fatal(err)
				}
			case "household":
				if _, err := f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic child',true)`); err != nil {
					t.Fatal(err)
				}
			}
			w := f.callPassword(t, ctx, "Synthetic profile", password, "198.51.100.8:1234")
			if w.Code != want || strings.Contains(w.Body.String(), "AccessToken") || strings.Contains(w.Body.String(), managedPasswordTestSecret) {
				t.Fatal("terminal managed failure leaked or fell through", kind, w.Code)
			}
			if kind == "unknown-ack" {
				if !proxy.dropped.Load() || f.sessionCount(t) != 1 {
					t.Fatal("missing actual committed but discarded ACK witness")
				}
			} else if f.sessionCount(t) != 0 {
				t.Fatal("failure created a legacy session")
			}
		})
	}
}

func TestManagedPasswordPublicProfileAliasBudget(t *testing.T) {
	f, _ := managedPasswordPG(t, false, false)
	for n := 0; n < 10; n++ {
		name := "Synthetic profile"
		if n%2 == 0 {
			name = "Synthetic actor/Synthetic profile"
		}
		w := f.callPassword(t, t.Context(), name, "synthetic wrong", fmt.Sprintf("198.51.100.%d:1234", n+1))
		if w.Code != http.StatusUnauthorized {
			t.Fatal("ordinary failed attempt", n, w.Code)
		}
	}
	w := f.callPassword(t, t.Context(), " SYNTHETIC PROFILE ", managedPasswordTestSecret, "203.0.113.9:1234")
	if w.Code != http.StatusTooManyRequests || f.sessionCount(t) != 0 {
		t.Fatal("profile aliases did not share pre-verification budget", w.Code)
	}
}

func TestManagedPasswordPublicLimiterSetRace(t *testing.T) {
	f, _ := managedPasswordPG(t, true, false)
	connection, err := f.ds.Pool().Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	pid := connection.Conn().PgConn().PID()
	connection.Release()
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	f.server.login.mu.Lock()
	locked := true
	defer func() {
		if locked {
			f.server.login.mu.Unlock()
		}
	}()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- f.callPassword(t, ctx, "Synthetic profile", managedPasswordTestSecret, "198.51.100.8:1234")
	}()
	defer func() {
		cancel()
		if locked {
			f.server.login.mu.Unlock()
			locked = false
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("public handler did not join")
		}
	}()
	seen := false
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND state='idle' AND query LIKE '%concat(u.tableoid::oid%')`, pid).Scan(&seen); err != nil {
			t.Fatal(err)
		}
		if seen {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !seen {
		t.Fatal("actual name snapshot did not reach limiter barrier")
	}
	if _, err = f.admin.Exec(t.Context(), `INSERT INTO `+pgx.Identifier{f.schema, "users"}.Sanitize()+`(id,account_id,name)VALUES('racing-profile','actor','Synthetic profile')`); err != nil {
		t.Fatal(err)
	}
	f.server.login.mu.Unlock()
	locked = false
	select {
	case w := <-done:
		// Restore the single-consumer completion signal for the cleanup join.
		done <- w
		if w.Code != http.StatusServiceUnavailable || f.sessionCount(t) != 0 {
			t.Fatal("uncharged new candidate reached authentication", w.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("limiter race did not finish")
	}
}

func TestManagedPasswordPublicCurrentPINPolicy(t *testing.T) {
	for _, kind := range []string{"optional-account", "enforced-account-rejected", "enforced-combined", "optional-pin-enabled", "optional-pin-disabled"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := managedPasswordPG(t, false, false)
			hash, err := bcrypt.GenerateFromPassword([]byte("2580"), bcrypt.MinCost)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.ds.Pool().Exec(t.Context(), `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='target'`, string(hash)); err != nil {
				t.Fatal(err)
			}
			settings, err := f.manager.Load()
			if err != nil {
				t.Fatal(err)
			}
			settings.Security.EnforceProfilePins = strings.HasPrefix(kind, "enforced")
			settings.JellyfinBridge.AllowPinLogin = kind == "optional-pin-enabled"
			if err = f.manager.Save(settings); err != nil {
				t.Fatal(err)
			}
			password := managedPasswordTestSecret
			if kind == "enforced-combined" {
				password += "2580"
			}
			if strings.HasPrefix(kind, "optional-pin") {
				password = "2580"
			}
			want := http.StatusOK
			if kind == "enforced-account-rejected" || kind == "optional-pin-disabled" {
				want = http.StatusUnauthorized
			}
			w := f.callPassword(t, t.Context(), "Synthetic profile", password, "198.51.100.8:1234")
			if w.Code != want {
				t.Fatal("public current PIN policy", kind, w.Code)
			}
			if want != http.StatusOK && f.sessionCount(t) != 0 {
				t.Fatal("rejected policy minted a session")
			}
		})
	}
}

func TestManagedPasswordPublicCancellationJoinsActualSQL(t *testing.T) {
	for _, kind := range []string{"caller", "startup-lifetime"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := managedPasswordPG(t, true, false)
			life, retire := context.WithCancel(t.Context())
			defer retire()
			adapter, err := NewManagedPasswordLogin(life, f.ds, f.manager, f.security, f.coordinator, profileunlock.NewPGProfilePins(f.ds))
			if err != nil {
				t.Fatal(err)
			}
			f.server.deps.ManagedPassword = adapter
			connection, err := f.ds.Pool().Acquire(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			pid := connection.Conn().PgConn().PID()
			connection.Release()
			tx, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err = tx.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "users"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				done <- f.callPassword(t, ctx, "Synthetic profile", managedPasswordTestSecret, "198.51.100.8:1234")
			}()
			defer func() {
				cancel()
				_ = tx.Rollback(context.Background())
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("handler cleanup did not join")
				}
			}()
			waiting := false
			until := time.Now().Add(time.Second)
			for time.Now().Before(until) {
				if err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if !waiting {
				t.Fatal("real password name SQL did not block")
			}
			if kind == "caller" {
				cancel()
			} else {
				retire()
			}
			select {
			case w := <-done:
				done <- w
				if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), "AccessToken") || f.ds.Pool().Stat().AcquiredConns() != 0 {
					t.Fatal("cancelled actual SQL escaped cleanup", w.Code)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("cancel did not join actual SQL")
			}
			guard, err := f.coordinator.TryWrite(t.Context())
			if err != nil {
				t.Fatal("original native hold not refunded after SQL cleanup", err)
			}
			_ = guard.Close()
		})
	}
}

// The listener only observes real transport calls; every Read and deadline
// demand is delegated to the actual accepted TCP connection.
type managedPasswordBodyListener struct {
	net.Listener
	armed                                atomic.Bool
	entered, returned, interrupted       chan struct{}
	enterOnce, returnOnce, interruptOnce sync.Once
}

func (l *managedPasswordBodyListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &managedPasswordBodyConn{Conn: connection, owner: l}, nil
}

type managedPasswordBodyConn struct {
	net.Conn
	owner *managedPasswordBodyListener
}

func (c *managedPasswordBodyConn) Read(value []byte) (int, error) {
	observed := c.owner.armed.Load()
	if observed {
		c.owner.enterOnce.Do(func() { close(c.owner.entered) })
	}
	n, err := c.Conn.Read(value)
	if observed {
		c.owner.returnOnce.Do(func() { close(c.owner.returned) })
	}
	return n, err
}

func (c *managedPasswordBodyConn) SetReadDeadline(deadline time.Time) error {
	err := c.Conn.SetReadDeadline(deadline)
	if err == nil && c.owner.armed.Load() && !deadline.IsZero() && !deadline.After(time.Now()) {
		c.owner.interruptOnce.Do(func() { close(c.owner.interrupted) })
	}
	return err
}

func TestManagedPasswordPublicCancellationInterruptsActualHTTPBody(t *testing.T) {
	for _, kind := range []string{"caller", "startup-lifetime"} {
		t.Run(kind, func(t *testing.T) {
			f, _ := managedPasswordPG(t, true, false)
			caller, cancelCaller := context.WithCancel(t.Context())
			defer cancelCaller()
			life, retire := context.WithCancel(t.Context())
			defer retire()
			adapter, err := NewManagedPasswordLogin(life, f.ds, f.manager, f.security, f.coordinator, profileunlock.NewPGProfilePins(f.ds))
			if err != nil {
				t.Fatal(err)
			}
			f.server.deps.ManagedPassword = adapter
			done := make(chan struct{})
			var listener *managedPasswordBodyListener
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				listener.armed.Store(true)
				f.server.Handler().ServeHTTP(w, r)
				close(done)
			}))
			listener = &managedPasswordBodyListener{Listener: server.Listener, entered: make(chan struct{}), returned: make(chan struct{}), interrupted: make(chan struct{})}
			server.Listener = listener
			server.Config.BaseContext = func(net.Listener) context.Context { return caller }
			server.Start()
			t.Cleanup(server.Close)
			client, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			defer func() {
				cancelCaller()
				retire()
				_ = client.Close()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("real held-body handler did not join")
				}
			}()
			if _, err = fmt.Fprint(client, "POST /Users/AuthenticateByName HTTP/1.1\r\nHost: synthetic.invalid\r\nContent-Length: 256\r\nConnection: close\r\n\r\n{\"Username\":\"Synthetic profile\","); err != nil {
				t.Fatal(err)
			}
			select {
			case <-listener.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("actual TCP body read never blocked")
			}
			select {
			case <-listener.returned:
				t.Fatal("body read completed before cancellation")
			default:
			}
			if kind == "caller" {
				cancelCaller()
			} else {
				retire()
			}
			select {
			case <-listener.interrupted:
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation did not demand an actual transport read interruption")
			}
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("handler kept the body read alive after cancellation")
			}
			select {
			case <-listener.returned:
			default:
				t.Fatal("handler returned before actual TCP Read completed")
			}
			_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
			response, err := http.ReadResponse(bufio.NewReader(client), nil)
			if err != nil {
				t.Fatal("actual cancellation response", err)
			}
			_ = response.Body.Close()
			if response.StatusCode != http.StatusServiceUnavailable || f.sessionCount(t) != 0 || f.ds.Pool().Stat().AcquiredConns() != 0 {
				t.Fatal("held body cancellation reached authentication or failed to join", response.StatusCode)
			}
		})
	}
}
