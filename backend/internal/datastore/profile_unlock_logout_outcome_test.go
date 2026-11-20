package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"novastream/models"
)

type logoutOutcomeFixture struct {
	ds, original *DataStore
	admin        *pgxpool.Pool
	schema       string
	session      models.Session
	request      *http.Request
	release      context.CancelFunc
	armed        atomic.Bool
	dropped      atomic.Bool
}

func logoutOutcomeToken(label string) string {
	sum := sha256.Sum256([]byte("synthetic-logout-" + label))
	return base64.URLEncoding.EncodeToString(sum[:])
}

// Only this fully migrated synthetic schema is owned. The extra connection
// wrapper observes an actual server COMMIT tag; it never supplies authority.
func logoutOutcomePG(t *testing.T, unknown bool) *logoutOutcomeFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/home/stefan/ops-runbook/tmp/") || !strings.HasPrefix(u.Path, "/synthetic_") {
		t.Fatal("synthetic Unix-socket fixture required")
	}
	f := &logoutOutcomeFixture{schema: fmt.Sprintf("synthetic_logout_%d", time.Now().UnixNano())}
	f.admin, err = pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	created := false
	t.Cleanup(func() {
		if f.release != nil {
			f.release()
		}
		if f.ds != nil && f.ds != f.original {
			f.ds.Close()
		}
		if f.original != nil {
			f.original.Close()
		}
		if created {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if _, err := f.admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{f.schema}.Sanitize()+" CASCADE"); err != nil {
				t.Error("remove own logout schema", err)
			}
		}
		f.admin.Close()
	})
	if _, err := f.admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{f.schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	created = true
	q := u.Query()
	q.Set("search_path", pgx.Identifier{f.schema}.Sanitize()+",pg_catalog")
	u.RawQuery = q.Encode()
	f.original, err = New(t.Context(), u.String(), WithMaxConns(4), WithMinConns(0))
	if err != nil {
		t.Fatal("migrate own logout schema", err)
	}
	f.ds = f.original
	if unknown {
		cfg := f.original.pool.Config().Copy()
		cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return &logoutOutcomeCommitConn{Conn: conn, armed: &f.armed, dropped: &f.dropped}, nil
		}
		configureQueryLifecycle(cfg.ConnConfig)
		pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		f.ds = &DataStore{pool: pool}
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic-logout-password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	account := models.Account{ID: "synthetic-actor", Username: "synthetic-logout", PasswordHash: string(hash), CreatedAt: now, UpdatedAt: now}
	if err := f.ds.Accounts().Create(t.Context(), &account); err != nil {
		t.Fatal(err)
	}
	pin, err := bcrypt.GenerateFromPassword([]byte("1357"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	profile := models.User{ID: "synthetic-profile", AccountID: account.ID, Name: "Synthetic logout profile", PinHash: string(pin), PinLength: 4, CreatedAt: now, UpdatedAt: now}
	if err := f.ds.Users().Create(t.Context(), &profile); err != nil {
		t.Fatal(err)
	}
	f.session = models.Session{Token: logoutOutcomeToken("original"), AccountID: account.ID, ClientID: "synthetic-logout-client", UserAgent: "synthetic logout", IPAddress: "127.0.0.1", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := f.ds.Sessions().Create(t.Context(), &f.session); err != nil {
		t.Fatal(err)
	}
	f.request, f.release, err = f.authenticate(t.Context(), f.session.Token)
	if err != nil {
		t.Fatal("capture original full PG frame", err)
	}
	return f
}

func (f *logoutOutcomeFixture) authenticate(ctx context.Context, token string) (*http.Request, context.CancelFunc, error) {
	r := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://synthetic.test/api/auth/logout", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	return f.ds.AuthenticateProfilePinRequest(r, token, f.session.ClientID)
}
func (f *logoutOutcomeFixture) count(t *testing.T, token string) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{f.schema, "sessions"}.Sanitize()+" WHERE token=$1", token).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (f *logoutOutcomeFixture) issue(t *testing.T) {
	t.Helper()
	issuer := NewPGPinIssuer(f.ds)
	intent, err := issuer.Prepare(f.request, "synthetic-profile", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	proof, err := issuer.Verify(intent, "1357")
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := issuer.Commit(intent, proof)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	if token, _, err := issuer.Release(f.request, receipt); err != nil || token == "" {
		t.Fatal("genuine original PIN grant was not released", err)
	}
}

func (f *logoutOutcomeFixture) grants(t *testing.T) int {
	t.Helper()
	var count int
	if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{f.schema, "profile_unlocks"}.Sanitize()).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

type logoutOutcomeCommitConn struct {
	net.Conn
	armed, dropped *atomic.Bool
	pending        []byte
}

func (c *logoutOutcomeCommitConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.pending) == 0 {
		header := make([]byte, 5)
		if _, err := io.ReadFull(c.Conn, header); err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint32(header[1:])
		if n < 4 || n > 4<<20 {
			return 0, errors.New("synthetic PG response exceeded budget")
		}
		body := make([]byte, n-4)
		if _, err := io.ReadFull(c.Conn, body); err != nil {
			return 0, err
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" && c.armed.CompareAndSwap(true, false) {
			c.dropped.Store(true)
			_ = c.Conn.Close()
			return 0, io.EOF
		}
		c.pending = append(header, body...)
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func TestPGLogoutOutcomeActualActorFailures(t *testing.T) {
	for _, kind := range []string{"unavailable", "missing", "changed"} {
		t.Run(kind, func(t *testing.T) {
			f := logoutOutcomePG(t, false)
			query := "UPDATE " + pgx.Identifier{f.schema, "accounts"}.Sanitize() + " SET username=username WHERE id=$1"
			want := ErrPGPinChanged
			switch kind {
			case "unavailable":
				query = "ALTER TABLE " + pgx.Identifier{f.schema, "accounts"}.Sanitize() + " RENAME TO unavailable_logout_accounts"
				want = ErrPGPinUnavailable
			case "missing":
				query = "DELETE FROM " + pgx.Identifier{f.schema, "accounts"}.Sanitize() + " WHERE id=$1"
			}
			args := []any{f.session.AccountID}
			if kind == "unavailable" {
				args = nil
			}
			if _, err := f.admin.Exec(t.Context(), query, args...); err != nil {
				t.Fatal(err)
			}
			if err := f.ds.LogoutProfilePinSession(f.request); !errors.Is(err, want) || errors.Is(err, ErrPGPinOutcomeUnknown) {
				t.Fatal("original actor outcome was misclassified", kind, err)
			}
			if kind == "unavailable" {
				if f.count(t, f.session.Token) != 1 {
					t.Fatal("store outage revoked the original session")
				}
				if _, err := f.admin.Exec(t.Context(), "ALTER TABLE "+pgx.Identifier{f.schema, "unavailable_logout_accounts"}.Sanitize()+" RENAME TO accounts"); err != nil {
					t.Fatal(err)
				}
				if err := f.ds.CheckProfilePinRequestFrame(f.request); err != nil {
					t.Fatal("recovery lost the same original frame", err)
				}
			}
		})
	}
}

func TestPGLogoutOutcomeOriginalLineageKeepsFreshLogin(t *testing.T) {
	f := logoutOutcomePG(t, false)
	f.issue(t)
	next, err := f.ds.RefreshProfilePinSession(f.request, logoutOutcomeToken("refreshed"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	fresh := f.session
	fresh.Token = logoutOutcomeToken("independent")
	if err := f.ds.Sessions().Create(t.Context(), &fresh); err != nil {
		t.Fatal(err)
	}
	_, release, err := f.authenticate(t.Context(), fresh.Token)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if err := f.ds.LogoutProfilePinSession(f.request); err != nil {
		t.Fatal("original A did not revoke its actual B lineage", err)
	}
	if f.count(t, f.session.Token) != 0 || f.count(t, next.Token) != 0 || f.count(t, fresh.Token) != 1 || f.grants(t) != 0 {
		t.Fatal("logout adopted an unrelated fresh login")
	}
}

func TestPGLogoutOutcomeCanceledAccountWaitJoins(t *testing.T) {
	f := logoutOutcomePG(t, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, release, err := f.authenticate(ctx, f.session.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	holder, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = holder.Rollback(cleanup)
	}()
	if _, err := holder.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, "accounts"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	var relation uint32
	if err := holder.QueryRow(t.Context(), "SELECT $1::regclass::oid", pgx.Identifier{f.schema, "accounts"}.Sanitize()).Scan(&relation); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- f.ds.LogoutProfilePinSession(r) }()
	pending := func() int {
		var count int
		if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	until := time.Now().Add(time.Second)
	for pending() == 0 {
		if time.Now().After(until) {
			t.Fatal("actual logout account query never blocked")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("pending logout finished before caller cancellation")
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("original cancellation was lost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("logout did not join canceled actual PG work")
	}
	if pending() != 0 || f.count(t, f.session.Token) != 1 {
		t.Fatal("pending backend lock/write outlived logout return")
	}
}

func TestPGLogoutOutcomeKnownAndUnknownCommit(t *testing.T) {
	for _, kind := range []string{"known", "unknown"} {
		t.Run(kind, func(t *testing.T) {
			f := logoutOutcomePG(t, kind == "unknown")
			f.issue(t)
			if f.grants(t) != 1 {
				t.Fatal("genuine grant precondition missing")
			}
			f.armed.Store(kind == "unknown")
			err := f.ds.LogoutProfilePinSession(f.request)
			if kind == "known" && err != nil || kind == "unknown" && (!errors.Is(err, ErrPGPinOutcomeUnknown) || !f.dropped.Load()) {
				t.Fatal("actual known/received-ACK-loss outcome changed", err)
			}
			if f.count(t, f.session.Token) != 0 || f.grants(t) != 0 {
				t.Fatal("COMMIT outcome was confused with rollback")
			}
		})
	}
}

func TestPGLogoutOutcomeOriginalExpiryAndReleasedFrame(t *testing.T) {
	for _, kind := range []string{"expired-before-auth", "released-frame"} {
		t.Run(kind, func(t *testing.T) {
			f := logoutOutcomePG(t, false)
			if kind == "expired-before-auth" {
				if _, err := f.ds.pool.Exec(t.Context(), "UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1", f.session.Token); err != nil {
					t.Fatal(err)
				}
				r, release, err := f.authenticate(t.Context(), f.session.Token)
				if release != nil {
					release()
				}
				if r != nil || !errors.Is(err, ErrProfilePinAccessChanged) {
					t.Fatal("genuine expired original session became store failure", err)
				}
			} else {
				f.release()
				if err := f.ds.LogoutProfilePinSession(f.request); !errors.Is(err, context.Canceled) || f.count(t, f.session.Token) != 1 {
					t.Fatal("released original frame could revoke or lost cancellation", err)
				}
			}
		})
	}
}
