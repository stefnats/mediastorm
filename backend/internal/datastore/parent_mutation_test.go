package datastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/models"
)

const parentTestPassword = "synthetic parent credential"

type parentPGFixture struct {
	ds      *DataStore
	admin   *pgxpool.Pool
	cfg     *pgxpool.Config
	session models.Session
	hash    string
}

func parentFixture(t *testing.T) *parentPGFixture {
	t.Helper()
	raw := os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket database required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect local fixture")
	}
	role := fmt.Sprintf("synthetic_pm_%d", time.Now().UnixNano())
	schema := role
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal("create own fixture role")
	}
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{role}.Sanitize()); err != nil {
		_, _ = admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{role}.Sanitize())
		admin.Close()
		t.Fatal("create own fixture schema")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("parse local fixture")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["application_name"] = schema
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{schema}.Sanitize() + ",pg_catalog"
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("connect scoped fixture")
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Error("remove own fixture schema")
		}
		if _, err := admin.Exec(cleanup, "DROP ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Error("remove own fixture role")
		}
		admin.Close()
	})
	f := &parentPGFixture{ds: &DataStore{pool: pool}, admin: admin, cfg: cfg}
	f.exec(t, `CREATE TABLE accounts(id TEXT PRIMARY KEY,username TEXT NOT NULL,password_hash TEXT NOT NULL,is_master BOOLEAN NOT NULL DEFAULT false,max_streams INT NOT NULL DEFAULT 0,expires_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
 CREATE TABLE sessions(token TEXT PRIMARY KEY,account_id TEXT NOT NULL,is_master BOOLEAN NOT NULL DEFAULT false,expires_at TIMESTAMPTZ NOT NULL,created_at TIMESTAMPTZ NOT NULL,user_agent TEXT NOT NULL DEFAULT '',ip_address TEXT NOT NULL DEFAULT '',scope TEXT NOT NULL DEFAULT '',scope_resource TEXT NOT NULL DEFAULT '',client_id TEXT NOT NULL DEFAULT '');
 CREATE TABLE users(id TEXT PRIMARY KEY,account_id TEXT NOT NULL,name TEXT NOT NULL,color TEXT NOT NULL DEFAULT '',icon_url TEXT NOT NULL DEFAULT '',pin_hash TEXT NOT NULL DEFAULT '',pin_length INT NOT NULL DEFAULT 0,trakt_account_id TEXT NOT NULL DEFAULT '',plex_account_id TEXT NOT NULL DEFAULT '',mdblist_account_id TEXT NOT NULL DEFAULT '',simkl_account_id TEXT NOT NULL DEFAULT '',scrob_account_id TEXT NOT NULL DEFAULT '',is_kids_profile BOOLEAN NOT NULL DEFAULT false,kids_mode TEXT NOT NULL DEFAULT '',kids_max_rating TEXT NOT NULL DEFAULT '',kids_max_movie_rating TEXT NOT NULL DEFAULT '',kids_max_tv_rating TEXT NOT NULL DEFAULT '',kids_allowed_lists JSONB NOT NULL DEFAULT '[]',allow_share_links BOOLEAN NOT NULL DEFAULT false,activity_privacy TEXT NOT NULL DEFAULT 'not_shared',allow_live_takeover BOOLEAN NOT NULL DEFAULT false,allow_recording_interruption BOOLEAN NOT NULL DEFAULT false,created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());`)
	hash, err := bcrypt.GenerateFromPassword([]byte(parentTestPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal("hash synthetic password")
	}
	f.hash = string(hash)
	f.exec(t, `INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Parent',$1),('other','Synthetic Other',$1)`, f.hash)
	f.exec(t, `INSERT INTO users(id,account_id,name,pin_hash,kids_mode,kids_max_rating,kids_allowed_lists,allow_live_takeover)VALUES('target','actor','Synthetic Target','PRIVATE_PIN_HASH','content_list','PG','["PRIVATE_LIST"]',true),('unrelated','other','Unrelated','','','', '[]',false),('own-second','actor','Own Second','','','', '[]',false)`)
	now := time.Now().UTC().Truncate(time.Microsecond)
	f.session = models.Session{Token: "PRIVATE_SYNTHETIC_TOKEN", AccountID: "actor", ExpiresAt: now.Add(time.Hour), CreatedAt: now, ClientID: "synthetic-device"}
	f.exec(t, `INSERT INTO sessions(token,account_id,expires_at,created_at,client_id)VALUES($1,$2,$3,$4,$5)`, f.session.Token, f.session.AccountID, f.session.ExpiresAt, f.session.CreatedAt, f.session.ClientID)
	return f
}
func (f *parentPGFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.ds.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal("synthetic statement failed")
	}
}
func (f *parentPGFixture) request(ctx context.Context) *http.Request {
	r := httptest.NewRequest("POST", "http://synthetic.invalid/internal-parent-flag", nil).WithContext(ctx)
	c := context.WithValue(r.Context(), auth.ContextKeySession, f.session)
	c = context.WithValue(c, auth.ContextKeyAccountID, f.session.AccountID)
	c = context.WithValue(c, auth.ContextKeyIsMaster, f.session.IsMaster)
	return r.WithContext(c)
}
func (f *parentPGFixture) prepare(t *testing.T) (*ParentKidsIntent, *ParentKidsProof) {
	t.Helper()
	i, err := f.ds.PrepareKidsProfileChange(f.request(t.Context()), "target", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(i.Close)
	p, err := i.VerifyCredential(parentTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	return i, p
}
func (f *parentPGFixture) flag(t *testing.T) bool {
	t.Helper()
	var flag bool
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT is_kids_profile FROM users WHERE id='target'`).Scan(&flag); err != nil {
		t.Fatal("read target")
	}
	return flag
}
func TestParentMutationFlagOnlyActualPostgres(t *testing.T) {
	f := parentFixture(t)
	var before, other []byte
	_ = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u)-'is_kids_profile'-'updated_at' FROM users u WHERE id='target'`).Scan(&before)
	_ = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='unrelated'`).Scan(&other)
	i, p := f.prepare(t)
	result, err := f.ds.CommitKidsProfileChange(i, p)
	if err != nil || result == nil || !result.IsKidsProfile {
		t.Fatalf("flag-only commit failed: %v", err)
	}
	var after, otherAfter []byte
	_ = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u)-'is_kids_profile'-'updated_at' FROM users u WHERE id='target'`).Scan(&after)
	_ = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='unrelated'`).Scan(&otherAfter)
	if !bytes.Equal(before, after) || !bytes.Equal(other, otherAfter) || result.KidsMode != "content_list" || !result.AllowLiveTakeover {
		t.Fatal("flag-only changed unrelated/default fields")
	}
	if _, err = f.ds.CommitKidsProfileChange(i, p); !errors.Is(err, ErrParentMutationUsed) {
		t.Fatal("proof replayed")
	}
}
func TestParentMutationOpaqueIdentityAndActualPassword(t *testing.T) {
	for _, mode := range []string{"wrong password", "empty password", "intent copy", "proof copy", "zero proof", "JSON import", "other store", "wrong intent", "wrong value", "closed", "master still password"} {
		t.Run(mode, func(t *testing.T) {
			f := parentFixture(t)
			if mode == "master still password" {
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.session.IsMaster = true
			}
			i, err := f.ds.PrepareKidsProfileChange(f.request(t.Context()), "target", true)
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			if mode == "wrong password" || mode == "empty password" || mode == "master still password" {
				password := "wrong synthetic password"
				if mode == "empty password" {
					password = ""
				}
				if _, err = i.VerifyCredential(password); !errors.Is(err, ErrParentMutationCredential) {
					t.Fatal("missing fresh original credential accepted")
				}
				if _, err = i.VerifyCredential(parentTestPassword); !errors.Is(err, ErrParentMutationUsed) {
					t.Fatal("failed guess reused")
				}
				return
			}
			p, err := i.VerifyCredential(parentTestPassword)
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal([]any{i, p})
			if string(raw) != "[null,null]" {
				t.Fatal("intent serialized")
			}
			for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
				text := fmt.Sprintf(verb, i) + fmt.Sprintf(verb, p)
				if strings.Contains(text, "PRIVATE_") || strings.Contains(text, f.hash) || strings.Contains(text, "actor") {
					t.Fatal("credential formatting leaked")
				}
			}
			ds := f.ds
			switch mode {
			case "intent copy":
				copy := *i
				i = &copy
			case "proof copy":
				copy := *p
				p = &copy
			case "zero proof":
				p = &ParentKidsProof{}
			case "JSON import":
				var imported ParentKidsProof
				_ = json.Unmarshal([]byte(`{"state":{"verified":true}}`), &imported)
				p = &imported
			case "other store":
				ds = &DataStore{pool: f.ds.pool}
			case "wrong intent", "wrong value":
				target := "own-second"
				if mode == "wrong value" {
					target = "target"
				}
				other, e := f.ds.PrepareKidsProfileChange(f.request(t.Context()), target, false)
				if e != nil {
					t.Fatal("mismatched intent fixture failed")
				}
				defer other.Close()
				i = other
			case "closed":
				i.Close()
			}
			if _, err = ds.CommitKidsProfileChange(i, p); err == nil || f.flag(t) {
				t.Fatal("forged/copied/imported proof wrote")
			}
		})
	}
}
func TestParentMutationPrepareRejectsUntrustedOriginal(t *testing.T) {
	for _, mode := range []string{"no context", "scope", "resource", "account mismatch", "role spoof", "client mismatch", "created mismatch", "expired claim", "cancelled", "foreign target", "missing target", "missing reader", "DB scope", "DB expiry"} {
		t.Run(mode, func(t *testing.T) {
			f := parentFixture(t)
			ctx := t.Context()
			target := "target"
			ds := f.ds
			switch mode {
			case "scope":
				f.session.Scope = "stream"
			case "resource":
				f.session.ScopeResource = "resource"
			case "account mismatch":
				f.session.AccountID = "other"
			case "role spoof":
				f.session.IsMaster = true
			case "client mismatch":
				f.session.ClientID = "other"
			case "created mismatch":
				f.session.CreatedAt = f.session.CreatedAt.Add(-time.Second)
			case "expired claim":
				f.session.ExpiresAt = time.Now().Add(-time.Second)
			case "cancelled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			case "foreign target":
				target = "unrelated"
			case "missing target":
				target = "absent"
			case "missing reader":
				ds = &DataStore{}
			case "DB scope":
				f.exec(t, `UPDATE sessions SET scope='stream'`)
			case "DB expiry":
				f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`)
			}
			r := f.request(ctx)
			if mode == "no context" {
				r = httptest.NewRequest("POST", "http://synthetic.invalid", nil)
			}
			if i, err := ds.PrepareKidsProfileChange(r, target, true); err == nil {
				if i != nil {
					i.Close()
				}
				t.Fatal("untrusted original accepted")
			}
			if f.flag(t) {
				t.Fatal("prepare wrote")
			}
		})
	}
}
func TestParentMutationPostProofChangesAndABA(t *testing.T) {
	cases := map[string]string{
		"revoke": `DELETE FROM sessions`, "demotion": `UPDATE accounts SET is_master=false WHERE id='actor'`, "credentials": `UPDATE accounts SET password_hash='changed-hash' WHERE id='actor'`, "actor expiry": `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`, "session expiry": `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second'`, "scope": `UPDATE sessions SET scope='jellyfin'`, "owner": `UPDATE users SET account_id='other' WHERE id='target'`, "PIN": `UPDATE users SET pin_hash='CHANGED_PIN' WHERE id='target'`, "kids": `UPDATE users SET kids_mode='rating' WHERE id='target'`, "delete": `DELETE FROM users WHERE id='target'`, "ABA": `UPDATE users SET name='changed' WHERE id='target';UPDATE users SET name='Synthetic Target' WHERE id='target'`, "same-value update": `UPDATE users SET name=name WHERE id='target'`,
		"actor ABA":             `UPDATE accounts SET is_master=true WHERE id='actor';UPDATE accounts SET is_master=false WHERE id='actor'`,
		"session role ABA":      `UPDATE sessions SET is_master=true;UPDATE sessions SET is_master=false`,
		"session cache update":  `UPDATE sessions SET is_master=true`,
		"session ABA":           `UPDATE sessions SET client_id='temporary';UPDATE sessions SET client_id='synthetic-device'`,
		"credential same-value": `UPDATE accounts SET password_hash=password_hash WHERE id='actor'`,
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			f := parentFixture(t)
			if name == "demotion" {
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.session.IsMaster = true
			}
			i, p := f.prepare(t)
			f.exec(t, sql)
			if _, err := f.ds.CommitKidsProfileChange(i, p); err == nil {
				t.Fatal("changed authoritative tuple committed")
			}
			var count int
			_ = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM users WHERE id='target' AND is_kids_profile=true`).Scan(&count)
			if count != 0 {
				t.Fatal("stale proof wrote")
			}
		})
	}
}
func waitParentStage(t *testing.T, i *ParentKidsIntent, phase parentMutationPhase) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		i.state.mu.Lock()
		got := i.state.phase
		i.state.mu.Unlock()
		if got == phase {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("phase barrier not reached")
}
func TestParentMutationPasswordOutsideLocksAndCredentialVersion(t *testing.T) {
	f := parentFixture(t)
	hash, err := bcrypt.GenerateFromPassword([]byte(parentTestPassword), 13)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='actor'`, string(hash))
	i, err := f.ds.PrepareKidsProfileChange(f.request(t.Context()), "target", true)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	proof := make(chan *ParentKidsProof, 1)
	go func() { p, _ := i.VerifyCredential(parentTestPassword); proof <- p }()
	waitParentStage(t, i, parentVerifying)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err = f.ds.pool.Exec(ctx, `UPDATE accounts SET password_hash=$1 WHERE id='actor'`, f.hash); err != nil {
		t.Fatal("bcrypt held database row locks")
	}
	i.state.mu.Lock()
	stillVerifying := i.state.phase == parentVerifying
	i.state.mu.Unlock()
	if !stillVerifying {
		t.Fatal("database concurrency was not observed during actual bcrypt")
	}
	p := <-proof
	if p == nil {
		t.Fatal("original actual hash verification failed unexpectedly")
	}
	if _, err = f.ds.CommitKidsProfileChange(i, p); err == nil || f.flag(t) {
		t.Fatal("changed credential row adopted")
	}
}
func TestParentMutationBlockedLockCancelAndOriginalExpiry(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "close", "renewed session", "commit ABA"} {
		t.Run(mode, func(t *testing.T) {
			f := parentFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := f.request(ctx)
			if mode == "deadline" {
				r = r.WithContext(ctx)
				f.session.ExpiresAt = time.Now().Add(500 * time.Millisecond)
				r = f.request(ctx)
			}
			i, err := f.ds.PrepareKidsProfileChange(r, "target", true)
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			p, err := i.VerifyCredential(parentTestPassword)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "renewed session" {
				f.exec(t, `UPDATE sessions SET expires_at=expires_at+interval '1 hour'`)
				if _, err = f.ds.CommitKidsProfileChange(i, p); err == nil {
					t.Fatal("renewed session adopted")
				}
				return
			}
			blocker, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			if _, err = blocker.Exec(t.Context(), `SELECT id FROM users WHERE id='target' FOR UPDATE`); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { _, err := f.ds.CommitKidsProfileChange(i, p); done <- err }()
			waitParentStage(t, i, parentCommitting)
			f.waitBlocked(t)
			switch mode {
			case "cancel":
				cancel()
			case "close":
				i.Close()
			case "commit ABA":
				_, err = blocker.Exec(t.Context(), `UPDATE users SET name='temporary' WHERE id='target';UPDATE users SET name='Synthetic Target' WHERE id='target'`)
				if err != nil {
					t.Fatal(err)
				}
				_ = blocker.Commit(t.Context())
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("blocked retired transaction committed")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("blocked operation ignored original cancellation/deadline")
			}
			_ = blocker.Rollback(context.Background())
			if f.flag(t) {
				t.Fatal("cancelled/expired/stale transaction wrote")
			}
		})
	}
}
func TestParentMutationParallelSameProofOnce(t *testing.T) {
	f := parentFixture(t)
	i, p := f.prepare(t)
	var success atomic.Int32
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := f.ds.CommitKidsProfileChange(i, p); err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 1 || !f.flag(t) {
		t.Fatal("proof not once-only")
	}
}

// This wire wrapper forwards the real COMMIT and waits for real PostgreSQL
// response bytes, then loses only its acknowledgement. It demonstrates why an
// unknown outcome must never be described as zero writes or auto-retried.
type parentDropCommit struct {
	net.Conn
	armed   atomic.Bool
	dropped *atomic.Bool
}

func (c *parentDropCommit) Write(p []byte) (int, error) {
	for rest := p; len(rest) >= 5; {
		n := int(binary.BigEndian.Uint32(rest[1:5]))
		if n < 4 || len(rest) < n+1 {
			break
		}
		if rest[0] == 'Q' && strings.EqualFold(strings.TrimRight(string(rest[5:n+1]), "\x00"), "commit") {
			c.armed.Store(true)
		}
		rest = rest[n+1:]
	}
	return c.Conn.Write(p)
}
func (c *parentDropCommit) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if c.armed.Load() && n > 0 {
		c.dropped.Store(true)
		return 0, io.EOF
	}
	return n, err
}
func TestParentMutationUnknownCommitMayHaveWritten(t *testing.T) {
	f := parentFixture(t)
	cfg := f.cfg.Copy()
	var dropped atomic.Bool
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &parentDropCommit{Conn: c, dropped: &dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("connect acknowledgement fixture")
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	i, err := ds.PrepareKidsProfileChange(f.request(t.Context()), "target", true)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	p, err := i.VerifyCredential(parentTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ds.CommitKidsProfileChange(i, p); !errors.Is(err, ErrParentMutationOutcomeUnknown) || !dropped.Load() {
		t.Fatalf("missing actual unknown commit: %v", err)
	}
	if !f.flag(t) {
		t.Fatal("test did not prove that lost acknowledgement can follow a real commit")
	}
	if _, err = ds.CommitKidsProfileChange(i, p); !errors.Is(err, ErrParentMutationUsed) {
		t.Fatal("unknown commit was replayed")
	}
}
func (f *parentPGFixture) waitBlocked(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock'`, f.cfg.ConnConfig.RuntimeParams["application_name"]).Scan(&n)
		if err != nil {
			t.Fatal("read only own fixture lock telemetry")
		}
		if n > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual blocked database lock was not observed")
}
func TestParentMutationForeignMasterAndOwnerRowVersion(t *testing.T) {
	for _, mode := range []string{"positive", "owner expiry", "owner ABA", "owner missing"} {
		t.Run(mode, func(t *testing.T) {
			f := parentFixture(t)
			f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';UPDATE users SET account_id='other' WHERE id='target'`)
			f.session.IsMaster = true
			i, p := f.prepare(t)
			switch mode {
			case "owner expiry":
				f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='other'`)
			case "owner ABA":
				f.exec(t, `UPDATE accounts SET username='temporary' WHERE id='other';UPDATE accounts SET username='Synthetic Other' WHERE id='other'`)
			case "owner missing":
				f.exec(t, `DELETE FROM accounts WHERE id='other'`)
			}
			result, err := f.ds.CommitKidsProfileChange(i, p)
			if mode == "positive" {
				if err != nil || result == nil || !result.IsKidsProfile {
					t.Fatal("fresh genuine master flag-only commit failed")
				}
			} else if err == nil || f.flag(t) {
				t.Fatal("foreign owner row change ignored")
			}
		})
	}
}
func TestParentMutationCancelDuringActualCredentialComparison(t *testing.T) {
	f := parentFixture(t)
	hash, err := bcrypt.GenerateFromPassword([]byte(parentTestPassword), 13)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='actor'`, string(hash))
	ctx, cancel := context.WithCancel(t.Context())
	i, err := f.ds.PrepareKidsProfileChange(f.request(ctx), "target", true)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	done := make(chan error, 1)
	go func() { _, err := i.VerifyCredential(parentTestPassword); done <- err }()
	waitParentStage(t, i, parentVerifying)
	cancel()
	if err = <-done; !errors.Is(err, ErrParentMutationExpired) || f.flag(t) {
		t.Fatal("cancelled password comparison issued usable proof")
	}
}
func TestParentMutationInitialSessionRoleIsOnlyCache(t *testing.T) {
	for _, mode := range []string{"normalized genuine master", "stale master cache cannot elevate"} {
		t.Run(mode, func(t *testing.T) {
			f := parentFixture(t)
			if mode == "normalized genuine master" {
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.session.IsMaster = true
			} else {
				f.exec(t, `UPDATE sessions SET is_master=true`)
			}
			target := "target"
			if mode == "stale master cache cannot elevate" {
				target = "unrelated"
			}
			i, err := f.ds.PrepareKidsProfileChange(f.request(t.Context()), target, true)
			if mode == "stale master cache cannot elevate" {
				if i != nil {
					i.Close()
				}
				if err == nil {
					t.Fatal("cached session master elevated real actor")
				}
				return
			}
			if err != nil {
				t.Fatal("normalized actual actor role denied solely by cached session role")
			}
			defer i.Close()
			p, err := i.VerifyCredential(parentTestPassword)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.ds.CommitKidsProfileChange(i, p); err != nil || !f.flag(t) {
				t.Fatal("fresh original normalized actor did not commit")
			}
		})
	}
}
