package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

	"novastream/internal/auth"
	"novastream/models"
)

type unlockQueryTrace struct{ calls atomic.Int64 }

func (tr *unlockQueryTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	tr.calls.Add(1)
	return ctx
}
func (*unlockQueryTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type unlockPGFixture struct {
	admin          *pgxpool.Pool
	cfg            *pgxpool.Config
	raw            string
	ds             *DataStore
	trace          *unlockQueryTrace
	session        models.Session
	token, pinHash string
	grant          models.ProfileUnlock
	application    string
}

// Only a dedicated synthetic Unix-socket database is accepted. Each case owns
// its schema and role and never inspects other schemas or product configuration.
func unlockFixture(t *testing.T) *unlockPGFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_UNLOCK_STATE_TEST_DATABASE_URL")
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
		t.Fatal("connect fixture admin")
	}
	role := fmt.Sprintf("synthetic_pu_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal("create own role")
	}
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{role}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatal("create own schema")
	}
	cfg, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatal("parse own fixture")
	}
	cfg.MaxConns = 8
	cfg.ConnConfig.RuntimeParams["application_name"] = role
	cfg.ConnConfig.RuntimeParams["search_path"] = pgx.Identifier{role}.Sanitize() + ",pg_catalog"
	trace := &unlockQueryTrace{}
	cfg.ConnConfig.Tracer = trace
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("connect own schema")
	}
	t.Cleanup(func() {
		pool.Close()
		ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{role}.Sanitize()+" CASCADE"); err != nil {
			t.Error("drop own schema")
		}
		if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Error("drop own role")
		}
		admin.Close()
	})
	f := &unlockPGFixture{ds: &DataStore{pool: pool}, trace: trace, application: role, cfg: cfg, raw: raw, admin: admin}
	f.exec(t, `CREATE TABLE accounts(id TEXT PRIMARY KEY,password_hash TEXT NOT NULL,is_master BOOLEAN NOT NULL DEFAULT false,expires_at TIMESTAMPTZ);
    CREATE TABLE sessions(token TEXT PRIMARY KEY,account_id TEXT NOT NULL,is_master BOOLEAN NOT NULL DEFAULT false,expires_at TIMESTAMPTZ NOT NULL,created_at TIMESTAMPTZ NOT NULL,scope TEXT NOT NULL DEFAULT '',scope_resource TEXT NOT NULL DEFAULT '',client_id TEXT NOT NULL DEFAULT '');
    CREATE TABLE users(id TEXT PRIMARY KEY,account_id TEXT NOT NULL,pin_hash TEXT NOT NULL);`)
	for _, name := range []string{"066_profile_unlocks.sql", "077_profile_unlock_revocations.sql"} {
		contents, err := embedMigrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		f.exec(t, strings.Split(string(contents), "-- +goose Down")[0])
	}
	pin, err := bcrypt.GenerateFromPassword([]byte("synthetic target PIN"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.pinHash = string(pin)
	f.exec(t, `INSERT INTO accounts(id,password_hash) VALUES('actor','synthetic actor password hash'),('other','synthetic other password hash')`)
	f.exec(t, `INSERT INTO users(id,account_id,pin_hash) VALUES('target','actor',$1),('other-target','other',$1)`, f.pinHash)
	now := time.Now().UTC().Truncate(time.Microsecond)
	f.session = models.Session{Token: "SYNTHETIC_ORIGINAL_SESSION", AccountID: "actor", ClientID: "synthetic-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	f.exec(t, `INSERT INTO sessions(token,account_id,client_id,created_at,expires_at)VALUES($1,$2,$3,$4,$5)`, f.session.Token, f.session.AccountID, f.session.ClientID, f.session.CreatedAt, f.session.ExpiresAt)
	f.token = "pu_" + base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	digest := sha256.Sum256([]byte(f.pinHash))
	f.grant = models.ProfileUnlock{TokenHash: profileUnlockDigest(f.token), SessionHash: profileUnlockDigest(f.session.Token), AccountID: "actor", ProfileID: "target", PinFingerprint: hex.EncodeToString(digest[:8]), CreatedAt: now, ExpiresAt: now.Add(15 * time.Minute)}
	f.create(t)
	return f
}
func (f *unlockPGFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.ds.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal("synthetic fixture statement failed")
	}
}
func (f *unlockPGFixture) create(t *testing.T) {
	t.Helper()
	if err := f.ds.ProfileUnlocks().Create(t.Context(), &f.grant); err != nil {
		t.Fatal(err)
	}
}
func (f *unlockPGFixture) request(ctx context.Context) *http.Request {
	r := httptest.NewRequest("GET", "http://synthetic.invalid/internal-unlock-observation", nil).WithContext(ctx)
	ctx = context.WithValue(r.Context(), auth.ContextKeySession, f.session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, f.session.IsMaster)
	return r.WithContext(ctx)
}
func (f *unlockPGFixture) read(t *testing.T) *ProfileUnlockState {
	t.Helper()
	state, err := f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", f.token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	return state
}
func (f *unlockPGFixture) generation(t *testing.T, scope, key string) int64 {
	t.Helper()
	var value int64
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope=$1 AND scope_key=$2),0)`, scope, key).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
func (f *unlockPGFixture) count(t *testing.T) int64 {
	t.Helper()
	var count int64
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_unlocks`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestProfileUnlockStatePositiveOpaqueOneShot(t *testing.T) {
	f := unlockFixture(t)
	state := f.read(t)
	if left := time.Until(state.state.ctxDeadline()); left <= 0 || left > profileUnlockStateBudget {
		t.Fatal("observation budget not bounded")
	}
	for _, render := range []string{fmt.Sprintf("%v", state), fmt.Sprintf("%+v", state), fmt.Sprintf("%#v", state)} {
		if render != "ProfileUnlockState(opaque)" {
			t.Fatal("private tuple formatting exposed")
		}
	}
	raw, err := json.Marshal(state)
	if err != nil || string(raw) != "null" {
		t.Fatal("private tuple JSON exposed")
	}
	current, err := f.ds.CheckProfileUnlockState(state)
	if err != nil || !current {
		t.Fatalf("unchanged current tuple denied: %v", err)
	}
	before := f.trace.calls.Load()
	if _, err = f.ds.CheckProfileUnlockState(state); !errors.Is(err, ErrProfileUnlockStateUsed) || f.trace.calls.Load() != before {
		t.Fatal("one-shot replay performed IO")
	}
	if f.count(t) != 1 || f.generation(t, "profile", "target") != 0 {
		t.Fatal("read/check changed storage")
	}
}
func (s *profileUnlockRead) ctxDeadline() time.Time { at, _ := s.ctx.Deadline(); return at }

func TestProfileUnlockStateIdentityBeforeIO(t *testing.T) {
	for _, mode := range []string{"nil", "zero", "copy", "closed", "other-store"} {
		t.Run(mode, func(t *testing.T) {
			f := unlockFixture(t)
			state := f.read(t)
			ds := f.ds
			switch mode {
			case "nil":
				state = nil
			case "zero":
				state = &ProfileUnlockState{}
			case "copy":
				copied := *state
				state = &copied
			case "closed":
				state.Close()
			case "other-store":
				ds = &DataStore{pool: f.ds.pool}
			}
			before := f.trace.calls.Load()
			current, err := ds.CheckProfileUnlockState(state)
			if current || !errors.Is(err, ErrProfileUnlockStateUsed) || f.trace.calls.Load() != before {
				t.Fatal("invalid receipt performed IO")
			}
		})
	}
}

func TestProfileUnlockStateChangedTuple(t *testing.T) {
	cases := map[string]string{
		"PIN changed":              `UPDATE users SET pin_hash='replacement hash' WHERE id='target'`,
		"PIN cleared":              `UPDATE users SET pin_hash='' WHERE id='target'`,
		"owner changed":            `UPDATE users SET account_id='other' WHERE id='target'`,
		"role changed":             `UPDATE accounts SET is_master=true WHERE id='actor'`,
		"actor password reset":     `UPDATE accounts SET password_hash='replacement password' WHERE id='actor'`,
		"actor expiry":             `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`,
		"session revoked":          `DELETE FROM sessions`,
		"session rotated":          `UPDATE sessions SET token='SYNTHETIC_REPLACEMENT_SESSION'`,
		"session client changed":   `UPDATE sessions SET client_id='replacement-client'`,
		"grant expired":            `UPDATE profile_unlocks SET expires_at=clock_timestamp()-interval '1 second'`,
		"grant changed":            `UPDATE profile_unlocks SET pin_fingerprint='invalid'`,
		"equal profile update ABA": `UPDATE users SET pin_hash=pin_hash WHERE id='target'`,
		"equal session update ABA": `UPDATE sessions SET token=token`,
		"equal grant upsert ABA":   `UPDATE profile_unlocks SET token_hash=token_hash`,
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			f := unlockFixture(t)
			state := f.read(t)
			f.exec(t, sql)
			current, err := f.ds.CheckProfileUnlockState(state)
			if current || !errors.Is(err, ErrProfileUnlockStateDenied) {
				t.Fatalf("changed tuple accepted: %v", err)
			}
		})
	}
}

func TestProfileUnlockStateInitialDenials(t *testing.T) {
	cases := map[string]string{
		"missing grant":       `DELETE FROM profile_unlocks`,
		"missing target":      `DELETE FROM users WHERE id='target'`,
		"invalid target PIN":  `UPDATE users SET pin_hash='not bcrypt' WHERE id='target'`,
		"no target PIN":       `UPDATE users SET pin_hash='' WHERE id='target'`,
		"wrong owner":         `UPDATE users SET account_id='other' WHERE id='target'`,
		"wrong role":          `UPDATE accounts SET is_master=true WHERE id='actor'`,
		"scoped session":      `UPDATE sessions SET scope='stream',scope_resource='target'`,
		"expired session":     `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second'`,
		"expired owner":       `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`,
		"wrong fingerprint":   `UPDATE profile_unlocks SET pin_fingerprint='invalid'`,
		"wrong grant account": `UPDATE profile_unlocks SET account_id='other'`,
		"wrong grant session": `UPDATE profile_unlocks SET session_hash=repeat('0',64)`,
		"expired grant":       `UPDATE profile_unlocks SET expires_at=clock_timestamp()-interval '1 second'`,
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			f := unlockFixture(t)
			f.exec(t, sql)
			state, err := f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", f.token)
			if state != nil || !errors.Is(err, ErrProfileUnlockStateDenied) {
				t.Fatalf("invalid initial state accepted: %v", err)
			}
		})
	}
}

func TestProfileUnlockStateMasterStillRequiresCurrentTargetPIN(t *testing.T) {
	f := unlockFixture(t)
	f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';UPDATE sessions SET is_master=true;UPDATE users SET account_id='other' WHERE id='target'`)
	f.session.IsMaster = true
	state := f.read(t)
	if current, err := f.ds.CheckProfileUnlockState(state); err != nil || !current {
		t.Fatal("current genuine master/target PIN tuple denied")
	}
	f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
	if state, err := f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", f.token); state != nil || !errors.Is(err, ErrProfileUnlockStateDenied) {
		t.Fatal("master bypassed target PIN")
	}
}

func TestProfileUnlockStateContextAndOriginalDeadline(t *testing.T) {
	f := unlockFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := f.trace.calls.Load()
	if state, err := f.ds.ReadProfileUnlockState(f.request(ctx), "target", f.token); state != nil || !errors.Is(err, context.Canceled) || f.trace.calls.Load() != before {
		t.Fatal("cancelled request read storage")
	}
	ctx, cancel = context.WithCancel(t.Context())
	state, err := f.ds.ReadProfileUnlockState(f.request(ctx), "target", f.token)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	before = f.trace.calls.Load()
	if current, err := f.ds.CheckProfileUnlockState(state); current || !errors.Is(err, context.Canceled) || f.trace.calls.Load() != before {
		t.Fatal("cancelled observation performed IO")
	}
	ctx, cancel = context.WithDeadline(t.Context(), time.Now().Add(5*time.Second))
	defer cancel()
	state, err = f.ds.ReadProfileUnlockState(f.request(ctx), "target", f.token)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	expected, _ := ctx.Deadline()
	if state.state.ctxDeadline().After(expected) {
		t.Fatal("request deadline renewed")
	}
	f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()+interval '2 seconds' WHERE id='actor'`)
	state, err = f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", f.token)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if time.Until(state.state.ctxDeadline()) > 2*time.Second {
		t.Fatal("owner expiry not retained")
	}
}

func TestProfileUnlockRevocationEmptySetAndCurrentRead(t *testing.T) {
	for _, scope := range []string{"profile", "session"} {
		t.Run(scope, func(t *testing.T) {
			f := unlockFixture(t)
			state := f.read(t)
			key := "target"
			revoke := func() error { return f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), key) }
			if scope == "session" {
				key = f.grant.SessionHash
				revoke = func() error { return f.ds.ProfileUnlocks().DeleteBySession(t.Context(), key) }
			}
			if err := revoke(); err != nil {
				t.Fatal(err)
			}
			if f.count(t) != 0 || f.generation(t, scope, key) != 1 {
				t.Fatal("revoke not atomic")
			}
			if err := revoke(); err != nil {
				t.Fatal(err)
			}
			if f.generation(t, scope, key) != 2 {
				t.Fatal("empty revoke lost durable generation")
			}
			// This is intentionally the unchanged legacy Create, not a new issuer.
			f.create(t)
			if current, err := f.ds.CheckProfileUnlockState(state); current || !errors.Is(err, ErrProfileUnlockStateDenied) {
				t.Fatal("revoke/recreate ABA revived old observation")
			}
			fresh := f.read(t)
			if current, err := f.ds.CheckProfileUnlockState(fresh); err != nil || !current {
				t.Fatal("fresh legacy storage observation failed")
			}
		})
	}
}

func TestProfileUnlockRevocationRollbackOverflowAndSavepoint(t *testing.T) {
	f := unlockFixture(t)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = (&pgProfileUnlockRepo{pool: tx}).DeleteByProfile(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.count(t) != 1 || f.generation(t, "profile", "target") != 0 {
		t.Fatal("nested success escaped outer rollback")
	}
	f.exec(t, `INSERT INTO profile_unlock_revocations VALUES('profile','target',9223372036854775807)`)
	if err = f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target"); !errors.Is(err, ErrProfileUnlockStateUnavailable) {
		t.Fatal("overflow succeeded")
	}
	if f.count(t) != 1 || f.generation(t, "profile", "target") != 9223372036854775807 {
		t.Fatal("overflow partially deleted or reset generation")
	}
	tx, err = f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if err = (&pgProfileUnlockRepo{pool: tx}).DeleteByProfile(t.Context(), "target"); !errors.Is(err, ErrProfileUnlockStateUnavailable) {
		t.Fatal("nested overflow succeeded")
	}
	if _, err = tx.Exec(t.Context(), `INSERT INTO profile_unlock_revocations VALUES('profile','outer-still-usable',0)`); err != nil {
		t.Fatal("savepoint failure poisoned outer transaction")
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func (f *unlockPGFixture) waitBlocked(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		var blocked bool
		err := f.admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, f.application).Scan(&blocked)
		if err != nil {
			t.Fatal("observe own blocked statement")
		}
		if blocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("own operation did not block")
		case <-time.After(time.Millisecond):
		}
	}
}
func TestProfileUnlockRevocationBlockedCancellationAtomic(t *testing.T) {
	for _, block := range []string{"generation before update", "grant after generation"} {
		t.Run(block, func(t *testing.T) {
			f := unlockFixture(t)
			f.exec(t, `INSERT INTO profile_unlock_revocations VALUES('profile','target',0)`)
			tx, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			statement := `SELECT generation FROM profile_unlock_revocations WHERE scope='profile' AND scope_key='target' FOR UPDATE`
			if block == "grant after generation" {
				statement = `SELECT token_hash FROM profile_unlocks FOR UPDATE`
			}
			if _, err = tx.Exec(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- f.ds.ProfileUnlocks().DeleteByProfile(ctx, "target") }()
			f.waitBlocked(t)
			cancel()
			if err = <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled revoke did not preserve cancellation: %v", err)
			}
			if err = tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			if f.count(t) != 1 || f.generation(t, "profile", "target") != 0 {
				t.Fatal("cancelled revoke partially changed storage")
			}
		})
	}
}
func TestProfileUnlockRevocationSerializesConcurrentEmptyRevokes(t *testing.T) {
	f := unlockFixture(t)
	f.exec(t, `DELETE FROM profile_unlocks`)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if err = (&pgProfileUnlockRepo{pool: tx}).DeleteByProfile(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target") }()
	f.waitBlocked(t)
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if f.generation(t, "profile", "target") != 2 || f.count(t) != 0 {
		t.Fatal("concurrent empty revoke generations lost")
	}
}
func TestProfileUnlockStateLegacyRebindAndCreateRemainExplicit(t *testing.T) {
	for _, operation := range []string{"create upsert", "rebind"} {
		t.Run(operation, func(t *testing.T) {
			f := unlockFixture(t)
			state := f.read(t)
			if operation == "create upsert" {
				f.create(t)
			} else {
				if err := f.ds.ProfileUnlocks().RebindSession(t.Context(), f.grant.SessionHash, strings.Repeat("0", 64), f.session.ExpiresAt); err != nil {
					t.Fatal(err)
				}
			}
			if current, err := f.ds.CheckProfileUnlockState(state); current || !errors.Is(err, ErrProfileUnlockStateDenied) {
				t.Fatal("legacy writer did not retire old observation")
			}
		})
	}
}

// Cookie authentication normalizes the original role from the current actor.
// The stored session role is a cache; its row version still fences later edits.
func TestProfileUnlockStateNormalizedActorRole(t *testing.T) {
	for _, mode := range []string{"promoted current actor", "stale cached master"} {
		t.Run(mode, func(t *testing.T) {
			f := unlockFixture(t)
			if mode == "promoted current actor" {
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.session.IsMaster = true
				state := f.read(t)
				if current, err := f.ds.CheckProfileUnlockState(state); err != nil || !current {
					t.Fatal("normalized current master denied by cached session role")
				}
			} else {
				f.exec(t, `UPDATE sessions SET is_master=true;UPDATE users SET account_id='other' WHERE id='target'`)
				if state, err := f.ds.ReadProfileUnlockState(f.request(t.Context()), "target", f.token); state != nil || !errors.Is(err, ErrProfileUnlockStateDenied) {
					t.Fatal("cached session role elevated actor")
				}
			}
		})
	}
}

func TestProfileUnlockStateFullDataStoreMigrations(t *testing.T) {
	f := unlockFixture(t)
	// Only this case's five synthetic bootstrap tables are removed, leaving its
	// private schema empty for the actual DataStore.New startup migration path.
	f.exec(t, `DROP TABLE profile_unlock_revocations,profile_unlocks,users,sessions,accounts`)
	target, err := url.Parse(f.raw)
	if err != nil {
		t.Fatal("parse own migration target")
	}
	query := target.Query()
	query.Set("search_path", pgx.Identifier{f.application}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+f.application)
	target.RawQuery = query.Encode()
	ds, err := New(t.Context(), target.String())
	if err != nil {
		t.Fatalf("actual DataStore.New failed: %v", err)
	}
	defer ds.Close()
	var role string
	if err = ds.pool.QueryRow(t.Context(), `SELECT current_user`).Scan(&role); err != nil || role != f.application {
		t.Fatal("migration escaped own fixture role")
	}
	if err = ds.ProfileUnlocks().Create(t.Context(), &f.grant); err != nil {
		t.Fatal(err)
	}
	if err = ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target"); err != nil {
		t.Fatal(err)
	}
	if f.generation(t, "profile", "target") != 1 || f.count(t) != 0 {
		t.Fatal("migration not wired to actual repository")
	}
	restarted, err := New(t.Context(), target.String())
	if err != nil {
		t.Fatalf("repeated DataStore.New failed: %v", err)
	}
	defer restarted.Close()
	if f.generation(t, "profile", "target") != 1 {
		t.Fatal("startup reset durable generation")
	}
}

func TestProfileUnlockRevocationUnknownCommitMayHaveDeleted(t *testing.T) {
	f := unlockFixture(t)
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
		t.Fatal("connect own acknowledgement fixture")
	}
	defer pool.Close()
	repo := &pgProfileUnlockRepo{pool: pool}
	if err = repo.DeleteByProfile(t.Context(), "target"); !errors.Is(err, ErrProfileUnlockRevokeUnknown) || !dropped.Load() {
		t.Fatalf("missing actual unknown commit: %v", err)
	}
	if f.count(t) != 0 || f.generation(t, "profile", "target") != 1 {
		t.Fatal("lost ACK did not follow actual atomic revocation")
	}
	// No retry: an unknown outcome is not evidence that no write occurred.
}
