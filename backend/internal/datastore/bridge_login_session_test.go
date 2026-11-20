//go:build linux

package datastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/settingscommit"
	"novastream/models"
)

type bridgeSessionFixture struct {
	ds          *DataStore
	admin       *pgxpool.Pool
	manager     *config.Manager
	settings    config.Settings
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	issuer      *BridgeLoginSessionIssuer
	schema      string
}

// All schemas run the actual current migrations. Optional test transport and
// tracing observe the real PG connection, never substitute credential evidence.
func bridgeSessionPG(t *testing.T, pin string, enforce, allow bool, tracer pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error)) *bridgeSessionFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic Unix PG fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("own synthetic Unix PG only")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	f := &bridgeSessionFixture{admin: admin, schema: fmt.Sprintf("synthetic_bmint_%d", time.Now().UnixNano())}
	ownedRole, ownedSchema := false, false
	var objects *settingscommit.LockObjects
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		if objects != nil {
			_ = objects.Close()
		}
		if f.ds != nil {
			f.ds.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if ownedSchema {
			if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{f.schema}.Sanitize()+" CASCADE"); err != nil {
				t.Error("remove own schema", err)
			}
		}
		if ownedRole {
			if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{f.schema}.Sanitize()); err != nil {
				t.Error("remove own role", err)
			}
		}
		admin.Close()
	})
	if _, err = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{f.schema}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	ownedRole = true
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{f.schema}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	ownedSchema = true
	query := u.Query()
	query.Set("search_path", pgx.Identifier{f.schema}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+f.schema)
	query.Set("application_name", f.schema)
	u.RawQuery = query.Encode()
	f.ds, err = New(t.Context(), u.String())
	if err != nil {
		t.Fatal("actual migrated original store", err)
	}
	if tracer != nil || dial != nil {
		cfg := f.ds.pool.Config().Copy()
		cfg.MinConns = 0
		cfg.ConnConfig.Tracer = tracer
		if dial != nil {
			cfg.ConnConfig.DialFunc = dial
		}
		configureQueryLifecycle(cfg.ConnConfig)
		pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
		if e != nil {
			t.Fatal(e)
		}
		f.ds.pool.Close()
		f.ds.pool = pool
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(parentTestPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Actor',$1),('other','Synthetic Other',$1)`, string(hash))
	pinHash := ""
	if pin != "" {
		hash, err = bcrypt.GenerateFromPassword([]byte(pin), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		pinHash = string(hash)
	}
	f.exec(t, `INSERT INTO users(id,account_id,name,pin_hash,pin_length)VALUES('target','actor','Synthetic Target',$1,$2)`, pinHash, len(pin))
	f.exec(t, `INSERT INTO user_settings_versions(user_id,generation)VALUES('target',0) ON CONFLICT(user_id) DO NOTHING`)
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	f.settings = config.DefaultSettings()
	f.settings.Database.URL = u.String()
	f.settings.Security.EnforceProfilePins = enforce
	f.settings.JellyfinBridge.Enabled = true
	f.settings.JellyfinBridge.AllowPinLogin = allow
	if err = f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	objects, err = settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.ds.pool, objects)
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
	f.issuer, err = NewBridgeLoginSessionIssuer(f.ds, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *bridgeSessionFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.ds.pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal("own synthetic SQL", err)
	}
}
func (f *bridgeSessionFixture) count(t *testing.T) int {
	t.Helper()
	var n int
	if err := f.ds.pool.QueryRow(t.Context(), "SELECT count(*) FROM sessions").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func (f *bridgeSessionFixture) evidence(t *testing.T, request *http.Request, kind bridgeCredentialKind) (*BridgeLoginCredentialIntent, *BridgeLoginCredentialProof, *config.BridgeLoginPolicyObservation) {
	t.Helper()
	intent, err := f.ds.PrepareBridgeLoginCredentials(request, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	var proof *BridgeLoginCredentialProof
	switch kind {
	case bridgeCredentialAccount:
		proof, err = f.ds.VerifyBridgeAccountPassword(request, intent, parentTestPassword)
	case bridgeCredentialCombined:
		proof, err = f.ds.VerifyBridgePasswordWithPIN(request, intent, parentTestPassword+"2580")
	case bridgeCredentialPIN:
		proof, err = f.ds.VerifyBridgeProfilePIN(request, intent, "2580")
	}
	if err != nil {
		t.Fatal("actual verifier evidence", err)
	}
	observation, err := f.security.CaptureBridgePolicy(request.Context())
	if err != nil {
		t.Fatal(err)
	}
	return intent, proof, observation
}
func bridgeSessionAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("actual owned operation failed to join")
	}
}
func bridgeSessionObserveBlocked(t *testing.T, f *bridgeSessionFixture, pid uint32) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		var n int
		if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE pid=$1 AND relation=$2::regclass AND NOT granted`, int32(pid), pgx.Identifier{f.schema, "accounts"}.Sanitize()).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("actual original accounts query never blocked")
}

func TestBridgeSessionActualFivePolicyRowsAndDistinctKinds(t *testing.T) {
	rows := []struct {
		name           string
		pin            string
		enforce, allow bool
	}{{"no-pin", "", false, false}, {"optional-password", "2580", false, false}, {"optional-PIN", "2580", false, true}, {"required-password", "2580", true, false}, {"required-PIN", "2580", true, true}}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			for _, kind := range []bridgeCredentialKind{bridgeCredentialAccount, bridgeCredentialCombined, bridgeCredentialPIN} {
				t.Run(fmt.Sprint(kind), func(t *testing.T) {
					f := bridgeSessionPG(t, row.pin, row.enforce, row.allow, nil, nil)
					request := bridgeCredentialRequest(t.Context())
					if row.pin == "" && kind != bridgeCredentialAccount {
						intent, e := f.ds.PrepareBridgeLoginCredentials(request, "actor", "target", "synthetic-device")
						if e != nil {
							t.Fatal(e)
						}
						t.Cleanup(intent.Close)
						var p *BridgeLoginCredentialProof
						if kind == bridgeCredentialCombined {
							p, e = f.ds.VerifyBridgePasswordWithPIN(request, intent, parentTestPassword+"2580")
						} else {
							p, e = f.ds.VerifyBridgeProfilePIN(request, intent, "2580")
						}
						if p != nil || e == nil || f.count(t) != 0 {
							t.Fatal("missing PIN acquired evidence or wrote session")
						}
						return
					}
					_, proof, observation := f.evidence(t, request, kind)
					receipt, err := f.issuer.Commit(request, proof, observation)
					wanted := kind == bridgeCredentialAccount
					if row.pin != "" {
						wanted = !row.enforce && kind == bridgeCredentialAccount || row.enforce && kind == bridgeCredentialCombined || row.allow && kind == bridgeCredentialPIN
					}
					if !wanted {
						if receipt != nil || err == nil || f.count(t) != 0 {
							t.Fatal("disallowed evidence kind minted")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					defer receipt.Close()
					if _, err := f.issuer.Commit(request, proof, observation); err == nil {
						t.Fatal("verified proof claimed twice")
					}
					session, err := f.issuer.Release(request, receipt)
					if err != nil {
						t.Fatal(err)
					}
					token, err := base64.URLEncoding.DecodeString(session.Token)
					if err != nil || len(token) != 32 || base64.URLEncoding.EncodeToString(token) != session.Token || session.Scope != models.SessionScopeJellyfin || session.ScopeResource != "target" || session.AccountID != "actor" || session.ClientID != "synthetic-device" || session.ExpiresAt.Sub(session.CreatedAt) != bridgeSessionTTL || !session.ExpiresAt.After(proof.ExpiresAt()) {
						t.Fatal("wrong session identity, token encoding or existing server TTL")
					}
					if _, err := f.issuer.Release(request, receipt); err == nil {
						t.Fatal("receipt released twice")
					}
				})
			}
		})
	}
}

func TestBridgeSessionDisabledBridgeCannotMint(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	changed := f.settings
	changed.JellyfinBridge.Enabled = false
	if err := f.manager.Save(changed); err != nil {
		t.Fatal(err)
	}
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	if receipt, err := f.issuer.Commit(r, p, o); receipt != nil || err == nil || f.count(t) != 0 {
		t.Fatal("disabled Bridge minted")
	}
}

func TestBridgeSessionIdleProofCloseCleansWithoutCallerReceiptClose(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	bridgeSessionAwait(t, done)
	bridgeSessionAwait(t, receipt.state.done)
	if f.ds.pool.Stat().AcquiredConns() != 0 {
		t.Fatal("original proof refund overtook real connection release")
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal("idle receipt left original native hold", err)
	}
	_ = guard.Close()
	if _, err := f.issuer.Release(r, receipt); err == nil {
		t.Fatal("closed original proof released token")
	}
}

func TestBridgeSessionOpaqueCopiesAndFreshRequestDoNotConsumeReceipt(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	copied := *receipt
	if _, err := f.issuer.Release(r, &copied); err == nil {
		t.Fatal("copied receipt accepted")
	}
	issuerCopy := *f.issuer
	if _, err := issuerCopy.Release(r, receipt); err == nil {
		t.Fatal("copied issuer accepted")
	}
	if _, err := f.issuer.Release(r.Clone(r.Context()), receipt); err == nil {
		t.Fatal("replacement request accepted")
	}
	original := r.Header.Get("Authorization")
	r.Header.Set("Authorization", `MediaBrowser DeviceId="foreign"`)
	if _, err := f.issuer.Release(r, receipt); err == nil {
		t.Fatal("changed original carrier accepted")
	}
	r.Header.Set("Authorization", original)
	for _, value := range []any{receipt, *receipt, f.issuer, *f.issuer} {
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != "null" || !strings.HasSuffix(fmt.Sprintf("%#v", value), "(opaque)") {
			t.Fatal("opaque state disclosed token/authority")
		}
	}
	if json.Unmarshal([]byte(`{}`), &copied) == nil {
		t.Fatal("imported receipt accepted")
	}
	if _, err := f.issuer.Release(r, receipt); err != nil {
		t.Fatal("foreign caller retired intact original receipt", err)
	}
}

func TestBridgeSessionActualCurrentRowsNeverAdoptNewAuthority(t *testing.T) {
	for _, kind := range []string{"account-version", "PIN", "epoch", "settings", "foreign-profile", "child-household", "missing-household", "overflow-household", "missing-settings-version"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionPG(t, "2580", true, false, nil, nil)
			r := bridgeCredentialRequest(t.Context())
			_, p, o := f.evidence(t, r, bridgeCredentialCombined)
			switch kind {
			case "account-version":
				f.exec(t, `UPDATE accounts SET max_streams=max_streams WHERE id='actor'`)
			case "PIN":
				f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
			case "epoch":
				f.exec(t, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','target',1) ON CONFLICT(scope,scope_key) DO UPDATE SET generation=profile_unlock_revocations.generation+1`)
			case "settings":
				f.exec(t, `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='target'`)
			case "foreign-profile":
				f.exec(t, `UPDATE users SET account_id='other' WHERE id='target'`)
			case "child-household":
				f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic Child',true)`)
			case "missing-household":
				f.exec(t, `DELETE FROM accounts WHERE id='actor'`)
			case "missing-settings-version":
				f.exec(t, `DELETE FROM user_settings_versions WHERE user_id='target'`)
			case "overflow-household":
				f.exec(t, `INSERT INTO users(id,account_id,name) SELECT 'member-'||n,'actor','Synthetic' FROM generate_series(1,257)n`)
			}
			if receipt, err := f.issuer.Commit(r, p, o); err == nil || receipt != nil || f.count(t) != 0 {
				t.Fatal("new/unknown PG authority adopted")
			}
		})
	}
}

func TestBridgeSessionKnownACKCannotPublishAfterOriginalDeparture(t *testing.T) {
	for _, mode := range []string{"cancel", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			defer unblock()
			var armed atomic.Bool
			trace := &accountRevokeTracer{end: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
				if data.Err == nil && data.CommandTag.String() == "COMMIT" && armed.CompareAndSwap(true, false) {
					close(entered)
					<-release
				}
			}}
			f := bridgeSessionPG(t, "", false, false, trace, nil)
			var ctx context.Context
			var cancel context.CancelFunc
			if mode == "expiry" {
				ctx, cancel = context.WithTimeout(t.Context(), 1500*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(t.Context())
			}
			defer cancel()
			r := bridgeCredentialRequest(ctx)
			_, p, o := f.evidence(t, r, bridgeCredentialAccount)
			armed.Store(true)
			finished := make(chan struct{})
			var receipt *BridgeLoginSessionReceipt
			var err error
			go func() { receipt, err = f.issuer.Commit(r, p, o); close(finished) }()
			bridgeSessionAwait(t, entered)
			if f.count(t) != 1 {
				t.Fatal("actual known commit was not observed before cancellation")
			}
			if mode == "expiry" {
				bridgeSessionAwait(t, ctx.Done())
			} else {
				cancel()
			}
			unblock()
			bridgeSessionAwait(t, finished)
			if err == nil || receipt != nil || f.count(t) != 1 {
				t.Fatal("known late ACK published or pretended rollback")
			}
		})
	}
}

func TestBridgeSessionOriginalExpiryClosesIdleReceiptWithoutTTLChange(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancel()
	r := bridgeCredentialRequest(ctx)
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	bridgeSessionAwait(t, ctx.Done())
	bridgeSessionAwait(t, receipt.state.done)
	if _, err := f.issuer.Release(r, receipt); err == nil || f.count(t) != 1 {
		t.Fatal("expired proof released token or replaced committed session TTL")
	}
	var lifetime float64
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT EXTRACT(EPOCH FROM expires_at-created_at) FROM sessions`).Scan(&lifetime); err != nil || lifetime != bridgeSessionTTL.Seconds() {
		t.Fatal("new session was capped to proof lifetime")
	}
}

func TestBridgeSessionReleaseOwnsNewActualSharedPGTransaction(t *testing.T) {
	ready, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	defer unblock()
	var armed atomic.Bool
	trace := &accountRevokeTracer{start: func(_ context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) {
		if strings.Contains(data.SQL, "SELECT id FROM accounts") && armed.CompareAndSwap(true, false) {
			close(ready)
			<-release
		}
	}}
	f := bridgeSessionPG(t, "", false, false, trace, nil)
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { unblock(); receipt.Close() }()
	observer, err := f.admin.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Release()
	check := func(want bool) {
		t.Helper()
		var got bool
		if err := observer.QueryRow(t.Context(), `SELECT pg_try_advisory_lock($1::integer,$2::integer)`, settingscommit.Namespace, settingscommit.Resource).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got {
			var unlocked bool
			if err := observer.QueryRow(t.Context(), `SELECT pg_advisory_unlock($1::integer,$2::integer)`, settingscommit.Namespace, settingscommit.Resource).Scan(&unlocked); err != nil || !unlocked {
				t.Fatal("release own EX observer")
			}
		}
		if got != want {
			t.Fatal("actual PG-only EX witness differs; native hold is not this witness")
		}
	}
	check(true)
	armed.Store(true)
	done := make(chan struct{})
	var session models.Session
	go func() { session, err = f.issuer.Release(r, receipt); close(done) }()
	bridgeSessionAwait(t, ready)
	check(false)
	unblock()
	bridgeSessionAwait(t, done)
	if err != nil || session.Token == "" {
		t.Fatal(err)
	}
	check(true)
}

func TestBridgeSessionProofCloseJoinsBlockedReleaseAndActualQueries(t *testing.T) {
	var armed atomic.Bool
	var pid atomic.Uint32
	query := make(chan struct{}, 1)
	trace := &accountRevokeTracer{start: func(_ context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) {
		if strings.Contains(data.SQL, "SELECT id FROM accounts") && armed.CompareAndSwap(true, false) {
			pid.Store(c.PgConn().PID())
			query <- struct{}{}
		}
	}}
	f := bridgeSessionPG(t, "", false, false, trace, nil)
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	holder, err := f.admin.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(holder, t.Context())
	if _, err = holder.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	returned := make(chan struct{})
	var releaseErr error
	go func() { _, releaseErr = f.issuer.Release(r, receipt); close(returned) }()
	bridgeSessionAwait(t, query)
	bridgeSessionObserveBlocked(t, f, pid.Load())
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	bridgeSessionAwait(t, closed)
	bridgeSessionAwait(t, returned)
	bridgeSessionAwait(t, receipt.state.done)
	if releaseErr == nil || f.ds.pool.Stat().AcquiredConns() != 0 {
		t.Fatal("proof Close overtook real Release cleanup")
	}
	var n int
	if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE pid=$1 AND NOT granted`, int32(pid.Load())).Scan(&n); err != nil || n != 0 {
		t.Fatal("actual pending query survived original quota refund")
	}
}

func TestBridgeSessionIntentCurrentCheckCannotMintAndRetirementCleansIdleReceipt(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	r := bridgeCredentialRequest(t.Context())
	intent, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	if err := intent.Check(t.Context()); err != nil {
		t.Fatal("current-only intent check changed original semantics", err)
	}
	if _, err := f.issuer.Commit(r, p, o); err == nil {
		t.Fatal("current observation granted second mint")
	}
	f.exec(t, `UPDATE accounts SET max_streams=max_streams WHERE id='actor'`)
	if err := intent.Check(t.Context()); err == nil {
		t.Fatal("changed original rows remained current")
	}
	bridgeSessionAwait(t, receipt.state.done)
	if _, err := f.issuer.Release(r, receipt); err == nil || f.ds.pool.Stat().AcquiredConns() != 0 {
		t.Fatal("retired observation released or leaked original resources")
	}
}

func TestBridgeSessionBothRevokeOrdersAndExceptSemantics(t *testing.T) {
	for _, mode := range []string{"revoke-before-mint", "revoke-after-mint", "except-kept", "except-other"} {
		t.Run(mode, func(t *testing.T) {
			f := bridgeSessionPG(t, "", false, false, nil, nil)
			r := bridgeCredentialRequest(t.Context())
			_, p, o := f.evidence(t, r, bridgeCredentialAccount)
			if mode == "revoke-before-mint" {
				if n, err := f.ds.RevokeAccountSessions(t.Context(), "actor"); err != nil || n != 0 {
					t.Fatal(err)
				}
			}
			receipt, err := f.issuer.Commit(r, p, o)
			if err != nil {
				t.Fatal("revoke is not password revocation", err)
			}
			defer receipt.Close()
			switch mode {
			case "revoke-after-mint":
				if n, err := f.ds.RevokeAccountSessions(t.Context(), "actor"); err != nil || n != 1 {
					t.Fatal(err)
				}
			case "except-kept":
				if n, err := f.ds.RevokeAccountSessionsExcept(t.Context(), "actor", receipt.state.session.Token); err != nil || n != 0 {
					t.Fatal(err)
				}
			case "except-other":
				if n, err := f.ds.RevokeAccountSessionsExcept(t.Context(), "actor", "synthetic-other"); err != nil || n != 1 {
					t.Fatal(err)
				}
			}
			session, err := f.issuer.Release(r, receipt)
			want := mode == "revoke-before-mint" || mode == "except-kept"
			if (err == nil) != want || (session.Token != "") != want {
				t.Fatal("wrong account revoke/except linearization", err)
			}
		})
	}
}

type bridgeSessionDropACK struct {
	net.Conn
	enabled *atomic.Bool
	actual  *parentDropCommit
}

func (c *bridgeSessionDropACK) Write(p []byte) (int, error) {
	if c.enabled.Load() {
		return c.actual.Write(p)
	}
	return c.Conn.Write(p)
}
func (c *bridgeSessionDropACK) Read(p []byte) (int, error) { return c.actual.Read(p) }
func TestBridgeSessionLostActualACKNeverReturnsOrRetriesToken(t *testing.T) {
	for _, phase := range []string{"mint", "release"} {
		t.Run(phase, func(t *testing.T) {
			var enabled, dropped atomic.Bool
			dial := func(ctx context.Context, network, address string) (net.Conn, error) {
				c, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &bridgeSessionDropACK{Conn: c, enabled: &enabled, actual: &parentDropCommit{Conn: c, dropped: &dropped}}, nil
			}
			f := bridgeSessionPG(t, "", false, false, nil, dial)
			r := bridgeCredentialRequest(t.Context())
			_, p, o := f.evidence(t, r, bridgeCredentialAccount)
			if phase == "mint" {
				enabled.Store(true)
			}
			receipt, err := f.issuer.Commit(r, p, o)
			if phase == "mint" {
				if !errors.Is(err, ErrBridgeSessionUnknown) || receipt != nil || !dropped.Load() || f.count(t) != 1 {
					t.Fatal("lost real mint ACK claimed rollback, token or success", err)
				}
				if _, err := f.issuer.Commit(r, p, o); err == nil {
					t.Fatal("unknown mint was retried")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer receipt.Close()
			enabled.Store(true)
			session, err := f.issuer.Release(r, receipt)
			if !errors.Is(err, ErrBridgeSessionUnknown) || session.Token != "" || !dropped.Load() {
				t.Fatal("lost real Release ACK returned token", err)
			}
			receipt.Close()
			if _, err := f.issuer.Release(r, receipt); err == nil {
				t.Fatal("unknown release was retried")
			}
		})
	}
}

func TestBridgeSessionSettingsSaveBusyDoesNotAdoptChangedPolicy(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	changed := f.settings
	changed.JellyfinBridge.Enabled = false
	changed.JellyfinBridge.AllowPinLogin = true
	changed.Security.EnforceProfilePins = true
	if err := f.manager.Save(changed); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("original native hold did not exclude writer", err)
	}
	if _, err := f.issuer.Release(r, receipt); err != nil {
		t.Fatal("foreign Busy attempt changed already-bound policy data", err)
	}
	receipt.Close()
	if err := f.manager.Save(changed); err != nil {
		t.Fatal("actual cleanup did not refund original hold", err)
	}
}

type bridgeSessionTraceSQLKey struct{}
type bridgeSessionTransactionTrace struct {
	start func(*pgx.Conn, string)
	end   func(*pgx.Conn, string, pgx.TraceQueryEndData)
}

func (tr *bridgeSessionTransactionTrace) TraceQueryStart(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if tr.start != nil {
		tr.start(c, data.SQL)
	}
	return context.WithValue(ctx, bridgeSessionTraceSQLKey{}, data.SQL)
}
func (tr *bridgeSessionTransactionTrace) TraceQueryEnd(ctx context.Context, c *pgx.Conn, data pgx.TraceQueryEndData) {
	if tr.end != nil {
		sql, _ := ctx.Value(bridgeSessionTraceSQLKey{}).(string)
		tr.end(c, sql, data)
	}
}

func TestBridgeSessionRealMintAndRevokeAccountLockOrders(t *testing.T) {
	for _, order := range []string{"mint-first", "revoke-first"} {
		t.Run(order, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var armed atomic.Bool
			var ownerPID, waiterPID atomic.Uint32
			waiting := make(chan struct{}, 1)
			trace := &bridgeSessionTransactionTrace{end: func(c *pgx.Conn, sql string, data pgx.TraceQueryEndData) {
				target := order == "mint-first" && strings.HasPrefix(sql, "INSERT INTO sessions(") || order == "revoke-first" && strings.HasPrefix(sql, "DELETE FROM sessions WHERE account_id=")
				if target && data.Err == nil && armed.CompareAndSwap(true, false) {
					ownerPID.Store(c.PgConn().PID())
					close(entered)
					<-release
				}
			}, start: func(c *pgx.Conn, sql string) {
				if strings.Contains(sql, "SELECT id FROM accounts") && ownerPID.Load() != 0 && c.PgConn().PID() != ownerPID.Load() {
					waiterPID.Store(c.PgConn().PID())
					select {
					case waiting <- struct{}{}:
					default:
					}
				}
			}}
			f := bridgeSessionPG(t, "", false, false, trace, nil)
			r := bridgeCredentialRequest(t.Context())
			_, p, o := f.evidence(t, r, bridgeCredentialAccount)
			armed.Store(true)
			mintDone, revokeDone := make(chan struct{}), make(chan struct{})
			var receipt *BridgeLoginSessionReceipt
			var mintErr, revokeErr error
			var count int64
			mint := func() { receipt, mintErr = f.issuer.Commit(r, p, o); close(mintDone) }
			revoke := func() { count, revokeErr = f.ds.RevokeAccountSessions(t.Context(), "actor"); close(revokeDone) }
			if order == "mint-first" {
				go mint()
				bridgeSessionAwait(t, entered)
				go revoke()
			} else {
				go revoke()
				bridgeSessionAwait(t, entered)
				go mint()
			}
			bridgeSessionAwait(t, waiting)
			until := time.Now().Add(5 * time.Second)
			blocked := false
			for time.Now().Before(until) {
				if err := f.admin.QueryRow(t.Context(), `SELECT $1::integer=ANY(pg_blocking_pids($2::integer))`, int32(ownerPID.Load()), int32(waiterPID.Load())).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !blocked {
				t.Fatal("actual account serializer did not block the later transaction")
			}
			unblock()
			bridgeSessionAwait(t, mintDone)
			bridgeSessionAwait(t, revokeDone)
			if mintErr != nil || revokeErr != nil || receipt == nil {
				t.Fatal("actual serialized operation failed", mintErr, revokeErr)
			}
			defer receipt.Close()
			session, err := f.issuer.Release(r, receipt)
			if order == "mint-first" {
				if count != 1 || err == nil || session.Token != "" {
					t.Fatal("later actual revoke failed to delete earlier mint")
				}
			} else {
				if count != 0 || err != nil || session.Token == "" {
					t.Fatal("later genuine password login was incorrectly revoked", err)
				}
			}
		})
	}
}

func TestBridgeSessionOriginalRefundWaitsActualClosedSocketCleanup(t *testing.T) {
	var first atomic.Bool
	var pointerMu sync.Mutex
	var actual *queryLifecycleGatedConn
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			gate := &queryLifecycleGatedConn{Conn: c, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			pointerMu.Lock()
			actual = gate
			pointerMu.Unlock()
			return gate, nil
		}
		return c, nil
	}
	f := bridgeSessionPG(t, "", false, false, nil, dial)
	pointerMu.Lock()
	gate := actual
	pointerMu.Unlock()
	if gate == nil {
		t.Fatal("actual primary transport was not captured")
	}
	defer gate.release()
	// Preparation/verification/receipt all use a healthy real original socket.
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { gate.release(); receipt.Close() }()
	physical := receipt.state.progress.connection.Conn().PgConn()
	gate.armed.Store(true)
	finished := make(chan struct{})
	go func() { _, _ = f.issuer.Release(r, receipt); close(finished) }()
	bridgeSessionAwait(t, gate.closeEntered)
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("proof refunded before actual main socket Close joined")
	default:
	}
	select {
	case <-finished:
		t.Fatal("Release returned before actual CleanupDone")
	default:
	}
	select {
	case <-physical.CleanupDone():
		t.Fatal("actual main cleanup was not retained by gate")
	default:
	}
	if f.ds.pool.Stat().AcquiredConns() != 1 {
		t.Fatal("original retained connection refunded early")
	}
	gate.release()
	bridgeSessionAwait(t, finished)
	bridgeSessionAwait(t, closed)
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, receipt.state.done)
	// Closed-connection Release schedules puddle destruction asynchronously.
	// Keep the actual zero-charge requirement after all physical cleanup joins.
	refundCtx, cancelRefund := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRefund()
	refundTick := time.NewTicker(time.Millisecond)
	defer refundTick.Stop()
	for f.ds.pool.Stat().AcquiredConns() != 0 {
		select {
		case <-refundCtx.Done():
			t.Fatal("actual cleanup did not refund connection", refundCtx.Err())
		case <-refundTick.C:
		}
	}
}

func TestBridgeSessionActualOwnedMasterKeepsServerTTLAfterAccountBoundary(t *testing.T) {
	f := bridgeSessionPG(t, "", false, false, nil, nil)
	f.exec(t, `UPDATE accounts SET is_master=true,expires_at=clock_timestamp()+interval '5 minutes' WHERE id='actor'`)
	var accountExpiry time.Time
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT expires_at FROM accounts WHERE id='actor'`).Scan(&accountExpiry); err != nil {
		t.Fatal(err)
	}
	r := bridgeCredentialRequest(t.Context())
	_, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	session, err := f.issuer.Release(r, receipt)
	if err != nil || !session.IsMaster || session.AccountID != "actor" || session.Scope != models.SessionScopeJellyfin || session.ScopeResource != "target" || !session.ExpiresAt.After(accountExpiry) || session.ExpiresAt.Sub(session.CreatedAt) != bridgeSessionTTL {
		t.Fatal("owned master changed scope or old account boundary capped new server TTL", err)
	}
}

func TestBridgeSessionNoPINUsesAccountEvidenceAcrossAllFlags(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		for _, allow := range []bool{false, true} {
			t.Run(fmt.Sprintf("%t-%t", enforce, allow), func(t *testing.T) {
				f := bridgeSessionPG(t, "", enforce, allow, nil, nil)
				r := bridgeCredentialRequest(t.Context())
				_, p, o := f.evidence(t, r, bridgeCredentialAccount)
				receipt, err := f.issuer.Commit(r, p, o)
				if err != nil {
					t.Fatal("configured flag invented a missing PIN requirement", err)
				}
				defer receipt.Close()
				if session, err := f.issuer.Release(r, receipt); err != nil || session.Scope != models.SessionScopeJellyfin {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestBridgeSessionIntentBusyDuringReleaseThenActualDriftRetiresIdleReceipt(t *testing.T) {
	var armed atomic.Bool
	var pid atomic.Uint32
	waiting := make(chan struct{}, 1)
	trace := &accountRevokeTracer{start: func(_ context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) {
		if strings.Contains(data.SQL, "SELECT id FROM accounts") && armed.CompareAndSwap(true, false) {
			pid.Store(c.PgConn().PID())
			waiting <- struct{}{}
		}
	}}
	f := bridgeSessionPG(t, "", false, false, trace, nil)
	r := bridgeCredentialRequest(t.Context())
	intent, p, o := f.evidence(t, r, bridgeCredentialAccount)
	receipt, err := f.issuer.Commit(r, p, o)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	holder, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(holder, t.Context())
	var id string
	if err := holder.QueryRow(t.Context(), `SELECT id FROM accounts WHERE id='actor' FOR UPDATE`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	holderPID := holder.Conn().PgConn().PID()
	armed.Store(true)
	finished := make(chan struct{})
	var releaseErr error
	go func() { _, releaseErr = f.issuer.Release(r, receipt); close(finished) }()
	bridgeSessionAwait(t, waiting)
	until := time.Now().Add(5 * time.Second)
	blocked := false
	for time.Now().Before(until) {
		if err := f.admin.QueryRow(t.Context(), `SELECT $1::integer=ANY(pg_blocking_pids($2::integer))`, int32(holderPID), int32(pid.Load())).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !blocked {
		t.Fatal("actual Release did not wait on original account row")
	}
	if err := intent.Check(t.Context()); !errors.Is(err, ErrBridgeCredentialBusy) || p.Context().Err() != nil {
		t.Fatal("third operation bypassed original cap or retired intact proof", err)
	}
	if err := holder.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	bridgeSessionAwait(t, finished)
	if releaseErr != nil {
		t.Fatal("Busy observer changed the genuine original release", releaseErr)
	}
	// Once actual Release IO ends, the unchanged two-operation cap admits the
	// current-only observer again. Real fence drift must retire this idle receipt.
	f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('other-profile','other','Synthetic Other Profile')`)
	if err := intent.Check(t.Context()); err == nil {
		t.Fatal("current-only observation failed to retire actual original fence drift")
	}
	bridgeSessionAwait(t, receipt.state.done)
	if f.ds.pool.Stat().AcquiredConns() != 0 {
		t.Fatal("actual retirement did not refund original connection")
	}
}

func TestBridgeSessionOriginalNativeHoldDepartureCancelsOwnedQueries(t *testing.T) {
	for _, phase := range []string{"mint", "release"} {
		t.Run(phase, func(t *testing.T) {
			var armed atomic.Bool
			var pid atomic.Uint32
			waiting := make(chan struct{}, 1)
			trace := &accountRevokeTracer{start: func(_ context.Context, c *pgx.Conn, data pgx.TraceQueryStartData) {
				if strings.Contains(data.SQL, "SELECT id FROM accounts") && armed.CompareAndSwap(true, false) {
					pid.Store(c.PgConn().PID())
					waiting <- struct{}{}
				}
			}}
			f := bridgeSessionPG(t, "", false, false, trace, nil)
			request := bridgeCredentialRequest(t.Context())
			_, proof, observation := f.evidence(t, request, bridgeCredentialAccount)
			var receipt *BridgeLoginSessionReceipt
			var err error
			if phase == "release" {
				receipt, err = f.issuer.Commit(request, proof, observation)
				if err != nil {
					t.Fatal(err)
				}
				defer receipt.Close()
			}
			holder, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackPinTx(holder, t.Context())
			if _, err = holder.Exec(t.Context(), `LOCK TABLE `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			armed.Store(true)
			finished := make(chan struct{})
			var operationErr error
			var delivered models.Session
			var returned *BridgeLoginSessionReceipt
			go func() {
				if phase == "mint" {
					returned, operationErr = f.issuer.Commit(request, proof, observation)
				} else {
					delivered, operationErr = f.issuer.Release(request, receipt)
				}
				close(finished)
			}()
			bridgeSessionAwait(t, waiting)
			bridgeSessionObserveBlocked(t, f, pid.Load())
			if request.Context().Err() != nil || proof.Context().Err() != nil || !proof.ExpiresAt().After(time.Now().Add(10*time.Second)) {
				t.Fatal("original request/proof expired before genuine native cancellation witness")
			}
			started := time.Now()
			if err := f.coordinator.Close(); err != nil {
				t.Fatal("close original native coordinator", err)
			}
			bridgeSessionAwait(t, finished)
			if operationErr == nil || returned != nil || delivered.Token != "" || request.Context().Err() != nil || time.Since(started) >= 8*time.Second {
				t.Fatal("original native departure did not promptly cancel owned query without client cancellation", operationErr)
			}
			if receipt != nil {
				bridgeSessionAwait(t, receipt.state.done)
			}
			proof.state.mu.Lock()
			operations := proof.state.operations
			proof.state.mu.Unlock()
			if operations != 0 || f.ds.pool.Stat().AcquiredConns() != 0 {
				t.Fatal("native cancellation overtook actual worker/operation/connection cleanup")
			}
			var pending int
			if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE pid=$1 AND NOT granted`, int32(pid.Load())).Scan(&pending); err != nil || pending != 0 {
				t.Fatal("actual pending backend query survived native cancellation cleanup")
			}
		})
	}
}

func TestBridgeSessionOriginalLongAccountAndProfileIDsRemainSupported(t *testing.T) {
	for _, length := range []int{129, 256} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			f := bridgeSessionPG(t, "", false, false, nil, nil)
			account, profile := strings.Repeat("a", length), strings.Repeat("p", length)
			// These are actual migrated rows with a real supported bcrypt hash, followed
			// by the genuine unchanged credential verifier; no constructed evidence.
			f.exec(t, `INSERT INTO accounts(id,username,password_hash) SELECT $1,$2,password_hash FROM accounts WHERE id='actor'`, account, fmt.Sprintf("Synthetic Long Account %d", length))
			f.exec(t, `INSERT INTO users(id,account_id,name)VALUES($1,$2,'Synthetic Long Profile')`, profile, account)
			f.exec(t, `INSERT INTO user_settings_versions(user_id,generation)VALUES($1,0) ON CONFLICT(user_id) DO NOTHING`, profile)
			request := bridgeCredentialRequest(t.Context())
			intent, err := f.ds.PrepareBridgeLoginCredentials(request, account, profile, "synthetic-device")
			if err != nil {
				t.Fatal("actual long selector preparation", err)
			}
			defer intent.Close()
			proof, err := f.ds.VerifyBridgeAccountPassword(request, intent, parentTestPassword)
			if err != nil {
				t.Fatal("actual long selector bcrypt verification", err)
			}
			observation, err := f.security.CaptureBridgePolicy(request.Context())
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := f.issuer.Commit(request, proof, observation)
			if err != nil {
				t.Fatal("actual long selector Commit", err)
			}
			defer receipt.Close()
			session, err := f.issuer.Release(request, receipt)
			if err != nil || session.AccountID != account || session.ScopeResource != profile || session.Scope != models.SessionScopeJellyfin || session.Token == "" {
				t.Fatal("existing 256-byte ID contract was narrowed at Release", err)
			}
		})
	}
}
