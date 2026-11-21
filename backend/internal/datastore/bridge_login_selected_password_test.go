//go:build linux

package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/settingscommit"
	"novastream/models"
)

func bridgeSelectedAdd(t *testing.T, f *bridgeSessionFixture, id, account, password string, child bool) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `INSERT INTO accounts(id,username,password_hash)VALUES($1,$2,$3) ON CONFLICT(id) DO UPDATE SET password_hash=EXCLUDED.password_hash`, account, "Synthetic "+account, string(hash))
	f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES($1,$2,'Shared',$3)`, id, account, child)
	f.exec(t, `INSERT INTO user_settings_versions(user_id,generation)VALUES($1,0) ON CONFLICT(user_id) DO NOTHING`, id)
}
func bridgeSelectedFixture(t *testing.T, tracer pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error)) *bridgeSessionFixture {
	t.Helper()
	f := bridgeSessionPG(t, "", false, false, tracer, dial)
	f.exec(t, `UPDATE users SET name='Shared' WHERE id='target'`)
	return f
}
func bridgeSelectedProof(t *testing.T, f *bridgeSessionFixture) (*http.Request, *BridgeSelectedPasswordProof, *config.BridgeLoginPolicyObservation) {
	t.Helper()
	r, _ := bridgeWorkflowRequest(t)
	policy := bridgeWorkflowObservation(t, f, r)
	p, err := f.issuer.VerifySelectedPassword(r, "Shared", "synthetic-device", parentTestPassword, policy)
	if err != nil || p == nil {
		t.Fatal("actual selected verification", err)
	}
	t.Cleanup(p.Close)
	return r, p, policy
}
func bridgeSelectedMint(t *testing.T, f *bridgeSessionFixture, r *http.Request, p *BridgeSelectedPasswordProof, o *config.BridgeLoginPolicyObservation) *BridgeLoginSessionReceipt {
	t.Helper()
	if f.count(t) != 0 {
		t.Fatal("verification minted a session")
	}
	receipt, err := f.issuer.CommitSelected(r, p, o)
	if err != nil || receipt == nil {
		t.Fatal("actual selected mint", err)
	}
	t.Cleanup(receipt.Close)
	return receipt
}
func bridgeSelectedRelease(t *testing.T, f *bridgeSessionFixture, r *http.Request, receipt *BridgeLoginSessionReceipt) {
	t.Helper()
	session, err := f.issuer.Release(r, receipt)
	if err != nil || session.Token == "" || session.AccountID != "actor" || session.ScopeResource != "target" || session.Scope != models.SessionScopeJellyfin || f.count(t) != 1 {
		t.Fatal("actual scoped selected release", err)
	}
}

type bridgeSelectedTestOperation struct {
	done    chan struct{}
	proof   *BridgeSelectedPasswordProof
	receipt *BridgeLoginSessionReceipt
	session models.Session
	err     error
}

func bridgeSelectedAsyncVerify(t *testing.T, f *bridgeSessionFixture, r *http.Request, raw string, o *config.BridgeLoginPolicyObservation, cancel context.CancelFunc, unblock func()) *bridgeSelectedTestOperation {
	t.Helper()
	op := &bridgeSelectedTestOperation{done: make(chan struct{})}
	t.Cleanup(func() {
		cancel()
		unblock()
		bridgeSessionAwait(t, op.done)
		if op.proof != nil {
			op.proof.Close()
		}
	})
	go func() {
		op.proof, op.err = f.issuer.VerifySelectedPassword(r, "Shared", "synthetic-device", raw, o)
		close(op.done)
	}()
	return op
}
func bridgeSelectedWaiting(t *testing.T, f *bridgeSessionFixture, r *http.Request, op *bridgeSelectedTestOperation) *BridgeLoginCredentialIntent {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		bridgeCredentialAdmission.Lock()
		var intents []*BridgeLoginCredentialIntent
		for intent := range bridgeCredentialAdmission.intents {
			if intent.state.store == f.ds && intent.state.request == r {
				intents = append(intents, intent)
			}
		}
		bridgeCredentialAdmission.Unlock()
		for _, intent := range intents {
			intent.state.mu.Lock()
			waiting := intent.state.waitingCPU
			intent.state.mu.Unlock()
			if waiting {
				return intent
			}
		}
		select {
		case <-op.done:
			t.Fatal("selected verification ended before CPU witness", op.err)
		default:
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("real selected comparator did not wait for CPU")
	return nil
}
func bridgeSelectedBlocked(t *testing.T, f *bridgeSessionFixture, pid uint32) {
	t.Helper()
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		var blocked bool
		if err := f.admin.QueryRow(t.Context(), `SELECT cardinality(pg_blocking_pids($1))>0`, int32(pid)).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual writer/query did not block on original PG locks")
}

func TestBridgeSelectedPasswordCompleteOriginalNameSet(t *testing.T) {
	for _, kind := range []string{"zero", "one", "two-one-success", "three-one-success", "four-before-cpu", "two-success", "initial-name-row-aba", "overlapping-alias-dedup"} {
		t.Run(kind, func(t *testing.T) {
			var f *bridgeSessionFixture
			trace := &bridgeCandidateTrace{}
			f = bridgeSelectedFixture(t, trace, nil)
			username := "Shared"
			switch kind {
			case "zero":
				username = "missing"
			case "two-one-success", "three-one-success", "four-before-cpu":
				bridgeSelectedAdd(t, f, "a-rejected", "other", "synthetic different password", false)
			case "two-success":
				bridgeSelectedAdd(t, f, "a-other", "other", parentTestPassword, false)
			case "overlapping-alias-dedup":
				username = "Synthetic Actor/Shared"
			}
			if kind == "three-one-success" || kind == "four-before-cpu" {
				bridgeSelectedAdd(t, f, "b-rejected", "third", "another synthetic password", false)
			}
			if kind == "four-before-cpu" {
				bridgeSelectedAdd(t, f, "c-rejected", "fourth", "fourth synthetic password", false)
				release := bridgeWorkflowBlockCPU(t)
				defer release()
			}
			if kind == "initial-name-row-aba" {
				trace.end = func(context.Context, *pgx.Conn) { f.exec(t, `UPDATE users SET name=name WHERE id='target'`) }
				trace.armed.Store(true)
			}
			r, _ := bridgeWorkflowRequest(t)
			o := bridgeWorkflowObservation(t, f, r)
			p, err := f.issuer.VerifySelectedPassword(r, username, "synthetic-device", parentTestPassword, o)
			if kind == "zero" || kind == "two-success" || kind == "four-before-cpu" || kind == "initial-name-row-aba" {
				if p != nil {
					p.Close()
					t.Fatal("nonunique/changed selection produced proof")
				}
				if err == nil || f.count(t) != 0 || bridgeWorkflowCount(f) != 0 {
					t.Fatal("selection denial retained work or session", err)
				}
				if (kind == "two-success" || kind == "four-before-cpu") && !errors.Is(err, ErrBridgeLoginCandidatesAmbiguous) {
					t.Fatal("ambiguous name/password classification", err)
				}
				return
			}
			if err != nil || p == nil {
				t.Fatal("complete unique selected name set", err)
			}
			defer p.Close()
			expected := 1
			if kind == "two-one-success" {
				expected = 2
			}
			if kind == "three-one-success" {
				expected = 3
			}
			if len(p.state.audits) != expected || len(p.state.initial) != expected || bridgeWorkflowCount(f) != 1 {
				t.Fatal("did not retain all original audits and exactly one admitted proof")
			}
			bridgeSelectedRelease(t, f, r, bridgeSelectedMint(t, f, r, p, o))
		})
	}
}
func TestBridgeSelectedPasswordOriginalRejectedReceipts(t *testing.T) {
	for _, kind := range []string{"rejected-account-hash-change", "rejected-pin-change", "rejected-settings-generation", "rejected-profile-epoch", "rejected-household", "rejected-account-aba", "rejected-profile-aba", "fallback-snapshot-drift"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "fallback-snapshot-drift" {
				f := bridgeSessionPG(t, "2580", false, true, nil, nil)
				f.exec(t, `UPDATE users SET name='Shared' WHERE id='target'`)
				r, cancel := bridgeWorkflowRequest(t)
				o := bridgeWorkflowObservation(t, f, r)
				release := bridgeWorkflowBlockCPU(t)
				op := bridgeSelectedAsyncVerify(t, f, r, "2580", o, cancel, release)
				intent := bridgeSelectedWaiting(t, f, r, op)
				var once sync.Once
				intent.state.mu.Lock()
				real := intent.state.compare
				intent.state.compare = func(h, p []byte) error {
					e := real(h, p)
					once.Do(func() { f.exec(t, `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='target'`) })
					return e
				}
				intent.state.mu.Unlock()
				release()
				bridgeSessionAwait(t, op.done)
				if op.err == nil || op.proof != nil || bridgeWorkflowCount(f) != 0 {
					t.Fatal("fallback adopted changed original snapshot", op.err)
				}
				return
			}
			f := bridgeSelectedFixture(t, nil, nil)
			bridgeSelectedAdd(t, f, "a-rejected", "other", "synthetic rejected password", false)
			r, p, o := bridgeSelectedProof(t, f)
			switch kind {
			case "rejected-account-hash-change":
				h, _ := bcrypt.GenerateFromPassword([]byte(parentTestPassword), bcrypt.MinCost)
				f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='other'`, string(h))
			case "rejected-pin-change":
				h, _ := bcrypt.GenerateFromPassword([]byte("2580"), bcrypt.MinCost)
				f.exec(t, `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='a-rejected'`, string(h))
			case "rejected-settings-generation":
				f.exec(t, `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='a-rejected'`)
			case "rejected-profile-epoch":
				f.exec(t, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','a-rejected',1)`)
			case "rejected-household":
				f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('unmatched-member','other','Unmatched')`)
			case "rejected-account-aba":
				var original models.Account
				var originalAccountRow, originalProfileRow string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT a.id,a.username,a.password_hash,a.is_master,a.max_streams,a.expires_at,a.created_at,a.updated_at,concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM accounts a JOIN users u ON u.account_id=a.id WHERE a.id='other' AND u.id='a-rejected'`).Scan(&original.ID, &original.Username, &original.PasswordHash, &original.IsMaster, &original.MaxStreams, &original.ExpiresAt, &original.CreatedAt, &original.UpdatedAt, &originalAccountRow, &originalProfileRow); err != nil {
					t.Fatal("read original rejected account/profile tuple", err)
				}
				f.exec(t, `DELETE FROM accounts WHERE id='other'`)
				f.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master,max_streams,expires_at,created_at,updated_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, original.ID, original.Username, original.PasswordHash, original.IsMaster, original.MaxStreams, original.ExpiresAt, original.CreatedAt, original.UpdatedAt)
				var replacedAccountRow, retainedProfileRow string
				if err := f.ds.pool.QueryRow(t.Context(), `SELECT concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text) FROM accounts a JOIN users u ON u.account_id=a.id WHERE a.id='other' AND u.id='a-rejected'`).Scan(&replacedAccountRow, &retainedProfileRow); err != nil {
					t.Fatal("read replaced rejected account and retained profile", err)
				}
				if replacedAccountRow == originalAccountRow || retainedProfileRow != originalProfileRow {
					t.Fatal("account ABA did not retain the original profile unchanged")
				}
			case "rejected-profile-aba":
				f.exec(t, `DELETE FROM users WHERE id='a-rejected'`)
				f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('a-rejected','other','Shared')`)
				f.exec(t, `INSERT INTO user_settings_versions(user_id,generation)VALUES('a-rejected',0) ON CONFLICT DO NOTHING`)
			}
			receipt, err := f.issuer.CommitSelected(r, p, o)
			if receipt != nil {
				receipt.Close()
			}
			if err == nil || receipt != nil || f.count(t) != 0 {
				t.Fatal("original rejected receipt was recaptured/adopted", err)
			}
		})
	}
}
func TestBridgeSelectedPasswordPolicyAndKinds(t *testing.T) {
	for _, kind := range []string{"account-no-pin", "enforce-false-account", "enforce-true-combined", "optional-pin", "bridge-disabled", "unsupported-competitor-terminal"} {
		t.Run(kind, func(t *testing.T) {
			pin, raw := "", parentTestPassword
			enforce, allow := false, false
			want := bridgeCredentialAccount
			if kind == "enforce-false-account" || kind == "enforce-true-combined" || kind == "optional-pin" {
				pin = "2580"
			}
			if kind == "enforce-true-combined" {
				enforce = true
				raw += "2580"
				want = bridgeCredentialCombined
			}
			if kind == "optional-pin" {
				allow = true
				raw = "2580"
				want = bridgeCredentialPIN
			}
			f := bridgeSessionPG(t, pin, enforce, allow, nil, nil)
			f.exec(t, `UPDATE users SET name='Shared' WHERE id='target'`)
			if kind == "bridge-disabled" {
				f.settings.JellyfinBridge.Enabled = false
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unsupported-competitor-terminal" {
				bridgeSelectedAdd(t, f, "a-unsupported", "other", "synthetic password", true)
			}
			r, _ := bridgeWorkflowRequest(t)
			o := bridgeWorkflowObservation(t, f, r)
			p, err := f.issuer.VerifySelectedPassword(r, "Shared", "synthetic-device", raw, o)
			if kind == "bridge-disabled" || kind == "unsupported-competitor-terminal" {
				if p != nil {
					p.Close()
				}
				if p != nil || err == nil || bridgeWorkflowCount(f) != 0 || f.count(t) != 0 {
					t.Fatal("unavailable competitor/policy treated as harmless rejection", err)
				}
				return
			}
			if p == nil || err != nil {
				t.Fatal("genuine policy kind selection", err)
			}
			defer p.Close()
			if p.state.credential.state.kind != want {
				t.Fatal("selected proof changed original credential kind")
			}
			bridgeSelectedRelease(t, f, r, bridgeSelectedMint(t, f, r, p, o))
		})
	}
}
func TestBridgeSelectedPasswordLateRelationGuards(t *testing.T) {
	for _, kind := range []string{"same-tx-all-seven", "accounts-conflict-55p03", "settings-version-conflict-55p03", "profile-epoch-conflict-55p03", "regional-policy-conflict-55p03", "acl-policy-conflict-55p03", "acl-account-grant-conflict-55p03", "acl-profile-grant-conflict-55p03", "no-foreign-row-wait"} {
		t.Run(kind, func(t *testing.T) {
			var armed atomic.Bool
			var observed atomic.Bool
			var f *bridgeSessionFixture
			var pid atomic.Uint32
			trace := &bridgeSessionTransactionTrace{end: func(c *pgx.Conn, sql string, data pgx.TraceQueryEndData) {
				if !armed.Load() {
					return
				}
				if strings.HasPrefix(sql, "LOCK TABLE ") && strings.Contains(sql, " NOWAIT") {
					if old := pid.Load(); old != 0 && old != c.PgConn().PID() {
						t.Error("late guards moved to another backend")
					}
					pid.Store(c.PgConn().PID())
					var pgError *pgconn.PgError
					if errors.As(data.Err, &pgError) && pgError.Code == "55P03" {
						observed.Store(true)
					}
					if strings.Contains(sql, `"media_library_profile_grants"`) && data.Err == nil {
						var count int
						if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid=l.relation JOIN pg_namespace n ON n.oid=c.relnamespace WHERE l.pid=$1 AND n.nspname=$2 AND l.mode='ShareLock' AND l.granted AND c.relname=ANY($3::text[])`, int32(c.PgConn().PID()), f.schema, append(bridgeSelectedLateRelations[:], "users")).Scan(&count); err != nil || count != 8 {
							t.Error("actual original backend did not retain all eight relation guards", err, count)
						}
						observed.Store(true)
					}
				}
			}}
			f = bridgeSelectedFixture(t, trace, nil)
			bridgeSelectedAdd(t, f, "a-rejected", "other", "synthetic rejected password", false)
			r, p, o := bridgeSelectedProof(t, f)
			var holder pgx.Tx
			if strings.Contains(kind, "conflict") || kind == "no-foreign-row-wait" {
				var err error
				holder, err = f.admin.Begin(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer rollbackPinTx(holder, t.Context())
				if kind == "no-foreign-row-wait" {
					_, err = holder.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` WHERE id='other' FOR UPDATE`)
				} else {
					tables := map[string]string{"accounts-conflict-55p03": "accounts", "settings-version-conflict-55p03": "user_settings_versions", "profile-epoch-conflict-55p03": "profile_unlock_revocations", "regional-policy-conflict-55p03": "regional_profile_policies", "acl-policy-conflict-55p03": "media_library_access_policies", "acl-account-grant-conflict-55p03": "media_library_account_grants", "acl-profile-grant-conflict-55p03": "media_library_profile_grants"}
					_, err = holder.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, tables[kind]}.Sanitize()+" IN ROW EXCLUSIVE MODE")
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			armed.Store(true)
			receipt, err := f.issuer.CommitSelected(r, p, o)
			if strings.Contains(kind, "conflict") {
				if receipt != nil {
					receipt.Close()
				}
				if receipt != nil || !errors.Is(err, ErrBridgeCredentialBusy) || !observed.Load() || f.count(t) != 0 {
					t.Fatal("actual late55P03 was not terminal Busy", err)
				}
				guard, e := f.coordinator.TryWrite(r.Context())
				if e != nil {
					t.Fatal("failed mint retained native charge", e)
				}
				_ = guard.Close()
				return
			}
			if err != nil || receipt == nil || !observed.Load() {
				t.Fatal("actual same-TX guards/foreign row nonwait", err)
			}
			defer receipt.Close()
			bridgeSelectedRelease(t, f, r, receipt)
		})
	}
}

func bridgeSelectedWriterSQL(kind string) string {
	switch strings.TrimSuffix(strings.TrimSuffix(kind, "-first"), "-after") {
	case "foreign-rename":
		return `UPDATE accounts SET username=username||' renamed' WHERE id='other'`
	case "account-bulk":
		return `UPDATE accounts SET max_streams=max_streams WHERE id IN('actor','other')`
	case "user-topology":
		return `UPDATE users SET name=name WHERE id='a-rejected'`
	case "settings-only":
		return `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='a-rejected'`
	case "epoch-only":
		return `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','a-rejected',1) ON CONFLICT(scope,scope_key) DO UPDATE SET generation=profile_unlock_revocations.generation+1`
	case "acl-fence":
		return `UPDATE media_library_access_policies SET access_mode='all' WHERE library_id='synthetic-selected-acl'`
	}
	panic("unknown selected writer fixture")
}
func TestBridgeSelectedPasswordWriterLinearization(t *testing.T) {
	for _, kind := range []string{"foreign-rename-first", "foreign-rename-after", "account-bulk-first", "account-bulk-after", "user-topology-first", "user-topology-after", "settings-only-first", "settings-only-after", "epoch-only-first", "epoch-only-after", "acl-fence-first", "acl-fence-after"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			t.Cleanup(unblock)
			var armed atomic.Bool
			var pid atomic.Uint32
			trace := &bridgeSessionTransactionTrace{end: func(c *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				if strings.Contains(sql, `"media_library_profile_grants" IN SHARE MODE NOWAIT`) && d.Err == nil && armed.CompareAndSwap(true, false) {
					pid.Store(c.PgConn().PID())
					close(entered)
					<-proceed
				}
			}}
			f := bridgeSelectedFixture(t, trace, nil)
			bridgeSelectedAdd(t, f, "a-rejected", "other", "synthetic rejected password", false)
			f.exec(t, `INSERT INTO media_library_access_policies(library_id)VALUES('synthetic-selected-acl')`)
			r, p, o := bridgeSelectedProof(t, f)
			if strings.HasSuffix(kind, "-first") {
				f.exec(t, bridgeSelectedWriterSQL(kind))
				receipt, err := f.issuer.CommitSelected(r, p, o)
				if receipt != nil {
					receipt.Close()
				}
				if err == nil || receipt != nil || f.count(t) != 0 {
					t.Fatal("writer-first changed original receipts adopted", err)
				}
				return
			}
			writer, err := f.ds.pool.Acquire(r.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(writer.Release)
			armed.Store(true)
			mint := &bridgeSelectedTestOperation{done: make(chan struct{})}
			go func() { mint.receipt, mint.err = f.issuer.CommitSelected(r, p, o); close(mint.done) }()
			t.Cleanup(func() {
				unblock()
				bridgeSessionAwait(t, mint.done)
				if mint.receipt != nil {
					mint.receipt.Close()
				}
			})
			bridgeSessionAwait(t, entered)
			writerDone := make(chan struct{})
			var writerErr error
			go func() { _, writerErr = writer.Exec(r.Context(), bridgeSelectedWriterSQL(kind)); close(writerDone) }()
			t.Cleanup(func() { unblock(); bridgeSessionAwait(t, writerDone) })
			bridgeSelectedBlocked(t, f, writer.Conn().PgConn().PID())
			var blockers []int32
			if err := f.admin.QueryRow(t.Context(), `SELECT pg_blocking_pids($1)`, int32(writer.Conn().PgConn().PID())).Scan(&blockers); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, blocker := range blockers {
				found = found || uint32(blocker) == pid.Load()
			}
			if !found {
				t.Fatal("writer did not wait for original mint backend")
			}
			unblock()
			bridgeSessionAwait(t, mint.done)
			bridgeSessionAwait(t, writerDone)
			if mint.err != nil || mint.receipt == nil || writerErr != nil {
				t.Fatal("serialized actual writer/mint failed", mint.err, writerErr)
			}
			session, releaseErr := f.issuer.Release(r, mint.receipt)
			if releaseErr == nil || session.Token != "" || f.count(t) != 1 {
				t.Fatal("postcommit drift was adopted on release", releaseErr)
			}
		})
	}
}
func TestBridgeSelectedPasswordReleaseOriginalAudit(t *testing.T) {
	for _, kind := range []string{"rejected-drift-before-release", "foreign-alias-before-release", "release-after-session-lock", "retained-shared-key", "second-release-used", "unknown-release-ack"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "unknown-release-ack" {
				bridgeSelectedUnknownACK(t, true)
				return
			}
			var released atomic.Bool
			var lateLocks atomic.Int32
			var sessionSeen atomic.Bool
			var wrongOrder atomic.Bool
			var shared atomic.Bool
			var f *bridgeSessionFixture
			trace := &bridgeSessionTransactionTrace{end: func(c *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
				if !released.Load() || d.Err != nil {
					return
				}
				if strings.Contains(sql, "FROM sessions WHERE token=$1 FOR UPDATE") {
					sessionSeen.Store(true)
				}
				if strings.HasPrefix(sql, "LOCK TABLE ") && strings.Contains(sql, " NOWAIT") {
					if !sessionSeen.Load() {
						wrongOrder.Store(true)
					}
					lateLocks.Add(1)
					if strings.Contains(sql, `"media_library_profile_grants"`) {
						var n int
						if err := f.admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND mode='ShareLock' AND granted`, int32(c.PgConn().PID())).Scan(&n); err != nil {
							t.Error(err)
						}
						shared.Store(n > 0)
					}
				}
			}}
			f = bridgeSelectedFixture(t, trace, nil)
			bridgeSelectedAdd(t, f, "a-rejected", "other", "synthetic rejected password", false)
			r, p, o := bridgeSelectedProof(t, f)
			receipt := bridgeSelectedMint(t, f, r, p, o)
			if kind == "rejected-drift-before-release" {
				f.exec(t, `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='a-rejected'`)
			}
			if kind == "foreign-alias-before-release" {
				f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('new-alias','other','Shared')`)
			}
			released.Store(true)
			session, err := f.issuer.Release(r, receipt)
			if strings.Contains(kind, "before-release") {
				if err == nil || session.Token != "" {
					t.Fatal("release adopted original rejected/name drift", err)
				}
				return
			}
			if err != nil || session.Token == "" || wrongOrder.Load() || lateLocks.Load() != 7 || !sessionSeen.Load() || !shared.Load() {
				t.Fatal("release lost original session-before-guard order/shared-key/backend", err)
			}
			if kind == "second-release-used" {
				again, e := f.issuer.Release(r, receipt)
				if !errors.Is(e, ErrBridgeSessionUsed) || again.Token != "" {
					t.Fatal("selected Release replayed", e)
				}
			}
		})
	}
}
func bridgeSelectedUnknownACK(t *testing.T, release bool) {
	t.Helper()
	var enabled, dropped atomic.Bool
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &bridgeSessionDropACK{Conn: c, enabled: &enabled, actual: &parentDropCommit{Conn: c, dropped: &dropped}}, nil
	}
	f := bridgeSelectedFixture(t, nil, dial)
	r, p, o := bridgeSelectedProof(t, f)
	if !release {
		enabled.Store(true)
	}
	receipt, err := f.issuer.CommitSelected(r, p, o)
	if !release {
		if err == nil || !errors.Is(err, ErrBridgeSessionUnknown) || receipt != nil || !dropped.Load() || f.count(t) != 1 {
			t.Fatal("unknown selected Commit ACK produced output/rollback", err)
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
		t.Fatal("unknown selected Release ACK produced token", err)
	}
	receipt.Close()
	if _, e := f.issuer.Release(r, receipt); e == nil {
		t.Fatal("unknown selected release replayed")
	}
}
func TestBridgeSelectedPasswordOriginalMinExpiry(t *testing.T) {
	for _, kind := range []string{"earlier-rejected-expiry", "actual-commit-query-deadline", "actual-release-query-deadline", "known-ack-after-expiry", "delayed-callback-direct-endguard", "no-credential-expiry-rewrite"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(proceed) }) }
			defer unblock()
			t.Cleanup(unblock)
			var armed atomic.Bool
			var observedDeadline time.Time
			var observedMu sync.Mutex
			waiting := make(chan struct{})
			var waitOnce sync.Once
			var blockedPID atomic.Uint32
			trace := &bridgeSessionTransactionTrace{
				start: func(c *pgx.Conn, sql string) {
					if kind == "actual-commit-query-deadline" && strings.Contains(sql, "SELECT id FROM accounts WHERE") || kind == "actual-release-query-deadline" && strings.Contains(sql, "FROM sessions WHERE token=$1 FOR UPDATE") {
						blockedPID.Store(c.PgConn().PID())
						waitOnce.Do(func() { close(waiting) })
					}
				},
				end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
					if sql == "commit" && d.Err == nil && armed.CompareAndSwap(true, false) {
						close(entered)
						<-proceed
					}
				},
			}
			// Query-context inspection uses the actual driver callback, not fabricated
			// proof expiry or a replacement request/context.
			deadlineTrace := &bridgeSelectedDeadlineTrace{next: trace, observe: func(ctx context.Context, sql string) {
				if strings.Contains(sql, "SELECT id FROM accounts WHERE") || strings.Contains(sql, "FROM sessions WHERE token=$1 FOR UPDATE") {
					if d, ok := ctx.Deadline(); ok {
						observedMu.Lock()
						observedDeadline = d
						observedMu.Unlock()
					}
				}
			}}
			f := bridgeSelectedFixture(t, deadlineTrace, nil)
			bridgeSelectedAdd(t, f, "a-rejected", "other", "synthetic rejected password", false)
			expires := time.Now().Add(3 * time.Second).UTC().Truncate(time.Microsecond)
			f.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='other'`, expires)
			r, p, o := bridgeSelectedProof(t, f)
			originalExpiry := p.state.credential.ExpiresAt()
			if !p.ExpiresAt().Equal(expires) || !originalExpiry.After(expires) {
				t.Fatal("rejected minimum replaced successful original credential expiry")
			}
			if kind == "earlier-rejected-expiry" || kind == "no-credential-expiry-rewrite" {
				receipt := bridgeSelectedMint(t, f, r, p, o)
				if !p.state.credential.ExpiresAt().Equal(originalExpiry) {
					t.Fatal("mint rewrote original credential expiry")
				}
				bridgeSelectedRelease(t, f, r, receipt)
				return
			}
			if kind == "known-ack-after-expiry" || kind == "delayed-callback-direct-endguard" {
				if kind == "delayed-callback-direct-endguard" {
					p.state.stop()
				}
				armed.Store(true)
				op := &bridgeSelectedTestOperation{done: make(chan struct{})}
				go func() { op.receipt, op.err = f.issuer.CommitSelected(r, p, o); close(op.done) }()
				t.Cleanup(func() {
					unblock()
					bridgeSessionAwait(t, op.done)
					if op.receipt != nil {
						op.receipt.Close()
					}
				})
				bridgeSessionAwait(t, entered)
				if f.count(t) != 1 {
					t.Fatal("held real ACK was not committed")
				}
				bridgeSessionAwait(t, p.Context().Done())
				unblock()
				bridgeSessionAwait(t, op.done)
				if op.err == nil || errors.Is(op.err, ErrBridgeSessionUnknown) || op.receipt != nil || f.count(t) != 1 {
					t.Fatal("late known ACK delivered token or falsely claimed unknown/rollback", op.err)
				}
				return
			}
			var receipt *BridgeLoginSessionReceipt
			if kind == "actual-release-query-deadline" {
				receipt = bridgeSelectedMint(t, f, r, p, o)
			}
			holder, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackPinTx(holder, t.Context())
			if receipt == nil {
				_, err = holder.Exec(t.Context(), `SELECT id FROM `+pgx.Identifier{f.schema, "accounts"}.Sanitize()+` WHERE id='actor' FOR UPDATE`)
			} else {
				_, err = holder.Exec(t.Context(), `SELECT token FROM `+pgx.Identifier{f.schema, "sessions"}.Sanitize()+` WHERE token=$1 FOR UPDATE`, receipt.state.session.Token)
			}
			if err != nil {
				t.Fatal(err)
			}
			op := &bridgeSelectedTestOperation{done: make(chan struct{})}
			go func() {
				if receipt == nil {
					op.receipt, op.err = f.issuer.CommitSelected(r, p, o)
				} else {
					op.session, op.err = f.issuer.Release(r, receipt)
				}
				close(op.done)
			}()
			t.Cleanup(func() {
				_ = holder.Rollback(context.Background())
				bridgeSessionAwait(t, op.done)
				if op.receipt != nil {
					op.receipt.Close()
				}
			})
			bridgeSessionAwait(t, waiting)
			bridgeSelectedBlocked(t, f, blockedPID.Load())
			bridgeSessionAwait(t, p.Context().Done())
			bridgeSessionAwait(t, op.done)
			observedMu.Lock()
			deadline := observedDeadline
			observedMu.Unlock()
			if !deadline.Equal(expires) || op.err == nil || op.receipt != nil || op.session.Token != "" || r.Context().Err() != nil || !p.state.credential.ExpiresAt().Equal(originalExpiry) {
				t.Fatal("actual SQL work ignored independent earlier selected deadline", op.err, deadline, expires)
			}
		})
	}
}

type bridgeSelectedDeadlineTrace struct {
	next    *bridgeSessionTransactionTrace
	observe func(context.Context, string)
}

func (tr *bridgeSelectedDeadlineTrace) TraceQueryStart(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	tr.observe(ctx, d.SQL)
	return tr.next.TraceQueryStart(ctx, c, d)
}
func (tr *bridgeSelectedDeadlineTrace) TraceQueryEnd(ctx context.Context, c *pgx.Conn, d pgx.TraceQueryEndData) {
	tr.next.TraceQueryEnd(ctx, c, d)
}

func bridgeSelectedHeldLateClose(t *testing.T, concurrent bool) {
	t.Helper()
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(proceed) }) }
	defer unblock()
	t.Cleanup(unblock)
	var armed atomic.Bool
	trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, d pgx.TraceQueryEndData) {
		if sql == bridgeLoginCandidateQuery && d.Err == nil && armed.CompareAndSwap(true, false) {
			close(entered)
			<-proceed
		}
	}}
	f := bridgeSelectedFixture(t, trace, nil)
	r, p, o := bridgeSelectedProof(t, f)
	armed.Store(true)
	op := &bridgeSelectedTestOperation{done: make(chan struct{})}
	go func() { op.receipt, op.err = f.issuer.CommitSelected(r, p, o); close(op.done) }()
	t.Cleanup(func() {
		unblock()
		bridgeSessionAwait(t, op.done)
		if op.receipt != nil {
			op.receipt.Close()
		}
	})
	bridgeSessionAwait(t, entered)
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	var second chan struct{}
	if concurrent {
		second = make(chan struct{})
		go func() { p.Close(); close(second) }()
	}
	bridgeSessionAwait(t, p.Context().Done())
	select {
	case <-closed:
		t.Fatal("selected Close returned before original query/receipt cleanup")
	default:
	}
	if concurrent {
		select {
		case <-second:
			t.Fatal("second Close bypassed original barrier")
		default:
		}
	}
	if bridgeWorkflowCount(f) != 1 {
		t.Fatal("credential capacity released before actual end")
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if guard != nil {
		_ = guard.Close()
	}
	if !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("native capacity released before original QueryEnd", err)
	}
	unblock()
	bridgeSessionAwait(t, op.done)
	bridgeSessionAwait(t, closed)
	if concurrent {
		bridgeSessionAwait(t, second)
	}
	if op.err == nil || op.receipt != nil || bridgeWorkflowCount(f) != 0 {
		t.Fatal("selected Close did not join/retire original work", op.err)
	}
	p.state.mu.Lock()
	remaining := len(p.state.audits)
	p.state.mu.Unlock()
	if remaining != 0 {
		t.Fatal("closed selected proof retained candidate audit")
	}
}
func bridgeSelectedPhysicalJoin(t *testing.T) {
	t.Helper()
	var first atomic.Bool
	var mu sync.Mutex
	var actual *queryLifecycleGatedConn
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		if first.CompareAndSwap(false, true) {
			gate := &queryLifecycleGatedConn{Conn: c, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			actual = gate
			mu.Unlock()
			return gate, nil
		}
		return c, nil
	}
	f := bridgeSelectedFixture(t, nil, dial)
	r, cancel := bridgeWorkflowRequest(t)
	o := bridgeWorkflowObservation(t, f, r)
	mu.Lock()
	gate := actual
	mu.Unlock()
	if gate == nil {
		t.Fatal("actual primary socket not captured")
	}
	t.Cleanup(gate.release)
	c, e := f.ds.pool.Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	physical := c.Conn().PgConn()
	c.Release()
	gate.armed.Store(true)
	op := bridgeSelectedAsyncVerify(t, f, r, parentTestPassword, o, cancel, gate.release)
	bridgeSessionAwait(t, gate.closeEntered)
	closed := make(chan struct{})
	go func() { _ = f.coordinator.Close(); close(closed) }()
	t.Cleanup(func() { gate.release(); bridgeSessionAwait(t, closed) })
	select {
	case <-physical.CleanupDone():
		t.Fatal("held actual primary socket already cleaned up")
	default:
	}
	select {
	case <-op.done:
		t.Fatal("selected scan returned before actual physical cleanup")
	default:
	}
	select {
	case <-closed:
		t.Fatal("native owner ended before physical cleanup")
	default:
	}
	gate.release()
	bridgeSessionAwait(t, physical.CleanupDone())
	bridgeSessionAwait(t, op.done)
	bridgeSessionAwait(t, closed)
	if op.err == nil || op.proof != nil || bridgeWorkflowCount(f) != 0 {
		t.Fatal("closed socket became selected evidence", op.err)
	}
}
func TestBridgeSelectedPasswordCleanupAndAdmission(t *testing.T) {
	for _, kind := range []string{"held-bcrypt-caller-cancel", "held-prepare-sql-cancel", "held-late-query-close", "closed-socket-cleanup", "concurrent-selected-close", "original-two-operation-cap", "all-candidate-proofs-closed"} {
		t.Run(kind, func(t *testing.T) {
			switch kind {
			case "held-late-query-close":
				bridgeSelectedHeldLateClose(t, false)
				return
			case "concurrent-selected-close":
				bridgeSelectedHeldLateClose(t, true)
				return
			case "closed-socket-cleanup":
				bridgeSelectedPhysicalJoin(t)
				return
			case "held-bcrypt-caller-cancel":
				f := bridgeSelectedFixture(t, nil, nil)
				r, cancel := bridgeWorkflowRequest(t)
				o := bridgeWorkflowObservation(t, f, r)
				releaseCPU := bridgeWorkflowBlockCPU(t)
				entered, proceed := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(proceed) }); releaseCPU() }
				op := bridgeSelectedAsyncVerify(t, f, r, parentTestPassword, o, cancel, unblock)
				intent := bridgeSelectedWaiting(t, f, r, op)
				intent.state.mu.Lock()
				real := intent.state.compare
				intent.state.compare = func(h, p []byte) error { e := real(h, p); close(entered); <-proceed; return e }
				intent.state.mu.Unlock()
				releaseCPU()
				bridgeSessionAwait(t, entered)
				cancel()
				select {
				case <-op.done:
					t.Fatal("verification returned before actual bcrypt return")
				default:
				}
				if bridgeWorkflowCount(f) != 1 {
					t.Fatal("original credential capacity refunded during real bcrypt")
				}
				unblock()
				bridgeSessionAwait(t, op.done)
				if op.err == nil || op.proof != nil || bridgeWorkflowCount(f) != 0 {
					t.Fatal("canceled actual bcrypt retained proof/work", op.err)
				}
				return
			case "held-prepare-sql-cancel":
				var pid atomic.Uint32
				trace := &bridgeSessionTransactionTrace{start: func(c *pgx.Conn, sql string) {
					if strings.Contains(sql, "FROM accounts a JOIN users u") && strings.Contains(sql, "user_settings_versions") {
						pid.Store(c.PgConn().PID())
					}
				}}
				f := bridgeSelectedFixture(t, trace, nil)
				holder, e := f.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer rollbackPinTx(holder, t.Context())
				_, e = holder.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, "user_settings_versions"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE")
				if e != nil {
					t.Fatal(e)
				}
				r, cancel := bridgeWorkflowRequest(t)
				o := bridgeWorkflowObservation(t, f, r)
				op := bridgeSelectedAsyncVerify(t, f, r, parentTestPassword, o, cancel, func() {})
				until := time.Now().Add(5 * time.Second)
				for pid.Load() == 0 && time.Now().Before(until) {
					time.Sleep(time.Millisecond)
				}
				if pid.Load() == 0 {
					t.Fatal("actual Prepare query did not start")
				}
				bridgeSelectedBlocked(t, f, pid.Load())
				cancel()
				bridgeSessionAwait(t, op.done)
				if op.err == nil || op.proof != nil || bridgeWorkflowCount(f) != 0 {
					t.Fatal("canceled Prepare did not join", op.err)
				}
				return
			case "original-two-operation-cap":
				f := bridgeSelectedFixture(t, nil, nil)
				r, p, o := bridgeSelectedProof(t, f)
				receipt := bridgeSelectedMint(t, f, r, p, o)
				_, finish, e := p.state.credential.state.begin(r.Context())
				if e != nil {
					t.Fatal(e)
				}
				var once sync.Once
				done := func() { once.Do(finish) }
				defer done()
				session, e := f.issuer.Release(r, receipt)
				if !errors.Is(e, ErrBridgeCredentialBusy) || session.Token != "" {
					t.Fatal("selected Release bypassed original two-operation cap", e)
				}
				done()
				bridgeSelectedRelease(t, f, r, receipt)
				return
			case "all-candidate-proofs-closed":
				f := bridgeSelectedFixture(t, nil, nil)
				bridgeSelectedAdd(t, f, "a-second", "other", parentTestPassword, false)
				bridgeSelectedAdd(t, f, "b-third", "third", parentTestPassword, false)
				r, _ := bridgeWorkflowRequest(t)
				o := bridgeWorkflowObservation(t, f, r)
				p, e := f.issuer.VerifySelectedPassword(r, "Shared", "synthetic-device", parentTestPassword, o)
				if p != nil {
					p.Close()
				}
				if !errors.Is(e, ErrBridgeLoginCandidatesAmbiguous) || p != nil || bridgeWorkflowCount(f) != 0 || f.count(t) != 0 {
					t.Fatal("ambiguous actual proofs were not all closed", e)
				}
				guard, e := f.coordinator.TryWrite(t.Context())
				if e != nil {
					t.Fatal("selection native charge not refunded", e)
				}
				_ = guard.Close()
				return
			}
		})
	}
}
func TestBridgeSelectedPasswordCommitOutcomesAndOneShot(t *testing.T) {
	for _, kind := range []string{"known-commit", "known-rollback", "unknown-commit-ack", "second-commit-used", "busy-no-retry"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "unknown-commit-ack" {
				bridgeSelectedUnknownACK(t, false)
				return
			}
			var locks atomic.Int32
			trace := &bridgeSessionTransactionTrace{start: func(_ *pgx.Conn, sql string) {
				if strings.HasPrefix(sql, "LOCK TABLE ") && strings.Contains(sql, `"accounts" IN SHARE MODE NOWAIT`) {
					locks.Add(1)
				}
			}}
			f := bridgeSelectedFixture(t, trace, nil)
			r, p, o := bridgeSelectedProof(t, f)
			if kind == "known-rollback" {
				f.exec(t, `CREATE FUNCTION selected_reject_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic selected insert failure'; END $$;CREATE TRIGGER selected_reject BEFORE INSERT ON sessions FOR EACH ROW EXECUTE FUNCTION selected_reject_insert()`)
			}
			var holder pgx.Tx
			if kind == "busy-no-retry" {
				var e error
				holder, e = f.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer rollbackPinTx(holder, t.Context())
				if _, e = holder.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, "accounts"}.Sanitize()+" IN ROW EXCLUSIVE MODE"); e != nil {
					t.Fatal(e)
				}
			}
			receipt, e := f.issuer.CommitSelected(r, p, o)
			if kind == "known-rollback" || kind == "busy-no-retry" {
				if receipt != nil {
					receipt.Close()
				}
				if e == nil || receipt != nil || f.count(t) != 0 {
					t.Fatal("failed actual mint returned token/committed row", e)
				}
				if kind == "busy-no-retry" {
					if !errors.Is(e, ErrBridgeCredentialBusy) || locks.Load() != 1 {
						t.Fatal("late conflict retried or misclassified", e)
					}
				}
				return
			}
			if e != nil || receipt == nil {
				t.Fatal(e)
			}
			defer receipt.Close()
			if kind == "second-commit-used" {
				again, err := f.issuer.CommitSelected(r, p, o)
				if again != nil {
					again.Close()
				}
				if !errors.Is(err, ErrBridgeSessionUsed) || again != nil {
					t.Fatal("selected mint replayed", err)
				}
			}
			bridgeSelectedRelease(t, f, r, receipt)
		})
	}
}
func TestBridgeSelectedPasswordProofOpacityAndOriginalBinding(t *testing.T) {
	for _, kind := range []string{"copied-proof", "json-import", "value-format", "foreign-issuer", "cloned-request", "changed-context", "changed-client", "recaptured-policy-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSelectedFixture(t, nil, nil)
			r, p, o := bridgeSelectedProof(t, f)
			bad := r
			policy := o
			issuer := f.issuer
			candidate := p
			restore := func() {}
			switch kind {
			case "copied-proof":
				copy := *p
				candidate = &copy
				candidate.Close()
				if p.Context().Err() != nil {
					t.Fatal("copied Close retired original")
				}
			case "json-import":
				copy := *p
				if json.Unmarshal([]byte(`{}`), &copy) == nil {
					t.Fatal("selected JSON import accepted")
				}
				return
			case "value-format":
				for _, v := range []any{p, *p} {
					raw, e := json.Marshal(v)
					if e != nil || string(raw) != "null" || fmt.Sprintf("%#v", v) != "BridgeSelectedPasswordProof(opaque)" {
						t.Fatal("selected private audit/credential disclosed")
					}
				}
				return
			case "foreign-issuer":
				var e error
				issuer, e = NewBridgeLoginSessionIssuer(f.ds, f.manager, f.security, f.coordinator)
				if e != nil {
					t.Fatal(e)
				}
			case "cloned-request":
				bad = r.Clone(r.Context())
			case "changed-context":
				bad = r.WithContext(context.WithValue(r.Context(), bridgeWorkflowContextKey{}, true))
			case "changed-client":
				old := r.Header.Get("X-Client-ID")
				r.Header.Set("X-Client-ID", "other-device")
				restore = func() {
					if old == "" {
						r.Header.Del("X-Client-ID")
					} else {
						r.Header.Set("X-Client-ID", old)
					}
				}
			case "recaptured-policy-denied":
				var e error
				policy, e = f.security.CaptureBridgePolicy(r.Context())
				if e != nil {
					t.Fatal(e)
				}
				if policy == o {
					t.Fatal("fixture did not recapture a distinct policy")
				}
			}
			receipt, e := issuer.CommitSelected(bad, candidate, policy)
			if receipt != nil {
				receipt.Close()
			}
			restore()
			if e == nil || receipt != nil || f.count(t) != 0 {
				t.Fatal("replacement/copied selected input became authority", e)
			}
			bridgeSelectedRelease(t, f, r, bridgeSelectedMint(t, f, r, p, o))
		})
	}
}
