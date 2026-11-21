//go:build linux

package datastore

import (
	"context"
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
	"novastream/internal/settingscommit"
	"novastream/models"
)

const quickConnectApproverToken = "synthetic-qc-approver"

type quickConnectFixture struct {
	*bridgeSessionFixture
	qc *BridgeQuickConnectIssuer
}

func quickConnectPG(t *testing.T, pin string, enforce bool, tracer pgx.QueryTracer, dial func(context.Context, string, string) (net.Conn, error)) *quickConnectFixture {
	t.Helper()
	base := bridgeSessionPG(t, pin, enforce, false, tracer, dial)
	base.settings.JellyfinBridge.QuickConnect = true
	if err := base.manager.Save(base.settings); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: quickConnectApproverToken, AccountID: "actor", ClientID: "approver-device", Scope: models.SessionScopeJellyfin, ScopeResource: "target", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := base.ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	qc, err := NewBridgeQuickConnectIssuer(t.Context(), base.ds, base.manager, base.security, base.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(qc.Close)
	return &quickConnectFixture{base, qc}
}
func quickConnectIngress(t *testing.T, client, ip string) (*http.Request, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	t.Cleanup(cancel)
	r := bridgeCredentialRequest(ctx)
	r.Header.Set("X-Client-ID", client)
	r.Header.Set("User-Agent", "Synthetic Target TV")
	r.RemoteAddr = ip
	return r, cancel
}
func quickConnectChallenge(t *testing.T, f *quickConnectFixture) *BridgeQuickConnectChallenge {
	t.Helper()
	r, _ := quickConnectIngress(t, "target-device", "198.51.100.1")
	c, err := f.qc.Initiate(r, "target-device")
	if err != nil {
		t.Fatal("genuine original QC challenge", err)
	}
	t.Cleanup(c.Close)
	return c
}
func quickConnectApprover(t *testing.T, f *quickConnectFixture) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, cancel := quickConnectIngress(t, "approver-device", "198.51.100.2")
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, quickConnectApproverToken, "approver-device")
	if err != nil {
		t.Fatal(err)
	}
	stop := func() { release(); cancel() }
	t.Cleanup(stop)
	return checked, stop
}
func quickConnectApproval(t *testing.T, f *quickConnectFixture, c *BridgeQuickConnectChallenge) *BridgeQuickConnectApproval {
	t.Helper()
	r, release := quickConnectApprover(t, f)
	a, err := f.qc.Approve(r, c)
	release()
	if err != nil || a == nil {
		t.Fatal("genuine durable QC handoff", err)
	}
	return a
}
func quickConnectCollect(t *testing.T, f *quickConnectFixture, c *BridgeQuickConnectChallenge, a *BridgeQuickConnectApproval) (*http.Request, *BridgeQuickConnectReceipt) {
	t.Helper()
	r, _ := quickConnectIngress(t, "target-device", "203.0.113.8")
	d, err := c.Details()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := f.qc.Commit(r, c, a, d.Secret, "target-device")
	if err != nil || receipt == nil {
		t.Fatal("actual QC mint", err)
	}
	t.Cleanup(receipt.Close)
	return r, receipt
}
func quickConnectMintCount(t *testing.T, f *quickConnectFixture) int {
	t.Helper()
	var n int
	if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{f.schema, "sessions"}.Sanitize()+" WHERE token<>$1", quickConnectApproverToken).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBridgeQuickConnectOriginalChallenge(t *testing.T) {
	for _, kind := range []string{"original", "copy", "foreign", "invalid-client", "capacity-per-ip", "capacity-global", "expiry-never-renew"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPG(t, "", false, nil, nil)
			r, _ := quickConnectIngress(t, "target-device", "198.51.100.1")
			if kind == "invalid-client" {
				if c, err := f.qc.Initiate(r, " bad "); err == nil || c != nil {
					t.Fatal("invalid challenge client accepted")
				}
				return
			}
			c := quickConnectChallenge(t, f)
			before, err := c.Details()
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "copy":
				copyOfChallenge := &BridgeQuickConnectChallenge{self: c, issuer: f.qc, ctx: c.ctx}
				if _, err = copyOfChallenge.Details(); err == nil {
					t.Fatal("copied challenge accepted")
				}
			case "foreign":
				foreign, err := NewBridgeQuickConnectIssuer(t.Context(), f.ds, f.manager, f.security, f.coordinator)
				if err != nil {
					t.Fatal(err)
				}
				defer foreign.Close()
				approver, _ := quickConnectApprover(t, f)
				if a, e := foreign.Approve(approver, c); e == nil || a != nil {
					t.Fatal("foreign challenge accepted")
				}
			case "capacity-per-ip", "capacity-global":
				limit := 5
				if kind == "capacity-global" {
					limit = 64
				}
				for n := 1; n < limit; n++ {
					ip := "198.51.100.1"
					if kind == "capacity-global" {
						ip = fmt.Sprintf("198.51.%d.%d", n/250, n%250+1)
					}
					request, _ := quickConnectIngress(t, "target-device", ip)
					value, e := f.qc.Initiate(request, "target-device")
					if e != nil {
						t.Fatal("within genuine bounded capacity", n, e)
					}
					t.Cleanup(value.Close)
				}
				request, _ := quickConnectIngress(t, "target-device", "198.51.100.1")
				if value, e := f.qc.Initiate(request, "target-device"); !errors.Is(e, ErrBridgeQuickConnectBusy) || value != nil {
					t.Fatal("challenge capacity exceeded", e)
				}
			case "expiry-never-renew":
				a := quickConnectApproval(t, f, c)
				after, e := c.Details()
				if e != nil || !after.ExpiresAt.Equal(before.ExpiresAt) || a.expiry.After(before.ExpiresAt) || before.ExpiresAt.Sub(before.CreatedAt) != bridgeQuickConnectTTL {
					t.Fatal("approval renewed original challenge lifetime", e)
				}
			default:
				if len(before.Secret) != 64 || len(before.Code) != 6 || before.Approved || before.ClientID != "target-device" || quickConnectMintCount(t, f) != 0 {
					t.Fatal("challenge DATA minted authority/session")
				}
			}
		})
	}
}

func TestBridgeQuickConnectApprovalHandoff(t *testing.T) {
	for _, kind := range []string{"adult", "request-cancel-after-handoff", "expired-before-handoff", "full-scope", "wrong-client", "child", "household", "enforced-pin", "unenforced-pin"} {
		t.Run(kind, func(t *testing.T) {
			pin := ""
			if strings.Contains(kind, "pin") {
				pin = "2580"
			}
			f := quickConnectPG(t, pin, kind == "enforced-pin", nil, nil)
			c := quickConnectChallenge(t, f)
			if kind == "full-scope" {
				f.exec(t, `UPDATE sessions SET scope='',scope_resource='' WHERE token=$1`, quickConnectApproverToken)
			}
			if kind == "child" {
				f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='target'`)
			}
			if kind == "household" {
				f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic child',true)`)
			}
			r, release := quickConnectApprover(t, f)
			if kind == "wrong-client" {
				r.Header.Set("X-Client-ID", "wrong-device")
			}
			if kind == "expired-before-handoff" {
				release()
			}
			a, err := f.qc.Approve(r, c)
			want := kind == "adult" || kind == "request-cancel-after-handoff" || kind == "unenforced-pin"
			if !want {
				if err == nil || a != nil || quickConnectMintCount(t, f) != 0 {
					t.Fatal("unsupported approval minted a handoff", kind, err)
				}
				return
			}
			if err != nil || a == nil {
				t.Fatal(err)
			}
			release()
			collector, receipt := quickConnectCollect(t, f, c, a)
			session, err := f.qc.Release(collector, receipt)
			receipt.Close()
			if err != nil || session.Token == "" || session.Scope != models.SessionScopeJellyfin || session.ScopeResource != "target" || session.ClientID != "target-device" || session.IsMaster || session.UserAgent != "Jellyfin: Synthetic Target TV" || quickConnectMintCount(t, f) != 1 {
				t.Fatal("durable handoff did not release exact scoped target", err)
			}
		})
	}
}

func TestBridgeQuickConnectOriginalCorrelations(t *testing.T) {
	for _, kind := range []string{"session-delete", "session-aba", "account-aba", "profile-aba", "settings", "profile-epoch", "session-epoch", "household", "global-fence", "policy-change"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPG(t, "", false, nil, nil)
			c := quickConnectChallenge(t, f)
			a := quickConnectApproval(t, f, c)
			switch kind {
			case "session-delete":
				f.exec(t, `DELETE FROM sessions WHERE token=$1`, quickConnectApproverToken)
			case "session-aba":
				f.exec(t, `WITH original AS(DELETE FROM sessions WHERE token=$1 RETURNING *)INSERT INTO sessions SELECT * FROM original`, quickConnectApproverToken)
			case "account-aba":
				f.exec(t, `WITH original AS(DELETE FROM accounts WHERE id='actor' RETURNING *)INSERT INTO accounts SELECT * FROM original`)
			case "profile-aba":
				f.exec(t, `WITH original AS(DELETE FROM users WHERE id='target' RETURNING *)INSERT INTO users SELECT * FROM original`)
			case "settings":
				f.exec(t, `UPDATE user_settings_versions SET generation=generation+1 WHERE user_id='target'`)
			case "profile-epoch":
				f.exec(t, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','target',1)`)
			case "session-epoch":
				f.exec(t, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('session',$1,1)`, profileUnlockDigest(quickConnectApproverToken))
			case "household":
				f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('member','actor','Synthetic new member')`)
			case "global-fence":
				f.exec(t, `UPDATE accounts SET username=username WHERE id='other'`)
			case "policy-change":
				f.settings.JellyfinBridge.QuickConnect = false
				if err := f.manager.Save(f.settings); err != nil {
					t.Fatal(err)
				}
			}
			r, _ := quickConnectIngress(t, "target-device", "203.0.113.8")
			d, _ := c.Details()
			receipt, err := f.qc.Commit(r, c, a, d.Secret, "target-device")
			if receipt != nil {
				receipt.Close()
			}
			if err == nil || receipt != nil || quickConnectMintCount(t, f) != 0 {
				t.Fatal("original durable approval correlations were refreshed", kind, err)
			}
		})
	}
}

func TestBridgeQuickConnectCollectBindingReplay(t *testing.T) {
	for _, kind := range []string{"wrong-secret", "wrong-client", "copied-receipt", "foreign-issuer", "copied-approval", "replay-commit", "replay-release", "collector-clone", "collector-cancel", "expiry"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPG(t, "", false, nil, nil)
			c := quickConnectChallenge(t, f)
			a := quickConnectApproval(t, f, c)
			r, cancel := quickConnectIngress(t, "target-device", "203.0.113.8")
			d, _ := c.Details()
			client, secret := "target-device", d.Secret
			if kind == "wrong-secret" {
				secret = strings.Repeat("0", 64)
			}
			if kind == "wrong-client" {
				client = "wrong-device"
			}
			if kind == "copied-approval" {
				copyOfApproval := *a
				a = &copyOfApproval
			}
			if kind == "expiry" {
				a.expiry = time.Now().Add(-time.Second)
			}
			receipt, err := f.qc.Commit(r, c, a, secret, client)
			if kind == "wrong-secret" || kind == "wrong-client" || kind == "copied-approval" || kind == "expiry" {
				if receipt != nil {
					receipt.Close()
				}
				if err == nil || receipt != nil || quickConnectMintCount(t, f) != 0 {
					t.Fatal("invalid collection minted", kind, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer receipt.Close()
			if kind == "replay-commit" {
				second, e := f.qc.Commit(r, c, a, secret, client)
				if second != nil {
					second.Close()
				}
				if e == nil || second != nil {
					t.Fatal("approval minted twice")
				}
				return
			}
			issuer := f.qc
			switch kind {
			case "copied-receipt":
				copyOfReceipt := *receipt
				receipt = &copyOfReceipt
			case "foreign-issuer":
				issuer, err = NewBridgeQuickConnectIssuer(t.Context(), f.ds, f.manager, f.security, f.coordinator)
				if err != nil {
					t.Fatal(err)
				}
				defer issuer.Close()
			case "collector-clone":
				r = r.Clone(r.Context())
			case "collector-cancel":
				cancel()
			}
			session, err := issuer.Release(r, receipt)
			if kind == "replay-release" {
				if err != nil || session.Token == "" {
					t.Fatal(err)
				}
				session, err = issuer.Release(r, receipt)
			}
			if err == nil || session.Token != "" {
				t.Fatal("foreign/replayed/changed collection delivered a token", kind, err)
			}
		})
	}
}

func TestBridgeQuickConnectUnknownACK(t *testing.T) {
	for _, kind := range []string{"mint", "release"} {
		t.Run(kind, func(t *testing.T) {
			var enabled, dropped atomic.Bool
			dial := func(ctx context.Context, network, address string) (net.Conn, error) {
				connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &bridgeSessionDropACK{Conn: connection, enabled: &enabled, actual: &parentDropCommit{Conn: connection, dropped: &dropped}}, nil
			}
			f := quickConnectPG(t, "", false, nil, dial)
			c := quickConnectChallenge(t, f)
			a := quickConnectApproval(t, f, c)
			r, _ := quickConnectIngress(t, "target-device", "203.0.113.8")
			d, _ := c.Details()
			if kind == "mint" {
				enabled.Store(true)
			}
			receipt, err := f.qc.Commit(r, c, a, d.Secret, "target-device")
			if kind == "mint" {
				if !errors.Is(err, ErrBridgeQuickConnectUnknown) || receipt != nil || !dropped.Load() || quickConnectMintCount(t, f) != 1 {
					t.Fatal("unknown actual mint ACK delivered authority or claimed rollback", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer receipt.Close()
			enabled.Store(true)
			session, err := f.qc.Release(r, receipt)
			receipt.Close()
			if !errors.Is(err, ErrBridgeQuickConnectUnknown) || session.Token != "" || !dropped.Load() {
				t.Fatal("unknown actual release ACK delivered a token", err)
			}
		})
	}
}

func TestBridgeQuickConnectCleanup(t *testing.T) {
	for _, kind := range []string{"blocked-read-close", "physical-cleanup", "issuer-close", "approval-close"} {
		t.Run(kind, func(t *testing.T) {
			entered, proceed := make(chan struct{}), make(chan struct{})
			var unblockOnce sync.Once
			unblock := func() { unblockOnce.Do(func() { close(proceed) }) }
			defer unblock()
			var armed atomic.Bool
			trace := &bridgeSessionTransactionTrace{end: func(_ *pgx.Conn, sql string, data pgx.TraceQueryEndData) {
				if kind == "blocked-read-close" && strings.HasPrefix(sql, "SELECT CASE WHEN octet_length(s.account_id)") && data.Err == nil && armed.CompareAndSwap(true, false) {
					close(entered)
					<-proceed
				}
			}}
			var first atomic.Bool
			var pointerMu sync.Mutex
			var gate *queryLifecycleGatedConn
			var dial func(context.Context, string, string) (net.Conn, error)
			if kind == "physical-cleanup" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					connection, err := (&net.Dialer{}).DialContext(ctx, network, address)
					if err != nil {
						return nil, err
					}
					if first.CompareAndSwap(false, true) {
						value := &queryLifecycleGatedConn{Conn: connection, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
						pointerMu.Lock()
						gate = value
						pointerMu.Unlock()
						return value, nil
					}
					return connection, nil
				}
			}
			f := quickConnectPG(t, "", false, trace, dial)
			c := quickConnectChallenge(t, f)
			a := quickConnectApproval(t, f, c)
			if kind == "issuer-close" {
				f.qc.Close()
				if _, err := c.Details(); err == nil {
					t.Fatal("issuer Close left challenge usable")
				}
				return
			}
			if kind == "approval-close" {
				a.Close()
				if _, err := c.Details(); err == nil {
					t.Fatal("approval Close left delegation usable")
				}
				return
			}
			if kind == "physical-cleanup" {
				r, receipt := quickConnectCollect(t, f, c, a)
				pointerMu.Lock()
				actual := gate
				pointerMu.Unlock()
				if actual == nil {
					t.Fatal("actual primary socket missing")
				}
				defer actual.release()
				physical := receipt.state.progress.connection.Conn().PgConn()
				actual.armed.Store(true)
				done := make(chan struct{})
				go func() { _, _ = f.qc.Release(r, receipt); close(done) }()
				defer func() { actual.release(); bridgeSessionAwait(t, done); receipt.Close() }()
				bridgeSessionAwait(t, actual.closeEntered)
				closed := make(chan struct{})
				go func() { c.Close(); close(closed) }()
				defer func() { actual.release(); bridgeSessionAwait(t, closed) }()
				select {
				case <-physical.CleanupDone():
					t.Fatal("held actual socket already cleaned")
				default:
				}
				select {
				case <-closed:
					t.Fatal("original challenge refunded before physical cleanup")
				default:
				}
				select {
				case <-done:
					t.Fatal("actual Release escaped physical cleanup")
				default:
				}
				actual.release()
				bridgeSessionAwait(t, physical.CleanupDone())
				bridgeSessionAwait(t, done)
				bridgeSessionAwait(t, closed)
				return
			}
			r, cancel := quickConnectIngress(t, "target-device", "203.0.113.8")
			d, _ := c.Details()
			done := make(chan struct{})
			var err error
			var receipt *BridgeQuickConnectReceipt
			armed.Store(true)
			go func() { receipt, err = f.qc.Commit(r, c, a, d.Secret, "target-device"); close(done) }()
			defer func() {
				cancel()
				unblock()
				bridgeSessionAwait(t, done)
				if receipt != nil {
					receipt.Close()
				}
			}()
			bridgeSessionAwait(t, entered)
			closed := make(chan struct{})
			go func() { c.Close(); close(closed) }()
			defer func() { unblock(); bridgeSessionAwait(t, closed) }()
			bridgeSessionAwait(t, c.ctx.Done())
			select {
			case <-closed:
				t.Fatal("challenge refunded before actual QueryEnd joined")
			default:
			}
			guard, lockErr := f.coordinator.TryWrite(t.Context())
			if guard != nil {
				_ = guard.Close()
			}
			if !errors.Is(lockErr, settingscommit.ErrBusy) {
				t.Fatal("native hold refunded before original SQL completed", lockErr)
			}
			unblock()
			bridgeSessionAwait(t, done)
			bridgeSessionAwait(t, closed)
			if err == nil || receipt != nil || quickConnectMintCount(t, f) != 0 {
				t.Fatal("closed challenge completed a mint", err)
			}
		})
	}
}

func TestBridgeQuickConnectSameTransactionWriters(t *testing.T) {
	for _, kind := range []string{"native-writer-busy", "sessions-writer-busy", "users-writer-busy", "global-policy-busy"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPG(t, "", false, nil, nil)
			c := quickConnectChallenge(t, f)
			a := quickConnectApproval(t, f, c)
			if kind == "native-writer-busy" {
				r, receipt := quickConnectCollect(t, f, c, a)
				_ = r
				if err := f.manager.Save(f.settings); err == nil {
					t.Fatal("receipt did not retain original native hold")
				}
				receipt.Close()
				return
			}
			tx, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			relation := "sessions"
			if kind == "users-writer-busy" {
				relation = "users"
			}
			if kind == "global-policy-busy" {
				relation = "regional_profile_policies"
			}
			if _, err = tx.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, relation}.Sanitize()+" IN ROW EXCLUSIVE MODE"); err != nil {
				t.Fatal(err)
			}
			r, _ := quickConnectIngress(t, "target-device", "203.0.113.8")
			d, _ := c.Details()
			receipt, err := f.qc.Commit(r, c, a, d.Secret, "target-device")
			if receipt != nil {
				receipt.Close()
			}
			if !errors.Is(err, ErrBridgeQuickConnectBusy) || receipt != nil || quickConnectMintCount(t, f) != 0 {
				t.Fatal("actual writer was not excluded by original NOWAIT TX guards", kind, err)
			}
		})
	}
}

func TestBridgeQuickConnectReceiptOriginalExpiry(t *testing.T) {
	for _, kind := range []string{"earlier-approver-session", "earlier-approver-account", "copied-receipt", "zero-receipt", "closed-receipt"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "zero-receipt" {
				var empty BridgeQuickConnectReceipt
				if !empty.ExpiresAt().IsZero() {
					t.Fatal("zero receipt has a deadline")
				}
				return
			}
			f := quickConnectPG(t, "", false, nil, nil)
			want := time.Now().Add(18 * time.Second).UTC().Truncate(time.Microsecond)
			if kind == "earlier-approver-session" || kind == "earlier-approver-account" {
				f.exec(t, `UPDATE sessions SET expires_at=$1 WHERE token=$2`, want, quickConnectApproverToken)
			}
			if kind == "earlier-approver-account" {
				want = want.Add(-3 * time.Second)
				f.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, want)
			}
			c := quickConnectChallenge(t, f)
			a := quickConnectApproval(t, f, c)
			r, receipt := quickConnectCollect(t, f, c, a)
			original := receipt.ExpiresAt()
			if original.IsZero() || original.After(c.expiry) || original.After(a.expiry) {
				t.Fatal("receipt deadline omitted an original minimum")
			}
			switch kind {
			case "earlier-approver-session", "earlier-approver-account":
				if !original.Equal(want) {
					t.Fatal("actual SQL receipt context extended approving session/account expiry")
				}
				session, err := f.qc.Release(r, receipt)
				if err != nil || session.Token == "" {
					t.Fatal("genuine bounded release", err)
				}
			case "copied-receipt":
				copyOfReceipt := *receipt
				if !copyOfReceipt.ExpiresAt().IsZero() {
					t.Fatal("copied receipt exposed a trusted deadline")
				}
				if session, err := f.qc.Release(r, &copyOfReceipt); err == nil || session.Token != "" {
					t.Fatal("copied expiry receipt authorized")
				}
			case "closed-receipt":
				receipt.Close()
				if !receipt.ExpiresAt().Equal(original) {
					t.Fatal("intentional cleanup recomputed/extended original expiry")
				}
				if session, err := f.qc.Release(r, receipt); err == nil || session.Token != "" {
					t.Fatal("closed expiry receipt authorized")
				}
			}
		})
	}
}

func TestBridgeQuickConnectOriginalPeerBucket(t *testing.T) {
	for _, kind := range []string{"ipv4-ports", "ipv6-ports", "mapped-ipv4", "bare-and-port", "invalid-peer", "forwarded-ignored"} {
		t.Run(kind, func(t *testing.T) {
			f := quickConnectPG(t, "", false, nil, nil)
			if kind == "invalid-peer" {
				for _, address := range []string{"", "synthetic.invalid:443", "198.51.100.2:70000", "[2001:db8::1", "198.51.100.2/24", " 198.51.100.2"} {
					r, _ := quickConnectIngress(t, "target-device", address)
					if c, err := f.qc.Initiate(r, "target-device"); err == nil || c != nil {
						if c != nil {
							c.Close()
						}
						t.Fatal("invalid actual peer accepted")
					}
				}
				return
			}
			for n := 0; n < 6; n++ {
				address := fmt.Sprintf("198.51.100.2:%d", 1200+n)
				switch kind {
				case "ipv6-ports":
					address = fmt.Sprintf("[2001:db8::1]:%d", 1200+n)
					if n == 0 {
						address = "2001:db8::1"
					}
					if n%2 == 1 {
						address = fmt.Sprintf("[2001:0db8:0000:0000:0000:0000:0000:0001]:%d", 1200+n)
					}
				case "mapped-ipv4":
					if n%2 == 1 {
						address = fmt.Sprintf("[::ffff:198.51.100.2]:%d", 1200+n)
					}
					if n == 2 {
						address = fmt.Sprintf("[0:0:0:0:0:ffff:c633:6402]:%d", 1200+n)
					}
				case "bare-and-port":
					if n%2 == 0 {
						address = "198.51.100.2"
					}
				}
				r, _ := quickConnectIngress(t, "target-device", address)
				if kind == "forwarded-ignored" {
					r.Header.Set("X-Forwarded-For", fmt.Sprintf("203.0.113.%d", n+1))
					r.Header.Set("Forwarded", fmt.Sprintf("for=203.0.113.%d", n+1))
				}
				c, err := f.qc.Initiate(r, "target-device")
				if n == 5 {
					if !errors.Is(err, ErrBridgeQuickConnectBusy) || c != nil {
						if c != nil {
							c.Close()
						}
						t.Fatal("same actual host escaped capacity through port/presentation/forwarded change", kind, err)
					}
					continue
				}
				if err != nil || c == nil {
					t.Fatal("genuine same-host challenge below bound", kind, n, err)
				}
				t.Cleanup(c.Close)
				details, err := c.Details()
				if err != nil || details.IPAddress != address || r.RemoteAddr != address {
					t.Fatal("private bucket rewrote original peer binding/descriptive data", kind, err)
				}
			}
		})
	}
}
