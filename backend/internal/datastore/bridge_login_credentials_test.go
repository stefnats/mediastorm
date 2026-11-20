package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"novastream/internal/auth"
	"novastream/models"
)

func bridgeCredentialPG(t *testing.T, pin string, length int, tracer pgx.QueryTracer) *ratingPGFixture {
	t.Helper()
	f := familyScopedPG(t, tracer)
	f.exec(t, "DELETE FROM sessions")
	hash := ""
	if pin != "" {
		raw, err := bcrypt.GenerateFromPassword([]byte(pin), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		hash = string(raw)
	}
	f.exec(t, "UPDATE users SET pin_hash=$1,pin_length=$2 WHERE id='target'", hash, length)
	return f
}
func bridgeCredentialRequest(ctx context.Context) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/users/authenticatebyname", nil).WithContext(ctx)
	r.Header.Set("Authorization", `MediaBrowser DeviceId="synthetic-device", Client="Synthetic"`)
	return r
}
func bridgeCredentialPrepare(t *testing.T, f *ratingPGFixture, r *http.Request) *BridgeLoginCredentialIntent {
	t.Helper()
	intent, err := f.ds.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
	if err != nil {
		t.Fatal("prepare genuine pre-auth PG credential observation", err)
	}
	t.Cleanup(intent.Close)
	return intent
}
func bridgeCredentialRows(t *testing.T, f *ratingPGFixture) []byte {
	t.Helper()
	var raw []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT jsonb_build_object('accounts',(SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM accounts a),'users',(SELECT jsonb_agg(to_jsonb(u) ORDER BY id) FROM users u),'sessions',(SELECT jsonb_agg(to_jsonb(s) ORDER BY token) FROM sessions s),'versions',(SELECT jsonb_agg(to_jsonb(v) ORDER BY user_id) FROM user_settings_versions v),'revocations',(SELECT jsonb_agg(to_jsonb(p) ORDER BY scope,scope_key) FROM profile_unlock_revocations p))`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestBridgeCredentialActualPGKindsStayTokenFreeAndNeverWrite(t *testing.T) {
	for _, kind := range []bridgeCredentialKind{bridgeCredentialAccount, bridgeCredentialCombined, bridgeCredentialPIN} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			f := bridgeCredentialPG(t, "2580", 4, nil)
			r := bridgeCredentialRequest(t.Context())
			before := bridgeCredentialRows(t, f)
			intent := bridgeCredentialPrepare(t, f, r)
			var proof *BridgeLoginCredentialProof
			var err error
			switch kind {
			case bridgeCredentialAccount:
				proof, err = f.ds.VerifyBridgeAccountPassword(r, intent, " \t"+parentTestPassword+"\n")
			case bridgeCredentialCombined:
				proof, err = f.ds.VerifyBridgePasswordWithPIN(r, intent, parentTestPassword+"2580")
			case bridgeCredentialPIN:
				proof, err = f.ds.VerifyBridgeProfilePIN(r, intent, "2580")
			}
			if err != nil || proof == nil || proof.state.kind != kind || proof.Check(t.Context()) != nil {
				t.Fatal("actual PG comparison did not retain its distinct evidence", err)
			}
			if !bytes.Equal(before, bridgeCredentialRows(t, f)) {
				t.Fatal("credential evidence changed users/PIN length/settings/grants/sessions")
			}
			for _, key := range []any{auth.ContextKeySession, auth.ContextKeyAccountID, auth.ContextKeyIsMaster, pgRequestFrameKey{}} {
				if proof.Context().Value(key) != nil {
					t.Fatal("credential lifetime exposed auth/frame values")
				}
			}
			if _, err = f.ds.VerifyBridgeAccountPassword(r, intent, parentTestPassword); !errors.Is(err, ErrBridgeCredentialUsed) {
				t.Fatal("one intent verified a second credential kind", err)
			}
		})
	}
}
func TestBridgeCredentialKnownPINLengthsAndRawUTF8SuffixMatchLegacy(t *testing.T) {
	for _, tc := range []struct {
		name, pin string
		length    int
		want      bool
	}{
		{"known3 trailing spaces", "7  ", 3, true}, {"known9", "abcdefghi", 9, true}, {"known UTF8 bytes", "Ä雪 ", len("Ä雪 "), true}, {"known raw bytes", string([]byte{255, 0, 65}), 3, true}, {"unknown4", "2468", 0, true}, {"negative unknown4", "2468", -1, true}, {"unknown9 stays outside fallback", "abcdefghi", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := bridgeCredentialPG(t, tc.pin, tc.length, nil)
			r := bridgeCredentialRequest(t.Context())
			i := bridgeCredentialPrepare(t, f, r)
			before := bridgeCredentialRows(t, f)
			proof, err := f.ds.VerifyBridgePasswordWithPIN(r, i, " \t"+parentTestPassword+" \n"+tc.pin)
			if (err == nil) != tc.want || (proof != nil) != tc.want {
				t.Fatal("known byte length/raw suffix/unknown4..8 compatibility changed", err)
			}
			if !bytes.Equal(before, bridgeCredentialRows(t, f)) {
				t.Fatal("legacy length was silently written during credential verification")
			}
		})
	}
}
func TestBridgeCredentialWrongEmptyNoPINAndUnsupportedHashesFailClosed(t *testing.T) {
	for _, kind := range []string{"wrong account", "blank account", "wrong PIN", "empty PIN", "no PIN", "combined no PIN", "oversize input", "account cost15", "PIN cost15", "malformed account"} {
		t.Run(kind, func(t *testing.T) {
			pin := "2580"
			if kind == "no PIN" || kind == "combined no PIN" {
				pin = ""
			}
			f := bridgeCredentialPG(t, pin, 4, nil)
			if kind == "account cost15" {
				f.exec(t, "UPDATE accounts SET password_hash='$2a$15$'||substr(password_hash,8) WHERE id='actor'")
			}
			if kind == "PIN cost15" {
				f.exec(t, "UPDATE users SET pin_hash='$2a$15$'||substr(pin_hash,8) WHERE id='target'")
			}
			if kind == "malformed account" {
				f.exec(t, "UPDATE accounts SET password_hash='malformed' WHERE id='actor'")
			}
			r := bridgeCredentialRequest(t.Context())
			before := bridgeCredentialRows(t, f)
			i, err := f.ds.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
			if strings.Contains(kind, "cost15") || kind == "malformed account" {
				if i != nil || err == nil {
					if i != nil {
						i.Close()
					}
					t.Fatal("unsupported hash reached an intent")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			var proof *BridgeLoginCredentialProof
			switch kind {
			case "wrong account":
				proof, err = f.ds.VerifyBridgeAccountPassword(r, i, "incorrect")
			case "blank account":
				proof, err = f.ds.VerifyBridgeAccountPassword(r, i, " \t\n")
			case "wrong PIN":
				proof, err = f.ds.VerifyBridgeProfilePIN(r, i, "0000")
			case "empty PIN":
				proof, err = f.ds.VerifyBridgeProfilePIN(r, i, "")
			case "no PIN":
				proof, err = f.ds.VerifyBridgeProfilePIN(r, i, "2580")
			case "combined no PIN":
				proof, err = f.ds.VerifyBridgePasswordWithPIN(r, i, parentTestPassword+"2580")
			case "oversize input":
				proof, err = f.ds.VerifyBridgeAccountPassword(r, i, strings.Repeat("x", 1025))
			}
			if proof != nil || err == nil || i.Context().Err() == nil {
				t.Fatal("invalid verification produced evidence or reusable attempt", err)
			}
			if !bytes.Equal(before, bridgeCredentialRows(t, f)) {
				t.Fatal("denied credential verification changed storage")
			}
		})
	}
}
func TestBridgeCredentialActualOwnershipAndInactiveHouseholdRequiredWithoutSession(t *testing.T) {
	for _, kind := range []string{"owned master", "foreign master", "child", "sibling child", "expired account", "missing profile", "decorative master without password"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeCredentialPG(t, "", 0, nil)
			account := "actor"
			profile := "target"
			switch kind {
			case "owned master":
				f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
			case "foreign master":
				f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor';UPDATE users SET account_id='other' WHERE id='target'")
			case "child":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='target'")
			case "sibling child":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second'")
			case "expired account":
				f.exec(t, "UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'")
			case "missing profile":
				profile = "missing"
			}
			r := bridgeCredentialRequest(t.Context())
			if kind == "decorative master without password" {
				ctx := context.WithValue(r.Context(), auth.ContextKeyIsMaster, true)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "actor")
				ctx = context.WithValue(ctx, auth.ContextKeySession, models.Session{AccountID: "actor", IsMaster: true})
				r = r.WithContext(ctx)
			}
			i, err := f.ds.PrepareBridgeLoginCredentials(r, account, profile, "synthetic-device")
			if kind != "owned master" && kind != "decorative master without password" {
				if i != nil || err == nil {
					if i != nil {
						i.Close()
					}
					t.Fatal("foreign/active/unknown selection acquired observation")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			credential := parentTestPassword
			if kind == "decorative master without password" {
				credential = "wrong"
			}
			proof, err := f.ds.VerifyBridgeAccountPassword(r, i, credential)
			if (proof != nil) != (kind == "owned master") || (err == nil) != (kind == "owned master") {
				t.Fatal("master/session decoration substituted for actual PG credential", err)
			}
		})
	}
}
func TestBridgeCredentialExactRequestCarrierAndCallerCancellationDoNotTransfer(t *testing.T) {
	f := bridgeCredentialPG(t, "2580", 4, nil)
	r := bridgeCredentialRequest(t.Context())
	i := bridgeCredentialPrepare(t, f, r)
	for _, bad := range []*http.Request{r.Clone(r.Context()), r.WithContext(context.WithoutCancel(r.Context()))} {
		if proof, err := f.ds.VerifyBridgeAccountPassword(bad, i, parentTestPassword); proof != nil || err == nil {
			t.Fatal("replacement request/context transferred intent")
		}
		if i.Check(t.Context()) != nil {
			t.Fatal("foreign request retired intact original")
		}
	}
	original := r.Header.Get("Authorization")
	r.Header.Set("Authorization", `MediaBrowser DeviceId="changed"`)
	if proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword); proof != nil || err == nil {
		t.Fatal("changed carrier was accepted")
	}
	r.Header.Set("Authorization", original)
	if i.Check(t.Context()) != nil {
		t.Fatal("rejected carrier consumed original before verification")
	}
	proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(t.Context())
	cancel()
	if proof.Check(caller) == nil || proof.Check(t.Context()) != nil {
		t.Fatal("departed current-check caller retired intact evidence")
	}
}

func bridgeCredentialCompareGate(i *BridgeLoginCredentialIntent) (<-chan struct{}, func()) {
	entered := make(chan struct{})
	deliver := make(chan struct{})
	real := i.state.compare
	var once sync.Once
	i.state.compare = func(hash, password []byte) error {
		result := real(hash, password)
		once.Do(func() { close(entered); <-deliver })
		return result
	}
	var release sync.Once
	return entered, func() { release.Do(func() { close(deliver) }) }
}
func bridgeCredentialWait(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}
func TestBridgeCredentialCloseJoinsActualBcryptResultBeforeLaneAndIntentRefund(t *testing.T) {
	f := bridgeCredentialPG(t, "", 0, nil)
	r := bridgeCredentialRequest(t.Context())
	i := bridgeCredentialPrepare(t, f, r)
	entered, release := bridgeCredentialCompareGate(i)
	defer release()
	verified := make(chan error, 1)
	go func() {
		proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
		if proof != nil {
			verified <- nil
		} else {
			verified <- err
		}
	}()
	bridgeCredentialWait(t, entered, "real bcrypt did not reach held result delivery")
	closed := make(chan struct{})
	go func() { i.Close(); close(closed) }()
	bridgeCredentialWait(t, i.Context().Done(), "Close did not signal original")
	select {
	case <-closed:
		t.Fatal("Close overtook actual compare call return")
	default:
	}
	bridgeCredentialAdmission.Lock()
	_, retained := bridgeCredentialAdmission.intents[i]
	bridgeCredentialAdmission.Unlock()
	if !retained || len(bridgeCredentialCPU) != 1 {
		t.Fatal("actual compare capacity refunded before result returned")
	}
	release()
	bridgeCredentialWait(t, closed, "Close failed after actual compare returned")
	if <-verified == nil {
		t.Fatal("closed comparison produced evidence")
	}
	if len(bridgeCredentialCPU) != 0 {
		t.Fatal("actual completed compare leaked CPU lane")
	}
}
func TestBridgeCredentialChangedRowsDuringActualCompareCannotPublishOrRevive(t *testing.T) {
	for _, change := range []string{"password ABA", "PIN ABA", "length ABA", "membership ABA", "child ABA", "epoch", "request carrier"} {
		t.Run(change, func(t *testing.T) {
			f := bridgeCredentialPG(t, "2580", 4, nil)
			r := bridgeCredentialRequest(t.Context())
			i := bridgeCredentialPrepare(t, f, r)
			entered, release := bridgeCredentialCompareGate(i)
			defer release()
			done := make(chan error, 1)
			go func() {
				proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
				if proof != nil {
					done <- nil
				} else {
					done <- err
				}
			}()
			bridgeCredentialWait(t, entered, "real comparison did not reach controlled result")
			switch change {
			case "password ABA":
				f.exec(t, "UPDATE accounts SET password_hash='temporary' WHERE id='actor'")
				f.exec(t, "UPDATE accounts SET password_hash=$1 WHERE id='actor'", f.hash)
			case "PIN ABA":
				f.exec(t, "UPDATE users SET pin_hash=pin_hash WHERE id='target'")
			case "length ABA":
				f.exec(t, "UPDATE users SET pin_length=9 WHERE id='target';UPDATE users SET pin_length=4 WHERE id='target'")
			case "membership ABA":
				f.exec(t, "UPDATE users SET account_id='other' WHERE id='target';UPDATE users SET account_id='actor' WHERE id='target'")
			case "child ABA":
				f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second';UPDATE users SET is_kids_profile=false WHERE id='own-second'")
			case "epoch":
				if err := revokeProfileUnlocks(t.Context(), f.ds.pool, "profile", "target"); err != nil {
					t.Fatal(err)
				}
			case "request carrier":
				r.Header.Set("Authorization", `MediaBrowser DeviceId="other"`)
			}
			release()
			if err := <-done; err == nil || i.Context().Err() == nil {
				t.Fatal("late original mutation produced evidence", change, err)
			}
			if i.Check(t.Context()) == nil {
				t.Fatal("retired evidence revived")
			}
		})
	}
}
func TestBridgeCredentialTwoRealCPULanesBoundThirdWaiterAndCancellation(t *testing.T) {
	f := bridgeCredentialPG(t, "", 0, nil)
	var intents []*BridgeLoginCredentialIntent
	var releases []func()
	var done []chan error
	defer func() {
		for _, release := range releases {
			release()
		}
		for _, i := range intents {
			i.Close()
		}
	}()
	for range 2 {
		r := bridgeCredentialRequest(t.Context())
		i := bridgeCredentialPrepare(t, f, r)
		intents = append(intents, i)
		entered, release := bridgeCredentialCompareGate(i)
		releases = append(releases, release)
		returned := make(chan error, 1)
		done = append(done, returned)
		go func() { _, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword); returned <- err }()
		bridgeCredentialWait(t, entered, "actual lane comparison not reached")
	}
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := bridgeCredentialRequest(caller)
	third := bridgeCredentialPrepare(t, f, r)
	intents = append(intents, third)
	var called atomic.Int64
	real := third.state.compare
	third.state.compare = func(h, p []byte) error { called.Add(1); return real(h, p) }
	returned := make(chan error, 1)
	go func() { _, err := f.ds.VerifyBridgeAccountPassword(r, third, parentTestPassword); returned <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		third.state.mu.Lock()
		waiting := third.state.waitingCPU
		third.state.mu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("third verification did not wait behind both actual lanes")
		}
		time.Sleep(time.Millisecond)
	}
	if len(bridgeCredentialCPU) != 2 || called.Load() != 0 {
		t.Fatal("third actual bcrypt bypassed global lane bound")
	}
	cancel()
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("canceled lane waiter succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled lane waiter survived")
	}
	if called.Load() != 0 || len(bridgeCredentialCPU) != 2 {
		t.Fatal("canceled waiter ran bcrypt or refunded another lane")
	}
	for _, release := range releases {
		release()
	}
	for _, returned := range done {
		if err := <-returned; err != nil {
			t.Fatal("healthy real lane failed", err)
		}
	}
}

func TestBridgeCredentialActualGlobalAndAccountAdmissionBeforeWork(t *testing.T) {
	f := bridgeCredentialPG(t, "", 0, nil)
	for n := range 9 {
		account := fmt.Sprintf("cap-account-%d", n)
		profile := fmt.Sprintf("cap-profile-%d", n)
		f.exec(t, "INSERT INTO accounts(id,username,password_hash)VALUES($1,$2,$3)", account, "Synthetic "+account, f.hash)
		f.exec(t, "INSERT INTO users(id,account_id,name)VALUES($1,$2,'Synthetic bounded profile')", profile, account)
	}
	var intents []*BridgeLoginCredentialIntent
	defer func() {
		for _, i := range intents {
			i.Close()
		}
	}()
	for account := range 8 {
		for range 8 {
			r := bridgeCredentialRequest(t.Context())
			i, err := f.ds.PrepareBridgeLoginCredentials(r, fmt.Sprintf("cap-account-%d", account), fmt.Sprintf("cap-profile-%d", account), "synthetic-device")
			if err != nil {
				t.Fatal("genuine capacity reservation", err)
			}
			intents = append(intents, i)
		}
		if account == 0 {
			if i, err := f.ds.PrepareBridgeLoginCredentials(bridgeCredentialRequest(t.Context()), "cap-account-0", "cap-profile-0", "synthetic-device"); i != nil || !errors.Is(err, ErrBridgeCredentialBusy) {
				if i != nil {
					i.Close()
				}
				t.Fatal("ninth same-account intent bypassed capacity", err)
			}
		}
	}
	if i, err := f.ds.PrepareBridgeLoginCredentials(bridgeCredentialRequest(t.Context()), "cap-account-8", "cap-profile-8", "synthetic-device"); i != nil || !errors.Is(err, ErrBridgeCredentialBusy) {
		if i != nil {
			i.Close()
		}
		t.Fatal("65th global intent bypassed capacity despite different account", err)
	}
	intents[0].Close()
	i, err := f.ds.PrepareBridgeLoginCredentials(bridgeCredentialRequest(t.Context()), "cap-account-8", "cap-profile-8", "synthetic-device")
	if err != nil {
		t.Fatal("actual closed original did not refund capacity", err)
	}
	defer i.Close()
}
func TestBridgeCredentialOriginalRequestAndAccountExpiryRejectLateComparison(t *testing.T) {
	for _, kind := range []string{"request", "account", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeCredentialPG(t, "", 0, nil)
			expiry := time.Now().UTC().Add(750 * time.Millisecond).Truncate(time.Microsecond)
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			if kind == "request" {
				var stop context.CancelFunc
				caller, stop = context.WithDeadline(caller, expiry)
				defer stop()
			}
			if kind == "account" {
				f.exec(t, "UPDATE accounts SET expires_at=$1 WHERE id='actor'", expiry)
			}
			r := bridgeCredentialRequest(caller)
			i := bridgeCredentialPrepare(t, f, r)
			if kind != "cancel" && !i.ExpiresAt().Equal(expiry) {
				t.Fatal("original minimum expiry changed")
			}
			if i.ExpiresAt().After(time.Now().Add(30 * time.Second)) {
				t.Fatal("capture exceeded30second cap")
			}
			entered, release := bridgeCredentialCompareGate(i)
			defer release()
			done := make(chan error, 1)
			go func() {
				proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
				if proof != nil {
					done <- nil
				} else {
					done <- err
				}
			}()
			bridgeCredentialWait(t, entered, "actual compare not held before expiry")
			if kind == "cancel" {
				cancel()
			}
			bridgeCredentialWait(t, i.Context().Done(), "original deadline/cancel did not reach clean lifetime")
			if len(bridgeCredentialCPU) != 1 {
				t.Fatal("original expiry refunded actual held comparison")
			}
			release()
			if <-done == nil {
				t.Fatal("late comparison published expired credential evidence")
			}
		})
	}
}
func TestBridgeCredentialCurrentFailureAndFreshCancelledCheckNeverAdoptNewRows(t *testing.T) {
	f := bridgeCredentialPG(t, "", 0, nil)
	r := bridgeCredentialRequest(t.Context())
	i := bridgeCredentialPrepare(t, f, r)
	proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	caller, cancel := context.WithCancel(t.Context())
	cancel()
	if proof.Check(caller) == nil || proof.Check(t.Context()) != nil {
		t.Fatal("caller-only cancellation retired evidence")
	}
	f.exec(t, "ALTER TABLE user_settings_versions RENAME TO unavailable_bridge_settings_versions")
	if proof.Check(t.Context()) == nil || proof.Context().Err() == nil {
		t.Fatal("current unavailable source used prior credential observation")
	}
	f.exec(t, "ALTER TABLE unavailable_bridge_settings_versions RENAME TO user_settings_versions")
	if proof.Check(t.Context()) == nil {
		t.Fatal("availability revived retired credential evidence")
	}
}
func TestBridgeCredentialOwnedCheckOperationsBoundBeforePGAndCloseJoins(t *testing.T) {
	f := bridgeCredentialPG(t, "", 0, nil)
	r := bridgeCredentialRequest(t.Context())
	i := bridgeCredentialPrepare(t, f, r)
	_, relation := familyScopedHoldAccounts(t, f)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if i.Check(context.Background()) == nil {
				t.Error("closed held query succeeded")
			}
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		i.state.mu.Lock()
		count := i.state.operations
		i.state.mu.Unlock()
		if count == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("two actual operations not admitted before PG")
		}
		time.Sleep(time.Millisecond)
	}
	familyScopedWaitPending(t, f, relation)
	if err := i.Check(t.Context()); !errors.Is(err, ErrBridgeCredentialBusy) {
		t.Fatal("third operation bypassed bound", err)
	}
	closed := make(chan struct{})
	go func() { i.Close(); close(closed) }()
	bridgeCredentialWait(t, closed, "Close failed to join actual locked PG checks")
	wait.Wait()
	var pending int
	if err := f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("credential Close refunded before actual PG cancel/drain", err, pending)
	}
}
func TestBridgeCredentialCloseJoinsActualFailedPGCleanupDone(t *testing.T) {
	f := bridgeCredentialPG(t, "", 0, nil)
	cfg := f.cfg.Copy()
	cfg.MaxConns = 1
	dial := cfg.ConnConfig.DialFunc
	var first atomic.Bool
	var mu sync.Mutex
	var actual *queryLifecycleGatedConn
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		if first.CompareAndSwap(false, true) {
			gate := &queryLifecycleGatedConn{Conn: conn, closeEntered: make(chan struct{}), closeGate: make(chan struct{})}
			mu.Lock()
			actual = gate
			mu.Unlock()
			return gate, nil
		}
		return conn, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	f.ds = &DataStore{pool: pool}
	r := bridgeCredentialRequest(t.Context())
	i := bridgeCredentialPrepare(t, f, r)
	owned, err := pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	pg := owned.Conn().PgConn()
	owned.Release()
	mu.Lock()
	gate := actual
	mu.Unlock()
	if gate == nil {
		t.Fatal("actual primary connection missing")
	}
	defer gate.release()
	gate.armed.Store(true)
	checked := make(chan error, 1)
	go func() { checked <- i.Check(context.Background()) }()
	bridgeCredentialWait(t, gate.closeEntered, "real failed query did not reach physical Close")
	if !pg.IsClosed() {
		t.Fatal("actual primary query connection did not fail")
	}
	select {
	case <-pg.CleanupDone():
		t.Fatal("driver cleanup completed before actual socket close")
	default:
	}
	closed := make(chan struct{})
	go func() { i.Close(); close(closed) }()
	bridgeCredentialWait(t, i.Context().Done(), "Close did not signal original")
	select {
	case <-closed:
		t.Fatal("Close overtook real driver cleanup")
	default:
	}
	bridgeCredentialAdmission.Lock()
	_, retained := bridgeCredentialAdmission.intents[i]
	bridgeCredentialAdmission.Unlock()
	if !retained {
		t.Fatal("capacity refunded before CleanupDone")
	}
	gate.release()
	bridgeCredentialWait(t, closed, "Close failed after actual connection cleanup")
	if <-checked == nil {
		t.Fatal("failed physical query published current evidence")
	}
	select {
	case <-pg.CleanupDone():
	default:
		t.Fatal("credential admission refunded before actual CleanupDone")
	}
}
func TestBridgeCredentialUnverifiedImportedCopiedAndOversizedRequestArePowerless(t *testing.T) {
	var zero BridgeLoginCredentialIntent
	var proof BridgeLoginCredentialProof
	if json.Unmarshal([]byte("{}"), &zero) == nil || json.Unmarshal([]byte("{}"), &proof) == nil || zero.Check(t.Context()) == nil || proof.Check(t.Context()) == nil || zero.Context().Err() == nil || !proof.ExpiresAt().IsZero() {
		t.Fatal("imported zero values gained evidence")
	}
	f := bridgeCredentialPG(t, "", 0, nil)
	r := bridgeCredentialRequest(t.Context())
	i := bridgeCredentialPrepare(t, f, r)
	copyIntent := *i
	copyIntent.Close()
	if copyIntent.Check(t.Context()) == nil || i.Check(t.Context()) != nil {
		t.Fatal("copied intent gained or retired original")
	}
	p, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	copyProof := *p
	copyProof.Close()
	if copyProof.Check(t.Context()) == nil || p.Check(t.Context()) != nil {
		t.Fatal("copied proof gained or retired original")
	}
	for _, value := range []any{i, *i, &copyIntent, copyIntent, p, *p, &copyProof, copyProof} {
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("private evidence serialized")
		}
		for _, format := range []string{"%+v", "%#v"} {
			got := fmt.Sprintf(format, value)
			if got != "BridgeLoginCredentialIntent(opaque)" && got != "BridgeLoginCredentialProof(opaque)" {
				t.Fatal("private evidence formatted fields")
			}
		}
	}
	oversized := bridgeCredentialRequest(t.Context())
	oversized.Header.Set("Authorization", strings.Repeat("x", 4097))
	if value, err := f.ds.PrepareBridgeLoginCredentials(oversized, "actor", "target", "synthetic-device"); value != nil || err == nil {
		if value != nil {
			value.Close()
		}
		t.Fatal("oversized carrier captured")
	}
}

func TestBridgeCredentialRealComparisonCountsRespectPerKindBounds(t *testing.T) {
	for _, tc := range []struct {
		name   string
		kind   bridgeCredentialKind
		length int
		input  string
		max    int
	}{
		{"account", bridgeCredentialAccount, 4, parentTestPassword, 1}, {"PIN", bridgeCredentialPIN, 4, "2580", 1}, {"known combined", bridgeCredentialCombined, 4, parentTestPassword + "2580", 2}, {"unknown combined", bridgeCredentialCombined, 0, parentTestPassword + "2580", 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := bridgeCredentialPG(t, "2580", tc.length, nil)
			r := bridgeCredentialRequest(t.Context())
			i := bridgeCredentialPrepare(t, f, r)
			real := i.state.compare
			count := 0
			i.state.compare = func(h, p []byte) error { count++; return real(h, p) }
			var proof *BridgeLoginCredentialProof
			var err error
			switch tc.kind {
			case bridgeCredentialAccount:
				proof, err = f.ds.VerifyBridgeAccountPassword(r, i, tc.input)
			case bridgeCredentialPIN:
				proof, err = f.ds.VerifyBridgeProfilePIN(r, i, tc.input)
			case bridgeCredentialCombined:
				proof, err = f.ds.VerifyBridgePasswordWithPIN(r, i, tc.input)
			}
			if err != nil || proof == nil || count < 1 || count > tc.max {
				t.Fatal("actual bcrypt comparison budget/kind changed", err, count)
			}
		})
	}
}

func TestBridgeCredentialSupportedStoredLayoutsUseActualPGAndRealBcrypt(t *testing.T) {
	for _, layout := range []string{"legacy2", "2a", "2b", "2y", "salt unused bits preserve Go"} {
		t.Run(layout, func(t *testing.T) {
			f := bridgeCredentialPG(t, "2580", 4, nil)
			var accountHash, pinHash string
			if err := f.ds.pool.QueryRow(t.Context(), "SELECT a.password_hash,u.pin_hash FROM accounts a JOIN users u ON u.account_id=a.id WHERE a.id='actor' AND u.id='target'").Scan(&accountHash, &pinHash); err != nil {
				t.Fatal(err)
			}
			convert := func(hash string) string {
				switch layout {
				case "legacy2":
					return "$2$" + hash[4:]
				case "2a", "2b", "2y":
					return "$" + layout + "$" + hash[4:]
				default:
					// Go's non-strict base64 decoder ignores unused salt bits.
					// Retain that compatibility instead of inventing a new rule.
					const alphabet = "./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
					changed := []byte(hash)
					changed[28] = alphabet[strings.IndexByte(alphabet, changed[28])+1]
					return string(changed)
				}
			}
			f.exec(t, "UPDATE accounts SET password_hash=$1 WHERE id='actor'", convert(accountHash))
			f.exec(t, "UPDATE users SET pin_hash=$1 WHERE id='target'", convert(pinHash))
			r := bridgeCredentialRequest(t.Context())
			i := bridgeCredentialPrepare(t, f, r)
			count := 0
			real := i.state.compare
			i.state.compare = func(hash, raw []byte) error { count++; return real(hash, raw) }
			before := bridgeCredentialRows(t, f)
			proof, err := f.ds.VerifyBridgePasswordWithPIN(r, i, parentTestPassword+"2580")
			if err != nil || proof == nil || count != 2 || proof.Check(t.Context()) != nil {
				t.Fatal("supported stored layout did not perform both real comparisons", err, count)
			}
			if !bytes.Equal(before, bridgeCredentialRows(t, f)) {
				t.Fatal("supported layout verification rewrote legacy credential data")
			}
		})
	}
}

func TestBridgeCredentialMalformedStoredLayoutsDenyBeforePrepareAndCPU(t *testing.T) {
	for _, shape := range []string{"account suffix", "PIN suffix", "account salt alphabet", "PIN salt alphabet", "account checksum alphabet", "PIN checksum alphabet", "account delimiter", "PIN delimiter", "account major", "PIN minor", "account truncate", "PIN truncate", "account cost digits", "PIN cost digits"} {
		t.Run(shape, func(t *testing.T) {
			f := bridgeCredentialPG(t, "2580", 4, nil)
			r := bridgeCredentialRequest(t.Context())
			baseline := bridgeCredentialPrepare(t, f, r)
			good, err := f.ds.VerifyBridgePasswordWithPIN(r, baseline, parentTestPassword+"2580")
			if err != nil || good == nil {
				t.Fatal("original valid PG hashes did not actually verify", err)
			}
			good.Close()
			pending := bridgeCredentialPrepare(t, f, r)
			count := 0
			real := pending.state.compare
			pending.state.compare = func(hash, raw []byte) error { count++; return real(hash, raw) }
			var hash string
			query := "SELECT password_hash FROM accounts WHERE id='actor'"
			update := "UPDATE accounts SET password_hash=$1 WHERE id='actor'"
			if strings.HasPrefix(shape, "PIN ") {
				query = "SELECT pin_hash FROM users WHERE id='target'"
				update = "UPDATE users SET pin_hash=$1 WHERE id='target'"
			}
			if err := f.ds.pool.QueryRow(t.Context(), query).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			changed := []byte(hash)
			switch {
			case strings.HasSuffix(shape, "suffix"):
				changed = append(changed, []byte("garbage")...)
			case strings.HasSuffix(shape, "salt alphabet"):
				changed[7] = '='
			case strings.HasSuffix(shape, "checksum alphabet"):
				changed[29] = '!'
			case strings.HasSuffix(shape, "delimiter"):
				changed[6] = ':'
			case strings.HasSuffix(shape, "major"):
				changed[1] = '1'
			case strings.HasSuffix(shape, "minor"):
				changed[2] = 'x'
			case strings.HasSuffix(shape, "truncate"):
				changed = changed[:len(changed)-1]
			case strings.HasSuffix(shape, "cost digits"):
				changed[4] = '+'
			}
			f.exec(t, update, string(changed))
			before := bridgeCredentialRows(t, f)
			// This is the primary shape regression: malformed data is already
			// present before capture, so tuple drift cannot explain rejection.
			malformed, err := f.ds.PrepareBridgeLoginCredentials(r, "actor", "target", "synthetic-device")
			if malformed != nil || err == nil {
				if malformed != nil {
					malformed.Close()
				}
				t.Fatal("malformed stored layout acquired a pre-comparison intent")
			}
			// Independently prove an existing intent never starts expensive
			// comparison after the actual stored credential observation changes.
			proof, err := f.ds.VerifyBridgePasswordWithPIN(r, pending, parentTestPassword+"2580")
			if proof != nil || err == nil || count != 0 {
				t.Fatal("malformed current data reached real bcrypt or evidence", err, count)
			}
			if !bytes.Equal(before, bridgeCredentialRows(t, f)) {
				t.Fatal("malformed hash rejection wrote credential data")
			}
		})
	}
}

func TestBridgeCredentialRequestMetadataFingerprintPreservesInvalidUTF8Bytes(t *testing.T) {
	for _, field := range []string{"method", "host", "remote", "query", "raw path", "header", "mux key", "mux value"} {
		t.Run(field, func(t *testing.T) {
			set := func(r *http.Request, value byte) *http.Request {
				raw := "original" + string([]byte{value})
				switch field {
				case "method":
					r.Method = raw
				case "host":
					r.Host = raw
				case "remote":
					r.RemoteAddr = raw
				case "query":
					r.URL.RawQuery = raw
				case "raw path":
					r.URL.RawPath = raw
				case "header":
					r.Header.Set("User-Agent", raw)
				case "mux key":
					r = mux.SetURLVars(r, map[string]string{raw: "original"})
				case "mux value":
					r = mux.SetURLVars(r, map[string]string{"original": raw})
				}
				return r
			}
			r := set(bridgeCredentialRequest(t.Context()), 0xff)
			original, err := bridgeCredentialRequestBinding(r, "synthetic-device")
			if err != nil {
				t.Fatal("original bounded metadata bytes rejected", err)
			}
			r = set(r, 0xfe)
			changed, err := bridgeCredentialRequestBinding(r, "synthetic-device")
			if err != nil || changed == original {
				t.Fatal("different invalid UTF-8 bytes collided in original binding", err)
			}
			r = set(r, 0xff)
			restored, err := bridgeCredentialRequestBinding(r, "synthetic-device")
			if err != nil || restored != original {
				t.Fatal("restored original bytes did not retain binding", err)
			}
		})
	}
}

func TestBridgeCredentialActualPGInvalidCarrierDriftDeniesBeforeCompareAndOriginalStillVerifies(t *testing.T) {
	f := bridgeCredentialPG(t, "2580", 4, nil)
	r := bridgeCredentialRequest(t.Context())
	original := "Synthetic " + string([]byte{0xff})
	r.Header.Set("User-Agent", original)
	i := bridgeCredentialPrepare(t, f, r)
	count := 0
	real := i.state.compare
	i.state.compare = func(hash, raw []byte) error { count++; return real(hash, raw) }
	before := bridgeCredentialRows(t, f)
	r.Header.Set("User-Agent", "Synthetic "+string([]byte{0xfe}))
	proof, err := f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
	if proof != nil || err == nil || count != 0 || i.Context().Err() != nil {
		t.Fatal("invalid-byte carrier drift reached comparison or retired original", err, count)
	}
	r.Header.Set("User-Agent", original)
	proof, err = f.ds.VerifyBridgeAccountPassword(r, i, parentTestPassword)
	if err != nil || proof == nil || count != 1 || proof.Check(t.Context()) != nil {
		t.Fatal("restored original carrier did not actually verify", err, count)
	}
	if !bytes.Equal(before, bridgeCredentialRows(t, f)) {
		t.Fatal("carrier drift or verification changed credential rows")
	}
}
