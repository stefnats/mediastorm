package datastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type removalPGFixture struct{ *parentPGFixture }

const removalTestURL = "PRIVATE_LIST"

func removalFixture(t *testing.T) *removalPGFixture {
	t.Helper()
	f := modeFixture(t)
	return &removalPGFixture{f.parentPGFixture}
}
func (f *removalPGFixture) request(t *testing.T, ctx context.Context, ds *DataStore) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Method = http.MethodDelete
	r.URL.Path = "/api/users/target/kids/lists"
	r.URL.RawQuery = "url=" + removalTestURL
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func (f *removalPGFixture) prepare(t *testing.T) (*ParentListRemovalIntent, *http.Request) {
	t.Helper()
	first, release := f.request(t, t.Context(), f.ds)
	intent, err := f.ds.PrepareParentListRemoval(first, "target", removalTestURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	release()
	retry, stop := f.request(t, t.Context(), f.ds)
	t.Cleanup(stop)
	t.Cleanup(intent.Close)
	retry.Header.Set(profilePinParentPasswordHeader, "base64:"+base64.StdEncoding.EncodeToString([]byte(" \t"+parentTestPassword+" \n")))
	return intent, retry
}
func (f *removalPGFixture) verify(t *testing.T, i *ParentListRemovalIntent, r *http.Request) {
	t.Helper()
	if err := f.ds.BindParentListRemovalRetry(r, i, "target", removalTestURL, nil); err != nil {
		t.Fatal(err)
	}
	r, err := f.ds.BoundParentListRemovalRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.ds.VerifyParentListRemovalPassword(r, i); err != nil {
		t.Fatal(err)
	}
}
func (f *removalPGFixture) lists(t *testing.T) string {
	t.Helper()
	var raw []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT kids_allowed_lists FROM users WHERE id='target'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var lists []string
	if err := json.Unmarshal(raw, &lists); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(lists)
	return string(encoded)
}
func TestParentListRemovalActualBoundHTTPHandoffAndListsOnlyCommit(t *testing.T) {
	f := removalFixture(t)
	intent, retry := f.prepare(t)
	// prepare deliberately completed/canceled the first request. This retry is
	// a different authenticated HTTP context under the unchanged original bound.
	if intent.state.life.Err() != nil {
		t.Fatal("first response canceled explicit challenge lifetime")
	}
	var before, after map[string]json.RawMessage
	var raw []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &before); err != nil {
		t.Fatal(err)
	}
	f.verify(t, intent, retry)
	record, err := f.ds.CommitParentListRemoval(intent)
	if err != nil || len(record.User.KidsAllowedLists) != 0 {
		t.Fatal("bound list removal commit failed", err)
	}
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &after); err != nil {
		t.Fatal(err)
	}
	delete(before, "kids_allowed_lists")
	delete(before, "updated_at")
	delete(after, "kids_allowed_lists")
	delete(after, "updated_at")
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("list removal mutation changed flag/defaults/other profile fields")
	}
	if _, err = f.ds.CommitParentListRemoval(intent); err == nil {
		t.Fatal("list removal commit replay accepted")
	}
}
func TestParentListRemovalCurrentTupleAndEpochChangesDeny(t *testing.T) {
	changes := map[string]string{
		"target ABA":      `UPDATE users SET name=name WHERE id='target'`,
		"target PIN":      `UPDATE users SET pin_hash='changed' WHERE id='target'`,
		"target owner":    `UPDATE users SET account_id='other' WHERE id='target'`,
		"target lists":    `UPDATE users SET kids_allowed_lists='["new"]'::jsonb WHERE id='target'`,
		"target kid flag": `UPDATE users SET is_kids_profile=true WHERE id='target'`,
		"actor password":  `UPDATE accounts SET password_hash=password_hash WHERE id='actor'`,
		"actor expiry":    `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`,
		"session expiry":  `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second'`,
		"actor role":      `UPDATE accounts SET is_master=true WHERE id='actor'`,
		"session row ABA": `UPDATE sessions SET token=token`,
		"profile revoke":  `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','target',1)`,
		"session revoke":  `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)SELECT 'session',pin_unlock_lineage,1 FROM sessions`,
	}
	for name, statement := range changes {
		t.Run(name, func(t *testing.T) {
			f := removalFixture(t)
			intent, retry := f.prepare(t)
			f.verify(t, intent, retry)
			f.exec(t, statement)
			before := f.lists(t)
			if _, err := f.ds.CommitParentListRemoval(intent); err == nil {
				t.Fatal("changed original list removal tuple committed")
			}
			if f.lists(t) != before {
				t.Fatal("denied mutation changed lists")
			}
		})
	}
}
func TestParentListRemovalRetryBoundaryIdentityAndDeadline(t *testing.T) {
	for _, mode := range []string{"URL", "target", "path", "raw path", "method", "query", "client", "copy", "wrong store", "expired", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := removalFixture(t)
			intent, retry := f.prepare(t)
			target, wanted := "target", removalTestURL
			ds := f.ds
			switch mode {
			case "URL":
				wanted = "missing"
			case "target":
				target = "own-second"
			case "path":
				retry.URL.Path = "/account/api/users/target/kids/lists"
			case "raw path":
				retry.URL.RawPath = "/api/users/%74arget/kids/lists"
			case "method":
				retry.Method = http.MethodPost
			case "query":
				retry.URL.RawQuery = "changed=true"
			case "client":
				retry.Header.Set("X-Client-ID", "replacement")
			case "copy":
				copy := *intent
				intent = &copy
			case "wrong store":
				ds = &DataStore{pool: f.ds.pool}
			case "expired":
				intent.state.stop()
			case "cancelled":
				ctx, cancel := context.WithCancel(retry.Context())
				cancel()
				retry = retry.WithContext(ctx)
			}
			if err := ds.BindParentListRemovalRetry(retry, intent, target, wanted, nil); err == nil {
				t.Fatal("invalid original retry admitted")
			}
			if f.lists(t) != `["PRIVATE_LIST"]` {
				t.Fatal("boundary failure changed lists")
			}
		})
	}
}
func TestParentListRemovalWrongPasswordMasterFreshAndOneShot(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular", true: "master"}[master], func(t *testing.T) {
			f := removalFixture(t)
			f.exec(t, `UPDATE accounts SET is_master=$1 WHERE id='actor'`, master)
			intent, retry := f.prepare(t)
			retry.Header.Set(profilePinParentPasswordHeader, "wrong")
			if err := f.ds.BindParentListRemovalRetry(retry, intent, "target", removalTestURL, nil); err != nil {
				t.Fatal(err)
			}
			retry, err := f.ds.BoundParentListRemovalRequest(retry, intent)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.ds.VerifyParentListRemovalPassword(retry, intent); !errors.Is(err, ErrParentListRemovalCredential) {
				t.Fatal("wrong original credential accepted", err)
			}
			retry.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
			if err := f.ds.VerifyParentListRemovalPassword(retry, intent); err == nil {
				t.Fatal("failed credential proof replayed")
			}
			if _, err := f.ds.CommitParentListRemoval(intent); err == nil || f.lists(t) != `["PRIVATE_LIST"]` {
				t.Fatal("unverified list removal write")
			}
		})
	}
}
func TestParentListRemovalActualTargetLockThenChangedRowDenies(t *testing.T) {
	f := removalFixture(t)
	intent, retry := f.prepare(t)
	f.verify(t, intent, retry)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(tx, t.Context())
	if _, err = tx.Exec(t.Context(), `UPDATE users SET name='new target row' WHERE id='target'`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := f.ds.CommitParentListRemoval(intent); done <- e }()
	f.waitBlocked(t)
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil || f.lists(t) != `["PRIVATE_LIST"]` {
		t.Fatal("late target result committed")
	}
}
func TestParentListRemovalActualBlockedCommitCancellationRollsBack(t *testing.T) {
	f := removalFixture(t)
	intent, retry := f.prepare(t)
	ctx, cancel := context.WithCancel(retry.Context())
	defer cancel()
	retry = retry.WithContext(ctx)
	f.verify(t, intent, retry)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(tx, t.Context())
	if _, err = tx.Exec(t.Context(), `SELECT id FROM users WHERE id='target' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := f.ds.CommitParentListRemoval(intent); done <- e }()
	f.waitBlocked(t)
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled list removal write did not preserve cancellation", err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.lists(t) != `["PRIVATE_LIST"]` {
		t.Fatal("canceled list removal write persisted")
	}
}
func TestParentListRemovalActualUnknownCommitNoSuccessAndNoReplay(t *testing.T) {
	f := removalFixture(t)
	cfg := f.cfg.Copy()
	var dropped atomic.Bool
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &parentDropCommit{Conn: c, dropped: &dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	// Confirm the first client/lineage on the ordinary pool, before arming a
	// connection whose first COMMIT acknowledgment intentionally disappears.
	first, release := f.request(t, t.Context(), f.ds)
	release()
	_ = first
	r, stop := f.request(t, t.Context(), ds)
	defer stop()
	intent, err := ds.PrepareParentListRemoval(r, "target", removalTestURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	if err = ds.BindParentListRemovalRetry(r, intent, "target", removalTestURL, nil); err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentListRemovalRequest(r, intent)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	if err = ds.VerifyParentListRemovalPassword(r, intent); err != nil {
		t.Fatal(err)
	}
	record, err := ds.CommitParentListRemoval(intent)
	if record.User.ID != "" || !errors.Is(err, ErrParentListRemovalUnknown) || !dropped.Load() {
		t.Fatal("unknown list removal commit reported success", err)
	}
	if f.lists(t) != `[]` {
		t.Fatal("lost ACK did not follow actual list removal commit")
	}
	if _, err = ds.CommitParentListRemoval(intent); err == nil {
		t.Fatal("unknown commit replayed")
	}
}

func TestParentListRemovalActualBcryptHasNoSQLRowLocks(t *testing.T) {
	f := removalFixture(t)
	intent, retry := f.prepare(t)
	if err := f.ds.BindParentListRemovalRetry(retry, intent, "target", removalTestURL, nil); err != nil {
		t.Fatal(err)
	}
	retry, err := f.ds.BoundParentListRemovalRequest(retry, intent)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	intent.state.compare = func(hash, password []byte) error {
		close(entered)
		<-release
		return bcrypt.CompareHashAndPassword(hash, password)
	}
	done := make(chan error, 1)
	go func() { done <- f.ds.VerifyParentListRemovalPassword(retry, intent) }()
	<-entered
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	defer rollbackPinTx(tx, t.Context())
	if _, err = tx.Exec(t.Context(), `SELECT id FROM users WHERE id='target' FOR UPDATE NOWAIT`); err != nil {
		close(release)
		t.Fatal("bcrypt held SQL target lock")
	}
	if _, err = tx.Exec(t.Context(), `UPDATE users SET name='changed during actual bcrypt' WHERE id='target'`); err != nil {
		close(release)
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal("actual original bcrypt comparison failed", err)
	}
	if _, err = f.ds.CommitParentListRemoval(intent); err == nil || f.lists(t) != `["PRIVATE_LIST"]` {
		t.Fatal("changed row after actual bcrypt committed")
	}
}

func TestParentListRemovalAbsentAndExactDuplicateSemantics(t *testing.T) {
	for _, tc := range []struct{ name, initial, wanted, expected string }{
		{"duplicates", `["other","PRIVATE_LIST","keep","PRIVATE_LIST","other"]`, removalTestURL, `["other","keep","other"]`},
		{"absent", `["other","other"]`, removalTestURL, `["other","other"]`},
		{"nil whole slice", `null`, removalTestURL, `[]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := removalFixture(t)
			f.exec(t, `UPDATE users SET kids_allowed_lists=$1::jsonb WHERE id='target'`, tc.initial)
			intent, retry := f.prepare(t)
			f.verify(t, intent, retry)
			record, err := f.ds.CommitParentListRemoval(intent)
			if err != nil || record.User.ID != "target" || f.lists(t) != tc.expected {
				t.Fatal("exact list identity/remainder changed", err)
			}
		})
	}
}
func TestParentListRemovalStrictStoredArray(t *testing.T) {
	for _, value := range []string{`[null]`, `["valid",4]`, `{}`, `"list"`} {
		if _, err := parentListRemovalLists(json.RawMessage(`{"kids_allowed_lists":` + value + `}`)); err == nil {
			t.Fatal("invalid complete stored array accepted")
		}
	}
	if _, err := parentListRemovalLists(json.RawMessage(`{}`)); err == nil {
		t.Fatal("missing stored field accepted")
	}
}
func TestParentListRemovalBoundariesFailBeforeIO(t *testing.T) {
	f := removalFixture(t)
	r, release := f.request(t, t.Context(), f.ds)
	defer release()
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Method = http.MethodPut },
		func(r *http.Request) { r.URL.RawQuery = "url=other" },
	} {
		copy := r.Clone(r.Context())
		u := *r.URL
		copy.URL = &u
		mutate(copy)
		before := f.ds.pool.Stat().AcquireCount()
		if intent, err := f.ds.PrepareParentListRemoval(copy, "target", removalTestURL, nil); err == nil || intent != nil {
			t.Fatal("invalid prepare accepted")
		}
		if f.ds.pool.Stat().AcquireCount() != before {
			t.Fatal("invalid prepare acquired database")
		}
	}
}
