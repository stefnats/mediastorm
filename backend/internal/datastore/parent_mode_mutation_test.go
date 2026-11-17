package datastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type modePGFixture struct{ *parentPGFixture }

func modeFixture(t *testing.T) *modePGFixture {
	t.Helper()
	f := parentFixture(t)
	for _, name := range []string{"066_profile_unlocks.sql", "077_profile_unlock_revocations.sql", "078_profile_unlock_bindings.sql"} {
		raw, err := embedMigrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		f.exec(t, strings.Split(string(raw), "-- +goose Down")[0])
	}
	f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
	return &modePGFixture{f}
}
func (f *modePGFixture) request(t *testing.T, ctx context.Context, ds *DataStore) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Method = http.MethodPut
	r.URL.Path = "/api/users/target/kids/mode"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func (f *modePGFixture) prepare(t *testing.T) (*ParentModeIntent, *http.Request) {
	t.Helper()
	first, release := f.request(t, t.Context(), f.ds)
	intent, err := f.ds.PrepareParentMode(first, "target", "rating", nil)
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
func (f *modePGFixture) verify(t *testing.T, i *ParentModeIntent, r *http.Request) {
	t.Helper()
	if err := f.ds.BindParentModeRetry(r, i, "target", "rating", nil); err != nil {
		t.Fatal(err)
	}
	r, err := f.ds.BoundParentModeRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.ds.VerifyParentModePassword(r, i); err != nil {
		t.Fatal(err)
	}
}
func (f *modePGFixture) mode(t *testing.T) string {
	t.Helper()
	var mode string
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT kids_mode FROM users WHERE id='target'`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	return mode
}
func TestParentModeActualBoundHTTPHandoffAndModeOnlyCommit(t *testing.T) {
	f := modeFixture(t)
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
	record, err := f.ds.CommitParentMode(intent)
	if err != nil || record.User.KidsMode != "rating" {
		t.Fatal("bound mode commit failed", err)
	}
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &after); err != nil {
		t.Fatal(err)
	}
	delete(before, "kids_mode")
	delete(before, "updated_at")
	delete(after, "kids_mode")
	delete(after, "updated_at")
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("mode mutation changed flag/defaults/other profile fields")
	}
	if _, err = f.ds.CommitParentMode(intent); err == nil {
		t.Fatal("mode commit replay accepted")
	}
}
func TestParentModeCurrentTupleAndEpochChangesDeny(t *testing.T) {
	changes := map[string]string{
		"target ABA":      `UPDATE users SET name=name WHERE id='target'`,
		"target PIN":      `UPDATE users SET pin_hash='changed' WHERE id='target'`,
		"target owner":    `UPDATE users SET account_id='other' WHERE id='target'`,
		"target kid flag": `UPDATE users SET is_kids_profile=true WHERE id='target'`,
		"actor password":  `UPDATE accounts SET password_hash=password_hash WHERE id='actor'`,
		"actor role":      `UPDATE accounts SET is_master=true WHERE id='actor'`,
		"session row ABA": `UPDATE sessions SET token=token`,
		"profile revoke":  `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)VALUES('profile','target',1)`,
		"session revoke":  `INSERT INTO profile_unlock_revocations(scope,scope_key,generation)SELECT 'session',pin_unlock_lineage,1 FROM sessions`,
	}
	for name, statement := range changes {
		t.Run(name, func(t *testing.T) {
			f := modeFixture(t)
			intent, retry := f.prepare(t)
			f.verify(t, intent, retry)
			f.exec(t, statement)
			if _, err := f.ds.CommitParentMode(intent); err == nil {
				t.Fatal("changed original mode tuple committed")
			}
			if f.mode(t) != "content_list" {
				t.Fatal("denied mutation wrote mode")
			}
		})
	}
}
func TestParentModeRetryBoundaryIdentityAndDeadline(t *testing.T) {
	for _, mode := range []string{"mode", "target", "path", "method", "query", "client", "copy", "wrong store", "expired", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := modeFixture(t)
			intent, retry := f.prepare(t)
			target, wanted := "target", "rating"
			ds := f.ds
			switch mode {
			case "mode":
				wanted = ""
			case "target":
				target = "own-second"
			case "path":
				retry.URL.Path = "/account/api/users/target/kids/mode"
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
			if err := ds.BindParentModeRetry(retry, intent, target, wanted, nil); err == nil {
				t.Fatal("invalid original retry admitted")
			}
			if f.mode(t) != "content_list" {
				t.Fatal("boundary failure wrote mode")
			}
		})
	}
}
func TestParentModeWrongPasswordMasterFreshAndOneShot(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(map[bool]string{false: "regular", true: "master"}[master], func(t *testing.T) {
			f := modeFixture(t)
			f.exec(t, `UPDATE accounts SET is_master=$1 WHERE id='actor'`, master)
			intent, retry := f.prepare(t)
			retry.Header.Set(profilePinParentPasswordHeader, "wrong")
			if err := f.ds.BindParentModeRetry(retry, intent, "target", "rating", nil); err != nil {
				t.Fatal(err)
			}
			retry, err := f.ds.BoundParentModeRequest(retry, intent)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.ds.VerifyParentModePassword(retry, intent); !errors.Is(err, ErrParentModeCredential) {
				t.Fatal("wrong original credential accepted", err)
			}
			retry.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
			if err := f.ds.VerifyParentModePassword(retry, intent); err == nil {
				t.Fatal("failed credential proof replayed")
			}
			if _, err := f.ds.CommitParentMode(intent); err == nil || f.mode(t) != "content_list" {
				t.Fatal("unverified mode write")
			}
		})
	}
}
func TestParentModeActualTargetLockThenChangedRowDenies(t *testing.T) {
	f := modeFixture(t)
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
	go func() { _, e := f.ds.CommitParentMode(intent); done <- e }()
	f.waitBlocked(t)
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err == nil || f.mode(t) != "content_list" {
		t.Fatal("late target result committed")
	}
}
func TestParentModeActualBlockedCommitCancellationRollsBack(t *testing.T) {
	f := modeFixture(t)
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
	go func() { _, e := f.ds.CommitParentMode(intent); done <- e }()
	f.waitBlocked(t)
	cancel()
	if err = <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("canceled mode write did not preserve cancellation", err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.mode(t) != "content_list" {
		t.Fatal("canceled mode write persisted")
	}
}
func TestParentModeActualUnknownCommitNoSuccessAndNoReplay(t *testing.T) {
	f := modeFixture(t)
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
	intent, err := ds.PrepareParentMode(r, "target", "rating", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	if err = ds.BindParentModeRetry(r, intent, "target", "rating", nil); err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentModeRequest(r, intent)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	if err = ds.VerifyParentModePassword(r, intent); err != nil {
		t.Fatal(err)
	}
	record, err := ds.CommitParentMode(intent)
	if record.User.ID != "" || !errors.Is(err, ErrParentModeUnknown) || !dropped.Load() {
		t.Fatal("unknown mode commit reported success", err)
	}
	if f.mode(t) != "rating" {
		t.Fatal("lost ACK did not follow actual mode commit")
	}
	if _, err = ds.CommitParentMode(intent); err == nil {
		t.Fatal("unknown commit replayed")
	}
}

func TestParentModeActualBcryptHasNoSQLRowLocks(t *testing.T) {
	f := modeFixture(t)
	intent, retry := f.prepare(t)
	if err := f.ds.BindParentModeRetry(retry, intent, "target", "rating", nil); err != nil {
		t.Fatal(err)
	}
	retry, err := f.ds.BoundParentModeRequest(retry, intent)
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
	go func() { done <- f.ds.VerifyParentModePassword(retry, intent) }()
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
	if _, err = f.ds.CommitParentMode(intent); err == nil || f.mode(t) != "content_list" {
		t.Fatal("changed row after actual bcrypt committed")
	}
}
