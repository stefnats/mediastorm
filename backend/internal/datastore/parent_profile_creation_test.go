package datastore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
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
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"novastream/models"
)

const parentCreationTestPassword = "synthetic original creation password"
const parentCreationTestBody = `{"name":"  Synthetic Child  ","color":" ","isKidsProfile":true}`

type parentCreationFixture struct {
	*settingsDSFixture
	session models.Session
}

func newParentCreationFixture(t *testing.T) *parentCreationFixture {
	t.Helper()
	f := creationOperationFixture(t)
	hash, err := bcrypt.GenerateFromPassword([]byte(parentCreationTestPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='default'`, string(hash))
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "SYNTHETIC_CREATION_SESSION", AccountID: "default", ClientID: "synthetic-creation-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err = f.ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	return &parentCreationFixture{f, session}
}
func (f *parentCreationFixture) request(t *testing.T, ds *DataStore, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(ctx)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, stop, err := ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, stop
}
func (f *parentCreationFixture) prepare(t *testing.T, ds *DataStore) (*ParentProfileCreationIntent, *http.Request) {
	t.Helper()
	r, stop := f.request(t, ds, t.Context())
	i, err := ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
	stop()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(i.Close)
	retry, release := f.request(t, ds, t.Context())
	t.Cleanup(release)
	if err = ds.BindParentProfileCreationManagementRetry(retry, i, []byte(parentCreationTestBody), nil); err != nil {
		t.Fatal(err)
	}
	retry, err = ds.BoundParentProfileCreationRequest(retry, i)
	if err != nil {
		t.Fatal(err)
	}
	retry.Header.Set("X-Account-Password", "base64:"+base64.StdEncoding.EncodeToString([]byte(" \t"+parentCreationTestPassword+" \n")))
	return i, retry
}
func creationTestLeases(t *testing.T, ds *DataStore, i *ParentProfileCreationIntent, r *http.Request) (*ParentProfileCreationCacheLease, *ParentProfileCreationCacheLease) {
	t.Helper()
	u, err := ds.BeginParentProfileCreationUsersLease(r.Context(), i)
	if err != nil {
		t.Fatal(err)
	}
	v, err := ds.BeginParentProfileCreationSettingsLease(r.Context(), i, u)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Retire(); u.Retire() })
	return u, v
}
func creationTestCounts(t *testing.T, f *parentCreationFixture, target string) (int, int) {
	t.Helper()
	var users, settings int
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM users WHERE id=$1),(SELECT count(*) FROM user_settings WHERE user_id=$1)`, target).Scan(&users, &settings); err != nil {
		t.Fatal(err)
	}
	return users, settings
}
func TestParentProfileCreationDecoderKeepsCookieSemanticsAndEmptyColor(t *testing.T) {
	for _, body := range []string{`{"name":" Child ","isKidsProfile":true}`, `{"Name":"Child","isKidsProfile":true,"future":1} trailing`, `{"name":"Child","isKidsProfile":true,"isKidsProfile":null}`, `{"name":"Child","color":null,"isKidsProfile":true}`} {
		d, err := parentCreationDecode([]byte(body))
		if err != nil || d.Name != "Child" || d.Color != "" {
			t.Fatal("existing decoder semantics changed", body, err)
		}
	}
	for _, body := range []string{`null`, `{}`, `{"name":" "}`, `{"name":"Child","isKidsProfile":false}`, `{"name":"Child","isKidsProfile":true,"isKidsProfile":false}`} {
		if _, err := parentCreationDecode([]byte(body)); err == nil {
			t.Fatal("invalid creation accepted", body)
		}
	}
	if _, err := parentCreationDecode(bytes.Repeat([]byte(" "), ParentProfileCreationBodyLimit+1)); !errors.Is(err, ErrParentProfileCreationBodyTooLarge) {
		t.Fatal("PG body cap missing", err)
	}
}
func TestParentProfileCreationActualCaptureStrictPairAndReceipt(t *testing.T) {
	f := newParentCreationFixture(t)
	i, r := f.prepare(t, f.ds)
	if i.state.life.Err() != nil {
		t.Fatal("initial response canceled explicit continuation")
	}
	if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	u, v := creationTestLeases(t, f.ds, i, r)
	receipt, err := f.ds.CommitParentProfileCreation(i)
	if err != nil {
		t.Fatal(err)
	}
	row, err := f.ds.StageParentProfileCreationUsers(u, receipt)
	if err != nil {
		t.Fatal(err)
	}
	settings, err := f.ds.StageParentProfileCreationSettings(v, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if row.User.Name != "Synthetic Child" || row.User.Color != "" || row.User.AccountID != "default" || !row.User.IsKidsProfile || row.User.KidsMode != "rating" || row.User.KidsMaxRating != "G" || row.User.KidsAllowedLists != nil || row.User.KidsMaxMovieRating != "" || row.User.KidsMaxTVRating != "" || row.User.HasPin() || row.User.HasIcon() || row.User.AllowShareLinks || row.User.AllowLiveTakeover || row.User.AllowRecordingInterruption || row.User.ActivityPrivacy != models.ActivityPrivacySharedAnonymous {
		t.Fatal("full actual Kids creation result incorrect")
	}
	if settings.Settings == nil || settings.Settings.Display.SimpleMode == nil || !*settings.Settings.Display.SimpleMode {
		t.Fatal("paired default absent")
	}
	if err = f.ds.TakeParentProfileCreationReceipt(i, receipt, u, v); err != nil {
		t.Fatal(err)
	}
	if err = f.ds.TakeParentProfileCreationReceipt(i, receipt, u, v); err == nil {
		t.Fatal("receipt replay")
	}
	if _, err = f.ds.CommitParentProfileCreation(i); err == nil {
		t.Fatal("second create")
	}
	if a, b := creationTestCounts(t, f, row.User.ID); a != 1 || b != 1 {
		t.Fatal("strict pair absent", a, b)
	}
	raw, _ := json.Marshal(i)
	if string(raw) != "null" || fmt.Sprintf("%+v", receipt) != "ParentProfileCreationReceipt(opaque)" {
		t.Fatal("private credentials exposed")
	}
}
func TestParentProfileCreationOriginalTupleRevalidatedAfterBcrypt(t *testing.T) {
	for name, sql := range map[string]string{
		"actor ABA":      `UPDATE accounts SET username=username WHERE id='default'`,
		"password":       `UPDATE accounts SET password_hash='changed' WHERE id='default'`,
		"actor role":     `UPDATE accounts SET is_master=true WHERE id='default'`,
		"owner expiry":   `UPDATE accounts SET expires_at=now()-interval '1 second' WHERE id='default'`,
		"session client": `UPDATE sessions SET client_id='changed'`,
		"session ABA":    `UPDATE sessions SET user_agent=user_agent`,
		"revocation":     `INSERT INTO profile_unlock_revocations(scope,scope_key,generation) SELECT 'session',pin_unlock_lineage,1 FROM sessions`,
	} {
		t.Run(name, func(t *testing.T) {
			f := newParentCreationFixture(t)
			i, r := f.prepare(t, f.ds)
			if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
				t.Fatal(err)
			}
			creationTestLeases(t, f.ds, i, r)
			f.exec(t, sql)
			if receipt, err := f.ds.CommitParentProfileCreation(i); err == nil || receipt != nil {
				t.Fatal("changed original tuple created a profile")
			}
			if a, b := creationTestCounts(t, f, i.state.reference.record.target); a != 0 || b != 0 {
				t.Fatal("denial wrote target", a, b)
			}
		})
	}
}
func TestParentProfileCreationRetryRawIdentityAndOriginalFrame(t *testing.T) {
	for _, kind := range []string{"body", "false", "query", "rawpath", "method", "client", "copy", "store", "closed", "cancel"} {
		t.Run(kind, func(t *testing.T) {
			f := newParentCreationFixture(t)
			first, stop := f.request(t, f.ds, t.Context())
			i, err := f.ds.PrepareParentProfileCreationManagement(first, []byte(parentCreationTestBody), nil)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r, release := f.request(t, f.ds, ctx)
			defer release()
			body := []byte(parentCreationTestBody)
			ds := f.ds
			switch kind {
			case "body":
				body = append(body, ' ')
			case "false":
				body = []byte(`{"name":"Synthetic Child","isKidsProfile":false}`)
			case "query":
				r.URL.RawQuery = "x=1"
			case "rawpath":
				r.URL.RawPath = "/account/api/%70rofiles"
			case "method":
				r.Method = "PUT"
			case "client":
				r.Header.Set("X-Client-ID", "other")
			case "copy":
				c := *i
				i = &c
			case "store":
				ds = &DataStore{pool: f.ds.pool}
			case "closed":
				i.Close()
			case "cancel":
				cancel()
			}
			if err = ds.BindParentProfileCreationManagementRetry(r, i, body, nil); err == nil {
				t.Fatal("changed retry accepted", kind)
			}
		})
	}
}
func TestParentProfileCreationOriginalBcryptHasNoSQLLocks(t *testing.T) {
	f := newParentCreationFixture(t)
	i, r := f.prepare(t, f.ds)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	i.state.compare = func(hash, password []byte) error {
		err := bcrypt.CompareHashAndPassword(hash, password)
		close(entered)
		<-release
		return err
	}
	worker := startCreationTestWorker(t, func() { unblock(); i.state.stop() }, func() error { return f.ds.VerifyParentProfileCreationPassword(r, i) })
	select {
	case <-entered:
	case <-worker.done:
		t.Fatal("bcrypt did not reach barrier", worker.err)
	case <-t.Context().Done():
		t.Fatal("test canceled before bcrypt")
	case <-time.After(5 * time.Second):
		t.Fatal("actual bcrypt barrier timeout")
	}
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer creationTestRollback(tx)
	_, err = tx.Exec(t.Context(), `SELECT id FROM accounts WHERE id='default' FOR UPDATE NOWAIT`)
	creationTestRollback(tx)
	unblock()
	verify := worker.join(t)
	if err != nil || verify != nil {
		t.Fatal("bcrypt retained SQL locks or verification failed", err, verify)
	}
}
func TestParentProfileCreationStrictCollisionAndSettingsFailureRollback(t *testing.T) {
	for _, kind := range []string{"existing user", "079 tombstone", "settings insert"} {
		t.Run(kind, func(t *testing.T) {
			f := newParentCreationFixture(t)
			i, r := f.prepare(t, f.ds)
			if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
				t.Fatal(err)
			}
			creationTestLeases(t, f.ds, i, r)
			target := i.state.reference.record.target
			switch kind {
			case "existing user":
				f.user(t, target)
			case "079 tombstone":
				f.exec(t, `INSERT INTO user_settings_versions(user_id,generation)VALUES($1,7)`, target)
			case "settings insert":
				f.exec(t, `CREATE FUNCTION reject_creation_settings() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic settings rejection'; END $$; CREATE TRIGGER reject_creation_settings BEFORE INSERT ON user_settings FOR EACH ROW EXECUTE FUNCTION reject_creation_settings()`)
			}
			if receipt, err := f.ds.CommitParentProfileCreation(i); receipt != nil || err == nil {
				t.Fatal("collision/second insert failure accepted")
			}
			users, settings := creationTestCounts(t, f, target)
			want := 0
			if kind == "existing user" {
				want = 1
			}
			if users != want || settings != 0 {
				t.Fatal("strict pair rollback incorrect", users, settings)
			}
		})
	}
}
func TestParentProfileCreationFenceWaitCancelAndWorkerJoin(t *testing.T) {
	f := newParentCreationFixture(t)
	i, r := f.prepare(t, f.ds)
	if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, f.ds, i, r)
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(t.Context(), `SELECT operation_id FROM profile_creation_operations WHERE operation_id=$1 FOR UPDATE`, i.state.reference.record.id); err != nil {
		t.Fatal(err)
	}
	worker := startCreationTestWorker(t, func() { i.state.stop(); creationTestRollback(blocker) }, func() error { _, e := f.ds.CommitParentProfileCreation(i); return e })
	creationOperationWaitLock(t, f.settingsDSFixture, "SELECT operation_id::text FROM profile_creation_operations")
	closer := startCreationTestWorker(t, func() { i.state.stop(); creationTestRollback(blocker) }, func() error { i.Close(); return nil })
	if err = closer.join(t); err != nil {
		t.Fatal(err)
	}
	err = worker.join(t)
	_ = blocker.Rollback(t.Context())
	if err == nil {
		t.Fatal("canceled real fence wait accepted")
	}
	if a, b := creationTestCounts(t, f, i.state.reference.record.target); a != 0 || b != 0 {
		t.Fatal("canceled waiter wrote target")
	}
}

type parentCreationInsertTracer struct {
	enable *atomic.Bool
	match  string
}

func (t *parentCreationInsertTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	match := t.match
	if match == "" {
		match = "INSERT INTO users AS u"
	}
	return context.WithValue(ctx, parentCreationInsertKey{}, strings.HasPrefix(d.SQL, match))
}
func (t *parentCreationInsertTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryEndData) {
	if value, _ := ctx.Value(parentCreationInsertKey{}).(bool); value && d.Err == nil {
		t.enable.Store(true)
	}
}

type parentCreationInsertKey struct{}

func TestParentProfileCreationRealLostCreateCommitAckNoSecondCreate(t *testing.T) {
	f := newParentCreationFixture(t)
	cfg, err := pgxpool.ParseConfig(f.uri)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 2
	cfg.MinConns = 0
	enabled, dropped := &atomic.Bool{}, &atomic.Bool{}
	cfg.ConnConfig.Tracer = &parentCreationInsertTracer{enable: enabled}
	expected := filepath.Join(cfg.ConnConfig.Host, fmt.Sprintf(".s.PGSQL.%d", cfg.ConnConfig.Port))
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "unix" || address != expected {
			return nil, errors.New("only exact synthetic Unix socket allowed")
		}
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &creationOperationLostAckConn{Conn: c, enabled: enabled, dropped: dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	i, r := f.prepare(t, ds)
	if err = ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, ds, i, r)
	receipt, err := ds.CommitParentProfileCreation(i)
	if receipt != nil || !errors.Is(err, ErrParentProfileCreationUnknown) || !dropped.Load() {
		t.Fatal("actual create COMMIT acknowledgement not lost", err)
	}
	if a, b := creationTestCounts(t, f, i.state.reference.record.target); a != 1 || b != 1 {
		t.Fatal("independent connection did not observe real committed pair", a, b)
	}
	if _, err = ds.CommitParentProfileCreation(i); err == nil {
		t.Fatal("unknown automatically created again")
	}
	var count int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations`).Scan(&count); err != nil || count != 1 {
		t.Fatal("another reservation created", err)
	}
}
func TestParentProfileCreationOnlyExpiredReservedRetires(t *testing.T) {
	f := newParentCreationFixture(t)
	for _, state := range []string{"reserved", "attempting", "unknown", "terminal"} {
		creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("synthetic_actor"), state, true)
	}
	count, err := f.ds.retireExpiredParentProfileReservations(t.Context())
	if err != nil || count != 1 {
		t.Fatal("expired-reserved maintenance wrong", count, err)
	}
	var unresolved int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations WHERE state IN ('attempting','unknown')`).Scan(&unresolved); err != nil || unresolved != 2 {
		t.Fatal("attempted rows retired or deleted", err)
	}
}

func TestParentProfileCreationCloseCancelsActualBlockedBindWithoutCopyAuthority(t *testing.T) {
	f := newParentCreationFixture(t)
	first, release := f.request(t, f.ds, t.Context())
	i, err := f.ds.PrepareParentProfileCreationManagement(first, []byte(parentCreationTestBody), nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	retry, stop := f.request(t, f.ds, t.Context())
	defer stop()
	copy := *i
	copy.Close()
	if i.state.life.Err() != nil {
		t.Fatal("copied handle canceled original")
	}
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(t.Context(), `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	cleanup := func() { i.state.stop(); creationTestRollback(blocker) }
	worker := startCreationTestWorker(t, cleanup, func() error {
		return f.ds.BindParentProfileCreationManagementRetry(retry, i, []byte(parentCreationTestBody), nil)
	})
	creationOperationWaitLock(t, f.settingsDSFixture, "FROM sessions s JOIN accounts")
	closer := startCreationTestWorker(t, cleanup, func() error { i.Close(); return nil })
	select {
	case <-closer.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close could not cancel SQL before mutex join")
	}
	err = worker.join(t)
	_ = blocker.Rollback(t.Context())
	if err == nil {
		t.Fatal("closed blocked Bind succeeded")
	}
}

// Existing parent mutations take epoch/account/session before Users. This
// control consumes the real Mode writer rather than adding a hypothetical FK.
func TestParentProfileCreationCoexistsWithExistingParentModeWriter(t *testing.T) {
	f := newParentCreationFixture(t)
	f.user(t, "target")
	i, r := f.prepare(t, f.ds)
	if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, f.ds, i, r)
	modeRequest, release := f.request(t, f.ds, t.Context())
	defer release()
	modeRequest.Method = http.MethodPut
	modeRequest.URL.Path = "/api/users/target/kids/mode"
	mode, err := f.ds.PrepareParentMode(modeRequest, "target", "rating", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mode.Close()
	if err = f.ds.BindParentModeRetry(modeRequest, mode, "target", "rating", nil); err != nil {
		t.Fatal(err)
	}
	modeRequest, err = f.ds.BoundParentModeRequest(modeRequest, mode)
	if err != nil {
		t.Fatal(err)
	}
	modeRequest.Header.Set("X-Account-Password", parentCreationTestPassword)
	if err = f.ds.VerifyParentModePassword(modeRequest, mode); err != nil {
		t.Fatal(err)
	}
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(t.Context(), `SELECT id FROM accounts WHERE id='default' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	cleanup := func() { creationTestRollback(blocker); i.state.stop(); mode.Close() }
	modeWorker := startCreationTestWorker(t, cleanup, func() error { _, e := f.ds.CommitParentMode(mode); return e })
	creationOperationWaitLock(t, f.settingsDSFixture, "SELECT id FROM accounts")
	createWorker := startCreationTestWorker(t, cleanup, func() error { _, e := f.ds.CommitParentProfileCreation(i); return e })
	creationOperationWaitLock(t, f.settingsDSFixture, "profile_unlock_revocations")
	if err = blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = modeWorker.join(t); err != nil {
		t.Fatal("existing Mode writer failed", err)
	}
	if err = createWorker.join(t); err != nil {
		t.Fatal("creation writer failed beside actual parent writer", err)
	}
	if a, b := creationTestCounts(t, f, i.state.reference.record.target); a != 1 || b != 1 {
		t.Fatal("strict pair absent after both workers joined")
	}
}
func TestParentProfileCreationRetryCancellationInterruptsRealClaimWait(t *testing.T) {
	f := newParentCreationFixture(t)
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, stop := f.request(t, f.ds, parent)
	defer stop()
	i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if err = f.ds.BindParentProfileCreationManagementRetry(r, i, []byte(parentCreationTestBody), nil); err != nil {
		t.Fatal(err)
	}
	r, err = f.ds.BoundParentProfileCreationRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", parentCreationTestPassword)
	if err = f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, f.ds, i, r)
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(t.Context(), `SELECT operation_id FROM profile_creation_operations WHERE operation_id=$1 FOR UPDATE`, i.state.reference.record.id); err != nil {
		t.Fatal(err)
	}
	worker := startCreationTestWorker(t, func() { cancel(); creationTestRollback(blocker) }, func() error { _, e := f.ds.CommitParentProfileCreation(i); return e })
	creationOperationWaitLock(t, f.settingsDSFixture, "SELECT operation_id::text FROM profile_creation_operations")
	cancel()
	err = worker.join(t)
	_ = blocker.Rollback(t.Context())
	if err == nil {
		t.Fatal("original retry cancellation missed real claim wait")
	}
}

func TestParentProfileCreationActualAdmissionAndClaimAckLossNeverReleasesAttempt(t *testing.T) {
	for _, phase := range []string{"admission", "claim"} {
		t.Run(phase, func(t *testing.T) {
			f := newParentCreationFixture(t)
			cfg, err := pgxpool.ParseConfig(f.uri)
			if err != nil {
				t.Fatal(err)
			}
			cfg.MaxConns = 2
			cfg.MinConns = 0
			enabled, dropped := &atomic.Bool{}, &atomic.Bool{}
			match := "INSERT INTO profile_creation_operations"
			if phase == "claim" {
				match = "UPDATE profile_creation_operations SET state='attempting'"
			}
			cfg.ConnConfig.Tracer = &parentCreationInsertTracer{enable: enabled, match: match}
			expected := filepath.Join(cfg.ConnConfig.Host, fmt.Sprintf(".s.PGSQL.%d", cfg.ConnConfig.Port))
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				if network != "unix" || address != expected {
					return nil, errors.New("only exact synthetic Unix socket allowed")
				}
				c, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				return &creationOperationLostAckConn{Conn: c, enabled: enabled, dropped: dropped}, nil
			}
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			ds := &DataStore{pool: pool}
			if phase == "admission" {
				r, stop := f.request(t, ds, t.Context())
				defer stop()
				i, e := ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
				if i != nil {
					defer i.Close()
				}
				if i != nil || !errors.Is(e, ErrParentProfileCreationUnknown) || !dropped.Load() {
					t.Fatal("admission ACK loss exposed usable reference/intent", e)
				}
			} else {
				i, r := f.prepare(t, ds)
				if err = ds.VerifyParentProfileCreationPassword(r, i); err != nil {
					t.Fatal(err)
				}
				creationTestLeases(t, ds, i, r)
				if receipt, e := ds.CommitParentProfileCreation(i); receipt != nil || !errors.Is(e, ErrParentProfileCreationUnknown) || !dropped.Load() {
					t.Fatal("claim ACK loss released write receipt", e)
				}
				if _, e := ds.CommitParentProfileCreation(i); e == nil {
					t.Fatal("unknown claim replay")
				}
			}
			var state string
			var count, users int
			if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*),min(state),(SELECT count(*) FROM users) FROM profile_creation_operations`).Scan(&count, &state, &users); err != nil {
				t.Fatal(err)
			}
			wanted := "reserved"
			if phase == "claim" {
				wanted = "attempting"
			}
			if count != 1 || users != 0 || state != wanted {
				t.Fatal("uncertain admission/claim wrote User or auto-reserved again", count, users, state)
			}
		})
	}
}
func TestParentProfileCreationDeadlineDoesNotRenewAtRetry(t *testing.T) {
	f := newParentCreationFixture(t)
	parent, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r, stop := f.request(t, f.ds, parent)
	defer stop()
	i, err := f.ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	expected, _ := parent.Deadline()
	if i.state.deadline.After(expected) {
		t.Fatal("capture renewed original request deadline")
	}
	<-i.state.life.Done()
	fresh, release := f.request(t, f.ds, t.Context())
	defer release()
	if err = f.ds.BindParentProfileCreationManagementRetry(fresh, i, []byte(parentCreationTestBody), nil); err == nil {
		t.Fatal("fresh HTTP request renewed expired original intent")
	}
}

func TestParentProfileCreationAdmissionRetiresEightExpiredOrphans(t *testing.T) {
	f := newParentCreationFixture(t)
	for n := 0; n < 8; n++ {
		creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("default"), "reserved", true)
	}
	first, stop := f.request(t, f.ds, t.Context())
	defer stop()
	i, err := f.ds.PrepareParentProfileCreationManagement(first, []byte(parentCreationTestBody), nil)
	if err != nil {
		t.Fatal("explicit fresh action could not reclaim expired reservation capacity", err)
	}
	defer i.Close()
	var active int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations WHERE actor_id='default' AND state IN ('reserved','attempting','unknown')`).Scan(&active); err != nil || active != 1 {
		t.Fatal("old reservations retained actor charge", active, err)
	}
}
func TestParentProfileCreationAdmissionPrunesClosedGlobalBudgetAndSkipsHeldRows(t *testing.T) {
	f := newParentCreationFixture(t)
	var held string
	for n := 0; n < creationOperationMaximumRows; n++ {
		id, _ := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("synthetic_prior_actor"), "terminal", true)
		if n == 0 {
			held = id
		}
	}
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(t.Context(), `SELECT operation_id FROM profile_creation_operations WHERE operation_id=$1 FOR UPDATE`, held); err != nil {
		t.Fatal(err)
	}
	first, stop := f.request(t, f.ds, t.Context())
	defer stop()
	worker := startCreationTestWorker(t, func() { stop(); creationTestRollback(blocker) }, func() error {
		value, e := f.ds.PrepareParentProfileCreationManagement(first, []byte(parentCreationTestBody), nil)
		if value != nil {
			value.Close()
		}
		return e
	})
	if err = worker.join(t); err != nil {
		t.Fatal("bounded admission failed beside held terminal row", err)
	}
	var retained bool
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM profile_creation_operations WHERE operation_id=$1)`, held).Scan(&retained); err != nil || !retained {
		t.Fatal("held terminal evidence not skipped", err)
	}
	if err = blocker.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestParentProfileCreationAdmissionNeverRetiresAttemptedUnknown(t *testing.T) {
	f := newParentCreationFixture(t)
	for _, state := range []string{"attempting", "unknown"} {
		creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("default"), state, true)
	}
	first, stop := f.request(t, f.ds, t.Context())
	defer stop()
	i, err := f.ds.PrepareParentProfileCreationManagement(first, []byte(parentCreationTestBody), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	var count int
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations WHERE state IN ('attempting','unknown')`).Scan(&count); err != nil || count != 2 {
		t.Fatal("admission maintenance cleared unresolved evidence", count, err)
	}
}

func TestParentProfileCreationMaintenanceLostAckIsNotNewAdmissionUnknown(t *testing.T) {
	for _, kind := range []string{"retire", "prune"} {
		t.Run(kind, func(t *testing.T) {
			f := newParentCreationFixture(t)
			state, match := "reserved", "WITH candidates AS"
			if kind == "prune" {
				state, match = "terminal", "WITH eligible AS"
			}
			creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("synthetic_prior_actor"), state, true)
			cfg, err := pgxpool.ParseConfig(f.uri)
			if err != nil {
				t.Fatal(err)
			}
			cfg.MaxConns = 2
			cfg.MinConns = 0
			enabled, dropped := &atomic.Bool{}, &atomic.Bool{}
			cfg.ConnConfig.Tracer = &parentCreationInsertTracer{enable: enabled, match: match}
			expected := filepath.Join(cfg.ConnConfig.Host, fmt.Sprintf(".s.PGSQL.%d", cfg.ConnConfig.Port))
			cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
				if network != "unix" || address != expected {
					return nil, errors.New("only exact synthetic Unix socket allowed")
				}
				c, e := (&net.Dialer{}).DialContext(ctx, network, address)
				if e != nil {
					return nil, e
				}
				return &creationOperationLostAckConn{Conn: c, enabled: enabled, dropped: dropped}, nil
			}
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			ds := &DataStore{pool: pool}
			r, stop := f.request(t, ds, t.Context())
			defer stop()
			i, err := ds.PrepareParentProfileCreationManagement(r, []byte(parentCreationTestBody), nil)
			if i != nil {
				defer i.Close()
			}
			if i != nil || !errors.Is(err, ErrParentProfileCreationUnavailable) || !dropped.Load() {
				t.Fatal("old maintenance ACK loss labeled as new admission", err)
			}
			var current int
			if err = f.ds.pool.QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_operations WHERE actor_id='default'`).Scan(&current); err != nil || current != 0 {
				t.Fatal("maintenance error reserved a new operation", err)
			}
		})
	}
}

// Cleanup joins every worker even after an observer or assertion calls Fatal.
type creationTestWorker struct {
	done chan struct{}
	err  error
}

func startCreationTestWorker(t *testing.T, cleanup func(), run func() error) *creationTestWorker {
	t.Helper()
	w := &creationTestWorker{done: make(chan struct{})}
	go func() { w.err = run(); close(w.done) }()
	t.Cleanup(func() {
		cleanup()
		select {
		case <-w.done:
		case <-time.After(5 * time.Second):
			t.Error("creation worker did not join after cleanup")
		}
	})
	return w
}
func (w *creationTestWorker) join(t *testing.T) error {
	t.Helper()
	select {
	case <-w.done:
		return w.err
	case <-time.After(5 * time.Second):
		t.Fatal("creation worker timeout")
		return context.DeadlineExceeded
	}
}
func creationTestRollback(tx pgx.Tx) {
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	_ = tx.Rollback(ctx)
}

func TestParentProfileCreationCloseInterruptsActualPostClaimCommitWait(t *testing.T) {
	f := newParentCreationFixture(t)
	i, r := f.prepare(t, f.ds)
	if err := f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	creationTestLeases(t, f.ds, i, r)
	blocker, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer creationTestRollback(blocker)
	if _, err = blocker.Exec(t.Context(), `SELECT id FROM accounts WHERE id='default' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	cleanup := func() { i.state.stop(); creationTestRollback(blocker) }
	worker := startCreationTestWorker(t, cleanup, func() error { _, e := f.ds.CommitParentProfileCreation(i); return e })
	creationOperationWaitLock(t, f.settingsDSFixture, "SELECT id FROM accounts")
	closer := startCreationTestWorker(t, cleanup, func() error { i.Close(); return nil })
	if err = closer.join(t); err != nil {
		t.Fatal(err)
	}
	if err = worker.join(t); err == nil {
		t.Fatal("closed post-claim SQL operation succeeded")
	}
	creationTestRollback(blocker)
	if a, b := creationTestCounts(t, f, i.state.reference.record.target); a != 0 || b != 0 {
		t.Fatal("closed post-claim operation wrote profile/settings")
	}
}
