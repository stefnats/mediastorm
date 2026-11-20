package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime"
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
)

type recordingProgressFixture struct {
	*recordingStatePGFixture
	manager                       *config.Manager
	settings                      config.Settings
	security                      *config.ManagedSecurity
	coordinator                   *settingscommit.Coordinator
	objects                       *settingscommit.LockObjects
	core                          *RecordingProgressCore
	observation                   RecordingProgressObservation
	path                          string
	ackEnabled, ackDropped        atomic.Bool
	closeEntered, closeRelease    chan struct{}
	closeSignal, closeReleaseOnce sync.Once
}

type progressCommitConnection struct {
	*parentDropCommit
	enabled   *atomic.Bool
	fixture   *recordingProgressFixture
	holdClose atomic.Bool
}

func (c *progressCommitConnection) Write(p []byte) (int, error) {
	if c.enabled.Load() {
		return c.parentDropCommit.Write(p)
	}
	return c.Conn.Write(p)
}

func (c *progressCommitConnection) Close() error {
	err := c.Conn.Close()
	if c.holdClose.Load() {
		c.fixture.closeSignal.Do(func() { close(c.fixture.closeEntered) })
		<-c.fixture.closeRelease
	}
	return err
}

func progressPG(t *testing.T, enforce bool) *recordingProgressFixture {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("advanced original policy capture is Linux-only")
	}
	base := recordingStatePG(t, 85)
	f := &recordingProgressFixture{recordingStatePGFixture: base, closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
	// A genuine one-connection runtime pool demonstrates there is no nested
	// pool acquisition while the fixed writer transaction owns its backend.
	// Encode the real startup role/schema in the same original URL whose
	// fingerprint the coordinator binds. A copied AfterConnect SET ROLE hook is
	// intentionally unsupported by the one-original-backend write protocol.
	target, err := url.Parse(base.ds.pool.Config().ConnString())
	if err != nil {
		t.Fatal("parse own fixture runtime URL")
	}
	query := target.Query()
	query.Set("search_path", pgx.Identifier{base.schema}.Sanitize()+",pg_catalog")
	query.Set("application_name", base.schema)
	query.Set("options", "-c role="+base.schema)
	target.RawQuery = query.Encode()
	runtimeURL := target.String()
	cfg, err := pgxpool.ParseConfig(runtimeURL)
	if err != nil {
		t.Fatal("parse explicit own startup role/schema")
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	configureQueryLifecycle(cfg.ConnConfig)
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &progressCommitConnection{parentDropCommit: &parentDropCommit{Conn: c, dropped: &f.ackDropped}, enabled: &f.ackEnabled, fixture: f}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("open original one-connection pool")
	}
	t.Cleanup(pool.Close)
	base.ds = &DataStore{pool: pool}
	if cfg.BeforeConnect != nil || cfg.AfterConnect != nil || cfg.BeforeAcquire != nil || cfg.PrepareConn != nil || cfg.AfterRelease != nil || cfg.BeforeClose != nil || cfg.ConnString() != runtimeURL {
		t.Fatal("fixture runtime has a foreign lifecycle hook or mismatched original URL")
	}
	var role, schema, application string
	if pool.QueryRow(t.Context(), `SELECT current_user,current_schema(),current_setting('application_name')`).Scan(&role, &schema, &application) != nil || role != base.schema || schema != base.schema || application != base.schema {
		t.Fatal("own actual startup role/schema/application mismatch")
	}
	f.path = filepath.Join(t.TempDir(), "settings.json")
	f.manager = config.NewManager(f.path)
	f.settings = config.DefaultSettings()
	f.settings.Database.URL = runtimeURL
	f.settings.Security.EnforceProfilePins = enforce
	if err = f.manager.Save(f.settings); err != nil {
		t.Fatal("save own original synthetic settings", err)
	}
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		if f.objects != nil {
			_ = f.objects.Close()
		}
	})
	f.objects, err = settingscommit.OpenLockObjects(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(t.Context(), pool, f.objects)
	if err != nil {
		t.Fatal("create own original coordinator", err)
	}
	g, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal("acquire own actual startup write guard", err)
	}
	if err = f.coordinator.Arm(g); err != nil {
		_ = g.Close()
		t.Fatal("arm own original coordinator", err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = g.Close()
	if err != nil {
		t.Fatal("install own managed policy", err)
	}
	f.core, err = NewRecordingProgressCore(base.ds, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	f.profiles(t)
	f.create(t, "progress")
	now := time.Now().UTC().Truncate(time.Microsecond)
	f.exec(t, `INSERT INTO sessions(token,account_id,client_id,created_at,expires_at) VALUES('synthetic-progress-session','account','progress-client',$1,$2)`, now, now.Add(time.Hour))
	f.observation = RecordingProgressObservation{ArtifactID: strings.Repeat("a", 32), MappingSHA256: strings.Repeat("b", 64), DurationMS: 60000, ObservedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
	return f
}

func (f *recordingProgressFixture) request(t *testing.T, ctx context.Context) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/private-recording-progress", nil).WithContext(ctx)
	r.Header.Set("X-Client-ID", "progress-client")
	r, release, err := f.ds.AuthenticateProfilePinRequest(r, "synthetic-progress-session", "progress-client")
	if err != nil {
		t.Fatal("authenticate original synthetic PG frame", err)
	}
	t.Cleanup(release)
	return r
}
func (f *recordingProgressFixture) prepare(t *testing.T, r *http.Request, command RecordingProgressCommand) *RecordingProgressIntent {
	t.Helper()
	identity, err := f.ds.ReadRecordingTimelineIdentity(r.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	i, err := f.core.Prepare(r, identity, f.observation, command)
	if err != nil {
		t.Fatal("prepare original recording intent", err)
	}
	t.Cleanup(i.Close)
	return i
}
func (f *recordingProgressFixture) issue(t *testing.T, r *http.Request, ttl time.Duration) (string, time.Time) {
	t.Helper()
	issuer := NewPGPinIssuer(f.ds)
	i, err := issuer.Prepare(r, "one", ttl)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	proof, err := issuer.Verify(i, "1234")
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	receipt, err := issuer.Commit(i, proof)
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	token, expiry, err := issuer.Release(r, receipt)
	if err != nil {
		t.Fatal(err)
	}
	return token, expiry
}
func (f *recordingProgressFixture) pin(t *testing.T) {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("1234"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='one'`, string(hash))
}
func progressCommand(action RecordingProgressAction, revision, generation, position int64) RecordingProgressCommand {
	return RecordingProgressCommand{Action: action, ExpectedRevision: revision, WriterGeneration: generation, PositionMS: position}
}

func TestRecordingProgressObservationBounds(t *testing.T) {
	now := time.Now().UTC()
	o := RecordingProgressObservation{strings.Repeat("a", 32), strings.Repeat("b", 64), 60000, now, now.Add(time.Hour)}
	c := progressCommand(RecordingProgressFromStart, 0, 0, 0)
	if !validProgressData(o, c, now) {
		t.Fatal("valid bounded observation denied")
	}
	for _, mutate := range []func(*RecordingProgressObservation){
		func(v *RecordingProgressObservation) { v.ArtifactID = strings.Repeat("A", 32) },
		func(v *RecordingProgressObservation) { v.MappingSHA256 = "bad" },
		func(v *RecordingProgressObservation) { v.DurationMS = 43200001 },
		func(v *RecordingProgressObservation) { v.ObservedAt = now.Add(time.Second) },
		func(v *RecordingProgressObservation) { v.ExpiresAt = now },
		func(v *RecordingProgressObservation) { v.ExpiresAt = now.Add(25 * time.Hour) },
	} {
		bad := o
		mutate(&bad)
		if validProgressData(bad, c, now) {
			t.Fatal("invalid observation accepted")
		}
	}
	for _, bad := range []RecordingProgressCommand{progressCommand("unknown", 0, 0, 0), progressCommand(RecordingProgressResume, 0, 0, 1), progressCommand(RecordingProgressTouch, -1, 0, 0), progressCommand(RecordingProgressTouch, 0, 0, 60001)} {
		if validProgressData(o, bad, now) {
			t.Fatal("invalid command accepted")
		}
	}
	for _, raw := range []string{`{"is_kids_profile":null,"pin_hash":""}`, `{"is_kids_profile":"false","pin_hash":""}`, `{"is_kids_profile":true,"pin_hash":""}`, `{"pin_hash":""}`} {
		if progressAdult(profilePinAccessTuple{profile: parentMutationRow{raw: []byte(raw)}}) {
			t.Fatal("unknown/malformed/child facts became adult")
		}
	}
}

func TestRecordingProgressOriginalFrameCopiesAndOpaqueState(t *testing.T) {
	f := progressPG(t, false)
	r := f.request(t, t.Context())
	i := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	for _, value := range []any{i, *i, f.core, *f.core} {
		for _, format := range []string{"%+v", "%#v"} {
			if strings.Contains(fmt.Sprintf(format, value), "synthetic-progress-session") {
				t.Fatal("opaque formatting disclosed original credential")
			}
		}
		b, e := json.Marshal(value)
		if e != nil || string(b) != "null" {
			t.Fatal("opaque JSON exposed state")
		}
	}
	copyIntent := *i
	copyIntent.Close()
	if _, err := f.core.Commit(&copyIntent); err == nil {
		t.Fatal("copied intent gained authority")
	}
	copyCore := *f.core
	if _, err := copyCore.Commit(i); err == nil {
		t.Fatal("copied core gained authority")
	}
	identity, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	plain := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/private-recording-progress", nil)
	if _, err = f.core.Prepare(plain, identity, f.observation, progressCommand(RecordingProgressFromStart, 0, 0, 0)); err == nil {
		t.Fatal("unminted frame authorized")
	}
	if _, err = f.core.Commit(i); err != nil {
		t.Fatal("copies retired original", err)
	}
	if _, err = f.core.Commit(i); err == nil {
		t.Fatal("consumed original replayed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	terminal := f.prepare(t, f.request(t, ctx), progressCommand(RecordingProgressFromStart, 1, 1, 0))
	cancel()
	if _, err = f.core.Commit(terminal); err == nil {
		t.Fatal("canceled original request was detached into fresh authority")
	}
}

func TestRecordingProgressPINPolicyMatrix(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		t.Run(fmt.Sprint(enforce), func(t *testing.T) {
			f := progressPG(t, enforce)
			for _, master := range []bool{false, true} {
				for _, outcome := range []string{"no-pin", "locked", "unlocked"} {
					t.Run(fmt.Sprintf("master-%t/%s", master, outcome), func(t *testing.T) {
						f.exec(t, `UPDATE accounts SET is_master=$1 WHERE id='account'`, master)
						owner := "account"
						if master {
							f.exec(t, `INSERT INTO accounts(id,username,password_hash) VALUES('owner','Synthetic Owner','synthetic') ON CONFLICT(id) DO NOTHING`)
							owner = "owner"
						}
						f.exec(t, `UPDATE users SET account_id=$1,pin_hash='',is_kids_profile=false WHERE id='one'`, owner)
						if outcome != "no-pin" {
							f.pin(t)
						}
						r := f.request(t, t.Context())
						if outcome == "unlocked" {
							token, _ := f.issue(t, r, time.Minute)
							r.Header.Set("X-Profile-Unlock", token)
						}
						state := f.state(t, "progress")
						i := f.prepare(t, r, progressCommand(RecordingProgressFromStart, state.revision, state.generation, 0))
						defer i.Close()
						_, err := f.core.Commit(i)
						if enforce && outcome == "locked" {
							if !errors.Is(err, ErrRecordingProgressLocked) {
								t.Fatal("PIN policy denial missing", err)
							}
						} else if err != nil {
							t.Fatal("existing DVR policy allowed case denied", err)
						}
					})
				}
			}
		})
	}
}

func TestRecordingProgressOriginalRowsAndEpochChanges(t *testing.T) {
	for index, change := range []string{
		`UPDATE accounts SET username='Changed' WHERE id='account'`,
		`UPDATE sessions SET client_id='other-client' WHERE token='synthetic-progress-session'`,
		`UPDATE users SET is_kids_profile=true WHERE id='one'`,
		`UPDATE users SET pin_hash='malformed' WHERE id='one'`,
		`INSERT INTO profile_unlock_revocations(scope,scope_key,generation) VALUES('profile','one',1) ON CONFLICT(scope,scope_key) DO UPDATE SET generation=profile_unlock_revocations.generation+1`,
	} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			f := progressPG(t, false)
			i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, 0, 0, 0))
			f.exec(t, change)
			if _, err := f.core.Commit(i); err == nil {
				t.Fatal("original tuple change gained permission")
			}
			if state := f.state(t, "progress"); state.revision != 0 {
				t.Fatal("denied write mutated counters")
			}
		})
	}
}

func TestRecordingProgressGrantExpiryPolicyParity(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		t.Run(fmt.Sprint(enforce), func(t *testing.T) {
			f := progressPG(t, enforce)
			f.pin(t)
			r := f.request(t, t.Context())
			token, expiry := f.issue(t, r, 3*time.Second)
			r.Header.Set("X-Profile-Unlock", token)
			i := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
			original, outcome, grantDeadline, err := readProfilePinAccess(i.state.ctx, f.ds.pool, i.state.original, i.state.identity.owner, i.state.hashes, true)
			if err != nil || outcome != ProfilePinUnlocked || !sameParentModeTuple(original, i.state.tuple) || !grantDeadline.Equal(expiry) || !expiry.After(time.Now()) {
				t.Fatal("actual original grant was not still Unlocked after Prepare")
			}
			if !i.state.deadline.After(expiry) {
				t.Fatal("grant expiry incorrectly became unconditional operation expiry")
			}
			timer := time.NewTimer(time.Until(expiry) + 20*time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-t.Context().Done():
				t.Fatal("fixture deadline")
			}
			_, err = f.core.Commit(i)
			if enforce {
				if !errors.Is(err, ErrRecordingProgressLocked) {
					t.Fatal("expired grant with enforcement allowed", err)
				}
			} else if err != nil {
				t.Fatal("pure-time Locked transition denied despite flag=false", err)
			}
			i.Close()
			// A real row deletion is different from the pure time transition.
			// Neither policy flag may adopt a replacement tuple after capture.
			token, _ = f.issue(t, r, time.Minute)
			r.Header.Set("X-Profile-Unlock", token)
			state := f.state(t, "progress")
			deleted := f.prepare(t, r, progressCommand(RecordingProgressFromStart, state.revision, state.generation, 0))
			f.exec(t, `DELETE FROM profile_unlocks WHERE token_hash=$1`, profileUnlockDigest(token))
			if _, err = f.core.Commit(deleted); err == nil {
				t.Fatal("deleted original grant was silently replaced by Locked allowance")
			}
		})
	}
}

func TestRecordingProgressAdmissionAndConcurrentClose(t *testing.T) {
	f := progressPG(t, false)
	r := f.request(t, t.Context())
	first := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	second := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	identity, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.core.Prepare(r, identity, f.observation, progressCommand(RecordingProgressFromStart, 0, 0, 0)); !errors.Is(err, ErrRecordingProgressBusy) {
		t.Fatal("third original-account operation queued")
	}
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() { defer group.Done(); first.Close() }()
	}
	group.Wait()
	second.Close()
	next := f.prepare(t, r, progressCommand(RecordingProgressFromStart, 0, 0, 0))
	next.Close()
	// Pure global admission bookkeeping is separate from the actual actor2
	// request path above; no claim of32 distinct PG actors is made.
	var admitted []string
	defer func() {
		for _, account := range admitted {
			releaseRecordingProgress(account)
		}
	}()
	for n := range 32 {
		account := fmt.Sprintf("synthetic-global-%d", n)
		if !admitRecordingProgress(account) {
			t.Fatal("bounded global slot rejected early")
		}
		admitted = append(admitted, account)
	}
	if admitRecordingProgress("synthetic-over-cap") {
		releaseRecordingProgress("synthetic-over-cap")
		t.Fatal("global33 admitted")
	}
}

func TestRecordingProgressPINHashGrammar(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("1234"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	valid := string(hash)
	for _, good := range []string{"", valid, "$2$" + valid[4:], "$2b$" + valid[4:], "$2y$" + valid[4:], valid[:4] + "31" + valid[6:]} {
		if good != "" {
			if _, err := bcrypt.Cost([]byte(good)); err != nil {
				t.Fatal("positive fixture is not accepted by actual existing Cost parser")
			}
		}
		if !progressPINHash(good) {
			t.Fatal("complete supported bcrypt grammar denied")
		}
	}
	for _, bad := range []string{valid + "suffix", valid[:59], valid[:59] + "_", valid[:6] + "!" + valid[7:], valid[:4] + "+4" + valid[6:], valid[:4] + "0x" + valid[6:]} {
		if progressPINHash(bad) {
			t.Fatal("incomplete/corrupt bcrypt grammar accepted")
		}
	}
}

func TestRecordingProgressMalformedPINStorageDenied(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		t.Run(fmt.Sprint(enforce), func(t *testing.T) {
			f := progressPG(t, enforce)
			hash, err := bcrypt.GenerateFromPassword([]byte("1234"), bcrypt.MinCost)
			if err != nil {
				t.Fatal(err)
			}
			valid := string(hash)
			for n, bad := range []string{valid + "suffix", valid[:59] + "_", valid[:4] + "+4" + valid[6:], valid[:6] + "!" + valid[7:]} {
				t.Run(fmt.Sprintf("malformed-%d", n), func(t *testing.T) {
					f.exec(t, `UPDATE users SET pin_hash=$1 WHERE id='one'`, bad)
					r := f.request(t, t.Context())
					identity, err := f.ds.ReadRecordingTimelineIdentity(t.Context(), "progress")
					if err != nil {
						t.Fatal(err)
					}
					i, err := f.core.Prepare(r, identity, f.observation, progressCommand(RecordingProgressFromStart, 0, 0, 0))
					if i != nil {
						i.Close()
					}
					if err == nil {
						t.Fatal("malformed stored PIN became a policy allowance")
					}
				})
			}
			for n, good := range []string{"$2$" + valid[4:], valid[:4] + "31" + valid[6:]} {
				t.Run(fmt.Sprintf("complete-%d", n), func(t *testing.T) {
					f.exec(t, `UPDATE users SET pin_hash=$1 WHERE id='one'`, good)
					state := f.state(t, "progress")
					i := f.prepare(t, f.request(t, t.Context()), progressCommand(RecordingProgressFromStart, state.revision, state.generation, 0))
					defer i.Close()
					_, err := f.core.Commit(i)
					if enforce {
						if !errors.Is(err, ErrRecordingProgressLocked) {
							t.Fatal("complete unsupported-login hash did not retain ordinary Locked policy", err)
						}
					} else if err != nil {
						t.Fatal("valid legacy/high-cost structure denied by new policy", err)
					}
				})
			}
		})
	}
}
