package datastore

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"novastream/models"
	"strings"
	"sync"
	"testing"
	"time"
)

const regionalBody = `{"version":1,"country":"DE","maxMovieAge":6,"maxEpisodeAge":12}`

func regionalPG(t *testing.T) *ratingPGFixture {
	t.Helper()
	f := ratingPG(t)
	f.exec(t, `CREATE TABLE user_settings(user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,settings JSONB NOT NULL DEFAULT '{}',updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`)
	for _, name := range []string{"079_user_settings_versions.sql", "082_regional_child_policy.sql"} {
		raw, err := embedMigrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		f.exec(t, strings.Split(string(raw), "-- +goose Down")[0])
	}
	return f
}
func regionalRequest(t *testing.T, f *ratingPGFixture, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Method = http.MethodPut
	r.URL.Path = "/api/users/target/kids/regional-policy"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func regionalPrepared(t *testing.T, f *ratingPGFixture) (*ParentRatingIntent, *http.Request) {
	t.Helper()
	r, release := regionalRequest(t, f, t.Context())
	intent, err := f.ds.PrepareParentRegionalPolicy(r, "target", []byte(regionalBody), nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	retry, stop := regionalRequest(t, f, t.Context())
	t.Cleanup(stop)
	retry.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	return intent, retry
}
func regionalVerified(t *testing.T, f *ratingPGFixture, intent *ParentRatingIntent, r *http.Request) *http.Request {
	t.Helper()
	if err := f.ds.BindParentRegionalPolicyRetry(r, intent, "target", []byte(regionalBody), nil); err != nil {
		t.Fatal(err)
	}
	bound, err := f.ds.BoundParentRatingRequest(r, intent)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ds.VerifyParentRatingPassword(bound, intent); err != nil {
		t.Fatal(err)
	}
	return bound
}
func regionalCommitted(t *testing.T, f *ratingPGFixture) models.RegionalChildPolicy {
	t.Helper()
	intent, r := regionalPrepared(t, f)
	bound := regionalVerified(t, f, intent, r)
	records, err := f.ds.LoadUserWriteSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var revision UserRevision
	for _, record := range records {
		if record.User.ID == "target" {
			revision = record.Revision
		}
	}
	if _, err = f.ds.ParentRegionalPolicyTarget(bound.Context(), intent, revision); err != nil {
		t.Fatal(err)
	}
	receipt, err := f.ds.CommitParentRegionalPolicy(intent)
	if err != nil {
		t.Fatal(err)
	}
	_, config, err := f.ds.AcknowledgeParentRegionalPolicy(bound.Context(), intent, receipt, revision)
	if err != nil {
		t.Fatal(err)
	}
	return config
}
func TestRegionalPolicyActualFreshParentCommitKeepsGenericFieldsAndCurrentReads(t *testing.T) {
	f := regionalPG(t)
	before := f.row(t)
	config := regionalCommitted(t, f)
	if config.MaxMovieAge != 6 || config.MaxEpisodeAge != 12 {
		t.Fatal("independent caps lost")
	}
	after := f.row(t)
	for key, value := range before {
		if key != "updated_at" && string(after[key]) != string(value) {
			t.Fatalf("regional commit changed generic profile field %s", key)
		}
	}
	snapshot, err := f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil || snapshot.Policy == nil || *snapshot.Policy != config {
		t.Fatal("current policy did not observe known commit")
	}
	// A legacy writer has no cap fields and cannot overwrite the dedicated row.
	user, err := f.ds.Users().Get(t.Context(), "target")
	if err != nil {
		t.Fatal(err)
	}
	user.Name = "Ordinary rename"
	if err = f.ds.Users().Update(t.Context(), user); err != nil {
		t.Fatal(err)
	}
	current, err := f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil || current.Policy == nil || *current.Policy != config || current.Revision == snapshot.Revision {
		t.Fatal("generic writer overwrote policy or revived stale revision")
	}
}
func TestRegionalPolicyCurrentMissingChildAndInactiveMembershipABAAreDistinct(t *testing.T) {
	f := regionalPG(t)
	f.exec(t, `UPDATE users SET is_kids_profile=true,kids_mode='' WHERE id='target'`)
	profile, err := f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil || !profile.IsKids || profile.Policy != nil {
		t.Fatal("missing child config was invented")
	}
	active, err := f.ds.CurrentRegionalAccount(t.Context(), "actor")
	if err != nil || !active.HasChildren {
		t.Fatal("unconfigured child disabled household applicability")
	}
	f.exec(t, `UPDATE users SET is_kids_profile=false WHERE id='target'`)
	first, err := f.ds.CurrentRegionalAccount(t.Context(), "actor")
	if err != nil || first.HasChildren {
		t.Fatal("known adult household lost inactive compatibility")
	}
	f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('temporary-child','actor','Synthetic Child',true);DELETE FROM users WHERE id='temporary-child'`)
	second, err := f.ds.CurrentRegionalAccount(t.Context(), "actor")
	if err != nil || second.HasChildren || second.Revision == first.Revision {
		t.Fatal("child create-delete revived an old inactive observation")
	}
	if _, err = f.ds.CurrentRegionalAccount(t.Context(), "missing"); err == nil {
		t.Fatal("missing account became inactive")
	}
}
func TestRegionalPolicyRevisionExcludesUnrelatedGlobalFenceAndSurvivesDeleteRecreate(t *testing.T) {
	f := regionalPG(t)
	regionalCommitted(t, f)
	before, err := f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE users SET name=name WHERE id='unrelated'`)
	after, err := f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil || after.Revision != before.Revision {
		t.Fatal("unrelated applicability fence changed per-profile policy")
	}
	f.exec(t, `DELETE FROM users WHERE id='target';INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic replacement')`)
	after, err = f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil || after.Policy != nil || after.Revision == before.Revision {
		t.Fatal("restored profile adopted old caps or incarnation")
	}
}
func TestRegionalPolicyOriginalSeparatePolicyStateAndContextCannotBeReplayed(t *testing.T) {
	for _, mode := range []string{"policy-only", "body", "cancel", "legacy action", "replay"} {
		t.Run(mode, func(t *testing.T) {
			f := regionalPG(t)
			intent, r := regionalPrepared(t, f)
			switch mode {
			case "policy-only":
				f.exec(t, `INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by)VALUES('target',1,'DE',18,18,1,now(),'synthetic')`)
			case "body":
				if err := f.ds.BindParentRegionalPolicyRetry(r, intent, "target", []byte(regionalBody+" "), nil); err == nil {
					t.Fatal("changed raw body adopted intent")
				}
				return
			case "cancel":
				ctx, cancel := context.WithCancel(r.Context())
				cancel()
				r = r.WithContext(ctx)
			case "legacy action":
				if err := f.ds.BindParentRatingPairRetry(r, intent, "target", []byte(regionalBody), nil); err == nil {
					t.Fatal("legacy action accepted regional intent")
				}
				return
			case "replay":
				bound := regionalVerified(t, f, intent, r)
				_ = bound
				if _, err := f.ds.CommitParentRegionalPolicy(intent); err != nil {
					t.Fatal(err)
				}
				if _, err := f.ds.CommitParentRegionalPolicy(intent); err == nil {
					t.Fatal("one-shot regional commit replayed")
				}
				return
			}
			if err := f.ds.BindParentRegionalPolicyRetry(r, intent, "target", []byte(regionalBody), nil); err == nil {
				t.Fatal("changed policy/context adopted original intent")
			}
		})
	}
}
func TestRegionalPolicyDatabaseConstraintsAndServerRevisionIgnoreForgedGeneration(t *testing.T) {
	f := regionalPG(t)
	regionalCommitted(t, f)
	var before, after int64
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT revision FROM regional_profile_policies WHERE user_id='target'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	f.exec(t, `UPDATE regional_profile_policies SET revision=1 WHERE user_id='target'`)
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT revision FROM regional_profile_policies WHERE user_id='target'`).Scan(&after); err != nil || after <= before {
		t.Fatal("caller-supplied policy revision was trusted")
	}
	for _, sql := range []string{`UPDATE regional_profile_policies SET country='US'`, `UPDATE regional_profile_policies SET max_movie_age=7`, `UPDATE regional_profile_policies SET max_episode_age=NULL`} {
		if _, err := f.ds.pool.Exec(t.Context(), sql); err == nil {
			t.Fatal("invalid stored policy accepted")
		}
	}
}
func TestRegionalRestoreGuardInsideTransactionDeniesCurrentOrImportedChildren(t *testing.T) {
	for _, mode := range []string{"current child", "imported child", "healthy adults"} {
		t.Run(mode, func(t *testing.T) {
			f := regionalPG(t)
			if mode == "current child" {
				f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='target'`)
			}
			err := f.ds.WithTx(t.Context(), func(tx *Tx) error { return tx.GuardRegionalRestore(t.Context(), mode == "imported child") })
			if mode == "healthy adults" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrProtectedFamilyRestore) {
				t.Fatal("protected restore was not denied inside transaction")
			}
			if _, err = f.ds.Users().Get(t.Context(), "target"); err != nil {
				t.Fatal("denied restore changed current rows")
			}
		})
	}
}
func TestRegionalRestoreGuardRetainsCurrentApplicabilityUntilOriginalTransactionEnds(t *testing.T) {
	f := regionalPG(t)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	guard := &Tx{tx: tx}
	if err = guard.GuardRegionalRestore(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = f.ds.pool.Exec(ctx, `UPDATE users SET is_kids_profile=true WHERE id='target'`)
	if err == nil || ctx.Err() == nil {
		t.Fatal("family mutation crossed held restore transaction locks")
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRegionalPolicyChangeDuringOriginalPasswordCheckCannotCommitOldDesiredCaps(t *testing.T) {
	f := regionalPG(t)
	intent, retry := regionalPrepared(t, f)
	if err := f.ds.BindParentRegionalPolicyRetry(retry, intent, "target", []byte(regionalBody), nil); err != nil {
		t.Fatal(err)
	}
	bound, err := f.ds.BoundParentRatingRequest(retry, intent)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	original := intent.state.compare
	intent.state.compare = func(hash, password []byte) error { close(entered); <-release; return original(hash, password) }
	result := make(chan error, 1)
	go func() { result <- f.ds.VerifyParentRatingPassword(bound, intent) }()
	<-entered
	_, writeErr := f.ds.pool.Exec(t.Context(), `INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by)VALUES('target',1,'DE',18,18,1,clock_timestamp(),'concurrent-trusted-writer')`)
	close(release)
	verifyErr := <-result
	if writeErr != nil || verifyErr != nil {
		t.Fatal("password comparison unexpectedly held SQL locks or failed", writeErr, verifyErr)
	}
	if _, err = f.ds.CommitParentRegionalPolicy(intent); err == nil {
		t.Fatal("late confirmation overwrote concurrently changed policy")
	}
	current, err := f.ds.CurrentRegionalProfile(t.Context(), "target")
	if err != nil || current.Policy == nil || current.Policy.MaxMovieAge != 18 || current.Policy.MaxEpisodeAge != 18 {
		t.Fatal("rejected original desired values overwrote current policy")
	}
}

// Trace the actual production config query, after its original PIN observation
// exists. This harness never adds a hook to application code or recaptures auth.
type regionalReadMutation struct {
	once   sync.Once
	before func()
}

func (t *regionalReadMutation) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(data.SQL, "SELECT u.id,u.account_id,u.is_kids_profile,") {
		t.once.Do(t.before)
	}
	return ctx
}
func (*regionalReadMutation) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func TestRegionalPolicyRequestUsesOneOriginalPINObservationAndRechecksAfterPolicyRead(t *testing.T) {
	for _, mode := range []string{"unchanged", "PIN changed", "owner changed"} {
		t.Run(mode, func(t *testing.T) {
			f := regionalPG(t)
			wanted := regionalCommitted(t, f)
			cfg := f.cfg.Copy()
			cfg.ConnConfig.Tracer = &regionalReadMutation{before: func() {
				switch mode {
				case "PIN changed":
					f.exec(t, `UPDATE users SET pin_hash='SYNTHETIC_NEW_PIN_HASH' WHERE id='target'`)
				case "owner changed":
					f.exec(t, `UPDATE users SET account_id='other' WHERE id='target'`)
				}
			}}
			pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			readerStore := &DataStore{pool: pool}
			request := f.parentPGFixture.request(t.Context())
			request.Method = http.MethodGet
			request.URL.Path = "/api/users/target/kids/regional-policy"
			request.Header.Set("X-Client-ID", f.session.ClientID)
			request, release, err := readerStore.AuthenticateProfilePinRequest(request, f.session.Token, f.session.ClientID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			got, err := readerStore.ReadRegionalPolicyRequest(request, "target", nil)
			if mode == "unchanged" {
				if err != nil || got == nil || *got != wanted {
					t.Fatal("one-shot original observation rejected an unchanged current policy", err)
				}
			} else if err == nil || got != nil {
				t.Fatal("policy read adopted changed PIN or ownership")
			}
		})
	}
}
