package sessions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

type accountRevokePGFixture struct {
	ds    *datastore.DataStore
	admin *pgxpool.Pool
	svc   *Service
}

func sessionAccountRevokePG(t *testing.T) *accountRevokePGFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic Unix-socket PG fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("own synthetic Unix-socket PG fixture required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("connect own fixture", err)
	}
	name := fmt.Sprintf("synthetic_account_revoke_%d", time.Now().UnixNano())
	var ds *datastore.DataStore
	ownedRole, ownedSchema := false, false
	// Register before the first owned mutation. A failed setup cannot skip
	// cleanup or remove a role/schema whose CREATE never succeeded here.
	t.Cleanup(func() {
		if ds != nil {
			ds.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if ownedSchema {
			if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
				t.Error("remove own schema", err)
			}
		}
		if ownedRole {
			if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); err != nil {
				t.Error("remove own role", err)
			}
		}
		admin.Close()
	})
	if _, err = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatal("create own role", err)
	}
	ownedRole = true
	if _, err = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("create own schema", err)
	}
	ownedSchema = true
	query := u.Query()
	query.Set("search_path", pgx.Identifier{name}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+name)
	query.Set("application_name", name)
	u.RawQuery = query.Encode()
	ds, err = datastore.New(t.Context(), u.String())
	if err != nil {
		t.Fatal("create actual fully migrated store", err)
	}
	if _, err = ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Actor','unchanged synthetic hash'),('other','Synthetic Other','unchanged synthetic hash')`); err != nil {
		t.Fatal(err)
	}
	if _, err = ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic Profile')`); err != nil {
		t.Fatal(err)
	}
	svc, err := NewServiceWithStore(ds, time.Hour)
	if err != nil || svc.Count() != 0 {
		t.Fatal("actual service did not load an initially empty session projection", err)
	}
	return &accountRevokePGFixture{ds: ds, admin: admin, svc: svc}
}
func (f *accountRevokePGFixture) seed(t *testing.T, token, account string) models.Session {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	value := models.Session{Token: token, AccountID: account, Scope: models.SessionScopeJellyfin, ScopeResource: "target", ClientID: "synthetic-device", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := f.ds.Sessions().Create(t.Context(), &value); err != nil {
		t.Fatal("seed PG without service cache adoption", err)
	}
	return value
}
func (f *accountRevokePGFixture) count(t *testing.T, account string) int {
	t.Helper()
	var n int
	if err := f.ds.Pool().QueryRow(t.Context(), "SELECT count(*) FROM sessions WHERE account_id=$1", account).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPGAccountRevokeColdCacheCheckedAndLegacyRoutesUseActualRows(t *testing.T) {
	for _, mode := range []string{"checked all", "checked except", "legacy all", "legacy except"} {
		t.Run(mode, func(t *testing.T) {
			f := sessionAccountRevokePG(t)
			keep := f.seed(t, "synthetic-keep", "actor")
			f.seed(t, "synthetic-other-device", "actor")
			foreign := f.seed(t, "synthetic-foreign", "other")
			if f.svc.Count() != 0 {
				t.Fatal("test accidentally warmed the startup cache")
			}
			var n int64
			var err error
			switch mode {
			case "checked all":
				n, err = f.svc.RevokeAllForAccountContext(t.Context(), "actor")
			case "checked except":
				n, err = f.svc.RevokeAllForAccountExceptContext(t.Context(), "actor", keep.Token)
			case "legacy all":
				n = int64(f.svc.RevokeAllForAccount("actor"))
			case "legacy except":
				n = int64(f.svc.RevokeAllForAccountExcept("actor", keep.Token))
			}
			want := int64(2)
			if strings.HasSuffix(mode, "except") {
				want = 1
				if _, e := f.svc.ValidateScoped(keep.Token, models.SessionScopeJellyfin); e != nil {
					t.Fatal("exact preserved session is not valid", e)
				}
			}
			if err != nil || n != want || f.count(t, "actor") != 2-int(want) || f.count(t, "other") != 1 {
				t.Fatal("cold-cache PG revoke used cache count instead of stored rows", err, n)
			}
			if _, e := f.svc.ValidateScoped(foreign.Token, models.SessionScopeJellyfin); e != nil {
				t.Fatal("other account was revoked", e)
			}
		})
	}
}

func TestPGAccountRevokeCheckedNoStoreAndMemoryJSONRemainUnchanged(t *testing.T) {
	dir := t.TempDir()
	svc, err := NewService(dir, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	value, err := svc.Create("actor", false, "synthetic", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, checked := range []func() (int64, error){
		func() (int64, error) { return svc.RevokeAllForAccountContext(t.Context(), "actor") },
		func() (int64, error) { return svc.RevokeAllForAccountExceptContext(t.Context(), "actor", value.Token) },
		func() (int64, error) { var zero *Service; return zero.RevokeAllForAccountContext(t.Context(), "actor") },
	} {
		if n, e := checked(); n != 0 || !errors.Is(e, datastore.ErrSessionAccountRevokeUnavailable) {
			t.Fatal("PG-only checked API adopted non-PG state", e, n)
		}
	}
	after, err := os.ReadFile(filepath.Join(dir, "sessions.json"))
	if err != nil || !bytes.Equal(before, after) || svc.Count() != 1 {
		t.Fatal("PG API changed legacy memory/JSON state", err)
	}
	if svc.RevokeAllForAccountExcept("actor", value.Token) != 0 || svc.RevokeAllForAccount("actor") != 1 {
		t.Fatal("legacy memory/JSON revoke behavior changed")
	}
}

func TestPGAccountRevokeDoesNotHoldServiceMutexDuringActualPGWait(t *testing.T) {
	f := sessionAccountRevokePG(t)
	value := f.seed(t, "synthetic-cached", "actor")
	if _, err := f.svc.ValidateScoped(value.Token, models.SessionScopeJellyfin); err != nil {
		t.Fatal(err)
	}
	var relation uint32
	if err := f.ds.Pool().QueryRow(t.Context(), "SELECT 'accounts'::regclass::oid").Scan(&relation); err != nil {
		t.Fatal(err)
	}
	holder, err := f.ds.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(t.Context()), 2*time.Second)
		defer stop()
		_ = holder.Rollback(cleanup)
	}()
	if _, err = holder.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		n, e := f.svc.RevokeAllForAccountContext(ctx, "actor")
		if n != 0 {
			t.Error("canceled blocked revoke acknowledged a count")
		}
		done <- e
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var n int
		if err = f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("actual service PG query never waited on original accounts relation")
		}
		time.Sleep(time.Millisecond)
	}
	if !f.svc.mu.TryLock() {
		t.Fatal("service mutex is held across actual PG wait")
	}
	f.svc.mu.Unlock()
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("canceled caller did not receive checked cancellation", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("checked service returned neither joined work nor cancellation")
	}
	var pending int
	if err = f.admin.QueryRow(t.Context(), "SELECT count(*) FROM pg_locks WHERE relation=$1 AND NOT granted", relation).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("service returned before actual query drain", err, pending)
	}
	f.svc.mu.RLock()
	_, retained := f.svc.sessions[value.Token]
	f.svc.mu.RUnlock()
	if !retained {
		t.Fatal("pre-COMMIT failure published a changed cache projection")
	}
	if err = holder.Rollback(t.Context()); err != nil || f.count(t, "actor") != 1 {
		t.Fatal("canceled work changed stored session", err)
	}
}

func TestPGAccountRevokeCacheProjectionNeverDeterminesDeletedCount(t *testing.T) {
	f := sessionAccountRevokePG(t)
	keep := f.seed(t, "synthetic-keep", "actor")
	f.svc.mu.Lock()
	f.svc.sessions[keep.Token] = keep
	for range 5 {
		stale := keep
		stale.Token = fmt.Sprintf("synthetic-stale-%d", len(f.svc.sessions))
		f.svc.sessions[stale.Token] = stale
	}
	f.svc.mu.Unlock()
	n, err := f.svc.RevokeAllForAccountExceptContext(t.Context(), "actor", keep.Token)
	if err != nil || n != 0 || f.count(t, "actor") != 1 || f.svc.Count() != 1 {
		t.Fatal("stale cache fabricated DELETE count or retained projection", err, n)
	}
}

func TestPGAccountRevokeUnavailableAndCancelledAreCheckedWithoutProjectionSuccess(t *testing.T) {
	f := sessionAccountRevokePG(t)
	value := f.seed(t, "synthetic-keep", "actor")
	if _, err := f.svc.ValidateScoped(value.Token, models.SessionScopeJellyfin); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n, err := f.svc.RevokeAllForAccountContext(ctx, "actor"); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled checked operation reached a success", err, n)
	}
	if _, err := f.ds.Pool().Exec(t.Context(), "ALTER TABLE sessions RENAME TO unavailable_account_revoke"); err != nil {
		t.Fatal(err)
	}
	for _, checked := range []func() (int64, error){
		func() (int64, error) { return f.svc.RevokeAllForAccountContext(t.Context(), "actor") },
		func() (int64, error) {
			return f.svc.RevokeAllForAccountExceptContext(t.Context(), "actor", value.Token)
		},
	} {
		if n, err := checked(); n != 0 || !errors.Is(err, datastore.ErrSessionAccountRevokeUnavailable) || errors.Is(err, datastore.ErrSessionAccountRevokeUnknown) {
			t.Fatal("unavailable store was mistaken for successful/unknown COMMIT", err, n)
		}
	}
	if f.svc.RevokeAllForAccount("actor") != 0 || f.svc.RevokeAllForAccountExcept("actor", value.Token) != 0 || f.svc.Count() != 1 {
		t.Fatal("legacy int projection falsely reported persistence success")
	}
	if _, err := f.ds.Pool().Exec(t.Context(), "ALTER TABLE unavailable_account_revoke RENAME TO sessions"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.ValidateScoped(value.Token, models.SessionScopeJellyfin); err != nil {
		t.Fatal("known failed revoke changed stored original", err)
	}
}

func TestPGAccountRevokeExceptPreservesActualScopedRequestFrame(t *testing.T) {
	f := sessionAccountRevokePG(t)
	keep := f.seed(t, "synthetic-kept-frame", "actor")
	f.seed(t, "synthetic-other-device", "actor")
	r := httptest.NewRequest("GET", "http://synthetic.invalid/jellyfin/items", nil)
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, keep.Token, keep.ClientID)
	if err != nil {
		t.Fatal("capture original actual PG frame", err)
	}
	defer release()
	proof, err := f.ds.CaptureInactiveJellyfinApplicability(checked)
	if err != nil {
		t.Fatal("capture original scoped applicability", err)
	}
	defer proof.Close()
	before, err := f.ds.Sessions().Get(t.Context(), keep.Token)
	if err != nil || before == nil {
		t.Fatal(err)
	}
	n, err := f.svc.RevokeAllForAccountExceptContext(t.Context(), "actor", keep.Token)
	after, readErr := f.ds.Sessions().Get(t.Context(), keep.Token)
	if err != nil || n != 1 || readErr != nil || after == nil || *before != *after || proof.Check(t.Context()) != nil || f.ds.CheckProfilePinRequestFrame(checked) != nil {
		t.Fatal("except revoke changed surviving frame or session row", err, n)
	}
}

func TestPGAccountRevokeConcurrentServiceProjectionAndAuthoritativeCounts(t *testing.T) {
	f := sessionAccountRevokePG(t)
	f.seed(t, "synthetic-first", "actor")
	f.seed(t, "synthetic-second", "actor")
	var mu sync.Mutex
	var total int64
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			n, err := f.svc.RevokeAllForAccountContext(t.Context(), "actor")
			if err != nil {
				t.Error("checked parallel revoke failed", err)
			}
			mu.Lock()
			total += n
			mu.Unlock()
		}()
	}
	wait.Wait()
	if total != 2 || f.count(t, "actor") != 0 || f.svc.Count() != 0 {
		t.Fatal("concurrent projection fabricated/deferred actual DELETE counts")
	}
}
