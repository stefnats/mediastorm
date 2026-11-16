package users

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/datastore"
	"novastream/models"
)

// Each fixture owns its schema and NOLOGIN role. The existing production
// constructor runs real schema/data migrations, never on the public schema.
type usersPGFixture struct {
	ds          *datastore.DataStore
	admin       *pgxpool.Pool
	uri, schema string
}

func usersPG(t *testing.T) *usersPGFixture {
	t.Helper()
	raw := os.Getenv("USERS_PERSISTENCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix socket fixture required")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal("connect synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_up_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		t.Fatalf("own role: %v",err)
	}
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("own schema")
	}
	q := u.Query()
	q.Set("search_path", name+",pg_catalog")
	q.Set("role", name)
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	f := &usersPGFixture{admin: admin, uri: u.String(), schema: name}
	t.Cleanup(func() {
		if f.ds != nil {
			f.ds.Close()
		}
		c, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, e := admin.Exec(c, "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE"); e != nil {
			t.Error("drop own schema")
		}
		if _, e := admin.Exec(c, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Error("drop own role")
		}
		admin.Close()
	})
	f.ds = f.open(t)
	f.exec(t, `INSERT INTO accounts(id,username,password_hash,is_master) VALUES('master','Synthetic Master','synthetic unused hash',true),('default','Synthetic Default','synthetic unused hash',false),('other','Synthetic Other','synthetic unused hash',false)`)
	return f
}
func (f *usersPGFixture) open(t *testing.T) *datastore.DataStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ds, err := datastore.New(ctx, f.uri, datastore.WithMaxConns(4), datastore.WithMinConns(1))
	if err != nil {
		t.Fatalf("own-schema datastore: %v", err)
	}
	var current, search string
	if err = ds.Pool().QueryRow(ctx, `SELECT current_user,current_schema()`).Scan(&current, &search); err != nil || current != f.schema || search != f.schema {
		ds.Close()
		t.Fatal("fixture scope not exact")
	}
	return ds
}
func (f *usersPGFixture) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, e := f.ds.Pool().Exec(t.Context(), q, args...); e != nil {
		t.Fatalf("synthetic statement: %v", e)
	}
}
func (f *usersPGFixture) service(t *testing.T, ds *datastore.DataStore) *Service {
	t.Helper()
	s, e := NewServiceWithStore(ds, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func mustProfile(t *testing.T, s *Service, name string) models.User {
	t.Helper()
	u, e := s.Create(name)
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func dbUser(t *testing.T, f *usersPGFixture, id string) *models.User {
	t.Helper()
	u, e := f.ds.Users().Get(t.Context(), id)
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func usersVersion(t *testing.T, f *usersPGFixture, id string) string {
	t.Helper()
	var v string
	if e := f.ds.Pool().QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM users WHERE id=$1`, id).Scan(&v); e != nil {
		t.Fatal(e)
	}
	return v
}

func TestUsersPersistenceTwoServicesUnchangedExternalRows(t *testing.T) {
	f := usersPG(t)
	a := f.service(t, f.ds)
	target := mustProfile(t, a, "Target")
	other := mustProfile(t, a, "Other")
	ds2 := f.open(t)
	defer ds2.Close()
	b := f.service(t, ds2)
	if _, e := b.SetKidsProfile(target.ID, true); e != nil {
		t.Fatal(e)
	}
	external := mustProfile(t, b, "External inserted")
	if e := b.Delete(other.ID); e != nil {
		t.Fatal(e)
	}
	before := usersVersion(t, f, target.ID)
	// A's unchanged target is stale, its absent external row is not a deletion,
	// and its stale cached deleted row must not be reinserted on another write.
	root := a.ListAll()[0]
	if root.ID == target.ID || root.ID == other.ID {
		t.Fatal("fixture initial profile order")
	}
	if _, e := a.Rename(root.ID, "Independent rename"); e != nil {
		t.Fatal(e)
	}
	if got := dbUser(t, f, target.ID); got == nil || !got.IsKidsProfile || usersVersion(t, f, target.ID) != before {
		t.Fatal("untouched external change overwritten")
	}
	if dbUser(t, f, external.ID) == nil || dbUser(t, f, other.ID) != nil {
		t.Fatal("external topology overwritten")
	}
	if _, e := a.Rename(target.ID, "Stale target"); !errors.Is(e, datastore.ErrUserWriteConflict) {
		t.Fatalf("stale target accepted: %v", e)
	}
	if _, e := a.Rename(other.ID, "Resurrect"); !errors.Is(e, datastore.ErrUserWriteConflict) {
		t.Fatalf("deleted target upserted: %v", e)
	}
	if e := a.ResyncStorage(t.Context()); e != nil {
		t.Fatal(e)
	}
	got, _ := a.Get(target.ID)
	if !got.IsKidsProfile || a.Exists(other.ID) || !a.Exists(external.ID) {
		t.Fatal("explicit resync not authoritative")
	}
	if _, e := a.Rename(target.ID, "Fresh rename"); e != nil {
		t.Fatal(e)
	}
}
func TestUsersPersistenceExternalFlagOnlySurvivesUnrelatedSave(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	target := mustProfile(t, s, "Original")
	other := mustProfile(t, s, "Other")
	// Equivalent targeted flag-only SQL shape to the uninstalled parent seam.
	f.exec(t, `UPDATE users SET is_kids_profile=true,updated_at=clock_timestamp() WHERE id=$1`, target.ID)
	before := usersVersion(t, f, target.ID)
	if _, e := s.Rename(other.ID, "Changed other"); e != nil {
		t.Fatal(e)
	}
	if !dbUser(t, f, target.ID).IsKidsProfile || usersVersion(t, f, target.ID) != before {
		t.Fatal("external flag overwritten")
	}
}
func TestUsersPersistenceABABatchAndNoop(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	a := mustProfile(t, s, "A")
	b := mustProfile(t, s, "B")
	va, vb := usersVersion(t, f, a.ID), usersVersion(t, f, b.ID)
	s.mu.Lock()
	e := s.saveLocked()
	s.mu.Unlock()
	if e != nil || usersVersion(t, f, a.ID) != va || usersVersion(t, f, b.ID) != vb {
		t.Fatal("empty diff wrote rows")
	}
	// Restore every user value including updated_at: physical revision still changes.
	f.exec(t, `UPDATE users SET name='temporary' WHERE id=$1`, b.ID)
	f.exec(t, `UPDATE users SET name=$2 WHERE id=$1`, b.ID, b.Name)
	s.mu.Lock()
	ua := s.users[a.ID]
	ua.Name = "A staged"
	s.users[a.ID] = ua
	ub := s.users[b.ID]
	ub.Name = "B staged"
	s.users[b.ID] = ub
	e = s.saveLocked()
	s.mu.Unlock()
	if !errors.Is(e, datastore.ErrUserWriteConflict) {
		t.Fatalf("ABA accepted: %v", e)
	}
	if dbUser(t, f, a.ID).Name != a.Name || dbUser(t, f, b.ID).Name != b.Name || usersVersion(t, f, a.ID) != va {
		t.Fatal("batch partially written")
	}
	ga, _ := s.Get(a.ID)
	gb, _ := s.Get(b.ID)
	if ga.Name != a.Name || gb.Name != b.Name {
		t.Fatal("failed staged cache published")
	}
}
func TestUsersPersistenceRollbackAfterSQLWrite(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	a := mustProfile(t, s, "A")
	b := mustProfile(t, s, "B")
	f.exec(t, `ALTER TABLE users ADD CONSTRAINT synthetic_reject_name CHECK(name <> 'reject-write')`)
	s.mu.Lock()
	ids := []string{a.ID, b.ID}
	if ids[0] > ids[1] {
		ids[0], ids[1] = ids[1], ids[0]
	}
	x := s.users[ids[0]]
	x.Name = "first statement succeeds"
	s.users[x.ID] = x
	y := s.users[ids[1]]
	y.Name = "reject-write"
	s.users[y.ID] = y
	e := s.saveLocked()
	s.mu.Unlock()
	if e == nil {
		t.Fatal("invalid batch succeeded")
	}
	if dbUser(t, f, a.ID).Name != a.Name || dbUser(t, f, b.ID).Name != b.Name {
		t.Fatal("SQL batch not rolled back")
	}
	for _, want := range []models.User{a, b} {
		got, _ := s.Get(want.ID)
		if got.Name != want.Name {
			t.Fatal("cache not restored")
		}
	}
}
func TestUsersPersistenceConcurrentBootstrapAndLastDelete(t *testing.T) {
	f := usersPG(t)
	ds2 := f.open(t)
	defer ds2.Close()
	var services [2]*Service
	var errs [2]error
	var wg sync.WaitGroup
	start := make(chan struct{})
	for n, ds := range []*datastore.DataStore{f.ds, ds2} {
		wg.Add(1)
		go func(n int, ds *datastore.DataStore) {
			defer wg.Done()
			<-start
			services[n], errs[n] = NewServiceWithStore(ds, t.TempDir())
		}(n, ds)
	}
	close(start)
	wg.Wait()
	if errs[0] != nil || errs[1] != nil || len(services[0].ListAll()) != 1 || services[0].ListAll()[0].ID != services[1].ListAll()[0].ID {
		t.Fatal("concurrent bootstrap did not adopt same profile")
	}
	second := mustProfile(t, services[0], "Second")
	if e := services[1].ResyncStorage(t.Context()); e != nil {
		t.Fatal(e)
	}
	ids := []string{services[0].ListAll()[0].ID, second.ID}
	start = make(chan struct{})
	for n := range services {
		wg.Add(1)
		go func(n int) { defer wg.Done(); <-start; errs[n] = services[n].Delete(ids[n]) }(n)
	}
	close(start)
	wg.Wait()
	if (errs[0] == nil) == (errs[1] == nil) {
		t.Fatal("last-profile concurrency should have exactly one success")
	}
	var count int
	_ = f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM users`).Scan(&count)
	if count != 1 {
		t.Fatal("last actual profile deleted")
	}
}
func TestUsersPersistenceJSONRollbackCopiesAndMigration(t *testing.T) {
	dir := t.TempDir()
	s, e := NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	id := s.ListAll()[0].ID
	input := []string{"synthetic-list"}
	returned, e := s.SetKidsAllowedLists(id, input)
	if e != nil {
		t.Fatal(e)
	}
	input[0] = "caller-mutated"
	returned.KidsAllowedLists[0] = "return-mutated"
	for _, read := range []func() []models.User{s.List, s.ListAll, func() []models.User { return s.ListForAccount(models.MasterAccountID) }} {
		rows := read()
		if len(rows) != 1 {
			t.Fatal("fixture")
		}
		rows[0].KidsAllowedLists[0] = "reader-mutated"
	}
	got, _ := s.Get(id)
	got.KidsAllowedLists[0] = "getter-mutated"
	got, _ = s.Get(id)
	if got.KidsAllowedLists[0] != "synthetic-list" {
		t.Fatal("mutable reader/return alias")
	}
	previous := cloneUsers(s.users)
	if e = os.Mkdir(filepath.Join(dir, "users.json.tmp"), 0700); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Rename(id, "must rollback"); e == nil {
		t.Fatal("blocked JSON write succeeded")
	}
	if _, e = s.Create("must not remain"); e == nil {
		t.Fatal("blocked JSON insert succeeded")
	}
	if !reflect.DeepEqual(previous, s.users) {
		t.Fatal("failed JSON save published cache")
	}
	if e = os.Remove(filepath.Join(dir, "users.json.tmp")); e != nil {
		t.Fatal(e)
	}
	if _, e = s.SetPin(id, "1234"); e != nil {
		t.Fatal(e)
	}
	reload, e := NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = reload.VerifyPin(id, "1234"); e != nil {
		t.Fatal("private PIN lost across JSON reload")
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "users.json"))
	if !strings.Contains(string(raw), `"pinHash"`) {
		t.Fatal("disk private hash missing")
	}
}
