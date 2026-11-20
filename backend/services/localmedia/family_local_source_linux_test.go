//go:build linux

package localmedia

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
)

type familyLocalFileFixture struct {
	ds          *datastore.DataStore
	admin       *pgxpool.Pool
	application string
	service     *Service
	source      *profileunlock.FamilyLocalSource
	owner       *profileunlock.FamilyMediaOwner
	root, path  string
	data        []byte
}

func familyLocalFilePG(t *testing.T) *familyLocalFileFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket database required")
	}
	admin, e := pgxpool.New(t.Context(), raw)
	if e != nil {
		t.Fatal("connect synthetic fixture")
	}
	name := fmt.Sprintf("synthetic_family_local_%d", time.Now().UnixNano())
	if _, e = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); e != nil {
		admin.Close()
		t.Fatal("create own role")
	}
	var ds *datastore.DataStore
	t.Cleanup(func() {
		if ds != nil {
			ds.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); e != nil {
			t.Error("drop own schema")
		}
		if _, e := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Error("drop own role")
		}
		admin.Close()
	})
	if _, e = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); e != nil {
		t.Fatal("create own schema")
	}
	query := u.Query()
	query.Set("search_path", pgx.Identifier{name}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+name)
	query.Set("application_name", name)
	u.RawQuery = query.Encode()
	ds, e = datastore.New(t.Context(), u.String())
	if e != nil {
		t.Fatal("create actual migrated store", e)
	}
	f := &familyLocalFileFixture{ds: ds, admin: admin, application: name, root: t.TempDir(), data: bytes.Repeat([]byte("Synthetic local bytes. "), 4096)}
	f.path = filepath.Join(f.root, "file.bin")
	if e = os.WriteFile(f.path, f.data, 0600); e != nil {
		t.Fatal(e)
	}
	for _, step := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Actor','synthetic')`, nil},
		{`INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic Target')`, nil},
		{`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('library','Synthetic Library','movie',$1)`, []any{f.root}},
		{`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('library','all')`, nil},
		{`INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('item','library','file.bin',$1,'file.bin','movie',$2)`, []any{f.path, int64(len(f.data))}},
	} {
		if _, e = ds.Pool().Exec(t.Context(), step.sql, step.args...); e != nil {
			t.Fatal("seed synthetic source", e)
		}
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "SYNTHETIC_FAMILY_LOCAL_SESSION", AccountID: "actor", ClientID: "synthetic-device", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if e = ds.Sessions().Create(t.Context(), &session); e != nil {
		t.Fatal(e)
	}
	r := httptest.NewRequest("GET", "http://synthetic.invalid/internal-family-local", nil)
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
	r = r.WithContext(ctx)
	pins := profileunlock.NewPGProfilePins(ds)
	checked, release, e := pins.Authenticate(r, session.Token, session.ClientID)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	f.owner, e = pins.CaptureFamilyMediaOwner(checked, "target")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.owner.Close)
	f.source, e = f.owner.CaptureLocalSource(t.Context(), "item")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.source.Close)
	f.service = &Service{store: ds, repo: ds.LocalMedia()}
	return f
}
func familyOpenLocal(t *testing.T, f *familyLocalFileFixture) profileunlock.FamilyLocalFile {
	t.Helper()
	opened, e := f.service.OpenFamilyLocalSource(t.Context(), f.source)
	if e != nil {
		t.Fatal("open actual held local source", e)
	}
	t.Cleanup(func() { _ = opened.Close() })
	return opened
}
func TestFamilyLocalHeldOriginalFDActualRangesAndSafeInfo(t *testing.T) {
	f := familyLocalFilePG(t)
	opened := familyOpenLocal(t, f)
	info := opened.Info()
	if info.ItemID != "item" || info.LibraryID != "library" || info.Name != "file.bin" || info.Size != int64(len(f.data)) {
		t.Fatal("safe original file metadata mismatch")
	}
	for _, span := range [][2]int64{{0, 7}, {32, 103}, {int64(len(f.data)) - 9, 9}, {int64(len(f.data)), 0}} {
		reader, e := opened.OpenReader(t.Context(), span[0], span[1])
		if e != nil {
			t.Fatal(e)
		}
		actual, e := io.ReadAll(reader)
		_ = reader.Close()
		if e != nil || !bytes.Equal(actual, f.data[span[0]:span[0]+span[1]]) {
			t.Fatal("exact held-FD range changed", e)
		}
	}
	for _, span := range [][2]int64{{-1, 1}, {0, -1}, {int64(len(f.data)) + 1, 0}, {1, int64(^uint64(0) >> 1)}} {
		if reader, e := opened.OpenReader(t.Context(), span[0], span[1]); e == nil || reader != nil {
			t.Fatal("invalid range opened reader")
		}
	}
	if second, e := f.service.OpenFamilyLocalSource(t.Context(), f.source); e == nil || second != nil {
		t.Fatal("original source acquired a second physical file")
	}
}
func TestFamilyLocalNamedRootAncestorAndLeafReplacementRetireHeldFD(t *testing.T) {
	for _, kind := range []string{"root", "leaf", "symlink", "hardlink", "inplace", "sqlpath", "acl"} {
		t.Run(kind, func(t *testing.T) {
			f := familyLocalFilePG(t)
			opened := familyOpenLocal(t, f)
			reader, e := opened.OpenReader(t.Context(), 0, 32)
			if e != nil {
				t.Fatal(e)
			}
			defer reader.Close()
			switch kind {
			case "root":
				if e = os.Rename(f.root, f.root+"-retired"); e != nil {
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = os.RemoveAll(f.root + "-retired") })
				if e = os.Mkdir(f.root, 0700); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(f.path, f.data, 0600); e != nil {
					t.Fatal(e)
				}
			case "leaf":
				if e = os.Rename(f.path, f.path+".retired"); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(f.path, f.data, 0600); e != nil {
					t.Fatal(e)
				}
			case "symlink":
				if e = os.Rename(f.path, f.path+".retired"); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(f.path+".retired", f.path); e != nil {
					t.Fatal(e)
				}
			case "hardlink":
				if e = os.Link(f.path, f.path+".alias"); e != nil {
					t.Fatal(e)
				}
			case "inplace":
				file, e := os.OpenFile(f.path, os.O_WRONLY, 0)
				if e != nil {
					t.Fatal(e)
				}
				_, e = file.WriteAt([]byte("Changed"), 0)
				file.Close()
				if e != nil {
					t.Fatal(e)
				}
			case "sqlpath":
				_, e = f.ds.Pool().Exec(t.Context(), "UPDATE local_media_items SET file_path=file_path WHERE id='item'")
				if e != nil {
					t.Fatal(e)
				}
			case "acl":
				_, e = f.ds.Pool().Exec(t.Context(), "UPDATE media_library_access_policies SET access_mode=access_mode WHERE library_id='library'")
				if e != nil {
					t.Fatal(e)
				}
			}
			if opened.Check(t.Context()) == nil {
				t.Fatal("old held source accepted named/physical/ACL replacement")
			}
			bytes := make([]byte, 32)
			if n, e := reader.Read(bytes); n != 0 || e == nil {
				t.Fatal("changed original leaked held bytes")
			}
		})
	}
}
func TestFamilyLocalUnsafeInitialFileAndForeignStoreDenied(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink", "size", "directory", "foreignstore", "missingrepo"} {
		t.Run(kind, func(t *testing.T) {
			f := familyLocalFilePG(t)
			switch kind {
			case "symlink":
				if e := os.Rename(f.path, f.path+".real"); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(f.path+".real", f.path); e != nil {
					t.Fatal(e)
				}
			case "hardlink":
				if e := os.Link(f.path, f.path+".alias"); e != nil {
					t.Fatal(e)
				}
			case "size":
				if e := os.WriteFile(f.path, []byte("short"), 0600); e != nil {
					t.Fatal(e)
				}
			case "directory":
				if e := os.Remove(f.path); e != nil {
					t.Fatal(e)
				}
				if e := os.Mkdir(f.path, 0700); e != nil {
					t.Fatal(e)
				}
			case "foreignstore":
				f.service.store = &datastore.DataStore{}
			case "missingrepo":
				f.service.repo = nil
			}
			if opened, e := f.service.OpenFamilyLocalSource(t.Context(), f.source); e == nil || opened != nil {
				t.Fatal("unsafe physical source acquired")
			}
		})
	}
}
func TestFamilyLocalReaderAdmissionRefundsOnlyAfterActualClose(t *testing.T) {
	f := familyLocalFilePG(t)
	opened := familyOpenLocal(t, f)
	var readers []io.ReadCloser
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()
	for range 8 {
		reader, e := opened.OpenReader(t.Context(), 0, 32)
		if e != nil {
			t.Fatal(e)
		}
		readers = append(readers, reader)
	}
	if reader, e := opened.OpenReader(t.Context(), 0, 32); reader != nil || e == nil {
		t.Fatal("per-original reader admission exceeded")
	}
	_ = readers[0].Close()
	reader, e := opened.OpenReader(t.Context(), 0, 32)
	if e != nil {
		t.Fatal("actual close failed to refund", e)
	}
	readers = append(readers, reader)
	if e = opened.Close(); e != nil {
		t.Fatal(e)
	}
	for _, reader := range readers {
		if n, e := reader.Read(make([]byte, 8)); n != 0 || e == nil {
			t.Fatal("closed owner retained duplicate bytes")
		}
	}
	concrete := opened.(*familyLocalFile)
	select {
	case <-concrete.done:
	default:
		t.Fatal("Close returned before original FD cleanup")
	}
	familyLocalReaderAdmission.Lock()
	count := familyLocalReaderAdmission.count
	familyLocalReaderAdmission.Unlock()
	if count != 0 {
		t.Fatal("actual closed duplicates retained admission")
	}
}
func TestFamilyLocalReaderCancellationAndConcurrentOwnerCloseJoin(t *testing.T) {
	f := familyLocalFilePG(t)
	opened := familyOpenLocal(t, f)
	ctx, cancel := context.WithCancel(t.Context())
	reader, e := opened.OpenReader(ctx, 0, 64)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	if n, e := reader.Read(make([]byte, 64)); n != 0 || e == nil {
		t.Fatal("canceled consumer published bytes")
	}
	_ = reader.Close()
	reader, e = opened.OpenReader(t.Context(), 0, 64)
	if e != nil {
		t.Fatal("consumer cancel retired original source", e)
	}
	done := make(chan struct{})
	go func() { f.owner.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actual owner/FD cleanup failed to join")
	}
	if n, e := reader.Read(make([]byte, 64)); n != 0 || e == nil {
		t.Fatal("original owner closure did not stop held duplicate")
	}
	_ = reader.Close()
}
func TestFamilyLocalReadStagesAtMost32KiBAndUsesOriginalDuplicate(t *testing.T) {
	f := familyLocalFilePG(t)
	opened := familyOpenLocal(t, f)
	first, e := opened.OpenReader(t.Context(), 0, int64(len(f.data)))
	if e != nil {
		t.Fatal(e)
	}
	defer first.Close()
	second, e := opened.OpenReader(t.Context(), 17, 32)
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	buffer := make([]byte, len(f.data))
	n, e := first.Read(buffer)
	if e != nil || n != 32<<10 || !bytes.Equal(buffer[:n], f.data[:n]) {
		t.Fatal("bounded staging changed")
	}
	actual, e := io.ReadAll(second)
	if e != nil || !bytes.Equal(actual, f.data[17:49]) {
		t.Fatal("duplicate cursor changed another reader")
	}
}
func TestFamilyLocalFailedOpenerCannotRetryOrRetarget(t *testing.T) {
	f := familyLocalFilePG(t)
	if e := os.Rename(f.path, f.path+".original"); e != nil {
		t.Fatal(e)
	}
	if opened, e := f.service.OpenFamilyLocalSource(t.Context(), f.source); e == nil || opened != nil {
		t.Fatal("missing physical file opened")
	}
	if e := os.Rename(f.path+".original", f.path); e != nil {
		t.Fatal(e)
	}
	if opened, e := f.service.OpenFamilyLocalSource(t.Context(), f.source); e == nil || opened != nil {
		t.Fatal("failed one-shot acquisition was retried")
	}
	if source, e := f.owner.CaptureLocalSource(t.Context(), "other"); e == nil || source != nil {
		t.Fatal("failed acquisition retargeted owner")
	}
	if (&Service{store: f.ds, repo: (&datastore.DataStore{}).LocalMedia()}).FamilyLocalSourceReady(f.ds) {
		t.Fatal("foreign repository pool reported ready")
	}
}
func TestFamilyLocalSourceCloseCancelsActualBlockedReadAndJoinsDuplicate(t *testing.T) {
	f := familyLocalFilePG(t)
	opened := familyOpenLocal(t, f)
	reader, e := opened.OpenReader(t.Context(), 0, 64)
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	tx, e := f.ds.Pool().Begin(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); e != nil {
		t.Fatal(e)
	}
	type result struct {
		n int
		e error
	}
	read := make(chan result, 1)
	go func() { n, e := reader.Read(make([]byte, 64)); read <- result{n, e} }()
	waiting := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		// The fixture's NOLOGIN SET ROLE cannot inspect the login role's
		// other backend query/wait fields. Observe only this exact application
		// through the already-owned admin connection, without granting rights.
		if e = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM accounts a WHERE a.id=%')`, f.application).Scan(&waiting); e != nil {
			t.Fatal(e)
		}
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		t.Fatal("actual reader never reached blocked current SQL")
	}
	done := make(chan struct{})
	go func() { f.source.Close(); close(done) }()
	select {
	case got := <-read:
		if got.n != 0 || got.e == nil {
			t.Fatal("late blocked read returned bytes")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("source cancellation did not join reader SQL")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("source close did not join actual duplicate")
	}
	concrete := reader.(*familyLocalReader)
	select {
	case <-concrete.done:
	default:
		t.Fatal("sourceClose returned before duplicate cleanup")
	}
}
func TestFamilyLocalOwnerRefundJoinsInterruptedReaderCallbackWrapper(t *testing.T) {
	f := familyLocalFilePG(t)
	opened := familyOpenLocal(t, f)
	raw, e := opened.OpenReader(t.Context(), 0, 64)
	if e != nil {
		t.Fatal(e)
	}
	reader := raw.(*familyLocalReader)
	defer reader.Close()
	// Detach both original still-live cancellation links first. A controlled
	// replacement uses the same production stop/join helper and actual interrupt,
	// but keeps its wrapper running after real FD/read completion.
	reader.stop()
	life, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	reader.stop = familyLocalAfterFunc(life, func() { reader.interrupt(); close(entered); <-release })
	cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("actual interrupt callback did not enter")
	}
	select {
	case <-reader.interruptDone:
	default:
		t.Fatal("barrier preceded actual FD/read completion")
	}
	concrete := opened.(*familyLocalFile)
	concrete.mu.Lock()
	registered := concrete.readers[reader]
	concrete.mu.Unlock()
	familyLocalReaderAdmission.Lock()
	charged := familyLocalReaderAdmission.count
	familyLocalReaderAdmission.Unlock()
	if !registered || charged != 1 {
		t.Fatal("interrupted reader disappeared/refunded before wrapper completion")
	}
	closed := make(chan struct{})
	go func() { f.owner.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("original PG owner refunded before interrupted callback joined")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("original cleanup did not join callback")
	}
	select {
	case <-reader.done:
	default:
		t.Fatal("original cleanup skipped full reader completion")
	}
	familyLocalReaderAdmission.Lock()
	charged = familyLocalReaderAdmission.count
	familyLocalReaderAdmission.Unlock()
	if charged != 0 {
		t.Fatal("joined callback retained global reservation")
	}
}
