//go:build linux

package backup

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingsbootstrap"
	"novastream/internal/settingscommit"
)

type settingsBackupFixture struct {
	service  *Service
	manager  *config.Manager
	store    *datastore.DataStore
	admin    *pgxpool.Pool
	startup  *settingsbootstrap.Startup
	settings config.Settings
	name     string
}

func settingsBackupPG(t *testing.T, installed, separate bool) *settingsBackupFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket fixture required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("open fixture admin")
	}
	name := fmt.Sprintf("synthetic_settings_backup_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own fixture database")
	}
	cache := t.TempDir()
	directory := cache
	if separate {
		directory = t.TempDir()
	}
	manager := config.NewManager(filepath.Join(directory, "settings.json"))
	f := &settingsBackupFixture{manager: manager, admin: admin, name: name}
	t.Cleanup(func() {
		if f.startup != nil {
			f.startup.Close()
		} else if f.store != nil {
			f.store.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
		admin.Close()
	})
	u.Path = "/" + name
	f.settings = config.DefaultSettings()
	f.settings.Database.URL = u.String()
	f.settings.Database.MaxOpenConns = 1
	f.settings.Database.MaxIdleConns = 1
	if err = manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	if installed {
		f.startup, err = settingsbootstrap.Begin(t.Context(), manager, "")
		if err == nil {
			f.store, err = f.startup.Migrate(t.Context())
		}
		if err == nil {
			err = f.startup.Finish()
		}
	} else {
		f.store, err = datastore.New(t.Context(), u.String(), datastore.WithMaxConns(1), datastore.WithMinConns(1))
	}
	if err != nil {
		t.Fatal("genuine original datastore startup", err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('one','Synthetic before','synthetic-hash')`); err != nil {
		t.Fatal(err)
	}
	f.service, err = NewService(cache, manager)
	if err != nil {
		t.Fatal(err)
	}
	f.service.SetDataStore(f.store)
	return f
}

type settingsCountReader struct{ reads int }

func (r *settingsCountReader) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }
func TestSecurityBackupBusyBeforeActualNamedAndUploadMutation(t *testing.T) {
	f := settingsBackupPG(t, true, false)
	info, err := f.service.CreateBackup(BackupTypeManual)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var locked bool
	if tx.QueryRow(t.Context(), `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, settingscommit.Namespace, settingscommit.Resource).Scan(&locked) != nil || !locked {
		t.Fatal("hold actual original transaction")
	}
	original, _ := os.ReadFile(f.manager.ConfigPath())
	names, _ := os.ReadDir(f.service.cacheDir)
	if err = f.service.RestoreBackup(info.Filename); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("named restore bypassed actual shared TX", err)
	}
	reader := &settingsCountReader{}
	if err = f.service.RestoreBackupUpload(reader, "synthetic.zip"); !errors.Is(err, settingscommit.ErrBusy) || reader.reads != 0 {
		t.Fatal("upload consumed/staged before real guard admission", err)
	}
	after, _ := os.ReadFile(f.manager.ConfigPath())
	afterNames, _ := os.ReadDir(f.service.cacheDir)
	if !bytes.Equal(original, after) || len(names) != len(afterNames) {
		t.Fatal("Busy restore changed original file/staging inventory")
	}
}
func settingsRewriteBackup(t *testing.T, f *settingsBackupFixture, filename string, settings []byte) []byte {
	t.Helper()
	reader, err := zip.OpenReader(filepath.Join(f.service.backupDir, filename))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	entries := map[string][]byte{}
	for _, file := range reader.File {
		rc, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		entries[file.Name], err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	var manifest Manifest
	if json.Unmarshal(entries["manifest.json"], &manifest) != nil {
		t.Fatal("decode own manifest")
	}
	entries["settings.json"] = settings
	digest := sha256.Sum256(settings)
	manifest.Files["settings.json"] = hex.EncodeToString(digest[:])
	entries["manifest.json"], _ = json.Marshal(manifest)
	var raw bytes.Buffer
	writer := zip.NewWriter(&raw)
	for name, data := range entries {
		file, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if writer.Close() != nil {
		t.Fatal("finish own fixture archive")
	}
	return raw.Bytes()
}
func TestSecurityStandaloneRestoreRefusesSourceTupleBeforeDatabaseMutation(t *testing.T) {
	for _, mode := range []string{"named", "upload"} {
		t.Run(mode, func(t *testing.T) {
			f := settingsBackupPG(t, false, false)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			changed := f.settings
			changed.Database.URL = "postgresql://synthetic_other@/different?host=/synthetic"
			changedRaw, _ := json.Marshal(changed)
			archive := settingsRewriteBackup(t, f, info.Filename, changedRaw)
			if _, err = f.store.Pool().Exec(t.Context(), `UPDATE accounts SET username='Synthetic current' WHERE id='one'`); err != nil {
				t.Fatal(err)
			}
			original, _ := os.ReadFile(f.manager.ConfigPath())
			if mode == "named" {
				name := "mediastorm_backup_changed.zip"
				if os.WriteFile(filepath.Join(f.service.backupDir, name), archive, 0600) != nil {
					t.Fatal("write own archive")
				}
				err = f.service.RestoreBackup(name)
			} else {
				err = f.service.RestoreBackupUpload(bytes.NewReader(archive), "changed.zip")
			}
			if !errors.Is(err, settingscommit.ErrDomain) {
				t.Fatal("standalone original source tuple was silently adopted", err)
			}
			account, err := f.store.Accounts().Get(t.Context(), "one")
			if err != nil || account.Username != "Synthetic current" {
				t.Fatal("denied source change performed database import")
			}
			after, _ := os.ReadFile(f.manager.ConfigPath())
			if !bytes.Equal(original, after) {
				t.Fatal("denied source change published settings")
			}
		})
	}
}
func TestSecurityBackupOriginalSessionSurvivesPool1ImportAndHeldFileWrite(t *testing.T) {
	f := settingsBackupPG(t, true, true)
	cacheSettings := []byte(`{"syntheticCache":"` + strings.Repeat("C", 1<<20) + `"}`)
	cachePath := filepath.Join(f.service.cacheDir, "settings.json")
	if os.WriteFile(cachePath, cacheSettings, 0600) != nil {
		t.Fatal("write own distinct cache settings")
	}
	info, err := f.service.CreateBackup(BackupTypeManual)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), `CREATE TABLE settings_backup_pid(backend int);CREATE FUNCTION settings_backup_insert() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO settings_backup_pid VALUES(pg_backend_pid());RETURN NEW;END$$;CREATE TRIGGER settings_backup_account AFTER INSERT ON accounts FOR EACH ROW EXECUTE FUNCTION settings_backup_insert();UPDATE accounts SET username='Synthetic current' WHERE id='one'`); err != nil {
		t.Fatal(err)
	}
	originalManager, _ := os.ReadFile(f.manager.ConfigPath())
	fifo := cachePath + ".restore.tmp"
	if unix.Mkfifo(fifo, 0600) != nil {
		t.Fatal("create own held extraction fixture")
	}
	reader, err := unix.Open(fifo, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	launched := false
	t.Cleanup(func() {
		if launched {
			buffer := make([]byte, 65536)
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case <-done:
					unix.Close(reader)
					return
				default:
				}
				_, _ = unix.Read(reader, buffer)
				time.Sleep(time.Millisecond)
			}
		}
		unix.Close(reader)
	})
	launched = true
	go func() { done <- f.service.RestoreBackup(info.Filename) }()
	deadline := time.Now().Add(8 * time.Second)
	entered := false
	for time.Now().Before(deadline) {
		n, _ := unix.IoctlGetInt(reader, unix.TIOCINQ)
		if n > 0 {
			entered = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !entered {
		t.Fatal("actual database-first restore did not enter held file write")
	}
	var backend uint32
	var count int
	if f.store.Pool().QueryRow(t.Context(), `SELECT count(*),min(backend) FROM settings_backup_pid`).Scan(&count, &backend) != nil || count != 1 {
		t.Fatal("actual restore transaction did not finish before file write")
	}
	var held bool
	if f.store.Pool().QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND classid=$2::oid AND objid=$3::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted)`, backend, settingscommit.Namespace, settingscommit.Resource).Scan(&held) != nil || !held {
		t.Fatal("actual import backend lost session guard before file completion")
	}
	tx, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var available bool
	if tx.QueryRow(t.Context(), `SELECT pg_try_advisory_xact_lock_shared($1::int,$2::int)`, settingscommit.Namespace, settingscommit.Resource).Scan(&available) != nil || available {
		tx.Rollback(context.Background())
		t.Fatal("actual held restore did not exclude progress transaction")
	}
	tx.Rollback(t.Context())
	// os.Create holds an O_RDWR FIFO, so closing our reader cannot generate
	// EPIPE. Unlink only this owned staging NAME while both real FDs stay open;
	// drain the finite extracted bytes. The final genuine Rename then fails
	// with ENOENT instead of installing a FIFO as the cache settings artifact.
	if err = os.Remove(fifo); err != nil {
		t.Fatal("unlink own held staging pathname", err)
	}
	buffer := make([]byte, 65536)
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			launched = false
			if err == nil || !errors.Is(err, os.ErrNotExist) {
				t.Fatal("missing owned staging did not cause actual final Rename failure", err)
			}
			after, _ := os.ReadFile(f.manager.ConfigPath())
			if !bytes.Equal(originalManager, after) {
				t.Fatal("restore changed separate original manager target")
			}
			cacheAfter, readErr := os.ReadFile(cachePath)
			if readErr != nil || !bytes.Equal(cacheSettings, cacheAfter) {
				t.Fatal("failed final Rename changed original cache settings")
			}
			return
		default:
		}
		_, _ = unix.Read(reader, buffer)
		time.Sleep(time.Millisecond)
	}

	t.Fatal("held actual file restore did not finish after release")
}
