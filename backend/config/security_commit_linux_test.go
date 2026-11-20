//go:build linux

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
	"novastream/internal/settingscommit"
)

type securityPGFixture struct {
	manager     *Manager
	settings    Settings
	security    *ManagedSecurity
	coordinator *settingscommit.Coordinator
	pool, admin *pgxpool.Pool
	objects     *settingscommit.LockObjects
	name        string
}

func securityPG(t *testing.T) *securityPGFixture {
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
	name := fmt.Sprintf("synthetic_security_%d", time.Now().UnixNano())
	if _, err = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatal("create own fixture database")
	}
	f := &securityPGFixture{admin: admin, name: name}
	t.Cleanup(func() {
		if f.security != nil {
			f.security.Close()
		}
		if f.coordinator != nil {
			f.coordinator.Close()
		}
		if f.objects != nil {
			f.objects.Close()
		}
		if f.pool != nil {
			f.pool.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); err != nil {
			t.Error("remove own fixture database")
		}
		admin.Close()
	})
	u.Path = "/" + name
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal("parse fixture")
	}
	cfg.MaxConns = 1
	cfg.MinConns = 0
	f.pool, err = pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("open actual original pool")
	}
	f.manager = NewManager(filepath.Join(t.TempDir(), "settings.json"))
	f.settings = managedSyntheticSettings(managedSyntheticToken)
	f.settings.Database.URL = u.String()
	f.settings.Security.EnforceProfilePins = true
	if err = f.manager.Save(f.settings); err != nil {
		t.Fatal("save actual original settings", err)
	}
	f.objects, err = settingscommit.OpenLockObjects(t.Context(), f.manager.path)
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.pool, f.objects)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		guard.Close()
		t.Fatal(err)
	}
	f.security, err = InstallManagedSecurity(f.manager, f.coordinator, "")
	guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func TestSecurityCurrentPolicyFenceKeepsOriginalFlagAcrossBusyAttempt(t *testing.T) {
	f := securityPG(t)
	observation, err := f.security.CapturePolicy(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hold, err := f.coordinator.TryProgressFileHold(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	fence, err := f.security.BindPolicy(t.Context(), observation, hold)
	if err != nil {
		t.Fatal(err)
	}
	lease := managedActualLease(t, f.manager)
	before, _ := os.ReadFile(f.manager.path)
	changed := f.settings
	changed.Security.EnforceProfilePins = false
	if err = NewManager(f.manager.path).Save(changed); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("real active native progress fence did not make Save Busy", err)
	}
	after, _ := os.ReadFile(f.manager.path)
	if !bytes.Equal(before, after) {
		t.Fatal("Busy settings changed original policy bytes")
	}
	if flag, err := fence.EnforceProfilePins(); err != nil || !flag {
		t.Fatal("Busy attempt changed already-bound original commit policy")
	}
	if lease.Check(context.Background()) == nil {
		t.Fatal("Busy Save did not invalidate original managed credentials")
	}
	if _, err = f.security.BindPolicy(t.Context(), observation, hold); err == nil {
		t.Fatal("old policy observation rebound after attempt history changed")
	}
	copyFence := *fence
	if _, err = copyFence.EnforceProfilePins(); err == nil {
		t.Fatal("copied policy proof gained authority")
	}
	t.Run("pending-capture-retired", func(t *testing.T) {
		fixture := newManagedHTTPFixture(t, false)
		ready, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		unblock := func() { once.Do(func() { close(release) }) }
		type captureResult struct {
			lease *ManagedJellyfinLease
			err   error
		}
		result := make(chan captureResult, 1)
		received := false
		var captured captureResult
		t.Cleanup(func() {
			unblock()
			if !received {
				select {
				case captured = <-result:
					received = true
				case <-time.After(5 * time.Second):
					t.Error("actual original capture did not join")
					return
				}
			}
			if captured.lease != nil {
				_ = captured.lease.Close()
			}
		})
		originalAttempt := settingsAttemptNow()
		ctx := managedHTTPBoundedTestContext(t)
		go func() {
			lease, err := captureManagedJellyfin(ctx, fixture.manager, "integration", func(ctx context.Context, path, id string) (managedSnapshotFile, managedJellyfinCredentials, error) {
				file, credentials, err := readManagedJellyfinFile(ctx, path, id)
				close(ready)
				<-release
				return file, credentials, err
			})
			result <- captureResult{lease, err}
		}()
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("original regular-file read did not enter")
		}
		if err := fixture.manager.Save(fixture.settings); !errors.Is(err, settingscommit.ErrBusy) {
			t.Fatal("actual original file hold did not refuse managed write", err)
		}
		unblock()
		select {
		case captured = <-result:
			received = true
		case <-time.After(5 * time.Second):
			t.Fatal("original captured data did not return")
		}
		if captured.err != nil || captured.lease == nil {
			t.Fatal("correct old capture was discarded instead of retired")
		}
		if captured.lease.state.attempt != originalAttempt || captured.lease.state.credentials.token != managedHTTPTestToken {
			t.Fatal("retired original capture adopted another attempt or mixed bytes")
		}
		if captured.lease.Context().Err() == nil || captured.lease.Check(context.Background()) == nil {
			t.Fatal("retired original capture regained permission")
		}
		if transport, err := captured.lease.BindJellyfinScope(managedHTTPScope()); err == nil {
			if transport != nil {
				transport.Close()
			}
			t.Fatal("retired original with valid scope gained a transport")
		}
	})
	hold.Close()
	if err = f.manager.Save(changed); err != nil {
		t.Fatal("actual ended progress did not restore ordinary Save", err)
	}
	if _, err = fence.EnforceProfilePins(); err == nil {
		t.Fatal("retired original hold remained valid")
	}
}
func TestSecurityOriginalManagerDomainAndOpaqueState(t *testing.T) {
	f := securityPG(t)
	original, _ := os.ReadFile(f.manager.path)
	value := f.settings
	value.Database.URL = "postgresql://synthetic_other@/foreign?host=/synthetic"
	if err := f.manager.Save(value); !errors.Is(err, settingscommit.ErrDomain) {
		t.Fatal("effective original domain changed without quiesced transition", err)
	}
	actual, _ := os.ReadFile(f.manager.path)
	if !bytes.Equal(original, actual) {
		t.Fatal("rejected domain switch wrote settings")
	}
	copied := *f.security
	if copied.Ready(f.manager, f.coordinator) || copied.Close() == nil {
		t.Fatal("copied security source became original")
	}
	for _, v := range []any{f.security, *f.security} {
		raw, _ := json.Marshal(v)
		if string(raw) != "null" || !strings.Contains(fmt.Sprintf("%#v", v), "opaque") {
			t.Fatal("security source leaked private state")
		}
	}
	other := NewManager(filepath.Join(t.TempDir(), "other.json"))
	if f.security.Ready(other, f.coordinator) {
		t.Fatal("unrelated manager gained registered policy readiness")
	}
}
func TestSecurityCredentialReadDoesNotMakeSaveJoinMediaCleanup(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	entered := make(chan struct{})
	fixture.setResponse(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
	})
	download := openManagedHTTPDownload(t, fixture)
	<-entered
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _ = download.Read(make([]byte, 1)) }()
	done := make(chan error, 1)
	go func() { done <- fixture.manager.Save(fixture.settings) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("managed Save failed while media cleanup belonged to source", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Save waited for media lifecycle instead of signaling invalidation")
	}
	select {
	case <-readDone:
	case <-time.After(5 * time.Second):
		t.Fatal("real native download read did not observe original invalidation")
	}
	if err := download.Close(); err != nil {
		t.Fatal("join real original download", err)
	}
}
func TestSecurityNativeFileProcessHelper(t *testing.T) {
	path := os.Getenv("MEDIASTORM_SYNTHETIC_CONFIG_BARRIER")
	if path == "" {
		return
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal("open independent original barrier")
	}
	defer unix.Close(fd)
	if err = unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err == nil {
		unix.Flock(fd, unix.LOCK_UN)
		t.Fatal("independent process bypassed unfinished original file work")
	}
	if err != unix.EWOULDBLOCK && err != unix.EAGAIN {
		t.Fatal("unexpected independent native conflict")
	}
}
func TestSecurityGuardLossCannotRefundBlockedActualFileWrite(t *testing.T) {
	f := securityPG(t)
	fifo := f.manager.path + ".tmp"
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal("create own blocking write fixture")
	}
	reader, err := unix.Open(fifo, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	launched := false
	// Register release before launching a real syscall. Test failure must not
	// strand the actual Save or the fixture's later coordinator Close.
	t.Cleanup(func() {
		if launched {
			deadline := time.Now().Add(10 * time.Second)
			buffer := make([]byte, 65536)
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
	value := f.settings
	value.Metadata.TMDBAPIKey = strings.Repeat("SYNTHETIC_NON_SECRET_", 65536)
	launched = true
	go func() { done <- f.manager.Save(value) }()
	deadline := time.Now().Add(5 * time.Second)
	written := false
	for time.Now().Before(deadline) {
		n, _ := unix.IoctlGetInt(reader, unix.TIOCINQ)
		if n > 0 {
			written = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !written {
		t.Fatal("actual Encoder write did not enter held FIFO")
	}
	select {
	case <-done:
		t.Fatal("actual FIFO write was not still pending")
	default:
	}
	var pid uint32
	if err = f.pool.QueryRow(t.Context(), `SELECT pid FROM pg_locks WHERE locktype='advisory' AND classid=$1::oid AND objid=$2::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())`, settingscommit.Namespace, settingscommit.Resource).Scan(&pid); err != nil {
		t.Fatal("actual session guard was not held during file write", err)
	}
	if _, err = f.admin.Exec(t.Context(), "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatal("terminate own guard backend")
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestSecurityNativeFileProcessHelper$", "-test.timeout=10s")
	command.Env = append(os.Environ(), "MEDIASTORM_SYNTHETIC_CONFIG_BARRIER="+filepath.Join(filepath.Dir(f.manager.path), ".mediastorm-settings-commit", "commit-barrier.lock"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("unfinished owned write lost native exclusion: %s", output)
	}
	// Drain the real FIFO until the owned Encode/Sync failure and cleanup end.
	buffer := make([]byte, 65536)
	deadline = time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			launched = false
			if err == nil {
				t.Fatal("FIFO Sync unexpectedly acted as a regular settings file")
			}
			return
		default:
		}
		_, _ = unix.Read(reader, buffer)
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual FIFO writer did not finish after release")
}

func TestSecurityRegularReadersRejectFIFOWithoutWriterAndPreserveRawLinks(t *testing.T) {
	for _, kind := range []string{"bootstrap", "policy", "metadata"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "settings.json")
			manager := NewManager(path)
			fifo := path
			if kind == "metadata" {
				if err := manager.Save(DefaultSettings()); err != nil {
					t.Fatal(err)
				}
				fifo = filepath.Join(directory, ".mediastorm-settings-commit", "state.json")
				if os.Remove(fifo) != nil {
					t.Fatal("remove own state fixture")
				}
			}
			if unix.Mkfifo(fifo, 0600) != nil {
				t.Fatal("create own writerless FIFO")
			}
			done := make(chan error, 1)
			t.Cleanup(func() {
				select {
				case <-done:
					return
				default:
				}
				fd, err := unix.Open(fifo, unix.O_WRONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
				if err == nil {
					_, _ = unix.Write(fd, []byte(`{}`))
					unix.Close(fd)
				}
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("writerless FIFO reader did not actually end")
				}
			})
			go func() {
				var err error
				switch kind {
				case "bootstrap":
					_, _, err = ReadManagedBootstrap(t.Context(), manager)
				case "policy":
					_, _, err = readOriginalSecurityPolicy(t.Context(), manager)
				case "metadata":
					err = manager.Save(DefaultSettings())
				}
				done <- err
			}()
			select {
			case err := <-done:
				done <- err
				if err == nil {
					t.Fatal("nonregular file was accepted")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("regular-file reader blocked opening a FIFO without writer")
			}
		})
	}
	original := filepath.Join(t.TempDir(), "settings.json")
	raw := []byte(`{"database":{"url":"synthetic-raw-link"}}`)
	if os.WriteFile(original, raw, 0600) != nil {
		t.Fatal("write regular data")
	}
	alias := filepath.Join(filepath.Dir(original), "alias.json")
	if os.Symlink(original, alias) != nil {
		t.Fatal("make own historical raw alias")
	}
	if value, exists, err := ReadManagedBootstrap(t.Context(), NewManager(alias)); err != nil || !exists || value.Database.URL != "synthetic-raw-link" {
		t.Fatal("ordinary bootstrap lost historical final-symlink semantics")
	}
	if _, _, err := readOriginalSecurityPolicy(t.Context(), NewManager(alias)); err == nil {
		t.Fatal("strict policy reader followed final symlink")
	}
	for _, bad := range []string{original + "/", original + "/.", original + "/../settings.json"} {
		if _, _, err := ReadManagedBootstrap(t.Context(), NewManager(bad)); err == nil {
			t.Fatal("raw kernel path meaning was silently cleaned")
		}
	}
}
func TestSecurityFinalRenameRechecksOriginalNamedObjectsAfterHistoryWait(t *testing.T) {
	for _, kind := range []string{"save", "replace"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "settings.json")
			manager := NewManager(path)
			settings := DefaultSettings()
			if manager.Save(settings) != nil {
				t.Fatal("seed actual original settings")
			}
			original, _ := os.ReadFile(path)
			staged := filepath.Join(directory, "staged.json")
			if os.WriteFile(staged, []byte(`{"synthetic":"new"}`), 0600) != nil {
				t.Fatal("make own staged bytes")
			}
			managedConfigHistory.RLock()
			var once sync.Once
			release := func() { once.Do(managedConfigHistory.RUnlock) }
			done := make(chan error, 1)
			t.Cleanup(func() {
				release()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("actual original writer remained pending")
				}
			})
			go func() {
				if kind == "save" {
					changed := settings
					changed.Security.EnforceProfilePins = true
					done <- manager.Save(changed)
				} else {
					done <- ReplaceManagedSettingsFile(staged, path)
				}
			}()
			waitManagedWriterPending(t)
			named := filepath.Join(directory, ".mediastorm-settings-commit", "commit-barrier.lock")
			if os.Rename(named, named+".retired") != nil || os.WriteFile(named, nil, 0600) != nil {
				t.Fatal("replace own named witness after actual writer wait")
			}
			release()
			select {
			case err := <-done:
				done <- err
				if err == nil {
					t.Fatal("late invalid original objects allowed settings publication")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("actual writer did not finish")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(original, after) {
				t.Fatal("late invalid witness changed original settings")
			}
			if kind == "save" {
				if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
					t.Fatal("denied final publication leaked owned temp")
				}
			} else {
				if _, err := os.Stat(staged); err != nil {
					t.Fatal("denied rename destroyed caller staging")
				}
			}
		})
	}
}
