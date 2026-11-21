//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/integration"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/internal/settingsbootstrap"
	"novastream/services/streaming"
)

func preparedNzbConfig(t *testing.T) integration.NzbConfig {
	t.Helper()
	root := t.TempDir()
	return integration.NzbConfig{QueueDatabasePath: filepath.Join(root, "queue.db"), MetadataRootPath: filepath.Join(root, "metadata"), MaxProcessorWorkers: 1}
}
func preparedNzbFixture(t *testing.T, legacy bool) (*integration.NzbSystem, context.CancelFunc) {
	t.Helper()
	cfg := preparedNzbConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	var ns *integration.NzbSystem
	var e error
	if legacy {
		ns, e = integration.NewNzbSystem(cfg, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} })
	} else {
		ns, e = integration.PrepareNzbSystem(ctx, cfg, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} })
	}
	if e != nil {
		cancel()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		cancel()
		if e := ns.Close(); e != nil {
			t.Error(e)
		}
	})
	return ns, cancel
}
func preparedNzbWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("original NZB work did not join")
	}
}

// This is the actual original startup factory, not a readiness mock. Its
// importer remains inactive throughout construction and denied file opening.
func preparedNzbPublicationInstall(t *testing.T, ns *integration.NzbSystem) {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket fixture required")
	}
	admin, e := pgxpool.New(t.Context(), raw)
	if e != nil {
		t.Fatal(e)
	}
	name := fmt.Sprintf("synthetic_prepared_publication_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{name}.Sanitize()
	var startup *settingsbootstrap.Startup
	t.Cleanup(func() {
		if startup != nil {
			_ = startup.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+identifier+" CASCADE"); e != nil {
			t.Error(e)
		}
		if _, e := admin.Exec(ctx, "DROP ROLE IF EXISTS "+identifier); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	if _, e = admin.Exec(t.Context(), "CREATE ROLE "+identifier+" NOLOGIN"); e != nil {
		t.Fatal(e)
	}
	if _, e = admin.Exec(t.Context(), "CREATE SCHEMA "+identifier+" AUTHORIZATION "+identifier); e != nil {
		t.Fatal(e)
	}
	query := u.Query()
	query.Set("search_path", identifier+",pg_catalog")
	query.Set("options", "-c role="+name)
	u.RawQuery = query.Encode()
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = u.String()
	settings.Cache.Directory = t.TempDir()
	if e = manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	startup, e = settingsbootstrap.Begin(t.Context(), manager, "")
	if e != nil {
		t.Fatal(e)
	}
	store, e := startup.Migrate(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = startup.Finish(); e != nil {
		t.Fatal(e)
	}
	issuer, e := startup.NewSABLibraryPublicationIssuer(store, ns.ImporterService())
	if e != nil || issuer == nil {
		t.Fatal("original prepared source could not be installed before activation", e)
	}
	defer issuer.Close()
	if ns.ImporterService().IsRunning() {
		t.Fatal("factory activated importer")
	}
	ns.Database().Connection().SetMaxOpenConns(1)
	holder, e := ns.Database().Connection().Conn(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	defer holder.Close()
	before := ns.Database().Connection().Stats().WaitCount
	bounded, cancelOpen := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelOpen()
	stage, target := t.TempDir(), t.TempDir()
	policy := config.SABPublicationPolicyValues{StageRoot: stage, MaxBytes: 32 << 20, SourceFingerprint: strings.Repeat("a", 64)}
	actual, e := ns.ImporterService().OpenSABStagedPublication(bounded, 1, policy, target, []int{1})
	if actual != nil {
		actual.Close()
	}
	if e == nil || actual != nil || ns.Database().Connection().Stats().WaitCount != before {
		t.Fatal("inactive publication source performed actual SQLite/file work", e)
	}
}

func TestPreparedNzbSystemLifecycle(t *testing.T) {
	t.Run("prepare-no-health-reset", func(t *testing.T) {
		cfg := preparedNzbConfig(t)
		db, e := database.NewDB(database.Config{DatabasePath: cfg.QueueDatabasePath})
		if e != nil {
			t.Fatal(e)
		}
		if e = database.NewHealthRepository(db.Connection()).UpdateFileHealth("synthetic.mkv", database.HealthStatusChecking, nil, nil, nil); e != nil {
			_ = db.Close()
			t.Fatal(e)
		}
		_ = db.Close()
		ns, e := integration.PrepareNzbSystem(t.Context(), cfg, nil, nil)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = ns.Close() })
		record, e := database.NewHealthRepository(ns.Database().Connection()).GetFileHealth("synthetic.mkv")
		if e != nil || record == nil || record.Status != database.HealthStatusChecking {
			t.Fatal("preparation reset health", e)
		}
	})
	t.Run("prepare-no-import-start", func(t *testing.T) {
		ns, _ := preparedNzbFixture(t, false)
		preparedNzbPublicationInstall(t, ns)
		if ns.ImporterService().IsRunning() {
			t.Fatal("preparation started import workers")
		}
		if _, e := ns.Database().Connection().Exec(`INSERT INTO import_queue(nzb_path,status,priority,retry_count,max_retries)VALUES('synthetic.nzb','processing',0,0,3)`); e != nil {
			t.Fatal(e)
		}
		var status string
		if e := ns.Database().Connection().QueryRow(`SELECT status FROM import_queue`).Scan(&status); e != nil || status != "processing" {
			t.Fatal("inactive queue was reset", e)
		}
	})
	t.Run("prepared-stream-denied-before-open", func(t *testing.T) {
		ns, _ := preparedNzbFixture(t, false)
		response, e := ns.Stream(t.Context(), streaming.Request{Method: http.MethodGet, Path: "/webdav/not-present.mkv"})
		if response != nil || e != integration.ErrNzbSystemUnavailable {
			if response != nil {
				_ = response.Close()
			}
			t.Fatal("inactive stream reached filesystem", e)
		}
	})
	t.Run("activate-health-and-queue-once", func(t *testing.T) {
		ns, _ := preparedNzbFixture(t, false)
		if e := database.NewHealthRepository(ns.Database().Connection()).UpdateFileHealth("synthetic.mkv", database.HealthStatusChecking, nil, nil, nil); e != nil {
			t.Fatal(e)
		}
		if e := ns.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		record, e := database.NewHealthRepository(ns.Database().Connection()).GetFileHealth("synthetic.mkv")
		if e != nil || record == nil || record.Status != database.HealthStatusPending || !ns.ImporterService().IsRunning() {
			t.Fatal("actual activation did not initialize dependencies", e)
		}
		if e = database.NewHealthRepository(ns.Database().Connection()).UpdateFileHealth("synthetic.mkv", database.HealthStatusChecking, nil, nil, nil); e != nil {
			t.Fatal(e)
		}
		if ns.Activate(t.Context()) == nil {
			t.Fatal("second activation accepted")
		}
		record, e = database.NewHealthRepository(ns.Database().Connection()).GetFileHealth("synthetic.mkv")
		if e != nil || record.Status != database.HealthStatusChecking {
			t.Fatal("rejected activation repeated health mutation", e)
		}
	})
	t.Run("cancel-before-activation", func(t *testing.T) {
		ns, cancel := preparedNzbFixture(t, false)
		cancel()
		if ns.Activate(t.Context()) == nil || ns.ImporterService().IsRunning() {
			t.Fatal("cancelled NZB system activated")
		}
	})
	t.Run("activation-failure-no-source", func(t *testing.T) {
		ns, _ := preparedNzbFixture(t, false)
		if _, e := ns.Database().Connection().Exec(`DROP TABLE file_health`); e != nil {
			t.Fatal(e)
		}
		if ns.Activate(t.Context()) == nil {
			t.Fatal("failed health reset activated source")
		}
		if r, e := ns.Stream(t.Context(), streaming.Request{Method: http.MethodGet, Path: "/webdav/synthetic.mkv"}); e != integration.ErrNzbSystemUnavailable || r != nil {
			if r != nil {
				_ = r.Close()
			}
			t.Fatal("failed activation published source", e)
		}
	})
	t.Run("partial-constructor-closes-database", func(t *testing.T) {
		cfg := preparedNzbConfig(t)
		cfg.MaxProcessorWorkers = 33
		ns, e := integration.PrepareNzbSystem(t.Context(), cfg, nil, nil)
		if e == nil || ns != nil {
			if ns != nil {
				_ = ns.Close()
			}
			t.Fatal("invalid importer configuration accepted")
		}
		entries, e := os.ReadDir("/proc/self/fd")
		if e != nil {
			t.Fatal(e)
		}
		for _, entry := range entries {
			target, _ := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
			if strings.HasPrefix(target, cfg.QueueDatabasePath) {
				t.Fatal("failed constructor retained actual SQLite descriptor")
			}
		}
	})
	t.Run("source-operation-close-joins", func(t *testing.T) {
		cfg := preparedNzbConfig(t)
		ns, e := integration.PrepareNzbSystem(t.Context(), cfg, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} })
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = ns.Close() })
		service := metadata.NewMetadataService(cfg.MetadataRootPath)
		meta := service.CreateFileMetadata(1000, "synthetic.nzb", metapb.FileStatus_FILE_STATUS_HEALTHY, nil, metapb.Encryption_NONE, "", "")
		if e = service.WriteFileMetadata("movies/synthetic.mkv", meta); e != nil {
			t.Fatal(e)
		}
		if e = ns.Activate(t.Context()); e != nil {
			t.Fatal(e)
		}
		response, e := ns.Stream(t.Context(), streaming.Request{Method: http.MethodGet, Path: "/webdav/movies/synthetic.mkv"})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = response.Close() })
		done, e := nzbfilesystem.SourceCleanupDone(response.Body)
		if e != nil {
			t.Fatal("real producer erased original F2 body", e)
		}
		if e = ns.Close(); e != nil {
			t.Fatal(e)
		}
		select {
		case <-done:
		default:
			t.Fatal("dependency closed before actual source cleanup")
		}
		if ns.Database().Connection().Ping() == nil {
			t.Fatal("actual SQLite dependency survived Close")
		}
	})
	t.Run("unstarted-close-database-closed", func(t *testing.T) {
		ns, _ := preparedNzbFixture(t, false)
		if e := ns.Close(); e != nil {
			t.Fatal(e)
		}
		if ns.Database().Connection().Ping() == nil || ns.Activate(t.Context()) == nil || ns.ImporterService().Start(t.Context()) == nil {
			t.Fatal("unstarted original dependencies remained live or restarted")
		}
		response, e := ns.Stream(t.Context(), streaming.Request{Method: http.MethodGet, Path: "/webdav/synthetic.mkv"})
		if response != nil {
			_ = response.Close()
		}
		if response != nil || e != integration.ErrNzbSystemUnavailable {
			t.Fatal("closed original source accepted a stream", e)
		}
	})
	t.Run("legacy-constructor-starts", func(t *testing.T) {
		ns, _ := preparedNzbFixture(t, true)
		if !ns.ImporterService().IsRunning() {
			t.Fatal("legacy constructor no longer starts queue workers")
		}
	})
}
