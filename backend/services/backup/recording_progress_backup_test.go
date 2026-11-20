//go:build linux

package backup

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/models"
)

// Only the dedicated existing Unix fixture is reachable. This controlled
// proxy drops an actually received server COMMIT acknowledgement, not a mock
// result or a fabricated transaction outcome. Close joins both real pumps.
type positionBackupProxy struct {
	listener            net.Listener
	ctx                 context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	closing             bool
	connections         []net.Conn
	wg                  sync.WaitGroup
	once                sync.Once
	armed, dropped      atomic.Bool
	directory, upstream string
}

func newPositionBackupProxy(t *testing.T, u *url.URL) *positionBackupProxy {
	t.Helper()
	temp := os.Getenv("TMPDIR")
	if !strings.HasPrefix(temp, "/home/stefan/ops-runbook/tmp/") {
		t.Fatal("private persistent fixture temporary root required")
	}
	directory, err := os.MkdirTemp(temp, "p085-")
	if err != nil {
		t.Fatal(err)
	}
	port := u.Query().Get("port")
	if _, err = strconv.Atoi(port); err != nil {
		os.RemoveAll(directory)
		t.Fatal("explicit own fixture port required")
	}
	path := filepath.Join(directory, ".s.PGSQL."+port)
	listener, err := net.Listen("unix", path)
	if err != nil {
		os.RemoveAll(directory)
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	p := &positionBackupProxy{listener: listener, ctx: ctx, cancel: cancel, directory: directory, upstream: filepath.Join(u.Query().Get("host"), ".s.PGSQL."+port)}
	t.Cleanup(p.close)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closing {
				p.mu.Unlock()
				client.Close()
				continue
			}
			p.connections = append(p.connections, client)
			p.wg.Add(1)
			p.mu.Unlock()
			go p.forward(client)
		}
	}()
	return p
}
func (p *positionBackupProxy) forward(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	dial, stop := context.WithTimeout(p.ctx, 5*time.Second)
	server, err := (&net.Dialer{}).DialContext(dial, "unix", p.upstream)
	stop()
	if err != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.closing {
		p.mu.Unlock()
		return
	}
	p.connections = append(p.connections, server)
	p.mu.Unlock()
	inputDone := make(chan struct{})
	go func() { _, _ = io.Copy(server, client); server.Close(); close(inputDone) }()
	defer func() { client.Close(); server.Close(); <-inputDone }()
	for {
		header := make([]byte, 5)
		if _, err = io.ReadFull(server, header); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(header[1:])
		if n < 4 || n > 64<<20 {
			return
		}
		body := make([]byte, int(n)-4)
		if _, err = io.ReadFull(server, body); err != nil {
			return
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" && p.armed.CompareAndSwap(true, false) {
			p.dropped.Store(true)
			return
		}
		if _, err = client.Write(header); err != nil {
			return
		}
		if _, err = client.Write(body); err != nil {
			return
		}
	}
}
func (p *positionBackupProxy) close() {
	p.once.Do(func() {
		p.cancel()
		p.mu.Lock()
		p.closing = true
		connections := append([]net.Conn(nil), p.connections...)
		p.mu.Unlock()
		p.listener.Close()
		for _, c := range connections {
			c.Close()
		}
		p.wg.Wait()
		os.RemoveAll(p.directory)
	})
}

type positionServiceFixture struct {
	*settingsBackupFixture
	observer    *pgxpool.Pool
	objects     *settingscommit.LockObjects
	coordinator *settingscommit.Coordinator
	security    *config.ManagedSecurity
	core        *datastore.RecordingProgressCore
	observation datastore.RecordingProgressObservation
	proxy       *positionBackupProxy
}

func positionServicePG(t *testing.T, withProxy bool) *positionServiceFixture {
	t.Helper()
	base := settingsBackupPG(t, false, false)
	f := &positionServiceFixture{settingsBackupFixture: base}
	if withProxy {
		u, err := url.Parse(f.settings.Database.URL)
		if err != nil {
			t.Fatal(err)
		}
		f.proxy = newPositionBackupProxy(t, u)
		q := u.Query()
		q.Set("host", f.proxy.directory)
		q.Set("sslmode", "disable")
		u.RawQuery = q.Encode()
		f.store.Close()
		f.settings.Database.URL = u.String()
		if err = f.manager.Save(f.settings); err != nil {
			t.Fatal(err)
		}
		f.store, err = datastore.New(t.Context(), u.String(), datastore.WithMaxConns(1), datastore.WithMinConns(1))
		if err != nil {
			t.Fatal(err)
		}
		f.service.SetDataStore(f.store)
	}
	var err error
	f.observer, err = pgxpool.New(t.Context(), f.settings.Database.URL)
	if err != nil {
		t.Fatal(err)
	}
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
		f.observer.Close()
	})
	f.objects, err = settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.store.Pool(), f.objects)
	if err != nil {
		t.Fatal(err)
	}
	g, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(g); err != nil {
		g.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	g.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.core, err = datastore.NewRecordingProgressCore(f.store, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('position-owner','one','Synthetic Position');INSERT INTO sessions(token,account_id,client_id,created_at,expires_at)VALUES('synthetic-position-backup-session','one','position-client',now(),now()+interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	record := models.Recording{ID: "position-recording", UserID: "position-owner", Type: models.RecordingTypeTimeBlock, Status: models.RecordingStatusCompleted, Title: "Synthetic Position Recording", StartAt: now.Add(-time.Hour), EndAt: now, CreatedAt: now, UpdatedAt: now}
	if err = f.store.Recordings().Create(t.Context(), &record); err != nil {
		t.Fatal(err)
	}
	f.observation = datastore.RecordingProgressObservation{ArtifactID: strings.Repeat("a", 32), MappingSHA256: strings.Repeat("b", 64), DurationMS: 60000, ObservedAt: now, ExpiresAt: now.Add(24 * time.Hour)}
	positionServiceCommit(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressFromStart})
	positionServiceCommit(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: 1, WriterGeneration: 1, PositionMS: 25000})
	return f
}
func positionServiceIntent(t *testing.T, f *positionServiceFixture, command datastore.RecordingProgressCommand) *datastore.RecordingProgressIntent {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/position-backup", nil)
	r.Header.Set("X-Client-ID", "position-client")
	r, release, err := f.store.AuthenticateProfilePinRequest(r, "synthetic-position-backup-session", "position-client")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	identity, err := f.store.ReadRecordingTimelineIdentity(r.Context(), "position-recording")
	if err != nil {
		t.Fatal(err)
	}
	intent, err := f.core.Prepare(r, identity, f.observation, command)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	return intent
}
func positionServiceCommit(t *testing.T, f *positionServiceFixture, command datastore.RecordingProgressCommand) {
	t.Helper()
	i := positionServiceIntent(t, f, command)
	if _, err := f.core.Commit(i); err != nil {
		t.Fatal(err)
	}
	i.Close()
}
func positionServiceRows(t *testing.T, f *positionServiceFixture) datastore.RecordingPlaybackPositionBackupRows {
	t.Helper()
	rows, err := f.store.ExportRecordingPlaybackPositions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
func positionServiceIdentity(t *testing.T, f *positionServiceFixture) string {
	t.Helper()
	v, err := f.store.ReadRecordingTimelineIdentity(t.Context(), "position-recording")
	if err != nil {
		t.Fatal(err)
	}
	return v.Incarnation().String()
}
func positionServiceFresh(t *testing.T, f *positionServiceFixture, old string) {
	t.Helper()
	if positionServiceIdentity(t, f) == old {
		t.Fatal("restore imported the old084 incarnation")
	}
	var rev, gen int64
	var operation *string
	var tombstone bool
	if f.store.Pool().QueryRow(t.Context(), `SELECT revision,writer_generation,last_operation_id::text,tombstone FROM recording_playback_state WHERE recording_id='position-recording'`).Scan(&rev, &gen, &operation, &tombstone) != nil || rev != 0 || gen != 0 || operation != nil || !tombstone {
		t.Fatal("restore imported writer counters or operation")
	}
}
func positionRewriteZIP(t *testing.T, f *positionServiceFixture, filename string, change func(map[string]json.RawMessage)) []byte {
	t.Helper()
	reader, err := zip.OpenReader(filepath.Join(f.service.backupDir, filename))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	entries := map[string][]byte{}
	for _, file := range reader.File {
		r, err := file.Open()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		entries[file.Name] = raw
	}
	var data map[string]json.RawMessage
	if json.Unmarshal(entries["database.json"], &data) != nil {
		t.Fatal("actual database member invalid")
	}
	change(data)
	entries["database.json"], err = json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if json.Unmarshal(entries["manifest.json"], &manifest) != nil {
		t.Fatal("actual manifest invalid")
	}
	digest := sha256.Sum256(entries["database.json"])
	manifest.Files["database.json"] = hex.EncodeToString(digest[:])
	entries["manifest.json"], err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	writer := zip.NewWriter(&raw)
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		out, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = out.Write(entries[name]); err != nil {
			t.Fatal(err)
		}
	}
	if writer.Close() != nil {
		t.Fatal("actual ZIP finish failed")
	}
	return raw.Bytes()
}

func TestRecordingPositionBackupActualZIPRestoreRetainsDataAndFreshState(t *testing.T) {
	for _, mode := range []string{"named", "upload"} {
		t.Run(mode, func(t *testing.T) {
			f := positionServicePG(t, false)
			before := positionServiceRows(t, f)
			old := positionServiceIdentity(t, f)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			positionServiceCommit(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: 2, WriterGeneration: 1, PositionMS: 30000})
			if mode == "named" {
				err = f.service.RestoreBackup(info.Filename)
			} else {
				raw, readErr := os.ReadFile(filepath.Join(f.service.backupDir, info.Filename))
				if readErr != nil {
					t.Fatal(readErr)
				}
				err = f.service.RestoreBackupUpload(bytes.NewReader(raw), "positions.zip")
			}
			if err != nil {
				t.Fatal("actual full ZIP restore", err)
			}
			after := positionServiceRows(t, f)
			if len(after) != 1 || after[0] != before[0] {
				t.Fatal("historical085 data not preserved exactly")
			}
			positionServiceFresh(t, f, old)
			if after[0].Incarnation == positionServiceIdentity(t, f) {
				t.Fatal("historical payload became current playable identity")
			}
		})
	}
}

func TestRecordingPositionBackupActualOldZIPWithoutPositions(t *testing.T) {
	f := positionServicePG(t, false)
	old := positionServiceIdentity(t, f)
	info, err := f.service.CreateBackup(BackupTypeManual)
	if err != nil {
		t.Fatal(err)
	}
	raw := positionRewriteZIP(t, f, info.Filename, func(data map[string]json.RawMessage) { delete(data, "recordingPlaybackPositions") })
	if err = f.service.RestoreBackupUpload(bytes.NewReader(raw), "old.zip"); err != nil {
		t.Fatal("old absent optional section rejected", err)
	}
	if len(positionServiceRows(t, f)) != 0 {
		t.Fatal("old backup fabricated retained positions")
	}
	positionServiceFresh(t, f, old)
}

func TestRecordingPositionBackupActualMalformedZIPDoesNotReplace(t *testing.T) {
	for _, kind := range []string{"duplicate", "orphan", "invalid"} {
		t.Run(kind, func(t *testing.T) {
			f := positionServicePG(t, false)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			raw := positionRewriteZIP(t, f, info.Filename, func(data map[string]json.RawMessage) {
				var rows datastore.RecordingPlaybackPositionBackupRows
				if json.Unmarshal(data["recordingPlaybackPositions"], &rows) != nil || len(rows) != 1 {
					t.Fatal("actual saved subset missing")
				}
				switch kind {
				case "duplicate":
					rows = append(rows, rows[0])
				case "orphan":
					rows[0].RecordingID = "missing"
				case "invalid":
					rows[0].PositionMS = 60001
				}
				type rawRows []datastore.RecordingPlaybackPositionBackup
				data["recordingPlaybackPositions"], _ = json.Marshal(rawRows(rows))
			})
			positionServiceCommit(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: 2, WriterGeneration: 1, PositionMS: 30000})
			before := positionServiceRows(t, f)
			old := positionServiceIdentity(t, f)
			settings, _ := os.ReadFile(f.manager.ConfigPath())
			if f.service.RestoreBackupUpload(bytes.NewReader(raw), "malformed.zip") == nil {
				t.Fatal("malformed/orphan data restored")
			}
			after := positionServiceRows(t, f)
			current, _ := os.ReadFile(f.manager.ConfigPath())
			if len(after) != 1 || after[0] != before[0] || positionServiceIdentity(t, f) != old || !bytes.Equal(current, settings) {
				t.Fatal("failed restore changed current data/settings")
			}
		})
	}
}

func positionServiceWait(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("actual original SQL witness absent")
}
func positionHoldCommit(t *testing.T, f *positionServiceFixture) func() {
	t.Helper()
	if _, err := f.observer.Exec(t.Context(), `CREATE FUNCTION position_backup_hold() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM pg_advisory_xact_lock(623991,85);RETURN NEW;END$$;CREATE CONSTRAINT TRIGGER position_backup_hold AFTER INSERT OR UPDATE ON recording_playback_positions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION position_backup_hold()`); err != nil {
		t.Fatal(err)
	}
	control, err := f.observer.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = control.Exec(t.Context(), `SELECT pg_advisory_lock(623991,85)`); err != nil {
		control.Release()
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = control.Exec(ctx, `SELECT pg_advisory_unlock(623991,85)`)
			control.Release()
		})
	}
	t.Cleanup(release)
	return release
}
func positionCommitWaiting(t *testing.T, f *positionServiceFixture) {
	t.Helper()
	positionServiceWait(t, func() bool {
		var count int
		err := f.observer.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname=$1 AND query='commit' AND wait_event_type='Lock'`, f.name).Scan(&count)
		return err == nil && count == 1
	})
}

func TestRecordingPositionBackupActualWriterSerializesRestore(t *testing.T) {
	for _, order := range []string{"writer-first", "restore-first"} {
		t.Run(order, func(t *testing.T) {
			f := positionServicePG(t, false)
			info, err := f.service.CreateBackup(BackupTypeManual)
			if err != nil {
				t.Fatal(err)
			}
			intent := positionServiceIntent(t, f, datastore.RecordingProgressCommand{Action: datastore.RecordingProgressTouch, ExpectedRevision: 2, WriterGeneration: 1, PositionMS: 30000})
			done := make(chan error, 1)
			started, joined := false, false
			t.Cleanup(func() {
				if started && !joined {
					select {
					case <-done:
					case <-time.After(8 * time.Second):
						t.Error("actual writer/restore did not join after owned key release")
					}
				}
			})
			release := positionHoldCommit(t, f)
			started = true
			if order == "writer-first" {
				go func() { _, err := f.core.Commit(intent); done <- err }()
			} else {
				go func() { done <- f.service.RestoreBackup(info.Filename) }()
			}
			positionCommitWaiting(t, f)
			if order == "writer-first" {
				if !errors.Is(f.service.RestoreBackup(info.Filename), settingscommit.ErrBusy) {
					t.Fatal("actual admitted writer did not exclude full restore")
				}
			} else {
				if _, err = f.core.Commit(intent); err == nil {
					t.Fatal("actual full restore did not exclude original writer")
				}
			}
			release()
			select {
			case err = <-done:
				joined = true
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(8 * time.Second):
				t.Fatal("actual committed operation did not join")
			}
			intent.Close()
			rows := positionServiceRows(t, f)
			want := int64(30000)
			if order == "restore-first" {
				want = 25000
			}
			if len(rows) != 1 || rows[0].PositionMS != want {
				t.Fatal("serialization lost known committed data")
			}
		})
	}
}

func TestRecordingPositionBackupActualLostACKNeverRenamesSettings(t *testing.T) {
	f := positionServicePG(t, true)
	before := positionServiceRows(t, f)
	old := positionServiceIdentity(t, f)
	info, err := f.service.CreateBackup(BackupTypeManual)
	if err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	current := append(append([]byte(nil), original...), byte(' '))
	if os.WriteFile(f.manager.ConfigPath(), current, 0600) != nil {
		t.Fatal("own distinguishable settings write")
	}
	f.proxy.armed.Store(true)
	if err = f.service.RestoreBackup(info.Filename); err == nil || !f.proxy.dropped.Load() {
		t.Fatal("actual server COMMIT acknowledgement was not dropped", err)
	}
	afterSettings, err := os.ReadFile(f.manager.ConfigPath())
	if err != nil || !bytes.Equal(afterSettings, current) {
		t.Fatal("failed ACK allowed settings rename")
	}
	after := positionServiceRows(t, f)
	if len(after) != 1 || after[0] != before[0] || positionServiceIdentity(t, f) == old {
		t.Fatal("lost ACK canary did not witness actual committed restore")
	}
	positionServiceFresh(t, f, old)
}
