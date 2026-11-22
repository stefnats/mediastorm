//go:build linux

package datastore

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/javi11/nntppool/v2"
	"golang.org/x/crypto/bcrypt"
	"hash/crc32"
	"io"
	"log/slog"
	"net"
	"net/url"
	"novastream/config"
	"novastream/internal/integration"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/settingscommit"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type familyNZBActualPool struct {
	nntppool.UsenetConnectionPool
	returned, release chan struct{}
	once              sync.Once
	calls             atomic.Int32
}

func (p *familyNZBActualPool) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	n, e := p.UsenetConnectionPool.Body(ctx, id, w, groups)
	p.calls.Add(1)
	p.once.Do(func() { close(p.returned) })
	<-p.release
	return n, e
}

type familyNZBActualManager struct{ actual *familyNZBActualPool }

func (m *familyNZBActualManager) GetPool() (nntppool.UsenetConnectionPool, error) {
	return m.actual, nil
}
func (*familyNZBActualManager) SetProviders([]nntppool.UsenetProviderConfig) error {
	return errors.New("fixture provider mutation forbidden")
}
func (*familyNZBActualManager) ClearPool() error {
	return errors.New("fixture pool mutation forbidden")
}
func (*familyNZBActualManager) HasPool() bool             { return true }
func (*familyNZBActualManager) AvailableConnections() int { return 4 }

type familyNZBFixture struct {
	*ratingPGFixture
	manager      *config.Manager
	settings     config.Settings
	security     *config.ManagedSecurity
	coordinator  *settingscommit.Coordinator
	objects      *settingscommit.LockObjects
	root         *config.FamilyNZBRoot
	ns           *integration.NzbSystem
	registration *FamilyNZBSources
	pool         *familyNZBActualPool
	metadataRoot string
	data         []byte
	owners       []*FamilyMediaOwner
	freeOnce     sync.Once
}

func (f *familyNZBFixture) free() {
	if f.pool != nil {
		f.freeOnce.Do(func() { close(f.pool.release) })
	}
}
func familyNZBWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(12 * time.Second):
		t.Fatal("actual original NZB operation did not join")
	}
}
func familyNZBPending(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("original physical work ended while BODY was held")
	case <-time.After(20 * time.Millisecond):
	}
}
func familyNZBWire(data []byte) string {
	var b strings.Builder
	column := 0
	for _, v := range data {
		x := v + 42
		escape := x == 0 || x == 9 || x == 10 || x == 13 || x == 32 || x == 46 || x == 61
		width := 1
		if escape {
			width = 2
		}
		if column+width > 128 {
			b.WriteString("\r\n")
			column = 0
		}
		if escape {
			b.WriteByte('=')
			x += 64
		}
		b.WriteByte(x)
		column += width
	}
	return fmt.Sprintf("=ybegin line=128 size=%d name=original.bin\r\n%s\r\n=yend size=%d crc32=%08x\r\n.\r\n", len(data), b.String(), len(data), crc32.ChecksumIEEE(data))
}
func familyNZBPG(t *testing.T) *familyNZBFixture {
	t.Helper()
	f := &familyNZBFixture{ratingPGFixture: familyMediaPG(t), data: make([]byte, 131072)}
	for i := range f.data {
		f.data[i] = byte((i*19 + i/251) % 256)
	}
	// The historic parent fixture uses an AfterConnect SET ROLE hook. Original
	// write guards deliberately reject foreign lifecycle hooks. Encode that
	// SAME restricted role/schema in the real startup URL before any actor is
	// captured, as the existing recording progress fixture does.
	originalSchema := f.cfg.ConnConfig.RuntimeParams["application_name"]
	if !strings.HasPrefix(originalSchema, "synthetic_pm_") || strings.IndexFunc(originalSchema, func(r rune) bool { return !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') }) >= 0 {
		t.Fatal("expected own generated original role/schema")
	}
	target, err := url.Parse(f.cfg.ConnConfig.ConnString())
	if err != nil {
		t.Fatal("parse own original fixture URL")
	}
	query := target.Query()
	query.Set("search_path", pgx.Identifier{originalSchema}.Sanitize()+",pg_catalog")
	query.Set("application_name", originalSchema)
	query.Set("options", "-c role="+originalSchema)
	target.RawQuery = query.Encode()
	runtimeURL := target.String()
	configuration, err := pgxpool.ParseConfig(runtimeURL)
	if err != nil {
		t.Fatal("parse genuine original role/schema URL")
	}
	configuration.MaxConns = f.cfg.MaxConns
	configuration.MinConns = f.cfg.MinConns
	configureQueryLifecycle(configuration.ConnConfig)
	actualPool, err := pgxpool.NewWithConfig(t.Context(), configuration)
	if err != nil {
		t.Fatal("open actual same-role hook-free fixture pool")
	}
	// Registered before source cleanup: all actual source/coordinator actors
	// end first; the historic fixture closes its own old pool/schema last.
	t.Cleanup(actualPool.Close)
	f.ds = &DataStore{pool: actualPool}
	f.cfg = configuration
	if configuration.BeforeConnect != nil || configuration.AfterConnect != nil || configuration.BeforeAcquire != nil || configuration.PrepareConn != nil || configuration.AfterRelease != nil || configuration.BeforeClose != nil || configuration.ConnString() != runtimeURL {
		t.Fatal("original fixture has a foreign lifecycle hook or URL")
	}
	var actualRole, actualSchema, actualApplication string
	if err = actualPool.QueryRow(t.Context(), `SELECT current_user,current_schema(),current_setting('application_name')`).Scan(&actualRole, &actualSchema, &actualApplication); err != nil || actualRole != originalSchema || actualSchema != originalSchema || actualApplication != originalSchema {
		t.Fatal("actual original restricted role/schema/application changed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var mu sync.Mutex
	connections := map[net.Conn]struct{}{}
	var actors sync.WaitGroup
	acceptDone := make(chan struct{})
	acceptStarted := false
	// Signal/unblock every actual actor before any failure cleanup joins.
	t.Cleanup(func() {
		f.free()
		cancel()
		for _, o := range f.owners {
			o.Close()
		}
		if f.registration != nil {
			f.registration.Close()
		}
		if f.ns != nil {
			if e := f.ns.Close(); e != nil {
				t.Error(e)
			}
		}
		if f.pool != nil {
			f.pool.Quit()
		}
		_ = listener.Close()
		if acceptStarted {
			<-acceptDone
		}
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		actors.Wait()
		if f.root != nil {
			_ = f.root.Close()
		}
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
	acceptStarted = true
	go func() {
		defer close(acceptDone)
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			connections[c] = struct{}{}
			mu.Unlock()
			actors.Add(1)
			go func() {
				defer actors.Done()
				defer c.Close()
				defer func() { mu.Lock(); delete(connections, c); mu.Unlock() }()
				_, _ = io.WriteString(c, "200 synthetic original ready\r\n")
				r := bufio.NewReader(c)
				for {
					line, e := r.ReadString('\n')
					if e != nil {
						return
					}
					line = strings.TrimSpace(line)
					switch {
					case strings.HasPrefix(line, "BODY "):
						_, _ = io.WriteString(c, "222 original body\r\n"+familyNZBWire(f.data))
					case line == "CAPABILITIES":
						_, _ = io.WriteString(c, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case line == "DATE":
						_, _ = io.WriteString(c, "111 20261121120000\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(c, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(c, "500 unsupported\r\n")
					}
				}
			}()
		}
	}()
	actual, err := nntppool.NewConnectionPoolWithContext(ctx, nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 4, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: time.Second, HealthCheckInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	f.pool = &familyNZBActualPool{UsenetConnectionPool: actual, returned: make(chan struct{}), release: make(chan struct{})}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	f.settings = config.DefaultSettings()
	f.settings.Cache.Directory = t.TempDir()
	f.settings.Database.URL = f.cfg.ConnConfig.ConnString()
	f.settings.Security.EnforceProfilePins = true
	if err = f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	f.objects, err = settingscommit.OpenLockObjects(ctx, f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(ctx, f.ds.pool, f.objects)
	if err != nil {
		t.Fatal(err)
	}
	// Readiness is the actual coordinator's exact pool pointer and original
	// source domain, not a caller boolean or a separate binding registry.
	if !f.ds.SettingsCommitReady(f.coordinator) || !f.coordinator.MatchesSourceURL(runtimeURL) || !f.coordinator.MatchesConfigPath(f.manager.ConfigPath()) {
		t.Fatal("actual coordinator differs from original store/URL/native path")
	}
	guard, err := f.coordinator.TryWrite(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	if e := guard.Close(); err == nil {
		err = e
	}
	if err != nil {
		t.Fatal(err)
	}
	f.root, err = config.CaptureFamilyNZBWorkingDirectory(ctx, f.manager)
	if err != nil {
		t.Fatal(err)
	}
	f.metadataRoot, err = f.security.PrepareFamilyNZBRoot(ctx, f.root)
	if err != nil {
		t.Fatal(err)
	}
	getter := func() *config.AltMountConfig {
		return &config.AltMountConfig{Import: config.ImportConfig{QueueProcessingIntervalSeconds: 1}, Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
	}
	f.ns, err = integration.PrepareNzbSystem(ctx, integration.NzbConfig{QueueDatabasePath: filepath.Join(t.TempDir(), "queue.db"), MetadataRootPath: f.metadataRoot, MaxProcessorWorkers: 1}, &familyNZBActualManager{f.pool}, getter)
	if err != nil {
		t.Fatal(err)
	}
	f.write(t, "original-nzb-article")
	f.registration, err = NewFamilyNZBSources(ctx, f.ds, f.ns, f.manager, f.security, f.coordinator, f.root)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ns.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *familyNZBFixture) write(t *testing.T, id string) {
	t.Helper()
	m := metadata.NewMetadataService(f.metadataRoot)
	if err := m.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: int64(len(f.data)), ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: id, SegmentSize: int64(len(f.data)), EndOffset: int64(len(f.data) - 1)}}}); err != nil {
		t.Fatal(err)
	}
}
func (f *familyNZBFixture) owner(t *testing.T, target string) *FamilyMediaOwner {
	t.Helper()
	r, release := familyMediaRequest(t, f.ratingPGFixture, t.Context())
	defer release()
	o, err := f.ds.CaptureFamilyMediaOwner(r, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.owners = append(f.owners, o)
	if err = o.Handoff(r); err != nil {
		t.Fatal(err)
	}
	return o
}
func (f *familyNZBFixture) source(t *testing.T, o *FamilyMediaOwner) *FamilyNZBSource {
	t.Helper()
	s, err := o.CaptureNZBSource(t.Context(), f.registration, "movie.meta")
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func (f *familyNZBFixture) read(t *testing.T, s *FamilyNZBSource) []byte {
	t.Helper()
	f.free()
	r, err := s.Open(t.Context(), 13, 64)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 64)
	if _, err = io.ReadFull(r, b); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, f.data[13:77]) {
		t.Fatal("actual original NNTP range bytes differ")
	}
	return b
}
func familyNZBFDs(t *testing.T, name string) int {
	t.Helper()
	entries, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, entry := range entries {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if e == nil && (target == name || target == name+" (deleted)") {
			n++
		}
	}
	return n
}

func TestFamilyNZBOriginalOwner(t *testing.T) {
	t.Run("actual-inactive-original-full-frame", func(t *testing.T) {
		f := familyNZBPG(t)
		o := f.owner(t, "")
		s := f.source(t, o)
		f.read(t, s)
		// A same-root native save retires THIS source, not future original owners.
		f.settings.Security.EnforceProfilePins = false
		if err := f.manager.Save(f.settings); err != nil {
			t.Fatal(err)
		}
		if s.Check(t.Context()) == nil {
			t.Fatal("old native observation refreshed")
		}
		fresh := f.source(t, f.owner(t, ""))
		if fresh.values.EnforceProfilePins || fresh.Check(t.Context()) != nil {
			t.Fatal("fresh original owner could not capture current same-root native policy")
		}
		f.read(t, fresh)
		if next, err := o.CaptureNZBSource(t.Context(), f.registration, "movie.meta"); err == nil || next != nil {
			t.Fatal("retired owner source slot reused")
		}
	})
	t.Run("actual-unlocked-adult-original", func(t *testing.T) {
		f := familyNZBPG(t)
		f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('nzb-child','actor','Synthetic Child',true)`)
		r, release, token, expiry := familyMediaUnlock(t, f.ratingPGFixture, time.Minute)
		defer release()
		o, err := f.ds.CaptureFamilyMediaOwner(r, "target", []string{token})
		if err != nil {
			t.Fatal(err)
		}
		f.owners = append(f.owners, o)
		if o.Mode() != FamilyMediaActiveAdult || o.Handoff(r) != nil {
			t.Fatal("genuine unlocked adult handoff absent")
		}
		s := f.source(t, o)
		if s.ExpiresAt().After(expiry) {
			t.Fatal("source extended original PIN expiry")
		}
		f.read(t, s)
		f.exec(t, `UPDATE users SET pin_hash='',pin_length=0 WHERE id='target'`)
		if s.Check(t.Context()) == nil {
			t.Fatal("original unlocked source adopted revoked PIN")
		}
	})
	t.Run("kids-scoped-and-foreign-profile-denied", func(t *testing.T) {
		f := familyNZBPG(t)
		r, release := familyMediaRequest(t, f.ratingPGFixture, t.Context())
		defer release()
		if o, err := f.ds.CaptureFamilyMediaOwner(r, "unrelated", nil); err == nil || o != nil {
			if o != nil {
				o.Close()
			}
			t.Fatal("foreign ordinary profile accepted")
		}
		// A real stored PIN under an inactive household still requires an actual
		// unlock when native enforcement is enabled. No PIN outcome is fabricated.
		hash, err := bcrypt.GenerateFromPassword([]byte("1357"), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		f.exec(t, `UPDATE users SET pin_hash=$1,pin_length=4 WHERE id='target'`, string(hash))
		locked := f.owner(t, "target")
		if locked.Mode() != FamilyMediaInactive {
			t.Fatal("actual no-child household was not inactive")
		}
		if source, err := locked.CaptureNZBSource(t.Context(), f.registration, "movie.meta"); !errors.Is(err, ErrFamilyMediaPINRequired) || source != nil {
			t.Fatal("enforced actual stored PIN bypassed source admission", err)
		}
		locked.mu.Lock()
		claimed, ok := locked.source.(*FamilyNZBSource)
		locked.mu.Unlock()
		if !ok || claimed == nil || claimed.lower != nil || f.pool.calls.Load() != 0 {
			t.Fatal("denied PIN did not retain its permanent claim before metadata/NNTP")
		}
		if next, err := locked.CaptureNZBSource(t.Context(), f.registration, "movie.meta"); err == nil || next != nil {
			t.Fatal("denied PIN source claim was reused")
		}
		f.exec(t, `UPDATE users SET is_kids_profile=true WHERE id='target'`)
		if o, err := f.ds.CaptureFamilyMediaOwner(r, "target", nil); err == nil || o != nil {
			if o != nil {
				o.Close()
			}
			t.Fatal("child became full NZB owner")
		}
		f.exec(t, `UPDATE sessions SET scope='stream',scope_resource='movie' WHERE token=$1`, f.session.Token)
		fresh, stop := familyMediaRequest(t, f.ratingPGFixture, t.Context())
		defer stop()
		if o, err := f.ds.CaptureFamilyMediaOwner(fresh, "", nil); err == nil || o != nil {
			if o != nil {
				o.Close()
			}
			t.Fatal("scoped source became full NZB owner")
		}
		if f.pool.calls.Load() != 0 {
			t.Fatal("denied actor opened NNTP")
		}
	})
	t.Run("original-account-session-profile-expiry", func(t *testing.T) {
		f := familyNZBPG(t)
		s := f.source(t, f.owner(t, "target"))
		r, err := s.Open(t.Context(), 0, 32)
		if err != nil {
			t.Fatal(err)
		}
		f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`)
		b := bytes.Repeat([]byte{0xa5}, 64)
		if n, err := r.Read(b); n != 0 || err == nil || !bytes.Equal(b, make([]byte, 64)) {
			t.Fatal("expired original actor returned bytes")
		}
		s.Close()
		familyNZBWait(t, r.CleanupDone())
		f.exec(t, `UPDATE accounts SET expires_at=NULL WHERE id='actor'`)
		sessionSource := f.source(t, f.owner(t, "target"))
		f.exec(t, `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, f.session.Token)
		if sessionSource.Check(t.Context()) == nil {
			t.Fatal("original session expiry did not retire captured source")
		}
		sessionSource.Close()
		f.exec(t, `UPDATE sessions SET expires_at=$2 WHERE token=$1`, f.session.Token, f.session.ExpiresAt)
		profileSource := f.source(t, f.owner(t, "target"))
		f.exec(t, `DELETE FROM users WHERE id='target'`)
		if profileSource.Check(t.Context()) == nil {
			t.Fatal("deleted original profile retained captured source")
		}
		profileSource.Close()
		if f.pool.calls.Load() != 0 {
			t.Fatal("revoked original actors reached NNTP")
		}
	})
	t.Run("original-pin-and-household-revocation", func(t *testing.T) {
		f := familyNZBPG(t)
		s := f.source(t, f.owner(t, "target"))
		f.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('new-nzb-child','actor','Synthetic Child',true)`)
		if s.Check(t.Context()) == nil {
			t.Fatal("captured inactive original survived household activation")
		}
		if f.pool.calls.Load() != 0 {
			t.Fatal("revoked metadata-only source performed NNTP work")
		}
	})
	t.Run("foreign-store-and-namespace-denied", func(t *testing.T) {
		f := familyNZBPG(t)
		pool, err := pgxpool.NewWithConfig(t.Context(), f.cfg.Copy())
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		if value, err := NewFamilyNZBSources(t.Context(), &DataStore{pool: pool}, f.ns, f.manager, f.security, f.coordinator, f.root); err == nil || value != nil {
			t.Fatal("foreign pool substituted original store")
		}
		copyNS := *f.ns
		if value, err := NewFamilyNZBSources(t.Context(), f.ds, &copyNS, f.manager, f.security, f.coordinator, f.root); err == nil || value != nil {
			t.Fatal("foreign/copied NS replaced permanent binding")
		}
		if f.pool.calls.Load() != 0 {
			t.Fatal("foreign wiring reached provider")
		}
	})
	t.Run("failed-capture-consumes-cross-provider-slot", func(t *testing.T) {
		f := familyNZBPG(t)
		o := f.owner(t, "")
		if s, err := o.CaptureNZBSource(t.Context(), f.registration, "queue/1/movie.meta"); err == nil || s != nil {
			t.Fatal("queue alias accepted")
		}
		if s, err := o.CaptureLocalSource(t.Context(), "unknown-item"); err == nil || s != nil {
			t.Fatal("failed NZB source fell back to a different provider")
		}
		if f.pool.calls.Load() != 0 {
			t.Fatal("failed claim reached NNTP")
		}
	})
	t.Run("current-real-metadata-retarget-denied", func(t *testing.T) {
		f := familyNZBPG(t)
		s := f.source(t, f.owner(t, ""))
		r, err := s.Open(t.Context(), 0, 32)
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Join(f.metadataRoot, "movie.meta")
		if err = os.Rename(name, name+".original"); err != nil {
			t.Fatal(err)
		}
		f.write(t, "foreign-replacement")
		b := bytes.Repeat([]byte{0xa5}, 64)
		if n, err := r.Read(b); n != 0 || err == nil || !bytes.Equal(b, make([]byte, 64)) {
			t.Fatal("changed metadata leaked caller bytes")
		}
		s.Close()
		familyNZBWait(t, r.CleanupDone())
		if f.pool.calls.Load() != 0 {
			t.Fatal("retarget opened foreign article")
		}
	})
	t.Run("one-original-opener-no-renewal", func(t *testing.T) {
		f := familyNZBPG(t)
		s := f.source(t, f.owner(t, ""))
		expiry := s.ExpiresAt()
		r, err := s.Open(t.Context(), 13, 64)
		if err != nil {
			t.Fatal(err)
		}
		if next, err := s.Open(t.Context(), 13, 64); err == nil || next != nil {
			t.Fatal("source opened twice")
		}
		if s.Context().Err() != nil || !s.ExpiresAt().Equal(expiry) {
			t.Fatal("local duplicate renewed or retired original")
		}
		f.free()
		b := make([]byte, 64)
		if _, err = io.ReadFull(r, b); err != nil || !bytes.Equal(b, f.data[13:77]) {
			t.Fatal("first original source lost after duplicate denial", err)
		}
	})
	t.Run("actual-open-and-f2-retirement-join", func(t *testing.T) { familyNZBHeld(t, false, false) })
	t.Run("store-close-joins-original-held-source", func(t *testing.T) { familyNZBHeld(t, true, false) })
	t.Run("all-original-sources-signalled-before-refund", func(t *testing.T) { familyNZBHeld(t, false, true) })
}
func familyNZBHeld(t *testing.T, closeStore, two bool) {
	t.Helper()
	f := familyNZBPG(t)
	count := 1
	if two {
		count = 2
	}
	var sources []*FamilyNZBSource
	var readers []*FamilyNZBStream
	var wg sync.WaitGroup
	var closedStarted bool
	closed := make(chan struct{})
	t.Cleanup(func() {
		f.free()
		for _, s := range sources {
			s.cancel()
		}
		wg.Wait()
		if closedStarted {
			familyNZBWait(t, closed)
		}
	})
	for i := 0; i < count; i++ {
		s := f.source(t, f.owner(t, ""))
		r, err := s.Open(t.Context(), 0, 32)
		if err != nil {
			t.Fatal(err)
		}
		sources = append(sources, s)
		readers = append(readers, r)
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = r.Read(make([]byte, 32)) }()
	}
	familyNZBWait(t, f.pool.returned)
	if two {
		until := time.Now().Add(5 * time.Second)
		for f.pool.calls.Load() < 2 {
			if time.Now().After(until) {
				t.Fatal("both actual BODY delegates not entered")
			}
			time.Sleep(time.Millisecond)
		}
	}
	if familyNZBFDs(t, filepath.Join(f.metadataRoot, "movie.meta")) == 0 {
		t.Fatal("actual original metadata descriptor absent")
	}
	closedStarted = true
	go func() {
		defer close(closed)
		if closeStore {
			f.ds.Close()
		} else if two {
			f.registration.Close()
		} else {
			sources[0].Close()
		}
	}()
	until := time.Now().Add(5 * time.Second)
	for {
		all := true
		for _, s := range sources {
			all = all && s.Context().Err() != nil
		}
		if all {
			break
		}
		if time.Now().After(until) {
			t.Fatal("all source contexts were not signalled")
		}
		time.Sleep(time.Millisecond)
	}
	familyNZBPending(t, closed)
	for _, r := range readers {
		familyNZBPending(t, r.CleanupDone())
	}
	f.free()
	wg.Wait()
	familyNZBWait(t, closed)
	for _, r := range readers {
		familyNZBWait(t, r.CleanupDone())
	}
	if familyNZBFDs(t, filepath.Join(f.metadataRoot, "movie.meta")) != 0 {
		t.Fatal("actual original metadata FD survived physical retirement")
	}
}
