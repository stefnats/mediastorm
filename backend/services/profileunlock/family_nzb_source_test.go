//go:build linux

package profileunlock

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/javi11/nntppool/v2"
	"hash/crc32"
	"io"
	"net"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/integration"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/pool"
	"novastream/internal/settingscommit"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type familyNZBFacadeFixture struct {
	*issuerFacadeFixture
	pins         *PGProfilePins
	manager      *config.Manager
	security     *config.ManagedSecurity
	coordinator  *settingscommit.Coordinator
	objects      *settingscommit.LockObjects
	root         *config.FamilyNZBRoot
	ns           *integration.NzbSystem
	registration *datastore.FamilyNZBSources
	pool         *pool.PreparedManager
	owners       []*FamilyMediaOwner
	data         []byte
	metadataRoot string
}

func familyNZBFacade(t *testing.T) *familyNZBFacadeFixture {
	t.Helper()
	f := &familyNZBFacadeFixture{issuerFacadeFixture: issuerFacadePG(t), data: bytes.Repeat([]byte("synthetic facade original body "), 4096)}
	if _, err := f.users.ClearPin("target"); err != nil {
		t.Fatal(err)
	}
	f.pins = NewPGProfilePins(f.ds)
	ctx, cancel := context.WithCancel(t.Context())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var actors sync.WaitGroup
	var mu sync.Mutex
	connections := map[net.Conn]struct{}{}
	acceptDone := make(chan struct{})
	acceptStarted := false
	t.Cleanup(func() {
		cancel()
		if f.registration != nil {
			f.registration.Close()
		}
		for _, o := range f.owners {
			o.Close()
		}
		if f.ns != nil {
			if e := f.ns.Close(); e != nil {
				t.Error(e)
			}
		}
		if f.pool != nil {
			if e := f.pool.Close(); e != nil {
				t.Error(e)
			}
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
				_, _ = io.WriteString(c, "200 synthetic facade ready\r\n")
				reader := bufio.NewReader(c)
				for {
					line, e := reader.ReadString('\n')
					if e != nil {
						return
					}
					switch {
					case strings.HasPrefix(line, "BODY "):
						_, _ = io.WriteString(c, "222 original body\r\n"+facadeNZBYenc(f.data))
					case strings.HasPrefix(line, "CAPABILITIES"):
						_, _ = io.WriteString(c, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case strings.HasPrefix(line, "DATE"):
						_, _ = io.WriteString(c, "111 20261121120000\r\n")
					case strings.HasPrefix(line, "QUIT"):
						_, _ = io.WriteString(c, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(c, "500 unsupported\r\n")
					}
				}
			}()
		}
	}()
	f.pool, err = pool.NewPreparedManager(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.pool.SetProviders([]nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 2, MaxConnectionTTLInSeconds: 60}}); err != nil {
		t.Fatal(err)
	}
	if err = f.pool.Activate(); err != nil {
		t.Fatal(err)
	}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Cache.Directory = t.TempDir()
	settings.Database.URL = f.ds.Pool().Config().ConnConfig.ConnString()
	settings.Security.EnforceProfilePins = true
	if err = f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	f.objects, err = settingscommit.OpenLockObjects(ctx, f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	f.coordinator, err = settingscommit.NewForPool(ctx, f.ds.Pool(), f.objects)
	if err != nil {
		t.Fatal(err)
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
	f.ns, err = integration.PrepareNzbSystem(ctx, integration.NzbConfig{QueueDatabasePath: filepath.Join(t.TempDir(), "queue.db"), MetadataRootPath: f.metadataRoot, MaxProcessorWorkers: 1}, f.pool, getter)
	if err != nil {
		t.Fatal(err)
	}
	if err = metadata.NewMetadataService(f.metadataRoot).WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: int64(len(f.data)), ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "facade-original-article", SegmentSize: int64(len(f.data)), EndOffset: int64(len(f.data) - 1)}}}); err != nil {
		t.Fatal(err)
	}
	f.registration, err = datastore.NewFamilyNZBSources(ctx, f.ds, f.ns, f.manager, f.security, f.coordinator, f.root)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ns.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	return f
}
func facadeNZBYenc(data []byte) string {
	var b strings.Builder
	n := 0
	for _, v := range data {
		x := v + 42
		escape := x == 0 || x == 9 || x == 10 || x == 13 || x == 32 || x == 46 || x == 61
		width := 1
		if escape {
			width = 2
		}
		if n+width > 128 {
			b.WriteString("\r\n")
			n = 0
		}
		if escape {
			b.WriteByte('=')
			x += 64
		}
		b.WriteByte(x)
		n += width
	}
	return fmt.Sprintf("=ybegin line=128 size=%d name=original.bin\r\n%s\r\n=yend size=%d crc32=%08x\r\n.\r\n", len(data), b.String(), len(data), crc32.ChecksumIEEE(data))
}
func (f *familyNZBFacadeFixture) capture(t *testing.T) *FamilyNZBSource {
	t.Helper()
	r := publicFacadeRequest(t, f.issuerFacadeFixture, f.pins)
	o, err := f.pins.CaptureFamilyMediaOwner(r, "target")
	if err != nil {
		t.Fatal(err)
	}
	f.owners = append(f.owners, o)
	if err = o.Handoff(r); err != nil {
		t.Fatal(err)
	}
	s, err := o.CaptureNZBSource(t.Context(), f.registration, "movie.meta")
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func (f *familyNZBFacadeFixture) read(t *testing.T, s *FamilyNZBSource) *FamilyNZBStream {
	t.Helper()
	r, err := s.Open(t.Context(), 3, 64)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64)
	if _, err = io.ReadFull(r, data); err != nil || !bytes.Equal(data, f.data[3:67]) {
		t.Fatal("actual facade original NNTP bytes", err)
	}
	return r
}
func facadeNZBFDs(t *testing.T, path string) int {
	t.Helper()
	entries, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, v := range entries {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", v.Name()))
		if e == nil && target == path {
			n++
		}
	}
	return n
}

func TestFamilyNZBOriginalFacade(t *testing.T) {
	t.Run("genuine-core-opens-original-only", func(t *testing.T) {
		f := familyNZBFacade(t)
		s := f.capture(t)
		if !s.ReadyForStore(f.ds) {
			t.Fatal("actual facade lost its core store")
		}
		r := f.read(t, s)
		if r.Check(t.Context()) != nil {
			t.Fatal("actual original stream lost current source")
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("zero-copied-closed-and-data-only-denied", func(t *testing.T) {
		f := familyNZBFacade(t)
		s := f.capture(t)
		copySource := *s
		copySource.Close()
		if copySource.Check(t.Context()) == nil || s.Check(t.Context()) != nil {
			t.Fatal("copied source accepted or retired original")
		}
		var imported FamilyNZBSource
		if json.Unmarshal([]byte("{}"), &imported) == nil || imported.Check(t.Context()) == nil {
			t.Fatal("DATA imported source")
		}
		for _, v := range []any{s, *s} {
			b, e := json.Marshal(v)
			if e != nil || string(b) != "null" || fmt.Sprintf("%#v", v) != "FamilyNZBSource(opaque)" {
				t.Fatal("opaque facade leaked fields", e)
			}
		}
		r := f.read(t, s)
		copyStream := *r
		if err := copyStream.Close(); err == nil || r.Check(t.Context()) != nil {
			t.Fatal("copied stream affected original")
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if s.Check(t.Context()) == nil {
			t.Fatal("closed source renewed")
		}
	})
	t.Run("foreign-captured-source-unusable", func(t *testing.T) {
		f := familyNZBFacade(t)
		s := f.capture(t)
		if s.ReadyForStore(&datastore.DataStore{}) {
			t.Fatal("source rebound to foreign store")
		}
		copyOwner := *s.owner
		if source, err := copyOwner.CaptureNZBSource(t.Context(), f.registration, "movie.meta"); err == nil || source != nil {
			t.Fatal("copied original owner captured source")
		}
		if next, err := s.owner.CaptureLocalSource(t.Context(), "arbitrary-item"); err == nil || next != nil {
			t.Fatal("facade source slot changed provider")
		}
		f.read(t, s)
	})
	t.Run("current-pin-policy-and-expiry", func(t *testing.T) {
		f := familyNZBFacade(t)
		if _, err := f.users.SetPin("target", "2580"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('nzb-facade-child','actor','Synthetic Child',true)`); err != nil {
			t.Fatal(err)
		}
		r := publicFacadeRequest(t, f.issuerFacadeFixture, f.pins)
		token, expiry, err := f.pins.Verify(r, "target", "2580", time.Minute, f.users)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set(Header, token)
		o, err := f.pins.CaptureFamilyMediaOwner(r, "target")
		if err != nil {
			t.Fatal(err)
		}
		f.owners = append(f.owners, o)
		if err = o.Handoff(r); err != nil {
			t.Fatal(err)
		}
		s, err := o.CaptureNZBSource(t.Context(), f.registration, "movie.meta")
		if err != nil || s.ExpiresAt().After(expiry) {
			t.Fatal("actual PIN source/expiry", err)
		}
		f.read(t, s)
		if _, err = f.users.ClearPin("target"); err != nil {
			t.Fatal(err)
		}
		if s.Check(t.Context()) == nil {
			t.Fatal("old source adopted revoked PIN or household mode")
		}
	})
	t.Run("actual-physical-completion-before-close", func(t *testing.T) {
		f := familyNZBFacade(t)
		s := f.capture(t)
		r := f.read(t, s)
		name := filepath.Join(f.metadataRoot, "movie.meta")
		if facadeNZBFDs(t, name) == 0 {
			t.Fatal("actual retained original metadata FD absent")
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-r.CleanupDone():
		case <-time.After(10 * time.Second):
			t.Fatal("actual original F2 completion absent")
		}
		if facadeNZBFDs(t, name) != 0 {
			t.Fatal("Close refunded while original metadata FD remained")
		}
	})
}
