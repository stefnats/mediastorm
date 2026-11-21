//go:build linux

package propfind_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sys/unix"
	"novastream/config"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzbfilesystem"
	"novastream/internal/settingsbootstrap"
	dav "novastream/internal/webdav"
)

type metadataBoundsFixture struct {
	handler *dav.ManagedHandler
	source  *metadata.MetadataService
}

func metadataBoundsPG(t *testing.T) *metadataBoundsFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		raw = os.Getenv("PARENT_MUTATION_TEST_DATABASE_URL")
	}
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("synthetic Unix-socket PostgreSQL required")
	}
	admin, e := pgxpool.New(t.Context(), raw)
	if e != nil {
		t.Fatal(e)
	}
	name := fmt.Sprintf("synthetic_webdav_bounds_%d", time.Now().UnixNano())
	if _, e = admin.Exec(t.Context(), "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, e := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH(FORCE)"); e != nil {
			t.Error(e)
		}
		admin.Close()
	})
	u.Path = "/" + name
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = u.String()
	settings.Security.EnforceProfilePins = false
	settings.WebDAV = config.WebDAVSettings{Enabled: true, Prefix: "/webdav", Username: "synthetic-basic", Password: "synthetic-password"}
	if e = manager.Save(settings); e != nil {
		t.Fatal(e)
	}
	startup, e := settingsbootstrap.Begin(t.Context(), manager, "")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := startup.Close(); e != nil {
			t.Error(e)
		}
	})
	store, e := startup.Migrate(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = startup.Finish(); e != nil {
		t.Fatal(e)
	}
	if _, e = store.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('original','Synthetic original','synthetic-unused');INSERT INTO users(id,account_id,name)VALUES('profile','original','Synthetic profile')`); e != nil {
		t.Fatal(e)
	}
	source := metadata.NewMetadataService(t.TempDir())
	if e = source.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
		t.Fatal(e)
	}
	fs := nzbfilesystem.NewNzbFilesystem(nzbfilesystem.NewMetadataRemoteFile(source, nil, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} })).(*nzbfilesystem.NzbFilesystem)
	issuer, e := startup.NewDeploymentWebDAVIssuer(store)
	if e != nil {
		t.Fatal(e)
	}
	handler, e := dav.NewManagedHandler("/webdav", fs, issuer)
	if e != nil {
		issuer.Close()
		t.Fatal(e)
	}
	t.Cleanup(handler.Close)
	return &metadataBoundsFixture{handler, source}
}
func metadataBoundsRequest(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, "http://synthetic.test"+target, body)
	r.RequestURI = r.URL.RequestURI()
	r.RemoteAddr = "127.0.0.1:24300"
	r.SetBasicAuth("synthetic-basic", "synthetic-password")
	return r
}
func metadataBoundsCall(f *metadataBoundsFixture, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
}
func TestManagedWebDAVMetadataBudget(t *testing.T) {
	for _, name := range []string{"propfind-depth-overflow", "bounded-listing-and-xml-output", "early-method-body-bound", "mutation-known-result-no-false-rollback"} {
		t.Run(name, func(t *testing.T) {
			f := metadataBoundsPG(t)
			switch name {
			case "propfind-depth-overflow":
				deep := strings.Repeat("nested/", 33) + "movie"
				if e := f.source.WriteFileMetadata(deep, &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
					t.Fatal(e)
				}
				r := metadataBoundsRequest("PROPFIND", "/webdav/", nil)
				r.Header.Set("Depth", "infinity")
				w := metadataBoundsCall(f, r)
				if w.Code == 207 || bytes.Contains(w.Body.Bytes(), []byte("<?xml")) {
					t.Fatal("overdepth walk published partial XML", w.Code)
				}
			case "bounded-listing-and-xml-output":
				for i := 0; i < 40; i++ {
					if e := f.source.WriteFileMetadata(fmt.Sprintf("movie-%04d", i), &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
						t.Fatal(e)
					}
				}
				r := metadataBoundsRequest("PROPFIND", "/webdav/", nil)
				r.Header.Set("Depth", "1")
				w := metadataBoundsCall(f, r)
				if w.Code != 207 || !bytes.Contains(w.Body.Bytes(), []byte("movie-0039")) {
					t.Fatal("ordinary bounded listing failed", w.Code)
				}
				var props strings.Builder
				props.WriteString(`<D:propfind xmlns:D="DAV:" xmlns:X="urn:synthetic"><D:prop>`)
				for i := 0; i < 400; i++ {
					fmt.Fprintf(&props, "<X:p%04d%s/>", i, strings.Repeat("x", 1000))
				}
				props.WriteString("</D:prop></D:propfind>")
				if props.Len() > 1<<20 {
					t.Fatal("XML witness itself exceeded input bound")
				}
				r = metadataBoundsRequest("PROPFIND", "/webdav/", strings.NewReader(props.String()))
				r.Header.Set("Depth", "1")
				w = metadataBoundsCall(f, r)
				if w.Code == 207 || w.Body.Len() > 8<<20 || bytes.Contains(w.Body.Bytes(), []byte("<?xml")) {
					t.Fatal("overbound XML escaped staging", w.Code, w.Body.Len())
				}
				for i := 0; i < 4097; i++ {
					if e := f.source.WriteFileMetadata(fmt.Sprintf("overflow-%04d", i), &metapb.FileMetadata{FileSize: 0, ModifiedAt: 1}); e != nil {
						t.Fatal(e)
					}
				}
				r = metadataBoundsRequest("PROPFIND", "/webdav/", nil)
				r.Header.Set("Depth", "1")
				w = metadataBoundsCall(f, r)
				if w.Code == 207 || bytes.Contains(w.Body.Bytes(), []byte("<?xml")) {
					t.Fatal("partial overbound directory published", w.Code)
				}
			case "early-method-body-bound":
				r := metadataBoundsRequest("PROPFIND", "/webdav/", strings.NewReader(strings.Repeat("x", 1<<20+1)))
				w := metadataBoundsCall(f, r)
				if w.Code != 413 {
					t.Fatal("known oversized body reached metadata", w.Code)
				}
				r = metadataBoundsRequest("PROPFIND", "/webdav/", io.NopCloser(strings.NewReader(strings.Repeat(" ", 1<<20+1))))
				r.ContentLength = -1
				w = metadataBoundsCall(f, r)
				if w.Code != 413 {
					t.Fatal("streamed oversized body was not bounded", w.Code)
				}
				for _, method := range []string{"PUT", "COPY", "MKCOL", "LOCK", "UNLOCK"} {
					w = metadataBoundsCall(f, metadataBoundsRequest(method, "/webdav/movie", nil))
					if w.Code != 405 {
						t.Fatal("unsupported managed mutation/locking admitted", method, w.Code)
					}
				}
				w = metadataBoundsCall(f, metadataBoundsRequest("OPTIONS", "/webdav/", nil))
				if w.Code != 200 || strings.Contains(w.Header().Get("Allow"), "LOCK") || strings.Contains(w.Header().Get("Allow"), "COPY") {
					t.Fatal("managed OPTIONS advertised unavailable features")
				}
			case "mutation-known-result-no-false-rollback":
				original, e := os.ReadFile(filepath.Join(f.source.RootPath(), "movie.meta"))
				if e != nil {
					t.Fatal(e)
				}
				fd, e := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = unix.InotifyAddWatch(fd, f.source.RootPath(), unix.IN_MOVED_TO); e != nil {
					_ = unix.Close(fd)
					t.Fatal(e)
				}
				ctx, cancel := context.WithCancel(t.Context())
				finished := make(chan struct{})
				var closeOnce sync.Once
				closeWatch := func() { closeOnce.Do(func() { _ = unix.Close(fd) }) }
				started := false
				var movedSeen atomic.Bool
				t.Cleanup(func() {
					cancel()
					closeWatch()
					if started {
						select {
						case <-finished:
						case <-time.After(5 * time.Second):
							t.Error("actual mutation observer not joined")
						}
					}
				})
				// A real successful filesystem rename is the observer; it is DATA only.
				started = true
				go func() {
					defer close(finished)
					raw := make([]byte, 4096)
					for {
						n, e := unix.Read(fd, raw)
						if e == nil && n > 0 {
							for offset := 0; offset+unix.SizeofInotifyEvent <= n; {
								length := int(binary.NativeEndian.Uint32(raw[offset+12 : offset+16]))
								end := offset + unix.SizeofInotifyEvent + length
								if end > n {
									return
								}
								mask := binary.NativeEndian.Uint32(raw[offset+4 : offset+8])
								name := strings.TrimRight(string(raw[offset+unix.SizeofInotifyEvent:end]), "\x00")
								if mask&unix.IN_MOVED_TO != 0 && name == "renamed.meta" {
									movedSeen.Store(true)
									cancel()
									return
								}
								offset = end
							}
						}
						if e != nil && e != unix.EAGAIN {
							return
						}
						select {
						case <-ctx.Done():
							return
						case <-time.After(time.Millisecond):
						}
					}
				}()
				r := metadataBoundsRequest("MOVE", "/webdav/movie", nil).WithContext(ctx)
				r.Header.Set("Destination", "http://synthetic.test/webdav/renamed")
				w := metadataBoundsCall(f, r)
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("actual rename event not observed")
				}
				closeWatch()
				if !movedSeen.Load() || ctx.Err() == nil {
					t.Fatal("genuine successful move did not trigger original cancellation")
				}
				moved, e := os.ReadFile(filepath.Join(f.source.RootPath(), "renamed.meta"))
				if e != nil || !bytes.Equal(original, moved) {
					t.Fatal("late cancellation pretended to roll back actual metadata move", e, w.Code)
				}
				if _, e = os.Stat(filepath.Join(f.source.RootPath(), "movie.meta")); !os.IsNotExist(e) {
					t.Fatal("source filename remained after known rename")
				}
				w = metadataBoundsCall(f, metadataBoundsRequest("DELETE", "/webdav/renamed", nil))
				if w.Code != 204 {
					t.Fatal("bounded regular-file DELETE failed", w.Code)
				}
				if e = os.Mkdir(filepath.Join(f.source.RootPath(), "directory"), 0700); e != nil {
					t.Fatal(e)
				}
				w = metadataBoundsCall(f, metadataBoundsRequest("DELETE", "/webdav/directory", nil))
				if w.Code >= 200 && w.Code < 300 {
					t.Fatal("managed directory deletion revived unbounded recursive mutation")
				}
				if _, e = os.Stat(filepath.Join(f.source.RootPath(), "directory")); e != nil {
					t.Fatal("denied directory mutation changed source")
				}
			}
		})
	}
}
