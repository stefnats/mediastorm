package webdav

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"novastream/internal/nzb/utils"
	"novastream/internal/nzbfilesystem"
)

func contextFilesystem(t *testing.T) (*fileSystem, *metadata.MetadataService) {
	t.Helper()
	service := metadata.NewMetadataService(t.TempDir())
	if err := service.WriteFileMetadata("movie", &metapb.FileMetadata{FileSize: 64, ModifiedAt: 1}); err != nil {
		t.Fatal(err)
	}
	remote := nzbfilesystem.NewMetadataRemoteFile(service, nil, nil, func() *config.AltMountConfig { return &config.AltMountConfig{} })
	return aferoToWebdavFS(nzbfilesystem.NewNzbFilesystem(remote)).(*fileSystem), service
}
func TestWebDAVContextFilesystem(t *testing.T) {
	for _, name := range []string{"actual-context-reaches-reader", "readonly-and-copy-rules-preserved", "seek-and-read-after-retire-denied", "metadata-mutation-context-is-not-background"} {
		t.Run(name, func(t *testing.T) {
			filesystem, service := contextFilesystem(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch name {
			case "actual-context-reaches-reader":
				file, err := filesystem.OpenFile(ctx, "movie", os.O_RDONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				if err = nzbfilesystem.RequireSourceCleanup(file); err != nil {
					t.Fatal(err)
				}
				cancel()
				if n, e := file.Read(make([]byte, 1)); n != 0 || !errors.Is(e, nzbfilesystem.ErrRequestSourceRetired) {
					t.Fatalf("actual request context lost before source Read: %d,%v", n, e)
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
				if err = nzbfilesystem.JoinSourceCleanup(file); err != nil {
					t.Fatal(err)
				}
			case "readonly-and-copy-rules-preserved":
				if _, err := filesystem.OpenFile(ctx, "movie", os.O_WRONLY|os.O_CREATE, 0600); !errors.Is(err, os.ErrPermission) {
					t.Fatalf("write accepted: %v", err)
				}
				copyContext := context.WithValue(ctx, utils.IsCopy, true)
				if _, err := filesystem.OpenFile(copyContext, "movie", os.O_RDONLY, 0); !errors.Is(err, os.ErrPermission) {
					t.Fatalf("COPY accepted: %v", err)
				}
				outside := t.TempDir()
				if e := os.Symlink(outside, filepath.Join(service.RootPath(), "outside-directory")); e != nil {
					t.Fatal(e)
				}
				if _, e := filesystem.Stat(ctx, "outside-directory"); e == nil {
					t.Fatal("request Stat used unconfined legacy directory probe")
				}
				if info, err := filesystem.Stat(ctx, "movie"); err != nil || info.Size() != 64 {
					t.Fatalf("actual readonly metadata changed: %v", err)
				}
			case "seek-and-read-after-retire-denied":
				bounded, stop := context.WithTimeout(ctx, 20*time.Millisecond)
				defer stop()
				file, err := filesystem.OpenFile(bounded, "movie", os.O_RDONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-bounded.Done():
				case <-time.After(time.Second):
					t.Fatal("original deadline did not retire")
				}
				if _, err = file.Seek(0, io.SeekStart); !errors.Is(err, nzbfilesystem.ErrRequestSourceRetired) {
					t.Fatalf("expired seek accepted: %v", err)
				}
				if n, e := file.Read(make([]byte, 1)); n != 0 || !errors.Is(e, nzbfilesystem.ErrRequestSourceRetired) {
					t.Fatalf("expired source Read: %d,%v", n, e)
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			case "metadata-mutation-context-is-not-background":
				original, err := os.ReadFile(filepath.Join(service.RootPath(), "movie.meta"))
				if err != nil {
					t.Fatal(err)
				}
				cancel()
				if err = filesystem.Rename(ctx, "movie", "changed"); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled Rename used Background: %v", err)
				}
				if err = filesystem.RemoveAll(ctx, "movie"); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled Remove used Background: %v", err)
				}
				current, err := os.ReadFile(filepath.Join(service.RootPath(), "movie.meta"))
				if err != nil || string(current) != string(original) {
					t.Fatal("cancelled mutation changed actual metadata")
				}
				if err = filesystem.Rename(context.Background(), "movie", "renamed"); err != nil {
					t.Fatal(err)
				}
				if _, err = filesystem.Stat(context.Background(), "renamed"); err != nil {
					t.Fatal(err)
				}
				moved, e := os.ReadFile(filepath.Join(service.RootPath(), "renamed.meta"))
				if e != nil || string(moved) != string(original) {
					t.Fatal("metadata move changed source provenance bytes")
				}
				oversized, e := os.OpenFile(filepath.Join(service.RootPath(), "oversized.meta"), os.O_CREATE|os.O_WRONLY, 0600)
				if e != nil {
					t.Fatal(e)
				}
				if e = oversized.Truncate(16<<20 + 1); e != nil {
					t.Fatal(e)
				}
				if e = oversized.Close(); e != nil {
					t.Fatal(e)
				}
				if e = filesystem.Rename(context.Background(), "oversized", "should-not-exist"); e == nil {
					t.Fatal("oversized metadata mutation bypassed byte bound")
				}
				if info, e := os.Stat(filepath.Join(service.RootPath(), "oversized.meta")); e != nil || info.Size() != 16<<20+1 {
					t.Fatal("rejected oversized mutation changed original")
				}
				if _, e = os.Stat(filepath.Join(service.RootPath(), "should-not-exist.meta")); !os.IsNotExist(e) {
					t.Fatal("rejected rename published destination")
				}
				if e = os.Mkdir(filepath.Join(service.RootPath(), "directory"), 0700); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(filepath.Join(service.RootPath(), "directory", "retained"), []byte("retained"), 0600); e != nil {
					t.Fatal(e)
				}
				if e = filesystem.Rename(context.Background(), "directory", "moved-directory"); !errors.Is(e, os.ErrPermission) {
					t.Fatalf("request directory rename accepted: %v", e)
				}
				if e = filesystem.RemoveAll(context.Background(), "directory"); !errors.Is(e, os.ErrPermission) {
					t.Fatalf("recursive request delete accepted: %v", e)
				}
				if e = filesystem.RemoveAll(context.Background(), "/"); !errors.Is(e, os.ErrPermission) {
					t.Fatalf("request root delete accepted: %v", e)
				}
				outside := filepath.Join(t.TempDir(), "outside.meta")
				if e = os.WriteFile(outside, original, 0600); e != nil {
					t.Fatal(e)
				}
				if e = os.Symlink(outside, filepath.Join(service.RootPath(), "alias.meta")); e != nil {
					t.Fatal(e)
				}
				if e = filesystem.Rename(context.Background(), "alias", "outside-moved"); e == nil {
					t.Fatal("symlink metadata rename accepted")
				}
				if e = filesystem.RemoveAll(context.Background(), "alias"); e == nil {
					t.Fatal("symlink metadata delete accepted")
				}
				if current, e := os.ReadFile(outside); e != nil || string(current) != string(original) {
					t.Fatal("request mutation changed outside symlink target")
				}
				if current, e := os.ReadFile(filepath.Join(service.RootPath(), "directory", "retained")); e != nil || string(current) != "retained" {
					t.Fatal("denied directory mutation changed a child")
				}
				if err = filesystem.RemoveAll(context.Background(), "renamed"); err != nil {
					t.Fatal(err)
				}
				if service.FileExists("renamed") {
					t.Fatal("joined actual metadata mutation did not finish")
				}
			}
		})
	}
}
