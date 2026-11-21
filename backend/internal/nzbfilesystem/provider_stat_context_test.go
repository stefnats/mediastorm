//go:build linux

package nzbfilesystem

import (
	"context"
	"novastream/config"
	"novastream/internal/nzb/metadata"
	metapb "novastream/internal/nzb/metadata/proto"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func providerMetadataDescriptors(t *testing.T, root string) int {
	t.Helper()
	entries, e := os.ReadDir("/proc/self/fd")
	if e != nil {
		t.Fatal(e)
	}
	n := 0
	for _, entry := range entries {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if e == nil && (target == root || strings.HasPrefix(target, root+"/")) {
			n++
		}
	}
	return n
}
func TestNzbFilesystemOwnedStatContext(t *testing.T) {
	for _, kind := range []string{"stat-pre-cancelled", "stat-during-original-cancel", "stat-oversized-metadata", "stat-symlink-denied", "stat-no-reader-actor-left", "openfile-error-source-joined"} {
		t.Run(kind, func(t *testing.T) {
			service := metadata.NewMetadataService(t.TempDir())
			meta := &metapb.FileMetadata{FileSize: 128, ModifiedAt: 1}
			if e := service.WriteFileMetadata("synthetic.bin", meta); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var cancelOnLoad atomic.Bool
			remote := NewMetadataRemoteFile(service, nil, nil, func() *config.AltMountConfig {
				if cancelOnLoad.Load() {
					cancel()
				}
				return &config.AltMountConfig{Streaming: config.StreamingConfig{MaxDownloadWorkers: 1, MaxCacheSizeMB: 1}}
			})
			source := NewNzbFilesystem(remote).(*NzbFilesystem)
			switch kind {
			case "stat-pre-cancelled":
				cancel()
			case "stat-during-original-cancel", "openfile-error-source-joined":
				cancelOnLoad.Store(true)
			case "stat-oversized-metadata":
				f, e := os.OpenFile(service.GetMetadataFilePath("synthetic.bin"), os.O_WRONLY, 0600)
				if e != nil {
					t.Fatal(e)
				}
				if e = f.Truncate((16 << 20) + 1); e != nil {
					f.Close()
					t.Fatal(e)
				}
				if e = f.Close(); e != nil {
					t.Fatal(e)
				}
			case "stat-symlink-denied":
				original := service.GetMetadataFilePath("synthetic.bin")
				outside := filepath.Join(t.TempDir(), "outside.meta")
				if e := os.Rename(original, outside); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(outside, original); e != nil {
					t.Fatal(e)
				}
			}
			before := providerMetadataDescriptors(t, service.RootPath())
			if kind == "openfile-error-source-joined" {
				file, e := source.OpenFileWithContext(ctx, "synthetic.bin", os.O_RDONLY, 0)
				if e == nil || file != nil {
					if file != nil {
						file.Close()
					}
					t.Fatal("late original cancellation published a file")
				}
			} else {
				repetitions := 1
				if kind == "stat-no-reader-actor-left" {
					repetitions = 64
				}
				for i := 0; i < repetitions; i++ {
					info, e := source.StatWithContext(ctx, "synthetic.bin")
					if kind == "stat-no-reader-actor-left" {
						if e != nil || info == nil || info.Size() != 128 {
							t.Fatal("actual owned stat unavailable", e)
						}
					} else if e == nil || info != nil {
						t.Fatal("cancelled/unconfined metadata published FileInfo", kind, e)
					}
				}
			}
			if got := providerMetadataDescriptors(t, service.RootPath()); got != before {
				t.Fatal("stat/open error returned before actual metadata descriptors ended", before, got)
			}
			if kind == "stat-no-reader-actor-left" && ctx.Err() != nil {
				t.Fatal("local stat close retired caller's original parent context")
			}
		})
	}
}
