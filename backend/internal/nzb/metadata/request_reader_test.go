package metadata

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	metapb "novastream/internal/nzb/metadata/proto"
)

type cancelAfterMetadataRead struct {
	file   *os.File
	cancel context.CancelFunc
}

func (r cancelAfterMetadataRead) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	r.cancel()
	return n, err
}
func assertNoMetadataDescriptors(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if e == nil && (target == root || strings.HasPrefix(target, root+string(os.PathSeparator))) {
			t.Fatalf("owned metadata descriptor still open: %s", target)
		}
	}
}
func TestMetadataRequestReadBounds(t *testing.T) {
	for _, name := range []string{"regular-file-byte-bound-before-decode", "nonregular-metadata-denied", "directory-entry-overflow", "depth-and-total-entry-budget", "original-context-before-and-after-io", "all-owned-files-joined"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			service := NewMetadataService(root)
			ctx := context.Background()
			value := &metapb.FileMetadata{FileSize: 64, ModifiedAt: 1, SegmentData: []*metapb.SegmentData{{Id: "synthetic", SegmentSize: 64}}}
			if err := service.WriteFileMetadata("movie", value); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "regular-file-byte-bound-before-decode":
				file, err := os.OpenFile(filepath.Join(root, "oversize.meta"), os.O_CREATE|os.O_WRONLY, 0600)
				if err != nil {
					t.Fatal(err)
				}
				if err = file.Truncate(16<<20 + 1); err != nil {
					t.Fatal(err)
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err = service.ReadRequestMetadata(ctx, "oversize", 16<<20); !errors.Is(err, ErrRequestMetadataBudget) {
					t.Fatalf("sparse oversize accepted: %v", err)
				}
				got, err := service.ReadRequestMetadata(ctx, "movie", 4096)
				if err != nil || got.FileSize != 64 {
					t.Fatalf("actual bounded protobuf read: %v", err)
				}
			case "nonregular-metadata-denied":
				if err := os.Mkdir(filepath.Join(root, "directory.meta"), 0700); err != nil {
					t.Fatal(err)
				}
				if _, err := service.ReadRequestMetadata(ctx, "directory", 4096); err == nil {
					t.Fatal("directory metadata accepted")
				}
				if err := os.Symlink(filepath.Join(root, "movie.meta"), filepath.Join(root, "alias.meta")); err != nil {
					t.Fatal(err)
				}
				if _, err := service.ReadRequestMetadata(ctx, "alias", 4096); err == nil {
					t.Fatal("symlink metadata accepted")
				}
				if _, err := service.ReadRequestMetadata(ctx, "../outside", 4096); err == nil {
					t.Fatal("path escaped root")
				}
			case "directory-entry-overflow":
				for i := 0; i < 4; i++ {
					if err := os.WriteFile(filepath.Join(root, fmt.Sprintf("unrelated-%d", i)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := service.ListRequestDirectory(ctx, "/", 3); !errors.Is(err, ErrRequestMetadataBudget) {
					t.Fatalf("actual enumeration exceeded limit: %v", err)
				}
			case "depth-and-total-entry-budget":
				if _, err := service.ReadRequestMetadata(ctx, strings.Repeat("part/", 33)+"movie", 4096); !errors.Is(err, ErrRequestMetadataBudget) {
					t.Fatalf("depth not bounded: %v", err)
				}
				for i := 0; i < 5; i++ {
					if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("dir-%d", i)), 0700); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := service.ListRequestDirectory(ctx, "/", 5); !errors.Is(err, ErrRequestMetadataBudget) {
					t.Fatalf("all actual entries must consume enumeration budget: %v", err)
				}
			case "original-context-before-and-after-io":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := service.ReadRequestMetadata(cancelled, "movie", 4096); !errors.Is(err, context.Canceled) {
					t.Fatal("cancelled original context ignored")
				}
				original, cancel := context.WithCancel(ctx)
				defer cancel()
				file, err := os.Open(filepath.Join(root, "movie.meta"))
				if err != nil {
					t.Fatal(err)
				}
				reader := requestMetadataReader{ctx: original, reader: cancelAfterMetadataRead{file, cancel}}
				if n, err := reader.Read(make([]byte, 4096)); n != 0 || !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel during actual file Read exposed bytes: %d,%v", n, err)
				}
				if err = file.Close(); err != nil {
					t.Fatal(err)
				}
			case "all-owned-files-joined":
				for i := 0; i < 10; i++ {
					if _, err := service.ReadRequestMetadata(ctx, "movie", 4096); err != nil {
						t.Fatal(err)
					}
					if _, err := service.ListRequestDirectory(ctx, "/", 4096); err != nil && err != io.EOF {
						t.Fatal(err)
					}
				}
				// Both the root descriptor and each actual file descriptor must be gone on return.
				assertNoMetadataDescriptors(t, root)
			}
			assertNoMetadataDescriptors(t, root)
		})
	}
}
