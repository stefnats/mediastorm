package metadata

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"
	metapb "novastream/internal/nzb/metadata/proto"
)

var ErrRequestMetadataBudget = errors.New("request metadata budget exceeded")

func requestRelativePath(value string) (string, error) {
	if !utf8.ValidString(value) || len(value) > 4096 || strings.ContainsRune(value, 0) || strings.Contains(value, "\\") {
		return "", fs.ErrInvalid
	}
	for _, part := range strings.Split(value, "/") {
		if part == ".." {
			return "", fs.ErrPermission
		}
	}
	clean := strings.TrimPrefix(path.Clean("/"+value), "/")
	if clean != "" && len(strings.Split(clean, "/")) > 32 {
		return "", ErrRequestMetadataBudget
	}
	if clean == "" {
		clean = "."
	}
	return clean, nil
}

type requestMetadataReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r requestMetadataReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if current := r.ctx.Err(); current != nil {
		return 0, current
	}
	return n, err
}

// ReadRequestMetadata confines and bounds the actual regular file before protobuf allocation.
// A cancellation signals work retirement; synchronous local filesystem calls are joined on return.
func (ms *MetadataService) ReadRequestMetadata(ctx context.Context, virtualPath string, limit int64) (*metapb.FileMetadata, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, context.Canceled
	}
	if limit < 1 || limit > 16<<20 {
		return nil, ErrRequestMetadataBudget
	}
	relative, err := requestRelativePath(virtualPath)
	if err != nil {
		return nil, err
	}
	name := relative + ".meta"
	root, err := os.OpenRoot(ms.rootPath)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	before, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > limit {
		return nil, ErrRequestMetadataBudget
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	current, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !current.Mode().IsRegular() || !os.SameFile(before, current) || current.Size() > limit {
		return nil, ErrRequestMetadataBudget
	}
	raw, err := io.ReadAll(io.LimitReader(requestMetadataReader{ctx, file}, limit+1))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || int64(len(raw)) > limit {
		return nil, ErrRequestMetadataBudget
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	value := new(metapb.FileMetadata)
	if err = proto.Unmarshal(raw, value); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return value, nil
}

// ListRequestDirectory limits actual enumeration before aggregation, including unrelated entries.
func (ms *MetadataService) ListRequestDirectory(ctx context.Context, virtualPath string, limit int) ([]fs.DirEntry, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, context.Canceled
	}
	if limit < 1 || limit > 4096 {
		return nil, ErrRequestMetadataBudget
	}
	relative, err := requestRelativePath(virtualPath)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(ms.rootPath)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	before, err := root.Lstat(relative)
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&fs.ModeSymlink != 0 {
		return nil, fs.ErrPermission
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	current, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !current.IsDir() || !os.SameFile(before, current) {
		return nil, fs.ErrPermission
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := file.ReadDir(limit + 1)
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(entries) > limit {
		return nil, ErrRequestMetadataBudget
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !utf8.ValidString(entry.Name()) || len(entry.Name()) > 256 || entry.Type()&fs.ModeSymlink != 0 {
			return nil, fs.ErrPermission
		}
	}
	return entries, nil
}
