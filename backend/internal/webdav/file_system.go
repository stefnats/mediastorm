package webdav

import (
	"context"
	"log/slog"
	"os"

	"github.com/spf13/afero"
	"golang.org/x/net/webdav"
	"novastream/internal/nzb/utils"
)

type fileSystem struct {
	afero.Fs
}

func aferoToWebdavFS(vfs afero.Fs) webdav.FileSystem {
	return &fileSystem{
		Fs: vfs,
	}
}

func (fs *fileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fs.Fs.Mkdir(name, perm)
}

func (fs *fileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	pa := utils.NewPathWithArgs(name)
	r, _ := ctx.Value(utils.RangeKey).(string)
	if r != "" {
		pa.SetRange(r)
	}

	s, _ := ctx.Value(utils.ContentLengthKey).(string)
	if s != "" {
		pa.SetFileSize(s)
	}

	isCopy, _ := ctx.Value(utils.IsCopy).(bool)
	if isCopy {
		pa.SetIsCopy()
	}

	origin, _ := ctx.Value(utils.Origin).(string)
	if origin != "" {
		pa.SetOrigin(origin)
	}

	showCorrupted, _ := ctx.Value(utils.ShowCorrupted).(bool)
	if showCorrupted {
		pa.SetShowCorrupted()
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source, ok := fs.Fs.(interface {
		OpenFileWithContext(context.Context, string, int, os.FileMode) (afero.File, error)
	}); ok {
		return source.OpenFileWithContext(ctx, pa.String(), flag, perm)
	}
	return fs.Fs.OpenFile(pa.String(), flag, perm)
}

func (fs *fileSystem) RemoveAll(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if source, ok := fs.Fs.(interface {
		RemoveAllWithContext(context.Context, string) error
	}); ok {
		return source.RemoveAllWithContext(ctx, name)
	}
	return fs.Fs.RemoveAll(name)
}

func (fs *fileSystem) Rename(ctx context.Context, oldName, newName string) error {
	// Add logging to understand when MOVE operations trigger renames
	slog.InfoContext(ctx, "WebDAV filesystem Rename called",
		"oldName", oldName,
		"newName", newName)
	if err := ctx.Err(); err != nil {
		return err
	}
	if source, ok := fs.Fs.(interface {
		RenameWithContext(context.Context, string, string) error
	}); ok {
		return source.RenameWithContext(ctx, oldName, newName)
	}
	return fs.Fs.Rename(oldName, newName)
}

func (fs *fileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if source, ok := fs.Fs.(interface {
		StatWithContext(context.Context, string) (os.FileInfo, error)
	}); ok {
		return source.StatWithContext(ctx, name)
	}
	return fs.Fs.Stat(name)
}
