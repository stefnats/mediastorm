package recordings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"syscall"

	"novastream/models"
)

type derivedVersion struct {
	index   int
	info    os.FileInfo
	changed string
}

func changeToken(info os.FileInfo) string {
	value := reflect.ValueOf(info.Sys())
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return ""
	}
	for _, name := range []string{"Ctim", "Ctimespec", "ChangeTime"} {
		field := value.FieldByName(name)
		if field.IsValid() && field.CanInterface() {
			return fmt.Sprint(field.Interface())
		}
	}
	return ""
}
func sameDerivedVersion(a, b derivedVersion) bool {
	return a.index == b.index && a.changed != "" && a.changed == b.changed && os.SameFile(a.info, b.info) && a.info.Mode() == b.info.Mode() && a.info.Size() == b.info.Size() && a.info.ModTime().Equal(b.info.ModTime())
}
func sameDerivedVersions(a, b []derivedVersion) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sameDerivedVersion(a[i], b[i]) {
			return false
		}
	}
	return true
}

type derivedInputs struct {
	files       []*timingInput
	total       int64
	paths       []string
	directories []os.FileInfo
}

func (in *derivedInputs) close() {
	for i := len(in.files) - 1; i >= 0; i-- {
		in.files[i].close()
	}
}
func (in *derivedInputs) versions() []derivedVersion {
	out := make([]derivedVersion, 0, len(in.files))
	for i, f := range in.files {
		out = append(out, derivedVersion{i + 1, f.info, changeToken(f.info)})
	}
	return out
}
func (in *derivedInputs) check() error {
	for i, path := range in.paths {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(info, in.directories[i]) {
			return ErrDerivedUnavailable
		}
		held, err := in.files[0].roots[i].Stat(".")
		if err != nil || !os.SameFile(held, in.directories[i]) {
			return ErrDerivedUnavailable
		}
	}
	for i, f := range in.files {
		if err := f.check(); err != nil {
			return ErrDerivedUnavailable
		}
		info, err := f.file.Stat()
		if err != nil || !sameDerivedVersion(derivedVersion{i + 1, f.info, changeToken(f.info)}, derivedVersion{i + 1, info, changeToken(info)}) {
			return ErrDerivedUnavailable
		}
	}
	return nil
}
func (s *Service) openDerivedInputs(ctx context.Context, r models.Recording, check func() error) (result *derivedInputs, err error) {
	if err = validateDerivedLedger(r); err != nil {
		return nil, err
	}
	in := &derivedInputs{}
	defer func() {
		if result == nil {
			in.close()
		}
	}()
	parts := append([]models.RecordingPart{}, r.Capture.Parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Index < parts[j].Index })
	for i, p := range parts {
		if err = check(); err != nil {
			return nil, err
		}
		var input *timingInput
		if i == 0 {
			input, err = s.openTimingInput(ctx, r.ID, 1, check)
		} else {
			input = &timingInput{job: in.files[0].job, name: mustExportPartName(i + 1)}
			info, e := input.job.Lstat(input.name)
			if e != nil || !info.Mode().IsRegular() {
				return nil, ErrDerivedUnavailable
			}
			input.file, err = input.job.OpenFile(input.name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
			if err == nil {
				input.info, err = input.file.Stat()
			}
			if err != nil {
				input.close()
				return nil, ErrDerivedUnavailable
			}
			if !os.SameFile(info, input.info) {
				input.close()
				return nil, ErrDerivedUnavailable
			}
		}
		if err != nil {
			if errors.Is(err, ErrDerivedAccess) || ctx.Err() != nil {
				return nil, ErrDerivedAccess
			}
			return nil, ErrDerivedUnavailable
		}
		in.files = append(in.files, input)
		if i == 0 {
			root, e := filepath.Abs(s.outputDir)
			if e != nil {
				return nil, ErrDerivedUnavailable
			}
			in.paths = []string{root, filepath.Join(root, r.UserID), filepath.Join(root, r.UserID, r.ID)}
			for _, dir := range input.roots {
				info, e := dir.Stat(".")
				if e != nil {
					return nil, ErrDerivedUnavailable
				}
				in.directories = append(in.directories, info)
			}
			if len(in.directories) != len(in.paths) {
				return nil, ErrDerivedUnavailable
			}
		}
		if input.info.Size() <= 0 || input.info.Size() != p.Bytes || changeToken(input.info) == "" {
			return nil, ErrDerivedUnavailable
		}
		if input.info.Size() > derivedInputLimit-in.total {
			return nil, ErrDerivedTooLarge
		}
		in.total += input.info.Size()
	}
	if err = check(); err != nil {
		return nil, err
	}
	if err = in.check(); err != nil {
		return nil, err
	}
	return in, nil
}

type derivedWork struct {
	history       *derivedHistoryCapture
	root          *os.Root
	parent        *os.Root
	dir           *os.File
	output        *os.File
	outputInfo    os.FileInfo
	directoryInfo os.FileInfo
	id            string
	name          string
	created       bool
	count         int
	total         int64
	digests       []string
	stageVersions []derivedVersion
	staged        bool
	mu            sync.Mutex
	once          sync.Once
}

func (s *Service) newDerivedWork(check func() error) (out *derivedWork, err error) {
	if runtime.GOOS != "linux" {
		return nil, ErrDerivedUnavailable
	}
	if err = check(); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(s.outputDir)
	if err != nil {
		return nil, ErrDerivedUnavailable
	}
	before, err := os.Lstat(path)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrDerivedUnavailable
	}
	parent, err := os.OpenRoot(path)
	if err != nil {
		return nil, ErrDerivedUnavailable
	}
	w := &derivedWork{parent: parent}
	defer func() {
		if out == nil {
			w.close()
		}
	}()
	actual, err := parent.Stat(".")
	if err != nil || !os.SameFile(actual, before) {
		return nil, ErrDerivedUnavailable
	}
	w.id, err = newDerivedID()
	if err != nil {
		return nil, err
	}
	w.name = ".recording-derived-" + w.id
	if err = parent.Mkdir(w.name, 0700); err != nil {
		return nil, ErrDerivedUnavailable
	}
	w.created = true
	w.directoryInfo, err = parent.Lstat(w.name)
	if err != nil || !w.directoryInfo.IsDir() || w.directoryInfo.Mode()&os.ModeSymlink != 0 {
		return nil, ErrDerivedUnavailable
	}
	w.root, err = openExportRoot(parent, w.name)
	if err != nil {
		return nil, ErrDerivedUnavailable
	}
	w.dir, err = w.root.Open(".")
	if err != nil {
		return nil, ErrDerivedUnavailable
	}
	if err = check(); err != nil {
		return nil, err
	}
	return w, nil
}
func (w *derivedWork) close() {
	w.once.Do(func() {
		if w.output != nil {
			w.output.Close()
		}
		if w.dir != nil {
			w.dir.Close()
		}
		if w.root != nil {
			for i := 1; i <= w.count; i++ {
				_ = w.root.Remove(mustExportPartName(i))
			}
			for _, name := range []string{"inputs.ffconcat", "output.part.mkv", "joined.mkv"} {
				_ = w.root.Remove(name)
			}
			w.root.Close()
		}
		if w.parent != nil {
			if w.created && w.directoryInfo != nil {
				current, e := w.parent.Lstat(w.name)
				if e == nil && current.IsDir() && current.Mode()&os.ModeSymlink == 0 && os.SameFile(current, w.directoryInfo) {
					_ = w.parent.Remove(w.name)
				}
			}
			w.parent.Close()
		}
	})
}
func (w *derivedWork) removeStaging() {
	for i := 1; i <= w.count; i++ {
		_ = w.root.Remove(mustExportPartName(i))
	}
	_ = w.root.Remove("inputs.ffconcat")
}
func (w *derivedWork) stage(ctx context.Context, in *derivedInputs, check func() error) error {
	if err := check(); err != nil {
		return err
	}
	// statfs is admission evidence, not a claim of physical reserved space.
	var stat syscall.Statfs_t
	if err := syscall.Fstatfs(int(w.dir.Fd()), &stat); err != nil {
		return ErrDerivedUnavailable
	}
	if stat.Bsize <= 0 || (stat.Bavail != 0 && uint64(stat.Bsize) > math.MaxUint64/stat.Bavail) {
		return ErrDerivedTooLarge
	}
	free := uint64(stat.Bsize) * stat.Bavail
	required := uint64(in.total)*2 + (256 << 20) + (64 << 20)
	if free < required {
		return ErrDerivedTooLarge
	}
	buffer := make([]byte, 1<<20)
	for i, input := range in.files {
		if err := check(); err != nil {
			return err
		}
		out, err := w.root.OpenFile(mustExportPartName(i+1), os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
		if err != nil {
			return ErrDerivedUnavailable
		}
		w.count = i + 1
		hash := sha256.New()
		remaining := input.info.Size()
		for remaining > 0 {
			if err = check(); err != nil {
				out.Close()
				return err
			}
			n, e := input.file.Read(buffer[:min(int64(len(buffer)), remaining)])
			if err = check(); err != nil {
				clear(buffer)
				out.Close()
				return err
			}
			if n > 0 {
				if _, err = out.Write(buffer[:n]); err != nil {
					out.Close()
					return ErrDerivedUnavailable
				}
				_, _ = hash.Write(buffer[:n])
				remaining -= int64(n)
			}
			if e != nil && !(e == io.EOF && remaining == 0) {
				out.Close()
				return ErrDerivedUnavailable
			}
			if n == 0 && e == nil {
				out.Close()
				return io.ErrNoProgress
			}
		}
		if err = out.Sync(); err != nil {
			out.Close()
			return ErrDerivedUnavailable
		}
		// Retain the digest privately; no source path or credentials are emitted.
		w.digests = append(w.digests, hex.EncodeToString(hash.Sum(nil)))
		info, e := out.Stat()
		if e != nil {
			out.Close()
			return ErrDerivedUnavailable
		}
		w.stageVersions = append(w.stageVersions, derivedVersion{i + 1, info, changeToken(info)})
		out.Close()
		w.total += input.info.Size()
	}
	w.staged = true
	return check()
}
func (w *derivedWork) checkStaging() error {
	if err := w.checkDirectory(); err != nil {
		return err
	}
	if !w.staged {
		return nil
	}
	for _, version := range w.stageVersions {
		info, err := w.root.Lstat(mustExportPartName(version.index))
		if err != nil || !info.Mode().IsRegular() || !sameDerivedVersion(version, derivedVersion{version.index, info, changeToken(info)}) {
			return ErrDerivedUnavailable
		}
	}
	return nil
}
func (w *derivedWork) checkDirectory() error {
	if w.root == nil || w.parent == nil || w.directoryInfo == nil {
		return ErrDerivedUnavailable
	}
	named, err := w.parent.Lstat(w.name)
	if err != nil || !named.IsDir() || named.Mode()&os.ModeSymlink != 0 || !os.SameFile(named, w.directoryInfo) {
		return ErrDerivedUnavailable
	}
	held, err := w.root.Stat(".")
	if err != nil || !os.SameFile(held, w.directoryInfo) {
		return ErrDerivedUnavailable
	}
	return nil
}
func (w *derivedWork) publish(check func() error) error {
	if err := check(); err != nil {
		return err
	}
	if err := w.output.Sync(); err != nil {
		return ErrDerivedUnavailable
	}
	info, err := w.output.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > w.outputLimit() {
		return ErrDerivedTooLarge
	}
	// Rename only this private generated file, never a source recording path.
	if _, err = w.root.Lstat("joined.mkv"); !os.IsNotExist(err) {
		return ErrDerivedUnavailable
	}
	if err = w.root.Rename("output.part.mkv", "joined.mkv"); err != nil {
		return ErrDerivedUnavailable
	}
	w.outputInfo, err = w.output.Stat()
	if err != nil {
		return ErrDerivedUnavailable
	}
	return check()
}
func (w *derivedWork) outputLimit() int64 {
	overhead := int64(64 << 20)
	if fraction := w.total / 50; fraction > overhead {
		overhead = fraction
	}
	return w.total + overhead
}
func (w *derivedWork) checkOutput(file *os.File) error {
	if err := w.checkDirectory(); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil || w.outputInfo == nil || !os.SameFile(info, w.outputInfo) || info.Size() != w.outputInfo.Size() || !info.ModTime().Equal(w.outputInfo.ModTime()) || changeToken(info) != changeToken(w.outputInfo) {
		return ErrDerivedUnavailable
	}
	path, err := w.root.Lstat("joined.mkv")
	if err != nil || !path.Mode().IsRegular() || !os.SameFile(info, path) {
		return ErrDerivedUnavailable
	}
	return nil
}
