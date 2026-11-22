//go:build linux

package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

func CaptureFamilyNZBWorkingDirectory(ctx context.Context, manager *Manager) (*FamilyNZBRoot, error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || manager.path == "" || len(manager.path) > 4096 || !utf8.ValidString(manager.path) || strings.ContainsAny(manager.path, "\\\x00") {
		return nil, ErrSecurityCoordinator
	}
	fd, err := unix.Open(".", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrSecurityCoordinator
	}
	file := os.NewFile(uintptr(fd), "original NZB startup directory")
	keep := false
	defer func() {
		if !keep {
			_ = file.Close()
		}
	}()
	buffer := make([]byte, 4097)
	n, err := unix.Getcwd(buffer)
	if err != nil || n < 2 || n > len(buffer) || buffer[n-1] != 0 {
		return nil, ErrSecurityCoordinator
	}
	cwd := string(buffer[:n-1])
	info, err := file.Stat()
	if err != nil || !info.IsDir() || !filepath.IsAbs(cwd) || filepath.Clean(cwd) != cwd || len(cwd) > 4096 || !utf8.ValidString(cwd) {
		return nil, ErrSecurityCoordinator
	}
	absolute := manager.path
	if !filepath.IsAbs(absolute) {
		absolute = filepath.Join(cwd, absolute)
	}
	absolute = filepath.Clean(absolute)
	if len(absolute) > 4096 || !utf8.ValidString(absolute) {
		return nil, ErrSecurityCoordinator
	}
	life, cancel := context.WithCancel(ctx)
	r := &FamilyNZBRoot{&familyNZBRootState{manager: manager, managerPath: manager.path, cwdName: cwd, nativeAbsolute: absolute, cwd: file, cwdInfo: info, ctx: life, cancel: cancel, done: make(chan struct{})}}
	r.self = r
	if checkFamilyNZBDirectory(ctx, r) != nil {
		cancel()
		return nil, ErrSecurityCoordinator
	}
	keep = true
	return r, nil
}
func checkFamilyNZBDirectory(ctx context.Context, r *FamilyNZBRoot) error {
	if ctx == nil || ctx.Err() != nil || r.ctx.Err() != nil {
		return ErrSecurityCoordinator
	}
	held, err := r.cwd.Stat()
	if err != nil {
		return ErrSecurityCoordinator
	}
	if !held.IsDir() || held.Mode() != r.cwdInfo.Mode() || !os.SameFile(held, r.cwdInfo) {
		return errFamilyNZBRootChanged
	}
	named, err := openManagedDirectory(r.cwdName)
	if err != nil {
		return ErrSecurityCoordinator
	}
	info, statErr := named.Stat()
	closeErr := named.Close()
	if statErr != nil || closeErr != nil {
		return ErrSecurityCoordinator
	}
	if !info.IsDir() || info.Mode() != r.cwdInfo.Mode() || !os.SameFile(info, r.cwdInfo) {
		return errFamilyNZBRootChanged
	}
	if ctx.Err() != nil || r.ctx.Err() != nil {
		return ErrSecurityCoordinator
	}
	return nil
}
func familyNZBMetadataRoot(cwd, cache string) (string, error) {
	if cache == "" || len(cache) > 4096 || !utf8.ValidString(cache) || strings.ContainsAny(cache, "\\\x00") || strings.Contains(cache, "://") {
		return "", ErrSecurityCoordinator
	}
	root := cache
	if !filepath.IsAbs(root) {
		root = filepath.Join(cwd, root)
	}
	root = filepath.Join(root, "streams")
	if len(root) > 4096 || !filepath.IsAbs(root) || root == "/" {
		return "", ErrSecurityCoordinator
	}
	return root, nil
}
func readOriginalFamilyNZBPolicy(ctx context.Context, r *FamilyNZBRoot) (out familyNZBPolicyRead, err error) {
	if !r.enter() {
		return out, ErrSecurityCoordinator
	}
	defer r.operations.Done()
	if r.current(ctx) != nil {
		return out, ErrSecurityCoordinator
	}
	snapshot := &managedLinuxSnapshot{parentPath: filepath.Dir(r.nativeAbsolute), base: filepath.Base(r.nativeAbsolute)}
	defer func() {
		if snapshot.close() != nil {
			out = familyNZBPolicyRead{}
			err = ErrSecurityCoordinator
		}
	}()
	snapshot.directory, err = openManagedDirectory(snapshot.parentPath)
	if err != nil || unix.Fstat(int(snapshot.directory.Fd()), &snapshot.parent) != nil {
		return out, ErrSecurityCoordinator
	}
	fd, err := unix.Openat(int(snapshot.directory.Fd()), snapshot.base, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return out, ErrSecurityCoordinator
	}
	snapshot.file = os.NewFile(uintptr(fd), "original NZB native policy")
	if unix.Fstat(fd, &snapshot.original) != nil || snapshot.original.Mode&unix.S_IFMT != unix.S_IFREG || snapshot.original.Size < 2 || snapshot.original.Size > managedSnapshotLimit {
		return out, ErrSecurityCoordinator
	}
	raw, err := io.ReadAll(io.LimitReader(snapshot.file, managedSnapshotLimit+1))
	defer clear(raw)
	if err != nil || int64(len(raw)) != snapshot.original.Size || !utf8.Valid(raw) || snapshot.check(ctx) != nil {
		return out, ErrSecurityCoordinator
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	count := 0
	if scanManagedJSON(decoder, 0, &count, true) != nil {
		return out, ErrSecurityCoordinator
	}
	if _, err = decoder.Token(); err != io.EOF {
		return out, ErrSecurityCoordinator
	}
	root, err := managedObject(raw)
	if err != nil || !managedCanonicalKeys(root, "cache", "security", "database") {
		return out, ErrSecurityCoordinator
	}
	cache, err := managedObject(root["cache"])
	if err != nil || !managedCanonicalKeys(cache, "directory") {
		return out, ErrSecurityCoordinator
	}
	var ok bool
	out.cacheDirectory, ok = managedString(cache, "directory", 4096, false)
	if !ok {
		return out, ErrSecurityCoordinator
	}
	security, err := managedObject(root["security"])
	if err != nil || !managedCanonicalKeys(security, "enforceProfilePins") {
		return out, ErrSecurityCoordinator
	}
	var enforce *bool
	if json.Unmarshal(security["enforceProfilePins"], &enforce) != nil || enforce == nil {
		return out, ErrSecurityCoordinator
	}
	out.enforce = *enforce
	if value, exists := root["database"]; exists {
		database, e := managedObject(value)
		if e != nil || !managedCanonicalKeys(database, "url") {
			return out, ErrSecurityCoordinator
		}
		out.databaseURL, ok = managedString(database, "url", managedSnapshotLimit, true)
		if !ok {
			return out, ErrSecurityCoordinator
		}
	}
	if snapshot.check(ctx) != nil || r.current(ctx) != nil {
		return familyNZBPolicyRead{}, ErrSecurityCoordinator
	}
	if err = r.observeCache(out.cacheDirectory); err != nil {
		return familyNZBPolicyRead{}, err
	}
	st := snapshot.original
	out.version = bridgeLoginPolicyVersion{device: st.Dev, inode: st.Ino, links: uint64(st.Nlink), mode: st.Mode, size: st.Size, modifiedSeconds: st.Mtim.Sec, modifiedNanos: st.Mtim.Nsec, changedSeconds: st.Ctim.Sec, changedNanos: st.Ctim.Nsec}
	out.digest = sha256.Sum256(raw)
	return out, nil
}
