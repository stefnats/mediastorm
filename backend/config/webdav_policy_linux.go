//go:build linux

package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

func webDAVPolicySupported() bool { return true }
func readOriginalWebDAVPolicy(ctx context.Context, manager *Manager) (out webDAVPolicyRead, err error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || manager.path == "" || len(manager.path) > 4096 {
		return out, ErrSecurityCoordinator
	}
	for _, part := range strings.Split(manager.path, string(filepath.Separator)) {
		if part == ".." {
			return out, ErrSecurityCoordinator
		}
	}
	absolute, err := filepath.Abs(manager.path)
	if err != nil || len(absolute) > 4096 {
		return out, ErrSecurityCoordinator
	}
	snapshot := &managedLinuxSnapshot{parentPath: filepath.Dir(absolute), base: filepath.Base(absolute)}
	defer func() {
		if snapshot.close() != nil {
			out = webDAVPolicyRead{}
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
	snapshot.file = os.NewFile(uintptr(fd), "original WebDAV policy")
	if unix.Fstat(fd, &snapshot.original) != nil || snapshot.original.Mode&unix.S_IFMT != unix.S_IFREG || snapshot.original.Size < 2 || snapshot.original.Size > managedSnapshotLimit || ctx.Err() != nil {
		return out, ErrSecurityCoordinator
	}
	raw, err := io.ReadAll(io.LimitReader(snapshot.file, managedSnapshotLimit+1))
	defer clear(raw)
	if err != nil || int64(len(raw)) != snapshot.original.Size || snapshot.check(ctx) != nil || !utf8.Valid(raw) {
		return out, ErrSecurityCoordinator
	}
	out, err = decodeWebDAVPolicy(raw)
	if err != nil || snapshot.check(ctx) != nil {
		return webDAVPolicyRead{}, ErrSecurityCoordinator
	}
	stat := snapshot.original
	out.version = bridgeLoginPolicyVersion{device: stat.Dev, inode: stat.Ino, links: uint64(stat.Nlink), mode: stat.Mode, size: stat.Size, modifiedSeconds: stat.Mtim.Sec, modifiedNanos: stat.Mtim.Nsec, changedSeconds: stat.Ctim.Sec, changedNanos: stat.Ctim.Nsec}
	out.digest = sha256.Sum256(raw)
	return out, nil
}
func decodeWebDAVPolicy(raw []byte) (out webDAVPolicyRead, err error) {
	if len(raw) > managedSnapshotLimit || !utf8.Valid(raw) {
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
	if err != nil || !managedCanonicalKeys(root, "security", "webdav", "database") {
		return out, ErrSecurityCoordinator
	}
	security, err := managedObject(root["security"])
	if err != nil || !managedCanonicalKeys(security, "enforceProfilePins") {
		return out, ErrSecurityCoordinator
	}
	dav, err := managedObject(root["webdav"])
	if err != nil || !managedCanonicalKeys(dav, "enabled", "prefix", "username", "password") {
		return out, ErrSecurityCoordinator
	}
	readBool := func(object map[string]json.RawMessage, key string) (bool, bool) {
		var value *bool
		if json.Unmarshal(object[key], &value) != nil || value == nil {
			return false, false
		}
		return *value, true
	}
	var valid bool
	if out.values.Enabled, valid = readBool(dav, "enabled"); !valid {
		return out, ErrSecurityCoordinator
	}
	if out.values.EnforceProfilePins, valid = readBool(security, "enforceProfilePins"); !valid {
		return out, ErrSecurityCoordinator
	}
	if out.values.Prefix, valid = managedString(dav, "prefix", 256, false); !valid || !validWebDAVPolicyValues(out.values) {
		return out, ErrSecurityCoordinator
	}
	if out.username, valid = managedString(dav, "username", 256, false); !valid || out.username == "" || strings.ContainsAny(out.username, ":\x00\r\n") {
		return out, ErrSecurityCoordinator
	}
	if out.password, valid = managedString(dav, "password", 4096, false); !valid || out.password == "" || strings.ContainsRune(out.password, 0) {
		return out, ErrSecurityCoordinator
	}
	if raw, exists := root["database"]; exists {
		database, e := managedObject(raw)
		if e != nil || !managedCanonicalKeys(database, "url") {
			return out, ErrSecurityCoordinator
		}
		if out.databaseURL, valid = managedString(database, "url", managedSnapshotLimit, true); !valid {
			return out, ErrSecurityCoordinator
		}
	}
	return out, nil
}
