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

func bridgeLoginPolicySupported() bool { return true }

func readOriginalBridgeLoginPolicy(ctx context.Context, manager *Manager) (out bridgeLoginPolicyRead, err error) {
	if ctx == nil || ctx.Err() != nil || manager == nil || manager.path == "" || len(manager.path) > 4096 {
		return out, ErrSecurityCoordinator
	}
	parts := strings.Split(manager.path, string(filepath.Separator))
	if last := parts[len(parts)-1]; last == "" || last == "." {
		return out, ErrSecurityCoordinator
	}
	for _, part := range parts {
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
			out = bridgeLoginPolicyRead{}
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
	snapshot.file = os.NewFile(uintptr(fd), "original bridge policy")
	if unix.Fstat(fd, &snapshot.original) != nil || snapshot.original.Mode&unix.S_IFMT != unix.S_IFREG || snapshot.original.Size < 2 || snapshot.original.Size > managedSnapshotLimit || ctx.Err() != nil {
		return out, ErrSecurityCoordinator
	}
	raw, err := io.ReadAll(io.LimitReader(snapshot.file, managedSnapshotLimit+1))
	defer clear(raw)
	if err != nil || int64(len(raw)) != snapshot.original.Size || snapshot.check(ctx) != nil || !utf8.Valid(raw) {
		return out, ErrSecurityCoordinator
	}
	out, err = decodeBridgeLoginPolicy(raw)
	if err != nil || snapshot.check(ctx) != nil {
		return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
	}
	stat := snapshot.original
	out.version = bridgeLoginPolicyVersion{device: stat.Dev, inode: stat.Ino, links: uint64(stat.Nlink), mode: stat.Mode, size: stat.Size, modifiedSeconds: stat.Mtim.Sec, modifiedNanos: stat.Mtim.Nsec, changedSeconds: stat.Ctim.Sec, changedNanos: stat.Ctim.Nsec}
	out.digest = sha256.Sum256(raw)
	return out, nil
}

func decodeBridgeLoginPolicy(raw []byte) (out bridgeLoginPolicyRead, err error) {
	if len(raw) > managedSnapshotLimit || !utf8.Valid(raw) {
		return out, ErrSecurityCoordinator
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	count := 0
	if scanManagedJSON(decoder, 0, &count, true) != nil {
		return out, ErrSecurityCoordinator
	}
	if _, err := decoder.Token(); err != io.EOF {
		return out, ErrSecurityCoordinator
	}
	root, err := managedObject(raw)
	if err != nil || !managedCanonicalKeys(root, "security", "jellyfinBridge", "database") {
		return out, ErrSecurityCoordinator
	}
	security, err := managedObject(root["security"])
	if err != nil || !managedCanonicalKeys(security, "enforceProfilePins") {
		return out, ErrSecurityCoordinator
	}
	bridge, err := managedObject(root["jellyfinBridge"])
	if err != nil || !managedCanonicalKeys(bridge, "allowPinLogin", "enabled") {
		return out, ErrSecurityCoordinator
	}
	readBool := func(object map[string]json.RawMessage, name string) (bool, bool) {
		var value *bool
		if json.Unmarshal(object[name], &value) != nil || value == nil {
			return false, false
		}
		return *value, true
	}
	var valid bool
	if out.values.EnforceProfilePins, valid = readBool(security, "enforceProfilePins"); !valid {
		return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
	}
	if out.values.AllowPinLogin, valid = readBool(bridge, "allowPinLogin"); !valid {
		return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
	}
	if out.values.BridgeEnabled, valid = readBool(bridge, "enabled"); !valid {
		return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
	}
	// Quick Connect is a distinct policy consumer. An absent or malformed
	// flag cannot authorize that flow, while the original password policy's
	// three-value decoding and compatibility remain unchanged.
	if managedCanonicalKeys(bridge, "quickConnect") {
		out.quickConnect, out.quickConnectValid = readBool(bridge, "quickConnect")
	}
	if databaseRaw, exists := root["database"]; exists {
		database, objectErr := managedObject(databaseRaw)
		if objectErr != nil || !managedCanonicalKeys(database, "url") {
			return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
		}
		if out.databaseURL, valid = managedString(database, "url", managedSnapshotLimit, true); !valid {
			return bridgeLoginPolicyRead{}, ErrSecurityCoordinator
		}
	}
	return out, nil
}
