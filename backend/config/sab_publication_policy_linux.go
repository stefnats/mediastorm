//go:build linux

package config

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

func sabPublicationPolicySupported() bool { return true }
func readOriginalSABPublicationPolicy(ctx context.Context, manager *Manager) (out sabPublicationPolicyRead, err error) {
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
			out = sabPublicationPolicyRead{}
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
	snapshot.file = os.NewFile(uintptr(fd), "original SAB publication policy")
	if unix.Fstat(fd, &snapshot.original) != nil || snapshot.original.Mode&unix.S_IFMT != unix.S_IFREG || snapshot.original.Size < 2 || snapshot.original.Size > managedSnapshotLimit || ctx.Err() != nil {
		return out, ErrSecurityCoordinator
	}
	raw, err := io.ReadAll(io.LimitReader(snapshot.file, managedSnapshotLimit+1))
	defer clear(raw)
	if err != nil || int64(len(raw)) != snapshot.original.Size || snapshot.check(ctx) != nil || !utf8.Valid(raw) {
		return out, ErrSecurityCoordinator
	}
	out, err = decodeSABPublicationPolicy(raw)
	if err != nil || snapshot.check(ctx) != nil {
		return sabPublicationPolicyRead{}, ErrSecurityCoordinator
	}
	stat := snapshot.original
	out.version = bridgeLoginPolicyVersion{device: stat.Dev, inode: stat.Ino, links: uint64(stat.Nlink), mode: stat.Mode, size: stat.Size, modifiedSeconds: stat.Mtim.Sec, modifiedNanos: stat.Mtim.Nsec, changedSeconds: stat.Ctim.Sec, changedNanos: stat.Ctim.Nsec}
	out.digest = sha256.Sum256(raw)
	return out, nil
}

func decodeSABPublicationPolicy(raw []byte) (out sabPublicationPolicyRead, err error) {
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
	if err != nil || !managedCanonicalKeys(root, "security", "sabnzbd", "database") {
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
	sab, err := managedObject(root["sabnzbd"])
	if err != nil || !managedCanonicalKeys(sab, "enabled", "fallbackHost", "fallbackApiKey", "importStageTargetRoot", "importStageMaxBytes") {
		return out, ErrSecurityCoordinator
	}
	var enabled *bool
	if json.Unmarshal(sab["enabled"], &enabled) != nil || enabled == nil || !*enabled {
		return out, ErrSecurityCoordinator
	}
	var valid bool
	if out.values.StageRoot, valid = managedString(sab, "importStageTargetRoot", 4096, false); !valid || !strings.HasPrefix(out.values.StageRoot, "/") || out.values.StageRoot == "/" || path.Clean(out.values.StageRoot) != out.values.StageRoot || strings.ContainsAny(out.values.StageRoot, "\\\x00") || strings.Contains(out.values.StageRoot, "://") {
		return out, ErrSecurityCoordinator
	}
	if json.Unmarshal(sab["importStageMaxBytes"], &out.values.MaxBytes) != nil || out.values.MaxBytes <= 0 || out.values.MaxBytes > 1<<40 {
		return out, ErrSecurityCoordinator
	}
	host, ok := managedString(sab, "fallbackHost", 8192, false)
	key, ok2 := managedString(sab, "fallbackApiKey", 8192, false)
	if !ok || !ok2 || strings.TrimSpace(host) == "" || key == "" {
		return out, ErrSecurityCoordinator
	}
	fingerprint, _ := json.Marshal([]string{strings.TrimSpace(host), key})
	digest := sha256.Sum256(fingerprint)
	clear(fingerprint)
	out.values.SourceFingerprint = hex.EncodeToString(digest[:])
	out.values.EnforceProfilePins = *enforce
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
