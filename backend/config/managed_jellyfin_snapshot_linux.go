//go:build linux

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"golang.org/x/sys/unix"
)

// CaptureManagedJellyfin reads only the original configured credential fields.
// It performs no Load, migration, defaults, directory creation or provider IO.
// Only participating in-process Save/restore mutations have history coverage.
func (manager *Manager) CaptureManagedJellyfin(ctx context.Context, integrationID string) (*ManagedJellyfinLease, error) {
	return captureManagedJellyfin(ctx, manager, integrationID, readManagedJellyfinFile)
}

type managedLinuxSnapshot struct {
	file       *os.File
	directory  *os.File
	parentPath string
	base       string
	original   unix.Stat_t
	parent     unix.Stat_t
}

func openManagedDirectory(path string) (*os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, ErrManagedConfigurationUnavailable
	}
	parts := strings.Split(strings.TrimPrefix(filepath.Clean(path), "/"), "/")
	if len(parts) > 64 {
		return nil, ErrManagedConfigurationUnavailable
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrManagedConfigurationUnavailable
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, ErrManagedConfigurationUnavailable
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), "managed configuration directory"), nil
}

func managedSameFile(left, right unix.Stat_t) bool {
	return left.Mode&unix.S_IFMT == unix.S_IFREG && right.Mode&unix.S_IFMT == unix.S_IFREG &&
		left.Dev == right.Dev && left.Ino == right.Ino && left.Mode == right.Mode && left.Nlink == right.Nlink &&
		left.Size == right.Size && left.Mtim == right.Mtim && left.Ctim == right.Ctim
}

func (snapshot *managedLinuxSnapshot) check(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil || snapshot.file == nil || snapshot.directory == nil {
		return ErrManagedConfigurationUnavailable
	}
	var held, originalParent unix.Stat_t
	if unix.Fstat(int(snapshot.file.Fd()), &held) != nil || unix.Fstat(int(snapshot.directory.Fd()), &originalParent) != nil || !managedSameFile(snapshot.original, held) || originalParent.Dev != snapshot.parent.Dev || originalParent.Ino != snapshot.parent.Ino {
		return ErrManagedConfigurationUnavailable
	}
	directory, err := openManagedDirectory(snapshot.parentPath)
	if err != nil {
		return ErrManagedConfigurationUnavailable
	}
	defer directory.Close()
	var parent, named unix.Stat_t
	if unix.Fstat(int(directory.Fd()), &parent) != nil || parent.Dev != snapshot.parent.Dev || parent.Ino != snapshot.parent.Ino || unix.Fstatat(int(directory.Fd()), snapshot.base, &named, unix.AT_SYMLINK_NOFOLLOW) != nil || !managedSameFile(snapshot.original, named) || ctx.Err() != nil {
		return ErrManagedConfigurationUnavailable
	}
	return nil
}

func (snapshot *managedLinuxSnapshot) close() error {
	var failed bool
	if snapshot.file != nil {
		failed = snapshot.file.Close() != nil
		snapshot.file = nil
	}
	if snapshot.directory != nil {
		failed = snapshot.directory.Close() != nil || failed
		snapshot.directory = nil
	}
	if failed {
		return ErrManagedConfigurationUnavailable
	}
	return nil
}

func readManagedJellyfinFile(ctx context.Context, path, integrationID string) (managedSnapshotFile, managedJellyfinCredentials, error) {
	var credentials managedJellyfinCredentials
	snapshot := &managedLinuxSnapshot{parentPath: filepath.Dir(path), base: filepath.Base(path)}
	fail := func() (managedSnapshotFile, managedJellyfinCredentials, error) {
		_ = snapshot.close()
		return nil, managedJellyfinCredentials{}, ErrManagedConfigurationUnavailable
	}
	directory, err := openManagedDirectory(snapshot.parentPath)
	if err != nil {
		return fail()
	}
	snapshot.directory = directory
	if unix.Fstat(int(directory.Fd()), &snapshot.parent) != nil {
		return fail()
	}
	fd, err := unix.Openat(int(directory.Fd()), snapshot.base, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return fail()
	}
	snapshot.file = os.NewFile(uintptr(fd), "managed configuration file")
	if unix.Fstat(fd, &snapshot.original) != nil || snapshot.original.Mode&unix.S_IFMT != unix.S_IFREG || snapshot.original.Size < 2 || snapshot.original.Size > managedSnapshotLimit || ctx.Err() != nil {
		return fail()
	}
	raw, err := io.ReadAll(io.LimitReader(snapshot.file, managedSnapshotLimit+1))
	defer clear(raw)
	if err != nil || int64(len(raw)) != snapshot.original.Size || snapshot.check(ctx) != nil {
		return fail()
	}
	credentials, err = decodeManagedJellyfin(raw, integrationID)
	if err != nil || ctx.Err() != nil || snapshot.check(ctx) != nil {
		return fail()
	}
	return snapshot, credentials, nil
}

func scanManagedJSON(decoder *json.Decoder, depth int, count *int, root bool) error {
	if depth > 64 || *count >= 131072 {
		return ErrManagedConfigurationUnavailable
	}
	token, err := decoder.Token()
	*count++
	if err != nil {
		return ErrManagedConfigurationUnavailable
	}
	delimiter, compound := token.(json.Delim)
	if root && (!compound || delimiter != '{') {
		return ErrManagedConfigurationUnavailable
	}
	if !compound {
		return nil
	}
	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			if len(keys) >= 4096 || *count >= 131072 {
				return ErrManagedConfigurationUnavailable
			}
			keyToken, keyErr := decoder.Token()
			*count++
			key, ok := keyToken.(string)
			if keyErr != nil || !ok || len(key) > 4096 {
				return ErrManagedConfigurationUnavailable
			}
			if _, duplicate := keys[key]; duplicate {
				return ErrManagedConfigurationUnavailable
			}
			keys[key] = struct{}{}
			if scanManagedJSON(decoder, depth+1, count, false) != nil {
				return ErrManagedConfigurationUnavailable
			}
		}
		if *count >= 131072 {
			return ErrManagedConfigurationUnavailable
		}
		end, endErr := decoder.Token()
		*count++
		if endErr != nil || end != json.Delim('}') {
			return ErrManagedConfigurationUnavailable
		}
	case '[':
		for decoder.More() {
			if scanManagedJSON(decoder, depth+1, count, false) != nil {
				return ErrManagedConfigurationUnavailable
			}
		}
		if *count >= 131072 {
			return ErrManagedConfigurationUnavailable
		}
		end, endErr := decoder.Token()
		*count++
		if endErr != nil || end != json.Delim(']') {
			return ErrManagedConfigurationUnavailable
		}
	default:
		return ErrManagedConfigurationUnavailable
	}
	return nil
}

func managedObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, ErrManagedConfigurationUnavailable
	}
	return object, nil
}

func managedCanonicalKeys(object map[string]json.RawMessage, relevant ...string) bool {
	for key := range object {
		for _, canonical := range relevant {
			if strings.EqualFold(key, canonical) && key != canonical {
				return false
			}
		}
	}
	return true
}

func managedString(object map[string]json.RawMessage, key string, maximum int, optional bool) (string, bool) {
	raw, exists := object[key]
	if !exists && optional {
		return "", true
	}
	var value string
	if !exists || len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &value) != nil || len(value) > maximum || strings.ContainsAny(value, "\x00\r\n") || (!optional && strings.TrimSpace(value) == "") {
		return "", false
	}
	return value, true
}

func decodeManagedJellyfin(raw []byte, integrationID string) (managedJellyfinCredentials, error) {
	var result managedJellyfinCredentials
	if len(raw) > managedSnapshotLimit || !utf8.Valid(raw) {
		return result, ErrManagedConfigurationUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	count := 0
	if scanManagedJSON(decoder, 0, &count, true) != nil {
		return result, ErrManagedConfigurationUnavailable
	}
	if _, err := decoder.Token(); err != io.EOF {
		return result, ErrManagedConfigurationUnavailable
	}
	root, err := managedObject(raw)
	if err != nil || !managedCanonicalKeys(root, "jellyfin") {
		return result, ErrManagedConfigurationUnavailable
	}
	jellyfin, err := managedObject(root["jellyfin"])
	if err != nil || !managedCanonicalKeys(jellyfin, "accounts") {
		return result, ErrManagedConfigurationUnavailable
	}
	var accounts []json.RawMessage
	if json.Unmarshal(jellyfin["accounts"], &accounts) != nil || accounts == nil || len(accounts) == 0 || len(accounts) > 256 {
		return result, ErrManagedConfigurationUnavailable
	}
	seen := make(map[string]struct{}, len(accounts))
	found := false
	for _, rawAccount := range accounts {
		account, accountErr := managedObject(rawAccount)
		if accountErr != nil || !managedCanonicalKeys(account, "id", "serverUrl", "token", "userId", "ownerAccountId") {
			return managedJellyfinCredentials{}, ErrManagedConfigurationUnavailable
		}
		id, valid := managedString(account, "id", 128, false)
		if !valid || strings.TrimSpace(id) != id {
			return managedJellyfinCredentials{}, ErrManagedConfigurationUnavailable
		}
		if _, duplicate := seen[id]; duplicate {
			return managedJellyfinCredentials{}, ErrManagedConfigurationUnavailable
		}
		seen[id] = struct{}{}
		if id != integrationID {
			continue
		}
		serverURL, serverOK := managedString(account, "serverUrl", 4096, false)
		token, tokenOK := managedString(account, "token", 16<<10, false)
		userID, userOK := managedString(account, "userId", 128, false)
		ownerID, ownerOK := managedString(account, "ownerAccountId", 128, true)
		parsed, urlErr := url.Parse(serverURL)
		if !serverOK || !tokenOK || !userOK || !ownerOK || urlErr != nil || parsed == nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.TrimSpace(serverURL) != serverURL {
			return managedJellyfinCredentials{}, ErrManagedConfigurationUnavailable
		}
		result = managedJellyfinCredentials{integrationID: id, serverURL: serverURL, token: token, userID: userID, ownerAccountID: ownerID}
		found = true
	}
	if !found {
		return result, ErrManagedConfigurationUnavailable
	}
	return result, nil
}
