//go:build linux

package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const managedSyntheticToken = "synthetic-original-token-not-a-real-credential"

func managedSyntheticJSON(token string) []byte {
	value := map[string]any{
		"legacyUnrelated": map[string]any{"keep": "raw legacy value", "enabled": false},
		"jellyfin": map[string]any{"accounts": []any{map[string]any{
			"id": "integration", "serverUrl": "https://provider.invalid/jellyfin/base", "token": token,
			"userId": "synthetic-upstream-user", "ownerAccountId": "synthetic-integration-owner",
		}}},
	}
	raw, _ := json.Marshal(value)
	return raw
}

func managedSyntheticSettings(token string) Settings {
	settings := DefaultSettings()
	settings.Jellyfin.Accounts = []JellyfinAccount{{ID: "integration", ServerURL: "https://provider.invalid/jellyfin/base", Token: token, UserID: "synthetic-upstream-user", OwnerAccountID: "synthetic-integration-owner"}}
	return settings
}

func managedFileFixture(t *testing.T, raw []byte) *Manager {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.json")
	if os.WriteFile(path, raw, 0o600) != nil {
		t.Fatal("write synthetic settings fixture")
	}
	return NewManager(path)
}

func managedActualLease(t *testing.T, manager *Manager) *ManagedJellyfinLease {
	t.Helper()
	lease, err := manager.CaptureManagedJellyfin(managedCoordinatorContext(t), "integration")
	if err != nil {
		t.Fatal("actual synthetic snapshot capture failed")
	}
	t.Cleanup(func() { _ = lease.Close() })
	return lease
}

func TestManagedJellyfinSnapshotPreservesOriginalRawCredentialsWithoutLoad(t *testing.T) {
	raw := managedSyntheticJSON("  synthetic token with raw spaces ä  ")
	manager := managedFileFixture(t, raw)
	lease := managedActualLease(t, manager)
	if lease.Check(context.Background()) != nil {
		t.Fatal("original snapshot is not current")
	}
	credentials := lease.state.credentials
	if credentials.token != "  synthetic token with raw spaces ä  " || credentials.serverURL != "https://provider.invalid/jellyfin/base" || credentials.userID != "synthetic-upstream-user" || credentials.ownerAccountID != "synthetic-integration-owner" || credentials.integrationID != "integration" {
		t.Fatal("pure snapshot normalized or substituted original credential fields")
	}
	after, err := os.ReadFile(manager.ConfigPath())
	if err != nil || !bytes.Equal(raw, after) {
		t.Fatal("snapshot rewrote or migrated the source")
	}
	entries, err := os.ReadDir(filepath.Dir(manager.ConfigPath()))
	if err != nil || len(entries) != 1 {
		t.Fatal("snapshot created default or staging files")
	}
}

func TestManagedJellyfinSnapshotMissingAndNonregularFilesDenyWithoutWrites(t *testing.T) {
	for _, kind := range []string{"missing", "missing-parent", "directory", "fifo", "final-symlink", "parent-symlink", "symlink-traversal", "empty"} {
		t.Run(kind, func(t *testing.T) {
			directory := t.TempDir()
			path := filepath.Join(directory, "settings.json")
			switch kind {
			case "missing-parent":
				path = filepath.Join(directory, "absent", "settings.json")
			case "directory":
				if os.Mkdir(path, 0o700) != nil {
					t.Fatal("create synthetic directory")
				}
			case "fifo":
				if unix.Mkfifo(path, 0o600) != nil {
					t.Fatal("create synthetic FIFO")
				}
			case "final-symlink":
				target := filepath.Join(directory, "target.json")
				if os.WriteFile(target, managedSyntheticJSON(managedSyntheticToken), 0o600) != nil || os.Symlink(target, path) != nil {
					t.Fatal("create synthetic symlink")
				}
			case "parent-symlink":
				target := filepath.Join(directory, "target")
				if os.Mkdir(target, 0o700) != nil || os.WriteFile(filepath.Join(target, "settings.json"), managedSyntheticJSON(managedSyntheticToken), 0o600) != nil || os.Symlink(target, filepath.Join(directory, "alias")) != nil {
					t.Fatal("create synthetic directory alias")
				}
				path = filepath.Join(directory, "alias", "settings.json")
			case "symlink-traversal":
				if os.WriteFile(path, managedSyntheticJSON(managedSyntheticToken), 0o600) != nil || os.Symlink(t.TempDir(), filepath.Join(directory, "alias")) != nil {
					t.Fatal("create synthetic traversal alias")
				}
				path = directory + "/alias/../settings.json"
			case "empty":
				if os.WriteFile(path, nil, 0o600) != nil {
					t.Fatal("create synthetic empty file")
				}
			}
			before, _ := os.ReadDir(directory)
			lease, err := NewManager(path).CaptureManagedJellyfin(managedCoordinatorContext(t), "integration")
			if lease != nil || !errors.Is(err, ErrManagedConfigurationUnavailable) {
				t.Fatal("nonregular or missing configuration was adopted")
			}
			after, _ := os.ReadDir(directory)
			if len(before) != len(after) {
				t.Fatal("denied snapshot created configuration files")
			}
			if kind == "missing-parent" {
				if _, err := os.Lstat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("snapshot created a missing parent directory")
				}
			}
		})
	}
}

func TestManagedJellyfinSnapshotRetainsOriginalRawFilePathSemantics(t *testing.T) {
	for _, kind := range []string{"trailing-separator", "final-dot", "oversized-dot-alias"} {
		t.Run(kind, func(t *testing.T) {
			raw := managedSyntheticJSON(managedSyntheticToken)
			original := managedFileFixture(t, raw)
			path := original.ConfigPath()
			wantError := unix.ENOTDIR
			switch kind {
			case "trailing-separator":
				path += "/"
			case "final-dot":
				path += "/."
			case "oversized-dot-alias":
				path = filepath.Dir(path) + strings.Repeat("/.", 2100) + "/settings.json"
				wantError = unix.ENAMETOOLONG
			}
			manager := NewManager(path)
			read := false
			lease, err := captureManagedJellyfin(managedCoordinatorContext(t), manager, "integration", func(context.Context, string, string) (managedSnapshotFile, managedJellyfinCredentials, error) {
				read = true
				return nil, managedJellyfinCredentials{}, nil
			})
			if lease != nil || !errors.Is(err, ErrManagedConfigurationUnavailable) || read {
				t.Fatal("capture normalized a rejected raw file path before reading")
			}
			if lease, err := manager.CaptureManagedJellyfin(managedCoordinatorContext(t), "integration"); lease != nil || !errors.Is(err, ErrManagedConfigurationUnavailable) {
				t.Fatal("actual capture adopted a different normalized file path")
			}
			if _, err := manager.Load(); !errors.Is(err, wantError) {
				t.Fatal("actual legacy Load did not reject the original raw path")
			}
			if err := manager.Save(managedSyntheticSettings(managedSyntheticToken)); !errors.Is(err, wantError) {
				t.Fatal("actual legacy Save changed its raw path rejection")
			}
			after, err := os.ReadFile(original.ConfigPath())
			if err != nil || !bytes.Equal(raw, after) {
				t.Fatal("rejected raw paths changed the actual regular-file canary")
			}
			if current := managedActualLease(t, original); current.Check(context.Background()) != nil {
				t.Fatal("the exact original regular path was incorrectly denied")
			}
		})
	}
}

func TestManagedJellyfinSnapshotRejectsAmbiguousMalformedAndUnboundedJSON(t *testing.T) {
	valid := string(managedSyntheticJSON(managedSyntheticToken))
	account := `{"id":"integration","serverUrl":"https://provider.invalid/base","token":"synthetic","userId":"synthetic-user"}`
	wrap := func(value string) []byte { return []byte(`{"jellyfin":{"accounts":` + value + `}}`) }
	cases := map[string][]byte{
		"null-root": []byte(`null`), "array-root": []byte(`[]`), "truncated": []byte(`{"jellyfin":`),
		"trailing-object": []byte(valid + `{}`), "trailing-garbage": []byte(valid + `x`),
		"duplicate-unrelated": []byte(`{"other":1,"other":2,"jellyfin":{"accounts":[` + account + `]}}`),
		"escaped-duplicate":   []byte(`{"jellyfin":{"accounts":[` + account + `]},"jell\u0079fin":{}}`),
		"root-case-alias":     []byte(strings.Replace(valid, `"jellyfin"`, `"Jellyfin"`, 1)),
		"accounts-case-alias": []byte(strings.Replace(valid, `"accounts"`, `"Accounts"`, 1)),
		"token-case-alias":    []byte(strings.Replace(valid, `"token"`, `"Token"`, 1)),
		"duplicate-source-id": wrap(`[` + account + `,` + account + `]`),
		"null-account":        wrap(`[null]`), "null-accounts": wrap(`null`), "object-accounts": wrap(`{}`),
		"empty-accounts": wrap(`[]`), "missing-user": wrap(`[{"id":"integration","serverUrl":"https://provider.invalid","token":"synthetic"}]`),
		"wrong-token-type": wrap(`[` + strings.Replace(account, `"token":"synthetic"`, `"token":4`, 1) + `]`),
		"null-token":       wrap(`[` + strings.Replace(account, `"token":"synthetic"`, `"token":null`, 1) + `]`),
		"blank-token":      wrap(`[` + strings.Replace(account, `"token":"synthetic"`, `"token":" "`, 1) + `]`),
		"header-injection": wrap(`[` + strings.Replace(account, `"token":"synthetic"`, `"token":"a\r\nb"`, 1) + `]`),
		"credential-url":   wrap(`[` + strings.Replace(account, `https://provider.invalid/base`, `https://synthetic:synthetic@provider.invalid/base`, 1) + `]`),
		"query-url":        wrap(`[` + strings.Replace(account, `https://provider.invalid/base`, `https://provider.invalid/base?key=synthetic`, 1) + `]`),
		"non-http-url":     wrap(`[` + strings.Replace(account, `https://provider.invalid/base`, `file:///synthetic`, 1) + `]`),
		"fragment-url":     wrap(`[` + strings.Replace(account, `https://provider.invalid/base`, `https://provider.invalid/base#synthetic`, 1) + `]`),
		"oversized-token":  wrap(`[` + strings.Replace(account, `"token":"synthetic"`, `"token":"`+strings.Repeat("s", (16<<10)+1)+`"`, 1) + `]`),
		"oversized-file":   []byte(strings.Repeat(" ", managedSnapshotLimit+1)),
		"deep-unrelated":   []byte(`{"other":` + strings.Repeat(`[`, 66) + `0` + strings.Repeat(`]`, 66) + `,"jellyfin":{"accounts":[` + account + `]}}`),
		"invalid-utf8":     append(append([]byte(`{"other":"`), 0xff), []byte(`","jellyfin":{"accounts":[`+account+`]}}`)...),
		"too-many-tokens":  []byte(`{"other":[` + strings.Repeat(`0,`, 131072) + `0],"jellyfin":{"accounts":[` + account + `]}}`),
	}
	var excessiveAccounts []string
	for index := 0; index < 257; index++ {
		excessiveAccounts = append(excessiveAccounts, strings.Replace(account, `"id":"integration"`, fmt.Sprintf(`"id":"id-%d"`, index), 1))
	}
	cases["too-many-accounts"] = wrap(`[` + strings.Join(excessiveAccounts, `,`) + `]`)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			manager := managedFileFixture(t, raw)
			lease, err := manager.CaptureManagedJellyfin(managedCoordinatorContext(t), "integration")
			if lease != nil || !errors.Is(err, ErrManagedConfigurationUnavailable) {
				t.Fatal("malformed, ambiguous or excessive credential input was adopted")
			}
			after, err := os.ReadFile(manager.ConfigPath())
			if err != nil || !bytes.Equal(raw, after) {
				t.Fatal("denied snapshot rewrote its source")
			}
		})
	}
}

func TestManagedJellyfinSnapshotSiblingAndAliasSavesRetireOriginal(t *testing.T) {
	for _, spelling := range []string{"absolute-sibling", "relative-sibling", "parent-alias", "final-alias"} {
		t.Run(spelling, func(t *testing.T) {
			manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
			lease := managedActualLease(t, manager)
			path := manager.ConfigPath()
			switch spelling {
			case "relative-sibling":
				working, err := os.Getwd()
				if err != nil {
					t.Fatal("read working path")
				}
				path, err = filepath.Rel(working, path)
				if err != nil {
					t.Fatal("construct relative sibling")
				}
			case "parent-alias":
				alias := filepath.Join(t.TempDir(), "alias")
				if os.Symlink(filepath.Dir(path), alias) != nil {
					t.Fatal("construct parent alias")
				}
				path = filepath.Join(alias, "settings.json")
			case "final-alias":
				alias := filepath.Join(t.TempDir(), "settings.json")
				if os.Symlink(path, alias) != nil {
					t.Fatal("construct final alias")
				}
				path = alias
			}
			if NewManager(path).Save(managedSyntheticSettings(managedSyntheticToken)) != nil {
				t.Fatal("actual sibling Save failed")
			}
			managedRequireRetired(t, lease)
		})
	}
}

func TestManagedJellyfinSnapshotManagedABAReleasesOnlyNewEpoch(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	original := managedActualLease(t, manager)
	if NewManager(manager.ConfigPath()).Save(managedSyntheticSettings("synthetic-version-B")) != nil {
		t.Fatal("write managed version B")
	}
	second := managedActualLease(t, manager)
	if NewManager(manager.ConfigPath()).Save(managedSyntheticSettings(managedSyntheticToken)) != nil {
		t.Fatal("write managed version A again")
	}
	current := managedActualLease(t, manager)
	managedRequireRetired(t, original)
	managedRequireRetired(t, second)
	if current.Check(context.Background()) != nil || current.state.credentials.token != managedSyntheticToken {
		t.Fatal("new original epoch did not retain version A")
	}
}

func TestManagedJellyfinSnapshotActualSameValueAndFailedSavesRetireDescriptors(t *testing.T) {
	for _, mode := range []string{"same-value", "normalization-failure", "filesystem-failure"} {
		t.Run(mode, func(t *testing.T) {
			manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
			settings := managedSyntheticSettings(managedSyntheticToken)
			if manager.Save(settings) != nil {
				t.Fatal("write normalized original settings")
			}
			before, err := os.ReadFile(manager.ConfigPath())
			if err != nil {
				t.Fatal("read original synthetic bytes")
			}
			lease := managedActualLease(t, manager)
			switch mode {
			case "normalization-failure":
				settings.Server.ExternalBackendURL = "file:///unsupported"
			case "filesystem-failure":
				if os.Mkdir(manager.ConfigPath()+".tmp", 0o700) != nil {
					t.Fatal("create actual staging-file conflict")
				}
			}
			err = NewManager(manager.ConfigPath()).Save(settings)
			if (err == nil) != (mode == "same-value") {
				t.Fatal("actual Save success/failure semantics changed")
			}
			managedRequireRetired(t, lease)
			after, err := os.ReadFile(manager.ConfigPath())
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("same-value or rejected Save changed original file bytes")
			}
		})
	}
}

func TestManagedJellyfinSnapshotVisibleExternalDriftDeniesWithoutHistoryClaim(t *testing.T) {
	for _, change := range []string{"replacement", "in-place", "parent-replacement", "delete"} {
		t.Run(change, func(t *testing.T) {
			manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
			lease := managedActualLease(t, manager)
			path := manager.ConfigPath()
			var err error
			switch change {
			case "replacement":
				staged := path + ".external"
				if os.WriteFile(staged, managedSyntheticJSON(managedSyntheticToken), 0o600) != nil {
					t.Fatal("stage synthetic external replacement")
				}
				err = os.Rename(staged, path)
			case "in-place":
				err = os.WriteFile(path, managedSyntheticJSON("synthetic-visible-change"), 0o600)
			case "parent-replacement":
				parent := filepath.Dir(path)
				err = os.Rename(parent, parent+".original")
				if err == nil {
					t.Cleanup(func() { _ = os.RemoveAll(parent + ".original") })
					err = os.Mkdir(parent, 0o700)
				}
				if err == nil {
					err = os.WriteFile(path, managedSyntheticJSON(managedSyntheticToken), 0o600)
				}
			case "delete":
				err = os.Remove(path)
			}
			if err != nil || lease.Check(context.Background()) == nil {
				t.Fatal("visible file/root drift remained usable")
			}
		})
	}
}

func TestManagedJellyfinSnapshotOriginalDeadlineCancellationAndLifetimeCap(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	for _, ctx := range []context.Context{nil, context.Background()} {
		if lease, err := manager.CaptureManagedJellyfin(ctx, "integration"); lease != nil || err == nil {
			t.Fatal("unbounded or absent context issued a lease")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	lease, err := manager.CaptureManagedJellyfin(ctx, "integration")
	if err != nil {
		cancel()
		t.Fatal("capture bounded original")
	}
	t.Cleanup(func() { _ = lease.Close() })
	originalDeadline, _ := ctx.Deadline()
	observedDeadline, _ := lease.Context().Deadline()
	if !observedDeadline.Equal(originalDeadline) {
		t.Fatal("capture renewed the original deadline")
	}
	cancel()
	if lease.Check(context.Background()) == nil {
		t.Fatal("original cancellation was ignored")
	}
	long, cancelLong := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancelLong()
	before := time.Now()
	capped, err := manager.CaptureManagedJellyfin(long, "integration")
	if err != nil {
		t.Fatal("capture long original")
	}
	t.Cleanup(func() { _ = capped.Close() })
	deadline, _ := capped.Context().Deadline()
	if deadline.After(time.Now().Add(managedMaximumLifetime)) || deadline.Before(before.Add(managedMaximumLifetime)) {
		t.Fatal("maximum original lifetime was not fixed at capture")
	}
	expiring, cancelExpiry := context.WithTimeout(context.Background(), time.Second)
	defer cancelExpiry()
	short, err := manager.CaptureManagedJellyfin(expiring, "integration")
	if err != nil {
		t.Fatal("capture expiring original")
	}
	t.Cleanup(func() { _ = short.Close() })
	select {
	case <-short.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("original expiry did not signal")
	}
	if short.Check(context.Background()) == nil {
		t.Fatal("expired original remained current")
	}
}

func TestManagedJellyfinSnapshotValuePointerCopyAndJSONRemainOpaque(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	lease := managedActualLease(t, manager)
	copy := *lease
	for _, value := range []any{lease, *lease, &copy, copy} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
			text := fmt.Sprintf(format, value)
			for _, sentinel := range []string{managedSyntheticToken, "provider.invalid", "synthetic-upstream-user", "synthetic-integration-owner"} {
				if strings.Contains(text, sentinel) {
					t.Fatal("formatted capability exposed private snapshot data")
				}
			}
		}
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != `{"state":"opaque-managed-configuration"}` {
			t.Fatal("JSON capability representation was not opaque")
		}
	}
	var imported ManagedJellyfinLease
	if json.Unmarshal([]byte(`{"state":"opaque-managed-configuration"}`), &imported) == nil || imported.Check(context.Background()) == nil || imported.Context() != nil || copy.Check(context.Background()) == nil || copy.Context() != nil || copy.Close() == nil {
		t.Fatal("copied or imported capability acquired original authority")
	}
	if lease.Check(context.Background()) != nil {
		t.Fatal("copied capability retired the actual original")
	}
}

type managedHeldClose struct {
	managedSnapshotFile
	started chan struct{}
	release chan struct{}
}

func (file *managedHeldClose) close() error {
	close(file.started)
	<-file.release
	return file.managedSnapshotFile.close()
}

func TestManagedJellyfinSnapshotFDAndAdmissionRemainChargedThroughActualClose(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	var leases []*ManagedJellyfinLease
	for index := 0; index < managedPathLeaseLimit; index++ {
		leases = append(leases, managedActualLease(t, manager))
	}
	original := leases[0]
	actual := original.state.file.(*managedLinuxSnapshot)
	fd := int(actual.file.Fd())
	held := &managedHeldClose{managedSnapshotFile: actual, started: make(chan struct{}), release: make(chan struct{})}
	original.state.file = held
	var release sync.Once
	unblock := func() { release.Do(func() { close(held.release) }) }
	defer unblock()
	closed := make(chan error, 1)
	go func() { closed <- original.Close() }()
	select {
	case <-held.started:
	case <-time.After(2 * time.Second):
		t.Fatal("actual Close did not enter descriptor cleanup")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
		t.Fatal("descriptor was not actually held at the cleanup barrier")
	}
	if original.Check(context.Background()) == nil {
		t.Fatal("closing owner remained usable")
	}
	if lease, err := manager.CaptureManagedJellyfin(managedCoordinatorContext(t), "integration"); lease != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("slot was refunded before actual descriptor cleanup")
	}
	written := make(chan error, 1)
	go func() { written <- manager.Save(managedSyntheticSettings(managedSyntheticToken)) }()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal("writer failed while consumer cleanup was blocked")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer joined consumer cleanup")
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal("actual descriptor Close failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("actual descriptor cleanup did not finish")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatal("slot was refunded without closing the actual descriptor")
	}
	current := managedActualLease(t, manager)
	if current.Check(context.Background()) != nil {
		t.Fatal("cleaned slot was not reusable by a new original")
	}
}

func TestManagedJellyfinSnapshotCaptureAndSaveSerializeOriginalBytes(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	ready := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	type captured struct {
		lease *ManagedJellyfinLease
		err   error
	}
	result := make(chan captured, 1)
	ctx := managedCoordinatorContext(t)
	go func() {
		lease, err := captureManagedJellyfin(ctx, manager, "integration", func(ctx context.Context, path, id string) (managedSnapshotFile, managedJellyfinCredentials, error) {
			file, credentials, err := readManagedJellyfinFile(ctx, path, id)
			close(ready)
			<-release
			return file, credentials, err
		})
		result <- captured{lease, err}
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("actual original file was not read")
	}
	written := make(chan error, 1)
	go func() { written <- manager.Save(managedSyntheticSettings("synthetic-version-B")) }()
	waitManagedWriterPending(t)
	if lease, err := manager.CaptureManagedJellyfin(ctx, "integration"); lease != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("capture adopted a replacement while a writer was pending")
	}
	unblock()
	var original *ManagedJellyfinLease
	select {
	case got := <-result:
		if got.err != nil || got.lease == nil {
			t.Fatal("original capture failed before the serialized writer")
		}
		original = got.lease
		t.Cleanup(func() { _ = original.Close() })
	case <-time.After(2 * time.Second):
		t.Fatal("original capture did not finish")
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatal("serialized writer failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serialized writer did not finish")
	}
	if original.state.credentials.token != managedSyntheticToken {
		t.Fatal("capture mixed replacement credentials into the original")
	}
	managedRequireRetired(t, original)
	current := managedActualLease(t, manager)
	if current.state.credentials.token != "synthetic-version-B" || current.Check(context.Background()) != nil {
		t.Fatal("new original did not use the completed managed write")
	}
}

func TestManagedJellyfinSnapshotFailedCaptureRetainsFDSlotUntilCleanup(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	for index := 0; index < managedPathLeaseLimit-1; index++ {
		managedActualLease(t, manager)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	result := make(chan error, 1)
	ctx := managedCoordinatorContext(t)
	go func() {
		_, err := captureManagedJellyfin(ctx, manager, "integration", func(ctx context.Context, path, id string) (managedSnapshotFile, managedJellyfinCredentials, error) {
			file, credentials, err := readManagedJellyfinFile(ctx, path, id)
			if err != nil {
				return file, credentials, err
			}
			return &managedHeldClose{managedSnapshotFile: file, started: started, release: release}, credentials, errors.New("synthetic late decode failure")
		})
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("failed capture did not enter actual file cleanup")
	}
	if lease, err := manager.CaptureManagedJellyfin(ctx, "integration"); lease != nil || !errors.Is(err, ErrManagedConfigurationBusy) {
		t.Fatal("failed capture refunded its slot before actual cleanup")
	}
	unblock()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("failed capture issued a capability")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed capture cleanup did not join")
	}
	if lease := managedActualLease(t, manager); lease.Check(context.Background()) != nil {
		t.Fatal("actual cleaned slot did not recover")
	}
}

func TestManagedJellyfinSnapshotRepeatedCloseReclaimsDescriptorsAndPathState(t *testing.T) {
	manager := managedFileFixture(t, managedSyntheticJSON(managedSyntheticToken))
	for index := 0; index < 32; index++ {
		lease := managedActualLease(t, manager)
		actual := lease.state.file.(*managedLinuxSnapshot)
		fileFD, rootFD := int(actual.file.Fd()), int(actual.directory.Fd())
		if lease.Close() != nil || lease.Close() != nil || lease.Check(context.Background()) == nil {
			t.Fatal("original Close was not final and idempotent")
		}
		for _, fd := range []int{fileFD, rootFD} {
			if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
				t.Fatal("actual descriptor survived final Close")
			}
		}
	}
	managedConfigAdmission.Lock()
	clean := managedConfigAdmission.leases == 0 && managedConfigAdmission.decoding == 0 && len(managedConfigAdmission.paths) == 0 && len(managedConfigAdmission.owners) == 0
	managedConfigAdmission.Unlock()
	if !clean {
		t.Fatal("closed originals retained admission or pathname registry state")
	}
}
