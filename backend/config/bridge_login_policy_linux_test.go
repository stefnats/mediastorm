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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sys/unix"
	"novastream/internal/settingscommit"
)

func bridgePolicyBind(t *testing.T, f *securityPGFixture, ctx context.Context) (*BridgeLoginPolicyObservation, *BridgeLoginPolicyFence, *settingscommit.ProgressFileHold) {
	t.Helper()
	observation, err := f.security.CaptureBridgePolicy(ctx)
	if err != nil {
		t.Fatal("capture actual original bridge policy", err)
	}
	hold, err := f.coordinator.TryProgressFileHold(ctx)
	if err != nil {
		t.Fatal("acquire original native hold", err)
	}
	t.Cleanup(func() { _ = hold.Close() })
	fence, err := f.security.BindBridgePolicy(ctx, observation, hold)
	if err != nil {
		t.Fatal("bind original policy data", err)
	}
	return observation, fence, hold
}

func TestBridgePolicyActualPGAllThreeFlagCombinations(t *testing.T) {
	f := securityPG(t)
	for bits := 0; bits < 8; bits++ {
		t.Run(fmt.Sprintf("flags-%d", bits), func(t *testing.T) {
			value := f.settings
			value.Security.EnforceProfilePins = bits&1 != 0
			value.JellyfinBridge.AllowPinLogin = bits&2 != 0
			value.JellyfinBridge.Enabled = bits&4 != 0
			if err := f.manager.Save(value); err != nil {
				t.Fatal("save own flag combination", err)
			}
			_, fence, hold := bridgePolicyBind(t, f, t.Context())
			actual, err := fence.Values()
			expected := BridgeLoginPolicyValues{bits&1 != 0, bits&2 != 0, bits&4 != 0}
			if err != nil || actual != expected {
				t.Fatal("three flags did not come from exact original file", err)
			}
			// This genuinely obtainable native hold is not an SQL transaction or mint permit.
			if err := hold.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := fence.Values(); err == nil {
				t.Fatal("closed hold retained data access")
			}
		})
	}
}

func TestBridgePolicyBusyAndFailedSaveRetireOnlyNewBindings(t *testing.T) {
	for _, kind := range []string{"busy", "failed-preflight"} {
		t.Run(kind, func(t *testing.T) {
			f := securityPG(t)
			observation, fence, hold := bridgePolicyBind(t, f, t.Context())
			before, err := os.ReadFile(f.manager.path)
			if err != nil {
				t.Fatal(err)
			}
			changed := f.settings
			changed.Security.EnforceProfilePins = false
			changed.JellyfinBridge.AllowPinLogin = true
			changed.JellyfinBridge.Enabled = true
			if kind == "failed-preflight" {
				changed.Server.AllowedPrivateMediaOrigins = []string{"not-an-origin"}
			}
			err = NewManager(f.manager.path).Save(changed)
			if err == nil || kind == "busy" && !errors.Is(err, settingscommit.ErrBusy) {
				t.Fatal("actual write attempt was not refused", err)
			}
			after, err := os.ReadFile(f.manager.path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("refused Save changed original file")
			}
			values, err := fence.Values()
			if err != nil || values != (BridgeLoginPolicyValues{true, false, false}) {
				t.Fatal("foreign attempt changed already-bound values", err)
			}
			if _, err := f.security.BindBridgePolicy(t.Context(), observation, hold); err == nil {
				t.Fatal("retired observation rebound after a genuine Save attempt")
			}
			if err := hold.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := fence.Values(); err == nil {
				t.Fatal("original hold close did not end bound data")
			}
		})
	}
}

func TestBridgePolicyOriginalContextsOwnerAndHold(t *testing.T) {
	for _, kind := range []string{"capture-context", "bind-context", "owner", "hold"} {
		t.Run(kind, func(t *testing.T) {
			f := securityPG(t)
			original, cancelOriginal := context.WithCancel(t.Context())
			defer cancelOriginal()
			binding, cancelBinding := context.WithCancel(t.Context())
			defer cancelBinding()
			observation, err := f.security.CaptureBridgePolicy(original)
			if err != nil {
				t.Fatal(err)
			}
			hold, err := f.coordinator.TryProgressFileHold(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer hold.Close()
			fence, err := f.security.BindBridgePolicy(binding, observation, hold)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "capture-context":
				cancelOriginal()
			case "bind-context":
				cancelBinding()
			case "owner":
				_ = f.security.Close()
			case "hold":
				_ = hold.Close()
			}
			if _, err := fence.Values(); err == nil {
				t.Fatal("departed original lifetime retained policy data")
			}
			if kind != "bind-context" {
				if _, err := f.security.BindBridgePolicy(t.Context(), observation, hold); err == nil {
					t.Fatal("departed original rebound through live replacement context")
				}
			}
		})
	}
}

func TestBridgePolicyOpaqueCopiesAndForeignHold(t *testing.T) {
	f := securityPG(t)
	observation, fence, hold := bridgePolicyBind(t, f, t.Context())
	copyOwner := *f.security
	if _, err := copyOwner.CaptureBridgePolicy(t.Context()); err == nil {
		t.Fatal("copied owner accepted")
	}
	if _, err := f.security.BindBridgePolicy(t.Context(), nil, hold); err == nil {
		t.Fatal("missing observation accepted")
	}
	if _, err := f.security.BindBridgePolicy(t.Context(), observation, nil); err == nil {
		t.Fatal("missing original hold accepted")
	}
	copiedObservation := *observation
	if _, err := f.security.BindBridgePolicy(t.Context(), &copiedObservation, hold); err == nil {
		t.Fatal("copied observation accepted")
	}
	copiedFence := *fence
	if _, err := copiedFence.Values(); err == nil {
		t.Fatal("copied fence accepted")
	}
	var empty BridgeLoginPolicyFence
	if _, err := empty.Values(); err == nil {
		t.Fatal("zero fence accepted")
	}
	var absent *BridgeLoginPolicyFence
	if _, err := absent.Values(); err == nil {
		t.Fatal("nil fence accepted")
	}
	for _, object := range []any{observation, *observation, fence, *fence} {
		raw, err := json.Marshal(object)
		if err != nil || string(raw) != "null" {
			t.Fatal("opaque JSON disclosed private values")
		}
		for _, verb := range []string{"%v", "%+v", "%#v"} {
			actual := fmt.Sprintf(verb, object)
			if !strings.HasSuffix(actual, "(opaque)") || strings.Contains(actual, f.manager.path) {
				t.Fatal("opaque format disclosed original state")
			}
		}
	}
	if json.Unmarshal([]byte(`{}`), &copiedObservation) == nil || json.Unmarshal([]byte(`{}`), &copiedFence) == nil {
		t.Fatal("imported observation or fence accepted")
	}
	other, err := settingscommit.NewForPool(t.Context(), f.pool, f.objects)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	foreign, err := other.TryProgressFileHold(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if _, err := f.security.BindBridgePolicy(t.Context(), observation, foreign); err == nil {
		t.Fatal("foreign coordinator hold accepted")
	}
	copyHold := *hold
	if _, err := f.security.BindBridgePolicy(t.Context(), observation, &copyHold); err == nil {
		t.Fatal("copied hold accepted")
	}
}

func TestBridgePolicyOriginalDatabaseAndPrivateOverride(t *testing.T) {
	for _, kind := range []string{"wrong-domain", "missing-domain", "override", "malformed-with-override"} {
		t.Run(kind, func(t *testing.T) {
			f := securityPG(t)
			originalURL := f.settings.Database.URL
			if kind == "override" || kind == "malformed-with-override" {
				if err := f.security.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				f.security, err = InstallManagedSecurity(f.manager, f.coordinator, originalURL)
				if err != nil {
					t.Fatal(err)
				}
			}
			raw, err := os.ReadFile(f.manager.path)
			if err != nil {
				t.Fatal(err)
			}
			var object map[string]json.RawMessage
			if json.Unmarshal(raw, &object) != nil {
				t.Fatal("decode own settings")
			}
			switch kind {
			case "wrong-domain":
				object["database"] = json.RawMessage(`{"url":"postgresql:///synthetic_other?host=/synthetic"}`)
			case "malformed-with-override":
				object["database"] = json.RawMessage(`null`)
			default:
				delete(object, "database")
			}
			raw, err = json.Marshal(object)
			if err != nil || os.WriteFile(f.manager.path, raw, 0600) != nil {
				t.Fatal("write synthetic external settings change")
			}
			observation, err := f.security.CaptureBridgePolicy(t.Context())
			if kind != "override" {
				if err == nil || observation != nil {
					t.Fatal("unproven effective original DB accepted")
				}
				return
			}
			if err != nil {
				t.Fatal("actual immutable original override lost", err)
			}
			hold, err := f.coordinator.TryProgressFileHold(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer hold.Close()
			fence, err := f.security.BindBridgePolicy(t.Context(), observation, hold)
			if err != nil {
				t.Fatal(err)
			}
			if values, err := fence.Values(); err != nil || !values.EnforceProfilePins {
				t.Fatal("private original override did not bind actual file data")
			}
		})
	}
}

func TestBridgePolicyNamedFileAndWholeVersionDrift(t *testing.T) {
	for _, kind := range []string{"replace", "same-inode", "unknown-field"} {
		t.Run(kind, func(t *testing.T) {
			f := securityPG(t)
			observation, err := f.security.CaptureBridgePolicy(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(f.manager.path)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "replace" {
				if err := os.Rename(f.manager.path, f.manager.path+".original"); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unknown-field" {
				raw = bytes.TrimSpace(raw)
				raw = append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"syntheticUnknown":"changed"}`)...)
			} else {
				raw = append(raw, ' ')
			}
			if err := os.WriteFile(f.manager.path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			hold, err := f.coordinator.TryProgressFileHold(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer hold.Close()
			if _, err := f.security.BindBridgePolicy(t.Context(), observation, hold); err == nil {
				t.Fatal("original file/version/digest drift was adopted")
			}
		})
	}
}

func TestBridgePolicyStrictSharedJSONShape(t *testing.T) {
	good := `{"security":{"enforceProfilePins":false},"jellyfinBridge":{"allowPinLogin":false,"enabled":false},"database":{"url":"synthetic"}}`
	cases := map[string]string{
		"missing-security":       `{"jellyfinBridge":{"allowPinLogin":false,"enabled":false}}`,
		"missing-bridge":         `{"security":{"enforceProfilePins":false}}`,
		"null-security":          strings.Replace(good, `{"enforceProfilePins":false}`, `null`, 1),
		"null-bridge":            strings.Replace(good, `{"allowPinLogin":false,"enabled":false}`, `null`, 1),
		"security-alias":         strings.Replace(good, `"security"`, `"Security"`, 1),
		"bridge-alias":           strings.Replace(good, `"jellyfinBridge"`, `"JellyfinBridge"`, 1),
		"database-alias":         strings.Replace(good, `"database"`, `"Database"`, 1),
		"url-alias":              strings.Replace(good, `"url"`, `"URL"`, 1),
		"root-duplicate":         strings.Replace(good, `{"security":`, `{"security":{},"security":`, 1),
		"escaped-duplicate":      strings.Replace(good, `"enabled":false`, `"enabled":false,"enabl\u0065d":true`, 1),
		"conflicting-case-alias": strings.Replace(good, `"enabled":false`, `"enabled":false,"Enabled":true`, 1),
		"trailing-json":          good + ` {}`,
		"malformed":              good[:len(good)-1],
		"database-null":          strings.Replace(good, `{"url":"synthetic"}`, `null`, 1),
		"url-null":               strings.Replace(good, `"synthetic"`, `null`, 1),
		"invalid-utf8":           good[:len(good)-1] + ",\"other\":\"" + string([]byte{0xff}) + "\"}",
		"depth-bound":            good[:len(good)-1] + `,"other":` + strings.Repeat(`[`, 65) + `0` + strings.Repeat(`]`, 65) + `}`,
		"size-bound":             good + strings.Repeat(" ", managedSnapshotLimit),
	}
	for _, flag := range []string{"enforceProfilePins", "allowPinLogin", "enabled"} {
		for _, invalid := range []string{"null", `"false"`, `1`, `[]`, `{}`} {
			cases[flag+"-"+invalid] = strings.Replace(good, `"`+flag+`":false`, `"`+flag+`":`+invalid, 1)
		}
		cases[flag+"-missing"] = strings.Replace(strings.Replace(good, `"`+flag+`":false,`, "", 1), `,"`+flag+`":false`, "", 1)
		if flag == "enforceProfilePins" {
			cases[flag+"-missing"] = strings.Replace(good, `"enforceProfilePins":false`, "", 1)
		}
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeBridgeLoginPolicy([]byte(raw)); err == nil {
				t.Fatal("malformed or ambiguous shared policy accepted")
			}
		})
	}
	if actual, err := decodeBridgeLoginPolicy([]byte(good)); err != nil || actual.values != (BridgeLoginPolicyValues{}) {
		t.Fatal("explicit three false values treated as missing")
	}
}

func TestBridgePolicyActualReaderRejectsRawPathsAndNonregularFiles(t *testing.T) {
	raw := []byte(`{"security":{"enforceProfilePins":true},"jellyfinBridge":{"allowPinLogin":false,"enabled":true}}`)
	directory := t.TempDir()
	path := filepath.Join(directory, "settings.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(directory, filepath.Join(directory, "alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, filepath.Join(directory, "linked.json")); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mkfifo(filepath.Join(directory, "fifo"), 0600); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"empty": "", "trailing-separator": path + "/", "trailing-dot": path + "/.", "traversal": directory + "/child/../settings.json", "symlink-parent": directory + "/alias/settings.json", "symlink-file": filepath.Join(directory, "linked.json"), "directory": directory, "missing": filepath.Join(directory, "absent.json"), "fifo-without-writer": filepath.Join(directory, "fifo"), "oversize-path": strings.Repeat("x", 4097),
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			result := make(chan error, 1)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			go func() { _, err := readOriginalBridgeLoginPolicy(ctx, NewManager(path)); result <- err }()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("raw path normalized or nonregular file accepted")
				}
			case <-ctx.Done():
				t.Fatal("bounded reader blocked on invalid original file")
			}
		})
	}
	t.Chdir(directory)
	actual, err := readOriginalBridgeLoginPolicy(t.Context(), NewManager("./settings.json"))
	if err != nil || actual.values != (BridgeLoginPolicyValues{true, false, true}) {
		t.Fatal("ordinary explicit relative path was rejected", err)
	}
}

func TestBridgePolicyActualCaptureRejectsMalformedFileWithoutFallback(t *testing.T) {
	f := securityPG(t)
	raw, err := os.ReadFile(f.manager.path)
	if err != nil {
		t.Fatal(err)
	}
	malformed := bytes.Replace(raw, []byte(`"allowPinLogin": false`), []byte(`"allowPinLogin": null`), 1)
	if bytes.Equal(raw, malformed) {
		t.Fatal("own actual serialized bridge flag was not replaced")
	}
	if err := os.WriteFile(f.manager.path, malformed, 0600); err != nil {
		t.Fatal(err)
	}
	if value, err := f.security.CaptureBridgePolicy(t.Context()); err == nil || value != nil {
		t.Fatal("invalid original bridge flag used defaults/cache")
	}
	after, err := os.ReadFile(f.manager.path)
	if err != nil || !bytes.Equal(after, malformed) {
		t.Fatal("policy observation rewrote or defaulted malformed original settings")
	}
}

func TestBridgePolicyActualSQLAndNativeHoldsKeepOriginalData(t *testing.T) {
	f := securityPG(t)
	observation, err := f.security.CaptureBridgePolicy(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hold, err := f.coordinator.TryProgressFileHold(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	// This is a genuine synthetic PG shared lock, but config still exposes DATA,
	// never a proof that a later issuer obtained this exact transaction itself.
	connection, err := pgx.ConnectConfig(t.Context(), f.pool.Config().ConnConfig.Copy())
	if err != nil {
		t.Fatal("open own synthetic transaction connection", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := connection.Close(cleanup); err != nil {
			t.Error("close own synthetic PG connection", err)
		}
	}()
	tx, err := connection.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := tx.Rollback(cleanup); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Error("rollback own synthetic transaction", err)
		}
	}()
	if _, err := tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock_shared($1::integer,$2::integer)`, settingscommit.Namespace, settingscommit.Resource); err != nil {
		t.Fatal(err)
	}
	fence, err := f.security.BindBridgePolicy(t.Context(), observation, hold)
	if err != nil {
		t.Fatal(err)
	}
	if values, err := fence.Values(); err != nil || !values.EnforceProfilePins {
		t.Fatal("original shared hold did not retain policy data")
	}
	if err := hold.Close(); err != nil {
		t.Fatal(err)
	}
	changed := f.settings
	changed.JellyfinBridge.Enabled = true
	if err := f.manager.Save(changed); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("actual still-owned SQL shared lock did not exclude settings Save", err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.manager.Save(changed); err != nil {
		t.Fatal("exact original lock retirement did not permit Save", err)
	}
}
