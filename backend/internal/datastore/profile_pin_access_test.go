package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type pinAccessFixture struct {
	*unlockPGFixture
	reader *ProfilePinAccessReader
}

func accessFixture(t *testing.T) *pinAccessFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL PIN access fixture required")
	}
	t.Setenv("PROFILE_UNLOCK_STATE_TEST_DATABASE_URL", raw)
	f := unlockFixture(t)
	return &pinAccessFixture{unlockPGFixture: f, reader: NewProfilePinAccessReader(f.ds)}
}
func (f *pinAccessFixture) read(t *testing.T, target string, tokens []string) *ProfilePinAccessState {
	t.Helper()
	state, err := f.reader.Read(f.request(t.Context()), target, tokens)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(state.Close)
	return state
}
func (f *pinAccessFixture) outcome(t *testing.T, target string, tokens []string) ProfilePinAccessOutcome {
	t.Helper()
	state := f.read(t, target, tokens)
	outcome, err := f.reader.Check(state)
	if err != nil {
		t.Fatal(err)
	}
	return outcome
}
func TestPGPinAccessCurrentTypedOutcomes(t *testing.T) {
	for _, mode := range []string{"unlocked", "locked", "no PIN", "foreign", "missing", "invalid token", "wrong fingerprint", "wrong session grant", "expired grant", "empty grants"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			target := "target"
			tokens := []string{f.token}
			want := ProfilePinUnlocked
			switch mode {
			case "locked":
				tokens = nil
				want = ProfilePinLocked
			case "no PIN":
				f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
				tokens = nil
				want = ProfilePinNoPin
			case "foreign":
				target = "other-target"
				want = ProfilePinNotFound
			case "missing":
				target = "missing-target"
				want = ProfilePinNotFound
			case "invalid token":
				tokens = []string{"not a credential"}
				want = ProfilePinLocked
			case "wrong fingerprint":
				f.exec(t, `UPDATE profile_unlocks SET pin_fingerprint='invalid'`)
				want = ProfilePinLocked
			case "wrong session grant":
				f.exec(t, `UPDATE profile_unlocks SET session_hash=repeat('0',64)`)
				want = ProfilePinLocked
			case "expired grant":
				f.exec(t, `UPDATE profile_unlocks SET expires_at=clock_timestamp()-interval '1 second'`)
				want = ProfilePinLocked
			case "empty grants":
				f.exec(t, `DELETE FROM profile_unlocks`)
				want = ProfilePinLocked
			}
			if got := f.outcome(t, target, tokens); got != want {
				t.Fatalf("typed classification got %d want %d", got, want)
			}
		})
	}
}
func TestPGPinAccessOriginalOwnedSnapshotRetiresOnChanges(t *testing.T) {
	cases := map[string]string{
		"no PIN to PIN":            `UPDATE users SET pin_hash='replacement' WHERE id='target'`,
		"owned profile reassigned": `UPDATE users SET account_id='other' WHERE id='target'`,
		"owned profile missing":    `DELETE FROM users WHERE id='target'`,
		"profile row ABA":          `UPDATE users SET pin_hash=pin_hash WHERE id='target'`,
		"actor role changed":       `UPDATE accounts SET is_master=true WHERE id='actor'`,
		"actor password changed":   `UPDATE accounts SET password_hash='replacement' WHERE id='actor'`,
		"actor expired":            `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='actor'`,
		"session revoked":          `DELETE FROM sessions`,
		"session rebound":          `UPDATE sessions SET token='SYNTHETIC_REPLACEMENT_SESSION'`,
		"session client changed":   `UPDATE sessions SET client_id='replacement'`,
		"session row ABA":          `UPDATE sessions SET token=token`,
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			f := accessFixture(t)
			f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
			state := f.read(t, "target", nil)
			f.exec(t, sql)
			got, err := f.reader.Check(state)
			if got != ProfilePinUnavailable || err == nil {
				t.Fatalf("changed owned snapshot allowed: %d %v", got, err)
			}
		})
	}
}
func TestPGPinAccessGenerationsAndGrantABA(t *testing.T) {
	for _, mode := range []string{"noPIN profile empty revoke", "noPIN session empty revoke", "grant revoke recreate", "grant equal update", "grant deleted", "grant cleared PIN"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			tokens := []string{f.token}
			if strings.HasPrefix(mode, "noPIN") {
				f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target';DELETE FROM profile_unlocks`)
				tokens = nil
			}
			state := f.read(t, "target", tokens)
			switch mode {
			case "noPIN profile empty revoke":
				if err := f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target"); err != nil {
					t.Fatal(err)
				}
			case "noPIN session empty revoke":
				if err := f.ds.ProfileUnlocks().DeleteBySession(t.Context(), f.grant.SessionHash); err != nil {
					t.Fatal(err)
				}
			case "grant revoke recreate":
				if err := f.ds.ProfileUnlocks().DeleteByProfile(t.Context(), "target"); err != nil {
					t.Fatal(err)
				}
				f.create(t)
			case "grant equal update":
				f.exec(t, `UPDATE profile_unlocks SET token_hash=token_hash`)
			case "grant deleted":
				f.exec(t, `DELETE FROM profile_unlocks`)
			case "grant cleared PIN":
				f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
			}
			got, err := f.reader.Check(state)
			if got != ProfilePinUnavailable || err == nil {
				t.Fatal("row/generation change reused old access snapshot")
			}
		})
	}
}
func TestPGPinAccessNotFoundPrivacyAndNoLatePromotion(t *testing.T) {
	for _, target := range []string{"missing-target", "other-target"} {
		t.Run(target, func(t *testing.T) {
			f := accessFixture(t)
			state := f.read(t, target, []string{f.token})
			if target == "missing-target" {
				f.exec(t, `INSERT INTO users(id,account_id,pin_hash)VALUES('missing-target','actor','')`)
			} else {
				f.exec(t, `UPDATE users SET account_id='actor',pin_hash='' WHERE id='other-target'`)
			}
			got, err := f.reader.Check(state)
			if got != ProfilePinNotFound || err != nil {
				t.Fatal("original foreign/missing target revealed state or became allowed")
			}
			if got = f.outcome(t, target, nil); got != ProfilePinNoPin {
				t.Fatal("fresh currently owned noPIN read denied")
			}
		})
	}
}
func TestPGPinAccessOriginalFrameAndCurrentActualRole(t *testing.T) {
	for _, mode := range []string{"normalized promoted master", "unverified promoted role", "stale master cache", "BindClient mismatch", "scoped session"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			switch mode {
			case "normalized promoted master":
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.session.IsMaster = true
			case "unverified promoted role":
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
			case "stale master cache":
				f.exec(t, `UPDATE sessions SET is_master=true`)
				f.session.IsMaster = true
			case "BindClient mismatch":
				f.exec(t, `UPDATE sessions SET client_id='newly-bound-client'`)
			case "scoped session":
				f.session.Scope = "stream"
				f.session.ScopeResource = "synthetic-resource"
			}
			state, err := f.reader.Read(f.request(t.Context()), "target", []string{f.token})
			if mode == "normalized promoted master" {
				if err != nil {
					t.Fatal(err)
				}
				defer state.Close()
				got, err := f.reader.Check(state)
				if got != ProfilePinUnlocked || err != nil {
					t.Fatal("real normalized actor denied by cached session role")
				}
			} else if state != nil || err == nil {
				t.Fatal("unverified or changed original auth frame repaired")
			}
		})
	}
}
func TestPGPinAccessOpaqueIdentityReplayAndLimitsBeforeIO(t *testing.T) {
	for _, mode := range []string{"zero", "copy", "closed", "other reader", "other store", "reader copy", "replay"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			state := f.read(t, "target", []string{f.token})
			reader := f.reader
			switch mode {
			case "zero":
				state = &ProfilePinAccessState{}
			case "copy":
				copy := *state
				state = &copy
			case "closed":
				state.Close()
			case "other reader":
				reader = NewProfilePinAccessReader(f.ds)
			case "other store":
				reader = NewProfilePinAccessReader(&DataStore{pool: f.ds.pool})
			case "reader copy":
				copy := *reader
				reader = &copy
			case "replay":
				if _, err := reader.Check(state); err != nil {
					t.Fatal(err)
				}
			}
			before := f.trace.calls.Load()
			got, err := reader.Check(state)
			if got != ProfilePinUnavailable || !errors.Is(err, ErrProfilePinAccessUsed) || f.trace.calls.Load() != before {
				t.Fatal("invalid access state performed IO")
			}
		})
	}
	for _, mode := range []string{"too many tokens", "too many bytes"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			tokens := make([]string, maxProfilePinAccessTokens+1)
			if mode == "too many bytes" {
				tokens = []string{strings.Repeat("x", maxProfilePinAccessCredentialBytes+1)}
			}
			before := f.trace.calls.Load()
			state, err := f.reader.Read(f.request(t.Context()), "target", tokens)
			if state != nil || err == nil || f.trace.calls.Load() != before {
				t.Fatal("over-budget credentials performed IO")
			}
		})
	}
}
func TestPGPinAccessFormattingDedupAndNoPolicyProof(t *testing.T) {
	f := accessFixture(t)
	state := f.read(t, "target", []string{f.token, "invalid", f.token})
	if len(state.state.tokenHashes) != 1 {
		t.Fatal("credential hashes not privately deduplicated")
	}
	for _, value := range []any{f.reader, state} {
		raw, err := json.Marshal(value)
		if err != nil || string(raw) != "null" {
			t.Fatal("opaque access data exposed JSON")
		}
		for _, text := range []string{fmt.Sprintf("%v", value), fmt.Sprintf("%+v", value), fmt.Sprintf("%#v", value)} {
			if strings.Contains(text, f.token) || strings.Contains(text, f.pinHash) || strings.Contains(text, f.session.Token) {
				t.Fatal("opaque access formatting leaked credentials")
			}
		}
	}
	if got, err := f.reader.Check(state); got != ProfilePinUnlocked || err != nil {
		t.Fatal("valid plus invalid presented token OR semantics changed")
	}
}
func TestPGPinAccessUnknownStorageAndInvalidPINNeverAllow(t *testing.T) {
	for _, mode := range []string{"missing revocation reader", "invalid PIN", "missing owner", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			switch mode {
			case "missing revocation reader":
				f.exec(t, `ALTER TABLE profile_unlock_revocations RENAME TO unavailable_revocations`)
			case "invalid PIN":
				f.exec(t, `UPDATE users SET pin_hash='invalid' WHERE id='target'`)
			case "missing owner":
				f.exec(t, `DELETE FROM accounts WHERE id='actor'`)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "cancelled" {
				cancel()
			}
			state, err := f.reader.Read(f.request(ctx), "target", nil)
			if state != nil || err == nil {
				t.Fatal("unknown state allowed or downgraded")
			}
		})
	}
}
func TestPGPinAccessCancellationBeforeAndDuringFinalRead(t *testing.T) {
	for _, mode := range []string{"before final IO", "blocked final SQL"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			state, err := f.reader.Read(f.request(ctx), "target", []string{f.token})
			if err != nil {
				t.Fatal(err)
			}
			defer state.Close()
			if mode == "before final IO" {
				cancel()
				before := f.trace.calls.Load()
				got, err := f.reader.Check(state)
				if got != ProfilePinUnavailable || !errors.Is(err, context.Canceled) || f.trace.calls.Load() != before {
					t.Fatal("cancelled final state read performed IO")
				}
				return
			}
			tx, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if _, err = tx.Exec(t.Context(), `LOCK TABLE users IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			type result struct {
				outcome ProfilePinAccessOutcome
				err     error
			}
			done := make(chan result, 1)
			go func() { got, err := f.reader.Check(state); done <- result{got, err} }()
			f.waitBlocked(t)
			cancel()
			got := <-done
			if got.outcome != ProfilePinUnavailable || !errors.Is(got.err, context.Canceled) {
				t.Fatal("blocked current reader ignored original cancellation")
			}
		})
	}
}
func TestPGPinAccessOriginalExpiryAndReadOnly(t *testing.T) {
	f := accessFixture(t)
	f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
	before := f.trace.calls.Load()
	state := f.read(t, "target", nil)
	if f.trace.calls.Load() != before+1 {
		t.Fatal("snapshot did not use one consistent query")
	}
	deadline, _ := state.state.ctx.Deadline()
	if time.Until(deadline) <= 0 || time.Until(deadline) > 30*time.Second {
		t.Fatal("current state attempt deadline renewed")
	}
	if got, err := f.reader.Check(state); got != ProfilePinNoPin || err != nil {
		t.Fatal("current noPIN unavailable")
	}
	if f.generation(t, "profile", "target") != 0 || f.generation(t, "session", f.grant.SessionHash) != 0 || f.count(t) != 1 {
		t.Fatal("read-only access changed storage")
	}
}

func TestPGPinAccessEssentialFieldsNeverDefaultToAllowance(t *testing.T) {
	for _, field := range []string{"users.pin_hash", "sessions.scope", "sessions.scope_resource", "sessions.client_id", "accounts.is_master", "accounts.expires_at", "profile_unlocks.created_at", "profile_unlocks.expires_at"} {
		t.Run(field, func(t *testing.T) {
			f := accessFixture(t)
			if !strings.HasPrefix(field, "profile_unlocks.") {
				f.exec(t, `UPDATE users SET pin_hash='' WHERE id='target'`)
			}
			pair := strings.Split(field, ".")
			// Only constant test-owned table/column names, never caller SQL.
			f.exec(t, "ALTER TABLE "+pair[0]+" DROP COLUMN "+pair[1])
			state, err := f.reader.Read(f.request(t.Context()), "target", []string{f.token})
			if state != nil || err == nil {
				t.Fatal("missing essential field became a default allowance")
			}
		})
	}
}
func TestPGPinAccessOriginalNotFoundSurvivesTargetLocalErrors(t *testing.T) {
	for _, mode := range []string{"missing then invalid PIN", "foreign then invalid PIN", "missing then expired owner", "missing then absent owner"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			target := "missing-target"
			if mode == "foreign then invalid PIN" {
				target = "other-target"
			}
			if strings.Contains(mode, "owner") {
				f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor'`)
				f.session.IsMaster = true
			}
			state := f.read(t, target, nil)
			switch mode {
			case "missing then invalid PIN":
				f.exec(t, `INSERT INTO users(id,account_id,pin_hash)VALUES('missing-target','actor','invalid')`)
			case "foreign then invalid PIN":
				f.exec(t, `UPDATE users SET account_id='actor',pin_hash='invalid' WHERE id='other-target'`)
			case "missing then expired owner":
				f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id='other';INSERT INTO users(id,account_id,pin_hash)VALUES('missing-target','other','')`)
			case "missing then absent owner":
				f.exec(t, `INSERT INTO users(id,account_id,pin_hash)VALUES('missing-target','absent-owner','')`)
			}
			got, err := f.reader.Check(state)
			if got != ProfilePinNotFound || err != nil {
				t.Fatal("unchanged original auth frame disclosed target-local error")
			}
		})
	}
}
func TestPGPinAccessOriginalNotFoundDoesNotHideStorageOrActorFailure(t *testing.T) {
	for _, mode := range []string{"storage", "actor"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			state := f.read(t, "missing-target", nil)
			if mode == "storage" {
				f.exec(t, `ALTER TABLE profile_unlock_revocations RENAME TO unavailable_revocations`)
			} else {
				f.exec(t, `UPDATE accounts SET password_hash='replacement' WHERE id='actor'`)
			}
			got, err := f.reader.Check(state)
			if got != ProfilePinUnavailable || err == nil {
				t.Fatal("notFound classification hid storage/auth-frame retirement")
			}
		})
	}
}
func TestPGPinAccessTwoValidTokensUseBoundedLaterExpiry(t *testing.T) {
	for _, mode := range []string{"later valid grant", "owner cap", "session cap", "attempt cap"} {
		t.Run(mode, func(t *testing.T) {
			f := accessFixture(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			first := f.grant
			first.ExpiresAt = now.Add(5 * time.Second)
			if err := f.ds.ProfileUnlocks().Create(t.Context(), &first); err != nil {
				t.Fatal(err)
			}
			secondToken := "pu_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
			second := first
			second.TokenHash = profileUnlockDigest(secondToken)
			second.ExpiresAt = now.Add(20 * time.Second)
			if mode == "attempt cap" {
				second.ExpiresAt = now.Add(time.Minute)
			}
			if err := f.ds.ProfileUnlocks().Create(t.Context(), &second); err != nil {
				t.Fatal(err)
			}
			expected := second.ExpiresAt
			if mode == "owner cap" {
				expected = now.Add(8 * time.Second)
				f.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, expected)
			}
			if mode == "session cap" {
				expected = now.Add(9 * time.Second)
				f.session.ExpiresAt = expected
				f.exec(t, `UPDATE sessions SET expires_at=$1`, expected)
			}
			state := f.read(t, "target", []string{f.token, secondToken})
			deadline, _ := state.state.ctx.Deadline()
			if mode == "attempt cap" {
				if deadline.After(now.Add(30*time.Second+time.Second)) || !deadline.Before(second.ExpiresAt) {
					t.Fatal("later grant extended original attempt budget")
				}
			} else if !deadline.Equal(expected) {
				t.Fatal("two-token OR expiry used expired-shorter or unbounded deadline")
			}
			if got, err := f.reader.Check(state); got != ProfilePinUnlocked || err != nil {
				t.Fatal("current later-valid OR grant denied")
			}
		})
	}
}
