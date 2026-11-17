package datastore

import (
	"context"
	"errors"
	"testing"
)

func recoveryPairFixture(t *testing.T) (*settingsDSFixture, context.Context, *ProfileRecoverySnapshot, *ProfileRecoverySlot, *ProfileRecoverySlot) {
	t.Helper()
	f := newSettingsDS(t)
	f.user(t, "a")
	f.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES('a','{"display":{"simpleMode":false}}')`)
	ctx := t.Context()
	read, users, settings := recoveryReadSlots(t, f.ds, ctx, "a")
	snapshot, e := f.ds.ReadProfileRecovery(read)
	if e != nil {
		t.Fatal(e)
	}
	return f, ctx, snapshot, users, settings
}
func TestProfileRecoveryPairOriginalOrderIdentityAndOnce(t *testing.T) {
	f, ctx, snapshot, users, settings := recoveryPairFixture(t)
	if _, _, _, e := f.ds.BeginUserSettingsRecoveryLease(ctx, settings, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("settings lease acquired without Users lock-order proof")
	}
	up, _, _, e := f.ds.BeginUsersRecoveryLease(ctx, users, snapshot)
	if e != nil {
		t.Fatal(e)
	}
	defer up.Close()
	sp, _, _, e := f.ds.BeginUserSettingsRecoveryLease(ctx, settings, snapshot)
	if e != nil {
		t.Fatal(e)
	}
	defer sp.Close()
	if _, _, e = f.ds.TakeUsersRecovery(ctx, users, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("independent Take bypassed lease")
	}
	copy := *up
	if e = f.ds.TakeProfileRecoveryPair(ctx, &copy, sp, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("copied proof accepted")
	}
	wrong := &DataStore{pool: f.ds.pool}
	if e = wrong.TakeProfileRecoveryPair(ctx, up, sp, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("wrong store paired")
	}
	if e = f.ds.TakeProfileRecoveryPair(context.Background(), up, sp, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("replacement context paired")
	}
	if users.used || settings.used {
		t.Fatal("rejected pair consumed one slot")
	}
	if e = f.ds.TakeProfileRecoveryPair(ctx, up, sp, snapshot); e != nil {
		t.Fatal(e)
	}
	if !users.used || !settings.used || !up.Paired() || !sp.Paired() {
		t.Fatal("pair did not consume both slots")
	}
	if e = f.ds.TakeProfileRecoveryPair(ctx, up, sp, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
		t.Fatal("pair replay")
	}
}
func TestProfileRecoveryPairCopyFailureBeforeConsumption(t *testing.T) {
	f, ctx, snapshot, users, settings := recoveryPairFixture(t)
	before := f.raw(t, "a")
	stamp := f.generation(t, "a")
	up, _, _, e := f.ds.BeginUsersRecoveryLease(ctx, users, snapshot)
	if e != nil {
		t.Fatal(e)
	}
	defer up.Close()
	// A deliberately invalid in-memory RawMessage exercises the fallible copy
	// boundary. PostgreSQL itself never stores malformed JSONB.
	snapshot.settings.Settings.SportsPreferences = []byte("{")
	if proof, _, _, e := f.ds.BeginUserSettingsRecoveryLease(ctx, settings, snapshot); proof != nil || !errors.Is(e, ErrProfileRecoveryUnavailable) {
		t.Fatal("copy failure was not rejected before pair", e)
	}
	if users.used || settings.used || up.Paired() || before != f.raw(t, "a") || stamp != f.generation(t, "a") {
		t.Fatal("copy failure consumed or changed state")
	}
}
func TestProfileRecoveryPairRetireLinearizesWithConsumption(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before", false: "after"}[before], func(t *testing.T) {
			f, ctx, snapshot, users, settings := recoveryPairFixture(t)
			up, _, _, e := f.ds.BeginUsersRecoveryLease(ctx, users, snapshot)
			if e != nil {
				t.Fatal(e)
			}
			defer up.Close()
			sp, _, _, e := f.ds.BeginUserSettingsRecoveryLease(ctx, settings, snapshot)
			if e != nil {
				t.Fatal(e)
			}
			defer sp.Close()
			if before {
				if up.Retire() {
					t.Fatal("unconsumed lease reported completion")
				}
				if e = f.ds.TakeProfileRecoveryPair(ctx, up, sp, snapshot); !errors.Is(e, ErrProfileRecoveryChanged) {
					t.Fatal("retired owner raced past consumption")
				}
				if users.used || settings.used {
					t.Fatal("rejected retirement consumed half")
				}
			} else {
				if e = f.ds.TakeProfileRecoveryPair(ctx, up, sp, snapshot); e != nil {
					t.Fatal(e)
				}
				snapshot.Close()
				if !up.Retire() || !sp.Retire() || !up.Paired() || !sp.Paired() {
					t.Fatal("post-consumption close erased historical completion")
				}
			}
		})
	}
}

func TestProfileRecoveryPairConcurrentRetireNeverConsumesHalf(t *testing.T) {
	f, ctx, snapshot, users, settings := recoveryPairFixture(t)
	up, _, _, e := f.ds.BeginUsersRecoveryLease(ctx, users, snapshot)
	if e != nil {
		t.Fatal(e)
	}
	defer up.Close()
	sp, _, _, e := f.ds.BeginUserSettingsRecoveryLease(ctx, settings, snapshot)
	if e != nil {
		t.Fatal(e)
	}
	defer sp.Close()
	start := make(chan struct{})
	taken := make(chan error, 1)
	retired := make(chan bool, 1)
	go func() { <-start; taken <- f.ds.TakeProfileRecoveryPair(ctx, up, sp, snapshot) }()
	go func() { <-start; retired <- up.Retire() }()
	close(start)
	e = <-taken
	wasPaired := <-retired
	if users.used != settings.used || up.Paired() != sp.Paired() {
		t.Fatal("concurrent retirement consumed only one participant")
	}
	if e == nil {
		if !wasPaired || !users.used {
			t.Fatal("retirement missed accepted pair")
		}
	} else if !errors.Is(e, ErrProfileRecoveryChanged) || wasPaired || users.used {
		t.Fatal("retired pair produced inconsistent result", e)
	}
}
