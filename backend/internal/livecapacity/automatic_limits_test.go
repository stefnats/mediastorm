package livecapacity

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func automaticTestReader(t *testing.T, m *Manager) *atomic.Value {
	t.Helper()
	value := &atomic.Value{}
	value.Store(AutomaticLimit{})
	m.SetAutomaticLimitReader(func(string) AutomaticLimit { return value.Load().(AutomaticLimit) })
	return value
}
func automaticFact(limit int) AutomaticLimit {
	return AutomaticLimit{Limit: limit, ExpiresAt: time.Now().Add(45 * time.Second)}
}

func TestAutomaticLimitsPreserveActiveLiveAndDVRAndExpireWithoutIdle(t *testing.T) {
	m := NewManager()
	fact := automaticTestReader(t, m)
	var stops atomic.Int32
	live, _ := m.AcquireOwned("account", "", 3, Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "old-live", Stop: func(context.Context) error { stops.Add(1); return nil }})
	defer live.Release()
	dvr, _ := m.AcquireOwned("account", "", 3, Owner{AccountID: "a", ProfileID: "p", Kind: KindRecording, RecordingID: "recording", Stop: func(context.Context) error { stops.Add(1); return nil }})
	defer dvr.Release()
	fact.Store(automaticFact(1))
	if current, limit := m.Usage("account", 3); current != 2 || limit != 1 {
		t.Fatalf("usage %d/%d", current, limit)
	}
	_, err := m.AcquireWithTakeover(context.Background(), "account", "", 3, Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "new"}, TakeoverPolicy{AccountID: "a", ProfileID: "p", Intent: "play", AllowLive: true, AllowRecording: true, Authorized: func() bool { return true }})
	if !errors.Is(err, ErrLimit) || stops.Load() != 0 || len(m.revoked) != 0 || len(m.confirmations) != 0 || len(m.claims) != 0 {
		t.Fatalf("extra cap caused takeover: %v stops=%d", err, stops.Load())
	}
	if m.buckets["account"].limit != 3 {
		t.Fatal("ephemeral cap contaminated sticky manual limit")
	}
	fact.Store(AutomaticLimit{Limit: 1, ExpiresAt: time.Now().Add(-time.Second)})
	if current, limit := m.Usage("account", 3); current != 2 || limit != 3 {
		t.Fatalf("expired limit stuck on active bucket: %d/%d", current, limit)
	}
	third, err := m.Acquire("account", "", 3)
	if err != nil {
		t.Fatal("manual fallback did not permit third slot", err)
	}
	third.Release()
	fact.Store(AutomaticLimit{})
	if _, limit := m.Usage("account", 3); limit != 3 {
		t.Fatal("opt out did not remove overlay")
	}
}

func TestAutomaticLimitsSharedSlotAndPrivateConversion(t *testing.T) {
	m := NewManager()
	fact := automaticTestReader(t, m)
	fact.Store(automaticFact(2))
	a, err := m.Acquire("account", "shared", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Release()
	b, err := m.Acquire("account", "shared", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Release()
	other, err := m.Acquire("account", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	fact.Store(automaticFact(1))
	c, err := m.Acquire("account", "shared", 0)
	if err != nil {
		t.Fatal("same upstream required a new slot", err)
	}
	c.Release()
	if err := a.MakePrivate(); !errors.Is(err, ErrLimit) {
		t.Fatal("private conversion bypassed automatic cap", err)
	}
	other.Release()
	fact.Store(automaticFact(2))
	if err := a.MakePrivate(); err != nil {
		t.Fatal(err)
	}
	if current, limit := m.Usage("account", 0); current != 2 || limit != 2 {
		t.Fatalf("converted usage %d/%d", current, limit)
	}
	if _, err := m.Acquire("account", "", 0); !errors.Is(err, ErrLimit) {
		t.Fatal("unconfigured manual policy bypassed report")
	}
	fact.Store(AutomaticLimit{})
	extra, err := m.Acquire("account", "", 0)
	if err != nil {
		t.Fatal("manual0 fallback changed", err)
	}
	extra.Release()
}

func TestAutomaticLimitsNeverRaiseManualAndRetainManualTakeover(t *testing.T) {
	m := NewManager()
	fact := automaticTestReader(t, m)
	fact.Store(automaticFact(2))
	var old *Lease
	var stops int
	old, _ = m.AcquireOwned("a", "", 1, Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "old", Stop: func(context.Context) error { stops++; old.Release(); return nil }})
	current, limit := m.Usage("a", 1)
	if current != 1 || limit != 1 {
		t.Fatal("report raised manual ceiling")
	}
	next, err := m.AcquireWithTakeover(context.Background(), "a", "", 1, Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "next"}, TakeoverPolicy{AccountID: "a", ProfileID: "p", Intent: "play", AllowLive: true, Authorized: func() bool { return true }})
	if err != nil || stops != 1 {
		t.Fatalf("manual-only takeover changed: %v stops=%d", err, stops)
	}
	next.Release()
}

func TestAutomaticLimitsPriorRecordingConfirmationCannotStop(t *testing.T) {
	m := NewManager()
	fact := automaticTestReader(t, m)
	var stops atomic.Int32
	for _, id := range []string{"one", "two"} {
		lease, _ := m.AcquireOwned("a", "", 2, Owner{AccountID: "a", ProfileID: "p", Kind: KindRecording, RecordingID: id, Stop: func(context.Context) error { stops.Add(1); return nil }})
		defer lease.Release()
	}
	owner := Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "new"}
	policy := TakeoverPolicy{AccountID: "a", ProfileID: "p", Intent: "play", AllowRecording: true, Authorized: func() bool { return true }}
	_, err := m.AcquireWithTakeover(context.Background(), "a", "", 2, owner, policy)
	var confirmation *ConfirmationRequired
	if !errors.As(err, &confirmation) {
		t.Fatal("manual baseline did not offer confirmation", err)
	}
	fact.Store(automaticFact(1))
	policy.Confirmation = confirmation.Token
	_, err = m.AcquireWithTakeover(context.Background(), "a", "", 2, owner, policy)
	if !errors.Is(err, ErrLimit) || stops.Load() != 0 || len(m.draining) != 0 || len(m.revoked) != 0 {
		t.Fatalf("old manual confirmation bypassed new cap: %v", err)
	}
}

func TestAutomaticLimitsRecheckAfterAuthorizationBeforeStop(t *testing.T) {
	for _, changeAt := range []int{1, 2} {
		t.Run(map[int]string{1: "before-selection", 2: "before-stop"}[changeAt], func(t *testing.T) {
			m := NewManager()
			fact := automaticTestReader(t, m)
			var stops int
			var calls int
			var old *Lease
			old, _ = m.AcquireOwned("a", "", 2, Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "old", Stop: func(context.Context) error { stops++; old.Release(); return nil }})
			defer old.Release()
			another, _ := m.AcquireOwned("a", "", 2, Owner{AccountID: "other", ProfileID: "other", Kind: KindLive})
			defer another.Release()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := m.AcquireWithTakeover(ctx, "a", "", 2, Owner{AccountID: "a", ProfileID: "p", Kind: KindLive, RevocationKey: "new"}, TakeoverPolicy{AccountID: "a", ProfileID: "p", Intent: "play", AllowLive: true, Authorized: func() bool {
				calls++
				if calls == changeAt {
					fact.Store(automaticFact(1))
				}
				return true
			}})
			if !errors.Is(err, ErrLimit) || stops != 0 || len(m.draining) != 0 || len(m.revoked) != 0 || len(m.claims) != 0 {
				t.Fatalf("late report mutated active owners: %v stops=%d calls=%d", err, stops, calls)
			}
		})
	}
}

func TestAutomaticLimitsRejectInvalidFactsAndKeepAccountsSeparate(t *testing.T) {
	m := NewManager()
	fact := automaticTestReader(t, m)
	for _, value := range []AutomaticLimit{{}, {Limit: -1, ExpiresAt: time.Now().Add(time.Second)}, {Limit: 1_000_001, ExpiresAt: time.Now().Add(time.Second)}, {Limit: 1}, {Limit: 1, ExpiresAt: time.Now().Add(-time.Second)}, {Limit: 1, ExpiresAt: time.Now().Add(time.Hour)}} {
		fact.Store(value)
		if _, limit := m.Usage("a", 4); limit != 4 {
			t.Fatalf("invalid fact changed policy %+v", value)
		}
	}
	m.SetAutomaticLimitReader(func(bucket string) AutomaticLimit {
		if bucket == "a" {
			return automaticFact(1)
		}
		return AutomaticLimit{}
	})
	a, _ := m.Acquire("a", "", 4)
	defer a.Release()
	if _, err := m.Acquire("a", "", 4); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	b, err := m.Acquire("b", "", 4)
	if err != nil {
		t.Fatal(err)
	}
	b.Release()
	m.SetAutomaticLimitReader(nil)
	if _, limit := m.Usage("a", 4); limit != 4 {
		t.Fatal("nil port changed manual fallback")
	}
}
