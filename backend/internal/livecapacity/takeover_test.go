package livecapacity

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testOwner(profile string) Owner {
	return Owner{AccountID: "household", ProfileID: profile, Kind: KindLive, RevocationKey: "attempt-" + profile}
}
func testPolicy() TakeoverPolicy {
	return TakeoverPolicy{AccountID: "household", ProfileID: "requester", Intent: "verified-tune", AllowLive: true, Authorized: func() bool { return true }}
}
func own(t *testing.T, m *Manager, share string, owner Owner) *Lease {
	t.Helper()
	lease, err := m.AcquireOwned("provider", share, 1, owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lease.Release)
	return lease
}
func take(m *Manager, p TakeoverPolicy) (*Lease, error) {
	return m.AcquireWithTakeover(context.Background(), "provider", "replacement", 1, testOwner("requester"), p)
}
func confirm(t *testing.T, m *Manager, p TakeoverPolicy) string {
	t.Helper()
	lease, err := take(m, p)
	var required *ConfirmationRequired
	if lease != nil || !errors.As(err, &required) || required.RecordingCount != 1 || len(required.Token) != 48 {
		t.Fatalf("unexpected confirmation: lease=%v error=%v", lease, err)
	}
	return required.Token
}

func TestTakeoverDefaultsAndUnregisteredConsumersStayProtected(t *testing.T) {
	cases := []struct {
		name   string
		owner  Owner
		policy TakeoverPolicy
	}{
		{"anonymous", Owner{}, testPolicy()},
		{"other-account", Owner{AccountID: "other", ProfileID: "viewer", Kind: KindLive}, testPolicy()},
		{"missing-profile", Owner{AccountID: "household", Kind: KindLive}, testPolicy()},
		{"probe", Owner{AccountID: "household", ProfileID: "viewer", Kind: "probe"}, testPolicy()},
		{"no-right", testOwner("viewer"), TakeoverPolicy{AccountID: "household", ProfileID: "requester", Intent: "tune", Authorized: func() bool { return true }}},
		{"no-authority", testOwner("viewer"), TakeoverPolicy{AccountID: "household", ProfileID: "requester", Intent: "tune", AllowLive: true}},
		{"wrong-actor", testOwner("viewer"), TakeoverPolicy{AccountID: "household", ProfileID: "different", Intent: "tune", AllowLive: true, Authorized: func() bool { return true }}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager()
			var stopped atomic.Bool
			tc.owner.Stop = func(context.Context) error { stopped.Store(true); return nil }
			old := own(t, m, "shared", tc.owner)
			lease, err := take(m, tc.policy)
			if lease != nil || !errors.Is(err, ErrLimit) || stopped.Load() || m.Current("provider") != 1 {
				t.Fatal("protected input was displaced")
			}
			old.Release()
		})
	}
}

func TestTakeoverWaitsForActualReleaseAndReservesTheHandoff(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	stopped := make(chan struct{})
	old.SetStop(func(context.Context) error { close(stopped); return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *Lease, 1)
	errs := make(chan error, 1)
	go func() {
		lease, err := m.AcquireWithTakeover(ctx, "provider", "new", 1, testOwner("requester"), testPolicy())
		done <- lease
		errs <- err
	}()
	<-stopped
	select {
	case <-done:
		t.Fatal("admitted before input closed")
	default:
	}
	if _, err := m.Acquire("provider", "", 1); !errors.Is(err, ErrLimit) {
		t.Fatal("another start stole closing slot")
	}
	if _, err := m.Acquire("provider", "old", 1); !errors.Is(err, ErrLimit) {
		t.Fatal("a new viewer joined the draining input")
	}
	if err := old.MakePrivate(); !errors.Is(err, ErrLimit) {
		t.Fatal("draining input converted to a new connection")
	}
	if lease, err := take(m, testPolicy()); lease != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("concurrent takeover bypassed claim")
	}
	old.Release()
	lease := <-done
	if err := <-errs; err != nil || lease == nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if m.Current("provider") != 1 {
		t.Fatal("handoff did not retain exactly one connection")
	}
}

func TestSharedTakeoverStopsEveryOwnedConsumerBeforeAdmission(t *testing.T) {
	m := NewManager()
	a := own(t, m, "shared", testOwner("one"))
	b := own(t, m, "shared", testOwner("two"))
	var stopped atomic.Int32
	for _, lease := range []*Lease{a, b} {
		lease.SetStop(func(context.Context) error { stopped.Add(1); lease.Release(); return nil })
	}
	fresh, err := take(m, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if stopped.Load() != 2 || m.Current("provider") != 1 {
		t.Fatal("shared input was only partly stopped")
	}
}

func TestSharedForeignOrAnonymousConsumerProtectsWholeConnection(t *testing.T) {
	for _, anonymous := range []bool{true, false} {
		t.Run(map[bool]string{true: "anonymous", false: "foreign"}[anonymous], func(t *testing.T) {
			m := NewManager()
			a := own(t, m, "shared", testOwner("one"))
			a.SetStop(func(context.Context) error { t.Error("owned viewer must not stop"); return nil })
			var b *Lease
			if anonymous {
				b, _ = m.Acquire("provider", "shared", 1)
			} else {
				b = own(t, m, "shared", Owner{AccountID: "foreign", ProfileID: "other", Kind: KindLive, Stop: func(context.Context) error { t.Error("foreign stop"); return nil }})
			}
			defer b.Release()
			fresh, err := take(m, testPolicy())
			if fresh != nil || !errors.Is(err, ErrLimit) {
				t.Fatal("shared protected input selected")
			}
		})
	}
}

func TestRecordingRequiresSeparateRightAndExplicitConfirmation(t *testing.T) {
	m := NewManager()
	owner := testOwner("recorder")
	owner.Kind = KindRecording
	owner.RecordingID = "recording-one"
	old := own(t, m, "", owner)
	var stopped atomic.Int32
	old.SetStop(func(context.Context) error { stopped.Add(1); old.Release(); return nil })
	if lease, err := take(m, testPolicy()); lease != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("live permission cancelled recording")
	}
	p := testPolicy()
	p.AllowLive = false
	p.AllowRecording = true
	token := confirm(t, m, p)
	if stopped.Load() != 0 || m.Current("provider") != 1 {
		t.Fatal("confirmation prompt changed recording")
	}
	p.Confirmation = token
	lease, err := take(m, p)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if stopped.Load() != 1 {
		t.Fatal("confirmed recording was not stopped exactly once")
	}
}

func TestRecordingConfirmationIsBoundToActorIntentAndCurrentMembers(t *testing.T) {
	for _, change := range []string{"account", "profile", "intent", "expired", "shared-member", "released", "replay"} {
		t.Run(change, func(t *testing.T) {
			m := NewManager()
			owner := testOwner("recorder")
			owner.Kind = KindRecording
			owner.RecordingID = "recording-one"
			old := own(t, m, "shared", owner)
			old.SetStop(func(context.Context) error { t.Error("invalid confirmation stopped recording"); return nil })
			p := testPolicy()
			p.AllowRecording = true
			token := confirm(t, m, p)
			p.Confirmation = token
			switch change {
			case "account":
				p.AccountID = "foreign"
			case "profile":
				p.ProfileID = "different"
			case "intent":
				p.Intent = "different-tune"
			case "expired":
				m.mu.Lock()
				c := m.confirmations[token]
				c.expires = time.Now().Add(-time.Second)
				m.confirmations[token] = c
				m.mu.Unlock()
			case "shared-member":
				extra := own(t, m, "shared", testOwner("extra"))
				extra.SetStop(func(context.Context) error { t.Error("new member stopped"); return nil })
			case "released":
				old.Release()
				replacement := own(t, m, "shared", owner)
				replacement.SetStop(func(context.Context) error { t.Error("replacement stopped"); return nil })
			case "replay":
				m.mu.Lock()
				delete(m.confirmations, token)
				m.mu.Unlock()
			}
			lease, err := take(m, p)
			if lease != nil || err == nil {
				t.Fatal("changed confirmation accepted")
			}
			if change != "account" && change != "profile" && !errors.Is(err, ErrConfirmationExpired) {
				t.Fatal(err)
			}
		})
	}
}

func TestOldestLiveInputIsPreferredBeforeARecording(t *testing.T) {
	m := NewManager()
	recordOwner := testOwner("recorder")
	recordOwner.Kind = KindRecording
	recordOwner.RecordingID = "recording"
	recording, err := m.AcquireOwned("provider", "", 3, recordOwner)
	if err != nil {
		t.Fatal(err)
	}
	defer recording.Release()
	recording.SetStop(func(context.Context) error { t.Error("recording selected before live"); return nil })
	oldest, _ := m.AcquireOwned("provider", "", 3, testOwner("older"))
	defer oldest.Release()
	var stopped atomic.Int32
	oldest.SetStop(func(context.Context) error { stopped.Add(1); oldest.Release(); return nil })
	younger, _ := m.AcquireOwned("provider", "", 3, testOwner("younger"))
	defer younger.Release()
	younger.SetStop(func(context.Context) error { t.Error("younger live selected"); return nil })
	p := testPolicy()
	p.AllowRecording = true
	fresh, err := m.AcquireWithTakeover(context.Background(), "provider", "new", 3, testOwner("requester"), p)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if stopped.Load() != 1 {
		t.Fatal("oldest input not selected")
	}
}

func TestCancelledTakeoverKeepsDrainingInputCountedAndDoesNotLeakClaim(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	ctx, cancel := context.WithCancel(context.Background())
	old.SetStop(func(context.Context) error { cancel(); return nil })
	lease, err := m.AcquireWithTakeover(ctx, "provider", "new", 1, testOwner("requester"), testPolicy())
	if lease != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if m.Current("provider") != 1 {
		t.Fatal("cancel freed an open input")
	}
	if _, err := m.Acquire("provider", "old", 1); !errors.Is(err, ErrLimit) {
		t.Fatal("cancel reopened a draining input")
	}
	old.Release()
	fresh, err := m.Acquire("provider", "new", 1)
	if err != nil {
		t.Fatal(err)
	}
	fresh.Release()
	if len(m.claims) != 0 || len(m.draining) != 0 || len(m.owners) != 0 || len(m.buckets) != 0 {
		t.Fatal("handoff leaked state")
	}
}

func TestStopFailureNeverCreatesCapacityAndKeepsOtherInputsIntact(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	failure := errors.New("controlled stop failure")
	old.SetStop(func(context.Context) error { return failure })
	lease, err := take(m, testPolicy())
	if lease != nil || !errors.Is(err, failure) || m.Current("provider") != 1 {
		t.Fatal("stop failure overbooked")
	}
	old.Release()
	if len(m.claims) != 0 || len(m.draining) != 0 {
		t.Fatal("stop failure leaked claim")
	}
}

func TestRevocationDuringShutdownPreventsReplacementAdmission(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	var allowed atomic.Bool
	allowed.Store(true)
	old.SetStop(func(context.Context) error { allowed.Store(false); old.Release(); return nil })
	p := testPolicy()
	p.Authorized = allowed.Load
	lease, err := take(m, p)
	if lease != nil || !errors.Is(err, ErrPermission) || m.Current("provider") != 0 {
		t.Fatal("revoked takeover was admitted")
	}
}

func TestSharedOrdinaryPlayAndBackgroundReservationsNeverNeedTakeover(t *testing.T) {
	m := NewManager()
	old := own(t, m, "same", testOwner("viewer"))
	old.SetStop(func(context.Context) error { t.Error("shared play stopped input"); return nil })
	lease, err := m.AcquireWithTakeover(context.Background(), "provider", "same", 1, testOwner("requester"), TakeoverPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if m.Current("provider") != 1 {
		t.Fatal("shared play opened a second input")
	}
	if _, err := m.AcquireOwned("provider", "other", 1, testOwner("background")); !errors.Is(err, ErrLimit) {
		t.Fatal("background reservation displaced input")
	}
}

func TestConfirmationStorageIsBoundedAndExpiredEntriesAreRemoved(t *testing.T) {
	m := NewManager()
	p := testPolicy()
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for range maxConfirmations + 4 {
		if _, err := m.confirmLocked("provider", 1, nil, p, now); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.confirmations) != maxConfirmations {
		t.Fatal("unbounded confirmations")
	}
	if _, err := m.confirmLocked("provider", 1, nil, p, now.Add(confirmationTTL)); err != nil {
		t.Fatal(err)
	}
	if len(m.confirmations) != 1 {
		t.Fatal("expired confirmations retained")
	}
}

func TestShutdownWaitCannotBeStolenByConcurrentOrdinaryStarts(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	stopped := make(chan struct{})
	allowClose := make(chan struct{})
	old.SetStop(func(context.Context) error { close(stopped); <-allowClose; old.Release(); return nil })
	result := make(chan *Lease, 1)
	errs := make(chan error, 1)
	go func() { l, e := take(m, testPolicy()); result <- l; errs <- e }()
	<-stopped
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			l, e := m.Acquire("provider", "other", 1)
			if e == nil {
				admitted.Add(1)
				l.Release()
			}
		})
	}
	wg.Wait()
	if admitted.Load() != 0 {
		t.Fatal("ordinary start stole handoff")
	}
	close(allowClose)
	fresh := <-result
	if e := <-errs; e != nil {
		t.Fatal(e)
	}
	fresh.Release()
}

func TestAlreadyDrainingInputIsNotSelectedTwice(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	ctx, cancel := context.WithCancel(context.Background())
	var stopped atomic.Int32
	old.SetStop(func(context.Context) error { stopped.Add(1); cancel(); return nil })
	_, _ = m.AcquireWithTakeover(ctx, "provider", "new", 1, testOwner("requester"), testPolicy())
	if lease, err := take(m, testPolicy()); lease != nil || !errors.Is(err, ErrLimit) || stopped.Load() != 1 {
		t.Fatal("draining input was selected again")
	}
}

func TestLoweredLimitDoesNotStopAnInputWithoutMakingRoom(t *testing.T) {
	m := NewManager()
	for i := range 3 {
		l, e := m.AcquireOwned("provider", "", 3, testOwner(string(rune('a'+i))))
		if e != nil {
			t.Fatal(e)
		}
		defer l.Release()
		l.SetStop(func(context.Context) error { t.Error("one stop cannot make room at lowered limit"); return nil })
	}
	if lease, err := take(m, testPolicy()); lease != nil || !errors.Is(err, ErrLimit) || m.Current("provider") != 3 {
		t.Fatal("lowered limit preemption was unsafe")
	}
}

func TestRevokedPlaybackAttemptCannotAutomaticallyTakeItsSlotBack(t *testing.T) {
	m := NewManager()
	old := own(t, m, "old", testOwner("viewer"))
	old.SetStop(func(context.Context) error { old.Release(); return nil })
	replacement, err := take(m, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Release()
	replacement.SetStop(func(context.Context) error { t.Error("automatic retry displaced new viewer"); return nil })
	actor := testOwner("viewer")
	policy := testPolicy()
	policy.ProfileID = "viewer"
	if lease, err := m.AcquireWithTakeover(context.Background(), "provider", "old", 1, actor, policy); lease != nil || !errors.Is(err, ErrTakenOver) {
		t.Fatal("revoked playback reacquired slot")
	}
	replacement.Release()
	if lease, err := m.AcquireOwned("provider", "old", 1, actor); lease != nil || !errors.Is(err, ErrTakenOver) {
		t.Fatal("revoked automatic retry resumed after capacity became free")
	}
	actor.RevocationKey = "new-explicit-play"
	fresh, err := m.AcquireWithTakeover(context.Background(), "provider", "old", 1, actor, policy)
	if err != nil {
		t.Fatal("explicit new Play did not obtain a free slot")
	}
	fresh.Release()
}

func TestPreviouslyAdmittedRetuneNeverDisplacesANewerViewer(t *testing.T) {
	m := NewManager()
	actor := testOwner("requester")
	policy := testPolicy()
	first, err := m.AcquireWithTakeover(context.Background(), "provider", "one", 1, actor, policy)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	newer := own(t, m, "other", testOwner("newer"))
	newer.SetStop(func(context.Context) error { t.Error("automatic retune stopped newer viewer"); return nil })
	if lease, err := take(m, policy); lease != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("retune preempted without a fresh explicit Play")
	}
	newer.Release()
	reused, err := take(m, policy)
	if err != nil {
		t.Fatal("retune could not reuse genuinely free capacity")
	}
	reused.Release()
}

func TestLiveAttemptRemainsAdmittedWhileItsLongRunningInputExists(t *testing.T) {
	m := NewManager()
	first := own(t, m, "old", testOwner("requester"))
	m.mu.Lock()
	m.admitted[firstOwnerKey()] = time.Now().Add(-time.Hour)
	if !m.wasAdmittedLocked(firstOwnerKey(), time.Now()) {
		t.Error("active long-running input lost its admitted identity")
	}
	m.mu.Unlock()
	first.Release()
	m.mu.Lock()
	ok := m.wasAdmittedLocked(firstOwnerKey(), time.Now())
	m.mu.Unlock()
	if !ok {
		t.Fatal("closing a long-running input did not retain its retry boundary")
	}
}
func firstOwnerKey() string { return "attempt-requester" }
