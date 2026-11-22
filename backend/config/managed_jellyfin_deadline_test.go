package config

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A deadline can pass before a parent's timer goroutine publishes Err.
// Reproduce that scheduler window without depending on a real timer race.
type pendingCallerDeadline struct {
	context.Context
	at time.Time
}

func (c pendingCallerDeadline) Deadline() (time.Time, bool) { return c.at, true }

func TestManagedJellyfinExpiredCallerDeadlineDoesNotRetireHeldPlayback(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	download := openManagedHTTPDownload(t, fixture)
	caller := pendingCallerDeadline{Context: t.Context(), at: time.Now().Add(-time.Millisecond)}
	if caller.Err() != nil {
		t.Fatal("fixture already published cancellation")
	}
	if err := fixture.transport.state.failure(caller); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller timeout=%v", err)
	}
	if fixture.transport.Context().Err() != nil || fixture.lease.Check(t.Context()) != nil {
		t.Fatal("expired request retired otherwise healthy playback")
	}
	if n, err := download.Read(make([]byte, 1)); n != 1 || err != nil {
		t.Fatal("held playback stopped after a separate request timed out")
	}
}

func TestManagedJellyfinActualFailureBeforeCallerDeadlineStillRetiresPlayback(t *testing.T) {
	fixture := newManagedHTTPFixture(t, false)
	caller := pendingCallerDeadline{Context: t.Context(), at: time.Now().Add(time.Minute)}
	if err := fixture.transport.state.failure(caller); !errors.Is(err, ErrManagedJellyfinDenied) {
		t.Fatalf("source failure=%v", err)
	}
	if fixture.transport.Context().Err() == nil {
		t.Fatal("actual source failure was mistaken for a future caller timeout")
	}
}
