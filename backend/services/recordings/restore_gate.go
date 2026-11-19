package recordings

import (
	"context"
	"sync"
)

var captureRestoreTurn = func() chan struct{} { turn := make(chan struct{}, 1); turn <- struct{}{}; return turn }()
var captureOwnersMu sync.Mutex
var captureOwners = map[*captureOwner]struct{}{}
var captureRestoring bool
var captureRestoreEpoch uint64
var captureRestoreDone chan struct{}

func derivedRestoreStamp() (uint64, bool) {
	captureOwnersMu.Lock()
	defer captureOwnersMu.Unlock()
	return captureRestoreEpoch, !captureRestoring
}

type captureOwner struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func registerCaptureOwner(ctx context.Context, cancel context.CancelFunc) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		captureOwnersMu.Lock()
		if !captureRestoring {
			owner := &captureOwner{cancel: cancel, done: make(chan struct{})}
			captureOwners[owner] = struct{}{}
			captureOwnersMu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() {
					captureOwnersMu.Lock()
					delete(captureOwners, owner)
					close(owner.done)
					captureOwnersMu.Unlock()
				})
			}, nil
		}
		barrier := captureRestoreDone
		captureOwnersMu.Unlock()
		select {
		case <-barrier:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// QuiesceForRestore atomically blocks new owner registration, then cancels and
// joins old owners before backup opens its database transaction. Every wait
// outside the short registry critical section observes the caller's context.
func QuiesceForRestore(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-captureRestoreTurn:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	captureOwnersMu.Lock()
	if err := ctx.Err(); err != nil {
		captureOwnersMu.Unlock()
		captureRestoreTurn <- struct{}{}
		return nil, err
	}
	captureRestoring = true
	captureRestoreEpoch++
	captureRestoreDone = make(chan struct{})
	barrier := captureRestoreDone
	owners := make([]*captureOwner, 0, len(captureOwners))
	for owner := range captureOwners {
		owners = append(owners, owner)
	}
	captureOwnersMu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			captureOwnersMu.Lock()
			captureRestoring = false
			close(barrier)
			captureOwnersMu.Unlock()
			captureRestoreTurn <- struct{}{}
		})
	}
	for _, owner := range owners {
		owner.cancel()
	}
	for _, owner := range owners {
		select {
		case <-owner.done:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
