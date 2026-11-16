package handlers

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"sync"

	"novastream/services/kidsvod"
	"novastream/services/playback"
)

var errKidsVODInput = errors.New("bound VOD input unavailable")

type kidsVODInputKey struct {
	ref                         kidsvod.ResourceRef
	attempt                     playback.AuthorizationStamp
	inputVersion, sourceVersion string
}

func (k kidsVODInputKey) valid() bool {
	return k.ref.ID() != "" && k.attempt.Valid() && k.inputVersion != "" && k.sourceVersion != ""
}

// The expected key is obtained from current private server receipts. It is not
// sufficient to label a newly opened file. No production route constructs this
// context yet; future gates must authorize before their provider open as well.
type kidsVODBoundLease struct {
	lease   *kidsvod.Lease
	key     kidsVODInputKey
	current func(context.Context) (kidsVODInputKey, error)
}

func (b *kidsVODBoundLease) check(ctx context.Context) error {
	if b == nil || b.lease == nil || !b.key.valid() || b.current == nil || ctx.Err() != nil {
		return errKidsVODInput
	}
	if !b.lease.Check(ctx).Allowed() {
		return errKidsVODInput
	}
	key, err := b.current(ctx)
	if err != nil || key != b.key || ctx.Err() != nil || b.lease.Context().Err() != nil {
		return errKidsVODInput
	}
	return nil
}

// Only a trusted integration holding an ALREADY OPEN normal provider input may
// implement this private interface. Its descriptor must come from that actual
// handle/version, not from a requested path, hash or a cached expected receipt.
// This packet intentionally installs no production implementation/open hook.
type kidsVODAlreadyOpenSource interface {
	consumeExistingInput() (kidsVODInputKey, io.ReadCloser, error)
}
type kidsVODInputAuthority struct{ nonce [32]byte }

func newKidsVODInputAuthority() (*kidsVODInputAuthority, error) {
	a := &kidsVODInputAuthority{}
	_, err := rand.Read(a.nonce[:])
	return a, err
}

type kidsVODCapturedInput struct {
	mu         sync.Mutex
	authority  *kidsVODInputAuthority
	key        kidsVODInputKey
	body       io.ReadCloser
	claimed    bool
	registered bool
}

func (input *kidsVODCapturedInput) discard() {
	input.mu.Lock()
	if input.claimed {
		input.mu.Unlock()
		return
	}
	input.claimed = true
	body := input.body
	input.mu.Unlock()
	if body != nil {
		_ = body.Close()
	}
}
func (a *kidsVODInputAuthority) capture(source kidsVODAlreadyOpenSource) (*kidsVODCapturedInput, error) {
	if a == nil || source == nil {
		return nil, errKidsVODInput
	}
	key, body, err := source.consumeExistingInput()
	if err != nil || body == nil || !key.valid() {
		if body != nil {
			_ = body.Close()
		}
		return nil, errKidsVODInput
	}
	return &kidsVODCapturedInput{authority: a, key: key, body: body}, nil
}
func (a *kidsVODInputAuthority) wrap(ctx context.Context, bound *kidsVODBoundLease, input *kidsVODCapturedInput) (*kidsVODReader, error) {
	if a == nil || input == nil || bound == nil || bound.check(ctx) != nil {
		return nil, errKidsVODInput
	}
	input.mu.Lock()
	defer input.mu.Unlock()
	if input.authority != a || input.claimed || input.body == nil || input.key != bound.key {
		return nil, errKidsVODInput
	}
	input.claimed = true
	r := &kidsVODReader{ctx: ctx, bound: bound, body: input.body}
	r.installStops(context.AfterFunc(bound.lease.Context(), func() { _ = r.Close() }), context.AfterFunc(ctx, func() { _ = r.Close() }))
	return r, nil
}

type kidsVODReader struct {
	ctx                    context.Context
	bound                  *kidsVODBoundLease
	body                   io.ReadCloser
	once                   sync.Once
	mu                     sync.Mutex
	closed                 bool
	stopLease, stopRequest func() bool
}

func (r *kidsVODReader) installStops(stops ...func() bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		for _, stop := range stops {
			stop()
		}
		return
	}
	r.stopLease = stops[0]
	r.stopRequest = stops[1]
}
func (r *kidsVODReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return 0, errKidsVODInput
	}
	if r.bound.check(r.ctx) != nil {
		_ = r.Close()
		return 0, errKidsVODInput
	}
	n, err := r.body.Read(p)
	// A blocked read may complete after revocation. Its bytes are discarded,
	// including caller-visible buffer contents, before returning the denial.
	if r.bound.check(r.ctx) != nil {
		clear(p[:n])
		_ = r.Close()
		return 0, errKidsVODInput
	}
	return n, err
}
func (r *kidsVODReader) Close() error {
	var err error
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		stops := []func() bool{r.stopLease, r.stopRequest}
		r.mu.Unlock()
		for _, stop := range stops {
			if stop != nil {
				stop()
			}
		}
		err = r.body.Close()
	})
	return err
}
