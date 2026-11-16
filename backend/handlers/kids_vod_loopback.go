package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"

	"novastream/internal/requestsecurity"
)

// This registry is not wired to buildLocalVideoStreamURL or an HTTP route.
// Tokens are private instance capabilities for an exact already-captured input,
// not permission to resolve any path or open a replacement provider input.
type kidsVODLoopback struct {
	mu        sync.Mutex
	authority *kidsVODInputAuthority
	entries   map[string]kidsVODLoopbackEntry
	now       func() time.Time
}
type kidsVODLoopbackEntry struct {
	input   *kidsVODCapturedInput
	bound   *kidsVODBoundLease
	expires time.Time
	stop    func()
}

func newKidsVODLoopback(authority *kidsVODInputAuthority) *kidsVODLoopback {
	return &kidsVODLoopback{authority: authority, entries: map[string]kidsVODLoopbackEntry{}, now: time.Now}
}
func (l *kidsVODLoopback) register(ctx context.Context, input *kidsVODCapturedInput, bound *kidsVODBoundLease, ttl time.Duration) (string, error) {
	if l == nil || l.authority == nil || l.now == nil || input == nil || bound == nil || ttl <= 0 || ttl > 2*time.Minute || bound.check(ctx) != nil {
		return "", errKidsVODInput
	}
	input.mu.Lock()
	valid := input.authority == l.authority && !input.claimed && !input.registered && input.body != nil && input.key == bound.key
	input.mu.Unlock()
	if !valid {
		return "", errKidsVODInput
	}
	expires := l.now().Add(ttl)
	if bound.lease.ExpiresAt().Before(expires) {
		expires = bound.lease.ExpiresAt()
	}
	if !expires.After(l.now()) {
		return "", errKidsVODInput
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	token := hex.EncodeToString(random[:])
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) >= 256 {
		return "", errKidsVODInput
	}
	input.mu.Lock()
	if input.claimed || input.registered {
		input.mu.Unlock()
		return "", errKidsVODInput
	}
	input.registered = true
	input.mu.Unlock()
	l.entries[token] = kidsVODLoopbackEntry{input: input, bound: bound, expires: expires}
	expire := func() {
		l.mu.Lock()
		entry, ok := l.entries[token]
		delete(l.entries, token)
		l.mu.Unlock()
		if ok && entry.stop != nil {
			entry.stop()
		}
		input.discard()
	}
	// Both callbacks own only the captured handle. They never open an input.
	timer := time.AfterFunc(expires.Sub(l.now()), expire)
	stop := context.AfterFunc(bound.lease.Context(), func() { timer.Stop(); expire() })
	entry := l.entries[token]
	entry.stop = func() { timer.Stop(); stop() }
	l.entries[token] = entry
	return token, nil
}
func (l *kidsVODLoopback) lookup(r *http.Request, token string, input *kidsVODCapturedInput) (kidsVODLoopbackEntry, error) {
	if l == nil || r == nil || !requestsecurity.IsLoopback(r) || (r.Method != http.MethodGet && r.Method != http.MethodHead) || len(token) != 64 || input == nil {
		return kidsVODLoopbackEntry{}, errKidsVODInput
	}
	l.mu.Lock()
	entry, ok := l.entries[token]
	expired := false
	if ok && !entry.expires.After(l.now()) {
		delete(l.entries, token)
		ok = false
		expired = true
	}
	l.mu.Unlock()
	if expired {
		if entry.stop != nil {
			entry.stop()
		}
		entry.input.discard()
	}
	if !ok || entry.input != input || entry.bound.check(r.Context()) != nil {
		return kidsVODLoopbackEntry{}, errKidsVODInput
	}
	return entry, nil
}
func (l *kidsVODLoopback) claim(r *http.Request, token string, input *kidsVODCapturedInput) (*kidsVODReader, error) {
	entry, err := l.lookup(r, token, input)
	if err != nil || r.Method != http.MethodGet {
		return nil, errKidsVODInput
	}
	// Consume a token on the sole body claim. HEAD inspection never consumes it.
	l.mu.Lock()
	current, ok := l.entries[token]
	if ok && current.input == entry.input {
		delete(l.entries, token)
	}
	l.mu.Unlock()
	if !ok {
		return nil, errKidsVODInput
	}
	if current.stop != nil {
		current.stop()
	}
	reader, err := l.authority.wrap(r.Context(), entry.bound, entry.input)
	if err != nil {
		entry.input.discard()
	}
	return reader, err
}
