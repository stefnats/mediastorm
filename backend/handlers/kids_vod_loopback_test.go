package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func kidsLoopRequest(method, peer string) *http.Request {
	r := httptest.NewRequest(method, "http://localhost/unregistered?path=caller-claim&profileId=other", nil)
	r.RemoteAddr = peer
	return r
}
func TestKidsVODLoopbackExactInputPeerInstanceAndMethod(t *testing.T) {
	for _, mode := range []string{"GET", "HEAD", "ipv6", "forwarded loopback", "malformed peer", "POST", "wrong input", "wrong instance", "wrong token", "expired", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			core, bound, a := kidsVODIOFixture(t, time.Minute)
			body := &kidsIOBody{}
			input := kidsIOCapture(t, a, bound, body)
			defer input.discard()
			registry := newKidsVODLoopback(a)
			token, err := registry.register(context.Background(), input, bound, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			r := kidsLoopRequest(http.MethodGet, "127.0.0.1:4444")
			candidate := input
			lookup := registry
			switch mode {
			case "HEAD":
				r.Method = http.MethodHead
			case "ipv6":
				r.RemoteAddr = "[::1]:4444"
			case "forwarded loopback":
				r.RemoteAddr = "198.51.100.10:4444"
				r.Header.Set("X-Forwarded-For", "127.0.0.1")
				r.Header.Set("X-Real-IP", "127.0.0.1")
			case "malformed peer":
				r.RemoteAddr = "localhost:4444"
			case "POST":
				r.Method = http.MethodPost
			case "wrong input":
				candidate = kidsIOCapture(t, a, bound, &kidsIOBody{})
				defer candidate.discard()
			case "wrong instance":
				lookup = newKidsVODLoopback(a)
			case "wrong token":
				token = strings.Repeat("0", 64)
			case "expired":
				registry.now = func() time.Time { return time.Now().Add(3 * time.Minute) }
			case "revoked":
				core.InvalidateProfile("child")
			}
			_, err = lookup.lookup(r, token, candidate)
			allowed := mode == "GET" || mode == "HEAD" || mode == "ipv6"
			if (err == nil) != allowed {
				t.Fatalf("unexpected lookup: %v", err)
			}
			if body.reads.Load() != 0 {
				t.Fatal("loopback inspection read/opened bytes")
			}
		})
	}
}
func TestKidsVODLoopbackBodyClaimIsSingleUseAndHeadDoesNotConsume(t *testing.T) {
	_, bound, a := kidsVODIOFixture(t, time.Minute)
	input := kidsIOCapture(t, a, bound, &kidsIOBody{})
	registry := newKidsVODLoopback(a)
	token, err := registry.register(context.Background(), input, bound, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = registry.register(context.Background(), input, bound, time.Minute); err == nil {
		t.Fatal("same captured input registered twice")
	}
	if _, err = registry.lookup(kidsLoopRequest(http.MethodHead, "127.0.0.1:1"), token, input); err != nil {
		t.Fatal(err)
	}
	if _, err = registry.claim(kidsLoopRequest(http.MethodHead, "127.0.0.1:1"), token, input); err == nil {
		t.Fatal("HEAD consumed body")
	}
	reader, err := registry.claim(kidsLoopRequest(http.MethodGet, "127.0.0.1:1"), token, input)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err = registry.claim(kidsLoopRequest(http.MethodGet, "127.0.0.1:1"), token, input); err == nil {
		t.Fatal("loopback token replayed")
	}
	buffer := make([]byte, 32)
	if n, _ := reader.Read(buffer); n == 0 {
		t.Fatal("valid captured input unavailable")
	}
}
func TestKidsVODLoopbackRevocationExpiryAndBoundedRegistryCloseUnclaimedHandles(t *testing.T) {
	for _, mode := range []string{"revoke", "expiry"} {
		t.Run(mode, func(t *testing.T) {
			core, bound, a := kidsVODIOFixture(t, time.Minute)
			body := &kidsIOBody{closed: make(chan struct{})}
			input := kidsIOCapture(t, a, bound, body)
			registry := newKidsVODLoopback(a)
			if _, err := registry.register(context.Background(), input, bound, 25*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			if mode == "revoke" {
				core.InvalidateProfile("child")
			}
			select {
			case <-body.closed:
			case <-time.After(time.Second):
				t.Fatal("unclaimed owned input leaked")
			}
			if body.closes.Load() != 1 {
				t.Fatal("input closed repeatedly")
			}
		})
	}
	_, bound, a := kidsVODIOFixture(t, time.Minute)
	registry := newKidsVODLoopback(a)
	for i := 0; i < 257; i++ {
		input := kidsIOCapture(t, a, bound, &kidsIOBody{})
		_, err := registry.register(context.Background(), input, bound, time.Minute)
		if (err != nil) != (i == 256) {
			t.Fatalf("registry bound at %d: %v", i, err)
		}
		if err != nil {
			input.discard()
		}
	}
	if _, err := registry.register(context.Background(), nil, bound, time.Minute); err == nil {
		t.Fatal("nil handle accepted")
	}
}
