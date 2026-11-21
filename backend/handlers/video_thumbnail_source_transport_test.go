package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type thumbnailPolicyTransport struct{ calls atomic.Int32 }

var errThumbnailPolicy = errors.New("synthetic thumbnail policy denied request")

func (p *thumbnailPolicyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	p.calls.Add(1)
	return nil, errThumbnailPolicy
}

func TestThumbnailSourceBridgePreservesTransportPolicy(t *testing.T) {
	var upstream atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	policy := &thumbnailPolicyTransport{}
	original := http.DefaultTransport
	http.DefaultTransport = policy
	t.Cleanup(func() { http.DefaultTransport = original })
	bridge := newThumbnailSourceBridge()
	response, err := bridge.doRequest(t.Context(), http.MethodGet, thumbnailSourceSession{sourceURL: server.URL}, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if !errors.Is(err, errThumbnailPolicy) || policy.calls.Load() != 1 || upstream.Load() != 0 {
		t.Fatalf("configured transport policy was bypassed: error=%v policy=%d upstream=%d", err, policy.calls.Load(), upstream.Load())
	}
}
