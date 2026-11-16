package handlers

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/metadata"
)

type collectionWireTransport struct {
	target   *url.URL
	actual   http.RoundTripper
	calls    atomic.Int32
	denied   atomic.Int32
	producer atomic.Int32
}

func (t *collectionWireTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.target == nil || t.actual == nil || r.URL.Scheme != "https" || r.URL.Host != "api.themoviedb.org" || r.URL.Path != "/3/collection/77" || r.URL.Query().Get("api_key") != "synthetic-collection-key" {
		t.denied.Add(1)
		return nil, fmt.Errorf("unexpected synthetic provider operation")
	}
	t.calls.Add(1)
	copy := r.Clone(r.Context())
	u := *r.URL
	u.Scheme = t.target.Scheme
	u.Host = t.target.Host
	copy.URL = &u
	return t.actual.RoundTrip(copy)
}

func collectionLoopbackTransport(address string) *http.Transport {
	return &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, target string) (net.Conn, error) {
		if target != address {
			return nil, fmt.Errorf("fixture denies nonlocal dial")
		}
		host, _, err := net.SplitHostPort(target)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return nil, fmt.Errorf("fixture requires literal loopback")
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}}
}
func collectionCacheWireFile(t *testing.T, dir, key string, value any) string {
	t.Helper()
	sum := sha1.Sum([]byte(key))
	path := filepath.Join(dir, "metadata", hex.EncodeToString(sum[:])+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func collectionCachedWireFixture(t *testing.T) (*collectionCachedFixture, *metadata.Service, *collectionWireTransport) {
	t.Helper()
	// Install the deny-by-default guard BEFORE any metadata/service constructor:
	// apiusage.TrackClient captures DefaultTransport during construction.
	transport := &collectionWireTransport{}
	original := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = original })
	f := newCollectionCachedFixture(t, 0)
	dir := t.TempDir()
	parts := make([]map[string]any, 0, 702)
	now := time.Now().UTC()
	for i := 1; i <= 702; i++ {
		parts = append(parts, map[string]any{"id": i, "title": fmt.Sprintf("Observed %04d", i), "release_date": "2000-01-01", "original_language": "de", "poster_path": "/observed.jpg"})
		// Existing release-window cache entries prevent only baseline normal GET
		// warmup. They are not original-language facts or part of cache browse.
		collectionCacheWireFile(t, dir, fmt.Sprintf("tmdb:movie:releases:v2:%d", i), map[string]any{"releases": []any{}})
	}
	for i, lang := range []string{"fra", "haw", "nob", "nno", "nor", "expired", "wrong", ""} {
		id := 651 + i
		if lang == "" {
			continue
		}
		fact := models.OriginalLanguageEvidence{Source: "tmdb", SourceTitleID: fmt.Sprintf("tmdb:movie:%d", id), Language: lang, ObservedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)}
		if lang == "expired" {
			fact.Language = "fra"
			fact.ExpiresAt = now.Add(-time.Minute)
		}
		if lang == "wrong" {
			fact.Language = "fra"
			fact.SourceTitleID = "tmdb:movie:99999"
		}
		collectionCacheWireFile(t, dir, fmt.Sprintf("tmdb:movie:original_language_evidence:v1:%d", id), fact)
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transport.producer.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": 77, "name": "Observed collection", "parts": parts, "private": "PRIVATE-NOT-A-SNAPSHOT-FIELD"})
	}))
	t.Cleanup(provider.Close)
	target, _ := url.Parse(provider.URL)
	actual := collectionLoopbackTransport(provider.Listener.Addr().String())
	t.Cleanup(actual.CloseIdleConnections)
	transport.target = target
	transport.actual = actual
	service := metadata.NewService("", "synthetic-collection-key", "eng", dir, 24, false, metadata.MDBListConfig{})
	f.port.actual = service
	f.port.languages = service
	return f, service, transport
}

func TestCollectionCachedActualProducerHTTPAndCacheBrowseWire(t *testing.T) {
	f, service, transport := collectionCachedWireFixture(t)
	normal := NewMetadataHandler(service, f.base.meta.CfgManager)
	normal.UsersService = f.base.meta.UsersService
	router := mux.NewRouter()
	router.HandleFunc("/api/users/{userID}/display-list", f.base.display.Get)
	router.HandleFunc("/api/metadata/collection", normal.CollectionDetails)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		profileaccess.Middleware(func(p, a string) bool { return p == "profile" && a == "account" })(router).ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	actual := collectionLoopbackTransport(server.Listener.Addr().String())
	t.Cleanup(actual.CloseIdleConnections)
	client := &http.Client{Transport: actual, Timeout: 5 * time.Second}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string][]byte{}
	get := func(path string) (*http.Response, []byte) {
		t.Helper()
		r, _ := http.NewRequest("GET", server.URL+path, nil)
		r.Header.Set("X-Client-ID", "fixture-client")
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		return response, body
	}
	response, body := get("/api/users/profile/display-list?source=collection-cached&collectionId=77")
	if response.StatusCode != 409 || transport.calls.Load() != 0 {
		t.Fatal("missing cache must not fetch", response.StatusCode)
	}
	fixtures["missing.json"] = body
	// Actual normal GET is the one producer. The existing metadata encoder is
	// used here; unrelated baseline optional services are not installed.
	response, body = get("/api/metadata/collection?id=77&userId=profile")
	if response.StatusCode != 200 || transport.calls.Load() != 1 || transport.producer.Load() != 1 || transport.denied.Load() != 0 {
		t.Fatal("actual HTTP producer", response.StatusCode, transport.calls.Load(), string(body))
	}
	if _, err := f.base.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:652", MediaType: "movie", Name: "Observed 0652"}); err != nil {
		t.Fatal(err)
	}
	originalSnapshot, err := service.GetCachedCollectionSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	originalBytes, _ := json.Marshal(originalSnapshot)
	for _, tc := range []struct {
		name, query  string
		total, count int
	}{
		{"unfiltered.json", "limit=2", 701, 2},
		{"late-french.json", "originalLanguages=fra&limit=1", 1, 1},
		{"late-norwegian.json", "originalLanguages=nob,nno,nor&limit=0", 3, 3},
		{"continuation.json", "limit=2&offset=650", 701, 2},
		{"unknown-only.json", "includeUnknownOriginalLanguage=true&limit=2", 697, 2},
		{"or-unknown.json", "originalLanguages=fra&includeUnknownOriginalLanguage=true&limit=2", 698, 2},
		{"empty-filter.json", "originalLanguages=isl", 0, 0},
		{"random-pool.json", "originalLanguages=fra,nob&limit=0", 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := "/api/users/profile/display-list?source=collection-cached&collectionId=77&" + tc.query
			response, body := get(path)
			if response.StatusCode != 200 {
				t.Fatal(response.StatusCode, string(body))
			}
			var wire struct {
				Items      []models.Title                    `json:"items"`
				Total      int                               `json:"total"`
				Supported  bool                              `json:"originalLanguageFilterSupported"`
				Collection cachedCollectionBrowseObservation `json:"collection"`
			}
			if err := json.Unmarshal(body, &wire); err != nil {
				t.Fatal(err)
			}
			if wire.Total != tc.total || len(wire.Items) != tc.count || !wire.Supported || !wire.Collection.ObservedAt.Equal(originalSnapshot.ObservedAt) || !wire.Collection.ExpiresAt.Equal(originalSnapshot.ExpiresAt) {
				t.Fatalf("wire total/count/time %d/%d", wire.Total, len(wire.Items))
			}
			if transport.calls.Load() != 1 || transport.producer.Load() != 1 || transport.denied.Load() != 0 || f.base.ratings.observes.Load() != 0 || f.base.repo.calls.Load() != 0 {
				t.Fatal("cache browse started excluded demand")
			}
			req, _ := http.NewRequest("GET", server.URL+path, nil)
			if err := validator.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			if err := validator.ValidateResponse(req, response.StatusCode, response.Header, body); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(body), "PRIVATE-NOT-A-SNAPSHOT-FIELD") {
				t.Fatal("unknown provider field leaked")
			}
			fixtures[tc.name] = body
		})
	}
	next, err := service.GetCachedCollectionSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	nextBytes, _ := json.Marshal(next)
	if !bytes.Equal(originalBytes, nextBytes) {
		t.Fatal("browse renewed or mutated observation")
	}
	if dir := os.Getenv("COLLECTION_CACHED_CONTRACT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		hashes := map[string]string{}
		for name, body := range fixtures {
			if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
				t.Fatal(err)
			}
			sum := sha256.Sum256(body)
			hashes[name] = hex.EncodeToString(sum[:])
		}
		raw, _ := json.MarshalIndent(hashes, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func collectionCachedOpenAPIContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	f := newCollectionCachedFixture(t, 2)
	r := collectionCachedRequest("limit=1")
	w := httptest.NewRecorder()
	f.base.display.Get(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := v.ValidateRequest(r, nil); err != nil {
		t.Fatal(err)
	}
	if err := v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionCachedWireTransportRejectsOtherURLsWithoutDial(t *testing.T) {
	target, _ := url.Parse("http://127.0.0.1:9")
	actual := &catalogueWireTransport{}
	guard := &collectionWireTransport{target: target, actual: actual}
	for _, raw := range []string{"https://foreign.invalid/3/collection/77", "http://api.themoviedb.org/3/collection/77", "https://api.themoviedb.org/3/movie/77", "https://api.themoviedb.org/3/collection/78", "https://api.themoviedb.org/3/collection/77?api_key=wrong"} {
		request, _ := http.NewRequest("GET", raw, nil)
		if response, err := guard.RoundTrip(request); err == nil || response != nil {
			t.Fatal("unexpected URL reached fixture network")
		}
	}
	if guard.denied.Load() != 5 || guard.calls.Load() != 0 || actual.calls.Load() != 0 {
		t.Fatal("deny-by-default bypass")
	}
	transport := collectionLoopbackTransport("127.0.0.1:9")
	if conn, err := transport.DialContext(t.Context(), "tcp", "203.0.113.1:80"); err == nil || conn != nil {
		t.Fatal("nonlocal dial was not rejected locally")
	}
}
