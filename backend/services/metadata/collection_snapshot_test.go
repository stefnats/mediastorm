package metadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type collectionSnapshotFixture struct {
	service  *Service
	client   *tmdbClient
	cache    *fileCache
	calls    atomic.Int32
	raw      atomic.Value
	truncate atomic.Bool
}

func collectionPayload(n int) string {
	parts := make([]map[string]any, 0, n)
	for i := 1; i <= n; i++ {
		parts = append(parts, map[string]any{"id": i, "title": fmt.Sprintf("Observed %04d", i), "release_date": "2000-01-01", "poster_path": "/poster.jpg", "original_language": "de", "private": "PRIVATE-UNBOUND-URL"})
	}
	value, _ := json.Marshal(map[string]any{"id": 77, "name": "Observed collection", "parts": parts, "private": "PRIVATE-UNBOUND-URL"})
	return string(value)
}
func newCollectionSnapshotFixture(t *testing.T, raw string) *collectionSnapshotFixture {
	t.Helper()
	f := &collectionSnapshotFixture{}
	f.raw.Store(raw)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.URL.Path != "/3/collection/77" {
			t.Errorf("unexpected synthetic provider path %s", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		raw := f.raw.Load().(string)
		if f.truncate.Load() {
			w.Header().Set("Content-Length", fmt.Sprint(len(raw)+100))
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, raw)
	}))
	t.Cleanup(server.Close)
	target, _ := url.Parse(server.URL)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		u := *r.URL
		u.Scheme = target.Scheme
		u.Host = target.Host
		copy.URL = &u
		return server.Client().Transport.RoundTrip(copy)
	})}
	f.cache = newFileCache(t.TempDir(), 48)
	f.client = newTMDBClient("synthetic-collection-key", "eng", client, f.cache)
	// Zero-valued releaseWarmer disables only existing background warmups here;
	// the normal collection fetch and its same-body producer are real HTTP.
	f.service = &Service{cache: f.cache, tmdb: f.client}
	return f
}

func TestCollectionSnapshotActualProducerCompleteReadAndNoBrowseDemand(t *testing.T) {
	f := newCollectionSnapshotFixture(t, collectionPayload(702))
	if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable || f.calls.Load() != 0 {
		t.Fatal("missing cache must not fetch", err)
	}
	legacy, err := f.service.CollectionDetails(t.Context(), 77)
	if err != nil || legacy == nil || len(legacy.Movies) != 702 || f.calls.Load() != 1 {
		t.Fatal("actual normal producer", err)
	}
	first, err := f.service.GetCachedCollectionSnapshot(77, time.Now())
	if err != nil || len(first.Movies) != 702 {
		t.Fatal("complete observation", err)
	}
	if first.ExpiresAt.Sub(first.ObservedAt) != 24*time.Hour {
		t.Fatal("24h maximum")
	}
	raw, err := os.ReadFile(filepath.Join(f.cache.dir, collectionSnapshotKey(77, first.MetadataLanguage)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "PRIVATE-UNBOUND-URL") || strings.Contains(string(raw), "original_language") {
		t.Fatal("unrelated/scalar provider data persisted")
	}
	titles := first.Titles()
	if titles[650].ID != "tmdb:movie:651" || titles[650].OriginalLanguageEvidence != nil {
		t.Fatal("part identity or scalar promotion")
	}
	if fact := f.service.GetCachedOriginalLanguageEvidence(titles[650].ID, "movie", time.Now()); fact != nil {
		t.Fatal("collection language scalar became proof")
	}
	first.Movies[0].Name = "mutated caller copy"
	for i := 0; i < 10; i++ {
		next, err := f.service.GetCachedCollectionSnapshot(77, time.Now())
		if err != nil || next.Movies[0].Name == "mutated caller copy" || !next.ObservedAt.Equal(first.ObservedAt) || !next.ExpiresAt.Equal(first.ExpiresAt) {
			t.Fatal("read mutation or freshness renewal", err)
		}
	}
	if f.calls.Load() != 1 {
		t.Fatal("cache browse made HTTP", f.calls.Load())
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := f.service.GetCachedCollectionSnapshot(77, time.Now())
			if err != nil {
				t.Error(err)
				return
			}
			s.Movies[0].Name = "isolated"
		}()
	}
	wg.Wait()
}

func TestCollectionSnapshotInvalidObservationsPreserveLegacyFirstJSON(t *testing.T) {
	valid := collectionPayload(1)
	for _, tc := range []struct {
		name, body string
		legacyOK   bool
	}{
		{"trailing second object", valid + ` {"parts":[]}`, true},
		{"trailing garbage", valid + ` garbage`, true},
		{"wrong actual id", strings.Replace(valid, `"id":77`, `"id":78`, 1), true},
		{"missing parts", `{"id":77,"name":"Missing"}`, true},
		{"null parts", `{"id":77,"name":"Null","parts":null}`, true},
		{"duplicate members", `{"id":77,"name":"Duplicate","parts":[{"id":1,"title":"A"},{"id":1,"title":"B"}]}`, true},
		{"duplicate top identity", `{"id":78,"id":77,"name":"Duplicate","parts":[]}`, true},
		{"duplicate member identity", `{"id":77,"name":"Duplicate","parts":[{"id":2,"id":1,"title":"A"}]}`, true},
		{"zero member", `{"id":77,"name":"Invalid","parts":[{"id":0,"title":"A"}]}`, true},
		{"missing member title", `{"id":77,"name":"Invalid","parts":[{"id":1}]}`, true},
		{"external image", strings.Replace(valid, `/poster.jpg`, `https://foreign.invalid/image`, 1), true},
		{"too many members", collectionPayload(4097), true},
		{"too many bytes", strings.TrimSuffix(valid, "}") + `,"padding":"` + strings.Repeat("x", collectionSnapshotMaxBytes) + `"}`, true},
		{"truncated object", valid[:len(valid)-2], false},
		{"wrong parts type", `{"id":77,"name":"Invalid","parts":"bad"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCollectionSnapshotFixture(t, tc.body)
			_, err := f.client.fetchCollectionDetails(t.Context(), 77)
			if (err == nil) != tc.legacyOK {
				t.Fatalf("legacy result changed: %v", err)
			}
			if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable {
				t.Fatal("invalid response created partial snapshot", err)
			}
			if f.calls.Load() != 1 {
				t.Fatal("extra provider request")
			}
		})
	}
}

func TestCollectionSnapshotKnownEmptyMaxMembersAndReadFailure(t *testing.T) {
	for _, n := range []int{0, 4096} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := newCollectionSnapshotFixture(t, collectionPayload(n))
			if _, err := f.client.fetchCollectionDetails(t.Context(), 77); err != nil {
				t.Fatal(err)
			}
			snapshot, err := f.service.GetCachedCollectionSnapshot(77, time.Now())
			if err != nil || snapshot.Movies == nil || len(snapshot.Movies) != n {
				t.Fatal("known complete size", n, err)
			}
		})
	}
	t.Run("valid first JSON but incomplete HTTP", func(t *testing.T) {
		f := newCollectionSnapshotFixture(t, collectionPayload(1))
		f.truncate.Store(true)
		if _, err := f.client.fetchCollectionDetails(t.Context(), 77); err != nil {
			t.Fatal("legacy first complete JSON should still succeed", err)
		}
		if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable {
			t.Fatal("incomplete HTTP created observation", err)
		}
	})
}

func TestCollectionSnapshotTTLIdentityLanguageAndCorruptFiles(t *testing.T) {
	f := newCollectionSnapshotFixture(t, collectionPayload(2))
	f.cache.ttl = time.Hour
	if _, err := f.client.fetchCollectionDetails(t.Context(), 77); err != nil {
		t.Fatal(err)
	}
	original, err := f.service.GetCachedCollectionSnapshot(77, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if original.ExpiresAt.Sub(original.ObservedAt) != time.Hour {
		t.Fatal("shorter existing TTL ignored")
	}
	path := filepath.Join(f.cache.dir, collectionSnapshotKey(77, original.MetadataLanguage)+".json")
	before, _ := os.Stat(path)
	if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("cache read rewrote observation file")
	}
	if _, err := f.service.GetCachedCollectionSnapshot(77, original.ExpiresAt); err != ErrCollectionSnapshotUnavailable {
		t.Fatal("expiry boundary", err)
	}
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.GetCachedCollectionSnapshot(77, original.ExpiresAt); err != ErrCollectionSnapshotUnavailable {
		t.Fatal("mtime revived original observation", err)
	}
	if _, err := f.service.GetCachedCollectionSnapshot(78, time.Now()); err != ErrCollectionSnapshotUnavailable {
		t.Fatal("wrong requested anchor", err)
	}
	localized := f.service.WithLanguage("deu")
	if _, err := localized.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable {
		t.Fatal("metadata language leaked", err)
	}
	german := newTMDBClient("synthetic-collection-key", "deu", f.client.httpc, f.cache)
	if _, err := german.fetchCollectionDetails(t.Context(), 77); err != nil {
		t.Fatal(err)
	}
	if s, err := localized.GetCachedCollectionSnapshot(77, time.Now()); err != nil || s.MetadataLanguage == original.MetadataLanguage {
		t.Fatal("actual localized producer did not create separate observation", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*CachedCollectionSnapshot)
	}{
		{"wrong anchor", func(s *CachedCollectionSnapshot) { s.CollectionID = 78 }},
		{"wrong provider", func(s *CachedCollectionSnapshot) { s.Provider = "manual" }},
		{"wrong language", func(s *CachedCollectionSnapshot) { s.MetadataLanguage = "foreign" }},
		{"future", func(s *CachedCollectionSnapshot) {
			s.ObservedAt = time.Now().Add(time.Hour)
			s.ExpiresAt = s.ObservedAt.Add(time.Hour)
		}},
		{"extended TTL", func(s *CachedCollectionSnapshot) { s.ExpiresAt = s.ObservedAt.Add(25 * time.Hour) }},
		{"nil collection", func(s *CachedCollectionSnapshot) { s.Movies = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			copy := *original
			tc.change(&copy)
			if err := f.cache.set(collectionSnapshotKey(77, original.MetadataLanguage), &copy); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable {
				t.Fatal(err)
			}
		})
	}
	for _, raw := range []string{`{"version":1`, strings.Repeat("x", collectionSnapshotMaxBytes+1)} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable {
			t.Fatal("malformed/oversized cache accepted", err)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := f.service.GetCachedCollectionSnapshot(77, time.Now()); err != ErrCollectionSnapshotUnavailable || time.Since(start) > time.Second {
		t.Fatal("nonregular cache must not block", err)
	}
	if f.calls.Load() != 2 {
		t.Fatal("invalid cache caused provider demand")
	}
}

type collectionErrorBody struct {
	data []byte
	err  error
}

func (r *collectionErrorBody) Read(p []byte) (int, error) {
	if len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, r.err
}
func TestCollectionSnapshotSameBodyReplayRetainsLegacyErrors(t *testing.T) {
	sentinel := errors.New("synthetic source error")
	for _, raw := range []string{collectionPayload(1), strings.Repeat(" ", collectionSnapshotMaxBytes+100) + collectionPayload(1)} {
		reader, observed := collectionResponseForLegacy(&collectionErrorBody{data: []byte(raw), err: sentinel})
		replayed, err := io.ReadAll(reader)
		if !bytes.Equal(replayed, []byte(raw)) || !errors.Is(err, sentinel) || observed != nil {
			t.Fatal("legacy body or original error changed", err)
		}
	}
}

type collectionCombinedErrorReader struct {
	data []byte
	err  error
}

func (r *collectionCombinedErrorReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}

func TestCollectionSnapshotCombinedReadErrorAtOverflowRetainsLegacy(t *testing.T) {
	sentinel := errors.New("synthetic combined read error")
	raw := append(bytes.Repeat([]byte(" "), collectionSnapshotMaxBytes-5), []byte(`{"id":`)...)
	if len(raw) != collectionSnapshotMaxBytes+1 {
		t.Fatal("fixture bound")
	}
	var original any
	if err := json.NewDecoder(&collectionCombinedErrorReader{data: append([]byte(nil), raw...), err: sentinel}).Decode(&original); !errors.Is(err, sentinel) {
		t.Fatal("original decoder control did not see error", err)
	}
	reader, observation := collectionResponseForLegacy(&collectionCombinedErrorReader{data: append([]byte(nil), raw...), err: sentinel})
	if observation != nil {
		t.Fatal("failed read became cache observation")
	}
	if err := json.NewDecoder(reader).Decode(new(any)); !errors.Is(err, sentinel) {
		t.Fatal("legacy replay lost original combined read error", err)
	}
}
