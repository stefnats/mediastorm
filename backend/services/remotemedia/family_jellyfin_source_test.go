package remotemedia

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/services/profileunlock"
)

const familyJellyfinTestToken = "SYNTHETIC_JELLYFIN_ADAPTER_NOT_A_REAL_TOKEN"

type familyJellyfinWireFixture struct {
	server     *httptest.Server
	data       []byte
	users      atomic.Int32
	items      atomic.Int32
	downloads  atomic.Int32
	bodyWrites atomic.Int32
	bad        atomic.Bool
	hold       atomic.Bool
	mu         sync.Mutex
	mode       string
	itemHook   func(*http.Request)
	userHook   func(int, *http.Request)
	entered    chan struct{}
	release    chan struct{}
	onceEnter  sync.Once
	onceFree   sync.Once
	peerGone   chan struct{}
	onceGone   sync.Once
}

func newFamilyJellyfinWireFixture(t *testing.T, data []byte) *familyJellyfinWireFixture {
	t.Helper()
	f := &familyJellyfinWireFixture{data: data, entered: make(chan struct{}), release: make(chan struct{}), peerGone: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	address, ok := f.server.Listener.Addr().(*net.TCPAddr)
	if !ok || !address.IP.IsLoopback() {
		t.Fatal("own literal-loopback fixture required")
	}
	t.Cleanup(func() { f.unblock(); f.server.Close() })
	return f
}
func (f *familyJellyfinWireFixture) unblock() { f.onceFree.Do(func() { close(f.release) }) }
func (f *familyJellyfinWireFixture) setMode(mode string) {
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
}
func (f *familyJellyfinWireFixture) setItemHook(hook func(*http.Request)) {
	f.mu.Lock()
	f.itemHook = hook
	f.mu.Unlock()
}
func (f *familyJellyfinWireFixture) setUserHook(hook func(int, *http.Request)) {
	f.mu.Lock()
	f.userHook = hook
	f.mu.Unlock()
}
func (f *familyJellyfinWireFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	mode, hook, userHook := f.mode, f.itemHook, f.userHook
	f.mu.Unlock()
	if r.Method != http.MethodGet || r.ProtoMajor != 1 || r.Header.Get("User-Agent") != "mediastorm/1.0" || !strings.Contains(r.Header.Get("Authorization"), `Token="`+familyJellyfinTestToken+`"`) || !strings.Contains(r.Header.Get("Authorization"), `Client="mediastorm"`) || r.Header.Get("Range") != "" || r.Header.Get("If-Range") != "" {
		f.bad.Store(true)
	}
	switch r.URL.Path {
	case "/original/Users/Me":
		count := f.users.Add(1)
		if userHook != nil {
			userHook(int(count), r)
		}
		if r.URL.RawQuery != "" {
			f.bad.Store(true)
		}
		w.Header().Set("Content-Type", "application/json")
		if mode == "no user" {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"Id":%q,"Policy":{"IsDisabled":false}}`, familyJellyfinTestUser)
	case "/original/Items/" + familyJellyfinTestPrimary, "/original/Items/" + familyJellyfinTestBacking:
		f.items.Add(1)
		if r.URL.Query().Get("userId") != familyJellyfinTestUser || len(r.URL.Query()) != 1 {
			f.bad.Store(true)
		}
		if hook != nil {
			hook(r)
		}
		if mode == "slow metadata" {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(6 * time.Second):
			}
		}
		id := strings.TrimPrefix(r.URL.Path, "/original/Items/")
		item := familyJellyfinTestItem(id, int64(len(f.data)))
		if mode == "download lost" {
			item["CanDownload"] = false
		}
		if mode == "size changed" {
			item["MediaSources"].([]any)[0].(map[string]any)["Size"] = len(f.data) + 1
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(item)
	case "/original/Items/" + familyJellyfinTestBacking + "/Ancestors":
		if r.URL.Query().Get("userId") != familyJellyfinTestUser || len(r.URL.Query()) != 1 {
			f.bad.Store(true)
		}
		library := familyJellyfinTestLibrary
		if mode == "membership lost" {
			library = familyJellyfinTestUser
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"Id":%q}]`, library)
	case "/original/Items/" + familyJellyfinTestBacking + "/Download":
		f.downloads.Add(1)
		if r.URL.RawQuery != "" {
			f.bad.Store(true)
		}
		if mode == "download denied" {
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(f.data)))
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		if f.hold.Load() {
			f.onceEnter.Do(func() { close(f.entered) })
			select {
			case <-f.release:
			case <-r.Context().Done():
				f.onceGone.Do(func() { close(f.peerGone) })
				return
			}
		}
		f.bodyWrites.Add(1)
		_, _ = w.Write(f.data)
	default:
		f.bad.Store(true)
		http.NotFound(w, r)
	}
}

func TestFamilyJellyfinSourceRejectsUnownedConstructorsAndOpaqueImports(t *testing.T) {
	manager := config.NewManager("unused-synthetic-settings.json")
	for _, remote := range []*Service{nil, {}, {cfg: manager, repo: &fakeRemoteMediaRepo{}}} {
		if adapter, err := NewFamilyJellyfinSources(&datastore.DataStore{}, manager, remote); adapter != nil || err == nil {
			t.Fatal("unowned repository/store gained source authority")
		}
	}
	var adapter FamilyJellyfinSources
	if json.Unmarshal([]byte("{}"), &adapter) == nil || adapter.ReadyFor(nil, manager) {
		t.Fatal("imported adapter became ready")
	}
	var source profileunlock.FamilyJellyfinSource
	if stream, err := adapter.Open(t.Context(), &source); stream != nil || err == nil {
		t.Fatal("unowned source or copied metadata opened media")
	}
	var stream FamilyJellyfinStream
	if json.Unmarshal([]byte("{}"), &stream) == nil || stream.Check(t.Context()) == nil || stream.Context() != nil || !stream.ExpiresAt().IsZero() || stream.Close() == nil {
		t.Fatal("imported stream acquired current or cleanup authority")
	}
	if n, err := stream.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("unowned stream read bytes")
	}
	for _, value := range []any{adapter, &adapter, stream, &stream, FamilyJellyfinStreamInfo{}} {
		for _, format := range []string{"%v", "%+v", "%#v"} {
			if text := fmt.Sprintf(format, value); strings.Contains(text, "settings.json") || strings.Contains(text, "state:") {
				t.Fatal("capability representation exposed private dependencies")
			}
		}
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("source capability became serializable authority")
		}
	}
}

func TestFamilyJellyfinSourceCancelLinkJoinsAndAccountsTrueStop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	var called atomic.Bool
	join := familyJellyfinAfterFunc(ctx, func() { called.Store(true) })
	if !join() {
		t.Fatal("genuine live callback could not be stopped")
	}
	cancel()
	if !join() || called.Load() {
		t.Fatal("true-stop then cancellation stranded callback ownership")
	}
	ctx, cancel = context.WithCancel(t.Context())
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	join = familyJellyfinAfterFunc(ctx, func() { close(entered); <-release })
	cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("actual callback did not enter")
	}
	completed := make(chan bool, 1)
	go func() { completed <- join() }()
	select {
	case <-completed:
		t.Fatal("running actual callback was not joined")
	default:
	}
	unblock()
	select {
	case prevented := <-completed:
		if prevented {
			t.Fatal("running callback was labeled true-stop")
		}
	case <-time.After(time.Second):
		t.Fatal("released callback did not join")
	}
}

func TestFamilyJellyfinSourceZeroLengthProtocolDoesNotInferPlayback(t *testing.T) {
	info, primary, backing := familyJellyfinProtocolFixture(0)
	if _, err := parseFamilyJellyfin(info, familyJellyfinTestObservation(primary, backing)); err == nil {
		t.Fatal("zero bytes became finite playback evidence")
	}
	if !familyJellyfinLive(t.Context()) || familyJellyfinLive(nil) {
		t.Fatal("direct lifetime selector changed")
	}
	var unowned FamilyJellyfinStream
	if _, err := io.ReadAll(&unowned); err == nil {
		t.Fatal("Reader convenience bypassed genuine stream ownership")
	}
}

type familyJellyfinDelayedCaller struct {
	context.Context
	entered, release, done, stopRequested chan struct{}
}

func (*familyJellyfinDelayedCaller) Value(any) any { return nil }
func (caller *familyJellyfinDelayedCaller) AfterFunc(callback func()) func() bool {
	stop := context.AfterFunc(caller.Context, func() {
		defer close(caller.done)
		close(caller.entered)
		<-caller.release
		callback()
	})
	var once sync.Once
	return func() bool {
		once.Do(func() { close(caller.stopRequested) })
		return stop()
	}
}
