package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/remotemedia"
	"novastream/services/sessions"
	"novastream/services/users"
)

const (
	familyJellyfinRawPrimary = "11111111222233334444555555555555"
	familyJellyfinRawBacking = "22222222333344445555666666666666"
	familyJellyfinRawLibrary = "33333333444455556666777777777777"
	familyJellyfinRawUser    = "44444444555566667777888888888888"
	familyJellyfinRawToken   = "SYNTHETIC_RAW_JELLYFIN_NOT_A_REAL_CREDENTIAL"
)

type familyJellyfinRawWire struct {
	server    *httptest.Server
	data      []byte
	mode      atomic.Int32
	requests  atomic.Int32
	downloads atomic.Int32
	bad       atomic.Bool
	hold      atomic.Bool
	entered   chan struct{}
	release   chan struct{}
	enterOnce sync.Once
	freeOnce  sync.Once
}

func familyJellyfinRawDTO(id string, size int) map[string]any {
	return map[string]any{"Id": id, "Type": "Movie", "CanDownload": true, "RunTimeTicks": 900000000,
		"MediaSources": []any{map[string]any{"Id": familyJellyfinRawBacking, "Type": "Default", "Protocol": "File", "VideoType": "VideoFile", "Container": "mkv", "Size": size, "RunTimeTicks": 900000000,
			"IsRemote": false, "IsInfiniteStream": false, "RequiresOpening": false, "RequiresClosing": false, "RequiresLooping": false, "Path": "/server/private/never-expose.mkv",
			"MediaStreams": []any{map[string]any{"Index": 0, "Type": "Video", "Codec": "h264", "IsExternal": false}, map[string]any{"Index": 1, "Type": "Audio", "Codec": "aac", "IsExternal": false}}}}}
}
func newFamilyJellyfinRawWire(t *testing.T, data []byte) *familyJellyfinRawWire {
	t.Helper()
	f := &familyJellyfinRawWire{data: data, entered: make(chan struct{}), release: make(chan struct{})}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	address, ok := f.server.Listener.Addr().(*net.TCPAddr)
	if !ok || !address.IP.IsLoopback() {
		t.Fatal("own literal-loopback listener required")
	}
	t.Cleanup(func() { f.unblock(); f.server.Close() })
	return f
}
func (f *familyJellyfinRawWire) unblock() { f.freeOnce.Do(func() { close(f.release) }) }
func (f *familyJellyfinRawWire) serve(w http.ResponseWriter, r *http.Request) {
	f.requests.Add(1)
	if r.Method != "GET" || r.ProtoMajor != 1 || r.Header.Get("User-Agent") != "mediastorm/1.0" || !strings.Contains(r.Header.Get("Authorization"), `Client="mediastorm"`) || !strings.Contains(r.Header.Get("Authorization"), `Token="`+familyJellyfinRawToken+`"`) || r.Header.Get("Range") != "" || strings.Contains(r.URL.RawQuery, "token") {
		f.bad.Store(true)
	}
	switch r.URL.Path {
	case "/original/Users/Me":
		w.Header().Set("Content-Type", "application/json")
		if f.mode.Load() == 1 {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"Id":%q,"Policy":{"IsDisabled":false}}`, familyJellyfinRawUser)
	case "/original/Items/" + familyJellyfinRawPrimary, "/original/Items/" + familyJellyfinRawBacking:
		if r.URL.Query().Get("userId") != familyJellyfinRawUser || len(r.URL.Query()) != 1 {
			f.bad.Store(true)
		}
		if f.mode.Load() == 4 {
			f.enterOnce.Do(func() { close(f.entered) })
			select {
			case <-r.Context().Done():
				return
			case <-f.release:
			}
		}
		item := familyJellyfinRawDTO(strings.TrimPrefix(r.URL.Path, "/original/Items/"), len(f.data))
		if f.mode.Load() == 2 {
			item["CanDownload"] = false
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(item)
	case "/original/Items/" + familyJellyfinRawBacking + "/Ancestors":
		library := familyJellyfinRawLibrary
		if f.mode.Load() == 3 {
			library = familyJellyfinRawUser
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `[{"Id":%q}]`, library)
	case "/original/Items/" + familyJellyfinRawBacking + "/Download":
		f.downloads.Add(1)
		if f.mode.Load() == 5 {
			w.WriteHeader(403)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(f.data)))
		w.Header().Set("ETag", `"untrusted-not-a-byte-version"`)
		w.Header().Set("Location", "http://unowned.invalid/never-follow")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		if f.hold.Load() {
			f.enterOnce.Do(func() { close(f.entered) })
			select {
			case <-r.Context().Done():
				return
			case <-f.release:
			}
		}
		_, _ = w.Write(f.data)
	default:
		f.bad.Store(true)
		http.NotFound(w, r)
	}
}

type familyJellyfinRawPG struct {
	*publicPinCarrierFixture
	handler  *familyJellyfinRawHandler
	manager  *config.Manager
	settings config.Settings
	wire     *familyJellyfinRawWire
	unlock   string
	child    string
	captures atomic.Int32
	pgClosed atomic.Int32
}

type familyJellyfinRawObservedOwner struct {
	*profileunlock.FamilyMediaOwner
	once     sync.Once
	pgClosed *atomic.Int32
}

func (o *familyJellyfinRawObservedOwner) Close() {
	o.FamilyMediaOwner.Close()
	o.once.Do(func() { o.pgClosed.Add(1) })
}

func newFamilyJellyfinRawPG(t *testing.T, data []byte, integrations int) *familyJellyfinRawPG {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("actual same-store family installer requires Linux")
	}
	f := newPublicPinCarrierFixture(t)
	if integrations == 8 {
		// Keep eight genuine request/PG/watch lifetimes from starving each
		// other in the helper's deliberately tiny default4 connection pool.
		store, err := datastore.New(t.Context(), f.store.Pool().Config().ConnString(), datastore.WithMaxConns(32))
		if err != nil {
			t.Fatal("open bounded own same-schema integration pool")
		}
		t.Cleanup(store.Close)
		f.store = store
		f.users, err = users.NewServiceWithStore(store, t.TempDir())
		if err != nil {
			t.Fatal("bind actual users service to own pool")
		}
		f.sessions, err = sessions.NewServiceWithStore(store, time.Hour)
		if err != nil {
			t.Fatal("bind actual sessions service to own pool")
		}
		f.pins = profileunlock.NewPGProfilePins(store)
	}
	child := familyActualPGChild(t, f)
	f.setPIN(t, "1357")
	installed := familyActualPGInstall(t, f)
	value := &familyJellyfinRawPG{publicPinCarrierFixture: f, child: child, wire: newFamilyJellyfinRawWire(t, data)}
	value.settings = config.DefaultSettings()
	for i := range integrations {
		integration, library, item := fmt.Sprintf("integration-%d", i), fmt.Sprintf("remote-library-%d", i), fmt.Sprintf("remote-item-%d", i)
		value.settings.Jellyfin.Accounts = append(value.settings.Jellyfin.Accounts, config.JellyfinAccount{ID: integration, UserID: familyJellyfinRawUser, Token: familyJellyfinRawToken, ServerURL: value.wire.server.URL + "/original"})
		if _, err := f.store.Pool().Exec(t.Context(), `INSERT INTO remote_media_libraries(id,name,library_type,provider,account_id,external_library_id)VALUES($1,'Synthetic remote library','movie','jellyfin',$2,$3)`, library, integration, familyJellyfinRawLibrary); err != nil {
			t.Fatal("seed own exact provider-integration library")
		}
		if _, err := f.store.Pool().Exec(t.Context(), `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES($1,'all')`, library); err != nil {
			t.Fatal("seed own ordinary viewer ACL")
		}
		if _, err := f.store.Pool().Exec(t.Context(), `INSERT INTO remote_media_items(id,library_id,external_item_id,external_media_id,group_key,library_type,title,file_name,size_bytes)VALUES($1,$2,$3,$4,'synthetic-group','movie','Synthetic Title','synthetic.mkv',$5)`, item, library, familyJellyfinRawPrimary, familyJellyfinRawBacking, len(data)); err != nil {
			t.Fatal("seed own finite selected source")
		}
	}
	raw, _ := json.Marshal(value.settings)
	value.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if os.WriteFile(value.manager.ConfigPath(), raw, 0o600) != nil {
		t.Fatal("write own synthetic settings")
	}
	remote, err := remotemedia.NewService(f.store, value.manager, nil, nil)
	if err != nil {
		t.Fatal("construct genuine remote repository service")
	}
	value.handler, err = newFamilyJellyfinRawHandler(FamilyMediaDependencies{Store: f.store, Pins: f.pins, Local: installed.ParentFiles.local, Parent: installed.ParentFiles.parent}, value.manager, remote)
	if err != nil {
		t.Fatal("construct actual same-store private attachment handler")
	}
	t.Cleanup(value.handler.Close)
	original := value.handler.installation.Boundary.capture
	value.handler.installation.Boundary.capture = func(r *http.Request, target string) (vodOriginalOwner, error) {
		value.captures.Add(1)
		proof, err := original(r, target)
		if err != nil {
			return proof, err
		}
		actual, ok := proof.(*profileunlock.FamilyMediaOwner)
		if !ok {
			t.Fatal("constructor did not use actual PG owner facade")
		}
		return &familyJellyfinRawObservedOwner{FamilyMediaOwner: actual, pgClosed: &value.pgClosed}, nil
	}
	request, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
	value.unlock = f.issue(t, request)
	release()
	return value
}
func (f *familyJellyfinRawPG) rawRequest(t *testing.T, method string, item int) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, release := f.request(t, method, "/?profileId="+f.profile.ID)
	r.Header.Set(profileunlock.Header, f.unlock)
	return mux.SetURLVars(r, map[string]string{"itemID": fmt.Sprintf("remote-item-%d", item)}), release
}
func familyJellyfinRawServe(h *familyJellyfinRawHandler, w http.ResponseWriter, r *http.Request) (aborted bool) {
	defer func() {
		if value := recover(); value != nil {
			if value != http.ErrAbortHandler {
				panic(value)
			}
			aborted = true
		}
	}()
	h.ServeHTTP(w, r)
	return false
}

func TestFamilyJellyfinRawActualGETPublishesFinalChunkAndOriginalDeadline(t *testing.T) {
	for _, length := range []int{17, (64 << 10) + 13} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			data := bytes.Repeat([]byte{0x35}, length)
			f := newFamilyJellyfinRawPG(t, data, 1)
			r, release := f.rawRequest(t, "GET", 0)
			defer release()
			// Compare the original persisted PG expiry, not the pre-INSERT
			// nanosecond value returned by the fixture session constructor.
			storedSession, err := f.store.Sessions().Get(t.Context(), f.session.Token)
			if err != nil || storedSession == nil {
				t.Fatal("read original persisted synthetic session")
			}
			originalExpiry := storedSession.ExpiresAt
			w := newFamilyLocalRawRecorder()
			if familyJellyfinRawServe(f.handler, w, r) || w.Code != 200 || sha256.Sum256(w.Body.Bytes()) != sha256.Sum256(data) || f.wire.bad.Load() || f.wire.downloads.Load() != 1 {
				t.Fatal("actual authorized finite attachment or final chunk failed")
			}
			if w.Header().Get("Content-Length") != strconv.Itoa(length) || w.Header().Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") || w.Header().Get("Accept-Ranges") != "" || w.Header().Get("ETag") != "" || w.Header().Get("Location") != "" {
				t.Fatal("raw finite attachment leaked upstream framing/validator/redirect")
			}
			w.mu.Lock()
			deadlines := append([]time.Time(nil), w.deadlines...)
			w.mu.Unlock()
			found := false
			for _, deadline := range deadlines {
				found = found || deadline.Equal(originalExpiry)
				if deadline.After(originalExpiry) {
					t.Fatal("writer deadline exceeded original full-session expiry")
				}
			}
			if !found {
				t.Fatalf("original persisted expiry %s absent from actual writer deadlines %v", originalExpiry, deadlines)
			}
			if f.pgClosed.Load() != 1 {
				t.Fatal("actual original PG cleanup did not complete")
			}
		})
	}
}

func TestFamilyJellyfinRawActualMethodsAndInvalidSelectorsAcquireNoMedia(t *testing.T) {
	f := newFamilyJellyfinRawPG(t, []byte("must never acquire"), 1)
	for _, method := range []string{"HEAD", "POST", "OPTIONS"} {
		r, release := f.rawRequest(t, method, 0)
		w := newFamilyLocalRawRecorder()
		f.handler.ServeHTTP(w, r)
		release()
		if w.Code != 405 && !(method == "OPTIONS" && w.Code == 204) || f.captures.Load() != 0 || f.wire.requests.Load() != 0 {
			t.Fatal("method gate entered the actual authority/source path")
		}
	}
	for _, query := range []string{"token=synthetic", "profileName=other", "profileId=one&profileId=one", "path=http://unowned.invalid"} {
		r, release := f.rawRequest(t, "GET", 0)
		r.URL.RawQuery = query
		w := newFamilyLocalRawRecorder()
		f.handler.ServeHTTP(w, r)
		release()
		if w.Code != 400 || f.captures.Load() != 0 || f.wire.requests.Load() != 0 {
			t.Fatal("invalid actual GET selector entered the source path")
		}
	}
}

func TestFamilyJellyfinRawActualAccessErrorsPrecedeMediaHeaders(t *testing.T) {
	for _, mode := range []string{"missing PIN", "wrong PIN", "child", "foreign profile", "scoped session", "missing item", "no token user", "upstream denial", "writer unsupported"} {
		t.Run(mode, func(t *testing.T) {
			f := newFamilyJellyfinRawPG(t, []byte("must not leak finite media"), 1)
			r, release := f.rawRequest(t, "GET", 0)
			defer release()
			switch mode {
			case "missing PIN":
				r.Header.Del(profileunlock.Header)
			case "wrong PIN":
				r.Header.Set(profileunlock.Header, "synthetic-unverified-unlock")
			case "child":
				r.URL.RawQuery = "profileId=" + f.child
			case "foreign profile":
				profile, err := f.users.CreateForAccount("other", "Synthetic foreign profile")
				if err != nil {
					t.Fatal("create own foreign profile fixture")
				}
				r.URL.RawQuery = "profileId=" + profile.ID
			case "scoped session":
				session := f.session
				session.Token = "SYNTHETIC_JELLYFIN_RAW_SCOPED_SESSION"
				session.ClientID = "synthetic-client"
				session.Scope = models.SessionScopeStream
				session.ScopeResource = "jellyfinmedia:remote-item-0"
				if f.store.Sessions().Create(t.Context(), &session) != nil {
					t.Fatal("create actual own scoped session")
				}
				fresh := mux.SetURLVars(httptest.NewRequest("GET", "/?profileId="+f.profile.ID, nil), map[string]string{"itemID": "remote-item-0"})
				checked, stop, err := f.pins.Authenticate(fresh, session.Token, session.ClientID)
				if err != nil {
					t.Fatal("authenticate actual scoped fixture")
				}
				defer stop()
				r = checked
				r.Header.Set(profileunlock.Header, f.unlock)
			case "missing item":
				r = mux.SetURLVars(r, map[string]string{"itemID": "missing-item"})
			case "no token user":
				f.wire.mode.Store(1)
			case "upstream denial":
				f.wire.mode.Store(5)
			}
			w := newFamilyLocalRawRecorder()
			var target http.ResponseWriter = w
			if mode == "writer unsupported" {
				target = w.ResponseRecorder
			}
			if familyJellyfinRawServe(f.handler, target, r) || w.Code == 200 || w.Header().Get("Content-Disposition") != "" || w.Header().Get("Content-Length") != "" || bytes.Equal(w.Body.Bytes(), f.wire.data) {
				t.Fatal("access/open/writer error published media headers or bytes")
			}
			for _, private := range []string{familyJellyfinRawToken, f.wire.server.URL, f.session.Token} {
				if strings.Contains(w.Body.String(), private) {
					t.Fatal("sanitized error exposed original credentials or origin")
				}
			}
		})
	}
}

type familyJellyfinRawLateReader struct {
	reader io.Reader
	change func()
	buffer []byte
}

func (r *familyJellyfinRawLateReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.buffer = p
	if n > 0 {
		r.change()
	}
	return n, err
}

func TestFamilyJellyfinRawActualLateChunkIsClearedBeforeOuterWrite(t *testing.T) {
	for _, change := range []string{"PIN", "ACL", "same-value Save", "backup replacement"} {
		t.Run(change, func(t *testing.T) {
			f := newFamilyJellyfinRawPG(t, bytes.Repeat([]byte("authorized before mutation"), 4096), 1)
			r, release := f.rawRequest(t, "GET", 0)
			defer release()
			w := newFamilyLocalRawRecorder()
			q, ok := f.handler.installation.Boundary.Begin(w, r, "")
			if !ok {
				t.Fatal("capture actual full-session boundary")
			}
			defer q.Close()
			runtime, err := q.adoptRuntime(r)
			if err != nil {
				t.Fatal("adopt actual original request")
			}
			consumer, _, err := runtime.beginConsumer(w, r, q.target)
			if err != nil {
				t.Fatal("admit actual original consumer")
			}
			stream, ready, err := f.handler.open(runtime, consumer.Context(), "remote-item-0")
			if err != nil {
				t.Fatal("open genuine PG/config/HTTP source")
			}
			body, err := consumer.OpenBody(func(context.Context) (io.ReadCloser, error) { return ready, nil })
			if err != nil {
				t.Fatal("register original already-open body")
			}
			defer body.Close()
			writer, err := consumer.ResponseWriter(w)
			if err != nil {
				t.Fatal("create actual checked writer")
			}
			late := &familyJellyfinRawLateReader{reader: body, change: func() {
				switch change {
				case "PIN":
					if _, err := f.users.SetPin(f.profile.ID, "2468"); err != nil {
						t.Fatal("change actual original PG PIN after inner read")
					}
				case "ACL":
					if _, err := f.store.Pool().Exec(t.Context(), `UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='remote-library-0'`); err != nil {
						t.Fatal("change own actual original ACL after inner read")
					}
				case "same-value Save":
					if f.manager.Save(f.settings) != nil {
						t.Fatal("actual same-value managed write after inner read")
					}
				case "backup replacement":
					raw, _ := json.Marshal(f.settings)
					staged := f.manager.ConfigPath() + ".staged"
					if os.WriteFile(staged, raw, 0o600) != nil || config.ReplaceManagedSettingsFile(staged, f.manager.ConfigPath()) != nil {
						t.Fatal("actual managed replacement after inner read")
					}
				}
			}}
			if familyJellyfinRawCopy(consumer, stream, writer, late, stream.Info().Length) == nil || w.Body.Len() != 0 || len(late.buffer) != 32<<10 || !bytes.Equal(late.buffer, make([]byte, len(late.buffer))) {
				t.Fatal("late real source chunk escaped original outer authorization/staging bound")
			}
		})
	}
}

type familyJellyfinRawBlockedWriter struct {
	*httptest.ResponseRecorder
	entered, shortened, release, returned chan struct{}
	enterOnce, shortOnce, freeOnce        sync.Once
	mu                                    sync.Mutex
	deadlines                             []time.Time
}

func newFamilyJellyfinRawBlockedWriter(t *testing.T) *familyJellyfinRawBlockedWriter {
	t.Helper()
	w := &familyJellyfinRawBlockedWriter{ResponseRecorder: httptest.NewRecorder(), entered: make(chan struct{}), shortened: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{})}
	t.Cleanup(w.unblock)
	return w
}
func (w *familyJellyfinRawBlockedWriter) unblock() { w.freeOnce.Do(func() { close(w.release) }) }
func (w *familyJellyfinRawBlockedWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	w.mu.Unlock()
	if !deadline.After(time.Now().Add(time.Second)) {
		w.shortOnce.Do(func() { close(w.shortened) })
		<-w.release // Controlled actual deadline callback completion too.
	}
	return nil
}
func (w *familyJellyfinRawBlockedWriter) Write([]byte) (int, error) {
	w.enterOnce.Do(func() { close(w.entered) })
	<-w.release
	close(w.returned)
	return 0, os.ErrDeadlineExceeded
}
func familyJellyfinRawWait(t *testing.T, done <-chan struct{}, duration time.Duration, reason string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(duration):
		t.Fatal(reason)
	}
}
func familyJellyfinRawConsumers() int {
	familyMediaConsumerBudget.Lock()
	defer familyMediaConsumerBudget.Unlock()
	return familyMediaConsumerBudget.active
}
func familyJellyfinRawActive(h *familyJellyfinRawHandler) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.active)
}

func TestFamilyJellyfinRawActualUpstreamLossUnblocksOriginalWriter(t *testing.T) {
	f := newFamilyJellyfinRawPG(t, bytes.Repeat([]byte("actual staged bytes"), 4096), 1)
	r, release := f.rawRequest(t, "GET", 0)
	defer release()
	w := newFamilyJellyfinRawBlockedWriter(t)
	served := make(chan bool, 1)
	go func() { served <- familyJellyfinRawServe(f.handler, w, r) }()
	familyJellyfinRawWait(t, w.entered, 10*time.Second, "actual authorized Write did not enter")
	if w.Code != 200 || f.wire.downloads.Load() != 1 {
		t.Fatal("blocked writer did not receive a real authorized media response")
	}
	f.wire.mode.Store(2) // Actual CanDownload loss; original PG rows stay unchanged.
	familyJellyfinRawWait(t, w.shortened, 7*time.Second, "stream-to-runtime bridge did not shorten actual blocked writer")
	if f.pgClosed.Load() != 0 || familyJellyfinRawActive(f.handler) != 1 || f.wire.downloads.Load() != 1 {
		t.Fatal("source loss refunded original request before actual downstream completion")
	}
	w.unblock()
	select {
	case aborted := <-served:
		if !aborted || w.Body.Len() != 0 || f.pgClosed.Load() != 1 {
			t.Fatal("late failed Write published bytes or skipped actual original cleanup")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("released actual writer/source cleanup did not join")
	}
}

func TestFamilyJellyfinRawActualConsumerAndShutdownJoinProducerAndWriter(t *testing.T) {
	for _, order := range []string{"producer first", "writer first"} {
		t.Run(order, func(t *testing.T) {
			f := newFamilyJellyfinRawPG(t, []byte("authorized final AND chunk"), 1)
			bridgeEntered, bridgeRelease := make(chan struct{}), make(chan struct{})
			var bridgeOnce, freeOnce sync.Once
			freeBridge := func() { freeOnce.Do(func() { close(bridgeRelease) }) }
			defer freeBridge()
			f.handler.bridge = func(ctx context.Context, cancel func()) func() {
				return familyMediaAfterFunc(ctx, func() {
					bridgeOnce.Do(func() { close(bridgeEntered) })
					<-bridgeRelease
					cancel() // Scheduling observer delegates the genuine cancel only.
				})
			}
			completions := make(chan (<-chan struct{}), 1)
			f.handler.completion = func(done <-chan struct{}) { completions <- done }
			beforeConsumers := familyJellyfinRawConsumers()
			r, release := f.rawRequest(t, "GET", 0)
			defer release()
			w := newFamilyJellyfinRawBlockedWriter(t)
			served := make(chan bool, 1)
			go func() { served <- familyJellyfinRawServe(f.handler, w, r) }()
			familyJellyfinRawWait(t, w.entered, 10*time.Second, "genuine PG/HTTP final Write never entered")
			if w.Code != 200 || f.wire.downloads.Load() != 1 {
				t.Fatal("AND fixture did not reach real authorized media publication")
			}
			var producerDone <-chan struct{}
			select {
			case producerDone = <-completions:
			case <-time.After(time.Second):
				t.Fatal("actual producer completion was not observed")
			}
			closed := make(chan struct{}, 2)
			go func() { f.handler.Close(); closed <- struct{}{} }()
			go func() { f.handler.Close(); closed <- struct{}{} }()
			familyJellyfinRawWait(t, bridgeEntered, 5*time.Second, "genuine producer bridge join did not enter")
			familyJellyfinRawWait(t, w.shortened, 5*time.Second, "original downstream deadline callback did not enter")
			if order == "producer first" {
				freeBridge()
				familyJellyfinRawWait(t, producerDone, 5*time.Second, "genuine producer did not fully finish after bridge release")
			} else {
				w.unblock()
				familyJellyfinRawWait(t, w.returned, 2*time.Second, "actual downstream Write did not return")
				select {
				case <-producerDone:
					t.Fatal("actual producer completed while its bridge join was held")
				default:
				}
			}
			// Leave time for the real consumer/runtime cleanup to run after
			// the first physical completion. An immediate snapshot could miss
			// the naked-stream consumer-refund bug before its goroutine runs.
			for until := time.Now().Add(200 * time.Millisecond); time.Now().Before(until); {
				if familyJellyfinRawConsumers() != beforeConsumers+1 || familyJellyfinRawActive(f.handler) != 1 || f.pgClosed.Load() != 0 {
					t.Fatal("consumer/registry/original PG refunded before BOTH actual producer and writer")
				}
				select {
				case <-closed:
					t.Fatal("concurrent handler.Close returned before the actual AND barrier")
				case <-time.After(2 * time.Millisecond):
				}
			}
			freeBridge()
			w.unblock()
			familyJellyfinRawWait(t, producerDone, 5*time.Second, "producer final completion did not join")
			for range 2 {
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("concurrent shutdown did not join producer and writer")
				}
			}
			select {
			case aborted := <-served:
				if !aborted {
					t.Fatal("canceled blocked response was reported successful")
				}
			case <-time.After(time.Second):
				t.Fatal("actual ServeHTTP did not return after cleanup")
			}
			if familyJellyfinRawConsumers() != beforeConsumers || familyJellyfinRawActive(f.handler) != 0 || f.pgClosed.Load() != 1 || w.Body.Len() != 0 {
				t.Fatal("actual completed AND ownership was not fully retired")
			}
		})
	}
}

func TestFamilyJellyfinRawActualEightRegistrySlotsAreNotTheTwoIntegrationLimit(t *testing.T) {
	f := newFamilyJellyfinRawPG(t, []byte("genuine eight-source chunk"), 8)
	beforeConsumers := familyJellyfinRawConsumers()
	writers := make([]*familyJellyfinRawBlockedWriter, 8)
	served := make(chan bool, 8)
	for i := range 8 {
		w := newFamilyJellyfinRawBlockedWriter(t)
		writers[i] = w
		r, release := f.rawRequest(t, "GET", i)
		go func() { defer release(); served <- familyJellyfinRawServe(f.handler, w, r) }()
	}
	for _, w := range writers {
		familyJellyfinRawWait(t, w.entered, 25*time.Second, "eight genuine distinct-integration writes did not all enter")
		if w.Code != 200 {
			t.Fatal("eight-slot fixture blocked an error response instead of genuine media")
		}
	}
	if familyJellyfinRawActive(f.handler) != 8 || f.captures.Load() != 8 || f.wire.downloads.Load() != 8 || familyJellyfinRawConsumers() != beforeConsumers+8 {
		t.Fatal("registry fixture stopped at a lower source limit instead of reaching genuine eight")
	}
	ninth, release := f.rawRequest(t, "GET", 0)
	w := newFamilyLocalRawRecorder()
	f.handler.ServeHTTP(w, ninth)
	release()
	if w.Code != 429 || f.captures.Load() != 8 || f.wire.downloads.Load() != 8 {
		t.Fatal("ninth handler request reached original capture or source capacity")
	}
	closed := make(chan struct{})
	go func() { f.handler.Close(); close(closed) }()
	for _, writer := range writers {
		familyJellyfinRawWait(t, writer.shortened, 5*time.Second, "shutdown failed to signal every actual writer before joins")
	}
	if familyJellyfinRawActive(f.handler) != 8 || f.pgClosed.Load() != 0 || familyJellyfinRawConsumers() != beforeConsumers+8 {
		t.Fatal("held actual writes refunded registry/consumer/PG ownership")
	}
	for _, writer := range writers {
		writer.unblock()
	}
	familyJellyfinRawWait(t, closed, 10*time.Second, "eight actual request/producer/writer completions did not join")
	for range 8 {
		select {
		case aborted := <-served:
			if !aborted {
				t.Fatal("canceled actual write was marked successful")
			}
		case <-time.After(time.Second):
			t.Fatal("actual admitted handler did not finish")
		}
	}
	if familyJellyfinRawActive(f.handler) != 0 || familyJellyfinRawConsumers() != beforeConsumers || f.pgClosed.Load() != 8 {
		t.Fatal("actual completed registry did not refund exact ownership")
	}
	late, releaseLate := f.rawRequest(t, "GET", 0)
	defer releaseLate()
	w = newFamilyLocalRawRecorder()
	f.handler.ServeHTTP(w, late)
	if w.Code != 503 || f.captures.Load() != 8 {
		t.Fatal("closed handler admitted a replacement original source")
	}
}

func TestFamilyJellyfinRawActualShutdownJoinsLateAcquisition(t *testing.T) {
	f := newFamilyJellyfinRawPG(t, []byte("never publish after acquisition cancel"), 1)
	f.wire.mode.Store(4)
	r, release := f.rawRequest(t, "GET", 0)
	defer release()
	w := newFamilyLocalRawRecorder()
	served := make(chan bool, 1)
	go func() { served <- familyJellyfinRawServe(f.handler, w, r) }()
	familyJellyfinRawWait(t, f.wire.entered, 5*time.Second, "actual upstream metadata acquisition did not enter")
	closed := make(chan struct{})
	go func() { f.handler.Close(); close(closed) }()
	familyJellyfinRawWait(t, closed, 5*time.Second, "shutdown did not join canceled original acquisition")
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("shutdown completed before late handler acquisition returned")
	}
	if f.wire.downloads.Load() != 0 || w.Header().Get("Content-Disposition") != "" || familyJellyfinRawActive(f.handler) != 0 || f.pgClosed.Load() != 1 {
		t.Fatal("canceled acquisition published/reopened media or skipped actual original cleanup")
	}
}
