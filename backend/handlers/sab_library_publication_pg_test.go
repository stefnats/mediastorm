//go:build linux

package handlers

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/database"
	"novastream/internal/datastore"
	"novastream/internal/importer"
	"novastream/internal/nzb/metadata"
	"novastream/services/localmedia"
	"novastream/services/streaming"
)

type publicationHTTPFixture struct {
	base    *progressHTTPFixture
	db      *database.DB
	source  *importer.Service
	handler *SABLibraryPublicationHandler
	admin   *AdminUIHandler
	root    string
	media   []byte
	router  *mux.Router
}

func publicationHTTPPG(t *testing.T) *publicationHTTPFixture {
	t.Helper()
	f := &publicationHTTPFixture{base: progressHTTPPG(t, false), root: t.TempDir(), media: shareHLSTestSubtitleMedia(t)}
	pin := f.base.base.pin
	if _, e := pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor'`); e != nil {
		t.Fatal(e)
	}
	stageRoot := t.TempDir()
	enabled := true
	f.base.settings.SABnzbd.Enabled = &enabled
	f.base.settings.SABnzbd.FallbackHost = "http://synthetic.invalid"
	f.base.settings.SABnzbd.FallbackAPIKey = "synthetic-unused"
	f.base.settings.SABnzbd.ImportStageTargetRoot = stageRoot
	f.base.settings.SABnzbd.ImportStageMaxBytes = 128 << 20
	if e := f.base.manager.Save(f.base.settings); e != nil {
		t.Fatal(e)
	}
	if _, e := pin.store.Pool().Exec(t.Context(), `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('publication-library','Synthetic Publication','other',$1)`, f.root); e != nil {
		t.Fatal(e)
	}
	if _, e := pin.store.Pool().Exec(t.Context(), `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('publication-library','all')`); e != nil {
		t.Fatal(e)
	}
	var e error
	f.db, e = database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "publication.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.db.Close() })
	f.source, e = importer.NewService(importer.ServiceConfig{}, metadata.NewMetadataService(t.TempDir()), f.db, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.source.Close() })
	publicationHTTPStage(t, f.db, stageRoot, f.base.settings.SABnzbd.FallbackHost, f.base.settings.SABnzbd.FallbackAPIKey, f.media)
	issuer, e := datastore.NewSABLibraryPublicationIssuer(t.Context(), pin.store, f.source, f.base.manager, f.base.security, f.base.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	publisher, e := localmedia.NewSABLibraryPublisher(issuer)
	if e != nil {
		issuer.Close()
		t.Fatal(e)
	}
	f.handler, e = NewSABLibraryPublicationHandler(publisher)
	if e != nil {
		publisher.Close()
		t.Fatal(e)
	}
	t.Cleanup(f.handler.Close)
	f.admin = pin.admin()
	f.router = mux.NewRouter()
	for _, route := range []struct {
		suffix, method string
		call           http.HandlerFunc
	}{{"", "POST", f.handler.Publish}, {"", "GET", f.handler.Status}, {"/reconcile", "POST", f.handler.Reconcile}} {
		f.router.Handle("/admin/api/usenet/fallback/{queueID}/publication"+route.suffix, f.handler.BoundedEntry(f.admin.RequireMasterAuth(route.call))).Methods(route.method)
	}
	return f
}
func publicationHTTPStage(t *testing.T, db *database.DB, root, host, key string, payload []byte) {
	t.Helper()
	operation := strings.Repeat("1", 32)
	dir := filepath.Join(root, operation)
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "data-0001.bin"), payload, 0600); e != nil {
		t.Fatal(e)
	}
	info, e := os.Stat(root)
	if e != nil {
		t.Fatal(e)
	}
	st := info.Sys().(*syscall.Stat_t)
	namespace := sha256.Sum256([]byte(fmt.Sprintf("sab-stage-root-v1:%d:%d", st.Dev, st.Ino)))
	sourceJSON, _ := json.Marshal([]string{host, key})
	source := sha256.Sum256(sourceJSON)
	hash := sha256.Sum256(payload)
	manifest, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])}})
	raw, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: map[string]any{"version": 1, "attempt": strings.Repeat("2", 32), "source": hex.EncodeToString(source[:]), "jobId": "synthetic-job", "status": map[string]any{"queueId": 1, "state": "download_completed"}}, "title": "Untrusted queue title"})
	saved := string(raw)
	item := &database.ImportQueueItem{NzbPath: "synthetic-unused.nzb", Status: database.QueueStatusFailed, Metadata: &saved}
	if e := db.Repository.AddToQueue(item); e != nil || item.ID != 1 {
		t.Fatal(e)
	}
	if _, e := db.Connection().Exec(`INSERT INTO sab_import_stages(operation_id,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count,state,file_manifest,created_at,updated_at)VALUES(?,1,?,?,?, ?,1,'copied',?,'synthetic','synthetic')`, operation, strings.Repeat("2", 32), hex.EncodeToString(source[:]), hex.EncodeToString(namespace[:]), len(payload), string(manifest)); e != nil {
		t.Fatal(e)
	}
}
func publicationHTTPWait(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatal("actual " + label + " not joined")
	}
}
func (f *publicationHTTPFixture) request(t *testing.T, server *httptest.Server, method, suffix, body string, cookie bool) (int, []byte) {
	t.Helper()
	r, e := http.NewRequestWithContext(t.Context(), method, server.URL+"/admin/api/usenet/fallback/1/publication"+suffix, strings.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	r.Header.Set("Content-Type", "application/json")
	if cookie {
		r.AddCookie(&http.Cookie{Name: adminSessionCookieName, Value: f.base.base.pin.session.Token})
	}
	client := server.Client()
	client.Timeout = 12 * time.Second
	resp, e := client.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	raw, re := io.ReadAll(io.LimitReader(resp.Body, 256<<10))
	ce := resp.Body.Close()
	if re != nil || ce != nil {
		t.Fatal("actual HTTP response did not finish", re, ce)
	}
	return resp.StatusCode, raw
}
func (f *publicationHTTPFixture) publish(t *testing.T, server *httptest.Server) datastore.SABLibraryPublicationView {
	t.Helper()
	code, raw := f.request(t, server, "POST", "", `{"libraryId":"publication-library","ordinals":[1]}`, true)
	var view datastore.SABLibraryPublicationView
	if code != 200 || json.Unmarshal(raw, &view) != nil || view.State != "registered" || len(view.Files) != 1 || view.Files[0].ItemID == "" {
		t.Fatalf("genuine registered response: code=%d body=%s", code, raw)
	}
	return view
}

type publicationObservedBody struct {
	io.ReadCloser
	entered, ended, closed        chan struct{}
	enterOnce, endOnce, closeOnce sync.Once
}

func (b *publicationObservedBody) Read(p []byte) (int, error) {
	b.enterOnce.Do(func() { close(b.entered) })
	n, e := b.ReadCloser.Read(p)
	if e != nil {
		b.endOnce.Do(func() { close(b.ended) })
	}
	return n, e
}
func (b *publicationObservedBody) Close() error {
	e := b.ReadCloser.Close()
	b.closeOnce.Do(func() { close(b.closed) })
	return e
}

type publicationObservedConn struct {
	net.Conn
	inWrite                atomic.Bool
	failed, deadline       chan struct{}
	failOnce, deadlineOnce sync.Once
}

func (c *publicationObservedConn) Write(p []byte) (int, error) {
	c.inWrite.Store(true)
	defer c.inWrite.Store(false)
	n, e := c.Conn.Write(p)
	if e != nil {
		c.failOnce.Do(func() { close(c.failed) })
	}
	return n, e
}
func (c *publicationObservedConn) SetWriteDeadline(at time.Time) error {
	if !at.IsZero() && at.Before(time.Now().Add(time.Second)) {
		c.deadlineOnce.Do(func() { close(c.deadline) })
	}
	return c.Conn.SetWriteDeadline(at)
}

type publicationObservedListener struct {
	net.Listener
	connections chan *publicationObservedConn
}

func (l *publicationObservedListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(1024)
	}
	owned := &publicationObservedConn{Conn: c, failed: make(chan struct{}), deadline: make(chan struct{})}
	l.connections <- owned
	return owned, nil
}

func publicationHTTPHeldOutput(t *testing.T, f *publicationHTTPFixture, revoke bool) {
	t.Helper()
	healthy := httptest.NewServer(f.router)
	view := f.publish(t, healthy)
	healthy.Close()
	// Expire a fresh actual authenticated status request, never an already
	// captured actor. The positive publication response above was fully read.
	if !revoke {
		if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=clock_timestamp()+interval '2 seconds' WHERE token=$1`, f.base.base.pin.session.Token); e != nil {
			t.Fatal(e)
		}
	}
	done := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		w.Header().Set("X-Synthetic-Held-Header", strings.Repeat("x", 256<<10))
		f.router.ServeHTTP(w, r)
	}))
	listener := &publicationObservedListener{Listener: server.Listener, connections: make(chan *publicationObservedConn, 1)}
	server.Listener = listener
	server.Start()
	var client net.Conn
	t.Cleanup(func() {
		if client != nil {
			client.Close()
		}
		f.handler.Close()
		server.Close()
	})
	var e error
	client, e = net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	if tcp, ok := client.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	if _, e = fmt.Fprintf(client, "GET /admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nCookie: %s=%s\r\nConnection: close\r\n\r\n", server.Listener.Addr(), adminSessionCookieName, f.base.base.pin.session.Token); e != nil {
		t.Fatal(e)
	}
	var conn *publicationObservedConn
	select {
	case conn = <-listener.connections:
	case <-time.After(5 * time.Second):
		t.Fatal("actual connection missing")
	}
	until := time.NewTimer(5 * time.Second)
	defer until.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !conn.inWrite.Load() {
		select {
		case <-ticker.C:
		case <-until.C:
			t.Fatal("actual physical write never blocked")
		}
	}
	if revoke {
		if _, e = f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=false WHERE id='actor'`); e != nil {
			t.Fatal(e)
		}
	}
	publicationHTTPWait(t, conn.failed, "held original transport write")
	publicationHTTPWait(t, done, "output owner")
	publicationHTTPWait(t, conn.deadline, "physical output deadline")
	f.handler.mu.Lock()
	remaining := len(f.handler.entries)
	f.handler.mu.Unlock()
	if remaining != 0 {
		t.Fatal("output entry refunded before actual cleanup")
	}
	var count int
	if e = f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM local_media_items WHERE id=$1`, view.Files[0].ItemID).Scan(&count); e != nil || count != 1 {
		t.Fatal("known registration was rolled back or duplicated after output interruption", e, count)
	}
}

func TestSABPublicationRegisteredHTTP(t *testing.T) {
	for _, name := range []string{"actual-cookie-origin-and-master-route", "missing-managed-dependencies-no-fallback", "strict-selectors-no-posted-title-path-profile-or-hash", "actual-held-body-and-early-peer-admission", "actual-held-output-earliest-expiry-and-healthy-final-chunk", "current-change-and-runtime-shutdown-join", "saved-status-data-not-permission", "actual-existing-provider-range-readback-and-child-denial"} {
		t.Run(name, func(t *testing.T) {
			f := publicationHTTPPG(t)
			if name == "actual-held-output-earliest-expiry-and-healthy-final-chunk" || name == "current-change-and-runtime-shutdown-join" {
				publicationHTTPHeldOutput(t, f, name == "current-change-and-runtime-shutdown-join")
				f.handler.Close()
				return
			}
			if name == "actual-held-body-and-early-peer-admission" {
				bodies := make(chan *publicationObservedBody, 66)
				ended := make(chan struct{}, 66)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer func() { ended <- struct{}{} }()
					b := &publicationObservedBody{ReadCloser: r.Body, entered: make(chan struct{}), ended: make(chan struct{}), closed: make(chan struct{})}
					r.Body = b
					bodies <- b
					f.router.ServeHTTP(w, r)
				}))
				var clients []net.Conn
				var observed []*publicationObservedBody
				var closed chan struct{}
				t.Cleanup(func() {
					for _, c := range clients {
						c.Close()
					}
					f.handler.Close()
					if closed != nil {
						<-closed
					}
					server.Close()
				})
				for i := 0; i < 64; i++ {
					dialer := net.Dialer{Timeout: time.Second, LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, byte(i/8+1))}}
					c, e := dialer.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
					if e != nil {
						t.Fatal(e)
					}
					clients = append(clients, c)
					if _, e = fmt.Fprintf(c, "POST /admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nCookie: %s=%s\r\nContent-Length: 1000\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{", server.Listener.Addr(), adminSessionCookieName, f.base.base.pin.session.Token); e != nil {
						t.Fatal(e)
					}
					var b *publicationObservedBody
					select {
					case b = <-bodies:
					case <-time.After(5 * time.Second):
						t.Fatal("accepted original body missing")
					}
					observed = append(observed, b)
					publicationHTTPWait(t, b.entered, "held body read")
				}
				f.handler.mu.Lock()
				entries, peers := len(f.handler.entries), len(f.handler.peers)
				f.handler.mu.Unlock()
				if entries != 64 || peers != 8 {
					t.Fatal("early canonical peer admission not owned", entries, peers)
				}
				// Same-host port changes cannot bypass eight per actual peer.
				// A genuinely distinct ninth loopback peer still cannot exceed 64.
				c, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if e != nil {
					t.Fatal(e)
				}
				clients = append(clients, c)
				_ = c.SetDeadline(time.Now().Add(5 * time.Second))
				fmt.Fprintf(c, "POST /admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", server.Listener.Addr())
				response, e := http.ReadResponse(bufio.NewReader(c), nil)
				if e != nil {
					t.Fatal(e)
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != 503 {
					t.Fatal("actual same-peer ninth request admitted", response.StatusCode)
				}
				globalDialer := net.Dialer{Timeout: time.Second, LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 9)}}
				global, e := globalDialer.DialContext(t.Context(), "tcp", server.Listener.Addr().String())
				if e != nil {
					t.Fatal(e)
				}
				clients = append(clients, global)
				_ = global.SetDeadline(time.Now().Add(5 * time.Second))
				fmt.Fprintf(global, "POST /admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nContent-Length: 0\r\nConnection: close\r\n\r\n", server.Listener.Addr())
				globalResponse, e := http.ReadResponse(bufio.NewReader(global), nil)
				if e != nil {
					t.Fatal(e)
				}
				io.Copy(io.Discard, globalResponse.Body)
				globalResponse.Body.Close()
				if globalResponse.StatusCode != 503 {
					t.Fatal("actual 65th global entry admitted", globalResponse.StatusCode)
				}
				closed = make(chan struct{})
				go func() { f.handler.Close(); close(closed) }()
				publicationHTTPWait(t, closed, "early ingress shutdown")
				for _, b := range observed {
					publicationHTTPWait(t, b.ended, "real transport body read")
					publicationHTTPWait(t, b.closed, "real body close")
				}
				for range 66 {
					select {
					case <-ended:
					case <-time.After(5 * time.Second):
						t.Fatal("request still alive after runtime close")
					}
				}
				var n int
				if e = f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&n); e != nil || n != 0 {
					t.Fatal("held ingress reached reserve", e, n)
				}
				return
			}
			server := httptest.NewServer(f.router)
			t.Cleanup(server.Close)
			if name == "missing-managed-dependencies-no-fallback" {
				if value, e := NewSABLibraryPublicationHandler(nil); e == nil || value != nil {
					t.Fatal("missing publisher accepted")
				}
				f.handler.Close()
				code, _ := f.request(t, server, "POST", "", `{"libraryId":"publication-library","ordinals":[1]}`, true)
				if code != 503 {
					t.Fatal("closed handler fell through", code)
				}
				return
			}
			if name == "strict-selectors-no-posted-title-path-profile-or-hash" {
				for _, body := range []string{`{"libraryId":"publication-library","ordinals":[1],"title":"Movie"}`, `{"libraryId":"publication-library","ordinals":[1],"path":"/foreign"}`, `{"libraryId":"publication-library","ordinals":[1],"profileId":"other"}`, `{"libraryId":"publication-library","ordinals":[1],"sha256":"x"}`, `{"libraryId":"publication-library","ordinals":[1,1]}`, `{"libraryId":"publication-library","libraryId":"other","ordinals":[1]}`} {
					code, _ := f.request(t, server, "POST", "", body, true)
					if code != 400 {
						t.Fatal("posted identity accepted", code)
					}
				}
				var n int
				if e := f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&n); e != nil || n != 0 {
					t.Fatal("invalid DATA created reservation", e, n)
				}
				return
			}
			if name == "saved-status-data-not-permission" {
				code, _ := f.request(t, server, "GET", "", "", true)
				if code != 404 {
					t.Fatal("missing record fabricated", code)
				}
				f.publish(t, server)
				code, raw := f.request(t, server, "GET", "", "", true)
				if code != 200 || bytes.Contains(raw, []byte(f.root)) || bytes.Contains(raw, []byte("sha256")) || bytes.Contains(raw, []byte("token")) {
					t.Fatal("status leaked private ownership DATA", code)
				}
				code, _ = f.request(t, server, "GET", "", "", false)
				if code == 200 {
					t.Fatal("saved status authorized unauthenticated collector")
				}
				return
			}
			view := f.publish(t, server)
			if name == "actual-cookie-origin-and-master-route" {
				if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=false WHERE id='actor'`); e != nil {
					t.Fatal(e)
				}
				code, _ := f.request(t, server, "POST", "/reconcile", `{"libraryId":"publication-library","ordinals":[1]}`, true)
				if code != 403 {
					t.Fatal("current master revocation bypassed", code)
				}
				return
			}
			service, e := localmedia.NewService(f.base.base.pin.store, nil, "")
			if e != nil {
				t.Fatal(e)
			}
			item, e := service.GetItem(t.Context(), view.Files[0].ItemID)
			if e != nil {
				t.Fatal(e)
			}
			provider := localmedia.NewProvider(service)
			response, e := provider.Stream(t.Context(), streaming.Request{Method: "GET", Path: localmedia.BuildStreamPath(*item), RangeHeader: "bytes=0-15"})
			if e != nil || response == nil {
				t.Fatal("existing provider rejected registered bytes", e)
			}
			raw, re := io.ReadAll(response.Body)
			ce := response.Body.Close()
			if re != nil || ce != nil || response.Status != 206 || response.ContentLength != 16 || !bytes.Equal(raw, f.media[:16]) {
				t.Fatal("actual registered range response mismatch", re, ce)
			}
			if _, e = f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true,kids_mode='strict' WHERE id=$1`, f.base.base.pin.profile.ID); e != nil {
				t.Fatal(e)
			}
			original := httptest.NewRequest("GET", "http://synthetic.invalid/api/local", nil)
			checked, release, e := f.base.base.pin.pins.Authenticate(original, f.base.base.pin.session.Token, "")
			if e != nil {
				t.Fatal(e)
			}
			defer release()
			owner, e := f.base.base.pin.pins.CaptureFamilyMediaOwner(checked, f.base.base.pin.profile.ID)
			if e != nil {
				if !errors.Is(e, datastore.ErrFamilyMediaChildDenied) {
					t.Fatal("child guard failed for an unrelated fixture reason", e)
				}
				return
			}
			defer owner.Close()
			source, e := owner.CaptureLocalSource(checked.Context(), view.Files[0].ItemID)
			if source != nil {
				defer source.Close()
			}
			if e == nil {
				t.Fatal("unmatched unclassified publication became child-playable")
			}
		})
	}
}
