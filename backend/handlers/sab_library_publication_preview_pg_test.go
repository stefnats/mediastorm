//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/services/localmedia"
)

func publicationPreviewHTTPRoutes(f *publicationHTTPFixture) {
	f.router.Handle("/admin/api/usenet/fallback/{queueID}/publication/preview", f.handler.BoundedEntry(f.admin.RequireMasterAuth(f.handler.Preview))).Methods(http.MethodPost)
}
func publicationPreviewHTTPUnchanged(t *testing.T, f *publicationHTTPFixture) {
	t.Helper()
	var count int
	for _, table := range []string{"sab_library_publications", "sab_library_publication_files", "local_media_items"} {
		if e := f.base.base.pin.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&count); e != nil || count != 0 {
			t.Fatal("preview changed PG catalogue/journal", table, e, count)
		}
	}
	if e := f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&count); e != nil || count != 0 {
		t.Fatal("preview reserved or copied", e, count)
	}
	if _, e := os.Lstat(filepath.Join(f.root, ".mediastorm-publications")); !os.IsNotExist(e) {
		t.Fatal("preview created physical destination", e)
	}
}

// The writer forwards every actual HTTP operation. Its only observation port
// changes a real PG account after the successful initial current check and
// after Content-Type was set, before the second pre-header current check.
type publicationPreviewRetiringWriter struct {
	http.ResponseWriter
	once     sync.Once
	mutate   func() error
	err      error
	observed atomic.Bool
}

func (w *publicationPreviewRetiringWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *publicationPreviewRetiringWriter) Header() http.Header {
	header := w.ResponseWriter.Header()
	if header.Get("Content-Type") == "application/json" {
		w.once.Do(func() { w.err = w.mutate(); w.observed.Store(true) })
	}
	return header
}
func publicationPreviewExpiryHTTP(t *testing.T, f *publicationHTTPFixture) {
	t.Helper()
	if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=clock_timestamp()+interval '2 seconds' WHERE token=$1`, f.base.base.pin.session.Token); e != nil {
		t.Fatal(e)
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
	body := `{"libraryId":"publication-library","ordinals":[1]}`
	if _, e = fmt.Fprintf(client, "POST /admin/api/usenet/fallback/1/publication/preview HTTP/1.1\r\nHost: %s\r\nCookie: %s=%s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", server.Listener.Addr(), adminSessionCookieName, f.base.base.pin.session.Token, len(body), body); e != nil {
		t.Fatal(e)
	}
	var actual *publicationObservedConn
	select {
	case actual = <-listener.connections:
	case <-time.After(5 * time.Second):
		t.Fatal("actual accepted preview output missing")
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !actual.inWrite.Load() {
		select {
		case <-tick.C:
		case <-timer.C:
			t.Fatal("actual preview output never physically blocked")
		}
	}
	publicationHTTPWait(t, actual.failed, "earliest actual preview output expiry")
	publicationHTTPWait(t, done, "actual preview handler cleanup")
	publicationHTTPWait(t, actual.deadline, "actual output deadline")
	f.handler.mu.Lock()
	entries := len(f.handler.entries)
	f.handler.mu.Unlock()
	if entries != 0 {
		t.Fatal("expired preview retained early entry")
	}
	publicationPreviewHTTPUnchanged(t, f)
}
func publicationPreviewDefensiveStatus(t *testing.T, f *publicationHTTPFixture, server *httptest.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	original := httptest.NewRequest("POST", "http://synthetic.invalid/admin/api/usenet/fallback/1/publication", strings.NewReader(`{"libraryId":"publication-library","ordinals":[1]}`)).WithContext(ctx)
	original.RequestURI = original.URL.RequestURI()
	original.RemoteAddr = "127.0.0.1:23456"
	r, release, e := f.base.base.pin.pins.Authenticate(original, f.base.base.pin.session.Token, "")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	result, e := f.handler.publisher.Publish(r, 1, []byte(`{"libraryId":"publication-library","ordinals":[1]}`))
	if e != nil {
		t.Fatal("actual registered result", e)
	}
	defer result.Close()
	view := result.View()
	if view.LibraryID != "publication-library" || len(view.Ordinals) != 1 || view.Ordinals[0] != 1 || len(view.Files) != 1 {
		t.Fatal("actual selected record DATA missing")
	}
	view.Ordinals[0] = 128
	view.Files[0].ItemID = "forged"
	if result.View().Ordinals[0] != 1 || result.View().Files[0].ItemID == "forged" {
		t.Fatal("result View exposed mutable selection")
	}
	result.Close()
	release()
	get := httptest.NewRequest("GET", "http://synthetic.invalid/admin/api/usenet/fallback/1/publication", nil).WithContext(ctx)
	get.RequestURI = get.URL.RequestURI()
	get.RemoteAddr = "127.0.0.1:23456"
	checked, finish, e := f.base.base.pin.pins.Authenticate(get, f.base.base.pin.session.Token, "")
	if e != nil {
		t.Fatal(e)
	}
	defer finish()
	status, e := f.handler.publisher.ReadStatus(checked, 1)
	if e != nil {
		t.Fatal(e)
	}
	defer status.Close()
	saved := status.View()
	if saved.LibraryID != "publication-library" || len(saved.Ordinals) != 1 || saved.Ordinals[0] != 1 {
		t.Fatal("actual status lacks recorded selection")
	}
	saved.Ordinals[0] = 128
	if status.View().Ordinals[0] != 1 || status.Check(checked) != nil {
		t.Fatal("status DATA mutation changed original current record")
	}
	status.Close()
	finish()
	code, wire := f.request(t, server, "GET", "", "", true)
	var actual struct {
		LibraryID string `json:"libraryId"`
		Ordinals  []int  `json:"ordinals"`
		State     string `json:"state"`
	}
	if code != 200 || json.Unmarshal(wire, &actual) != nil || actual.LibraryID != "publication-library" || len(actual.Ordinals) != 1 || actual.Ordinals[0] != 1 || actual.State != "registered" {
		t.Fatal("fresh genuine HTTP status lost recorded selection", code, string(wire))
	}
	var raw string
	if e = f.base.base.pin.store.Pool().QueryRow(t.Context(), `SELECT selected_ordinals::text FROM sab_library_publications`).Scan(&raw); e != nil || raw != "[1]" {
		t.Fatal("descriptive View mutated actual PG record", e, raw)
	}
}
func TestSABPublicationPreviewHTTP(t *testing.T) {
	for _, name := range []string{"actual-cookie-preview-current-stage", "strict-selectors-no-source-effect", "current-master-denied-before-preview", "original-body-close-on-shutdown", "retired-before-header-generic503", "earliest-expiry-joins-output", "current-status-selection-defensive-data"} {
		t.Run(name, func(t *testing.T) {
			f := publicationHTTPPG(t)
			publicationPreviewHTTPRoutes(f)
			if name == "earliest-expiry-joins-output" {
				publicationPreviewExpiryHTTP(t, f)
				return
			}
			if name == "original-body-close-on-shutdown" {
				entered, bodyEnd, bodyClose, ended := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(ended)
					r.Body = &publicationObservedBody{ReadCloser: r.Body, entered: entered, ended: bodyEnd, closed: bodyClose}
					f.router.ServeHTTP(w, r)
				}))
				var conn net.Conn
				var closed chan struct{}
				t.Cleanup(func() {
					if conn != nil {
						conn.Close()
					}
					f.handler.Close()
					if closed != nil {
						<-closed
					}
					server.Close()
				})
				var e error
				conn, e = net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = fmt.Fprintf(conn, "POST /admin/api/usenet/fallback/1/publication/preview HTTP/1.1\r\nHost: %s\r\nCookie: %s=%s\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{", server.Listener.Addr(), adminSessionCookieName, f.base.base.pin.session.Token); e != nil {
					t.Fatal(e)
				}
				publicationHTTPWait(t, entered, "original preview body read")
				closed = make(chan struct{})
				go func() { f.handler.Close(); close(closed) }()
				publicationHTTPWait(t, closed, "preview runtime stop")
				publicationHTTPWait(t, bodyEnd, "actual held body read")
				publicationHTTPWait(t, bodyClose, "original body close")
				publicationHTTPWait(t, ended, "actual original handler")
				publicationPreviewHTTPUnchanged(t, f)
				return
			}
			if name == "retired-before-header-generic503" {
				var observed *publicationPreviewRetiringWriter
				ended := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(ended)
					observed = &publicationPreviewRetiringWriter{ResponseWriter: w, mutate: func() error {
						ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
						defer cancel()
						_, e := f.base.base.pin.store.Pool().Exec(ctx, `UPDATE accounts SET is_master=false WHERE id='actor'`)
						return e
					}}
					f.router.ServeHTTP(observed, r)
				}))
				t.Cleanup(func() { f.handler.Close(); server.Close() })
				code, raw := f.request(t, server, "POST", "/preview", `{"libraryId":"publication-library","ordinals":[1]}`, true)
				publicationHTTPWait(t, ended, "actual pre-header retirement response")
				if observed == nil || !observed.observed.Load() || observed.err != nil || code != 503 || bytes.Contains(raw, []byte(`"files"`)) || len(raw) == 0 {
					t.Fatal("actual pre-header retirement emitted success/empty200 or fake fixture boundary", code, observed)
				}
				publicationPreviewHTTPUnchanged(t, f)
				return
			}
			server := httptest.NewServer(f.router)
			t.Cleanup(server.Close)
			if name == "current-status-selection-defensive-data" {
				publicationPreviewDefensiveStatus(t, f, server)
				return
			}
			if name == "current-master-denied-before-preview" {
				if _, e := f.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=false WHERE id='actor'`); e != nil {
					t.Fatal(e)
				}
				code, _ := f.request(t, server, "POST", "/preview", `{"libraryId":"publication-library","ordinals":[1]}`, true)
				if code != 403 {
					t.Fatal("current master guard bypassed", code)
				}
				publicationPreviewHTTPUnchanged(t, f)
				return
			}
			if name == "strict-selectors-no-source-effect" {
				for _, body := range []string{`{"libraryId":"publication-library","ordinals":[1],"path":"/foreign"}`, `{"libraryId":"publication-library","ordinals":[1],"title":"Untrusted"}`, `{"libraryId":"publication-library","ordinals":[1,1]}`, `{"libraryId":"publication-library","libraryId":"other","ordinals":[1]}`} {
					code, _ := f.request(t, server, "POST", "/preview", body, true)
					if code != 400 {
						t.Fatal("invalid descriptive selectors accepted", code)
					}
				}
				publicationPreviewHTTPUnchanged(t, f)
				return
			}
			code, raw := f.request(t, server, "POST", "/preview", `{"libraryId":"publication-library","ordinals":[1]}`, true)
			var view localmedia.SABLibraryPublicationPreviewView
			if code != 200 || json.Unmarshal(raw, &view) != nil || view.QueueID != 1 || view.LibraryID != "publication-library" || len(view.Files) != 1 || view.Files[0].Ordinal != 1 || !view.Files[0].Eligible || view.Files[0].Container != "matroska" || view.Files[0].Bytes != int64(len(f.media)) {
				t.Fatal("actual cookie/current-stage byte preview unavailable", code, string(raw))
			}
			if bytes.Contains(raw, []byte(f.root)) || bytes.Contains(raw, []byte("sha256")) || bytes.Contains(raw, []byte("title")) {
				t.Fatal("preview leaked evidence or invented identity")
			}
			publicationPreviewHTTPUnchanged(t, f)
		})
	}
}
