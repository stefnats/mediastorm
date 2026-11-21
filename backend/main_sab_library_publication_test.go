//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/handlers"
	"novastream/internal/database"
	"novastream/internal/datastore"
	"novastream/internal/importer"
	"novastream/internal/nzb/metadata"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/utils"
)

// This real Main fixture is also usable by the follow-up preview/browser
// tests. It retains the original Startup, source, managed native domain and
// current PG cookie actor; no publisher or authorization result is replaced.
type mainSABPublicationFixture struct {
	base        *mainWebDAVFixture
	handler     *handlers.SABLibraryPublicationHandler
	source      *importer.Service
	db          *database.DB
	pins        *profileunlock.PGProfilePins
	admin       *handlers.AdminUIHandler
	token, root string
	payload     []byte
	router      *mux.Router
	entry       http.Handler
}

func mainSABPublicationPG(t *testing.T) *mainSABPublicationFixture {
	t.Helper()
	f := &mainSABPublicationFixture{base: mainWebDAVPG(t), root: t.TempDir()}
	var e error
	if _, e = f.base.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor'`); e != nil {
		t.Fatal(e)
	}
	if e = f.base.store.Pool().QueryRow(t.Context(), `SELECT token FROM sessions WHERE account_id='actor' ORDER BY created_at LIMIT 1`).Scan(&f.token); e != nil {
		t.Fatal(e)
	}
	stageRoot := t.TempDir()
	enabled := true
	f.base.settings.SABnzbd.Enabled = &enabled
	f.base.settings.SABnzbd.FallbackHost = "http://synthetic.invalid"
	f.base.settings.SABnzbd.FallbackAPIKey = "synthetic-unused"
	f.base.settings.SABnzbd.ImportStageTargetRoot = stageRoot
	f.base.settings.SABnzbd.ImportStageMaxBytes = 128 << 20
	if e = f.base.manager.Save(f.base.settings); e != nil {
		t.Fatal(e)
	}
	if _, e = f.base.store.Pool().Exec(t.Context(), `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('publication-library','Synthetic Publication','other',$1)`, f.root); e != nil {
		t.Fatal(e)
	}
	if _, e = f.base.store.Pool().Exec(t.Context(), `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('publication-library','all')`); e != nil {
		t.Fatal(e)
	}
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
	f.payload = mainSABPublicationMedia(t)
	mainSABPublicationStage(t, f, stageRoot)
	f.pins = profileunlock.NewPGProfilePins(f.base.store)
	ss, e := sessions.NewServiceWithStore(f.base.store, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	f.admin = handlers.NewAdminUIHandler("", "", nil, nil, nil, nil)
	f.admin.SetSessionsService(ss)
	f.admin.SetPGProfilePins(f.pins)
	f.handler, e = newSABLibraryPublicationRuntime(f.base.startup, f.base.store, f.source, f.pins)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.handler.Close)
	f.router = utils.NewRouter()
	registerSABLibraryPublicationRoutes(f.router, f.admin, f.handler)
	f.entry = utils.BasePathHandler("/app", f.router)
	return f
}
func mainSABPublicationMedia(t *testing.T) []byte {
	t.Helper()
	tool := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG")
	want := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG_SHA256")
	if !filepath.IsAbs(tool) || len(want) != 64 {
		t.Fatal("explicit pinned synthetic fixture FFmpeg required")
	}
	binary, e := os.ReadFile(tool)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(binary)
	if hex.EncodeToString(sum[:]) != want {
		t.Fatal("synthetic tool changed")
	}
	path := filepath.Join(t.TempDir(), "media.mkv")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, tool, "-v", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "color=c=blue:s=32x32:r=25:d=0.16", "-c:v", "ffv1", "-threads", "1", "-f", "matroska", path)
	cmd.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatal(e, string(out))
	}
	payload, e := os.ReadFile(path)
	if e != nil || len(payload) == 0 {
		t.Fatal(e)
	}
	return payload
}
func mainSABPublicationStage(t *testing.T, f *mainSABPublicationFixture, root string) {
	t.Helper()
	op := strings.Repeat("1", 32)
	directory := filepath.Join(root, op)
	if e := os.Mkdir(directory, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(directory, "data-0001.bin"), f.payload, 0600); e != nil {
		t.Fatal(e)
	}
	info, e := os.Stat(root)
	if e != nil {
		t.Fatal(e)
	}
	st := info.Sys().(*syscall.Stat_t)
	namespace := sha256.Sum256([]byte(fmt.Sprintf("sab-stage-root-v1:%d:%d", st.Dev, st.Ino)))
	sourceRaw, _ := json.Marshal([]string{f.base.settings.SABnzbd.FallbackHost, f.base.settings.SABnzbd.FallbackAPIKey})
	source := sha256.Sum256(sourceRaw)
	sum := sha256.Sum256(f.payload)
	manifest, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: int64(len(f.payload)), SHA256: hex.EncodeToString(sum[:])}})
	wire, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: map[string]any{"version": 1, "attempt": strings.Repeat("2", 32), "source": hex.EncodeToString(source[:]), "jobId": "synthetic-job", "status": map[string]any{"queueId": 1, "state": "download_completed"}}, "title": "Generic queue DATA"})
	raw := string(wire)
	item := &database.ImportQueueItem{NzbPath: "synthetic-unused.nzb", Status: database.QueueStatusFailed, Metadata: &raw}
	if e := f.db.Repository.AddToQueue(item); e != nil || item.ID != 1 {
		t.Fatal(e)
	}
	if _, e := f.db.Connection().Exec(`INSERT INTO sab_import_stages(operation_id,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count,state,file_manifest,created_at,updated_at)VALUES(?,1,?,?,?, ?,1,'copied',?,'synthetic','synthetic')`, op, strings.Repeat("2", 32), hex.EncodeToString(source[:]), hex.EncodeToString(namespace[:]), len(f.payload), string(manifest)); e != nil {
		t.Fatal(e)
	}
}
func (f *mainSABPublicationFixture) request(t *testing.T, server *httptest.Server, method, suffix, body string, origin string, cookie bool) (int, []byte) {
	t.Helper()
	r, e := http.NewRequestWithContext(t.Context(), method, server.URL+"/app/admin/api/usenet/fallback/1/publication"+suffix, strings.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	if cookie {
		r.AddCookie(&http.Cookie{Name: "strmr_admin_session", Value: f.token})
	}
	r.Header.Set("Content-Type", "application/json")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	client := server.Client()
	client.Timeout = 15 * time.Second
	response, e := client.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	raw, re := io.ReadAll(io.LimitReader(response.Body, 256<<10))
	ce := response.Body.Close()
	if re != nil || ce != nil {
		t.Fatal("actual Main HTTP final chunk failed", re, ce)
	}
	return response.StatusCode, raw
}

// Active output owns an actual registered result and its original current
// actor/source. The real TCP write is deliberately unread by its client.
func mainSABPublicationActiveShutdown(t *testing.T) {
	t.Helper()
	f := mainSABPublicationPG(t)
	healthy := httptest.NewServer(f.entry)
	code, raw := f.request(t, healthy, "POST", "", `{"libraryId":"publication-library","ordinals":[1]}`, healthy.URL, true)
	healthy.Close()
	var view datastore.SABLibraryPublicationView
	if code != 200 || json.Unmarshal(raw, &view) != nil || len(view.Files) != 1 {
		t.Fatal("actual active fixture publish failed", code, string(raw))
	}
	ended := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(ended)
		w.Header().Set("X-Synthetic-Held-Header", strings.Repeat("x", 256<<10))
		f.entry.ServeHTTP(w, r)
	}))
	listener := &mainWebDAVListener{Listener: server.Listener, connections: make(chan *mainWebDAVConn, 1)}
	server.Listener = listener
	server.Start()
	var client net.Conn
	var closed chan struct{}
	t.Cleanup(func() {
		if client != nil {
			client.Close()
		}
		f.handler.Close()
		if closed != nil {
			<-closed
		}
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
	fmt.Fprintf(client, "GET /app/admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nCookie: strmr_admin_session=%s\r\nConnection: close\r\n\r\n", server.Listener.Addr(), f.token)
	var conn *mainWebDAVConn
	select {
	case conn = <-listener.connections:
	case <-time.After(5 * time.Second):
		t.Fatal("actual accepted output connection missing")
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !conn.inWrite.Load() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("actual Main output was not physically held")
		}
	}
	closed = make(chan struct{})
	go func() { f.handler.Close(); close(closed) }()
	mainWebDAVWait(t, conn.writeFailed, "active physical write")
	mainWebDAVWait(t, ended, "active Main output")
	mainWebDAVWait(t, closed, "active publisher shutdown")
	var count int
	if e = f.base.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM local_media_items WHERE id=$1`, view.Files[0].ItemID).Scan(&count); e != nil || count != 1 {
		t.Fatal("known ACK row changed during actual shutdown", e, count)
	}
	if e = f.source.Close(); e != nil {
		t.Fatal(e)
	}
	if e = f.base.startup.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestMainSABLibraryPublicationRoutes(t *testing.T) {
	for _, name := range []string{"actual-admin-origin-basepath-and-methods", "missing-closed-or-foreign-dependencies-generic503", "startup-shutdown-joins-early-and-active-jobs"} {
		t.Run(name, func(t *testing.T) {
			f := mainSABPublicationPG(t)
			if name == "startup-shutdown-joins-early-and-active-jobs" {
				bodyRead, bodyEnd, bodyClosed, ended := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(ended)
					r.Body = &mainWebDAVBody{ReadCloser: r.Body, entered: bodyRead, ended: bodyEnd, closed: bodyClosed}
					f.entry.ServeHTTP(w, r)
				}))
				var conn net.Conn
				var closeDone chan struct{}
				t.Cleanup(func() {
					if conn != nil {
						conn.Close()
					}
					f.handler.Close()
					if closeDone != nil {
						<-closeDone
					}
					server.Close()
				})
				var e error
				conn, e = net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if e != nil {
					t.Fatal(e)
				}
				fmt.Fprintf(conn, "POST /app/admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nOrigin: %s\r\nCookie: strmr_admin_session=%s\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{", server.Listener.Addr(), server.URL, f.token)
				mainWebDAVWait(t, bodyRead, "Main pre-Capture body read")
				closeDone = make(chan struct{})
				go func() { f.handler.Close(); close(closeDone) }()
				mainWebDAVWait(t, closeDone, "publication shutdown")
				mainWebDAVWait(t, bodyEnd, "actual HTTP body read")
				mainWebDAVWait(t, bodyClosed, "actual body close")
				mainWebDAVWait(t, ended, "Main request ownership")
				var count int
				if e = f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&count); e != nil || count != 0 {
					t.Fatal("held early request reached source or Reserve", e, count)
				}
				// Main ordering is publisher/HTTP owners first, then importer/native store.
				if e = f.source.Close(); e != nil {
					t.Fatal(e)
				}
				if e = f.base.startup.Close(); e != nil {
					t.Fatal(e)
				}
				mainSABPublicationActiveShutdown(t)
				return
			}
			server := httptest.NewServer(f.entry)
			t.Cleanup(server.Close)
			if name == "missing-closed-or-foreign-dependencies-generic503" {
				for _, bad := range []struct {
					startup bool
					store   bool
					source  bool
					pins    bool
				}{{false, true, true, true}, {true, false, true, true}, {true, true, false, true}, {true, true, true, false}} {
					startup, store, source, pins := f.base.startup, f.base.store, f.source, f.pins
					if !bad.startup {
						startup = nil
					}
					if !bad.store {
						store = nil
					}
					if !bad.source {
						source = nil
					}
					if !bad.pins {
						pins = nil
					}
					if h, e := newSABLibraryPublicationRuntime(startup, store, source, pins); e == nil || h != nil {
						if h != nil {
							h.Close()
						}
						t.Fatal("missing managed dependency accepted")
					}
				}
				foreign, e := datastore.New(t.Context(), f.base.settings.Database.URL)
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				if h, e := newSABLibraryPublicationRuntime(f.base.startup, foreign, f.source, profileunlock.NewPGProfilePins(foreign)); e == nil || h != nil {
					if h != nil {
						h.Close()
					}
					t.Fatal("uncoordinated foreign store accepted")
				}
				f.handler.Close()
				code, _ := f.request(t, server, "GET", "", "", "", true)
				if code != 503 {
					t.Fatal("closed Main runtime fell through", code)
				}
				missing := mux.NewRouter()
				registerSABLibraryPublicationRoutes(missing, f.admin, nil)
				fallback := httptest.NewServer(utils.BasePathHandler("/app", missing))
				defer fallback.Close()
				for _, method := range []string{"GET", "POST"} {
					code, _ = f.request(t, fallback, method, "", `{"libraryId":"publication-library","ordinals":[1]}`, fallback.URL, true)
					if code != 503 {
						t.Fatal("missing Main runtime fell through", code)
					}
				}
				return
			}
			code, _ := f.request(t, server, "POST", "", `{"libraryId":"publication-library","ordinals":[1]}`, "http://foreign.invalid", true)
			if code != 403 {
				t.Fatal("actual Main origin guard bypassed", code)
			}
			code, _ = f.request(t, server, "GET", "", "", "", false)
			if code != 403 {
				t.Fatal("actual cookie guard bypassed", code)
			}
			code, _ = f.request(t, server, "PUT", "", `{}`, server.URL, true)
			if code != 405 {
				t.Fatal("unsupported method dispatched", code)
			}
			code, raw := f.request(t, server, "POST", "", `{"libraryId":"publication-library","ordinals":[1]}`, server.URL, true)
			var view datastore.SABLibraryPublicationView
			if code != 200 || json.Unmarshal(raw, &view) != nil || view.State != "registered" || len(view.Files) != 1 {
				t.Fatal("actual Main basepath publish failed", code, string(raw))
			}
			if bytes.Contains(raw, []byte(f.root)) || bytes.Contains(raw, []byte("sha256")) || view.Files[0].MatchStatus != "unmatched" {
				t.Fatal("Main output exposed private evidence or invented title")
			}
			code, raw = f.request(t, server, "GET", "", "", "", true)
			if code != 200 || !json.Valid(raw) {
				t.Fatal("actual Main status unavailable", code, string(raw))
			}
			// Raw HTTP framing is independently parsed; arbitrary XML/JSON substrings
			// cannot be confused with a second HTTP status line.
			conn, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
			if e != nil {
				t.Fatal(e)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			fmt.Fprintf(conn, "GET /app/admin/api/usenet/fallback/1/publication HTTP/1.1\r\nHost: %s\r\nCookie: strmr_admin_session=%s\r\nConnection: close\r\n\r\n", server.Listener.Addr(), f.token)
			reader := bufio.NewReader(conn)
			response, e := http.ReadResponse(reader, nil)
			if e != nil {
				t.Fatal(e)
			}
			body, re := io.ReadAll(response.Body)
			ce := response.Body.Close()
			tail, te := io.ReadAll(reader)
			if re != nil || ce != nil || te != nil || response.StatusCode != 200 || !json.Valid(body) || len(tail) != 0 {
				t.Fatal("healthy actual Main final chunk/framing invalid", re, ce, te, response.StatusCode, len(tail))
			}
		})
	}
}
