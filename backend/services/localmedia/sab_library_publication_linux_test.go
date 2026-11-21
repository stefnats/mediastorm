//go:build linux

package localmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/datastore"
	"novastream/internal/importer"
	"novastream/internal/nzb/metadata"
	"novastream/internal/settingsbootstrap"
	"novastream/models"
	"novastream/services/streaming"
)

// This proxy transports the actual original PostgreSQL session to the existing
// synthetic Unix server. It can hold/drop only a real server COMMIT response;
// it never supplies a query result, role, identity, receipt or successful ACK.
type publicationPGWire struct {
	listener                    net.Listener
	target                      string
	armed, dropped              atomic.Bool
	hold                        bool
	seen, release, stop         chan struct{}
	seenOnce, releaseOnce, once sync.Once
	mu                          sync.Mutex
	connections                 map[net.Conn]struct{}
	pending                     sync.WaitGroup
	acceptDone                  chan struct{}
}

func publicationWire(t *testing.T, target string, hold bool) *publicationPGWire {
	t.Helper()
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	p := &publicationPGWire{listener: listener, target: target, hold: hold, seen: make(chan struct{}), release: make(chan struct{}), stop: make(chan struct{}), connections: map[net.Conn]struct{}{}, acceptDone: make(chan struct{})}
	t.Cleanup(p.Close)
	go func() {
		defer close(p.acceptDone)
		for {
			client, e := listener.Accept()
			if e != nil {
				return
			}
			p.mu.Lock()
			p.connections[client] = struct{}{}
			p.pending.Add(1)
			p.mu.Unlock()
			go p.serve(client)
		}
	}()
	return p
}
func (p *publicationPGWire) serve(client net.Conn) {
	defer p.pending.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	server, e := (&net.Dialer{}).DialContext(ctx, "unix", p.target)
	cancel()
	if e != nil {
		client.Close()
		p.mu.Lock()
		delete(p.connections, client)
		p.mu.Unlock()
		return
	}
	p.mu.Lock()
	p.connections[server] = struct{}{}
	p.mu.Unlock()
	copied := make(chan struct{})
	go func() { defer close(copied); _, _ = io.Copy(server, client); _ = server.Close() }()
	defer func() {
		client.Close()
		server.Close()
		<-copied
		p.mu.Lock()
		delete(p.connections, client)
		delete(p.connections, server)
		p.mu.Unlock()
	}()
	for {
		header := make([]byte, 5)
		if _, e := io.ReadFull(server, header); e != nil {
			return
		}
		size := binary.BigEndian.Uint32(header[1:])
		if size < 4 || size > 1<<20 {
			return
		}
		body := make([]byte, int(size)-4)
		if _, e := io.ReadFull(server, body); e != nil {
			return
		}
		if header[0] == 'C' && string(body) == "COMMIT\x00" && p.armed.CompareAndSwap(true, false) {
			p.seenOnce.Do(func() { close(p.seen) })
			if !p.hold {
				p.dropped.Store(true)
				return
			}
			select {
			case <-p.release:
			case <-p.stop:
				return
			}
		}
		if _, e := client.Write(header); e != nil {
			return
		}
		if _, e := client.Write(body); e != nil {
			return
		}
	}
}
func (p *publicationPGWire) unblock() { p.releaseOnce.Do(func() { close(p.release) }) }
func (p *publicationPGWire) Close() {
	p.once.Do(func() {
		close(p.stop)
		p.unblock()
		p.listener.Close()
		<-p.acceptDone
		p.mu.Lock()
		var peers []net.Conn
		for c := range p.connections {
			peers = append(peers, c)
		}
		p.mu.Unlock()
		for _, c := range peers {
			c.Close()
		}
		p.pending.Wait()
	})
}

type publicationFixture struct {
	store                                                *datastore.DataStore
	admin                                                *pgxpool.Pool
	startup                                              *settingsbootstrap.Startup
	manager                                              *config.Manager
	settings                                             config.Settings
	db                                                   *database.DB
	source                                               *importer.Service
	issuer                                               *datastore.SABLibraryPublicationIssuer
	publisher                                            *SABLibraryPublisher
	proxy                                                *publicationPGWire
	root, stageRoot, stageFile, schema, token, queuePath string
	payload                                              []byte
}

func publicationFixturePG(t *testing.T, proxyMode string) *publicationFixture {
	t.Helper()
	raw := os.Getenv("SETTINGS_COMMIT_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic Unix PG fixture required")
	}
	uri, e := url.Parse(raw)
	if e != nil || uri.Scheme != "postgresql" || uri.Host != "" || uri.User == nil || !strings.HasPrefix(uri.User.Username(), "synthetic_") || !filepath.IsAbs(uri.Query().Get("host")) {
		t.Fatal("synthetic Unix PostgreSQL required")
	}
	admin, e := pgxpool.New(t.Context(), raw)
	if e != nil {
		t.Fatal(e)
	}
	f := &publicationFixture{admin: admin, schema: fmt.Sprintf("synthetic_publication_%d", time.Now().UnixNano()), root: t.TempDir(), stageRoot: t.TempDir(), token: "synthetic-publication-master", payload: publicationTestMedia(t)}
	ownedRole, ownedSchema := false, false
	t.Cleanup(func() {
		if f.publisher != nil {
			f.publisher.Close()
		}
		if f.issuer != nil {
			f.issuer.Close()
		}
		if f.source != nil {
			_ = f.source.Close()
		}
		if f.db != nil {
			_ = f.db.Close()
		}
		if f.startup != nil {
			_ = f.startup.Close()
		}
		if f.store != nil {
			f.store.Close()
		}
		if f.proxy != nil {
			f.proxy.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if ownedSchema {
			if _, e := admin.Exec(ctx, "DROP SCHEMA "+pgx.Identifier{f.schema}.Sanitize()+" CASCADE"); e != nil {
				t.Error("drop synthetic schema", e)
			}
		}
		if ownedRole {
			if _, e := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{f.schema}.Sanitize()); e != nil {
				t.Error("drop synthetic role", e)
			}
		}
		admin.Close()
	})
	if _, e = admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{f.schema}.Sanitize()+" NOLOGIN"); e != nil {
		t.Fatal(e)
	}
	ownedRole = true
	if _, e = admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{f.schema}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	ownedSchema = true
	query := uri.Query()
	query.Set("search_path", pgx.Identifier{f.schema}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+f.schema)
	query.Set("application_name", f.schema)
	if proxyMode != "" {
		port := query.Get("port")
		if port == "" {
			port = "5432"
		}
		f.proxy = publicationWire(t, filepath.Join(query.Get("host"), ".s.PGSQL."+port), proxyMode == "hold")
		uri.Host = f.proxy.listener.Addr().String()
		query.Del("host")
		query.Del("port")
		query.Set("sslmode", "disable")
	}
	uri.RawQuery = query.Encode()
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	f.settings = config.DefaultSettings()
	f.settings.Database.URL = uri.String()
	f.settings.Database.MaxOpenConns = 1
	f.settings.Database.MaxIdleConns = 0
	enabled := true
	f.settings.SABnzbd.Enabled = &enabled
	f.settings.SABnzbd.FallbackHost = "http://synthetic.invalid"
	f.settings.SABnzbd.FallbackAPIKey = "synthetic-never-used"
	f.settings.SABnzbd.ImportStageTargetRoot = f.stageRoot
	f.settings.SABnzbd.ImportStageMaxBytes = 128 << 20
	if e = f.manager.Save(f.settings); e != nil {
		t.Fatal(e)
	}
	f.startup, e = settingsbootstrap.Begin(t.Context(), f.manager, "")
	if e != nil {
		t.Fatal(e)
	}
	f.store, e = f.startup.Migrate(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = f.startup.Finish(); e != nil {
		t.Fatal(e)
	}
	for _, step := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO accounts(id,username,password_hash,is_master)VALUES('actor','Synthetic Publisher','synthetic-not-used',true)`, nil},
		{`INSERT INTO users(id,account_id,name)VALUES('profile','actor','Synthetic Adult')`, nil},
		{`INSERT INTO sessions(token,account_id,is_master,client_id,expires_at)VALUES($1,'actor',false,'synthetic-publisher',clock_timestamp()+interval '1 hour')`, []any{f.token}},
		{`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('library','Synthetic Publication','other',$1)`, []any{f.root}},
		{`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('library','all')`, nil},
	} {
		if _, e = f.store.Pool().Exec(t.Context(), step.query, step.args...); e != nil {
			t.Fatal("seed actual publication authority", e)
		}
	}
	f.queuePath = filepath.Join(t.TempDir(), "stage.db")
	f.db, e = database.NewDB(database.Config{DatabasePath: f.queuePath})
	if e != nil {
		t.Fatal(e)
	}
	f.db.Connection().SetMaxOpenConns(8)
	f.source, e = importer.NewService(importer.ServiceConfig{}, metadata.NewMetadataService(t.TempDir()), f.db, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	operation := strings.Repeat("1", 32)
	directory := filepath.Join(f.stageRoot, operation)
	if e = os.Mkdir(directory, 0700); e != nil {
		t.Fatal(e)
	}
	f.stageFile = filepath.Join(directory, "data-0001.bin")
	if e = os.WriteFile(f.stageFile, f.payload, 0600); e != nil {
		t.Fatal(e)
	}
	info, e := os.Stat(f.stageRoot)
	if e != nil {
		t.Fatal(e)
	}
	stat := info.Sys().(*syscall.Stat_t)
	namespace := sha256.Sum256([]byte(fmt.Sprintf("sab-stage-root-v1:%d:%d", stat.Dev, stat.Ino)))
	sourceRaw, _ := json.Marshal([]string{f.settings.SABnzbd.FallbackHost, f.settings.SABnzbd.FallbackAPIKey})
	sourceHash := sha256.Sum256(sourceRaw)
	hash := sha256.Sum256(f.payload)
	manifest, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: int64(len(f.payload)), SHA256: hex.EncodeToString(hash[:])}})
	receipt, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: map[string]any{"version": 1, "attempt": strings.Repeat("2", 32), "source": hex.EncodeToString(sourceHash[:]), "jobId": "synthetic-job", "status": map[string]any{"queueId": 1, "state": "download_completed"}}, "title": "untrusted generic queue title", "tmdbId": "999"})
	metadataRaw := string(receipt)
	item := &database.ImportQueueItem{NzbPath: "synthetic-unused.nzb", Status: database.QueueStatusFailed, Metadata: &metadataRaw}
	if e = f.db.Repository.AddToQueue(item); e != nil || item.ID != 1 {
		t.Fatal("synthetic original receipt", e)
	}
	if _, e = f.db.Connection().Exec(`INSERT INTO sab_import_stages(operation_id,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count,state,file_manifest,created_at,updated_at)VALUES(?,1,?,?,?,? ,1,'copied',?,'synthetic','synthetic')`, operation, strings.Repeat("2", 32), hex.EncodeToString(sourceHash[:]), hex.EncodeToString(namespace[:]), len(f.payload), string(manifest)); e != nil {
		t.Fatal(e)
	}
	f.issuer, e = f.startup.NewSABLibraryPublicationIssuer(f.store, f.source)
	if e != nil {
		t.Fatal(e)
	}
	f.publisher, e = NewSABLibraryPublisher(f.issuer)
	if e != nil {
		t.Fatal(e)
	}
	return f
}

// End every real native issuer/domain before constructing the next isolated
// current fixture. Each owned schema/role is still removed by its original
// registered cleanup; no production state or callback replaces the runtime.
func closePublicationRuntime(t *testing.T, f *publicationFixture) {
	t.Helper()
	if f.publisher != nil {
		f.publisher.Close()
		f.publisher = nil
	}
	if f.issuer != nil {
		f.issuer.Close()
		f.issuer = nil
	}
	if f.source != nil {
		if e := f.source.Close(); e != nil {
			t.Fatal(e)
		}
		f.source = nil
	}
	if f.db != nil {
		if e := f.db.Close(); e != nil {
			t.Fatal(e)
		}
		f.db = nil
	}
	if f.startup != nil {
		if e := f.startup.Close(); e != nil {
			t.Fatal(e)
		}
		f.startup = nil
	}
	if f.store != nil {
		f.store.Close()
		f.store = nil
	}
}

func publicationTestMedia(t *testing.T) []byte {
	t.Helper()
	tool := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG")
	wanted := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG_SHA256")
	if !filepath.IsAbs(tool) || len(wanted) != 64 {
		t.Fatal("explicit pinned synthetic FFmpeg required")
	}
	binary, e := os.ReadFile(tool)
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256(binary)
	if hex.EncodeToString(hash[:]) != wanted {
		t.Fatal("fixture tool changed")
	}
	file := filepath.Join(t.TempDir(), "media.mkv")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, tool, "-v", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "color=c=blue:s=32x32:r=25:d=0.16", "-f", "lavfi", "-i", "sine=frequency=660:duration=0.16", "-map", "0:v:0", "-map", "1:a:0", "-c:v", "ffv1", "-threads", "1", "-c:a", "pcm_s16le", "-f", "matroska", file)
	command.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	if output, e := command.CombinedOutput(); e != nil {
		t.Fatal("generate synthetic media", e, string(output))
	}
	payload, e := os.ReadFile(file)
	if e != nil || len(payload) == 0 {
		t.Fatal(e)
	}
	return payload
}
func (f *publicationFixture) request(t *testing.T, method, suffix, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(method, "http://synthetic.invalid/admin/api/usenet/fallback/1/publication"+suffix, strings.NewReader(body))
	r.RequestURI = r.URL.RequestURI()
	r.RemoteAddr = "127.0.0.1:23456"
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	t.Cleanup(cancel)
	r = r.WithContext(ctx)
	checked, release, e := f.store.AuthenticateProfilePinRequest(r, f.token, "synthetic-publisher")
	if e != nil {
		t.Fatal("genuine original PG frame", e)
	}
	t.Cleanup(release)
	return checked
}
func (f *publicationFixture) intent(t *testing.T, r *http.Request) *datastore.SABLibraryPublicationIntent {
	t.Helper()
	intent, e := f.issuer.Capture(r, 1, []byte(`{"libraryId":"library","ordinals":[1]}`))
	if e != nil {
		t.Fatal("capture current master", e)
	}
	t.Cleanup(intent.Close)
	return intent
}
func (f *publicationFixture) copy(t *testing.T, r *http.Request, intent *datastore.SABLibraryPublicationIntent) *importer.SABPublicationCopy {
	t.Helper()
	container, e := detectFamilyLocalProbeFormat(sabPublicationFormatReader{r, intent, 1}, int64(len(f.payload)))
	if e != nil {
		t.Fatal(e)
	}
	copy, e := intent.Copy(r, []importer.SABPublicationFormat{{Ordinal: 1, Container: container}})
	if e != nil {
		t.Fatal("actual current-master copy", e)
	}
	t.Cleanup(copy.Close)
	return copy
}
func (f *publicationFixture) count(t *testing.T, table string) int {
	t.Helper()
	if table != "local_media_items" && table != "sab_library_publications" && table != "sab_library_publication_files" {
		t.Fatal("unexpected synthetic count table")
	}
	var n int
	if e := f.store.Pool().QueryRow(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func (f *publicationFixture) registered(t *testing.T) (*http.Request, *datastore.SABLibraryPublicationIntent, datastore.SABLibraryPublicationView) {
	t.Helper()
	r := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
	intent := f.intent(t, r)
	copy := f.copy(t, r, intent)
	view, e := intent.Register(r, copy)
	if e != nil {
		t.Fatal("known actual registration ACK", e)
	}
	return r, intent, view
}

func TestSABPublicationCompletionRegistration(t *testing.T) {
	for _, name := range []string{"actual-copy-fsync-before-one-use-consume", "foreign-context-source-operation-and-issuer-denied", "copied-zero-closed-and-replayed-completion-denied", "actual-create-only-unmatched-items-and-mappings", "item-and-relative-path-conflict-no-overwrite", "actual-current-master-session-and-library-change", "late-policy-grant-and-household-fence-change", "late-nowait-table-contention", "same-pool-one-no-connection-held-across-copy", "actual-pg-commit-ack-loss-no-success", "known-pg-ack-sqlite-final-unavailable-one-row", "consumed-failure-never-reused", "external-reference-and-malformed-container-denied"} {
		t.Run(name, func(t *testing.T) {
			mode := ""
			if name == "actual-pg-commit-ack-loss-no-success" {
				mode = "drop"
			}
			if name == "known-pg-ack-sqlite-final-unavailable-one-row" {
				mode = "hold"
			}
			f := publicationFixturePG(t, mode)
			if name == "same-pool-one-no-connection-held-across-copy" {
				r := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
				result, e := f.publisher.Publish(r, 1, []byte(`{"libraryId":"library","ordinals":[1]}`))
				if e != nil {
					t.Fatal("actual pool-one publication", e)
				}
				defer result.Close()
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				connection, e := f.store.Pool().Acquire(ctx)
				if e != nil {
					t.Fatal("publication retained pool-one connection across physical result lifetime", e)
				}
				if f.store.Pool().Config().MaxConns != 1 || len(result.View().Files) != 1 {
					connection.Release()
					t.Fatal("actual pool-one or physical result missing")
				}
				connection.Release()
				if result.Check(r) != nil {
					t.Fatal("original result lost current source")
				}
				return
			}
			if name == "external-reference-and-malformed-container-denied" {
				for _, kind := range []string{"text", "external-mp4"} {
					payload := []byte("this is ordinary non-media text")
					if kind == "external-mp4" {
						ftyp, moov := familyProbeMovieBoxes(false)
						payload = bytes.Join([][]byte{ftyp, familyProbeBox("mdat", []byte{1, 2, 3}), moov}, nil)
					}
					if e := os.WriteFile(f.stageFile, payload, 0600); e != nil {
						t.Fatal(e)
					}
					sum := sha256.Sum256(payload)
					wire, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}})
					if _, e := f.db.Connection().Exec(`UPDATE sab_import_stages SET expected_bytes=?,file_manifest=?`, len(payload), string(wire)); e != nil {
						t.Fatal(e)
					}
					r := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
					result, e := f.publisher.Publish(r, 1, []byte(`{"libraryId":"library","ordinals":[1]}`))
					if e == nil || result != nil {
						t.Fatal("actual authorized ReaderAt parser accepted malformed/external input")
					}
					if f.count(t, "local_media_items") != 0 || f.count(t, "sab_library_publications") != 0 {
						t.Fatal("rejected container reserved or registered")
					}
				}
				return
			}
			r := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
			intent := f.intent(t, r)
			copy := f.copy(t, r, intent)
			if name == "foreign-context-source-operation-and-issuer-denied" {
				otherIssuer, e := f.startup.NewSABLibraryPublicationIssuer(f.store, f.source)
				if e != nil {
					t.Fatal("second genuine issuer on same original domain", e)
				}
				defer otherIssuer.Close()
				foreignR := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
				foreign, e := otherIssuer.Capture(foreignR, 1, []byte(`{"libraryId":"library","ordinals":[1]}`))
				if e != nil {
					t.Fatal("genuine distinct original source capture", e)
				}
				defer foreign.Close()
				if view, e := foreign.Register(foreignR, copy); e == nil || view.Reference != "" {
					t.Fatal("foreign issuer/source completion accepted")
				}
				cloned := r.Clone(context.WithValue(r.Context(), struct{}{}, "changed-original"))
				if view, e := intent.Register(cloned, copy); e == nil || view.Reference != "" {
					t.Fatal("replacement original context accepted")
				}
				if f.count(t, "local_media_items") != 0 {
					t.Fatal("foreign attempt inserted items")
				}
				return
			}
			if name == "copied-zero-closed-and-replayed-completion-denied" {
				copied := new(importer.SABPublicationCopy)
				reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(copy).Elem())
				for _, bad := range []*importer.SABPublicationCopy{nil, {}, copied} {
					if view, e := intent.Register(r, bad); e == nil || view.Reference != "" {
						t.Fatal("zero/copied completion accepted")
					}
				}
				view, e := intent.Register(r, copy)
				if e != nil || len(view.Files) != 1 {
					t.Fatal(e)
				}
				if again, e := intent.Register(r, copy); e == nil || again.Reference != "" {
					t.Fatal("completion replayed")
				}
				copy.Close()
				if intent.CheckPublished(r) == nil || f.count(t, "local_media_items") != 1 {
					t.Fatal("closed completion remained current or replay mutated row")
				}
				return
			}
			if name == "actual-current-master-session-and-library-change" || name == "late-policy-grant-and-household-fence-change" {
				mutations := []string{`UPDATE accounts SET is_master=false WHERE id='actor'`, `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, `UPDATE local_media_libraries SET name='Changed actual library' WHERE id='library'`}
				if name == "late-policy-grant-and-household-fence-change" {
					mutations = []string{`UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='library'`, `INSERT INTO media_library_account_grants(library_id,account_id)VALUES('library','actor')`, `UPDATE users SET is_kids_profile=true WHERE id='profile'`}
				}
				for index, query := range mutations {
					actual, request, original, completion := f, r, intent, copy
					if index > 0 {
						actual = publicationFixturePG(t, "")
						request = actual.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
						original = actual.intent(t, request)
						completion = actual.copy(t, request, original)
					}
					var args []any
					if strings.Contains(query, "$1") {
						args = []any{actual.token}
					}
					if _, e := actual.store.Pool().Exec(t.Context(), query, args...); e != nil {
						t.Fatal(e)
					}
					if view, e := original.Register(request, completion); e == nil || view.Reference != "" {
						t.Fatal("independently retired actual authority/policy emitted registration", index)
					}
					if actual.count(t, "local_media_items") != 0 {
						t.Fatal("retired source inserted item")
					}
					original.Close()
					closePublicationRuntime(t, actual)
				}
				return
			}
			if name == "late-nowait-table-contention" || name == "consumed-failure-never-reused" {
				tx, e := f.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer tx.Rollback(context.Background())
				if _, e = tx.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.schema, "local_media_items"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); e != nil {
					t.Fatal(e)
				}
				started := time.Now()
				view, e := intent.Register(r, copy)
				if e == nil || view.Reference != "" || time.Since(started) > 3*time.Second {
					t.Fatal("actual late NOWAIT guard waited or emitted success", e)
				}
				if e = tx.Rollback(t.Context()); e != nil {
					t.Fatal(e)
				}
				if name == "consumed-failure-never-reused" {
					if view, e := intent.Register(r, copy); e == nil || view.Reference != "" {
						t.Fatal("failed consumed completion became reusable")
					}
				}
				if f.count(t, "local_media_items") != 0 {
					t.Fatal("late guard failure inserted item")
				}
				return
			}
			if name == "item-and-relative-path-conflict-no-overwrite" {
				var operation string
				if e := f.db.Connection().QueryRow(`SELECT operation_id FROM sab_library_publications`).Scan(&operation); e != nil {
					t.Fatal(e)
				}
				relative := filepath.Join(".mediastorm-publications", operation, "data-0001.bin")
				if _, e := f.store.Pool().Exec(t.Context(), `INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type)VALUES('foreign','library',$1,$2,'foreign-sentinel','other')`, relative, filepath.Join(f.root, "foreign-sentinel")); e != nil {
					t.Fatal(e)
				}
				if view, e := intent.Register(r, copy); e == nil || view.Reference != "" {
					t.Fatal("existing relative path overwritten")
				}
				var file string
				if e := f.store.Pool().QueryRow(t.Context(), `SELECT file_name FROM local_media_items WHERE id='foreign'`).Scan(&file); e != nil || file != "foreign-sentinel" || f.count(t, "local_media_items") != 1 || f.count(t, "sab_library_publication_files") != 0 {
					t.Fatal("create-only rollback changed foreign row", e)
				}
				return
			}
			if name == "actual-pg-commit-ack-loss-no-success" {
				f.proxy.armed.Store(true)
				view, e := intent.Register(r, copy)
				if !errors.Is(e, datastore.ErrSABLibraryPublicationUnknown) || view.Reference != "" || !f.proxy.dropped.Load() {
					t.Fatal("actual COMMIT ACK loss produced success or was not observed", e)
				}
				if f.count(t, "local_media_items") != 1 || f.count(t, "sab_library_publication_files") != 1 {
					t.Fatal("actual known backend commit missing after lost response")
				}
				return
			}
			if name == "known-pg-ack-sqlite-final-unavailable-one-row" {
				f.db.Connection().SetMaxOpenConns(2)
				f.db.Connection().SetMaxIdleConns(2)
				type outcome struct {
					view datastore.SABLibraryPublicationView
					err  error
				}
				done := make(chan outcome, 1)
				var held *sql.Conn
				started, joined := false, false
				defer func() {
					f.proxy.unblock()
					if held != nil {
						_, _ = held.ExecContext(context.Background(), "ROLLBACK")
						held.Close()
					}
					if started && !joined {
						<-done
					}
				}()
				f.proxy.armed.Store(true)
				started = true
				go func() { view, e := intent.Register(r, copy); done <- outcome{view, e} }()
				select {
				case <-f.proxy.seen:
				case <-time.After(5 * time.Second):
					t.Fatal("actual PG COMMIT response not held")
				}
				var e error
				held, e = f.db.Connection().Conn(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				if _, e = held.ExecContext(t.Context(), "BEGIN IMMEDIATE"); e != nil {
					t.Fatal(e)
				}
				f.proxy.unblock()
				var actual outcome
				select {
				case actual = <-done:
					joined = true
				case <-time.After(12 * time.Second):
					t.Fatal("actual final SQLite unavailability did not return/join")
				}
				if !errors.Is(actual.err, datastore.ErrSABLibraryPublicationUnknown) || actual.view.Reference != "" || f.proxy.dropped.Load() {
					t.Fatal("known PG ACK/final SQLite uncertainty misreported", actual.err)
				}
				if f.count(t, "local_media_items") != 1 {
					t.Fatal("known PG commit lost or duplicated")
				}
				cleanupCtx, cleanupCancel := context.WithTimeout(t.Context(), time.Second)
				defer cleanupCancel()
				restored, e := f.db.Connection().Conn(cleanupCtx)
				if e != nil {
					t.Fatal("journal connection not returned after actual cleanup", e)
				}
				defer restored.Close()
				var busy, synchronous int
				if e = restored.QueryRowContext(cleanupCtx, "PRAGMA busy_timeout").Scan(&busy); e != nil || busy != 30000 {
					t.Fatal("journal leaked its bounded busy timeout", e, busy)
				}
				if e = restored.QueryRowContext(cleanupCtx, "PRAGMA synchronous").Scan(&synchronous); e != nil || synchronous != 1 {
					t.Fatal("journal failed to restore original synchronous level", e, synchronous)
				}
				return
			}
			view, e := intent.Register(r, copy)
			if e != nil {
				t.Fatal("actual create-only registration", e)
			}
			if view.State != "registered" || len(view.Files) != 1 || f.count(t, "local_media_items") != 1 || f.count(t, "sab_library_publication_files") != 1 {
				t.Fatal("actual item+mapping+operation transaction missing")
			}
			item, e := f.store.LocalMedia().GetItem(t.Context(), view.Files[0].ItemID)
			if e != nil || item == nil || item.MatchStatus != models.LocalMediaMatchStatusUnmatched || item.Confidence != 0 || item.Metadata != nil || item.Probe != nil || item.MatchedTitleID != "" || item.DetectedTitle != "" || item.SeasonNumber != 0 || item.EpisodeNumber != 0 {
				t.Fatal("generic stage DATA invented title/episode/probe", e)
			}
			wire, e := json.Marshal(view)
			if e != nil || bytes.Contains(wire, []byte(f.root)) || bytes.Contains(wire, []byte(f.stageRoot)) || bytes.Contains(wire, []byte("sha256")) || bytes.Contains(wire, []byte("receipt")) {
				t.Fatal("wire view exposed private physical evidence")
			}
			var state string
			if e := f.db.Connection().QueryRow(`SELECT state FROM sab_library_publications`).Scan(&state); e != nil || state != "registered" {
				t.Fatal("known final operational ACK missing", e)
			}
			provider := NewProvider(&Service{store: f.store, repo: f.store.LocalMedia()})
			response, e := provider.Stream(t.Context(), streaming.Request{Path: BuildStreamPath(*item), Method: "GET", RangeHeader: "bytes=2-17"})
			if e != nil || response == nil || response.Body == nil {
				t.Fatal("existing provider could not open actual opaque stored file", e)
			}
			actual, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			if readErr != nil || closeErr != nil || response.Status != 206 || response.ContentLength != 16 || !bytes.Equal(actual, f.payload[2:18]) {
				t.Fatal("actual published range differs", readErr, closeErr)
			}
			if name != "actual-copy-fsync-before-one-use-consume" && name != "actual-create-only-unmatched-items-and-mappings" {
				t.Fatal("unhandled completion canary")
			}
		})
	}
}

func TestSABPublicationReconcile(t *testing.T) {
	for _, name := range []string{"fresh-current-master-no-old-http-resurrection", "old-reserve-binding-remains-unchanged", "durable-exact-files-register-with-fresh-authority", "unknown-pg-restart-finds-exact-registered-operation", "pending-or-unacknowledged-physical-receipt-manual", "same-name-hash-and-recycled-marker-not-adopted", "changed-source-root-or-destination-not-reconstructed", "deleted-library-or-item-not-recreated", "known-final-ack-state-never-recopies"} {
		t.Run(name, func(t *testing.T) {
			mode := ""
			if name == "unknown-pg-restart-finds-exact-registered-operation" {
				mode = "drop"
			}
			f := publicationFixturePG(t, mode)
			r := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
			intent := f.intent(t, r)
			copy := f.copy(t, r, intent)
			registered := name == "deleted-library-or-item-not-recreated" || name == "known-final-ack-state-never-recopies" || mode != ""
			if registered {
				if f.proxy != nil {
					f.proxy.armed.Store(true)
				}
				view, e := intent.Register(r, copy)
				if f.proxy != nil {
					if !errors.Is(e, datastore.ErrSABLibraryPublicationUnknown) || !f.proxy.dropped.Load() {
						t.Fatal("actual lost PG response missing", e)
					}
				} else if e != nil || view.State != "registered" {
					t.Fatal(e)
				}
			}
			var operation, binding string
			if e := f.db.Connection().QueryRow(`SELECT operation_id FROM sab_library_publications`).Scan(&operation); e != nil {
				t.Fatal(e)
			}
			if e := f.store.Pool().QueryRow(t.Context(), `SELECT original_binding FROM sab_library_publications WHERE operation_id=$1`, operation).Scan(&binding); e != nil {
				t.Fatal(e)
			}
			target := filepath.Join(f.root, ".mediastorm-publications", operation, "data-0001.bin")
			before, e := os.Stat(target)
			if e != nil {
				t.Fatal(e)
			}
			intent.Close()
			switch name {
			case "fresh-current-master-no-old-http-resurrection":
				if view, e := intent.Reconcile(r); e == nil || view.Reference != "" {
					t.Fatal("closed original HTTP intent resurrected")
				}
			case "unknown-pg-restart-finds-exact-registered-operation":
				f.publisher.Close()
				f.issuer.Close()
				_ = f.source.Close()
				if e := f.db.Close(); e != nil {
					t.Fatal(e)
				}
				var e error
				f.db, e = database.NewDB(database.Config{DatabasePath: f.queuePath})
				if e != nil {
					t.Fatal(e)
				}
				f.source, e = importer.NewService(importer.ServiceConfig{}, metadata.NewMetadataService(t.TempDir()), f.db, nil, nil)
				if e != nil {
					t.Fatal(e)
				}
				f.issuer, e = f.startup.NewSABLibraryPublicationIssuer(f.store, f.source)
				if e != nil {
					t.Fatal(e)
				}
				f.publisher, e = NewSABLibraryPublisher(f.issuer)
				if e != nil {
					t.Fatal(e)
				}
			case "pending-or-unacknowledged-physical-receipt-manual":
				if _, e := f.db.Connection().Exec(`UPDATE sab_library_publications SET state='files_prepared',durable_receipt=NULL,durable_receipt_digest=NULL`); e != nil {
					t.Fatal(e)
				}
			case "same-name-hash-and-recycled-marker-not-adopted":
				marker := filepath.Join(f.root, ".mediastorm-publications", ".owner")
				raw, e := os.ReadFile(marker)
				if e != nil {
					t.Fatal(e)
				}
				if e = os.Rename(marker, marker+"-old"); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(marker, raw, 0600); e != nil {
					t.Fatal(e)
				}
			case "changed-source-root-or-destination-not-reconstructed":
				if e := os.WriteFile(target, bytes.Repeat([]byte("x"), len(f.payload)), 0600); e != nil {
					t.Fatal(e)
				}
			case "deleted-library-or-item-not-recreated":
				if _, e := f.store.Pool().Exec(t.Context(), `DELETE FROM local_media_items`); e != nil {
					t.Fatal(e)
				}
			}
			freshR := f.request(t, "POST", "/reconcile", `{"libraryId":"library","ordinals":[1]}`)
			fresh := f.intent(t, freshR)
			view, e := fresh.Reconcile(freshR)
			denied := name == "pending-or-unacknowledged-physical-receipt-manual" || name == "same-name-hash-and-recycled-marker-not-adopted" || name == "changed-source-root-or-destination-not-reconstructed" || name == "deleted-library-or-item-not-recreated"
			if denied {
				if e == nil || view.Reference != "" {
					t.Fatal("uncertain/deleted physical evidence reconstructed")
				}
				if f.count(t, "local_media_items") != 0 {
					t.Fatal("denied reconcile inserted/recreated item")
				}
				return
			}
			if e != nil || view.State != "registered" || len(view.Files) != 1 || f.count(t, "local_media_items") != 1 {
				t.Fatal("fresh genuine reconciliation failed", e)
			}
			after, e := os.Stat(target)
			if e != nil || !os.SameFile(before, after) || before.ModTime() != after.ModTime() {
				t.Fatal("reconcile re-copied or adopted another destination", e)
			}
			var retained string
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT original_binding FROM sab_library_publications WHERE operation_id=$1`, operation).Scan(&retained); e != nil || retained != binding {
				t.Fatal("fresh authority rewrote original HTTP binding", e)
			}
		})
	}
}

type publicationMetadataSpy struct{ calls atomic.Int32 }

func (s *publicationMetadataSpy) Search(context.Context, string, string) ([]models.SearchResult, error) {
	s.calls.Add(1)
	return nil, nil
}
func (*publicationMetadataSpy) MovieDetails(context.Context, models.MovieDetailsQuery) (*models.Title, error) {
	return nil, nil
}
func (*publicationMetadataSpy) SeriesDetails(context.Context, models.SeriesDetailsQuery) (*models.SeriesDetails, error) {
	return nil, nil
}
func TestSABPublicationScannerCoexistence(t *testing.T) {
	for _, name := range []string{"bin-leaves-not-candidates-before-registration", "registered-items-retained-subsequent-and-concurrent-scan", "ordinary-files-filter-cleanup-and-foreign-namespace-unchanged"} {
		t.Run(name, func(t *testing.T) {
			f := publicationFixturePG(t, "")
			spy := &publicationMetadataSpy{}
			service := &Service{store: f.store, repo: f.store.LocalMedia(), metadata: spy, ffprobePath: "/usr/bin/ffprobe", scans: map[string]scanState{}}
			if name == "ordinary-files-filter-cleanup-and-foreign-namespace-unchanged" {
				foreign := filepath.Join(f.root, ".mediastorm-publications")
				if e := os.Mkdir(foreign, 0700); e != nil {
					t.Fatal(e)
				}
				path := filepath.Join(foreign, "ordinary.mkv")
				if e := os.WriteFile(path, f.payload, 0600); e != nil {
					t.Fatal(e)
				}
				if service.publicationScanNamespace(t.Context(), "library", f.root) {
					t.Fatal("foreign namespace was silently excluded")
				}
				if _, e := f.store.Pool().Exec(t.Context(), `UPDATE local_media_libraries SET library_type='movie' WHERE id='library'`); e != nil {
					t.Fatal(e)
				}
				if _, e := service.RunScan(t.Context(), "library"); e != nil {
					t.Fatal(e)
				}
				if spy.calls.Load() == 0 || f.count(t, "local_media_items") != 1 {
					t.Fatal("ordinary foreign-namespace scan behavior changed")
				}
				if e := service.repo.DeleteItemsNotSeenInScan(t.Context(), "library", "other-scan"); e != nil || f.count(t, "local_media_items") != 0 {
					t.Fatal("ordinary unmapped stale cleanup was exempted", e)
				}
				return
			}
			r := f.request(t, "POST", "", `{"libraryId":"library","ordinals":[1]}`)
			intent := f.intent(t, r)
			copy := f.copy(t, r, intent)
			if name == "bin-leaves-not-candidates-before-registration" {
				files, e := collectVideoFiles(f.root)
				if e != nil || len(files) != 0 || spy.calls.Load() != 0 {
					t.Fatal("opaque unpublished bytes became scanner candidates", e)
				}
				if _, e = service.RunScan(t.Context(), "library"); e != nil || spy.calls.Load() != 0 {
					t.Fatal("unpublished source probed/matched", e)
				}
				if intent.Check(r) == nil {
					t.Fatal("concurrent actual scan did not retire original library tuple")
				}
				return
			}
			view, e := intent.Register(r, copy)
			if e != nil {
				t.Fatal(e)
			}
			intent.Close()
			if _, e = f.store.Pool().Exec(t.Context(), `UPDATE local_media_items SET match_status='manual',matched_title_id='synthetic-manual-data' WHERE id=$1`, view.Files[0].ItemID); e != nil {
				t.Fatal(e)
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			started := false
			joined := false
			defer func() {
				cancel()
				if started && !joined {
					<-done
				}
			}()
			started = true
			go func() {
				for {
					select {
					case <-ctx.Done():
						done <- nil
						return
					default:
						if e := service.repo.DeleteItemsNotSeenInScan(ctx, "library", "concurrent-stale-scan"); e != nil {
							if ctx.Err() != nil {
								done <- nil
							} else {
								done <- e
							}
							return
						}
					}
				}
			}()
			if _, e = service.RunScan(t.Context(), "library"); e != nil {
				t.Fatal(e)
			}
			cancel()
			e = <-done
			joined = true
			if e != nil {
				t.Fatal(e)
			}
			item, e := f.store.LocalMedia().GetItem(t.Context(), view.Files[0].ItemID)
			if e != nil || item == nil || item.MatchStatus != models.LocalMediaMatchStatusManual || item.MatchedTitleID != "synthetic-manual-data" || spy.calls.Load() != 0 {
				t.Fatal("actual concurrent scanner removed/rematched published item", e)
			}
		})
	}
}
