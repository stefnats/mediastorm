package importer

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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

	"novastream/config"
	"novastream/internal/database"
	nzbmetadata "novastream/internal/nzb/metadata"
)

type previewOnlyTransport struct {
	address string
	inner   *http.Transport
	denied  *atomic.Int32
}

func (p previewOnlyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "http" || r.URL.Host != p.address {
		p.denied.Add(1)
		return nil, errors.New("synthetic transport denied")
	}
	return p.inner.RoundTrip(r)
}

type previewFixture struct {
	service                   *Service
	db                        *database.DB
	dbPath, local             string
	mu                        sync.RWMutex
	cfg                       config.AltMountConfig
	remote                    string
	outputs                   map[string]string
	providerStatus            string
	submits, statuses, denied atomic.Int32
	afterStatus               func(*http.Request)
}

func newPreviewFixture(t *testing.T) *previewFixture {
	t.Helper()
	f := &previewFixture{local: filepath.Join(t.TempDir(), "private-completed"), remote: "/PRIVATE_REMOTE_COMPLETED", outputs: map[string]string{}, providerStatus: "Completed"}
	if e := os.MkdirAll(f.local, 0700); e != nil {
		t.Fatal(e)
	}
	listener, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal("synthetic loopback")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Error("unexpected synthetic path")
			w.WriteHeader(500)
			return
		}
		switch r.URL.Query().Get("mode") {
		case "addfile":
			n := f.submits.Add(1)
			if r.Method != "POST" {
				t.Error("submit not POST")
			}
			if e := r.ParseMultipartForm(1 << 20); e != nil {
				t.Error("multipart")
			}
			if r.MultipartForm != nil {
				defer r.MultipartForm.RemoveAll()
			}
			file, _, e := r.FormFile("nzbfile")
			if e != nil {
				t.Error("NZB multipart")
			} else {
				_, _ = io.Copy(io.Discard, file)
				file.Close()
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": true, "nzo_ids": []string{fmt.Sprintf("private_job_%d", n)}})
		case "queue":
			f.statuses.Add(1)
			if f.afterStatus != nil {
				f.afterStatus(r)
			}
			fmt.Fprint(w, `{"queue":{"slots":[]}}`)
		case "history":
			f.statuses.Add(1)
			job := r.URL.Query().Get("nzo_ids")
			f.mu.RLock()
			output, ok := f.outputs[job]
			status := f.providerStatus
			f.mu.RUnlock()
			if !ok {
				t.Error("unknown synthetic original job")
				w.WriteHeader(500)
				return
			}
			if f.afterStatus != nil {
				f.afterStatus(r)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"history": map[string]any{"slots": []any{map[string]any{"nzo_id": job, "status": status, "storage": output, "fail_message": "PRIVATE_ERROR_CANARY", "filename": "PRIVATE_TITLE_CANARY"}}}})
		default:
			t.Error("unexpected SAB operation")
			w.WriteHeader(500)
		}
	}))
	server.Listener = listener
	server.Start()
	t.Cleanup(server.Close)
	original := http.DefaultTransport
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != listener.Addr().String() {
			f.denied.Add(1)
			return nil, errors.New("literal synthetic dial denied")
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
	}}
	http.DefaultTransport = previewOnlyTransport{listener.Addr().String(), transport, &f.denied}
	t.Cleanup(func() { http.DefaultTransport = original; transport.CloseIdleConnections() })
	enabled := true
	f.cfg.SABnzbd = config.SABnzbdConfig{Enabled: &enabled, FallbackHost: server.URL, FallbackAPIKey: "synthetic-preview-private-key"}
	f.dbPath = filepath.Join(t.TempDir(), "queue.db")
	f.open(t)
	return f
}
func (f *previewFixture) open(t *testing.T) {
	t.Helper()
	db, e := database.NewDB(database.Config{DatabasePath: f.dbPath})
	if e != nil {
		t.Fatal(e)
	}
	f.db = db
	t.Cleanup(func() { db.Close() })
	service, e := NewService(ServiceConfig{}, nzbmetadata.NewMetadataService(filepath.Join(t.TempDir(), "virtual-metadata")), db, nil, func() *config.AltMountConfig { f.mu.RLock(); defer f.mu.RUnlock(); copy := f.cfg; return &copy })
	if e != nil {
		t.Fatal(e)
	}
	service.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	f.service = service
}
func (f *previewFixture) settings(ctx context.Context) (config.SABnzbdSettings, error) {
	if ctx.Err() != nil {
		return config.SABnzbdSettings{}, ctx.Err()
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	return config.SABnzbdSettings{Enabled: f.cfg.SABnzbd.Enabled, FallbackHost: f.cfg.SABnzbd.FallbackHost, FallbackAPIKey: f.cfg.SABnzbd.FallbackAPIKey, ImportPreviewRemoteRoot: f.remote, ImportPreviewLocalRoot: f.local}, nil
}
func (f *previewFixture) add(t *testing.T) (int64, string) {
	t.Helper()
	n := f.submits.Load() + 1
	name := fmt.Sprintf("job-%d", n)
	dir := filepath.Join(f.local, name)
	if e := os.Mkdir(dir, 0700); e != nil {
		t.Fatal(e)
	}
	file := filepath.Join(dir, "PRIVATE_TITLE.mkv")
	if e := os.WriteFile(file, []byte("synthetic metadata-only bytes"), 0600); e != nil {
		t.Fatal(e)
	}
	f.mu.Lock()
	f.outputs[fmt.Sprintf("private_job_%d", n)] = f.remote + "/" + name
	f.mu.Unlock()
	nzb := filepath.Join(t.TempDir(), "synthetic.nzb")
	if e := os.WriteFile(nzb, []byte("<nzb>synthetic payload</nzb>"), 0600); e != nil {
		t.Fatal(e)
	}
	foreign := `{"foreign":{"keep":[1,"value"]}}`
	item := &database.ImportQueueItem{NzbPath: nzb, Status: database.QueueStatusFailed, Metadata: &foreign}
	if e := f.db.Repository.AddToQueue(item); e != nil {
		t.Fatal(e)
	}
	if e := f.service.submitSABFallback(item); e != nil {
		t.Fatal(e)
	}
	status, e := f.service.RefreshSABFallbackStatus(t.Context(), item.ID)
	if e != nil || status.State != "download_completed" {
		t.Fatal("actual completed observation")
	}
	return item.ID, file
}
func previewPermit(ctx context.Context) error { return ctx.Err() }
func (f *previewFixture) call(ctx context.Context, id int64) (SABImportPreview, error) {
	return f.service.PreviewSABImport(ctx, id, f.settings, previewPermit)
}
func (f *previewFixture) mutate(t *testing.T, id int64, fn func(*sabFallbackReceipt)) {
	t.Helper()
	item, fields, receipt, e := f.service.fallbackItem(t.Context(), id)
	if e != nil || receipt == nil {
		t.Fatal("receipt")
	}
	fn(receipt)
	raw, e := json.Marshal(receipt)
	if e != nil {
		t.Fatal(e)
	}
	fields[database.SABFallbackMetadataKey] = raw
	raw, e = json.Marshal(fields)
	if e != nil {
		t.Fatal(e)
	}
	next := string(raw)
	ok, e := f.db.Repository.CompareAndSwapMetadata(t.Context(), id, item.Metadata, &next, false)
	if e != nil || !ok {
		t.Fatal("controlled receipt CAS")
	}
}

type previewWatch struct{ fd int }

func watchPreview(t *testing.T, path string) *previewWatch {
	t.Helper()
	fd, e := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
	if e != nil {
		t.Fatal("own metadata watch")
	}
	if _, e = syscall.InotifyAddWatch(fd, path, syscall.IN_OPEN|syscall.IN_ACCESS|syscall.IN_CLOSE_NOWRITE); e != nil {
		syscall.Close(fd)
		t.Fatal("watch own file")
	}
	w := &previewWatch{fd}
	t.Cleanup(func() { syscall.Close(fd) })
	return w
}
func (w *previewWatch) events() uint32 {
	var result uint32
	var buf [8192]byte
	for {
		n, e := syscall.Read(w.fd, buf[:])
		if e == syscall.EAGAIN || n <= 0 {
			return result
		}
		if e != nil {
			return result
		}
		for offset := 0; offset+16 <= n; {
			result |= binary.LittleEndian.Uint32(buf[offset+4:])
			offset += 16 + int(binary.LittleEndian.Uint32(buf[offset+12:]))
		}
	}
}
func TestSABImportPreviewActualProducerMetadataOnlyReopenAndSafeDTO(t *testing.T) {
	f := newPreviewFixture(t)
	id, file := f.add(t)
	if e := os.WriteFile(filepath.Join(filepath.Dir(file), "PRIVATE_URL.api_key_secret"), []byte("other"), 0600); e != nil {
		t.Fatal(e)
	}
	watch := watchPreview(t, file)
	before := f.submits.Load()
	beforeStatus := f.statuses.Load()
	result, e := f.call(t.Context(), id)
	if e != nil {
		t.Fatal(e)
	}
	if result.State != "local_inventory" || result.EntryCount != 2 || len(result.Files) != 2 || result.ObservedBytes != int64(len("synthetic metadata-only bytes")+5) || result.ObservedAt.IsZero() {
		t.Fatal("incomplete metadata observation")
	}
	if result.Files[0].Extension != "mkv" && result.Files[1].Extension != "mkv" {
		t.Fatal("media enum missing")
	}
	for _, row := range result.Files {
		if row.Kind != "media_candidate" && row.Extension != "other" {
			t.Fatal("arbitrary suffix exposed")
		}
	}
	raw, _ := json.Marshal(result)
	for _, secret := range []string{f.local, f.remote, "private_job", "PRIVATE_", "synthetic-preview-private-key", "storage", "outputPath"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("private preview bytes disclosed")
		}
	}
	events := watch.events()
	if events&syscall.IN_OPEN == 0 || events&syscall.IN_CLOSE_NOWRITE == 0 || events&syscall.IN_ACCESS != 0 {
		t.Fatalf("payload read or unclosed descriptor: %x", events)
	}
	if _, e = os.ReadFile(file); e != nil {
		t.Fatal(e)
	}
	if watch.events()&syscall.IN_ACCESS == 0 {
		t.Fatal("working read canary did not observe actual file read")
	}
	if f.submits.Load() != before || f.statuses.Load() != beforeStatus+2 || f.denied.Load() != 0 {
		t.Fatal("unexpected network work")
	}
	item, e := f.db.Repository.GetQueueItem(id)
	if e != nil || item.Status != database.QueueStatusFailed || !strings.Contains(*item.Metadata, `"foreign"`) {
		t.Fatal("preview changed import or foreign metadata")
	}
	f.db.Close()
	f.open(t)
	again, e := f.call(t.Context(), id)
	if e != nil || again.EntryCount != 2 || f.submits.Load() != before {
		t.Fatal("durable receipt reopen failed")
	}
}
func TestSABImportPreviewMappingAndReceiptPreconditionsZeroDemand(t *testing.T) {
	for _, mode := range []string{"disabled", "nil reader", "empty root", "relative root", "whole root", "backslash", "source changed", "no receipt", "not completed", "no job", "cancelled", "authority"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, _ := f.add(t)
			start := f.statuses.Load()
			reader := SABImportPreviewSettings(f.settings)
			permit := previewPermit
			ctx := t.Context()
			switch mode {
			case "disabled":
				disabled := false
				f.cfg.SABnzbd.Enabled = &disabled
			case "nil reader":
				reader = nil
			case "empty root":
				f.remote = ""
			case "relative root":
				f.local = "relative"
			case "whole root":
				f.local = "/"
			case "backslash":
				f.remote = "/root\\bad"
			case "source changed":
				f.mu.Lock()
				f.cfg.SABnzbd.FallbackAPIKey = "different-synthetic-key"
				f.mu.Unlock()
			case "no receipt":
				id = 99999
			case "not completed":
				f.mutate(t, id, func(r *sabFallbackReceipt) { r.Status.State = "downloading" })
			case "no job":
				f.mutate(t, id, func(r *sabFallbackReceipt) {
					r.JobID = ""
					r.Status.State = "submission_unknown"
					r.Status.Reason = "acknowledgement_unavailable"
				})
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "authority":
				permit = func(context.Context) error { return errors.New("retired original master") }
			}
			if _, e := f.service.PreviewSABImport(ctx, id, reader, permit); e == nil {
				t.Fatal("unknown mapping/receipt accepted")
			}
			if f.statuses.Load() != start {
				t.Fatal("denied preview contacted SAB")
			}
		})
	}
}
func TestSABImportPreviewUnsafeOrIncompleteDirectoryNeverReturnsSubset(t *testing.T) {
	for _, mode := range []string{"remote escape", "remote same root", "remote URL", "root symlink", "component symlink", "file symlink", "FIFO", "subdirectory", "129 entries", "one oversize", "total oversize"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, file := f.add(t)
			dir := filepath.Dir(file)
			switch mode {
			case "remote escape":
				f.outputs["private_job_1"] = f.remote + "/../other"
			case "remote same root":
				f.outputs["private_job_1"] = f.remote
			case "remote URL":
				f.outputs["private_job_1"] = "https://synthetic.invalid/private"
			case "root symlink":
				original := f.local
				f.local = filepath.Join(t.TempDir(), "link")
				if e := os.Symlink(original, f.local); e != nil {
					t.Fatal(e)
				}
			case "component symlink":
				moved := dir + "-original"
				if e := os.Rename(dir, moved); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(moved, dir); e != nil {
					t.Fatal(e)
				}
			case "file symlink":
				if e := os.Symlink(file, filepath.Join(dir, "link.mkv")); e != nil {
					t.Fatal(e)
				}
			case "FIFO":
				if e := syscall.Mkfifo(filepath.Join(dir, "fifo.ts"), 0600); e != nil {
					t.Fatal(e)
				}
			case "subdirectory":
				if e := os.Mkdir(filepath.Join(dir, "nested"), 0700); e != nil {
					t.Fatal(e)
				}
			case "129 entries":
				for n := 0; n < 128; n++ {
					if e := os.WriteFile(filepath.Join(dir, fmt.Sprintf("other-%03d.unknown", n)), nil, 0600); e != nil {
						t.Fatal(e)
					}
				}
			case "one oversize":
				if e := os.Truncate(file, sabPreviewMaxBytes+1); e != nil {
					t.Fatal(e)
				}
			case "total oversize":
				if e := os.Truncate(file, sabPreviewMaxBytes/2+1); e != nil {
					t.Fatal(e)
				}
				second := filepath.Join(dir, "other.ts")
				f, e := os.Create(second)
				if e != nil {
					t.Fatal(e)
				}
				if e = f.Truncate(sabPreviewMaxBytes/2 + 1); e != nil {
					t.Fatal(e)
				}
				f.Close()
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if result, e := f.call(ctx, id); e == nil || len(result.Files) != 0 {
				t.Fatal("unsafe or incomplete subset accepted")
			}
			if f.denied.Load() != 0 {
				t.Fatal("unexpected external dial")
			}
		})
	}
}
func TestSABImportPreviewLateHTTPRetirementBeforeFileOpen(t *testing.T) {
	for _, mode := range []string{"authority", "mapping", "credentials", "receipt", "provider not complete"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, file := f.add(t)
			watch := watchPreview(t, file)
			var once sync.Once
			var revoked atomic.Bool
			if mode != "provider not complete" {
				f.afterStatus = func(r *http.Request) {
					if r.URL.Query().Get("mode") != "history" {
						return
					}
					once.Do(func() {
						switch mode {
						case "authority":
							revoked.Store(true)
						case "mapping":
							f.mu.Lock()
							f.remote = "/changed-root"
							f.mu.Unlock()
						case "credentials":
							f.mu.Lock()
							f.cfg.SABnzbd.FallbackAPIKey = "changed-synthetic"
							f.mu.Unlock()
						case "receipt":
							f.mutate(t, id, func(r *sabFallbackReceipt) { r.Attempt = strings.Repeat("f", 32) })
						}
					})
				}
			} else {
				f.mu.Lock()
				f.providerStatus = "Failed"
				f.mu.Unlock()
			}
			permit := func(ctx context.Context) error {
				if revoked.Load() {
					return errors.New("original authority retired")
				}
				return ctx.Err()
			}
			result, e := f.service.PreviewSABImport(t.Context(), id, f.settings, permit)
			if e == nil || len(result.Files) != 0 {
				t.Fatal("retired snapshot published")
			}
			if watch.events()&syscall.IN_OPEN != 0 {
				t.Fatal("retirement opened file")
			}
		})
	}
}
func TestSABImportPreviewActualOpenedFileMutationAndRevocation(t *testing.T) {
	for _, mode := range []string{"replacement", "size", "FIFO replacement", "authority"} {
		t.Run(mode, func(t *testing.T) {
			f := newPreviewFixture(t)
			id, file := f.add(t)
			watch := watchPreview(t, file)
			var changed atomic.Bool
			permit := func(ctx context.Context) error {
				if watch.events()&syscall.IN_OPEN != 0 && changed.CompareAndSwap(false, true) {
					switch mode {
					case "replacement":
						if e := os.Rename(file, file+"-old"); e != nil {
							t.Fatal(e)
						}
						if e := os.WriteFile(file, []byte("replacement"), 0600); e != nil {
							t.Fatal(e)
						}
					case "size":
						if e := os.Truncate(file, 1); e != nil {
							t.Fatal(e)
						}
					case "FIFO replacement":
						if e := os.Remove(file); e != nil {
							t.Fatal(e)
						}
						if e := syscall.Mkfifo(file, 0600); e != nil {
							t.Fatal(e)
						}
					case "authority":
						return errors.New("original role revoked")
					}
				}
				return ctx.Err()
			}
			result, e := f.service.PreviewSABImport(t.Context(), id, f.settings, permit)
			if !changed.Load() || e == nil || len(result.Files) != 0 {
				t.Fatal("late actual FD mutation accepted or barrier missed")
			}
			if len(f.service.fallbackSlots) != 0 {
				t.Fatal("owned slot not released after actual close")
			}
		})
	}
}

func TestSABImportPreviewComplete128EntriesAndPassiveGet(t *testing.T) {
	f := newPreviewFixture(t)
	id, file := f.add(t)
	for n := 0; n < 127; n++ {
		if e := os.WriteFile(filepath.Join(filepath.Dir(file), fmt.Sprintf("other-%03d.unknown", n)), nil, 0600); e != nil {
			t.Fatal(e)
		}
	}
	watch := watchPreview(t, file)
	before := f.statuses.Load()
	if _, e := f.service.GetSABFallbackStatus(t.Context(), id); e != nil {
		t.Fatal(e)
	}
	if f.statuses.Load() != before || watch.events() != 0 {
		t.Fatal("passive status demanded network or media file")
	}
	result, e := f.call(t.Context(), id)
	if e != nil || result.EntryCount != 128 || len(result.Files) != 128 {
		t.Fatal("complete bounded inventory unavailable")
	}
}

func TestSABImportPreviewTwoOwnedSlotsUntilActualReturn(t *testing.T) {
	f := newPreviewFixture(t)
	first, a := f.add(t)
	second, b := f.add(t)
	third, _ := f.add(t)
	watches := []*previewWatch{watchPreview(t, a), watchPreview(t, b)}
	entered := make(chan int, 2)
	release := make(chan struct{})
	var once [2]atomic.Bool
	run := func(n int, id int64, ctx context.Context, done chan error) {
		_, e := f.service.PreviewSABImport(ctx, id, f.settings, func(work context.Context) error {
			if watches[n].events()&syscall.IN_OPEN != 0 && once[n].CompareAndSwap(false, true) {
				entered <- n
				<-release
			}
			return work.Err()
		})
		done <- e
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 2)
	go run(0, first, ctx, done)
	go run(1, second, t.Context(), done)
	for n := 0; n < 2; n++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("actual open barrier")
		}
	}
	if len(f.service.fallbackSlots) != 2 {
		close(release)
		t.Fatal("two operations did not retain slots")
	}
	before := f.statuses.Load()
	cancel()
	if _, e := f.call(t.Context(), third); !errors.Is(e, ErrSABFallbackBusy) {
		close(release)
		t.Fatal("third operation accepted while two descriptors held")
	}
	if f.statuses.Load() != before || len(f.service.fallbackSlots) != 2 {
		close(release)
		t.Fatal("cancel pretended actual descriptor cleanup")
	}
	close(release)
	failures := 0
	for n := 0; n < 2; n++ {
		select {
		case e := <-done:
			if e != nil {
				failures++
			}
		case <-time.After(3 * time.Second):
			t.Fatal("owned operations not joined")
		}
	}
	if failures != 1 || len(f.service.fallbackSlots) != 0 {
		t.Fatal("canceled operation or slot cleanup incorrect")
	}
	if _, e := f.call(t.Context(), third); e != nil {
		t.Fatal("slot not recovered after actual cleanup")
	}
}
