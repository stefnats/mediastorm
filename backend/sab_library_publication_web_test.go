//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"novastream/config"
	"novastream/handlers"
	"novastream/internal/database"
	"novastream/internal/datastore"
	"novastream/internal/importer"
	"novastream/services/accounts"
	"novastream/services/localmedia"
	"novastream/services/sessions"
	"novastream/services/users"
	"novastream/utils"
)

type publicationBrowserCounts struct {
	stageGets, stagePosts, previewPosts, publishPosts       atomic.Int32
	reconcilePosts, statusGets, librariesGets, refreshPosts atomic.Int32
}

func publicationBrowserServer(t *testing.T, guard *previewRouteTransport) *httptest.Server {
	t.Helper()
	f := mainSABPublicationPG(t)
	settings := f.base.settings
	settings.Server.BasePath = "/app"
	settings.UI.OnboardingCompleted = true
	settings.UI.AdminWalkthroughDismissed = true
	settings.SABnzbd.ImportPreviewRemoteRoot = "/synthetic-downloads"
	settings.SABnzbd.ImportPreviewLocalRoot = t.TempDir()
	if err := f.base.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	// These descriptive legacy projections are recorded from the actual held
	// synthetic stage root. They do not grant publication/source permission.
	root, err := os.Open(settings.SABnzbd.ImportStageTargetRoot)
	if err != nil {
		t.Fatal(err)
	}
	info, err := root.Stat()
	if err != nil || !info.IsDir() {
		root.Close()
		t.Fatal("actual synthetic stage root unavailable", err)
	}
	physical, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		root.Close()
		t.Fatal("actual synthetic stage root identity unavailable")
	}
	namespace := sha256.Sum256([]byte(fmt.Sprintf("sab-stage-root-v1:%d:%d", physical.Dev, physical.Ino)))
	pathHash := sha256.Sum256([]byte(settings.SABnzbd.ImportStageTargetRoot))
	if err = database.NewSABStageRepository(f.db.Connection()).BindObservedPath(t.Context(), hex.EncodeToString(pathHash[:]), hex.EncodeToString(namespace[:])); err != nil {
		root.Close()
		t.Fatal("record actual observed synthetic stage root", err)
	}
	named, err := os.Stat(settings.SABnzbd.ImportStageTargetRoot)
	if err != nil || !os.SameFile(info, named) {
		root.Close()
		t.Fatal("observed stage root changed", err)
	}
	if err = root.Close(); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err = f.db.Connection().Exec(`UPDATE sab_import_stages SET created_at=?,updated_at=?`, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	accountService, err := accounts.NewServiceWithStore(f.base.store)
	if err != nil {
		t.Fatal(err)
	}
	sessionService, err := sessions.NewServiceWithStore(f.base.store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	userService, err := users.NewServiceWithStore(f.base.store, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	libraries, err := localmedia.NewService(f.base.store, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	admin := handlers.NewAdminUIHandler(f.base.manager.ConfigPath(), "", nil, userService, nil, f.base.manager)
	admin.SetAccountsService(accountService)
	admin.SetSessionsService(sessionService)
	admin.SetPGProfilePins(f.pins)
	admin.SetLocalMediaService(libraries)
	fallback := handlers.NewUsenetFallbackHandler(f.source, sessionService, f.base.store.Accounts())
	fallback.SetImportPreviewSettings(func(ctx context.Context) (config.SABnzbdSettings, error) {
		if err := ctx.Err(); err != nil {
			return config.SABnzbdSettings{}, err
		}
		current, err := f.base.manager.Load()
		return current.SABnzbd, err
	})
	router := utils.NewRouter()
	registerUsenetFallbackRoutes(router, admin, fallback)
	registerSABLibraryPublicationRoutes(router, admin, f.handler)
	router.HandleFunc("/admin/tools", admin.RequireAuth(admin.ToolsPage)).Methods("GET")
	router.HandleFunc("/admin/api/library/libraries", admin.RequireMasterAuth(admin.ListLocalMediaLibraries)).Methods("GET")
	router.PathPrefix("/api/static/").Handler(http.StripPrefix("/api/static/", handlers.NewStaticHandler()))
	router.HandleFunc("/fixture/sign-in", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "strmr_admin_session", Value: f.token, Path: "/app", HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
	}).Methods("GET")
	router.HandleFunc("/fixture/demote", admin.RequireMasterAuth(recordingWebOrigin(func(w http.ResponseWriter, r *http.Request) {
		if _, err := f.base.store.Pool().Exec(r.Context(), `UPDATE accounts SET is_master=false WHERE id='actor'`); err != nil {
			http.Error(w, "synthetic mutation failed", 500)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))).Methods("POST")
	// This fixture-only operation stops before Register. Its format comes from
	// the real original ReaderAt preview, and the actual fsynced completion is
	// closed before responding. The UI must later use fresh Reconcile authority.
	router.HandleFunc("/fixture/prepare-unregistered", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		admin.RequireMasterAuth(recordingWebOrigin(func(w http.ResponseWriter, r *http.Request) {
			selectors, err := io.ReadAll(io.LimitReader(r.Body, 16385))
			defer clear(selectors)
			if err != nil || len(selectors) > 16384 || datastore.ValidateSABLibraryPublicationSelection(selectors) != nil {
				http.Error(w, "invalid synthetic selection", 400)
				return
			}
			issuer, err := f.base.startup.NewSABLibraryPublicationIssuer(f.base.store, f.source)
			if err != nil {
				http.Error(w, "synthetic issuer unavailable", 503)
				return
			}
			publisher, err := localmedia.NewSABLibraryPublisher(issuer)
			if err != nil {
				issuer.Close()
				http.Error(w, "synthetic publisher unavailable", 503)
				return
			}
			defer publisher.Close()
			preview, err := publisher.Preview(r, 1, selectors)
			if err != nil {
				http.Error(w, "synthetic original preview unavailable", 503)
				return
			}
			view := preview.View()
			preview.Close()
			if len(view.Files) != 1 || !view.Files[0].Eligible {
				http.Error(w, "synthetic actual media not recognized", 503)
				return
			}
			intent, err := issuer.Capture(r, 1, selectors)
			if err != nil {
				http.Error(w, "synthetic current intent unavailable", 503)
				return
			}
			defer intent.Close()
			completion, err := intent.Copy(r, []importer.SABPublicationFormat{{Ordinal: view.Files[0].Ordinal, Container: view.Files[0].Container}})
			if err != nil {
				http.Error(w, "synthetic physical copy unavailable", 503)
				return
			}
			completion.Close()
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(w, r.WithContext(ctx))
	}).Methods("POST")
	counts := &publicationBrowserCounts{}
	router.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		var items, records, operations int
		if err := f.base.store.Pool().QueryRow(r.Context(), `SELECT count(*) FROM local_media_items`).Scan(&items); err != nil {
			http.Error(w, "synthetic state unavailable", 500)
			return
		}
		if err := f.base.store.Pool().QueryRow(r.Context(), `SELECT count(*) FROM sab_library_publications`).Scan(&records); err != nil {
			http.Error(w, "synthetic state unavailable", 500)
			return
		}
		if err := f.db.Connection().QueryRowContext(r.Context(), `SELECT count(*) FROM sab_library_publications`).Scan(&operations); err != nil {
			http.Error(w, "synthetic state unavailable", 500)
			return
		}
		files, digest := 0, sha256.New()
		err := filepath.WalkDir(filepath.Join(f.root, ".mediastorm-publications"), func(path string, entry fs.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bin") {
				return nil
			}
			info, err := entry.Info()
			if err != nil || !info.Mode().IsRegular() {
				return errors.New("unexpected synthetic destination")
			}
			identity, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				return errors.New("synthetic identity unavailable")
			}
			files++
			fmt.Fprintf(digest, "%d:%d:%d:%d:%d\n", identity.Dev, identity.Ino, info.Size(), identity.Mtim.Sec, identity.Mtim.Nsec)
			return nil
		})
		if err != nil {
			http.Error(w, "synthetic physical state unavailable", 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "records": records, "copyOperations": operations, "physicalFiles": files, "fingerprint": hex.EncodeToString(digest.Sum(nil)), "deniedHTTP": guard.denied.Load(), "stageGets": counts.stageGets.Load(), "stagePosts": counts.stagePosts.Load(), "previewPosts": counts.previewPosts.Load(), "publishPosts": counts.publishPosts.Load(), "reconcilePosts": counts.reconcilePosts.Load(), "statusGets": counts.statusGets.Load(), "librariesGets": counts.librariesGets.Load(), "refreshPosts": counts.refreshPosts.Load()})
	}).Methods("GET")
	// Existing page chrome may request unrelated passive diagnostics. Those
	// fixture responses carry no publisher result, file identity or permission.
	router.PathPrefix("/admin/api/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"tasks":[],"workers":[],"defaultPassword":false,"adminWalkthroughDismissed":true}`)
	})
	entry := utils.BasePathHandler("/app", router)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/stage") && r.Method == "GET":
			counts.stageGets.Add(1)
		case strings.HasSuffix(path, "/stage") && r.Method == "POST":
			counts.stagePosts.Add(1)
		case strings.HasSuffix(path, "/publication/preview"):
			counts.previewPosts.Add(1)
		case strings.HasSuffix(path, "/publication/reconcile"):
			counts.reconcilePosts.Add(1)
		case strings.HasSuffix(path, "/publication") && r.Method == "POST":
			counts.publishPosts.Add(1)
		case strings.HasSuffix(path, "/publication") && r.Method == "GET":
			counts.statusGets.Add(1)
		case strings.HasSuffix(path, "/library/libraries"):
			counts.librariesGets.Add(1)
		case strings.HasSuffix(path, "/refresh"):
			counts.refreshPosts.Add(1)
		}
		entry.ServeHTTP(w, r)
	}))
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server.Listener.Close()
	server.Listener = listener
	guard.mu.Lock()
	guard.hosts[listener.Addr().String()] = true
	guard.mu.Unlock()
	server.Start()
	t.Cleanup(func() { f.handler.Close(); server.Close() })
	return server
}

func TestSABLibraryPublicationProductionBrowser(t *testing.T) {
	python, chromium := os.Getenv("MEDIASTORM_BROWSER_PYTHON"), os.Getenv("MEDIASTORM_CHROMIUM")
	if python == "" || chromium == "" {
		t.Skip("dedicated local browser fixture required")
	}
	// The real managed coordinator is exclusive. Each synchronous child closes
	// its actual runtime and server before the next fixture is constructed.
	for _, kind := range []string{"happy", "unknown", "reconcile"} {
		t.Run(kind, func(t *testing.T) {
			guard := &previewRouteTransport{hosts: map[string]bool{}}
			guard.inner = &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				guard.mu.RLock()
				allowed := guard.hosts[address]
				guard.mu.RUnlock()
				if !allowed {
					guard.denied.Add(1)
					return nil, errors.New("synthetic publication literal dial denied")
				}
				return (&net.Dialer{}).DialContext(ctx, "tcp4", address)
			}}
			original := http.DefaultTransport
			http.DefaultTransport = guard
			t.Cleanup(func() { http.DefaultTransport = original; guard.inner.CloseIdleConnections() })
			server := publicationBrowserServer(t, guard)
			artifacts := os.Getenv("MEDIASTORM_BROWSER_ARTIFACTS")
			if artifacts == "" {
				artifacts = t.TempDir()
			}
			artifacts = filepath.Join(artifacts, "library-publication-web", kind)
			if err := os.MkdirAll(artifacts, 0700); err != nil {
				t.Fatal(err)
			}
			scratch := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, "testdata/sab_library_publication_web_browser.py", "--case", kind, "--base-url", server.URL, "--unknown-url", server.URL, "--reconcile-url", server.URL, "--chromium", chromium, "--artifacts", artifacts)
			cmd.Env = append(os.Environ(), "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch, "MEDIASTORM_BROWSER_TMPDIR="+scratch)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("actual synthetic publication browser: %v\n%s", err, output)
			}
			if guard.denied.Load() != 0 {
				t.Fatal("publication browser attempted an external provider")
			}
		})
	}
}
