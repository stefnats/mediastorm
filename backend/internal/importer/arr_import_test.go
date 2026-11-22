package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/services/arrclient"
)

type arrLibraryFixture struct {
	calls  int
	result ARRLibraryResult
	err    error
	paths  []arrclient.ImportedFile
}

func (f *arrLibraryFixture) ValidateARRLibrary(context.Context, string, string) error { return nil }
func (f *arrLibraryFixture) ImportARRFiles(_ context.Context, _ string, _ string, files []arrclient.ImportedFile) (ARRLibraryResult, error) {
	f.calls++
	f.paths = files
	return f.result, f.err
}
func TestCompletedDownloadARRImportAndLibraryStatus(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	f.status.Store("Completed")
	var submits atomic.Int32
	var state atomic.Value
	state.Store("queued")
	var imported atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Api-Key") != "synthetic-arr-key" {
			t.Error("missing configured app key")
		}
		if r.Method == http.MethodPost {
			submits.Add(1)
			var command map[string]any
			if json.NewDecoder(r.Body).Decode(&command) != nil {
				t.Error("bad command")
			}
			if command["name"] != "DownloadedMoviesScan" || command["importMode"] != "copy" || command["downloadClientId"] != "own_job_1" || command["path"] != "/PRIVATE/COMPLETED" {
				t.Error("incorrect existing completed-download submission")
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 7, "name": "DownloadedMoviesScan", "status": "queued", "result": "unknown", "body": command})
			return
		}
		if r.URL.Path == "/api/v3/command/7" {
			if state.Load().(string) == "gone" {
				http.NotFound(w, r)
				return
			}
			fmt.Fprintf(w, `{"id":7,"name":"DownloadedMoviesScan","status":%q,"result":"unknown"}`, state.Load().(string))
			return
		}
		if r.URL.Path == "/api/v3/history" {
			if r.URL.Query().Get("downloadId") != "own_job_1" {
				t.Error("different download checked")
			}
			if !imported.Load() {
				fmt.Fprint(w, `{"totalRecords":0,"records":[]}`)
				return
			}
			fmt.Fprint(w, `{"totalRecords":1,"records":[{"downloadId":"own_job_1","eventType":"downloadFolderImported","data":{"importedPath":"/library/Synthetic Movie.mkv"},"movie":{"title":"Synthetic Movie","tmdbId":303}}]}`)
			return
		}
		t.Error("unexpected ARR route")
		http.NotFound(w, r)
	}))
	defer server.Close()
	cfg := config.SABnzbdSettings{Radarr: config.ARRImportSettings{BaseURL: server.URL, APIKey: "synthetic-arr-key", LibraryID: "movies"}}
	settings := func(context.Context) (config.SABnzbdSettings, error) { return cfg, nil }
	check := func(ctx context.Context) error { return ctx.Err() }
	library := &arrLibraryFixture{result: ARRLibraryResult{ItemIDs: []string{"local-version-1"}}}
	status, e := f.service.SubmitARRImport(t.Context(), f.item.ID, "radarr", false, settings, library, check)
	if e != nil || status.State != "importing" || status.CommandID != 7 {
		t.Fatal(status, e)
	}
	if _, e = f.service.SubmitARRImport(t.Context(), f.item.ID, "radarr", false, settings, library, check); e != nil || submits.Load() != 1 {
		t.Fatal("repeated ordinary click resubmitted import", e)
	}
	state.Store("completed")
	status, e = f.service.RefreshARRImport(t.Context(), f.item.ID, settings, library, check)
	if e != nil || status.State != "awaiting_files" || library.calls != 0 {
		t.Fatal("command completion invented local file", status, e)
	}
	imported.Store(true)
	status, e = f.service.RefreshARRImport(t.Context(), f.item.ID, settings, library, check)
	if e != nil || status.State != "available" || len(status.ItemIDs) != 1 || status.ItemIDs[0] != "local-version-1" || library.calls != 1 {
		t.Fatal("library import did not finish", status, e)
	}
	// A failed playback probe remains actionable instead of "available".
	library.result.InvalidMedia = true
	status, e = f.service.RefreshARRImport(t.Context(), f.item.ID, settings, library, check)
	if e != nil || status.State != "media_unavailable" || status.Reason != "media_not_playable" {
		t.Fatal("invalid media promoted to available", status, e)
	}
	library.result.InvalidMedia = false
	// A pruned command must not hide an import confirmed in history.
	state.Store("gone")
	status, e = f.service.RefreshARRImport(t.Context(), f.item.ID, settings, library, check)
	if e != nil || status.State != "available" {
		t.Fatal("expired command hid imported library file", status, e)
	}
	// Durable receipt survives rebuilding the actual queue service/database.
	f.open(t)
	saved, e := f.service.GetARRImportStatus(t.Context(), f.item.ID)
	if e != nil || saved.State != "available" || len(saved.ItemIDs) != 1 {
		t.Fatal("import status was not durable", saved, e)
	}
}
func TestARRFailedImportExplicitRetry(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	f.status.Store("Completed")
	var submissions atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			n := submissions.Add(1)
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": int(n), "name": "DownloadedEpisodesScan", "status": "failed", "result": "unsuccessful", "body": body})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	cfg := config.SABnzbdSettings{Sonarr: config.ARRImportSettings{BaseURL: server.URL, APIKey: "synthetic-arr-key", LibraryID: "shows"}}
	settings := func(context.Context) (config.SABnzbdSettings, error) { return cfg, nil }
	check := func(ctx context.Context) error { return ctx.Err() }
	library := &arrLibraryFixture{}
	first, e := f.service.SubmitARRImport(t.Context(), f.item.ID, "sonarr", false, settings, library, check)
	if e != nil || first.State != "import_failed" {
		t.Fatal(first, e)
	}
	repeated, e := f.service.SubmitARRImport(t.Context(), f.item.ID, "sonarr", false, settings, library, check)
	if e != nil || repeated.CommandID != first.CommandID || submissions.Load() != 1 {
		t.Fatal("implicit retry", e)
	}
	retried, e := f.service.SubmitARRImport(t.Context(), f.item.ID, "sonarr", true, settings, library, check)
	if e != nil || retried.CommandID == first.CommandID || submissions.Load() != 2 {
		t.Fatal("explicit retry unavailable", retried, e)
	}
}

func TestOlderARRRefreshCannotReplaceAvailableStatus(t *testing.T) {
	f := newSABFallbackFixture(t)
	f.fail()
	f.status.Store("Completed")
	entered, release, firstDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var history atomic.Int32
	var freeOnce sync.Once
	free := func() { freeOnce.Do(func() { close(release) }) }
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 7, "name": "DownloadedMoviesScan", "status": "queued", "result": "unknown", "body": body})
			return
		}
		if r.URL.Path == "/api/v3/command/7" {
			fmt.Fprint(w, `{"id":7,"name":"DownloadedMoviesScan","status":"completed","result":"successful"}`)
			return
		}
		if r.URL.Path == "/api/v3/history" {
			if history.Add(1) == 1 {
				close(entered)
				<-release
				fmt.Fprint(w, `{"totalRecords":0,"records":[]}`)
				return
			}
			fmt.Fprint(w, `{"totalRecords":1,"records":[{"downloadId":"own_job_1","eventType":"downloadFolderImported","data":{"importedPath":"/library/Movie.mkv"},"movie":{"title":"Movie","tmdbId":303}}]}`)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)
	cfg := config.SABnzbdSettings{Radarr: config.ARRImportSettings{BaseURL: server.URL, APIKey: "synthetic-key", LibraryID: "movies"}}
	settings := func(context.Context) (config.SABnzbdSettings, error) { return cfg, nil }
	check := func(ctx context.Context) error { return ctx.Err() }
	library := &arrLibraryFixture{result: ARRLibraryResult{ItemIDs: []string{"actual-local-version"}}}
	if _, e := f.service.SubmitARRImport(t.Context(), f.item.ID, "radarr", false, settings, library, check); e != nil {
		t.Fatal(e)
	}
	started := false
	var old ARRImportStatus
	var oldErr error
	t.Cleanup(func() {
		free()
		if started {
			select {
			case <-firstDone:
			case <-time.After(6 * time.Second):
				t.Error("old refresh did not return")
			}
		}
	})
	started = true
	go func() {
		defer close(firstDone)
		old, oldErr = f.service.RefreshARRImport(t.Context(), f.item.ID, settings, library, check)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old history response not held")
	}
	latest, e := f.service.RefreshARRImport(t.Context(), f.item.ID, settings, library, check)
	if e != nil || latest.State != "available" {
		t.Fatal("newer refresh did not publish local version", latest, e)
	}
	free()
	select {
	case <-firstDone:
	case <-time.After(6 * time.Second):
		t.Fatal("old refresh did not finish")
	}
	saved, e := f.service.GetARRImportStatus(t.Context(), f.item.ID)
	if e != nil || saved.State != "available" || len(saved.ItemIDs) != 1 || oldErr != nil || old.State != "available" {
		t.Fatal("older refresh erased a newer available result", saved, old, e, oldErr)
	}
}
