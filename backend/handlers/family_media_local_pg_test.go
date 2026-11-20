package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	"novastream/services/profileunlock"
)

type familyLocalRawPG struct {
	*publicPinCarrierFixture
	handler    *familyLocalRawHandler
	deps       FamilyMediaDependencies
	root, path string
	data       []byte
}

// The observer delegates every authority/open/read to the actual same-store
// implementation; it cannot manufacture a source or substitute file bytes.
type familyLocalRawObservedOpener struct {
	familyLocalRawOpener
	opens, readers atomic.Int32
}

func (o *familyLocalRawObservedOpener) OpenFamilyLocalSource(ctx context.Context, source *profileunlock.FamilyLocalSource) (profileunlock.FamilyLocalFile, error) {
	o.opens.Add(1)
	file, err := o.familyLocalRawOpener.OpenFamilyLocalSource(ctx, source)
	if err != nil {
		return nil, err
	}
	return &familyLocalRawObservedFile{FamilyLocalFile: file, readers: &o.readers}, nil
}

type familyLocalRawObservedFile struct {
	profileunlock.FamilyLocalFile
	readers *atomic.Int32
}

func (f *familyLocalRawObservedFile) OpenReader(ctx context.Context, start, length int64) (io.ReadCloser, error) {
	f.readers.Add(1)
	return f.FamilyLocalFile.OpenReader(ctx, start, length)
}

func familyLocalRawFixture(t *testing.T) *familyLocalRawPG {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("actual checked LOCAL source requires Linux")
	}
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	value := &familyLocalRawPG{publicPinCarrierFixture: f, root: t.TempDir(), data: make([]byte, 1<<20)}
	for i := range value.data {
		value.data[i] = byte(i % 251)
	}
	value.path = filepath.Join(value.root, "synthetic-actual-file.bin")
	if err := os.WriteFile(value.path, value.data, 0600); err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('raw-library','Synthetic raw library','movie',$1)`, []any{value.root}},
		{`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('raw-library','all')`, nil},
		{`INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('raw-item','raw-library','synthetic-actual-file.bin',$1,'synthetic-actual-file.bin','movie',$2)`, []any{value.path, int64(len(value.data))}},
	} {
		if _, err := f.store.Pool().Exec(t.Context(), step.sql, step.args...); err != nil {
			t.Fatal("seed actual local raw source", err)
		}
	}
	value.deps = FamilyMediaDependencies{Store: f.store, Pins: f.pins, Local: installed.ParentFiles.local, Parent: installed.ParentFiles.parent}
	var err error
	value.handler, err = newFamilyLocalRawHandler(value.deps)
	if err != nil {
		t.Fatal("construct exact actual local raw handler", err)
	}
	return value
}

type familyLocalRawRecorder struct {
	*httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
}

func newFamilyLocalRawRecorder() *familyLocalRawRecorder {
	return &familyLocalRawRecorder{ResponseRecorder: httptest.NewRecorder()}
}
func (w *familyLocalRawRecorder) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	w.mu.Unlock()
	return nil
}
func (f *familyLocalRawPG) rawRequest(t *testing.T, method string) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, release := f.request(t, method, "/?profileId="+f.profile.ID)
	return mux.SetURLVars(r, map[string]string{"itemID": "raw-item"}), release
}
func familyLocalRawFDs(t *testing.T, path string) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal("read synthetic process descriptor list", err)
	}
	count := 0
	for _, entry := range entries {
		target, e := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if e == nil && strings.TrimSuffix(target, " (deleted)") == path {
			count++
		}
	}
	return count
}

func TestFamilyLocalRawActualPGFullRangeHEADAndObservedElapsed(t *testing.T) {
	f := familyLocalRawFixture(t)
	observer := &familyLocalRawObservedOpener{familyLocalRawOpener: f.handler.opener}
	f.handler.opener = observer
	for _, tc := range []struct {
		method, span, ifRange string
		status, start, length int
	}{
		{"GET", "", "", 200, 0, 1 << 20},
		{"GET", "bytes=0-31", "", 206, 0, 32},
		{"GET", "bytes=1048560-", "", 206, (1 << 20) - 16, 16},
		{"GET", "bytes=-17", "", 206, (1 << 20) - 17, 17},
		{"GET", "bytes=10-19", `"unproven-label"`, 200, 0, 1 << 20},
		{"HEAD", "bytes=10-19", "", 200, 0, 1 << 20},
		{"GET", "bytes=9223372036854775808-", "", 416, 0, 0},
	} {
		r, release := f.rawRequest(t, tc.method)
		r.Header.Set("Range", tc.span)
		r.Header.Set("If-Range", tc.ifRange)
		w := newFamilyLocalRawRecorder()
		previousReaders := observer.readers.Load()
		started := time.Now()
		f.handler.ServeHTTP(w, r)
		elapsed := time.Since(started)
		release()
		if w.Code != tc.status {
			t.Fatalf("actual raw %s %q status=%d responseBytes=%d", tc.method, tc.span, w.Code, w.Body.Len())
		}
		if tc.method == "HEAD" || tc.status == 416 {
			if observer.readers.Load() != previousReaders {
				t.Fatal("HEAD/unsatisfiable response accidentally opened an actual media reader")
			}
			if w.Body.Len() != 0 {
				t.Fatal("HEAD or unsatisfiable range returned media bytes")
			}
		} else if !bytes.Equal(w.Body.Bytes(), f.data[tc.start:tc.start+tc.length]) {
			t.Fatal("actual held-FD response bytes/range changed")
		}
		if tc.status != 416 && w.Header().Get("Content-Length") != strconv.Itoa(tc.length) {
			t.Fatal("actual file response length changed")
		}
		if tc.status == 206 && w.Header().Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", tc.start, tc.start+tc.length-1, len(f.data)) {
			t.Fatal("actual partial content range changed")
		}
		if tc.status == 416 && w.Header().Get("Content-Range") != fmt.Sprintf("bytes */%d", len(f.data)) {
			t.Fatal("unsatisfiable range omitted actual checked size")
		}
		if w.Header().Get("ETag") != "" || w.Header().Get("Last-Modified") != "" || w.Header().Get("Location") != "" {
			t.Fatal("raw file manufactured validator or delegated source")
		}
		if w.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatal("file response became permission cache")
		}
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("actual raw request retained file FD after response cleanup")
		}
		if tc.method == "GET" && tc.span == "" {
			t.Logf("observed synthetic full GET: bytes=%d elapsed=%s; no performance threshold or cache claim", w.Body.Len(), elapsed)
		}
	}
	if err := os.WriteFile(f.path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Pool().Exec(t.Context(), "UPDATE local_media_items SET size_bytes=0 WHERE id='raw-item'"); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"GET", "HEAD"} {
		r, release := f.rawRequest(t, method)
		previousReaders := observer.readers.Load()
		previousOpens := observer.opens.Load()
		w := newFamilyLocalRawRecorder()
		f.handler.ServeHTTP(w, r)
		release()
		// The actual frozen source requires a positive-sized local file. A
		// mathematically valid empty range does not widen that source contract.
		if w.Code != 403 || observer.readers.Load() != previousReaders || observer.opens.Load() != previousOpens {
			t.Fatal("empty actual source bypassed positive-size source contract")
		}
	}
}

func TestFamilyLocalRawActualPGActiveAdultRequiresExactPINAndChildNeverUsesAdultFile(t *testing.T) {
	f := familyLocalRawFixture(t)
	observer := &familyLocalRawObservedOpener{familyLocalRawOpener: f.handler.opener}
	f.handler.opener = observer
	child := familyActualPGChild(t, f.publicPinCarrierFixture)
	foreign, err := f.users.CreateForAccount("other", "Synthetic foreign profile")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"no-pin", "locked", "unlocked", "child", "foreign-profile", "profile-name", "ambiguous"} {
		if mode == "locked" {
			f.setPIN(t, "1357")
		}
		r, release := f.rawRequest(t, "GET")
		if mode == "unlocked" || mode == "child" || mode == "foreign-profile" {
			r.Header.Set(profileunlock.Header, f.issue(t, r))
		}
		switch mode {
		case "child":
			r.URL.RawQuery = "profileId=" + child
		case "foreign-profile":
			r.URL.RawQuery = "profileId=" + foreign.ID
		case "profile-name":
			r.URL.RawQuery += "&profileName=other"
		case "ambiguous":
			r.URL.RawQuery += "&userId=other"
		}
		w := newFamilyLocalRawRecorder()
		previousOpens := observer.opens.Load()
		f.handler.ServeHTTP(w, r)
		release()
		if mode == "unlocked" {
			if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), f.data) {
				t.Fatal("actual original active adult PIN denied", w.Code)
			}
		} else if w.Code != 400 && w.Code != 403 {
			t.Fatalf("unsupported %s became adult file access status=%d", mode, w.Code)
		}
		if mode != "unlocked" && observer.opens.Load() != previousOpens {
			t.Fatal("rejected adult/child selectors reached actual filesystem opener")
		}
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("denied or finished adult file request retained source descriptor")
		}
	}
}

func TestFamilyLocalRawActualPGConstructorRejectsForeignStorePinsAndService(t *testing.T) {
	f := familyLocalRawFixture(t)
	other := familyLocalRawFixture(t)
	for _, deps := range []FamilyMediaDependencies{
		{},
		{Store: f.store, Pins: other.pins, Local: f.deps.Local, Parent: f.deps.Parent},
		{Store: f.store, Pins: f.pins, Local: other.deps.Local, Parent: f.deps.Parent},
	} {
		if h, err := newFamilyLocalRawHandler(deps); h != nil || err == nil {
			t.Fatal("foreign/unavailable actual source dependencies constructed handler")
		}
	}
	if f.handler.installation.Boundary == nil || f.handler.opener == nil || f.handler.installation.ParentFiles.store != f.store {
		t.Fatal("private constructor lost exact original same-store factory")
	}
}

func TestFamilyLocalRawActualPGBlockedWriteRevocationAndDisconnectJoinBeforeRefund(t *testing.T) {
	for _, mutation := range []string{"acl", "pin", "file", "disconnect"} {
		t.Run(mutation, func(t *testing.T) {
			f := familyLocalRawFixture(t)
			if mutation == "pin" {
				familyActualPGChild(t, f.publicPinCarrierFixture)
				f.setPIN(t, "1357")
			}
			r, release := f.rawRequest(t, "GET")
			defer release()
			if mutation == "pin" {
				r.Header.Set(profileunlock.Header, f.issue(t, r))
			}
			caller, cancel := context.WithCancel(r.Context())
			defer cancel()
			r = r.WithContext(caller)
			target := newFamilyControlledWriter()
			var releaseWrite, releaseCallback sync.Once
			t.Cleanup(func() {
				releaseWrite.Do(func() { close(target.writeGate) })
				releaseCallback.Do(func() { close(target.callbackGate) })
			})
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				defer func() {
					if recovered := recover(); recovered != nil && recovered != http.ErrAbortHandler {
						t.Errorf("unexpected private raw panic: %v", recovered)
					}
				}()
				f.handler.ServeHTTP(target, r)
			}()
			t.Cleanup(func() {
				releaseWrite.Do(func() { close(target.writeGate) })
				releaseCallback.Do(func() { close(target.callbackGate) })
				familyWait(t, finished, "private raw handler survived fixture cleanup")
			})
			familyWait(t, target.entered, "actual raw file write never blocked")
			if familyLocalRawFDs(t, f.path) < 2 {
				t.Fatal("raw write did not originate from actual held and duplicate file descriptors")
			}
			fresh, stop := f.rawRequest(t, "GET")
			defer stop()
			if mutation == "pin" {
				fresh.Header.Set(profileunlock.Header, r.Header.Get(profileunlock.Header))
			}
			var held []*profileunlock.FamilyMediaOwner
			t.Cleanup(func() {
				for _, owner := range held {
					owner.Close()
				}
			})
			for range 31 {
				owner, err := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID)
				if err != nil {
					t.Fatal("fill actual original owner admission", err)
				}
				held = append(held, owner)
			}
			switch mutation {
			case "acl":
				if _, err := f.store.Pool().Exec(t.Context(), "UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='raw-library'"); err != nil {
					t.Fatal(err)
				}
			case "pin":
				if _, err := f.users.SetPin(f.profile.ID, "2468"); err != nil {
					t.Fatal(err)
				}
			case "file":
				if err := os.Rename(f.path, f.path+".retired"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.path, f.data, 0600); err != nil {
					t.Fatal(err)
				}
			case "disconnect":
				cancel()
			}
			familyWait(t, target.interrupted, "actual current source/PIN/consumer change failed to interrupt blocked write")
			familyPending(t, finished, "handler completed before blocked actual write joined")
			if extra, err := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); extra != nil || !errors.Is(err, profileunlock.ErrFamilyMediaBusy) {
				if extra != nil {
					extra.Close()
				}
				t.Fatal("original PG quota refunded during blocked write", err)
			}
			releaseWrite.Do(func() { close(target.writeGate) })
			familyPending(t, finished, "actual running write-deadline callback was not joined")
			if extra, err := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); extra != nil || !errors.Is(err, profileunlock.ErrFamilyMediaBusy) {
				if extra != nil {
					extra.Close()
				}
				t.Fatal("running callback released original PG quota", err)
			}
			releaseCallback.Do(func() { close(target.callbackGate) })
			familyWait(t, finished, "actual file/request cleanup did not finish")
			if familyLocalRawFDs(t, f.path) != 0 || familyLocalRawFDs(t, f.path+".retired") != 0 {
				t.Fatal("retired actual source retained an original or duplicate FD")
			}
			if mutation != "pin" {
				if extra, err := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); err != nil {
					t.Fatal("actual cleanup did not refund original quota", err)
				} else {
					extra.Close()
				}
			}
		})
	}
}

func TestFamilyLocalRawActualIncomingDisconnectCancelsHeldPGSourceAcquisition(t *testing.T) {
	f := familyLocalRawFixture(t)
	observer := &familyLocalRawObservedOpener{familyLocalRawOpener: f.handler.opener}
	f.handler.opener = observer
	original, release := f.rawRequest(t, "GET")
	defer release()
	caller, cancel := context.WithCancel(original.Context())
	defer cancel()
	original = original.WithContext(caller)
	tx, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE local_media_items IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	w := newFamilyLocalRawRecorder()
	finished := make(chan struct{})
	go func() { defer close(finished); f.handler.ServeHTTP(w, original) }()
	t.Cleanup(func() {
		cancel()
		_ = tx.Rollback(context.Background())
		familyWait(t, finished, "raw acquisition survived fixture cleanup")
	})
	waiting := false
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		// pg_locks exposes the exact schema-local relation wait without
		// requiring privilege to read another backend's query text.
		if err = f.store.Pool().QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='local_media_items'::regclass AND mode='AccessShareLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal("observe actual blocked acquisition", err)
		}
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		t.Fatal("actual source acquisition did not reach held local item SQL")
	}
	if observer.opens.Load() != 0 || observer.readers.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("source acquisition reached filesystem before current SQL completed")
	}
	fresh, stop := f.rawRequest(t, "GET")
	defer stop()
	var held []*profileunlock.FamilyMediaOwner
	t.Cleanup(func() {
		for _, owner := range held {
			owner.Close()
		}
	})
	for range 31 {
		owner, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID)
		if e != nil {
			t.Fatal("fill actual admission during pending acquisition", e)
		}
		held = append(held, owner)
	}
	if extra, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); extra != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		if extra != nil {
			extra.Close()
		}
		t.Fatal("pending actual acquisition did not retain original owner admission", e)
	}
	familyPending(t, finished, "acquisition completed by its own budget before incoming cancel")
	if err = f.store.Pool().QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='local_media_items'::regclass AND mode='AccessShareLock' AND NOT granted)`).Scan(&waiting); err != nil || !waiting {
		t.Fatal("actual acquisition no longer held immediately before incoming cancel", err)
	}
	cancel()
	// Keep the actual table lock held: release by timeout or lock rollback
	// would not prove that the incoming consumer canceled acquisition.
	familyWait(t, finished, "incoming disconnect did not cancel held source acquisition")
	if w.Code != 403 || observer.opens.Load() != 0 || observer.readers.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("canceled setup published or opened actual local media")
	}
	if err = f.store.Pool().QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='local_media_items'::regclass AND mode='AccessShareLock' AND NOT granted)`).Scan(&waiting); err != nil || waiting {
		t.Fatal("handler returned before actual blocked SQL retired", err)
	}
	owner, err := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID)
	if err != nil {
		t.Fatal("canceled setup retained original PG owner admission", err)
	}
	owner.Close()
}

func TestFamilyLocalRawActualSetupDisconnectCancelsPreliminaryPGGuardAndJoinsOwner(t *testing.T) {
	f := familyLocalRawFixture(t)
	observer := &familyLocalRawObservedOpener{familyLocalRawOpener: f.handler.opener}
	f.handler.opener = observer
	original, release := f.rawRequest(t, "GET")
	defer release()
	caller, cancel := context.WithCancel(original.Context())
	defer cancel()
	original = original.WithContext(caller)
	q, ok := f.handler.installation.Boundary.Begin(httptest.NewRecorder(), original, "")
	if !ok {
		t.Fatal("actual original request denied")
	}
	t.Cleanup(q.Close)
	runtime, err := q.adoptRuntime(original)
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := runtime.beginConsumer(httptest.NewRecorder(), original, q.target)
	if err != nil {
		t.Fatal(err)
	}
	fresh, stop := f.rawRequest(t, "GET")
	defer stop()
	var held []*profileunlock.FamilyMediaOwner
	t.Cleanup(func() {
		for _, owner := range held {
			owner.Close()
		}
	})
	for range 31 {
		owner, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID)
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, owner)
	}
	tx, err := f.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		file, e := f.handler.open(runtime, c.Context(), "raw-item")
		if file != nil || e == nil {
			t.Error("canceled preliminary guard opened actual source")
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = tx.Rollback(context.Background())
		familyWait(t, finished, "preliminary source setup survived fixture cleanup")
	})
	waitCount := func() int {
		t.Helper()
		var count int
		if e := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM pg_locks WHERE relation='accounts'::regclass AND mode='AccessShareLock' AND NOT granted`).Scan(&count); e != nil {
			t.Fatal("observe actual preliminary PG guard", e)
		}
		return count
	}
	until := time.Now().Add(3 * time.Second)
	for waitCount() < 2 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	// Before a file/source watcher exists, only the single serial q watcher
	// and the newly reserved h.open guard can issue these blocked checks.
	if waitCount() < 2 {
		t.Fatal("preliminary guard was not distinguished from unrelated original watcher")
	}
	runtime.mu.Lock()
	var pending *familyMediaCleanup
	if len(runtime.cleanups) == 1 {
		for job := range runtime.cleanups {
			pending = job
		}
	}
	runtime.mu.Unlock()
	if pending == nil {
		t.Fatal("actual preliminary guard did not reserve resource setup")
	}
	familyPending(t, pending.ready, "resource setup had already left its preliminary guard")
	familyPending(t, finished, "preliminary guard completed before incoming cancel")
	if observer.opens.Load() != 0 || observer.readers.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("preliminary guard reached actual filesystem source")
	}
	if extra, e := f.pins.CaptureFamilyMediaOwner(fresh, f.profile.ID); extra != nil || !errors.Is(e, profileunlock.ErrFamilyMediaBusy) {
		if extra != nil {
			extra.Close()
		}
		t.Fatal("pending preliminary setup released original quota", e)
	}
	if waitCount() < 2 {
		t.Fatal("actual guard no longer pending immediately before cancellation")
	}
	cancel()
	familyWait(t, finished, "incoming setup cancel did not retire actual preliminary PG guard")
	closed := make(chan struct{})
	go func() { q.Close(); close(closed) }()
	familyWait(t, closed, "original cleanup failed to join preliminary guard/watch while lock stayed held")
	if waitCount() != 0 || observer.opens.Load() != 0 || observer.readers.Load() != 0 || familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("original close returned with pending SQL or physical source")
	}
	// Prove quota was refunded before unlocking accounts: a fresh captured
	// owner can now reserve that slot and reach its own genuine PG check.
	probeCtx, cancelProbe := context.WithCancel(fresh.Context())
	defer cancelProbe()
	probe := fresh.WithContext(probeCtx)
	probed := make(chan struct{})
	go func() {
		defer close(probed)
		owner, e := f.pins.CaptureFamilyMediaOwner(probe, f.profile.ID)
		if owner != nil {
			owner.Close()
		}
		if e == nil {
			t.Error("canceled quota probe unexpectedly succeeded under held accounts")
		}
	}()
	t.Cleanup(func() {
		cancelProbe()
		_ = tx.Rollback(context.Background())
		familyWait(t, probed, "quota probe survived fixture cleanup")
	})
	until = time.Now().Add(3 * time.Second)
	for waitCount() < 1 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if waitCount() != 1 {
		t.Fatal("actual joined cleanup did not refund original quota before lock release")
	}
	cancelProbe()
	familyWait(t, probed, "fresh admitted quota probe did not cancel under actual held PG lock")
	if waitCount() != 0 {
		t.Fatal("quota probe returned before real SQL retirement")
	}
}
