//go:build linux

package handlers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
	"novastream/services/remotemedia"
)

type familyKnownFixture struct {
	*familyJellyfinRawPG
	installed    *FamilyMediaInstallation
	usersHandler *UsersHandler
	local        *localmedia.Service
	remote       *remotemedia.Service
	original     context.Context
	cancel       context.CancelFunc
	path         string
	data         []byte
}

func newFamilyKnownFixture(t *testing.T, active bool) *familyKnownFixture {
	t.Helper()
	data := bytes.Repeat([]byte("actual-original-source-"), 17)
	f := newFamilyJellyfinRawPG(t, data, 1)
	ctx, cancel := context.WithCancel(t.Context())
	value := &familyKnownFixture{familyJellyfinRawPG: f, original: ctx, cancel: cancel, data: data, path: filepath.Join(t.TempDir(), "actual-checked-local.bin")}
	// Register the original stop before constructing any owned consumer.
	t.Cleanup(func() {
		cancel()
		if value.installed != nil {
			value.installed.Close()
		}
		if value.local != nil {
			_ = value.local.Close()
		}
	})
	var e error
	value.local, e = localmedia.PrepareService(ctx, f.store, nil, "/synthetic-probe-must-not-run")
	if e != nil {
		t.Fatal(e)
	}
	value.remote, e = remotemedia.NewService(f.store, f.manager, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	account, e := accounts.NewServiceWithStore(f.store)
	if e != nil {
		t.Fatal(e)
	}
	value.usersHandler = NewUsersHandler(f.users, account)
	value.usersHandler.SetPGProfilePins(f.pins)
	if e = os.WriteFile(value.path, data, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Pool().Exec(t.Context(), `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('known-local-library','Actual checked source','movie',$1);`, filepath.Dir(value.path)); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Pool().Exec(t.Context(), `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('known-local-library','all')`); e != nil {
		t.Fatal(e)
	}
	if _, e = f.store.Pool().Exec(t.Context(), `INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('known-local-item','known-local-library','actual-checked-local.bin',$1,'actual-checked-local.bin','movie',$2)`, value.path, len(data)); e != nil {
		t.Fatal(e)
	}
	value.installed, e = value.usersHandler.PrepareFamilyMediaConsumers(ctx, f.store, f.pins, value.local, f.manager, value.remote)
	if e != nil {
		t.Fatal("actual selected preparation", e)
	}
	if active {
		if e = value.local.Activate(value.original); e != nil {
			t.Fatal(e)
		}
	}
	return value
}
func (f *familyKnownFixture) localRequest(t *testing.T, method string) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, release := f.rawRequest(t, method, 0)
	return mux.SetURLVars(r, map[string]string{"itemID": "known-local-item"}), release
}
func familyKnownServe(fn http.HandlerFunc, w http.ResponseWriter, r *http.Request) (aborted bool) {
	defer func() {
		if p := recover(); p != nil {
			if p != http.ErrAbortHandler {
				panic(p)
			}
			aborted = true
		}
	}()
	fn(w, r)
	return false
}

type familyKnownRecorder struct {
	*httptest.ResponseRecorder
	flushes       atomic.Int32
	deadlineCalls atomic.Int32
	onFlush       func()
}

func newFamilyKnownRecorder() *familyKnownRecorder {
	return &familyKnownRecorder{ResponseRecorder: httptest.NewRecorder()}
}
func (w *familyKnownRecorder) SetWriteDeadline(time.Time) error { w.deadlineCalls.Add(1); return nil }
func (w *familyKnownRecorder) FlushError() error {
	w.flushes.Add(1)
	if w.onFlush != nil {
		w.onFlush()
	}
	w.ResponseRecorder.Flush()
	return nil
}
func familyKnownDenied(t *testing.T, w *familyKnownRecorder, aborted bool) {
	t.Helper()
	if !aborted && (w.Code < 400 || w.Code >= 500 && w.Code != 503) {
		t.Fatal("expected explicit source denial", w.Code)
	}
	if bytes.Contains(w.Body.Bytes(), []byte(familyJellyfinRawToken)) || strings.Contains(w.Header().Get("Location"), "http") {
		t.Fatal("private upstream capability exposed")
	}
}
func TestFamilyInstallationActualKnownSources(t *testing.T) {
	t.Run("actual-local-full-file", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		w := newFamilyKnownRecorder()
		if familyKnownServe(f.installed.ServeLocalRaw, w, r) || w.Code != 200 || !bytes.Equal(w.Body.Bytes(), f.data) || w.flushes.Load() < 1 || familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("actual local body/output/descriptor did not complete", w.Code)
		}
	})
	t.Run("actual-local-current-range", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		r.Header.Set("Range", "bytes=3-19")
		w := newFamilyKnownRecorder()
		if familyKnownServe(f.installed.ServeLocalRaw, w, r) || w.Code != 206 || !bytes.Equal(w.Body.Bytes(), f.data[3:20]) || !strings.HasPrefix(w.Header().Get("Content-Range"), "bytes 3-19/") {
			t.Fatal("actual current range changed", w.Code)
		}
	})
	t.Run("actual-local-head-opens-no-reader", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		observer := &familyLocalRawObservedOpener{familyLocalRawOpener: f.installed.localRaw.opener}
		f.installed.localRaw.opener = observer
		r, release := f.localRequest(t, "HEAD")
		defer release()
		w := newFamilyKnownRecorder()
		if familyKnownServe(f.installed.ServeLocalRaw, w, r) || w.Code != 200 || w.Body.Len() != 0 || w.Header().Get("Content-Length") == "" || observer.opens.Load() != 1 || observer.readers.Load() != 0 || w.flushes.Load() < 1 {
			t.Fatal("HEAD did not retain actual descriptor-only semantics", w.Code)
		}
	})
	t.Run("actual-jellyfin-genuine-current-body", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.rawRequest(t, "GET", 0)
		defer release()
		w := newFamilyKnownRecorder()
		if familyKnownServe(f.installed.ServeJellyfinRaw, w, r) || w.Code != 200 || !bytes.Equal(w.Body.Bytes(), f.data) || f.wire.downloads.Load() != 1 || f.wire.bad.Load() {
			t.Fatal("actual managed current body changed", w.Code)
		}
		if w.Header().Get("Location") != "" || w.Header().Get("ETag") != "" || bytes.Contains(w.Body.Bytes(), []byte(familyJellyfinRawToken)) {
			t.Fatal("upstream metadata/capability leaked")
		}
	})
	t.Run("source-metadata-and-missing-id-cannot-open", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		r = mux.SetURLVars(r, map[string]string{"itemID": "unregistered-item"})
		w := newFamilyKnownRecorder()
		familyKnownDenied(t, w, familyKnownServe(f.installed.ServeLocalRaw, w, r))
		if familyLocalRawFDs(t, f.path) != 0 || f.wire.requests.Load() != 0 {
			t.Fatal("unregistered metadata acquired a source")
		}
		r2, stop := f.rawRequest(t, "GET", 0)
		defer stop()
		r2 = mux.SetURLVars(r2, map[string]string{"itemID": "http://unowned.invalid/file"})
		w2 := newFamilyKnownRecorder()
		familyKnownDenied(t, w2, familyKnownServe(f.installed.ServeJellyfinRaw, w2, r2))
		if f.wire.requests.Load() != 0 {
			t.Fatal("URL became a source identifier")
		}
	})
	t.Run("local-current-acl-revocation", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		q, ok := f.installed.Boundary.Begin(newFamilyKnownRecorder(), r, "")
		if !ok {
			t.Fatal("original PG capture failed")
		}
		defer q.Close()
		runtime, e := q.adoptRuntime(r)
		if e != nil {
			t.Fatal(e)
		}
		c, _, e := runtime.beginConsumer(newFamilyKnownRecorder(), r, q.target)
		if e != nil {
			t.Fatal(e)
		}
		file, e := f.installed.localRaw.open(runtime, c.Context(), "known-local-item")
		if e != nil {
			t.Fatal(e)
		}
		if familyLocalRawCurrent(c, file) != nil {
			t.Fatal("actual initial ACL not allowed")
		}
		if _, e = f.store.Pool().Exec(t.Context(), `UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='known-local-library'`); e != nil {
			t.Fatal(e)
		}
		if familyLocalRawCurrent(c, file) == nil {
			t.Fatal("cached allow survived actual ACL revocation")
		}
		q.Close()
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("revoked original retained FD")
		}
	})
	t.Run("jellyfin-current-config-and-acl-revocation", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.rawRequest(t, "GET", 0)
		defer release()
		q, ok := f.installed.Boundary.Begin(newFamilyKnownRecorder(), r, "")
		if !ok {
			t.Fatal("original capture failed")
		}
		defer q.Close()
		runtime, e := q.adoptRuntime(r)
		if e != nil {
			t.Fatal(e)
		}
		c, _, e := runtime.beginConsumer(newFamilyKnownRecorder(), r, q.target)
		if e != nil {
			t.Fatal(e)
		}
		stream, body, e := f.installed.jellyfinRaw.open(runtime, c.Context(), "remote-item-0")
		if e != nil {
			t.Fatal(e)
		}
		defer body.Close()
		if familyJellyfinRawCurrent(c, stream) != nil {
			t.Fatal("actual initial remote ACL not allowed")
		}
		if _, e = f.store.Pool().Exec(t.Context(), `UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='remote-library-0'`); e != nil {
			t.Fatal(e)
		}
		if familyJellyfinRawCurrent(c, stream) == nil {
			t.Fatal("remote ACL revoke did not deny original")
		}
		_ = body.Close()
		q.Close()
	})
	t.Run("original-adult-pin-revocation", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		q, ok := f.installed.Boundary.Begin(newFamilyKnownRecorder(), r, "")
		if !ok {
			t.Fatal("actual unlocked capture failed")
		}
		defer q.Close()
		if e := f.store.ProfileUnlocks().DeleteByProfile(t.Context(), f.profile.ID); e != nil {
			t.Fatal(e)
		}
		if q.Check(t.Context()) == nil {
			t.Fatal("actual PIN revocation retained old owner")
		}
		fresh, stop := f.localRequest(t, "GET")
		defer stop()
		fresh.Header.Del(profileunlock.Header)
		w := newFamilyKnownRecorder()
		familyKnownDenied(t, w, familyKnownServe(f.installed.ServeLocalRaw, w, fresh))
	})
	t.Run("generic-child-remains-denied", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		r.URL.RawQuery = "profileId=" + f.child
		r.Header.Del(profileunlock.Header)
		w := newFamilyKnownRecorder()
		familyKnownDenied(t, w, familyKnownServe(f.installed.ServeLocalRaw, w, r))
		if familyLocalRawFDs(t, f.path) != 0 || f.wire.requests.Load() != 0 {
			t.Fatal("generic child gained actual source")
		}
	})
	t.Run("scoped-session-remains-denied", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		session, e := f.sessions.CreateScopedWithResource("actor", false, "synthetic-browser", "192.0.2.8", time.Hour, models.SessionScopeStream, "known-local-item")
		if e != nil {
			t.Fatal(e)
		}
		raw := httptest.NewRequest("GET", "/?profileId="+f.profile.ID, nil)
		raw.Header.Set("X-Client-ID", session.ClientID)
		r, release, e := f.store.AuthenticateProfilePinRequest(raw, session.Token, session.ClientID)
		if e != nil {
			t.Fatal(e)
		}
		defer release()
		r = mux.SetURLVars(r, map[string]string{"itemID": "known-local-item"})
		w := newFamilyKnownRecorder()
		familyKnownDenied(t, w, familyKnownServe(f.installed.ServeLocalRaw, w, r))
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("scoped session became full source owner")
		}
	})
	t.Run("same-original-one-source-claim", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.localRequest(t, "GET")
		defer release()
		q, ok := f.installed.Boundary.Begin(newFamilyKnownRecorder(), r, "")
		if !ok {
			t.Fatal("capture failed")
		}
		defer q.Close()
		owner, ok := q.owner.original.(*profileunlock.FamilyMediaOwner)
		if !ok {
			t.Fatal("not actual original facade")
		}
		local, e := owner.CaptureLocalSource(t.Context(), "unregistered-item")
		if local != nil || e == nil {
			t.Fatal("missing item authorized")
		}
		remote, e := owner.CaptureJellyfinSource(t.Context(), "remote-item-0")
		if remote != nil || e == nil || f.wire.requests.Load() != 0 {
			t.Fatal("failed original source claim renewed across providers")
		}
	})
	t.Run("jellyfin-redirect-and-wire-privacy", func(t *testing.T) {
		f := newFamilyKnownFixture(t, true)
		r, release := f.rawRequest(t, "GET", 0)
		defer release()
		w := newFamilyKnownRecorder()
		if familyKnownServe(f.installed.ServeJellyfinRaw, w, r) || w.Code != 200 {
			t.Fatal("genuine managed source failed")
		}
		for _, key := range []string{"Location", "ETag", "Last-Modified", "Accept-Ranges", "Content-Range", "Authorization"} {
			if w.Header().Get(key) != "" {
				t.Fatal("untrusted upstream header escaped", key)
			}
		}
		if strings.Contains(w.Body.String(), "http://") || strings.Contains(w.Body.String(), familyJellyfinRawToken) || f.wire.bad.Load() {
			t.Fatal("raw upstream capability escaped")
		}
		var foreignRequests atomic.Int32
		foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { foreignRequests.Add(1); _, _ = w.Write(f.data) }))
		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/Download") {
				http.Redirect(w, r, foreign.URL+"/unowned", http.StatusTemporaryRedirect)
				return
			}
			f.wire.serve(w, r)
		}))
		t.Cleanup(func() { f.installed.Close(); redirect.Close(); foreign.Close() })
		f.settings.Jellyfin.Accounts[0].ServerURL = redirect.URL + "/original"
		if e := f.manager.Save(f.settings); e != nil {
			t.Fatal(e)
		}
		fresh, stop := f.rawRequest(t, "GET", 0)
		defer stop()
		denied := newFamilyKnownRecorder()
		familyKnownDenied(t, denied, familyKnownServe(f.installed.ServeJellyfinRaw, denied, fresh))
		if foreignRequests.Load() != 0 || bytes.Equal(denied.Body.Bytes(), f.data) {
			t.Fatal("actual redirected source escaped original managed scope")
		}

	})
}
