//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/services/kidsvod"
)

type parentOriginalFixture struct {
	*familyKnownFixture
	t                *testing.T
	h                *ParentLocalFileHandler
	approval, rawURL string
	expires          time.Time
}

func parentOriginal(t *testing.T) *parentOriginalFixture {
	t.Helper()
	f := newFamilyKnownFixture(t, true)
	if _, err := f.store.Pool().Exec(t.Context(), `UPDATE users SET kids_mode='rating' WHERE id=$1`, f.child); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Pool().Exec(t.Context(), `INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by)VALUES($1,1,'DE',6,6,1,clock_timestamp(),'actor')`, f.child); err != nil {
		t.Fatal(err)
	}
	return &parentOriginalFixture{familyKnownFixture: f, t: t, h: f.installed.originalParentFiles}
}
func (f *parentOriginalFixture) approvalPath() string {
	return "/api/users/" + f.child + "/kids/local-file-approvals"
}
func (f *parentOriginalFixture) request(method, target, body, intent, password string) *http.Request {
	var input io.Reader
	if body != "" {
		input = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, target, input)
	r.Header.Set("X-Client-ID", "synthetic-client")
	if intent != "" {
		r.Header.Set("X-Parent-Intent", intent)
	}
	if password != "" {
		r.Header.Set("X-Account-Password", password)
	}
	return r
}
func (f *parentOriginalFixture) handler(serve http.HandlerFunc) http.Handler {
	return f.installed.ManagedKnownSourceEntry("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		checked, release, err := f.pins.Authenticate(r, f.session.Token, "synthetic-client")
		if err != nil {
			http.Error(w, "actual synthetic original unavailable", 503)
			return
		}
		defer release()
		vars := map[string]string{"userID": f.child, "itemID": "known-local-item"}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 8 && parts[6] == "approved-hls" {
			vars["hlsID"] = parts[7]
			if len(parts) == 9 {
				vars["segment"] = parts[8]
			}
		}
		serve(w, mux.SetURLVars(checked, vars))
	}))
}

type parentOriginalRecorder struct {
	*familyKnownRecorder
	aborted bool
}

func (*parentOriginalRecorder) SetReadDeadline(time.Time) error { return nil }
func (f *parentOriginalFixture) serve(serve http.HandlerFunc, r *http.Request) *parentOriginalRecorder {
	w := &parentOriginalRecorder{familyKnownRecorder: newFamilyKnownRecorder()}
	w.aborted = familyKnownServe(f.handler(serve).ServeHTTP, w, r)
	return w
}
func (f *parentOriginalFixture) challenge() string {
	w := f.serve(f.installed.ServeParentApproval, f.request("POST", f.approvalPath(), `{"itemId":"known-local-item"}`, "", ""))
	var value struct {
		Intent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &value) != nil || !strings.HasPrefix(value.Intent, "pfa_") {
		f.t.Fatal("actual original parent challenge unavailable", w.Code, w.Body.String())
	}
	return value.Intent
}
func (f *parentOriginalFixture) approve() {
	intent := f.challenge()
	w := f.serve(f.installed.ServeParentApproval, f.request("POST", f.approvalPath(), `{"itemId":"known-local-item"}`, intent, "synthetic-parent-password"))
	var value struct {
		ID      string    `json:"approvalId"`
		URL     string    `json:"streamUrl"`
		Expires time.Time `json:"expiresAt"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &value) != nil || !strings.HasPrefix(value.ID, "lfa_") {
		f.t.Fatal("actual fresh original parent approval unavailable", w.Code, w.Body.String())
	}
	f.approval, f.rawURL, f.expires = value.ID, value.URL, value.Expires
}
func (f *parentOriginalFixture) approveWire() {
	intent := f.challenge()
	server := httptest.NewServer(f.handler(f.installed.ServeParentApproval))
	defer server.Close()
	r := f.request("POST", f.approvalPath(), `{"itemId":"known-local-item"}`, intent, "synthetic-parent-password")
	request, err := http.NewRequest(r.Method, server.URL+r.URL.RequestURI(), r.Body)
	if err != nil {
		f.t.Fatal(err)
	}
	request.Header = r.Header.Clone()
	request.ContentLength = r.ContentLength
	response, err := server.Client().Do(request)
	if err != nil {
		f.t.Fatal(err)
	}
	raw, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	var value struct {
		ID      string    `json:"approvalId"`
		URL     string    `json:"streamUrl"`
		Expires time.Time `json:"expiresAt"`
	}
	if err != nil || response.StatusCode != 200 || response.ContentLength != int64(len(raw)) || len(response.TransferEncoding) != 0 || json.Unmarshal(raw, &value) != nil || value.ID == "" || familyLocalRawFDs(f.t, f.path) == 0 {
		f.t.Fatal("actual complete owner-live approval framing absent", err, response.StatusCode)
	}
	f.approval, f.rawURL, f.expires = value.ID, value.URL, value.Expires
}

func (f *parentOriginalFixture) media() {
	args := []string{"-nostdin", "-v", "error", "-threads", "1", "-filter_threads", "1", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "8", "-map", "0:v:0", "-map", "1:a:0", "-c:v", "libx264", "-threads:v", "1", "-preset", "ultrafast", "-g", "25", "-c:a", "aac", "-threads:a", "1", "-f", "matroska", "-y", f.path}
	if err := exec.CommandContext(f.t.Context(), "/usr/bin/ffmpeg", args...).Run(); err != nil {
		f.t.Fatal("actual synthetic owned media", err)
	}
	info, err := os.Stat(f.path)
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err = f.store.Pool().Exec(f.t.Context(), `UPDATE local_media_items SET size_bytes=$1 WHERE id='known-local-item'`, info.Size()); err != nil {
		f.t.Fatal(err)
	}
}
func (f *parentOriginalFixture) startHLS() string {
	f.t.Helper()
	f.media()
	f.approve()
	options := ParentFileHLSOptions{Directory: f.t.TempDir(), FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe"}
	if err := f.h.ConfigureHLS(f.original, options, func(ctx context.Context) (ParentFileHLSOptions, error) { return options, ctx.Err() }); err != nil {
		f.t.Fatal(err)
	}
	base := "/api/users/" + f.child + "/library/items/known-local-item/approved-hls"
	w := f.serve(f.installed.ServeParentHLSStart, f.request("POST", base, fmt.Sprintf(`{"approvalId":%q}`, f.approval), "", ""))
	var value struct {
		ID string `json:"sessionId"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &value) != nil || len(value.ID) != 32 {
		f.t.Fatal("actual parent HLS publication failed", w.Code, w.Body.String())
	}
	return base + "/" + value.ID + "/segment000000.ts"
}

func (f *parentOriginalFixture) authenticated(method, target string) (*http.Request, context.CancelFunc) {
	f.t.Helper()
	r, release, err := f.pins.Authenticate(f.request(method, target, "", "", ""), f.session.Token, "synthetic-client")
	if err != nil {
		f.t.Fatal(err)
	}
	return mux.SetURLVars(r, map[string]string{"userID": f.child, "itemID": "known-local-item"}), release
}
func parentOriginalAsync(t *testing.T, release func(), fn func()) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() { release(); familyKnownAwait(t, done, "actual original parent work did not join") })
	go func() { defer close(done); fn() }()
	return done
}
func parentOriginalProcessWait(t *testing.T) {
	t.Helper()
	familyKnownUntil(t, "actual parent process Wait scope absent", func() bool {
		b := make([]byte, 2<<20)
		n := runtime.Stack(b, true)
		for _, g := range strings.Split(string(b[:n]), "\n\n") {
			if strings.Contains(g, "(*ParentFileProcessLease).Run(") && strings.Contains(g, "os/exec.(*Cmd).Wait(") {
				return true
			}
		}
		return false
	})
}

type parentOriginalHeldSource struct {
	kidsvod.ParentFileSource
	entered, release chan struct{}
	once             sync.Once
	reads            sync.WaitGroup
}

func (s *parentOriginalHeldSource) unblock() { s.once.Do(func() { close(s.release) }) }

type parentOriginalReader struct {
	kidsvod.FileReadAtCloser
	source *parentOriginalHeldSource
}

func (s *parentOriginalHeldSource) OpenReader(ctx context.Context) (kidsvod.FileReadAtCloser, error) {
	r, err := s.ParentFileSource.OpenReader(ctx)
	if err != nil {
		return r, err
	}
	return &parentOriginalReader{FileReadAtCloser: r, source: s}, nil
}
func (r *parentOriginalReader) ReadAt(p []byte, offset int64) (int, error) {
	r.source.reads.Add(1)
	defer r.source.reads.Done()
	select {
	case <-r.source.entered:
	default:
		close(r.source.entered)
	}
	<-r.source.release
	return r.FileReadAtCloser.ReadAt(p, offset)
}
func (s *parentOriginalHeldSource) Close() error {
	err := s.ParentFileSource.Close()
	s.reads.Wait()
	return err
}
func parentOriginalHeldRead(t *testing.T) {
	f := parentOriginal(t)
	r, release := f.authenticated("POST", f.approvalPath())
	defer release()
	var held *parentOriginalHeldSource
	intent, err := f.h.core.PrepareParentFileRequest(r, f.child, "known-local-item", func(ctx context.Context, subject kidsvod.Subject, item string) (kidsvod.ParentFileSource, error) {
		source, err := f.local.OpenParentFile(ctx, subject, item)
		if err != nil {
			return source, err
		}
		held = &parentOriginalHeldSource{ParentFileSource: source, entered: make(chan struct{}), release: make(chan struct{})}
		return held, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.unblock(); intent.Close() })
	body := []byte(`{"itemId":"known-local-item"}`)
	challenge, err := f.pins.ParentLocalFileChallenge(r, f.child, body, intent)
	if err != nil {
		t.Fatal(err)
	}
	w := f.serve(f.installed.ServeParentApproval, f.request("POST", f.approvalPath(), string(body), challenge, "synthetic-parent-password"))
	var approval struct {
		URL string `json:"streamUrl"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &approval) != nil || approval.URL == "" {
		t.Fatal("genuine wrapped original source approval failed", w.Code)
	}
	done := parentOriginalAsync(t, func() { held.unblock(); f.cancel() }, func() { f.serve(f.installed.ServeParentRaw, f.request("GET", approval.URL, "", "", "")) })
	familyKnownAwait(t, held.entered, "actual original duplicate ReadAt was not entered")
	closed := parentOriginalAsync(t, held.unblock, f.installed.Close)
	familyKnownUntil(t, "original close gate not reached", func() bool { return !f.h.managedAvailable() })
	familyKnownPending(t, closed, "Close skipped actual source-owned held ReadAt")
	held.unblock()
	familyKnownAwait(t, done, "actual ReadAt did not join")
	familyKnownAwait(t, closed, "Close did not join actual read/source cleanup")
	if familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("actual read/source descriptors leaked")
	}
}
func parentOriginalHeldPreparation(t *testing.T, closeOriginal bool) {
	f := parentOriginal(t)
	r, release := f.authenticated("POST", f.approvalPath())
	defer release()
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(unblock) }) }
	var result *kidsvod.ParentFileIntent
	var operationErr error
	done := parentOriginalAsync(t, func() { free(); f.cancel() }, func() {
		result, operationErr = f.h.core.PrepareParentFileRequest(r, f.child, "known-local-item", func(ctx context.Context, subject kidsvod.Subject, item string) (kidsvod.ParentFileSource, error) {
			source, err := f.local.OpenParentFile(ctx, subject, item)
			if err == nil {
				close(entered)
				<-unblock
			}
			return source, err
		})
	})
	familyKnownAwait(t, entered, "actual parent source opener did not return original FD")
	if familyLocalRawFDs(t, f.path) == 0 {
		t.Fatal("real source FD not held before retirement")
	}
	closed := parentOriginalAsync(t, free, f.installed.Close)
	familyKnownUntil(t, "parent root gate not closed", func() bool { return !f.h.managedAvailable() })
	familyKnownPending(t, closed, "Close skipped actual pending opener")
	if closeOriginal {
		f.cancel()
	}
	free()
	familyKnownAwait(t, done, "opener did not physically return")
	familyKnownAwait(t, closed, "Close did not join opener")
	if result != nil {
		result.Close()
	}
	if operationErr == nil || familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("late original opener published or leaked", operationErr)
	}
}

type parentOriginalHeldWriter struct {
	*familyKnownRecorder
	entered, release chan struct{}
	once             sync.Once
}

func (w *parentOriginalHeldWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return 0, context.Canceled
}
func parentOriginalHeldOutput(t *testing.T, hls bool) {
	f := parentOriginal(t)
	target := f.rawURL
	serve := http.HandlerFunc(f.installed.ServeParentRaw)
	if hls {
		target = f.startHLS()
		serve = f.installed.ServeParentHLSAsset
	} else {
		f.approve()
		target = f.rawURL
	}
	w := &parentOriginalHeldWriter{familyKnownRecorder: newFamilyKnownRecorder(), entered: make(chan struct{}), release: make(chan struct{})}
	var freeOnce sync.Once
	free := func() { freeOnce.Do(func() { close(w.release) }) }
	done := parentOriginalAsync(t, func() { free(); f.cancel() }, func() {
		familyKnownServe(f.handler(serve).ServeHTTP, w, f.request("GET", target, "", "", ""))
	})
	familyKnownAwait(t, w.entered, "actual parent body Write not entered")
	if familyLocalRawFDs(t, f.path) < 2 {
		t.Fatal("original source and actual duplicate not held")
	}
	closed := parentOriginalAsync(t, free, f.installed.Close)
	familyKnownUntil(t, "closed root not observed", func() bool { return !f.h.managedAvailable() })
	familyKnownPending(t, closed, "Close skipped actual held response work")

	free()
	familyKnownAwait(t, done, "held actual Write did not end")
	familyKnownAwait(t, closed, "Close did not join output")
	if familyLocalRawFDs(t, f.path) != 0 {
		t.Fatal("parent source/duplicate leaked")
	}
}

func TestParentFileOriginalIngressLifetime(t *testing.T) {
	t.Run("inactive-before-body-read", func(t *testing.T) {
		var i *FamilyMediaInstallation
		var read, closed atomic.Int32
		r := httptest.NewRequest("POST", "/api/users/child/kids/local-file-approvals", nil)
		r.Body = &parentOriginalCountBody{reads: &read, closes: &closed}
		w := newFamilyKnownRecorder()
		i.ManagedKnownSourceEntry("")(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("missing installation entered handler") })).ServeHTTP(w, r)
		if w.Code != 503 || read.Load() != 0 || closed.Load() != 1 {
			t.Fatal("unavailable entry read or failed to close original body")
		}
		closed.Store(0)
		r = httptest.NewRequest("POST", "/api/users/child/kids/local-file-approvals", nil)
		r.Body = &parentOriginalCountBody{reads: &read, closes: &closed}
		unsupported := httptest.NewRecorder() // No transport deadline support.
		if !familyKnownServe(i.ManagedKnownSourceEntry("")(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Fatal("unsupported output entered missing installation")
		})).ServeHTTP, unsupported, r) || read.Load() != 0 || closed.Load() != 1 {
			t.Fatal("unsupported output did not abort after original body close")
		}
	})
	t.Run("close-before-entry-zero-io", func(t *testing.T) {
		f := parentOriginal(t)
		f.installed.Close()
		before := f.wire.requests.Load()
		w := f.serve(f.installed.ServeParentApproval, f.request("POST", f.approvalPath(), `{}`, "", ""))
		if w.Code != 503 || f.wire.requests.Load() != before || familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("closed original readmitted IO")
		}
	})
	t.Run("runtime-cancel-held-approval-body", func(t *testing.T) { parentOriginalHeldInput(t, false) })
	t.Run("caller-cancel-held-approval-body", func(t *testing.T) { parentOriginalHeldInput(t, true) })
	t.Run("held-parent-read-open-before-refund", func(t *testing.T) { parentOriginalHeldPreparation(t, false) })
	t.Run("held-parent-read-before-close-join", func(t *testing.T) { parentOriginalHeldRead(t) })
	t.Run("held-parent-write-before-close-join", func(t *testing.T) { parentOriginalHeldOutput(t, false) })
	t.Run("all-admissions-signal-before-join", func(t *testing.T) {
		h := newFamilyKnownLifetime(t)
		opener := h.holdOpen()
		r, release := h.f.localRequest(t, "GET")
		h.frees = append(h.frees, release)
		done := h.start(func() { familyKnownServe(h.f.installed.ServeLocalRaw, newFamilyKnownRecorder(), r) })
		familyKnownAwait(t, opener.entered, "actual local opener not held")
		closed := h.start(h.f.installed.Close)
		familyKnownUntil(t, "all source gates were not signalled before held opener join", func() bool {
			h.f.installed.jellyfinRaw.mu.Lock()
			closed := h.f.installed.jellyfinRaw.closed
			h.f.installed.jellyfinRaw.mu.Unlock()
			return !h.f.installed.originalParentFiles.managedAvailable() && closed
		})
		familyKnownPending(t, closed, "Close skipped held actual opener")
		opener.unblock()
		familyKnownAwait(t, done, "actual opener did not end")
		familyKnownAwait(t, closed, "Close did not join both source gates")
	})
	t.Run("close-retained-prompt-source", func(t *testing.T) {
		f := parentOriginal(t)
		_ = f.challenge()
		if familyLocalRawFDs(t, f.path) == 0 {
			t.Fatal("prompt did not retain actual source")
		}
		f.installed.Close()
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("completed HTTP prompt retained source after Close")
		}
	})
	t.Run("close-retained-approval-source", func(t *testing.T) {
		f := parentOriginal(t)
		f.approve()
		if familyLocalRawFDs(t, f.path) == 0 {
			t.Fatal("approval did not retain actual source")
		}
		f.installed.Close()
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("retained approval source survived Close")
		}
	})
	t.Run("close-active-parent-hls-worker", func(t *testing.T) {
		f := parentOriginal(t)
		f.media()
		f.approve()
		r, release := f.authenticated("POST", f.approvalPath())
		defer release()
		lease, err := f.h.core.PrepareParentFileProcessRequest(r, f.child, "known-local-item", f.approval)
		if err != nil {
			t.Fatal(err)
		}
		var operationErr error
		done := parentOriginalAsync(t, func() { lease.Cancel() }, func() {
			operationErr = lease.Run(t.Context(), "/usr/bin/ffmpeg", []string{"-nostdin", "-v", "error", "-re", "-protocol_whitelist", "fd", "-fd", "3", "-i", "fd:", "-f", "null", "-"}, nil, io.Discard, io.Discard)
		})
		parentOriginalProcessWait(t)
		f.installed.Close()
		familyKnownAwait(t, done, "actual FFmpeg run not Waited")
		if operationErr == nil || familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("original FFmpeg/descriptor survived retirement", operationErr)
		}
		if _, err = f.h.core.PrepareParentFileProcessRequest(r, f.child, "known-local-item", f.approval); err == nil {
			t.Fatal("closed core readmitted process")
		}
		if _, err = lease.ReserveOutput(r, f.child, "known-local-item"); err == nil {
			t.Fatal("closed core readmitted output")
		}
	})
	t.Run("close-hls-output-reader", func(t *testing.T) { parentOriginalHeldOutput(t, true) })
	t.Run("concurrent-close-no-readmit", func(t *testing.T) {
		f := parentOriginal(t)
		_ = f.challenge()
		var wg sync.WaitGroup
		for n := 0; n < 5; n++ {
			wg.Add(1)
			go func() { defer wg.Done(); f.installed.Close() }()
		}
		wg.Wait()
		if f.h.managedAvailable() || familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("concurrent Close did not join source")
		}
		if _, finish, err := f.h.httpRuntime.begin(t.Context(), false); err == nil {
			finish()
			t.Fatal("closed ingress re-admitted")
		}
	})
	t.Run("healthy-raw-range-head-and-416-flush", func(t *testing.T) {
		f := parentOriginal(t)
		f.approve()
		for _, tc := range []struct {
			method, header string
			status         int
		}{{"GET", "", 200}, {"GET", "bytes=1-4", 206}, {"HEAD", "", 200}, {"GET", "bytes=999999-", 416}} {
			r := f.request(tc.method, f.rawURL, "", "", "")
			if tc.header != "" {
				r.Header.Set("Range", tc.header)
			}
			w := f.serve(f.installed.ServeParentRaw, r)
			if w.Code != tc.status || w.flushes.Load() == 0 {
				t.Fatal("actual healthy parent response not physically flushed", tc.method, tc.status, w.Code)
			}
		}
	})
	t.Run("healthy-approval-known-publication-flush", func(t *testing.T) {
		f := parentOriginal(t)
		f.approveWire()
		w := f.serve(f.installed.ServeParentRaw, f.request("GET", f.rawURL, "", "", ""))
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), f.data) || !f.expires.After(time.Now().Add(30*time.Second)) {
			t.Fatal("healthy original grant publication/lifetime lost")
		}
	})
	t.Run("cancelled-preheader-no-implicit-success", func(t *testing.T) {
		f := parentOriginal(t)
		f.approve()
		f.cancel()
		w := f.serve(f.installed.ServeParentRaw, f.request("GET", f.rawURL, "", "", ""))
		if w.Code < 400 || w.Body.String() == string(f.data) {
			t.Fatal("cancelled original emitted implicit success")
		}
	})
	t.Run("bounded-generic-error-framing", func(t *testing.T) {
		f := parentOriginal(t)
		r := f.request("POST", f.approvalPath(), `{"unknown":true}`, "", "")
		w := f.serve(f.installed.ServeParentApproval, r)
		if w.Code < 400 || w.Header().Get("Content-Length") != fmt.Sprint(w.Body.Len()) || w.Body.Len() == 0 || w.Body.Len() > 4096 {
			t.Fatal("bounded original denial framing incomplete", w.Code)
		}
	})
	t.Run("fixed-grant-expiry-not-http-renewed", func(t *testing.T) {
		f := parentOriginal(t)
		f.approve()
		first := f.expires
		r, release := f.authenticated("GET", f.rawURL)
		defer release()
		reader, err := f.h.core.OpenParentFileRequest(r, f.child, "known-local-item", f.approval)
		if err != nil {
			release()
			t.Fatal(err)
		}
		defer reader.Close()
		if !reader.ExpiresAt().Equal(first) {
			t.Fatal("source expiry renewed")
		}
		_ = reader.Close()
		release()
		if !f.h.managedAvailable() || familyLocalRawFDs(t, f.path) == 0 {
			t.Fatal("completed setup retired persistent original grant")
		}
		f.cancel()
		f.h.Close()
		if familyLocalRawFDs(t, f.path) != 0 {
			t.Fatal("original root cancellation did not retire retained grant")
		}
	})
	t.Run("source-identity-policy-and-pin-still-current", func(t *testing.T) {
		f := parentOriginal(t)
		f.approve()
		if err := os.Rename(f.path, f.path+".retired"); err != nil {
			t.Fatal(err)
		}
		w := f.serve(f.installed.ServeParentRaw, f.request("GET", f.rawURL, "", "", ""))
		if !w.aborted && (w.Code == 200 || w.Code == 206) || bytes.Contains(w.Body.Bytes(), f.data) {
			t.Fatal("changed source became permission")
		}
	})
	t.Run("shared-existing-capacity-no-new-permission", func(t *testing.T) {
		f := parentOriginal(t)
		f.media()
		f.approve()
		r, release := f.authenticated("POST", f.approvalPath())
		defer release()
		lease, err := f.h.core.PrepareParentFileProcessRequest(r, f.child, "known-local-item", f.approval)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Close()
		if _, err = f.h.core.PrepareParentFileProcessRequest(r, f.child, "known-local-item", f.approval); !errors.Is(err, kidsvod.ErrParentFileProcessBusy) {
			t.Fatal("same approval duplicated process reservation", err)
		}
		handlers := make([]func(), 0, 32)
		defer func() {
			for _, finish := range handlers {
				finish()
			}
		}()
		for n := 0; n < 32; n++ {
			_, finish, err := f.h.httpRuntime.begin(t.Context(), false)
			if err != nil {
				t.Fatal(err)
			}
			handlers = append(handlers, finish)
		}
		if _, finish, err := f.h.httpRuntime.begin(t.Context(), false); err == nil {
			finish()
			t.Fatal("bounded original ingress exceeded actual32")
		}
		for _, finish := range handlers {
			finish()
		}
		handlers = nil
		f.h.core.SignalParentFilesClose()
		if _, err = lease.ReserveOutput(r, f.child, "known-local-item"); err == nil {
			t.Fatal("closed same core admitted output")
		}
		lateOutput := make(chan struct{})
		defer close(lateOutput) // Failure cleanup must also retire an erroneously admitted actor.
		if err = lease.BindOutputRetirement(func() {}, lateOutput); err == nil {
			t.Fatal("closed core accepted late output owner")
		}
		if err = lease.Handoff(r); err == nil {
			t.Fatal("closed core handed off persistent owner")
		}
	})
}

type parentOriginalCountBody struct{ reads, closes *atomic.Int32 }

func (b *parentOriginalCountBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (b *parentOriginalCountBody) Close() error {
	if b.closes != nil {
		b.closes.Add(1)
	}
	return nil
}
func parentOriginalHeldInput(t *testing.T, caller bool) {
	f := parentOriginal(t)
	done := make(chan struct{})
	ready := make(chan context.CancelFunc, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		ready <- cancel
		f.handler(f.installed.ServeParentApproval).ServeHTTP(w, r.WithContext(ctx))
	}))
	listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
	server.Listener = listener
	server.Start()
	connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	var stop context.CancelFunc
	t.Cleanup(func() {
		if stop != nil {
			stop()
		}
		f.cancel()
		_ = connection.Close()
		server.Close()
	})
	if _, err = fmt.Fprintf(connection, "POST %s HTTP/1.1\r\nHost: synthetic\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{", f.approvalPath()); err != nil {
		t.Fatal(err)
	}
	var actual *shareEntryConn
	select {
	case actual = <-listener.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("no actual accepted body socket")
	}
	select {
	case stop = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no original request scope")
	}
	familyKnownUntil(t, "actual incomplete parent Body read was not blocked", func() bool { return actual.reading.Load() > 0 })
	familyKnownPending(t, done, "incomplete original body returned early")
	if caller {
		stop()
	} else {
		f.cancel()
	}
	familyKnownAwait(t, done, "actual body handler did not join cancellation")
	if !actual.demanded.Load() || actual.reading.Load() != 0 {
		t.Fatal("original cancellation did not join actual transport Read")
	}
	server.Close()
}
