//go:build linux

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/parentauth"
	"novastream/services/accounts"
	"novastream/services/kidsvod"
	"novastream/services/localmedia"
)

// This fixture obtains a real PostgreSQL parent permit for an unmatched local
// file. It tests private-output IO ownership, not successful media playback.
func TestParentFileHLSPermanentDeletionFailureClosesSourceAndRetainsAdmission(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := f.store.Pool().Exec(t.Context(), sql, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("UPDATE users SET is_kids_profile=true WHERE id=$1", f.profile.ID)
	exec("INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by) VALUES($1,1,'DE',6,6,1,clock_timestamp(),'actor')", f.profile.ID)
	sourceRoot := t.TempDir()
	sourcePath := filepath.Join(sourceRoot, "unmatched.bin")
	original := []byte("Synthetic local ownership input, never passed to a media decoder.")
	if err := os.WriteFile(sourcePath, original, 0600); err != nil {
		t.Fatal(err)
	}
	roots, _ := json.Marshal([]string{sourceRoot})
	exec("INSERT INTO local_media_libraries(id,name,library_type,root_path,root_paths) VALUES('cleanup-library','Synthetic cleanup','movie',$1,$2)", sourceRoot, roots)
	exec("INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes) VALUES('cleanup-item','cleanup-library','unmatched.bin',$1,'unmatched.bin','movie',$2)", sourcePath, len(original))
	exec("INSERT INTO media_library_access_policies(library_id,access_mode) VALUES('cleanup-library','all')")
	local, err := localmedia.NewService(f.store, nil, "/synthetic-never-run")
	if err != nil {
		t.Fatal(err)
	}
	account, err := accounts.NewServiceWithStore(f.store)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewParentLocalFileHandler(f.store, f.pins, local, parentauth.New(account))
	if err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string) *http.Request {
		t.Helper()
		r, release, e := f.pins.Authenticate(httptest.NewRequest(method, path, strings.NewReader(body)), f.session.Token, "synthetic-client")
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(release)
		return mux.SetURLVars(r, map[string]string{"userID": f.profile.ID, "itemID": "cleanup-item"})
	}
	path := "/api/users/" + f.profile.ID + "/kids/local-file-approvals"
	body := `{"itemId":"cleanup-item"}`
	first := request("POST", path, body)
	w := httptest.NewRecorder()
	h.Approve(w, first)
	var challenge struct {
		Intent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &challenge) != nil || challenge.Intent == "" {
		t.Fatalf("actual parent challenge HTTP%d", w.Code)
	}
	retry := request("POST", path, body)
	retry.Header.Set("X-Parent-Intent", challenge.Intent)
	retry.Header.Set("X-Account-Password", "synthetic-parent-password")
	w = httptest.NewRecorder()
	h.Approve(w, retry)
	var approval struct {
		ID string `json:"approvalId"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &approval) != nil || approval.ID == "" {
		t.Fatalf("actual parent approval HTTP%d", w.Code)
	}
	live := request("POST", "/synthetic-output-owner", "")
	t.Cleanup(func() { _ = h.core.RevokeParentFileRequest(live, f.profile.ID, "cleanup-item", approval.ID) })
	options := ParentFileHLSOptions{Directory: t.TempDir(), FFmpegPath: "/synthetic-never-run", FFprobePath: "/synthetic-never-run"}
	if err = h.ConfigureHLS(t.Context(), options, func(ctx context.Context) (ParentFileHLSOptions, error) { return options, ctx.Err() }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.CloseHLS)
	lease, err := h.core.PrepareParentFileProcessRequest(live, f.profile.ID, "cleanup-item", approval.ID)
	if err != nil {
		t.Fatal(err)
	}
	m := h.hlsManager()
	session, err := m.newSession(t.Context(), f.profile.ID, "cleanup-item", lease)
	if err != nil {
		lease.Close()
		t.Fatal(err)
	}
	if err = lease.Handoff(live); err != nil {
		session.close()
		t.Fatal(err)
	}
	if err = session.root.WriteFile("segment000000.ts", []byte("Synthetic private output; not claimed as valid media."), 0600); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.remove = func(*os.Root, string) error { return errors.New("synthetic permanent deletion failure") }
	m.mu.Unlock()
	// Always restore cleanup, including a failed assertion, so no charged timer
	// or source FD escapes this exact synthetic schema.
	t.Cleanup(func() { m.mu.Lock(); m.remove = nil; m.mu.Unlock(); session.retryCleanup() })
	closed := make(chan struct{})
	go func() { session.close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("output retirement hung source shutdown")
	}
	if lease.Check(t.Context()) == nil || lease.Context().Err() == nil {
		t.Fatal("retained output kept input authority alive")
	}
	if next, e := h.core.PrepareParentFileProcessRequest(live, f.profile.ID, "cleanup-item", approval.ID); next != nil || !errors.Is(e, kidsvod.ErrParentFileProcessBusy) {
		if next != nil {
			next.Close()
		}
		t.Fatal("failed deletion refunded actual private-output admission", e)
	}
	raw, e := h.core.OpenParentFileRequest(live, f.profile.ID, "cleanup-item", approval.ID)
	if e != nil {
		t.Fatal("output deletion failure revoked original raw approval", e)
	}
	raw.Close()
	m.mu.Lock()
	m.remove = nil
	m.mu.Unlock()
	session.retryCleanup()
	next, e := h.core.PrepareParentFileProcessRequest(live, f.profile.ID, "cleanup-item", approval.ID)
	if e != nil {
		t.Fatal("actual deletion did not refund reservation", e)
	}
	next.Close()
	now, e := os.ReadFile(sourcePath)
	if e != nil || string(now) != string(original) {
		t.Fatal("private-output cleanup changed original source")
	}
	m.mu.Lock()
	remaining := len(m.sessions)
	m.mu.Unlock()
	if remaining != 0 {
		t.Fatal(fmt.Sprintf("retired output registration retained: %d", remaining))
	}
}
