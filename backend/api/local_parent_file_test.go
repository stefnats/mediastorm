//go:build linux

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/parentauth"
	"novastream/services/accounts"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
)

type localParentFixture struct {
	*publicPGPinFixture
	path, root, item, token string
	files                   *handlers.ParentLocalFileHandler
}

const parentLocalSyntheticBytes = "Synthetic local file bytes: no provider, catalogue mapping, metadata probe or real media."

type localDeadlineRecorder struct {
	*httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
}

func (w *localDeadlineRecorder) SetWriteDeadline(at time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadlines = append(w.deadlines, at)
	return nil
}
func (f *localParentFixture) serve(r *http.Request) *localDeadlineRecorder {
	w := &localDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	f.router.ServeHTTP(w, r)
	return w
}
func localParentPG(t *testing.T) *localParentFixture {
	t.Helper()
	base := parentModeAPIFixture(t)
	base.enforce.Store(false)
	accountService, err := accounts.NewServiceWithStore(base.ds)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	path := filepath.Join(root, "Unmatched synthetic file [äé].bin")
	if err = os.WriteFile(path, []byte(parentLocalSyntheticBytes), 0600); err != nil {
		t.Fatal(err)
	}
	item := "synthetic-unmatched-local-file"
	roots, _ := json.Marshal([]string{root})
	base.exec(`INSERT INTO local_media_libraries(id,name,library_type,root_path,root_paths) VALUES('synthetic-local-library','Synthetic Local','movie',$1,$2)`, root, roots)
	base.exec(`INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes) VALUES($1,'synthetic-local-library',$2,$3,$2,'movie',$4)`, item, filepath.Base(path), path, len(parentLocalSyntheticBytes))
	base.exec(`INSERT INTO media_library_access_policies(library_id,access_mode) VALUES('synthetic-local-library','all')`)
	base.exec(`INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by) VALUES($1,1,'DE',6,6,1,clock_timestamp(),$2)`, base.open, base.account)
	local, err := localmedia.NewService(base.ds, nil, "/synthetic-probe-must-never-run")
	if err != nil {
		t.Fatal(err)
	}
	pins := profileunlock.NewPGProfilePins(base.ds, base.sessions)
	files, err := handlers.NewParentLocalFileHandler(base.ds, pins, local, parentauth.New(accountService))
	if err != nil {
		t.Fatal(err)
	}
	meta := handlers.NewMetadataHandler(base.facts, nil)
	meta.UsersService = base.users
	list := handlers.NewDisplayListHandler(nil, nil, base.users)
	list.SetMetadataService(base.facts)
	list.SetMetadataHandler(meta)
	media := handlers.NewLocalMediaHandler(local, base.users, true)
	base.router = mux.NewRouter()
	Register(base.router, nil, meta,
		nil, nil, nil, nil, nil, nil, &handlers.VideoHandler{},
		handlers.NewUsersHandler(base.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, nil, media,
		nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil, nil,
		accountService, base.sessions, base.users, nil, nil, nil, "", nil,
		WithPGProfilePins(pins), WithProfileUnlocks(profileunlock.NewService(base.ds.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: false} }), WithParentLocalFiles(files))
	return &localParentFixture{publicPGPinFixture: base, path: path, root: root, item: item, token: base.session(false), files: files}
}
func (f *localParentFixture) request(method, path, body, intent, password string) *http.Request {
	r := parentListWritesRequest(method, path, f.token, body, intent, password)
	return r
}
func (f *localParentFixture) challenge(t *testing.T) string {
	t.Helper()
	w := f.serve(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), "", ""))
	var out struct {
		Intent string `json:"parentIntent"`
	}
	if w.Code != 403 || json.Unmarshal(w.Body.Bytes(), &out) != nil || !strings.HasPrefix(out.Intent, "pfa_") {
		t.Fatalf("file challenge HTTP %d body=%q", w.Code, w.Body.String())
	}
	return out.Intent
}
func (f *localParentFixture) approve(t *testing.T) (string, string) {
	t.Helper()
	intent := f.challenge(t)
	w := f.serve(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), intent, "synthetic-api-password"))
	var out struct {
		ID      string `json:"approvalId"`
		URL     string `json:"streamUrl"`
		Outcome string `json:"outcome"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || !strings.HasPrefix(out.ID, "lfa_") || out.Outcome != "parent-file-exception" || !strings.Contains(out.URL, "/approved-file?") {
		t.Fatalf("actual parent file approval HTTP %d body=%q", w.Code, w.Body.String())
	}
	t.Cleanup(func() { _ = f.serve(f.request("DELETE", out.URL, "", "", "")) })
	return out.ID, out.URL
}
func TestParentLocalFileRegisteredActualPGUnmatchedFileFreshPasswordAndRanges(t *testing.T) {
	f := localParentPG(t)
	before := parentModeAPIRow(t, f.publicPGPinFixture, f.open)
	id, path := f.approve(t)
	if id == "" || strings.Contains(path, "/video/stream") {
		t.Fatal("file exception escaped exact route")
	}
	for _, tc := range []struct {
		method, header, want string
		status               int
	}{{"GET", "", parentLocalSyntheticBytes, 200}, {"GET", "bytes=10-19", parentLocalSyntheticBytes[10:20], 206}, {"GET", "bytes=80-", parentLocalSyntheticBytes[80:], 206}, {"GET", "bytes=-6", parentLocalSyntheticBytes[len(parentLocalSyntheticBytes)-6:], 206}, {"HEAD", "bytes=2-8", "", 206}} {
		r := f.request(tc.method, path, "", "", "")
		if tc.header != "" {
			r.Header.Set("Range", tc.header)
		}
		w := f.serve(r)
		if w.Code != tc.status || w.Body.String() != tc.want || w.Header().Get("Cache-Control") != "private, no-store" || len(w.deadlines) == 0 {
			t.Fatalf("range %q/%s HTTP %d bytes=%q", tc.header, tc.method, w.Code, w.Body.String())
		}
	}
	after := parentModeAPIRow(t, f.publicPGPinFixture, f.open)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Fatal("file approval changed profile fields")
	}
}
func TestParentLocalFileRegisteredConcurrentRangesHaveIndependentOffsets(t *testing.T) {
	f := localParentPG(t)
	_, path := f.approve(t)
	var wg sync.WaitGroup
	results := make(chan bool, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			r := f.request("GET", path, "", "", "")
			r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+3))
			w := f.serve(r)
			results <- w.Code == 206 && w.Body.String() == parentLocalSyntheticBytes[offset:offset+4]
		}(i * 3)
	}
	wg.Wait()
	close(results)
	for ok := range results {
		if !ok {
			t.Fatal("concurrent range shared offset or lost permission")
		}
	}
}
func TestParentLocalFileRegisteredMalformedRangesAreCheckedAndBounded(t *testing.T) {
	f := localParentPG(t)
	_, path := f.approve(t)
	for _, header := range []string{"bytes=999999-", "bytes=2-1", "bytes=-0", "bytes=0-1,3-4", "items=0-1", "bytes=+1-2", "bytes=0-999999999999999999999999"} {
		r := f.request("GET", path, "", "", "")
		r.Header.Set("Range", header)
		w := f.serve(r)
		if w.Code != 416 || w.Body.Len() != 0 || !strings.HasPrefix(w.Header().Get("Content-Range"), "bytes */") {
			t.Fatalf("malformed range HTTP%d", w.Code)
		}
	}
	r := f.request("GET", path, "", "", "")
	r.Header.Set("If-Range", "arbitrary")
	if w := f.serve(r); w.Code != 400 {
		t.Fatal("conditional range fallback")
	}
}
func TestParentLocalFileRegisteredPromptHoldsOriginalFDThroughPathReplacement(t *testing.T) {
	f := localParentPG(t)
	intent := f.challenge(t)
	if err := os.Rename(f.path, f.path+".original"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.path, []byte(strings.Repeat("x", len(parentLocalSyntheticBytes))), 0600); err != nil {
		t.Fatal(err)
	}
	w := f.serve(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), intent, "synthetic-api-password"))
	if w.Code == 200 {
		t.Fatal("parent approved replacement pathname")
	}
}
func TestParentLocalFileRegisteredSourceACLPolicyAndIdentityDriftDenyBytes(t *testing.T) {
	for _, change := range []string{"same inode write", "file replacement", "root replacement", "ACL revoke restore", "local row ABA", "regional policy", "profile ownership", "PIN added", "session client", "session scope", "account expiry"} {
		t.Run(change, func(t *testing.T) {
			f := localParentPG(t)
			_, path := f.approve(t)
			switch change {
			case "same inode write":
				if err := os.WriteFile(f.path, []byte(strings.Repeat("x", len(parentLocalSyntheticBytes))), 0600); err != nil {
					t.Fatal(err)
				}
			case "file replacement":
				if err := os.Rename(f.path, f.path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.path, []byte(parentLocalSyntheticBytes), 0600); err != nil {
					t.Fatal(err)
				}
			case "root replacement":
				if err := os.Rename(f.root, f.root+".old"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.RemoveAll(f.root + ".old") })
				if err := os.Mkdir(f.root, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.path, []byte(parentLocalSyntheticBytes), 0600); err != nil {
					t.Fatal(err)
				}
			case "ACL revoke restore":
				f.exec(`UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='synthetic-local-library'; UPDATE media_library_access_policies SET access_mode='all' WHERE library_id='synthetic-local-library'`)
			case "local row ABA":
				f.exec(`UPDATE local_media_items SET is_missing=true WHERE id=$1`, f.item)
				f.exec(`UPDATE local_media_items SET is_missing=false WHERE id=$1`, f.item)
			case "regional policy":
				f.exec(`UPDATE regional_profile_policies SET max_movie_age=18 WHERE user_id=$1`, f.open)
			case "profile ownership":
				f.exec(`UPDATE users SET account_id=$1 WHERE id=$2`, f.other, f.open)
			case "PIN added":
				if _, err := f.users.SetPin(f.open, "synthetic-pin"); err != nil {
					t.Fatal(err)
				}
			case "session client":
				f.exec(`UPDATE sessions SET client_id='other' WHERE token=$1`, f.token)
			case "session scope":
				f.exec(`UPDATE sessions SET scope='stream',scope_resource='unrelated' WHERE token=$1`, f.token)
			case "account expiry":
				f.exec(`UPDATE accounts SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, f.account)
			}
			w := f.serve(f.request("GET", path, "", "", ""))
			if w.Code == 200 || w.Code == 206 || strings.Contains(w.Body.String(), parentLocalSyntheticBytes) {
				t.Fatalf("changed %s still emitted file HTTP%d", change, w.Code)
			}
		})
	}
}
func TestParentLocalFileRegisteredGlobalPINFlagFalseDoesNotUnlockChild(t *testing.T) {
	f := localParentPG(t)
	if _, err := f.users.SetPin(f.open, "synthetic-parent-file-pin"); err != nil {
		t.Fatal(err)
	}
	w := f.serve(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), "", "synthetic-api-password"))
	if w.Code != 403 || strings.Contains(w.Body.String(), "parentIntent") {
		t.Fatal("fresh password or global false bypassed child PIN")
	}
}
func TestParentLocalFileRegisteredWrongPasswordRetiresHeldPromptAndReplay(t *testing.T) {
	f := localParentPG(t)
	intent := f.challenge(t)
	path := "/api/users/" + f.open + "/kids/local-file-approvals"
	body := fmt.Sprintf(`{"itemId":%q}`, f.item)
	if w := f.serve(f.request("POST", path, body, intent, "wrong")); w.Code != 403 {
		t.Fatal("wrong parent proof accepted")
	}
	if w := f.serve(f.request("POST", path, body, intent, "synthetic-api-password")); w.Code != 409 {
		t.Fatal("failed prompt reusable")
	}
}
func TestParentLocalFileRegisteredChildPlaybackNeverProbesOrDelegates(t *testing.T) {
	f := localParentPG(t)
	path := "/api/library/items/" + f.item + "/playback?profileId=" + f.open
	if w := f.serve(f.request("GET", path, "", "", "")); w.Code != 403 || !strings.Contains(w.Body.String(), "kids_vod_parent_approval_required") {
		t.Fatal("unapproved child reached probe/remote fallback")
	}
	id, _ := f.approve(t)
	w := f.serve(f.request("GET", path+"&approvalId="+id, "", "", ""))
	if w.Code != 200 || strings.Contains(w.Body.String(), "/api/video/") || !strings.Contains(w.Body.String(), "approved-file") || strings.Contains(w.Body.String(), f.root) {
		t.Fatalf("approved playback escaped exact raw path HTTP%d", w.Code)
	}
}
func TestParentLocalFileRegisteredLocatorCannotTransferToOtherSessionOrProfile(t *testing.T) {
	f := localParentPG(t)
	_, path := f.approve(t)
	original := f.token
	f.token = f.session(false)
	if w := f.serve(f.request("GET", path, "", "", "")); w.Code == 200 || w.Code == 206 {
		t.Fatal("file locator transferred to another full session")
	}
	f.token = original
	for _, changed := range []string{strings.Replace(path, f.open, f.foreign, 1), strings.Replace(path, f.item, "other-item", 1), path + "&profileId=" + f.foreign, path + "&approvalId=other", path + "&path=/etc/passwd"} {
		if w := f.serve(f.request("GET", changed, "", "", "")); w.Code == 200 || w.Code == 206 {
			t.Fatal("locator adopted changed selector")
		}
	}
}
func TestParentLocalFileRegisteredRevokeClosesEveryLaterRange(t *testing.T) {
	f := localParentPG(t)
	_, path := f.approve(t)
	if w := f.serve(f.request("DELETE", path, "", "", "")); w.Code != 204 {
		t.Fatalf("revoke HTTP%d", w.Code)
	}
	if w := f.serve(f.request("GET", path, "", "", "")); w.Code == 200 || w.Code == 206 {
		t.Fatal("revoked file emitted more bytes")
	}
}

type blockedLocalFileWriter struct {
	*localDeadlineRecorder
	entered, unblock chan struct{}
	once             sync.Once
}

func (w *blockedLocalFileWriter) Write(_ []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.unblock
	return 0, context.DeadlineExceeded
}
func (w *blockedLocalFileWriter) SetWriteDeadline(at time.Time) error {
	_ = w.localDeadlineRecorder.SetWriteDeadline(at)
	if !at.After(time.Now()) {
		select {
		case <-w.unblock:
		default:
			close(w.unblock)
		}
	}
	return nil
}
func TestParentLocalFileRegisteredBlockedWriteCanceledByCurrentACLOrRequest(t *testing.T) {
	for _, change := range []string{"current ACL", "original request"} {
		t.Run(change, func(t *testing.T) {
			f := localParentPG(t)
			_, path := f.approve(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := f.request("GET", path, "", "", "").WithContext(ctx)
			w := &blockedLocalFileWriter{localDeadlineRecorder: &localDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}, entered: make(chan struct{}), unblock: make(chan struct{})}
			done := make(chan struct{})
			start := time.Now()
			go func() { defer close(done); f.router.ServeHTTP(w, r) }()
			select {
			case <-w.entered:
			case <-time.After(10 * time.Second):
				cancel()
				t.Fatal("checked stream never reached controlled write")
			}
			if change == "current ACL" {
				f.exec(`UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='synthetic-local-library'`)
			} else {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				cancel()
				t.Fatal("blocked write survived current cancellation")
			}
			w.mu.Lock()
			deadlines := append([]time.Time(nil), w.deadlines...)
			w.mu.Unlock()
			if len(deadlines) < 2 || !deadlines[0].After(start) || deadlines[len(deadlines)-1].After(time.Now()) || w.Body.Len() != 0 {
				t.Fatal("original deadline was not installed first, or canceled bytes escaped")
			}
		})
	}
}
func TestParentLocalFileRegisteredUnsafeFilesAndUnconfiguredChildrenDenyBeforePrompt(t *testing.T) {
	for _, change := range []string{"symlink", "hard link", "empty file", "missing file", "unconfigured child", "missing ACL", "restricted master"} {
		t.Run(change, func(t *testing.T) {
			f := localParentPG(t)
			switch change {
			case "symlink":
				if err := os.Rename(f.path, f.path+".real"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(f.path+".real", f.path); err != nil {
					t.Fatal(err)
				}
			case "hard link":
				if err := os.Link(f.path, f.path+".alias"); err != nil {
					t.Fatal(err)
				}
			case "empty file":
				if err := os.WriteFile(f.path, nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "missing file":
				if err := os.Remove(f.path); err != nil {
					t.Fatal(err)
				}
			case "unconfigured child":
				f.exec(`DELETE FROM regional_profile_policies WHERE user_id=$1`, f.open)
			case "missing ACL":
				f.exec(`DELETE FROM media_library_access_policies WHERE library_id='synthetic-local-library'`)
			case "restricted master":
				f.exec(`UPDATE accounts SET is_master=true WHERE id=$1`, f.account)
				f.exec(`UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='synthetic-local-library'`)
			}
			w := f.serve(f.request("POST", "/api/users/"+f.open+"/kids/local-file-approvals", fmt.Sprintf(`{"itemId":%q}`, f.item), "", "synthetic-api-password"))
			if w.Code == 200 || strings.Contains(w.Body.String(), "parentIntent") {
				t.Fatalf("unsupported %s created a file prompt/approval", change)
			}
		})
	}
}
