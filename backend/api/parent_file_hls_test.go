//go:build linux

package api

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"novastream/api/openapi"
	"novastream/handlers"
)

type parentHLSAPIReply struct {
	SessionID   string    `json:"sessionId"`
	PlaylistURL string    `json:"playlistUrl"`
	ExpiresAt   time.Time `json:"expiresAt"`
	Outcome     string    `json:"outcome"`
}

type parentHLSAPIFixture struct {
	*localParentFixture
	approval, rawURL string
	original         []byte
	mu               sync.Mutex
	options          handlers.ParentFileHLSOptions
}

// All media is generated locally before the actual parent FD/approval is captured.
// The real process runs only when Root explicitly executes this synthetic test suite.
func parentHLSAPIMedia(t *testing.T, extraAudio bool) *parentHLSAPIFixture {
	t.Helper()
	f := parentHLSAPIUnapprovedMedia(t, "mpegts", "3", extraAudio)
	f.approval, f.rawURL = f.approve(t)
	f.configure(t)
	return f
}
func parentHLSAPIUnapprovedMedia(t *testing.T, container, duration string, extraAudio bool) *parentHLSAPIFixture {
	t.Helper()
	f := &parentHLSAPIFixture{localParentFixture: localParentPG(t)}
	args := []string{"-nostdin", "-v", "error", "-threads", "1", "-filter_threads", "1", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", duration, "-map", "0:v:0", "-map", "1:a:0"}
	if extraAudio {
		args = append(args, "-map", "1:a:0")
	}
	// Default MP4 placement deliberately leaves moov after mdat. No faststart,
	// fragmented container, or preparatory path-based conversion is permitted.
	args = append(args, "-c:v", "libx264", "-threads:v", "1", "-preset", "ultrafast", "-g", "25", "-keyint_min", "25", "-sc_threshold", "0", "-c:a", "aac", "-threads:a", "1", "-f", container, "-y", f.path)
	if err := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", args...).Run(); err != nil {
		t.Fatalf("synthetic parent HLS input: %v", err)
	}
	var err error
	f.original, err = os.ReadFile(f.path)
	if err != nil || len(f.original) == 0 {
		t.Fatal("synthetic parent media is empty", err)
	}
	f.exec(`UPDATE local_media_items SET size_bytes=$1 WHERE id=$2`, len(f.original), f.item)
	return f
}

// Inspect top-level boxes, rather than finding an incidental byte string inside
// encoded samples, to prove that the accepted MP4 requires a seek back to mdat.
func parentHLSTailMoov(raw []byte) bool {
	var mdat, moov int = -1, -1
	for at := 0; at < len(raw); {
		if len(raw)-at < 8 {
			return false
		}
		size := uint64(binary.BigEndian.Uint32(raw[at : at+4]))
		header := uint64(8)
		if size == 1 {
			if len(raw)-at < 16 {
				return false
			}
			size = binary.BigEndian.Uint64(raw[at+8 : at+16])
			header = 16
		}
		if size == 0 {
			size = uint64(len(raw) - at)
		}
		if size < header || size > uint64(len(raw)-at) {
			return false
		}
		switch string(raw[at+4 : at+8]) {
		case "mdat":
			mdat = at
		case "moov":
			moov = at
		}
		at += int(size)
	}
	return mdat >= 0 && moov > mdat
}
func (f *parentHLSAPIFixture) configure(t *testing.T) {
	t.Helper()
	f.options = handlers.ParentFileHLSOptions{Directory: t.TempDir(), FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe"}
	if err := f.files.ConfigureHLS(t.Context(), f.options, func(ctx context.Context) (handlers.ParentFileHLSOptions, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.options, ctx.Err()
	}); err != nil {
		t.Fatal("configure isolated synthetic parent HLS", err)
	}
	// Registered after approval cleanup: join HLS workers/readers before raw grant teardown.
	t.Cleanup(f.files.CloseHLS)
}
func (f *parentHLSAPIFixture) base() string {
	return "/api/users/" + f.open + "/library/items/" + f.item + "/approved-hls"
}
func (f *parentHLSAPIFixture) start(t *testing.T) parentHLSAPIReply {
	t.Helper()
	// Observe the actual raw approval deadline before every HLS start. HLS
	// must reuse that original deadline instead of starting another grant TTL.
	rawHead := f.serve(f.request(http.MethodHead, f.rawURL, "", "", ""))
	if rawHead.Code != 200 || rawHead.Body.Len() != 0 || len(rawHead.deadlines) == 0 {
		t.Fatalf("original raw HEAD HTTP%d", rawHead.Code)
	}
	originalExpiry := rawHead.deadlines[0]
	body := fmt.Sprintf(`{"approvalId":%q}`, f.approval)
	w := f.serve(f.request(http.MethodPost, f.base(), body, "", ""))
	var reply parentHLSAPIReply
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &reply) != nil || !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(reply.SessionID) || reply.PlaylistURL != f.base()+"/"+reply.SessionID+"/stream.m3u8" || reply.Outcome != "parent-file-exception" || !reply.ExpiresAt.After(time.Now()) || reply.ExpiresAt.After(time.Now().Add(2*time.Hour)) {
		t.Fatalf("actual approved HLS start HTTP%d", w.Code)
	}
	if !reply.ExpiresAt.Equal(originalExpiry) {
		t.Fatal("HLS renewed the actual raw approval deadline")
	}
	if strings.Contains(reply.PlaylistURL, "?") || strings.Contains(w.Body.String(), f.root) || strings.Contains(w.Body.String(), f.approval) {
		t.Fatal("HLS locator leaked source or grant")
	}
	v, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r := f.request(http.MethodPost, f.base(), body, "", "")
	if err = v.ValidateRequest(r, []byte(body)); err != nil {
		t.Fatal(err)
	}
	if err = v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
	return reply
}
func parentHLSAPISegments(t *testing.T, playlist []byte) []string {
	t.Helper()
	if !strings.HasPrefix(string(playlist), "#EXTM3U") || strings.Contains(string(playlist), "URI=") || strings.Contains(string(playlist), "://") || strings.Contains(string(playlist), "?") {
		t.Fatal("not a checked HLS playlist")
	}
	var names []string
	for _, line := range strings.Split(string(playlist), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !regexp.MustCompile(`^segment[0-9]{6}\.ts$`).MatchString(line) || strings.ContainsAny(line, "/?#:") {
			t.Fatal("playlist contains nonlocal media reference")
		}
		names = append(names, line)
	}
	if len(names) == 0 || len(names) > 2048 {
		t.Fatal("playlist inventory is not bounded")
	}
	return names
}
func (f *parentHLSAPIFixture) sameSource(t *testing.T) {
	t.Helper()
	now, err := os.ReadFile(f.path)
	if err != nil || sha256.Sum256(now) != sha256.Sum256(f.original) {
		t.Fatal("HLS modified original approved file")
	}
}

func TestParentFileHLSRegisteredUnconfiguredNeverPromotesRawApproval(t *testing.T) {
	f := localParentPG(t)
	id, _ := f.approve(t)
	base := "/api/users/" + f.open + "/library/items/" + f.item + "/approved-hls"
	w := f.serve(f.request(http.MethodPost, base, fmt.Sprintf(`{"approvalId":%q}`, id), "", ""))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured HLS HTTP%d", w.Code)
	}
}
func TestParentFileHLSRegisteredActualPGPlaylistSegmentsHEADAndStop(t *testing.T) {
	f := parentHLSAPIMedia(t, false)
	before := parentModeAPIRow(t, f.publicPGPinFixture, f.open)
	reply := f.start(t)
	playlist := f.serve(f.request(http.MethodGet, reply.PlaylistURL, "", "", ""))
	if playlist.Code != 200 || playlist.Header().Get("Content-Type") != "application/vnd.apple.mpegurl" || playlist.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatalf("playlist HTTP%d", playlist.Code)
	}
	names := parentHLSAPISegments(t, playlist.Body.Bytes())
	segmentURL := strings.TrimSuffix(reply.PlaylistURL, "stream.m3u8") + names[0]
	for _, path := range []string{reply.PlaylistURL, segmentURL} {
		get := f.serve(f.request(http.MethodGet, path, "", "", ""))
		head := f.serve(f.request(http.MethodHead, path, "", "", ""))
		if get.Code != 200 || get.Body.Len() == 0 || head.Code != 200 || head.Body.Len() != 0 || len(get.deadlines) == 0 || len(head.deadlines) == 0 || head.Header().Get("Content-Type") != get.Header().Get("Content-Type") {
			t.Fatal("actual GET/HEAD lost original bounded media authority")
		}
		v, err := openapi.NewValidator(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			r := f.request(method, path, "", "", "")
			w := get
			if method == http.MethodHead {
				w = head
			}
			if err = v.ValidateRequest(r, nil); err != nil {
				t.Fatal(err)
			}
			if err = v.ValidateResponse(r, w.Code, w.Header(), w.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
		}
	}
	segment := f.serve(f.request(http.MethodGet, segmentURL, "", "", ""))
	if segment.Header().Get("Content-Type") != "video/mp2t" || segment.Body.Len() < 188 || segment.Body.Bytes()[0] != 0x47 {
		t.Fatal("checked segment is not MPEGTS")
	}
	stopURL := f.base() + "/" + reply.SessionID
	if w := f.serve(f.request(http.MethodDelete, stopURL, "", "", "")); w.Code != 204 || w.Body.Len() != 0 {
		t.Fatalf("stop HTTP%d", w.Code)
	}
	if w := f.serve(f.request(http.MethodGet, reply.PlaylistURL, "", "", "")); w.Code == 200 {
		t.Fatal("retired HLS locator remained readable")
	}
	raw := f.serve(f.request(http.MethodGet, f.rawURL, "", "", ""))
	if raw.Code != 200 || sha256.Sum256(raw.Body.Bytes()) != sha256.Sum256(f.original) {
		t.Fatal("stopping HLS revoked or altered original raw file exception")
	}
	f.sameSource(t)
	if fmt.Sprint(before) != fmt.Sprint(parentModeAPIRow(t, f.publicPGPinFixture, f.open)) {
		t.Fatal("HLS changed child classification/profile fields")
	}
}
func TestParentFileHLSRegisteredStrictStartBodyAndSelectors(t *testing.T) {
	f := parentHLSAPIMedia(t, false)
	for _, body := range []string{"", `{}`, `{"approvalId":"invalid"}`, fmt.Sprintf(`{"approvalId":%q,"approvalId":%q}`, f.approval, f.approval), fmt.Sprintf(`{"approvalId":%q,"url":"http://127.0.0.1/forbidden"}`, f.approval), fmt.Sprintf(`{"approvalId":%q} {}`, f.approval), strings.Repeat("x", 1025)} {
		if w := f.serve(f.request(http.MethodPost, f.base(), body, "", "")); w.Code == 200 {
			t.Fatal("malformed/unsupported start accepted")
		}
	}
	body := fmt.Sprintf(`{"approvalId":%q}`, f.approval)
	for _, query := range []string{"?approvalId=" + f.approval, "?profileId=" + f.open, "?source=unsupported"} {
		if w := f.serve(f.request(http.MethodPost, f.base()+query, body, "", "")); w.Code == 200 {
			t.Fatal("HLS accepted query selector")
		}
	}
	f.sameSource(t)
}
func TestParentFileHLSRegisteredNoLocatorTransferOrRangeFallback(t *testing.T) {
	f := parentHLSAPIMedia(t, false)
	reply := f.start(t)
	for _, change := range []string{"other session", "client", "scope", "foreign profile", "foreign item", "query", "body", "range", "filename"} {
		r := f.request(http.MethodGet, reply.PlaylistURL, "", "", "")
		switch change {
		case "other session":
			r.Header.Set("Authorization", "Bearer "+f.session(false))
		case "client":
			r.Header.Set("X-Client-ID", "replacement-client")
		case "scope":
			scoped, err := f.sessions.CreateScopedWithResource(f.account, false, "synthetic-hls", "127.0.0.1", time.Hour, "stream", "recording:synthetic")
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Authorization", "Bearer "+scoped.Token)
		case "foreign profile":
			r = f.request(http.MethodGet, strings.Replace(reply.PlaylistURL, f.open, f.foreign, 1), "", "", "")
		case "foreign item":
			r = f.request(http.MethodGet, strings.Replace(reply.PlaylistURL, f.item, "other-local-item", 1), "", "", "")
		case "query":
			r.URL.RawQuery = "approvalId=" + f.approval
		case "body":
			r = f.request(http.MethodGet, reply.PlaylistURL, `{}`, "", "")
		case "range":
			r.Header.Set("Range", "bytes=0-1")
		case "filename":
			r = f.request(http.MethodGet, strings.Replace(reply.PlaylistURL, "stream.m3u8", "segment002048.ts", 1), "", "", "")
		}
		w := f.serve(r)
		if w.Code == 200 || w.Code == 206 {
			t.Fatalf("HLS locator accepted %s", change)
		}
		if change == "range" && w.Code != 416 {
			t.Fatalf("authorized Range HTTP%d", w.Code)
		}
		if original := f.serve(f.request(http.MethodGet, reply.PlaylistURL, "", "", "")); original.Code != 200 {
			t.Fatalf("rejected %s retired valid original session", change)
		}
	}
	f.sameSource(t)
}
func TestParentFileHLSRegisteredCurrentGrantDriftClosesAllLaterReads(t *testing.T) {
	for _, change := range []string{"ACL ABA", "source bytes", "PIN added", "session client", "regional policy", "current tools"} {
		t.Run(change, func(t *testing.T) {
			f := parentHLSAPIMedia(t, false)
			reply := f.start(t)
			switch change {
			case "ACL ABA":
				f.exec(`UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='synthetic-local-library'; UPDATE media_library_access_policies SET access_mode='all' WHERE library_id='synthetic-local-library'`)
			case "source bytes":
				file, err := os.OpenFile(f.path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = file.Write([]byte("changed"))
				file.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "PIN added":
				if _, err := f.users.SetPin(f.open, "synthetic-new-pin"); err != nil {
					t.Fatal(err)
				}
			case "session client":
				f.exec(`UPDATE sessions SET client_id='replacement-client' WHERE token=$1`, f.token)
			case "regional policy":
				f.exec(`UPDATE regional_profile_policies SET max_movie_age=18 WHERE user_id=$1`, f.open)
			case "current tools":
				f.mu.Lock()
				f.options.FFprobePath = "/synthetic-replaced-tool"
				f.mu.Unlock()
			}
			if w := f.serve(f.request(http.MethodGet, reply.PlaylistURL, "", "", "")); w.Code == 200 {
				t.Fatalf("changed %s still readable", change)
			}
		})
	}
}

type parentHLSBlockedWriter struct {
	*localDeadlineRecorder
	entered, unblock         chan struct{}
	enteredOnce, unblockOnce sync.Once
}

func (w *parentHLSBlockedWriter) Write(_ []byte) (int, error) {
	w.enteredOnce.Do(func() { close(w.entered) })
	<-w.unblock
	return 0, context.DeadlineExceeded
}
func (w *parentHLSBlockedWriter) SetWriteDeadline(at time.Time) error {
	_ = w.localDeadlineRecorder.SetWriteDeadline(at)
	if !at.After(time.Now()) {
		w.unblockOnce.Do(func() { close(w.unblock) })
	}
	return nil
}

func TestParentFileHLSRegisteredBlockedWritesJoinOnACLOrOriginalRequest(t *testing.T) {
	for _, change := range []string{"ACL", "request"} {
		t.Run(change, func(t *testing.T) {
			f := parentHLSAPIMedia(t, false)
			reply := f.start(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := f.request(http.MethodGet, reply.PlaylistURL, "", "", "").WithContext(ctx)
			w := &parentHLSBlockedWriter{localDeadlineRecorder: &localDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}, entered: make(chan struct{}), unblock: make(chan struct{})}
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				defer func() {
					value := recover()
					if value != nil && value != http.ErrAbortHandler {
						done <- fmt.Errorf("unexpected HLS transfer panic: %T", value)
					} else {
						done <- nil
					}
				}()
				f.router.ServeHTTP(w, r)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-finished:
				case <-time.After(10 * time.Second):
					_ = w.SetWriteDeadline(time.Now())
					select {
					case <-finished:
					case <-time.After(time.Second):
						t.Error("controlled HLS response failed to join")
					}
				}
			})
			select {
			case <-w.entered:
			case err := <-done:
				t.Fatalf("HLS never reached controlled write: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("HLS write not reached")
			}
			if change == "ACL" {
				f.exec(`UPDATE media_library_access_policies SET access_mode='restricted' WHERE library_id='synthetic-local-library'`)
			} else {
				cancel()
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				cancel()
				t.Fatal("HLS write survived original/current cancellation")
			}
			w.mu.Lock()
			deadlines := append([]time.Time(nil), w.deadlines...)
			w.mu.Unlock()
			if len(deadlines) < 2 || deadlines[len(deadlines)-1].After(time.Now()) || w.Body.Len() != 0 {
				t.Fatal("HLS blocked transport did not retire without bytes")
			}
		})
	}
}
func TestParentFileHLSRegisteredNestedPlaylistCannotFetchOutsideApprovedFD(t *testing.T) {
	var hits atomic.Int32
	canary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); w.WriteHeader(500) }))
	defer canary.Close()
	f := &parentHLSAPIFixture{localParentFixture: localParentPG(t)}
	f.original = []byte("#EXTM3U\n#EXTINF:3,\n" + canary.URL + "/outside-grant.ts\n#EXT-X-ENDLIST\n")
	if err := os.WriteFile(f.path, f.original, 0600); err != nil {
		t.Fatal(err)
	}
	f.exec(`UPDATE local_media_items SET size_bytes=$1 WHERE id=$2`, len(f.original), f.item)
	f.approval, f.rawURL = f.approve(t)
	f.configure(t)
	w := f.serve(f.request(http.MethodPost, f.base(), fmt.Sprintf(`{"approvalId":%q}`, f.approval), "", ""))
	if w.Code != 409 || hits.Load() != 0 {
		t.Fatalf("nested source HTTP%d canary=%d", w.Code, hits.Load())
	}
	f.sameSource(t)
}
func TestParentFileHLSRegisteredExtraAudioCannotBeSilentlyDropped(t *testing.T) {
	f := parentHLSAPIMedia(t, true)
	w := f.serve(f.request(http.MethodPost, f.base(), fmt.Sprintf(`{"approvalId":%q}`, f.approval), "", ""))
	if w.Code != 409 {
		t.Fatalf("extra source track HTTP%d", w.Code)
	}
	f.sameSource(t)
}
func TestParentFileHLSRegisteredOneApprovalAdmissionAndExpiryAreNotRenewed(t *testing.T) {
	f := parentHLSAPIMedia(t, false)
	first := f.start(t)
	w := f.serve(f.request(http.MethodPost, f.base(), fmt.Sprintf(`{"approvalId":%q}`, f.approval), "", ""))
	if w.Code != 429 {
		t.Fatalf("same approval admitted simultaneous HLS HTTP%d", w.Code)
	}
	stopDone := make(chan *localDeadlineRecorder, 1)
	go func() { stopDone <- f.serve(f.request(http.MethodDelete, f.base()+"/"+first.SessionID, "", "", "")) }()
	select {
	case response := <-stopDone:
		if response.Code != 204 {
			t.Fatalf("stop HTTP%d", response.Code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("actual successful handoff did not join DELETE stop")
	}

	second := f.start(t)
	if second.SessionID == first.SessionID || !second.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatal("explicit HLS restart renewed original grant or reused retired locator")
	}
	f.sameSource(t)
}

func TestParentFileHLSRegisteredOriginalRequestCancelsActualProbeAndRefundsCapacity(t *testing.T) {
	f := parentHLSAPIMedia(t, false)
	scratch := t.TempDir()
	pidFile := filepath.Join(scratch, "probe.pid")
	script := filepath.Join(scratch, "blocked-probe")
	quoted := "'" + strings.ReplaceAll(pidFile, "'", "'\"'\"'") + "'"
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' \"$$\" > "+quoted+"\nexec /usr/bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	configure := func(tool string) {
		t.Helper()
		f.mu.Lock()
		f.options.FFprobePath = tool
		options := f.options
		f.mu.Unlock()
		if err := f.files.ConfigureHLS(t.Context(), options, func(ctx context.Context) (handlers.ParentFileHLSOptions, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.options, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
	}
	configure(script)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r := f.request(http.MethodPost, f.base(), fmt.Sprintf(`{"approvalId":%q}`, f.approval), "", "").WithContext(ctx)
	done := make(chan *localDeadlineRecorder, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- f.serve(r) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("actual probe request failed to join cleanup")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	pid := 0
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(pidFile)
		if err == nil {
			pid, err = strconv.Atoi(string(raw))
			if err == nil && pid > 1 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid <= 1 || syscall.Kill(pid, 0) != nil {
		cancel()
		t.Fatal("actual configured probe did not reach held process")
	}
	cancel()
	select {
	case response := <-done:
		if response.Code == 200 {
			t.Fatal("cancelled original preparation returned ready")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("actual probe preparation did not join cancellation")
	}
	if !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		t.Fatal("actual probe PID survived completed cancellation")
	}
	configure("/usr/bin/ffprobe")
	f.start(t) // The original raw approval remains valid; all preparation budget was refunded.
	f.sameSource(t)
}

func TestParentFileHLSRegisteredSeekableTSMKVAndTailMetadataMP4(t *testing.T) {
	for _, container := range []string{"mpegts", "matroska", "mp4"} {
		t.Run(container, func(t *testing.T) {
			f := parentHLSAPIUnapprovedMedia(t, container, "3", false)
			if container == "mp4" && !parentHLSTailMoov(f.original) {
				t.Fatal("synthetic MP4 did not place its actual moov box after mdat")
			}
			f.approval, f.rawURL = f.approve(t)
			f.configure(t)
			reply := f.start(t)
			playlist := f.serve(f.request(http.MethodGet, reply.PlaylistURL, "", "", ""))
			if playlist.Code != 200 {
				t.Fatalf("seekable %s playlist HTTP%d", container, playlist.Code)
			}
			names := parentHLSAPISegments(t, playlist.Body.Bytes())
			for _, name := range names {
				path := strings.TrimSuffix(reply.PlaylistURL, "stream.m3u8") + name
				response := f.serve(f.request(http.MethodGet, path, "", "", ""))
				if response.Code != 200 || response.Body.Len() == 0 || response.Header().Get("Content-Type") != "video/mp2t" {
					t.Fatalf("seekable %s segment HTTP%d", container, response.Code)
				}
			}
			f.sameSource(t)
		})
	}
}

// The wrapper immediately execs the real installed FFmpeg with real-time input
// pacing. Its observed executable/PID must still be live after the first closed
// segment is returned, so revocation/expiry proves actual worker termination.
func TestParentFileHLSRegisteredRunningFFmpegJoinsOriginalRevokeAndExpiry(t *testing.T) {
	for _, change := range []string{"revoke", "original-expiry"} {
		t.Run(change, func(t *testing.T) {
			f := parentHLSAPIUnapprovedMedia(t, "mpegts", "30", false)
			var originalExpiry time.Time
			if change == "original-expiry" {
				originalExpiry = time.Now().UTC().Add(14 * time.Second).Truncate(time.Microsecond)
				f.exec(`UPDATE sessions SET expires_at=$1 WHERE token=$2`, originalExpiry, f.token)
			}
			f.approval, f.rawURL = f.approve(t)
			f.configure(t)
			scratch := t.TempDir()
			pidFile := filepath.Join(scratch, "running-ffmpeg.pid")
			script := filepath.Join(scratch, "real-ffmpeg")
			quoted := "'" + strings.ReplaceAll(pidFile, "'", "'\"'\"'") + "'"
			if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' \"$$\" > "+quoted+"\nexec /usr/bin/ffmpeg -re \"$@\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.options.FFmpegPath = script
			options := f.options
			f.mu.Unlock()
			if err := f.files.ConfigureHLS(t.Context(), options, func(ctx context.Context) (handlers.ParentFileHLSOptions, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				return f.options, ctx.Err()
			}); err != nil {
				t.Fatal(err)
			}
			reply := f.start(t)
			if change == "original-expiry" && !reply.ExpiresAt.Equal(originalExpiry) {
				t.Fatal("HLS replaced the actual preconfigured PG session expiry")
			}
			raw, err := os.ReadFile(pidFile)
			if err != nil {
				t.Fatal("real FFmpeg PID was not captured", err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err != nil || pid <= 1 || syscall.Kill(pid, 0) != nil {
				t.Fatal("real FFmpeg was not live after first closed segment")
			}
			executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
			expected, expectedErr := filepath.EvalSymlinks("/usr/bin/ffmpeg")
			if err != nil || expectedErr != nil || executable != expected {
				t.Fatal("observed process was not the installed real FFmpeg")
			}
			t.Logf("observed real FFmpeg PID=%d; cancellation=%s; original expiry=%s", pid, change, reply.ExpiresAt.Format(time.RFC3339Nano))
			if change == "revoke" {
				done := make(chan *localDeadlineRecorder, 1)
				go func() { done <- f.serve(f.request(http.MethodDelete, f.rawURL, "", "", "")) }()
				select {
				case response := <-done:
					if response.Code != 204 {
						t.Fatalf("original raw revoke HTTP%d", response.Code)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("original revoke did not join running real FFmpeg")
				}
			} else {
				// No authority mutation occurs after the original expiry was captured.
				// Keep observing the actual worker until just before its original deadline.
				for time.Now().Before(originalExpiry.Add(-500 * time.Millisecond)) {
					if syscall.Kill(pid, 0) != nil {
						t.Fatal("real FFmpeg exited before original approval expiry")
					}
					time.Sleep(20 * time.Millisecond)
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) && !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
				time.Sleep(10 * time.Millisecond)
			}
			if !errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
				t.Fatal("real FFmpeg PID survived original retirement")
			}
			response := f.serve(f.request(http.MethodGet, reply.PlaylistURL, "", "", ""))
			if response.Code == 200 || response.Header().Get("Content-Type") == "application/vnd.apple.mpegurl" || response.Header().Get("Content-Type") == "video/mp2t" {
				t.Fatal("retired original authority still served HLS media")
			}
			f.sameSource(t)
		})
	}
}
