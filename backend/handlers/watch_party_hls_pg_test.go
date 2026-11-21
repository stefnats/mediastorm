//go:build linux

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
)

func partyHLSInstall(t *testing.T, f *partyHTTPFixture, p *shareHLSTestProvider) {
	t.Helper()
	m := NewHLSManager(t.TempDir(), "/usr/bin/ffmpeg", "/usr/bin/ffprobe", p)
	f.video.hlsManager = m
	t.Cleanup(func() { p.release(); m.Shutdown(); f.video.hlsManager = nil })
}
func partyHLSStart(t *testing.T, f *partyHTTPFixture, p *shareHLSTestProvider) (*HLSSession, context.Context) {
	t.Helper()
	partyHLSInstall(t, f, p)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "http://synthetic.test/api/video/hls/start?path="+url.QueryEscape(f.resource)+"&profileId="+url.QueryEscape(f.native.profile)+"&target=web&audioTrack=1", nil)
	var original context.Context
	f.serveVideo(t, w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { original = r.Context(); f.video.StartHLSSession(w, r) }))
	var result struct {
		ID string `json:"sessionId"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.ID == "" {
		t.Fatal("actual party HLS start", w.Code, w.Body.String())
	}
	m := f.video.hlsManager
	m.mu.RLock()
	s := m.sessions[result.ID]
	m.mu.RUnlock()
	if s == nil || s.shareOwned == nil || s.shareOwned.guard.owner.party == nil || s.shareOwned.guard.owner.share != nil {
		t.Fatal("HLS source did not retain the concrete party owner")
	}
	shareHLSTestPoll(t, "party FFmpeg child", func() bool { return shareHLSTestCommand(s) != nil && shareHLSTestCommand(s).Process != nil })
	if p.direct.Load() != 0 {
		t.Fatal("party source escaped retained provider pipe")
	}
	return s, original
}
func partyHLSRequest(t *testing.T, f *partyHTTPFixture, s *HLSSession, method, suffix string, next http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	r := mux.SetURLVars(httptest.NewRequest(method, "http://synthetic.test/api/video/hls/"+s.ID+suffix, nil), map[string]string{"sessionID": s.ID, "segment": "segment0.ts"})
	f.serveVideo(t, w, r, next)
	return w
}
func partyHLSSegment(t *testing.T, s *HLSSession) string {
	t.Helper()
	var segment string
	shareHLSTestPoll(t, "actual party segment", func() bool {
		s.mu.RLock()
		extension, output := s.SegmentExt, s.OutputDir
		s.mu.RUnlock()
		switch extension {
		case ".ts", ".m4s", ".mp4":
		default:
			return false
		}
		candidate := "segment0" + extension
		info, err := os.Stat(filepath.Join(output, candidate))
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
			return false
		}
		segment = candidate
		return true
	})
	return segment
}

func partyHLSBorrowRetirement(t *testing.T, f *partyHTTPFixture, s *HLSSession, p *shareHLSTestProvider) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	entered, canceled, allowReturn, oldDone, stopDone := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowReturn) }) }
	oldStarted, stopStarted := false, false
	w := httptest.NewRecorder()
	var stopped *httptest.ResponseRecorder
	t.Cleanup(func() {
		cancel()
		release()
		p.release()
		if oldStarted {
			shareHLSTestWait(t, oldDone, "original party borrow")
		}
		if stopStarted {
			shareHLSTestWait(t, stopDone, "noncounted party stop")
		}
	})
	r := mux.SetURLVars(httptest.NewRequest("GET", "http://synthetic.test/api/video/hls/"+s.ID+"/status", nil).WithContext(ctx), map[string]string{"sessionID": s.ID})
	oldStarted = true
	go func() {
		defer close(oldDone)
		f.serveVideo(t, w, r, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			out, checked, finish, ok := f.video.beginHLSVOD(w, r, s.ID, false)
			if !ok {
				return
			}
			defer finish()
			if !f.video.requireHLSSessionAccess(out, checked, s.ID) {
				return
			}
			close(entered)
			<-checked.Context().Done()
			close(canceled)
			<-allowReturn
			_ = http.NewResponseController(out).Flush()
			_, _ = out.Write([]byte("SYNTHETIC-RETIRED-PARTY-REF"))
		}))
	}()
	select {
	case <-entered:
	case <-oldDone:
		t.Fatal("actual old party borrow not attached", w.Code)
	case <-time.After(12 * time.Second):
		t.Fatal("old party borrow timeout")
	}
	cancel()
	shareHLSTestWait(t, canceled, "original continuation cancellation")
	if s.shareOwned.guard.ctx.Err() != nil {
		t.Fatal("caller cancellation retired the otherwise current original party owner")
	}
	stopStarted = true
	go func() {
		defer close(stopDone)
		stopped = partyHLSRequest(t, f, s, "POST", "/stop", f.video.StopHLSSession)
	}()
	shareHLSTestPending(t, stopDone, "stop with actual old response ref held")
	release()
	shareHLSTestWait(t, oldDone, "physical old response/callback join")
	shareHLSTestWait(t, stopDone, "noncounted stop after actual response end")
	if w.Code != 403 || bytes.Contains(w.Body.Bytes(), []byte("SYNTHETIC-RETIRED-PARTY-REF")) || stopped == nil || stopped.Code != 200 {
		t.Fatal("old borrow output or noncounted stop invalid", w.Code, stopped)
	}
}

func TestWatchPartyOriginalHLSLifetime(t *testing.T) {
	media := managedHLSMedia(t)
	for _, kind := range []string{"start-original-owner", "playlist-current-owner", "segment-held-body", "seek-same-original", "restart-same-original", "old-recipient-no-adoption", "stop-noncounted-ref", "borrow-cancel-wait-end", "source-revoke-held-ffmpeg", "runtime-close-actual-join", "original-expiry-no-renewal", "nested-input-no-job"} {
		t.Run(kind, func(t *testing.T) {
			p := &shareHLSTestProvider{data: media, holdMain: kind == "source-revoke-held-ffmpeg" || kind == "runtime-close-actual-join" || kind == "stop-noncounted-ref" || kind == "original-expiry-no-renewal"}
			f := newPartyHTTP(t, t.Context(), p, "", kind == "original-expiry-no-renewal")
			f.resource = "/synthetic-original-party.ts"
			if kind == "nested-input-no-job" {
				f.resource = "/synthetic-original-party.m3u8"
			}
			f.join(t)
			f.bind(t)
			f.grant(t)
			if kind == "nested-input-no-job" {
				partyHLSInstall(t, f, p)
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.WriteHeader(500) }))
				t.Cleanup(server.Close)
				file := filepath.Join(t.TempDir(), "secondary.ts")
				if e := os.WriteFile(file, []byte("SYNTHETIC-UNAUTHORIZED-SECONDARY"), 0600); e != nil {
					t.Fatal(e)
				}
				fd, e := syscall.InotifyInit1(syscall.IN_CLOEXEC | syscall.IN_NONBLOCK)
				if e != nil {
					t.Fatal(e)
				}
				defer syscall.Close(fd)
				if _, e = syscall.InotifyAddWatch(fd, file, syscall.IN_OPEN|syscall.IN_ACCESS); e != nil {
					t.Fatal(e)
				}
				for index, target := range []string{server.URL + "/secondary.ts", "file://" + file} {
					p.mu.Lock()
					p.data = []byte("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2,\n" + target + "\n#EXT-X-ENDLIST\n")
					p.mu.Unlock()
					w := httptest.NewRecorder()
					r := httptest.NewRequest("POST", "http://synthetic.test/api/video/hls/start?path="+url.QueryEscape(f.resource)+"&profileId="+f.native.profile+"&target=web", nil)
					f.serveVideo(t, w, r, http.HandlerFunc(f.video.StartHLSSession))
					f.video.hlsManager.mu.RLock()
					count := len(f.video.hlsManager.sessions)
					f.video.hlsManager.mu.RUnlock()
					bodies, _ := p.state()
					if w.Code == 200 || count != 0 || len(bodies) != index+1 || p.direct.Load() != 0 {
						t.Fatal("nested party input published a job or escaped confined probe", w.Code, count)
					}
					shareHLSTestWait(t, bodies[index].joined, "failed confined probe source")
				}
				var events [4096]byte
				n, e := syscall.Read(fd, events[:])
				if requests.Load() != 0 || n > 0 || e != syscall.EAGAIN {
					t.Fatal("nested party source opened a secondary network/file capability", requests.Load(), n, e)
				}
				return
			}
			s, original := partyHLSStart(t, f, p)
			o := s.shareOwned
			cmd := shareHLSTestCommand(s)
			expiry, ok := o.guard.ctx.Deadline()
			if !ok || !expiry.Equal(o.guard.owner.ExpiresAt()) || f.video.vodBoundary != nil {
				t.Fatal("party source fixed lifetime/global boundary changed")
			}
			bodies, contexts := p.state()
			if len(bodies) < 2 {
				t.Fatal("actual party probe/transcode bodies missing")
			}
			if p.holdMain {
				shareHLSTestWait(t, bodies[1].reading, "actual held FFmpeg party input")
			}
			switch kind {
			case "start-original-owner":
				if original == nil || original.Err() == nil || o.guard.ctx.Err() != nil || sharedHLSContext(contexts[1]) != o || !strings.Contains(strings.Join(cmd.Args, " "), "-i pipe:0") || !strings.Contains(strings.Join(cmd.Args, " "), "-protocol_whitelist pipe") {
					t.Fatal("original request lifetime or actual confined party child lost")
				}
			case "playlist-current-owner":
				partyHLSSegment(t, s)
				w := partyHLSRequest(t, f, s, "GET", "/index.m3u8", f.video.ServeHLSPlaylist)
				if w.Code != 200 || !strings.Contains(w.Body.String(), "#EXTM3U") {
					t.Fatal("current original playlist", w.Code, w.Body.String())
				}
				if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE watch_rooms SET status='ended' WHERE id=$1`, f.room.ID); e != nil {
					t.Fatal(e)
				}
				w = partyHLSRequest(t, f, s, "GET", "/index.m3u8", f.video.ServeHLSPlaylist)
				if w.Code == 200 || strings.Contains(w.Body.String(), "#EXTM3U") {
					t.Fatal("retired party returned cached playlist", w.Code)
				}
			case "old-recipient-no-adoption":
				previous := f.token
				oldCookie := f.cookie
				f.cookie = nil
				f.join(t)
				f.grant(t)
				if f.token == previous || f.cookie.Value == oldCookie.Value {
					t.Fatal("test did not create different genuine recipient")
				}
				w := partyHLSRequest(t, f, s, "GET", "/status", f.video.GetHLSSessionStatus)
				if w.Code == 200 || o.guard.ctx.Err() != nil {
					t.Fatal("different recipient adopted/retired old source", w.Code)
				}
				f.token = previous
				f.cookie = oldCookie
			case "borrow-cancel-wait-end":
				partyHLSBorrowRetirement(t, f, s, p)
			case "seek-same-original", "restart-same-original":
				var w *httptest.ResponseRecorder
				if kind == "seek-same-original" {
					w = partyHLSRequest(t, f, s, "POST", "/seek?time=0.25", f.video.SeekHLSSession)
				} else {
					w = partyHLSRequest(t, f, s, "GET", "/status", func(w http.ResponseWriter, r *http.Request) {
						out, checked, finish, ok := f.video.beginHLSVOD(w, r, s.ID, false)
						if !ok {
							return
						}
						defer finish()
						if !f.video.requireHLSSessionAccess(out, checked, s.ID) {
							return
						}
						f.video.hlsManager.restartCastTranscodingAt(s, castSegmentRestart{TranscodingOffset: 0.25}, checked)
					})
				}
				if w.Code != 200 {
					t.Fatal("actual original same-source mutation", w.Code, w.Body.String())
				}
				shareHLSTestPoll(t, "actual party replacement child", func() bool { next := shareHLSTestCommand(s); return next != nil && next != cmd })
				if cmd.ProcessState == nil {
					t.Fatal("old actual child not Waited before replacement")
				}
				_, after := p.state()
				for _, ctx := range after[len(contexts):] {
					deadline, ok := ctx.Deadline()
					if sharedHLSContext(ctx) != o || !ok || !deadline.Equal(expiry) {
						t.Fatal("seek/restart renewed or substituted original party source")
					}
				}
			case "source-revoke-held-ffmpeg", "runtime-close-actual-join", "stop-noncounted-ref", "original-expiry-no-renewal":
				finished := make(chan struct{})
				started := false
				var w *httptest.ResponseRecorder
				t.Cleanup(func() {
					p.release()
					if started {
						shareHLSTestWait(t, finished, "party source close/stop")
					}
				})
				if kind == "source-revoke-held-ffmpeg" {
					if _, e := f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.native.profile); e != nil {
						t.Fatal(e)
					}
				}
				if kind == "runtime-close-actual-join" || kind == "stop-noncounted-ref" {
					started = true
					go func() {
						defer close(finished)
						if kind == "runtime-close-actual-join" {
							f.native.authority.Close()
						} else {
							w = partyHLSRequest(t, f, s, "POST", "/stop", f.video.StopHLSSession)
						}
					}()
				}
				shareHLSTestWait(t, bodies[1].closeEntered, "actual party input cancellation")
				shareHLSTestPending(t, o.done, "party source before physical reader end")
				if started {
					shareHLSTestPending(t, finished, "stop/runtime before actual source join")
				}
				if _, e := os.Stat(s.OutputDir); e != nil {
					t.Fatal("party output deleted before actual reader end", e)
				}
				p.release()
				shareHLSTestRetired(t, s, cmd, p)
				if started {
					shareHLSTestWait(t, finished, "actual party close/stop")
				}
				if kind == "stop-noncounted-ref" && (w == nil || w.Code != 200) {
					t.Fatal("stop self-waited/failed", w)
				}
				if kind == "original-expiry-no-renewal" && time.Now().Before(expiry) {
					t.Fatal("original source expired before the actual fixed limit")
				}
			case "segment-held-body":
				segment := partyHLSSegment(t, s)
				// Keep the actual generated media segment prefix, then grow this
				// owned output to force a real response socket write to remain held.
				file, e := os.OpenFile(filepath.Join(s.OutputDir, segment), os.O_APPEND|os.O_WRONLY, 0600)
				if e != nil {
					t.Fatal(e)
				}
				_, e = file.Write(bytes.Repeat([]byte("SYNTHETIC-PARTY-SEGMENT"), 1<<19))
				closeErr := file.Close()
				if e != nil || closeErr != nil {
					t.Fatal(e, closeErr)
				}
				done := make(chan struct{})
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(done)
					r = mux.SetURLVars(r, map[string]string{"sessionID": s.ID, "segment": segment})
					f.serveVideo(t, w, r, http.HandlerFunc(f.video.ServeHLSSegment))
				}))
				listener := &shareEntryListener{Listener: server.Listener, accepted: make(chan *shareEntryConn, 1)}
				server.Listener = listener
				server.Start()
				c, e := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
				if e != nil {
					server.Close()
					t.Fatal(e)
				}
				t.Cleanup(func() { _ = c.Close(); server.Close() })
				if tcp, ok := c.(*net.TCPConn); ok {
					_ = tcp.SetReadBuffer(1024)
				}
				if _, e = io.WriteString(c, "GET /api/video/hls/"+s.ID+"/"+segment+" HTTP/1.1\r\nHost: synthetic.test\r\nConnection: close\r\n\r\n"); e != nil {
					t.Fatal(e)
				}
				var actual *shareEntryConn
				select {
				case actual = <-listener.accepted:
				case <-time.After(5 * time.Second):
					t.Fatal("no actual party segment socket")
				}
				shareHLSTestPoll(t, "actual held segment output", func() bool { return actual.writing.Load() > 0 })
				if _, e = f.native.base.base.pin.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=false WHERE id=$1`, f.native.profile); e != nil {
					t.Fatal(e)
				}
				shareHLSTestWait(t, done, "actual segment response end")
				shareHLSTestRetired(t, s, cmd, p)
				if actual.writing.Load() != 0 || !actual.writeDemanded.Load() {
					t.Fatal("actual segment body/callback was not interrupted/joined")
				}
			}
		})
	}
}
