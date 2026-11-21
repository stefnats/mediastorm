package handlers

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/services/kidsvod"
)

func parentHLSChange(info os.FileInfo) string {
	value := reflect.ValueOf(info.Sys())
	if value.Kind() == reflect.Pointer {
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return ""
	}
	for _, name := range []string{"Ctim", "Ctimespec"} {
		field := value.FieldByName(name)
		if field.IsValid() && field.CanInterface() {
			return fmt.Sprint(field.Interface())
		}
	}
	return ""
}

func sameParentHLSFile(a, b os.FileInfo) bool {
	return a != nil && b != nil && a.Mode().IsRegular() && b.Mode().IsRegular() && os.SameFile(a, b) && a.Mode() == b.Mode() && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime()) && parentHLSChange(a) != "" && parentHLSChange(a) == parentHLSChange(b)
}
func (s *parentFileHLSSession) readPlaylist(ctx context.Context) ([]byte, error) {
	if s.check(ctx) != nil || s.outputBounds() != nil {
		return nil, errParentFileHLS
	}
	file, err := parentHLSOpen(s.root, "stream.m3u8")
	if err != nil {
		return nil, errParentFileHLS
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil || !before.Mode().IsRegular() || before.Size() < 1 || before.Size() > 64<<10 {
		return nil, errParentFileHLS
	}
	raw, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	after, e := file.Stat()
	named, f := s.root.Lstat("stream.m3u8")
	if err != nil || e != nil || f != nil || len(raw) > 64<<10 || !sameParentHLSFile(before, after) || !sameParentHLSFile(after, named) || s.check(ctx) != nil {
		return nil, errParentFileHLS
	}
	if !validParentHLSPlaylist(raw) {
		return nil, errParentFileHLS
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		info, e := s.root.Lstat(line)
		if e != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > 64<<20 {
			return nil, errParentFileHLS
		}
		s.mu.Lock()
		original := s.segments[line]
		if original == nil {
			s.segments[line] = info
		}
		s.mu.Unlock()
		if original != nil && !sameParentHLSFile(original, info) {
			return nil, errParentFileHLS
		}
	}
	return raw, nil
}
func validParentHLSPlaylist(raw []byte) bool {
	if len(raw) == 0 || len(raw) > 64<<10 || !utf8.Valid(raw) || strings.Contains(string(raw), "\r") {
		return false
	}
	lines := strings.Split(string(raw), "\n")
	if len(lines) > 8192 || lines[0] != "#EXTM3U" {
		return false
	}
	pending := false
	count := 0
	duration := 0.0
	ended := false
	target, sequence := false, false
	for _, line := range lines[1:] {
		if line == "" {
			continue
		}
		if ended {
			return false
		}
		switch {
		case line == "#EXT-X-ENDLIST":
			if pending || count == 0 {
				return false
			}
			ended = true
		case strings.HasPrefix(line, "#EXTINF:"):
			if pending {
				return false
			}
			value := strings.TrimSuffix(strings.TrimPrefix(line, "#EXTINF:"), ",")
			n, e := strconv.ParseFloat(value, 64)
			if e != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n > 7200 {
				return false
			}
			duration += n
			if duration > 7200 {
				return false
			}
			pending = true
		case strings.HasPrefix(line, "#EXT-X-TARGETDURATION:"):
			if target {
				return false
			}
			n, e := strconv.Atoi(strings.TrimPrefix(line, "#EXT-X-TARGETDURATION:"))
			if e != nil || n < 1 || n > 7200 {
				return false
			}
			target = true
		case line == "#EXT-X-MEDIA-SEQUENCE:0":
			if sequence {
				return false
			}
			sequence = true
		case line == "#EXT-X-INDEPENDENT-SEGMENTS", line == "#EXT-X-PLAYLIST-TYPE:VOD", line == "#EXT-X-VERSION:3", line == "#EXT-X-VERSION:6":
			if pending {
				return false
			}
		default:
			n, ok := parentHLSSegmentIndex(line)
			if !ok || n != count || !pending {
				return false
			}
			count++
			if count > 2048 {
				return false
			}
			pending = false
		}
	}
	return count > 0 && !pending && target && sequence
}
func parentHLSLocator(profile, item, id string) string {
	return "/api/users/" + url.PathEscape(profile) + "/library/items/" + url.PathEscape(item) + "/approved-hls/" + id + "/stream.m3u8"
}
func parentHLSOpaqueID(id string) bool {
	if len(id) != 32 {
		return false
	}
	_, err := hex.DecodeString(id)
	return err == nil && strings.ToLower(id) == id
}
func (h *ParentLocalFileHandler) hlsError(w http.ResponseWriter, status int) {
	apierror.Write(w, status, "kids_local_hls_unavailable", "Parent-approved local HLS is unavailable.")
}
func (h *ParentLocalFileHandler) StartHLS(w http.ResponseWriter, r *http.Request) {
	if !h.available(w) {
		return
	}
	m := h.hlsManager()
	if m == nil || m.check(r.Context()) != nil {
		h.hlsError(w, 503)
		return
	}
	if r.Method != http.MethodPost {
		h.hlsError(w, 405)
		return
	}
	profile, item := mux.Vars(r)["userID"], mux.Vars(r)["itemID"]
	if h.current(r, profile) != nil {
		h.deny(w, r)
		return
	}
	if r.URL.RawQuery != "" || len(r.TransferEncoding) != 0 || r.ContentLength > 1024 {
		h.hlsError(w, 400)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1025))
	if err != nil || len(raw) > 1024 {
		h.hlsError(w, 400)
		return
	}
	var body struct {
		Approval string `json:"approvalId"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || !validParentHLSApprovalBody(raw) || decoder.Decode(new(any)) != io.EOF {
		h.hlsError(w, 400)
		return
	}
	setup, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	bound := r.WithContext(setup)
	lease, err := h.core.PrepareParentFileProcessRequest(bound, profile, item, body.Approval)
	if err != nil {
		if errors.Is(err, kidsvod.ErrParentFileProcessBusy) {
			h.hlsError(w, 429)
		} else {
			h.deny(w, r)
		}
		return
	}
	published := false
	defer func() {
		if !published {
			_ = lease.Close()
		}
	}()
	session, err := m.newSession(setup, profile, item, lease)
	if err != nil {
		h.hlsError(w, 409)
		return
	}
	defer func() {
		if !published {
			session.close()
		}
	}()
	defer h.flushParentOnReturn(w, r)
	video, audio, err := session.probe(setup)
	if err != nil || h.current(bound, profile) != nil || session.start(setup, video, audio) != nil {
		h.hlsError(w, 409)
		return
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		playlist, e := session.readPlaylist(setup)
		if e == nil && len(playlist) > 0 {
			break
		}
		select {
		case <-setup.Done():
			h.hlsError(w, 409)
			return
		case <-session.ctx.Done():
			h.hlsError(w, 409)
			return
		case <-ticker.C:
		}
	}
	if h.current(bound, profile) != nil || session.check(setup) != nil || lease.Handoff(bound) != nil {
		h.deny(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	err = h.writeParentSuccess(w, r, struct {
		ID      string    `json:"sessionId"`
		URL     string    `json:"playlistUrl"`
		Expires time.Time `json:"expiresAt"`
		Outcome string    `json:"outcome"`
	}{session.id, parentHLSLocator(profile, item, session.id), lease.ExpiresAt(), "parent-file-exception"})
	published = err == nil && setup.Err() == nil
}
func validParentHLSApprovalBody(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	opening, e := d.Token()
	if e != nil || opening != json.Delim('{') || !d.More() {
		return false
	}
	key, e := d.Token()
	var value string
	if e != nil || key != "approvalId" || d.Decode(&value) != nil || len(value) != 68 || !strings.HasPrefix(value, "lfa_") || d.More() {
		return false
	}
	digits, e := hex.DecodeString(value[4:])
	if e != nil || len(digits) != 32 || hex.EncodeToString(digits) != value[4:] {
		return false
	}
	close, e := d.Token()
	return e == nil && close == json.Delim('}') && d.Decode(new(any)) == io.EOF
}
func (h *ParentLocalFileHandler) lookupHLS(w http.ResponseWriter, r *http.Request) (*parentFileHLSSession, bool) {
	if !h.available(w) {
		return nil, false
	}
	m := h.hlsManager()
	if m == nil {
		h.hlsError(w, 503)
		return nil, false
	}
	profile, item, id := mux.Vars(r)["userID"], mux.Vars(r)["itemID"], mux.Vars(r)["hlsID"]
	if r.URL.RawQuery != "" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || !parentHLSOpaqueID(id) || h.current(r, profile) != nil {
		h.deny(w, r)
		return nil, false
	}
	m.mu.Lock()
	session := m.sessions[id]
	m.mu.Unlock()
	if session == nil || session.profile != profile || session.item != item || session.lease.CheckRequest(r, profile, item) != nil || session.check(r.Context()) != nil {
		h.deny(w, r)
		return nil, false
	}
	return session, true
}
func (h *ParentLocalFileHandler) StopHLS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		h.hlsError(w, 405)
		return
	}
	session, ok := h.lookupHLS(w, r)
	if !ok {
		return
	}
	session.close()
	w.WriteHeader(http.StatusNoContent)
}
func (h *ParentLocalFileHandler) ServeHLS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		h.hlsError(w, 405)
		return
	}
	session, ok := h.lookupHLS(w, r)
	if !ok {
		return
	}
	name := mux.Vars(r)["segment"]
	if name != "stream.m3u8" {
		if _, valid := parentHLSSegmentIndex(name); !valid {
			h.hlsError(w, 400)
			return
		}
	}
	if len(r.Header.Values("Range")) > 0 || r.Header.Get("If-Range") != "" {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	release, err := session.lease.ReserveOutput(r, session.profile, session.item)
	if err != nil {
		h.hlsError(w, 429)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	owner := &parentHLSOutputOwner{cancel: cancel, done: make(chan struct{})}
	session.mu.Lock()
	if session.ctx.Err() != nil || len(session.readers) >= 64 {
		session.mu.Unlock()
		cancel()
		release()
		h.hlsError(w, 429)
		return
	}
	session.readers[owner] = true
	session.mu.Unlock()
	var file *os.File
	defer func() {
		cancel()
		if file != nil {
			file.Close()
		}
		release()
		session.mu.Lock()
		delete(session.readers, owner)
		session.mu.Unlock()
		close(owner.done)
	}()
	controller := http.NewResponseController(w)
	deadline := session.lease.ExpiresAt()
	if at, ok := ctx.Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	if controller.SetWriteDeadline(deadline) != nil {
		h.hlsError(w, 503)
		return
	}
	interrupt := make(chan struct{})
	stopSession := parentHLSAfterFunc(session.ctx, cancel)
	defer stopSession()
	stop := context.AfterFunc(ctx, func() { defer close(interrupt); _ = controller.SetWriteDeadline(time.Now()) })
	defer func() {
		if !stop() {
			<-interrupt
		}
	}()
	defer h.flushParentOnReturn(w, r)
	check := func() bool {
		return ctx.Err() == nil && h.current(r.WithContext(ctx), session.profile) == nil && session.lease.CheckRequest(r.WithContext(ctx), session.profile, session.item) == nil && session.check(ctx) == nil
	}
	var playlist []byte
	if name == "stream.m3u8" {
		playlist, err = session.readPlaylist(ctx)
		if err != nil {
			session.cancel()
			h.deny(w, r)
			return
		}
	}
	if !check() {
		session.cancel()
		h.deny(w, r)
		return
	}
	file, err = parentHLSOpen(session.root, name)
	if err != nil {
		h.hlsError(w, 404)
		return
	}
	before, err := file.Stat()
	named, e := session.root.Lstat(name)
	limit := int64(64 << 20)
	content := "video/mp2t"
	if name == "stream.m3u8" {
		limit = 64 << 10
		content = "application/vnd.apple.mpegurl"
	}
	if err != nil || e != nil || !sameParentHLSFile(before, named) || before.Size() < 1 || before.Size() > limit || !check() {
		session.cancel()
		h.deny(w, r)
		return
	}
	if name != "stream.m3u8" {
		session.mu.Lock()
		registered := session.segments[name]
		session.mu.Unlock()
		if !sameParentHLSFile(registered, before) {
			session.cancel()
			h.deny(w, r)
			return
		}
	}
	if playlist != nil {
		actual, e := io.ReadAll(io.LimitReader(file, (64<<10)+1))
		if e != nil || !bytes.Equal(actual, playlist) {
			h.hlsError(w, 409)
			return
		}
		_, _ = file.Seek(0, io.SeekStart)
	}
	if !check() {
		session.cancel()
		h.deny(w, r)
		return
	}
	w.Header().Set("Content-Type", content)
	w.Header().Set("Content-Length", strconv.FormatInt(before.Size(), 10))
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	remaining := before.Size()
	for remaining > 0 {
		amount := int64(len(buffer))
		if remaining < amount {
			amount = remaining
		}
		n, readErr := file.Read(buffer[:amount])
		info, statErr := file.Stat()
		pathInfo, pathErr := session.root.Lstat(name)
		if n == 0 || readErr != nil && readErr != io.EOF || statErr != nil || pathErr != nil || !sameParentHLSFile(before, info) || !sameParentHLSFile(info, pathInfo) || !check() {
			clear(buffer)
			session.cancel()
			panic(http.ErrAbortHandler)
		}
		written, writeErr := w.Write(buffer[:n])
		clear(buffer)
		if writeErr != nil || written != n || !check() {
			session.cancel()
			panic(http.ErrAbortHandler)
		}
		remaining -= int64(n)
	}
}
