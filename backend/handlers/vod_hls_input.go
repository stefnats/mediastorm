package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"novastream/services/kidsvod"
	"novastream/services/streaming"
)

// This dormant port supports one ordinary H264/AAC TS pipe attempt. Owner
// correlation never constitutes a catalogue, exact file or regional child grant.
func (h *VideoHandler) installHLSInputBoundary(b *vodBoundary) error {
	if h == nil || h.hlsManager == nil || b == nil {
		return errVODOwner
	}
	m := h.hlsManager
	m.vodInputMu.Lock()
	defer m.vodInputMu.Unlock()
	if m.vodInputsClosed {
		return errVODOwner
	}
	if m.vodInputHandler != nil && (m.vodInputHandler != h || h.vodBoundary != b) {
		return errVODOwner
	}
	if h.vodBoundary != nil && h.vodBoundary != b {
		return errVODOwner
	}
	if h.vodBoundary == nil {
		h.vodBoundary = b
	}
	m.vodInputHandler = h
	if m.vodInputs == nil {
		m.vodInputs = make(map[*vodHLSInput]bool)
	}
	return nil
}
func (m *HLSManager) hlsInputConfigured() bool {
	if m == nil {
		return false
	}
	m.vodInputMu.Lock()
	defer m.vodInputMu.Unlock()
	return m.vodInputHandler != nil
}
func (h *VideoHandler) configuredHLSInput() bool {
	return h != nil && (h.vodBoundary != nil || h.hlsManager != nil && h.hlsManager.hlsInputConfigured())
}

type vodHLSInput struct {
	opened, probeReady                bool
	source                            *streaming.Response
	prefix                            []byte
	inventory                         vodHLSInventory
	setupCtx                          context.Context
	workerStarted, probed             bool
	handler                           *VideoHandler
	manager                           *HLSManager
	owner                             *vodOwnerContext
	raw                               *vodRawOwner
	ctx                               context.Context
	cancel                            context.CancelFunc
	mu                                sync.Mutex
	phase                             int // 0 setup, 1 handed off, 2 retired
	setupStop                         func() bool
	session                           *HLSSession
	snapshot                          hlsSessionSnapshot
	published, started                bool
	done                              chan struct{}
	doneOnce, retireOnce, releaseOnce sync.Once
}

func (c *vodHLSInput) String() string               { return "private HLS input attempt" }
func (c *vodHLSInput) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (c *vodHLSInput) UnmarshalJSON([]byte) error   { *c = vodHLSInput{}; return errVODOwner }

func (h *VideoHandler) beginVODHLSInput(w http.ResponseWriter, r *http.Request, source string) (*vodHLSInput, bool) {
	if h.vodBoundary == nil || h.hlsManager == nil || h.installHLSInputBoundary(h.vodBoundary) != nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return nil, false
	}
	var selectors []string
	for k, v := range r.URL.Query() {
		if strings.EqualFold(k, "profileId") || strings.EqualFold(k, "userId") {
			selectors = append(selectors, v...)
		}
	}
	owner, ok := h.vodBoundary.requestOwner(w, r, vodMedia, "", selectors...)
	if !ok {
		return nil, false
	}
	// Delegated tokens must not be converted into a full owner by this issuer.
	if owner.session.Scope != "" || owner.profile.ID == "" || !owner.notApplicable() || captureHLSAuthValues(r, owner) != nil {
		owner.release()
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return nil, false
	}
	for k, v := range r.URL.Query() {
		if strings.EqualFold(k, "clientId") {
			for _, client := range v {
				if !h.vodBoundary.selectClient(w, owner, client) {
					owner.release()
					return nil, false
				}
			}
		}
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return nil, false
	}
	raw := &vodRawOwner{handler: h, owner: owner, source: source, key: "hls-input:" + hex.EncodeToString(id[:])}
	// This context is the actual context later supplied to the body and process.
	// It retains the original absolute authority expiry, independently of the
	// HTTP setup cancellation hook. No requestOwner is issued at handoff.
	ctx, cancel := vodWorkerContext(owner.request.Context(), owner)
	ctx = context.WithValue(ctx, vodRawOwnerKey{}, raw)
	c := &vodHLSInput{setupCtx: r.Context(), handler: h, manager: h.hlsManager, owner: owner, raw: raw, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	m := c.manager
	m.vodInputMu.Lock()
	if m.vodInputsClosed || len(m.vodInputs) >= 64 {
		m.vodInputMu.Unlock()
		cancel()
		owner.release()
		vodDeny(w, kidsvod.CodeContextChanged)
		return nil, false
	}
	m.vodInputs[c] = true
	m.vodInputMu.Unlock()
	b := h.vodBoundary
	b.mu.Lock()
	if len(b.active) >= 512 {
		b.mu.Unlock()
		c.finishSetup()
		vodDeny(w, kidsvod.CodeContextChanged)
		return nil, false
	}
	b.active[raw.key] = owner
	b.mu.Unlock()
	stop := context.AfterFunc(r.Context(), func() {
		c.mu.Lock()
		retire := c.phase == 0
		if retire {
			c.phase = 2
		}
		c.mu.Unlock()
		if retire {
			c.cancel()
		}
	})
	c.mu.Lock()
	c.setupStop = stop
	c.mu.Unlock()
	context.AfterFunc(c.ctx, func() { c.retire() })
	if c.check(r.Context()) != nil {
		c.finishSetup()
		vodDeny(w, kidsvod.CodeContextChanged)
		return nil, false
	}
	return c, true
}
func (c *vodHLSInput) check(ctx context.Context) error {
	if c == nil || c.handler == nil || c.manager == nil || c.owner == nil || c.raw == nil || c.raw.owner != c.owner || c.raw.handler != c.handler || c.ctx == nil || ctx == nil || c.ctx.Err() != nil || ctx.Err() != nil {
		return errVODOwner
	}
	c.manager.vodInputMu.Lock()
	registered := c.manager.vodInputs[c] && c.manager.vodInputHandler == c.handler
	c.manager.vodInputMu.Unlock()
	if !registered {
		return errVODOwner
	}
	c.mu.Lock()
	retired := c.phase == 2 || c.phase == 0 && (c.setupCtx == nil || c.setupCtx.Err() != nil)
	published := c.published
	snapshot := c.snapshot
	c.mu.Unlock()
	if retired || c.handler.vodRawCheck(c.ctx) != nil {
		return errVODOwner
	}
	// Operation-local child cancellation must not retire the original owner.
	// Recheck it after the bounded stable-authority/source callback returns.
	c.mu.Lock()
	retired = c.phase == 2 || c.phase == 0 && (c.setupCtx == nil || c.setupCtx.Err() != nil)
	c.mu.Unlock()
	if retired || ctx.Err() != nil || c.ctx.Err() != nil {
		return errVODOwner
	}
	if published && !snapshot.current() {
		return errVODOwner
	}
	return nil
}
func ctxWithHLSRaw(ctx context.Context, raw *vodRawOwner) context.Context {
	return context.WithValue(ctx, vodRawOwnerKey{}, raw)
}
func (c *vodHLSInput) bindPublication(s *HLSSession) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != 0 || c.ctx.Err() != nil {
		return false
	}
	c.session = s
	c.snapshot = snapshotHLSLocked(c.manager, s)
	c.published = true
	return true
}
func (c *vodHLSInput) handoff(setup context.Context) error {
	if c.check(setup) != nil {
		c.retire()
		return errVODOwner
	}
	c.mu.Lock()
	if c.phase != 0 || !c.workerStarted || !c.published || c.ctx.Err() != nil {
		c.mu.Unlock()
		c.retire()
		return errVODOwner
	}
	// A false stop means the cancellation callback already started: it wins.
	if c.setupStop == nil || !c.setupStop() || c.setupCtx == nil || c.setupCtx.Err() != nil || setup.Err() != nil {
		c.phase = 2
		c.mu.Unlock()
		c.retire()
		return errVODOwner
	}
	c.phase = 1
	c.setupStop = nil
	c.mu.Unlock()
	return nil
}
func (c *vodHLSInput) finishSetup() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.phase == 1 {
		c.mu.Unlock()
		return
	}
	c.phase = 2
	started := c.workerStarted
	source := c.source
	c.mu.Unlock()
	c.retire()
	if !started {
		if source != nil {
			source.Close()
		}
		c.doneOnce.Do(func() { close(c.done) })
		c.releaseRetired()
	}
}
func (c *vodHLSInput) retire() {
	if c == nil {
		return
	}
	c.retireOnce.Do(func() {
		c.mu.Lock()
		c.phase = 2
		stop := c.setupStop
		c.setupStop = nil
		c.mu.Unlock()
		if stop != nil {
			stop()
		}
		c.cancel()
		c.owner.release()
		go func() { <-c.done; c.releaseRetired() }()
	})
}
func (c *vodHLSInput) releaseRetired() {
	c.releaseOnce.Do(func() {
		b := c.owner.issuer
		b.mu.Lock()
		if b.active[c.raw.key] == c.owner {
			delete(b.active, c.raw.key)
		}
		b.mu.Unlock()
		c.manager.vodInputMu.Lock()
		delete(c.manager.vodInputs, c)
		c.manager.vodInputMu.Unlock()
		c.mu.Lock()
		snapshot := c.snapshot
		published := c.published
		c.mu.Unlock()
		if published && !c.manager.cleanupHLSSessionSnapshot(snapshot) {
			claim := snapshot.directory
			if claim != nil && claim.manager == c.manager && claim.published == snapshot.session && claim.path == snapshot.output {
				c.manager.removeDetachedHLSInputOutput(snapshot)
			}
		}
	})
}

// A failed generation comparison never authorizes deleting files still owned
// by any current runtime, even if its original input has already retired.
func (m *HLSManager) removeDetachedHLSInputOutput(snapshot hlsSessionSnapshot) {
	m.sessionLifecycleMu.Lock()
	defer m.sessionLifecycleMu.Unlock()
	m.mu.RLock()
	owned := false
	for _, session := range m.sessions {
		if session == nil {
			continue
		}
		session.mu.RLock()
		same := session.vodDirectory == snapshot.directory || session.OutputDir == snapshot.output
		session.mu.RUnlock()
		if same {
			owned = true
			break
		}
	}
	m.mu.RUnlock()
	if !owned {
		_ = os.RemoveAll(snapshot.output)
	}
}
func (m *HLSManager) shutdownHLSInputs() {
	m.vodInputMu.Lock()
	m.vodInputsClosed = true
	inputs := make([]*vodHLSInput, 0, len(m.vodInputs))
	for c := range m.vodInputs {
		inputs = append(inputs, c)
	}
	m.vodInputMu.Unlock()
	for _, c := range inputs {
		c.retire()
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for _, c := range inputs {
		select {
		case <-c.done:
		case <-timer.C:
			return
		}
	}
}
func (c *vodHLSInput) stopAndWait() bool {
	c.mu.Lock()
	c.phase = 2
	unstarted := !c.workerStarted
	c.mu.Unlock()
	if unstarted {
		c.doneOnce.Do(func() { close(c.done) })
	}
	c.retire()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-c.done:
		return true
	case <-timer.C:
		return false
	}
}

// Only the actual returned body is wrapped. This is not an input/file proof.
type vodHLSBody struct {
	input *vodHLSInput
	ctx   context.Context
	body  io.ReadCloser
	once  sync.Once
}

func (b *vodHLSBody) Read(p []byte) (int, error) {
	if b.input.check(b.ctx) != nil {
		b.Close()
		return 0, errVODOwner
	}
	n, err := b.body.Read(p)
	if b.input.check(b.ctx) != nil {
		clear(p[:n])
		b.Close()
		return 0, errVODOwner
	}
	return n, err
}
func (b *vodHLSBody) Close() error {
	var err error
	b.once.Do(func() { err = b.body.Close() })
	return err
}
func (c *vodHLSInput) open(ctx context.Context, method, rangeHeader string) (*streaming.Response, error) {
	if c == nil {
		return nil, errVODOwner
	}
	c.mu.Lock()
	if c.phase != 0 || c.opened || method != http.MethodGet || rangeHeader != "" {
		c.mu.Unlock()
		return nil, errVODOwner
	}
	c.opened = true
	c.mu.Unlock()

	if c.check(ctx) != nil {
		return nil, errVODOwner
	}
	var resp *streaming.Response
	var err error
	if strings.HasPrefix(c.raw.source, "http://") || strings.HasPrefix(c.raw.source, "https://") {
		req, makeErr := http.NewRequestWithContext(ctx, method, c.raw.source, nil)
		if makeErr != nil {
			return nil, makeErr
		}
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		result, e := c.handler.vodHTTPDo(hlsRedirectHTTPClient, req)
		err = e
		if result != nil {
			resp = &streaming.Response{Body: result.Body, Headers: result.Header, Status: result.StatusCode, ContentLength: result.ContentLength}
		}
	} else if c.manager.streamer == nil {
		return nil, errors.New("stream provider unavailable")
	} else {
		resp, err = c.manager.streamer.Stream(ctx, streaming.Request{Path: c.raw.source, Method: method, RangeHeader: rangeHeader})
	}
	if resp != nil {
		captured := *resp
		captured.Headers = resp.Headers.Clone()
		resp = &captured
	}
	if err != nil || c.check(ctx) != nil {
		if resp != nil {
			resp.Close()
		}
		if err != nil {
			return nil, err
		}
		return nil, errVODOwner
	}
	if resp == nil {
		return nil, errors.New("stream input unavailable")
	}
	if resp.Body == nil || resp.Status != http.StatusOK && resp.Status != http.StatusPartialContent {
		resp.Close()
		return nil, errors.New("stream input unavailable")
	}
	owned, wrapErr := c.handler.wrapVODRawBody(ctxWithHLSRaw(ctx, c.raw), resp.Body)
	if wrapErr != nil {
		return nil, wrapErr
	}
	wrapped := &vodHLSBody{input: c, ctx: ctx, body: owned}
	resp.Body = wrapped
	return resp, nil
}

// These input-only options prohibit nested URL/file references. Outputs remain
// ordinary owned files. Real process canaries verify the installed binaries.
var vodHLSPipeOptions = []string{"-protocol_whitelist", "pipe", "-format_whitelist", "matroska,webm,mov,mpegts,avi,wav"}

type vodHLSInventory struct {
	Streams []struct {
		Index       int            `json:"index"`
		Type        string         `json:"codec_type"`
		Codec       string         `json:"codec_name"`
		PixFmt      string         `json:"pix_fmt"`
		Color       string         `json:"color_transfer"`
		Width       int            `json:"width"`
		Height      int            `json:"height"`
		Disposition map[string]int `json:"disposition"`
	} `json:"streams"`
}

// A bounded prefix comes from the SAME retained response later replayed into
// FFmpeg. It is neither an immutable-file receipt nor regional eligibility.
func (c *vodHLSInput) probe(ctx context.Context) (vodHLSInventory, error) {
	var result vodHLSInventory
	c.mu.Lock()
	if c.phase != 0 || c.probed || c.started {
		c.mu.Unlock()
		return result, errVODOwner
	}
	c.probed = true
	c.mu.Unlock()
	if c.manager.ffprobePath == "" {
		return result, errors.New("ffprobe unavailable")
	}
	setup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	stop := context.AfterFunc(setup, func() { c.retire() })
	defer stop()
	// The body belongs to the original runtime context, never the shorter
	// setup/probe timeout. The removable setup hook retires it on failure only.
	resp, err := c.open(c.ctx, http.MethodGet, "")
	if err != nil {
		return result, err
	}
	c.mu.Lock()
	c.source = resp
	c.mu.Unlock()
	prefix, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil || len(prefix) == 0 || c.check(setup) != nil {
		resp.Close()
		return result, errVODOwner
	}
	args := append([]string{"-v", "error", "-probesize", "1000000", "-analyzeduration", "500000"}, vodHLSPipeOptions...)
	args = append(args, "-print_format", "json", "-show_streams", "-i", "pipe:0")
	output, err := c.runPipeCommand(setup, c.manager.ffprobePath, args, io.NopCloser(bytes.NewReader(prefix)), nil)
	if err != nil || c.check(setup) != nil {
		resp.Close()
		return result, errors.New("confined media probe unavailable")
	}
	if json.Unmarshal(output, &result) != nil || len(result.Streams) > 128 {
		resp.Close()
		return vodHLSInventory{}, errors.New("invalid media inventory")
	}
	// Callback cancellation and successful release have one linearization point.
	if !stop() || setup.Err() != nil {
		resp.Close()
		c.retire()
		return result, errVODOwner
	}
	c.mu.Lock()
	if c.phase != 0 || c.ctx.Err() != nil {
		c.mu.Unlock()
		resp.Close()
		return result, errVODOwner
	}
	c.prefix = prefix
	c.inventory = result
	c.probeReady = true
	c.mu.Unlock()
	return result, nil
}

type vodHLSCapture struct{ bytes.Buffer }

func (b *vodHLSCapture) Write(p []byte) (int, error) {
	if len(p) > 1<<20-b.Len() {
		return 0, errors.New("media probe output limit")
	}
	return b.Buffer.Write(p)
}

// Child stdin is an owned OS pipe, not os/exec's hidden source-copy goroutine.
// Child exit closes the captured reader before joining our copier. No source
// reader or process can be silently detached by Cmd.Wait/WaitDelay.
func (c *vodHLSInput) runPipeCommand(ctx context.Context, binary string, args []string, source io.ReadCloser, started func(*exec.Cmd)) ([]byte, error) {
	command, cancel := context.WithCancel(ctx)
	defer cancel()
	body := &vodHLSBody{input: c, ctx: command, body: source}
	stop := context.AfterFunc(command, func() { body.Close() })
	defer stop()
	defer body.Close()
	cmd := exec.CommandContext(command, binary, args...)
	writer, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	var output vodHLSCapture
	if started == nil {
		cmd.Stdout = &output
	}
	if c.check(command) != nil {
		return nil, errVODOwner
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	if started != nil {
		started(cmd)
	}
	type pumpResult struct {
		err         error
		childClosed bool
	}
	var childExited atomic.Bool
	pump := make(chan pumpResult, 1)
	go func() {
		_, err := io.Copy(writer, body)
		_ = writer.Close()
		pump <- pumpResult{err, childExited.Load()}
	}()
	if c.check(command) != nil {
		cancel()
	}
	err = cmd.Wait()
	childExited.Store(true)
	cancel()
	_ = body.Close()
	_ = writer.Close()
	copyResult := <-pump
	copyErr := copyResult.err
	if err == nil {
		if c.check(ctx) != nil {
			return output.Bytes(), errVODOwner
		}
		benignProbeClose := started == nil && (errors.Is(copyErr, syscall.EPIPE) || copyResult.childClosed && (errors.Is(copyErr, os.ErrClosed) || errors.Is(copyErr, errVODOwner)))
		if copyErr != nil && !benignProbeClose {
			err = copyErr
		}
	}
	return output.Bytes(), err
}

func chooseVODHLSTracks(inventory vodHLSInventory, audio int) (int, int, error) {
	videoIndex, audioIndex := -1, -1
	defaultAudio := -1
	audioCount := 0
	seen := map[int]bool{}
	for _, s := range inventory.Streams {
		if s.Index < 0 || seen[s.Index] {
			return -1, -1, errVODOwner
		}
		seen[s.Index] = true
		if s.Type == "video" {
			if videoIndex != -1 || s.Codec != "h264" || s.Disposition["attached_pic"] != 0 || (s.PixFmt != "yuv420p" && s.PixFmt != "yuvj420p") || s.Color == "smpte2084" || s.Color == "arib-std-b67" {
				return -1, -1, errVODOwner
			}
			videoIndex = s.Index
		}
		if s.Type == "audio" {
			audioCount++
			if audio >= 0 && s.Index == audio {
				if s.Codec != "aac" {
					return -1, -1, errVODOwner
				}
				audioIndex = s.Index
			}
			if audio < 0 && s.Disposition["default"] == 1 {
				if defaultAudio != -1 || s.Codec != "aac" {
					return -1, -1, errVODOwner
				}
				defaultAudio = s.Index
			}
		}
	}
	if audio < 0 {
		if defaultAudio >= 0 {
			audioIndex = defaultAudio
		} else if audioCount == 1 {
			for _, s := range inventory.Streams {
				if s.Type == "audio" && s.Codec == "aac" {
					audioIndex = s.Index
				}
			}
		}
	}
	if videoIndex < 0 || audioIndex < 0 {
		return -1, -1, errVODOwner
	}
	return videoIndex, audioIndex, nil
}

type vodHLSReplay struct {
	io.Reader
	source *streaming.Response
}

func (r *vodHLSReplay) Close() error { return r.source.Close() }
func (c *vodHLSInput) startPipe(video, audio int) error {
	if c.manager.ffmpegPath == "" {
		return errors.New("ffmpeg unavailable")
	}
	c.mu.Lock()
	if c.phase != 0 || c.started || !c.probeReady || c.source == nil || c.ctx.Err() != nil {
		c.mu.Unlock()
		return errVODOwner
	}
	actualVideo, actualAudio, err := chooseVODHLSTracks(c.inventory, audio)
	if err != nil || actualVideo != video || actualAudio != audio {
		c.mu.Unlock()
		return errVODOwner
	}
	c.started = true
	c.workerStarted = true
	source := c.source
	replay := &vodHLSReplay{Reader: io.MultiReader(bytes.NewReader(c.prefix), source.Body), source: source}
	c.prefix = nil
	c.mu.Unlock()
	go func() {
		defer c.doneOnce.Do(func() { close(c.done) })
		defer replay.Close()
		s := c.session
		args := append([]string{"-nostdin", "-hide_banner", "-loglevel", "error"}, vodHLSPipeOptions...)
		args = append(args, "-i", "pipe:0", "-map", fmt.Sprintf("0:%d", video), "-map", fmt.Sprintf("0:%d", audio), "-sn", "-dn", "-c:v", "copy", "-c:a", "copy", "-f", "hls", "-hls_time", "4", "-hls_list_size", "0", "-hls_flags", "independent_segments+temp_file", "-hls_segment_filename", filepath.Join(s.OutputDir, "segment%d.ts"), filepath.Join(s.OutputDir, "stream.m3u8"))
		_, runErr := c.runPipeCommand(c.ctx, c.manager.ffmpegPath, args, replay, func(cmd *exec.Cmd) { s.mu.Lock(); s.FFmpegCmd = cmd; s.FFmpegPID = cmd.Process.Pid; s.mu.Unlock() })
		s.mu.Lock()
		s.Completed = true
		if runErr != nil && !s.Stopped {
			s.FatalError = "Playback input unavailable"
			s.FatalErrorTime = time.Now()
		}
		s.FFmpegPID = 0
		s.mu.Unlock()
		if runErr != nil {
			c.retire()
		}
	}()
	return nil
}
func singleVODHLSInt(q url.Values, name string, defaultValue int) (int, error) {
	values, ok := q[name]
	if !ok {
		return defaultValue, nil
	}
	if len(values) != 1 {
		return 0, errVODOwner
	}
	n, err := strconv.Atoi(values[0])
	if err != nil || n < 0 || n > 10000 {
		return 0, errVODOwner
	}
	return n, nil
}
func (h *VideoHandler) startVODPipeHLS(w http.ResponseWriter, r *http.Request, source string) {
	c, ok := h.beginVODHLSInput(w, r, source)
	if !ok {
		return
	}
	defer c.finishSetup()
	q := r.URL.Query()
	// This first managed subset has no seek, DV/HDR, forced conversion or separate
	// subtitle input. Unsupported intent cannot silently select a different track.
	for _, name := range []string{"start", "startOffset", "startPercent", "subtitleTrack"} {
		if values, exists := q[name]; exists && (len(values) != 1 || values[0] != "0" && !(name == "subtitleTrack" && values[0] == "-1")) {
			vodDeny(w, kidsvod.CodeUnsupportedSource)
			return
		}
	}
	if q.Get("subtitleTrack") == "0" || q.Get("dv") == "true" || q.Get("hdr") == "true" || q.Get("forceAAC") == "true" {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	audio, err := singleVODHLSInt(q, "audioTrack", -1)
	if err != nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	if c.check(r.Context()) != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	if !h.requireAllowedExternalPath(w, r, source) {
		return
	}
	if c.check(r.Context()) != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	probe, err := c.probe(c.ctx)
	if err != nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	video, audio, err := chooseVODHLSTracks(probe, audio)
	if err != nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	if c.check(r.Context()) != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	claim, err := c.manager.claimHLSSessionDirectory(generateSessionID())
	if err != nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	defer c.manager.discardHLSSessionDirectory(claim)
	now := time.Now()
	s := &HLSSession{ID: claim.id, ProfileID: c.owner.profile.ID, ClientID: c.owner.client, Path: source, OriginalPath: source, RequestedSource: source, OutputDir: claim.path, vodDirectory: claim, vodInput: c, CreatedAt: now, StreamStartTime: now, LastAccess: now, LastSegmentRequest: now, AudioTrackIndex: audio, SubtitleTrackIndex: -1, MinSegmentRequested: -1, MaxSegmentRequested: -1, LastPlaybackSegment: -1, LastSegmentServed: -1, FinalSegmentCount: -1}
	c.mu.Lock()
	c.session = s
	c.mu.Unlock()
	s.Cancel = c.retire
	if c.check(r.Context()) != nil || !c.manager.publishHLSSession(s) {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	if err = c.startPipe(video, audio); err != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	writer := &vodHLSWriter{ResponseWriter: w, scope: &vodHLSScope{handler: h, owner: c.owner, snapshot: c.snapshot}, ctx: c.ctx, baseline: w.Header().Clone()}
	writer.Header().Set("Content-Type", "application/json")
	playlist, err := hlsOwnedInitialURL(s.ID, c.owner.profile.ID)
	if err != nil {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return
	}
	if c.check(r.Context()) != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return
	}
	if json.NewEncoder(writer).Encode(map[string]any{"sessionId": s.ID, "playlistUrl": playlist, "startOffset": 0, "actualStartOffset": 0, "keyframeDelta": 0, "stableCastTimeline": false}) != nil {
		return
	}
	if c.handoff(r.Context()) != nil {
		return
	}
}
