package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image/png"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/requestsecurity"
	"novastream/internal/streamheaders"
	"novastream/services/credits"
	"novastream/services/streaming"
	"novastream/utils"
)

const shareRawFormats = "matroska,webm,mov,mpegts,mpeg,mpegvideo,avi,flv,ogg,wav,aac,ac3,eac3,mp3,flac"

var errShareAncillaryBound = errors.New("shared ancillary resource bound exceeded")

// These counters account for actual retained producers/files, never permission.
var shareAncillaryBudget = struct {
	sync.Mutex
	retained, running int
	bytes             int64
	accounts, active  map[string]int
}{accounts: make(map[string]int), active: make(map[string]int)}

type shareAncillaryRegistry struct {
	mu     sync.Mutex
	closed bool
	jobs   map[string]*shareAncillaryJob
}
type shareAncillaryJob struct {
	handler                                          *VideoHandler
	registry                                         *shareAncillaryRegistry
	guard                                            *shareMediaGuard
	id, kind, account, token, profile, resource, dir string
	ctx                                              context.Context
	cancel                                           context.CancelFunc
	mu                                               sync.Mutex
	closed                                           bool
	err                                              error
	bytes                                            int64
	thumbnails                                       []thumbnailDetails
	duration                                         float64
	interval, total                                  int
	detected                                         bool
	creditsStart                                     float64
	refs                                             sync.WaitGroup
	producerDone, done                               chan struct{}
	once                                             sync.Once
	commands                                         []*exec.Cmd
	created                                          time.Time
	watchDone                                        chan struct{}
}

func (h *VideoHandler) ancillaryRegistry() *shareAncillaryRegistry {
	h.shareAncillaryMu.Lock()
	defer h.shareAncillaryMu.Unlock()
	if h.shareAncillary == nil {
		h.shareAncillary = &shareAncillaryRegistry{jobs: make(map[string]*shareAncillaryJob)}
	}
	return h.shareAncillary
}
func (h *VideoHandler) shutdownShareAncillary() {
	registry := h.ancillaryRegistry()
	registry.mu.Lock()
	registry.closed = true
	all := make([]*shareAncillaryJob, 0, len(registry.jobs))
	for _, j := range registry.jobs {
		all = append(all, j)
		j.cancel()
	}
	registry.mu.Unlock()
	for _, j := range all {
		j.close()
	}
}
func ancillaryJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func ancillaryError(w http.ResponseWriter, err error) {
	shareMediaError(w, errors.Join(datastore.ErrSharePlaybackUnavailable, err))
}
func ancillaryFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func shareAncillarySource(r *http.Request, g *shareMediaGuard) error {
	if r == nil || g == nil {
		return datastore.ErrSharePlaybackChanged
	}
	source := normalizeShareResource(resolveScopedPlaybackPath(r, r.URL.Query().Get("path")))
	if source == "" || source != g.owner.Resource() {
		return datastore.ErrSharePlaybackChanged
	}
	return g.owner.CheckRequest(r, g.owner.ProfileID(), source)
}
func (h *VideoHandler) reserveShareSetup(r *http.Request) (func(), error) {
	g := shareMediaFrom(r.Context())
	if err := shareAncillarySource(r, g); err != nil {
		return nil, err
	}
	session, ok := auth.GetSession(r)
	if !ok {
		return nil, datastore.ErrSharePlaybackChanged
	}
	registry := h.ancillaryRegistry()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, datastore.ErrSharePlaybackUnavailable
	}
	return reserveShareAncillary(session.AccountID)
}

func (h *VideoHandler) ancillaryMetadata(ctx context.Context, resource string) (*ffprobeOutput, error) {
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// The existing shared probe retains/closes/joins its original body before
	// decoding only bounded raw container bytes through the pipe-only demuxer.
	return h.runShareMetadataProbe(bounded, resource)
}
func (h *VideoHandler) runShareSampleProbe(ctx context.Context, sample []byte) (*ffprobeOutput, error) {
	if h.ffprobePath == "" || len(sample) == 0 || len(sample) > 16<<20 {
		return nil, datastore.ErrSharePlaybackUnavailable
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var stdout bytes.Buffer
	stderr := &shareStderr{}
	cmd := exec.CommandContext(bounded, h.ffprobePath, "-v", "error", "-protocol_whitelist", "pipe", "-format_whitelist", shareRawFormats, "-print_format", "json", "-show_streams", "-show_chapters", "-show_format", "-i", "pipe:0")
	cmd.Stdin = bytes.NewReader(sample)
	cmd.Stdout = &shareCurrentWriter{ctx: bounded, guard: shareMediaFrom(ctx), destination: &shareLimitedWriter{destination: &stdout, remaining: 1 << 20}, cancel: cancel}
	cmd.Stderr = stderr
	if err := shareMediaFrom(ctx).current(bounded); err != nil {
		return nil, err
	}
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	if err := shareMediaFrom(ctx).current(bounded); err != nil {
		return nil, err
	}
	var out ffprobeOutput
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func shareAncillaryDuration(meta *ffprobeOutput) (float64, error) {
	if meta == nil {
		return 0, datastore.ErrSharePlaybackUnavailable
	}
	d, err := strconv.ParseFloat(meta.Format.Duration, 64)
	if err != nil || !ancillaryFinite(d) || d <= 0 || d > float64(1<<31-1) {
		return 0, datastore.ErrSharePlaybackUnavailable
	}
	return d, nil
}

// shareSourceCommand owns the actual child, original body, pipe and copier.
// Wait completing alone cannot refund its input: Close and the copier join are
// unconditional, including children which stop reading their input early.
type shareSourceCommand struct {
	cmd                   *exec.Cmd
	body                  io.ReadCloser
	inputRead, inputWrite *os.File
	copyDone              chan struct{}
	copyErr               error
	readErr               error
	stderr                *shareStderr
}
type shareStderr struct {
	mu   sync.Mutex
	data []byte
}

func (b *shareStderr) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if len(p) >= 65536 {
		b.data = append(b.data[:0], p[len(p)-65536:]...)
	} else {
		if over := len(b.data) + len(p) - 65536; over > 0 {
			copy(b.data, b.data[over:])
			b.data = b.data[:len(b.data)-over]
		}
		b.data = append(b.data, p...)
	}
	return n, nil
}
func (b *shareStderr) String() string { b.mu.Lock(); defer b.mu.Unlock(); return string(b.data) }

// Original composite identity is checked before reducing it to HTTP wire DATA.
// Existing fragment/header normalization never refreshes source authority.
func (h *VideoHandler) shareHTTPInputRequest(ctx context.Context, original, method, rangeHeader string) (*http.Request, error) {
	g := shareMediaFrom(ctx)
	if g == nil || g.handler != h || normalizeShareResource(original) != g.owner.Resource() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	if err := g.current(ctx); err != nil {
		return nil, err
	}
	raw, headers := streamheaders.Extract(original)
	encoded, err := utils.EncodeURLWithSpaces(raw)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, encoded, nil)
	if err != nil {
		return nil, err
	}
	streamheaders.Apply(request.Header, headers)
	h.applyExternalUsenetWebDAVAuth(request)
	if rangeHeader != "" {
		request.Header.Set("Range", rangeHeader)
	}
	if err = requestsecurity.ValidateOutboundURL(ctx, request.URL.String(), h.configuredExternalHostPolicy()); err != nil {
		return nil, err
	}
	if err = g.current(ctx); err != nil {
		return nil, err
	}
	return request, nil
}

func (h *VideoHandler) ancillaryOpen(ctx context.Context, resource string) (*streaming.Response, error) {
	g := shareMediaFrom(ctx)
	if g == nil || g.handler != h || normalizeShareResource(resource) != g.owner.Resource() {
		return nil, datastore.ErrSharePlaybackChanged
	}
	if err := g.current(ctx); err != nil {
		return nil, err
	}
	if strings.HasPrefix(resource, "http://") || strings.HasPrefix(resource, "https://") {
		req, err := h.shareHTTPInputRequest(ctx, resource, http.MethodGet, "")
		if err != nil {
			return nil, err
		}
		res, err := h.vodHTTPDo(h.externalProxyHTTPClient, req)
		if err != nil {
			return nil, err
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			_ = res.Body.Close()
			return nil, datastore.ErrSharePlaybackUnavailable
		}
		return &streaming.Response{Body: res.Body, Headers: res.Header, Status: res.StatusCode, ContentLength: res.ContentLength}, nil
	}
	return h.vodOpenStream(ctx, streaming.Request{Path: resource, Method: http.MethodGet})
}
func (h *VideoHandler) runShareSourceCommand(ctx context.Context, resource, tool string, args []string, output io.Writer, job *shareAncillaryJob) (string, error) {
	g := shareMediaFrom(ctx)
	if g == nil || tool == "" {
		return "", datastore.ErrSharePlaybackUnavailable
	}
	finish, err := g.beginAncillaryOperation(ctx)
	if err != nil {
		return "", err
	}
	defer finish()
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	response, err := h.ancillaryOpen(child, resource)
	if err != nil {
		return "", err
	}
	if response == nil || response.Body == nil {
		if response != nil {
			_ = response.Close()
		}
		return "", datastore.ErrSharePlaybackUnavailable
	}
	owner := &shareSourceCommand{body: response.Body, copyDone: make(chan struct{}), stderr: &shareStderr{}}
	owner.inputRead, owner.inputWrite, err = os.Pipe()
	if err != nil {
		_ = response.Close()
		return "", err
	}
	// Signal before the provider's read mutex and join the actual close callback.
	closeDone := make(chan struct{})
	stopClose := context.AfterFunc(child, func() { defer close(closeDone); _ = response.Close(); _ = owner.inputWrite.Close() })
	var closeOnce sync.Once
	joinClose := func() {
		closeOnce.Do(func() {
			if !stopClose() {
				<-closeDone
			}
		})
	}
	defer func() { joinClose(); _ = response.Close(); _ = owner.inputRead.Close(); _ = owner.inputWrite.Close() }()
	confined := []string{"-nostdin", "-protocol_whitelist", "pipe", "-format_whitelist", shareRawFormats}
	owner.cmd = exec.CommandContext(child, tool, append(confined, args...)...)
	owner.cmd.Stdin = owner.inputRead
	owner.cmd.Stdout = &shareCurrentWriter{ctx: child, guard: g, destination: output, cancel: cancel}
	owner.cmd.Stderr = owner.stderr
	if job != nil {
		if job.guard != g || job.handler != h {
			return "", datastore.ErrSharePlaybackChanged
		}
		job.mu.Lock()
		job.commands = append(job.commands, owner.cmd)
		job.mu.Unlock()
	}
	if err = g.current(child); err != nil {
		return "", err
	}
	if err = owner.cmd.Start(); err != nil {
		return "", err
	}
	_ = owner.inputRead.Close()
	go func() {
		defer close(owner.copyDone)
		buffer := make([]byte, 32<<10)
		for {
			n, readErr := owner.body.Read(buffer)
			if readErr != nil && readErr != io.EOF {
				owner.readErr = readErr
			}
			if n > 0 {
				if _, writeErr := owner.inputWrite.Write(buffer[:n]); writeErr != nil {
					owner.copyErr = writeErr
					break
				}
			}
			if readErr != nil {
				if readErr != io.EOF {
					owner.readErr = readErr
				}
				break
			}
		}
		_ = owner.inputWrite.Close()
	}()
	waitErr := owner.cmd.Wait()
	var originalReadErr error
	select {
	case <-owner.copyDone:
		originalReadErr = owner.readErr
	default:
	}
	// FFmpeg may finish a bounded sample before consuming the whole source.
	// Retire this exact read, but retain the job's original source owner/lease.
	cancel()
	closeErr := response.Close()
	_ = owner.inputWrite.Close()
	<-owner.copyDone
	joinClose()
	if waitErr != nil {
		return owner.stderr.String(), waitErr
	}
	if closeErr != nil {
		return owner.stderr.String(), closeErr
	}
	if originalReadErr != nil {
		return owner.stderr.String(), originalReadErr
	}
	if ctx.Err() != nil {
		return owner.stderr.String(), ctx.Err()
	}
	if err = g.current(ctx); err != nil {
		return owner.stderr.String(), err
	}
	return owner.stderr.String(), nil
}

type shareCurrentWriter struct {
	ctx         context.Context
	guard       *shareMediaGuard
	destination io.Writer
	cancel      context.CancelFunc
}

func (w *shareCurrentWriter) Write(p []byte) (int, error) {
	if err := w.guard.current(w.ctx); err != nil {
		w.cancel()
		return 0, err
	}
	n, err := w.destination.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.cancel()
	}
	return n, err
}

type shareLimitedWriter struct {
	destination io.Writer
	remaining   int64
}

func (w *shareLimitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errShareAncillaryBound
	}
	n, err := w.destination.Write(p)
	w.remaining -= int64(n)
	return n, err
}

func reserveShareAncillary(account string) (func(), error) {
	shareAncillaryBudget.Lock()
	defer shareAncillaryBudget.Unlock()
	if account == "" || shareAncillaryBudget.retained >= 64 || shareAncillaryBudget.accounts[account] >= 16 || shareAncillaryBudget.running >= 8 || shareAncillaryBudget.active[account] >= 2 {
		return nil, errShareAncillaryBound
	}
	shareAncillaryBudget.retained++
	shareAncillaryBudget.running++
	shareAncillaryBudget.accounts[account]++
	shareAncillaryBudget.active[account]++
	var once sync.Once
	return func() {
		once.Do(func() {
			shareAncillaryBudget.Lock()
			shareAncillaryBudget.retained--
			shareAncillaryBudget.running--
			shareAncillaryBudget.accounts[account]--
			shareAncillaryBudget.active[account]--
			if shareAncillaryBudget.accounts[account] == 0 {
				delete(shareAncillaryBudget.accounts, account)
			}
			if shareAncillaryBudget.active[account] == 0 {
				delete(shareAncillaryBudget.active, account)
			}
			shareAncillaryBudget.Unlock()
		})
	}, nil
}

func (h *VideoHandler) newShareAncillaryJob(r *http.Request, kind string) (*shareAncillaryJob, error) {
	g := shareMediaFrom(r.Context())
	if err := shareAncillarySource(r, g); err != nil {
		return nil, err
	}
	session, ok := auth.GetSession(r)
	if g == nil || g.handler != h || !ok || session.AccountID == "" || g.owner.CheckRequest(r, g.owner.ProfileID(), g.owner.Resource()) != nil {
		return nil, datastore.ErrSharePlaybackChanged
	}
	registry := h.ancillaryRegistry()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil, datastore.ErrSharePlaybackUnavailable
	}
	refund, err := reserveShareAncillary(session.AccountID)
	if err != nil {
		return nil, err
	}
	reserved := true
	defer func() {
		if reserved {
			refund()
		}
	}()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(nonce[:])
	source, err := g.transfer(g.owner.ProfileID(), g.owner.Resource())
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(10 * time.Minute)
	if g.owner.ExpiresAt().Before(deadline) {
		deadline = g.owner.ExpiresAt()
	}
	ctx, cancel := context.WithDeadline(source, deadline)
	base := ""
	if kind == "subtitle" && h.subtitleExtractManager != nil {
		base = h.subtitleExtractManager.baseDir
	}
	if kind == "thumbnail" && h.thumbnailManager != nil {
		base = h.thumbnailManager.baseDir
	}
	var dir string
	if base != "" {
		dir, err = os.MkdirTemp(base, "share-"+id+"-")
		if err != nil {
			cancel()
			g.Close()
			return nil, err
		}
	}
	j := &shareAncillaryJob{created: time.Now(), handler: h, registry: registry, guard: g, id: id, kind: kind, account: session.AccountID, token: session.Token, profile: g.owner.ProfileID(), resource: g.owner.Resource(), dir: dir, ctx: ctx, cancel: cancel, producerDone: make(chan struct{}), done: make(chan struct{}), watchDone: make(chan struct{})}
	registry.jobs[id] = j
	reserved = false
	return j, nil
}
func (j *shareAncillaryJob) start(produce func() error) {
	go func() {
		defer close(j.producerDone)
		err := produce()
		j.mu.Lock()
		j.err = err
		j.mu.Unlock()
		shareAncillaryBudget.Lock()
		shareAncillaryBudget.running--
		shareAncillaryBudget.active[j.account]--
		if shareAncillaryBudget.active[j.account] == 0 {
			delete(shareAncillaryBudget.active, j.account)
		}
		shareAncillaryBudget.Unlock()
	}()
	go func() { defer close(j.watchDone); <-j.ctx.Done(); j.finishClose() }()
}
func (j *shareAncillaryJob) close() { j.finishClose(); <-j.watchDone }
func (j *shareAncillaryJob) finishClose() {
	j.once.Do(func() {
		j.mu.Lock()
		j.closed = true
		j.cancel()
		j.mu.Unlock()
		j.guard.stopAndWait()
		<-j.producerDone
		j.refs.Wait()
		// Failed removal deliberately retains the actual output/admission charge.
		if j.dir != "" {
			if err := os.RemoveAll(j.dir); err != nil {
				j.mu.Lock()
				j.err = err
				j.mu.Unlock()
				close(j.done)
				return
			}
		}
		j.guard.Close()
		j.registry.mu.Lock()
		if j.registry.jobs[j.id] == j {
			delete(j.registry.jobs, j.id)
		}
		j.registry.mu.Unlock()
		shareAncillaryBudget.Lock()
		shareAncillaryBudget.retained--
		shareAncillaryBudget.accounts[j.account]--
		if shareAncillaryBudget.accounts[j.account] == 0 {
			delete(shareAncillaryBudget.accounts, j.account)
		}
		shareAncillaryBudget.bytes -= j.bytes
		shareAncillaryBudget.Unlock()
		close(j.done)
	})
	<-j.done
}
func (j *shareAncillaryJob) borrow(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, *http.Request, func(), error) {
	session, ok := auth.GetSession(r)
	if raw := r.URL.Query().Get("path"); raw != "" && normalizeShareResource(resolveScopedPlaybackPath(r, raw)) != j.resource {
		return w, r, func() {}, datastore.ErrSharePlaybackChanged
	}
	if !ok || session.Token != j.token || session.AccountID != j.account {
		return w, r, func() {}, datastore.ErrSharePlaybackChanged
	}
	j.mu.Lock()
	if j.closed || j.ctx.Err() != nil {
		j.mu.Unlock()
		return w, r, func() {}, datastore.ErrSharePlaybackChanged
	}
	j.refs.Add(1)
	j.mu.Unlock()
	writer, request, flush, cleanup, err := j.guard.borrowHTTP(w, r, j.profile, j.resource)
	if err != nil {
		j.refs.Done()
		return w, r, func() {}, err
	}
	deadline := j.guard.owner.ExpiresAt()
	if original, ok := j.ctx.Deadline(); ok && original.Before(deadline) {
		deadline = original
	}
	if incoming, ok := r.Context().Deadline(); ok && incoming.Before(deadline) {
		deadline = incoming
	}
	_ = http.NewResponseController(writer).SetWriteDeadline(deadline)
	return writer, request, func() { flush(); cleanup(); j.refs.Done() }, nil
}
func (h *VideoHandler) findShareAncillary(r *http.Request, id, kind string) *shareAncillaryJob {
	registry := h.ancillaryRegistry()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	session, ok := auth.GetSession(r)
	if !ok {
		return nil
	}
	if id != "" {
		j := registry.jobs[id]
		if j != nil && j.kind == kind {
			return j
		}
		return nil
	}
	var newest *shareAncillaryJob
	for _, j := range registry.jobs {
		if j.kind == kind && j.token == session.Token && j.account == session.AccountID && normalizeShareResource(resolveScopedPlaybackPath(r, r.URL.Query().Get("path"))) == j.resource {
			if newest == nil || j.created.After(newest.created) || (j.created.Equal(newest.created) && j.id > newest.id) {
				newest = j
			}
		}
	}
	return newest
}

type shareJobFileWriter struct {
	job       *shareAncillaryJob
	file      *os.File
	remaining int64
}

func (w *shareJobFileWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errShareAncillaryBound
	}
	shareAncillaryBudget.Lock()
	if shareAncillaryBudget.bytes+int64(len(p)) > 256<<20 || w.job.bytes+int64(len(p)) > 64<<20 {
		shareAncillaryBudget.Unlock()
		return 0, errShareAncillaryBound
	}
	shareAncillaryBudget.bytes += int64(len(p))
	w.job.bytes += int64(len(p))
	shareAncillaryBudget.Unlock()
	// Charge before the actual encoded file write, retaining partial/error writes
	// conservatively until the original directory is physically removed.
	n, err := w.file.Write(p)
	w.remaining -= int64(n)
	return n, err
}
func (j *shareAncillaryJob) output(name string, limit int64) (*shareJobFileWriter, error) {
	f, err := os.OpenFile(filepath.Join(j.dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &shareJobFileWriter{job: j, file: f, remaining: limit}, nil
}
func (j *shareAncillaryJob) serveFile(w http.ResponseWriter, r *http.Request, name, contentType string) {
	writer, request, finish, err := j.borrow(w, r)
	if err != nil {
		shareMediaError(w, err)
		return
	}
	defer finish()
	if filepath.Base(name) != name || name == "." {
		http.NotFound(writer, request)
		return
	}
	j.mu.Lock()
	failed := j.err
	closed := j.closed
	j.mu.Unlock()
	if failed != nil || closed {
		ancillaryError(writer, failed)
		return
	}
	select {
	case <-j.producerDone:
	default:
		ancillaryJSON(writer, http.StatusAccepted, map[string]string{"status": "generating"})
		return
	}
	f, err := os.Open(filepath.Join(j.dir, name))
	if err != nil {
		http.NotFound(writer, request)
		return
	}
	defer f.Close()
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Cache-Control", "private, no-store")
	_, _ = io.Copy(writer, f)
}

type shareStartedOutput struct {
	destination io.Writer
	attempted   bool
}

func (w *shareStartedOutput) Write(p []byte) (int, error) {
	w.attempted = true
	return w.destination.Write(p)
}

func (h *VideoHandler) shareTransmux(w http.ResponseWriter, r *http.Request, source string, forceAAC bool, container transmuxContainer) (bool, error) {
	refund, reserveErr := h.reserveShareSetup(r)
	if reserveErr != nil {
		ancillaryError(w, reserveErr)
		return true, reserveErr
	}
	defer refund()
	meta, err := h.ancillaryMetadata(r.Context(), source)
	if err != nil {
		ancillaryError(w, err)
		return true, err
	}
	plan := h.buildTransmuxPlan(meta, "pipe:0", forceAAC, "")
	if container == outputMpegTs {
		if hdr, _ := sourceNeedsToneMapping(selectPrimaryVideoStream(meta)); hdr {
			err = datastore.ErrSharePlaybackUnavailable
			ancillaryError(w, err)
			return true, err
		}
		plan = buildMpegTsPlanWithCaps(meta, "pipe:0", "", HWAccelCaps{Encode: HWNone})
		writeDlnaMpegTsHeaders(w, r)
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", container.contentType())
		w.Header().Set("Accept-Ranges", "none")
		w.WriteHeader(http.StatusOK)
		return true, nil
	}
	w.Header().Set("Content-Type", container.contentType())
	w.Header().Set("Accept-Ranges", "none")
	output := &shareStartedOutput{destination: w}
	_, err = h.runShareSourceCommand(r.Context(), source, h.ffmpegPath, plan.args, output, nil)
	if err != nil && !output.attempted {
		ancillaryError(w, err)
	}
	return true, err
}
func (h *VideoHandler) shareCrop(w http.ResponseWriter, r *http.Request) {
	refund, reserveErr := h.reserveShareSetup(r)
	if reserveErr != nil {
		ancillaryError(w, reserveErr)
		return
	}
	defer refund()
	g := shareMediaFrom(r.Context())
	meta, err := h.ancillaryMetadata(r.Context(), g.owner.Resource())
	if err != nil {
		ancillaryError(w, err)
		return
	}
	duration, err := shareAncillaryDuration(meta)
	video := selectPrimaryVideoStream(meta)
	if err != nil || video == nil || video.Height <= 0 {
		ancillaryError(w, datastore.ErrSharePlaybackUnavailable)
		return
	}
	samples := []float64{duration * .25, duration * .5}
	if duration > 3600 {
		samples = []float64{300, 1800}
	} else if duration > 600 {
		samples = []float64{300, duration * .5}
	}
	threshold := 24
	if video.ColorTransfer == "smpte2084" || video.ColorTransfer == "arib-std-b67" {
		threshold = 100
	}
	var top, bottom []float64
	for _, sample := range samples {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		stderr, e := h.runShareSourceCommand(ctx, g.owner.Resource(), h.ffmpegPath, []string{"-i", "pipe:0", "-ss", fmt.Sprintf("%.3f", sample), "-an", "-vframes", "5", "-vf", fmt.Sprintf("cropdetect=%d:16:0", threshold), "-f", "null", "pipe:1"}, io.Discard, nil)
		cancel()
		if e != nil {
			ancillaryError(w, e)
			return
		}
		matches := cropdetectRegex.FindAllStringSubmatch(stderr, -1)
		if len(matches) == 0 {
			ancillaryError(w, datastore.ErrSharePlaybackUnavailable)
			return
		}
		m := matches[len(matches)-1]
		height, _ := strconv.Atoi(m[2])
		y, _ := strconv.Atoi(m[4])
		if height < video.Height/2 || height > video.Height || y < 0 || y+height > video.Height {
			ancillaryError(w, datastore.ErrSharePlaybackUnavailable)
			return
		}
		top = append(top, float64(y)/float64(video.Height))
		bottom = append(bottom, float64(video.Height-y-height)/float64(video.Height))
	}
	sort.Float64s(top)
	sort.Float64s(bottom)
	t, b := top[len(top)/2], bottom[len(bottom)/2]
	if math.Abs(t-b) > .03 {
		t, b = 0, 0
	}
	ancillaryJSON(w, http.StatusOK, cropDetectResponse{LetterboxTop: math.Round(t*1000) / 1000, LetterboxBottom: math.Round(b*1000) / 1000})
}

func shareSubtitleTracks(meta *ffprobeOutput) []SubtitleTrackInfo {
	out := make([]SubtitleTrackInfo, 0)
	if meta == nil {
		return out
	}
	for _, s := range meta.Streams {
		if s.CodecType == "subtitle" {
			out = append(out, SubtitleTrackInfo{Index: len(out), AbsoluteIndex: s.Index, Language: s.Tags["language"], Title: s.Tags["title"], Codec: s.CodecName, Forced: s.Disposition["forced"] == 1})
		}
	}
	return out
}
func (h *VideoHandler) shareProbeSubtitles(w http.ResponseWriter, r *http.Request) {
	refund, reserveErr := h.reserveShareSetup(r)
	if reserveErr != nil {
		ancillaryError(w, reserveErr)
		return
	}
	defer refund()
	g := shareMediaFrom(r.Context())
	meta, err := h.ancillaryMetadata(r.Context(), g.owner.Resource())
	if err != nil {
		ancillaryError(w, err)
		return
	}
	ancillaryJSON(w, http.StatusOK, map[string]any{"tracks": shareSubtitleTracks(meta)})
}
func (h *VideoHandler) shareStartSubtitle(w http.ResponseWriter, r *http.Request) {
	if h.subtitleExtractManager == nil {
		ancillaryError(w, datastore.ErrSharePlaybackUnavailable)
		return
	}
	g := shareMediaFrom(r.Context())
	track, err := strconv.Atoi(r.URL.Query().Get("subtitleTrack"))
	if err != nil || track < 0 {
		http.Error(w, "invalid subtitleTrack parameter", 400)
		return
	}
	offset := 0.0
	if text := r.URL.Query().Get("startOffset"); text != "" {
		offset, err = strconv.ParseFloat(text, 64)
		if err != nil || !ancillaryFinite(offset) || offset < 0 {
			http.Error(w, "invalid startOffset parameter", 400)
			return
		}
	}
	_, ok := auth.GetSession(r)
	if !ok {
		shareMediaError(w, datastore.ErrSharePlaybackChanged)
		return
	}
	refund, err := h.reserveShareSetup(r)
	if err != nil {
		ancillaryError(w, err)
		return
	}
	defer refund()
	meta, err := h.ancillaryMetadata(r.Context(), g.owner.Resource())
	if err != nil {
		ancillaryError(w, err)
		return
	}
	tracks := shareSubtitleTracks(meta)
	if track >= len(tracks) || !isTextBasedSubtitle(tracks[track].Codec) {
		http.Error(w, "unsupported subtitle track", 400)
		return
	}
	format := normalizeSubtitleExtractFormat(r.URL.Query().Get("format"))
	refund()
	job, err := h.newShareAncillaryJob(r, "subtitle")
	if err != nil {
		ancillaryError(w, err)
		return
	}
	job.start(func() (result error) {
		output, e := job.output(subtitleExtractOutputFileName(format), 16<<20)
		if e != nil {
			return e
		}
		defer func() { result = errors.Join(result, output.file.Close()) }()
		_, e = h.runShareSourceCommand(job.ctx, job.resource, h.ffmpegPath, []string{"-i", "pipe:0", "-ss", fmt.Sprintf("%.3f", offset), "-map", fmt.Sprintf("0:%d", tracks[track].AbsoluteIndex), "-vn", "-an", "-f", map[string]string{"vtt": "webvtt", "ass": "ass"}[format], "pipe:1"}, output, job)
		return e
	})
	ancillaryJSON(w, http.StatusOK, map[string]any{"sessionId": job.id, "subtitleUrl": fmt.Sprintf("/api/video/subtitles/%s/%s", job.id, subtitleExtractOutputFileName(format)), "format": format, "firstCueTime": 0})
}
func (h *VideoHandler) shareServeSubtitle(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["sessionID"]
	j := h.findShareAncillary(r, id, "subtitle")
	if j == nil {
		shareMediaError(w, datastore.ErrSharePlaybackChanged)
		return
	}
	name := mux.Vars(r)["file"]
	if name == "" {
		name = filepath.Base(r.URL.Path)
	}
	if name != "subtitles.vtt" && name != "subtitles.ass" {
		http.NotFound(w, r)
		return
	}
	content := "text/vtt; charset=utf-8"
	if strings.HasSuffix(name, ".ass") {
		content = "text/x-ssa; charset=utf-8"
	}
	j.serveFile(w, r, name, content)
}

// Actual bounded capability children use the same original job lifetime. They
// generate no external source and cannot publish or adopt media authority.
func (h *VideoHandler) shareThumbnailToneMode(job *shareAncillaryJob, toneMap bool, dv string) (thumbnailToneMapMode, error) {
	if !toneMap {
		return thumbnailToneMapNone, nil
	}
	ctx, cancel := context.WithTimeout(job.ctx, 8*time.Second)
	defer cancel()
	var listing bytes.Buffer
	stderr := &shareStderr{}
	cmd := exec.CommandContext(ctx, h.ffmpegPath, "-hide_banner", "-filters")
	cmd.Stdout = &shareCurrentWriter{ctx: ctx, guard: job.guard, destination: &shareLimitedWriter{destination: &listing, remaining: 1 << 20}, cancel: cancel}
	cmd.Stderr = stderr
	job.mu.Lock()
	job.commands = append(job.commands, cmd)
	job.mu.Unlock()
	if err := job.guard.current(ctx); err != nil {
		return "", err
	}
	if err := cmd.Run(); err != nil {
		return "", err
	}
	caps := make(map[string]bool)
	for _, line := range strings.Split(listing.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 {
			caps[fields[1]] = true
		}
	}
	if caps["libplacebo"] {
		probe := exec.CommandContext(ctx, h.ffmpegPath, "-nostdin", "-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=16x16:d=0.1", "-frames:v", "1", "-vf", "libplacebo=w=16:h=16:colorspace=bt709:color_primaries=bt709:color_trc=bt709,format=yuv420p", "-f", "null", "pipe:1")
		probe.Stdout = io.Discard
		probe.Stderr = &shareStderr{}
		job.mu.Lock()
		job.commands = append(job.commands, probe)
		job.mu.Unlock()
		caps[thumbnailLibplaceboRuntime] = probe.Run() == nil
	}
	if err := job.guard.current(ctx); err != nil {
		return "", err
	}
	switch {
	case isDolbyVisionProfile5(dv) && caps[thumbnailLibplaceboRuntime]:
		return thumbnailToneMapLibplacebo, nil
	case isDolbyVisionProfile5(dv):
		return "", datastore.ErrSharePlaybackUnavailable
	case caps["zscale"]:
		return thumbnailToneMapZscale, nil
	case caps[thumbnailLibplaceboRuntime]:
		return thumbnailToneMapLibplacebo, nil
	case caps["tonemap"]:
		return thumbnailToneMapFFmpeg, nil
	default:
		return "", datastore.ErrSharePlaybackUnavailable
	}
}

func (h *VideoHandler) shareStartThumbnails(w http.ResponseWriter, r *http.Request) {
	if parseBoolQuery(r.URL.Query().Get("defer")) || parseBoolQuery(r.URL.Query().Get("seekrOnly")) {
		http.Error(w, "shared deferred/external thumbnail generation unavailable", http.StatusNotImplemented)
		return
	}
	if h.thumbnailManager == nil || !h.thumbnailGenerationSettings().Enabled {
		ancillaryError(w, datastore.ErrSharePlaybackUnavailable)
		return
	}
	job, err := h.newShareAncillaryJob(r, "thumbnail")
	if err != nil {
		ancillaryError(w, err)
		return
	}
	requested, _ := strconv.Atoi(r.URL.Query().Get("interval"))
	dv := parseThumbnailDVProfile(r)
	job.start(func() error {
		meta, e := h.ancillaryMetadata(job.ctx, job.resource)
		if e != nil {
			return e
		}
		duration, e := shareAncillaryDuration(meta)
		if e != nil {
			return e
		}
		chapters := make([]float64, 0, len(meta.Chapters))
		for _, chapter := range meta.Chapters {
			v, e := strconv.ParseFloat(chapter.StartTime, 64)
			if e == nil && ancillaryFinite(v) {
				chapters = append(chapters, v)
			}
		}
		interval, targets := thumbnailGenerationTargets(duration, requested, chapters)
		if len(targets) == 0 || len(targets) > 120 {
			return errShareAncillaryBound
		}
		toneMap := false
		if v := selectPrimaryVideoStream(meta); v != nil {
			toneMap = v.ColorTransfer == "smpte2084" || v.ColorTransfer == "arib-std-b67"
		}
		// Existing capability selection is synchronous; only the resulting filter
		// DATA is used. No source bridge, global file or Seekr work is admitted.
		mode, e := h.shareThumbnailToneMode(job, toneMap, dv)
		if e != nil {
			return e
		}
		job.mu.Lock()
		job.duration = duration
		job.interval = interval
		job.total = len(targets)
		job.mu.Unlock()
		work, cancel := context.WithCancel(job.ctx)
		defer cancel()
		queue := make(chan thumbnailTarget)
		errs := make(chan error, 2)
		var workers sync.WaitGroup
		count := h.thumbnailGenerationSettings().Workers
		if count < 1 {
			count = 1
		}
		if count > 2 {
			count = 2
		}
		for n := 0; n < count; n++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for target := range queue {
					name := fmt.Sprintf("thumb_%04d.jpg", target.Index)
					output, e := job.output(name, 4<<20)
					if e == nil {
						ctx, stop := context.WithTimeout(work, 45*time.Second)
						_, e = h.runShareSourceCommand(ctx, job.resource, h.ffmpegPath, []string{"-i", "pipe:0", "-ss", fmt.Sprintf("%.3f", target.TimeSec), "-an", "-frames:v", "1", "-vf", thumbnailFilter(mode), "-f", "image2pipe", "-vcodec", "mjpeg", "pipe:1"}, output, job)
						stop()
						e = errors.Join(e, output.file.Close())
					}
					if e != nil {
						select {
						case errs <- e:
						default:
						}
						cancel()
						return
					}
					job.mu.Lock()
					job.thumbnails = append(job.thumbnails, thumbnailDetails{TimeSec: target.TimeSec, File: name})
					job.mu.Unlock()
				}
			}()
		}
	send:
		for _, target := range targets {
			select {
			case queue <- target:
			case <-work.Done():
				break send
			}
		}
		close(queue)
		workers.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				return e
			}
		}
		return work.Err()
	})
	ancillaryJSON(w, http.StatusAccepted, map[string]any{"key": job.id, "status": "generating", "started": true})
}
func (h *VideoHandler) shareThumbnailStatus(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("key")
	job := h.findShareAncillary(r, id, "thumbnail")
	if job == nil {
		shareMediaError(w, datastore.ErrSharePlaybackChanged)
		return
	}
	writer, request, finish, err := job.borrow(w, r)
	if err != nil {
		shareMediaError(w, err)
		return
	}
	defer finish()
	job.mu.Lock()
	response := thumbnailStatusResponse{Key: job.id, Status: "generating", DurationSec: job.duration, IntervalSec: job.interval, Total: job.total, Generated: len(job.thumbnails), UpdatedAt: time.Now(), Thumbnails: make([]thumbnailURLItem, 0, len(job.thumbnails))}
	for _, thumb := range job.thumbnails {
		u := fmt.Sprintf("/api/video/thumbnails/image/%s/%s", job.id, thumb.File)
		if token := request.URL.Query().Get("token"); token != "" {
			u += "?token=" + url.QueryEscape(token)
		}
		response.Thumbnails = append(response.Thumbnails, thumbnailURLItem{TimeSec: thumb.TimeSec, URL: u})
	}
	if job.err != nil {
		response.Status = "failed"
		response.Error = "Shared thumbnail generation unavailable."
	}
	job.mu.Unlock()
	select {
	case <-job.producerDone:
		if response.Status != "failed" {
			response.Status = "ready"
		}
	default:
	}
	sort.Slice(response.Thumbnails, func(i, j int) bool { return response.Thumbnails[i].TimeSec < response.Thumbnails[j].TimeSec })
	ancillaryJSON(writer, http.StatusOK, response)
}
func (h *VideoHandler) shareServeThumbnail(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	job := h.findShareAncillary(r, vars["key"], "thumbnail")
	if job == nil {
		shareMediaError(w, datastore.ErrSharePlaybackChanged)
		return
	}
	name := vars["file"]
	if filepath.Base(name) != name || !strings.HasSuffix(name, ".jpg") {
		http.NotFound(w, r)
		return
	}
	job.serveFile(w, r, name, "image/jpeg")
}

// PNG boundaries and dimensions are validated before decoding. Only one bounded
// encoded frame and its decoded pixels are retained; phase output has a separate
// encoded 32MiB bound enforced before bytes enter this parser.
func shareReadCreditFrames(reader io.Reader, count int) ([]credits.FrameFeatures, error) {
	features := make([]credits.FrameFeatures, 0, count)
	for n := 0; n < count; n++ {
		signature := make([]byte, 8)
		if _, err := io.ReadFull(reader, signature); err != nil {
			return nil, err
		}
		if !bytes.Equal(signature, []byte{137, 80, 78, 71, 13, 10, 26, 10}) {
			return nil, credits.ErrOwnedAnalysis
		}
		frame := append([]byte(nil), signature...)
		first := true
		for {
			header := make([]byte, 8)
			if _, err := io.ReadFull(reader, header); err != nil {
				return nil, err
			}
			size := binary.BigEndian.Uint32(header[:4])
			if size > 4<<20 || int64(len(frame))+int64(size)+12 > 4<<20 {
				return nil, errShareAncillaryBound
			}
			if first && (string(header[4:]) != "IHDR" || size != 13) {
				return nil, credits.ErrOwnedAnalysis
			}
			data := make([]byte, int(size)+4)
			if _, err := io.ReadFull(reader, data); err != nil {
				return nil, err
			}
			if first {
				width, height := binary.BigEndian.Uint32(data[:4]), binary.BigEndian.Uint32(data[4:8])
				if width < 3 || height < 3 || width > 960 || height > 960 {
					return nil, credits.ErrOwnedAnalysis
				}
				first = false
			}
			frame = append(frame, header...)
			frame = append(frame, data...)
			if string(header[4:]) == "IEND" {
				if size != 0 {
					return nil, credits.ErrOwnedAnalysis
				}
				break
			}
		}
		image, err := png.Decode(bytes.NewReader(frame))
		if err != nil {
			return nil, err
		}
		measured, err := credits.MeasureOwnedFrame(image)
		if err != nil {
			return nil, err
		}
		features = append(features, measured)
	}
	var extra [1]byte
	n, err := reader.Read(extra[:])
	if n != 0 || err != io.EOF {
		return nil, credits.ErrOwnedAnalysis
	}
	return features, nil
}
func (h *VideoHandler) shareCreditPhase(job *shareAncillaryJob, start, interval float64, count, width int) ([]credits.FrameFeatures, error) {
	if count < 1 || count > 50 || width < 3 || width > 960 {
		return nil, credits.ErrOwnedAnalysis
	}
	pr, pw := io.Pipe()
	done := make(chan struct{})
	var measured []credits.FrameFeatures
	var decodeErr error
	go func() {
		defer close(done)
		measured, decodeErr = shareReadCreditFrames(pr, count)
		_ = pr.CloseWithError(decodeErr)
	}()
	_, err := h.runShareSourceCommand(job.ctx, job.resource, h.ffmpegPath, []string{"-i", "pipe:0", "-ss", fmt.Sprintf("%.3f", start), "-an", "-vf", fmt.Sprintf("fps=1/%.6f,scale=%d:-2", interval, width), "-frames:v", strconv.Itoa(count), "-f", "image2pipe", "-vcodec", "png", "pipe:1"}, &shareLimitedWriter{destination: pw, remaining: 32 << 20}, job)
	_ = pw.CloseWithError(err)
	<-done
	return measured, errors.Join(err, decodeErr)
}
func (h *VideoHandler) shareDetectCredits(w http.ResponseWriter, r *http.Request) {
	job, err := h.newShareAncillaryJob(r, "credits")
	if err != nil {
		ancillaryError(w, err)
		return
	}
	job.start(func() error {
		meta, e := h.ancillaryMetadata(job.ctx, job.resource)
		if e != nil {
			return e
		}
		duration, e := shareAncillaryDuration(meta)
		if e != nil {
			return e
		}
		tail := math.Min(duration*.15, 480)
		start := duration - tail
		count := int(math.Ceil(tail / 10))
		if count > 50 {
			count = 50
		}
		if count < 1 {
			return credits.ErrOwnedAnalysis
		}
		coarse, e := h.shareCreditPhase(job, start, 10, count, 320)
		if e != nil {
			return e
		}
		estimated, found, e := credits.OwnedCoarseStart(duration, start, 10, coarse)
		if e != nil || !found {
			return e
		}
		fineStart := math.Max(0, estimated-15)
		fineCount := int(math.Min(30, math.Ceil(duration-fineStart)))
		fine, e := h.shareCreditPhase(job, fineStart, 1, fineCount, 320)
		if e != nil {
			return e
		}
		refined, e := credits.OwnedFineStart(fineStart, 1, fine)
		if e != nil {
			return e
		}
		edgeStart := math.Min(duration-3, refined+(duration-refined)*.5)
		if edgeStart < 0 {
			return credits.ErrOwnedAnalysis
		}
		edges, e := h.shareCreditPhase(job, edgeStart, 1, 3, 960)
		if e != nil {
			return e
		}
		hasText, e := credits.OwnedHasText(edges)
		if e != nil {
			return e
		}
		job.mu.Lock()
		job.detected = hasText
		job.creditsStart = refined
		job.duration = duration
		job.mu.Unlock()
		return nil
	})
	ancillaryJSON(w, http.StatusAccepted, map[string]string{"status": "detecting"})
}
func (h *VideoHandler) shareCreditsStatus(w http.ResponseWriter, r *http.Request) {
	job := h.findShareAncillary(r, "", "credits")
	if job == nil {
		ancillaryJSON(w, http.StatusOK, map[string]string{"status": "pending"})
		return
	}
	writer, _, finish, err := job.borrow(w, r)
	if err != nil {
		shareMediaError(w, err)
		return
	}
	defer finish()
	select {
	case <-job.producerDone:
	default:
		ancillaryJSON(writer, http.StatusOK, map[string]string{"status": "detecting"})
		return
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.err != nil {
		ancillaryError(writer, job.err)
		return
	}
	result := map[string]any{"status": "complete", "detected": job.detected}
	if job.detected {
		result["creditsStartSec"] = job.creditsStart
	}
	ancillaryJSON(writer, http.StatusOK, result)
}
