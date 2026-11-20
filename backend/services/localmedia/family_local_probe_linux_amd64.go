//go:build linux && amd64

package localmedia

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"novastream/services/profileunlock"
)

var familyLocalProbeAdmission = struct {
	sync.Mutex
	files map[*familyLocalFile]bool
}{files: make(map[*familyLocalFile]bool)}

func newFamilyLocalProbeTools(config FamilyLocalProbeConfig) (*FamilyLocalProbeTools, error) {
	helper, hinfo, err := openFamilyProbeExecutable(config.HelperPath, config.HelperSHA256)
	if err != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	probe, pinfo, err := openFamilyProbeExecutable(config.FFprobePath, config.FFprobeSHA256)
	if err != nil {
		_ = helper.Close()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &FamilyLocalProbeTools{&familyLocalProbeToolsState{ctx: ctx, cancel: cancel, helper: helper, probe: probe, helperInfo: hinfo, probeInfo: pinfo, config: config, jobs: make(map[*familyLocalProbeToken]bool)}}
	t.self = t
	return t, nil
}
func openFamilyProbeExecutable(path, expected string) (*os.File, os.FileInfo, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 4096 || len(expected) != 64 || strings.ToLower(expected) != expected {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	if _, err := hex.DecodeString(expected); err != nil {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	// Reject special configured paths without first waiting for a FIFO writer
	// or device. os.OpenFile also owns close-on-exec on the returned descriptor.
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	keep := false
	defer func() {
		if !keep {
			_ = f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 64 || info.Size() > 64<<20 || info.Mode().Perm()&0111 == 0 {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Nlink != 1 {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	header := make([]byte, 20)
	if n, e := f.ReadAt(header, 0); e != nil || n != len(header) || !bytes.Equal(header[:6], []byte{0x7f, 'E', 'L', 'F', 2, 1}) || binary.LittleEndian.Uint16(header[18:20]) != 62 {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, io.NewSectionReader(f, 0, info.Size())); err != nil || hex.EncodeToString(hash.Sum(nil)) != expected {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	if !familyProbeExecutableCurrent(f, path, info) {
		return nil, nil, ErrFamilyLocalProbeUnavailable
	}
	keep = true
	return f, info, nil
}
func familyProbeExecutableCurrent(f *os.File, path string, original os.FileInfo) bool {
	if f == nil || original == nil {
		return false
	}
	held, err := f.Stat()
	named, e := os.Stat(path)
	if err != nil || e != nil || !os.SameFile(held, original) || !os.SameFile(named, original) || held.Size() != original.Size() || held.Mode() != original.Mode() {
		return false
	}
	a, ok := held.Sys().(*syscall.Stat_t)
	b, ok2 := original.Sys().(*syscall.Stat_t)
	c, ok3 := named.Sys().(*syscall.Stat_t)
	// Reads may update atime. Pin the executable's physical identity, content
	// version and link/mode state, without treating an access timestamp as data.
	same := func(x, y *syscall.Stat_t) bool {
		return x.Dev == y.Dev && x.Ino == y.Ino && x.Size == y.Size && x.Mtim == y.Mtim && x.Ctim == y.Ctim && x.Mode == y.Mode && x.Nlink == y.Nlink
	}
	return ok && ok2 && ok3 && same(a, b) && same(c, b)
}
func duplicateFamilyProbeFile(f *os.File) (*os.File, error) {
	fd, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	return os.NewFile(uintptr(fd), "family-local-probe-owned"), nil
}

type familyLocalProbeBuffer struct {
	mu      sync.Mutex
	data    []byte
	maximum int
	cancel  context.CancelFunc
	failed  bool
}

func (b *familyLocalProbeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || len(p) > b.maximum-len(b.data) {
		b.failed = true
		b.cancel()
		return 0, ErrFamilyLocalProbeUnavailable
	}
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *familyLocalProbeBuffer) snapshot() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte{}, b.data...), b.failed
}
func (b *familyLocalProbeBuffer) clear() { b.mu.Lock(); clear(b.data); b.data = nil; b.mu.Unlock() }

type familyLocalProbeReadAt struct {
	ctx    context.Context
	reader *familyLocalReader
	file   *familyLocalFile
}

func (r familyLocalProbeReadAt) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) > 32<<10 || offset < 0 || r.ctx.Err() != nil || r.file.Check(r.ctx) != nil {
		return 0, ErrFamilyLocalProbeUnavailable
	}
	r.reader.mu.Lock()
	closed := r.reader.closed
	r.reader.mu.Unlock()
	if closed {
		return 0, ErrFamilyLocalProbeUnavailable
	}
	n, err := r.reader.file.ReadAt(p, offset)
	stat, e := r.reader.file.Stat()
	version, ok := parentFileStat{}, false
	if e == nil {
		version, ok = parentStat(stat)
	}
	if n < 0 || n > len(p) || err != nil && err != io.EOF || !ok || version != r.file.stat || r.ctx.Err() != nil || r.file.Check(r.ctx) != nil {
		clear(p)
		return 0, ErrFamilyLocalProbeUnavailable
	}
	return n, err
}

type familyLocalProbeJob struct{ *familyLocalProbeJobState }
type familyLocalProbeJobState struct {
	self                                                          *familyLocalProbeJob
	file                                                          *familyLocalFile
	tools                                                         *FamilyLocalProbeTools
	token                                                         *familyLocalProbeToken
	reader                                                        *familyLocalReader
	readerHeld                                                    bool
	helper, probe, readyRead, readyWrite, ackRead, ackWrite, zero *os.File
	stopTools, stopSource, stopReader                             func()
	watchDone, readyDone                                          chan struct{}
	cmd                                                           *exec.Cmd
	started, waited, acked                                        bool
	format                                                        string
	stdout, stderr                                                *familyLocalProbeBuffer
	once                                                          sync.Once
}

func (familyLocalProbeJob) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("familyLocalProbeJob(opaque)"))
}
func (familyLocalProbeJob) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (j *familyLocalProbeJob) valid() bool {
	return j != nil && j.familyLocalProbeJobState != nil && j.self == j
}

func (s *Service) ProbeFamilyLocalFile(ctx context.Context, input profileunlock.FamilyLocalFile, tools *FamilyLocalProbeTools) (FamilyLocalProbeResult, error) {
	job, err := startFamilyLocalProbe(ctx, s, input, tools, false)
	if err != nil {
		return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
	}
	defer job.close()
	out, err := job.finish()
	job.close()
	// Cleanup is joined before returning metadata. Recheck the original source
	// after that potentially blocking join, using the original caller context.
	if err != nil || ctx.Err() != nil || tools.ctx.Err() != nil || !familyProbeExecutableCurrent(tools.helper, tools.config.HelperPath, tools.helperInfo) || !familyProbeExecutableCurrent(tools.probe, tools.config.FFprobePath, tools.probeInfo) || job.file.Check(ctx) != nil {
		return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
	}
	return out, nil
}

// verify selects only the fixed helper self-test, never arbitrary tool argv.
// It is private and is not selected by the public service probe method.
func startFamilyLocalProbe(ctx context.Context, s *Service, input profileunlock.FamilyLocalFile, tools *FamilyLocalProbeTools, verify bool) (job *familyLocalProbeJob, err error) {
	f, ok := input.(*familyLocalFile)
	if !ok || f == nil || f.self != f || s == nil || !s.FamilyLocalSourceReady(s.store) || !f.source.ReadyForStore(s.store) || ctx == nil || ctx.Err() != nil || !tools.valid() || f.info.Size <= 0 || f.info.Size > familyLocalProbeMaxInput {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	familyLocalProbeAdmission.Lock()
	if len(familyLocalProbeAdmission.files) >= 2 || familyLocalProbeAdmission.files[f] {
		familyLocalProbeAdmission.Unlock()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	familyLocalProbeAdmission.files[f] = true
	familyLocalProbeAdmission.Unlock()
	deadline := time.Now().Add(15 * time.Second)
	if expiry, ok := ctx.Deadline(); ok && expiry.Before(deadline) {
		deadline = expiry
	}
	if expiry := f.source.ExpiresAt(); expiry.Before(deadline) {
		deadline = expiry
	}
	life, cancel := context.WithDeadline(ctx, deadline)
	token := &familyLocalProbeToken{ctx: life, cancel: cancel, done: make(chan struct{})}
	j := &familyLocalProbeJob{&familyLocalProbeJobState{file: f, tools: tools, token: token}}
	j.self = j
	defer func() {
		if job == nil {
			j.close()
		}
	}()
	// Source.Check begins with an original-owner PG check. Link its actual
	// source cancellation before that check, including the pre-reader phase.
	j.stopSource = familyLocalAfterFunc(f.source.Context(), cancel)
	tools.mu.Lock()
	if tools.closed || tools.ctx.Err() != nil {
		tools.mu.Unlock()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	tools.jobs[token] = true
	j.stopTools = familyLocalAfterFunc(tools.ctx, cancel)
	tools.mu.Unlock()
	// Before the original registered reader hold exists, this preparation
	// owns only bounded cancellation/admission, never job FD copies.
	if f.Check(life) != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	opened, e := f.OpenReader(life, 0, f.info.Size)
	if e != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	j.reader, ok = opened.(*familyLocalReader)
	if !ok {
		_ = opened.Close()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	j.reader.mu.Lock()
	if j.reader.closed || j.reader.ctx.Err() != nil {
		j.reader.mu.Unlock()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	j.reader.reads.Add(1)
	j.readerHeld = true
	j.reader.mu.Unlock()
	j.stopReader = familyLocalAfterFunc(j.reader.ctx, cancel)
	// Every job FD is now covered by the actual source retirement barrier.
	// Recheck tools after the potentially blocking original PG acquisition.
	tools.mu.Lock()
	if tools.closed || tools.ctx.Err() != nil || life.Err() != nil || !familyProbeExecutableCurrent(tools.helper, tools.config.HelperPath, tools.helperInfo) || !familyProbeExecutableCurrent(tools.probe, tools.config.FFprobePath, tools.probeInfo) {
		tools.mu.Unlock()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	j.helper, err = duplicateFamilyProbeFile(tools.helper)
	if err == nil {
		j.probe, err = duplicateFamilyProbeFile(tools.probe)
	}
	tools.mu.Unlock()
	if err != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	j.format, err = detectFamilyLocalProbeFormat(familyLocalProbeReadAt{ctx: life, reader: j.reader, file: f}, f.info.Size)
	if err != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	j.readyRead, j.readyWrite, err = os.Pipe()
	if err == nil {
		j.ackRead, j.ackWrite, err = os.Pipe()
	}
	if err == nil {
		j.zero, err = os.Open(os.DevNull)
	}
	if err != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	args := []string{"--probe", j.format}
	if verify {
		args = []string{"--verify"}
	}
	j.cmd = exec.CommandContext(life, "/proc/self/fd/4", args...)
	j.cmd.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	j.cmd.Stdin = j.zero
	j.cmd.ExtraFiles = []*os.File{j.reader.file, j.helper, j.probe, j.readyWrite, j.ackRead}
	j.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	j.cmd.WaitDelay = 2 * time.Second
	j.cmd.Cancel = func() error {
		if j.cmd.Process == nil {
			return os.ErrProcessDone
		}
		return syscall.Kill(-j.cmd.Process.Pid, syscall.SIGKILL)
	}
	j.stdout = &familyLocalProbeBuffer{maximum: familyLocalProbeOutputLimit, cancel: cancel}
	j.stderr = &familyLocalProbeBuffer{maximum: familyLocalProbeErrorLimit, cancel: cancel}
	j.cmd.Stdout = j.stdout
	j.cmd.Stderr = j.stderr
	j.watchDone = make(chan struct{})
	go func() {
		defer close(j.watchDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-life.Done():
				return
			case <-ticker.C:
				if f.Check(life) != nil {
					cancel()
					return
				}
			}
		}
	}()
	if f.Check(life) != nil || !j.toolsCurrent() {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	// Keep the original registered parent FD stable until the child has
	// inherited it. Cancellation cannot close/reuse its numeric FD mid-Start.
	j.reader.mu.Lock()
	if j.reader.closed || life.Err() != nil {
		j.reader.mu.Unlock()
		return nil, ErrFamilyLocalProbeUnavailable
	}
	err = j.cmd.Start()
	j.started = err == nil
	j.reader.mu.Unlock()
	if err != nil {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	_ = j.readyWrite.Close()
	_ = j.ackRead.Close()
	j.readyDone = make(chan struct{})
	ready := make(chan string, 1)
	go func() {
		defer close(j.readyDone)
		line, _ := bufio.NewReader(io.LimitReader(j.readyRead, 128)).ReadString('\n')
		ready <- line
	}()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case line := <-ready:
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != familyLocalProbeABI {
			return nil, ErrFamilyLocalProbeUnavailable
		}
		pid, e := strconv.Atoi(fields[1])
		if e != nil || pid != j.cmd.Process.Pid {
			return nil, ErrFamilyLocalProbeUnavailable
		}
	case <-life.Done():
		return nil, ErrFamilyLocalProbeUnavailable
	case <-timer.C:
		return nil, ErrFamilyLocalProbeUnavailable
	}
	if life.Err() != nil || f.Check(life) != nil || !j.toolsCurrent() {
		return nil, ErrFamilyLocalProbeUnavailable
	}
	return j, nil
}
func (j *familyLocalProbeJob) toolsCurrent() bool {
	return j.valid() && familyProbeExecutableCurrent(j.helper, j.tools.config.HelperPath, j.tools.helperInfo) && familyProbeExecutableCurrent(j.probe, j.tools.config.FFprobePath, j.tools.probeInfo)
}
func (j *familyLocalProbeJob) ack() error {
	if !j.valid() || !j.started || j.acked || j.token.ctx.Err() != nil || j.file.Check(j.token.ctx) != nil || !j.toolsCurrent() {
		return ErrFamilyLocalProbeUnavailable
	}
	if _, err := j.ackWrite.Write([]byte{'S'}); err != nil {
		return ErrFamilyLocalProbeUnavailable
	}
	_ = j.ackWrite.Close()
	j.acked = true
	return nil
}
func (j *familyLocalProbeJob) wait() error {
	if !j.valid() || !j.started || j.waited {
		return ErrFamilyLocalProbeUnavailable
	}
	err := j.cmd.Wait()
	j.waited = true
	return err
}
func (j *familyLocalProbeJob) finish() (FamilyLocalProbeResult, error) {
	if j.ack() != nil || j.wait() != nil || j.token.ctx.Err() != nil {
		return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
	}
	return j.result()
}

// Publication is separate from waiting so every result, including one whose
// bytes arrived before revocation, passes the current original-source check.
func (j *familyLocalProbeJob) result() (FamilyLocalProbeResult, error) {
	if !j.valid() || !j.waited || !j.acked || j.token.ctx.Err() != nil {
		return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
	}
	raw, failed := j.stdout.snapshot()
	defer clear(raw)
	if failed {
		return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
	}
	out, err := reduceFamilyLocalProbe(raw, j.format)
	if err != nil || j.token.ctx.Err() != nil || j.file.Check(j.token.ctx) != nil || !j.toolsCurrent() {
		return FamilyLocalProbeResult{}, ErrFamilyLocalProbeUnavailable
	}
	return out, nil
}
func (j *familyLocalProbeJob) close() {
	if !j.valid() {
		return
	}
	j.once.Do(func() {
		j.token.cancel()
		if j.started && !j.waited {
			_ = syscall.Kill(-j.cmd.Process.Pid, syscall.SIGKILL)
		}
		for _, f := range []*os.File{j.ackWrite, j.readyRead, j.ackRead, j.readyWrite} {
			if f != nil {
				_ = f.Close()
			}
		}
		if j.started && !j.waited {
			_ = j.cmd.Wait()
			j.waited = true
		}
		if j.readyDone != nil {
			<-j.readyDone
		}
		if j.watchDone != nil {
			<-j.watchDone
		}
		if j.stopReader != nil {
			j.stopReader()
		}
		if j.stopTools != nil {
			j.stopTools()
		}
		if j.stopSource != nil {
			j.stopSource()
		}
		for _, f := range []*os.File{j.zero, j.helper, j.probe} {
			if f != nil {
				_ = f.Close()
			}
		}
		if j.stdout != nil {
			j.stdout.clear()
		}
		if j.stderr != nil {
			j.stderr.clear()
		}
		// Every process, pump, watcher, callback and job-owned FD is now
		// retired. Only now may original source.Close finish its reader wait.
		if j.readerHeld {
			j.reader.reads.Done()
		}
		if j.reader != nil {
			_ = j.reader.Close()
		}
		j.tools.mu.Lock()
		delete(j.tools.jobs, j.token)
		j.tools.mu.Unlock()
		familyLocalProbeAdmission.Lock()
		delete(familyLocalProbeAdmission.files, j.file)
		familyLocalProbeAdmission.Unlock()
		close(j.token.done)
	})
}
