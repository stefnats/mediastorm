package kidsvod

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const ParentFileProcessInputLimit int64 = 8 << 30
const parentFileProcessOutputBudget int64 = 16 << 30

var ErrParentFileProcessBusy = errors.New("parent file process capacity unavailable")

// Only a source retaining the actual original file may attest its duplicate.
type ParentFileDescriptorSource interface {
	ParentFileSource
	CheckDescriptor(context.Context, *os.File) error
}

// Admission includes preparations, completed private sessions and owners still
// closing. These are reservations, not physical filesystem space allocations.
var parentFileProcessAdmission = struct {
	sync.Mutex
	accounts map[string]bool
	entries  map[*parentFileApproval]bool
	bytes    int64
}{accounts: map[string]bool{}, entries: map[*parentFileApproval]bool{}}

type ParentFileProcessLease struct {
	mu                sync.Mutex
	self              *ParentFileProcessLease
	service           *Service
	entry             *parentFileApproval
	ctx               context.Context
	cancel            context.CancelFunc
	file              *os.File
	reservation       int64
	ready             chan struct{}
	done              chan struct{}
	once              sync.Once
	runDone           chan struct{}
	setupStop         func() bool
	grantStop         func()
	setupDone         chan struct{}
	handed            bool
	closed            bool
	outputCancel      context.CancelFunc
	outputDone        <-chan struct{}
	retainReservation bool
	releaseOnce       sync.Once
}

func parentProcessAfterFunc(ctx context.Context, callback func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); callback() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if stop() {
				close(done)
			}
		})
		<-done
	}
}

func (*ParentFileProcessLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentFileProcessLease) UnmarshalJSON([]byte) error   { return ErrOpaqueCapability }
func (*ParentFileProcessLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentFileProcessLease(opaque)"))
}
func (l *ParentFileProcessLease) Context() context.Context {
	if l == nil || l.self != l {
		return closedContext
	}
	return l.ctx
}
func (l *ParentFileProcessLease) ExpiresAt() time.Time {
	if l == nil || l.self != l {
		return time.Time{}
	}
	return l.entry.intent.expires
}
func (l *ParentFileProcessLease) Size() int64 {
	if l == nil || l.self != l {
		return 0
	}
	return l.entry.intent.source.Size()
}
func (l *ParentFileProcessLease) OutputLimit() int64 {
	if l == nil || l.self != l {
		return 0
	}
	return l.reservation
}

// PrepareParentFileProcessRequest opens only a duplicate of the already
// approved actual FD. It never resolves a title, path, provider or classification.
func (s *Service) PrepareParentFileProcessRequest(r *http.Request, profile, item, id string) (lease *ParentFileProcessLease, err error) {
	if runtime.GOOS != "linux" || s == nil || r == nil || !token(id, 128) {
		return nil, ErrParentFileChanged
	}
	a, _ := s.requestActor(r)
	s.mu.Lock()
	entry := s.fileGrants[id]
	s.mu.Unlock()
	if entry == nil || entry.intent.permit == nil || a != entry.intent.actor || profile != entry.intent.profile || item != entry.intent.item || entry.ctx.Err() != nil || s.checkFile(r.Context(), entry.intent) != nil {
		return nil, ErrParentFileChanged
	}
	size := entry.intent.source.Size()
	if size <= 0 || size > ParentFileProcessInputLimit {
		return nil, ErrParentFileChanged
	}
	overhead := size / 20
	if overhead < 64<<20 {
		overhead = 64 << 20
	}
	reservation := size + overhead
	life, cancel := context.WithDeadline(context.Background(), entry.intent.expires)
	l := &ParentFileProcessLease{service: s, entry: entry, ctx: life, cancel: cancel, reservation: reservation, ready: make(chan struct{}), done: make(chan struct{}), setupDone: make(chan struct{})}
	l.self = l
	s.mu.Lock()
	if s.fileProcesses == nil {
		s.fileProcesses = map[*ParentFileProcessLease]*parentFileApproval{}
	}
	if s.fileGrants[id] != entry || entry.ctx.Err() != nil || len(s.leases)+len(s.fileReaders)+len(s.fileProcesses)+s.fileOutputReaders >= MaxLeases || r.Context().Err() != nil {
		s.mu.Unlock()
		cancel()
		return nil, ErrParentFileProcessBusy
	}
	parentFileProcessAdmission.Lock()
	account := entry.intent.actor.account
	if len(parentFileProcessAdmission.entries) >= 2 || parentFileProcessAdmission.accounts[account] || parentFileProcessAdmission.entries[entry] || reservation > parentFileProcessOutputBudget-parentFileProcessAdmission.bytes {
		parentFileProcessAdmission.Unlock()
		s.mu.Unlock()
		cancel()
		return nil, ErrParentFileProcessBusy
	}
	parentFileProcessAdmission.accounts[account] = true
	parentFileProcessAdmission.entries[entry] = true
	parentFileProcessAdmission.bytes += reservation
	s.fileProcesses[l] = entry
	parentFileProcessAdmission.Unlock()
	s.mu.Unlock()
	defer func() {
		close(l.ready)
		if lease == nil {
			_ = l.Close()
		}
	}()
	l.mu.Lock()
	l.setupStop = context.AfterFunc(r.Context(), func() {
		defer close(l.setupDone)
		l.mu.Lock()
		if !l.handed {
			l.cancel()
		}
		l.mu.Unlock()
	})
	l.grantStop = parentProcessAfterFunc(entry.ctx, cancel)
	l.mu.Unlock()
	openCtx, stop := context.WithTimeout(r.Context(), parentFileCheckBudget)
	stopGrantOpen := parentProcessAfterFunc(l.ctx, stop)
	source, e := entry.intent.source.OpenReader(openCtx)
	stop()
	stopGrantOpen()
	if e != nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, ErrParentFileChanged
	}
	file, ok := source.(*os.File)
	if !ok || file == nil {
		if source != nil {
			_ = source.Close()
		}
		return nil, ErrParentFileChanged
	}
	l.mu.Lock()
	l.file = file
	l.mu.Unlock()
	identity, ok := entry.intent.source.(ParentFileDescriptorSource)
	if !ok || identity.CheckDescriptor(r.Context(), file) != nil {
		return nil, ErrParentFileChanged
	}
	if l.Check(r.Context()) != nil {
		return nil, ErrParentFileChanged
	}
	return l, nil
}

// Check only signals denial. It never synchronously retires an approval from
// inside its own command watcher; external retirement owns the completion join.
func (l *ParentFileProcessLease) Check(ctx context.Context) error {
	if l == nil || l.self != l || ctx == nil || ctx.Err() != nil || l.ctx.Err() != nil || l.entry.ctx.Err() != nil {
		return ErrParentFileChanged
	}
	l.mu.Lock()
	closed := l.closed
	file := l.file
	l.mu.Unlock()
	l.service.mu.Lock()
	current := l.service.fileProcesses[l] == l.entry && l.service.fileGrants[l.entry.id] == l.entry
	l.service.mu.Unlock()
	if closed || !current || l.entry.intent.permit == nil || l.service.checkFile(ctx, l.entry.intent) != nil || l.ctx.Err() != nil {
		return ErrParentFileChanged
	}
	identity, ok := l.entry.intent.source.(ParentFileDescriptorSource)
	if !ok || file == nil || identity.CheckDescriptor(ctx, file) != nil {
		return ErrParentFileChanged
	}
	return nil
}
func (l *ParentFileProcessLease) CheckRequest(r *http.Request, profile, item string) error {
	if l == nil || l.self != l || r == nil {
		return ErrParentFileChanged
	}
	a, _ := l.service.requestActor(r)
	if a != l.entry.intent.actor || profile != l.entry.intent.profile || item != l.entry.intent.item {
		return ErrParentFileChanged
	}
	return l.Check(r.Context())
}
func (l *ParentFileProcessLease) Handoff(r *http.Request) error {
	if l == nil || l.self != l || r == nil || l.CheckRequest(r, l.entry.intent.profile, l.entry.intent.item) != nil {
		return ErrParentFileChanged
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.handed || l.setupStop == nil {
		l.cancel()
		return ErrParentFileChanged
	}
	stopped := l.setupStop()
	if stopped {
		l.setupStop = nil
		close(l.setupDone)
	}
	if !stopped || r.Context().Err() != nil || l.ctx.Err() != nil {
		l.cancel()
		return ErrParentFileChanged
	}
	l.handed = true
	return nil
}

// The output adapter refunds this shared slot only AFTER its actual FD closes.
// Its registered output owner is joined by BindOutputRetirement on revocation.
func (l *ParentFileProcessLease) ReserveOutput(r *http.Request, profile, item string) (func(), error) {
	if l.CheckRequest(r, profile, item) != nil {
		return nil, ErrParentFileChanged
	}
	s := l.service
	s.mu.Lock()
	if s.fileProcesses[l] != l.entry || l.ctx.Err() != nil || len(s.leases)+len(s.fileReaders)+len(s.fileProcesses)+s.fileOutputReaders >= MaxLeases {
		s.mu.Unlock()
		return nil, ErrParentFileProcessBusy
	}
	s.fileOutputReaders++
	s.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { s.mu.Lock(); s.fileOutputReaders--; s.mu.Unlock() }) }, nil
}

// The private output owner signals actual worker/output-reader retirement BEFORE
// Close, avoiding self-join. Residual-file deletion failure must retain the
// separate global reservation until successful original-root cleanup.
func (l *ParentFileProcessLease) BindOutputRetirement(cancel context.CancelFunc, done <-chan struct{}) error {
	if l == nil || l.self != l || cancel == nil || done == nil {
		return ErrParentFileChanged
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.outputDone != nil || l.ctx.Err() != nil {
		return ErrParentFileChanged
	}
	l.outputCancel, l.outputDone = cancel, done
	return nil
}

// Run serializes ffprobe and FFmpeg on the SAME retained seekable description.
// Raw range clients use ReadAt, so no independent caller can change its offset.
// Input is inherited as descriptor3; no source pathname/URL leaves this lease.
// Reads by the decoder are watcher-canceled, not PG-checked per decoder read.
func (l *ParentFileProcessLease) Run(ctx context.Context, binary string, args []string, directory *os.File, stdout, stderr io.Writer) error {
	if l == nil || l.self != l || ctx == nil || l.Check(ctx) != nil {
		return ErrParentFileChanged
	}
	command, cancel := context.WithCancel(ctx)
	stop := parentProcessAfterFunc(l.ctx, cancel)
	defer func() { cancel(); stop() }()
	l.mu.Lock()
	if l.closed || l.runDone != nil || l.file == nil || command.Err() != nil {
		l.mu.Unlock()
		return ErrParentFileChanged
	}
	if _, err := l.file.Seek(0, io.SeekStart); err != nil {
		l.mu.Unlock()
		return ErrParentFileChanged
	}
	cmd := exec.CommandContext(command, binary, args...)
	cmd.ExtraFiles = []*os.File{l.file}
	if directory != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, directory)
		// Linux Go changes directory before it remaps ExtraFiles.
		cmd.Dir = "/proc/self/fd/" + strconv.Itoa(int(directory.Fd()))
	}
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C", "TMPDIR=."}
	cmd.WaitDelay = time.Second
	cmd.Stdout, cmd.Stderr = stdout, stderr
	done := make(chan struct{})
	l.runDone = done
	if err := cmd.Start(); err != nil {
		l.runDone = nil
		close(done)
		l.mu.Unlock()
		return err
	}
	l.mu.Unlock()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-command.Done():
				return
			case <-ticker.C:
				if l.Check(command) != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := cmd.Wait()
	cancel()
	<-watchDone
	l.mu.Lock()
	l.runDone = nil
	close(done)
	l.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if l.Check(ctx) != nil {
		return ErrParentFileChanged
	}
	return err
}
func (l *ParentFileProcessLease) Cancel() {
	if l != nil && l.self == l {
		l.cancel()
	}
}
func (l *ParentFileProcessLease) Close() error {
	if l == nil || l.self != l {
		return nil
	}
	var err error
	l.once.Do(func() {
		l.cancel()
		<-l.ready
		l.mu.Lock()
		l.closed = true
		done := l.runDone
		outputCancel, outputDone := l.outputCancel, l.outputDone
		l.mu.Unlock()
		if outputCancel != nil {
			outputCancel()
		}
		if done != nil {
			<-done
		}
		if outputDone != nil {
			<-outputDone
		}
		l.mu.Lock()
		setupStop, grantStop := l.setupStop, l.grantStop
		l.setupStop = nil
		l.grantStop = nil
		if l.file != nil {
			err = l.file.Close()
		}
		l.mu.Unlock()
		if setupStop != nil {
			if setupStop() {
				close(l.setupDone)
			}
			<-l.setupDone
		}
		if grantStop != nil {
			grantStop()
		}
		l.service.mu.Lock()
		delete(l.service.fileProcesses, l)
		l.service.mu.Unlock()
		l.mu.Lock()
		retained := l.retainReservation
		l.mu.Unlock()
		if !retained {
			l.releaseReservation()
		}
		close(l.done)
	})
	return err
}

// Failed private-output deletion keeps global bytes/account/process admission
// charged, while input FD/worker shutdown may still finish without hanging a
// profile revoke. Only the original output owner releases it after actual retry.
func (l *ParentFileProcessLease) RetainOutputReservation() {
	if l == nil || l.self != l {
		return
	}
	l.mu.Lock()
	l.retainReservation = true
	l.mu.Unlock()
}
func (l *ParentFileProcessLease) ReleaseRetainedOutputReservation() {
	if l == nil || l.self != l {
		return
	}
	<-l.done
	l.releaseReservation()
}
func (l *ParentFileProcessLease) releaseReservation() {
	l.releaseOnce.Do(func() {
		parentFileProcessAdmission.Lock()
		delete(parentFileProcessAdmission.accounts, l.entry.intent.actor.account)
		delete(parentFileProcessAdmission.entries, l.entry)
		parentFileProcessAdmission.bytes -= l.reservation
		parentFileProcessAdmission.Unlock()
	})
}
func (s *Service) closeParentFileProcesses(entry *parentFileApproval) {
	s.mu.Lock()
	var leases []*ParentFileProcessLease
	for lease, owner := range s.fileProcesses {
		if owner == entry {
			leases = append(leases, lease)
		}
	}
	s.mu.Unlock()
	for _, lease := range leases {
		lease.Cancel()
	}
	for _, lease := range leases {
		_ = lease.Close()
	}
}
