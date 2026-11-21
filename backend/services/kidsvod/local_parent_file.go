package kidsvod

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"novastream/internal/datastore"
)

var ErrParentFileChanged = errors.New("parent-approved local file changed or unavailable")

const parentFileCheckBudget = 5 * time.Second
const parentFilePreparationLifetime = 30 * time.Second
const maxParentFileAccountPreparations = 8

// BindParentFileRuntime binds lifetime only, never file/profile authority.
// Managed composition calls it once before any actual parent source admission.
func (s *Service) BindParentFileRuntime(original context.Context) error {
	if s == nil || original == nil || original.Err() != nil {
		return ErrParentFileChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.parentFilesClosed || s.parentFileRoot != nil || len(s.filePreparing)+len(s.fileGrants)+len(s.fileReaders)+len(s.fileProcesses) != 0 {
		return ErrParentFileChanged
	}
	s.parentFileRoot = original
	return nil
}
func (s *Service) parentFilesAvailableLocked() bool {
	return !s.parentFilesClosed && (s.parentFileRoot == nil || s.parentFileRoot.Err() == nil)
}

// SignalParentFilesClose gates every parent admission before signalling ALL
// original actors. No callback or signal below waits for physical retirement.
func (s *Service) SignalParentFilesClose() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.parentFilesClosed = true
	pending := make([]*ParentFileIntent, 0, len(s.filePreparing))
	for i := range s.filePreparing {
		pending = append(pending, i)
	}
	entries := make([]*parentFileApproval, 0, len(s.fileGrants))
	for _, a := range s.fileGrants {
		entries = append(entries, a)
	}
	readers := make([]*ParentFileReader, 0, len(s.fileReaders))
	for r := range s.fileReaders {
		readers = append(readers, r)
	}
	processes := make([]*ParentFileProcessLease, 0, len(s.fileProcesses))
	for l := range s.fileProcesses {
		processes = append(processes, l)
	}
	s.mu.Unlock()
	for _, i := range pending {
		i.mu.Lock()
		cancel := i.preparationCancel
		i.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
	for _, a := range entries {
		a.cancel()
	}
	for _, r := range readers {
		r.cancel()
	}
	for _, l := range processes {
		l.Cancel()
	}
	for _, l := range processes {
		l.mu.Lock()
		cancel := l.outputCancel
		l.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
}

// CloseParentFiles is external to admitted HTTP/Check/worker stacks. Existing
// source/ref capacity is refunded only by each actual original Close operation.
func (s *Service) CloseParentFiles() {
	if s == nil {
		return
	}
	s.parentFilesClose.Do(func() {
		s.SignalParentFilesClose()
		s.mu.Lock()
		s.parentFilesDone = make(chan struct{})
		pending := make([]*ParentFileIntent, 0, len(s.filePreparing))
		for i := range s.filePreparing {
			pending = append(pending, i)
		}
		entries := make([]*parentFileApproval, 0, len(s.fileGrants))
		for _, a := range s.fileGrants {
			entries = append(entries, a)
		}
		readers := make([]*ParentFileReader, 0, len(s.fileReaders))
		for r := range s.fileReaders {
			readers = append(readers, r)
		}
		processes := make([]*ParentFileProcessLease, 0, len(s.fileProcesses))
		for l := range s.fileProcesses {
			processes = append(processes, l)
		}
		s.mu.Unlock()
		for _, i := range pending {
			i.Close()
		}
		for _, a := range entries {
			a.close()
		}
		for _, r := range readers {
			_ = r.Close()
		}
		for _, l := range processes {
			_ = l.Close()
		}
		close(s.parentFilesDone)
	})
	// sync.Once joins concurrent callers; no service mutex spans retirement.
}

type FileReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

// ParentFileSource is configured server code retaining an actual opened local
// regular file and current SQL ACL. It must never open a provider or probe.
// Fingerprint and Check identify the original FD/root/row/ACL, not a title.
type ParentFileSource interface {
	Fingerprint() string
	ItemID() string
	Size() int64
	Name() string
	Check(context.Context) error
	OpenReader(context.Context) (FileReadAtCloser, error)
	Close() error
}

type ParentFileIntent struct {
	mu                                    sync.Mutex
	service                               *Service
	actor                                 actor
	profile, item, authority, fingerprint string
	epoch                                 uint64
	expires                               time.Time
	source                                ParentFileSource
	permit                                *datastore.ParentLocalFilePermit
	transferred, closed                   bool
	preparationTimer                      Timer
	preparationExpires                    time.Time
	preparationCancel                     context.CancelFunc
	preparationReady                      chan struct{}
	closeOnce                             sync.Once
}

func (*ParentFileIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentFileIntent) UnmarshalJSON([]byte) error   { return ErrOpaqueCapability }
func (*ParentFileIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentFileIntent(opaque)"))
}
func (i *ParentFileIntent) Fingerprint() string {
	if i == nil {
		return ""
	}
	return i.fingerprint
}
func (i *ParentFileIntent) Close() {
	if i == nil {
		return
	}
	i.closeOnce.Do(func() {
		i.mu.Lock()
		if i.transferred {
			i.mu.Unlock()
			return
		}
		i.closed = true
		cancel, timer, ready := i.preparationCancel, i.preparationTimer, i.preparationReady
		i.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if timer != nil {
			timer.Stop()
		}
		if ready != nil {
			<-ready
		}
		i.mu.Lock()
		source := i.source
		i.mu.Unlock()
		if source != nil {
			_ = source.Close()
		}
		// Refund only after the opener and the actual held root/FD have retired.
		if i.service != nil {
			i.service.mu.Lock()
			delete(i.service.filePreparing, i)
			i.service.mu.Unlock()
		}
	})
}

type parentFileApproval struct {
	service *Service
	intent  *ParentFileIntent
	id      string
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
	timer   Timer
}

func (a *parentFileApproval) close() {
	a.once.Do(func() {
		a.cancel()
		if a.timer != nil {
			a.timer.Stop()
		}
		if a.service != nil {
			a.service.closeParentFileReaders(a)
			a.service.closeParentFileProcesses(a)
		}
		_ = a.intent.source.Close()
		if a.service != nil {
			a.service.mu.Lock()
			if a.service.fileGrants[a.id] == a {
				delete(a.service.fileGrants, a.id)
			}
			a.service.mu.Unlock()
		}
	})
}

// fileAuthority deliberately has no catalogue/classification input. A parent
// can approve this actual file without asserting an FSK or episode identity.
func (s *Service) fileAuthority(ctx context.Context, a actor, profile string) (Subject, string, time.Time, uint64, error) {
	if s == nil || s.deps.Clock == nil || s.deps.Sessions == nil || s.deps.Profiles == nil || s.deps.Policies == nil || ctx == nil || ctx.Err() != nil || !token(profile, 256) {
		return Subject{}, "", time.Time{}, 0, ErrParentFileChanged
	}
	s.mu.Lock()
	if !s.parentFilesAvailableLocked() {
		s.mu.Unlock()
		return Subject{}, "", time.Time{}, 0, ErrParentFileChanged
	}
	epoch := s.profileEpochLocked(profile)
	s.mu.Unlock()
	session, err := s.deps.Sessions.CurrentSession(ctx, a.token)
	if err != nil || session.Token != a.token || session.AccountID != a.account || session.Scope != "" || session.ScopeResource != "" || session.IsMaster != a.master || session.ClientID != "" && session.ClientID != a.client || !session.ExpiresAt.After(s.deps.Clock.Now()) {
		return Subject{}, "", time.Time{}, 0, ErrParentFileChanged
	}
	p, err := s.deps.Profiles.CurrentProfile(ctx, profile)
	if err != nil || p.ID != profile || !p.IsKids || !token(p.Revision, 256) || !token(p.AccountID, 256) || p.AccountID != a.account && !a.master {
		return Subject{}, "", time.Time{}, 0, ErrParentFileChanged
	}
	policy, err := s.deps.Policies.CurrentPolicy(ctx, profile)
	if err != nil || !policy.valid() || !policy.Enabled {
		return Subject{}, "", time.Time{}, 0, ErrParentFileChanged
	}
	// Hash before another dependency can mutate a returned pointer.
	raw, _ := json.Marshal([]any{session, p, policy, epoch})
	sum := sha256.Sum256(raw)
	s.mu.Lock()
	unchanged := s.parentFilesAvailableLocked() && s.epochs[profile] == epoch
	s.mu.Unlock()
	if !unchanged || ctx.Err() != nil || !session.ExpiresAt.After(s.deps.Clock.Now()) {
		return Subject{}, "", time.Time{}, 0, ErrParentFileChanged
	}
	return Subject{account: p.AccountID, profile: profile, client: a.client}, hex.EncodeToString(sum[:]), session.ExpiresAt, epoch, nil
}

// PrepareParentFileRequest performs current identity/policy checks before the
// configured metadata-only FD opener. The caller also enforces current PG PIN.
func (s *Service) PrepareParentFileRequest(r *http.Request, profile, item string, open func(context.Context, Subject, string) (ParentFileSource, error)) (*ParentFileIntent, error) {
	a, _ := s.requestActor(r)
	if a.token == "" || !token(item, 128) || open == nil {
		return nil, ErrParentFileChanged
	}
	prepCtx, cancel := context.WithTimeout(r.Context(), parentFilePreparationLifetime)
	defer cancel()
	subject, authority, expiry, epoch, err := s.fileAuthority(prepCtx, a, profile)
	if err != nil {
		return nil, err
	}
	if max := s.deps.Clock.Now().Add(MaxGrantTTL); expiry.After(max) {
		expiry = max
	}
	lifetime := parentFilePreparationLifetime
	if deadline, ok := prepCtx.Deadline(); ok && time.Until(deadline) < lifetime {
		lifetime = time.Until(deadline)
	}
	if lifetime <= 0 {
		return nil, ErrParentFileChanged
	}
	i := &ParentFileIntent{service: s, actor: a, profile: profile, item: item, authority: authority, epoch: epoch, expires: expiry, preparationCancel: cancel, preparationReady: make(chan struct{})}
	// Reserve a transient held-root/FD slot before the configured opener. These
	// slots share the canonical grant capacity; the existing PG prompt registry
	// independently retains its shared eight/account confirmation budget.
	s.mu.Lock()
	s.pruneLocked()
	accountCount := 0
	for pending := range s.filePreparing {
		if pending.actor.account == a.account {
			accountCount++
		}
	}
	if !s.parentFilesAvailableLocked() || len(s.grants)+len(s.fileGrants)+len(s.filePreparing) >= MaxGrants || accountCount >= maxParentFileAccountPreparations || prepCtx.Err() != nil {
		s.mu.Unlock()
		return nil, ErrParentFileChanged
	}
	s.filePreparing[i] = struct{}{}
	s.mu.Unlock()
	keep := false
	defer func() {
		if !keep {
			i.Close()
		}
	}()
	defer close(i.preparationReady)
	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		return nil, ErrParentFileChanged
	}
	i.preparationExpires = s.deps.Clock.Now().Add(lifetime)
	i.preparationTimer = s.deps.Clock.AfterFunc(lifetime, func() { i.Close() })
	i.mu.Unlock()
	source, err := open(prepCtx, subject, item)
	i.mu.Lock()
	closed := i.closed
	if !closed {
		i.source = source
	}
	i.mu.Unlock()
	if closed {
		if source != nil {
			_ = source.Close()
		}
		return nil, ErrParentFileChanged
	}
	if err != nil {
		return nil, err
	}
	if source == nil || source.ItemID() != item || source.Size() <= 0 || len(source.Fingerprint()) != 64 || source.Check(prepCtx) != nil {
		return nil, ErrParentFileChanged
	}
	_, after, _, _, err := s.fileAuthority(prepCtx, a, profile)
	if err != nil || after != authority || source.Check(prepCtx) != nil {
		return nil, ErrParentFileChanged
	}
	raw, _ := json.Marshal([]any{authority, source.Fingerprint(), item, expiry})
	sum := sha256.Sum256(raw)
	i.mu.Lock()
	i.fingerprint = hex.EncodeToString(sum[:])
	closed = i.closed
	i.mu.Unlock()
	if closed || s.checkFile(prepCtx, i) != nil {
		return nil, ErrParentFileChanged
	}
	keep = true
	return i, nil
}

func (s *Service) checkFile(ctx context.Context, i *ParentFileIntent) error {
	if s == nil || s.seal == nil || i == nil || i.service != s || i.source == nil || ctx == nil || ctx.Err() != nil || !i.expires.After(s.deps.Clock.Now()) {
		return ErrParentFileChanged
	}
	i.mu.Lock()
	closed := i.closed
	i.mu.Unlock()
	if closed {
		return ErrParentFileChanged
	}
	ctx, cancel := context.WithTimeout(ctx, parentFileCheckBudget)
	defer cancel()
	if i.permit != nil && i.permit.Check(ctx) != nil {
		return ErrParentFileChanged
	}
	_, authority, _, epoch, err := s.fileAuthority(ctx, i.actor, i.profile)
	if err != nil || authority != i.authority || epoch != i.epoch || i.source.Check(ctx) != nil {
		return ErrParentFileChanged
	}
	// Repeat original authority after blocking filesystem/SQL source checks.
	_, authority, _, epoch, err = s.fileAuthority(ctx, i.actor, i.profile)
	if err != nil || authority != i.authority || epoch != i.epoch || ctx.Err() != nil || !i.expires.After(s.deps.Clock.Now()) || i.permit != nil && i.permit.Check(ctx) != nil {
		return ErrParentFileChanged
	}
	if ctx.Err() != nil || !i.expires.After(s.deps.Clock.Now()) {
		return ErrParentFileChanged
	}
	i.mu.Lock()
	closed = i.closed
	i.mu.Unlock()
	if closed {
		return ErrParentFileChanged
	}
	return nil
}

// ApproveParentFile requires the actual one-shot PG password-purpose permit.
// No parent gate boolean, generic adult receipt or imported grant is accepted.
func (s *Service) ApproveParentFile(r *http.Request, i *ParentFileIntent, permit *datastore.ParentLocalFilePermit) (string, time.Time, error) {
	if r == nil || i == nil || i.service != s {
		return "", time.Time{}, ErrParentFileChanged
	}
	a, _ := s.requestActor(r)
	if a != i.actor || s.checkFile(r.Context(), i) != nil {
		return "", time.Time{}, ErrParentFileChanged
	}
	if permit.Consume(r.Context(), i.profile, i.fingerprint) != nil || permit.Check(r.Context()) != nil || s.checkFile(r.Context(), i) != nil {
		return "", time.Time{}, ErrParentFileChanged
	}
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", time.Time{}, err
	}
	id := "lfa_" + hex.EncodeToString(raw[:])
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed || i.transferred {
		return "", time.Time{}, ErrParentFileChanged
	}
	i.permit = permit
	if expiry := permit.ExpiresAt(); expiry.Before(i.expires) {
		i.expires = expiry
	}
	s.mu.Lock()
	s.pruneLocked()
	if s.fileGrants == nil {
		s.fileGrants = map[string]*parentFileApproval{}
	}
	if !s.parentFilesAvailableLocked() || s.epochs[i.profile] != i.epoch || len(s.grants)+len(s.fileGrants)+len(s.filePreparing) > MaxGrants || !i.expires.After(s.deps.Clock.Now()) || r.Context().Err() != nil {
		s.mu.Unlock()
		return "", time.Time{}, ErrParentFileChanged
	}
	if _, reserved := s.filePreparing[i]; !reserved {
		s.mu.Unlock()
		return "", time.Time{}, ErrParentFileChanged
	}
	delete(s.filePreparing, i)
	if i.preparationTimer != nil {
		i.preparationTimer.Stop()
	}
	root := s.parentFileRoot
	if root == nil {
		root = context.Background()
	} // Isolated legacy construction only.
	ctx, cancel := context.WithDeadline(root, i.expires)
	entry := &parentFileApproval{service: s, intent: i, id: id, ctx: ctx, cancel: cancel}
	s.fileGrants[id] = entry
	i.transferred = true
	// Publish timer under the same lock so expiry cannot race its assignment.
	entry.timer = s.deps.Clock.AfterFunc(i.expires.Sub(s.deps.Clock.Now()), func() { s.retireParentFile(entry) })
	s.mu.Unlock()
	return id, i.expires, nil
}
func (s *Service) retireParentFile(entry *parentFileApproval) {
	// Keep capacity charged while canceled FD owners are still closing.
	entry.close()
}

// CancelParentFile retires only the exact typed server intent, including an
// approval whose response could not be published after its request canceled.
func (s *Service) CancelParentFile(i *ParentFileIntent) {
	if s == nil || i == nil || i.service != s {
		return
	}
	s.mu.Lock()
	var entries []*parentFileApproval
	for _, entry := range s.fileGrants {
		if entry.intent == i {
			entries = append(entries, entry)
		}
	}
	s.mu.Unlock()
	for _, entry := range entries {
		entry.close()
	}
	i.Close()
}

type ParentFileReader struct {
	mu                     sync.Mutex
	ready                  chan struct{}
	service                *Service
	entry                  *parentFileApproval
	reader                 FileReadAtCloser
	ctx                    context.Context
	cancel                 context.CancelFunc
	once                   sync.Once
	stopRequest            func() bool
	stopGrant              func() bool
	requestDone, grantDone chan struct{}
}

func (*ParentFileReader) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentFileReader) UnmarshalJSON([]byte) error   { return ErrOpaqueCapability }
func (*ParentFileReader) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentFileReader(opaque)"))
}

func (l *ParentFileReader) Context() context.Context {
	if l == nil || l.ctx == nil {
		return closedContext
	}
	return l.ctx
}
func (l *ParentFileReader) ExpiresAt() time.Time {
	if l == nil || l.entry == nil || l.entry.intent == nil {
		return time.Time{}
	}
	return l.entry.intent.expires
}
func (l *ParentFileReader) Size() int64 {
	if l == nil || l.entry == nil || l.entry.intent == nil || l.entry.intent.source == nil {
		return 0
	}
	return l.entry.intent.source.Size()
}
func (l *ParentFileReader) Name() string {
	if l == nil || l.entry == nil || l.entry.intent == nil || l.entry.intent.source == nil {
		return ""
	}
	return l.entry.intent.source.Name()
}
func (l *ParentFileReader) CancelAccess() {
	if l != nil && l.service != nil && l.entry != nil {
		l.service.retireParentFile(l.entry)
	}
}
func (l *ParentFileReader) Check(ctx context.Context) error {
	if l == nil || l.service == nil || l.entry == nil || l.entry.ctx == nil || l.ctx == nil || l.reader == nil || l.ctx.Err() != nil || ctx == nil || ctx.Err() != nil || l.entry.ctx.Err() != nil {
		return ErrParentFileChanged
	}
	ctx, cancel := context.WithTimeout(ctx, parentFileCheckBudget)
	done := make(chan struct{})
	stop := context.AfterFunc(l.ctx, func() { defer close(done); cancel() })
	defer func() {
		cancel()
		if !stop() {
			<-done
		}
	}()
	if err := l.service.checkFile(ctx, l.entry.intent); err != nil {
		if l.entry.ctx.Err() == nil {
			l.service.retireParentFile(l.entry)
		}
		return err
	}
	if l.ctx.Err() != nil || l.entry.ctx.Err() != nil || ctx.Err() != nil {
		return ErrParentFileChanged
	}
	return nil
}
func (l *ParentFileReader) ReadAt(p []byte, off int64) (int, error) {
	if err := l.Check(l.ctx); err != nil {
		clear(p)
		return 0, err
	}
	n, err := l.reader.ReadAt(p, off)
	if after := l.Check(l.ctx); after != nil {
		clear(p)
		return 0, after
	}
	return n, err
}

// cancelIO never recursively joins its own callback. The cleanup owner below
// joins callbacks and closes the actual duplicate after acquisition completes.
func (l *ParentFileReader) cancelIO(done chan struct{}) {
	defer close(done)
	l.cancel()
	l.mu.Lock()
	reader := l.reader
	l.mu.Unlock()
	if reader != nil {
		_ = reader.Close()
	}
}
func (l *ParentFileReader) Close() error {
	if l == nil || l.service == nil || l.cancel == nil || l.ready == nil {
		return nil
	}
	var err error
	l.once.Do(func() {
		l.cancel()
		<-l.ready
		l.mu.Lock()
		reader := l.reader
		l.mu.Unlock()
		if reader != nil {
			err = reader.Close()
		}
		if !l.stopRequest() {
			<-l.requestDone
		}
		if !l.stopGrant() {
			<-l.grantDone
		}
		l.service.mu.Lock()
		delete(l.service.fileReaders, l)
		l.service.mu.Unlock()
	})
	return err
}
func (s *Service) closeParentFileReaders(entry *parentFileApproval) {
	s.mu.Lock()
	var readers []*ParentFileReader
	for reader, owner := range s.fileReaders {
		if owner == entry {
			readers = append(readers, reader)
		}
	}
	s.mu.Unlock()
	// Cancel every owner before joining any acquisition/FD cleanup. No service
	// lock or wait for a caller's synchronous Check stack is held here.
	for _, reader := range readers {
		reader.cancel()
	}
	for _, reader := range readers {
		_ = reader.Close()
	}
}

// OpenParentFileRequest authenticates each seek/HEAD independently. The ID is
// only a locator; the original full session/client/profile must still match.
func (s *Service) OpenParentFileRequest(r *http.Request, profile, item, id string) (*ParentFileReader, error) {
	if s == nil || r == nil || !token(id, 128) {
		return nil, ErrParentFileChanged
	}
	a, _ := s.requestActor(r)
	s.mu.Lock()
	entry := s.fileGrants[id]
	s.mu.Unlock()
	if entry == nil || entry.intent.permit == nil || a != entry.intent.actor || profile != entry.intent.profile || item != entry.intent.item || entry.ctx.Err() != nil {
		return nil, ErrParentFileChanged
	}
	if err := s.checkFile(r.Context(), entry.intent); err != nil {
		s.retireParentFile(entry)
		return nil, err
	}
	return s.acquireParentFileReader(r, entry)
}

// acquireParentFileReader owns admission and IO only, after the public method
// has checked the actual original PG file-purpose permit.
func (s *Service) acquireParentFileReader(r *http.Request, entry *parentFileApproval) (*ParentFileReader, error) {
	ctx, cancel := context.WithCancel(r.Context())
	l := &ParentFileReader{service: s, entry: entry, ctx: ctx, cancel: cancel, ready: make(chan struct{}), requestDone: make(chan struct{}), grantDone: make(chan struct{})}
	s.mu.Lock()
	s.pruneLocked()
	if !s.parentFilesAvailableLocked() || s.fileGrants[entry.id] != entry || entry.ctx.Err() != nil || len(s.leases)+len(s.fileReaders)+len(s.fileProcesses)+s.fileOutputReaders >= MaxLeases || r.Context().Err() != nil {
		s.mu.Unlock()
		cancel()
		return nil, ErrParentFileChanged
	}
	s.fileReaders[l] = entry
	s.mu.Unlock()
	l.stopRequest = context.AfterFunc(r.Context(), func() { l.cancelIO(l.requestDone) })
	l.stopGrant = context.AfterFunc(entry.ctx, func() { l.cancelIO(l.grantDone) })
	openCtx, openCancel := context.WithTimeout(ctx, parentFileCheckBudget)
	reader, err := entry.intent.source.OpenReader(openCtx)
	openCancel()
	l.mu.Lock()
	l.reader = reader
	l.mu.Unlock()
	close(l.ready)
	if err != nil || reader == nil || l.Check(ctx) != nil {
		_ = l.Close()
		if entry.ctx.Err() == nil {
			s.retireParentFile(entry)
		}
		return nil, ErrParentFileChanged
	}

	return l, nil
}

// RevokeParentFileRequest only retires an original same-session file approval.
// Creating or extending an exception still requires the separate fresh proof.
func (s *Service) RevokeParentFileRequest(r *http.Request, profile, item, id string) error {
	l, err := s.OpenParentFileRequest(r, profile, item, id)
	if err != nil {
		return err
	}
	entry := l.entry
	_ = l.Close()
	s.retireParentFile(entry)
	return nil
}
