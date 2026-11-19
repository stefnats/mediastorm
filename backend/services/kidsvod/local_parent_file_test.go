package kidsvod

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pendingFileSource struct {
	closes     atomic.Int32
	check      func(context.Context) error
	opens      atomic.Int32
	openReader func(context.Context) (FileReadAtCloser, error)
}

func (*pendingFileSource) Fingerprint() string { return strings.Repeat("b", 64) }
func (*pendingFileSource) ItemID() string      { return "actual-unmatched-file" }
func (*pendingFileSource) Name() string        { return "Synthetic unmatched file.bin" }
func (*pendingFileSource) Size() int64         { return 81 }
func (f *pendingFileSource) Check(ctx context.Context) error {
	if f.check != nil {
		return f.check(ctx)
	}
	return ctx.Err()
}
func (f *pendingFileSource) OpenReader(ctx context.Context) (FileReadAtCloser, error) {
	f.opens.Add(1)
	if f.openReader != nil {
		return f.openReader(ctx)
	}
	return nil, ErrParentFileChanged
}
func (f *pendingFileSource) Close() error { f.closes.Add(1); return nil }

func TestParentFilePendingUsesActualFileWithoutCatalogueClassificationOrProbe(t *testing.T) {
	s, f := newFixture(t)
	source := &pendingFileSource{}
	calls := 0
	i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(ctx context.Context, subject Subject, item string) (ParentFileSource, error) {
		calls++
		if subject.AccountID() != "household" || subject.ProfileID() != "child" || item != source.ItemID() {
			t.Fatal("opener lacked current original subject")
		}
		return source, nil
	})
	if err != nil || i == nil || calls != 1 || f.cacheCalls.Load() != 0 || f.providerCalls.Load() != 0 || source.opens.Load() != 0 {
		t.Fatal("pending actual file required catalogue, classification or provider", err)
	}
	if _, _, err = s.ApproveParentFile(f.request(true), i, nil); err == nil {
		t.Fatal("ordinary password gate without actual PG file permit approved bytes")
	}
	i.Close()
	i.Close()
	if source.closes.Load() != 1 {
		t.Fatal("pending file retirement leaked or double-closed")
	}
}
func TestParentFilePendingChecksChildPolicyAndSessionBeforeOpening(t *testing.T) {
	for _, change := range []string{"disabled child", "adult", "foreign owner", "expired session", "scoped session", "missing policy"} {
		t.Run(change, func(t *testing.T) {
			s, f := newFixture(t)
			f.mu.Lock()
			switch change {
			case "disabled child":
				f.policy.Enabled = false
			case "adult":
				f.profile.IsKids = false
			case "foreign owner":
				f.profile.AccountID = "other"
			case "expired session":
				f.session.ExpiresAt = f.clock.Now()
			case "scoped session":
				f.session.Scope = "stream"
			case "missing policy":
				f.policy.Revision = ""
			}
			f.mu.Unlock()
			calls := 0
			i, err := s.PrepareParentFileRequest(f.request(false), "child", "actual-unmatched-file", func(context.Context, Subject, string) (ParentFileSource, error) {
				calls++
				return &pendingFileSource{}, nil
			})
			if i != nil || err == nil || calls != 0 {
				t.Fatal("invalid original authority touched a file")
			}
		})
	}
}
func TestParentFilePendingLatePolicyDriftClosesHeldFile(t *testing.T) {
	s, f := newFixture(t)
	source := &pendingFileSource{}
	i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) {
		f.mu.Lock()
		f.policy.Revision = "policy-v2"
		f.mu.Unlock()
		return source, nil
	})
	if err == nil || i != nil || source.closes.Load() != 1 {
		t.Fatal("blocked opener adopted replacement policy or leaked FD")
	}
}
func TestParentFilePendingOriginalExpiryAndProfileEpochDoNotRenew(t *testing.T) {
	for _, change := range []string{"expiry", "profile invalidation", "late source change"} {
		t.Run(change, func(t *testing.T) {
			s, f := newFixture(t)
			source := &pendingFileSource{}
			i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) { return source, nil })
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			expiry := i.expires
			switch change {
			case "expiry":
				f.clock.advance(MaxGrantTTL)
			case "profile invalidation":
				s.InvalidateProfile("child")
			case "late source change":
				source.check = func(context.Context) error { return ErrParentFileChanged }
			}
			if s.checkFile(t.Context(), i) == nil || i.expires != expiry {
				t.Fatal("pending file adopted changed source/epoch or renewed expiry")
			}
		})
	}
}
func TestParentFileCapabilitiesRejectImportAndConcealOriginalAuthority(t *testing.T) {
	s, f := newFixture(t)
	source := &pendingFileSource{}
	i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	data, err := json.Marshal(i)
	if err != nil || string(data) != "null" || strings.Contains(fmt.Sprintf("%+v", i), "synthetic-session") {
		t.Fatal("file intent revealed authority")
	}
	var imported ParentFileIntent
	if json.Unmarshal([]byte(`{}`), &imported) == nil {
		t.Fatal("file intent imported")
	}
	imported.Close()
	var reader ParentFileReader
	if json.Unmarshal([]byte(`{}`), &reader) == nil {
		t.Fatal("file reader imported")
	}
	if s.checkFile(context.Background(), &imported) == nil {
		t.Fatal("zero capability adopted current actor")
	}
}
func TestParentFilePreparationExpiryBoundaryRetiresHeldSource(t *testing.T) {
	s, f := newFixture(t)
	source := &pendingFileSource{}
	i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	f.clock.advance(i.preparationExpires.Sub(f.clock.Now()) - time.Nanosecond)
	if s.checkFile(t.Context(), i) != nil {
		t.Fatal("pending file expired before original preparation boundary")
	}
	f.clock.advance(time.Nanosecond)
	if s.checkFile(t.Context(), i) == nil {
		t.Fatal("exact original preparation expiry allowed")
	}
}

type delayedFileRead struct{ entered, release chan struct{} }

func (r *delayedFileRead) ReadAt(p []byte, _ int64) (int, error) {
	close(r.entered)
	<-r.release
	return copy(p, "late synthetic bytes"), io.EOF
}
func (*delayedFileRead) Close() error { return nil }

// This exercises the production chunk reader with controlled dependency drift,
// independently of the separate actual-PG permit/FD positive API tests. No
// synthetic permit or automatic classification is installed by this harness.
func TestParentFileReaderClearsLateBufferAfterBlockedReadAuthorityChange(t *testing.T) {
	for _, change := range []string{"policy", "source", "request cancellation", "expiry"} {
		t.Run(change, func(t *testing.T) {
			s, f := newFixture(t)
			changed := atomic.Bool{}
			source := &pendingFileSource{check: func(ctx context.Context) error {
				if changed.Load() {
					return ErrParentFileChanged
				}
				return ctx.Err()
			}}
			i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) { return source, nil })
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			grantCtx, cancelGrant := context.WithCancel(t.Context())
			defer cancelGrant()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entry := &parentFileApproval{intent: i, id: "internal-chunk-harness", ctx: grantCtx, cancel: cancelGrant}
			blocked := &delayedFileRead{entered: make(chan struct{}), release: make(chan struct{})}
			reader := &ParentFileReader{service: s, entry: entry, reader: blocked, ctx: ctx, cancel: cancel}
			buffer := make([]byte, 64)
			done := make(chan error, 1)
			go func() { _, err := reader.ReadAt(buffer, 0); done <- err }()
			<-blocked.entered
			switch change {
			case "policy":
				f.mu.Lock()
				f.policy.Revision = "changed-policy"
				f.mu.Unlock()
			case "source":
				changed.Store(true)
			case "request cancellation":
				cancel()
			case "expiry":
				f.clock.advance(MaxGrantTTL)
			}
			close(blocked.release)
			if err := <-done; err == nil {
				t.Fatal("late unchecked chunk accepted")
			}
			for _, b := range buffer {
				if b != 0 {
					t.Fatal("late bytes survived original authority change")
				}
			}
		})
	}
}

func TestParentFilePreparatoryAdmissionPrecedesBlockedOpenerAndSharesGrantCapacity(t *testing.T) {
	for _, normalCount := range []int{0, MaxGrants - 7} {
		t.Run(fmt.Sprint(normalCount), func(t *testing.T) {
			s, f := newFixture(t)
			for n := 0; n < normalCount; n++ {
				s.grants[fmt.Sprint(n)] = &approval{intent: Intent{profile: "another-profile"}, expires: f.clock.Now().Add(time.Hour)}
			}
			limit := maxParentFileAccountPreparations
			if MaxGrants-normalCount < limit {
				limit = MaxGrants - normalCount
			}
			entered := make(chan struct{}, limit)
			release := make(chan struct{})
			var released sync.Once
			defer released.Do(func() { close(release) })
			type result struct {
				i   *ParentFileIntent
				err error
			}
			results := make(chan result, limit)
			open := func(ctx context.Context, _ Subject, _ string) (ParentFileSource, error) {
				entered <- struct{}{}
				select {
				case <-release:
					return &pendingFileSource{}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			for n := 0; n < limit; n++ {
				go func() {
					i, err := s.PrepareParentFileRequest(f.request(false), "child", "actual-unmatched-file", open)
					results <- result{i, err}
				}()
			}
			for n := 0; n < limit; n++ {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("reserved source opener not reached")
				}
			}
			calls := 0
			i, err := s.PrepareParentFileRequest(f.request(false), "child", "actual-unmatched-file", func(context.Context, Subject, string) (ParentFileSource, error) {
				calls++
				return &pendingFileSource{}, nil
			})
			if err == nil || i != nil || calls != 0 {
				t.Fatal("source opened before shared grant/account resource admission")
			}
			released.Do(func() { close(release) })
			var intents []*ParentFileIntent
			for n := 0; n < limit; n++ {
				r := <-results
				if r.err != nil || r.i == nil {
					t.Fatal("reserved preparation failed", r.err)
				}
				intents = append(intents, r.i)
			}
			s.mu.Lock()
			count := len(s.filePreparing)
			s.mu.Unlock()
			if count != limit {
				t.Fatal("completed held prompts dropped resource reservations")
			}
			for _, i := range intents {
				i.Close()
			}
			s.mu.Lock()
			count = len(s.filePreparing)
			s.mu.Unlock()
			if count != 0 {
				t.Fatal("preparatory reservations leaked after actual close")
			}
		})
	}
}
func TestParentFilePreparatoryFailureAndCancellationRefundAfterOpenerRetires(t *testing.T) {
	for _, mode := range []string{"failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			ctx, cancel := context.WithCancel(f.request(false).Context())
			defer cancel()
			entered := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				_, err := s.PrepareParentFileRequest(f.request(false).WithContext(ctx), "child", "actual-unmatched-file", func(ctx context.Context, _ Subject, _ string) (ParentFileSource, error) {
					close(entered)
					if mode == "cancel" {
						<-ctx.Done()
					} else {
						<-release
					}
					return nil, ErrParentFileChanged
				})
				done <- err
			}()
			<-entered
			s.mu.Lock()
			count := len(s.filePreparing)
			s.mu.Unlock()
			if count != 1 {
				t.Fatal("opener has no resource reservation")
			}
			if mode == "cancel" {
				cancel()
			} else {
				close(release)
			}
			if err := <-done; err == nil {
				t.Fatal("failed/canceled opener returned an intent")
			}
			s.mu.Lock()
			count = len(s.filePreparing)
			s.mu.Unlock()
			if count != 0 {
				t.Fatal("failed opener leaked reservation")
			}
		})
	}
}

// The following helper stages an internal IO owner, never a public permission.
// Public OpenParentFileRequest still rejects it without an actual PG permit;
// positive password/FD route tests use real PostgreSQL separately.
func fileIOOwner(t *testing.T, s *Service, f *fixture, source *pendingFileSource) *parentFileApproval {
	t.Helper()
	i, err := s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) { return source, nil })
	if err != nil {
		t.Fatal(err)
	}
	i.mu.Lock()
	i.transferred = true
	i.preparationTimer.Stop()
	i.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	entry := &parentFileApproval{service: s, intent: i, id: "internal-resource-owner", ctx: ctx, cancel: cancel}
	s.mu.Lock()
	delete(s.filePreparing, i)
	s.fileGrants[entry.id] = entry
	s.mu.Unlock()
	t.Cleanup(entry.close)
	if l, err := s.OpenParentFileRequest(f.request(false), "child", source.ItemID(), entry.id); l != nil || err == nil {
		t.Fatal("IO-only test owner became public parent permission")
	}
	return entry
}
func TestParentFileReaderAdmissionPrecedesBlockedDuplicateAndSharesNormalLeases(t *testing.T) {
	for _, mode := range []string{"failure", "cancellation"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			source := &pendingFileSource{openReader: func(ctx context.Context) (FileReadAtCloser, error) {
				close(entered)
				if mode == "cancellation" {
					<-ctx.Done()
				} else {
					<-release
				}
				return nil, ErrParentFileChanged
			}}
			entry := fileIOOwner(t, s, f, source)
			for n := 0; n < MaxLeases-1; n++ {
				s.leases[&Lease{}] = "normal-occupancy"
			}
			ctx, cancel := context.WithCancel(f.request(false).Context())
			defer cancel()
			result := make(chan error, 1)
			go func() { _, err := s.acquireParentFileReader(f.request(false).WithContext(ctx), entry); result <- err }()
			<-entered
			s.mu.Lock()
			reserved := len(s.fileReaders)
			s.mu.Unlock()
			if reserved != 1 {
				t.Fatal("duplicate opener has no reserved descriptor slot")
			}
			if l, err := s.acquireParentFileReader(f.request(false), entry); l != nil || err == nil || source.opens.Load() != 1 {
				t.Fatal("extra FD opened before shared normal-lease admission")
			}
			if mode == "cancellation" {
				cancel()
			} else {
				close(release)
			}
			if err := <-result; err == nil {
				t.Fatal("failed duplicate opener returned a reader")
			}
			s.mu.Lock()
			reserved = len(s.fileReaders)
			normal := len(s.leases)
			s.mu.Unlock()
			if reserved != 0 || normal != MaxLeases-1 {
				t.Fatal("failed duplicate leaked its reservation or removed normal leases")
			}
		})
	}
}

type joinedCloseFile struct {
	*os.File
	once             sync.Once
	entered, release chan struct{}
	closes           atomic.Int32
}

func (f *joinedCloseFile) Close() error {
	var err error
	f.once.Do(func() { close(f.entered); <-f.release; err = f.File.Close(); f.closes.Add(1) })
	return err
}
func TestParentFileRetirementJoinsActualDuplicateCloseBeforeCapacityRefund(t *testing.T) {
	for _, mode := range []string{"retire", "synchronous Check", "profile invalidation", "original expiry"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			path := filepath.Join(t.TempDir(), "synthetic-close-proof.bin")
			if err := os.WriteFile(path, []byte("synthetic actual descriptor"), 0600); err != nil {
				t.Fatal(err)
			}
			actual, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer actual.Close()
			file := &joinedCloseFile{File: actual, entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(file.release) })
			source := &pendingFileSource{openReader: func(context.Context) (FileReadAtCloser, error) { return file, nil }}
			entry := fileIOOwner(t, s, f, source)
			reader, err := s.acquireParentFileReader(f.request(false), entry)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				switch mode {
				case "retire":
					s.retireParentFile(entry)
				case "synchronous Check":
					f.mu.Lock()
					f.policy.Revision = "changed-policy"
					f.mu.Unlock()
					if reader.Check(t.Context()) == nil {
						t.Error("changed synchronous check allowed")
					}
				case "profile invalidation":
					s.InvalidateProfile("child")
				case "original expiry":
					entry.timer = f.clock.AfterFunc(time.Hour, func() { s.retireParentFile(entry) })
					f.clock.advance(time.Hour)
				}
			}()
			select {
			case <-file.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("actual duplicate cleanup not reached")
			}
			select {
			case <-done:
				t.Fatal("retirement returned before actual descriptor cleanup")
			default:
			}
			s.mu.Lock()
			readers, grants := len(s.fileReaders), len(s.fileGrants)
			s.mu.Unlock()
			if readers != 1 || grants != 1 {
				t.Fatal("closing descriptor capacity refunded early")
			}
			releaseOnce.Do(func() { close(file.release) })
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("retirement or synchronous Check self-deadlocked")
			}
			if _, err := actual.Stat(); err == nil || file.closes.Load() != 1 {
				t.Fatal("retirement did not close the actual duplicate exactly once")
			}
			s.mu.Lock()
			readers, grants = len(s.fileReaders), len(s.fileGrants)
			s.mu.Unlock()
			if readers != 0 || grants != 0 || source.closes.Load() != 1 {
				t.Fatal("retirement left a reader/callback/source reservation")
			}
		})
	}
}

func TestParentFileActiveEpochSurvivesUnrelatedProfileChurn(t *testing.T) {
	for _, mode := range []string{"pending held file", "IO owner"} {
		t.Run(mode, func(t *testing.T) {
			s, f := newFixture(t)
			source := &pendingFileSource{}
			var i *ParentFileIntent
			if mode == "IO owner" {
				i = fileIOOwner(t, s, f, source).intent
			} else {
				var err error
				i, err = s.PrepareParentFileRequest(f.request(false), "child", source.ItemID(), func(context.Context, Subject, string) (ParentFileSource, error) { return source, nil })
				if err != nil {
					t.Fatal(err)
				}
				defer i.Close()
			}
			s.mu.Lock()
			original := s.epochs["child"]
			for n := 0; n < 4096; n++ {
				s.profileEpochLocked(fmt.Sprintf("unrelated-%d", n))
			}
			after := s.epochs["child"]
			size := len(s.epochs)
			s.mu.Unlock()
			if original != after || size > 2048 || s.checkFile(t.Context(), i) != nil {
				t.Fatal("unrelated idle profile churn evicted active file authority")
			}
		})
	}
}

func TestParentFilePreparatoryAdmissionReclaimsExpiredCanonicalCapacity(t *testing.T) {
	s, f := newFixture(t)
	for n := 0; n < MaxGrants; n++ {
		s.grants[fmt.Sprint(n)] = &approval{intent: Intent{profile: "expired-other-profile"}, expires: f.clock.Now()}
	}
	calls := 0
	i, err := s.PrepareParentFileRequest(f.request(false), "child", "actual-unmatched-file", func(context.Context, Subject, string) (ParentFileSource, error) {
		calls++
		return &pendingFileSource{}, nil
	})
	if err != nil || i == nil || calls != 1 {
		t.Fatal("expired canonical grants permanently consumed file admission", err)
	}
	defer i.Close()
	s.mu.Lock()
	normal, pending := len(s.grants), len(s.filePreparing)
	s.mu.Unlock()
	if normal != 0 || pending != 1 {
		t.Fatal("shared expired capacity was not pruned before opening")
	}
}
