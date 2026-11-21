package localmedia

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/internal/importer"
)

// The publisher takes ownership of one original concrete issuer. Close must
// precede importer/Startup/SQL teardown; no alternate source or store is passed.
type SABLibraryPublisher struct {
	self    *SABLibraryPublisher
	issuer  *datastore.SABLibraryPublicationIssuer
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
	jobs    map[*sabPublicationJob]struct{}
	pending sync.WaitGroup
	once    sync.Once
}
type sabPublicationJob struct {
	owner    *SABLibraryPublisher
	account  string
	request  *http.Request
	mu       sync.Mutex
	intent   *datastore.SABLibraryPublicationIntent
	read     *datastore.SABLibraryPublicationRead
	copy     *importer.SABPublicationCopy
	prepared chan struct{}
	once     sync.Once
}
type SABLibraryPublicationResult struct {
	self   *SABLibraryPublicationResult
	job    *sabPublicationJob
	view   datastore.SABLibraryPublicationView
	expiry time.Time
}

func (SABLibraryPublisher) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABLibraryPublisher) UnmarshalJSON([]byte) error {
	return datastore.ErrSABLibraryPublicationChanged
}
func (SABLibraryPublisher) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABLibraryPublisher(opaque)"))
}
func (SABLibraryPublicationResult) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABLibraryPublicationResult) UnmarshalJSON([]byte) error {
	return datastore.ErrSABLibraryPublicationChanged
}
func (SABLibraryPublicationResult) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABLibraryPublicationResult(opaque)"))
}
func NewSABLibraryPublisher(issuer *datastore.SABLibraryPublicationIssuer) (*SABLibraryPublisher, error) {
	if !sabPublicationPlatformReady() || issuer == nil || issuer.Context().Err() != nil {
		return nil, datastore.ErrSABLibraryPublicationUnavailable
	}
	life, cancel := context.WithCancel(issuer.Context())
	value := &SABLibraryPublisher{issuer: issuer, ctx: life, cancel: cancel, jobs: map[*sabPublicationJob]struct{}{}}
	value.self = value
	return value, nil
}
func (p *SABLibraryPublisher) Context() context.Context {
	if p != nil && p.self == p && p.ctx != nil {
		return p.ctx
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (p *SABLibraryPublisher) begin(r *http.Request) (*sabPublicationJob, error) {
	if p == nil || p.self != p || p.ctx.Err() != nil || r == nil || r.Context().Err() != nil {
		return nil, datastore.ErrSABLibraryPublicationUnavailable
	}
	account := auth.GetAccountID(r)
	if len(account) == 0 || len(account) > 256 {
		return nil, datastore.ErrSABLibraryPublicationChanged
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.jobs) >= 2 {
		return nil, datastore.ErrSABLibraryPublicationBusy
	}
	for job := range p.jobs {
		if job.account == account {
			return nil, datastore.ErrSABLibraryPublicationBusy
		}
	}
	job := &sabPublicationJob{owner: p, account: account, request: r, prepared: make(chan struct{})}
	p.jobs[job] = struct{}{}
	p.pending.Add(1)
	return job, nil
}
func (j *sabPublicationJob) close() {
	if j == nil {
		return
	}
	j.once.Do(func() {
		j.mu.Lock()
		intent, read, copy := j.intent, j.read, j.copy
		j.mu.Unlock()
		if intent != nil {
			intent.Close()
		}
		if read != nil {
			read.Close()
		}
		if copy != nil {
			copy.Close()
		}
		j.owner.mu.Lock()
		delete(j.owner.jobs, j)
		j.owner.mu.Unlock()
		j.owner.pending.Done()
	})
}
func (p *SABLibraryPublisher) Close() {
	if p == nil || p.self != p {
		return
	}
	p.once.Do(func() {
		p.cancel()
		p.mu.Lock()
		p.closed = true
		var jobs []*sabPublicationJob
		for job := range p.jobs {
			jobs = append(jobs, job)
		}
		p.mu.Unlock()
		p.issuer.Close()
		for _, job := range jobs {
			<-job.prepared
			job.close()
		}
		p.pending.Wait()
	})
}
func (p *SABLibraryPublisher) Publish(r *http.Request, queueID int64, raw []byte) (*SABLibraryPublicationResult, error) {
	return p.runPublication(r, queueID, raw, false)
}
func (p *SABLibraryPublisher) Reconcile(r *http.Request, queueID int64, raw []byte) (*SABLibraryPublicationResult, error) {
	return p.runPublication(r, queueID, raw, true)
}
func (p *SABLibraryPublisher) ReadStatus(r *http.Request, queueID int64) (result *SABLibraryPublicationResult, err error) {
	job, e := p.begin(r)
	if e != nil {
		return nil, e
	}
	defer close(job.prepared)
	defer func() {
		if result == nil {
			job.close()
		}
	}()
	read, e := p.issuer.ReadStatus(r, queueID)
	if e != nil {
		return nil, e
	}
	job.mu.Lock()
	job.read = read
	job.mu.Unlock()
	if read.Check(r) != nil || p.ctx.Err() != nil {
		return nil, datastore.ErrSABLibraryPublicationChanged
	}
	value := &SABLibraryPublicationResult{job: job, view: read.View(), expiry: read.ExpiresAt()}
	value.self = value
	return value, nil
}
func (r *SABLibraryPublicationResult) valid() bool {
	return r != nil && r.self == r && r.job != nil && r.job.owner != nil && r.job.owner.self == r.job.owner
}
func (r *SABLibraryPublicationResult) Check(original *http.Request) error {
	if !r.valid() || original != r.job.request || original.Context().Err() != nil || r.job.owner.ctx.Err() != nil || !r.expiry.After(time.Now()) {
		return datastore.ErrSABLibraryPublicationChanged
	}
	r.job.mu.Lock()
	intent, read := r.job.intent, r.job.read
	r.job.mu.Unlock()
	if read != nil {
		return read.Check(original)
	}
	if intent == nil {
		return datastore.ErrSABLibraryPublicationChanged
	}
	return intent.CheckPublished(original)
}
func (r *SABLibraryPublicationResult) View() datastore.SABLibraryPublicationView {
	if !r.valid() {
		return datastore.SABLibraryPublicationView{}
	}
	out := r.view
	out.Files = append([]datastore.SABLibraryPublicationFileView(nil), out.Files...)
	return out
}
func (r *SABLibraryPublicationResult) ExpiresAt() time.Time {
	if r.valid() {
		return r.expiry
	}
	return time.Time{}
}
func (r *SABLibraryPublicationResult) Close() {
	if r.valid() {
		r.job.close()
	}
}
