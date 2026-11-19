package kidsvod

import (
	"context"
	"time"
)

var closedContext = func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }()

// Lease is an internal cancellation seam for a future byte/provider gate. It
// opens nothing. Incoming HTTP ownership/PIN checks remain mandatory, and the
// future reader must use Context and Check before emitting additional bytes.
type Lease struct {
	service    *Service
	ctx        context.Context
	cancel     context.CancelFunc
	intent     *Intent
	grant      *Grant
	timer      Timer
	stopParent func() bool
	closed     bool
	reason     string
	expires    time.Time
}

func (Lease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*Lease) UnmarshalJSON([]byte) error  { return ErrOpaqueCapability }

func (l *Lease) Context() context.Context {
	if l == nil || l.ctx == nil {
		return closedContext
	}
	return l.ctx
}
func (l *Lease) ExpiresAt() time.Time {
	if l == nil {
		return time.Time{}
	}
	return l.expires
}
func (l *Lease) Reason() string {
	if l == nil || l.service == nil {
		return CodeContextChanged
	}
	l.service.mu.Lock()
	defer l.service.mu.Unlock()
	return l.reason
}
func (l *Lease) Close() {
	if l == nil || l.service == nil {
		return
	}
	l.service.mu.Lock()
	defer l.service.mu.Unlock()
	l.service.closeLeaseLocked(l, CodeContextChanged)
}
func (l *Lease) Check(ctx context.Context) Decision {
	if l == nil || l.service == nil {
		return Decision{}
	}
	l.service.mu.Lock()
	closed, reason := l.closed, l.reason
	l.service.mu.Unlock()
	if closed {
		return l.service.decision(Denied, reason)
	}
	decision := l.service.EvaluateStored(ctx, l.intent, l.grant)
	l.service.mu.Lock()
	defer l.service.mu.Unlock()
	if l.closed {
		return l.service.decision(Denied, l.reason)
	}
	if !decision.Allowed() {
		reason := decision.Code()
		if reason == "" {
			reason = CodeContextChanged
		}
		l.service.closeLeaseLocked(l, reason)
		return l.service.decision(Denied, reason)
	}
	return decision
}
func (s *Service) OpenLease(ctx context.Context, intent *Intent, grant *Grant) (*Lease, Decision) {
	decision := s.EvaluateStored(ctx, intent, grant)
	if !decision.Allowed() {
		return nil, decision
	}
	if ctx == nil || ctx.Err() != nil {
		return nil, s.unknown(CodeContextChanged)
	}
	s.mu.Lock()
	decision = s.evaluateLocked(intent, grant)
	if !decision.Allowed() || ctx.Err() != nil || len(s.leases) >= MaxLeases {
		s.mu.Unlock()
		return nil, s.unknown(CodeContextChanged)
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	expiry := intent.expires
	grantID := ""
	if grant != nil {
		entry := s.grants[grant.id]
		if entry == nil {
			cancel()
			s.mu.Unlock()
			return nil, s.unknown(CodeContextChanged)
		}
		expiry = entry.expires
		grantID = grant.id
	} else if intent.classificationExpires.Before(expiry) {
		expiry = intent.classificationExpires
	}
	lease := &Lease{service: s, ctx: leaseCtx, cancel: cancel, intent: intent, grant: grant, expires: expiry}
	s.leases[lease] = grantID
	s.mu.Unlock()
	stopParent := context.AfterFunc(ctx, func() { lease.Close() })
	timer := s.deps.Clock.AfterFunc(lease.expires.Sub(s.deps.Clock.Now()), func() { s.mu.Lock(); s.closeLeaseLocked(lease, CodeApprovalExpired); s.mu.Unlock() })
	s.mu.Lock()
	if lease.closed {
		timer.Stop()
		stopParent()
		decision = s.decision(Denied, lease.reason)
	} else {
		lease.timer = timer
		lease.stopParent = stopParent
	}
	s.mu.Unlock()
	if !decision.Allowed() {
		return nil, decision
	}
	return lease, decision
}
func (s *Service) closeLeaseLocked(lease *Lease, reason string) {
	if lease.closed {
		return
	}
	lease.closed = true
	lease.reason = reason
	if lease.timer != nil {
		lease.timer.Stop()
	}
	if lease.stopParent != nil {
		lease.stopParent()
	}
	delete(s.leases, lease)
	lease.cancel()
}
func (s *Service) cancelGrantLeasesLocked(id, reason string) {
	for lease, grantID := range s.leases {
		if grantID == id {
			s.closeLeaseLocked(lease, reason)
		}
	}
}
