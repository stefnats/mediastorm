package settingsbootstrap

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
)

// DeploymentWebDAVIssuer observes the complete inactive deployment. It cannot
// produce a PG actor, a PIN proof or a source permission for the shared endpoint.
type DeploymentWebDAVIssuer struct {
	self    *DeploymentWebDAVIssuer
	startup *Startup
	core    *datastore.DeploymentWebDAVIssuer
}
type DeploymentWebDAVApplicability struct {
	self *DeploymentWebDAVApplicability
	core *datastore.DeploymentWebDAVApplicability
}

func (s *Startup) NewDeploymentWebDAVIssuer(store *datastore.DataStore) (*DeploymentWebDAVIssuer, error) {
	if s == nil || s.self != s || s.ctx.Err() != nil || s.source == nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	finishing := s.finishing
	s.mu.Unlock()
	if !finishing {
		return nil, config.ErrSecurityCoordinator
	}
	if err := s.Finish(); err != nil {
		return nil, config.ErrSecurityCoordinator
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil || s.recordingProgressClosing {
		return nil, config.ErrSecurityCoordinator
	}
	core, err := datastore.NewDeploymentWebDAVIssuer(s.ctx, store, s.manager, s.security, s.source.Coordinator())
	if err != nil {
		return nil, err
	}
	value := &DeploymentWebDAVIssuer{startup: s, core: core}
	value.self = value
	return value, nil
}
func (i *DeploymentWebDAVIssuer) Capture(r *http.Request) (*DeploymentWebDAVApplicability, error) {
	if i == nil || i.self != i || i.startup == nil || i.startup.self != i.startup || i.startup.ctx.Err() != nil || i.core == nil {
		return nil, datastore.ErrDeploymentWebDAVUnavailable
	}
	original, err := i.startup.security.CaptureWebDAVBasicPolicy(r)
	if err != nil {
		return nil, err
	}
	core, err := i.core.Capture(r, original)
	if err != nil {
		return nil, err
	}
	value := &DeploymentWebDAVApplicability{core: core}
	value.self = value
	return value, nil
}
func (i *DeploymentWebDAVIssuer) Close() {
	if i != nil && i.self == i && i.core != nil {
		i.core.Close()
	}
}
func (DeploymentWebDAVIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("DeploymentWebDAVIssuer(opaque)"))
}
func (DeploymentWebDAVIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*DeploymentWebDAVIssuer) UnmarshalJSON([]byte) error {
	return datastore.ErrDeploymentWebDAVChanged
}
func (DeploymentWebDAVApplicability) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("DeploymentWebDAVApplicability(opaque)"))
}
func (DeploymentWebDAVApplicability) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*DeploymentWebDAVApplicability) UnmarshalJSON([]byte) error {
	return datastore.ErrDeploymentWebDAVChanged
}
func (p *DeploymentWebDAVApplicability) Check(r *http.Request) error {
	if p == nil || p.self != p || p.core == nil {
		return datastore.ErrDeploymentWebDAVChanged
	}
	return p.core.Check(r)
}
func (p *DeploymentWebDAVApplicability) Context() context.Context {
	if p == nil || p.self != p || p.core == nil {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	return p.core.Context()
}
func (p *DeploymentWebDAVApplicability) ExpiresAt() time.Time {
	if p == nil || p.self != p || p.core == nil {
		return time.Time{}
	}
	return p.core.ExpiresAt()
}

// Prefix is immutable original native DATA; callers still need Check before IO.
func (p *DeploymentWebDAVApplicability) Prefix() string {
	if p == nil || p.self != p || p.core == nil {
		return ""
	}
	return p.core.Prefix()
}
func (p *DeploymentWebDAVApplicability) Close() {
	if p != nil && p.self == p && p.core != nil {
		p.core.Close()
	}
}
