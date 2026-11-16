// Package kidsvod provides an internal regional VOD decision foundation.
// It is not wired to public routes and does not activate a household policy.
// Consumers must retain account, profile PIN, library and source authorization.
package kidsvod

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"novastream/models"
)

const (
	CodeProfileRequired        = "kids_vod_profile_required"
	CodeAccessDenied           = "kids_vod_access_denied"
	CodeClassificationUnknown  = "kids_vod_classification_unknown"
	CodeParentApprovalRequired = "kids_vod_parent_approval_required"
	CodeApprovalExpired        = "kids_vod_approval_expired"
	CodeContextChanged         = "kids_vod_context_changed"
	CodeUnsupportedSource      = "kids_vod_unsupported_source"
	MaxGrantTTL                = 2 * time.Hour
	MaxBindingTTL              = 24 * time.Hour
	MaxGrants                  = 512
	MaxLeases                  = 1024
)

// UnknownDenied deliberately occupies the zero value. Neither zero/imported
// values nor an absent profile may select a legacy-policy fallback.
type Outcome uint8

const (
	UnknownDenied Outcome = iota
	NotApplicable
	AllowedByParent
	Denied
)

var ErrOpaqueCapability = errors.New("kids VOD capabilities cannot be imported")

type Decision struct {
	outcome Outcome
	code    string
	seal    *serviceSeal
}

func (d Decision) Outcome() Outcome {
	if d.seal == nil {
		return UnknownDenied
	}
	return d.outcome
}
func (d Decision) Code() string {
	if d.seal != nil && (d.outcome == AllowedByParent || d.outcome == NotApplicable) {
		return ""
	}
	if d.code == "" {
		return CodeClassificationUnknown
	}
	return d.code
}
func (d Decision) Allowed() bool                { return d.seal != nil && d.outcome == AllowedByParent }
func (d Decision) IsNotApplicable() bool        { return d.seal != nil && d.outcome == NotApplicable }
func (d Decision) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (d *Decision) UnmarshalJSON([]byte) error  { *d = Decision{}; return ErrOpaqueCapability }

type Policy struct {
	Enabled       bool
	Country       string
	MaxMovieAge   *int
	MaxEpisodeAge *int
	Revision      string
}

func (p Policy) valid() bool {
	if !token(p.Revision, 256) {
		return false
	}
	if !p.Enabled {
		return true
	}
	return p.Country == "DE" && validAge(p.MaxMovieAge) && validAge(p.MaxEpisodeAge)
}
func validAge(age *int) bool {
	if age == nil {
		return false
	}
	switch *age {
	case 0, 6, 12, 16, 18:
		return true
	}
	return false
}

// Profile revisions must change when ownership, kids state, PIN or regional
// policy prerequisites change. Readers are configured server dependencies.
type Profile struct {
	ID, AccountID, Revision string
	IsKids                  bool
}
type Profiles interface {
	CurrentProfile(context.Context, string) (Profile, error)
}
type Policies interface {
	CurrentPolicy(context.Context, string) (Policy, error)
}

// Sessions must return current effective account roles and revocation state,
// not merely a cached token with stale IsMaster. Future adapters retain the
// entrypoint authentication checks as well as this ongoing current-state check.
type Sessions interface {
	CurrentSession(context.Context, string) (models.Session, error)
}
type ParentGate interface {
	Require(http.ResponseWriter, *http.Request) bool
}

type ResourceKind string

const (
	Prequeue  ResourceKind = "prequeue"
	Local     ResourceKind = "local"
	Plex      ResourceKind = "plex"
	Jellyfin  ResourceKind = "jellyfin"
	Recording ResourceKind = "recording"
)

type ResourceRef struct {
	kind ResourceKind
	id   string
	part int
}

func NewResourceRef(kind ResourceKind, id string, part int) (ResourceRef, error) {
	switch kind {
	case Prequeue, Local, Plex, Jellyfin, Recording:
	default:
		return ResourceRef{}, errors.New("unsupported resource kind")
	}
	if !token(id, 128) || strings.ContainsAny(id, "/\\?") || strings.Contains(id, "://") {
		return ResourceRef{}, errors.New("invalid opaque resource ID")
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == ':' || r == '.') {
			return ResourceRef{}, errors.New("invalid opaque resource ID")
		}
	}
	if part < 0 || part > 256 || part != 0 && kind != Recording {
		return ResourceRef{}, errors.New("invalid recording part")
	}
	return ResourceRef{kind, id, part}, nil
}
func (r ResourceRef) Kind() ResourceKind { return r.kind }
func (r ResourceRef) ID() string         { return r.id }
func (r ResourceRef) Part() int          { return r.part }

// Subject is created only after a current authenticated session and owned
// concrete profile have been resolved. Its zero/imported value is unusable.
type Subject struct{ account, profile, client string }

func (s Subject) AccountID() string { return s.account }
func (s Subject) ProfileID() string { return s.profile }
func (s Subject) ClientID() string  { return s.client }

// Binding is the output of a configured cache-only server resolver, never an
// HTTP DTO. ResolveCacheOnly must check current source/library rights, return
// a canonical server catalogue identity and exact episode/file correspondence,
// and never fetch metadata, probe, prepare, open a provider or refresh expiry.
// Path/title/name matching and caller-supplied Title metadata are insufficient.
type Binding struct {
	Resource                                   ResourceRef
	AccountID, ProfileID, CatalogID, MediaType string
	Episode                                    *models.EpisodeReference
	SourceVersion, Revision                    string
	ObservedAt, ExpiresAt                      time.Time
	// Raw provider metadata is informational only in this foundation. Neither
	// these rows nor a US scalar is a verified edition/episode classification.
	CertificationEvidence []models.CertificationEvidence
	LegacyCertification   string
}
type Resources interface {
	ResolveCacheOnly(context.Context, Subject, ResourceRef) (Binding, error)
}

type Timer interface{ Stop() bool }
type Clock interface {
	Now() time.Time
	AfterFunc(time.Duration, func()) Timer
}
type realClock struct{}

func (realClock) Now() time.Time                            { return time.Now() }
func (realClock) AfterFunc(d time.Duration, f func()) Timer { return time.AfterFunc(d, f) }

type Dependencies struct {
	Profiles  Profiles
	Policies  Policies
	Sessions  Sessions
	Resources Resources
	Parent    ParentGate
	Clock     Clock
}
type serviceSeal struct{ nonce [32]byte }

func token(value string, max int) bool {
	if len(value) == 0 || len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
