package kidsvod

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"novastream/internal/auth"
)

type actor struct {
	token, account, client string
	master                 bool
}
type Intent struct {
	seal                             *serviceSeal
	actor                            actor
	profile                          string
	ref                              ResourceRef
	fingerprint                      string
	classificationFingerprint        string
	classificationBindingFingerprint string
	classificationOutcome            Outcome
	classificationExpires            time.Time
	epoch                            uint64
	expires                          time.Time
}

func (i Intent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (i *Intent) UnmarshalJSON([]byte) error  { return ErrOpaqueCapability }
func (i Intent) String() string               { return "kids VOD intent" }

type Grant struct {
	seal    *serviceSeal
	id      string
	expires time.Time
}

func (g Grant) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (g *Grant) UnmarshalJSON([]byte) error  { return ErrOpaqueCapability }
func (g Grant) String() string               { return "kids VOD parent exception" }

type approval struct {
	intent  Intent
	expires time.Time
	revoked bool
}

type Service struct {
	deps              Dependencies
	seal              *serviceSeal
	mu                sync.Mutex
	epochs            map[string]uint64
	nextEpoch         uint64
	grants            map[string]*approval
	leases            map[*Lease]string
	fileGrants        map[string]*parentFileApproval
	filePreparing     map[*ParentFileIntent]struct{}
	fileReaders       map[*ParentFileReader]*parentFileApproval
	fileProcesses     map[*ParentFileProcessLease]*parentFileApproval
	fileOutputReaders int
	parentFileRoot    context.Context
	parentFilesClosed bool
	parentFilesClose  sync.Once
	parentFilesDone   chan struct{}
}

func New(deps Dependencies) (*Service, error) {
	seal := &serviceSeal{}
	if _, err := rand.Read(seal.nonce[:]); err != nil {
		return nil, err
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	return &Service{deps: deps, seal: seal, epochs: map[string]uint64{}, grants: map[string]*approval{}, leases: map[*Lease]string{}, fileGrants: map[string]*parentFileApproval{}, filePreparing: map[*ParentFileIntent]struct{}{}, fileReaders: map[*ParentFileReader]*parentFileApproval{}}, nil
}
func (s *Service) decision(outcome Outcome, code string) Decision {
	if s == nil {
		return Decision{}
	}
	return Decision{outcome: outcome, code: code, seal: s.seal}
}
func (s *Service) unknown(code string) Decision { return s.decision(UnknownDenied, code) }

func (s *Service) requestActor(r *http.Request) (actor, Decision) {
	if s == nil || s.seal == nil || r == nil || s.deps.Sessions == nil {
		return actor{}, s.unknown(CodeAccessDenied)
	}
	session, ok := auth.GetSession(r)
	if !ok || session.Token == "" || auth.GetAccountID(r) == "" || session.AccountID != auth.GetAccountID(r) || session.Scope != "" {
		return actor{}, s.unknown(CodeAccessDenied)
	}
	current, err := s.deps.Sessions.CurrentSession(r.Context(), session.Token)
	if err != nil || current.Token != session.Token || current.AccountID != session.AccountID || current.Scope != "" || !current.ExpiresAt.After(s.deps.Clock.Now()) || current.IsMaster != auth.IsMaster(r) {
		return actor{}, s.unknown(CodeAccessDenied)
	}
	client := strings.TrimSpace(r.Header.Get("X-Client-ID"))
	if current.ClientID != "" {
		if client != "" && client != current.ClientID {
			return actor{}, s.unknown(CodeContextChanged)
		}
		client = current.ClientID
	}
	if client != "" && !token(client, 256) {
		return actor{}, s.unknown(CodeAccessDenied)
	}
	return actor{token: session.Token, account: current.AccountID, client: client, master: current.IsMaster}, Decision{}
}

// ResolveRequest accepts only an opaque server resource reference. Client title,
// path, age, provenance or source-version claims are never an argument here.
// A returned Intent is correlation for subsequent current checks, not a grant.
func (s *Service) ResolveRequest(r *http.Request, profile string, ref ResourceRef) (*Intent, Decision) {
	a, denial := s.requestActor(r)
	if a.token == "" {
		return nil, denial
	}
	return s.resolve(r.Context(), a, profile, ref)
}
func (s *Service) resolve(ctx context.Context, a actor, profile string, ref ResourceRef) (*Intent, Decision) {
	first, decision := s.resolveCurrent(ctx, a, profile, ref, true)
	if first == nil {
		return nil, decision
	}
	// Dependencies may block. A second cache-only observation must still agree
	// on the original base authority before publication. No transaction is claimed.
	current, _ := s.resolveCurrent(ctx, a, profile, ref, false)
	if current == nil || first.fingerprint != current.fingerprint {
		return nil, s.unknown(CodeContextChanged)
	}
	if !first.expires.After(s.deps.Clock.Now()) || ctx.Err() != nil {
		return nil, s.unknown(CodeContextChanged)
	}
	if first.classificationBindingFingerprint != current.classificationBindingFingerprint ||
		first.classificationOutcome != UnknownDenied && !first.classificationExpires.After(s.deps.Clock.Now()) {
		first.classificationOutcome = UnknownDenied
		first.classificationFingerprint = ""
		first.classificationExpires = time.Time{}
	}
	return first, decisionForIntent(s, first)
}
func decisionForIntent(s *Service, intent *Intent) Decision {
	if intent.classificationOutcome == AllowedByClassification {
		return s.decision(AllowedByClassification, "")
	}
	if intent.classificationOutcome == Denied {
		return s.decision(Denied, CodeAccessDenied)
	}
	return s.unknown(CodeClassificationUnknown)
}
func (s *Service) resolveCurrent(ctx context.Context, a actor, profile string, ref ResourceRef, classify bool) (*Intent, Decision) {
	if s == nil || s.seal == nil || s.deps.Clock == nil || s.deps.Profiles == nil || s.deps.Policies == nil || s.deps.Sessions == nil {
		return nil, s.unknown(CodeClassificationUnknown)
	}
	if ctx == nil || ctx.Err() != nil {
		return nil, s.unknown(CodeContextChanged)
	}
	if !token(profile, 256) {
		return nil, s.unknown(CodeProfileRequired)
	}
	s.mu.Lock()
	epoch := s.profileEpochLocked(profile)
	s.mu.Unlock()
	now := s.deps.Clock.Now()
	session, err := s.deps.Sessions.CurrentSession(ctx, a.token)
	if err != nil || session.Token != a.token || session.AccountID != a.account || session.Scope != "" || session.IsMaster != a.master || !session.ExpiresAt.After(now) || session.ClientID != "" && session.ClientID != a.client {
		return nil, s.unknown(CodeAccessDenied)
	}
	p, err := s.deps.Profiles.CurrentProfile(ctx, profile)
	if err != nil || p.ID != profile || !token(p.AccountID, 256) || !token(p.Revision, 256) || p.AccountID != a.account && !a.master {
		return nil, s.unknown(CodeAccessDenied)
	}
	policy, err := s.deps.Policies.CurrentPolicy(ctx, profile)
	if err != nil || !policy.valid() {
		return nil, s.unknown(CodeClassificationUnknown)
	}
	if policy.MaxMovieAge != nil {
		value := *policy.MaxMovieAge
		policy.MaxMovieAge = &value
	}
	if policy.MaxEpisodeAge != nil {
		value := *policy.MaxEpisodeAge
		policy.MaxEpisodeAge = &value
	}
	if !policy.Enabled {
		s.mu.Lock()
		unchanged := s.epochs[profile] == epoch
		s.mu.Unlock()
		if !unchanged || ctx.Err() != nil {
			return nil, s.unknown(CodeContextChanged)
		}
		if p.IsKids {
			return nil, s.unknown(CodeContextChanged)
		}
		// An inactive adult still needs fresh current identity after a blocking
		// policy lookup. Missing or changed prerequisites are never inactivity.
		currentProfile, profileErr := s.deps.Profiles.CurrentProfile(ctx, profile)
		currentSession, sessionErr := s.deps.Sessions.CurrentSession(ctx, a.token)
		if profileErr != nil || sessionErr != nil || currentProfile != p || currentSession.Token != session.Token || currentSession.AccountID != session.AccountID || currentSession.IsMaster != session.IsMaster || currentSession.Scope != "" || currentSession.ClientID != session.ClientID || currentSession.ExpiresAt != session.ExpiresAt || ctx.Err() != nil {
			return nil, s.unknown(CodeContextChanged)
		}
		currentPolicy, policyErr := s.deps.Policies.CurrentPolicy(ctx, profile)
		finalProfile, finalProfileErr := s.deps.Profiles.CurrentProfile(ctx, profile)
		s.mu.Lock()
		currentEpoch := s.epochs[profile]
		s.mu.Unlock()
		if finalProfileErr != nil || finalProfile != p || policyErr != nil || !currentPolicy.valid() || currentPolicy.Enabled || currentPolicy.Revision != policy.Revision || currentEpoch != epoch || !currentSession.ExpiresAt.After(s.deps.Clock.Now()) || ctx.Err() != nil {
			return nil, s.unknown(CodeContextChanged)
		}
		return nil, s.decision(NotApplicable, "")
	}
	if !p.IsKids {
		return nil, s.unknown(CodeContextChanged)
	}
	if s.deps.Resources == nil || ref.id == "" {
		return nil, s.unknown(CodeUnsupportedSource)
	}
	checked, err := NewResourceRef(ref.kind, ref.id, ref.part)
	if err != nil || checked != ref {
		return nil, s.unknown(CodeUnsupportedSource)
	}
	binding, err := s.deps.Resources.ResolveCacheOnly(ctx, Subject{account: p.AccountID, profile: profile, client: a.client}, ref)
	if err != nil || binding.AccountID != p.AccountID || binding.ProfileID != profile || binding.Resource != ref || !validBinding(binding, now) {
		return nil, s.unknown(CodeClassificationUnknown)
	}
	binding = copyBinding(binding)
	var classification *VerifiedRegionalClassification
	classificationOutcome := UnknownDenied
	classificationFingerprint := ""
	classificationExpires := time.Time{}
	if classify && s.deps.Classifications != nil {
		verified, readErr := s.deps.Classifications.ResolveVerifiedCacheOnly(ctx, Subject{account: p.AccountID, profile: profile, client: a.client}, copyBinding(binding))
		if readErr == nil && verified != nil && verified.facts.validFor(binding, s.deps.Clock.Now()) {
			// Clone again so the configured reader cannot later mutate its own object.
			frozen := *verified
			frozen.facts.Episode = copyEpisode(verified.facts.Episode)
			classification = &frozen
			encoded, encodeErr := classification.fingerprint()
			if encodeErr == nil {
				digest := sha256.Sum256(encoded)
				classificationFingerprint = hex.EncodeToString(digest[:])
				classificationExpires = classification.facts.ExpiresAt
				cap := policy.MaxMovieAge
				if binding.MediaType == "series" {
					cap = policy.MaxEpisodeAge
				}
				classificationOutcome = Denied
				if cap != nil && classification.facts.Age <= *cap {
					classificationOutcome = AllowedByClassification
				}
			}
		}
	}
	expiry := binding.ExpiresAt
	if session.ExpiresAt.Before(expiry) {
		expiry = session.ExpiresAt
	}
	episode := any(nil)
	if ep := binding.Episode; ep != nil {
		episode = []any{ep.EpisodeID, ep.SeasonNumber, ep.EpisodeNumber, ep.Numbering.SeriesID, ep.Numbering.Ordering}
	}
	// Only security/identity fields are hashed. Raw classification metadata is
	// informational: changing a US scalar or copying TMDB rows cannot allow.
	data, err := json.Marshal([]any{a.account, a.client, a.master, session.Token, session.ExpiresAt, p, policy, ref.kind, ref.id, ref.part, binding.AccountID, binding.ProfileID, binding.CatalogID, binding.MediaType, episode, binding.SourceVersion, binding.Revision, binding.ObservedAt, binding.ExpiresAt, epoch})
	if err != nil || ctx.Err() != nil {
		return nil, s.unknown(CodeContextChanged)
	}
	s.mu.Lock()
	unchanged := s.epochs[profile] == epoch
	s.mu.Unlock()
	if !unchanged {
		return nil, s.unknown(CodeContextChanged)
	}
	sum := sha256.Sum256(data)
	// Optional exact played-edition continuity has its own binding fingerprint;
	// it never changes the existing parent-exception base authority or expiry.
	classificationBindingData, err := json.Marshal([]any{hex.EncodeToString(sum[:]), binding.EditionID})
	if err != nil {
		return nil, s.unknown(CodeContextChanged)
	}
	classificationBindingSum := sha256.Sum256(classificationBindingData)
	intent := &Intent{seal: s.seal, actor: a, profile: profile, ref: ref, fingerprint: hex.EncodeToString(sum[:]), epoch: epoch, expires: expiry, classificationFingerprint: classificationFingerprint, classificationBindingFingerprint: hex.EncodeToString(classificationBindingSum[:]), classificationOutcome: classificationOutcome, classificationExpires: classificationExpires}
	if classificationOutcome == AllowedByClassification {
		return intent, s.decision(AllowedByClassification, "")
	}
	if classificationOutcome == Denied {
		return intent, s.decision(Denied, CodeAccessDenied)
	}
	return intent, s.unknown(CodeClassificationUnknown)
}
func validBinding(b Binding, now time.Time) bool {
	if !catalogID(b.CatalogID, b.MediaType) || !token(b.Revision, 256) || !fileVersion(b.SourceVersion) || b.ObservedAt.IsZero() || b.ObservedAt.After(now) || now.Sub(b.ObservedAt) > MaxBindingTTL || b.ExpiresAt.Sub(b.ObservedAt) > MaxBindingTTL || !b.ExpiresAt.After(now) || !b.ExpiresAt.After(b.ObservedAt) {
		return false
	}
	if b.MediaType == "movie" {
		return b.Episode == nil
	}
	if b.MediaType != "series" || b.Episode == nil {
		return false
	}
	ep := b.Episode
	return token(ep.EpisodeID, 1024) && ep.SeasonNumber >= 0 && ep.SeasonNumber <= 10000 && ep.EpisodeNumber > 0 && ep.EpisodeNumber <= 100000 && ep.Numbering != nil && token(ep.Numbering.SeriesID, 1024) && token(ep.Numbering.Ordering, 128)
}
func fileVersion(value string) bool {
	if !strings.HasPrefix(value, "file_v1_") || len(value) != 72 {
		return false
	}
	tail := strings.TrimPrefix(value, "file_v1_")
	if tail != strings.ToLower(tail) {
		return false
	}
	data, err := hex.DecodeString(tail)
	return err == nil && len(data) == 32
}

// IssueParentException consumes the existing fresh parent-authentication seam.
// It re-resolves the same current binding after authentication; an approval
// cannot be stamped onto a resource/configuration changed during the prompt.
func (s *Service) IssueParentException(w http.ResponseWriter, r *http.Request, profile string, ref ResourceRef, ttl time.Duration) (*Grant, Decision) {
	before, decision := s.ResolveRequest(r, profile, ref)
	if before == nil {
		return nil, decision
	}
	if ttl <= 0 || ttl > MaxGrantTTL {
		return nil, s.unknown(CodeApprovalExpired)
	}
	if s.deps.Parent == nil || !s.deps.Parent.Require(w, r) {
		return nil, s.unknown(CodeParentApprovalRequired)
	}
	after, decision := s.ResolveRequest(r, profile, ref)
	if after == nil || before.fingerprint != after.fingerprint {
		return nil, s.unknown(CodeContextChanged)
	}
	if r.Context().Err() != nil {
		return nil, s.unknown(CodeContextChanged)
	}
	var id [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, s.unknown(CodeAccessDenied)
	}
	expires := s.deps.Clock.Now().Add(ttl)
	if after.expires.Before(expires) {
		expires = after.expires
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked()
	if s.epochs[profile] != after.epoch || !expires.After(s.deps.Clock.Now()) || len(s.grants)+len(s.fileGrants)+len(s.filePreparing) >= MaxGrants {
		return nil, s.unknown(CodeContextChanged)
	}
	grant := &Grant{seal: s.seal, id: hex.EncodeToString(id[:]), expires: expires}
	s.grants[grant.id] = &approval{intent: *after, expires: expires}
	return grant, s.decision(AllowedByParent, "")
}

// EvaluateStored checks a server-held opaque intent. HTTP consumers still need
// the normal incoming ownership/PIN check before choosing that stored intent.
// Query parameters cannot replace its stored profile, source or exact episode.
func (s *Service) EvaluateStored(ctx context.Context, intent *Intent, grant *Grant) Decision {
	if s == nil || intent == nil || intent.seal != s.seal || s.seal == nil {
		return s.unknown(CodeClassificationUnknown)
	}
	current, decision := s.resolve(ctx, intent.actor, intent.profile, intent.ref)
	if current == nil {
		return decision
	}
	if current.fingerprint != intent.fingerprint || grant == nil && current.classificationFingerprint != intent.classificationFingerprint {
		return s.unknown(CodeContextChanged)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evaluateLocked(current, grant)
}
func (s *Service) evaluateLocked(intent *Intent, grant *Grant) Decision {
	if intent == nil || intent.seal != s.seal || s.epochs[intent.profile] != intent.epoch || !intent.expires.After(s.deps.Clock.Now()) {
		return s.unknown(CodeContextChanged)
	}
	if grant == nil {
		if intent.classificationOutcome == AllowedByClassification && intent.classificationFingerprint != "" && intent.classificationExpires.After(s.deps.Clock.Now()) {
			return s.decision(AllowedByClassification, "")
		}
		if intent.classificationOutcome == Denied {
			return s.decision(Denied, CodeAccessDenied)
		}
		return s.unknown(CodeParentApprovalRequired)
	}
	if grant.seal != s.seal {
		return s.unknown(CodeParentApprovalRequired)
	}
	entry := s.grants[grant.id]
	if entry == nil || entry.intent.fingerprint != intent.fingerprint || s.epochs[intent.profile] != intent.epoch {
		return s.unknown(CodeContextChanged)
	}
	if entry.revoked {
		return s.decision(Denied, CodeAccessDenied)
	}
	if !entry.expires.After(s.deps.Clock.Now()) {
		return s.decision(Denied, CodeApprovalExpired)
	}
	return s.decision(AllowedByParent, "")
}

// RevokeParentException is the explicit parent mutation seam, not a route.
// Stopping one's own Lease is independent and does not need parent credentials.
func (s *Service) RevokeParentException(w http.ResponseWriter, r *http.Request, grant *Grant) Decision {
	if s == nil || grant == nil || grant.seal != s.seal {
		return s.unknown(CodeAccessDenied)
	}
	s.mu.Lock()
	entry := s.grants[grant.id]
	var profile string
	if entry != nil {
		profile = entry.intent.profile
	}
	s.mu.Unlock()
	if profile == "" {
		return s.unknown(CodeAccessDenied)
	}
	a, denial := s.requestActor(r)
	if a.token == "" {
		return denial
	}
	p, err := s.deps.Profiles.CurrentProfile(r.Context(), profile)
	if err != nil || p.ID != profile || p.AccountID != a.account && !a.master {
		return s.unknown(CodeAccessDenied)
	}
	if s.deps.Parent == nil || !s.deps.Parent.Require(w, r) {
		return s.unknown(CodeParentApprovalRequired)
	}
	if r.Context().Err() != nil {
		return s.unknown(CodeContextChanged)
	}
	currentActor, currentDenial := s.requestActor(r)
	if currentActor.token == "" {
		return currentDenial
	}
	currentProfile, err := s.deps.Profiles.CurrentProfile(r.Context(), profile)
	if err != nil || currentProfile.ID != p.ID || currentProfile.AccountID != p.AccountID || currentProfile.Revision != p.Revision || currentActor != a {
		return s.unknown(CodeContextChanged)
	}
	s.mu.Lock()
	if entry = s.grants[grant.id]; entry != nil {
		entry.revoked = true
		s.cancelGrantLeasesLocked(grant.id, CodeAccessDenied)
	}
	s.mu.Unlock()
	return s.decision(Denied, CodeAccessDenied)
}

// InvalidateProfile is a trusted mutation callback for policy, source rights,
// ownership, PIN, session or restore changes. It never initiates external work.
func (s *Service) InvalidateProfile(profile string) {
	if s == nil || !token(profile, 256) {
		return
	}
	s.mu.Lock()
	var retired []*parentFileApproval
	var pending []*ParentFileIntent
	s.profileEpochLocked(profile)
	s.nextEpoch++
	s.epochs[profile] = s.nextEpoch
	for lease := range s.leases {
		if lease.intent.profile == profile {
			s.closeLeaseLocked(lease, CodeContextChanged)
		}
	}
	for id, entry := range s.grants {
		if entry.intent.profile == profile {
			entry.revoked = true
			s.cancelGrantLeasesLocked(id, CodeContextChanged)
		}
	}
	for _, entry := range s.fileGrants {
		if entry.intent.profile == profile {
			retired = append(retired, entry)
		}
	}
	for intent := range s.filePreparing {
		if intent.profile == profile {
			pending = append(pending, intent)
		}
	}
	s.mu.Unlock()
	for _, intent := range pending {
		intent.Close()
	}
	for _, entry := range retired {
		entry.close()
	}
}
func (s *Service) pruneLocked() {
	now := s.deps.Clock.Now()
	for id, entry := range s.grants {
		if !entry.expires.After(now) {
			s.cancelGrantLeasesLocked(id, CodeApprovalExpired)
			delete(s.grants, id)
		}
	}
}

// Epoch state is bounded independently of active approvals. Evicting an idle
// profile makes its old intents unknown; active approvals protect their entry.
func (s *Service) profileEpochLocked(profile string) uint64 {
	if epoch, ok := s.epochs[profile]; ok {
		return epoch
	}
	if len(s.epochs) >= 2048 {
		for candidate := range s.epochs {
			active := false
			for _, entry := range s.grants {
				if entry.intent.profile == candidate && !entry.revoked && entry.expires.After(s.deps.Clock.Now()) {
					active = true
					break
				}
			}
			if !active {
				for lease := range s.leases {
					if lease.intent.profile == candidate && !lease.closed {
						active = true
						break
					}
				}
			}
			if !active {
				for _, entry := range s.fileGrants {
					if entry.intent.profile == candidate && entry.ctx.Err() == nil {
						active = true
						break
					}
				}
			}
			if !active {
				for intent := range s.filePreparing {
					if intent.profile == candidate {
						active = true
						break
					}
				}
			}
			if !active {
				for reader, entry := range s.fileReaders {
					if entry.intent.profile == candidate && reader.ctx.Err() == nil {
						active = true
						break
					}
				}
			}
			if !active {
				delete(s.epochs, candidate)
				break
			}
		}
	}
	s.nextEpoch++
	s.epochs[profile] = s.nextEpoch
	return s.nextEpoch
}

func catalogID(value, kind string) bool {
	if !token(value, 1024) || kind != "movie" && kind != "series" {
		return false
	}
	imdb := func(id string) bool {
		if !strings.HasPrefix(id, "tt") || len(id) < 3 || len(id) > 18 {
			return false
		}
		positive := false
		for _, r := range id[2:] {
			if r < '0' || r > '9' {
				return false
			}
			if r != '0' {
				positive = true
			}
		}
		return positive
	}
	if imdb(value) {
		return true
	}
	parts := strings.Split(value, ":")
	if len(parts) == 2 && parts[0] == "imdb" {
		return imdb(parts[1])
	}
	if len(parts) == 2 && parts[0] == "tvdb" && kind == "series" {
		n, err := strconv.ParseUint(parts[1], 10, 64)
		return err == nil && n > 0 && strconv.FormatUint(n, 10) == parts[1]
	}
	if len(parts) != 3 || parts[0] != "tmdb" && parts[0] != "tvdb" || parts[1] != kind && !(parts[0] == "tmdb" && kind == "series" && parts[1] == "tv") {
		return false
	}
	if parts[0] == "tmdb" {
		n, err := strconv.ParseUint(parts[2], 10, 64)
		return err == nil && n > 0 && strconv.FormatUint(n, 10) == parts[2]
	}
	if parts[2] == "" {
		return false
	}
	for _, r := range parts[2] {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}
