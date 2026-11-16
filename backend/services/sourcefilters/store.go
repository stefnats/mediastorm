package sourcefilters

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"novastream/models"
	"novastream/services/mediaevidence"
)

const MaxCandidates = 512
const MaxPageSize = 100
const snapshotTTL = 15 * time.Minute
const MaxSnapshotBytes = 8 << 20
const maxSnapshotBytes = MaxSnapshotBytes
const maxStoreBytes = 32 << 20
const maxSnapshots = 64
const maxViews = 4

var (
	ErrUnavailable     = errors.New("source_snapshot_unavailable")
	ErrSnapshotExpired = errors.New("source_snapshot_expired")
	ErrContextChanged  = errors.New("source_snapshot_context_changed")
	ErrViewChanged     = errors.New("source_view_changed")
	ErrViewExpired     = errors.New("source_view_expired")
)

type Scope struct{ AccountID, ProfileID, ClientID, Signature string }
type Candidate struct {
	Result   models.NZBResult
	Raw      json.RawMessage
	Evidence *models.MediaEvidence
}
type Row struct {
	SourceIndex      int                   `json:"sourceIndex"`
	CandidateVersion string                `json:"candidateVersion"`
	Result           json.RawMessage       `json:"result"`
	Verdict          string                `json:"verdict"`
	MediaEvidence    *models.MediaEvidence `json:"mediaEvidence,omitempty"`
}
type Coverage struct {
	RetainedCandidates int  `json:"retainedCandidates"`
	Truncated          bool `json:"truncated"`
}
type Page struct {
	Version           int       `json:"sourceFiltersVersion"`
	SnapshotID        string    `json:"snapshotId"`
	ViewID            string    `json:"viewId"`
	ExpiresAt         time.Time `json:"expiresAt"`
	Target            Target    `json:"target"`
	Filters           Filters   `json:"filters"`
	Rows              []Row     `json:"rows"`
	Total             int       `json:"total"`
	MatchedTotal      int       `json:"matchedTotal"`
	UnknownTotal      int       `json:"unknownTotal"`
	NextCursor        string    `json:"nextCursor,omitempty"`
	Coverage          Coverage  `json:"coverage"`
	SourceHealthToken string    `json:"sourceHealthToken,omitempty"`
}
type storedCandidate struct {
	Candidate
	version    string
	generation uint64
}
type view struct {
	id               string
	generation       uint64
	filters          Filters
	expires          time.Time
	rows             []Row
	matched, unknown int
}
type snapshot struct {
	id          string
	scope       Scope
	target      Target
	expires     time.Time
	created     time.Time
	rows        []storedCandidate
	truncated   bool
	bytes       int
	generation  uint64
	views       []*view
	healthToken string
}
type Store struct {
	mu        sync.Mutex
	snapshots map[string]*snapshot
	now       func() time.Time
}

func NewStore() *Store { return NewStoreWithClock(time.Now) }

// NewStoreWithClock makes expiry deterministic for embedded consumers and tests.
// The clock is a server dependency, never supplied through HTTP.
func NewStoreWithClock(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{snapshots: map[string]*snapshot{}, now: now}
}
func token() string {
	var value [24]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic("random source unavailable")
	}
	return hex.EncodeToString(value[:])
}
func copyFilters(value Filters) Filters {
	if value.HeightAtLeast != nil {
		v := *value.HeightAtLeast
		value.HeightAtLeast = &v
	}
	if value.AudioDescription != nil {
		v := *value.AudioDescription
		value.AudioDescription = &v
	}
	return value
}
func copyTarget(value Target) Target {
	data, _ := json.Marshal(value)
	var copy Target
	_ = json.Unmarshal(data, &copy)
	return copy
}
func copyResult(value models.NZBResult) models.NZBResult {
	value.Categories = append([]string(nil), value.Categories...)
	if value.Attributes != nil {
		attrs := make(map[string]string, len(value.Attributes))
		for k, v := range value.Attributes {
			attrs[k] = v
		}
		value.Attributes = attrs
	}
	return value
}
func sameOwner(a, b Scope) bool {
	return a.AccountID == b.AccountID && a.ProfileID == b.ProfileID && a.ClientID == b.ClientID
}
func (s *Store) lookup(id string, scope Scope) (*snapshot, error) {
	v := s.snapshots[id]
	if v == nil || !sameOwner(v.scope, scope) {
		return nil, ErrUnavailable
	}
	if !v.expires.After(s.now()) {
		return nil, ErrSnapshotExpired
	}
	if scope.Signature != "" && scope.Signature != v.scope.Signature {
		return nil, ErrContextChanged
	}
	return v, nil
}

// Create copies server-owned candidates. Safety bounds do not cause additional
// provider pages; coverage explicitly describes the retained subset only.
func (s *Store) Create(scope Scope, target Target, candidates []Candidate, truncated bool, healthToken string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	v := &snapshot{id: token(), scope: scope, target: copyTarget(target), created: now, expires: now.Add(snapshotTTL), truncated: truncated, healthToken: healthToken}
	for _, candidate := range candidates {
		if len(v.rows) >= MaxCandidates {
			v.truncated = true
			break
		}
		size := len(candidate.Raw)
		facts, _ := json.Marshal(candidate.Evidence)
		size += len(facts)
		if size == 0 || size > maxSnapshotBytes || v.bytes+size > maxSnapshotBytes {
			v.truncated = true
			break
		}
		candidate.Result = copyResult(candidate.Result)
		candidate.Raw = append(json.RawMessage(nil), candidate.Raw...)
		candidate.Evidence = models.CopyMediaEvidence(candidate.Evidence)
		v.rows = append(v.rows, storedCandidate{Candidate: candidate, version: mediaevidence.CandidateVersion(candidate.Result)})
		v.bytes += size
	}
	for id, old := range s.snapshots {
		if !old.expires.After(now) {
			delete(s.snapshots, id)
		}
	}
	for {
		total := v.bytes
		var oldest *snapshot
		for _, old := range s.snapshots {
			total += old.bytes
			if oldest == nil || old.created.Before(oldest.created) {
				oldest = old
			}
		}
		if len(s.snapshots) < maxSnapshots && total <= maxStoreBytes {
			break
		}
		if oldest == nil {
			break
		}
		delete(s.snapshots, oldest.id)
	}
	s.snapshots[v.id] = v
	return v.id
}

// Target reveals only request correlation after the account/profile/device
// check. The handler must then compute a current scope before Page/Resolve.
func (s *Store) Target(id string, scope Scope) (Target, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.lookup(id, scope)
	if err != nil {
		return Target{}, err
	}
	return copyTarget(v.target), nil
}

func (s *Store) Page(id string, scope Scope, filters Filters, cursor string, limit int) (Page, error) {
	if err := filters.Validate(); err != nil {
		return Page{}, err
	}
	if limit < 1 || limit > MaxPageSize {
		return Page{}, errors.New("invalid page limit")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.lookup(id, scope)
	if err != nil {
		return Page{}, err
	}
	now := s.now()
	var current *view
	offset := 0
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || len(decoded) > 1024 {
			return Page{}, ErrViewChanged
		}
		parts := strings.Split(string(decoded), ":")
		if len(parts) != 2 {
			return Page{}, ErrViewChanged
		}
		offset, err = strconv.Atoi(parts[1])
		if err != nil || offset < 0 || strconv.Itoa(offset) != parts[1] {
			return Page{}, ErrViewChanged
		}
		for _, item := range v.views {
			if item.id == parts[0] {
				current = item
				break
			}
		}
		if current == nil || current.generation != v.generation {
			return Page{}, ErrViewChanged
		}
		if !current.expires.After(now) {
			return Page{}, ErrViewExpired
		}
		a, _ := json.Marshal(filters)
		b, _ := json.Marshal(current.filters)
		if string(a) != string(b) || offset >= len(current.rows) {
			return Page{}, ErrViewChanged
		}
	} else {
		current = &view{id: token(), generation: v.generation, filters: copyFilters(filters), expires: v.expires, rows: []Row{}}
		for index, candidate := range v.rows {
			evidence := models.CurrentMediaEvidence(candidate.Evidence, now)
			if evidence != nil && candidate.Evidence != nil && (evidence.Candidate == nil) == (candidate.Evidence.Candidate == nil) && (evidence.Probed == nil) == (candidate.Evidence.Probed == nil) {
				evidence = candidate.Evidence
			}
			if evidence != nil && (evidence.Probed != nil && evidence.CandidateVersion != candidate.version || evidence.CandidateVersion != "" && evidence.CandidateVersion != candidate.version || evidence.Probed == nil && evidence.SourceVersion != candidate.version) {
				evidence = nil
			}
			verdict := Evaluate(filters, evidence, v.target, now)
			if verdict == "match" {
				current.matched++
			}
			if verdict == "unknown" {
				current.unknown++
			}
			if evidence != nil {
				for _, expiry := range evidenceExpiries(evidence) {
					if expiry.Before(current.expires) {
						current.expires = expiry
					}
				}
			}
			if verdict == "no_match" || verdict == "unknown" && filters.Unknown == "exclude" {
				continue
			}
			current.rows = append(current.rows, Row{SourceIndex: index, CandidateVersion: candidate.version, Result: candidate.Raw, Verdict: verdict, MediaEvidence: evidence})
		}
		v.views = append(v.views, current)
		if len(v.views) > maxViews {
			v.views = v.views[len(v.views)-maxViews:]
		}
	}
	end := offset + limit
	if end > len(current.rows) {
		end = len(current.rows)
	}
	rows := make([]Row, 0, end-offset)
	for _, row := range current.rows[offset:end] {
		row.Result = append(json.RawMessage(nil), row.Result...)
		row.MediaEvidence = models.CopyMediaEvidence(row.MediaEvidence)
		rows = append(rows, row)
	}
	result := Page{Version: 1, SnapshotID: id, ViewID: current.id, ExpiresAt: current.expires, Target: copyTarget(v.target), Filters: copyFilters(filters), Rows: rows, Total: len(current.rows), MatchedTotal: current.matched, UnknownTotal: current.unknown, Coverage: Coverage{len(v.rows), v.truncated}, SourceHealthToken: v.healthToken}
	if end < len(current.rows) {
		result.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(current.id + ":" + strconv.Itoa(end)))
	}
	return result, nil
}
func evidenceExpiries(value *models.MediaEvidence) []time.Time {
	var times []time.Time
	if value.Candidate != nil {
		times = append(times, value.Candidate.ExpiresAt)
	}
	if value.Probed != nil {
		times = append(times, value.Probed.ExpiresAt)
	}
	return times
}

// ObserveExisting accepts already captured preparation facts, never initiates a
// preparation. Identical copies are a no-op and retain their original times.
func (s *Store) ObserveExisting(id string, scope Scope, values map[string]*models.MediaEvidence) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.lookup(id, scope)
	if err != nil {
		return
	}
	changed := false
	for i := range v.rows {
		row := &v.rows[i]
		value := models.CurrentMediaEvidence(values[row.version], s.now())
		if value == nil || value.Probed == nil || value.CandidateVersion != row.version || !ExactEpisodeBinding(v.target, value) {
			continue
		}
		if previous := row.Evidence; previous != nil && previous.Probed != nil && previous.Probed.ObservedAt.After(value.Probed.ObservedAt) {
			continue
		}
		a, _ := json.Marshal(row.Evidence)
		b, _ := json.Marshal(value)
		if string(a) == string(b) {
			continue
		}
		row.Evidence = value
		row.generation++
		changed = true
	}
	if changed {
		v.generation++
	}
}

// Resolve returns the immutable server candidate and a generation receipt.
// A late explicit probe can never replace a newer observation for this row.
func (s *Store) Resolve(id string, index int, scope Scope) (models.NZBResult, string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.lookup(id, scope)
	if err != nil {
		return models.NZBResult{}, "", 0, err
	}
	if index < 0 || index >= len(v.rows) {
		return models.NZBResult{}, "", 0, ErrUnavailable
	}
	row := &v.rows[index]
	row.generation++
	return copyResult(row.Result), row.version, row.generation, nil
}
func (s *Store) Attach(id string, index int, scope Scope, version string, generation uint64, evidence *models.MediaEvidence) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.lookup(id, scope)
	if err != nil || index < 0 || index >= len(v.rows) {
		return false
	}
	row := &v.rows[index]
	evidence = models.CurrentMediaEvidence(evidence, s.now())
	if generation != row.generation || version != row.version || evidence == nil || evidence.Probed == nil || evidence.CandidateVersion != version {
		return false
	}
	row.Evidence = models.CopyMediaEvidence(evidence)
	row.generation++
	v.generation++
	return true
}
