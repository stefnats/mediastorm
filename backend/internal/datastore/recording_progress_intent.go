package datastore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrRecordingProgressChanged     = errors.New("recording progress changed or expired")
	ErrRecordingProgressUnavailable = errors.New("recording progress unavailable")
	ErrRecordingProgressBusy        = errors.New("recording progress busy")
	ErrRecordingProgressLocked      = errors.New("profile locked")
	ErrRecordingProgressUnknown     = errors.New("recording progress commit outcome unknown")
)

// These are DATA from a trusted future concrete GetDerived attachment. They
// never authenticate a request or certify that files/cache are still current.
// The existing twelve-element mapping digest and descriptor TTL are unchanged.
type RecordingProgressObservation struct {
	ArtifactID, MappingSHA256 string
	DurationMS                int64
	ObservedAt, ExpiresAt     time.Time
}

type RecordingProgressAction string

const (
	RecordingProgressFromStart RecordingProgressAction = "from-start"
	RecordingProgressResume    RecordingProgressAction = "resume"
	RecordingProgressTouch     RecordingProgressAction = "touch"
)

type RecordingProgressCommand struct {
	Action                                         RecordingProgressAction
	ExpectedRevision, WriterGeneration, PositionMS int64
}

// RecordingProgressResult contains only acknowledged durable data. A terminal
// chunk is not a programme-completion or watched assertion. Decimal strings
// preserve counters for consumers whose numbers cannot represent int64 exactly.
type RecordingProgressResult struct {
	RecordingID      string `json:"recordingId"`
	Incarnation      string `json:"incarnation"`
	ArtifactID       string `json:"artifactId"`
	MappingSHA256    string `json:"mappingSha256"`
	DurationMS       int64  `json:"durationMs"`
	PositionMS       int64  `json:"positionMs"`
	AtDerivedEnd     bool   `json:"atDerivedEnd"`
	Revision         string `json:"revision"`
	WriterGeneration string `json:"writerGeneration"`
	OperationID      string `json:"operationId"`
}

type RecordingProgressCore struct{ state *recordingProgressCoreState }
type recordingProgressCoreState struct {
	self        *RecordingProgressCore
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
}

// No transport, worker, PG frame or settings policy is fabricated here.
func NewRecordingProgressCore(ds *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*RecordingProgressCore, error) {
	if !ds.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrRecordingProgressUnavailable
	}
	v := &RecordingProgressCore{state: &recordingProgressCoreState{store: ds, manager: manager, security: security, coordinator: coordinator}}
	v.state.self = v
	return v, nil
}
func (c *RecordingProgressCore) valid() bool {
	return c != nil && c.state != nil && c.state.self == c && c.state.store.SettingsCommitReady(c.state.coordinator) && c.state.security.Ready(c.state.manager, c.state.coordinator)
}

var recordingProgressAdmission = struct {
	sync.Mutex
	total    int
	accounts map[string]int
}{accounts: make(map[string]int)}

func admitRecordingProgress(account string) bool {
	recordingProgressAdmission.Lock()
	defer recordingProgressAdmission.Unlock()
	if recordingProgressAdmission.total >= 32 || recordingProgressAdmission.accounts[account] >= 2 {
		return false
	}
	recordingProgressAdmission.total++
	recordingProgressAdmission.accounts[account]++
	return true
}
func releaseRecordingProgress(account string) {
	recordingProgressAdmission.Lock()
	defer recordingProgressAdmission.Unlock()
	recordingProgressAdmission.total--
	recordingProgressAdmission.accounts[account]--
	if recordingProgressAdmission.accounts[account] == 0 {
		delete(recordingProgressAdmission.accounts, account)
	}
}

// The exported value contains no credential-bearing fields, including when
// formatted/copied by value. A copied wrapper cannot exercise or close proof.
type RecordingProgressIntent struct{ state *recordingProgressIntentState }
type recordingProgressIntentState struct {
	self              *RecordingProgressIntent
	core              *RecordingProgressCore
	frame             *pgRequestFrame
	original          models.Session
	identity          RecordingTimelineIdentity
	observation       RecordingProgressObservation
	command           RecordingProgressCommand
	tuple             profilePinAccessTuple
	hashes            []string
	policy            *config.SecurityPolicyObservation
	ctx               context.Context
	cancel            context.CancelFunc
	frameStop         func() bool
	frameDone         chan struct{}
	deadline          time.Time
	mu                sync.Mutex
	consumed, closing bool
	operations        sync.WaitGroup
	done              chan struct{}
}

func (RecordingProgressCore) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (RecordingProgressCore) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("RecordingProgressCore(opaque)"))
}
func (RecordingProgressIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (RecordingProgressIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("RecordingProgressIntent(opaque)"))
}

func progressHex(value string, size int) bool {
	if len(value) != size || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func validProgressData(o RecordingProgressObservation, command RecordingProgressCommand, at time.Time) bool {
	return progressHex(o.ArtifactID, 32) && progressHex(o.MappingSHA256, 64) && o.DurationMS > 0 && o.DurationMS <= 43200000 &&
		!o.ObservedAt.IsZero() && !o.ObservedAt.After(at) && o.ExpiresAt.After(at) && o.ExpiresAt.After(o.ObservedAt) && !o.ExpiresAt.After(o.ObservedAt.Add(24*time.Hour)) &&
		command.ExpectedRevision >= 0 && command.WriterGeneration >= 0 && command.PositionMS >= 0 && command.PositionMS <= o.DurationMS &&
		(command.Action == RecordingProgressTouch || (command.Action == RecordingProgressFromStart || command.Action == RecordingProgressResume) && command.PositionMS == 0)
}

func progressAdult(tuple profilePinAccessTuple) bool {
	var p struct {
		Kids *bool   `json:"is_kids_profile"`
		PIN  *string `json:"pin_hash"`
	}
	return json.Unmarshal(tuple.profile.raw, &p) == nil && p.Kids != nil && !*p.Kids && p.PIN != nil && progressPINHash(*p.PIN)
}

// Cost remains the existing authoritative version/cost parser. This adds only
// complete field grammar; no login-specific version list, cost ceiling or
// unused-bit canonicalization is introduced. Legacy no-minor form is 59 bytes.
func progressPINHash(hash string) bool {
	if hash == "" {
		return true
	}
	if len(hash) != 59 && len(hash) != 60 || hash[0] != '$' || hash[1] == '$' {
		return false
	}
	delimiter := 2
	if hash[2] != '$' {
		delimiter = 3
	}
	tail := delimiter + 4
	if len(hash) != tail+53 || hash[delimiter] != '$' || hash[delimiter+3] != '$' || hash[delimiter+1] < '0' || hash[delimiter+1] > '9' || hash[delimiter+2] < '0' || hash[delimiter+2] > '9' {
		return false
	}
	for _, c := range []byte(hash[tail:]) {
		if c != '.' && c != '/' && !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	_, err := bcrypt.Cost([]byte(hash))
	return err == nil
}

// The access reader's returned Unlocked deadline includes a grant expiry.
// Ordinary DVR permits Locked when enforcement is false, so base operation
// lifetime must instead remain independent of that grant-only deadline.
func progressBaseExpiry(tuple profilePinAccessTuple, original models.Session, deadline time.Time) (time.Time, error) {
	for _, raw := range [][]byte{tuple.actor.raw, tuple.owner.raw} {
		var row struct {
			Expiry *time.Time `json:"expires_at"`
		}
		if !profilePinRequiredFields(raw, []string{"expires_at"}, true) || json.Unmarshal(raw, &row) != nil {
			return time.Time{}, ErrRecordingProgressUnavailable
		}
		deadline = parentMutationMin(deadline, row.Expiry)
	}
	return parentMutationMin(deadline, &original.ExpiresAt), nil
}

// Prepare uses the original normalized full PG request. It does not mint an
// independent HTTP authority, accept a viewer selector, or prepare media. The
// caller must Close even a consumed intent, after its actual work is complete.
func (c *RecordingProgressCore) Prepare(r *http.Request, identity RecordingTimelineIdentity, observation RecordingProgressObservation, command RecordingProgressCommand) (*RecordingProgressIntent, error) {
	if !c.valid() || r == nil || r.URL == nil || r.Method != http.MethodPost || r.URL.RawQuery != "" || len(r.URL.EscapedPath()) > 4096 || !validProgressData(observation, command, time.Now()) || identity.incarnation.String() == "00000000-0000-0000-0000-000000000000" || !parentMutationID(identity.owner) || identity.recordingID == "" || len(identity.recordingID) > 128 {
		return nil, ErrRecordingProgressChanged
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	original, ok := auth.GetSession(r)
	if frame == nil || frame.self != frame || frame.store != c.state.store || frame.ctx.Err() != nil || r.Context().Err() != nil || !ok || original != frame.session || auth.GetAccountID(r) != original.AccountID || auth.IsMaster(r) != original.IsMaster || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrRecordingProgressChanged
	}
	// Parse a fixed header-only credential surface before admission/SQL.
	var tokens []string
	credentialBytes := 0
	for _, value := range r.Header.Values("X-Profile-Unlock") {
		if len(value) > 4096-credentialBytes {
			return nil, ErrRecordingProgressChanged
		}
		credentialBytes += len(value)
		for _, part := range strings.Split(value, ",") {
			if part = strings.TrimSpace(part); part != "" {
				if len(tokens) >= maxProfilePinAccessTokens {
					return nil, ErrRecordingProgressChanged
				}
				tokens = append(tokens, part)
			}
		}
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil {
		return nil, ErrRecordingProgressChanged
	}
	if !admitRecordingProgress(original.AccountID) {
		return nil, ErrRecordingProgressBusy
	}
	deadline := parentMutationMin(time.Now().Add(30*time.Second), &original.ExpiresAt, &observation.ExpiresAt)
	if at, ok := r.Context().Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	s := &recordingProgressIntentState{core: c, frame: frame, original: original, identity: identity, observation: observation, command: command, hashes: hashes, ctx: ctx, cancel: cancel, deadline: deadline, frameDone: make(chan struct{}), done: make(chan struct{})}
	v := &RecordingProgressIntent{state: s}
	s.self = v
	s.frameStop = context.AfterFunc(frame.ctx, func() { cancel(); close(s.frameDone) })
	fail := func() (*RecordingProgressIntent, error) { v.Close(); return nil, ErrRecordingProgressChanged }
	s.policy, err = c.state.security.CapturePolicy(ctx)
	if err != nil {
		return fail()
	}
	current, err := c.state.store.ReadRecordingTimelineIdentity(ctx, identity.recordingID)
	if err != nil || !identity.Same(current) {
		return fail()
	}
	connection, err := c.state.store.pool.Acquire(ctx)
	if err != nil {
		return fail()
	}
	func() {
		defer func() {
			physical := connection.Conn().PgConn()
			if physical.IsClosed() {
				<-physical.CleanupDone()
			}
			connection.Release()
		}()
		s.tuple, _, _, err = readProfilePinAccess(ctx, connection, original, identity.owner, hashes, true)
	}()
	if err != nil || !sameParentMutationRow(frame.sessionRow, s.tuple.session) || !sameParentMutationRow(frame.actorRow, s.tuple.actor) || !progressAdult(s.tuple) {
		return fail()
	}
	bytes := len(s.tuple.session.raw) + len(s.tuple.actor.raw) + len(s.tuple.owner.raw) + len(s.tuple.profile.raw) + len(s.tuple.grants) + len(identity.run) + len(identity.receipt) + len(hashes)*64 + 2048
	if bytes > 64<<10 {
		return fail()
	}
	s.deadline, err = progressBaseExpiry(s.tuple, original, deadline)
	if err != nil || !s.deadline.After(time.Now()) || ctx.Err() != nil || frame.ctx.Err() != nil || r.Context().Err() != nil {
		return fail()
	}
	return v, nil
}

// Close signals first, then joins actual work and the original cancellation
// callback outside the mutex. Every concurrent caller waits for the same end.
func (i *RecordingProgressIntent) Close() {
	if i == nil || i.state == nil || i.state.self != i {
		return
	}
	s := i.state
	s.mu.Lock()
	if s.closing {
		done := s.done
		s.mu.Unlock()
		<-done
		return
	}
	s.closing = true
	s.cancel()
	s.mu.Unlock()
	s.operations.Wait()
	if s.frameStop != nil && !s.frameStop() {
		<-s.frameDone
	}
	releaseRecordingProgress(s.original.AccountID)
	close(s.done)
}
