package recordings

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"novastream/models"
)

var (
	ErrDerivedUnavailable  = errors.New("derived recording unavailable")
	ErrDerivedBusy         = errors.New("derived recording preparation busy")
	ErrDerivedTooLarge     = errors.New("derived recording exceeds resource budget")
	ErrDerivedIncompatible = errors.New("recording parts are incompatible or unverified")
	ErrDerivedAccess       = errors.New("derived recording authority changed")
	derivedSlots           = make(chan struct{}, 1)
	derivedReadSlots       = make(chan struct{}, 2)
	derivedStatusSlots     = make(chan struct{}, 2)
)

const derivedBudget = 300 * time.Second
const derivedInputLimit int64 = 8 << 30
const derivedCacheLimit int64 = 16 << 30
const derivedTTL = 24 * time.Hour

type DerivedTools struct {
	FFmpegPath       string
	FFprobePath      string
	ConfigurationKey string
}
type DerivedPartOffset struct {
	PartIndex  int   `json:"partIndex"`
	StartMS    int64 `json:"startMs"`
	DurationMS int64 `json:"durationMs"`
}

// A derived identifier correlates immutable local media, never an access grant.
type DerivedRecording struct {
	RecordingID      string                 `json:"recordingId"`
	ID               string                 `json:"id,omitempty"`
	State            string                 `json:"state"`
	Reason           string                 `json:"reason,omitempty"`
	DurationMS       int64                  `json:"durationMs,omitempty"`
	Bytes            int64                  `json:"bytes,omitempty"`
	SourceIncomplete bool                   `json:"sourceIncomplete"`
	GapsOmitted      bool                   `json:"gapsOmitted"`
	SourceStatus     models.RecordingStatus `json:"sourceStatus"`
	PartOffsets      []DerivedPartOffset    `json:"partOffsets"`
	ObservedAt       *time.Time             `json:"observedAt,omitempty"`
	ExpiresAt        *time.Time             `json:"expiresAt,omitempty"`
}

func copyDerived(v DerivedRecording) DerivedRecording {
	v.PartOffsets = append([]DerivedPartOffset{}, v.PartOffsets...)
	if v.ObservedAt != nil {
		n := *v.ObservedAt
		v.ObservedAt = &n
	}
	if v.ExpiresAt != nil {
		n := *v.ExpiresAt
		v.ExpiresAt = &n
	}
	return v
}

// DerivedHistoryData is an immutable observation of actual preparation, never
// permission to open media, write progress, renew a TTL or resume an old mapping.
type DerivedHistoryInput struct {
	PartIndex  int
	ByteLength int64
	SHA256     string
}

// Nil tool/transform means unverified transformation evidence even when an
// output digest exists. No legacy DATA classification flag certifies rebind.
type DerivedHistoryData struct {
	RecordingID, ArtifactID                   string
	Inputs                                    []DerivedHistoryInput
	LedgerSHA256                              string
	ToolSHA256, TransformSHA256, OutputSHA256 *string
}

func copyDerivedHistory(v DerivedHistoryData) DerivedHistoryData {
	v.Inputs = append([]DerivedHistoryInput(nil), v.Inputs...)
	clone := func(p *string) *string {
		if p == nil {
			return nil
		}
		s := *p
		return &s
	}
	v.ToolSHA256, v.TransformSHA256, v.OutputSHA256 = clone(v.ToolSHA256), clone(v.TransformSHA256), clone(v.OutputSHA256)
	return v
}

// Private, bounded DATA collector on one already-owned preparation. It carries
// no authority; every actual file/process operation retains the original check.
type derivedHistoryCapture struct {
	tools       [2]string
	unsupported bool
	recipe      string
}

func derivedHistoryDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func hashDerivedHeldFile(file *os.File, limit int64, check func() error) (string, error) {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > limit {
		return "", ErrDerivedUnavailable
	}
	original := derivedVersion{1, info, changeToken(info)}
	hash := sha256.New()
	buffer := make([]byte, 1<<20)
	for offset := int64(0); offset < info.Size(); {
		if err = check(); err != nil {
			return "", err
		}
		count := int64(len(buffer))
		if count > info.Size()-offset {
			count = info.Size() - offset
		}
		n, e := file.ReadAt(buffer[:int(count)], offset)
		if e != nil || int64(n) != count {
			return "", ErrDerivedUnavailable
		}
		_, _ = hash.Write(buffer[:n])
		offset += int64(n)
	}
	after, err := file.Stat()
	if err != nil || !sameDerivedVersion(original, derivedVersion{1, after, changeToken(after)}) {
		return "", ErrDerivedUnavailable
	}
	if err = check(); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func captureDerivedHistory(work *derivedWork, ledger [32]byte, capture *derivedHistoryCapture, check func() error) (DerivedHistoryData, error) {
	value := DerivedHistoryData{ArtifactID: work.id, LedgerSHA256: hex.EncodeToString(ledger[:])}
	if len(work.digests) < 2 || len(work.digests) > 256 || len(work.stageVersions) != len(work.digests) {
		return value, ErrDerivedUnavailable
	}
	for i, digest := range work.digests {
		value.Inputs = append(value.Inputs, DerivedHistoryInput{i + 1, work.stageVersions[i].info.Size(), digest})
	}
	output, err := hashDerivedHeldFile(work.output, derivedCacheLimit, func() error {
		if e := check(); e != nil {
			return e
		}
		return work.checkOutput(work.output)
	})
	if err != nil {
		return DerivedHistoryData{}, err
	}
	value.OutputSHA256 = &output
	if capture != nil && !capture.unsupported && capture.tools[0] != "" && capture.tools[1] != "" && capture.recipe != "" {
		raw, _ := json.Marshal([3]string{"recording-tools/v1", capture.tools[0], capture.tools[1]})
		tool := derivedHistoryDigest(raw)
		value.ToolSHA256 = &tool
		recipe := capture.recipe
		value.TransformSHA256 = &recipe
	}
	return value, nil
}

type derivedState struct {
	ctx         context.Context
	cancel      context.CancelFunc
	tools       DerivedTools
	current     func(context.Context) (DerivedTools, error)
	mu          sync.Mutex
	entries     map[string]*derivedEntry
	generations map[string]uint64
	next        uint64
	owners      map[*derivedOwner]bool
	cleanupDone chan struct{}
}
type derivedOwner struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{}
}
type derivedEntry struct {
	value        DerivedRecording
	history      DerivedHistoryData
	ledger       [32]byte
	versions     []derivedVersion
	work         *derivedWork
	retired      bool
	readers      int
	restoreEpoch uint64
}

// ConfigureDerivedJoiner opens no media and launches no process. Replacement
// cancels and joins all old request owners before returning success.
func (s *Service) ConfigureDerivedJoiner(ctx context.Context, tools DerivedTools, current func(context.Context) (DerivedTools, error)) error {
	if ctx == nil || ctx.Err() != nil {
		return ErrDerivedUnavailable
	}
	tools.FFmpegPath = strings.TrimSpace(tools.FFmpegPath)
	tools.FFprobePath = strings.TrimSpace(tools.FFprobePath)
	life, cancel := context.WithCancel(context.Background())
	state := &derivedState{ctx: life, cancel: cancel, tools: tools, current: current, entries: map[string]*derivedEntry{}, generations: map[string]uint64{}, owners: map[*derivedOwner]bool{}, cleanupDone: make(chan struct{})}
	s.mu.Lock()
	old := s.derived
	s.derived = state
	s.mu.Unlock()
	go state.cleanup()
	if old != nil {
		if err := old.stop(ctx); err != nil {
			state.cancel()
			return err
		}
	}
	return nil
}
func (state *derivedState) cleanup() {
	defer close(state.cleanupDone)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-state.ctx.Done():
			return
		case now := <-ticker.C:
			state.mu.Lock()
			for id, e := range state.entries {
				if e.value.ExpiresAt == nil || !now.Before(*e.value.ExpiresAt) {
					state.retire(e)
					delete(state.entries, id)
				}
			}
			state.mu.Unlock()
		}
	}
}
func (s *Service) derivedState() *derivedState { s.mu.Lock(); defer s.mu.Unlock(); return s.derived }
func (s *Service) derivedCurrent(ctx context.Context, state *derivedState) error {
	if ctx == nil || ctx.Err() != nil || state == nil || state.ctx.Err() != nil || state.current == nil || state.tools.FFmpegPath == "" || state.tools.FFprobePath == "" {
		return ErrDerivedUnavailable
	}
	if s.derivedState() != state {
		return ErrDerivedUnavailable
	}
	if _, ok := derivedRestoreStamp(); !ok {
		return ErrDerivedUnavailable
	}
	tools, err := state.current(ctx)
	if err != nil || tools != state.tools || ctx.Err() != nil {
		return ErrDerivedUnavailable
	}
	return nil
}
func (state *derivedState) owner(id string, cancel context.CancelFunc) (*derivedOwner, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.ctx.Err() != nil {
		return nil, ErrDerivedUnavailable
	}
	o := &derivedOwner{id: id, cancel: cancel, done: make(chan struct{})}
	state.owners[o] = true
	return o, nil
}
func (state *derivedState) release(o *derivedOwner) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.owners[o] {
		delete(state.owners, o)
		close(o.done)
	}
}
func (state *derivedState) retire(entry *derivedEntry) {
	if entry == nil || entry.retired {
		return
	}
	entry.retired = true
	if entry.readers == 0 {
		entry.work.close()
	}
}
func (state *derivedState) stop(ctx context.Context) error {
	state.cancel()
	state.mu.Lock()
	owners := make([]*derivedOwner, 0, len(state.owners))
	for o := range state.owners {
		owners = append(owners, o)
		o.cancel()
	}
	for _, e := range state.entries {
		state.retire(e)
	}
	state.entries = map[string]*derivedEntry{}
	state.mu.Unlock()
	for _, o := range owners {
		select {
		case <-o.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if state.cleanupDone != nil {
		select {
		case <-state.cleanupDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func (s *Service) retireDerivedRecording(ctx context.Context, id string) error {
	state := s.derivedState()
	if state == nil {
		return nil
	}
	state.mu.Lock()
	known := state.entries[id] != nil
	if _, ok := state.generations[id]; ok {
		known = true
	}
	for o := range state.owners {
		if o.id == id {
			known = true
		}
	}
	if !known {
		state.mu.Unlock()
		return nil
	}
	state.next++
	state.generations[id] = state.next
	var owners []*derivedOwner
	for o := range state.owners {
		if o.id == id {
			owners = append(owners, o)
			o.cancel()
		}
	}
	state.retire(state.entries[id])
	delete(state.entries, id)
	state.mu.Unlock()
	for _, o := range owners {
		select {
		case <-o.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func derivedCheck(ctx context.Context, authorize func() error) error {
	if ctx == nil || ctx.Err() != nil || authorize == nil {
		return ErrDerivedAccess
	}
	if authorize() != nil || ctx.Err() != nil {
		return ErrDerivedAccess
	}
	return nil
}
func derivedUnknown(r models.Recording, reason string) DerivedRecording {
	return DerivedRecording{RecordingID: r.ID, State: "unknown", Reason: reason, SourceIncomplete: r.Capture != nil && r.Capture.Incomplete, SourceStatus: r.Status, PartOffsets: []DerivedPartOffset{}}
}
func (s *Service) derivedRecording(ctx context.Context, id string, authorize func() error) (models.Recording, [32]byte, error) {
	var zero [32]byte
	if err := derivedCheck(ctx, authorize); err != nil {
		return models.Recording{}, zero, err
	}
	r, err := s.exportRecording(ctx, id)
	if err != nil {
		return models.Recording{}, zero, ErrDerivedUnavailable
	}
	if err := derivedCheck(ctx, authorize); err != nil {
		return models.Recording{}, zero, err
	}
	fp, err := exportFingerprint(r)
	return r, fp, err
}
func (s *Service) GetDerived(ctx context.Context, id string, authorize func() error) (DerivedRecording, error) {
	value, _, err := s.GetDerivedWithHistoryData(ctx, id, authorize)
	return value, err
}

// GetDerivedWithHistoryData snapshots the same genuine live entry as its media
// descriptor. The returned DATA cannot replace any current authority checks.
func (s *Service) GetDerivedWithHistoryData(ctx context.Context, id string, authorize func() error) (DerivedRecording, DerivedHistoryData, error) {
	r, fp, err := s.derivedRecording(ctx, id, authorize)
	if err != nil {
		return DerivedRecording{}, DerivedHistoryData{}, err
	}
	select {
	case derivedStatusSlots <- struct{}{}:
	default:
		return DerivedRecording{}, DerivedHistoryData{}, ErrDerivedBusy
	}
	defer func() { <-derivedStatusSlots }()
	state := s.derivedState()
	unknown := derivedUnknown(r, "not_prepared")
	if s.derivedCurrent(ctx, state) != nil {
		unknown.Reason = "configuration_changed"
		return unknown, DerivedHistoryData{}, nil
	}
	state.mu.Lock()
	e := state.entries[id]
	if e == nil || e.retired {
		state.mu.Unlock()
		return unknown, DerivedHistoryData{}, nil
	}
	if e.value.ExpiresAt == nil || !time.Now().Before(*e.value.ExpiresAt) {
		state.retire(e)
		delete(state.entries, id)
		state.mu.Unlock()
		unknown.Reason = "expired"
		return unknown, DerivedHistoryData{}, nil
	}
	epoch, restored := derivedRestoreStamp()
	if e.ledger != fp || !restored || epoch != e.restoreEpoch {
		state.retire(e)
		delete(state.entries, id)
		state.mu.Unlock()
		unknown.Reason = "source_changed"
		return unknown, DerivedHistoryData{}, nil
	}
	state.mu.Unlock()
	retireInvalid := func() {
		state.mu.Lock()
		if state.entries[id] == e {
			state.retire(e)
			delete(state.entries, id)
		}
		state.mu.Unlock()
	}
	inputs, err := s.openDerivedInputs(ctx, r, func() error { return derivedCheck(ctx, authorize) })
	if err != nil {
		if errors.Is(err, ErrDerivedAccess) {
			return DerivedRecording{}, DerivedHistoryData{}, err
		}
		retireInvalid()
		unknown.Reason = "source_changed"
		return unknown, DerivedHistoryData{}, nil
	}
	defer inputs.close()
	if !sameDerivedVersions(inputs.versions(), e.versions) || inputs.check() != nil {
		retireInvalid()
		unknown.Reason = "source_changed"
		return unknown, DerivedHistoryData{}, nil
	}
	if e.work.checkOutput(e.work.output) != nil {
		retireInvalid()
		unknown.Reason = "source_changed"
		return unknown, DerivedHistoryData{}, nil
	}
	_, latest, err := s.derivedRecording(ctx, id, authorize)
	if err != nil {
		return DerivedRecording{}, DerivedHistoryData{}, err
	}
	if latest != fp || s.derivedCurrent(ctx, state) != nil {
		retireInvalid()
		unknown.Reason = "source_changed"
		return unknown, DerivedHistoryData{}, nil
	}
	state.mu.Lock()
	valid := !e.retired && state.entries[id] == e
	state.mu.Unlock()
	if !valid {
		return unknown, DerivedHistoryData{}, nil
	}
	return copyDerived(e.value), copyDerivedHistory(e.history), nil
}

// PrepareDerived is an explicit, synchronous request. No worker survives its
// original authority frame, and success never changes the original ledger.
func (s *Service) PrepareDerived(parent context.Context, id string, authorize func() error) (result DerivedRecording, err error) {
	if err = derivedCheck(parent, authorize); err != nil {
		return result, err
	}
	state := s.derivedState()
	if err = s.derivedCurrent(parent, state); err != nil {
		return result, err
	}
	existing, e := s.GetDerived(parent, id, authorize)
	if e != nil {
		return result, e
	}
	if existing.State == "ready" {
		return existing, nil
	}
	select {
	case derivedSlots <- struct{}{}:
		defer func() { <-derivedSlots }()
	default:
		return result, ErrDerivedBusy
	}
	ctx, cancel := context.WithTimeout(parent, derivedBudget)
	defer cancel()
	stop := context.AfterFunc(state.ctx, cancel)
	defer stop()
	owner, err := state.owner(id, cancel)
	if err != nil {
		return result, err
	}
	defer state.release(owner)
	unregister, err := registerCaptureOwner(ctx, cancel)
	if err != nil {
		return result, err
	}
	defer unregister()
	restoreEpoch, restored := derivedRestoreStamp()
	if !restored {
		return result, ErrDerivedUnavailable
	}
	r, fp, err := s.derivedRecording(ctx, id, authorize)
	if err != nil {
		return result, err
	}
	if err = validateDerivedLedger(r); err != nil {
		return result, err
	}
	state.mu.Lock()
	if _, known := state.generations[id]; !known && len(state.generations) >= 1024 {
		for candidate := range state.generations {
			if state.entries[candidate] != nil {
				continue
			}
			active := false
			for owner := range state.owners {
				if owner.id == candidate {
					active = true
					break
				}
			}
			if !active {
				delete(state.generations, candidate)
				break
			}
		}
		if len(state.generations) >= 1024 {
			state.mu.Unlock()
			return result, ErrDerivedBusy
		}
	}
	state.next++
	generation := state.next
	state.generations[id] = generation
	state.mu.Unlock()
	check := func() error {
		if err := derivedCheck(ctx, authorize); err != nil {
			return err
		}
		if s.derivedCurrent(ctx, state) != nil {
			return ErrDerivedUnavailable
		}
		epoch, ok := derivedRestoreStamp()
		if !ok || epoch != restoreEpoch {
			return ErrDerivedUnavailable
		}
		_, current, err := s.derivedRecording(ctx, id, authorize)
		if err != nil {
			return err
		}
		if current != fp {
			return ErrDerivedUnavailable
		}
		state.mu.Lock()
		currentGeneration := state.generations[id]
		state.mu.Unlock()
		if currentGeneration != generation {
			return ErrDerivedUnavailable
		}
		return ctx.Err()
	}
	inputs, err := s.openDerivedInputs(ctx, r, check)
	if err != nil {
		return result, err
	}
	defer inputs.close()
	work, err := s.newDerivedWork(check)
	if err != nil {
		return result, err
	}
	published := false
	defer func() {
		if !published {
			work.close()
		}
	}()
	checkAll := func() error {
		if err := check(); err != nil {
			return err
		}
		if err := inputs.check(); err != nil {
			return err
		}
		return work.checkStaging()
	}
	if err = work.stage(ctx, inputs, checkAll); err != nil {
		return result, err
	}
	capture := &derivedHistoryCapture{}
	work.history = capture
	media := make([]derivedMedia, 0, len(inputs.files))
	var totalMS int64
	offsets := []DerivedPartOffset{}
	for index := range inputs.files {
		if err = checkAll(); err != nil {
			return result, err
		}
		facts, e := probeDerivedMedia(ctx, state.tools.FFprobePath, work, index+1, checkAll)
		if e != nil {
			return result, e
		}
		if len(media) > 0 && !compatibleDerived(media[0], facts) {
			return result, ErrDerivedIncompatible
		}
		if facts.durationMS <= 0 || totalMS > 12*60*60*1000-facts.durationMS {
			return result, ErrDerivedTooLarge
		}
		offsets = append(offsets, DerivedPartOffset{PartIndex: index + 1, StartMS: totalMS, DurationMS: facts.durationMS})
		totalMS += facts.durationMS
		media = append(media, facts)
	}
	if err = work.remux(ctx, state.tools.FFmpegPath, media, checkAll); err != nil {
		return result, err
	}
	output, err := probeDerivedOutput(ctx, state.tools.FFprobePath, work, checkAll)
	if err != nil {
		return result, err
	}
	if !validDerivedOutput(media, output, totalMS) {
		return result, ErrDerivedIncompatible
	}
	last := len(offsets) - 1
	if output.durationMS <= offsets[last].StartMS {
		return result, ErrDerivedIncompatible
	}
	// These offsets describe the derived container within its validated frame
	// precision; the final end is the freshly measured derived end.
	offsets[last].DurationMS = output.durationMS - offsets[last].StartMS
	if err = checkAll(); err != nil {
		return result, err
	}
	if err = work.publish(checkAll); err != nil {
		return result, err
	}
	history, historyErr := captureDerivedHistory(work, fp, capture, checkAll)
	if historyErr != nil {
		return DerivedRecording{}, historyErr
	}
	history.RecordingID = id
	now := time.Now().UTC()
	expiry := now.Add(derivedTTL)
	result = DerivedRecording{RecordingID: id, ID: work.id, State: "ready", DurationMS: output.durationMS, Bytes: work.outputInfo.Size(), SourceIncomplete: r.Capture.Incomplete, GapsOmitted: len(r.Capture.Gaps) > 0, SourceStatus: r.Status, PartOffsets: offsets, ObservedAt: &now, ExpiresAt: &expiry}
	if result.GapsOmitted {
		result.SourceIncomplete = true
	}
	entry := &derivedEntry{value: copyDerived(result), history: copyDerivedHistory(history), ledger: fp, versions: inputs.versions(), work: work, restoreEpoch: restoreEpoch}
	if err = checkAll(); err != nil {
		return DerivedRecording{}, err
	}
	state.mu.Lock()
	if state.ctx.Err() != nil || state.generations[id] != generation {
		state.mu.Unlock()
		return DerivedRecording{}, ErrDerivedUnavailable
	}
	var bytes int64
	for _, e := range state.entries {
		bytes += e.value.Bytes
	}
	for key, e := range state.entries {
		if e.value.ExpiresAt == nil || !now.Before(*e.value.ExpiresAt) {
			state.retire(e)
			delete(state.entries, key)
			bytes -= e.value.Bytes
		}
	}
	if len(state.entries) >= 64 || bytes+result.Bytes > derivedCacheLimit {
		state.mu.Unlock()
		return DerivedRecording{}, ErrDerivedTooLarge
	}
	state.retire(state.entries[id])
	state.entries[id] = entry
	state.mu.Unlock()
	published = true
	work.removeStaging()
	return copyDerived(result), nil
}

func validateDerivedLedger(r models.Recording) error {
	c := r.Capture
	if !validCaptureParts(c) || len(c.Parts) < 2 || len(c.Parts) > models.MaxRecordingParts {
		return ErrDerivedUnavailable
	}
	parts := append([]models.RecordingPart{}, c.Parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Index < parts[j].Index })
	for i, p := range parts {
		if p.Index != i+1 || p.Bytes <= 0 || !capturePartClosed(&p) || !closedExportReason(p.EndReason) {
			return ErrDerivedUnavailable
		}
		if i > 0 && p.StartedAt.Before(*parts[i-1].EndedAt) {
			return ErrDerivedIncompatible
		}
	}
	for _, g := range c.Gaps {
		if g.StartedAt.IsZero() || g.EndedAt == nil || g.EndedAt.Before(g.StartedAt) || (g.Reason != "capacity" && g.Reason != "source_retry") {
			return ErrDerivedUnavailable
		}
	}
	// Every nontrivial inter-part gap needs an explicit closed ledger interval.
	for i := 1; i < len(parts); i++ {
		end, start := *parts[i-1].EndedAt, parts[i].StartedAt
		if start.Sub(end) <= 250*time.Millisecond {
			continue
		}
		covered := false
		for _, g := range c.Gaps {
			if !g.StartedAt.After(end.Add(250*time.Millisecond)) && !g.EndedAt.Before(start.Add(-250*time.Millisecond)) {
				covered = true
			}
		}
		if !covered {
			return ErrDerivedUnavailable
		}
	}
	return nil
}
func newDerivedID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
func validDerivedID(id string) bool {
	if len(id) != 32 || strings.ToLower(id) != id {
		return false
	}
	b, e := hex.DecodeString(id)
	return e == nil && len(b) == 16
}

type DerivedReader struct {
	ctx        context.Context
	cancel     context.CancelFunc
	check      func() error
	file       *os.File
	entry      *derivedEntry
	state      *derivedState
	inputs     *derivedInputs
	once       sync.Once
	stop       func() bool
	unregister func()
	owner      *derivedOwner
	stopState  func() bool
	watchDone  chan struct{}
}

func (r *DerivedReader) Descriptor() DerivedRecording { return copyDerived(r.entry.value) }

// Context also bounds a blocked network writer after source/config revocation.
func (r *DerivedReader) Context() context.Context { return r.ctx }
func (r *DerivedReader) Read(p []byte) (int, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	n, err := r.file.Read(p)
	if e := r.check(); e != nil {
		clear(p[:n])
		return 0, e
	}
	return n, err
}
func (r *DerivedReader) Seek(offset int64, whence int) (int64, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	n, err := r.file.Seek(offset, whence)
	if e := r.check(); e != nil {
		return 0, e
	}
	return n, err
}
func (r *DerivedReader) Close() error {
	var err error
	r.once.Do(func() {
		if r.stop != nil {
			r.stop()
		}
		if r.stopState != nil {
			r.stopState()
		}
		r.cancel()
		err = r.file.Close()
		if r.watchDone != nil {
			<-r.watchDone
		}
		r.inputs.close()
		r.state.mu.Lock()
		r.entry.readers--
		if r.entry.retired && r.entry.readers == 0 {
			r.entry.work.close()
		}
		r.state.mu.Unlock()
		r.unregister()
		r.state.release(r.owner)
		<-derivedReadSlots
	})
	return err
}
func (s *Service) OpenDerived(parent context.Context, id, derivedID string, authorize func() error) (reader *DerivedReader, err error) {
	if !validDerivedID(derivedID) {
		return nil, ErrDerivedUnavailable
	}
	r, fp, err := s.derivedRecording(parent, id, authorize)
	if err != nil {
		return nil, err
	}
	state := s.derivedState()
	if s.derivedCurrent(parent, state) != nil {
		return nil, ErrDerivedUnavailable
	}
	select {
	case derivedReadSlots <- struct{}{}:
	default:
		return nil, ErrDerivedBusy
	}
	defer func() {
		if reader == nil {
			<-derivedReadSlots
		}
	}()
	ctx, cancel := context.WithCancel(parent)
	owner, err := state.owner(id, cancel)
	if err != nil {
		cancel()
		return nil, err
	}
	release, err := registerCaptureOwner(ctx, cancel)
	if err != nil {
		cancel()
		state.release(owner)
		return nil, err
	}
	state.mu.Lock()
	e := state.entries[id]
	epoch, restored := derivedRestoreStamp()
	if e == nil || e.retired || e.value.ID != derivedID || e.ledger != fp || !restored || e.restoreEpoch != epoch || e.value.ExpiresAt == nil || !time.Now().Before(*e.value.ExpiresAt) {
		state.mu.Unlock()
		cancel()
		release()
		state.release(owner)
		return nil, ErrDerivedUnavailable
	}
	e.readers++
	state.mu.Unlock()
	var inputs *derivedInputs
	var file *os.File
	defer func() {
		if reader == nil {
			if file != nil {
				file.Close()
			}
			if inputs != nil {
				inputs.close()
			}
			cancel()
			release()
			state.release(owner)
			state.mu.Lock()
			e.readers--
			if e.retired && e.readers == 0 {
				e.work.close()
			}
			state.mu.Unlock()
		}
	}()
	check := func() error {
		if derivedCheck(ctx, authorize) != nil {
			return ErrDerivedAccess
		}
		if s.derivedCurrent(ctx, state) != nil {
			return ErrDerivedUnavailable
		}
		currentEpoch, ok := derivedRestoreStamp()
		if !ok || currentEpoch != epoch {
			return ErrDerivedUnavailable
		}
		_, latest, err := s.derivedRecording(ctx, id, authorize)
		if err != nil {
			return err
		}
		if latest != fp {
			return ErrDerivedUnavailable
		}
		state.mu.Lock()
		valid := !e.retired && state.entries[id] == e
		state.mu.Unlock()
		if !valid || !time.Now().Before(*e.value.ExpiresAt) {
			return ErrDerivedUnavailable
		}
		if inputs != nil && (inputs.check() != nil || !sameDerivedVersions(inputs.versions(), e.versions)) {
			return ErrDerivedUnavailable
		}
		if file != nil {
			return e.work.checkOutput(file)
		}
		return nil
	}
	inputs, err = s.openDerivedInputs(ctx, r, check)
	if err != nil {
		return nil, err
	}
	file, err = e.work.root.OpenFile("joined.mkv", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, ErrDerivedUnavailable
	}
	if err = check(); err != nil {
		return nil, err
	}
	reader = &DerivedReader{ctx: ctx, cancel: cancel, check: check, file: file, entry: e, state: state, inputs: inputs, unregister: release, owner: owner}
	// Closing a blocked read is independent of the caller's next Read.
	reader.stop = context.AfterFunc(ctx, func() { _ = file.Close() })
	reader.stopState = context.AfterFunc(state.ctx, cancel)
	reader.watchDone = make(chan struct{})
	go func() {
		defer close(reader.watchDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = file.Close()
				return
			case <-ticker.C:
				if check() != nil {
					cancel()
					_ = file.Close()
					return
				}
			}
		}
	}()
	return reader, nil
}

var _ io.ReadSeekCloser = (*DerivedReader)(nil)
