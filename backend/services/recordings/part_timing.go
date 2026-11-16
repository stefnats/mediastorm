package recordings

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const partTimingBudget = 10 * time.Second
const partTimingTTL = 15 * time.Minute
const partTimingCacheLimit = 128

var partTimingSlots = make(chan struct{}, 2)
var ErrPartTimingBusy = errors.New("recording part timing is busy")

// PartTiming is an observation of one closed container, never a playback
// timeline, exact media duration, immutable-content proof or file capability.
// GET does not revalidate its previously observed file binding.
type PartTiming struct {
	RecordingID         string     `json:"recordingId"`
	PartIndex           int        `json:"partIndex"`
	State               string     `json:"state"`
	Reason              string     `json:"reason,omitempty"`
	ContainerDurationMS *int64     `json:"containerDurationMs,omitempty"`
	ObservedAt          *time.Time `json:"observedAt,omitempty"`
	ExpiresAt           *time.Time `json:"expiresAt,omitempty"`
	Binding             string     `json:"binding,omitempty"`
	ObservedBytes       *int64     `json:"observedBytes,omitempty"`
}
type partTimingEntry struct {
	value  PartTiming
	ledger [32]byte
}
type partTimingState struct {
	mu      sync.Mutex
	path    string
	ctx     context.Context
	cancel  context.CancelFunc
	entries map[string]partTimingEntry
	active  map[string]bool
}

// SetPartTimingProber only configures explicit inspections. It starts no process.
// Reconfiguration retires in-flight work and all old observations.
func (s *Service) SetPartTimingProber(path string) {
	ctx, cancel := context.WithCancel(context.Background())
	state := &partTimingState{path: strings.TrimSpace(path), ctx: ctx, cancel: cancel, entries: map[string]partTimingEntry{}, active: map[string]bool{}}
	s.mu.Lock()
	old := s.partTiming
	s.partTiming = state
	s.mu.Unlock()
	if old != nil {
		old.cancel()
	}
}
func (s *Service) timingState() *partTimingState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.partTiming
}
func (s *Service) currentTimingState(state *partTimingState) bool {
	if state == nil || state.ctx.Err() != nil {
		return false
	}
	s.mu.Lock()
	current := s.partTiming == state
	s.mu.Unlock()
	return current && state.ctx.Err() == nil
}

func unknownPartTiming(id string, index int, reason string) PartTiming {
	return PartTiming{RecordingID: id, PartIndex: index, State: "unknown", Reason: reason}
}
func timingKey(id string, index int) string { return fmt.Sprintf("%s/%d", id, index) }
func (s *Service) timingLedger(ctx context.Context, id string, index int, authorize func() error) ([32]byte, error) {
	var zero [32]byte
	if authorize == nil {
		return zero, ErrRecordingExportAccess
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if err := authorize(); err != nil {
		return zero, err
	}
	r, err := s.exportRecording(ctx, id)
	if err != nil {
		return zero, err
	}
	found := false
	for _, p := range r.Capture.Parts {
		if p.Index == index {
			found = true
		}
	}
	if !found {
		return zero, ErrRecordingExportUnavailable
	}
	fp, err := exportFingerprint(r)
	if err != nil {
		return zero, err
	}
	if err = authorize(); err != nil {
		return zero, err
	}
	if err = ctx.Err(); err != nil {
		return zero, err
	}
	return fp, nil
}

// GetPartTiming reads only the repository and bounded in-memory observations.
// It does not stat/open media, run FFprobe, or refresh an observation's timestamps.
func (s *Service) GetPartTiming(ctx context.Context, id string, index int, authorize func() error) (PartTiming, error) {
	unknown := unknownPartTiming(id, index, "no_observation")
	ledger, err := s.timingLedger(ctx, id, index, authorize)
	if err != nil {
		return unknown, err
	}
	state := s.timingState()
	if state == nil {
		return unknown, nil
	}
	state.mu.Lock()
	entry, exists := state.entries[timingKey(id, index)]
	state.mu.Unlock()
	if !exists {
		return unknown, nil
	}
	if entry.ledger != ledger {
		return unknownPartTiming(id, index, "ledger_changed"), nil
	}
	if !s.currentTimingState(state) || entry.value.ExpiresAt == nil || !time.Now().Before(*entry.value.ExpiresAt) {
		return unknownPartTiming(id, index, "observation_expired"), nil
	}
	current, err := s.timingLedger(ctx, id, index, authorize)
	if err != nil {
		return unknown, err
	}
	if current != ledger {
		return unknownPartTiming(id, index, "ledger_changed"), nil
	}
	if !s.currentTimingState(state) {
		return unknown, ErrRecordingExportUnavailable
	}
	if !time.Now().Before(*entry.value.ExpiresAt) {
		return unknownPartTiming(id, index, "observation_expired"), nil
	}
	return copyPartTiming(entry.value), nil
}

func copyPartTiming(value PartTiming) PartTiming {
	if value.ContainerDurationMS != nil {
		n := *value.ContainerDurationMS
		value.ContainerDurationMS = &n
	}
	if value.ObservedBytes != nil {
		n := *value.ObservedBytes
		value.ObservedBytes = &n
	}
	if value.ObservedAt != nil {
		n := *value.ObservedAt
		value.ObservedAt = &n
	}
	if value.ExpiresAt != nil {
		n := *value.ExpiresAt
		value.ExpiresAt = &n
	}
	return value
}

type timingInput struct {
	file  *os.File
	job   *os.Root
	roots []*os.Root
	name  string
	info  os.FileInfo
}

func (i *timingInput) close() {
	if i.file != nil {
		_ = i.file.Close()
	}
	for n := len(i.roots) - 1; n >= 0; n-- {
		_ = i.roots[n].Close()
	}
}
func (i *timingInput) check() error {
	fd, err := i.file.Stat()
	if err != nil || !fd.Mode().IsRegular() || !os.SameFile(fd, i.info) || fd.Size() != i.info.Size() || !fd.ModTime().Equal(i.info.ModTime()) {
		return ErrRecordingExportUnavailable
	}
	name, err := i.job.Lstat(i.name)
	if err != nil || !name.Mode().IsRegular() || !os.SameFile(fd, name) || name.Size() != i.info.Size() || !name.ModTime().Equal(i.info.ModTime()) {
		return ErrRecordingExportUnavailable
	}
	return nil
}
func (s *Service) openTimingInput(ctx context.Context, id string, index int, check func() error) (out *timingInput, err error) {
	input := &timingInput{}
	defer func() {
		if out == nil {
			input.close()
		}
	}()
	r, err := s.exportRecording(ctx, id)
	if err != nil {
		return nil, err
	}
	if err = check(); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(s.outputDir)
	if err != nil {
		return nil, ErrRecordingExportUnavailable
	}
	before, err := os.Lstat(path)
	if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, ErrRecordingExportUnavailable
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, ErrRecordingExportUnavailable
	}
	input.roots = append(input.roots, root)
	actual, err := root.Stat(".")
	if err != nil || !os.SameFile(before, actual) {
		return nil, ErrRecordingExportUnavailable
	}
	if err = check(); err != nil {
		return nil, err
	}
	profile, err := openExportRoot(root, r.UserID)
	if err != nil {
		return nil, err
	}
	input.roots = append(input.roots, profile)
	if err = check(); err != nil {
		return nil, err
	}
	job, err := openExportRoot(profile, r.ID)
	if err != nil {
		return nil, err
	}
	input.roots = append(input.roots, job)
	input.job = job
	if err = check(); err != nil {
		return nil, err
	}
	input.name = mustExportPartName(index)
	info, err := job.Lstat(input.name)
	if err != nil || !info.Mode().IsRegular() {
		return nil, ErrRecordingExportUnavailable
	}
	input.file, err = job.OpenFile(input.name, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrRecordingExportUnavailable
	}
	input.info, err = input.file.Stat()
	if err != nil || !input.info.Mode().IsRegular() || !os.SameFile(info, input.info) || input.info.Size() <= 0 {
		return nil, ErrRecordingExportUnavailable
	}
	if err = input.check(); err != nil {
		return nil, err
	}
	if err = check(); err != nil {
		return nil, err
	}
	return input, nil
}

type timingOutput struct {
	bytes.Buffer
	limit    int
	overflow bool
}

func (b *timingOutput) Write(p []byte) (int, error) {
	n := len(p)
	remaining := b.limit - b.Len()
	if len(p) > remaining {
		b.overflow = true
		p = p[:remaining]
	}
	_, _ = b.Buffer.Write(p)
	return n, nil
}
func partTimingArgs() []string {
	return []string{"-hide_banner", "-v", "error", "-protocol_whitelist", "fd", "-format_whitelist", "mpegts", "-f", "mpegts", "-fd", "3", "-i", "fd:", "-show_entries", "format=format_name,duration,size:stream=codec_type,codec_name,duration", "-of", "json"}
}

// RefreshPartTiming admits one explicit inspection and retains its slot and FD
// through actual process Wait and authority-watch join. There is no work queue.
func (s *Service) RefreshPartTiming(parent context.Context, id string, index int, authorize func() error) (PartTiming, error) {
	unknown := unknownPartTiming(id, index, "probe_unavailable")
	ledger, err := s.timingLedger(parent, id, index, authorize)
	if err != nil {
		return unknown, err
	}
	state := s.timingState()
	if state == nil || state.path == "" {
		return unknown, nil
	}
	ctx, cancel := context.WithTimeout(parent, partTimingBudget)
	defer cancel()
	stop := context.AfterFunc(state.ctx, cancel)
	defer stop()
	key := timingKey(id, index)
	state.mu.Lock()
	if state.active[key] {
		state.mu.Unlock()
		return unknown, ErrPartTimingBusy
	}
	state.active[key] = true
	state.mu.Unlock()
	defer func() { state.mu.Lock(); delete(state.active, key); state.mu.Unlock() }()
	select {
	case partTimingSlots <- struct{}{}:
		defer func() { <-partTimingSlots }()
	default:
		return unknown, ErrPartTimingBusy
	}
	state.mu.Lock()
	delete(state.entries, key)
	state.mu.Unlock()
	check := func() error {
		if !s.currentTimingState(state) {
			return ErrRecordingExportUnavailable
		}
		current, e := s.timingLedger(ctx, id, index, authorize)
		if e != nil {
			return e
		}
		if current != ledger || !s.currentTimingState(state) {
			return ErrRecordingExportUnavailable
		}
		return ctx.Err()
	}
	input, err := s.openTimingInput(ctx, id, index, check)
	if err != nil {
		return unknown, err
	}
	defer input.close()
	checkAll := func() error {
		if err := check(); err != nil {
			return err
		}
		return input.check()
	}
	if err = checkAll(); err != nil {
		return unknown, err
	}
	cmd := exec.CommandContext(ctx, state.path, partTimingArgs()...)
	cmd.ExtraFiles = []*os.File{input.file}
	cmd.WaitDelay = time.Second
	stdout := &timingOutput{limit: 64 << 10}
	stderr := &timingOutput{limit: 4 << 10}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	done := make(chan struct{})
	watchDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				watchDone <- nil
				return
			case <-ctx.Done():
				watchDone <- ctx.Err()
				return
			case <-ticker.C:
				if e := checkAll(); e != nil {
					cancel()
					watchDone <- e
					return
				}
			}
		}
	}()
	err = cmd.Run()
	close(done)
	watchErr := <-watchDone
	if watchErr != nil {
		return unknown, watchErr
	}
	if e := checkAll(); e != nil {
		return unknown, e
	}
	if err != nil || stdout.overflow {
		return unknown, nil
	}
	duration, valid := parsePartTiming(stdout.Bytes(), input.info.Size())
	if !valid {
		return unknownPartTiming(id, index, "unsupported_container"), nil
	}
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return unknown, err
	}
	observed := time.Now().UTC()
	expires := observed.Add(partTimingTTL)
	size := input.info.Size()
	value := PartTiming{RecordingID: id, PartIndex: index, State: "container_estimate", ContainerDurationMS: &duration, ObservedAt: &observed, ExpiresAt: &expires, Binding: hex.EncodeToString(nonce[:]), ObservedBytes: &size}
	if err = checkAll(); err != nil {
		return unknown, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !s.currentTimingState(state) {
		return unknown, ErrRecordingExportUnavailable
	}
	for k, v := range state.entries {
		if v.value.ExpiresAt == nil || !observed.Before(*v.value.ExpiresAt) {
			delete(state.entries, k)
		}
	}
	if len(state.entries) >= partTimingCacheLimit {
		var oldest string
		var at time.Time
		for k, v := range state.entries {
			if oldest == "" || v.value.ObservedAt.Before(at) {
				oldest = k
				at = *v.value.ObservedAt
			}
		}
		delete(state.entries, oldest)
	}
	state.entries[key] = partTimingEntry{value: copyPartTiming(value), ledger: ledger}
	return value, nil
}
func parsePartTiming(raw []byte, size int64) (int64, bool) {
	var v struct {
		Format struct {
			Name     string `json:"format_name"`
			Duration string `json:"duration"`
			Size     string `json:"size"`
		} `json:"format"`
		Streams []struct {
			Type     string `json:"codec_type"`
			Codec    string `json:"codec_name"`
			Duration string `json:"duration"`
		} `json:"streams"`
	}
	if json.Unmarshal(raw, &v) != nil || v.Format.Name != "mpegts" {
		return 0, false
	}
	bytes, e := strconv.ParseInt(v.Format.Size, 10, 64)
	if e != nil || bytes != size {
		return 0, false
	}
	seconds, e := strconv.ParseFloat(v.Format.Duration, 64)
	if e != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds <= 0 || seconds > float64(math.MaxInt64/1000) {
		return 0, false
	}
	max := float64(0)
	count := 0
	for _, stream := range v.Streams {
		if stream.Type != "video" && stream.Type != "audio" {
			continue
		}
		n, e := strconv.ParseFloat(stream.Duration, 64)
		if stream.Codec == "" || stream.Codec == "unknown" || e != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n > seconds+1 {
			return 0, false
		}
		if n > max {
			max = n
		}
		count++
	}
	if count == 0 || seconds > max+1 {
		return 0, false
	}
	rounded := math.Round(seconds * 1000)
	if rounded >= float64(math.MaxInt64) {
		return 0, false
	}
	ms := int64(rounded)
	return ms, ms > 0
}
