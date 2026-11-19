package recordings

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"novastream/models"
)

type derivedNumber string

func (n *derivedNumber) UnmarshalJSON(b []byte) error {
	var s string
	if len(b) > 0 && b[0] == '"' {
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
	} else {
		s = string(b)
	}
	*n = derivedNumber(s)
	return nil
}
func (n derivedNumber) integer() (int64, bool) {
	v, e := strconv.ParseInt(string(n), 10, 64)
	return v, e == nil
}

type derivedStream struct {
	Index          int    `json:"index"`
	StreamID       string `json:"id"`
	Type           string `json:"codec_type"`
	Codec          string `json:"codec_name"`
	Profile        string `json:"profile"`
	Level          int    `json:"level"`
	Reorder        *int   `json:"has_b_frames"`
	TimeBase       string `json:"time_base"`
	Width          int    `json:"width"`
	Height         int    `json:"height"`
	Pixel          string `json:"pix_fmt"`
	Aspect         string `json:"sample_aspect_ratio"`
	Rate           string `json:"r_frame_rate"`
	FieldOrder     string `json:"field_order"`
	ColorRange     string `json:"color_range"`
	ColorSpace     string `json:"color_space"`
	ColorTransfer  string `json:"color_transfer"`
	ColorPrimaries string `json:"color_primaries"`
	SampleRate     string `json:"sample_rate"`
	SampleFormat   string `json:"sample_fmt"`
	Channels       int    `json:"channels"`
	Layout         string `json:"channel_layout"`
	Extra          string `json:"extradata_hash"`
	Tags           struct {
		Language string `json:"language"`
	} `json:"tags"`
	Disposition struct {
		Default int `json:"default"`
		Forced  int `json:"forced"`
	} `json:"disposition"`
}
type derivedMedia struct {
	streams    []derivedStream
	durationMS int64
	packets    map[int]int
	frames     map[int]int
	gaps       map[int][]derivedPacketGap
	frameGaps  map[int][]derivedPacketGap
}
type derivedPacketGap struct {
	ordinal int
	seconds float64
}

func rational(value string) (*big.Rat, bool) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return nil, false
	}
	a, e := strconv.ParseInt(parts[0], 10, 64)
	b, f := strconv.ParseInt(parts[1], 10, 64)
	if e != nil || f != nil || a <= 0 || b <= 0 || a > 1_000_000_000 || b > 1_000_000_000 || strconv.FormatInt(a, 10) != parts[0] || strconv.FormatInt(b, 10) != parts[1] {
		return nil, false
	}
	return new(big.Rat).SetFrac64(a, b), true
}
func normalizedUnknown(s string) string {
	if s == "unknown" || s == "N/A" || s == "unspecified" {
		return ""
	}
	return s
}
func normalizeDerivedStream(s derivedStream) derivedStream {
	s.ColorRange = normalizedUnknown(s.ColorRange)
	s.ColorSpace = normalizedUnknown(s.ColorSpace)
	s.ColorTransfer = normalizedUnknown(s.ColorTransfer)
	s.ColorPrimaries = normalizedUnknown(s.ColorPrimaries)
	s.Tags.Language = strings.ToLower(s.Tags.Language)
	if s.Tags.Language == "und" {
		s.Tags.Language = ""
	}
	if len(s.Tags.Language) == 2 || len(s.Tags.Language) == 3 {
		if canonical := models.NormalizeEvidenceLanguage(s.Tags.Language); canonical != "" {
			s.Tags.Language = canonical
		}
	}
	return s
}
func validDerivedReorder(s derivedStream) bool {
	return s.Type == "video" && s.Codec == "h264" && s.Reorder != nil && *s.Reorder >= 0 && *s.Reorder <= 16
}
func validDerivedStreams(streams []derivedStream) bool {
	if len(streams) < 2 || len(streams) > 9 {
		return false
	}
	video, audio := 0, 0
	seen := map[int]bool{}
	streamIDs := map[string]bool{}
	for _, s := range streams {
		if s.Index < 0 || seen[s.Index] {
			return false
		}
		seen[s.Index] = true
		pid, e := strconv.ParseUint(strings.TrimPrefix(s.StreamID, "0x"), 16, 16)
		if !strings.HasPrefix(s.StreamID, "0x") || e != nil || pid == 0 || pid > 8191 || streamIDs[s.StreamID] {
			return false
		}
		streamIDs[s.StreamID] = true
		if _, ok := rational(s.TimeBase); !ok {
			return false
		}
		if s.Type == "video" {
			video++
			hash, hashErr := hex.DecodeString(strings.TrimPrefix(s.Extra, "SHA256:"))
			aspect, aspectOK := rational(strings.ReplaceAll(s.Aspect, ":", "/"))
			if !validDerivedReorder(s) || s.Profile == "" || s.Level <= 0 || s.Level > 51 || s.Width <= 0 || s.Height <= 0 || s.Width > 4096 || s.Height > 2160 || s.FieldOrder != "progressive" || normalizedUnknown(s.Pixel) == "" || !aspectOK || aspect == nil || !strings.HasPrefix(s.Extra, "SHA256:") || hashErr != nil || len(hash) != 32 {
				return false
			}
			if _, ok := rational(s.Rate); !ok {
				return false
			}
		} else if s.Type == "audio" {
			audio++
			rate, e := strconv.Atoi(s.SampleRate)
			if s.Codec != "aac" || s.Profile != "LC" || e != nil || rate < 8000 || rate > 192000 || normalizedUnknown(s.SampleFormat) == "" || s.Channels < 1 || s.Channels > 8 || normalizedUnknown(s.Layout) == "" {
				return false
			}
		} else {
			return false
		}
		if s.Disposition.Default < 0 || s.Disposition.Default > 1 || s.Disposition.Forced < 0 || s.Disposition.Forced > 1 || len(s.Tags.Language) > 16 {
			return false
		}
		for _, ch := range s.Tags.Language {
			if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch == '-' || ch == '_') {
				return false
			}
		}
	}
	return video == 1 && audio >= 1 && audio <= 8
}
func compatibleDerived(a, b derivedMedia) bool { return reflect.DeepEqual(a.streams, b.streams) }
func validDerivedOutput(inputs []derivedMedia, out derivedMedia, total int64) bool {
	if len(inputs) == 0 || len(out.streams) != len(inputs[0].streams) || out.durationMS <= 0 {
		return false
	}
	// Container muxing changes timebase and may remove codec-container details.
	for i, s := range out.streams {
		a := inputs[0].streams[i]
		if a.Type == "video" && (!validDerivedReorder(a) || !validDerivedReorder(s) || *a.Reorder != *s.Reorder) {
			return false
		}
		if a.Type != s.Type || a.Codec != s.Codec || a.Profile != s.Profile || a.Level != s.Level || a.Width != s.Width || a.Height != s.Height || a.Pixel != s.Pixel || a.Aspect != s.Aspect || a.FieldOrder != s.FieldOrder || a.ColorRange != s.ColorRange || a.ColorSpace != s.ColorSpace || a.ColorTransfer != s.ColorTransfer || a.ColorPrimaries != s.ColorPrimaries || a.Channels != s.Channels || a.Layout != s.Layout || a.SampleRate != s.SampleRate || a.SampleFormat != s.SampleFormat || a.Tags != s.Tags || a.Disposition != s.Disposition {
			return false
		}
		count := 0
		for _, part := range inputs {
			count += part.frames[part.streams[i].Index]
		}
		if out.frames[s.Index] != count {
			return false
		}
		boundaries := map[int]int{}
		packets := 0
		boundaryFrames := 0
		for partIndex, part := range inputs {
			packets += part.packets[part.streams[i].Index]
			boundaryFrames += part.frames[part.streams[i].Index]
			if partIndex < len(inputs)-1 {
				boundaries[packets] = boundaryFrames
			}
		}
		if out.packets[s.Index] != packets {
			return false
		}
		if !validDerivedDTSBoundaries(s, out.gaps[s.Index], out.frameGaps[s.Index], boundaries) {
			return false
		}
		frameBoundaries := map[int]bool{}
		frames := 0
		for partIndex, part := range inputs {
			frames += part.frames[part.streams[i].Index]
			if partIndex < len(inputs)-1 {
				frameBoundaries[frames] = true
			}
		}
		for _, gap := range out.frameGaps[s.Index] {
			if !frameBoundaries[gap.ordinal] || gap.seconds < 0 || gap.seconds > 0.1 {
				return false
			}
		}
	}
	// Explicitly bounded packet/frame precision, not an exact wall-clock claim.
	delta := out.durationMS - total
	if delta < 0 {
		delta = -delta
	}
	return delta <= 100
}

// Demuxed H.264 DTS can expose a part boundary only after its observed reorder
// prefix. The alternate location requires the independently decoded PTS gap at
// that original boundary; each boundary can explain at most one DTS gap.
func validDerivedDTSBoundaries(stream derivedStream, gaps, presentation []derivedPacketGap, boundaries map[int]int) bool {
	if len(gaps) > len(boundaries) || len(presentation) > len(boundaries) {
		return false
	}
	decoded := map[int]derivedPacketGap{}
	for _, gap := range presentation {
		if _, duplicate := decoded[gap.ordinal]; duplicate {
			return false
		}
		decoded[gap.ordinal] = gap
	}
	used := map[int]bool{}
	for _, gap := range gaps {
		if math.IsNaN(gap.seconds) || math.IsInf(gap.seconds, 0) || gap.seconds < 0 || gap.seconds > 0.1 {
			return false
		}
		matched, matches := 0, 0
		for packetBoundary, frameBoundary := range boundaries {
			if gap.ordinal == packetBoundary {
				matched, matches = packetBoundary, matches+1
				continue
			}
			if !validDerivedReorder(stream) || *stream.Reorder == 0 || gap.ordinal != packetBoundary+*stream.Reorder {
				continue
			}
			base, ok := rational(stream.TimeBase)
			if !ok {
				return false
			}
			tick, _ := base.Float64()
			frameGap, observed := decoded[frameBoundary]
			if observed && !math.IsNaN(frameGap.seconds) && !math.IsInf(frameGap.seconds, 0) && frameGap.seconds >= 0 && frameGap.seconds <= 0.1 && math.Abs(frameGap.seconds-gap.seconds) <= tick+1e-9 {
				matched, matches = packetBoundary, matches+1
			}
		}
		if matches != 1 || used[matched] {
			return false
		}
		used[matched] = true
	}
	return true
}
func probeDerivedMedia(ctx context.Context, tool string, work *derivedWork, index int, check func() error) (derivedMedia, error) {
	file, err := work.root.OpenFile(mustExportPartName(index), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return derivedMedia{}, ErrDerivedUnavailable
	}
	defer file.Close()
	info, e := file.Stat()
	if e != nil || index < 1 || index > len(work.stageVersions) || !sameDerivedVersion(work.stageVersions[index-1], derivedVersion{index, info, changeToken(info)}) {
		return derivedMedia{}, ErrDerivedUnavailable
	}
	return inspectDerived(ctx, tool, file, "mpegts", check, true)
}
func probeDerivedOutput(ctx context.Context, tool string, work *derivedWork, check func() error) (derivedMedia, error) {
	file, err := work.root.OpenFile("output.part.mkv", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return derivedMedia{}, ErrDerivedUnavailable
	}
	defer file.Close()
	actual, e := file.Stat()
	expected, f := work.output.Stat()
	if e != nil || f != nil || !actual.Mode().IsRegular() || !os.SameFile(actual, expected) {
		return derivedMedia{}, ErrDerivedUnavailable
	}
	return inspectDerived(ctx, tool, file, "matroska", check, false)
}
func runDerivedProcess(parent context.Context, cmd *exec.Cmd, check func() error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// CommandContext's own cancellation is retained; the watcher cancels it via
	// its explicit Cancel function so actual Wait is always joined below.
	if err := check(); err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	watched := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				watched <- nil
				return
			case <-ctx.Done():
				if cmd.Cancel != nil {
					_ = cmd.Cancel()
				}
				watched <- ctx.Err()
				return
			case <-ticker.C:
				if err := check(); err != nil {
					if cmd.Cancel != nil {
						_ = cmd.Cancel()
					}
					watched <- err
					return
				}
			}
		}
	}()
	err := cmd.Wait()
	close(done)
	watchErr := <-watched
	if watchErr != nil {
		return watchErr
	}
	if e := check(); e != nil {
		return e
	}
	return err
}
func probeDerivedCommand(ctx context.Context, tool string, file *os.File, format string, extra ...string) *exec.Cmd {
	args := []string{"-hide_banner", "-v", "error", "-max_alloc", "67108864", "-threads", "1", "-protocol_whitelist", "fd", "-format_whitelist", format, "-f", format, "-fd", "3", "-i", "fd:"}
	args = append(args, extra...)
	args = append(args, "-of", "json")
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.ExtraFiles = []*os.File{file}
	cmd.WaitDelay = time.Second
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C"}
	return cmd
}
func inspectDerived(parent context.Context, tool string, file *os.File, format string, check func() error, input bool) (derivedMedia, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	result := derivedMedia{packets: map[int]int{}, frames: map[int]int{}, gaps: map[int][]derivedPacketGap{}, frameGaps: map[int][]derivedPacketGap{}}
	cmd := probeDerivedCommand(ctx, tool, file, format, "-show_streams", "-show_data_hash", "sha256", "-show_entries", "stream=index,id,codec_type,codec_name,profile,level,has_b_frames,time_base,width,height,pix_fmt,sample_aspect_ratio,r_frame_rate,field_order,color_range,color_space,color_transfer,color_primaries,sample_rate,sample_fmt,channels,channel_layout,extradata_hash:stream_tags=language:stream_disposition=default,forced")
	stdout := &timingOutput{limit: 64 << 10}
	stderr := &timingOutput{limit: 4 << 10}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := runDerivedProcess(ctx, cmd, check); err != nil {
		return result, derivedProbeError(ctx, err)
	}
	var meta struct {
		Streams []derivedStream `json:"streams"`
	}
	if stdout.overflow || stderr.Len() != 0 || json.Unmarshal(stdout.Bytes(), &meta) != nil {
		return result, ErrDerivedIncompatible
	}
	for _, s := range meta.Streams {
		result.streams = append(result.streams, normalizeDerivedStream(s))
	}
	if input && !validDerivedStreams(result.streams) {
		return result, ErrDerivedIncompatible
	}
	if !input {
		if len(result.streams) < 2 || len(result.streams) > 9 {
			return result, ErrDerivedIncompatible
		}
		for _, s := range result.streams {
			if _, ok := rational(s.TimeBase); !ok {
				return result, ErrDerivedIncompatible
			}
			if s.Type == "video" && !validDerivedReorder(s) {
				return result, ErrDerivedIncompatible
			}
		}
	}
	states := map[int]*derivedPacketState{}
	for _, s := range result.streams {
		base, ok := rational(s.TimeBase)
		if !ok {
			return result, ErrDerivedIncompatible
		}
		states[s.Index] = &derivedPacketState{base: base, stream: s}
	}
	err := scanDerivedProcess(ctx, tool, file, format, "packets", "packet=stream_index,pts,dts,duration,flags", check, func(raw json.RawMessage) error {
		var p struct {
			Index    int           `json:"stream_index"`
			PTS      derivedNumber `json:"pts"`
			DTS      derivedNumber `json:"dts"`
			Duration derivedNumber `json:"duration"`
			Flags    string        `json:"flags"`
		}
		if json.Unmarshal(raw, &p) != nil {
			return ErrDerivedIncompatible
		}
		state := states[p.Index]
		if state == nil {
			return ErrDerivedIncompatible
		}
		pts, a := p.PTS.integer()
		duration, c := p.Duration.integer()
		if !a || !c || duration <= 0 || duration > 1_000_000_000_000 || pts > 1_000_000_000_000 || pts < -1_000_000_000_000 || strings.ContainsAny(p.Flags, "CD") {
			return ErrDerivedIncompatible
		}
		gap, ok := state.observeDTS(p.DTS, duration, input)
		if !ok {
			return ErrDerivedIncompatible
		}
		if gap != nil {
			result.gaps[p.Index] = append(result.gaps[p.Index], *gap)
		}
		if input && state.count == 0 {
			for _, s := range result.streams {
				if s.Index == p.Index && s.Type == "video" && !strings.Contains(p.Flags, "K") {
					return ErrDerivedIncompatible
				}
			}
		}
		if state.count == 0 || pts < state.first {
			state.first = pts
		}
		if state.count == 0 || pts+duration > state.end {
			state.end = pts + duration
		}
		state.count++
		result.packets[p.Index]++
		return nil
	})
	if err != nil {
		return result, err
	}
	var first, last *big.Rat
	for _, state := range states {
		if state.count == 0 || !state.haveDTS {
			return result, ErrDerivedIncompatible
		}
		start := new(big.Rat).Mul(new(big.Rat).SetInt64(state.first), state.base)
		end := new(big.Rat).Mul(new(big.Rat).SetInt64(state.end), state.base)
		if first == nil || start.Cmp(first) < 0 {
			first = start
		}
		if last == nil || end.Cmp(last) > 0 {
			last = end
		}
	}
	span := new(big.Rat).Sub(last, first)
	seconds, _ := span.Float64()
	if seconds <= 0 || seconds > 12*60*60 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return result, ErrDerivedIncompatible
	}
	result.durationMS = int64(math.Ceil(seconds * 1000))
	if input {
		for _, state := range states {
			start := new(big.Rat).Mul(new(big.Rat).SetInt64(state.first), state.base)
			end := new(big.Rat).Mul(new(big.Rat).SetInt64(state.end), state.base)
			lead, _ := new(big.Rat).Sub(start, first).Float64()
			lag, _ := new(big.Rat).Sub(last, end).Float64()
			if lead > 0.1 || lag > 0.1 {
				return result, ErrDerivedIncompatible
			}
		}
	}
	// Decode every frame and compare observable format/layout parameters. This
	// does not claim to detect arbitrary same-format in-band parameter changes.
	sequences := map[int]*derivedFrameSequence{}
	for _, stream := range result.streams {
		sequences[stream.Index] = &derivedFrameSequence{base: states[stream.Index].base}
	}
	err = scanDerivedProcess(ctx, tool, file, format, "frames", "frame=media_type,stream_index,pts,best_effort_timestamp,width,height,pix_fmt,sample_aspect_ratio,interlaced_frame,color_range,color_space,color_transfer,color_primaries,sample_fmt,channels,channel_layout,nb_samples,duration,pkt_duration", check, func(raw json.RawMessage) error {
		var frame derivedFrame
		if json.Unmarshal(raw, &frame) != nil {
			return ErrDerivedIncompatible
		}
		var stream *derivedStream
		for i := range result.streams {
			if result.streams[i].Index == frame.Index {
				stream = &result.streams[i]
			}
		}
		if stream == nil || !validDerivedFrame(*stream, frame) {
			return ErrDerivedIncompatible
		}
		if !sequences[frame.Index].observe(frame, input) {
			return ErrDerivedIncompatible
		}
		result.frames[frame.Index]++
		return nil
	})
	if err != nil {
		return result, err
	}
	for _, s := range result.streams {
		if result.frames[s.Index] == 0 {
			return result, ErrDerivedIncompatible
		}
		sequence, packets := sequences[s.Index], states[s.Index]
		if sequence.count != packets.count || sequence.first != packets.first || sequence.end < packets.end-1 || sequence.end > packets.end+1 {
			return result, ErrDerivedIncompatible
		}
		result.frameGaps[s.Index] = sequence.gaps
	}
	return result, nil
}

type derivedPacketState struct {
	base                *big.Rat
	stream              derivedStream
	first, end, lastDTS int64
	lastDuration        int64
	count               int
	missingDTS          int
	haveDTS             bool
}

// Matroska may omit only the initial decoder-reorder prefix's DTS. Those
// timestamps stay unknown: complete decoded PTS and packet counts are still
// required, and every DTS actually present keeps strict progression checks.
func (s *derivedPacketState) observeDTS(raw derivedNumber, duration int64, input bool) (*derivedPacketGap, bool) {
	if raw == "" {
		if input || s.haveDTS || !validDerivedReorder(s.stream) || s.missingDTS >= *s.stream.Reorder {
			return nil, false
		}
		s.missingDTS++
		return nil, true
	}
	dts, ok := raw.integer()
	if !ok || dts > 1_000_000_000_000 || dts < -1_000_000_000_000 {
		return nil, false
	}
	var gap *derivedPacketGap
	if s.haveDTS {
		step := dts - s.lastDTS
		if dts <= s.lastDTS || step < s.lastDuration-1 {
			return nil, false
		}
		if step > s.lastDuration+1 {
			if input {
				return nil, false
			}
			delta, _ := new(big.Rat).Mul(new(big.Rat).SetInt64(step-s.lastDuration), s.base).Float64()
			gap = &derivedPacketGap{s.count, delta}
		}
	}
	s.haveDTS = true
	s.lastDTS, s.lastDuration = dts, duration
	return gap, true
}

type derivedFrame struct {
	Index          int           `json:"stream_index"`
	Type           string        `json:"media_type"`
	Width          int           `json:"width"`
	Height         int           `json:"height"`
	Pixel          string        `json:"pix_fmt"`
	Aspect         string        `json:"sample_aspect_ratio"`
	Interlaced     *int          `json:"interlaced_frame"`
	ColorRange     string        `json:"color_range"`
	ColorSpace     string        `json:"color_space"`
	ColorTransfer  string        `json:"color_transfer"`
	ColorPrimaries string        `json:"color_primaries"`
	SampleFormat   string        `json:"sample_fmt"`
	Channels       int           `json:"channels"`
	Layout         string        `json:"channel_layout"`
	Samples        derivedNumber `json:"nb_samples"`
	Duration       derivedNumber `json:"duration"`
	PacketDuration derivedNumber `json:"pkt_duration"`
	PTS            derivedNumber `json:"pts"`
	BestPTS        derivedNumber `json:"best_effort_timestamp"`
}
type derivedFrameSequence struct {
	base                           *big.Rat
	count                          int
	first, end, last, lastDuration int64
	gaps                           []derivedPacketGap
}

func (s *derivedFrameSequence) observe(frame derivedFrame, input bool) bool {
	pts, a := frame.PTS.integer()
	best, b := frame.BestPTS.integer()
	duration, c := frame.Duration.integer()
	if !c {
		duration, c = frame.PacketDuration.integer()
	}
	if !a || !b || pts != best || !c || duration <= 0 || duration > 1_000_000_000_000 || pts > 1_000_000_000_000 || pts < -1_000_000_000_000 {
		return false
	}
	if s.count > 0 {
		if pts <= s.last {
			return false
		}
		step := pts - s.last
		if step < s.lastDuration-1 {
			return false
		}
		if step > s.lastDuration+1 {
			if input {
				return false
			}
			delta, _ := new(big.Rat).Mul(new(big.Rat).SetInt64(step-s.lastDuration), s.base).Float64()
			s.gaps = append(s.gaps, derivedPacketGap{s.count, delta})
		}
	} else {
		s.first = pts
	}
	s.last = pts
	s.end = pts + duration
	s.lastDuration = duration
	s.count++
	return true
}

func validDerivedFrame(s derivedStream, f derivedFrame) bool {
	if f.Type != s.Type {
		return false
	}
	if s.Type == "video" {
		if f.Width != s.Width || f.Height != s.Height || f.Pixel != s.Pixel || f.Aspect != s.Aspect || f.Interlaced == nil || *f.Interlaced != 0 || normalizedUnknown(f.ColorRange) != s.ColorRange || normalizedUnknown(f.ColorSpace) != s.ColorSpace || normalizedUnknown(f.ColorTransfer) != s.ColorTransfer || normalizedUnknown(f.ColorPrimaries) != s.ColorPrimaries {
			return false
		}
		duration, ok := f.Duration.integer()
		if !ok {
			duration, ok = f.PacketDuration.integer()
		}
		base, valid := rational(s.TimeBase)
		rate, known := rational(s.Rate)
		if !ok || duration <= 0 || !valid || !known {
			return false
		}
		delta := new(big.Rat).Sub(new(big.Rat).Mul(new(big.Rat).SetInt64(duration), base), new(big.Rat).Inv(rate))
		if delta.Sign() < 0 {
			delta.Neg(delta)
		}
		return delta.Cmp(base) <= 0
	}
	if s.Type != "audio" || f.SampleFormat != s.SampleFormat || f.Channels != s.Channels || f.Layout != s.Layout {
		return false
	}
	samples, ok := f.Samples.integer()
	duration, valid := f.Duration.integer()
	if !valid {
		duration, valid = f.PacketDuration.integer()
	}
	if !ok || samples != 1024 || !valid || duration <= 0 {
		return false
	}
	base, ok := rational(s.TimeBase)
	if !ok {
		return false
	}
	expected, err := strconv.ParseInt(s.SampleRate, 10, 64)
	if err != nil {
		return false
	}
	actual := new(big.Rat).Quo(new(big.Rat).SetInt64(samples), new(big.Rat).Mul(new(big.Rat).SetInt64(duration), base))
	// Matroska packet timestamps quantize to milliseconds. One tick is an
	// explicit precision bound, rather than a fabricated per-frame sample rate.
	frameSeconds := new(big.Rat).Quo(new(big.Rat).SetInt64(samples), new(big.Rat).SetInt64(expected))
	delta := new(big.Rat).Sub(new(big.Rat).Mul(new(big.Rat).SetInt64(duration), base), frameSeconds)
	if delta.Sign() < 0 {
		delta.Neg(delta)
	}
	return actual.Sign() > 0 && delta.Cmp(base) <= 0
}
func scanDerivedProcess(ctx context.Context, tool string, file *os.File, format, key, entries string, check func() error, consume func(json.RawMessage) error) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return ErrDerivedUnavailable
	}
	original := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := probeDerivedCommand(ctx, tool, file, format, "-show_"+key, "-show_entries", entries)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	cmd.Stdout = writer
	stderr := &timingOutput{limit: 4 << 10}
	cmd.Stderr = stderr
	readDone := make(chan error, 1)
	// Streaming decoder keeps bounded per-entry state and a fixed total read cap.
	go func() {
		err := readDerivedArray(io.LimitReader(reader, 512<<20), key, consume)
		if err != nil {
			reader.CloseWithError(err)
			cancel()
		}
		readDone <- err
	}()
	processErr := runDerivedProcess(ctx, cmd, check)
	writer.Close()
	readErr := <-readDone
	if errors.Is(processErr, ErrDerivedAccess) || errors.Is(processErr, ErrDerivedUnavailable) || errors.Is(processErr, ErrDerivedTooLarge) {
		return processErr
	}
	if original.Err() != nil {
		return original.Err()
	}
	if readErr != nil {
		if errors.Is(readErr, ErrDerivedTooLarge) {
			return readErr
		}
		if original.Err() != nil {
			return original.Err()
		}
		return ErrDerivedIncompatible
	}
	if processErr != nil {
		return derivedProbeError(ctx, processErr)
	}
	if stderr.Len() != 0 {
		return ErrDerivedIncompatible
	}
	return nil
}
func derivedProbeError(ctx context.Context, err error) error {
	if errors.Is(err, ErrDerivedAccess) || errors.Is(err, ErrDerivedTooLarge) || errors.Is(err, ErrDerivedUnavailable) {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrDerivedIncompatible
}
func (w *derivedWork) remux(parent context.Context, tool string, media []derivedMedia, check func() error) error {
	if err := check(); err != nil {
		return err
	}
	var text strings.Builder
	text.WriteString("ffconcat version 1.0\n")
	for i, m := range media {
		fmt.Fprintf(&text, "file '%s'\nduration %.6f\n", mustExportPartName(i+1), float64(m.durationMS)/1000)
	}
	manifest, err := w.root.OpenFile("inputs.ffconcat", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ErrDerivedUnavailable
	}
	_, err = io.WriteString(manifest, text.String())
	closeErr := manifest.Close()
	if err != nil || closeErr != nil {
		return ErrDerivedUnavailable
	}
	w.output, err = w.root.OpenFile("output.part.mkv", os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ErrDerivedUnavailable
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "warning", "-xerror", "-protocol_whitelist", "file", "-format_whitelist", "concat,mpegts", "-f", "concat", "-safe", "1", "-auto_convert", "0", "-i", "inputs.ffconcat", "-map", "0", "-map_metadata", "-1", "-c", "copy", "-avoid_negative_ts", "make_zero", "-f", "matroska", "-fd", "4", "fd:"}
	outputArgs := append([]string{}, args[len(args)-5:]...)
	args = args[:len(args)-5]
	for i, stream := range media[0].streams {
		disposition := "0"
		if stream.Disposition.Default == 1 {
			disposition = "default"
		}
		if stream.Disposition.Forced == 1 {
			if disposition == "0" {
				disposition = "forced"
			} else {
				disposition += "+forced"
			}
		}
		args = append(args, "-disposition:"+strconv.Itoa(i), disposition)
		if stream.Tags.Language != "" {
			args = append(args, "-metadata:s:"+strconv.Itoa(i), "language="+stream.Tags.Language)
		}
	}
	args = append(args, outputArgs...)
	cmd := exec.CommandContext(ctx, tool, args...)
	cmd.ExtraFiles = []*os.File{w.dir, w.output}
	// Linux Go performs chdir before remapping ExtraFiles: use the original
	// inherited directory FD here, not the later child descriptor3.
	cmd.Dir = "/proc/self/fd/" + strconv.Itoa(int(w.dir.Fd()))
	cmd.WaitDelay = time.Second
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C", "LC_ALL=C", "TMPDIR=."}
	stderr := &timingOutput{limit: 4 << 10}
	cmd.Stderr = stderr
	boundedCheck := func() error {
		if err := check(); err != nil {
			return err
		}
		info, e := w.output.Stat()
		if e != nil {
			return ErrDerivedUnavailable
		}
		if info.Size() > w.outputLimit() {
			return ErrDerivedTooLarge
		}
		return nil
	}
	if err = runDerivedProcess(ctx, cmd, boundedCheck); err != nil {
		return err
	}
	if stderr.Len() != 0 {
		return ErrDerivedIncompatible
	}
	return boundedCheck()
}
