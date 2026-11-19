package recordings

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
)

func derivedLedgerFixture(t *testing.T) *exportFixture {
	t.Helper()
	f := newExportFixture(t)
	at := time.Now().UTC().Add(-time.Minute)
	firstEnd := at.Add(time.Second)
	secondStart := firstEnd.Add(time.Second)
	last := secondStart.Add(time.Second)
	f.job.Capture.Parts = []models.RecordingPart{{Index: 1, StartedAt: at, EndedAt: &firstEnd, EndReason: "source_ended", Bytes: int64(len(f.data[0]))}, {Index: 2, StartedAt: secondStart, EndedAt: &last, EndReason: "schedule", Bytes: int64(len(f.data[1]))}}
	f.job.Capture.Gaps = []models.RecordingGap{{StartedAt: firstEnd, EndedAt: &secondStart, Reason: "source_retry"}}
	if err := os.Rename(filepath.Join(f.dir, mustExportPartName(3)), filepath.Join(f.dir, mustExportPartName(2))); err != nil {
		t.Fatal(err)
	}
	if err := f.repo.Update(context.Background(), &f.job); err != nil {
		t.Fatal(err)
	}
	return f
}
func configureDerivedFixture(t *testing.T, f *exportFixture, tools DerivedTools, current func(context.Context) (DerivedTools, error)) {
	t.Helper()
	if err := f.service.ConfigureDerivedJoiner(context.Background(), tools, current); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.service.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
}
func TestDerivedRecordingRejectsWholeUnfinishedSparseAndUnknownGapLedger(t *testing.T) {
	for _, name := range []string{"sparse", "duplicate", "zero", "open", "overlap", "open-gap", "unexplained-gap", "restored"} {
		t.Run(name, func(t *testing.T) {
			f := derivedLedgerFixture(t)
			c := f.job.Capture
			switch name {
			case "sparse":
				c.Parts[1].Index = 3
			case "duplicate":
				c.Parts[1].Index = 1
			case "zero":
				c.Parts[0].Bytes = 0
			case "open":
				c.Parts[0].EndedAt = nil
			case "overlap":
				c.Parts[1].StartedAt = c.Parts[0].StartedAt
			case "open-gap":
				c.Gaps[0].EndedAt = nil
			case "unexplained-gap":
				c.Gaps = nil
			case "restored":
				c.Restored = true
			}
			if err := validateDerivedLedger(f.job); err == nil {
				t.Fatal("invalid whole ledger accepted")
			}
		})
	}
	f := derivedLedgerFixture(t)
	if err := validateDerivedLedger(f.job); err != nil {
		t.Fatal("known closed gap rejected", err)
	}
}
func TestDerivedRecordingPassiveReadAndDeniedRequestDoNotCreateWork(t *testing.T) {
	f := derivedLedgerFixture(t)
	tools := DerivedTools{FFmpegPath: "nonexistent-derived-ffmpeg", FFprobePath: "nonexistent-derived-ffprobe"}
	configureDerivedFixture(t, f, tools, func(context.Context) (DerivedTools, error) { return tools, nil })
	before, err := os.ReadDir(f.service.outputDir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		v, err := f.service.GetDerived(context.Background(), f.job.ID, exportAllowed)
		if err != nil || v.State != "unknown" || v.Reason != "not_prepared" || v.ID != "" || v.DurationMS != 0 || v.PartOffsets == nil {
			t.Fatalf("unexpected passive descriptor: %+v %v", v, err)
		}
	}
	if _, err = f.service.PrepareDerived(context.Background(), f.job.ID, func() error { return errors.New("denied") }); !errors.Is(err, ErrDerivedAccess) {
		t.Fatal("denied preparation", err)
	}
	after, _ := os.ReadDir(f.service.outputDir)
	if len(before) != len(after) {
		t.Fatal("passive or denied work wrote media")
	}
}
func TestDerivedRecordingUnknownAndReadyDescriptorsDoNotContainPathsOrAuthority(t *testing.T) {
	f := derivedLedgerFixture(t)
	v := derivedUnknown(f.job, "not_prepared")
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "PRIVATE_") || strings.Contains(string(raw), "null") || strings.Contains(string(raw), "durationMs") || strings.Contains(string(raw), "observedAt") {
		t.Fatalf("unproven private field: %s", raw)
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 7 {
		t.Fatalf("unexpected unknown shape: %s", raw)
	}
}
func TestDerivedRecordingConfigurationChangeFailsBeforeAnyTool(t *testing.T) {
	f := derivedLedgerFixture(t)
	tools := DerivedTools{FFmpegPath: "not-executed", FFprobePath: "not-executed"}
	var changed atomic.Bool
	configureDerivedFixture(t, f, tools, func(context.Context) (DerivedTools, error) {
		if changed.Load() {
			return DerivedTools{}, errors.New("changed")
		}
		return tools, nil
	})
	changed.Store(true)
	if _, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed); !errors.Is(err, ErrDerivedUnavailable) {
		t.Fatal("changed configuration admitted", err)
	}
	v, err := f.service.GetDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil || v.State != "unknown" || v.Reason != "configuration_changed" {
		t.Fatalf("configuration fell back: %+v %v", v, err)
	}
}
func TestDerivedRecordingReservedProviderSelectorsNeverFallBackToFirstPart(t *testing.T) {
	for _, tail := range []string{"derived/x", "DERIVED/x", "%64erived/x", "%2564erived/x", "folder/derived/x", "derived"} {
		if _, _, _, valid := parsePartStreamPath("recording:synthetic/" + tail); valid {
			t.Fatal("derived selector treated as legacy filename", tail)
		}
	}
	if _, _, _, valid := parsePartStreamPath("recording:synthetic/old-filename.ts"); !valid {
		t.Fatal("legacy unrelated tail changed")
	}
}
func TestDerivedRecordingVersionRejectsSameSizeWriteWithRestoredModtime(t *testing.T) {
	f := derivedLedgerFixture(t)
	in, err := f.service.openDerivedInputs(context.Background(), f.job, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	defer in.close()
	before := in.files[0].info
	path := filepath.Join(f.dir, mustExportPartName(1))
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteAt([]byte("different"), 0); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err = os.Chtimes(path, before.ModTime(), before.ModTime()); err != nil {
		t.Fatal(err)
	}
	if in.check() == nil {
		t.Fatal("same-size/mtime mutation retained source proof")
	}
}
func TestDerivedRecordingOpaqueIdentifierValidation(t *testing.T) {
	id, err := newDerivedID()
	if err != nil || !validDerivedID(id) {
		t.Fatal(id, err)
	}
	for _, value := range []string{"", strings.Repeat("A", 32), id + "/x", "../" + id, strings.Repeat("g", 32), strings.Repeat("a", 31)} {
		if validDerivedID(value) {
			t.Fatal("invalid derived ID", value)
		}
	}
}
func TestDerivedRecordingReadParserRejectsUnknownIncompleteAndOversizedEntries(t *testing.T) {
	cases := []string{`{}`, `{"packets":null}`, `{"packets":[]}`, `{"packets":[{}]} trailing`, `{"packets":[{"name":"` + strings.Repeat("x", 4097) + `"}]}`}
	for _, raw := range cases {
		if readDerivedArray(strings.NewReader(raw), "packets", func(json.RawMessage) error { return nil }) == nil {
			t.Fatal("unbounded/incomplete payload accepted")
		}
	}
	if err := readDerivedArray(strings.NewReader(`{"packets":[{"stream_index":0}]}`), "packets", func(json.RawMessage) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
func TestDerivedRecordingDecodedFrameParametersRejectObservableReconfiguration(t *testing.T) {
	video := derivedStream{Type: "video", Width: 64, Height: 64, Pixel: "yuv420p", Aspect: "1:1", TimeBase: "1/90000", Rate: "25/1"}
	progressive := 0
	frame := derivedFrame{Type: "video", Width: 64, Height: 64, Pixel: "yuv420p", Aspect: "1:1", Interlaced: &progressive, Duration: "3600"}
	if !validDerivedFrame(video, frame) {
		t.Fatal("stable decoded video rejected")
	}
	frame.Width = 96
	if validDerivedFrame(video, frame) {
		t.Fatal("decoded resolution change accepted")
	}
	audio := derivedStream{Type: "audio", SampleFormat: "fltp", Channels: 1, Layout: "mono", SampleRate: "48000", TimeBase: "1/90000"}
	sound := derivedFrame{Type: "audio", SampleFormat: "fltp", Channels: 1, Layout: "mono", Samples: "1024", Duration: "1920"}
	if !validDerivedFrame(audio, sound) {
		t.Fatal("stable decoded AAC rejected")
	}
	for _, kind := range []string{"rate", "layout", "format", "HE-samples", "unknown-duration"} {
		change := sound
		switch kind {
		case "rate":
			change.Duration = "2089"
		case "layout":
			change.Channels = 2
			change.Layout = "stereo"
		case "format":
			change.SampleFormat = "s16"
		case "HE-samples":
			change.Samples = "2048"
		case "unknown-duration":
			change.Duration = ""
		}
		if validDerivedFrame(audio, change) {
			t.Fatal("decoded audio change accepted", kind)
		}
	}
}
func TestDerivedRecordingBusyAdmissionHasNoQueueOrFileWork(t *testing.T) {
	f := derivedLedgerFixture(t)
	tools := DerivedTools{FFmpegPath: "never-executed", FFprobePath: "never-executed"}
	configureDerivedFixture(t, f, tools, func(context.Context) (DerivedTools, error) { return tools, nil })
	derivedSlots <- struct{}{}
	defer func() { <-derivedSlots }()
	before, _ := os.ReadDir(f.service.outputDir)
	if _, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed); !errors.Is(err, ErrDerivedBusy) {
		t.Fatal("worker queued rather than rejected", err)
	}
	after, _ := os.ReadDir(f.service.outputDir)
	if len(before) != len(after) {
		t.Fatal("busy request created work")
	}
}
func TestDerivedRecordingCancellationActuallyJoinsStartedLocalProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sleep", "30")
	cmd.WaitDelay = time.Second
	var scheduled atomic.Bool
	check := func() error {
		if cmd.Process != nil && !scheduled.Swap(true) {
			time.AfterFunc(20*time.Millisecond, cancel)
		}
		return nil
	}
	if err := runDerivedProcess(ctx, cmd, check); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation not retained", err)
	}
	if cmd.ProcessState == nil {
		t.Fatal("returned before actual child Wait")
	}
}
func TestDerivedRecordingRejectedAuthorityNeverStartsProcess(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "/bin/sleep", "30")
	err := runDerivedProcess(context.Background(), cmd, func() error { return ErrDerivedAccess })
	if !errors.Is(err, ErrDerivedAccess) || cmd.Process != nil {
		t.Fatal("authority failure started child", err)
	}
}

type derivedCountingInput struct {
	input io.Reader
	bytes int
}

func (r *derivedCountingInput) Read(p []byte) (int, error) {
	n, e := r.input.Read(p)
	r.bytes += n
	return n, e
}
func TestDerivedRecordingJSONRejectsLargeSingleValueBeforeReadingOrAllocatingIt(t *testing.T) {
	for _, known := range []bool{true, false} {
		prefix := `{"packets":[{"large":"`
		suffix := `"}]}`
		limit := 8 << 10
		if !known {
			prefix = `{"ignored":{"large":"`
			suffix = `"},"packets":[{}]}`
			limit = 68 << 10
		}
		reader := &derivedCountingInput{input: strings.NewReader(prefix + strings.Repeat("x", 1<<20) + suffix)}
		calls := 0
		err := readDerivedArray(reader, "packets", func(json.RawMessage) error { calls++; return nil })
		if !errors.Is(err, ErrDerivedTooLarge) || reader.bytes > limit || calls != 0 {
			t.Fatalf("unbounded value acquisition known%v bytes%d calls%d: %v", known, reader.bytes, calls, err)
		}
	}
}
func TestDerivedRecordingPresentationRejectsDuplicateMissingAndRepairedFramePTS(t *testing.T) {
	base, _ := rational("1/1000")
	makeFrame := func(pts derivedNumber) derivedFrame { return derivedFrame{PTS: pts, BestPTS: pts, Duration: "40"} }
	valid := &derivedFrameSequence{base: base}
	for _, pts := range []derivedNumber{"0", "40", "80"} {
		if !valid.observe(makeFrame(pts), true) {
			t.Fatal("valid presentation order rejected")
		}
	}
	for _, kind := range []string{"duplicate", "missing", "repaired", "jump"} {
		sequence := &derivedFrameSequence{base: base}
		if !sequence.observe(makeFrame("0"), true) {
			t.Fatal("first frame")
		}
		frame := makeFrame("40")
		switch kind {
		case "duplicate":
			frame = makeFrame("0")
		case "missing":
			frame.PTS = ""
		case "repaired":
			frame.BestPTS = "80"
		case "jump":
			frame = makeFrame("80")
		}
		if sequence.observe(frame, true) {
			t.Fatal("invalid decoded presentation accepted", kind)
		}
	}
}
func TestDerivedRecordingHeldInputsRejectRenamedCanonicalDirectory(t *testing.T) {
	f := derivedLedgerFixture(t)
	inputs, err := f.service.openDerivedInputs(context.Background(), f.job, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	defer inputs.close()
	moved := f.dir + "-retired"
	if err = os.Rename(f.dir, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(f.dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err = inputs.files[0].check(); err != nil {
		t.Fatal("fixture did not retain old opened root", err)
	}
	if inputs.check() == nil {
		t.Fatal("canonical directory replacement retained current source proof")
	}
}
func TestDerivedRecordingCleanupDoesNotRemoveReplacementWorkDirectory(t *testing.T) {
	f := derivedLedgerFixture(t)
	work, err := f.service.newDerivedWork(exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.service.outputDir, work.name)
	if err = os.Rename(path, path+"-retired"); err != nil {
		work.close()
		t.Fatal(err)
	}
	if err = os.Mkdir(path, 0700); err != nil {
		work.close()
		t.Fatal(err)
	}
	work.close()
	if info, err := os.Lstat(path); err != nil || !info.IsDir() {
		t.Fatal("cleanup removed an unowned replacement", err)
	}
}

func TestDerivedRecordingPacketParserRestrictsUnknownDTSByObservedCodecReorder(t *testing.T) {
	base, _ := rational("1/1000")
	packet := func(dts string) string {
		value := `{"stream_index":0,"pts":80,"duration":40`
		if dts != "" {
			value += `,"dts":` + dts
		}
		return value + "}"
	}
	packets := func(values ...string) string {
		raw := make([]string, 0, len(values))
		for _, value := range values {
			raw = append(raw, packet(value))
		}
		return `{"packets":[` + strings.Join(raw, ",") + "]}"
	}
	for _, tc := range []struct {
		name, kind, reorder, data string
		input, allowed            bool
	}{
		{"known", "video", "0", packets("0", "40", "80"), false, true},
		{"maximum-observed-depth", "video", "16", packets("0", "40"), false, true},
		{"valid-reordered-pts", "video", "2", `{"packets":[{"pts":80,"duration":40},{"pts":200,"duration":40},{"pts":120,"dts":80,"duration":40},{"pts":160,"dts":120,"duration":40}]}`, false, true},
		{"bounded-prefix", "video", "2", packets("", "", "80", "120"), false, true},
		{"shorter-prefix", "video", "2", packets("", "40", "80"), false, true},
		{"input-missing", "video", "2", packets("", "", "80"), true, false},
		{"audio-missing", "audio", "2", packets("", "40"), false, false},
		{"zero-depth", "video", "0", packets("", "40"), false, false},
		{"depth-absent", "video", "", packets("", "40"), false, false},
		{"depth-absent-known-dts", "video", "", packets("0", "40"), false, false},
		{"depth-null", "video", "null", packets("", "40"), false, false},
		{"depth-negative", "video", "-1", packets("", "40"), false, false},
		{"depth-too-large", "video", "17", packets("", "40"), false, false},
		{"depth-fraction", "video", "2.5", packets("", "40"), false, false},
		{"depth-string", "video", `"2"`, packets("", "40"), false, false},
		{"depth-overflow", "video", "9223372036854775808", packets("", "40"), false, false},
		{"prefix-too-long", "video", "2", packets("", "", "", "120"), false, false},
		{"later-hole", "video", "2", packets("", "40", "", "120"), false, false},
		{"all-unknown", "video", "2", packets("", ""), false, false},
		{"known-backwards", "video", "2", packets("", "", "80", "40"), false, false},
		{"known-duplicate", "video", "2", packets("", "", "80", "80"), false, false},
		{"known-step-too-short", "video", "2", packets("", "", "80", "100"), false, false},
		{"known-invalid", "video", "2", packets("", "", `"N/A"`), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"codec_type":"` + tc.kind + `","codec_name":"h264"`
			if tc.reorder != "" {
				raw += `,"has_b_frames":` + tc.reorder
			}
			raw += "}"
			var stream derivedStream
			err := json.Unmarshal([]byte(raw), &stream)
			if err == nil && stream.Type == "video" && !validDerivedReorder(stream) {
				err = ErrDerivedIncompatible
			}
			state := &derivedPacketState{base: base, stream: stream}
			if err == nil {
				err = readDerivedArray(strings.NewReader(tc.data), "packets", func(raw json.RawMessage) error {
					var parsed struct {
						PTS      derivedNumber `json:"pts"`
						DTS      derivedNumber `json:"dts"`
						Duration derivedNumber `json:"duration"`
					}
					if json.Unmarshal(raw, &parsed) != nil {
						return ErrDerivedIncompatible
					}
					if _, ok := parsed.PTS.integer(); !ok {
						return ErrDerivedIncompatible
					}
					duration, ok := parsed.Duration.integer()
					if !ok || duration <= 0 {
						return ErrDerivedIncompatible
					}
					if _, ok = state.observeDTS(parsed.DTS, duration, tc.input); !ok {
						return ErrDerivedIncompatible
					}
					state.count++
					return nil
				})
				if !state.haveDTS {
					err = ErrDerivedIncompatible
				}
			}
			if (err == nil) != tc.allowed {
				t.Fatalf("reorder-bound packet admission = %v, want %v: %v", err == nil, tc.allowed, err)
			}
		})
	}
	t.Run("observed-depth-is-part-compatibility", func(t *testing.T) {
		two, otherTwo, one := 2, 2, 1
		first := derivedMedia{streams: []derivedStream{{Type: "video", Codec: "h264", Reorder: &two}}}
		same := derivedMedia{streams: []derivedStream{{Type: "video", Codec: "h264", Reorder: &otherTwo}}}
		different := derivedMedia{streams: []derivedStream{{Type: "video", Codec: "h264", Reorder: &one}}}
		if !compatibleDerived(first, same) || compatibleDerived(first, different) {
			t.Fatal("part compatibility ignored the actual decoder reorder depth")
		}
	})
}

func TestDerivedRecordingOutputValidatorBindsDelayedDTSGapToOriginalPresentationBoundary(t *testing.T) {
	makeFacts := func() ([]derivedMedia, derivedMedia) {
		depth := 2
		streams := []derivedStream{{Index: 0, Type: "video", Codec: "h264", Reorder: &depth, TimeBase: "1/90000"}, {Index: 1, Type: "audio", Codec: "aac", TimeBase: "1/90000"}}
		part := derivedMedia{streams: streams, packets: map[int]int{0: 20, 1: 39}, frames: map[int]int{0: 20, 1: 39}, durationMS: 832}
		outputStreams := append([]derivedStream{}, streams...)
		for i := range outputStreams {
			outputStreams[i].TimeBase = "1/1000"
		}
		output := derivedMedia{streams: outputStreams, packets: map[int]int{0: 40, 1: 78}, frames: map[int]int{0: 40, 1: 78}, gaps: map[int][]derivedPacketGap{0: {{22, 0.032}}}, frameGaps: map[int][]derivedPacketGap{0: {{20, 0.032}}}, durationMS: 1664}
		return []derivedMedia{part, part}, output
	}
	for _, tc := range []struct {
		name    string
		allowed bool
	}{
		{"actual-reorder-offset", true}, {"within-one-output-tick", true}, {"ordinary-direct-boundary", true},
		{"missing-decoded-gap", false}, {"mismatched-gap", false}, {"over100ms", false},
		{"wrong-packet-ordinal", false}, {"wrong-decoded-ordinal", false}, {"direct-and-delayed-same-boundary", false},
		{"repeated-delayed-boundary", false}, {"unknown-reorder", false}, {"unknown-output-timebase", false},
		{"audio-cannot-use-video-reorder", false}, {"decoded-frame-loss", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputs, output := makeFacts()
			switch tc.name {
			case "within-one-output-tick":
				output.frameGaps[0] = []derivedPacketGap{{20, 0.033}}
			case "ordinary-direct-boundary":
				output.gaps[0] = []derivedPacketGap{{20, 0.032}}
				output.frameGaps[0] = nil
			case "missing-decoded-gap":
				output.frameGaps[0] = nil
			case "mismatched-gap":
				output.frameGaps[0] = []derivedPacketGap{{20, 0.034}}
			case "over100ms":
				output.gaps[0] = []derivedPacketGap{{22, 0.101}}
				output.frameGaps[0] = []derivedPacketGap{{20, 0.101}}
			case "wrong-packet-ordinal":
				output.gaps[0] = []derivedPacketGap{{21, 0.032}}
			case "wrong-decoded-ordinal":
				output.frameGaps[0] = []derivedPacketGap{{21, 0.032}}
			case "direct-and-delayed-same-boundary":
				output.gaps[0] = []derivedPacketGap{{20, 0.032}, {22, 0.032}}
			case "repeated-delayed-boundary":
				output.gaps[0] = []derivedPacketGap{{22, 0.032}, {22, 0.032}}
			case "unknown-reorder":
				output.streams[0].Reorder = nil
			case "unknown-output-timebase":
				output.streams[0].TimeBase = ""
			case "audio-cannot-use-video-reorder":
				output.gaps[1] = []derivedPacketGap{{41, 0.032}}
				output.frameGaps[1] = []derivedPacketGap{{39, 0.032}}
			case "decoded-frame-loss":
				output.frames[0]--
			}
			if validDerivedOutput(inputs, output, 1664) != tc.allowed {
				t.Fatalf("complete output boundary validation did not enforce %s", tc.name)
			}
		})
	}
	t.Run("ambiguous-original-boundary-is-rejected", func(t *testing.T) {
		_, output := makeFacts()
		if validDerivedDTSBoundaries(output.streams[0], output.gaps[0], output.frameGaps[0], map[int]int{20: 20, 22: 22}) {
			t.Fatal("one delayed DTS gap matched two different original boundaries")
		}
	})
}
