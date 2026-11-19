package recordings

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// These generate only tiny local synthetic inputs when the approved suite runs.
// The implementation author does not execute them while the packet is changing.
func derivedSyntheticPart(t *testing.T, path, color string, width int, rate int, reorder ...string) {
	t.Helper()
	frequency := "440"
	if color == "blue" {
		frequency = "880"
	}
	bframes := "0"
	if len(reorder) == 1 {
		bframes = reorder[0]
	}
	args := []string{"-nostdin", "-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=" + color + ":s=" + strconv.Itoa(width) + "x64:r=25", "-f", "lavfi", "-i", "sine=frequency=" + frequency + ":sample_rate=" + strconv.Itoa(rate), "-t", "0.8", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-g", "10", "-bf", bframes, "-c:a", "aac", "-ac", "1", "-ar", strconv.Itoa(rate), "-f", "mpegts", path}
	if raw, err := exec.Command("/usr/bin/ffmpeg", args...).CombinedOutput(); err != nil {
		t.Fatalf("local synthetic generation: %v %s", err, raw)
	}
}
func realDerivedFixture(t *testing.T) *exportFixture {
	t.Helper()
	f := derivedLedgerFixture(t)
	for i, color := range []string{"red", "blue"} {
		name := filepath.Join(f.dir, mustExportPartName(i+1))
		if err := os.Remove(name); err != nil {
			t.Fatal(err)
		}
		derivedSyntheticPart(t, name, color, 64, 48000)
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		f.job.Capture.Parts[i].Bytes = info.Size()
	}
	if err := f.repo.Update(context.Background(), &f.job); err != nil {
		t.Fatal(err)
	}
	tools := DerivedTools{FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe"}
	configureDerivedFixture(t, f, tools, func(context.Context) (DerivedTools, error) { return tools, nil })
	return f
}
func sourceDerivedHashes(t *testing.T, f *exportFixture) [][32]byte {
	t.Helper()
	out := [][32]byte{}
	for i := 1; i <= 2; i++ {
		raw, e := os.ReadFile(filepath.Join(f.dir, mustExportPartName(i)))
		if e != nil {
			t.Fatal(e)
		}
		out = append(out, sha256.Sum256(raw))
	}
	return out
}
func TestDerivedRecordingActualCompatibleMediaPreservesOriginalsAndReusesExactReadyOutput(t *testing.T) {
	f := realDerivedFixture(t)
	before := sourceDerivedHashes(t, f)
	ledgerBefore, _ := exportFingerprint(f.job)
	value, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil || value.State != "ready" || value.Bytes <= 0 || value.DurationMS < 1500 || value.DurationMS > 1900 || len(value.PartOffsets) != 2 || !value.SourceIncomplete || !value.GapsOmitted {
		t.Fatalf("actual prepare: %+v %v", value, err)
	}
	if value.PartOffsets[1].StartMS+value.PartOffsets[1].DurationMS != value.DurationMS {
		t.Fatal("derived offset end is not measured output end")
	}
	next, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil || next.ID != value.ID {
		t.Fatal("unchanged preparation replaced active artifact", err)
	}
	reader, err := f.service.OpenDerived(context.Background(), f.job.ID, value.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || int64(len(raw)) != value.Bytes {
		t.Fatal("actual derived bytes", err)
	}
	// Decode the actual artifact and verify both distinct source sections survived.
	name := filepath.Join(t.TempDir(), "actual-derived.mkv")
	if err = os.WriteFile(name, raw, 0600); err != nil {
		t.Fatal(err)
	}
	decoded, err := exec.Command("/usr/bin/ffmpeg", "-v", "error", "-threads", "1", "-i", name, "-map", "0:v:0", "-fps_mode", "passthrough", "-f", "framemd5", "-").Output()
	if err != nil {
		t.Fatal("decode derived", err)
	}
	lines := bytes.Split(bytes.TrimSpace(decoded), []byte("\n"))
	checks := map[string]bool{}
	frames := 0
	for _, line := range lines {
		if len(line) > 0 && line[0] != '#' {
			fields := bytes.Split(line, []byte(","))
			checks[string(bytes.TrimSpace(fields[len(fields)-1]))] = true
			frames++
		}
	}
	if frames != 40 || len(checks) != 2 {
		t.Fatalf("derived frame sequence lost an input section: frames%d distinct%d", frames, len(checks))
	}
	pcm, err := exec.Command("/usr/bin/ffmpeg", "-v", "error", "-threads", "1", "-i", name, "-map", "0:a:0", "-c:a", "pcm_s16le", "-f", "s16le", "-").Output()
	if err != nil {
		t.Fatal("decode derived audio", err)
	}
	crossings := func(start int) int {
		count := 0
		var previous int16
		for i := start; i < start+9600; i++ {
			if 2*i+2 > len(pcm) {
				t.Fatal("derived audio section missing")
			}
			sample := int16(binary.LittleEndian.Uint16(pcm[2*i : 2*i+2]))
			if previous <= 0 && sample > 0 {
				count++
			}
			previous = sample
		}
		return count
	}
	firstTone, secondTone := crossings(9600), crossings(48000)
	if firstTone < 80 || firstTone > 100 || secondTone < 160 || secondTone > 190 {
		t.Fatalf("distinct input audio order not preserved: %d/%d", firstTone, secondTone)
	}
	after := sourceDerivedHashes(t, f)
	for i := range before {
		if before[i] != after[i] {
			t.Fatal("join modified original source")
		}
	}
	saved, err := f.repo.Get(context.Background(), f.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	ledgerAfter, _ := exportFingerprint(*saved)
	if ledgerBefore != ledgerAfter {
		t.Fatal("join rewrote recording ledger")
	}
	archive := f.open(t, context.Background(), exportAllowed)
	var zipped bytes.Buffer
	if err = archive.WriteZIP(&zipped); err != nil {
		t.Fatal("original ZIP no longer works", err)
	}
}
func TestDerivedRecordingActualChangedResolutionAndAudioRateAreRejected(t *testing.T) {
	for _, kind := range []string{"resolution", "sample-rate"} {
		t.Run(kind, func(t *testing.T) {
			f := realDerivedFixture(t)
			path := filepath.Join(f.dir, mustExportPartName(2))
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			width, rate := 64, 48000
			if kind == "resolution" {
				width = 96
			} else {
				rate = 44100
			}
			derivedSyntheticPart(t, path, "blue", width, rate)
			info, _ := os.Stat(path)
			f.job.Capture.Parts[1].Bytes = info.Size()
			_ = f.repo.Update(context.Background(), &f.job)
			before := sourceDerivedHashes(t, f)
			if _, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed); !errors.Is(err, ErrDerivedIncompatible) {
				t.Fatal("incompatible actual media accepted", err)
			}
			for i, after := range sourceDerivedHashes(t, f) {
				if before[i] != after {
					t.Fatal("failure repaired an original")
				}
			}
			v, err := f.service.GetDerived(context.Background(), f.job.ID, exportAllowed)
			if err != nil || v.State != "unknown" {
				t.Fatal("failed join published ready output", err)
			}
		})
	}
}
func TestDerivedRecordingActualCurrentAuthorityRevokesReaderWithoutChangingSource(t *testing.T) {
	f := realDerivedFixture(t)
	value, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Bool
	allowed.Store(true)
	auth := func() error {
		if !allowed.Load() {
			return errors.New("revoked")
		}
		return nil
	}
	reader, err := f.service.OpenDerived(context.Background(), f.job.ID, value.ID, auth)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	allowed.Store(false)
	buf := make([]byte, 64)
	if n, err := reader.Read(buf); n != 0 || !errors.Is(err, ErrDerivedAccess) {
		t.Fatalf("revoked reader returned %d bytes: %v", n, err)
	}
}
func TestDerivedRecordingActualRestoreRetiresOldReadyArtifactAndReleasesOwners(t *testing.T) {
	f := realDerivedFixture(t)
	value, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.service.OpenDerived(context.Background(), f.job.ID, value.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		release, e := QuiesceForRestore(ctx)
		if e == nil {
			release()
		}
		done <- e
	}()
	select {
	case <-reader.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("restore did not cancel reader")
	}
	reader.Close()
	if err = <-done; err != nil {
		t.Fatal("restore did not join actual owner", err)
	}
	state, err := f.service.GetDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil || state.State != "unknown" || state.Reason != "source_changed" {
		t.Fatalf("pre-restore cache survived: %+v %v", state, err)
	}
}
func TestDerivedRecordingActualReadersAreBoundedAndSourceMutationRetiresBytes(t *testing.T) {
	f := realDerivedFixture(t)
	value, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	first, err := f.service.OpenDerived(context.Background(), f.job.ID, value.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := f.service.OpenDerived(context.Background(), f.job.ID, value.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err = f.service.OpenDerived(context.Background(), f.job.ID, value.ID, exportAllowed); !errors.Is(err, ErrDerivedBusy) {
		t.Fatal("unbounded concurrent readers", err)
	}
	path := filepath.Join(f.dir, mustExportPartName(1))
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 32)
	if n, err := first.Read(buffer); n != 0 || err == nil {
		t.Fatal("source removal retained derived bytes", n, err)
	}
}
func TestDerivedRecordingActualProbeCannotFollowEmbeddedNetworkManifest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	f := realDerivedFixture(t)
	path := filepath.Join(f.dir, mustExportPartName(1))
	data := []byte("#EXTM3U\n#EXTINF:1,\n" + server.URL + "/not-a-provider.ts\n")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	f.job.Capture.Parts[0].Bytes = int64(len(data))
	_ = f.repo.Update(context.Background(), &f.job)
	if _, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed); err == nil {
		t.Fatal("manifest accepted as MPEGTS")
	}
	if calls.Load() != 0 {
		t.Fatal("probe contacted embedded URL")
	}
}
func TestDerivedRecordingActualHeldReaderRejectsCanonicalJobDirectoryReplacement(t *testing.T) {
	f := realDerivedFixture(t)
	value, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := f.service.OpenDerived(context.Background(), f.job.ID, value.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	moved := f.dir + "-retired"
	if err = os.Rename(f.dir, moved); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(f.dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		name := mustExportPartName(i)
		raw, e := os.ReadFile(filepath.Join(moved, name))
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(f.dir, name), raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	buffer := make([]byte, 64)
	if n, e := reader.Read(buffer); n != 0 || e == nil {
		t.Fatal("old held directory returned derived bytes after replacement", n, e)
	}
}
func TestDerivedRecordingActualBFramesRetainDecodedPresentationOrder(t *testing.T) {
	f := realDerivedFixture(t)
	for i, color := range []string{"red", "blue"} {
		path := filepath.Join(f.dir, mustExportPartName(i+1))
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		derivedSyntheticPart(t, path, color, 64, 48000, "2")
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		f.job.Capture.Parts[i].Bytes = info.Size()
	}
	if err := f.repo.Update(context.Background(), &f.job); err != nil {
		t.Fatal(err)
	}
	value, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil || value.State != "ready" {
		t.Fatalf("valid packet reordering was mistaken for invalid presentation: %+v %v", value, err)
	}
}

func TestDerivedRecordingActualInvalidSourceVersionCanRebuildAtFullCache(t *testing.T) {
	f := realDerivedFixture(t)
	first, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	state := f.service.derivedState()
	state.mu.Lock()
	for i := 0; i < 63; i++ {
		state.entries[fmt.Sprintf("synthetic-other-%d", i)] = &derivedEntry{work: &derivedWork{}, value: DerivedRecording{Bytes: 1, ExpiresAt: first.ExpiresAt}}
	}
	// The current source is still valid media, but its prior version evidence
	// has expired. A full registry must not prevent explicit recovery.
	state.entries[f.job.ID].versions = nil
	state.mu.Unlock()
	second, err := f.service.PrepareDerived(context.Background(), f.job.ID, exportAllowed)
	if err != nil || second.State != "ready" || second.ID == first.ID {
		t.Fatalf("full cache prevented current-source recovery: %+v %v", second, err)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.entries) != 64 {
		t.Fatal("recovery changed the bounded registry", len(state.entries))
	}
}
