package recordings

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

var timingMediaOnce sync.Once
var timingMedia []byte
var timingMediaErr error

func timingMediaBytes(t *testing.T) []byte {
	t.Helper()
	timingMediaOnce.Do(func() {
		cmd := exec.Command("/usr/bin/ffmpeg", "-hide_banner", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=160x90:rate=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "2", "-c:v", "libx264", "-preset", "ultrafast", "-c:a", "aac", "-f", "mpegts", "pipe:1")
		timingMedia, timingMediaErr = cmd.Output()
	})
	if timingMediaErr != nil {
		t.Fatal(timingMediaErr)
	}
	return timingMedia
}
func timingFixture(t *testing.T) *exportFixture {
	t.Helper()
	f := newExportFixture(t)
	if err := os.WriteFile(filepath.Join(f.dir, mustExportPartName(1)), timingMediaBytes(t), 0600); err != nil {
		t.Fatal(err)
	}
	f.service.SetPartTimingProber("/usr/bin/ffprobe")
	return f
}
func TestPartTimingActualFDAndPassiveObservation(t *testing.T) {
	f := timingFixture(t)
	// A missing sibling cannot prevent inspection of this ONE selected part.
	if err := os.Remove(filepath.Join(f.dir, mustExportPartName(3))); err != nil {
		t.Fatal(err)
	}
	before, err := f.service.GetPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if err != nil || before.State != "unknown" {
		t.Fatalf("before: %+v %v", before, err)
	}
	got, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if err != nil || got.State != "container_estimate" || got.ContainerDurationMS == nil || *got.ContainerDurationMS < 1900 || *got.ContainerDurationMS > 2200 || got.Binding == "" {
		t.Fatalf("probe: %+v %v", got, err)
	}
	if got.ObservedBytes == nil || *got.ObservedBytes != int64(len(timingMediaBytes(t))) {
		t.Fatal("fixed FD size absent")
	}
	// The known ledger interval is unrelated; format.duration is not wall time.
	if *got.ContainerDurationMS == f.job.Capture.Parts[1].EndedAt.Sub(f.job.Capture.Parts[1].StartedAt).Milliseconds() {
		t.Fatal("wall clock substituted")
	}
	raw, _ := json.Marshal(got)
	if bytes.Contains(raw, []byte("PRIVATE_")) || bytes.Contains(raw, []byte(f.dir)) {
		t.Fatal("private fields escaped")
	}
	if err = os.RemoveAll(f.service.outputDir); err != nil {
		t.Fatal(err)
	}
	cached, err := f.service.GetPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(got)
	b, _ := json.Marshal(cached)
	if !bytes.Equal(a, b) {
		t.Fatal("passive GET touched files/renewed observation")
	}
	*cached.ContainerDurationMS = 1
	*cached.ObservedBytes = 1
	*cached.ObservedAt = time.Time{}
	next, _ := f.service.GetPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if *next.ContainerDurationMS != *got.ContainerDurationMS || next.ObservedAt.IsZero() {
		t.Fatal("cache was aliased")
	}
	state := f.service.timingState()
	state.mu.Lock()
	e := state.entries[timingKey(f.job.ID, 1)]
	past := time.Now().Add(-time.Second)
	e.value.ExpiresAt = &past
	state.entries[timingKey(f.job.ID, 1)] = e
	state.mu.Unlock()
	expired, err := f.service.GetPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if err != nil || expired.State != "unknown" || expired.Reason != "observation_expired" {
		t.Fatalf("expired %+v %v", expired, err)
	}
}
func TestPartTimingRejectsAuthorityLedgerAndPathBeforeProcess(t *testing.T) {
	for _, mode := range []string{"deny", "active", "restored", "unclosed", "invalid time", "missing index", "symlink", "fifo", "empty", "missing", "wrong extension content"} {
		t.Run(mode, func(t *testing.T) {
			f := timingFixture(t)
			name := filepath.Join(f.dir, mustExportPartName(1))
			index := 1
			authorize := exportAllowed
			switch mode {
			case "deny":
				authorize = func() error { return ErrRecordingExportAccess }
				f.service.outputDir = "must-not-touch"
			case "active":
				f.job.Status = "running"
			case "restored":
				f.job.Capture.Restored = true
			case "unclosed":
				f.job.Capture.Parts[1].EndedAt = nil
			case "invalid time":
				f.job.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			case "missing index":
				index = 2
			case "symlink":
				_ = os.Remove(name)
				if err := os.Symlink(filepath.Join(f.dir, mustExportPartName(3)), name); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				_ = os.Remove(name)
				if err := syscall.Mkfifo(name, 0600); err != nil {
					t.Fatal(err)
				}
			case "empty":
				_ = os.WriteFile(name, nil, 0600)
			case "missing":
				_ = os.Remove(name)
			case "wrong extension content":
				_ = os.WriteFile(name, []byte("not MPEGTS despite its suffix"), 0600)
			}
			_ = f.repo.Update(context.Background(), &f.job)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			got, err := f.service.RefreshPartTiming(ctx, f.job.ID, index, authorize)
			if mode == "wrong extension content" {
				if err != nil || got.State != "unknown" {
					t.Fatalf("unsupported %+v %v", got, err)
				}
			} else if err == nil {
				t.Fatalf("accepted %s %+v", mode, got)
			}
			if got.ContainerDurationMS != nil {
				t.Fatal("failed inspection published numeric fact")
			}
		})
	}
}
func timingProcessHelper(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "probe")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
func waitTimingFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("process did not reach barrier")
}
func TestPartTimingRetirementKillsAndJoinsBeforeSlotRelease(t *testing.T) {
	for _, mode := range []string{"cancel", "authority", "ledger", "grow", "replace", "deadline", "reconfigure"} {
		t.Run(mode, func(t *testing.T) {
			f := timingFixture(t)
			barrier := filepath.Join(t.TempDir(), "started")
			f.service.SetPartTimingProber(timingProcessHelper(t, fmt.Sprintf("echo $$ > %q\nexec sleep 60\n", barrier)))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var valid atomic.Bool
			valid.Store(true)
			authorize := func() error {
				if !valid.Load() {
					return ErrRecordingExportAccess
				}
				return nil
			}
			if mode == "deadline" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 300*time.Millisecond)
				defer stop()
			}
			result := make(chan error, 1)
			go func() { _, err := f.service.RefreshPartTiming(ctx, f.job.ID, 1, authorize); result <- err }()
			waitTimingFile(t, barrier)
			name := filepath.Join(f.dir, mustExportPartName(1))
			switch mode {
			case "cancel":
				cancel()
			case "authority":
				valid.Store(false)
			case "ledger":
				f.repo.mu.Lock()
				r := f.repo.recordings[f.job.ID]
				r.Capture = nil
				f.repo.recordings[f.job.ID] = r
				f.repo.mu.Unlock()
			case "grow":
				fd, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = fd.Write([]byte("changed"))
				_ = fd.Close()
			case "replace":
				_ = os.Rename(name, name+".old")
				_ = os.WriteFile(name, timingMediaBytes(t), 0600)
			case "reconfigure":
				f.service.SetPartTimingProber("/usr/bin/ffprobe")
			}
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("retired probe succeeded")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("probe not joined")
			}
			pidRaw, err := os.ReadFile(barrier)
			if err != nil {
				t.Fatal(err)
			}
			var pid int
			if _, err = fmt.Sscan(string(pidRaw), &pid); err != nil {
				t.Fatal(err)
			}
			if err = syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
				t.Fatalf("child still exists: %v", err)
			}
			if len(partTimingSlots) != 0 {
				t.Fatal("slot released inaccurately")
			}
			state := f.service.timingState()
			state.mu.Lock()
			n := len(state.entries)
			state.mu.Unlock()
			if n != 0 {
				t.Fatal("retired probe published observation")
			}
		})
	}
}
func TestPartTimingTwoProcessesAndSameTargetNoQueue(t *testing.T) {
	fixtures := []*exportFixture{timingFixture(t), timingFixture(t), timingFixture(t)}
	var cancels []context.CancelFunc
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		f := fixtures[i]
		barrier := filepath.Join(t.TempDir(), "started")
		f.service.SetPartTimingProber(timingProcessHelper(t, fmt.Sprintf("echo $$ > %q\nexec sleep 60\n", barrier)))
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		defer cancel()
		go func() { _, err := f.service.RefreshPartTiming(ctx, f.job.ID, 1, exportAllowed); results <- err }()
		waitTimingFile(t, barrier)
	}
	for _, f := range []*exportFixture{fixtures[0], fixtures[2]} {
		if _, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed); !errors.Is(err, ErrPartTimingBusy) {
			t.Fatalf("expected busy: %v", err)
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	for range cancels {
		select {
		case err := <-results:
			if err == nil {
				t.Fatal("cancel accepted")
			}
		case <-time.After(3 * time.Second):
			t.Fatal("process not joined")
		}
	}
	got, err := fixtures[2].service.RefreshPartTiming(context.Background(), fixtures[2].job.ID, 1, exportAllowed)
	if err != nil || got.State != "container_estimate" {
		t.Fatalf("slots not reusable %+v %v", got, err)
	}
}
func TestPartTimingParserFiniteConsistentOnly(t *testing.T) {
	for _, duration := range []string{"0", "-1", "NaN", "+Inf", "1e100", "100"} {
		raw := []byte(fmt.Sprintf(`{"format":{"format_name":"mpegts","duration":%q,"size":"10"},"streams":[{"codec_type":"video","codec_name":"h264","duration":"2"}]}`, duration))
		if _, ok := parsePartTiming(raw, 10); ok {
			t.Fatalf("accepted %s", duration)
		}
	}
	for _, raw := range []string{`{}`, `{"format":{"format_name":"hls","duration":"2","size":"10"}}`, `{"format":{"format_name":"mpegts","duration":"2","size":"9"}}`} {
		if _, ok := parsePartTiming([]byte(raw), 10); ok {
			t.Fatal("accepted unbound inventory")
		}
	}
	args := strings.Join(partTimingArgs(), " ")
	if strings.Contains(args, "http") || strings.Contains(args, "file,") {
		t.Fatal("unsafe protocol arguments")
	}
}
func TestPartTimingSynchronousReconfigureWithinAuthority(t *testing.T) {
	for _, operation := range []string{"GET after cache read", "POST before open"} {
		t.Run(operation, func(t *testing.T) {
			f := timingFixture(t)
			if operation == "GET after cache read" {
				if _, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed); err != nil {
					t.Fatal(err)
				}
			}
			original := f.service.timingState()
			calls := 0
			authorize := func() error {
				calls++
				if calls == 3 {
					f.service.SetPartTimingProber("must-not-run-reconfigured-probe")
					if f.service.currentTimingState(original) {
						t.Fatal("retired state remains current synchronously")
					}
				}
				return nil
			}
			var got PartTiming
			var err error
			if operation == "GET after cache read" {
				got, err = f.service.GetPartTiming(context.Background(), f.job.ID, 1, authorize)
			} else {
				got, err = f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, authorize)
			}
			if err == nil || got.ContainerDurationMS != nil || calls < 3 {
				t.Fatalf("old state adopted: calls=%d %+v %v", calls, got, err)
			}
		})
	}
}
func TestPartTimingCachedObservationIsolatedConcurrentCopies(t *testing.T) {
	f := timingFixture(t)
	original, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := json.Marshal(original)
	var wg sync.WaitGroup
	for n := 0; n < 12; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				got, err := f.service.GetPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
				if err != nil {
					t.Error(err)
					return
				}
				raw, _ := json.Marshal(got)
				if !bytes.Equal(raw, expected) {
					t.Error("another caller mutated stored observation")
					return
				}
				*got.ContainerDurationMS = 1
				*got.ObservedBytes = 1
				*got.ObservedAt = time.Time{}
				*got.ExpiresAt = time.Time{}
			}
		}()
	}
	wg.Wait()
}
func TestPartTimingMalformedSuccessfulProcessCannotPublish(t *testing.T) {
	for _, mode := range []string{"malformed JSON", "oversized stdout", "missing binary", "mismatched size", "non finite duration"} {
		t.Run(mode, func(t *testing.T) {
			f := timingFixture(t)
			switch mode {
			case "missing binary":
				f.service.SetPartTimingProber(filepath.Join(t.TempDir(), "absent"))
			case "oversized stdout":
				f.service.SetPartTimingProber(timingProcessHelper(t, "exec head -c 131072 /dev/zero\n"))
			case "malformed JSON":
				f.service.SetPartTimingProber(timingProcessHelper(t, "printf 'not json'\n"))
			case "mismatched size":
				f.service.SetPartTimingProber(timingProcessHelper(t, `printf '%s' '{"format":{"format_name":"mpegts","duration":"2","size":"1"},"streams":[{"codec_type":"video","codec_name":"h264","duration":"2"}]}'`))
			case "non finite duration":
				f.service.SetPartTimingProber(timingProcessHelper(t, fmt.Sprintf(`printf '%%s' '{"format":{"format_name":"mpegts","duration":"NaN","size":"%d"},"streams":[{"codec_type":"video","codec_name":"h264","duration":"2"}]}'`, len(timingMediaBytes(t)))))
			}
			got, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
			if err != nil || got.State != "unknown" || got.ContainerDurationMS != nil {
				t.Fatalf("invalid result %+v %v", got, err)
			}
			if len(partTimingSlots) != 0 {
				t.Fatal("process result failed to release joined slot")
			}
			again, err := f.service.GetPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
			if err != nil || again.State != "unknown" {
				t.Fatal("malformed result was cached")
			}
		})
	}
}
func TestPartTimingSuccessfulChildThenAuthorityLossCannotPublish(t *testing.T) {
	f := timingFixture(t)
	marker := filepath.Join(t.TempDir(), "child-completed")
	// A process0 result alone is not authority. This stand-in returns valid shaped
	// inventory, then the original authority reader sees its completion barrier.
	inventory := fmt.Sprintf(`{"format":{"format_name":"mpegts","duration":"2","size":"%d"},"streams":[{"codec_type":"video","codec_name":"h264","duration":"2"}]}`, len(timingMediaBytes(t)))
	f.service.SetPartTimingProber(timingProcessHelper(t, fmt.Sprintf("printf '%%s' '%s'\ntouch %q\n", inventory, marker)))
	authorize := func() error {
		if _, err := os.Stat(marker); err == nil {
			return ErrRecordingExportAccess
		}
		return nil
	}
	got, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, authorize)
	if !errors.Is(err, ErrRecordingExportAccess) || got.ContainerDurationMS != nil {
		t.Fatalf("process0 regained authority %+v %v", got, err)
	}
	state := f.service.timingState()
	state.mu.Lock()
	count := len(state.entries)
	state.mu.Unlock()
	if count != 0 {
		t.Fatal("denied final result was cached")
	}
}
