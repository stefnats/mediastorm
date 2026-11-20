//go:build linux && amd64

package localmedia

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/services/profileunlock"
)

// The helper is built by the reviewed explicit script before these opt-in
// tests. Tests never compile/download a helper or discover an unpinned tool.
func familyProbeTestTools(t *testing.T) (*FamilyLocalProbeTools, string) {
	t.Helper()
	path := os.Getenv("FAMILY_LOCAL_PROBE_TEST_TOOLS_MANIFEST")
	if path == "" {
		t.Skip("explicit Root-built local probe artifact manifest required")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("absolute probe artifact manifest required")
	}
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) > 16<<10 {
		t.Fatal("bounded probe artifact manifest required")
	}
	var manifest struct {
		FamilyLocalProbeConfig
		ABI, Platform string
		InputCapBytes int64
		Installed     bool
	}
	if json.Unmarshal(raw, &manifest) != nil || manifest.ABI != familyLocalProbeABI || manifest.Platform != "linux/amd64" || manifest.InputCapBytes != familyLocalProbeMaxInput || manifest.Installed {
		t.Fatal("unreviewed helper ABI/platform/cap/installation")
	}
	tools, err := NewFamilyLocalProbeTools(manifest.FamilyLocalProbeConfig)
	if err != nil {
		t.Fatal("open explicitly pinned probe tools", err)
	}
	t.Cleanup(tools.Close)
	ffmpeg := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG")
	hash := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG_SHA256")
	opened, _, err := openFamilyProbeExecutable(ffmpeg, hash)
	if err != nil {
		t.Fatal("explicit pinned fixture-generator executable required")
	}
	_ = opened.Close()
	return tools, ffmpeg
}

func familyProbeActualFile(t *testing.T, ffmpeg, variant string, originalLifetime ...time.Duration) (*familyLocalFileFixture, profileunlock.FamilyLocalFile) {
	t.Helper()
	f := familyLocalFilePG(t)
	// Retire the old synthetic seed before generating a DIFFERENT file and
	// capturing a new genuine PG owner/source. The old proof is never amended.
	f.owner.Close()
	format, video, audio := "matroska", "libx264", "aac"
	switch variant {
	case "ts":
		format = "mpegts"
	case "tail-mp4":
		format = "mp4"
	case "legacy-mkv":
		video = "mpeg4"
		audio = "ac3"
	case "mkv":
	default:
		t.Fatal("unknown synthetic media variant")
	}
	args := []string{"-v", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "color=c=blue:s=64x48:r=24:d=0.5", "-f", "lavfi", "-i", "sine=frequency=880:sample_rate=48000:duration=0.5", "-map", "0:v:0", "-map", "1:a:0", "-c:v", video, "-threads", "1", "-pix_fmt", "yuv420p", "-c:a", audio, "-metadata:s:a:0", "language=fra", "-f", format, f.path}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, ffmpeg, args...)
	cmd.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("generate bounded synthetic media", err, string(out))
	}
	info, err := os.Stat(f.path)
	if err != nil || info.Size() <= 0 || info.Size() > 1<<20 {
		t.Fatal("synthetic media size invalid")
	}
	if _, err = f.ds.Pool().Exec(t.Context(), "UPDATE local_media_items SET size_bytes=$1 WHERE id='item'", info.Size()); err != nil {
		t.Fatal(err)
	}
	if len(originalLifetime) > 0 {
		if _, err = f.ds.Pool().Exec(t.Context(), "UPDATE sessions SET expires_at=$1 WHERE token='SYNTHETIC_FAMILY_LOCAL_SESSION'", time.Now().UTC().Add(originalLifetime[0])); err != nil {
			t.Fatal(err)
		}
	}
	pins := profileunlock.NewPGProfilePins(f.ds)
	r := httptest.NewRequest("GET", "http://synthetic.invalid/private-probe", nil).WithContext(t.Context())
	checked, release, err := pins.Authenticate(r, "SYNTHETIC_FAMILY_LOCAL_SESSION", "synthetic-device")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	f.owner, err = pins.CaptureFamilyMediaOwner(checked, "target")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.owner.Close)
	f.source, err = f.owner.CaptureLocalSource(t.Context(), "item")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.source.Close)
	return f, familyOpenLocal(t, f)
}

func familyProbeAssertRetired(t *testing.T, j *familyLocalProbeJob) {
	t.Helper()
	select {
	case <-j.token.done:
	default:
		t.Fatal("job returned before lifetime completion")
	}
	if j.started && (!j.waited || j.cmd.ProcessState == nil || syscall.Kill(j.cmd.Process.Pid, 0) != syscall.ESRCH) {
		t.Fatal("actual process was not Wait-joined")
	}
	for _, f := range []*os.File{j.helper, j.probe, j.readyRead, j.readyWrite, j.ackRead, j.ackWrite, j.zero, j.reader.file} {
		if f != nil {
			if _, err := f.Stat(); err == nil {
				t.Fatal("job retained an actual descriptor")
			}
		}
	}
	j.file.mu.Lock()
	registered := j.file.readers[j.reader]
	j.file.mu.Unlock()
	familyLocalProbeAdmission.Lock()
	charged := familyLocalProbeAdmission.files[j.file]
	familyLocalProbeAdmission.Unlock()
	if registered || charged {
		t.Fatal("joined job retained reader/job admission")
	}
}

func TestFamilyLocalProbeActualPGFourMediaVariants(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	for _, variant := range []string{"ts", "mkv", "tail-mp4", "legacy-mkv"} {
		t.Run(variant, func(t *testing.T) {
			f, file := familyProbeActualFile(t, ffmpeg, variant)
			j, err := startFamilyLocalProbe(t.Context(), f.service, file, tools, false)
			if err != nil {
				t.Fatal("start actual pinned FD helper", err)
			}
			defer j.close()
			// The versioned ready record comes from the actual sandboxed helper PID,
			// which is still waiting for our source-checked start acknowledgement.
			if j.acked || j.cmd.Process.Pid <= 0 {
				t.Fatal("missing actual pre-exec helper boundary")
			}
			if variant == "tail-mp4" {
				at, err := j.reader.file.Seek(0, io.SeekCurrent)
				if err != nil || at != 0 {
					t.Fatal("preflight changed original duplicate offset")
				}
			}
			out, err := j.finish()
			if err != nil {
				t.Fatal("actual ffprobe output rejected", err)
			}
			wantVideo, wantAudio := "h264", "aac"
			if variant == "legacy-mkv" {
				wantVideo, wantAudio = "mpeg4", "ac3"
			}
			video, audio := false, false
			for _, s := range out.Streams {
				if s.Kind == "video" && s.Codec == wantVideo && s.Width == 64 && s.Height == 48 {
					video = true
				}
				if s.Kind == "audio" && s.Codec == wantAudio && s.SampleRate == 48000 {
					audio = true
				}
			}
			if !video || !audio || len(out.Streams) != 2 {
				t.Fatal("actual technical media metadata mismatch", out)
			}
			if variant == "tail-mp4" {
				at, err := j.reader.file.Seek(0, io.SeekCurrent)
				if err != nil || at <= 0 {
					t.Fatal("tail movie did not use actual seekable original FD")
				}
			}
			encoded, _ := json.Marshal(out)
			if strings.Contains(string(encoded), f.path) || strings.Contains(string(encoded), "SYNTHETIC_") {
				t.Fatal("source or credential escaped reduced output")
			}
			j.close()
			familyProbeAssertRetired(t, j)
			if variant == "mkv" {
				public, err := f.service.ProbeFamilyLocalFile(t.Context(), file, tools)
				if err != nil || len(public.Streams) != 2 || public.Container != "matroska" {
					t.Fatal("private service API did not join and publish the actual probe", err)
				}
			}
		})
	}
}

func TestFamilyLocalProbeActualHelperInstallsNetworkDenials(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	f, file := familyProbeActualFile(t, ffmpeg, "mkv")
	j, err := startFamilyLocalProbe(t.Context(), f.service, file, tools, true)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if j.ack() != nil || j.wait() != nil {
		t.Fatal("actual helper filter self-test failed")
	}
	raw, failed := j.stdout.snapshot()
	defer clear(raw)
	var out struct {
		ABI                            string
		DeniedChecks, Expected, Filter int
	}
	if failed || json.Unmarshal(raw, &out) != nil || out.ABI != familyLocalProbeABI || out.DeniedChecks != 11 || out.Expected != 11 || out.Filter != 2 {
		t.Fatal("actual socket/io_uring/x32/injection denials were not observed")
	}
	j.close()
	familyProbeAssertRetired(t, j)
}

func TestFamilyLocalProbeRejectsInterfaceWrappersForeignStoreAndClosedFile(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	f, file := familyProbeActualFile(t, ffmpeg, "mkv")
	type wrapped struct{ profileunlock.FamilyLocalFile }
	for _, input := range []profileunlock.FamilyLocalFile{wrapped{file}, &familyLocalFile{self: file.(*familyLocalFile)}, nil} {
		if out, err := f.service.ProbeFamilyLocalFile(t.Context(), input, tools); err == nil || len(out.Streams) != 0 {
			t.Fatal("imported/wrapped file acquired process")
		}
	}
	foreign := &Service{store: &datastore.DataStore{}}
	if _, err := foreign.ProbeFamilyLocalFile(t.Context(), file, tools); err == nil {
		t.Fatal("foreign service acquired original file")
	}
	copyTools := *tools
	if _, err := f.service.ProbeFamilyLocalFile(t.Context(), file, &copyTools); err == nil {
		t.Fatal("copied tool authority used")
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.ProbeFamilyLocalFile(t.Context(), file, tools); err == nil {
		t.Fatal("closed file probed")
	}
}

func TestFamilyLocalProbePinnedExecutableIdentityAndHash(t *testing.T) {
	tools, _ := familyProbeTestTools(t)
	for _, config := range []FamilyLocalProbeConfig{{}, {HelperPath: tools.config.HelperPath, HelperSHA256: strings.Repeat("0", 64), FFprobePath: tools.config.FFprobePath, FFprobeSHA256: tools.config.FFprobeSHA256}, {HelperPath: "relative", HelperSHA256: tools.config.HelperSHA256, FFprobePath: tools.config.FFprobePath, FFprobeSHA256: tools.config.FFprobeSHA256}} {
		if got, err := NewFamilyLocalProbeTools(config); err == nil || got != nil {
			if got != nil {
				got.Close()
			}
			t.Fatal("invalid pins acquired tool descriptors")
		}
	}
	data, err := os.ReadFile(tools.config.HelperPath)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "helper")
	if err = os.WriteFile(path, data, 0700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	opened, stat, err := openFamilyProbeExecutable(path, hex.EncodeToString(hash[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if err = os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0700); err != nil {
		t.Fatal(err)
	}
	if familyProbeExecutableCurrent(opened, path, stat) {
		t.Fatal("same bytes on a substituted executable inode accepted")
	}
}

func TestFamilyLocalProbeConfiguredFIFORejectsWithoutWriterAndClosesDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "configured-helper-fifo")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		file *os.File
		err  error
	}
	entered, done := make(chan struct{}), make(chan outcome, 1)
	go func() {
		close(entered)
		file, _, err := openFamilyProbeExecutable(path, strings.Repeat("0", 64))
		done <- outcome{file, err}
	}()
	<-entered
	var got outcome
	select {
	case got = <-done:
	case <-time.After(time.Second):
		// If a future change restores blocking open, release that actual FIFO
		// wait and join the worker before reporting the regression.
		writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			_ = writer.Close()
		}
		select {
		case got = <-done:
			if got.file != nil {
				_ = got.file.Close()
			}
		case <-time.After(time.Second):
			t.Fatal("configured FIFO acquisition remained blocked after cleanup release")
		}
		t.Fatal("configured FIFO waited for a writer before rejecting its type")
	}
	if got.file != nil {
		_ = got.file.Close()
	}
	if got.file != nil || got.err == nil {
		t.Fatal("configured FIFO became executable tools")
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		named, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && named == path {
			t.Fatal("rejected FIFO retained its actual descriptor")
		}
	}
}

func TestFamilyLocalProbeCurrentPinACLAndFileRejectLateActualOutput(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	for _, change := range []string{"pin", "acl", "file", "session"} {
		t.Run(change, func(t *testing.T) {
			f, file := familyProbeActualFile(t, ffmpeg, "mkv")
			j, err := startFamilyLocalProbe(t.Context(), f.service, file, tools, false)
			if err != nil {
				t.Fatal(err)
			}
			defer j.close()
			if j.ack() != nil || j.wait() != nil {
				t.Fatal("real ffprobe did not finish before late change")
			}
			raw, failed := j.stdout.snapshot()
			if failed || len(raw) == 0 {
				t.Fatal("no actual staged metadata before change")
			}
			clear(raw)
			switch change {
			case "pin":
				_, err = f.ds.Pool().Exec(t.Context(), "UPDATE users SET pin_hash='synthetic-changed' WHERE id='target'")
			case "acl":
				_, err = f.ds.Pool().Exec(t.Context(), "UPDATE media_library_access_policies SET access_mode=access_mode WHERE library_id='library'")
			case "session":
				_, err = f.ds.Pool().Exec(t.Context(), "UPDATE sessions SET expires_at=now()-interval '1 second' WHERE token='SYNTHETIC_FAMILY_LOCAL_SESSION'")
			case "file":
				err = os.Rename(f.path, f.path+".retired")
			}
			if err != nil {
				t.Fatal(err)
			}
			if out, err := j.result(); err == nil || len(out.Streams) != 0 || out.DurationSeconds != nil || out.Container != "" {
				t.Fatal("late metadata escaped original revocation")
			}
			j.close()
			familyProbeAssertRetired(t, j)
		})
	}
}

func TestFamilyLocalProbeSourceRetirementWaitsForActualJobResources(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	f, file := familyProbeActualFile(t, ffmpeg, "mkv")
	j, err := startFamilyLocalProbe(t.Context(), f.service, file, tools, false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	// Replace only this job's cancellation wrapper with a controlled delayed
	// completion around its real cancel. No PG authority or source is replaced.
	j.stopReader()
	life, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	j.stopReader = familyLocalAfterFunc(life, func() { j.token.cancel(); close(entered); <-release })
	cancel()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("actual cancellation wrapper not entered")
	}
	ownerDone := make(chan struct{})
	go func() { f.owner.Close(); close(ownerDone) }()
	jobDone := make(chan struct{})
	go func() { j.close(); close(jobDone) }()
	select {
	case <-ownerDone:
		t.Fatal("original owner refunded before job resources/callback joined")
	case <-jobDone:
		t.Fatal("job skipped cancellation wrapper join")
	case <-time.After(20 * time.Millisecond):
	}
	j.file.mu.Lock()
	registered := j.file.readers[j.reader]
	j.file.mu.Unlock()
	familyLocalProbeAdmission.Lock()
	charged := familyLocalProbeAdmission.files[j.file]
	familyLocalProbeAdmission.Unlock()
	if !registered || !charged {
		t.Fatal("original registered reader/job refunded while callback held")
	}
	if _, err = j.helper.Stat(); err != nil {
		t.Fatal("test did not retain actual job-owned executable FD before barrier")
	}
	unblock()
	for _, done := range []chan struct{}{jobDone, ownerDone} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("actual source/process retirement did not join")
		}
	}
	familyProbeAssertRetired(t, j)
}

func TestFamilyLocalProbeSourceCloseDuringOriginalPreReaderGuardOwnsNoJobFDs(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	f, file := familyProbeActualFile(t, ffmpeg, "mkv")
	countToolFDs := func() [2]int {
		t.Helper()
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		var count [2]int
		for _, entry := range entries {
			actual, err := os.Stat(filepath.Join("/proc/self/fd", entry.Name()))
			if err != nil {
				continue
			}
			for index, original := range []os.FileInfo{tools.helperInfo, tools.probeInfo} {
				if os.SameFile(actual, original) {
					count[index]++
				}
			}
		}
		return count
	}
	before := countToolFDs()
	if before[0] < 1 || before[1] < 1 {
		t.Fatal("missing actual base pinned tool descriptors")
	}
	tx, err := f.ds.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		job *familyLocalProbeJob
		err error
	}
	result, exited := make(chan outcome, 1), make(chan struct{})
	go func() {
		defer close(exited)
		job, err := startFamilyLocalProbe(t.Context(), f.service, file, tools, false)
		if job != nil {
			job.close()
		}
		result <- outcome{job, err}
	}()
	// On every failure retire the genuine source and join startup before test
	// cleanup can release tools or its original PG fixture.
	defer func() {
		f.source.Close()
		select {
		case <-exited:
		case <-time.After(3 * time.Second):
			t.Error("pre-reader startup did not join actual cancellation")
		}
	}()
	waiting := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if err = f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM accounts a WHERE a.id=%')`, f.application).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !waiting {
		t.Fatal("startup never reached its actual initial blocked PG guard")
	}
	select {
	case <-exited:
		t.Fatal("startup finished before held original guard cancellation")
	default:
	}
	file.(*familyLocalFile).mu.Lock()
	readers := len(file.(*familyLocalFile).readers)
	file.(*familyLocalFile).mu.Unlock()
	tools.mu.Lock()
	pending := len(tools.jobs)
	tools.mu.Unlock()
	familyLocalProbeAdmission.Lock()
	charged := familyLocalProbeAdmission.files[file.(*familyLocalFile)]
	familyLocalProbeAdmission.Unlock()
	if readers != 0 || pending != 1 || !charged {
		t.Fatal("test did not hold bounded original pre-reader acquisition")
	}
	if actual := countToolFDs(); actual != before {
		t.Fatal("job duplicated executable FDs before genuine reader hold", before, actual)
	}
	closed := make(chan struct{})
	go func() { f.source.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("source Close did not finish actual pre-reader cancellation")
	}
	select {
	case got := <-result:
		if got.job != nil || got.err == nil {
			t.Fatal("retired initial guard progressed to actual process startup")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initial PG query did not join while lock remained held")
	}
	<-exited
	if actual := countToolFDs(); actual != before {
		t.Fatal("canceled pre-reader acquisition retained job descriptors")
	}
	tools.mu.Lock()
	pending = len(tools.jobs)
	tools.mu.Unlock()
	familyLocalProbeAdmission.Lock()
	charged = familyLocalProbeAdmission.files[file.(*familyLocalFile)]
	familyLocalProbeAdmission.Unlock()
	if pending != 0 || charged {
		t.Fatal("joined pre-reader preparation retained bounded admission")
	}
}

func TestFamilyLocalProbeToolsCloseCancelsAndJoinsActualReadyHelper(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	f, file := familyProbeActualFile(t, ffmpeg, "mkv")
	j, err := startFamilyLocalProbe(t.Context(), f.service, file, tools, false)
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	finished := make(chan error, 1)
	go func() { <-j.token.ctx.Done(); _, err := j.finish(); j.close(); finished <- err }()
	closed := make(chan struct{})
	go func() { tools.Close(); close(closed) }()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("canceled ready helper published output")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tool cancellation did not stop actual helper")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("tools Close returned before/join failed actual owned job")
	}
	familyProbeAssertRetired(t, j)
	if _, err := f.service.ProbeFamilyLocalFile(t.Context(), file, tools); err == nil {
		t.Fatal("closed tools started another job")
	}
}

func TestFamilyLocalProbeCallerCancelAndOriginalSessionExpiryStopReadyHelper(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	for _, kind := range []string{"caller", "original-expiry"} {
		t.Run(kind, func(t *testing.T) {
			var lifetimes []time.Duration
			if kind == "original-expiry" {
				lifetimes = []time.Duration{3 * time.Second}
			}
			f, file := familyProbeActualFile(t, ffmpeg, "mkv", lifetimes...)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			j, err := startFamilyLocalProbe(ctx, f.service, file, tools, false)
			if err != nil {
				t.Fatal("start actual helper before cancellation boundary", err)
			}
			defer j.close()
			if kind == "caller" {
				cancel()
			} else {
				deadline, ok := j.token.ctx.Deadline()
				if !ok || !deadline.Equal(f.source.ExpiresAt()) {
					t.Fatal("worker did not retain exact original PG expiry")
				}
			}
			select {
			case <-j.token.ctx.Done():
			case <-time.After(4 * time.Second):
				t.Fatal("original cancel/expiry did not signal the actual helper")
			}
			if out, err := j.finish(); err == nil || len(out.Streams) != 0 {
				t.Fatal("canceled original published metadata")
			}
			j.close()
			familyProbeAssertRetired(t, j)
		})
	}
}

func TestFamilyLocalProbeAdmissionBeforeCurrentWorkAndRealReaderCap(t *testing.T) {
	tools, ffmpeg := familyProbeTestTools(t)
	f1, p1 := familyProbeActualFile(t, ffmpeg, "mkv")
	f2, p2 := familyProbeActualFile(t, ffmpeg, "mkv")
	f3, p3 := familyProbeActualFile(t, ffmpeg, "mkv")
	j1, err := startFamilyLocalProbe(t.Context(), f1.service, p1, tools, false)
	if err != nil {
		t.Fatal(err)
	}
	defer j1.close()
	if j, err := startFamilyLocalProbe(t.Context(), f1.service, p1, tools, false); err == nil || j != nil {
		if j != nil {
			j.close()
		}
		t.Fatal("per-original probe admission exceeded")
	}
	j2, err := startFamilyLocalProbe(t.Context(), f2.service, p2, tools, false)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.close()
	if j, err := startFamilyLocalProbe(t.Context(), f3.service, p3, tools, false); err == nil || j != nil {
		if j != nil {
			j.close()
		}
		t.Fatal("global two-job admission exceeded")
	}
	p3.(*familyLocalFile).mu.Lock()
	readers := len(p3.(*familyLocalFile).readers)
	p3.(*familyLocalFile).mu.Unlock()
	if readers != 0 {
		t.Fatal("rejected job opened actual source reader")
	}
	j1.close()
	familyProbeAssertRetired(t, j1)
	j3, err := startFamilyLocalProbe(t.Context(), f3.service, p3, tools, false)
	if err != nil {
		t.Fatal("joined cleanup did not refund", err)
	}
	j3.close()
	familyProbeAssertRetired(t, j3)
}

func TestFamilyLocalProbeBoundedWritersCancelAndClear(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b := &familyLocalProbeBuffer{maximum: 8, cancel: cancel}
	if n, err := b.Write([]byte("12345678")); n != 8 || err != nil {
		t.Fatal("bounded output not accepted")
	}
	if n, err := b.Write([]byte("9")); n != 0 || err == nil || ctx.Err() == nil {
		t.Fatal("oversized output did not cancel producer")
	}
	b.clear()
	raw, failed := b.snapshot()
	if len(raw) != 0 || !failed {
		t.Fatal("failed output retained bytes or failure state")
	}
}

func TestFamilyLocalProbeJobFormattingAndSelfBinding(t *testing.T) {
	j := &familyLocalProbeJob{familyLocalProbeJobState: &familyLocalProbeJobState{format: "PRIVATE_METADATA"}}
	j.self = j
	copy := *j
	if copy.valid() {
		t.Fatal("copied private job acquired cleanup authority")
	}
	for _, value := range []any{j, *j, &copy, copy} {
		if s := fmt.Sprintf("%#v", value); strings.Contains(s, "PRIVATE") || !strings.Contains(s, "opaque") {
			t.Fatal("private job state disclosed")
		}
		if b, err := json.Marshal(value); err != nil || string(b) != "null" {
			t.Fatal("private job serialized")
		}
	}
}
