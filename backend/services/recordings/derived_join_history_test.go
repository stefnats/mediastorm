//go:build linux

package recordings

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func historyPrepared(t *testing.T, f *exportFixture) (DerivedRecording, DerivedHistoryData) {
	t.Helper()
	if _, err := f.service.PrepareDerived(t.Context(), f.job.ID, exportAllowed); err != nil {
		t.Fatal(err)
	}
	v, h, err := f.service.GetDerivedWithHistoryData(t.Context(), f.job.ID, exportAllowed)
	if err != nil || v.State != "ready" || h.RecordingID != v.RecordingID || h.ArtifactID != v.ID {
		t.Fatal("actual paired observation unavailable", err)
	}
	return v, h
}
func TestDerivedHistoryActualCapture(t *testing.T) {
	for _, kind := range []string{"actual-input-ledger-output", "deep-owned-original-ttl", "source-change-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := realDerivedFixture(t)
			v, h := historyPrepared(t, f)
			switch kind {
			case "actual-input-ledger-output":
				if len(h.Inputs) != 2 || h.OutputSHA256 == nil || h.ToolSHA256 == nil || h.TransformSHA256 == nil {
					t.Fatal("actual evidence missing")
				}
				for i, input := range h.Inputs {
					raw, err := os.ReadFile(filepath.Join(f.dir, mustExportPartName(i+1)))
					if err != nil || input.PartIndex != i+1 || input.ByteLength != int64(len(raw)) || input.SHA256 != derivedHistoryDigest(raw) {
						t.Fatal("input digest did not describe actual staged bytes", err)
					}
				}
				ledger, err := exportFingerprint(f.job)
				if err != nil || h.LedgerSHA256 != fmtHistoryHash(ledger) {
					t.Fatal("ledger changed", err)
				}
				reader, err := f.service.OpenDerived(t.Context(), f.job.ID, v.ID, exportAllowed)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := io.ReadAll(reader)
				closeErr := reader.Close()
				if err != nil || closeErr != nil || *h.OutputSHA256 != derivedHistoryDigest(raw) {
					t.Fatal("output hash did not describe actual held artifact", err, closeErr)
				}
				tools := []string{"recording-tools/v1"}
				for _, name := range []string{"ffmpeg", "ffprobe"} {
					raw, err := os.ReadFile("/usr/bin/" + name)
					if err != nil {
						t.Fatal(err)
					}
					tools = append(tools, derivedHistoryDigest(raw))
				}
				raw, _ = json.Marshal(tools)
				if *h.ToolSHA256 != derivedHistoryDigest(raw) {
					t.Fatal("tool digest was not the actual executed ELF pair")
				}
			case "deep-owned-original-ttl":
				original := h.Inputs[0].SHA256
				tool := *h.ToolSHA256
				h.Inputs[0].SHA256 = strings.Repeat("0", 64)
				*h.ToolSHA256 = strings.Repeat("1", 64)
				*v.ExpiresAt = v.ExpiresAt.AddDate(1, 0, 0)
				next, nextHistory, err := f.service.GetDerivedWithHistoryData(t.Context(), f.job.ID, exportAllowed)
				if err != nil || nextHistory.Inputs[0].SHA256 != original || *nextHistory.ToolSHA256 != tool || next.ExpiresAt.Sub(*next.ObservedAt) != derivedTTL || !next.ObservedAt.Equal(*v.ObservedAt) {
					t.Fatal("returned DATA mutated original observation or TTL", err)
				}
			case "source-change-denied":
				if err := os.WriteFile(filepath.Join(f.dir, mustExportPartName(1)), []byte("changed synthetic source"), 0600); err != nil {
					t.Fatal(err)
				}
				next, nextHistory, err := f.service.GetDerivedWithHistoryData(t.Context(), f.job.ID, exportAllowed)
				if err != nil || next.State != "unknown" || len(nextHistory.Inputs) != 0 || nextHistory.OutputSHA256 != nil {
					t.Fatal("stale source retained paired evidence", err)
				}
			}
		})
	}
}
func fmtHistoryHash(v [32]byte) string {
	const alphabet = "0123456789abcdef"
	out := make([]byte, 64)
	for i, b := range v {
		out[2*i] = alphabet[b>>4]
		out[2*i+1] = alphabet[b&15]
	}
	return string(out)
}
func TestDerivedHistoryExecutedTools(t *testing.T) {
	for _, kind := range []string{"actual-elf-fd", "path-replacement-held-fd", "script-unverifiable", "recipe-sensitive"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "actual-elf-fd" || kind == "path-replacement-held-fd" {
				dir := t.TempDir()
				name := filepath.Join(dir, "tool")
				raw, err := os.ReadFile("/usr/bin/true")
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(name, raw, 0700); err != nil {
					t.Fatal(err)
				}
				input, err := os.Open(dir)
				if err != nil {
					t.Fatal(err)
				}
				defer input.Close()
				capture := &derivedHistoryCapture{}
				cmd := exec.CommandContext(t.Context(), name)
				cmd.ExtraFiles = []*os.File{input}
				replaced := false
				check := func() error {
					if kind == "path-replacement-held-fd" && !replaced {
						replaced = true
						replacement := filepath.Join(dir, "replacement")
						wrong, e := os.ReadFile("/usr/bin/false")
						if e != nil {
							return e
						}
						if e = os.WriteFile(replacement, wrong, 0700); e != nil {
							return e
						}
						if e = os.Rename(replacement, name); e != nil {
							return e
						}
					}
					return nil
				}
				err = runDerivedHistoryProcess(t.Context(), cmd, check, 1, capture)
				if kind == "path-replacement-held-fd" {
					// Replacing a name changes the old inode's ctime. Conservative rejection
					// is valid; attribution to the replacement executable is never valid.
					if err == nil || capture.tools[1] != "" {
						t.Fatal("replaced tool path retained unverified execution evidence", err)
					}
				} else if err != nil || capture.tools[1] != derivedHistoryDigest(raw) || capture.unsupported {
					t.Fatal("actual execution was not bound to the measured held FD", err)
				}
				return
			}
			f := realDerivedFixture(t)
			if kind == "script-unverifiable" {
				dir := t.TempDir()
				for _, name := range []string{"ffmpeg", "ffprobe"} {
					if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\nexec /usr/bin/"+name+" \"$@\"\n"), 0700); err != nil {
						t.Fatal(err)
					}
				}
				tools := DerivedTools{FFmpegPath: filepath.Join(dir, "ffmpeg"), FFprobePath: filepath.Join(dir, "ffprobe"), ConfigurationKey: "a label is not evidence"}
				configureDerivedFixture(t, f, tools, func(context.Context) (DerivedTools, error) { return tools, nil })
				_, h := historyPrepared(t, f)
				if h.ToolSHA256 != nil || h.TransformSHA256 != nil || h.OutputSHA256 == nil {
					t.Fatal("script playback was rejected or given fabricated tool/recipe evidence")
				}
				return
			}
			_, first := historyPrepared(t, f)
			for i := 1; i <= 2; i++ {
				name := filepath.Join(f.dir, mustExportPartName(i))
				changed := filepath.Join(t.TempDir(), "changed.ts")
				cmd := exec.CommandContext(t.Context(), "/usr/bin/ffmpeg", "-nostdin", "-hide_banner", "-v", "error", "-i", name, "-map", "0", "-c", "copy", "-metadata:s:a:0", "language=fra", "-f", "mpegts", changed)
				if raw, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("change synthetic stream language: %v %s", err, raw)
				}
				raw, err := os.ReadFile(changed)
				if err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(name, raw, 0600); err != nil {
					t.Fatal(err)
				}
				f.job.Capture.Parts[i-1].Bytes = int64(len(raw))
			}
			if err := f.repo.Update(t.Context(), &f.job); err != nil {
				t.Fatal(err)
			}
			_, second := historyPrepared(t, f)
			if first.TransformSHA256 == nil || second.TransformSHA256 == nil || *first.TransformSHA256 == *second.TransformSHA256 || *first.ToolSHA256 != *second.ToolSHA256 {
				t.Fatal("actual language/remux recipe did not change independently of executed tools")
			}
		})
	}
}
