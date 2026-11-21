//go:build linux

package importer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"novastream/internal/database"
)

// These tests concern genuine physical copying and operational receipts. They
// do not claim that a source actor alone establishes PG master authorization.
func publicationCopySource(t *testing.T, payloads [][]byte, selection []int) (*sabPublicationSourceFixture, *SABStagedPublication, database.SABPublicationRecord, []SABPublicationFormat) {
	t.Helper()
	f := newSABPublicationSourceFixture(t)
	var files []database.SABStageFile
	var total int64
	for i, payload := range payloads {
		name := filepath.Join(filepath.Dir(f.file), "data-"+fmtCopyOrdinal(i+1)+".bin")
		if e := os.WriteFile(name, payload, 0600); e != nil {
			t.Fatal(e)
		}
		hash := sha256.Sum256(payload)
		files = append(files, database.SABStageFile{Ordinal: i + 1, Bytes: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])})
		total += int64(len(payload))
	}
	wire, e := json.Marshal(files)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.db.Connection().Exec(`UPDATE sab_import_stages SET expected_bytes=?,file_count=?,file_manifest=?`, total, len(files), string(wire)); e != nil {
		t.Fatal(e)
	}
	actor, e := f.service.OpenSABStagedPublication(f.ctx, 1, f.policy, f.target, selection)
	if e != nil {
		t.Fatal("open actual copied stage", e)
	}
	t.Cleanup(actor.Close)
	record, e := actor.Claim(f.ctx, "publication-library")
	if e != nil {
		t.Fatal("actual durable reservation", e)
	}
	var formats []SABPublicationFormat
	for _, ordinal := range selection {
		formats = append(formats, SABPublicationFormat{ordinal, "matroska"})
	}
	return f, actor, record, formats
}
func fmtCopyOrdinal(n int) string {
	return string([]byte{'0' + byte(n/1000%10), '0' + byte(n/100%10), '0' + byte(n/10%10), '0' + byte(n%10)})
}
func publicationCopyReady(t *testing.T, actor *SABStagedPublication, record database.SABPublicationRecord, formats []SABPublicationFormat) *SABPublicationCopy {
	t.Helper()
	copy, e := actor.Copy(actor.original, record, formats)
	if e != nil {
		t.Fatal("actual copy and fsync", e)
	}
	t.Cleanup(copy.Close)
	return copy
}
func publicationCopyConsume(t *testing.T, copy *SABPublicationCopy) *SABPublicationCopyConsumption {
	t.Helper()
	value, e := copy.Consume(copy.original, copy.source, copy.record.OperationID)
	if e != nil {
		t.Fatal("consume genuine physical completion", e)
	}
	t.Cleanup(value.Close)
	return value
}
func publicationCopyCharge(t *testing.T, f *sabPublicationSourceFixture, table string) int64 {
	t.Helper()
	if table != "sab_import_stages" && table != "sab_library_publications" {
		t.Fatal("unexpected synthetic charge table")
	}
	var bytes int64
	if e := f.db.Connection().QueryRow("SELECT COALESCE(SUM(expected_bytes),0) FROM " + table).Scan(&bytes); e != nil {
		t.Fatal(e)
	}
	return bytes
}
func publicationCopyMedia(t *testing.T, container string) []byte {
	t.Helper()
	tool := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG")
	digest := os.Getenv("FAMILY_LOCAL_PROBE_TEST_FFMPEG_SHA256")
	if tool == "" || !filepath.IsAbs(tool) || len(digest) != 64 {
		t.Fatal("explicit pinned synthetic media generator required")
	}
	raw, e := os.ReadFile(tool)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != digest {
		t.Fatal("fixture generator changed")
	}
	path := filepath.Join(t.TempDir(), "media.bin")
	format, video, audio := "matroska", "ffv1", "pcm_s16le"
	switch container {
	case "mov":
		format, video, audio = "mp4", "mpeg4", "aac"
	case "mpegts":
		format, video, audio = "mpegts", "mpeg2video", "mp2"
	}
	args := []string{"-v", "error", "-nostdin", "-y", "-f", "lavfi", "-i", "color=c=black:s=32x32:r=25:d=0.16", "-f", "lavfi", "-i", "sine=frequency=660:duration=0.16", "-map", "0:v:0", "-map", "1:a:0", "-c:v", video, "-threads", "1", "-c:a", audio, "-f", format, path}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, tool, args...)
	command.Env = []string{"LANG=C", "LC_ALL=C", "TZ=UTC"}
	if output, e := command.CombinedOutput(); e != nil {
		t.Fatal("generate real synthetic media", e, string(output))
	}
	payload, e := os.ReadFile(path)
	if e != nil || len(payload) == 0 {
		t.Fatal("actual generated media unavailable", e)
	}
	return payload
}
func TestSABPublicationPhysicalCopy(t *testing.T) {
	for _, name := range []string{"real-matroska-mp4-ts-unmatched", "selected-ordinals-source-and-charge-retained", "foreign-namespace-and-leaf-collision-denied", "root-marker-and-operation-physical-aba", "actual-space-preallocation-and-short-write", "actual-file-and-directory-sync-failures", "same-and-distinct-local-filesystem-copy", "source-inplace-and-root-replacement", "held-read-write-and-sync-cancel-join", "closed-copy-zero-and-foreign-source-denied", "concurrent-same-stage-one-physical-copy", "prepared-and-durable-ack-unknown-no-completion", "source-close-joins-copy-and-consumer"} {
		t.Run(name, func(t *testing.T) {
			if name == "real-matroska-mp4-ts-unmatched" {
				for _, container := range []string{"matroska", "mov", "mpegts"} {
					payload := publicationCopyMedia(t, container)
					f, actor, record, formats := publicationCopySource(t, [][]byte{payload}, []int{1})
					formats[0].Container = container
					copy := publicationCopyReady(t, actor, record, formats)
					consumed := publicationCopyConsume(t, copy)
					data := consumed.Data()
					if len(data.Files) != 1 || data.Files[0].Container != container || !strings.HasSuffix(data.Files[0].RelativePath, ".bin") {
						t.Fatal("technical DATA or opaque physical name changed")
					}
					actual, e := os.ReadFile(filepath.Join(f.target, data.Files[0].RelativePath))
					if e != nil || !bytes.Equal(actual, payload) {
						t.Fatal("actual container bytes changed", e)
					}
					if bytes.Contains(data.Receipt, []byte("matchedTitleId")) || bytes.Contains(data.Receipt, []byte("episodeNumber")) {
						t.Fatal("physical receipt invented catalogue identity")
					}
					consumed.Close()
					copy.Close()
					actor.Close()
				}
				return
			}
			if name == "selected-ordinals-source-and-charge-retained" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef"), []byte("ghijkl")}, []int{1})
				copy := publicationCopyReady(t, actor, record, formats)
				value := publicationCopyConsume(t, copy)
				if len(value.Data().Files) != 1 || publicationCopyCharge(t, f, "sab_import_stages") != 12 || publicationCopyCharge(t, f, "sab_library_publications") != 6 {
					t.Fatal("selection changed original complete-stage charge")
				}
				if _, e := os.Stat(filepath.Join(f.target, sabPublicationNamespace, record.OperationID, "data-0002.bin")); !errors.Is(e, os.ErrNotExist) {
					t.Fatal("unselected ordinal was copied")
				}
				for i, want := range []string{"abcdef", "ghijkl"} {
					actual, e := os.ReadFile(filepath.Join(filepath.Dir(f.file), "data-"+fmtCopyOrdinal(i+1)+".bin"))
					if e != nil || string(actual) != want {
						t.Fatal("original stage changed", e)
					}
				}
				return
			}
			if name == "foreign-namespace-and-leaf-collision-denied" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				namespace := filepath.Join(f.target, sabPublicationNamespace)
				if e := os.Mkdir(namespace, 0700); e != nil {
					t.Fatal(e)
				}
				sentinel := filepath.Join(namespace, "foreign")
				if e := os.WriteFile(sentinel, []byte("foreign sentinel"), 0600); e != nil {
					t.Fatal(e)
				}
				if value, e := actor.Copy(f.ctx, record, formats); e == nil || value != nil {
					t.Fatal("foreign namespace adopted")
				}
				actual, e := os.ReadFile(sentinel)
				if e != nil || string(actual) != "foreign sentinel" {
					t.Fatal("foreign file changed", e)
				}
				if publicationCopyCharge(t, f, "sab_library_publications") != 6 {
					t.Fatal("collision refunded durable charge")
				}
				return
			}
			if name == "root-marker-and-operation-physical-aba" {
				for _, kind := range []string{"marker", "operation", "namespace"} {
					f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
					copy := publicationCopyReady(t, actor, record, formats)
					path := filepath.Join(f.target, sabPublicationNamespace)
					if kind == "marker" {
						path = filepath.Join(path, ".owner")
					} else if kind == "operation" {
						path = filepath.Join(path, record.OperationID)
					}
					if e := os.Rename(path, path+"-old"); e != nil {
						t.Fatal(e)
					}
					if kind == "marker" {
						raw, e := os.ReadFile(path + "-old")
						if e != nil {
							t.Fatal(e)
						}
						if e = os.WriteFile(path, raw, 0600); e != nil {
							t.Fatal(e)
						}
					} else if e := os.Mkdir(path, 0700); e != nil {
						t.Fatal(e)
					}
					if value, e := copy.Consume(f.ctx, actor, record.OperationID); e == nil || value != nil {
						t.Fatal("physical ABA adopted by name/hash")
					}
					copy.Close()
					actor.Close()
				}
				return
			}
			if name == "actual-space-preallocation-and-short-write" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				var writes atomic.Int32
				actor.copyHooks = &sabPublicationCopyHooks{writeLimit: 2, afterIO: func(phase string) error {
					if phase == "write" {
						writes.Add(1)
					}
					return nil
				}}
				copy := publicationCopyReady(t, actor, record, formats)
				value := publicationCopyConsume(t, copy)
				actual, e := os.ReadFile(filepath.Join(f.target, value.Data().Files[0].RelativePath))
				if e != nil || string(actual) != "abcdef" || writes.Load() != 3 {
					t.Fatal("actual partial writes lost bytes", e, writes.Load())
				}
				info, e := copy.files[0].Stat()
				if e != nil || info.Size() != 6 {
					t.Fatal("actual allocated/written size missing", e)
				}
				return
			}
			if name == "actual-file-and-directory-sync-failures" {
				for _, phase := range []string{"write", "file-sync", "directory-sync"} {
					f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
					var observed atomic.Bool
					actor.copyHooks = &sabPublicationCopyHooks{writeLimit: 2, afterIO: func(actual string) error {
						if actual == phase {
							observed.Store(true)
							if phase == "write" {
								return syscall.ENOSPC
							}
							return syscall.EIO
						}
						return nil
					}}
					if value, e := actor.Copy(f.ctx, record, formats); e == nil || value != nil || !observed.Load() {
						t.Fatal("injected error after actual syscall produced completion")
					}
					var state string
					if e := f.db.Connection().QueryRow(`SELECT state FROM sab_library_publications WHERE operation_id=?`, record.OperationID).Scan(&state); e != nil || state != "files_prepared" {
						t.Fatal("failed actual IO marked durable", e, state)
					}
					if publicationCopyCharge(t, f, "sab_import_stages") != 6 || publicationCopyCharge(t, f, "sab_library_publications") != 6 {
						t.Fatal("failed IO refunded charge")
					}
					actor.Close()
				}
				return
			}
			if name == "same-and-distinct-local-filesystem-copy" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				copy := publicationCopyReady(t, actor, record, formats)
				value := publicationCopyConsume(t, copy)
				actual, e := os.ReadFile(filepath.Join(f.target, value.Data().Files[0].RelativePath))
				if e != nil || string(actual) != "abcdef" {
					t.Fatal("actual descriptor copy failed", e)
				}
				// A second actual persistent filesystem is an explicit environment
				// capability, never manufactured by writing outside the task root.
				t.Log("same-filesystem actual copy executed; distinct persistent filesystem fixture is not configured, no cross-filesystem pass is claimed")
				return
			}
			if name == "source-inplace-and-root-replacement" {
				for _, kind := range []string{"source", "root"} {
					f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
					if kind == "source" {
						if e := os.WriteFile(f.file, []byte("ghijkl"), 0600); e != nil {
							t.Fatal(e)
						}
					} else {
						if e := os.Rename(f.target, f.target+"-old"); e != nil {
							t.Fatal(e)
						}
						if e := os.Mkdir(f.target, 0700); e != nil {
							t.Fatal(e)
						}
					}
					if value, e := actor.Copy(f.ctx, record, formats); e == nil || value != nil {
						t.Fatal("changed original source/root copied")
					}
					actor.Close()
				}
				return
			}
			if name == "held-read-write-and-sync-cancel-join" {
				for _, phase := range []string{"read", "write", "file-sync", "directory-sync"} {
					f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
					entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					var gated atomic.Bool
					actor.copyHooks = &sabPublicationCopyHooks{afterIO: func(actual string) error {
						if actual == phase && gated.CompareAndSwap(false, true) {
							close(entered)
							<-release
						}
						return nil
					}}
					type outcome struct {
						copy *SABPublicationCopy
						err  error
					}
					done := make(chan outcome, 1)
					started, joined, closeStarted, closeJoined := false, false, false, false
					func() {
						defer func() {
							f.cancel()
							unblock()
							if started && !joined {
								result := <-done
								if result.copy != nil {
									result.copy.Close()
								}
							}
							actor.Close()
							if closeStarted && !closeJoined {
								<-closed
							}
						}()
						started = true
						go func() { copy, e := actor.Copy(f.ctx, record, formats); done <- outcome{copy, e} }()
						select {
						case <-entered:
						case <-time.After(5 * time.Second):
							t.Fatal("actual IO return was not held")
						}
						f.cancel()
						closeStarted = true
						go func() { actor.Close(); close(closed) }()
						select {
						case <-closed:
							t.Fatal("source refunded before held actual IO ended")
						case <-time.After(30 * time.Millisecond):
						}
						unblock()
						result := <-done
						joined = true
						if result.err == nil || result.copy != nil {
							t.Fatal("late successful completion after cancel")
						}
						<-closed
						closeJoined = true
					}()
				}
				return
			}
			if name == "closed-copy-zero-and-foreign-source-denied" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				copy := publicationCopyReady(t, actor, record, formats)
				otherF, other, _, _ := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				if value, e := copy.Consume(otherF.ctx, other, record.OperationID); e == nil || value != nil {
					t.Fatal("foreign source/context accepted")
				}
				zero := &SABPublicationCopy{}
				if value, e := zero.Consume(f.ctx, actor, record.OperationID); e == nil || value != nil {
					t.Fatal("zero completion accepted")
				}
				copied := new(SABPublicationCopy)
				reflect.ValueOf(copied).Elem().Set(reflect.ValueOf(copy).Elem())
				if value, e := copied.Consume(f.ctx, actor, record.OperationID); e == nil || value != nil {
					t.Fatal("copied completion accepted")
				}
				raw, e := json.Marshal(copy)
				if e != nil || string(raw) != "null" {
					t.Fatal("completion exported private receipt")
				}
				value := publicationCopyConsume(t, copy)
				value.Close()
				if again, e := copy.Consume(f.ctx, actor, record.OperationID); e == nil || again != nil {
					t.Fatal("completion consumed twice")
				}
				copy.Close()
				if copy.CheckOriginal(f.ctx, actor) == nil {
					t.Fatal("closed completion current")
				}
				return
			}
			if name == "concurrent-same-stage-one-physical-copy" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				other, e := f.service.OpenSABStagedPublication(f.ctx, 1, f.policy, f.target, []int{1})
				if e != nil {
					t.Fatal(e)
				}
				defer other.Close()
				type outcome struct {
					copy *SABPublicationCopy
					err  error
				}
				done := make(chan outcome, 2)
				joined := 0
				defer func() {
					f.cancel()
					for joined < 2 {
						value := <-done
						joined++
						if value.copy != nil {
							value.copy.Close()
						}
					}
				}()
				for _, source := range []*SABStagedPublication{actor, other} {
					go func(source *SABStagedPublication) {
						value, e := source.Copy(f.ctx, record, formats)
						done <- outcome{value, e}
					}(source)
				}
				success := 0
				for joined < 2 {
					value := <-done
					joined++
					if value.err == nil && value.copy != nil {
						success++
						value.copy.Close()
					}
				}
				if success != 1 {
					t.Fatal("concurrent stage did not produce exactly one copy", success)
				}
				var operations int
				if e := f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&operations); e != nil || operations != 1 {
					t.Fatal("duplicate reservation", e)
				}
				return
			}
			if name == "prepared-and-durable-ack-unknown-no-completion" {
				for _, phase := range []string{"files_prepared", "files_durable"} {
					f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
					var actual atomic.Bool
					actor.copyHooks = &sabPublicationCopyHooks{afterCommit: func(state string) error {
						if state == phase {
							actual.Store(true)
							return errors.New("injected lost result AFTER actual SQLite commit")
						}
						return nil
					}}
					if value, e := actor.Copy(f.ctx, record, formats); !errors.Is(e, database.ErrSABPublicationOutcomeUnknown) || value != nil || !actual.Load() {
						t.Fatal("unknown actual commit emitted completion", e)
					}
					var state string
					var receipt *string
					if e := f.db.Connection().QueryRow(`SELECT state,CASE WHEN state='files_durable' THEN durable_receipt ELSE prepared_receipt END FROM sab_library_publications WHERE operation_id=?`, record.OperationID).Scan(&state, &receipt); e != nil || state != phase || receipt == nil {
						t.Fatal("actual committed receipt lost", e)
					}
					if publicationCopyCharge(t, f, "sab_library_publications") != 6 {
						t.Fatal("unknown result refunded charge")
					}
					actor.Close()
				}
				return
			}
			if name == "source-close-joins-copy-and-consumer" {
				f, actor, record, formats := publicationCopySource(t, [][]byte{[]byte("abcdef")}, []int{1})
				copy := publicationCopyReady(t, actor, record, formats)
				value := publicationCopyConsume(t, copy)
				closed := make(chan struct{})
				started, joined := false, false
				defer func() {
					value.Close()
					actor.Close()
					if started && !joined {
						<-closed
					}
				}()
				started = true
				go func() { actor.Close(); close(closed) }()
				select {
				case <-closed:
					t.Fatal("source closed before actual consumer ended")
				case <-time.After(30 * time.Millisecond):
				}
				if _, e := copy.files[0].Stat(); e != nil {
					t.Fatal("held consumer descriptor closed early", e)
				}
				value.Close()
				<-closed
				joined = true
				if _, e := copy.files[0].Stat(); e == nil {
					t.Fatal("actual target descriptor leaked after join")
				}
				return
			}
			t.Fatal("unhandled physical-copy canary")
		})
	}
}
