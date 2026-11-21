//go:build linux

package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"novastream/config"
	"novastream/internal/database"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type sabPublicationSourceFixture struct {
	service                 *Service
	db                      *database.DB
	policy                  config.SABPublicationPolicyValues
	target, operation, file string
	ctx                     context.Context
	cancel                  context.CancelFunc
}

func newSABPublicationSourceFixture(t *testing.T) *sabPublicationSourceFixture {
	t.Helper()
	f := &sabPublicationSourceFixture{target: t.TempDir(), operation: strings.Repeat("1", 32)}
	var e error
	f.db, e = database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "staging.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.db.Close() })
	f.service, e = NewService(ServiceConfig{}, nil, f.db, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.service.cancel)
	f.policy = config.SABPublicationPolicyValues{StageRoot: t.TempDir(), MaxBytes: 32 << 20, SourceFingerprint: strings.Repeat("a", 64)}
	root, info, e := stageOpenRoot(f.policy.StageRoot)
	if e != nil {
		t.Fatal(e)
	}
	root.Close()
	namespace, e := stageNamespace(info)
	if e != nil {
		t.Fatal(e)
	}
	directory := filepath.Join(f.policy.StageRoot, f.operation)
	if e = os.Mkdir(directory, 0700); e != nil {
		t.Fatal(e)
	}
	f.file = filepath.Join(directory, "data-0001.bin")
	if e = os.WriteFile(f.file, []byte("abcdef"), 0600); e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256([]byte("abcdef"))
	files, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: 6, SHA256: hex.EncodeToString(sum[:])}})
	receiptJSON, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: map[string]any{"version": 1, "attempt": strings.Repeat("2", 32), "source": f.policy.SourceFingerprint, "jobId": "synthetic-job", "status": map[string]any{"queueId": 1, "state": "download_completed"}}})
	metadata := string(receiptJSON)
	item := &database.ImportQueueItem{NzbPath: "synthetic-unused.nzb", Status: database.QueueStatusFailed, Metadata: &metadata}
	if e = f.db.Repository.AddToQueue(item); e != nil || item.ID != 1 {
		t.Fatal("original synthetic receipt", e, item.ID)
	}

	_, e = f.db.Connection().Exec(`INSERT INTO sab_import_stages(operation_id,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count,state,file_manifest,created_at,updated_at)VALUES(?,1,?,?,?,6,1,'copied',?,'synthetic','synthetic')`, f.operation, strings.Repeat("2", 32), f.policy.SourceFingerprint, namespace, string(files))
	if e != nil {
		t.Fatal(e)
	}
	f.ctx, f.cancel = context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(f.cancel)
	return f
}
func (f *sabPublicationSourceFixture) open() (*SABStagedPublication, error) {
	return f.service.OpenSABStagedPublication(f.ctx, 1, f.policy, f.target, []int{1})
}
func TestSABPublicationStagedSource(t *testing.T) {
	for _, name := range []string{"copied-manifest-only-no-provider", "manifest-oversize-count-and-byte-overflow", "nofollow-root-directory-leaf", "hardlink-fifo-network-filesystem-denied", "same-size-payload-replacement-digest-denied", "complete-directory-entry-set-required", "held-read-cancel-keeps-admission", "closed-copy-zero-source-denied"} {
		t.Run(name, func(t *testing.T) {
			f := newSABPublicationSourceFixture(t)
			switch name {
			case "manifest-oversize-count-and-byte-overflow":
				if _, e := f.db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=?`, strings.Repeat("x", 65537)); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("oversized manifest decoded")
				}
				if _, e := f.db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=?`, strings.Repeat("é", 40000)); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("overbound multibyte manifest accepted")
				}

				invalid := make([]database.SABStageFile, 129)
				for i := range invalid {
					invalid[i] = database.SABStageFile{Ordinal: i + 1, Bytes: 0, SHA256: strings.Repeat("a", 64)}
				}
				wire, _ := json.Marshal(invalid)
				if _, e := f.db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=?,file_count=128`, string(wire)); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("overbound complete manifest accepted")
				}
				if _, e := f.db.Connection().Exec(`UPDATE sab_import_stages SET expected_bytes=9223372036854775807`); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("unbounded stage bytes accepted")
				}
				return
			case "nofollow-root-directory-leaf":
				if e := os.Remove(f.file); e != nil {
					t.Fatal(e)
				}
				other := filepath.Join(t.TempDir(), "foreign.bin")
				if e := os.WriteFile(other, []byte("abcdef"), 0600); e != nil {
					t.Fatal(e)
				}
				if e := os.Symlink(other, f.file); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("symlink adopted")
				}
				return
			case "hardlink-fifo-network-filesystem-denied":
				other := filepath.Join(t.TempDir(), "hardlink")
				if e := os.Link(f.file, other); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("shared hardlink adopted")
				}
				if e := os.Remove(other); e != nil {
					t.Fatal(e)
				}
				if e := os.Remove(f.file); e != nil {
					t.Fatal(e)
				}
				if e := syscall.Mkfifo(f.file, 0600); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("FIFO adopted")
				}
				proc, e := os.Open("/proc")
				if e != nil {
					t.Fatal(e)
				}
				defer proc.Close()
				if sabPublicationLocal(proc) {
					t.Fatal("unsupported filesystem accepted")
				}
				return
			case "same-size-payload-replacement-digest-denied":
				if e := os.WriteFile(f.file, []byte("ghijkl"), 0600); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("old manifest adopted changed actual payload")
				}
				return
			case "complete-directory-entry-set-required":
				if e := os.WriteFile(filepath.Join(filepath.Dir(f.file), "unlisted.bin"), []byte("foreign"), 0600); e != nil {
					t.Fatal(e)
				}
				if actor, e := f.open(); e == nil {
					actor.Close()
					t.Fatal("unlisted entry omitted")
				}
				return
			}
			actor, e := f.open()
			if e != nil {
				t.Fatal("actual copied-stage actor", e)
			}
			defer actor.Close()
			switch name {
			case "copied-manifest-only-no-provider":
				raw := make([]byte, 6)
				n, e := actor.ReadAt(f.ctx, 1, raw, 0)
				if e != nil || n != 6 || string(raw) != "abcdef" || len(actor.TargetBinding()) != 64 {
					t.Fatal("actual held staged bytes differ", e)
				}
				if _, e = actor.ReadAt(f.ctx, 2, raw, 0); e == nil {
					t.Fatal("ordinal substituted")
				}
				if actor.Stage().ManifestDigest == "" {
					t.Fatal("durable complete manifest missing")
				}
				if e := f.db.Repository.UpdateMetadata(1, func() *string { raw := `{"_invalid":"changed-original-attempt"}`; return &raw }()); e != nil {
					t.Fatal(e)
				}
				if actor.Check(f.ctx) == nil {
					t.Fatal("replaced original durable receipt accepted old staged copy")
				}

			case "closed-copy-zero-source-denied":
				copy := *actor
				if copy.Check(f.ctx) == nil {
					t.Fatal("copied actor accepted")
				}
				if (&SABStagedPublication{}).Check(f.ctx) == nil {
					t.Fatal("zero actor accepted")
				}
				actor.Close()
				if actor.Check(f.ctx) == nil {
					t.Fatal("closed actor accepted")
				}
			case "held-read-cancel-keeps-admission":
				entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				done := make(chan error, 1)
				readJoined, closeStarted, closeJoined := false, false, false
				defer func() {
					f.cancel()
					unblock()
					if !readJoined {
						<-done
					}
					actor.Close()
					if closeStarted && !closeJoined {
						<-closed
					}
				}()
				actor.afterRead = func() { close(entered); <-release }
				go func() { buffer := make([]byte, 6); _, e := actor.ReadAt(f.ctx, 1, buffer, 0); done <- e }()
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("actual file read return not held")
				}
				closeStarted = true
				go func() { actor.Close(); close(closed) }()
				select {
				case <-closed:
					t.Fatal("held actual read refunded source before return")
				case <-time.After(40 * time.Millisecond):
				}
				unblock()
				select {
				case e := <-done:
					readJoined = true
					if e == nil {
						t.Fatal("late bytes leaked after close")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("actual read did not join")
				}
				select {
				case <-closed:
					closeJoined = true
				case <-time.After(5 * time.Second):
					t.Fatal("source Close did not join actual read")
				}
			}
		})
	}
}
