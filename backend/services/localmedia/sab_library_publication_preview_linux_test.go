//go:build linux

package localmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/internal/database"
)

func publicationPreviewNoMutation(t *testing.T, f *publicationFixture) {
	t.Helper()
	for _, table := range []string{"sab_library_publications", "sab_library_publication_files", "local_media_items"} {
		if f.count(t, table) != 0 {
			t.Fatal("preview created catalogue or registration DATA", table)
		}
	}
	var count int
	if e := f.db.Connection().QueryRow(`SELECT count(*) FROM sab_library_publications`).Scan(&count); e != nil || count != 0 {
		t.Fatal("preview reserved/copied publication", e, count)
	}
	if _, e := os.Lstat(filepath.Join(f.root, ".mediastorm-publications")); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("preview created destination namespace", e)
	}
}
func TestSABPublicationSavedStagePreview(t *testing.T) {
	for _, name := range []string{"original-bytes-no-copy-or-registration", "unsupported-format-descriptive-only", "current-master-retirement", "original-request-not-clone", "closed-result-no-current-data", "runtime-close-joins-original-query", "copied-preview-never-authority"} {
		t.Run(name, func(t *testing.T) {
			f := publicationFixturePG(t, "")
			if name == "unsupported-format-descriptive-only" {
				payload := []byte("ordinary non-media saved file")
				if e := os.WriteFile(f.stageFile, payload, 0600); e != nil {
					t.Fatal(e)
				}
				sum := sha256.Sum256(payload)
				manifest, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}})
				if _, e := f.db.Connection().Exec(`UPDATE sab_import_stages SET expected_bytes=?,file_manifest=?`, len(payload), string(manifest)); e != nil {
					t.Fatal(e)
				}
			}
			r := f.request(t, "POST", "/preview", `{"libraryId":"library","ordinals":[1]}`)
			if name == "runtime-close-joins-original-query" {
				tx, e := f.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				rolled := false
				defer func() {
					if !rolled {
						tx.Rollback(context.Background())
					}
				}()
				if _, e = tx.Exec(t.Context(), "SELECT id FROM "+pgx.Identifier{f.schema, "accounts"}.Sanitize()+" WHERE id='actor' FOR UPDATE"); e != nil {
					t.Fatal(e)
				}
				type result struct {
					preview *SABLibraryPublicationPreview
					err     error
				}
				done := make(chan result, 1)
				closed := make(chan struct{})
				started, joined, closeStarted, closeJoined := false, false, false, false
				defer func() {
					if !rolled {
						tx.Rollback(context.Background())
						rolled = true
					}
					f.publisher.Close()
					if started && !joined {
						actual := <-done
						if actual.preview != nil {
							actual.preview.Close()
						}
					}
					if closeStarted && !closeJoined {
						<-closed
					}
				}()
				started = true
				go func() {
					preview, e := f.publisher.Preview(r, 1, []byte(`{"libraryId":"library","ordinals":[1]}`))
					done <- result{preview, e}
				}()
				witness, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					var blocked bool
					e = f.admin.QueryRow(witness, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%accounts%FOR UPDATE%')`, f.schema).Scan(&blocked)
					if e != nil {
						t.Fatal("actual held query witness", e)
					}
					if blocked {
						break
					}
					select {
					case <-ticker.C:
					case <-witness.Done():
						t.Fatal("original actual PostgreSQL query never waited on held actor row")
					}
				}
				closeStarted = true
				go func() { f.publisher.Close(); close(closed) }()
				var actual result
				select {
				case actual = <-done:
					joined = true
				case <-time.After(8 * time.Second):
					t.Fatal("actual query did not retire while actor row remained held")
				}
				if actual.preview != nil {
					actual.preview.Close()
				}
				if actual.err == nil || actual.preview != nil {
					t.Fatal("closed runtime emitted late preview")
				}
				select {
				case <-closed:
					closeJoined = true
				case <-time.After(8 * time.Second):
					t.Fatal("publisher returned without actual query cleanup join")
				}
				if e = tx.Rollback(t.Context()); e != nil {
					t.Fatal(e)
				}
				rolled = true
				publicationPreviewNoMutation(t, f)
				return
			}
			preview, e := f.publisher.Preview(r, 1, []byte(`{"libraryId":"library","ordinals":[1]}`))
			if e != nil || preview == nil {
				t.Fatal("genuine original byte preview", e)
			}
			defer preview.Close()
			view := preview.View()
			if view.QueueID != 1 || view.LibraryID != "library" || len(view.Files) != 1 || view.Files[0].Ordinal != 1 || view.Files[0].Bytes <= 0 || preview.Check(r) != nil {
				t.Fatal("preview lost actual original saved stage")
			}
			if name == "unsupported-format-descriptive-only" {
				if view.Files[0].Eligible || view.Files[0].Container != "" {
					t.Fatal("unsupported bytes became eligible")
				}
			} else if !view.Files[0].Eligible || view.Files[0].Container != "matroska" || view.Files[0].Bytes != int64(len(f.payload)) {
				t.Fatal("genuine stored Matroska not described accurately")
			}
			if name == "current-master-retirement" {
				if _, e = f.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=false WHERE id='actor'`); e != nil {
					t.Fatal(e)
				}
				if preview.Check(r) == nil {
					t.Fatal("preview stayed current after actual role revocation")
				}
			}
			if name == "original-request-not-clone" {
				if preview.Check(r.Clone(r.Context())) == nil || preview.Check(r) != nil {
					t.Fatal("request clone became original or retired original")
				}
			}
			if name == "closed-result-no-current-data" {
				preview.Close()
				if preview.Check(r) == nil {
					t.Fatal("closed preview retained current authority")
				}
				f.publisher.mu.Lock()
				jobs := len(f.publisher.jobs)
				f.publisher.mu.Unlock()
				if jobs != 0 {
					t.Fatal("closed original preview retained admission")
				}
			}
			if name == "copied-preview-never-authority" {
				copied := *preview
				copied.Close()
				if copied.Check(r) == nil || preview.Check(r) != nil {
					t.Fatal("copied preview authorized or retired original")
				}
				var zero SABLibraryPublicationPreview
				if zero.Check(r) == nil || json.Unmarshal([]byte(`{}`), &zero) == nil {
					t.Fatal("zero/imported preview acquired authority")
				}
				wire, e := json.Marshal(preview)
				if e != nil || string(wire) != "null" {
					t.Fatal("preview exported original actor")
				}
			}
			// Descriptive view copies can be changed without mutating the private
			// preview or the true source. Cached DATA is never a current proof.
			view.Files[0].Container = "forged"
			if preview.View().Files[0].Container == "forged" {
				t.Fatal("preview exposed mutable view backing")
			}
			wire, e := json.Marshal(preview.View())
			if e != nil || bytes.Contains(wire, []byte(f.root)) || bytes.Contains(wire, []byte(f.stageRoot)) || bytes.Contains(wire, []byte("sha256")) {
				t.Fatal("preview leaked paths/digests")
			}
			original, e := os.ReadFile(f.stageFile)
			if e != nil {
				t.Fatal(e)
			}
			if name != "unsupported-format-descriptive-only" && !bytes.Equal(original, f.payload) {
				t.Fatal("preview modified original stored bytes")
			}
			publicationPreviewNoMutation(t, f)
		})
	}
}
