package database

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func sabPublicationDatabaseFixture(t *testing.T) (*DB, *SABLibraryPublicationRepository, SABPublicationStage, string) {
	t.Helper()
	name := filepath.Join(t.TempDir(), "publication.db")
	db, e := NewDB(Config{DatabasePath: name})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	claim := stageClaimFixture(t, db, 1, 6)
	repo := NewSABStageRepository(db.Connection())
	if _, e = repo.Claim(t.Context(), claim, 64); e != nil {
		t.Fatal(e)
	}
	digest := sha256.Sum256([]byte("abcdef"))
	if e = repo.Finish(t.Context(), claim.OperationID, "copied", []SABStageFile{{1, 6, hex.EncodeToString(digest[:])}}); e != nil {
		t.Fatal(e)
	}
	publication := NewSABLibraryPublicationRepository(db.Connection())
	stage, e := publication.LoadCopied(t.Context(), claim.QueueID, claim.ReceiptAttempt, claim.SourceHash, claim.Namespace)
	if e != nil {
		t.Fatal(e)
	}
	return db, publication, stage, name
}
func TestSABPublicationSQLiteJournal(t *testing.T) {
	for _, name := range []string{"claim-before-copy-durable", "same-stage-different-target-conflict", "exact-canonical-manifest-required", "claim-commit-unknown-no-second-attempt", "terminal-transition-cas", "staged-charge-never-refunded", "restart-retains-unknown-operation"} {
		t.Run(name, func(t *testing.T) {
			db, repo, stage, file := sabPublicationDatabaseFixture(t)
			binding := strings.Repeat("d", 64)
			if name == "exact-canonical-manifest-required" {
				raw, _ := json.MarshalIndent(stage.Files, "", " ")
				if _, e := db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=? WHERE operation_id=?`, string(raw), stage.OperationID); e != nil {
					t.Fatal(e)
				}
				if _, e := repo.LoadCopied(t.Context(), stage.QueueID, stage.ReceiptAttempt, stage.SourceHash, stage.Namespace); e == nil {
					t.Fatal("noncanonical manifest adopted")
				}
				canonical, _ := json.Marshal(stage.Files)
				oversized := strings.Repeat("é", 40000) // 40k code points, 80k actual SQL bytes.
				var loadedSeen, loadedPresent bool
				loadedBytes := -1
				repo.afterManifestScan = func(phase string, size int, present bool) {
					if phase == "loaded" {
						loadedSeen = true
						loadedBytes = size
						loadedPresent = present
					}
				}
				if _, e := db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=? WHERE operation_id=?`, oversized, stage.OperationID); e != nil {
					t.Fatal(e)
				}
				if _, e := repo.LoadCopied(t.Context(), stage.QueueID, stage.ReceiptAttempt, stage.SourceHash, stage.Namespace); e == nil || !loadedSeen || loadedPresent || loadedBytes != 0 {
					t.Fatal("actual SQL returned overbound multibyte payload before decode", e, loadedSeen, loadedPresent, loadedBytes)
				}
				if _, e := db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=? WHERE operation_id=?`, string(canonical), stage.OperationID); e != nil {
					t.Fatal(e)
				}
				transactionSeen, transactionPresent, transactionBytes := false, false, -1
				repo.afterManifestScan = func(phase string, size int, present bool) {
					if phase == "transaction" {
						transactionSeen = true
						transactionBytes = size
						transactionPresent = present
					}
				}
				// This writes the real SQLite fixture only AFTER the genuine initial read;
				// the actual transaction must independently bound its later SQL projection.
				repo.afterStageRead = func() {
					if _, e := db.Connection().Exec(`UPDATE sab_import_stages SET file_manifest=? WHERE operation_id=?`, oversized, stage.OperationID); e != nil {
						t.Fatal(e)
					}
				}
				if _, e := repo.Claim(t.Context(), stage, []int{1}, "library", binding, 64); e == nil || !transactionSeen || transactionPresent || transactionBytes != 0 {
					t.Fatal("actual in-TX reread returned unbounded replacement", e, transactionSeen, transactionPresent, transactionBytes)
				}
				var count int
				if e := db.Connection().QueryRow(`SELECT count(*)FROM sab_library_publications`).Scan(&count); e != nil || count != 0 || stageCharge(t, db) != 6 {
					t.Fatal("failed byte-bounded claim changed original charges", e, count)
				}
				return
			}
			unknown := name == "claim-commit-unknown-no-second-attempt" || name == "restart-retains-unknown-operation"
			if unknown {
				repo.afterCommit = func() error { return errors.New("synthetic acknowledgement lost after actual SQLite COMMIT") }
			}
			value, e := repo.Claim(t.Context(), stage, []int{1}, "library", binding, 64)
			if unknown {
				if !errors.Is(e, ErrSABPublicationOutcomeUnknown) {
					t.Fatal("actual committed claim lost ambiguity", e)
				}
			} else if e != nil {
				t.Fatal(e)
			}
			repo.afterCommit = nil
			switch name {
			case "claim-before-copy-durable":
				found, e := repo.Find(t.Context(), stage.OperationID)
				if e != nil || found == nil || found.State != "claimed" || found.Bytes != 6 || found.OperationID != value.OperationID {
					t.Fatal("claim not durable before IO", e)
				}
				conn, e := db.Connection().Conn(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer conn.Close()
				var synchronous int
				if e = conn.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&synchronous); e != nil || synchronous != 2 {
					t.Fatal("journal write did not use FULL synchronous", e, synchronous)
				}
			case "same-stage-different-target-conflict":
				if _, e = repo.Claim(t.Context(), stage, []int{1}, "foreign-library", binding, 64); !errors.Is(e, ErrSABPublicationConflict) {
					t.Fatal("different target reset stage operation", e)
				}
			case "claim-commit-unknown-no-second-attempt":
				again, e := repo.Claim(t.Context(), stage, []int{1}, "library", binding, 64)
				if e != nil || again.OperationID != value.OperationID {
					t.Fatal("unknown actual claim retried another nonce", e)
				}
				var count int
				if e = db.Connection().QueryRow("SELECT count(*) FROM sab_library_publications").Scan(&count); e != nil || count != 1 {
					t.Fatal("duplicate durable operation", e)
				}
			case "terminal-transition-cas":
				if e = repo.Advance(t.Context(), value.OperationID, 1, "claimed", "reconciliation_required"); e != nil {
					t.Fatal(e)
				}
				if e = repo.Advance(t.Context(), value.OperationID, 1, "claimed", "reconciliation_required"); !errors.Is(e, ErrSABPublicationConflict) {
					t.Fatal("stale transition replay accepted", e)
				}
				if e = repo.Advance(t.Context(), value.OperationID, 2, "reconciliation_required", "claimed"); e == nil {
					t.Fatal("terminal state was renewed")
				}
				// No physical copier/receipt exists in this foundation. Even a caller with
				// the operational row ID cannot claim durable files from state DATA alone.
				if _, e = db.Connection().Exec(`UPDATE sab_library_publications SET state='files_durable' WHERE operation_id=?`, value.OperationID); e == nil {
					t.Fatal("receipt-free file durability was invented")
				}

			case "staged-charge-never-refunded":
				if _, e = db.Connection().Exec("DELETE FROM import_queue"); e != nil {
					t.Fatal(e)
				}
				if stageCharge(t, db) != 6 {
					t.Fatal("publication erased original stage charge")
				}
			case "restart-retains-unknown-operation":
				db.Close()
				reopened, e := NewDB(Config{DatabasePath: file})
				if e != nil {
					t.Fatal(e)
				}
				defer reopened.Close()
				found, e := NewSABLibraryPublicationRepository(reopened.Connection()).Find(context.Background(), stage.OperationID)
				if e != nil || found == nil || found.OperationID != value.OperationID || found.State != "claimed" {
					t.Fatal("restart lost unknown committed operation", e)
				}
			}
		})
	}
}
