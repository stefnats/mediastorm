package database

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func stageDatabase(t *testing.T) (*DB, string) {
	t.Helper()
	name := filepath.Join(t.TempDir(), "queue.db")
	db, e := NewDB(Config{DatabasePath: name})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	var version int
	if e := db.Connection().QueryRow("SELECT max(version_id) FROM goose_db_version WHERE is_applied=1").Scan(&version); e != nil || version != 5 {
		t.Fatal("embedded migration005 not automatically loaded")
	}
	return db, name
}
func stageClaimFixture(t *testing.T, db *DB, n int, bytes int64) SABStageClaim {
	t.Helper()
	item := &ImportQueueItem{NzbPath: fmt.Sprintf("synthetic-%d.nzb", n), Status: QueueStatusFailed}
	if e := db.Repository.AddToQueue(item); e != nil {
		t.Fatal(e)
	}
	attempt := fmt.Sprintf("%032x", n)
	source := strings.Repeat("a", 64)
	data, _ := json.Marshal(map[string]any{SABFallbackMetadataKey: map[string]any{"version": 1, "attempt": attempt, "source": source, "jobId": "private_synthetic_job", "status": map[string]any{"queueId": item.ID, "state": "download_completed"}}, "foreign": "keep"})
	metadata := string(data)
	if e := db.Repository.UpdateMetadata(item.ID, &metadata); e != nil {
		t.Fatal(e)
	}
	return SABStageClaim{OperationID: fmt.Sprintf("%032x", n), QueueID: item.ID, ReceiptAttempt: attempt, SourceHash: source, Namespace: strings.Repeat("b", 64), PathHash: strings.Repeat("c", 64), ExpectedMetadata: metadata, ExpectedBytes: bytes, FileCount: 1}
}
func stageCharge(t *testing.T, db *DB) int64 {
	t.Helper()
	var size int64
	if e := db.Connection().QueryRow("SELECT COALESCE(SUM(expected_bytes),0) FROM sab_import_stages").Scan(&size); e != nil {
		t.Fatal(e)
	}
	return size
}
func TestSABStageRepositoryAutomaticMigrationAndConservedCharges(t *testing.T) {
	db, path := stageDatabase(t)
	repo := NewSABStageRepository(db.Connection())
	a := stageClaimFixture(t, db, 1, 6)
	b := stageClaimFixture(t, db, 2, 6)
	claim, e := repo.Claim(t.Context(), a, 10)
	if e != nil {
		t.Fatal(e)
	}
	if e := repo.Finish(t.Context(), claim.OperationID, "failed_partial", nil); e != nil {
		t.Fatal(e)
	}
	b.PathHash = strings.Repeat("d", 64) // Same actual namespace via another configured path.
	if _, e := repo.Claim(t.Context(), b, 10); !errors.Is(e, ErrSABStageQuota) {
		t.Fatal("alias reset old charge")
	}
	if stageCharge(t, db) != 6 {
		t.Fatal("failed partial lost charge")
	}
	if _, e := repo.Claim(t.Context(), b, 12); e != nil {
		t.Fatal("explicit quota increase lost old accounting", e)
	}
	if stageCharge(t, db) != 12 {
		t.Fatal("quota change reset charges")
	}
	if _, e := db.Connection().Exec("DELETE FROM import_queue"); e != nil {
		t.Fatal(e)
	}
	if stageCharge(t, db) != 12 {
		t.Fatal("queue deletion erased independent charge")
	}
	db.Close()
	reopened, e := NewDB(Config{DatabasePath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if stageCharge(t, reopened) != 12 {
		t.Fatal("restart lost charges")
	}
	prior, e := NewSABStageRepository(reopened.Connection()).Find(t.Context(), a.QueueID, a.ReceiptAttempt, a.SourceHash, a.Namespace)
	if e != nil || prior == nil || prior.State != "failed_partial" {
		t.Fatal("restart forgot attempt")
	}
}
func TestSABStageRepositoryTwoActualConnectionsAtomicQuotaAndReceipt(t *testing.T) {
	db, path := stageDatabase(t)
	second, e := NewDB(Config{DatabasePath: path})
	if e != nil {
		t.Fatal(e)
	}
	defer second.Close()
	claims := []SABStageClaim{stageClaimFixture(t, db, 1, 6), stageClaimFixture(t, db, 2, 6)}
	repos := []*SABStageRepository{NewSABStageRepository(db.Connection()), NewSABStageRepository(second.Connection())}
	start := make(chan struct{})
	done := make(chan error, 2)
	var wg sync.WaitGroup
	for i := range repos {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; _, e := repos[i].Claim(t.Context(), claims[i], 10); done <- e }(i)
	}
	close(start)
	wg.Wait()
	close(done)
	admitted, rejected := 0, 0
	for e := range done {
		if e == nil {
			admitted++
		} else if errors.Is(e, ErrSABStageQuota) {
			rejected++
		} else {
			t.Fatal(e)
		}
	}
	if admitted != 1 || rejected != 1 || stageCharge(t, db) != 6 {
		t.Fatal("concurrent read-modify-write exceeded quota")
	}
	c := stageClaimFixture(t, db, 3, 1)
	c.Namespace = strings.Repeat("e", 64)
	if _, e := repos[0].Claim(t.Context(), c, 10); !errors.Is(e, ErrSABStageUnavailable) {
		t.Fatal("same path adopted replacement root")
	}
	d := stageClaimFixture(t, db, 4, 1)
	d.ExpectedMetadata = "{}"
	if _, e := repos[0].Claim(t.Context(), d, 10); !errors.Is(e, ErrSABStageUnavailable) {
		t.Fatal("changed receipt accepted")
	}
	if stageCharge(t, db) != 6 {
		t.Fatal("failed admission charged or released existing work")
	}
}
func TestSABStageRepositoryPostRealCommitErrorIsUnknown(t *testing.T) {
	for _, phase := range []string{"claim", "finish"} {
		t.Run(phase, func(t *testing.T) {
			db, path := stageDatabase(t)
			c := stageClaimFixture(t, db, 1, 4)
			repo := NewSABStageRepository(db.Connection())
			if phase == "claim" {
				repo.afterCommit = func() error { return errors.New("simulated application acknowledgement loss after real SQL commit") }
			}
			claim, e := repo.Claim(t.Context(), c, 4)
			if phase == "claim" {
				if !errors.Is(e, ErrSABStageOutcomeUnknown) {
					t.Fatal("actual committed claim misreported")
				}
			} else {
				if e != nil {
					t.Fatal(e)
				}
				repo.afterCommit = func() error { return errors.New("simulated application acknowledgement loss after real SQL commit") }
				e = repo.Finish(t.Context(), claim.OperationID, "copied", []SABStageFile{{Ordinal: 1, Bytes: 4, SHA256: strings.Repeat("f", 64)}})
				if !errors.Is(e, ErrSABStageOutcomeUnknown) {
					t.Fatal("actual committed finish misreported")
				}
			}
			if stageCharge(t, db) != 4 {
				t.Fatal("unknown outcome pretended rollback")
			}
			db.Close()
			reopened, e := NewDB(Config{DatabasePath: path})
			if e != nil {
				t.Fatal(e)
			}
			defer reopened.Close()
			next := NewSABStageRepository(reopened.Connection())
			c.ExpectedMetadata = func() string {
				var v string
				if e := reopened.Connection().QueryRow("SELECT metadata FROM import_queue WHERE id=?", c.QueueID).Scan(&v); e != nil {
					t.Fatal(e)
				}
				return v
			}()
			if _, e := next.Claim(t.Context(), c, 100); !errors.Is(e, ErrSABStageExists) {
				t.Fatal("unknown attempt retried after reopen")
			}
			var state string
			if e := reopened.Connection().QueryRow("SELECT state FROM sab_import_stages").Scan(&state); e != nil {
				t.Fatal(e)
			}
			want := "pending"
			if phase == "finish" {
				want = "copied"
			}
			if state != want {
				t.Fatal("real persisted outcome not observed")
			}
		})
	}
}
func TestSABStageRepositoryCancelAndInvalidManifestFailClosed(t *testing.T) {
	db, _ := stageDatabase(t)
	repo := NewSABStageRepository(db.Connection())
	c := stageClaimFixture(t, db, 1, 4)
	db.Connection().SetMaxOpenConns(1)
	held, e := db.Connection().Conn(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	_, e = repo.Claim(ctx, c, 4)
	held.Close()
	if e == nil || stageCharge(t, db) != 0 {
		t.Fatal("canceled connection wait admitted")
	}
	claim, e := repo.Claim(t.Context(), c, 4)
	if e != nil {
		t.Fatal(e)
	}
	for _, files := range [][]SABStageFile{nil, {{Ordinal: 1, Bytes: 3, SHA256: strings.Repeat("a", 64)}}, {{Ordinal: 2, Bytes: 4, SHA256: strings.Repeat("a", 64)}}, {{Ordinal: 1, Bytes: 4, SHA256: "PRIVATE_PATH"}}} {
		if e := repo.Finish(t.Context(), claim.OperationID, "copied", files); e == nil {
			t.Fatal("invalid or incomplete manifest accepted")
		}
	}
	if stageCharge(t, db) != 4 {
		t.Fatal("invalid finish released charge")
	}
}
