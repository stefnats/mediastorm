package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"novastream/internal/database"
)

func seedSABListReceipt(t *testing.T, f *sabFallbackFixture, n int, raw string) int64 {
	t.Helper()
	metadata := `{"unrelated":"PRIVATE-URL-AND-PATH-CANARY"}`
	path := "PRIVATE-RELATIVE-PATH"
	item := &database.ImportQueueItem{NzbPath: fmt.Sprintf("synthetic-list-%d.nzb", n), RelativePath: &path, Status: database.QueueStatusFailed, Metadata: &metadata}
	if err := f.db.Repository.AddToQueue(item); err != nil {
		t.Fatal(err)
	}
	if raw != "" {
		fields := map[string]json.RawMessage{database.SABFallbackMetadataKey: json.RawMessage(raw), "unrelated": json.RawMessage(`"PRIVATE-URL-AND-PATH-CANARY"`)}
		value, err := json.Marshal(fields)
		if err != nil {
			t.Fatal(err)
		}
		next := string(value)
		if err := f.db.Repository.UpdateMetadata(item.ID, &next); err != nil {
			t.Fatal(err)
		}
	}
	return item.ID
}

func TestSABFallbackListActualProducerPagesAndReopen(t *testing.T) {
	f := newSABFallbackFixture(t)
	if err := f.service.submitSABFallback(f.item); err != nil {
		t.Fatal(err)
	}
	_, _, original, err := f.service.fallbackItem(t.Context(), f.item.ID)
	if err != nil || original == nil {
		t.Fatal("actual handoff receipt missing", err)
	}
	for n := 0; n < 207; n++ {
		id := seedSABListReceipt(t, f, n, "")
		receipt := *original
		receipt.Status.QueueID = id
		receipt.Status.State = []string{"unknown", "downloading", "download_completed", "submission_unknown"}[n%4]
		receipt.Status.Reason = ""
		raw, _ := json.Marshal(receipt)
		fields := map[string]json.RawMessage{database.SABFallbackMetadataKey: raw, "private": json.RawMessage(`"APIKEY-PRIVATE-NZOID"`)}
		data, _ := json.Marshal(fields)
		value := string(data)
		if err := f.db.Repository.UpdateMetadata(id, &value); err != nil {
			t.Fatal(err)
		}
	}
	seedSABListReceipt(t, f, 999, "") // An ordinary queue row is never a handoff.
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	f.open(t) // Actual SQLite close/reopen persistence, not a copied status map.
	f.service.configGetter = nil
	reads, submits, statuses := f.cfgReads.Load(), f.submits.Load(), f.statuses.Load()
	cursor := ""
	seen := map[int64]bool{}
	prior := int64(1<<63 - 1)
	pages := 0
	for {
		page, err := f.service.ListSABFallbackStatuses(t.Context(), cursor, 50)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, item := range page.Items {
			if item.QueueID >= prior || seen[item.QueueID] {
				t.Fatal("unstable or repeated keyset", item.QueueID)
			}
			prior = item.QueueID
			seen[item.QueueID] = true
		}
		data, _ := json.Marshal(page)
		for _, secret := range []string{"PRIVATE", "jobId", "source", "attempt", "metadata", "nzbPath"} {
			if strings.Contains(string(data), secret) {
				t.Fatal("private projection", secret)
			}
		}
		if pages == 1 {
			id := seedSABListReceipt(t, f, 1000, "")
			receipt := *original
			receipt.Status.QueueID = id
			raw, _ := json.Marshal(receipt)
			value := `{"_sabFallbackV1":` + string(raw) + `}`
			if err := f.db.Repository.UpdateMetadata(id, &value); err != nil {
				t.Fatal(err)
			}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 208 || pages != 5 {
		t.Fatalf("complete receipt pages: %d/%d", len(seen), pages)
	}
	if f.cfgReads.Load() != reads || f.submits.Load() != submits || f.statuses.Load() != statuses {
		t.Fatal("browse caused configuration/provider work")
	}
	fresh, err := f.service.ListSABFallbackStatuses(t.Context(), "", 100)
	if err != nil || len(fresh.Items) != 100 || seen[fresh.Items[0].QueueID] {
		t.Fatal("explicit reload must see later insertion", err)
	}
}

func TestSABFallbackListInvalidBoundsAndMalformedReceipts(t *testing.T) {
	for _, cursor := range []string{"x", "v1.01.1", "v1.1.2", "v1.2.0", "v1.2.-1", "v1.2.1.extra", strings.Repeat("a", 100), "v1.9223372036854775808.1"} {
		t.Run(cursor, func(t *testing.T) {
			if _, err := (*Service)(nil).ListSABFallbackStatuses(t.Context(), cursor, 50); err != ErrSABFallbackInvalidPage {
				t.Fatal(err)
			}
		})
	}
	for _, limit := range []int{-1, 0, 101} {
		if _, err := (*Service)(nil).ListSABFallbackStatuses(t.Context(), "", limit); err != ErrSABFallbackInvalidPage {
			t.Fatal(err)
		}
	}
	for _, raw := range []string{`null`, `[]`, `"raw-private-error"`, `{"version":99}`, `{"private":"secret"}`, `"` + strings.Repeat("x", 9000) + `"`} {
		t.Run(fmt.Sprintf("receipt_%d", len(raw)), func(t *testing.T) {
			f := newSABFallbackFixture(t)
			seedSABListReceipt(t, f, 1, raw)
			if _, err := f.service.ListSABFallbackStatuses(t.Context(), "", 50); err != ErrSABFallbackUnavailable {
				t.Fatalf("malformed receipt must fail visibly: %v", err)
			}
			if f.statuses.Load() != 0 || f.submits.Load() != 0 {
				t.Fatal("malformed receipt caused provider work")
			}
		})
	}
}

func TestSABFallbackListSQLFailureAndCancellation(t *testing.T) {
	t.Run("SQL failure is not empty success", func(t *testing.T) {
		f := newSABFallbackFixture(t)
		if _, err := f.db.Connection().Exec("DROP TABLE import_queue"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.service.ListSABFallbackStatuses(t.Context(), "", 50); err != ErrSABFallbackUnavailable {
			t.Fatal(err)
		}
	})
	t.Run("waiting actual SQL connection obeys cancellation", func(t *testing.T) {
		f := newSABFallbackFixture(t)
		f.db.Connection().SetMaxOpenConns(1)
		conn, err := f.db.Connection().Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err = f.service.ListSABFallbackStatuses(ctx, "", 50)
		if err == nil || ctx.Err() == nil || time.Since(started) > time.Second {
			t.Fatal("blocked SQL did not stop", err)
		}
		if f.submits.Load() != 0 || f.statuses.Load() != 0 {
			t.Fatal("cancellation caused provider work")
		}
	})
}

func TestSABFallbackListCeilingDoesNotFreezeOlderRows(t *testing.T) {
	f := newSABFallbackFixture(t)
	if err := f.service.submitSABFallback(f.item); err != nil {
		t.Fatal(err)
	}
	_, _, receipt, err := f.service.fallbackItem(t.Context(), f.item.ID)
	if err != nil || receipt == nil {
		t.Fatal("producer receipt", err)
	}
	older := seedSABListReceipt(t, f, 2000, "")
	latest := seedSABListReceipt(t, f, 2001, "")
	attach := func(id int64, state string) {
		copy := *receipt
		copy.Status.QueueID = id
		copy.Status.State = state
		raw, err := json.Marshal(copy)
		if err != nil {
			t.Fatal(err)
		}
		next := `{"_sabFallbackV1":` + string(raw) + `}`
		if err := f.db.Repository.UpdateMetadata(id, &next); err != nil {
			t.Fatal(err)
		}
	}
	attach(latest, "submitted")
	first, err := f.service.ListSABFallbackStatuses(t.Context(), "", 1)
	if err != nil || len(first.Items) != 1 || first.Items[0].QueueID != latest || first.NextCursor == "" {
		t.Fatal("first page", err)
	}
	attach(older, "downloading") // This old queue row acquires its receipt later.
	if err := f.service.updateFallback(t.Context(), f.item.ID, receipt.Attempt, func(current *sabFallbackReceipt) error { current.Status.State = "unknown"; return nil }); err != nil {
		t.Fatal(err)
	}
	page, err := f.service.ListSABFallbackStatuses(t.Context(), first.NextCursor, 100)
	if err != nil || len(page.Items) != 2 || page.Items[0].QueueID != older || page.Items[0].State != "downloading" || page.Items[1].State != "unknown" {
		t.Fatal("cursor is not an immutable handoff snapshot", page, err)
	}
	if f.submits.Load() != 1 || f.statuses.Load() != 0 {
		t.Fatal("browse did external work")
	}
}

func TestSABFallbackListRepositoryByteBound(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		valid       bool
	}{
		{"ASCII exact byte limit", strings.Repeat("a", 8192), true},
		{"NUL does not truncate byte limit", "\x00" + strings.Repeat("a", 20000), false},
		{"multibyte byte limit", strings.Repeat("界", 3000), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSABFallbackFixture(t)
			raw, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			id := seedSABListReceipt(t, f, 3000, string(raw))
			rows, err := f.db.Repository.ListSABFallbackReceipts(t.Context(), id, 0, 1)
			if err != nil || len(rows) != 1 {
				t.Fatal("actual repository page", err)
			}
			if rows[0].Receipt.Valid != tc.valid || len(rows[0].Receipt.String) > 8192 {
				t.Fatalf("repository byte bound: returned=%d valid=%v want=%v", len(rows[0].Receipt.String), rows[0].Receipt.Valid, tc.valid)
			}
			if f.submits.Load() != 0 || f.statuses.Load() != 0 {
				t.Fatal("repository caused provider work")
			}
		})
	}
}
