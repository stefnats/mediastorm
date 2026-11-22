package database

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestARRImportReceiptSurvivesRequeueAndMetadataUpdate(t *testing.T) {
	db := setupTestDB(t)
	metadata := `{"_sabFallbackV1":{"jobId":"original-job"},"arr_import_v1":{"attempt":"original-arr-attempt","revision":3,"status":{"commandId":7,"state":"available"}},"titleId":"old"}`
	item := &ImportQueueItem{NzbPath: "/synthetic/arr-requeue.nzb", Status: QueueStatusFailed, Priority: QueuePriorityNormal, Metadata: &metadata}
	if e := db.Repository.AddToQueue(item); e != nil {
		t.Fatal(e)
	}
	var original map[string]json.RawMessage
	if e := json.Unmarshal([]byte(metadata), &original); e != nil {
		t.Fatal(e)
	}
	replacement := `{"titleId":"new","arr_import_v1":{"attempt":"replacement"}}`
	requeued := &ImportQueueItem{NzbPath: item.NzbPath, Status: QueueStatusPending, Priority: QueuePriorityNormal, Metadata: &replacement}
	if e := db.Repository.AddToQueue(requeued); e != nil {
		t.Fatal(e)
	}
	check := func() {
		saved, e := db.Repository.GetQueueItem(item.ID)
		if e != nil || saved.Metadata == nil {
			t.Fatal(e)
		}
		var fields map[string]json.RawMessage
		if e = json.Unmarshal([]byte(*saved.Metadata), &fields); e != nil {
			t.Fatal(e)
		}
		for _, key := range []string{SABFallbackMetadataKey, ARRImportMetadataKey} {
			var want, got any
			_ = json.Unmarshal(original[key], &want)
			_ = json.Unmarshal(fields[key], &got)
			if !reflect.DeepEqual(want, got) {
				t.Fatal("ordinary queue metadata replacement lost original command/status", key)
			}
		}
	}
	check()
	playback := `{"playback":"new-metadata"}`
	if e := db.Repository.UpdateMetadata(item.ID, &playback); e != nil {
		t.Fatal(e)
	}
	check()
}
