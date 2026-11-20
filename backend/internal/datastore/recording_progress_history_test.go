package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func historyExample() recordingHistoryEnvelope {
	t := recordingHistoryTuple{recording: "progress", incarnation: "11111111-1111-4111-8111-111111111111", artifact: strings.Repeat("a", 32), duration: 4000, outputBytes: 4096, status: "completed", incomplete: true, gaps: true, observed: "2020-01-02T03:04:05.123456789Z", expires: "2020-01-03T03:04:05.123456789Z", spans: [][3]int64{{1, 0, 1001}, {2, 1001, 2999}}}
	raw, _ := t.bytes()
	hash := sha256.Sum256(raw)
	return recordingHistoryEnvelope{Version: 1, Timeline: raw, Mapping: hex.EncodeToString(hash[:]), Position: 1001, Revision: 2, Generation: 1, Operation: "22222222-2222-4222-8222-222222222222", Manifest: []recordingHistoryInput{{1, 32, strings.Repeat("b", 64)}, {2, 64, strings.Repeat("c", 64)}}, Ledger: strings.Repeat("d", 64)}
}
func historyEncoded(t *testing.T, e recordingHistoryEnvelope) []byte {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func historyDecoded(t *testing.T, e recordingHistoryEnvelope) *recordingProgressHistoryData {
	t.Helper()
	d, err := decodeRecordingProgressHistory(historyEncoded(t, e))
	if err != nil {
		t.Fatal("valid historical datum", err)
	}
	return d
}
func historyEditTuple(t *testing.T, e *recordingHistoryEnvelope, edit func(*recordingHistoryTuple)) {
	t.Helper()
	d := historyDecoded(t, *e)
	edit(&d.tuple)
	raw, err := d.tuple.bytes()
	if err != nil {
		t.Fatal(err)
	}
	e.Timeline = raw
	h := sha256.Sum256(raw)
	e.Mapping = hex.EncodeToString(h[:])
}
func historyExpectInvalid(t *testing.T, raw []byte) {
	t.Helper()
	if d, err := decodeRecordingProgressHistory(raw); err == nil || d != nil {
		t.Fatal("invalid historical data accepted")
	}
}
func historySetTupleRaw(t *testing.T, e *recordingHistoryEnvelope, index int, raw string) {
	t.Helper()
	var a []json.RawMessage
	if json.Unmarshal(e.Timeline, &a) != nil {
		t.Fatal("tuple")
	}
	a[index] = json.RawMessage(raw)
	e.Timeline, _ = json.Marshal(a)
	h := sha256.Sum256(e.Timeline)
	e.Mapping = hex.EncodeToString(h[:])
}

func TestRecordingProgressHistoryCanonicalGolden(t *testing.T) {
	for _, kind := range []string{"two-parts", "max-256-parts", "expired-data", "counter-above-2pow53", "nil-evidence"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			switch kind {
			case "max-256-parts":
				spans := make([][3]int64, 256)
				e.Manifest = make([]recordingHistoryInput, 256)
				for i := range spans {
					spans[i] = [3]int64{int64(i + 1), int64(i), 1}
					e.Manifest[i] = recordingHistoryInput{int64(i + 1), 1, strings.Repeat("b", 64)}
				}
				// Edit the original finite tuple without retaining an alias to the result.
				var a []json.RawMessage
				_ = json.Unmarshal(e.Timeline, &a)
				a[4] = json.RawMessage(`256`)
				a[11], _ = json.Marshal(spans)
				e.Timeline, _ = json.Marshal(a)
				h := sha256.Sum256(e.Timeline)
				e.Mapping = hex.EncodeToString(h[:])
				e.Position = 1
			case "counter-above-2pow53":
				e.Revision = 9007199254740993
				e.Generation = math.MaxInt64
			}
			raw := historyEncoded(t, e)
			d, err := decodeRecordingProgressHistory(raw)
			if err != nil || !bytes.Equal(raw, d.raw) || !bytes.Equal(e.Timeline, d.envelope.Timeline) || d.envelope.Revision != e.Revision || d.envelope.Generation != e.Generation {
				t.Fatal("canonical golden data changed", err)
			}
			if !d.expires.Before(time.Now()) {
				t.Fatal("fixture must be expired historical DATA")
			}
			if kind == "two-parts" && string(e.Timeline) != `["recording-derived-timeline/v1","progress","11111111-1111-4111-8111-111111111111","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",4000,4096,"completed",true,true,"2020-01-02T03:04:05.123456789Z","2020-01-03T03:04:05.123456789Z",[[1,0,1001],[2,1001,2999]]]` {
				t.Fatal("golden tuple differs")
			}
		})
	}
}
func TestRecordingProgressHistoryExactTimeText(t *testing.T) {
	for _, kind := range []string{"submicrosecond", "year-1-nonzero", "year-9999", "pgx-microsecond-projection", "nanosecond-change-digest", "noncanonical-offset", "zero-time", "expiry-not-after", "expiry-over-24h"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			invalid := false
			historyEditTuple(t, &e, func(v *recordingHistoryTuple) {
				switch kind {
				case "year-1-nonzero":
					v.observed = "0001-01-01T00:00:00.000000001Z"
					v.expires = "0001-01-02T00:00:00.000000001Z"
				case "year-9999":
					v.observed = "9999-12-30T00:00:00.123456789Z"
					v.expires = "9999-12-31T00:00:00.123456789Z"
				case "noncanonical-offset":
					v.observed = "2020-01-02T03:04:05.123456789+00:00"
					invalid = true
				case "zero-time":
					v.observed = "0001-01-01T00:00:00Z"
					v.expires = "0001-01-02T00:00:00Z"
					invalid = true
				case "expiry-not-after":
					v.expires = v.observed
					invalid = true
				case "expiry-over-24h":
					v.expires = "2020-01-03T03:04:06.123456789Z"
					invalid = true
				case "nanosecond-change-digest":
					v.observed = "2020-01-02T03:04:05.123456788Z"
				}
			})
			if invalid {
				historyExpectInvalid(t, historyEncoded(t, e))
				return
			}
			d := historyDecoded(t, e)
			if d.tuple.observed != stringTime(d.observed) {
				t.Fatal("original nanos lost")
			}
			if kind == "nanosecond-change-digest" && e.Mapping == historyExample().Mapping {
				t.Fatal("nanos omitted from fingerprint")
			}
			if kind == "pgx-microsecond-projection" {
				for _, at := range []time.Time{d.observed, time.Date(1960, 1, 1, 1, 2, 3, 123456789, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), time.Date(9999, 1, 1, 0, 0, 0, 123456789, time.UTC)} {
					got := historyPGTime(at)
					want := time.Unix(at.Unix(), int64(at.Nanosecond()/1000)*1000).UTC()
					if !got.Equal(want) {
						t.Fatal("pgx micro projection changed")
					}
				}
			}
		})
	}
}
func stringTime(v time.Time) string { return v.UTC().Format(time.RFC3339Nano) }
func TestRecordingProgressHistoryStrictEnvelope(t *testing.T) {
	for _, kind := range []string{"unknown-field", "duplicate-field", "missing-field", "wrong-case", "null-required", "trailing-json", "invalid-utf8", "noncanonical-whitespace", "noncanonical-order", "url-field", "path-field", "credential-field"} {
		t.Run(kind, func(t *testing.T) {
			raw := historyEncoded(t, historyExample())
			switch kind {
			case "unknown-field", "url-field", "path-field", "credential-field":
				key := "unknown"
				if kind == "url-field" {
					key = "url"
				}
				if kind == "path-field" {
					key = "path"
				}
				if kind == "credential-field" {
					key = "credential"
				}
				raw = append([]byte(`{"`+key+`":"synthetic",`), raw[1:]...)
			case "duplicate-field":
				raw = bytes.Replace(raw, []byte(`"schemaVersion":1`), []byte(`"schemaVersion":1,"schemaVersion":1`), 1)
			case "missing-field":
				raw = bytes.Replace(raw, []byte(`"atDerivedEnd":false,`), nil, 1)
			case "wrong-case":
				raw = bytes.Replace(raw, []byte(`"positionMs"`), []byte(`"PositionMs"`), 1)
			case "null-required":
				raw = bytes.Replace(raw, []byte(`"atDerivedEnd":false`), []byte(`"atDerivedEnd":null`), 1)
			case "trailing-json":
				raw = append(raw, []byte(`{}`)...)
			case "invalid-utf8":
				raw = append(raw, 0xff)
			case "noncanonical-whitespace":
				raw = append([]byte(" "), raw...)
			case "noncanonical-order":
				raw = bytes.Replace(raw, []byte(`"schemaVersion":1,`), nil, 1)
				raw = append(raw[:len(raw)-1], []byte(`,"schemaVersion":1}`)...)
			}
			historyExpectInvalid(t, raw)
		})
	}
}
func TestRecordingProgressHistoryStrictTuple(t *testing.T) {
	for _, kind := range []string{"wrong-arity", "wrong-tag", "bad-recording-component", "nil-uuid", "noncanonical-uuid", "bad-artifact", "wrong-mapping-digest", "bad-status", "nonboolean", "gaps-without-incomplete", "decimal-integer", "exponent-integer", "int64-overflow", "duration-zero", "duration-over-12h", "output-zero", "output-over-16gib"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			switch kind {
			case "wrong-arity":
				e.Timeline = []byte(`[]`)
			case "wrong-tag":
				historySetTupleRaw(t, &e, 0, `"wrong"`)
			case "bad-recording-component":
				historySetTupleRaw(t, &e, 1, `"../other"`)
			case "nil-uuid":
				historySetTupleRaw(t, &e, 2, `"00000000-0000-0000-0000-000000000000"`)
			case "noncanonical-uuid":
				historySetTupleRaw(t, &e, 2, `"{11111111-1111-4111-8111-111111111111}"`)
			case "bad-artifact":
				historySetTupleRaw(t, &e, 3, `"wrong"`)
			case "wrong-mapping-digest":
				e.Mapping = strings.Repeat("f", 64)
			case "bad-status":
				historySetTupleRaw(t, &e, 6, `"running"`)
			case "nonboolean":
				historySetTupleRaw(t, &e, 7, `1`)
			case "gaps-without-incomplete":
				historySetTupleRaw(t, &e, 7, `false`)
			case "decimal-integer":
				historySetTupleRaw(t, &e, 4, `4000.0`)
			case "exponent-integer":
				historySetTupleRaw(t, &e, 4, `4e3`)
			case "int64-overflow":
				historySetTupleRaw(t, &e, 4, `9223372036854775808`)
			case "duration-zero":
				historySetTupleRaw(t, &e, 4, `0`)
			case "duration-over-12h":
				historySetTupleRaw(t, &e, 4, `43200001`)
			case "output-zero":
				historySetTupleRaw(t, &e, 5, `0`)
			case "output-over-16gib":
				historySetTupleRaw(t, &e, 5, `17179869185`)
			}
			historyExpectInvalid(t, historyEncoded(t, e))
		})
	}
}
func TestRecordingProgressHistoryPartManifest(t *testing.T) {
	for _, kind := range []string{"one-part", "257-parts", "index-zero", "index-repeated", "noncontiguous-span", "zero-span", "span-sum-overflow", "span-duration-mismatch", "manifest-count-mismatch", "manifest-index-mismatch", "negative-byte-length", "byte-sum-overflow", "input-over-8gib", "uppercase-digest"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			switch kind {
			case "one-part":
				historySetTupleRaw(t, &e, 11, `[[1,0,4000]]`)
				e.Manifest = e.Manifest[:1]
			case "257-parts":
				spans := make([][3]int64, 257)
				e.Manifest = make([]recordingHistoryInput, 257)
				for i := range spans {
					spans[i] = [3]int64{int64(i + 1), int64(i), 1}
					e.Manifest[i] = recordingHistoryInput{int64(i + 1), 1, strings.Repeat("b", 64)}
				}
				raw, _ := json.Marshal(spans)
				historySetTupleRaw(t, &e, 11, string(raw))
			case "index-zero":
				historySetTupleRaw(t, &e, 11, `[[0,0,1001],[2,1001,2999]]`)
			case "index-repeated":
				historySetTupleRaw(t, &e, 11, `[[1,0,1001],[1,1001,2999]]`)
			case "noncontiguous-span":
				historySetTupleRaw(t, &e, 11, `[[1,0,1001],[2,1002,2999]]`)
			case "zero-span":
				historySetTupleRaw(t, &e, 11, `[[1,0,0],[2,0,4000]]`)
			case "span-sum-overflow":
				historySetTupleRaw(t, &e, 11, `[[1,0,1001],[2,1001,9223372036854775807]]`)
			case "span-duration-mismatch":
				historySetTupleRaw(t, &e, 11, `[[1,0,1001],[2,1001,2998]]`)
			case "manifest-count-mismatch":
				e.Manifest = e.Manifest[:1]
			case "manifest-index-mismatch":
				e.Manifest[1].index = 1
			case "negative-byte-length":
				e.Manifest[0].size = -1
			case "byte-sum-overflow":
				e.Manifest[1].size = math.MaxInt64
			case "input-over-8gib":
				e.Manifest[0].size = 8 << 30
			case "uppercase-digest":
				e.Manifest[0].digest = strings.Repeat("A", 64)
			}
			historyExpectInvalid(t, historyEncoded(t, e))
		})
	}
}
func TestRecordingProgressHistoryBudgetAndCopies(t *testing.T) {
	for _, kind := range []string{"oversize-utf8-before-decode", "bounded-largest-valid", "input-mutation", "returned-span-mutation", "returned-manifest-mutation"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			raw := historyEncoded(t, e)
			if kind == "oversize-utf8-before-decode" {
				historyExpectInvalid(t, []byte(strings.Repeat("é", 32769)))
				return
			}
			if kind == "bounded-largest-valid" {
				spans := make([][3]int64, 256)
				e.Manifest = make([]recordingHistoryInput, 256)
				for i := range spans {
					spans[i] = [3]int64{int64(i + 1), int64(i * 1000), 1000}
					e.Manifest[i] = recordingHistoryInput{int64(i + 1), 33554432, strings.Repeat("e", 64)}
				}
				partRaw, _ := json.Marshal(spans)
				historySetTupleRaw(t, &e, 11, string(partRaw))
				historySetTupleRaw(t, &e, 4, `256000`)
				raw = historyEncoded(t, e)
				if len(raw) > 65536 {
					t.Fatal("finite valid datum above budget")
				}
			}
			d, err := decodeRecordingProgressHistory(raw)
			if err != nil {
				t.Fatal(err)
			}
			original := append([]byte(nil), d.raw...)
			switch kind {
			case "input-mutation":
				raw[0] = '!'
			case "returned-span-mutation":
				d.tuple.spans[0][2] = 99
			case "returned-manifest-mutation":
				d.envelope.Manifest[0].size = 99
			}
			another, err := decodeRecordingProgressHistory(original)
			if err != nil || !bytes.Equal(another.raw, original) || !bytes.Equal(d.raw, original) || another.tuple.spans[0][2] != func() int64 {
				if kind == "bounded-largest-valid" {
					return 1000
				}
				return 1001
			}() || another.envelope.Manifest[0].size != func() int64 {
				if kind == "bounded-largest-valid" {
					return 33554432
				}
				return 32
			}() {
				t.Fatal("mutable alias escaped finite data")
			}
		})
	}
}
func TestRecordingProgressHistoryOptionalEvidenceNeverAllowsResume(t *testing.T) {
	for _, kind := range []string{"no-output-or-tools", "output-hash-only", "tool-and-transform-hashes", "partial-tool-pair-denied", "labels-not-digests"} {
		t.Run(kind, func(t *testing.T) {
			e := historyExample()
			hash := strings.Repeat("e", 64)
			label := "ffmpeg-synthetic"
			switch kind {
			case "output-hash-only":
				e.Output = &hash
			case "tool-and-transform-hashes":
				e.Tool = &hash
				e.Transform = &hash
			case "partial-tool-pair-denied":
				e.Tool = &hash
			case "labels-not-digests":
				e.Tool = &label
				e.Transform = &hash
			}
			if kind == "partial-tool-pair-denied" || kind == "labels-not-digests" {
				historyExpectInvalid(t, historyEncoded(t, e))
				return
			}
			d := historyDecoded(t, e)
			if !d.expires.Before(time.Now()) || d.envelope.Version != 1 {
				t.Fatal("historical hashes changed lifetime")
			}
			// There is deliberately no permission, current mapper or resume API on d.
		})
	}
}

func historyPG(t *testing.T) *recordingProgressFixture {
	t.Helper()
	f := progressPG(t, false)
	f.migrate(t, 86, false)
	return f
}
func historyGuard(t *testing.T, f *recordingProgressFixture, ctx context.Context) *settingsProgressTx {
	t.Helper()
	g, err := f.ds.beginSettingsProgressTx(ctx, f.coordinator)
	if err != nil {
		t.Fatal("genuine original guard", err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error("close original guard", err)
		}
	})
	return g
}
func historyInsertPosition(t *testing.T, tx pgx.Tx, d *recordingProgressHistoryData) {
	t.Helper()
	e, v := d.envelope, d.tuple
	_, err := tx.Exec(t.Context(), `INSERT INTO recording_playback_positions(recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id) VALUES($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid) ON CONFLICT(recording_id) DO UPDATE SET incarnation=EXCLUDED.incarnation,artifact_id=EXCLUDED.artifact_id,mapping_sha256=EXCLUDED.mapping_sha256,duration_ms=EXCLUDED.duration_ms,position_ms=EXCLUDED.position_ms,at_derived_end=EXCLUDED.at_derived_end,descriptor_observed_at=EXCLUDED.descriptor_observed_at,descriptor_expires_at=EXCLUDED.descriptor_expires_at,revision=EXCLUDED.revision,writer_generation=EXCLUDED.writer_generation,operation_id=EXCLUDED.operation_id`, v.recording, v.incarnation, v.artifact, e.Mapping, v.duration, e.Position, e.End, d.observed, d.expires, e.Revision, e.Generation, e.Operation)
	if err != nil {
		t.Fatal("seed historical085 DATA on caller TX", err)
	}
}
func historyStore(t *testing.T, f *recordingProgressFixture) *recordingProgressHistoryData {
	t.Helper()
	d := historyDecoded(t, historyExample())
	g := historyGuard(t, f, t.Context())
	historyInsertPosition(t, g.tx, d)
	if err := g.writeRecordingProgressHistory(d); err != nil {
		t.Fatal(err)
	}
	if err := g.tx.Commit(g.ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	return d
}
func historyRead(t *testing.T, f *recordingProgressFixture) (recordingProgressHistoryRead, error) {
	t.Helper()
	g := historyGuard(t, f, t.Context())
	defer g.Close()
	return g.readRecordingProgressHistory("progress")
}
func TestRecordingProgressHistoryCallerOwnsTransaction(t *testing.T) {
	for _, kind := range []string{"same-backend-no-second-acquire", "uncommitted-invisible", "caller-commit-publishes-both", "caller-rollback-publishes-neither", "helper-does-not-close-guard"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			g := historyGuard(t, f, t.Context())
			d := historyDecoded(t, historyExample())
			pid := g.connection.Conn().PgConn().PID()
			historyInsertPosition(t, g.tx, d)
			if err := g.writeRecordingProgressHistory(d); err != nil {
				t.Fatal(err)
			}
			var backend uint32
			if g.tx.QueryRow(g.ctx, `SELECT pg_backend_pid()`).Scan(&backend) != nil || backend != pid || f.ds.pool.Stat().AcquiredConns() != 1 {
				t.Fatal("helper did not preserve original one-connection TX")
			}
			var before int
			table := pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()
			if f.admin.QueryRow(t.Context(), `SELECT count(*) FROM `+table).Scan(&before) != nil || before != 0 {
				t.Fatal("uncommitted history visible")
			}
			if kind == "caller-rollback-publishes-neither" {
				if err := g.tx.Rollback(g.ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := g.tx.Commit(g.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			var history, position int
			expected := 1
			if kind == "caller-rollback-publishes-neither" {
				expected = 0
			}
			if f.ds.pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM recording_progress_history),(SELECT count(*) FROM recording_playback_positions)`).Scan(&history, &position) != nil || history != expected || position != expected {
				t.Fatal("caller transaction did not own both rows")
			}
		})
	}
}
func TestRecordingProgressHistoryHistoricalPayloadAssociation(t *testing.T) {
	for _, kind := range []string{"exact-original-payload", "incarnation-mismatch", "artifact-mismatch", "mapping-mismatch", "duration-mismatch", "position-mismatch", "end-mismatch", "revision-mismatch", "generation-mismatch", "operation-mismatch", "observed-microsecond-mismatch", "expires-microsecond-mismatch"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			d := historyDecoded(t, historyExample())
			g := historyGuard(t, f, t.Context())
			historyInsertPosition(t, g.tx, d)
			sql := ""
			switch kind {
			case "incarnation-mismatch":
				sql = `incarnation=gen_random_uuid()`
			case "artifact-mismatch":
				sql = `artifact_id=repeat('e',32)`
			case "mapping-mismatch":
				sql = `mapping_sha256=repeat('e',64)`
			case "duration-mismatch":
				sql = `duration_ms=duration_ms+1`
			case "position-mismatch":
				sql = `position_ms=position_ms+1`
			case "end-mismatch":
				sql = `position_ms=duration_ms,at_derived_end=true`
			case "revision-mismatch":
				sql = `revision=revision+1`
			case "generation-mismatch":
				sql = `writer_generation=writer_generation+1`
			case "operation-mismatch":
				sql = `operation_id=gen_random_uuid()`
			case "observed-microsecond-mismatch":
				sql = `descriptor_observed_at=descriptor_observed_at+interval '1 microsecond'`
			case "expires-microsecond-mismatch":
				sql = `descriptor_expires_at=descriptor_expires_at-interval '1 microsecond'`
			}
			if sql != "" {
				if _, err := g.tx.Exec(g.ctx, `UPDATE recording_playback_positions SET `+sql); err != nil {
					t.Fatal(err)
				}
			}
			err := g.writeRecordingProgressHistory(d)
			if kind != "exact-original-payload" {
				if !errors.Is(err, errRecordingHistoryAssociation) {
					t.Fatal("mismatched085 accepted", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := g.readRecordingProgressHistory("progress")
			if err != nil || got.state != "historical-data" || got.data == nil || !bytes.Equal(got.data.raw, d.raw) {
				t.Fatal("exact original historical association lost", err)
			}
		})
	}
}
func TestRecordingProgressHistoryReadStates(t *testing.T) {
	for _, kind := range []string{"absent-legacy", "malformed-bounded", "stale-after-existing-085-update", "missing-strong-evidence", "sql-unavailable", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			if kind != "absent-legacy" {
				historyStore(t, f)
			}
			expected := "historical-data"
			switch kind {
			case "absent-legacy":
				expected = "absent"
			case "malformed-bounded":
				f.exec(t, `UPDATE recording_progress_history SET data_text='{}'`)
				expected = "invalid"
			case "stale-after-existing-085-update":
				f.commit(t, progressCommand(RecordingProgressFromStart, 0, 0, 0))
				expected = "stale"
			case "sql-unavailable":
				f.exec(t, `ALTER TABLE recording_progress_history RENAME TO held_history`)
			}
			g := historyGuard(t, f, t.Context())
			if kind == "cancelled" {
				g.cancel()
			}
			got, err := g.readRecordingProgressHistory("progress")
			if kind == "sql-unavailable" || kind == "cancelled" {
				if err == nil || got.data != nil || got.state != "" {
					t.Fatal("SQL/lifetime error became empty success")
				}
				return
			}
			if err != nil || got.state != expected {
				t.Fatal("read-state distinction lost", got.state, err)
			}
			if expected != "historical-data" && got.data != nil {
				t.Fatal("invalid/stale data leaked position")
			}
			if kind == "malformed-bounded" {
				if err := g.Close(); err != nil {
					t.Fatal(err)
				}
				original := historyDecoded(t, historyExample())
				f.exec(t, `UPDATE recording_progress_history SET data_text=$1`, string(original.raw))
				positive, err := historyRead(t, f)
				if err != nil || positive.state != "historical-data" {
					t.Fatal("malformed canary positive control", err)
				}
				f.exec(t, `ALTER TABLE recording_progress_history DROP CONSTRAINT recording_progress_history_artifact_id_check;UPDATE recording_progress_history SET artifact_id=repeat('x',33)`)
				artifact, err := historyRead(t, f)
				if err != nil || artifact.state != "invalid" || artifact.data != nil {
					t.Fatal("NULL artifact projection became SQL error or stale", err)
				}
				f.exec(t, `UPDATE recording_progress_history SET artifact_id=repeat('a',32);ALTER TABLE recording_progress_history DROP CONSTRAINT recording_progress_history_mapping_sha256_check;UPDATE recording_progress_history SET mapping_sha256=repeat('x',65)`)
				mapping, err := historyRead(t, f)
				if err != nil || mapping.state != "invalid" || mapping.data != nil {
					t.Fatal("NULL mapping projection became SQL error or stale", err)
				}
			}
			if kind == "missing-strong-evidence" && (!got.missingStrongEvidence || got.data == nil) {
				t.Fatal("missing evidence silently adopted")
			}
		})
	}
}
func TestRecordingProgressHistoryWireBudgetBeforeScan(t *testing.T) {
	for _, kind := range []string{"bounded-original-rawvalue", "oversize-null-rawvalue", "oversize-invalid-json-null", "byte-not-character-limit"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			d := historyStore(t, f)
			if kind != "bounded-original-rawvalue" {
				f.exec(t, `ALTER TABLE recording_progress_history DROP CONSTRAINT recording_progress_history_data_budget`)
				raw := strings.Repeat(" ", 65537)
				if kind == "oversize-invalid-json-null" {
					raw = strings.Repeat("!", 65537)
				}
				if kind == "byte-not-character-limit" {
					raw = strings.Repeat("é", 32769)
				}
				f.exec(t, `UPDATE recording_progress_history SET data_text=$1`, raw)
			}
			g := historyGuard(t, f, t.Context())
			rows, err := g.tx.Query(g.ctx, recordingHistoryReadQuery(f.schema), "progress")
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				rows.Close()
				t.Fatal("actual raw DataRow missing")
			}
			raw := append([]byte(nil), rows.RawValues()[0]...)
			isnull := rows.RawValues()[0] == nil
			if rows.Next() {
				rows.Close()
				t.Fatal("unbounded history log")
			}
			if rows.Err() != nil {
				t.Fatal(rows.Err())
			}
			rows.Close()
			if kind == "bounded-original-rawvalue" {
				if isnull || len(raw) > 65536 || !bytes.Equal(raw, d.raw) {
					t.Fatal("bounded exact DataRow changed")
				}
			} else if !isnull {
				t.Fatal("oversized value reached Go BEFORE Scan")
			}
			got, err := g.readRecordingProgressHistory("progress")
			if err != nil {
				t.Fatal(err)
			}
			want := "invalid"
			if kind == "bounded-original-rawvalue" {
				want = "historical-data"
			}
			if got.state != want {
				t.Fatal("budget not separately validated")
			}
		})
	}
}
func TestRecordingProgressHistoryOriginalTransactionLifetime(t *testing.T) {
	for _, kind := range []string{"nil-guard", "nil-tx", "closed-tx", "cancelled-guard", "held-sidecar-lock-cancel-and-cleanup"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "nil-guard" {
				var g *settingsProgressTx
				if _, err := g.readRecordingProgressHistory("progress"); err == nil {
					t.Fatal("nil guard accepted")
				}
				return
			}
			if kind == "nil-tx" {
				g := &settingsProgressTx{ctx: t.Context()}
				if _, err := g.readRecordingProgressHistory("progress"); err == nil {
					t.Fatal("missing original TX accepted")
				}
				return
			}
			f := historyPG(t)
			historyStore(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			g := historyGuard(t, f, ctx)
			switch kind {
			case "closed-tx":
				if err := g.tx.Rollback(g.ctx); err != nil {
					t.Fatal(err)
				}
			case "cancelled-guard":
				cancel()
			}
			if kind != "held-sidecar-lock-cancel-and-cleanup" {
				if _, err := g.readRecordingProgressHistory("progress"); err == nil {
					t.Fatal("closed/canceled original guard accepted")
				}
				return
			}
			holder, err := f.admin.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { recordingStateRollback(holder) })
			table := pgx.Identifier{f.schema, "recording_progress_history"}.Sanitize()
			if _, err = holder.Exec(t.Context(), `LOCK TABLE `+table+` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			pid := g.connection.Conn().PgConn().PID()
			done := make(chan error, 1)
			finished := make(chan struct{})
			go func() { defer close(finished); _, err := g.readRecordingProgressHistory("progress"); done <- err }()
			t.Cleanup(func() { cancel(); recordingStateRollback(holder); <-finished })
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond * 5)
			defer tick.Stop()
			for {
				var blocked bool
				if f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted AND relation=$2::regclass)`, pid, table).Scan(&blocked) != nil {
					t.Fatal("observe actual blocked query")
				}
				if blocked {
					break
				}
				select {
				case err := <-done:
					t.Fatal("query completed before held-lock witness", err)
				case <-deadline.C:
					t.Fatal("actual pending query not observed")
				case <-tick.C:
				}
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancel yielded data")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("original query not canceled")
			}
			physical := g.connection.Conn().PgConn()
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if physical.IsClosed() {
				select {
				case <-physical.CleanupDone():
				default:
					t.Fatal("driver cleanup not joined")
				}
			}
			var pending bool
			if f.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)`, pid).Scan(&pending) != nil || pending {
				t.Fatal("original query remained pending after caller cleanup")
			}
		})
	}
}
func TestRecordingProgressHistoryRestoreAndLegacyRemainData(t *testing.T) {
	for _, kind := range []string{"fresh-084-historical-data", "legacy-position-without-history", "missing-backup-section-not-backfilled", "current-084-rotation-does-not-delete-history"} {
		t.Run(kind, func(t *testing.T) {
			f := historyPG(t)
			d := historyDecoded(t, historyExample())
			if kind == "legacy-position-without-history" || kind == "missing-backup-section-not-backfilled" {
				g := historyGuard(t, f, t.Context())
				historyInsertPosition(t, g.tx, d)
				if err := g.tx.Commit(g.ctx); err != nil {
					t.Fatal(err)
				}
				g.Close()
			} else {
				historyStore(t, f)
			}
			var current string
			if f.ds.pool.QueryRow(t.Context(), `SELECT incarnation::text FROM recording_playback_state WHERE recording_id='progress'`).Scan(&current) != nil || current == d.tuple.incarnation {
				t.Fatal("fixture lacks genuine fresh different084")
			}
			if kind == "current-084-rotation-does-not-delete-history" {
				f.exec(t, `UPDATE recordings SET user_id='two' WHERE id='progress';UPDATE recordings SET user_id='one' WHERE id='progress'`)
				var next string
				if f.ds.pool.QueryRow(t.Context(), `SELECT incarnation::text FROM recording_playback_state WHERE recording_id='progress'`).Scan(&next) != nil || next == current {
					t.Fatal("actual ownerABA did not rotate084")
				}
			}
			got, err := historyRead(t, f)
			if err != nil {
				t.Fatal(err)
			}
			want := "historical-data"
			if kind == "legacy-position-without-history" || kind == "missing-backup-section-not-backfilled" {
				want = "absent"
			}
			if got.state != want {
				t.Fatal("legacy or fresh084 was reinterpreted")
			}
			if got.data != nil && (got.data.tuple.incarnation != d.tuple.incarnation || !got.data.expires.Before(time.Now())) {
				t.Fatal("historical data was rebased or renewed")
			}
		})
	}
}
