package datastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const recordingHistoryByteLimit = 65536

var (
	errRecordingHistoryInvalid     = errors.New("recording history data invalid")
	errRecordingHistoryAssociation = errors.New("recording history payload association changed")
	errRecordingHistoryTransaction = errors.New("recording history original transaction unavailable")
)

// All values in this file are historical DATA. Even supplied byte/tool hashes
// are not verified here. No value is a source, writer or resume permission.
type recordingHistoryTuple struct {
	recording, incarnation, artifact string
	duration, outputBytes            int64
	status                           string
	incomplete, gaps                 bool
	observed, expires                string
	spans                            [][3]int64
}

func (t recordingHistoryTuple) bytes() ([]byte, error) {
	return json.Marshal([12]any{"recording-derived-timeline/v1", t.recording, t.incarnation, t.artifact, t.duration, t.outputBytes, t.status, t.incomplete, t.gaps, t.observed, t.expires, t.spans})
}

type recordingHistoryEnvelope struct {
	Version    int                     `json:"schemaVersion"`
	Timeline   json.RawMessage         `json:"timeline"`
	Mapping    string                  `json:"mappingSha256"`
	Position   int64                   `json:"positionMs"`
	End        bool                    `json:"atDerivedEnd"`
	Revision   int64                   `json:"revision"`
	Generation int64                   `json:"writerGeneration"`
	Operation  string                  `json:"operationId"`
	Manifest   []recordingHistoryInput `json:"inputManifest"`
	Ledger     string                  `json:"ledgerSha256"`
	Tool       *string                 `json:"toolSha256"`
	Transform  *string                 `json:"transformSha256"`
	Output     *string                 `json:"outputSha256"`
}

// Fixed array encoding is [partIndex,byteLength,sha256]; the digest is DATA.
type recordingHistoryInput struct {
	index, size int64
	digest      string
}

func (p recordingHistoryInput) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]any{p.index, p.size, p.digest})
}
func (p *recordingHistoryInput) UnmarshalJSON(raw []byte) error {
	var a []json.RawMessage
	if json.Unmarshal(raw, &a) != nil || len(a) != 3 || json.Unmarshal(a[0], &p.index) != nil || json.Unmarshal(a[1], &p.size) != nil || json.Unmarshal(a[2], &p.digest) != nil {
		return errRecordingHistoryInvalid
	}
	return nil
}

type recordingProgressHistoryData struct {
	raw               []byte
	envelope          recordingHistoryEnvelope
	tuple             recordingHistoryTuple
	observed, expires time.Time
}

// Read states never imply current matching or compatibility. Invalid/stale
// results omit the datum altogether; absence is distinct from a SQL error.
type recordingProgressHistoryRead struct {
	state                 string
	data                  *recordingProgressHistoryData
	missingStrongEvidence bool
}

func historySafeRecording(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
func historyUUID(s string) bool {
	u, e := uuid.Parse(s)
	return e == nil && u != uuid.Nil && u.String() == s
}
func historyHex(s string, n int) bool {
	return len(s) == n && strings.IndexFunc(s, func(c rune) bool { return !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') }) < 0
}
func historyTime(s string) (time.Time, bool) {
	t, e := time.Parse(time.RFC3339Nano, s)
	return t, e == nil && !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t.UTC().Format(time.RFC3339Nano) == s
}

// Use the actual pinned pgx timestamptz projection, including pre-epoch dates.
// UnixNano would overflow for valid supported years. Never rebuild tuple text.
func historyPGTime(t time.Time) time.Time {
	return time.UnixMicro(t.Unix()*1000000 + int64(t.Nanosecond())/1000).UTC()
}
func decodeRecordingProgressHistory(raw []byte) (*recordingProgressHistoryData, error) {
	if len(raw) > recordingHistoryByteLimit || len(raw) < 2 || !utf8.Valid(raw) {
		return nil, errRecordingHistoryInvalid
	}
	var e recordingHistoryEnvelope
	if json.Unmarshal(raw, &e) != nil || e.Version != 1 {
		return nil, errRecordingHistoryInvalid
	}
	var a []json.RawMessage
	if json.Unmarshal(e.Timeline, &a) != nil || len(a) != 12 {
		return nil, errRecordingHistoryInvalid
	}
	var tag string
	t := recordingHistoryTuple{}
	destinations := []any{&tag, &t.recording, &t.incarnation, &t.artifact, &t.duration, &t.outputBytes, &t.status, &t.incomplete, &t.gaps, &t.observed, &t.expires, &t.spans}
	for i, d := range destinations {
		if json.Unmarshal(a[i], d) != nil {
			return nil, errRecordingHistoryInvalid
		}
	}
	observed, ok := historyTime(t.observed)
	expires, xok := historyTime(t.expires)
	if tag != "recording-derived-timeline/v1" || !historySafeRecording(t.recording) || !historyUUID(t.incarnation) || !historyHex(t.artifact, 32) || t.duration <= 0 || t.duration > 43200000 || t.outputBytes <= 0 || t.outputBytes > 16<<30 || !(t.status == "completed" || t.status == "failed" || t.status == "cancelled") || t.gaps && !t.incomplete || !ok || !xok || !expires.After(observed) || expires.After(observed.Add(24*time.Hour)) || len(t.spans) < 2 || len(t.spans) > 256 || len(e.Manifest) != len(t.spans) || e.Position < 0 || e.Position > t.duration || e.End != (e.Position == t.duration) || e.Revision < 0 || e.Generation < 0 || !historyUUID(e.Operation) || !historyHex(e.Mapping, 64) || !historyHex(e.Ledger, 64) {
		return nil, errRecordingHistoryInvalid
	}
	var end, total int64
	for i, s := range t.spans {
		p := e.Manifest[i]
		if s[0] != int64(i+1) || s[1] != end || s[2] <= 0 || s[2] > 43200000-end || p.index != s[0] || p.size <= 0 || p.size > (8<<30)-total || !historyHex(p.digest, 64) {
			return nil, errRecordingHistoryInvalid
		}
		end += s[2]
		total += p.size
	}
	if end != t.duration || (e.Tool == nil) != (e.Transform == nil) {
		return nil, errRecordingHistoryInvalid
	}
	for _, h := range []*string{e.Tool, e.Transform, e.Output} {
		if h != nil && !historyHex(*h, 64) {
			return nil, errRecordingHistoryInvalid
		}
	}
	tuple, err := t.bytes()
	if err != nil || !bytes.Equal(tuple, e.Timeline) {
		return nil, errRecordingHistoryInvalid
	}
	digest := sha256.Sum256(tuple)
	if hex.EncodeToString(digest[:]) != e.Mapping {
		return nil, errRecordingHistoryInvalid
	}
	canonical, err := json.Marshal(e)
	// Byte equality also rejects duplicate/missing/case-folded fields, null
	// primitives and alternate integer spellings accepted by generic JSON parsers.
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errRecordingHistoryInvalid
	}
	return &recordingProgressHistoryData{raw: append([]byte(nil), canonical...), envelope: e, tuple: t, observed: observed, expires: expires}, nil
}

func (g *settingsProgressTx) historyTransaction() error {
	if g == nil || g.tx == nil || g.ctx == nil || g.ctx.Err() != nil {
		return errRecordingHistoryTransaction
	}
	return nil
}

// The private guard has no store/self receipt. Authentic construction, lock
// order, serial transaction use and lifetime are trusted internal caller duties.
// This helper never obtains another connection or owns COMMIT/Close/rollback.
func (g *settingsProgressTx) historySchema() (string, error) {
	if err := g.historyTransaction(); err != nil {
		return "", err
	}
	c, err := readRecordingPlaybackCatalog(g.ctx, g.tx)
	if err != nil {
		return "", err
	}
	var valid bool
	err = g.tx.QueryRow(g.ctx, `SELECT EXISTS(
 SELECT 1 FROM pg_catalog.pg_class h JOIN pg_catalog.pg_namespace n ON n.oid=h.relnamespace
 WHERE n.nspname=$1 AND h.relname='recording_progress_history' AND h.relkind='r' AND h.relpersistence='p' AND NOT h.relispartition
 AND (SELECT count(*) FROM pg_catalog.pg_attribute a WHERE a.attrelid=h.oid AND a.attnum>0 AND NOT a.attisdropped)=13
 AND NOT EXISTS(SELECT 1 FROM (VALUES
 ('recording_id','text'),('incarnation','uuid'),('artifact_id','text'),('mapping_sha256','text'),
 ('duration_ms','bigint'),('position_ms','bigint'),('at_derived_end','boolean'),
 ('descriptor_observed_at','timestamp with time zone'),('descriptor_expires_at','timestamp with time zone'),
 ('revision','bigint'),('writer_generation','bigint'),('operation_id','uuid'),('data_text','text')) expected(name,kind)
 WHERE NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=h.oid AND a.attname=expected.name AND a.attnum>0 AND NOT a.attisdropped AND a.attnotnull AND pg_catalog.format_type(a.atttypid,a.atttypmod)=expected.kind))
 AND EXISTS(SELECT 1 FROM pg_catalog.pg_constraint k WHERE k.conrelid=h.oid AND k.contype='p' AND k.conkey=ARRAY[1]::smallint[])
 AND EXISTS(SELECT 1 FROM pg_catalog.pg_constraint k JOIN pg_catalog.pg_class p ON p.oid=k.confrelid
 WHERE k.conrelid=h.oid AND k.contype='f' AND k.conkey=ARRAY[1]::smallint[] AND k.confkey=ARRAY[1]::smallint[] AND k.confdeltype='c' AND k.convalidated AND p.relnamespace=n.oid AND p.relname='recording_playback_positions'))`, c.schema).Scan(&valid)
	if err != nil {
		return "", err
	}
	if !valid {
		return "", errRecordingHistoryTransaction
	}
	return c.schema, nil
}

const historyPayloadEqual = `h.incarnation=p.incarnation AND h.artifact_id=p.artifact_id AND h.mapping_sha256=p.mapping_sha256 AND h.duration_ms=p.duration_ms AND h.position_ms=p.position_ms AND h.at_derived_end=p.at_derived_end AND h.descriptor_observed_at=p.descriptor_observed_at AND h.descriptor_expires_at=p.descriptor_expires_at AND h.revision=p.revision AND h.writer_generation=p.writer_generation AND h.operation_id=p.operation_id`

// CASE bounds the real DataRow value before Scan/JSON allocation, even if an
// operator has explicitly corrupted this new table's own budget constraint.
func recordingHistoryReadQuery(schema string) string {
	h := pgx.Identifier{schema, "recording_progress_history"}.Sanitize()
	p := pgx.Identifier{schema, "recording_playback_positions"}.Sanitize()
	return `SELECT CASE WHEN octet_length(h.data_text)<=65536 THEN h.data_text ELSE NULL END,octet_length(h.data_text)<=65536,(` + historyPayloadEqual + `),
 h.incarnation::text,CASE WHEN octet_length(h.artifact_id)=32 THEN h.artifact_id ELSE NULL END,CASE WHEN octet_length(h.mapping_sha256)=64 THEN h.mapping_sha256 ELSE NULL END,h.duration_ms,h.position_ms,h.at_derived_end,h.descriptor_observed_at,h.descriptor_expires_at,h.revision,h.writer_generation,h.operation_id::text
 FROM ` + h + ` h JOIN ` + p + ` p USING(recording_id) WHERE h.recording_id=$1`
}
func (g *settingsProgressTx) readRecordingProgressHistory(id string) (recordingProgressHistoryRead, error) {
	zero := recordingProgressHistoryRead{}
	if !historySafeRecording(id) {
		return zero, errRecordingHistoryInvalid
	}
	schema, err := g.historySchema()
	if err != nil {
		return zero, err
	}
	var raw *string
	var budget, same bool
	var incarnation, operation string
	var artifact, mapping *string
	var duration, position, revision, generation int64
	var end bool
	var observed, expires time.Time
	err = g.tx.QueryRow(g.ctx, recordingHistoryReadQuery(schema), id).Scan(&raw, &budget, &same, &incarnation, &artifact, &mapping, &duration, &position, &end, &observed, &expires, &revision, &generation, &operation)
	if g.ctx.Err() != nil {
		return zero, g.ctx.Err()
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return recordingProgressHistoryRead{state: "absent"}, nil
	}
	if err != nil {
		return zero, err
	}
	if g.ctx.Err() != nil {
		return zero, g.ctx.Err()
	}
	if !budget || raw == nil || artifact == nil || mapping == nil {
		return recordingProgressHistoryRead{state: "invalid"}, nil
	}
	if !same {
		return recordingProgressHistoryRead{state: "stale"}, nil
	}
	data, err := decodeRecordingProgressHistory([]byte(*raw))
	if err != nil {
		return recordingProgressHistoryRead{state: "invalid"}, nil
	}
	e, t := data.envelope, data.tuple
	if t.recording != id || t.incarnation != incarnation || t.artifact != *artifact || e.Mapping != *mapping || t.duration != duration || e.Position != position || e.End != end || e.Revision != revision || e.Generation != generation || e.Operation != operation || !historyPGTime(data.observed).Equal(observed) || !historyPGTime(data.expires).Equal(expires) {
		return recordingProgressHistoryRead{state: "invalid"}, nil
	}
	if g.ctx.Err() != nil {
		return zero, g.ctx.Err()
	}
	return recordingProgressHistoryRead{state: "historical-data", data: data, missingStrongEvidence: e.Output == nil && (e.Tool == nil || e.Transform == nil)}, nil
}
func (g *settingsProgressTx) writeRecordingProgressHistory(data *recordingProgressHistoryData) error {
	if data == nil {
		return errRecordingHistoryInvalid
	}
	// Re-validate owned canonical bytes, so internal typed aliases cannot alter a
	// validated object into a different write. No input slice is retained.
	d, err := decodeRecordingProgressHistory(data.raw)
	if err != nil {
		return err
	}
	schema, err := g.historySchema()
	if err != nil {
		return err
	}
	p := pgx.Identifier{schema, "recording_playback_positions"}.Sanitize()
	h := pgx.Identifier{schema, "recording_progress_history"}.Sanitize()
	e, t := d.envelope, d.tuple
	args := []any{t.recording, t.incarnation, t.artifact, e.Mapping, t.duration, e.Position, e.End, historyPGTime(d.observed), historyPGTime(d.expires), e.Revision, e.Generation, e.Operation, string(d.raw)}
	// The original caller already holds 085; this reentrant comparison can never
	// acquire 084 or authorize anything. The sidecar is locked only afterwards.
	var exists bool
	err = g.tx.QueryRow(g.ctx, `SELECT true FROM `+p+` WHERE recording_id=$1 AND incarnation=$2::uuid AND artifact_id=$3 AND mapping_sha256=$4 AND duration_ms=$5 AND position_ms=$6 AND at_derived_end=$7 AND descriptor_observed_at=$8 AND descriptor_expires_at=$9 AND revision=$10 AND writer_generation=$11 AND operation_id=$12::uuid FOR UPDATE`, args[:12]...).Scan(&exists)
	if g.ctx.Err() != nil {
		return g.ctx.Err()
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return errRecordingHistoryAssociation
	}
	if err != nil {
		return err
	}
	columns := `recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,data_text`
	tag, err := g.tx.Exec(g.ctx, `INSERT INTO `+h+` (`+columns+`) VALUES($1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid,$13) ON CONFLICT(recording_id) DO UPDATE SET incarnation=EXCLUDED.incarnation,artifact_id=EXCLUDED.artifact_id,mapping_sha256=EXCLUDED.mapping_sha256,duration_ms=EXCLUDED.duration_ms,position_ms=EXCLUDED.position_ms,at_derived_end=EXCLUDED.at_derived_end,descriptor_observed_at=EXCLUDED.descriptor_observed_at,descriptor_expires_at=EXCLUDED.descriptor_expires_at,revision=EXCLUDED.revision,writer_generation=EXCLUDED.writer_generation,operation_id=EXCLUDED.operation_id,data_text=EXCLUDED.data_text`, args...)
	if err != nil {
		return err
	}
	if g.ctx.Err() != nil {
		return g.ctx.Err()
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: sidecar row count", errRecordingHistoryAssociation)
	}
	return nil
}
