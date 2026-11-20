package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	errRecordingPlaybackUnavailable = errors.New("recording playback state unavailable")
	errRecordingPlaybackChanged     = errors.New("recording playback state changed")
	errRecordingPlaybackOverflow    = errors.New("recording playback state counter exhausted")
)

// This private bookkeeping has no media, session, profile or PIN authority.
// A future authenticated writer must own the actual transaction/serializer.
// Neither helper begins, commits, retries or authenticates a transaction.
type recordingPlaybackState struct {
	recordingID, ownerID, incarnation string
	createdAt, updatedAt              time.Time
	runID, receipt                    json.RawMessage
	revision, generation              int64
	tombstone                         bool
	operationID                       *string
}

func (recordingPlaybackState) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (recordingPlaybackState) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("[recording playback bookkeeping]"))
}

type recordingPlaybackMutation uint8

const (
	recordingPlaybackClaim recordingPlaybackMutation = iota + 1
	recordingPlaybackTouch
	recordingPlaybackDelete
)

type recordingPlaybackCatalog struct {
	schema   string
	relation uint32
	receipt  json.RawMessage
}

// Exact reviewed prosrc comes from the embedded migration, not a live function.
func recordingPlaybackFunctionSource(delimiter string) (string, error) {
	raw, err := embedMigrations.ReadFile("migrations/084_recording_playback_state.sql")
	if err != nil {
		return "", errRecordingPlaybackUnavailable
	}
	parts := strings.Split(string(raw), delimiter)
	if len(parts) != 3 || parts[1] == "" {
		return "", errRecordingPlaybackUnavailable
	}
	return parts[1], nil
}

func readRecordingPlaybackCatalog(ctx context.Context, tx pgx.Tx) (recordingPlaybackCatalog, error) {
	var out recordingPlaybackCatalog
	if tx == nil || ctx == nil || ctx.Err() != nil {
		return out, errRecordingPlaybackUnavailable
	}
	if err := tx.QueryRow(ctx, `SELECT n.nspname,c.oid FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=pg_catalog.to_regclass('recordings') AND c.relname='recordings'
 AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition`).Scan(&out.schema, &out.relation); err != nil {
		return out, errRecordingPlaybackUnavailable
	}
	for _, fn := range []struct{ name, arguments, result, volatility, delimiter string }{
		{"recording_playback_observer_receipt", "26", "jsonb", "s", "$recording_receipt$"},
		{"observe_recording_playback_identity", "", "trigger", "v", "$recording_identity$"},
	} {
		expected, err := recordingPlaybackFunctionSource(fn.delimiter)
		if err != nil {
			return out, err
		}
		var actual string
		if tx.QueryRow(ctx, `SELECT p.prosrc FROM pg_catalog.pg_proc p
 JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 JOIN pg_catalog.pg_type t ON t.oid=p.prorettype
 JOIN pg_catalog.pg_namespace tn ON tn.oid=t.typnamespace
 WHERE n.nspname=$1 AND p.proname=$2 AND p.proargtypes::text=$3
 AND tn.nspname='pg_catalog' AND t.typname=$4 AND p.provolatile::text=$5
 AND l.lanname='plpgsql' AND NOT p.prosecdef AND p.proconfig IS NULL`, out.schema, fn.name, fn.arguments, fn.result, fn.volatility).Scan(&actual) != nil || actual != expected {
			return out, errRecordingPlaybackUnavailable
		}
	}
	query := "SELECT " + pgx.Identifier{out.schema, "recording_playback_observer_receipt"}.Sanitize() + "($1::oid)"
	if tx.QueryRow(ctx, query, out.relation).Scan(&out.receipt) != nil || len(out.receipt) == 0 || len(out.receipt) > 16*1024 || bytes.Equal(out.receipt, []byte("null")) || ctx.Err() != nil {
		return out, errRecordingPlaybackUnavailable
	}
	return out, nil
}

// Lock the recording BEFORE its state, matching the recording observer's order.
// This does not lock or validate authentication/household/PIN predicates.
func lockRecordingPlaybackState(ctx context.Context, tx pgx.Tx, recordingID string) (*recordingPlaybackState, error) {
	catalog, err := readRecordingPlaybackCatalog(ctx, tx)
	if err != nil {
		return nil, err
	}
	state := &recordingPlaybackState{recordingID: recordingID}
	var currentOwner string
	var currentCreated time.Time
	var currentRun json.RawMessage
	query := "SELECT user_id,created_at,capture_state->'runId' FROM " + pgx.Identifier{catalog.schema, "recordings"}.Sanitize() + " WHERE id=$1 FOR UPDATE"
	if err = tx.QueryRow(ctx, query, recordingID).Scan(&currentOwner, &currentCreated, &currentRun); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errRecordingPlaybackChanged
		}
		return nil, errRecordingPlaybackUnavailable
	}
	query = `SELECT incarnation::text,owner_user_id,recording_created_at,capture_run_id,
 observer_receipt,revision,writer_generation,tombstone,last_operation_id::text,updated_at FROM ` + pgx.Identifier{catalog.schema, "recording_playback_state"}.Sanitize() + ` WHERE recording_id=$1 FOR UPDATE`
	if tx.QueryRow(ctx, query, recordingID).Scan(&state.incarnation, &state.ownerID, &state.createdAt, &state.runID, &state.receipt, &state.revision, &state.generation, &state.tombstone, &state.operationID, &state.updatedAt) != nil || ctx.Err() != nil {
		return nil, errRecordingPlaybackUnavailable
	}
	// A row lock may have waited while an observer was changed. A fresh catalog
	// observation must still match the original receipt after both actual locks.
	finalCatalog, err := readRecordingPlaybackCatalog(ctx, tx)
	if err != nil || finalCatalog.schema != catalog.schema || finalCatalog.relation != catalog.relation || !bytes.Equal(finalCatalog.receipt, catalog.receipt) {
		return nil, errRecordingPlaybackUnavailable
	}
	identity, parseErr := uuid.Parse(state.incarnation)
	if parseErr != nil || identity == uuid.Nil || state.revision < 0 || state.generation < 0 || currentOwner != state.ownerID || !currentCreated.Equal(state.createdAt) || !bytes.Equal(currentRun, state.runID) || !bytes.Equal(catalog.receipt, state.receipt) {
		return nil, errRecordingPlaybackChanged
	}
	return state, nil
}

func sameRecordingPlaybackState(a, b *recordingPlaybackState) bool {
	return a != nil && b != nil && a.recordingID == b.recordingID && a.ownerID == b.ownerID && a.incarnation == b.incarnation && a.createdAt.Equal(b.createdAt) && bytes.Equal(a.runID, b.runID) && bytes.Equal(a.receipt, b.receipt) && a.revision == b.revision && a.generation == b.generation && a.tombstone == b.tombstone
}

func compareAndSwapRecordingPlaybackState(ctx context.Context, tx pgx.Tx, expected *recordingPlaybackState, mutation recordingPlaybackMutation) (*recordingPlaybackState, error) {
	if expected == nil || expected.revision < 0 || expected.generation < 0 || mutation < recordingPlaybackClaim || mutation > recordingPlaybackDelete {
		return nil, errRecordingPlaybackChanged
	}
	current, err := lockRecordingPlaybackState(ctx, tx, expected.recordingID)
	if err != nil {
		return nil, err
	}
	if !sameRecordingPlaybackState(current, expected) || mutation == recordingPlaybackTouch && current.tombstone {
		return nil, errRecordingPlaybackChanged
	}
	delta, tombstone := int64(1), mutation == recordingPlaybackDelete
	if mutation == recordingPlaybackTouch {
		delta, tombstone = 0, current.tombstone
	}
	if current.revision == math.MaxInt64 || delta == 1 && current.generation == math.MaxInt64 {
		return nil, errRecordingPlaybackOverflow
	}
	operation, err := uuid.NewRandom()
	if err != nil {
		return nil, errRecordingPlaybackUnavailable
	}
	catalog, err := readRecordingPlaybackCatalog(ctx, tx)
	if err != nil || !bytes.Equal(catalog.receipt, current.receipt) {
		return nil, errRecordingPlaybackUnavailable
	}
	query := "UPDATE " + pgx.Identifier{catalog.schema, "recording_playback_state"}.Sanitize() + `
 SET revision=revision+1,writer_generation=writer_generation+$6,
 tombstone=$7,last_operation_id=$8,updated_at=clock_timestamp()
 WHERE recording_id=$1 AND incarnation=$2 AND owner_user_id=$3
 AND revision=$4 AND writer_generation=$5 AND revision<9223372036854775807
 AND ($6=0 OR ($6=1 AND writer_generation<9223372036854775807))
 RETURNING revision,writer_generation,tombstone,last_operation_id::text,updated_at`
	if err = tx.QueryRow(ctx, query, current.recordingID, current.incarnation, current.ownerID, current.revision, current.generation, delta, tombstone, operation.String()).Scan(&current.revision, &current.generation, &current.tombstone, &current.operationID, &current.updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errRecordingPlaybackChanged
		}
		return nil, errRecordingPlaybackUnavailable
	}
	if ctx.Err() != nil {
		return nil, errRecordingPlaybackUnavailable
	}
	// The caller must classify Commit: nil=acknowledged, explicit rollback=known
	// rollback, other post-dispatch errors may be unknown. Never retry here.
	return current, nil
}
