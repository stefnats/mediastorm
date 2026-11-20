package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Commit consumes exactly one original prepared intent before acquiring SQL.
// The configuration shared lock, auth locks, CAS and payload all belong to the
// SAME actual transaction. No provider, policy callback or second pool is used
// inside it. Unknown ACK is terminal and is never retried on another backend.
func (c *RecordingProgressCore) Commit(i *RecordingProgressIntent) (RecordingProgressResult, error) {
	var zero RecordingProgressResult
	if !c.valid() || i == nil || i.state == nil || i.state.self != i || i.state.core != c {
		return zero, ErrRecordingProgressChanged
	}
	s := i.state
	s.mu.Lock()
	if s.consumed || s.closing {
		s.mu.Unlock()
		return zero, ErrRecordingProgressChanged
	}
	s.consumed = true
	s.operations.Add(1)
	s.mu.Unlock()
	defer s.operations.Done()
	ctx, cancel := context.WithDeadline(s.ctx, s.deadline)
	defer cancel()
	current := func() bool {
		return ctx.Err() == nil && s.ctx.Err() == nil && s.frame.ctx.Err() == nil && s.deadline.After(time.Now()) && c.valid()
	}
	if !current() {
		return zero, ErrRecordingProgressChanged
	}
	guard, err := c.state.store.beginSettingsProgressTx(ctx, c.state.coordinator)
	if err != nil {
		return zero, ErrRecordingProgressUnavailable
	}
	defer guard.Close()
	fence, err := c.state.security.BindPolicy(guard.ctx, s.policy, guard.hold)
	if err != nil {
		return zero, ErrRecordingProgressChanged
	}
	enforce, err := fence.EnforceProfilePins()
	if err != nil {
		return zero, ErrRecordingProgressChanged
	}
	if err = lockRecordingProgressAuthority(guard.ctx, guard.tx, s); err != nil {
		return zero, err
	}
	frame, _, err := readPGRequestFrame(guard.ctx, guard.tx, s.original.Token)
	if err != nil || frame.session != s.original || !sameParentMutationRow(frame.sessionRow, s.frame.sessionRow) || !sameParentMutationRow(frame.actorRow, s.frame.actorRow) {
		return zero, ErrRecordingProgressChanged
	}
	tuple, outcome, accessDeadline, err := readProfilePinAccess(guard.ctx, guard.tx, s.original, s.identity.owner, s.hashes, true)
	if err != nil || !sameParentModeTuple(tuple, s.tuple) || !progressAdult(tuple) {
		return zero, ErrRecordingProgressChanged
	}
	switch outcome {
	case ProfilePinNoPin:
	case ProfilePinUnlocked:
	case ProfilePinLocked:
		if enforce {
			return zero, ErrRecordingProgressLocked
		}
	default:
		return zero, ErrRecordingProgressChanged
	}
	deadline := s.deadline
	if enforce && outcome == ProfilePinUnlocked {
		deadline = parentMutationMin(deadline, &accessDeadline)
	}
	state, err := lockRecordingPlaybackState(guard.ctx, guard.tx, s.identity.recordingID)
	if err != nil || state.ownerID != s.identity.owner || state.incarnation != s.identity.incarnation.String() || !state.createdAt.Equal(s.identity.createdAt) || !bytes.Equal(state.runID, s.identity.run) || !bytes.Equal(state.receipt, s.identity.receipt) || state.revision != s.command.ExpectedRevision || state.generation != s.command.WriterGeneration {
		return zero, ErrRecordingProgressChanged
	}
	catalog, err := readRecordingPlaybackCatalog(guard.ctx, guard.tx)
	if err != nil {
		return zero, ErrRecordingProgressUnavailable
	}
	position, err := recordingProgressPosition(guard.ctx, guard.tx, catalog.schema, s, state)
	if err != nil {
		return zero, err
	}
	action := recordingPlaybackClaim
	if s.command.Action == RecordingProgressTouch {
		action = recordingPlaybackTouch
	}
	updated, err := compareAndSwapRecordingPlaybackState(guard.ctx, guard.tx, state, action)
	if err != nil {
		return zero, ErrRecordingProgressChanged
	}
	if !current() || guard.ctx.Err() != nil {
		return zero, ErrRecordingProgressChanged
	}
	if _, err = fence.EnforceProfilePins(); err != nil {
		return zero, ErrRecordingProgressChanged
	}
	query := `INSERT INTO ` + pgx.Identifier{catalog.schema, "recording_playback_positions"}.Sanitize() + `
 (recording_id,incarnation,artifact_id,mapping_sha256,duration_ms,position_ms,at_derived_end,
 descriptor_observed_at,descriptor_expires_at,revision,writer_generation,operation_id,updated_at)
 SELECT $1,$2::uuid,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::uuid,clock_timestamp()
 WHERE clock_timestamp()<$13
 ON CONFLICT(recording_id) DO UPDATE SET
 incarnation=EXCLUDED.incarnation,artifact_id=EXCLUDED.artifact_id,mapping_sha256=EXCLUDED.mapping_sha256,
 duration_ms=EXCLUDED.duration_ms,position_ms=EXCLUDED.position_ms,at_derived_end=EXCLUDED.at_derived_end,
 descriptor_observed_at=EXCLUDED.descriptor_observed_at,descriptor_expires_at=EXCLUDED.descriptor_expires_at,
 revision=EXCLUDED.revision,writer_generation=EXCLUDED.writer_generation,operation_id=EXCLUDED.operation_id,updated_at=EXCLUDED.updated_at`
	tag, err := guard.tx.Exec(guard.ctx, query, s.identity.recordingID, updated.incarnation, s.observation.ArtifactID, s.observation.MappingSHA256, s.observation.DurationMS, position, position == s.observation.DurationMS, s.observation.ObservedAt, s.observation.ExpiresAt, updated.revision, updated.generation, *updated.operationID, deadline)
	if err != nil || tag.RowsAffected() != 1 {
		return zero, ErrRecordingProgressUnavailable
	}
	if !current() || guard.ctx.Err() != nil || !deadline.After(time.Now()) {
		return zero, ErrRecordingProgressChanged
	}
	if _, err = fence.EnforceProfilePins(); err != nil {
		return zero, ErrRecordingProgressChanged
	}
	if err = guard.tx.Commit(guard.ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return zero, ErrRecordingProgressUnavailable
		}
		return zero, ErrRecordingProgressUnknown
	}
	return RecordingProgressResult{RecordingID: s.identity.recordingID, Incarnation: updated.incarnation, ArtifactID: s.observation.ArtifactID, MappingSHA256: s.observation.MappingSHA256, DurationMS: s.observation.DurationMS, PositionMS: position, AtDerivedEnd: position == s.observation.DurationMS, Revision: strconv.FormatInt(updated.revision, 10), WriterGeneration: strconv.FormatInt(updated.generation, 10), OperationID: *updated.operationID}, nil
}

func lockRecordingProgressAuthority(ctx context.Context, tx pgx.Tx, s *recordingProgressIntentState) error {
	for _, epoch := range []struct {
		scope, key string
		generation int64
	}{
		{"profile", s.identity.owner, s.tuple.profileGeneration},
		{"session", pinSessionLineage(s.tuple.session.raw, s.original.Token), s.tuple.sessionGeneration},
	} {
		value, err := lockPinEpoch(ctx, tx, epoch.scope, epoch.key)
		if err != nil || value != epoch.generation {
			return ErrRecordingProgressChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	// Strict ownership was established by the original access reader. Decode
	// only the actual owner row; the recording never accepts a viewer selector.
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil || owner.ID == "" {
		return ErrRecordingProgressChanged
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found) != nil {
			return ErrRecordingProgressChanged
		}
	}
	var found string
	if tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found) != nil {
		return ErrRecordingProgressChanged
	}
	if tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.identity.owner).Scan(&found) != nil {
		return ErrRecordingProgressChanged
	}
	// Epoch-aware issuance/revocation cannot pass the locks above. These
	// additional locks cover originally presented legacy single-row mutations.
	rows, err := tx.Query(ctx, `SELECT token_hash FROM profile_unlocks WHERE token_hash=ANY($1::text[]) AND profile_id=$2 AND account_id=$3 AND session_hash=$4 ORDER BY token_hash FOR UPDATE`, s.hashes, s.identity.owner, s.original.AccountID, profileUnlockDigest(s.original.Token))
	if err != nil {
		return ErrRecordingProgressChanged
	}
	for rows.Next() {
		if rows.Scan(&found) != nil {
			rows.Close()
			return ErrRecordingProgressChanged
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrRecordingProgressChanged
	}
	return nil
}

func recordingProgressPosition(ctx context.Context, tx pgx.Tx, schema string, s *recordingProgressIntentState, state *recordingPlaybackState) (int64, error) {
	if s.command.Action == RecordingProgressFromStart {
		return 0, nil
	}
	var incarnation, artifact, mapping, operation string
	var duration, position, revision, generation int64
	// Historical descriptor expiry is deliberately not a durable-position TTL.
	// A NEW intent already carries a currently validated exact observation.
	query := `SELECT incarnation::text,artifact_id,mapping_sha256,duration_ms,position_ms,revision,writer_generation,operation_id::text FROM ` + pgx.Identifier{schema, "recording_playback_positions"}.Sanitize() + ` WHERE recording_id=$1 FOR UPDATE`
	if tx.QueryRow(ctx, query, s.identity.recordingID).Scan(&incarnation, &artifact, &mapping, &duration, &position, &revision, &generation, &operation) != nil || state.tombstone || state.operationID == nil || incarnation != state.incarnation || artifact != s.observation.ArtifactID || mapping != s.observation.MappingSHA256 || duration != s.observation.DurationMS || position < 0 || position > duration || revision != state.revision || generation != state.generation || operation != *state.operationID {
		return 0, ErrRecordingProgressChanged
	}
	if s.command.Action == RecordingProgressTouch {
		return s.command.PositionMS, nil
	}
	return position, nil
}
