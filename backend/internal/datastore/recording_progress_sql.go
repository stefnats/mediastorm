package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
)

// Commit consumes exactly one original prepared intent before acquiring SQL.
// The configuration shared lock, auth locks, CAS and payload all belong to the
// SAME actual transaction. No provider, policy callback or second pool is used
// inside it. Unknown ACK is terminal and is never retried on another backend.
func (c *RecordingProgressCore) Commit(i *RecordingProgressIntent) (RecordingProgressResult, error) {
	var zero RecordingProgressResult
	s, ctx, finish, err := c.beginProgressOperation(i, false)
	if err != nil {
		return zero, err
	}
	defer finish()
	guard, fence, state, deadline, err := c.lockProgressTransaction(ctx, s)
	if err != nil {
		return zero, err
	}
	defer guard.Close()
	if state.revision != s.command.ExpectedRevision || state.generation != s.command.WriterGeneration {
		return zero, ErrRecordingProgressChanged
	}
	catalog, err := readRecordingPlaybackCatalog(guard.ctx, guard.tx)
	if err != nil {
		return zero, ErrRecordingProgressUnavailable
	}
	position, err := recordingProgressPosition(guard, catalog.schema, s, state)
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
	if !s.current(ctx) || guard.ctx.Err() != nil {
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
	if s.history != nil {
		// The original CAS and085 insert have succeeded on this SAME actual tx.
		// Only actual server values may fill the durable historical envelope.
		envelope := s.history.envelope
		envelope.Position, envelope.End = position, position == s.observation.DurationMS
		envelope.Revision, envelope.Generation = updated.revision, updated.generation
		envelope.Operation = *updated.operationID
		raw, e := json.Marshal(envelope)
		if e != nil {
			return zero, ErrRecordingProgressUnavailable
		}
		data, e := decodeRecordingProgressHistory(raw)
		if e != nil {
			return zero, ErrRecordingProgressUnavailable
		}
		if e = guard.writeRecordingProgressHistory(data); e != nil {
			return zero, ErrRecordingProgressUnavailable
		}
	}
	if !s.current(ctx) || guard.ctx.Err() != nil || !deadline.After(time.Now()) {
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
	return RecordingProgressResult{ValidUntil: deadline.UTC().Round(0), RecordingID: s.identity.recordingID, Incarnation: updated.incarnation, ArtifactID: s.observation.ArtifactID, MappingSHA256: s.observation.MappingSHA256, DurationMS: s.observation.DurationMS, PositionMS: position, AtDerivedEnd: position == s.observation.DurationMS, Revision: strconv.FormatInt(updated.revision, 10), WriterGeneration: strconv.FormatInt(updated.generation, 10), OperationID: *updated.operationID}, nil
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

func recordingProgressPosition(guard *settingsProgressTx, schema string, s *recordingProgressIntentState, state *recordingPlaybackState) (int64, error) {
	if s.command.Action == RecordingProgressFromStart {
		return 0, nil
	}
	payload, err := readRecordingProgressPayload(guard.ctx, guard.tx, schema, s.identity.recordingID)
	if err != nil {
		if s.command.Action == RecordingProgressResumeHistory {
			return 0, err
		}
		return 0, ErrRecordingProgressChanged
	}
	if s.command.Action == RecordingProgressResumeHistory {
		position, status, e := recordingHistoricalResumePosition(guard, s, state)
		if e != nil {
			return 0, e
		}
		if status != "compatible" {
			return 0, ErrRecordingProgressChanged
		}
		return position, nil
	}
	if !payload.matches(s, state) {
		return 0, ErrRecordingProgressChanged
	}
	if s.command.Action == RecordingProgressTouch {
		return s.command.PositionMS, nil
	}
	return payload.position, nil
}

func (s *recordingProgressIntentState) current(ctx context.Context) bool {
	return ctx.Err() == nil && s.ctx.Err() == nil && s.frame.ctx.Err() == nil && s.deadline.After(time.Now()) && s.core.valid()
}

func (c *RecordingProgressCore) beginProgressOperation(i *RecordingProgressIntent, reader bool) (*recordingProgressIntentState, context.Context, func(), error) {
	if !c.valid() || i == nil || i.state == nil || i.state.self != i || i.state.core != c || i.state.reader != reader {
		return nil, nil, nil, ErrRecordingProgressChanged
	}
	s := i.state
	s.mu.Lock()
	if s.consumed || s.closing {
		s.mu.Unlock()
		return nil, nil, nil, ErrRecordingProgressChanged
	}
	s.consumed = true
	s.operations.Add(1)
	s.mu.Unlock()
	ctx, cancel := context.WithDeadline(s.ctx, s.deadline)
	finish := func() { cancel(); s.operations.Done() }
	if !s.current(ctx) {
		finish()
		return nil, nil, nil, ErrRecordingProgressChanged
	}
	return s, ctx, finish, nil
}

// Shared original authority and lock order, independent of caller CAS data.
func (c *RecordingProgressCore) lockProgressTransaction(ctx context.Context, s *recordingProgressIntentState) (*settingsProgressTx, *config.SecurityPolicyFence, *recordingPlaybackState, time.Time, error) {
	guard, err := c.state.store.beginSettingsProgressTx(ctx, c.state.coordinator)
	if err != nil {
		return nil, nil, nil, time.Time{}, ErrRecordingProgressUnavailable
	}
	fail := func(err error) (*settingsProgressTx, *config.SecurityPolicyFence, *recordingPlaybackState, time.Time, error) {
		_ = guard.Close()
		return nil, nil, nil, time.Time{}, err
	}
	fence, err := c.state.security.BindPolicy(guard.ctx, s.policy, guard.hold)
	if err != nil {
		return fail(ErrRecordingProgressChanged)
	}
	enforce, err := fence.EnforceProfilePins()
	if err != nil {
		return fail(ErrRecordingProgressChanged)
	}
	if err = lockRecordingProgressAuthority(guard.ctx, guard.tx, s); err != nil {
		return fail(err)
	}
	frame, _, err := readPGRequestFrame(guard.ctx, guard.tx, s.original.Token)
	if err != nil || frame.session != s.original || !sameParentMutationRow(frame.sessionRow, s.frame.sessionRow) || !sameParentMutationRow(frame.actorRow, s.frame.actorRow) {
		return fail(ErrRecordingProgressChanged)
	}
	tuple, outcome, accessDeadline, err := readProfilePinAccess(guard.ctx, guard.tx, s.original, s.identity.owner, s.hashes, true)
	if err != nil || !sameParentModeTuple(tuple, s.tuple) || !progressAdult(tuple) {
		return fail(ErrRecordingProgressChanged)
	}
	switch outcome {
	case ProfilePinNoPin:
	case ProfilePinUnlocked:
	case ProfilePinLocked:
		if enforce {
			return fail(ErrRecordingProgressLocked)
		}
	default:
		return fail(ErrRecordingProgressChanged)
	}
	deadline := s.deadline
	if enforce && outcome == ProfilePinUnlocked {
		deadline = parentMutationMin(deadline, &accessDeadline)
	}
	state, err := lockRecordingPlaybackState(guard.ctx, guard.tx, s.identity.recordingID)
	if err != nil || state.ownerID != s.identity.owner || state.incarnation != s.identity.incarnation.String() || !state.createdAt.Equal(s.identity.createdAt) || !bytes.Equal(state.runID, s.identity.run) || !bytes.Equal(state.receipt, s.identity.receipt) {
		return fail(ErrRecordingProgressChanged)
	}
	return guard, fence, state, deadline, nil
}

type recordingProgressPayload struct {
	incarnation, artifact, mapping, operation string
	duration, position, revision, generation  int64
}

func readRecordingProgressPayload(ctx context.Context, tx pgx.Tx, schema, id string) (*recordingProgressPayload, error) {
	var p recordingProgressPayload
	query := `SELECT incarnation::text,artifact_id,mapping_sha256,duration_ms,position_ms,revision,writer_generation,operation_id::text FROM ` + pgx.Identifier{schema, "recording_playback_positions"}.Sanitize() + ` WHERE recording_id=$1 FOR UPDATE`
	err := tx.QueryRow(ctx, query, id).Scan(&p.incarnation, &p.artifact, &p.mapping, &p.duration, &p.position, &p.revision, &p.generation, &p.operation)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, ErrRecordingProgressUnavailable
	}
	return &p, nil
}
func (p *recordingProgressPayload) matches(s *recordingProgressIntentState, state *recordingPlaybackState) bool {
	return p != nil && !state.tombstone && state.operationID != nil && p.incarnation == state.incarnation && p.artifact == s.observation.ArtifactID && p.mapping == s.observation.MappingSHA256 && p.duration == s.observation.DurationMS && p.position >= 0 && p.position <= p.duration && p.revision == state.revision && p.generation == state.generation && p.operation == *state.operationID
}

// ReadSnapshot returns DATA after the original transaction has acknowledged
// ROLLBACK and all owned cleanup has joined. It never claims/touches progress.
// A trusted future attachment still must supply genuine current GetDerived.
func (c *RecordingProgressCore) ReadSnapshot(r *http.Request, identity RecordingTimelineIdentity, observation RecordingProgressObservation) (RecordingProgressSnapshot, error) {
	var zero RecordingProgressSnapshot
	i, err := c.prepareOriginal(r, identity, observation, nil)
	if err != nil {
		return zero, err
	}
	return c.readPreparedRecordingSnapshot(r, i)
}

// ReadSnapshotWithHistory returns candidate DATA only after the same genuine
// reader transaction acknowledges rollback and all physical cleanup joins.
func (c *RecordingProgressCore) ReadSnapshotWithHistory(r *http.Request, identity RecordingTimelineIdentity, observation RecordingProgressObservation, history RecordingProgressHistoryObservation) (RecordingProgressSnapshot, error) {
	var zero RecordingProgressSnapshot
	data, err := ownRecordingProgressHistory(identity, observation, history)
	if err != nil {
		return zero, err
	}
	i, err := c.prepareOriginal(r, identity, observation, nil)
	if err != nil {
		return zero, err
	}
	i.state.history = data
	return c.readPreparedRecordingSnapshot(r, i)
}
func (c *RecordingProgressCore) readPreparedRecordingSnapshot(r *http.Request, i *RecordingProgressIntent) (RecordingProgressSnapshot, error) {
	var zero RecordingProgressSnapshot
	defer i.Close()
	value, deadline, err := c.readSnapshotIntent(i)
	// readSnapshotIntent has ended its active operation before this Close.
	i.Close()
	if err != nil {
		return zero, err
	}
	// Own guard/intent contexts were intentionally canceled by cleanup. Only
	// ORIGINAL request/frame signals and original effective deadlines apply now.
	if r.Context().Err() != nil || i.state.frame.ctx.Err() != nil || !deadline.After(time.Now()) || !c.valid() {
		return zero, ErrRecordingProgressChanged
	}
	return value, nil
}

func (c *RecordingProgressCore) readSnapshotIntent(i *RecordingProgressIntent) (RecordingProgressSnapshot, time.Time, error) {
	var zero RecordingProgressSnapshot
	s, ctx, finish, err := c.beginProgressOperation(i, true)
	if err != nil {
		return zero, time.Time{}, err
	}
	defer finish()
	guard, fence, state, deadline, err := c.lockProgressTransaction(ctx, s)
	if err != nil {
		return zero, time.Time{}, err
	}
	defer guard.Close()
	catalog, err := readRecordingPlaybackCatalog(guard.ctx, guard.tx)
	if err != nil {
		return zero, deadline, ErrRecordingProgressUnavailable
	}
	payload, err := readRecordingProgressPayload(guard.ctx, guard.tx, catalog.schema, s.identity.recordingID)
	if err != nil {
		return zero, deadline, err
	}
	value := RecordingProgressSnapshot{ValidUntil: deadline.UTC().Round(0), RecordingID: s.identity.recordingID, Incarnation: state.incarnation, ArtifactID: s.observation.ArtifactID, MappingSHA256: s.observation.MappingSHA256, DurationMS: s.observation.DurationMS, Revision: strconv.FormatInt(state.revision, 10), WriterGeneration: strconv.FormatInt(state.generation, 10), PositionState: "none"}
	if payload != nil {
		value.PositionState = "conflict"
		if payload.matches(s, state) {
			value.PositionState = "matching"
			position := payload.position
			end := position == s.observation.DurationMS
			value.PositionMS = &position
			value.AtDerivedEnd = &end
		}
	}
	if s.history != nil {
		position, status, e := recordingHistoricalResumePosition(guard, s, state)
		if e != nil {
			return zero, deadline, e
		}
		value.HistoryState = status
		if status == "compatible" {
			end := position == s.observation.DurationMS
			value.HistoricalPositionMS = &position
			value.HistoricalAtDerivedEnd = &end
		}
	}
	var databaseNow time.Time
	if guard.tx.QueryRow(guard.ctx, `SELECT clock_timestamp()`).Scan(&databaseNow) != nil || !deadline.After(databaseNow) || !s.current(guard.ctx) {
		return zero, deadline, ErrRecordingProgressChanged
	}
	if _, err = fence.EnforceProfilePins(); err != nil {
		return zero, deadline, ErrRecordingProgressChanged
	}
	// Existing epoch locks may INSERT missing zero rows. Always roll this
	// transaction back, including success: no durable auth/CAS/payload mutation.
	// A failed/lost ACK or ErrTxClosed is not a successful read publication.
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	rollbackErr := guard.tx.Rollback(cleanup)
	cancel()
	closeErr := guard.Close()
	if rollbackErr != nil || closeErr != nil {
		return zero, deadline, ErrRecordingProgressUnavailable
	}
	return value, deadline, nil
}
