-- +goose Up
-- Durable DATA only. No imported row grants session, profile or media access.
-- The current descriptor/identity and original authorization are checked by a
-- separate concrete writer. Historical descriptor expiry does not delete data.
CREATE TABLE recording_playback_positions (
 recording_id TEXT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
 incarnation UUID NOT NULL,
 artifact_id TEXT NOT NULL CHECK (artifact_id ~ '^[0-9a-f]{32}$'),
 mapping_sha256 TEXT NOT NULL CHECK (mapping_sha256 ~ '^[0-9a-f]{64}$'),
 duration_ms BIGINT NOT NULL CHECK (duration_ms BETWEEN 1 AND 43200000),
 position_ms BIGINT NOT NULL CHECK (position_ms >= 0 AND position_ms <= duration_ms),
 at_derived_end BOOLEAN NOT NULL CHECK (at_derived_end = (position_ms = duration_ms)),
 descriptor_observed_at TIMESTAMPTZ NOT NULL,
 descriptor_expires_at TIMESTAMPTZ NOT NULL CHECK (descriptor_expires_at > descriptor_observed_at AND descriptor_expires_at <= descriptor_observed_at + INTERVAL '24 hours'),
 revision BIGINT NOT NULL CHECK (revision >= 0),
 writer_generation BIGINT NOT NULL CHECK (writer_generation >= 0),
 operation_id UUID NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
-- No incarnation FK: rotating 084 must not fail or reactivate old positions.

-- +goose Down
DROP TABLE recording_playback_positions;
