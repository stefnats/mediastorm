-- +goose Up
-- Historical DATA only. This sidecar is deliberately not wired to capture or
-- backup. It cannot grant source access, create a writer or allow resume.
CREATE TABLE recording_progress_history (
 recording_id TEXT PRIMARY KEY REFERENCES recording_playback_positions(recording_id) ON DELETE CASCADE,
 incarnation UUID NOT NULL CHECK (incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
 artifact_id TEXT NOT NULL CHECK (artifact_id ~ '^[0-9a-f]{32}$'),
 mapping_sha256 TEXT NOT NULL CHECK (mapping_sha256 ~ '^[0-9a-f]{64}$'),
 duration_ms BIGINT NOT NULL CHECK (duration_ms BETWEEN 1 AND 43200000),
 position_ms BIGINT NOT NULL CHECK (position_ms BETWEEN 0 AND duration_ms),
 at_derived_end BOOLEAN NOT NULL CHECK (at_derived_end = (position_ms = duration_ms)),
 descriptor_observed_at TIMESTAMPTZ NOT NULL,
 descriptor_expires_at TIMESTAMPTZ NOT NULL CHECK (descriptor_expires_at > descriptor_observed_at AND descriptor_expires_at <= descriptor_observed_at + INTERVAL '24 hours'),
 revision BIGINT NOT NULL CHECK (revision >= 0),
 writer_generation BIGINT NOT NULL CHECK (writer_generation >= 0),
 operation_id UUID NOT NULL CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'::uuid),
 data_text TEXT NOT NULL CONSTRAINT recording_progress_history_data_budget CHECK (octet_length(data_text) BETWEEN 2 AND 65536)
);
-- No relationship to current 084 incarnation/counters: a restored historical
-- 085 payload intentionally coexists with a fresh, different 084 incarnation.

-- +goose Down
DROP TABLE recording_progress_history;
