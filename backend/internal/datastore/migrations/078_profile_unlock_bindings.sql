-- +goose Up
-- NULL marks grants whose original PIN/epoch verification cannot be proven.
-- There is intentionally no legacy backfill or automatic adoption.
ALTER TABLE sessions ADD COLUMN pin_unlock_lineage TEXT CHECK (pin_unlock_lineage IS NULL OR pin_unlock_lineage ~ '^[0-9a-f]{64}$');

ALTER TABLE profile_unlocks
    ADD COLUMN pin_proof_version SMALLINT,
    ADD COLUMN pin_profile_generation BIGINT,
    ADD COLUMN pin_session_generation BIGINT,
    ADD COLUMN pin_verified_hash TEXT,
    ADD COLUMN pin_binding_digest TEXT,
    ADD COLUMN pin_session_lineage TEXT,
    ADD CONSTRAINT profile_unlock_binding_shape CHECK (
        (pin_proof_version IS NULL AND pin_profile_generation IS NULL
            AND pin_session_generation IS NULL AND pin_verified_hash IS NULL
            AND pin_binding_digest IS NULL AND pin_session_lineage IS NULL)
        OR (pin_proof_version IS NOT NULL AND pin_proof_version = 1 AND pin_profile_generation >= 0
            AND pin_session_generation >= 0
            AND pin_verified_hash ~ '^[0-9a-f]{64}$'
            AND pin_binding_digest ~ '^[0-9a-f]{64}$'
            AND pin_session_lineage ~ '^[0-9a-f]{64}$' AND pin_session_lineage IS NOT NULL
            AND pin_profile_generation IS NOT NULL
            AND pin_session_generation IS NOT NULL
            AND pin_verified_hash IS NOT NULL AND pin_binding_digest IS NOT NULL
            AND pin_session_lineage IS NOT NULL AND pin_session_lineage ~ '^[0-9a-f]{64}$')
    );

-- +goose Down
ALTER TABLE sessions DROP COLUMN pin_unlock_lineage;
ALTER TABLE profile_unlocks
    DROP CONSTRAINT profile_unlock_binding_shape,
    DROP COLUMN pin_proof_version,
    DROP COLUMN pin_profile_generation,
    DROP COLUMN pin_session_generation,
    DROP COLUMN pin_verified_hash,
    DROP COLUMN pin_binding_digest,
    DROP COLUMN pin_session_lineage;
