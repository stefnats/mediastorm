-- +goose Up
-- Persistent revocation generations fence future prepared PIN attempts even
-- when a revoke finds no grants. These rows must not be expired or pruned.
CREATE TABLE profile_unlock_revocations (
    scope TEXT NOT NULL CHECK (scope IN ('profile', 'session')),
    scope_key TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation >= 0),
    PRIMARY KEY (scope, scope_key),
    CHECK ((scope = 'profile' AND length(scope_key) BETWEEN 1 AND 256
            AND scope_key = btrim(scope_key))
        OR (scope = 'session' AND scope_key ~ '^[0-9a-f]{64}$'))
);

-- +goose Down
-- Downgrading discards revocation history; no prepared state may survive it.
DROP TABLE profile_unlock_revocations;
