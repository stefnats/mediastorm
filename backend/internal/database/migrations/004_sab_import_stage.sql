-- +goose Up
CREATE TABLE sab_import_stage_roots (
    path_hash TEXT PRIMARY KEY NOT NULL,
    namespace TEXT NOT NULL
);
CREATE TABLE sab_import_stages (
    operation_id TEXT PRIMARY KEY NOT NULL,
    queue_id INTEGER NOT NULL CHECK (queue_id > 0),
    receipt_attempt TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    namespace TEXT NOT NULL,
    expected_bytes INTEGER NOT NULL CHECK (expected_bytes >= 0),
    file_count INTEGER NOT NULL CHECK (file_count BETWEEN 1 AND 128),
    state TEXT NOT NULL CHECK (state IN ('pending', 'failed_partial', 'copied')),
    file_manifest TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (queue_id, receipt_attempt, source_hash, namespace)
);
CREATE INDEX sab_import_stage_budget ON sab_import_stages(namespace);
-- No automatic expiry or foreign-key cascade can release a charge or erase a
-- receipt-bound attempt after queue deletion. Reconciliation is a later feature.

-- +goose Down
DROP INDEX sab_import_stage_budget;
DROP TABLE sab_import_stages;
DROP TABLE sab_import_stage_roots;
