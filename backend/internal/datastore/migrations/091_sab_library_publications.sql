-- +goose Up
-- +goose StatementBegin
CREATE TABLE sab_library_publications (
 operation_id TEXT PRIMARY KEY CHECK(operation_id ~ '^[0-9a-f]{32}$'),
 stage_operation_id TEXT NOT NULL CHECK(stage_operation_id ~ '^[0-9a-f]{32}$'),
 stage_namespace TEXT NOT NULL CHECK(stage_namespace ~ '^[0-9a-f]{64}$'),
 manifest_digest TEXT NOT NULL CHECK(manifest_digest ~ '^[0-9a-f]{64}$'),
 target_library_id TEXT NOT NULL CHECK(octet_length(target_library_id) BETWEEN 1 AND 256),
 target_binding TEXT NOT NULL CHECK(target_binding ~ '^[0-9a-f]{64}$'),
 original_binding TEXT NOT NULL CHECK(original_binding ~ '^[0-9a-f]{64}$'),
 selected_ordinals JSONB NOT NULL CHECK(jsonb_typeof(selected_ordinals)='array' AND jsonb_array_length(selected_ordinals) BETWEEN 1 AND 128 AND octet_length(selected_ordinals::text)<=1024),
 expected_bytes BIGINT NOT NULL CHECK(expected_bytes BETWEEN 1 AND 68719476736),
 destination_receipt JSONB CHECK(destination_receipt IS NULL OR (jsonb_typeof(destination_receipt)='object' AND octet_length(destination_receipt::text)<=65536)),
 destination_receipt_digest TEXT CHECK(destination_receipt_digest IS NULL OR destination_receipt_digest ~ '^[0-9a-f]{64}$'),
 state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN('reserved','registered')),
 CHECK((destination_receipt IS NULL)=(destination_receipt_digest IS NULL)),
 CHECK(state<>'registered' OR destination_receipt IS NOT NULL),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(stage_namespace,stage_operation_id)
);
CREATE TABLE sab_library_publication_files (
 operation_id TEXT NOT NULL REFERENCES sab_library_publications(operation_id),
 ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 128),
 item_id TEXT NOT NULL UNIQUE CHECK(octet_length(item_id) BETWEEN 1 AND 128),
 relative_path TEXT NOT NULL CHECK(octet_length(relative_path) BETWEEN 1 AND 4096),
 size_bytes BIGINT NOT NULL CHECK(size_bytes BETWEEN 1 AND 8589934592),
 sha256 TEXT NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 PRIMARY KEY(operation_id,ordinal)
);
-- No FK to libraries/items: deletion must retain operational evidence and must
-- never turn reconciliation into implicit recreation or physical cleanup.
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP TABLE sab_library_publication_files;
DROP TABLE sab_library_publications;
-- +goose StatementEnd
