-- +goose Up
CREATE TABLE sab_library_publication_roots (
 target_binding TEXT PRIMARY KEY NOT NULL CHECK(length(target_binding)=64 AND target_binding NOT GLOB '*[^0-9a-f]*'),
 owner_nonce TEXT UNIQUE NOT NULL CHECK(length(owner_nonce)=32 AND owner_nonce NOT GLOB '*[^0-9a-f]*'),
 state TEXT NOT NULL CHECK(state IN('pending','owned','reconciliation_required')),
 root_receipt TEXT CHECK(root_receipt IS NULL OR (length(CAST(root_receipt AS BLOB))<=65536 AND json_valid(root_receipt) AND json_type(root_receipt)='object')),
 root_receipt_digest TEXT CHECK(root_receipt_digest IS NULL OR (length(root_receipt_digest)=64 AND root_receipt_digest NOT GLOB '*[^0-9a-f]*')),
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 CHECK((root_receipt IS NULL)=(root_receipt_digest IS NULL)),
 CHECK(state<>'owned' OR root_receipt IS NOT NULL)
);
-- The root receipt must retain the original exclusive marker nonce and actual
-- physical directory/marker identity. Same name/hash is never adoption proof.
CREATE TABLE sab_library_publications (
 operation_id TEXT PRIMARY KEY NOT NULL CHECK(length(operation_id)=32),
 stage_operation_id TEXT UNIQUE NOT NULL CHECK(length(stage_operation_id)=32),
 queue_id INTEGER NOT NULL CHECK(queue_id>0),
 stage_namespace TEXT NOT NULL CHECK(length(stage_namespace)=64),
 manifest_digest TEXT NOT NULL CHECK(length(manifest_digest)=64),
 selected_ordinals TEXT NOT NULL CHECK(length(selected_ordinals)<=1024),
 target_library_id TEXT NOT NULL CHECK(length(target_library_id) BETWEEN 1 AND 256),
 target_binding TEXT NOT NULL CHECK(length(target_binding)=64),
 expected_bytes INTEGER NOT NULL CHECK(expected_bytes BETWEEN 1 AND 68719476736),
 state TEXT NOT NULL CHECK(state IN('claimed','files_prepared','files_durable','registration_unknown','registered','reconciliation_required')),
 prepared_receipt TEXT CHECK(prepared_receipt IS NULL OR (length(CAST(prepared_receipt AS BLOB))<=65536 AND json_valid(prepared_receipt) AND json_type(prepared_receipt)='object')),
 prepared_receipt_digest TEXT CHECK(prepared_receipt_digest IS NULL OR (length(prepared_receipt_digest)=64 AND prepared_receipt_digest NOT GLOB '*[^0-9a-f]*')),
 durable_receipt TEXT CHECK(durable_receipt IS NULL OR (length(CAST(durable_receipt AS BLOB))<=65536 AND json_valid(durable_receipt) AND json_type(durable_receipt)='object')),
 durable_receipt_digest TEXT CHECK(durable_receipt_digest IS NULL OR (length(durable_receipt_digest)=64 AND durable_receipt_digest NOT GLOB '*[^0-9a-f]*')),
 revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
 created_at TEXT NOT NULL,
 updated_at TEXT NOT NULL,
 CHECK((prepared_receipt IS NULL)=(prepared_receipt_digest IS NULL)),
 CHECK((durable_receipt IS NULL)=(durable_receipt_digest IS NULL)),
 CHECK(state<>'files_prepared' OR prepared_receipt IS NOT NULL),
 CHECK(state NOT IN('files_durable','registration_unknown','registered') OR (prepared_receipt IS NOT NULL AND durable_receipt IS NOT NULL))
);
CREATE INDEX sab_library_publication_budget ON sab_library_publications(target_binding);
-- This operational journal deliberately has no cascading foreign key, expiry,
-- delete API or byte refund. A lost acknowledgement retains the reservation.
-- +goose Down
DROP INDEX sab_library_publication_budget;
DROP TABLE sab_library_publications;
DROP TABLE sab_library_publication_roots;
