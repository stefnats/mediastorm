-- +goose Up
ALTER TABLE share_links
 ADD COLUMN provenance_version SMALLINT,
 ADD COLUMN source_profile_id TEXT,
 ADD COLUMN source_account_id TEXT;

-- Existing JSON profile tags cannot attest an original authenticated creator.
-- Only the concrete current-request writer supplies this complete new tuple.
ALTER TABLE share_links ADD CONSTRAINT share_link_provenance_shape CHECK (
 (provenance_version IS NULL AND source_profile_id IS NULL AND source_account_id IS NULL)
 OR (provenance_version IS NOT NULL AND provenance_version=1
     AND source_profile_id IS NOT NULL AND octet_length(source_profile_id) BETWEEN 1 AND 256
     AND source_profile_id=btrim(source_profile_id)
     AND source_account_id IS NOT NULL AND octet_length(source_account_id) BETWEEN 1 AND 256
     AND source_account_id=btrim(source_account_id))
);

-- +goose Down
ALTER TABLE share_links DROP CONSTRAINT share_link_provenance_shape;
ALTER TABLE share_links DROP COLUMN provenance_version,
 DROP COLUMN source_profile_id, DROP COLUMN source_account_id;
