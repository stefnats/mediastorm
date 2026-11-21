-- +goose Up
-- Private row identity is not original creation consent. Legacy rooms receive
-- no provenance row; neither live IDs nor ordinary JSON can attest a room.
ALTER TABLE watch_rooms ADD COLUMN room_incarnation UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid();

-- +goose StatementBegin
CREATE FUNCTION rotate_watch_room_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.room_incarnation:=pg_catalog.gen_random_uuid();
 ELSIF (NEW.id,NEW.creator_profile_id,NEW.created_at,NEW.expires_at)
       IS DISTINCT FROM (OLD.id,OLD.creator_profile_id,OLD.created_at,OLD.expires_at)
       OR NEW.room_incarnation IS DISTINCT FROM OLD.room_incarnation THEN
  NEW.room_incarnation:=pg_catalog.gen_random_uuid();
 ELSE
  NEW.room_incarnation:=OLD.room_incarnation;
 END IF;
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER watch_rooms_original_incarnation BEFORE INSERT OR UPDATE ON watch_rooms
 FOR EACH ROW EXECUTE FUNCTION rotate_watch_room_incarnation();

CREATE TABLE watch_room_provenance (
 room_id TEXT PRIMARY KEY REFERENCES watch_rooms(id) ON DELETE CASCADE
 CHECK (octet_length(room_id) BETWEEN 1 AND 256 AND room_id=btrim(room_id)),
 version SMALLINT NOT NULL DEFAULT 1 CHECK (version=1),
 room_incarnation UUID NOT NULL CHECK (room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_profile_id TEXT NOT NULL CHECK (octet_length(creator_profile_id) BETWEEN 1 AND 256 AND creator_profile_id=btrim(creator_profile_id)),
 creator_profile_incarnation UUID NOT NULL CHECK (creator_profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_account_id TEXT NOT NULL CHECK (octet_length(creator_account_id) BETWEEN 1 AND 256 AND creator_account_id=btrim(creator_account_id)),
 creator_account_incarnation UUID NOT NULL CHECK (creator_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 created_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at>created_at AND expires_at<=created_at+INTERVAL '24 hours')
);

-- The foreign key follows deletion only. A changed original room nonce cannot
-- cascade into a new attestation; every consumer must compare the actual nonce.
-- Invite provenance is admission-only and has no guest/session foreign keys.
CREATE TABLE watch_room_invite_provenance (
 room_id TEXT PRIMARY KEY REFERENCES watch_room_external_invites(room_id) ON DELETE CASCADE
 CHECK (octet_length(room_id) BETWEEN 1 AND 256 AND room_id=btrim(room_id)),
 version SMALLINT NOT NULL DEFAULT 1 CHECK (version=1),
 room_incarnation UUID NOT NULL CHECK (room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 admission_nonce UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid()
 CHECK (admission_nonce<>'00000000-0000-0000-0000-000000000000'::uuid),
 token_hash TEXT NOT NULL CHECK (octet_length(token_hash)=43 AND token_hash ~ '^[A-Za-z0-9_-]{43}$'),
 short_code TEXT NOT NULL CHECK (short_code ~ '^[A-Z2-7]{4}-[A-Z2-7]{4}$'),
 admission_sha256 TEXT NOT NULL CHECK (admission_sha256 ~ '^[0-9a-f]{64}$'),
 physical_receipt TEXT NOT NULL CHECK (octet_length(physical_receipt) BETWEEN 1 AND 256),
 created_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at>created_at AND expires_at<=created_at+INTERVAL '24 hours')
);

CREATE TABLE watch_room_source_provenance (
 room_id TEXT PRIMARY KEY REFERENCES watch_room_external_sources(room_id) ON DELETE CASCADE
 CHECK (octet_length(room_id) BETWEEN 1 AND 256 AND room_id=btrim(room_id)),
 version SMALLINT NOT NULL DEFAULT 1 CHECK (version=1),
 room_incarnation UUID NOT NULL CHECK (room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_nonce UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid()
 CHECK (source_nonce<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_profile_id TEXT NOT NULL CHECK (octet_length(source_profile_id) BETWEEN 1 AND 256 AND source_profile_id=btrim(source_profile_id)),
 source_profile_incarnation UUID NOT NULL CHECK (source_profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_account_id TEXT NOT NULL CHECK (octet_length(source_account_id) BETWEEN 1 AND 256 AND source_account_id=btrim(source_account_id)),
 source_account_incarnation UUID NOT NULL CHECK (source_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 resource TEXT NOT NULL CHECK (octet_length(resource) BETWEEN 1 AND 1048576 AND resource=btrim(resource)),
 params_sha256 TEXT NOT NULL CHECK (params_sha256 ~ '^[0-9a-f]{64}$'),
 source_sha256 TEXT NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
 physical_receipt TEXT NOT NULL CHECK (octet_length(physical_receipt) BETWEEN 1 AND 256),
 bound_at TIMESTAMPTZ NOT NULL
);

-- +goose Down
DROP TABLE watch_room_source_provenance;
DROP TABLE watch_room_invite_provenance;
DROP TABLE watch_room_provenance;
DROP TRIGGER watch_rooms_original_incarnation ON watch_rooms;
DROP FUNCTION rotate_watch_room_incarnation();
ALTER TABLE watch_rooms DROP COLUMN room_incarnation;
