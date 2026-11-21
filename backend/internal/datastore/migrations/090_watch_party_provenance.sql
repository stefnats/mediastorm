-- +goose Up
-- Legacy membership identity alone is not an original admission. No private
-- binding is backfilled from live IDs, ordinary Params or session models.
ALTER TABLE watch_room_guest_members ADD COLUMN guest_incarnation UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid();

-- +goose StatementBegin
CREATE FUNCTION rotate_watch_party_guest_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.guest_incarnation:=pg_catalog.gen_random_uuid();
 ELSIF (NEW.room_id,NEW.guest_id,NEW.joined_at) IS DISTINCT FROM (OLD.room_id,OLD.guest_id,OLD.joined_at) OR
    NEW.guest_incarnation IS DISTINCT FROM OLD.guest_incarnation THEN
  NEW.guest_incarnation:=pg_catalog.gen_random_uuid();
 ELSE
  NEW.guest_incarnation:=OLD.guest_incarnation;
 END IF;
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER watch_party_guest_original_incarnation BEFORE INSERT OR UPDATE ON watch_room_guest_members
 FOR EACH ROW EXECUTE FUNCTION rotate_watch_party_guest_incarnation();

ALTER TABLE sessions ADD COLUMN watch_party_guest_binding_id UUID,
 ADD COLUMN watch_party_playback_binding_id UUID,
 ADD CONSTRAINT sessions_watch_party_guest_identity UNIQUE(token,watch_party_guest_binding_id),
 ADD CONSTRAINT sessions_watch_party_playback_identity UNIQUE(token,watch_party_playback_binding_id),
 ADD CONSTRAINT sessions_scoped_private_purpose CHECK (
  pg_catalog.num_nonnulls(share_binding_id,watch_party_guest_binding_id,watch_party_playback_binding_id)<=1 AND
  (watch_party_guest_binding_id IS NULL OR (scope='watch_party' AND NOT is_master)) AND
  (watch_party_playback_binding_id IS NULL OR (scope='stream' AND NOT is_master))
 );

CREATE TABLE watch_party_guest_bindings (
 session_token TEXT PRIMARY KEY CHECK (octet_length(session_token)=43 AND session_token ~ '^[A-Za-z0-9_-]{43}$'),
 binding_id UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid() UNIQUE
  CHECK (binding_id<>'00000000-0000-0000-0000-000000000000'::uuid),
 version SMALLINT NOT NULL DEFAULT 1 CHECK(version=1),
 room_id TEXT NOT NULL CHECK (octet_length(room_id) BETWEEN 1 AND 256 AND room_id=btrim(room_id)),
 room_incarnation UUID NOT NULL CHECK (room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 guest_id TEXT NOT NULL CHECK (octet_length(guest_id) BETWEEN 1 AND 256 AND guest_id=btrim(guest_id)),
 guest_incarnation UUID NOT NULL CHECK (guest_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_profile_id TEXT NOT NULL CHECK (octet_length(creator_profile_id) BETWEEN 1 AND 256 AND creator_profile_id=btrim(creator_profile_id)),
 creator_profile_incarnation UUID NOT NULL CHECK (creator_profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_account_id TEXT NOT NULL CHECK (octet_length(creator_account_id) BETWEEN 1 AND 256 AND creator_account_id=btrim(creator_account_id)),
 creator_account_incarnation UUID NOT NULL CHECK (creator_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 -- Audit of original admission only; invitation rotation/revoke never owns
 -- already joined guest identity or its fixed session lifetime.
 admission_nonce UUID NOT NULL CHECK (admission_nonce<>'00000000-0000-0000-0000-000000000000'::uuid),
 admission_sha256 TEXT NOT NULL CHECK(admission_sha256 ~ '^[0-9a-f]{64}$'),
 created_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK(expires_at>created_at AND expires_at<=created_at+INTERVAL '24 hours'),
 UNIQUE(session_token,binding_id),
 FOREIGN KEY(session_token,binding_id) REFERENCES sessions(token,watch_party_guest_binding_id) ON DELETE CASCADE,
 FOREIGN KEY(room_id,guest_id) REFERENCES watch_room_guest_members(room_id,guest_id) ON DELETE CASCADE,
 FOREIGN KEY(room_id) REFERENCES watch_room_provenance(room_id) ON DELETE CASCADE
);
CREATE INDEX watch_party_guest_bindings_room ON watch_party_guest_bindings(room_id,guest_id);

CREATE TABLE watch_party_playback_bindings (
 session_token TEXT PRIMARY KEY CHECK(octet_length(session_token)=43 AND session_token ~ '^[A-Za-z0-9_-]{43}$'),
 binding_id UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid()
  CHECK(binding_id<>'00000000-0000-0000-0000-000000000000'::uuid),
 version SMALLINT NOT NULL DEFAULT 1 CHECK(version=1),
 guest_session_token TEXT NOT NULL CHECK(octet_length(guest_session_token)=43 AND guest_session_token ~ '^[A-Za-z0-9_-]{43}$'),
 guest_binding_id UUID NOT NULL CHECK(guest_binding_id<>'00000000-0000-0000-0000-000000000000'::uuid),
 room_id TEXT NOT NULL CHECK(octet_length(room_id) BETWEEN 1 AND 256 AND room_id=btrim(room_id)),
 room_incarnation UUID NOT NULL CHECK(room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 guest_id TEXT NOT NULL CHECK(octet_length(guest_id) BETWEEN 1 AND 256 AND guest_id=btrim(guest_id)),
 guest_incarnation UUID NOT NULL CHECK(guest_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_profile_id TEXT NOT NULL CHECK(octet_length(creator_profile_id) BETWEEN 1 AND 256 AND creator_profile_id=btrim(creator_profile_id)),
 creator_profile_incarnation UUID NOT NULL CHECK(creator_profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_account_id TEXT NOT NULL CHECK(octet_length(creator_account_id) BETWEEN 1 AND 256 AND creator_account_id=btrim(creator_account_id)),
 creator_account_incarnation UUID NOT NULL CHECK(creator_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_nonce UUID NOT NULL CHECK(source_nonce<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_physical TEXT NOT NULL CHECK(octet_length(source_physical) BETWEEN 1 AND 256),
 source_profile_id TEXT NOT NULL CHECK(octet_length(source_profile_id) BETWEEN 1 AND 256 AND source_profile_id=btrim(source_profile_id)),
 source_profile_incarnation UUID NOT NULL CHECK(source_profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 source_account_id TEXT NOT NULL CHECK(octet_length(source_account_id) BETWEEN 1 AND 256 AND source_account_id=btrim(source_account_id)),
 source_account_incarnation UUID NOT NULL CHECK(source_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 resource TEXT NOT NULL CHECK(octet_length(resource) BETWEEN 1 AND 1048576 AND resource=btrim(resource)),
 params_sha256 TEXT NOT NULL CHECK(params_sha256 ~ '^[0-9a-f]{64}$'),
 profile_generation BIGINT NOT NULL CHECK(profile_generation>=0),
 created_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK(expires_at>created_at AND expires_at<=created_at+INTERVAL '6 hours'),
 FOREIGN KEY(session_token,binding_id) REFERENCES sessions(token,watch_party_playback_binding_id) ON DELETE CASCADE,
 FOREIGN KEY(guest_session_token,guest_binding_id) REFERENCES watch_party_guest_bindings(session_token,binding_id) ON DELETE CASCADE,
 FOREIGN KEY(room_id) REFERENCES watch_room_source_provenance(room_id) ON DELETE CASCADE
);
CREATE INDEX watch_party_playback_bindings_guest ON watch_party_playback_bindings(guest_binding_id,expires_at);

-- +goose Down
DROP TABLE watch_party_playback_bindings;
DROP TABLE watch_party_guest_bindings;
ALTER TABLE sessions DROP CONSTRAINT sessions_scoped_private_purpose,
 DROP CONSTRAINT sessions_watch_party_playback_identity,
 DROP CONSTRAINT sessions_watch_party_guest_identity,
 DROP COLUMN watch_party_playback_binding_id,
 DROP COLUMN watch_party_guest_binding_id;
DROP TRIGGER watch_party_guest_original_incarnation ON watch_room_guest_members;
DROP FUNCTION rotate_watch_party_guest_incarnation();
ALTER TABLE watch_room_guest_members DROP COLUMN guest_incarnation;
