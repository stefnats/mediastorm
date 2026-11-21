-- +goose Up
-- Ordinary restored/native rows receive identity only, never original consent.
ALTER TABLE watch_room_members ADD COLUMN member_incarnation UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid();
ALTER TABLE watch_room_account_invites ADD COLUMN invite_incarnation UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid();

-- +goose StatementBegin
CREATE FUNCTION rotate_watch_room_member_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.member_incarnation:=pg_catalog.gen_random_uuid();
 ELSIF (NEW.room_id,NEW.profile_id,NEW.joined_at) IS DISTINCT FROM (OLD.room_id,OLD.profile_id,OLD.joined_at) THEN
  NEW.member_incarnation:=pg_catalog.gen_random_uuid();
 ELSE
  NEW.member_incarnation:=OLD.member_incarnation;
 END IF;
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER watch_room_members_original_identity BEFORE INSERT OR UPDATE ON watch_room_members
 FOR EACH ROW EXECUTE FUNCTION rotate_watch_room_member_incarnation();

-- +goose StatementBegin
CREATE FUNCTION override_watch_room_member_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.member_incarnation:=pg_catalog.gen_random_uuid();
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
-- Name ordering keeps explicit assignments from restoring an old identity.
CREATE TRIGGER watch_room_members_z_explicit_identity BEFORE UPDATE OF member_incarnation ON watch_room_members
 FOR EACH ROW EXECUTE FUNCTION override_watch_room_member_incarnation();

-- +goose StatementBegin
CREATE FUNCTION rotate_watch_room_account_invite_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  NEW.invite_incarnation:=pg_catalog.gen_random_uuid();
 ELSIF (NEW.id,NEW.room_id,NEW.inviter_account_id,NEW.invitee_account_id,NEW.created_at,NEW.expires_at)
       IS DISTINCT FROM (OLD.id,OLD.room_id,OLD.inviter_account_id,OLD.invitee_account_id,OLD.created_at,OLD.expires_at) THEN
  NEW.invite_incarnation:=pg_catalog.gen_random_uuid();
 ELSE
  NEW.invite_incarnation:=OLD.invite_incarnation;
 END IF;
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER watch_room_account_invites_original_identity BEFORE INSERT OR UPDATE ON watch_room_account_invites
 FOR EACH ROW EXECUTE FUNCTION rotate_watch_room_account_invite_incarnation();

-- +goose StatementBegin
CREATE FUNCTION override_watch_room_account_invite_incarnation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.invite_incarnation:=pg_catalog.gen_random_uuid();
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER watch_room_account_invites_z_explicit_identity BEFORE UPDATE OF invite_incarnation ON watch_room_account_invites
 FOR EACH ROW EXECUTE FUNCTION override_watch_room_account_invite_incarnation();

CREATE TABLE watch_room_native_admissions (
 room_id TEXT NOT NULL REFERENCES watch_rooms(id) ON DELETE CASCADE
 CHECK(octet_length(room_id) BETWEEN 1 AND 256 AND room_id=btrim(room_id)),
 profile_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE
 CHECK(octet_length(profile_id) BETWEEN 1 AND 256 AND profile_id=btrim(profile_id)),
 version SMALLINT NOT NULL DEFAULT 1 CHECK(version=1),
 room_incarnation UUID NOT NULL CHECK(room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 profile_incarnation UUID NOT NULL CHECK(profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE
 CHECK(octet_length(account_id) BETWEEN 1 AND 256 AND account_id=btrim(account_id)),
 account_incarnation UUID NOT NULL CHECK(account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 admission_nonce UUID NOT NULL DEFAULT pg_catalog.gen_random_uuid()
 CHECK(admission_nonce<>'00000000-0000-0000-0000-000000000000'::uuid),
 admission_kind TEXT NOT NULL CHECK(admission_kind IN ('creator','household','account')),
 native_invite_receipt TEXT,
 admitted_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK(expires_at>admitted_at AND expires_at<=admitted_at+INTERVAL '24 hours'),
 origin_invite_nonce UUID,
 member_incarnation UUID,
 member_joined_at TIMESTAMPTZ,
 PRIMARY KEY(room_id,profile_id),
 CHECK((admission_kind='creator' AND native_invite_receipt IS NULL) OR (admission_kind IN ('household','account') AND native_invite_receipt IS NOT NULL AND octet_length(native_invite_receipt) BETWEEN 1 AND 256)),
 CHECK((admission_kind='account' AND origin_invite_nonce IS NOT NULL
        AND origin_invite_nonce<>'00000000-0000-0000-0000-000000000000'::uuid)
       OR (admission_kind IN ('creator','household') AND origin_invite_nonce IS NULL)),
 CHECK((member_incarnation IS NULL AND member_joined_at IS NULL)
       OR (member_incarnation IS NOT NULL AND member_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid
           AND member_joined_at IS NOT NULL))
);

-- No FK from an accepted admission to an account invitation: a subsequent
-- revoke/reissue controls new admission, not the accepted member's identity.
CREATE TABLE watch_room_account_invite_provenance (
 invite_id TEXT PRIMARY KEY REFERENCES watch_room_account_invites(id) ON DELETE CASCADE
 CHECK(octet_length(invite_id) BETWEEN 1 AND 256 AND invite_id=btrim(invite_id)),
 version SMALLINT NOT NULL DEFAULT 1 CHECK(version=1),
 invite_incarnation UUID NOT NULL CHECK(invite_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 room_incarnation UUID NOT NULL CHECK(room_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_profile_id TEXT NOT NULL CHECK(octet_length(creator_profile_id) BETWEEN 1 AND 256 AND creator_profile_id=btrim(creator_profile_id)),
 creator_profile_incarnation UUID NOT NULL CHECK(creator_profile_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 creator_account_id TEXT NOT NULL CHECK(octet_length(creator_account_id) BETWEEN 1 AND 256 AND creator_account_id=btrim(creator_account_id)),
 creator_account_incarnation UUID NOT NULL CHECK(creator_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 recipient_account_id TEXT NOT NULL CHECK(octet_length(recipient_account_id) BETWEEN 1 AND 256 AND recipient_account_id=btrim(recipient_account_id)),
 recipient_account_incarnation UUID NOT NULL CHECK(recipient_account_incarnation<>'00000000-0000-0000-0000-000000000000'::uuid),
 created_at TIMESTAMPTZ NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL CHECK(expires_at>created_at AND expires_at<=created_at+INTERVAL '24 hours')
);

-- +goose Down
DROP TABLE watch_room_account_invite_provenance;
DROP TABLE watch_room_native_admissions;
DROP TRIGGER watch_room_account_invites_z_explicit_identity ON watch_room_account_invites;
DROP TRIGGER watch_room_account_invites_original_identity ON watch_room_account_invites;
DROP FUNCTION override_watch_room_account_invite_incarnation();
DROP FUNCTION rotate_watch_room_account_invite_incarnation();
DROP TRIGGER watch_room_members_z_explicit_identity ON watch_room_members;
DROP TRIGGER watch_room_members_original_identity ON watch_room_members;
DROP FUNCTION override_watch_room_member_incarnation();
DROP FUNCTION rotate_watch_room_member_incarnation();
ALTER TABLE watch_room_account_invites DROP COLUMN invite_incarnation;
ALTER TABLE watch_room_members DROP COLUMN member_incarnation;
