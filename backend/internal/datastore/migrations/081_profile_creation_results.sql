-- +goose Up
-- Result evidence is independent of executable080 rows and profile cascades.
-- Lock User before result: enrolled birth invalidation follows this same order.
LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
DECLARE user_schema TEXT;
BEGIN
    SELECT n.nspname INTO user_schema FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('users') AND c.relkind='r' AND c.relpersistence='p';
    IF user_schema IS DISTINCT FROM current_schema() THEN RAISE EXCEPTION 'creation result migration must use the actual users namespace'; END IF;
END;
$$;
-- +goose StatementEnd
CREATE TABLE profile_creation_results (
    operation_id UUID PRIMARY KEY,
    target_id UUID NOT NULL UNIQUE,
    marker_id UUID NOT NULL UNIQUE,
    reference_hash TEXT NOT NULL UNIQUE CHECK (reference_hash ~ '^[0-9a-f]{64}$'),
    owner_id TEXT NOT NULL CHECK (octet_length(owner_id) BETWEEN 1 AND 256),
    protocol_version INTEGER NOT NULL CHECK (protocol_version IN (0,1)),
    outcome TEXT NOT NULL CHECK (outcome IN ('legacy_unqualified','prepared','created','not_created')),
    attempted BOOLEAN NOT NULL DEFAULT false,
    result_version BIGINT NOT NULL DEFAULT 1 CHECK (result_version > 0),
    captured_at TIMESTAMPTZ NOT NULL,
    write_until TIMESTAMPTZ NOT NULL,
    read_until TIMESTAMPTZ NOT NULL,
    birth_invalidated BOOLEAN NOT NULL DEFAULT false,
    initial_user_digest TEXT NOT NULL DEFAULT '' CHECK (initial_user_digest='' OR initial_user_digest ~ '^[0-9a-f]{64}$'),
    initial_settings_digest TEXT NOT NULL DEFAULT '' CHECK (initial_settings_digest='' OR initial_settings_digest ~ '^[0-9a-f]{64}$'),
    initial_settings_generation BIGINT NOT NULL DEFAULT 0 CHECK (initial_settings_generation >= 0),
    CHECK ((protocol_version=0)=(outcome='legacy_unqualified')),
    CHECK (NOT birth_invalidated OR outcome='created'),
    CHECK (outcome<>'created' OR attempted AND initial_user_digest<>'' AND initial_settings_digest<>''),
    CHECK (write_until>captured_at AND read_until>=write_until)
);
CREATE INDEX profile_creation_results_created_birth ON profile_creation_results(target_id) WHERE outcome='created' AND NOT birth_invalidated;
INSERT INTO profile_creation_results(operation_id,target_id,marker_id,reference_hash,owner_id,protocol_version,outcome,attempted,captured_at,write_until,read_until)
SELECT operation_id,target_id,marker_id,reference_hash,metadata->>'ownerId',0,'legacy_unqualified',state<>'reserved' AND state<>'retired_unattempted',captured_at,write_until,read_until
FROM profile_creation_operations;
-- +goose StatementBegin
CREATE FUNCTION guard_profile_creation_result() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF (NEW.operation_id,NEW.target_id,NEW.marker_id,NEW.reference_hash,NEW.owner_id,NEW.protocol_version,NEW.captured_at,NEW.write_until,NEW.read_until)
       IS DISTINCT FROM
       (OLD.operation_id,OLD.target_id,OLD.marker_id,OLD.reference_hash,OLD.owner_id,OLD.protocol_version,OLD.captured_at,OLD.write_until,OLD.read_until)
       OR NEW.result_version<>OLD.result_version+1 OR OLD.birth_invalidated AND NOT NEW.birth_invalidated
       OR OLD.attempted AND NOT NEW.attempted THEN
        RAISE EXCEPTION 'immutable creation result identity';
    END IF;
    IF NOT (
        OLD.outcome='prepared' AND NEW.outcome IN ('prepared','created','not_created') AND NOT OLD.birth_invalidated
        OR OLD.outcome='created' AND NEW.outcome='created' AND NOT OLD.birth_invalidated AND NEW.birth_invalidated
    ) THEN
        RAISE EXCEPTION 'invalid creation result transition';
    END IF;
    IF OLD.outcome<>'prepared' AND (NEW.initial_user_digest,NEW.initial_settings_digest,NEW.initial_settings_generation,NEW.attempted)
       IS DISTINCT FROM (OLD.initial_user_digest,OLD.initial_settings_digest,OLD.initial_settings_generation,OLD.attempted) THEN
        RAISE EXCEPTION 'immutable creation result projection';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER profile_creation_results_immutable BEFORE UPDATE ON profile_creation_results FOR EACH ROW EXECUTE FUNCTION guard_profile_creation_result();
-- +goose StatementBegin
CREATE FUNCTION invalidate_profile_creation_birth() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    IF TG_OP='TRUNCATE' THEN
        EXECUTE format('UPDATE %I.profile_creation_results SET birth_invalidated=true,result_version=result_version+1 WHERE outcome=''created'' AND NOT birth_invalidated',TG_TABLE_SCHEMA);
    ELSIF (TG_OP='DELETE' OR OLD.id IS DISTINCT FROM NEW.id) AND OLD.id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        EXECUTE format('UPDATE %I.profile_creation_results SET birth_invalidated=true,result_version=result_version+1 WHERE target_id=$1::uuid AND outcome=''created'' AND NOT birth_invalidated',TG_TABLE_SCHEMA) USING OLD.id;
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER users_invalidate_creation_birth AFTER DELETE OR UPDATE OF id ON users FOR EACH ROW EXECUTE FUNCTION invalidate_profile_creation_birth();
CREATE TRIGGER users_truncate_creation_birth AFTER TRUNCATE ON users FOR EACH STATEMENT EXECUTE FUNCTION invalidate_profile_creation_birth();
-- +goose Down
-- User-trigger order is User -> results; no result-first downgrade lock.
LOCK TABLE profile_creation_operations IN ACCESS EXCLUSIVE MODE;
LOCK TABLE users IN ACCESS EXCLUSIVE MODE;
LOCK TABLE profile_creation_results IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
DECLARE result_schema TEXT; user_schema TEXT; retained BOOLEAN;
BEGIN
    SELECT n.nspname INTO result_schema FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('profile_creation_results') AND c.relkind='r' AND c.relpersistence='p';
    SELECT n.nspname INTO user_schema FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('users') AND c.relkind='r' AND c.relpersistence='p';
    IF result_schema IS NULL OR user_schema IS DISTINCT FROM result_schema THEN RAISE EXCEPTION 'unsupported creation result relation'; END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.profile_creation_results)',result_schema) INTO retained;
    IF retained THEN RAISE EXCEPTION 'creation result evidence must be empty before downgrade'; END IF;
    EXECUTE format('DROP TRIGGER users_invalidate_creation_birth ON %I.users',user_schema);
    EXECUTE format('DROP TRIGGER users_truncate_creation_birth ON %I.users',user_schema);
    EXECUTE format('DROP FUNCTION %I.invalidate_profile_creation_birth()',result_schema);
    EXECUTE format('DROP TABLE %I.profile_creation_results',result_schema);
    EXECUTE format('DROP FUNCTION %I.guard_profile_creation_result()',result_schema);
END;
$$;
-- +goose StatementEnd
