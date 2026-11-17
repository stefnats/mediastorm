-- +goose Up
-- Storage-only operation provenance. No user/account/session foreign keys:
-- deletion of those objects must not erase an unresolved operation's evidence.
CREATE TABLE profile_creation_operations (
    operation_id UUID PRIMARY KEY,
    target_id UUID NOT NULL UNIQUE,
    marker_id UUID NOT NULL UNIQUE,
    reference_hash TEXT NOT NULL UNIQUE CHECK (reference_hash ~ '^[0-9a-f]{64}$'),
    actor_id TEXT NOT NULL CHECK (octet_length(actor_id) BETWEEN 1 AND 256),
    metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata) = 'object' AND octet_length(metadata::text) <= 65536),
    charged_bytes INTEGER NOT NULL CHECK (charged_bytes BETWEEN 1 AND 65536),
    captured_at TIMESTAMPTZ NOT NULL,
    write_until TIMESTAMPTZ NOT NULL,
    read_until TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved','attempting','unknown','terminal','retired_unattempted')),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    settled_at TIMESTAMPTZ,
    CHECK (write_until > captured_at AND write_until <= captured_at + INTERVAL '30 seconds'),
    CHECK (read_until >= write_until AND read_until <= captured_at + INTERVAL '5 minutes'),
    CHECK ((state IN ('terminal','retired_unattempted')) = (settled_at IS NOT NULL)),
    CHECK (settled_at IS NULL OR settled_at >= captured_at)
);
CREATE INDEX profile_creation_operations_actor_active
    ON profile_creation_operations(actor_id) WHERE state IN ('reserved','attempting','unknown');
CREATE INDEX profile_creation_operations_terminal
    ON profile_creation_operations(read_until,operation_id) WHERE state IN ('terminal','retired_unattempted');

-- +goose StatementBegin
CREATE FUNCTION guard_profile_creation_operation() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF (NEW.operation_id,NEW.target_id,NEW.marker_id,NEW.reference_hash,NEW.actor_id,
        NEW.metadata,NEW.charged_bytes,NEW.captured_at,NEW.write_until,NEW.read_until)
       IS DISTINCT FROM
       (OLD.operation_id,OLD.target_id,OLD.marker_id,OLD.reference_hash,OLD.actor_id,
        OLD.metadata,OLD.charged_bytes,OLD.captured_at,OLD.write_until,OLD.read_until) THEN
        RAISE EXCEPTION 'immutable creation operation metadata';
    END IF;
    IF NEW.version <> OLD.version + 1 OR NOT (
        (OLD.state='reserved' AND NEW.state IN ('attempting','retired_unattempted')) OR
        (OLD.state='attempting' AND NEW.state IN ('unknown','terminal')) OR
        (OLD.state='unknown' AND NEW.state='terminal')
    ) THEN
        RAISE EXCEPTION 'invalid creation operation transition';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER profile_creation_operations_immutable
    BEFORE UPDATE ON profile_creation_operations
    FOR EACH ROW EXECUTE FUNCTION guard_profile_creation_operation();

-- +goose Down
-- Stabilize actual resolution before any row test or schema-specific drop.
LOCK TABLE profile_creation_operations IN ACCESS EXCLUSIVE MODE;
-- +goose StatementBegin
DO $$
DECLARE
    ledger_schema TEXT;
    ledger_name TEXT;
    retained BOOLEAN;
BEGIN
    SELECT n.nspname,c.relname INTO ledger_schema,ledger_name
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE c.oid=to_regclass('profile_creation_operations')
        AND c.relkind='r' AND c.relpersistence='p';
    IF ledger_schema IS NULL THEN
        RAISE EXCEPTION 'unsupported creation operation ledger relation';
    END IF;
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.%I)',ledger_schema,ledger_name) INTO retained;
    IF retained THEN
        RAISE EXCEPTION 'creation operation ledger must be empty before downgrade';
    END IF;
    -- A leading search_path schema may have a same-name unrelated function.
    -- Drop only the helper belonging to the actual locked ledger namespace.
    EXECUTE format('DROP TABLE %I.%I',ledger_schema,ledger_name);
    EXECUTE format('DROP FUNCTION %I.%I()',ledger_schema,'guard_profile_creation_operation');
END;
$$;
-- +goose StatementEnd
