-- +goose Up
-- Tombstones deliberately have no users foreign key: deleting and recreating a
-- profile must not revive an earlier absent-settings snapshot.
LOCK TABLE users, user_settings IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE user_settings_versions (
    user_id TEXT PRIMARY KEY,
    generation BIGINT NOT NULL CHECK (generation >= 0)
);

INSERT INTO user_settings_versions (user_id, generation)
SELECT id, 0 FROM users;

-- +goose StatementBegin
CREATE FUNCTION observe_user_settings_version() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE
    old_key TEXT;
    new_key TEXT;
    item TEXT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        old_key := CASE WHEN TG_TABLE_NAME = 'users'
            THEN to_jsonb(OLD)->>'id' ELSE to_jsonb(OLD)->>'user_id' END;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        new_key := CASE WHEN TG_TABLE_NAME = 'users'
            THEN to_jsonb(NEW)->>'id' ELSE to_jsonb(NEW)->>'user_id' END;
    END IF;
    IF TG_TABLE_NAME = 'users' AND TG_OP = 'UPDATE'
        AND old_key IS NOT DISTINCT FROM new_key THEN
        RETURN NULL;
    END IF;
    -- Sorted keys avoid opposite ordering when a primary key is moved. Overflow
    -- aborts the whole original write, including cascade and version changes.
    FOR item IN SELECT DISTINCT value FROM unnest(ARRAY[old_key, new_key]) value
        WHERE value IS NOT NULL ORDER BY value LOOP
        EXECUTE format('INSERT INTO %I.user_settings_versions AS v (user_id,generation)
            VALUES ($1,1) ON CONFLICT (user_id) DO UPDATE
            SET generation = v.generation + 1', TG_TABLE_SCHEMA) USING item;
    END LOOP;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER user_settings_observe_version
AFTER INSERT OR UPDATE OR DELETE ON user_settings
FOR EACH ROW EXECUTE FUNCTION observe_user_settings_version();

CREATE TRIGGER users_observe_settings_lifecycle
AFTER INSERT OR DELETE OR UPDATE OF id ON users
FOR EACH ROW EXECUTE FUNCTION observe_user_settings_version();

-- +goose Down
-- Downgrade discards concurrency history; no prepared snapshot may survive it.
DROP TRIGGER users_observe_settings_lifecycle ON users;
DROP TRIGGER user_settings_observe_version ON user_settings;
DROP FUNCTION observe_user_settings_version();
DROP TABLE user_settings_versions;
