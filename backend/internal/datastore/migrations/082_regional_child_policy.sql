-- +goose Up
-- Configuration is separate from generic profile/settings writes. No implicit
-- conversion of US scalar ratings and no enabled/disabled child bypass exists.
CREATE SEQUENCE regional_policy_revision_seq AS BIGINT NO CYCLE;
SELECT nextval('regional_policy_revision_seq');
CREATE TABLE regional_profile_policies (
 user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
 policy_version SMALLINT NOT NULL CHECK(policy_version=1),
 country TEXT NOT NULL CHECK(country='DE'),
 max_movie_age SMALLINT NOT NULL CHECK(max_movie_age IN(0,6,12,16,18)),
 max_episode_age SMALLINT NOT NULL CHECK(max_episode_age IN(0,6,12,16,18)),
 revision BIGINT NOT NULL CHECK(revision>0),
 updated_at TIMESTAMPTZ NOT NULL,
 updated_by TEXT NOT NULL CHECK(length(updated_by) BETWEEN 1 AND 256 AND updated_by=btrim(updated_by))
);
-- Sequence-only triggers avoid reversing existing account-before-profile row
-- locks. The global fence conservatively invalidates household applicability;
-- per-profile parent policy revisions do not include this unrelated fence.
-- +goose StatementBegin
CREATE FUNCTION observe_regional_policy_revision() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
DECLARE value BIGINT;
BEGIN
 EXECUTE format('SELECT nextval(%L::regclass)',format('%I.regional_policy_revision_seq',TG_TABLE_SCHEMA)) INTO value;
 IF TG_TABLE_NAME='regional_profile_policies' AND TG_OP<>'DELETE' THEN NEW.revision=value; END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 RETURN NEW;
END;
$$;
-- +goose StatementEnd
CREATE TRIGGER regional_policy_revision BEFORE INSERT OR UPDATE OR DELETE ON regional_profile_policies FOR EACH ROW EXECUTE FUNCTION observe_regional_policy_revision();
CREATE TRIGGER users_regional_policy_fence BEFORE INSERT OR UPDATE OR DELETE ON users FOR EACH ROW EXECUTE FUNCTION observe_regional_policy_revision();
CREATE TRIGGER accounts_regional_policy_fence BEFORE INSERT OR UPDATE OR DELETE ON accounts FOR EACH ROW EXECUTE FUNCTION observe_regional_policy_revision();
-- +goose Down
DROP TRIGGER accounts_regional_policy_fence ON accounts;
DROP TRIGGER users_regional_policy_fence ON users;
DROP TRIGGER regional_policy_revision ON regional_profile_policies;
DROP FUNCTION observe_regional_policy_revision();
DROP TABLE regional_profile_policies;
DROP SEQUENCE regional_policy_revision_seq;
