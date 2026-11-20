-- +goose Up
-- Reuse the existing non-cycling current-family fence. Even an ACL row that
-- appears and disappears, or a rolled-back write, invalidates original owners.
-- This is deliberately conservative across unrelated libraries/households.
CREATE TRIGGER family_media_acl_policy_fence BEFORE INSERT OR UPDATE OR DELETE ON media_library_access_policies FOR EACH ROW EXECUTE FUNCTION observe_regional_policy_revision();
CREATE TRIGGER family_media_acl_account_fence BEFORE INSERT OR UPDATE OR DELETE ON media_library_account_grants FOR EACH ROW EXECUTE FUNCTION observe_regional_policy_revision();
CREATE TRIGGER family_media_acl_profile_fence BEFORE INSERT OR UPDATE OR DELETE ON media_library_profile_grants FOR EACH ROW EXECUTE FUNCTION observe_regional_policy_revision();

-- +goose Down
-- A retained library may still have an original absent-policy/empty-grant
-- observation. Never discard its permission-change history during downgrade.
-- The source adapter separately verifies the installed fence before use.
-- +goose StatementBegin
DO $$
DECLARE own_schema TEXT; relation_name TEXT; current_schema_name TEXT; retained BOOLEAN;
BEGIN
 SELECT n.nspname INTO own_schema FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('media_library_access_policies') AND c.relkind='r' AND c.relpersistence='p';
 IF own_schema IS NULL THEN RAISE EXCEPTION 'unsupported family media ACL relation'; END IF;
 FOREACH relation_name IN ARRAY ARRAY['local_media_libraries','remote_media_libraries','media_library_access_policies','media_library_account_grants','media_library_profile_grants'] LOOP
  SELECT n.nspname INTO current_schema_name FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass(relation_name) AND c.relkind='r' AND c.relpersistence='p';
  IF current_schema_name IS DISTINCT FROM own_schema THEN RAISE EXCEPTION 'unsupported family media ACL relation'; END IF;
  EXECUTE format('LOCK TABLE %I.%I IN ACCESS EXCLUSIVE MODE',own_schema,relation_name);
 END LOOP;
 EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.local_media_libraries) OR EXISTS(SELECT 1 FROM %I.remote_media_libraries) OR EXISTS(SELECT 1 FROM %I.media_library_access_policies) OR EXISTS(SELECT 1 FROM %I.media_library_account_grants) OR EXISTS(SELECT 1 FROM %I.media_library_profile_grants)',own_schema,own_schema,own_schema,own_schema,own_schema) INTO retained;
 IF retained THEN RAISE EXCEPTION 'family media library evidence must be empty before downgrade'; END IF;
 EXECUTE format('DROP TRIGGER family_media_acl_policy_fence ON %I.media_library_access_policies',own_schema);
 EXECUTE format('DROP TRIGGER family_media_acl_account_fence ON %I.media_library_account_grants',own_schema);
 EXECUTE format('DROP TRIGGER family_media_acl_profile_fence ON %I.media_library_profile_grants',own_schema);
END;
$$;
-- +goose StatementEnd
