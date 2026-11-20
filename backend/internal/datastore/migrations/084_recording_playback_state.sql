-- +goose Up
-- This is storage correlation, not a session, PIN proof or media permission.
-- Existing recordings, including legacy owners/capture JSON, are not rewritten.
LOCK TABLE recordings IN SHARE ROW EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
BEGIN
 IF NOT EXISTS (
  SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
  WHERE c.oid=pg_catalog.to_regclass('recordings') AND n.nspname=pg_catalog.current_schema()
   AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
 ) THEN RAISE EXCEPTION 'unsupported recording playback relation'; END IF;
END;
$$;
-- +goose StatementEnd

CREATE TABLE recording_playback_state (
 recording_id TEXT PRIMARY KEY REFERENCES recordings(id) ON DELETE CASCADE,
 incarnation UUID NOT NULL DEFAULT gen_random_uuid(),
 owner_user_id TEXT NOT NULL,
 recording_created_at TIMESTAMPTZ NOT NULL,
 capture_run_id JSONB,
 observer_receipt JSONB NOT NULL,
 revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
 writer_generation BIGINT NOT NULL DEFAULT 0 CHECK (writer_generation >= 0),
 tombstone BOOLEAN NOT NULL DEFAULT true,
 last_operation_id UUID,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- No owner-user FK: preserve existing missing-account/unknown-owner metadata.
-- Relation OID/filenode ignores ANALYZE statistics while detecting replacement.
-- Catalog tuple versions make replaced/reenabled observers retire old state.
-- +goose StatementBegin
CREATE FUNCTION recording_playback_observer_receipt(recording_relation OID) RETURNS JSONB
LANGUAGE plpgsql STABLE AS $recording_receipt$
DECLARE r RECORD; s RECORD; t RECORD; h RECORD; o RECORD;
BEGIN
 SELECT c.oid,c.relnamespace,c.relfilenode INTO r FROM pg_catalog.pg_class c
 WHERE c.oid=recording_relation AND c.relname='recordings'
  AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT c.oid,c.relfilenode INTO s FROM pg_catalog.pg_class c
 WHERE c.relnamespace=r.relnamespace AND c.relname='recording_playback_state'
  AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT p.oid,p.xmin::TEXT AS version,p.ctid::TEXT AS position INTO h
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 WHERE p.pronamespace=r.relnamespace AND p.proname='recording_playback_observer_receipt'
  AND p.proargtypes::TEXT='26' AND p.prorettype='pg_catalog.jsonb'::pg_catalog.regtype
  AND l.lanname='plpgsql' AND p.provolatile='s' AND NOT p.prosecdef AND p.proconfig IS NULL;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT p.oid,p.xmin::TEXT AS version,p.ctid::TEXT AS position INTO o
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 WHERE p.pronamespace=r.relnamespace AND p.proname='observe_recording_playback_identity'
  AND p.pronargs=0 AND p.prorettype='pg_catalog.trigger'::pg_catalog.regtype
  AND l.lanname='plpgsql' AND p.provolatile='v' AND NOT p.prosecdef AND p.proconfig IS NULL;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT g.oid,g.xmin::TEXT AS version,g.ctid::TEXT AS position INTO t
 FROM pg_catalog.pg_trigger g WHERE g.tgrelid=r.oid
  AND g.tgname='recording_playback_identity' AND g.tgfoid=o.oid
  AND g.tgtype=21 AND g.tgenabled='O' AND NOT g.tgisinternal
  AND g.tgnargs=0 AND g.tgqual IS NULL AND g.tgattr::TEXT=''
  AND NOT g.tgdeferrable AND NOT g.tginitdeferred AND g.tgconstraint=0;
 IF NOT FOUND THEN RETURN NULL; END IF;
 RETURN pg_catalog.jsonb_build_array(1,r.oid,r.relfilenode,s.oid,s.relfilenode,
  t.oid,t.version,t.position,h.oid,h.version,h.position,o.oid,o.version,o.position);
END;
$recording_receipt$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION observe_recording_playback_identity() RETURNS TRIGGER
LANGUAGE plpgsql AS $recording_identity$
DECLARE receipt JSONB;
BEGIN
 EXECUTE pg_catalog.format('SELECT %I.recording_playback_observer_receipt($1)',TG_TABLE_SCHEMA)
 INTO receipt USING TG_RELID;
 IF receipt IS NULL THEN RAISE EXCEPTION 'recording playback observer unavailable'; END IF;
 -- Ordinary title/status/byte checkpoints keep the same incarnation/counters.
 -- Actual owner, creation or capture-run replacement rotates even A->B->A.
 EXECUTE pg_catalog.format(
  'INSERT INTO %I.recording_playback_state AS s(recording_id,owner_user_id,recording_created_at,capture_run_id,observer_receipt)
   VALUES($1,$2,$3,$4,$5)
   ON CONFLICT(recording_id) DO UPDATE SET incarnation=pg_catalog.gen_random_uuid(),
    owner_user_id=EXCLUDED.owner_user_id,recording_created_at=EXCLUDED.recording_created_at,
    capture_run_id=EXCLUDED.capture_run_id,observer_receipt=EXCLUDED.observer_receipt,
    revision=0,writer_generation=0,tombstone=true,last_operation_id=NULL,updated_at=pg_catalog.clock_timestamp()
   WHERE s.owner_user_id IS DISTINCT FROM EXCLUDED.owner_user_id
    OR s.recording_created_at IS DISTINCT FROM EXCLUDED.recording_created_at
    OR s.capture_run_id IS DISTINCT FROM EXCLUDED.capture_run_id
    OR s.observer_receipt IS DISTINCT FROM EXCLUDED.observer_receipt',TG_TABLE_SCHEMA)
 USING NEW.id,NEW.user_id,NEW.created_at,NEW.capture_state->'runId',receipt;
 RETURN NEW;
END;
$recording_identity$;
-- +goose StatementEnd

CREATE TRIGGER recording_playback_identity AFTER INSERT OR UPDATE ON recordings
 FOR EACH ROW EXECUTE FUNCTION observe_recording_playback_identity();

INSERT INTO recording_playback_state(recording_id,owner_user_id,recording_created_at,capture_run_id,observer_receipt)
 SELECT id,user_id,created_at,capture_state->'runId',recording_playback_observer_receipt('recordings'::regclass)
 FROM recordings;

-- +goose Down
DROP TRIGGER recording_playback_identity ON recordings;
DROP FUNCTION observe_recording_playback_identity();
DROP FUNCTION recording_playback_observer_receipt(OID);
DROP TABLE recording_playback_state;
