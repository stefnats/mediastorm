package datastore

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func TestProfileCreationResultsMigrationFull081AndEmptyPerStepDown(t *testing.T) {
	f := creationOperationFixture(t)
	latest := latestEmbeddedMigrationVersion(t)
	creationOperationGoose(t)
	if v := creationOperationVersion(t, f.ds); v != latest {
		t.Fatalf("startup migration version = %d, want latest embedded %d", v, latest)
	}
	if err := creationOperationDown(t.Context(), f.ds); err != nil {
		t.Fatal("empty081 then080 per-step Down", err)
	}
	if v := creationOperationVersion(t, f.ds); v != 79 {
		t.Fatal("empty per-step downgrade", v)
	}
	var result, trigger bool
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_regclass('profile_creation_results') IS NOT NULL,EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='users_invalidate_creation_birth' AND tgrelid='users'::regclass)`).Scan(&result, &trigger); err != nil || result || trigger {
		t.Fatal("Down retained owned081 objects", err)
	}
	f.ds.Close()
	f.ds = f.open(t)
	if v := creationOperationVersion(t, f.ds); v != latest {
		t.Fatalf("restart migration version = %d, want latest embedded %d", v, latest)
	}
}
func TestProfileCreationResultsMigrationRetainedProofRefusesDown(t *testing.T) {
	f := newParentCreationFixture(t)
	i, _ := creationResultCreate(t, f, f.ds)
	// Later DOWN steps commit independently; isolate081 before observing its refusal.
	creationOperationGoose(t)
	db := stdlib.OpenDBFromPool(f.ds.pool)
	if err := goose.DownToContext(t.Context(), db, "migrations", 81); err != nil {
		db.Close()
		t.Fatal("prepare exact081 retained proof", err)
	}
	db.Close()
	if version := creationOperationVersion(t, f.ds); version != 81 {
		t.Fatal("retained proof fixture is not exact081", version)
	}
	before := creationOperationExistingRows(t, f.settingsDSFixture)
	if err := creationOperationDown(t.Context(), f.ds); err == nil {
		t.Fatal("retained actual created proof discarded")
	}
	if creationOperationVersion(t, f.ds) != 81 || creationOperationExistingRows(t, f.settingsDSFixture) != before {
		t.Fatal("refused081 Down changed version/User/Settings")
	}
	var outcome string
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT outcome FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&outcome); err != nil || outcome != "created" {
		t.Fatal("refused Down erased proof", err)
	}
}
func TestProfileCreationResultsMigrationLegacyUpgradeNeverFabricatesCreated(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	id, raw := creationOperationSeed(t, f.ds.pool, creationOperationTestMetadata("actor"), "terminal", true)
	before := creationOperationExistingRows(t, f)
	creationOperationUpTo(t, f.ds, 81)
	var protocol int
	var outcome string
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT protocol_version,outcome FROM profile_creation_results WHERE operation_id=$1`, id).Scan(&protocol, &outcome); err != nil || protocol != 0 || outcome != "legacy_unqualified" {
		t.Fatal("legacy terminal fabricated created", err)
	}
	if creationOperationExistingRows(t, f) != before {
		t.Fatal("upgrade changed historical User/Settings")
	}
	if creationOperationObserve(t, f.ds, t.Context(), raw).record.state != "terminal" {
		t.Fatal("upgrade changed executable ledger")
	}
	if err := creationOperationDown(t.Context(), f.ds); err == nil || creationOperationVersion(t, f.ds) != 81 {
		t.Fatal("retained legacy proof Down must refuse", err)
	}
	if creationOperationExistingRows(t, f) != before {
		t.Fatal("refused081 Down changed historical User/Settings")
	}
	// New must independently discover every later migration, without upgrading
	// this legacy result into an invented successful profile creation.
	latest := latestEmbeddedMigrationVersion(t)
	f.ds.Close()
	f.ds = f.open(t)
	if version := creationOperationVersion(t, f.ds); version != latest {
		t.Fatalf("legacy restart migration version = %d, want latest embedded %d", version, latest)
	}
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT protocol_version,outcome FROM profile_creation_results WHERE operation_id=$1`, id).Scan(&protocol, &outcome); err != nil || protocol != 0 || outcome != "legacy_unqualified" {
		t.Fatal("latest startup fabricated legacy creation evidence", err)
	}
	if creationOperationObserve(t, f.ds, t.Context(), raw).record.state != "terminal" {
		t.Fatal("latest startup changed legacy executable ledger")
	}
}
func TestProfileCreationResultsMigrationIdentityAndBirthCannotBeCleared(t *testing.T) {
	f := newParentCreationFixture(t)
	i, receipt := creationResultCreate(t, f, f.ds)
	for _, sql := range []string{`UPDATE profile_creation_results SET owner_id='other',result_version=result_version+1 WHERE operation_id=$1`, `UPDATE profile_creation_results SET reference_hash=repeat('0',64),result_version=result_version+1 WHERE operation_id=$1`, `UPDATE profile_creation_results SET outcome='not_created',result_version=result_version+1 WHERE operation_id=$1`, `UPDATE profile_creation_results SET attempted=false,result_version=result_version+1 WHERE operation_id=$1`, `UPDATE profile_creation_results SET result_version=9223372036854775807 WHERE operation_id=$1`} {
		if _, err := f.ds.pool.Exec(t.Context(), sql, i.state.reference.record.id); err == nil {
			t.Fatal("immutable result accepted rewrite")
		}
	}
	if err := f.ds.Users().Delete(t.Context(), receipt.user.User.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ds.pool.Exec(t.Context(), `UPDATE profile_creation_results SET birth_invalidated=false,result_version=result_version+1 WHERE operation_id=$1`, i.state.reference.record.id); err == nil {
		t.Fatal("birth tombstone cleared")
	}
}
func TestProfileCreationResultsMigrationAliasDownPreservesLeadingHelpers(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationGoose(t)
	leading := f.schema + "_result_function"
	if _, err := f.admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{leading}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := f.admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{leading}.Sanitize()+" CASCADE")
		if err != nil {
			t.Error(err)
		}
	})
	for _, fn := range []string{"guard_profile_creation_result", "invalidate_profile_creation_birth"} {
		f.exec(t, "CREATE FUNCTION "+pgx.Identifier{leading, fn}.Sanitize()+"() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$")
	}
	alias := creationOperationPool(t, f, leading+",", nil)
	if err := creationOperationDown(t.Context(), alias); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []string{"guard_profile_creation_result", "invalidate_profile_creation_birth"} {
		var present bool
		if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_regprocedure($1) IS NOT NULL`, pgx.Identifier{leading, fn}.Sanitize()+"()").Scan(&present); err != nil || !present {
			t.Fatal("Down dropped unrelated leading helper", err)
		}
	}
}
func TestProfileCreationResultsMigrationRejectsMisplacedUpgradeAtomically(t *testing.T) {
	f := creationOperationFixture(t)
	creationOperationUse080(t, f)
	leading := f.schema + "_wrong_result"
	if _, err := f.admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{leading}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{f.schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, err := f.admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{leading}.Sanitize()+" CASCADE")
		if err != nil {
			t.Error(err)
		}
	})
	alias := creationOperationPool(t, f, leading+",", nil)
	db := stdlib.OpenDBFromPool(alias.pool)
	defer db.Close()
	creationOperationGoose(t)
	if err := goose.UpContext(t.Context(), db, "migrations"); err == nil {
		t.Fatal("migration installed inconsistent result namespace")
	}
	var table, trigger bool
	if err := alias.pool.QueryRow(t.Context(), `SELECT to_regclass('profile_creation_results') IS NOT NULL,EXISTS(SELECT 1 FROM pg_trigger WHERE tgname='users_invalidate_creation_birth' AND tgrelid='users'::regclass)`).Scan(&table, &trigger); err != nil || table || trigger || creationOperationVersion(t, f.ds) != 80 {
		t.Fatal("rejected migration left partial schema", err)
	}
}

func TestProfileCreationResultsMigrationActualBirthOverflowRollsBackUserLifecycle(t *testing.T) {
	f := newParentCreationFixture(t)
	i, receipt := creationResultCreate(t, f, f.ds)
	// INSERT setup models an already-maximal retained result version; no trigger
	// disabling or result UPDATE bypass is used. This is a migration control, not
	// evidence that an arbitrary synthetic row authorized actual public creation.
	f.exec(t, `WITH original AS (DELETE FROM profile_creation_results WHERE operation_id=$1 RETURNING to_jsonb(profile_creation_results) AS payload)
 INSERT INTO profile_creation_results SELECT (jsonb_populate_record(NULL::profile_creation_results,jsonb_set(payload,'{result_version}','9223372036854775807'::jsonb))).* FROM original`, i.state.reference.record.id)
	before := creationOperationExistingRows(t, f.settingsDSFixture)
	if err := f.ds.Users().Delete(t.Context(), receipt.user.User.ID); err == nil {
		t.Fatal("real lifecycle birth increment did not overflow")
	}
	if creationOperationExistingRows(t, f.settingsDSFixture) != before {
		t.Fatal("overflow partially deleted User/Settings079")
	}
	var invalid bool
	var version int64
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT birth_invalidated,result_version FROM profile_creation_results WHERE operation_id=$1`, i.state.reference.record.id).Scan(&invalid, &version); err != nil || invalid || version != 9223372036854775807 {
		t.Fatal("overflow changed retained birth", err)
	}
}
