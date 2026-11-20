package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func familyLocalPG(t *testing.T) *ratingPGFixture {
	t.Helper()
	f := familyMediaPG(t)
	f.exec(t, "CREATE TABLE remote_media_libraries(id TEXT PRIMARY KEY)")
	for _, name := range []string{"005_local_media.sql", "009_local_media_missing_state.sql", "063_local_media_multiple_roots.sql", "042_media_library_access.sql", "083_family_media_acl_fence.sql"} {
		raw, e := embedMigrations.ReadFile("migrations/" + name)
		if e != nil {
			t.Fatal(e)
		}
		f.exec(t, strings.Split(string(raw), "-- +goose Down")[0])
	}
	f.exec(t, `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('library','Synthetic Library','movie','/synthetic');INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('item','library','file.bin','/synthetic/file.bin','file.bin','movie',32)`)
	return f
}
func familyLocalOwner(t *testing.T, f *ratingPGFixture, target string) *FamilyMediaOwner {
	t.Helper()
	r, release := familyMediaRequest(t, f, t.Context())
	t.Cleanup(release)
	return familyMediaCapture(t, f, r, target, nil)
}
func familyLocalSource(t *testing.T, owner *FamilyMediaOwner) *FamilyLocalSource {
	t.Helper()
	source, e := owner.CaptureLocalSource(t.Context(), "item")
	if e != nil {
		t.Fatal("capture actual local source", e)
	}
	t.Cleanup(source.Close)
	return source
}
func TestFamilyLocalActualACLMatrixAndOriginalActor(t *testing.T) {
	cases := []struct {
		name, target, setup string
		master, allowed     bool
	}{
		{"master absent policy", "target", "", true, true},
		{"nonmaster absent policy", "target", "", false, false},
		{"all inactive no profile", "", `INSERT INTO media_library_access_policies VALUES('library','all',now())`, false, true},
		{"restricted actor grant", "target", `INSERT INTO media_library_access_policies VALUES('library','restricted',now());INSERT INTO media_library_account_grants VALUES('library','actor')`, false, true},
		{"restricted profile grant", "target", `INSERT INTO media_library_access_policies VALUES('library','restricted',now());INSERT INTO media_library_profile_grants VALUES('library','target')`, false, true},
		{"master selected foreign grant", "target", `INSERT INTO media_library_access_policies VALUES('library','restricted',now());INSERT INTO media_library_account_grants VALUES('library','other')`, true, false},
		{"master empty target legacy", "", `INSERT INTO media_library_access_policies VALUES('library','restricted',now());INSERT INTO media_library_account_grants VALUES('library','other')`, true, true},
		{"master foreign selected profile", "unrelated", `INSERT INTO media_library_access_policies VALUES('library','all',now())`, true, false},
		{"nonmaster empty target actor grant", "", `INSERT INTO media_library_access_policies VALUES('library','restricted',now());INSERT INTO media_library_account_grants VALUES('library','actor')`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := familyLocalPG(t)
			f.exec(t, "UPDATE accounts SET is_master=$1 WHERE id='actor'", tc.master)
			if tc.setup != "" {
				f.exec(t, tc.setup)
			}
			owner := familyLocalOwner(t, f, tc.target)
			source, e := owner.CaptureLocalSource(t.Context(), "item")
			if (e == nil) != tc.allowed || (source != nil) != tc.allowed {
				t.Fatal("ordinary LOCAL ACL changed", e)
			}
			if source != nil {
				defer source.Close()
				if source.Check(t.Context()) != nil {
					t.Fatal("allowed source not current")
				}
			}
		})
	}
}
func TestFamilyLocalPhysicalRowsAndAbsenceHistoryCannotABA(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE local_media_items SET file_name=file_name WHERE id='item'`,
		`UPDATE local_media_libraries SET name=name WHERE id='library'`,
		`INSERT INTO media_library_access_policies VALUES('library','restricted',now());DELETE FROM media_library_access_policies WHERE library_id='library'`,
		`INSERT INTO media_library_access_policies VALUES('library','restricted',now());INSERT INTO media_library_account_grants VALUES('library','other');DELETE FROM media_library_account_grants WHERE library_id='library';DELETE FROM media_library_access_policies WHERE library_id='library'`,
		`BEGIN;INSERT INTO media_library_access_policies VALUES('library','all',now());ROLLBACK`,
		`ALTER TABLE media_library_account_grants DISABLE TRIGGER family_media_acl_account_fence;ALTER TABLE media_library_account_grants ENABLE TRIGGER family_media_acl_account_fence`,
	} {
		t.Run(fmt.Sprint(len(mutation), strings.Fields(mutation)[0]), func(t *testing.T) {
			f := familyLocalPG(t)
			f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
			source := familyLocalSource(t, familyLocalOwner(t, f, "target"))
			f.exec(t, mutation)
			if source.Check(t.Context()) == nil || source.Context().Err() == nil {
				t.Fatal("old original survived physical or absent ACL ABA")
			}
		})
	}
}
func TestFamilyLocalGrantHistoryAndBoundedReturnedEvidence(t *testing.T) {
	for _, tc := range []struct{ name, setup string }{
		{"overflow grants", `INSERT INTO media_library_access_policies VALUES('library','all',now());INSERT INTO accounts(id,username,password_hash)SELECT 'grant-'||i,'Synthetic '||i,'synthetic' FROM generate_series(1,129)i;INSERT INTO media_library_account_grants SELECT 'library','grant-'||i FROM generate_series(1,129)i`},
		{"oversize path", `UPDATE local_media_items SET file_path=repeat('x',4097) WHERE id='item'`},
		{"oversize roots", `UPDATE local_media_libraries SET root_paths=(SELECT jsonb_agg('/root-'||i)FROM generate_series(1,65)i)WHERE id='library'`},
		{"missing file", `UPDATE local_media_items SET is_missing=true WHERE id='item'`},
		{"disabled fence", `ALTER TABLE media_library_profile_grants DISABLE TRIGGER family_media_acl_profile_fence`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := familyLocalPG(t)
			f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
			f.exec(t, tc.setup)
			owner := familyLocalOwner(t, f, "target")
			source, e := owner.CaptureLocalSource(t.Context(), "item")
			if e == nil || source != nil {
				t.Fatal("unsafe or unbounded source accepted")
			}
		})
	}
}
func TestFamilyLocalEmptyGrantABAAndRollbackRetireCurrentOwner(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(fmt.Sprint(rollback), func(t *testing.T) {
			f := familyLocalPG(t)
			f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
			f.exec(t, `INSERT INTO media_library_access_policies VALUES('library','restricted',now())`)
			source := familyLocalSource(t, familyLocalOwner(t, f, "target"))
			mutation := `INSERT INTO media_library_account_grants VALUES('library','other');DELETE FROM media_library_account_grants WHERE library_id='library'`
			if rollback {
				mutation = "BEGIN;" + mutation + ";ROLLBACK"
			}
			f.exec(t, mutation)
			if source.Check(t.Context()) == nil {
				t.Fatal("empty grant history was discarded")
			}
		})
	}
}
func TestFamilyLocalOpaqueCopiesAndMetadataCannotAcquire(t *testing.T) {
	var zero FamilyLocalSource
	if json.Unmarshal([]byte("{}"), &zero) == nil || zero.Check(t.Context()) == nil || zero.Context().Err() == nil {
		t.Fatal("imported source became authority")
	}
	f := familyLocalPG(t)
	f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
	owner := familyLocalOwner(t, f, "target")
	source := familyLocalSource(t, owner)
	copy := *source
	copy.Close()
	if copy.Check(t.Context()) == nil || source.Check(t.Context()) != nil {
		t.Fatal("copy gained or retired original authority")
	}
	for _, value := range []any{source, *source, copy, &copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyLocalSource(opaque)" {
				t.Fatal("source leaked original private data")
			}
		}
		raw, e := json.Marshal(value)
		if e != nil || string(raw) != "null" {
			t.Fatal("source serialized")
		}
	}
	info, e := source.Info(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	info.Roots[0] = "/changed"
	current, e := source.Info(t.Context())
	if e != nil || current.Roots[0] != "/synthetic" {
		t.Fatal("metadata mutation retargeted source")
	}
	for _, value := range []any{info, &info} {
		raw, e := json.Marshal(value)
		if e != nil || string(raw) != "null" || fmt.Sprintf("%+v", value) != "FamilyLocalSourceInfo(metadata)" {
			t.Fatal("metadata exposed private source path")
		}
	}
	source.Close()
	if another, e := owner.CaptureLocalSource(t.Context(), "item"); e == nil || another != nil {
		t.Fatal("closed original source was recaptured")
	}
}
func TestFamilyLocalOwnerCloseJoinsActualOpenAndRetirement(t *testing.T) {
	f := familyLocalPG(t)
	f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
	owner := familyLocalOwner(t, f, "target")
	source := familyLocalSource(t, owner)
	finish, e := source.BeginOpen(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	retired := make(chan struct{})
	started := make(chan struct{})
	allow := make(chan struct{})
	if source.BindRetirement(func() { close(started); <-allow; close(retired) }, retired) != nil {
		t.Fatal("bind actual owned retirement")
	}
	done := make(chan struct{})
	go func() { owner.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("owner refunded before pending opener")
	case <-time.After(20 * time.Millisecond):
	}
	finish()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("retirement not entered")
	}
	select {
	case <-done:
		t.Fatal("owner refunded before actual retirement")
	case <-time.After(20 * time.Millisecond):
	}
	close(allow)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owner cleanup did not join")
	}
	if source.Check(context.Background()) == nil {
		t.Fatal("closed source remained current")
	}
}
func TestFamilyLocalACL083ProtectedDownKeepsEvidence(t *testing.T) {
	f := familyLocalPG(t)
	raw, e := embedMigrations.ReadFile("migrations/083_family_media_acl_fence.sql")
	if e != nil {
		t.Fatal(e)
	}
	down := strings.Split(string(raw), "-- +goose Down")[1]
	if _, e = f.ds.pool.Exec(t.Context(), down); e == nil {
		t.Fatal("retained library history downgraded")
	}
	if _, e = f.ds.familyLocalFence(t.Context()); e != nil {
		t.Fatal("refused downgrade changed actual fence")
	}
	f.exec(t, "DELETE FROM local_media_libraries")
	if _, e = f.ds.pool.Exec(t.Context(), down); e != nil {
		t.Fatal("empty schema protected downgrade failed")
	}
	if _, e = f.ds.familyLocalFence(t.Context()); e == nil {
		t.Fatal("missing fence became permissive empty ACL")
	}
}

func familyLocalWaitSQL(t *testing.T, f *ratingPGFixture, fragment string) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		var waiting bool
		if e := f.admin.QueryRow(t.Context(), "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2)", f.cfg.ConnConfig.RuntimeParams["application_name"], "%"+fragment+"%").Scan(&waiting); e != nil {
			t.Fatal("observe own blocked SQL", e)
		}
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual owned SQL never reached held relation")
}
func TestFamilyLocalOwnerCloseJoinsBlockedCaptureAndOpener(t *testing.T) {
	for _, stage := range []string{"capture", "open"} {
		t.Run(stage, func(t *testing.T) {
			f := familyLocalPG(t)
			f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
			owner := familyLocalOwner(t, f, "target")
			var source *FamilyLocalSource
			if stage == "open" {
				source = familyLocalSource(t, owner)
			}
			tx, e := f.ds.pool.Begin(t.Context())
			if e != nil {
				t.Fatal(e)
			}
			defer tx.Rollback(context.Background())
			table, fragment := "local_media_libraries", "WITH selected AS"
			if stage == "open" {
				table = "accounts"
				fragment = "FROM accounts a WHERE a.id="
			}
			if _, e = tx.Exec(t.Context(), "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE"); e != nil {
				t.Fatal(e)
			}
			acquired := make(chan error, 1)
			go func() {
				if stage == "capture" {
					value, e := owner.CaptureLocalSource(t.Context(), "item")
					if value != nil {
						value.Close()
					}
					acquired <- e
				} else {
					finish, e := source.BeginOpen(t.Context())
					if finish != nil {
						finish()
					}
					acquired <- e
				}
			}()
			familyLocalWaitSQL(t, f, fragment)
			closed := make(chan struct{})
			go func() { owner.Close(); close(closed) }()
			select {
			case e := <-acquired:
				if e == nil {
					t.Fatal("closed acquisition authorized")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("blocked acquisition did not cancel/join")
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("owner refunded without actual acquisition completion")
			}
		})
	}
}
func TestFamilyLocalFenceFunctionReplacementCannotAuthorize(t *testing.T) {
	for _, mutation := range []string{
		`CREATE OR REPLACE FUNCTION observe_regional_policy_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$BEGIN IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;END;$$`,
		`DROP TRIGGER family_media_acl_account_fence ON media_library_account_grants`,
	} {
		t.Run(strings.Fields(mutation)[0], func(t *testing.T) {
			f := familyLocalPG(t)
			f.exec(t, "UPDATE accounts SET is_master=true WHERE id='actor'")
			owner := familyLocalOwner(t, f, "target")
			f.exec(t, mutation)
			if source, e := owner.CaptureLocalSource(t.Context(), "item"); e == nil || source != nil {
				t.Fatal("missing/replaced actual permission fence accepted")
			}
		})
	}
}
