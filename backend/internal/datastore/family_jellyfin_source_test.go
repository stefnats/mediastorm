package datastore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	familyJellyfinPrimary = "11111111222233334444555555555555"
	familyJellyfinBacking = "22222222333344445555666666666666"
	familyJellyfinLibrary = "33333333444455556666777777777777"
)

func familyJellyfinPG(t *testing.T) *ratingPGFixture {
	t.Helper()
	return familyJellyfinTracedPG(t, nil)
}

func familyJellyfinTracedPG(t *testing.T, tracer pgx.QueryTracer) *ratingPGFixture {
	t.Helper()
	f := familyMediaPG(t)
	for _, name := range []string{"005_local_media.sql", "009_local_media_missing_state.sql", "063_local_media_multiple_roots.sql", "032_remote_media.sql", "033_remote_media_duration.sql", "041_remote_media_server_url.sql", "042_media_library_access.sql", "083_family_media_acl_fence.sql"} {
		raw, err := embedMigrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal("read actual source migration")
		}
		f.exec(t, strings.Split(string(raw), "-- +goose Down")[0])
	}
	f.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';
 INSERT INTO accounts(id,username,password_hash)VALUES('integration','Synthetic Integration','synthetic');
 INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('local-library','Synthetic LOCAL','movie','/synthetic');
 INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('local-item','local-library','file.bin','/synthetic/file.bin','file.bin','movie',32)`)
	f.exec(t, `INSERT INTO remote_media_libraries(id,name,library_type,provider,account_id,external_library_id)VALUES('remote-library','Synthetic Jellyfin','movie','jellyfin','integration',$1)`, familyJellyfinLibrary)
	f.exec(t, `INSERT INTO remote_media_items(id,library_id,external_item_id,external_media_id,group_key,library_type,title,file_name,size_bytes)VALUES('remote-item','remote-library',$1,$2,'synthetic-group','movie','Synthetic Title','synthetic.bin',32)`, familyJellyfinPrimary, familyJellyfinBacking)
	// Exercise the actual production cancellation/physical-cleanup policy too.
	// This is a genuine second pool in the same own synthetic role/schema;
	// no owner/frame exists until this store has been selected.
	cfg := f.cfg.Copy()
	cfg.ConnConfig.Tracer = tracer
	configureQueryLifecycle(cfg.ConnConfig)
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal("open actual scoped production-lifecycle pool")
	}
	t.Cleanup(pool.Close)
	f.ds = &DataStore{pool: pool}
	f.cfg = cfg
	return f
}

func familyJellyfinOwner(t *testing.T, f *ratingPGFixture, target string) *FamilyMediaOwner {
	t.Helper()
	r, release := familyMediaRequest(t, f, t.Context())
	t.Cleanup(release)
	return familyMediaCapture(t, f, r, target, nil)
}

func familyJellyfinSource(t *testing.T, owner *FamilyMediaOwner) *FamilyJellyfinSource {
	t.Helper()
	source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item")
	if err != nil || source == nil {
		t.Fatal("capture genuine original Jellyfin PG source")
	}
	t.Cleanup(source.Close)
	return source
}

func TestFamilyJellyfinActualOrdinaryACLAndIntegrationDomain(t *testing.T) {
	for _, tc := range []struct {
		name, target, setup string
		master, allowed     bool
	}{
		{"master absent policy", "target", "", true, true},
		{"nonmaster absent policy", "target", "", false, false},
		{"all inactive empty target", "", `INSERT INTO media_library_access_policies VALUES('remote-library','all',now())`, false, true},
		{"restricted empty master", "target", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now())`, true, true},
		{"actor grant", "target", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_account_grants VALUES('remote-library','actor')`, false, true},
		{"integration is not viewer", "target", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_account_grants VALUES('remote-library','integration')`, false, false},
		{"selected profile grant", "target", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_profile_grants VALUES('remote-library','target')`, false, true},
		{"master selected foreign grant", "target", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_account_grants VALUES('remote-library','other')`, true, false},
		{"master empty target legacy", "", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_account_grants VALUES('remote-library','other')`, true, true},
		{"master foreign selected profile", "unrelated", `INSERT INTO media_library_access_policies VALUES('remote-library','all',now())`, true, false},
		{"nonmaster empty target actor grant", "", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_account_grants VALUES('remote-library','actor')`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := familyJellyfinPG(t)
			f.exec(t, "UPDATE accounts SET is_master=$1 WHERE id='actor'", tc.master)
			if tc.setup != "" {
				f.exec(t, tc.setup)
			}
			owner := familyJellyfinOwner(t, f, tc.target)
			source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item")
			if (source != nil) != tc.allowed || (err == nil) != tc.allowed {
				t.Fatal("ordinary fresh ACL or identifier domains changed")
			}
			if source != nil {
				defer source.Close()
				info, err := source.Info(t.Context())
				if err != nil || info.IntegrationID != "integration" || info.LibraryID != "remote-library" || info.PrimaryGUID != familyJellyfinPrimary || info.BackingGUID != familyJellyfinBacking || info.ExternalLibraryGUID != familyJellyfinLibrary || !source.ExpiresAt().Equal(owner.ExpiresAt()) {
					t.Fatal("original PG selection or fixed expiry changed")
				}
			}
		})
	}
}

func TestFamilyJellyfinActualAdultPINAndChildDenialRemainOriginal(t *testing.T) {
	f := familyJellyfinPG(t)
	f.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='own-second'")
	r, release, token, expiry := familyMediaUnlock(t, f, time.Minute)
	defer release()
	owner := familyMediaCapture(t, f, r, "target", []string{token})
	source := familyJellyfinSource(t, owner)
	if owner.Mode() != FamilyMediaActiveAdult || !source.ExpiresAt().Equal(expiry) || source.Check(t.Context()) != nil {
		t.Fatal("original active adult grant was lost or renewed")
	}
	if err := revokeProfileUnlocks(t.Context(), f.ds.pool, "session", profileUnlockDigest(f.session.Token)); err != nil {
		t.Fatal("revoke actual original PIN")
	}
	if source.Check(t.Context()) == nil || source.Context().Err() == nil {
		t.Fatal("original PIN revocation did not retire source")
	}
	child := familyJellyfinPG(t)
	child.exec(t, "UPDATE users SET is_kids_profile=true WHERE id='target'")
	request, stop := familyMediaRequest(t, child, t.Context())
	defer stop()
	if value, err := child.ds.CaptureFamilyMediaOwner(request, "target", nil); value != nil || !errors.Is(err, ErrFamilyMediaChildDenied) {
		if value != nil {
			value.Close()
		}
		t.Fatal("family-disabled child gained ordinary media authority")
	}
}

func TestFamilyJellyfinWhitespaceOnlyGrantsMatchOrdinaryLocalEmptyRule(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprint(master), func(t *testing.T) {
			f := familyJellyfinPG(t)
			f.exec(t, "UPDATE accounts SET is_master=$1 WHERE id='actor'", master)
			f.exec(t, `INSERT INTO accounts(id,username,password_hash)VALUES(' ','Synthetic blank grant','synthetic');
 INSERT INTO users(id,account_id,name)VALUES(E' \t','other','Synthetic blank profile');
 INSERT INTO media_library_access_policies VALUES('local-library','restricted',now()),('remote-library','restricted',now());
 INSERT INTO media_library_account_grants VALUES('local-library',' '),('remote-library',' ');
 INSERT INTO media_library_profile_grants VALUES('local-library',E' \t'),('remote-library',E' \t')`)
			remoteOwner := familyJellyfinOwner(t, f, "target")
			remote, remoteErr := remoteOwner.CaptureJellyfinSource(t.Context(), "remote-item")
			if remote != nil {
				defer remote.Close()
			}
			localOwner := familyJellyfinOwner(t, f, "target")
			local, localErr := localOwner.CaptureLocalSource(t.Context(), "local-item")
			if local != nil {
				defer local.Close()
			}
			if (remoteErr == nil) != master || (localErr == nil) != master || (remote != nil) != master || (local != nil) != master {
				t.Fatal("normalized empty grants changed ordinary LOCAL master-only semantics")
			}
		})
	}
}

func TestFamilyJellyfinStrictConcreteGUIDSyntax(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{familyJellyfinPrimary, familyJellyfinPrimary},
		{"11111111-2222-3333-4444-555555555555", familyJellyfinPrimary},
		{"ABCDEF01-2345-6789-ABCD-EF0123456789", "abcdef0123456789abcdef0123456789"},
		{"ABCDEF0123456789ABCDEF0123456789", "abcdef0123456789abcdef0123456789"},
	} {
		if got, ok := familyJellyfinGUID(tc.value); !ok || got != tc.want {
			t.Fatal("ordinary N/D resource syntax was not normalized")
		}
	}
	for _, value := range []string{"", strings.Repeat("0", 32), "00000000-0000-0000-0000-000000000000", "urn:uuid:11111111-2222-3333-4444-555555555555", "{" + familyJellyfinPrimary + "}", " " + familyJellyfinPrimary, familyJellyfinPrimary + " ", "11111111_2222-3333-4444-555555555555", "g" + familyJellyfinPrimary[1:]} {
		if _, ok := familyJellyfinGUID(value); ok {
			t.Fatal("nonconcrete or alternate GUID syntax was accepted")
		}
	}
}

func TestFamilyJellyfinInvalidOrExcessivePGSelectionDenies(t *testing.T) {
	for _, tc := range []struct{ name, mutation string }{
		{"empty backing GUID", `UPDATE remote_media_items SET external_media_id=repeat('0',32)`},
		{"empty primary GUID", `UPDATE remote_media_items SET external_item_id=repeat('0',32)`},
		{"empty library GUID", `UPDATE remote_media_libraries SET external_library_id=repeat('0',32)`},
		{"URN GUID", `UPDATE remote_media_items SET external_item_id='urn:uuid:11111111-2222-3333-4444-555555555555'`},
		{"different provider", `UPDATE remote_media_libraries SET provider='plex'`},
		{"missing row", `DELETE FROM remote_media_items`},
		{"missing source", `UPDATE remote_media_items SET is_missing=true`},
		{"zero size", `UPDATE remote_media_items SET size_bytes=0`},
		{"excessive size", `UPDATE remote_media_items SET size_bytes=8589934593`},
		{"excessive filename", `UPDATE remote_media_items SET file_name=repeat('f',1025)`},
		{"excessive integration", `UPDATE remote_media_libraries SET account_id=repeat('a',129)`},
		{"type mismatch", `UPDATE remote_media_items SET library_type='show'`},
		{"unsupported kind", `UPDATE remote_media_items SET library_type='other';UPDATE remote_media_libraries SET library_type='other'`},
		{"excessive grants", `INSERT INTO media_library_access_policies VALUES('remote-library','all',now());INSERT INTO accounts(id,username,password_hash)SELECT 'grant-'||i,'Synthetic '||i,'synthetic' FROM generate_series(1,129)i;INSERT INTO media_library_account_grants SELECT 'remote-library','grant-'||i FROM generate_series(1,129)i`},
		{"disabled fence", `ALTER TABLE media_library_profile_grants DISABLE TRIGGER family_media_acl_profile_fence`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := familyJellyfinPG(t)
			f.exec(t, tc.mutation)
			owner := familyJellyfinOwner(t, f, "target")
			if source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item"); source != nil || err == nil {
				if source != nil {
					source.Close()
				}
				t.Fatal("invalid or excessive source evidence was accepted")
			}
		})
	}
}

func TestFamilyJellyfinProviderPayloadAndURLAreNotPGPermission(t *testing.T) {
	f := familyJellyfinPG(t)
	f.exec(t, `UPDATE remote_media_items SET provider_data=jsonb_build_object('synthetic_untrusted',repeat('x',1048576)),stream_path='https://foreign.invalid/not-an-authority';UPDATE remote_media_libraries SET server_url='https://foreign.invalid/not-the-config-origin'`)
	owner := familyJellyfinOwner(t, f, "target")
	source := familyJellyfinSource(t, owner)
	info, err := source.Info(t.Context())
	if err != nil || info.IntegrationID != "integration" || info.PrimaryGUID != familyJellyfinPrimary || info.BackingGUID != familyJellyfinBacking {
		t.Fatal("unused provider payload or URL was promoted into PG source selection")
	}
	// No HTTP/server permission is claimed; this port exposes only PG metadata.
	if _, err := json.Marshal(info); err != nil {
		t.Fatal("safe metadata representation failed")
	}
}

func TestFamilyJellyfinPhysicalRowsAndACLHistoryCannotRebindOriginal(t *testing.T) {
	for _, tc := range []struct{ name, mutation string }{
		{"same item update", `UPDATE remote_media_items SET title=title`},
		{"same library update", `UPDATE remote_media_libraries SET name=name`},
		{"item values ABA", `UPDATE remote_media_items SET file_name='temporary';UPDATE remote_media_items SET file_name='synthetic.bin'`},
		{"item replacement", `CREATE TEMP TABLE saved_item AS SELECT * FROM remote_media_items;DELETE FROM remote_media_items;INSERT INTO remote_media_items SELECT * FROM saved_item`},
		{"library replacement", `CREATE TEMP TABLE saved_library AS SELECT * FROM remote_media_libraries;CREATE TEMP TABLE saved_item AS SELECT * FROM remote_media_items;DELETE FROM remote_media_libraries;INSERT INTO remote_media_libraries SELECT * FROM saved_library;INSERT INTO remote_media_items SELECT * FROM saved_item`},
		{"absent policy ABA", `INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());DELETE FROM media_library_access_policies`},
		{"grant absence rollback", `BEGIN;INSERT INTO media_library_access_policies VALUES('remote-library','restricted',now());INSERT INTO media_library_account_grants VALUES('remote-library','other');ROLLBACK`},
		{"trigger restored", `ALTER TABLE media_library_account_grants DISABLE TRIGGER family_media_acl_account_fence;ALTER TABLE media_library_account_grants ENABLE TRIGGER family_media_acl_account_fence`},
		{"wrong fence function", `CREATE OR REPLACE FUNCTION observe_regional_policy_revision() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$BEGIN IF TG_OP='DELETE' THEN RETURN OLD;END IF;RETURN NEW;END;$$`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := familyJellyfinPG(t)
			source := familyJellyfinSource(t, familyJellyfinOwner(t, f, "target"))
			f.exec(t, tc.mutation)
			if source.Check(t.Context()) == nil || source.Context().Err() == nil {
				t.Fatal("original source adopted changed/restored physical evidence")
			}
		})
	}
}

func TestFamilyJellyfinAndLocalShareOnePermanentOriginalClaim(t *testing.T) {
	for _, first := range []string{"local", "jellyfin", "failed local", "failed jellyfin"} {
		t.Run(first, func(t *testing.T) {
			f := familyJellyfinPG(t)
			owner := familyJellyfinOwner(t, f, "target")
			switch first {
			case "local":
				source, err := owner.CaptureLocalSource(t.Context(), "local-item")
				if err != nil {
					t.Fatal("capture actual LOCAL source")
				}
				source.Close()
			case "jellyfin":
				familyJellyfinSource(t, owner).Close()
			case "failed local":
				if source, err := owner.CaptureLocalSource(t.Context(), "absent-valid-id"); source != nil || err == nil {
					t.Fatal("missing LOCAL row unexpectedly authorized")
				}
			case "failed jellyfin":
				if source, err := owner.CaptureJellyfinSource(t.Context(), "absent-valid-id"); source != nil || err == nil {
					t.Fatal("missing Jellyfin row unexpectedly authorized")
				}
			}
			if source, err := owner.CaptureLocalSource(t.Context(), "local-item"); source != nil || !errors.Is(err, ErrFamilyMediaBusy) {
				t.Fatal("consumed owner acquired a second LOCAL source")
			}
			if source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item"); source != nil || !errors.Is(err, ErrFamilyMediaBusy) {
				t.Fatal("consumed owner acquired a second Jellyfin source")
			}
		})
	}
}

func TestFamilyJellyfinInvalidSelectorsDoNotChangeLocalEarlyValidation(t *testing.T) {
	f := familyJellyfinPG(t)
	owner := familyJellyfinOwner(t, f, "target")
	if source, err := owner.CaptureLocalSource(t.Context(), ""); source != nil || err == nil {
		t.Fatal("invalid LOCAL selector did not deny early")
	}
	if source, err := owner.CaptureJellyfinSource(t.Context(), " bad-selector"); source != nil || err == nil {
		t.Fatal("invalid Jellyfin selector did not deny early")
	}
	if source := familyJellyfinSource(t, owner); source.Check(t.Context()) != nil {
		t.Fatal("early invalid selector consumed the original source claim")
	}
}

func TestFamilyJellyfinConcurrentCrossProviderClaimHasOneWinner(t *testing.T) {
	f := familyJellyfinPG(t)
	owner := familyJellyfinOwner(t, f, "target")
	start := make(chan struct{})
	results := make(chan bool, 2)
	go func() {
		<-start
		source, err := owner.CaptureLocalSource(t.Context(), "local-item")
		if source != nil {
			source.Close()
		}
		results <- err == nil
	}()
	go func() {
		<-start
		source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item")
		if source != nil {
			source.Close()
		}
		results <- err == nil
	}()
	close(start)
	winners := 0
	for index := 0; index < 2; index++ {
		select {
		case won := <-results:
			if won {
				winners++
			}
		case <-time.After(3 * time.Second):
			t.Fatal("cross-provider source claim did not complete")
		}
	}
	if winners != 1 {
		t.Fatal("cross-provider original claim did not have exactly one winner")
	}
}

func TestFamilyJellyfinSourceOnlyCancellationCoversPreliminaryOwnerSQL(t *testing.T) {
	for _, operation := range []string{"Check", "Info", "BeginOpen", "Capture"} {
		t.Run(operation, func(t *testing.T) {
			f := familyJellyfinPG(t)
			owner := familyJellyfinOwner(t, f, "target")
			var source *FamilyJellyfinSource
			if operation != "Capture" {
				source = familyJellyfinSource(t, owner)
			}
			tx, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal("begin own lock transaction")
			}
			defer tx.Rollback(context.Background())
			if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal("hold actual preliminary owner relation")
			}
			finished := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case "Check":
					err = source.Check(t.Context())
				case "Info":
					_, err = source.Info(t.Context())
				case "BeginOpen":
					finish, e := source.BeginOpen(t.Context())
					if finish != nil {
						finish()
					}
					err = e
				case "Capture":
					value, e := owner.CaptureJellyfinSource(t.Context(), "remote-item")
					if value != nil {
						value.Close()
					}
					err = e
				}
				finished <- err
			}()
			familyLocalWaitSQL(t, f, "FROM accounts a WHERE a.id=")
			if operation == "Capture" {
				owner.mu.Lock()
				source, _ = owner.source.(*FamilyJellyfinSource)
				owner.mu.Unlock()
				if source == nil {
					t.Fatal("actual capture did not publish its original claim")
				}
			}
			source.Invalidate()
			select {
			case err := <-finished:
				if err == nil {
					t.Fatal("cancelled preliminary operation authorized")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("source-only cancel missed preliminary owner SQL")
			}
			if err := tx.Rollback(t.Context()); err != nil {
				t.Fatal("release actual held relation")
			}
			if owner.Context().Err() != nil || owner.Check(t.Context()) != nil {
				t.Fatal("source-only cancellation retired the healthy original owner")
			}
		})
	}
}

type familyJellyfinTraceKey struct{}
type familyJellyfinEndBarrier struct {
	armed   atomic.Bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (barrier *familyJellyfinEndBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, familyJellyfinTraceKey{}, barrier.armed.Load() && strings.Contains(data.SQL, "FROM accounts a WHERE a.id="))
}
func (barrier *familyJellyfinEndBarrier) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if selected, _ := ctx.Value(familyJellyfinTraceKey{}).(bool); selected {
		barrier.once.Do(func() { close(barrier.entered); <-barrier.release })
	}
}

func TestFamilyJellyfinOwnerCloseJoinsActualStandaloneCheckAndInfo(t *testing.T) {
	for _, operation := range []string{"Check", "Info"} {
		t.Run(operation, func(t *testing.T) {
			barrier := &familyJellyfinEndBarrier{entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			unblock := func() { once.Do(func() { close(barrier.release) }) }
			defer unblock()
			f := familyJellyfinTracedPG(t, barrier)
			owner := familyJellyfinOwner(t, f, "target")
			source := familyJellyfinSource(t, owner)
			tx, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal("begin own held relation")
			}
			defer tx.Rollback(context.Background())
			if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
				t.Fatal("hold actual authority relation")
			}
			barrier.armed.Store(true)
			checked := make(chan error, 1)
			go func() {
				if operation == "Check" {
					checked <- source.Check(t.Context())
				} else {
					_, err := source.Info(t.Context())
					checked <- err
				}
			}()
			familyLocalWaitSQL(t, f, "FROM accounts a WHERE a.id=")
			closed := make(chan struct{})
			go func() { owner.Close(); close(closed) }()
			select {
			case <-barrier.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("actual cancelled PG completion did not reach its join barrier")
			}
			select {
			case <-closed:
				t.Fatal("owner refunded while standalone source PG work was still running")
			default:
			}
			familyMediaAdmission.Lock()
			_, charged := familyMediaAdmission.owners[owner]
			familyMediaAdmission.Unlock()
			if !charged {
				t.Fatal("actual original account admission was refunded early")
			}
			unblock()
			select {
			case err := <-checked:
				if err == nil {
					t.Fatal("closed standalone source check authorized")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("actual standalone source check did not join")
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("actual original owner close did not finish after query join")
			}
		})
	}
}

func TestFamilyJellyfinFreshCancelledQueryCannotRetireOriginalSource(t *testing.T) {
	f := familyJellyfinPG(t)
	r, release := familyMediaRequest(t, f, t.Context())
	owner := familyMediaCapture(t, f, r, "target", nil)
	if owner.Handoff(r) != nil {
		release()
		t.Fatal("actual original handoff failed")
	}
	release()
	source := familyJellyfinSource(t, owner)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal("begin own lock")
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal("hold real owner guard")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- source.Check(ctx) }()
	familyLocalWaitSQL(t, f, "FROM accounts a WHERE a.id=")
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("cancelled fresh query authorized")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled fresh query did not join")
	}
	if tx.Rollback(t.Context()) != nil {
		t.Fatal("release real relation")
	}
	if source.Context().Err() != nil || owner.Context().Err() != nil || source.Check(t.Context()) != nil {
		t.Fatal("departed fresh consumer retired a healthy original")
	}
}

// Delay delivery of the source's cancel-only link while preserving the real
// source Done/Err signal. This models scheduling delay without fabricating a
// successful PG result or changing the original owner's context/authority.
type familyJellyfinDelayedCancel struct {
	context.Context
	entered, release, done chan struct{}
	calls                  atomic.Int32
}

func (*familyJellyfinDelayedCancel) Value(any) any { return nil }
func (delayed *familyJellyfinDelayedCancel) AfterFunc(callback func()) func() bool {
	if delayed.calls.Add(1) != 1 {
		return context.AfterFunc(delayed.Context, callback)
	}
	return context.AfterFunc(delayed.Context, func() {
		defer close(delayed.done)
		close(delayed.entered)
		<-delayed.release
		callback()
	})
}

type familyJellyfinFinalTraceKey struct{}
type familyJellyfinFinalTrace struct {
	armed      atomic.Bool
	accounts   atomic.Int32
	invalidate func()
}

func (trace *familyJellyfinFinalTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	last := trace.armed.Load() && strings.Contains(data.SQL, "FROM accounts a WHERE a.id=") && trace.accounts.Add(1) == 4
	return context.WithValue(ctx, familyJellyfinFinalTraceKey{}, last)
}
func (trace *familyJellyfinFinalTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if last, _ := ctx.Value(familyJellyfinFinalTraceKey{}).(bool); last {
		trace.invalidate()
	}
}

func TestFamilyJellyfinFinalPGCheckRechecksSourceSignalBeforeDelayedCallback(t *testing.T) {
	trace := &familyJellyfinFinalTrace{}
	f := familyJellyfinTracedPG(t, trace)
	owner := familyJellyfinOwner(t, f, "target")
	source := familyJellyfinSource(t, owner)
	delayed := &familyJellyfinDelayedCancel{Context: source.ctx, entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	source.ctx = delayed
	var once sync.Once
	unblock := func() { once.Do(func() { close(delayed.release) }) }
	defer unblock()
	trace.invalidate = source.Invalidate
	trace.armed.Store(true)
	checked := make(chan error, 1)
	go func() { checked <- source.Check(t.Context()) }()
	select {
	case <-delayed.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("final actual owner query did not invalidate the source")
	}
	select {
	case err := <-checked:
		if err == nil {
			t.Fatal("final PG proof ignored source loss before its callback ran")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("final source signal was not independently rechecked")
	}
	if trace.accounts.Load() != 4 || delayed.calls.Load() != 1 || owner.Context().Err() != nil {
		t.Fatal("controlled query/callback boundary did not preserve the real original owner")
	}
	unblock()
	select {
	case <-delayed.done:
	case <-time.After(2 * time.Second):
		t.Fatal("controlled delayed callback did not finish")
	}
	trace.armed.Store(false)
	if owner.Check(t.Context()) != nil {
		t.Fatal("source-only final signal retired healthy original owner")
	}
}

func TestFamilyJellyfinOwnerCloseJoinsAcquisitionAndRetirementBeforeRefund(t *testing.T) {
	f := familyJellyfinPG(t)
	owner := familyJellyfinOwner(t, f, "target")
	source := familyJellyfinSource(t, owner)
	finish, err := source.BeginOpen(t.Context())
	if err != nil {
		t.Fatal("reserve original acquisition")
	}
	defer finish()
	entered, release, retired := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	if source.BindRetirement(func() { close(entered); <-release; close(retired) }, retired) != nil {
		t.Fatal("bind the original retirement barrier")
	}
	closed := make(chan struct{})
	go func() { owner.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("owner closed before actual acquisition completion")
	case <-time.After(20 * time.Millisecond):
	}
	finish()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("original retirement was not called")
	}
	familyMediaAdmission.Lock()
	_, charged := familyMediaAdmission.owners[owner]
	familyMediaAdmission.Unlock()
	if !charged {
		t.Fatal("original admission refunded before actual retirement")
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("owner close did not join retirement")
	}
	if source.Check(t.Context()) == nil {
		t.Fatal("closed source remained current")
	}
}

type familyJellyfinClaimedRepository struct{ RemoteMediaRepository }

func TestFamilyJellyfinOpaqueMetadataCopiesAndRepositoryReadiness(t *testing.T) {
	var zero FamilyJellyfinSource
	if json.Unmarshal([]byte("{}"), &zero) == nil || zero.Check(t.Context()) == nil || zero.Context().Err() == nil || !zero.ExpiresAt().IsZero() || zero.ReadyForStore(nil) {
		t.Fatal("imported/zero source acquired authority")
	}
	f := familyJellyfinPG(t)
	owner := familyJellyfinOwner(t, f, "target")
	source := familyJellyfinSource(t, owner)
	copy := *source
	copy.Close()
	if copy.Check(t.Context()) == nil || source.Check(t.Context()) != nil || !source.ReadyForStore(f.ds) || source.ReadyForStore(&DataStore{}) {
		t.Fatal("copy or foreign store gained/retired original permission")
	}
	for _, value := range []any{source, *source, copy, &copy} {
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("opaque source serialized")
		}
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyJellyfinSource(opaque)" {
				t.Fatal("opaque source exposed private original data")
			}
		}
	}
	info, err := source.Info(t.Context())
	if err != nil {
		t.Fatal("read safe copied metadata")
	}
	info.IntegrationID = "another-integration"
	current, err := source.Info(t.Context())
	if err != nil || current.IntegrationID != "integration" {
		t.Fatal("metadata copy retargeted original")
	}
	for _, value := range []any{current, &current} {
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" || fmt.Sprintf("%#v", value) != "FamilyJellyfinSourceInfo(metadata)" {
			t.Fatal("metadata representation exposed selection details")
		}
	}
	if !f.ds.FamilyRemoteRepositoryReady(f.ds.RemoteMedia()) || f.ds.FamilyRemoteRepositoryReady(&pgRemoteMediaRepo{}) || f.ds.FamilyRemoteRepositoryReady(&familyJellyfinClaimedRepository{}) || (&DataStore{}).FamilyRemoteRepositoryReady(f.ds.RemoteMedia()) {
		t.Fatal("readiness did not bind the concrete actual pool")
	}
	foreign := familyJellyfinPG(t)
	if f.ds.FamilyRemoteRepositoryReady(foreign.ds.RemoteMedia()) || source.ReadyForStore(foreign.ds) {
		t.Fatal("foreign actual store claimed original source readiness")
	}
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal("begin actual transaction wrapper")
	}
	defer tx.Rollback(context.Background())
	if f.ds.FamilyRemoteRepositoryReady(&pgRemoteMediaRepo{pool: tx}) {
		t.Fatal("transaction wrapper claimed original pool readiness")
	}
}
