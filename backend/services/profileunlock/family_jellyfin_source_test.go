package profileunlock

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"novastream/internal/datastore"
)

func TestFamilyJellyfinFacadeRejectsImportedAndUnownedSources(t *testing.T) {
	var source FamilyJellyfinSource
	if json.Unmarshal([]byte("{}"), &source) == nil || source.Check(t.Context()) == nil || source.Context().Err() == nil || source.ReadyForStore(nil) || !source.ExpiresAt().IsZero() {
		t.Fatal("zero/imported Jellyfin facade acquired authority")
	}
	if finish, err := source.BeginOpen(t.Context()); finish != nil || err == nil {
		t.Fatal("unowned facade reserved acquisition")
	}
	if source.BindRetirement(func() {}, make(chan struct{})) == nil {
		t.Fatal("unowned facade bound cleanup")
	}
	if _, err := source.Info(t.Context()); err == nil {
		t.Fatal("unowned facade returned authoritative metadata")
	}
	var owner FamilyMediaOwner
	if remote, err := owner.CaptureJellyfinSource(t.Context(), "remote-item"); remote != nil || err == nil {
		t.Fatal("unowned facade minted a remote source")
	}
	source.Invalidate()
	source.Close()
}

func TestFamilyJellyfinFacadeUsesActualOriginalStoreAndSharedSourceLifetime(t *testing.T) {
	f := issuerFacadePG(t)
	for _, query := range []string{
		`INSERT INTO remote_media_libraries(id,name,library_type,provider,account_id,external_library_id)VALUES('remote-library','Synthetic Jellyfin','movie','jellyfin','integration','33333333444455556666777777777777')`,
		`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('remote-library','all')`,
		`INSERT INTO remote_media_items(id,library_id,external_item_id,external_media_id,group_key,library_type,title,file_name,size_bytes)VALUES('remote-item','remote-library','11111111-2222-3333-4444-555555555555','22222222333344445555666666666666','synthetic-group','movie','Synthetic Title','file.bin',32)`,
		`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('local-library','Synthetic LOCAL','movie','/synthetic')`,
		`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('local-library','all')`,
		`INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('local-item','local-library','file.bin','/synthetic/file.bin','file.bin','movie',32)`,
	} {
		if _, err := f.ds.Pool().Exec(t.Context(), query); err != nil {
			t.Fatal("seed actual source catalogue")
		}
	}
	pins := NewPGProfilePins(f.ds)
	request := publicFacadeRequest(t, f, pins)
	owner, err := pins.CaptureFamilyMediaOwner(request, "target")
	if err != nil {
		t.Fatal("capture actual original owner")
	}
	defer owner.Close()
	source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item")
	if err != nil {
		t.Fatal("capture actual original Jellyfin source")
	}
	defer source.Close()
	if !source.ReadyForStore(f.ds) || source.ReadyForStore(&datastore.DataStore{}) || !source.ExpiresAt().Equal(owner.ExpiresAt()) {
		t.Fatal("facade lost exact original store or fixed expiry")
	}
	copy := *source
	copy.Close()
	copy.Invalidate()
	if copy.Check(t.Context()) == nil || source.Check(t.Context()) != nil {
		t.Fatal("copied facade gained or retired original authority")
	}
	for _, value := range []any{source, *source, copy, &copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyJellyfinSource(opaque)" {
				t.Fatal("facade exposed private original source data")
			}
		}
		if raw, err := json.Marshal(value); err != nil || string(raw) != "null" {
			t.Fatal("facade serialized source authority")
		}
	}
	info, err := source.Info(t.Context())
	if err != nil || info.PrimaryGUID != "11111111222233334444555555555555" || info.IntegrationID != "integration" {
		t.Fatal("facade did not retain raw original PG selection")
	}
	info.IntegrationID = "other"
	again, err := source.Info(t.Context())
	if err != nil || again.IntegrationID != "integration" {
		t.Fatal("copied metadata retargeted original facade")
	}
	if local, err := owner.CaptureLocalSource(t.Context(), "local-item"); local != nil || !errors.Is(err, ErrFamilyMediaBusy) {
		t.Fatal("facade opened a second provider on the same original")
	}
	finish, err := source.BeginOpen(t.Context())
	if err != nil || finish == nil {
		t.Fatal("actual original acquisition could not be reserved")
	}
	defer finish()
	retired := make(chan struct{})
	if source.BindRetirement(func() { close(retired) }, retired) != nil {
		t.Fatal("original cleanup binding failed")
	}
	finish()
	owner.Close()
	select {
	case <-retired:
	default:
		t.Fatal("owner did not join the original source cleanup")
	}
	if source.Check(t.Context()) == nil || source.Context().Err() == nil {
		t.Fatal("owner close left facade source current")
	}
}
