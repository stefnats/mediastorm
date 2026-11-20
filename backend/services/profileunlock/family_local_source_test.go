package profileunlock

import (
	"encoding/json"
	"fmt"
	"testing"

	"novastream/internal/datastore"
)

func TestFamilyLocalFacadeImportedCopiesAndMetadataAreNotAuthority(t *testing.T) {
	var source FamilyLocalSource
	if json.Unmarshal([]byte("{}"), &source) == nil || source.Check(t.Context()) == nil || source.Context().Err() == nil || source.ReadyForStore(nil) || !source.ExpiresAt().IsZero() {
		t.Fatal("zero/imported local source acquired authority")
	}
	if finish, e := source.BeginOpen(t.Context()); e == nil || finish != nil {
		t.Fatal("imported source opened file")
	}
	if source.BindRetirement(func() {}, make(chan struct{})) == nil {
		t.Fatal("imported source bound retirement")
	}
	var owner FamilyMediaOwner
	if local, e := owner.CaptureLocalSource(t.Context(), "item"); e == nil || local != nil {
		t.Fatal("imported owner minted local source")
	}
}
func TestFamilyLocalFacadeExactStoreAndActualOriginalLifetime(t *testing.T) {
	f := issuerFacadePG(t)
	for _, sql := range []string{
		`INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('library','Synthetic Library','movie','/synthetic')`,
		`INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('library','all')`,
		`INSERT INTO local_media_items(id,library_id,relative_path,file_path,file_name,library_type,size_bytes)VALUES('item','library','file.bin','/synthetic/file.bin','file.bin','movie',32)`,
	} {
		if _, e := f.ds.Pool().Exec(t.Context(), sql); e != nil {
			t.Fatal(e)
		}
	}
	pins := NewPGProfilePins(f.ds)
	request := publicFacadeRequest(t, f, pins)
	owner, e := pins.CaptureFamilyMediaOwner(request, "target")
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Close()
	source, e := owner.CaptureLocalSource(t.Context(), "item")
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	if !source.ReadyForStore(f.ds) || source.ReadyForStore(&datastore.DataStore{}) || !source.ExpiresAt().Equal(owner.ExpiresAt()) {
		t.Fatal("original exact store/deadline lost")
	}
	copy := *source
	copy.Close()
	if copy.Check(t.Context()) == nil || source.Check(t.Context()) != nil {
		t.Fatal("copied facade granted or retired original")
	}
	for _, value := range []any{source, *source, copy, &copy} {
		for _, format := range []string{"%+v", "%#v"} {
			if fmt.Sprintf(format, value) != "FamilyLocalSource(opaque)" {
				t.Fatal("opaque facade exposed private source")
			}
		}
		raw, e := json.Marshal(value)
		if e != nil || string(raw) != "null" {
			t.Fatal("facade serialized authority")
		}
	}
	owner.Close()
	if source.Check(t.Context()) == nil || source.Context().Err() == nil {
		t.Fatal("owner close left facade authority live")
	}
}
