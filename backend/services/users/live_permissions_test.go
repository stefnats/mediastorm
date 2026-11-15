package users_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"novastream/services/users"
)

func TestLivePermissionsDefaultOffPersistAndRemainIndependent(t *testing.T) {
	dir := t.TempDir()
	service, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount("household", "Synthetic viewer")
	if err != nil {
		t.Fatal(err)
	}
	if profile.AllowLiveTakeover || profile.AllowRecordingInterruption {
		t.Fatal("new profile has takeover rights")
	}
	for _, rights := range [][2]bool{{true, false}, {false, true}, {true, true}, {false, false}} {
		if _, err := service.SetLivePermissions(profile.ID, rights[0], rights[1]); err != nil {
			t.Fatal(err)
		}
		reopened, err := users.NewService(dir)
		if err != nil {
			t.Fatal(err)
		}
		got, ok := reopened.Get(profile.ID)
		if !ok || got.AllowLiveTakeover != rights[0] || got.AllowRecordingInterruption != rights[1] {
			t.Fatal("rights were not independently persisted")
		}
	}
}

func TestLivePermissionsRejectKidsAndMissingProfiles(t *testing.T) {
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	child, err := service.CreateForAccount("household", "Synthetic child")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetKidsProfile(child.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetLivePermissions(child.ID, false, true); !errors.Is(err, users.ErrLivePermissions) {
		t.Fatal("child granted")
	}
	if _, err := service.SetLivePermissions("missing", true, true); !errors.Is(err, users.ErrUserNotFound) {
		t.Fatal(err)
	}
	if _, err := service.SetLivePermissions(child.ID, false, false); err != nil {
		t.Fatal("could not revoke child rights")
	}
}

func TestLivePermissionsRollbackAfterPersistenceFailure(t *testing.T) {
	dir := t.TempDir()
	service, err := users.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount("household", "Synthetic viewer")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "users.json.tmp"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetLivePermissions(profile.ID, true, true); err == nil {
		t.Fatal("expected persistence failure")
	}
	got, _ := service.Get(profile.ID)
	if got.AllowLiveTakeover || got.AllowRecordingInterruption || !got.UpdatedAt.Equal(profile.UpdatedAt) {
		t.Fatal("failed permission write changed live state")
	}
}

func TestLivePermissionsAreRevokedOnAccountTransferAndKidsConversion(t *testing.T) {
	service, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := service.CreateForAccount("first", "Synthetic viewer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SetLivePermissions(profile.ID, true, true); err != nil {
		t.Fatal(err)
	}
	moved, err := service.Reassign(profile.ID, "second")
	if err != nil {
		t.Fatal(err)
	}
	if moved.AllowLiveTakeover || moved.AllowRecordingInterruption {
		t.Fatal("rights followed profile into a different account")
	}
	if _, err := service.SetLivePermissions(profile.ID, true, true); err != nil {
		t.Fatal(err)
	}
	child, err := service.SetKidsProfile(profile.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if child.AllowLiveTakeover || child.AllowRecordingInterruption {
		t.Fatal("kids conversion retained rights")
	}
	adult, err := service.SetKidsProfile(profile.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if adult.AllowLiveTakeover || adult.AllowRecordingInterruption {
		t.Fatal("disabling kids mode restored old grants")
	}
}
