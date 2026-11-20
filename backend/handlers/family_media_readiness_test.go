package handlers

import (
	"errors"
	"testing"
	"time"

	"novastream/internal/parentauth"
	"novastream/services/accounts"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
)

func familyReadinessDependencies(t *testing.T, fixture *publicPinCarrierFixture) FamilyMediaDependencies {
	t.Helper()
	local, err := localmedia.NewService(fixture.store, nil, "/synthetic-never-probe")
	if err != nil {
		t.Fatal("construct own LOCAL fixture")
	}
	account, err := accounts.NewServiceWithStore(fixture.store)
	if err != nil {
		t.Fatal("construct own actual account service")
	}
	return FamilyMediaDependencies{Store: fixture.store, Pins: fixture.pins, Local: local, Parent: parentauth.New(account)}
}

func TestFamilyInstallerReadinessRequiresAllActualSameStoreComponents(t *testing.T) {
	first, second := newPublicPinCarrierFixture(t), newPublicPinCarrierFixture(t)
	good, foreign := familyReadinessDependencies(t, first), familyReadinessDependencies(t, second)
	if _, supported := any(good.Local).(familyLocalRawOpener); !supported {
		if installed, err := InstallFamilyMedia(good); installed != nil || !errors.Is(err, profileunlock.ErrFamilyMediaUnavailable) {
			t.Fatal("unsupported LOCAL platform constructed installation")
		}
		return
	}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic readiness JSON parent password")
	jsonAccounts, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal("create own legacy JSON account fixture")
	}
	for _, test := range []struct {
		name   string
		change func(*FamilyMediaDependencies)
	}{
		{"missing local", func(d *FamilyMediaDependencies) { d.Local = nil }},
		{"zero local", func(d *FamilyMediaDependencies) { d.Local = &localmedia.Service{} }},
		{"foreign local", func(d *FamilyMediaDependencies) { d.Local = foreign.Local }},
		{"missing parent", func(d *FamilyMediaDependencies) { d.Parent = nil }},
		{"zero parent", func(d *FamilyMediaDependencies) { d.Parent = &parentauth.Guard{} }},
		{"zero concrete accounts", func(d *FamilyMediaDependencies) { d.Parent = parentauth.New(&accounts.Service{}) }},
		{"foreign parent", func(d *FamilyMediaDependencies) { d.Parent = foreign.Parent }},
		{"JSON parent", func(d *FamilyMediaDependencies) { d.Parent = parentauth.New(jsonAccounts) }},
		{"foreign pins", func(d *FamilyMediaDependencies) { d.Pins = foreign.Pins }},
		{"foreign expected store", func(d *FamilyMediaDependencies) { d.Store = foreign.Store }},
	} {
		t.Run(test.name, func(t *testing.T) {
			deps := good
			test.change(&deps)
			if installed, err := InstallFamilyMedia(deps); installed != nil || !errors.Is(err, profileunlock.ErrFamilyMediaUnavailable) {
				t.Fatal("mismatched family wiring constructed an installation")
			}
		})
	}
	installed, err := InstallFamilyMedia(good)
	if err != nil || installed == nil || installed.Boundary == nil || installed.ParentFiles == nil {
		t.Fatal("actual same-store installation unavailable", err)
	}
	if installed.ParentFiles.store != first.store || installed.ParentFiles.pins != first.pins || installed.ParentFiles.local != good.Local || installed.ParentFiles.parent != good.Parent || installed.ParentFiles.hlsManager() != nil || installed.Boundary.boundary.original == nil {
		t.Fatal("readiness replaced actual source/parent wiring or activated HLS")
	}
}

func TestFamilyInstallerReadinessDoesNotQueryOrChallengeAtConstruction(t *testing.T) {
	fixture := newPublicPinCarrierFixture(t)
	deps := familyReadinessDependencies(t, fixture)
	if _, supported := any(deps.Local).(familyLocalRawOpener); !supported {
		if installed, err := InstallFamilyMedia(deps); installed != nil || !errors.Is(err, profileunlock.ErrFamilyMediaUnavailable) {
			t.Fatal("unsupported LOCAL platform constructed installation")
		}
		return
	}
	tx, err := fixture.store.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	type result struct {
		installation *FamilyMediaInstallation
		err          error
	}
	finished := make(chan result, 1)
	go func() { v, err := InstallFamilyMedia(deps); finished <- result{v, err} }()
	select {
	case got := <-finished:
		if got.err != nil || got.installation == nil {
			t.Fatal("actual pure constructor denied", got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness queried account rows or attempted authorization")
	}
}
