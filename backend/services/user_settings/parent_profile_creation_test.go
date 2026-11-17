package user_settings

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
)

func creationSettingsIntent(t *testing.T, f *settingsFlagFixture) (*datastore.ParentProfileCreationIntent, context.Context) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(t.Context())
	r.Header.Set("X-Client-ID", f.session.ClientID)
	r, stop, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	body := []byte(`{"name":"Synthetic child","isKidsProfile":true}`)
	i, err := f.ds.PrepareParentProfileCreationManagement(r, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(i.Close)
	if err = f.ds.BindParentProfileCreationManagementRetry(r, i, body, nil); err != nil {
		t.Fatal(err)
	}
	r, err = f.ds.BoundParentProfileCreationRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", settingsFlagPassword)
	if err = f.ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	return i, r.Context()
}
func TestParentProfileCreationSettingsNewTargetACKPreservesForeignDirtyAndUnknown(t *testing.T) {
	mutatorsNoHTTP(t)
	f := settingsFlagSetup(t, `{"display":{"simpleMode":false},"future":{"n":9007199254740993}}`)
	f.settings.mu.Lock()
	dirty := f.settings.settings["target"]
	dirty.Metadata.PrimaryLanguage = "ita"
	f.settings.settings["target"] = dirty
	record := f.settings.pg.records["target"]
	f.settings.markUserSettingsUnknownLocked([]string{"other"})
	foreign := f.settings.pg.unknownMarkers["other"]
	f.settings.mu.Unlock()
	i, ctx := creationSettingsIntent(t, f)
	u, err := f.profiles.AcquireParentProfileCreationLease(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	v, err := f.settings.AcquireParentProfileCreationDefaultsLease(ctx, i, u.ReceiptLease())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	receipt, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	created, err := u.Stage(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err = v.Stage(receipt); err != nil {
		t.Fatal(err)
	}
	if err = f.ds.TakeParentProfileCreationReceipt(i, receipt, u.ReceiptLease(), v.ReceiptLease()); err != nil {
		t.Fatal(err)
	}
	u.Complete()
	v.Complete()
	v.Close()
	u.Close()
	prefs, err := f.settings.Get(created.ID)
	if err != nil || prefs == nil || prefs.Display.SimpleMode == nil || !*prefs.Display.SimpleMode {
		t.Fatal("strict new settings not installed", err)
	}
	f.settings.mu.RLock()
	same := reflect.DeepEqual(f.settings.settings["target"], dirty) && f.settings.pg.records["target"].Revision == record.Revision && f.settings.pg.unknownMarkers["other"] == foreign && f.settings.pg.uncertain["other"]
	f.settings.mu.RUnlock()
	if !same {
		t.Fatal("new-target ACK changed foreign dirty/unknown state")
	}
}
func TestParentProfileCreationSettingsAbsenceDoesNotAcquireOrRebase(t *testing.T) {
	mutatorsNoHTTP(t)
	for _, kind := range []string{"dirty", "record", "confirmed", "unknown", "copy users"} {
		t.Run(kind, func(t *testing.T) {
			f := settingsFlagSetup(t, "")
			i, ctx := creationSettingsIntent(t, f)
			target, _, err := f.ds.ParentProfileCreationTarget(ctx, i)
			if err != nil {
				t.Fatal(err)
			}
			u, err := f.profiles.AcquireParentProfileCreationLease(ctx, i)
			if err != nil {
				t.Fatal(err)
			}
			defer u.Close()
			f.settings.mu.Lock()
			switch kind {
			case "dirty":
				f.settings.settings[target] = models.UserSettings{}
			case "record":
				f.settings.pg.records[target] = datastore.UserSettingsRecord{ID: target}
			case "confirmed":
				f.settings.pg.confirmed[target] = models.UserSettings{}
			case "unknown":
				f.settings.markUserSettingsUnknownLocked([]string{target})
			}
			f.settings.mu.Unlock()
			proof := u.ReceiptLease()
			if kind == "copy users" {
				c := *proof
				proof = &c
			}
			before := f.ds.Pool().Stat().AcquireCount()
			l, err := f.settings.AcquireParentProfileCreationDefaultsLease(ctx, i, proof)
			if l != nil {
				l.Close()
			}
			if err == nil || l != nil {
				t.Fatal("existing local settings acquired for new target", kind)
			}
			if f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("absent new User triggered SQL acquisition")
			}
		})
	}
}
func TestParentProfileCreationSettingsOperationMarkersBlockBothGenericResetPaths(t *testing.T) {
	mutatorsNoHTTP(t)
	f := settingsFlagSetup(t, "")
	i, ctx := creationSettingsIntent(t, f)
	target, operation, err := f.ds.ParentProfileCreationTarget(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	u, err := f.profiles.AcquireParentProfileCreationLease(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	v, err := f.settings.AcquireParentProfileCreationDefaultsLease(ctx, i, u.ReceiptLease())
	if err != nil {
		t.Fatal(err)
	}
	v.Quarantine()
	v.Close()
	u.Close()
	f.settings.mu.Lock()
	marker := f.settings.pg.unknownMarkers[target]
	f.settings.markUserSettingsUnknownLocked([]string{"other"})
	foreign := f.settings.pg.unknownMarkers["other"]
	f.settings.markUserSettingsUnknownLocked([]string{target})
	successor := f.settings.pg.unknownMarkers[target]
	f.settings.mu.Unlock()
	if marker == nil || marker.creation != operation || successor == marker || successor.creation != operation {
		t.Fatal("operation marker not bound")
	}
	read, err := f.ds.PrepareProfileRecovery(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	if ticket, err := f.settings.PrepareUserSettingsRecovery(t.Context(), read); err == nil || ticket != nil {
		if ticket != nil {
			ticket.Close()
		}
		t.Fatal("generic recovery accepted creation-owned marker")
	}
	before := f.ds.Pool().Stat().AcquireCount()
	if err = f.settings.ResyncUserSettings(t.Context()); !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("whole reset accepted creation uncertainty", err)
	}
	f.settings.mu.Lock()
	f.settings.installPGRecordsLocked(nil)
	same := f.settings.pg.unknownMarkers[target] == successor && f.settings.pg.unknownMarkers["other"] == foreign && f.settings.pg.uncertain[target] && f.settings.pg.uncertain["other"]
	f.settings.mu.Unlock()
	if !same || f.ds.Pool().Stat().AcquireCount() != before {
		t.Fatal("generic reset cleared markers or performed unnecessary SQL")
	}
	if got, err := f.settings.Get(target); got != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("quarantined target became readable", err)
	}
}
func TestParentProfileCreationSettingsSuccessorMarkerAfterReadCannotBeConsumed(t *testing.T) {
	mutatorsNoHTTP(t)
	f := settingsFlagSetup(t, "")
	read, err := f.ds.PrepareProfileRecovery(t.Context(), "target")
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	ticket, err := f.settings.PrepareUserSettingsRecovery(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	defer ticket.Close()
	snapshot, err := f.ds.ReadProfileRecovery(read)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	f.settings.mu.Lock()
	f.settings.markUserSettingsUnknownLocked([]string{"target"})
	marker := f.settings.pg.unknownMarkers["target"]
	marker.creation = "synthetic-later-operation"
	f.settings.mu.Unlock()
	if err = f.settings.ValidateUserSettingsRecovery(t.Context(), ticket, snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("late operation marker not detected", err)
	}
	if _, err = f.settings.InstallUserSettingsRecovery(t.Context(), ticket, snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("stale generic recovery adopted operation outcome", err)
	}
	f.settings.mu.RLock()
	same := f.settings.pg.unknownMarkers["target"] == marker
	f.settings.mu.RUnlock()
	if !same {
		t.Fatal("later marker lost")
	}
}
