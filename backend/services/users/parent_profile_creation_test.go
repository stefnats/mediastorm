package users

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/user_settings"
)

func creationUsersIntent(t *testing.T, ds *datastore.DataStore, session models.Session) (*datastore.ParentProfileCreationIntent, context.Context, context.CancelFunc) {
	t.Helper()
	parent, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(parent)
	r.Header.Set("X-Client-ID", session.ClientID)
	r, release, err := ds.AuthenticateProfilePinRequest(r, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	body := []byte(`{"name":"Synthetic newly created child","accountId":"master","isKidsProfile":true}`)
	i, err := ds.PrepareParentProfileCreationManagement(r, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(i.Close)
	if err = ds.BindParentProfileCreationManagementRetry(r, i, body, nil); err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentProfileCreationRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("X-Account-Password", modeUserPassword)
	if err = ds.VerifyParentProfileCreationPassword(r, i); err != nil {
		t.Fatal(err)
	}
	return i, r.Context(), cancel
}
func TestParentProfileCreationUsersHistoricalPairACKAfterLateCancel(t *testing.T) {
	creationUsersNoHTTP(t)
	f, s, other, session := modeUsersSetup(t)
	settings, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	dirty := s.users[other]
	dirty.Color = "unsaved original color"
	s.users[other] = dirty
	revision := s.persistence.revisions[other]
	s.mu.Unlock()
	i, ctx, cancel := creationUsersIntent(t, f.ds, session)
	u, err := s.AcquireParentProfileCreationLease(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	v, err := settings.AcquireParentProfileCreationDefaultsLease(ctx, i, u.ReceiptLease())
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
	cancel()
	u.Complete()
	v.Complete()
	v.Close()
	u.Close()
	saved, ok := s.Get(created.ID)
	if !ok || !reflect.DeepEqual(saved, created) {
		t.Fatal("known user ACK skipped after cancel")
	}
	prefs, err := settings.Get(created.ID)
	if err != nil || prefs == nil || prefs.Display.SimpleMode == nil || !*prefs.Display.SimpleMode {
		t.Fatal("known second ACK skipped after cancel", err)
	}
	s.mu.RLock()
	untouched := s.users[other]
	actualRevision := s.persistence.revisions[other]
	s.mu.RUnlock()
	if !reflect.DeepEqual(untouched, dirty) || actualRevision != revision {
		t.Fatal("new-target ACK replaced unrelated dirty profile")
	}
}
func TestParentProfileCreationUsersAbsenceLeaseRejectsLocalSuccessors(t *testing.T) {
	creationUsersNoHTTP(t)
	for _, kind := range []string{"current", "confirmed", "tombstone", "unknown", "context", "copy"} {
		t.Run(kind, func(t *testing.T) {
			f, s, _, session := modeUsersSetup(t)
			i, ctx, _ := creationUsersIntent(t, f.ds, session)
			target, _, err := f.ds.ParentProfileCreationTarget(ctx, i)
			if err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			switch kind {
			case "current":
				s.users[target] = models.User{ID: target, Name: "pending"}
			case "confirmed":
				s.persistence.confirmed[target] = models.User{ID: target}
			case "tombstone":
				s.persistence.revisions[target] = datastore.UserRevision{}
			case "unknown":
				s.markStorageUnknownLocked([]string{target})
			}
			s.mu.Unlock()
			if kind == "context" {
				ctx = context.WithValue(ctx, struct{}{}, true)
			}
			if kind == "copy" {
				c := *i
				i = &c
			}
			before := f.ds.Pool().Stat().AcquireCount()
			l, err := s.AcquireParentProfileCreationLease(ctx, i)
			if l != nil {
				l.Close()
			}
			if err == nil || l != nil {
				t.Fatal("local absence/scope mismatch accepted", kind)
			}
			if f.ds.Pool().Stat().AcquireCount() != before {
				t.Fatal("invalid local lease performed SQL")
			}
		})
	}
}
func TestParentProfileCreationUsersOperationMarkerSurvivesGenericRecoveryAndReset(t *testing.T) {
	creationUsersNoHTTP(t)
	f, s, other, session := modeUsersSetup(t)
	i, ctx, _ := creationUsersIntent(t, f.ds, session)
	target, operation, err := f.ds.ParentProfileCreationTarget(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	u, err := s.AcquireParentProfileCreationLease(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	u.Quarantine()
	u.Close()
	s.mu.Lock()
	marker := s.persistence.unknownTargets[target]
	s.markStorageUnknownLocked([]string{other})
	foreign := s.persistence.unknownTargets[other]
	s.persistence.unknownUnscoped = true
	s.markStorageUnknownLocked([]string{target})
	successor := s.persistence.unknownTargets[target]
	s.mu.Unlock()
	if marker == nil || marker.creation != operation || successor == marker || successor.creation != operation {
		t.Fatal("operation-owned marker absent")
	}
	read, err := f.ds.PrepareProfileRecovery(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	if ticket, err := s.PrepareStorageRecovery(t.Context(), read); err == nil || ticket != nil {
		if ticket != nil {
			ticket.Close()
		}
		t.Fatal("generic recovery acquired creation-owned target")
	}
	if err = s.ResyncStorage(t.Context()); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("whole resync cleared operation evidence", err)
	}
	s.mu.Lock()
	s.installDatabaseSnapshotLocked(nil)
	same := s.persistence.unknownTargets[target] == successor && s.persistence.unknownTargets[other] == foreign && s.persistence.unknownUnscoped && s.persistence.uncertain
	s.mu.Unlock()
	if !same {
		t.Fatal("reset or successor marker erased operation/foreign/unscoped uncertainty")
	}
	if _, err = s.Rename(other, "must remain blocked"); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("global Users write gate weakened", err)
	}
}
func TestParentProfileCreationUsersMarkerArrivingAfterReadCaptureRejectsInstall(t *testing.T) {
	creationUsersNoHTTP(t)
	f, s, target, _ := modeUsersSetup(t)
	read, err := f.ds.PrepareProfileRecovery(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	ticket, err := s.PrepareStorageRecovery(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	defer ticket.Close()
	snapshot, err := f.ds.ReadProfileRecovery(read)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	s.mu.Lock()
	s.markStorageUnknownLocked([]string{target})
	marker := s.persistence.unknownTargets[target]
	marker.creation = "synthetic-successor-operation"
	s.mu.Unlock()
	if err = s.ValidateStorageRecovery(t.Context(), ticket, snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("new creation marker not detected", err)
	}
	if _, err = s.InstallStorageRecovery(t.Context(), ticket, snapshot); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("stale generic install removed marker", err)
	}
	s.mu.RLock()
	retained := s.persistence.unknownTargets[target] == marker
	s.mu.RUnlock()
	if !retained {
		t.Fatal("successor marker lost")
	}
}

func TestParentProfileCreationUsersRealLostClaimAckQuarantinesBoth(t *testing.T) {
	creationUsersNoHTTP(t)
	f, _, other, session := modeUsersSetup(t)
	uri, proxy := usersLostCommitProxy(t, f)
	ds, err := datastore.New(t.Context(), uri, datastore.WithMaxConns(3), datastore.WithMinConns(1))
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	s := f.service(t, ds)
	settings, err := user_settings.NewServiceWithStore(ds)
	if err != nil {
		t.Fatal(err)
	}
	i, ctx, _ := creationUsersIntent(t, ds, session)
	target, operation, err := ds.ParentProfileCreationTarget(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	u, err := s.AcquireParentProfileCreationLease(ctx, i)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	v, err := settings.AcquireParentProfileCreationDefaultsLease(ctx, i, u.ReceiptLease())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	proxy.drop.Store(true)
	receipt, err := u.Commit()
	if receipt != nil || !errors.Is(err, datastore.ErrParentProfileCreationUnknown) || !proxy.dropped.Load() {
		t.Fatal("actual claim COMMIT ACK not unknown", err)
	}
	u.Quarantine()
	v.Quarantine()
	v.Close()
	u.Close()
	s.mu.RLock()
	marker := s.persistence.unknownTargets[target]
	s.mu.RUnlock()
	if marker == nil || marker.creation != operation {
		t.Fatal("uncertain operation marker absent")
	}
	if value, err := settings.Get(target); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("paired Settings uncertainty absent", err)
	}
	var count int
	var state string
	if err = f.ds.Pool().QueryRow(t.Context(), `SELECT (SELECT count(*) FROM users WHERE id=$1),state FROM profile_creation_operations WHERE operation_id=$2`, target, operation).Scan(&count, &state); err != nil || count != 0 || state != "attempting" {
		t.Fatal("claim loss confused with Create loss", count, state, err)
	}
	if _, err = s.Rename(other, "blocked"); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("global write stop removed", err)
	}
	if err = s.ResyncStorage(t.Context()); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("generic reset cleared unknown creation", err)
	}
	if _, err = u.Commit(); err == nil {
		t.Fatal("uncertain claim replayed")
	}
}

type creationUsersDenyHTTP struct{}

func (creationUsersDenyHTTP) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("HTTP denied in synthetic creation persistence fixture")
}
func creationUsersNoHTTP(t *testing.T) {
	t.Helper()
	before := http.DefaultTransport
	http.DefaultTransport = creationUsersDenyHTTP{}
	t.Cleanup(func() { http.DefaultTransport = before })
}
