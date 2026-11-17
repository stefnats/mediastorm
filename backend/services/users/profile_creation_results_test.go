package users

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/user_settings"
)

type creationResultUsersFixture struct {
	fixture                           *usersPGFixture
	users                             *Service
	settings                          *user_settings.Service
	session                           models.Session
	target, operation, locator, other string
}

// This fixture knows the actual Create COMMIT succeeded. It deliberately omits
// the cache receipt ACK and quarantines both owners; it does not simulate a lost
// database acknowledgement. Actual lost-COMMIT controls live in datastore tests.
func creationResultUsers(t *testing.T) *creationResultUsersFixture {
	t.Helper()
	creationUsersNoHTTP(t)
	f, service, other, session := modeUsersSetup(t)
	prefs, err := user_settings.NewServiceWithStore(f.ds)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(t.Context())
	request.Header.Set("X-Client-ID", session.ClientID)
	request, release, err := f.ds.AuthenticateProfilePinRequest(request, session.Token, session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	body := []byte(`{"name":"Initial creation result child","accountId":"master","isKidsProfile":true}`)
	intent, err := f.ds.PrepareParentProfileCreationManagement(request, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	locator, err := f.ds.ParentProfileCreationLocator(intent)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(datastore.ProfileCreationOperationHeader, locator)
	if err = f.ds.BindParentProfileCreationManagementRetry(request, intent, body, nil); err != nil {
		t.Fatal(err)
	}
	request, err = f.ds.BoundParentProfileCreationRequest(request, intent)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Account-Password", modeUserPassword)
	if err = f.ds.VerifyParentProfileCreationPassword(request, intent); err != nil {
		t.Fatal(err)
	}
	target, operation, err := f.ds.ParentProfileCreationTarget(request.Context(), intent)
	if err != nil {
		t.Fatal(err)
	}
	u, err := service.AcquireParentProfileCreationLease(request.Context(), intent)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	s, err := prefs.AcquireParentProfileCreationDefaultsLease(request.Context(), intent, u.ReceiptLease())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	receipt, err := u.Commit()
	if err != nil || receipt == nil {
		t.Fatal("synthetic actual Create did not commit", err)
	}
	u.Quarantine()
	s.Quarantine()
	s.Close()
	u.Close()
	if _, ok := service.Get(target); ok {
		t.Fatal("unacknowledged Create was installed")
	}
	if value, err := prefs.Get(target); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("paired settings not quarantined", err)
	}
	actual, err := f.ds.Users().Get(t.Context(), target)
	if err != nil || actual.ID != target {
		t.Fatal("fixture did not observe the actual committed new User", err)
	}
	return &creationResultUsersFixture{fixture: f, users: service, settings: prefs, session: session, target: target, operation: operation, locator: locator, other: other}
}

func (f *creationResultUsersFixture) read(t *testing.T) (*datastore.ProfileCreationRead, context.Context, context.CancelFunc) {
	t.Helper()
	parent, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles/creation-operations/"+f.locator+"/read", nil).WithContext(parent)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	r, release, err := f.fixture.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	read, err := f.fixture.ds.PrepareProfileCreationReadManagement(r, f.locator, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(read.Close)
	return read, r.Context(), cancel
}

func (f *creationResultUsersFixture) capture(t *testing.T, read *datastore.ProfileCreationRead, ctx context.Context) (*StorageRecoveryTicket, *user_settings.UserSettingsRecoveryTicket, *datastore.ProfileCreationResultSnapshot) {
	t.Helper()
	u, err := f.users.PrepareProfileCreationRecovery(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(u.Close)
	s, err := f.settings.PrepareProfileCreationRecovery(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	snapshot, err := f.fixture.ds.ReadProfileCreationResult(read)
	if err != nil || snapshot == nil || snapshot.Outcome() != "created" {
		t.Fatal("qualified current creation read failed", err)
	}
	return u, s, snapshot
}

func TestProfileCreationResultsUsersPairedCurrentACKPreservesForeignAndUnscoped(t *testing.T) {
	f := creationResultUsers(t)
	f.fixture.exec(t, `UPDATE users SET name='Current same-birth child',color='blue' WHERE id=$1`, f.target)
	f.fixture.exec(t, `UPDATE user_settings SET settings='{"display":{"simpleMode":false},"metadata":{"primaryLanguage":"fra"},"futureRoot":{"precise":9007199254740993}}'::jsonb WHERE user_id=$1`, f.target)
	f.users.mu.Lock()
	dirty := f.users.users[f.other]
	dirty.Name = "Unrelated local edit"
	f.users.users[f.other] = dirty
	confirmed := cloneUser(f.users.persistence.confirmed[f.other])
	revision := f.users.persistence.revisions[f.other]
	f.users.markStorageUnknownLocked([]string{f.other})
	foreign := f.users.persistence.unknownTargets[f.other]
	f.users.persistence.unknownUnscoped = true
	f.users.mu.Unlock()
	before := recoveryFingerprint(t, f.fixture)
	read, ctx, cancel := f.read(t)
	ut, st, snapshot := f.capture(t, read, ctx)
	ul, err := f.users.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer ul.Close()
	sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Close()
	profile, present, err := ul.Profile()
	if err != nil || !present || profile.Name != "Current same-birth child" || profile.Color != "blue" {
		t.Fatal("staged original Create values over newer same-birth User", err)
	}
	if ul.PairLease() != nil || sl.PairLease() != nil {
		t.Fatal("typed owners exposed generic paired authority")
	}
	if err = f.fixture.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
		t.Fatal(err)
	}
	cancel()
	us, ss := ul.Complete(), sl.Complete()
	if !us.TargetPresent || !us.PendingUncertainty || !ss.TargetPresent || ss.PendingUncertainty {
		t.Fatal("paired status lost unrelated Users-global uncertainty")
	}
	if ul.Complete() != us || sl.Complete() != ss {
		t.Fatal("historical ACK was not idempotent after cancellation")
	}
	sl.Close()
	ul.Close()
	f.users.mu.RLock()
	actual := f.users.users[f.target]
	intact := reflect.DeepEqual(f.users.users[f.other], dirty) && reflect.DeepEqual(f.users.persistence.confirmed[f.other], confirmed) && f.users.persistence.revisions[f.other] == revision && f.users.persistence.unknownTargets[f.other] == foreign && f.users.persistence.unknownTargets[f.target] == nil && f.users.persistence.unknownUnscoped && f.users.persistence.uncertain
	f.users.mu.RUnlock()
	settings, err := f.settings.Get(f.target)
	if actual.Name != "Current same-birth child" || !intact || err != nil || settings == nil || settings.Display.SimpleMode == nil || *settings.Display.SimpleMode || settings.Metadata.PrimaryLanguage != "fra" {
		t.Fatal("current paired ACK changed foreign state or skipped Settings", err)
	}
	if recoveryFingerprint(t, f.fixture) != before {
		t.Fatal("recovery wrote User, Settings or lifecycle records")
	}
	if _, err = f.users.Rename(f.other, "must stay blocked"); !errors.Is(err, ErrPersistenceUncertain) {
		t.Fatal("target recovery removed global Users write stop", err)
	}
	var retained int
	if err = f.fixture.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM profile_creation_results WHERE operation_id=$1`, f.operation).Scan(&retained); err != nil || retained != 1 {
		t.Fatal("one local cache ACK erased durable result proof", err)
	}
}

func TestProfileCreationResultsUsersLocalCaptureRejectsDirtyForeignAndSuccessors(t *testing.T) {
	for _, mode := range []string{"dirty-current", "revision-arrived", "successor-marker", "foreign-operation", "different-context", "copied-ticket"} {
		t.Run(mode, func(t *testing.T) {
			f := creationResultUsers(t)
			read, ctx, _ := f.read(t)
			ut, st, snapshot := f.capture(t, read, ctx)
			f.users.mu.Lock()
			switch mode {
			case "dirty-current":
				f.users.users[f.target] = models.User{ID: f.target, Name: "unsaved local target"}
			case "revision-arrived":
				f.users.persistence.revisions[f.target] = datastore.UserRevision{}
			case "successor-marker":
				f.users.markStorageUnknownLocked([]string{f.target})
			case "foreign-operation":
				f.users.persistence.unknownTargets[f.target].creation = "synthetic-other-operation"
			}
			marker := f.users.persistence.unknownTargets[f.target]
			f.users.mu.Unlock()
			if mode == "different-context" {
				ctx = context.WithValue(ctx, struct{}{}, true)
			}
			if mode == "copied-ticket" {
				copy := *ut
				ut = &copy
			}
			before := recoveryFingerprint(t, f.fixture)
			lease, err := f.users.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
			if lease != nil {
				lease.Close()
			}
			if err == nil || lease != nil {
				t.Fatal("stale local capture accepted", mode)
			}
			f.users.mu.RLock()
			same := f.users.persistence.unknownTargets[f.target] == marker && f.users.persistence.uncertain
			f.users.mu.RUnlock()
			if !same || recoveryFingerprint(t, f.fixture) != before {
				t.Fatal("failed recovery changed marker or database")
			}
			if value, err := f.settings.Get(f.target); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
				t.Fatal("rejected Users recovery half-adopted Settings", err)
			}
			st.Close()
		})
	}
}

func TestProfileCreationResultsUsersGenericCleanServicesCannotConsumeOrigin(t *testing.T) {
	f := creationResultUsers(t)
	read, ctx, _ := f.read(t)
	inner, _, err := f.fixture.ds.ProfileCreationRecoveryRead(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	cleanUsers := f.fixture.service(t, f.fixture.ds)
	cleanSettings, err := user_settings.NewServiceWithStore(f.fixture.ds)
	if err != nil {
		t.Fatal(err)
	}
	if ticket, err := cleanUsers.PrepareStorageRecovery(ctx, inner); ticket != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		if ticket != nil {
			ticket.Close()
		}
		t.Fatal("clean Users bypassed creation provenance", err)
	}
	if ticket, err := cleanSettings.PrepareUserSettingsRecovery(ctx, inner); ticket != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		if ticket != nil {
			ticket.Close()
		}
		t.Fatal("clean Settings bypassed creation provenance", err)
	}
	if slot, err := f.fixture.ds.ReserveUsersRecovery(ctx, inner); slot != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		if slot != nil {
			slot.Close()
		}
		t.Fatal("generic Users slot acquired creation origin", err)
	}
	if slot, err := f.fixture.ds.ReserveUserSettingsRecovery(ctx, inner); slot != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		if slot != nil {
			slot.Close()
		}
		t.Fatal("generic Settings slot acquired creation origin", err)
	}
	if snapshot, err := f.fixture.ds.ReadProfileRecovery(inner); snapshot != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		if snapshot != nil {
			snapshot.Close()
		}
		t.Fatal("generic read consumed creation origin", err)
	}
	ut, st, snapshot := f.capture(t, read, ctx)
	projection, err := f.fixture.ds.ProfileCreationRecoverySnapshot(ctx, read, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.users.InstallStorageRecovery(ctx, ut, projection); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("individual Users install bypassed typed PairTake", err)
	}
	if _, err = f.settings.InstallUserSettingsRecovery(ctx, st, projection); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("individual Settings install bypassed typed PairTake", err)
	}
	if _, _, err = f.fixture.ds.TakeUsersRecovery(ctx, ut.state.slot, projection); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("generic direct Users take accepted typed slot", err)
	}
	if lease, _, _, err := f.fixture.ds.BeginUsersRecoveryLease(ctx, ut.state.slot, projection); lease != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("generic direct Users lease accepted typed slot", err)
	}
	ul, err := f.users.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer ul.Close()
	sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Close()
	if ul.PairLease() != nil || sl.PairLease() != nil || ul.CreationPairLease() == nil || sl.CreationPairLease() == nil {
		t.Fatal("typed proof origin not preserved")
	}
	if err = f.fixture.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
		t.Fatal("denied generic probes consumed typed authority", err)
	}
	if us, ss := ul.Complete(), sl.Complete(); !us.TargetPresent || us.PendingUncertainty || !ss.TargetPresent || ss.PendingUncertainty {
		t.Fatal("legitimate typed pair not adopted")
	}
	sl.Close()
	ul.Close()
}

func TestProfileCreationResultsUsersPairCopyReplayAndCancellation(t *testing.T) {
	for _, mode := range []string{"copied-proof", "copied-read", "copied-snapshot", "different-context", "cancel-before", "replay"} {
		t.Run(mode, func(t *testing.T) {
			f := creationResultUsers(t)
			read, ctx, cancel := f.read(t)
			ut, st, snapshot := f.capture(t, read, ctx)
			ul, err := f.users.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			defer ul.Close()
			sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			defer sl.Close()
			actualRead, actualSnapshot, actualCtx, proof := read, snapshot, ctx, ul.CreationPairLease()
			switch mode {
			case "copied-proof":
				copy := *proof
				proof = &copy
			case "copied-read":
				copy := *read
				actualRead = &copy
			case "copied-snapshot":
				copy := *snapshot
				actualSnapshot = &copy
			case "different-context":
				actualCtx = context.WithValue(ctx, struct{}{}, true)
			case "cancel-before":
				cancel()
			}
			err = f.fixture.ds.TakeProfileCreationRecoveryPair(actualCtx, actualRead, actualSnapshot, proof, sl.CreationPairLease())
			if mode == "replay" {
				if err != nil {
					t.Fatal(err)
				}
				if err = f.fixture.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err == nil {
					t.Fatal("typed pair replay accepted")
				}
				if us, ss := ul.Complete(), sl.Complete(); !us.TargetPresent || !ss.TargetPresent {
					t.Fatal("replay denial undid historical ACK")
				}
			} else {
				if err == nil || ul.CreationPairLease().Paired() || sl.CreationPairLease().Paired() {
					t.Fatal("invalid pair consumed either cache proof", mode, err)
				}
				if !ul.Complete().PendingUncertainty || !sl.Complete().PendingUncertainty {
					t.Fatal("failed pair installed one cache")
				}
			}
			sl.Close()
			ul.Close()
			if mode != "replay" {
				f.users.mu.RLock()
				marker := f.users.persistence.unknownTargets[f.target]
				f.users.mu.RUnlock()
				if marker == nil || marker.creation != f.operation {
					t.Fatal("invalid pair erased original operation marker")
				}
				if value, err := f.settings.Get(f.target); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
					t.Fatal("invalid pair erased settings uncertainty", err)
				}
			}
		})
	}
}

func TestProfileCreationResultsUsersUnattemptedReadPreservesFiniteCacheAndForeignMarker(t *testing.T) {
	f := creationResultUsers(t)
	f.users.mu.Lock()
	f.users.persistence.unknownUnscoped = true
	baseline := [4]int{len(f.users.users), len(f.users.persistence.confirmed), len(f.users.persistence.revisions), len(f.users.persistence.unknownTargets)}
	originalMarker := f.users.persistence.unknownTargets[f.target]
	f.users.mu.Unlock()
	before := recoveryFingerprint(t, f.fixture)
	for n := 0; n < 3; n++ {
		parent, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		request := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(parent)
		request.Header.Set("X-Client-ID", f.session.ClientID)
		request, release, err := f.fixture.ds.AuthenticateProfilePinRequest(request, f.session.Token, f.session.ClientID)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		intent, err := f.fixture.ds.PrepareParentProfileCreationManagement(request, []byte(`{"name":"Abandoned Users child","accountId":"master","isKidsProfile":true}`), nil)
		if err != nil {
			release()
			cancel()
			t.Fatal(err)
		}
		locator, err := f.fixture.ds.ParentProfileCreationLocator(intent)
		if err != nil {
			intent.Close()
			release()
			cancel()
			t.Fatal(err)
		}
		intent.Close()
		release()
		<-parent.Done()
		cancel()
		originalLocator := f.locator
		f.locator = locator
		read, ctx, _ := f.read(t)
		f.locator = originalLocator
		ut, err := f.users.PrepareProfileCreationRecovery(ctx, read)
		if err != nil {
			t.Fatal(err)
		}
		st, err := f.settings.PrepareProfileCreationRecovery(ctx, read)
		if err != nil {
			ut.Close()
			t.Fatal(err)
		}
		snapshot, err := f.fixture.ds.ReadProfileCreationResult(read)
		if err != nil || snapshot == nil || snapshot.Outcome() != "not_created" {
			st.Close()
			ut.Close()
			t.Fatal("expired never-attempted read not qualified", err)
		}
		ul, err := f.users.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
		if err != nil {
			st.Close()
			ut.Close()
			t.Fatal(err)
		}
		sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
		if err != nil {
			ul.Close()
			st.Close()
			ut.Close()
			t.Fatal(err)
		}
		if err = f.fixture.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
			sl.Close()
			ul.Close()
			st.Close()
			ut.Close()
			t.Fatal(err)
		}
		us, ss := ul.Complete(), sl.Complete()
		sl.Close()
		ul.Close()
		st.Close()
		ut.Close()
		read.Close()
		if us.TargetPresent || ss.TargetPresent || !us.PendingUncertainty || !ss.PendingUncertainty {
			t.Fatal("never-attempted absence cleared foreign uncertainty")
		}
		f.users.mu.RLock()
		actual := [4]int{len(f.users.users), len(f.users.persistence.confirmed), len(f.users.persistence.revisions), len(f.users.persistence.unknownTargets)}
		intact := f.users.persistence.unknownTargets[f.target] == originalMarker && f.users.persistence.unknownUnscoped && f.users.persistence.uncertain
		f.users.mu.RUnlock()
		if actual != baseline || !intact {
			t.Fatal("never-attempted read grew Users absence entries or cleared foreign evidence")
		}
	}
	if recoveryFingerprint(t, f.fixture) != before {
		t.Fatal("never-attempted read wrote a User, Settings or lifecycle row")
	}
}
