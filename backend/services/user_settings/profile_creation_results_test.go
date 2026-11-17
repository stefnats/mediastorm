package user_settings

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/services/users"
)

type creationResultSettingsFixture struct {
	*settingsFlagFixture
	target, operation, locator string
}

// The actual Create receipt is known, deliberately left unadopted, and both
// cache owners are quarantined. This is not a simulated lost-COMMIT ACK.
func creationResultSettings(t *testing.T) *creationResultSettingsFixture {
	t.Helper()
	mutatorsNoHTTP(t)
	f := settingsFlagSetup(t, `{"display":{"simpleMode":false},"metadata":{"primaryLanguage":"eng"}}`)
	request := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(t.Context())
	request.Header.Set("X-Client-ID", f.session.ClientID)
	request, release, err := f.ds.AuthenticateProfilePinRequest(request, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	body := []byte(`{"name":"Initial result settings child","isKidsProfile":true}`)
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
	request.Header.Set("X-Account-Password", settingsFlagPassword)
	if err = f.ds.VerifyParentProfileCreationPassword(request, intent); err != nil {
		t.Fatal(err)
	}
	target, operation, err := f.ds.ParentProfileCreationTarget(request.Context(), intent)
	if err != nil {
		t.Fatal(err)
	}
	u, err := f.profiles.AcquireParentProfileCreationLease(request.Context(), intent)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	s, err := f.settings.AcquireParentProfileCreationDefaultsLease(request.Context(), intent, u.ReceiptLease())
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
	if _, ok := f.profiles.Get(target); ok {
		t.Fatal("unadopted Create installed its User")
	}
	if value, err := f.settings.Get(target); value != nil || !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("unadopted Create did not quarantine settings", err)
	}
	actual, err := f.ds.Users().Get(t.Context(), target)
	if err != nil || actual.ID != target {
		t.Fatal("actual committed fixture User missing", err)
	}
	return &creationResultSettingsFixture{settingsFlagFixture: f, target: target, operation: operation, locator: locator}
}

func creationSettingsResultRead(t *testing.T, f *settingsFlagFixture, locator string) (*datastore.ProfileCreationRead, context.Context, context.CancelFunc) {
	t.Helper()
	parent, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	request := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles/creation-operations/"+locator+"/read", nil).WithContext(parent)
	request.Header.Set("X-Client-ID", f.session.ClientID)
	request, release, err := f.ds.AuthenticateProfilePinRequest(request, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(release)
	read, err := f.ds.PrepareProfileCreationReadManagement(request, locator, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(read.Close)
	return read, request.Context(), cancel
}

func creationSettingsResultCapture(t *testing.T, f *settingsFlagFixture, read *datastore.ProfileCreationRead, ctx context.Context, outcome string) (*users.StorageRecoveryTicket, *UserSettingsRecoveryTicket, *datastore.ProfileCreationResultSnapshot) {
	t.Helper()
	u, err := f.profiles.PrepareProfileCreationRecovery(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(u.Close)
	s, err := f.settings.PrepareProfileCreationRecovery(ctx, read)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	snapshot, err := f.ds.ReadProfileCreationResult(read)
	if err != nil || snapshot == nil || snapshot.Outcome() != outcome {
		t.Fatal("qualified Settings creation read failed", err)
	}
	return u, s, snapshot
}

func creationSettingsResultPhysical(t *testing.T, f *settingsPGFixture) string {
	t.Helper()
	var value string
	err := f.ds.Pool().QueryRow(t.Context(), `SELECT jsonb_build_object(
 'users',(SELECT jsonb_agg(to_jsonb(u)||jsonb_build_object('physical',concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY id) FROM users u),
 'settings',(SELECT jsonb_agg(to_jsonb(s)||jsonb_build_object('physical',concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text)) ORDER BY user_id) FROM user_settings s),
 'versions',(SELECT jsonb_agg(to_jsonb(v)||jsonb_build_object('physical',concat(v.tableoid::oid,':',v.xmin::text,':',v.ctid::text)) ORDER BY user_id) FROM user_settings_versions v))`).Scan(&value)
	if err != nil {
		t.Fatal("observe own synthetic profile records", err)
	}
	return value
}

func TestProfileCreationResultsSettingsCurrentRawACKPreservesForeignDirtyAndUnknown(t *testing.T) {
	f := creationResultSettings(t)
	f.exec(t, `UPDATE users SET name='Current settings child' WHERE id=$1`, f.target)
	f.exec(t, `UPDATE user_settings SET settings='{"display":{"simpleMode":false,"future":{"n":9007199254740995}},"metadata":{"primaryLanguage":"fra"},"futureRoot":{"n":9007199254740993}}'::jsonb WHERE user_id=$1`, f.target)
	f.settings.mu.Lock()
	dirty := f.settings.settings["target"]
	dirty.Metadata.PrimaryLanguage = "ita"
	f.settings.settings["target"] = dirty
	foreignConfirmed := cloneSettingsValue(f.settings.pg.confirmed["target"])
	foreignRevision := f.settings.pg.records["target"].Revision
	f.settings.markUserSettingsUnknownLocked([]string{"other"})
	foreignMarker := f.settings.pg.unknownMarkers["other"]
	f.settings.mu.Unlock()
	before := creationSettingsResultPhysical(t, f.settingsPGFixture)
	read, ctx, cancel := creationSettingsResultRead(t, f.settingsFlagFixture, f.locator)
	ut, st, snapshot := creationSettingsResultCapture(t, f.settingsFlagFixture, read, ctx, "created")
	ul, err := f.profiles.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer ul.Close()
	sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer sl.Close()
	if ul.PairLease() != nil || sl.PairLease() != nil {
		t.Fatal("creation leases exposed generic authority")
	}
	if err = f.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
		t.Fatal(err)
	}
	cancel()
	us, ss := ul.Complete(), sl.Complete()
	if !us.TargetPresent || us.PendingUncertainty || !ss.TargetPresent || !ss.PendingUncertainty {
		t.Fatal("paired status lost per-target foreign Settings uncertainty")
	}
	sl.Close()
	ul.Close()
	current, err := f.settings.Get(f.target)
	if err != nil || current == nil || current.Metadata.PrimaryLanguage != "fra" || current.Display.SimpleMode == nil || *current.Display.SimpleMode {
		t.Fatal("current same-birth Settings were not adopted", err)
	}
	f.settings.mu.RLock()
	intact := reflect.DeepEqual(f.settings.settings["target"], dirty) && reflect.DeepEqual(f.settings.pg.confirmed["target"], foreignConfirmed) && f.settings.pg.records["target"].Revision == foreignRevision && f.settings.pg.unknownMarkers["other"] == foreignMarker && f.settings.pg.uncertain["other"] && f.settings.pg.unknownMarkers[f.target] == nil && !f.settings.pg.uncertain[f.target]
	f.settings.mu.RUnlock()
	if !intact || creationSettingsResultPhysical(t, f.settingsPGFixture) != before {
		t.Fatal("recovery wrote storage or replaced unrelated local state")
	}
	// This explicit subsequent save verifies that the recovered raw revision,
	// including unknown root/nested fields and exact large numbers, was retained.
	current.Metadata.PrimaryLanguage = "spa"
	if err = f.settings.Update(f.target, *current); err != nil {
		t.Fatal("explicit later target save lost recovered revision", err)
	}
	var root, nested string
	if err = f.ds.Pool().QueryRow(t.Context(), `SELECT settings#>>'{futureRoot,n}',settings#>>'{display,future,n}' FROM user_settings WHERE user_id=$1`, f.target).Scan(&root, &nested); err != nil || root != "9007199254740993" || nested != "9007199254740995" {
		t.Fatal("unknown raw values changed after explicit target save", err)
	}
	if _, err = f.settings.Get("other"); !errors.Is(err, datastore.ErrUserSettingsOutcomeUnknown) {
		t.Fatal("recovery cleared foreign Settings write stop", err)
	}
}

func TestProfileCreationResultsSettingsLocalCASRejectsDirtyForeignAndSuccessors(t *testing.T) {
	for _, mode := range []string{"dirty-current", "record-arrived", "successor-marker", "foreign-operation", "uncertainty-cleared", "different-context", "copied-ticket"} {
		t.Run(mode, func(t *testing.T) {
			f := creationResultSettings(t)
			read, ctx, _ := creationSettingsResultRead(t, f.settingsFlagFixture, f.locator)
			ut, st, snapshot := creationSettingsResultCapture(t, f.settingsFlagFixture, read, ctx, "created")
			selected := st
			actualCtx := ctx
			f.settings.mu.Lock()
			switch mode {
			case "dirty-current":
				dirty := f.settings.settings[f.target]
				dirty.Metadata.PrimaryLanguage = "ita"
				f.settings.settings[f.target] = dirty
			case "record-arrived":
				f.settings.pg.records[f.target] = datastore.UserSettingsRecord{ID: f.target}
			case "successor-marker":
				f.settings.markUserSettingsUnknownLocked([]string{f.target})
			case "foreign-operation":
				f.settings.pg.unknownMarkers[f.target].creation = "synthetic-foreign-operation"
			case "uncertainty-cleared":
				delete(f.settings.pg.uncertain, f.target)
			case "different-context":
				actualCtx = context.WithValue(ctx, struct{}{}, true)
			case "copied-ticket":
				copy := *st
				selected = &copy
			}
			marker := f.settings.pg.unknownMarkers[f.target]
			value, present := f.settings.settings[f.target]
			record, hasRecord := f.settings.pg.records[f.target]
			uncertain := f.settings.pg.uncertain[f.target]
			f.settings.mu.Unlock()
			before := creationSettingsResultPhysical(t, f.settingsPGFixture)
			ul, err := f.profiles.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			defer ul.Close()
			sl, err := f.settings.AcquireProfileCreationRecoveryLease(actualCtx, selected, snapshot)
			if sl != nil {
				sl.Close()
			}
			if err == nil || sl != nil {
				t.Fatal("late or foreign Settings state was silently adopted", mode)
			}
			ul.Close()
			f.settings.mu.RLock()
			after, afterPresent := f.settings.settings[f.target]
			afterRecord, afterHasRecord := f.settings.pg.records[f.target]
			same := f.settings.pg.unknownMarkers[f.target] == marker && reflect.DeepEqual(after, value) && afterPresent == present && reflect.DeepEqual(afterRecord, record) && afterHasRecord == hasRecord && f.settings.pg.uncertain[f.target] == uncertain
			f.settings.mu.RUnlock()
			if !same || creationSettingsResultPhysical(t, f.settingsPGFixture) != before {
				t.Fatal("failed Settings lease changed local or durable state")
			}
		})
	}
}

func TestProfileCreationResultsSettingsGenericOriginAndPairProofsRemainSealed(t *testing.T) {
	for _, mode := range []string{"copied-settings-proof", "reversed-proofs"} {
		t.Run(mode, func(t *testing.T) {
			f := creationResultSettings(t)
			read, ctx, _ := creationSettingsResultRead(t, f.settingsFlagFixture, f.locator)
			inner, _, err := f.ds.ProfileCreationRecoveryRead(ctx, read)
			if err != nil {
				t.Fatal(err)
			}
			clean, err := NewServiceWithStore(f.ds)
			if err != nil {
				t.Fatal(err)
			}
			if ticket, err := clean.PrepareUserSettingsRecovery(ctx, inner); ticket != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				if ticket != nil {
					ticket.Close()
				}
				t.Fatal("clean second Settings service bypassed creation origin", err)
			}
			ut, st, snapshot := creationSettingsResultCapture(t, f.settingsFlagFixture, read, ctx, "created")
			projection, err := f.ds.ProfileCreationRecoverySnapshot(ctx, read, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err = f.ds.ValidateProfileRecoverySlot(ctx, st.state.slot, projection); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("generic Settings validation accepted typed slot", err)
			}
			if _, _, err = f.ds.TakeUserSettingsRecovery(ctx, st.state.slot, projection); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("generic Settings take accepted typed slot", err)
			}
			if lease, _, _, err := f.ds.BeginUserSettingsRecoveryLease(ctx, st.state.slot, projection); lease != nil || !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("generic Settings lease accepted typed slot", err)
			}
			if _, err = f.settings.InstallUserSettingsRecovery(ctx, st, projection); !errors.Is(err, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("individual Settings install consumed typed ticket", err)
			}
			ul, err := f.profiles.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			defer ul.Close()
			sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
			if err != nil {
				t.Fatal(err)
			}
			defer sl.Close()
			up, sp := ul.CreationPairLease(), sl.CreationPairLease()
			if mode == "copied-settings-proof" {
				copy := *sp
				sp = &copy
			} else {
				up, sp = sp, up
			}
			if err = f.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, up, sp); err == nil || ul.CreationPairLease().Paired() || sl.CreationPairLease().Paired() {
				t.Fatal("invalid second proof or order consumed half a pair", err)
			}
			if !ul.Complete().PendingUncertainty || !sl.Complete().PendingUncertainty {
				t.Fatal("failed proof check acknowledged one cache")
			}
			if err = f.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
				t.Fatal("rejected proof probe consumed valid typed pair", err)
			}
			if us, ss := ul.Complete(), sl.Complete(); !us.TargetPresent || !ss.TargetPresent || us.PendingUncertainty || ss.PendingUncertainty {
				t.Fatal("valid pair did not acknowledge both owners")
			}
			sl.Close()
			ul.Close()
		})
	}
}

func TestProfileCreationResultsSettingsUnattemptedReadDoesNotGrowAbsentCacheRecords(t *testing.T) {
	mutatorsNoHTTP(t)
	f := settingsFlagSetup(t, "")
	f.settings.mu.RLock()
	baseline := [5]int{len(f.settings.settings), len(f.settings.pg.confirmed), len(f.settings.pg.records), len(f.settings.pg.uncertain), len(f.settings.pg.unknownMarkers)}
	f.settings.mu.RUnlock()
	before := creationSettingsResultPhysical(t, f.settingsPGFixture)
	for n := 0; n < 3; n++ {
		parent, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		request := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/account/api/profiles", nil).WithContext(parent)
		request.Header.Set("X-Client-ID", f.session.ClientID)
		request, release, err := f.ds.AuthenticateProfilePinRequest(request, f.session.Token, f.session.ClientID)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		intent, err := f.ds.PrepareParentProfileCreationManagement(request, []byte(`{"name":"Abandoned synthetic dialog","isKidsProfile":true}`), nil)
		if err != nil {
			release()
			cancel()
			t.Fatal(err)
		}
		locator, err := f.ds.ParentProfileCreationLocator(intent)
		if err != nil {
			intent.Close()
			release()
			cancel()
			t.Fatal(err)
		}
		// No Bind, password verification, claim, Create or cache lease ever occurs.
		intent.Close()
		release()
		<-parent.Done()
		cancel()
		read, ctx, _ := creationSettingsResultRead(t, f, locator)
		ut, st, snapshot := creationSettingsResultCapture(t, f, read, ctx, "not_created")
		ul, err := f.profiles.AcquireProfileCreationRecoveryLease(ctx, ut, snapshot)
		if err != nil {
			t.Fatal(err)
		}
		sl, err := f.settings.AcquireProfileCreationRecoveryLease(ctx, st, snapshot)
		if err != nil {
			ul.Close()
			t.Fatal(err)
		}
		if err = f.ds.TakeProfileCreationRecoveryPair(ctx, read, snapshot, ul.CreationPairLease(), sl.CreationPairLease()); err != nil {
			sl.Close()
			ul.Close()
			t.Fatal(err)
		}
		us, ss := ul.Complete(), sl.Complete()
		sl.Close()
		ul.Close()
		st.Close()
		ut.Close()
		read.Close()
		if us.TargetPresent || ss.TargetPresent || us.PendingUncertainty || ss.PendingUncertainty {
			t.Fatal("unattempted result became a profile or uncertainty")
		}
		f.settings.mu.RLock()
		actual := [5]int{len(f.settings.settings), len(f.settings.pg.confirmed), len(f.settings.pg.records), len(f.settings.pg.uncertain), len(f.settings.pg.unknownMarkers)}
		f.settings.mu.RUnlock()
		if actual != baseline || creationSettingsResultPhysical(t, f.settingsPGFixture) != before {
			t.Fatal("unattempted read installed unbounded absence entries or wrote profiles")
		}
	}
}
