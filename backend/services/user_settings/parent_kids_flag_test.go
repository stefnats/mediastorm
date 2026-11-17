package user_settings

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/users"
)

const settingsFlagPassword = "synthetic flag parent password"

type settingsFlagFixture struct {
	*settingsPGFixture
	profiles *users.Service
	settings *Service
	session  models.Session
}

func settingsFlagSetup(t *testing.T, raw string) *settingsFlagFixture {
	t.Helper()
	f := newSettingsPG(t)
	f.user(t, "target")
	f.user(t, "other")
	hash, e := bcrypt.GenerateFromPassword([]byte(settingsFlagPassword), bcrypt.MinCost)
	if e != nil {
		t.Fatal(e)
	}
	f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='default'`, string(hash))
	if raw != "" {
		f.exec(t, `INSERT INTO user_settings(user_id,settings)VALUES('target',$1)`, raw)
	}
	u, e := users.NewServiceWithStore(f.ds, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	settings := f.service(t, f.ds)
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "SYNTHETIC_KIDS_FLAG_SETTINGS_SESSION", AccountID: "default", ClientID: "synthetic-settings-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if e = f.ds.Sessions().Create(t.Context(), &session); e != nil {
		t.Fatal(e)
	}
	return &settingsFlagFixture{settingsPGFixture: f, profiles: u, settings: settings, session: session}
}
func (f *settingsFlagFixture) intent(t *testing.T) (*datastore.ParentKidsFlagIntent, context.Context) {
	t.Helper()
	body := []byte(`{"isKidsProfile":true}`)
	r := httptest.NewRequest(http.MethodPut, "http://synthetic.invalid/account/api/profiles/kids?profileId=target", nil).WithContext(t.Context())
	r.Header.Set("X-Client-ID", f.session.ClientID)
	r, release, e := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	i, e := f.ds.PrepareParentKidsFlagManagement(r, "target", body, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	if e = f.ds.BindParentKidsFlagManagementRetry(r, i, "target", body, nil); e != nil {
		t.Fatal(e)
	}
	r, e = f.ds.BoundParentKidsFlagRequest(r, i)
	if e != nil {
		t.Fatal(e)
	}
	r.Header.Set("X-Account-Password", settingsFlagPassword)
	if e = f.ds.VerifyParentKidsFlagPassword(r, i); e != nil {
		t.Fatal(e)
	}
	return i, r.Context()
}
func TestParentKidsFlagSettingsPreparedPairPreservesUnknownAndForeignDirty(t *testing.T) {
	f := settingsFlagSetup(t, `{"futureRoot":{"n":9007199254740993},"display":{"future":{"n":9007199254740995}}}`)
	f.settings.mu.Lock()
	var dirty models.UserSettings
	dirty.Metadata.PrimaryLanguage = "ita"
	f.settings.settings["other"] = dirty
	otherRecord := f.settings.pg.records["other"]
	f.settings.mu.Unlock()
	i, ctx := f.intent(t)
	u, e := f.profiles.AcquireParentKidsFlagLease(ctx, "target", i)
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	l, e := f.settings.AcquireParentKidsFlagDefaultsLease(ctx, "target", i, u.ReceiptLease())
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	receipt, e := u.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if _, e = u.Stage(receipt); e != nil {
		t.Fatal(e)
	}
	if e = l.Stage(receipt); e != nil {
		t.Fatal(e)
	}
	if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, u.ReceiptLease(), l.ReceiptLease()); e != nil {
		t.Fatal(e)
	}
	u.Complete()
	l.Complete()
	l.Close()
	u.Close()
	current, e := f.settings.Get("target")
	if e != nil || current == nil || current.Display.SimpleMode == nil || !*current.Display.SimpleMode {
		t.Fatal("default missing", e)
	}
	f.settings.mu.RLock()
	actualDirty := f.settings.settings["other"]
	actualRecord := f.settings.pg.records["other"]
	f.settings.mu.RUnlock()
	if !reflect.DeepEqual(dirty, actualDirty) || actualRecord.Revision != otherRecord.Revision {
		t.Fatal("compound operation replaced foreign dirty target")
	}
	var n, child string
	if e = f.ds.Pool().QueryRow(t.Context(), `SELECT settings#>>'{futureRoot,n}',settings#>>'{display,future,n}' FROM user_settings WHERE user_id='target'`).Scan(&n, &child); e != nil || n != "9007199254740993" || child != "9007199254740995" {
		t.Fatal("unknown values changed", e)
	}
}
func TestParentKidsFlagSettingsLocalDirtyStaleAndQuarantineDeny(t *testing.T) {
	for _, scenario := range []string{"dirty", "stale", "quarantine", "copy users proof"} {
		t.Run(scenario, func(t *testing.T) {
			f := settingsFlagSetup(t, `{"display":{"simpleMode":false}}`)
			if scenario == "stale" {
				f.exec(t, `UPDATE user_settings SET settings='{"display":{"simpleMode":true}}' WHERE user_id='target'`)
			}
			i, ctx := f.intent(t)
			u, e := f.profiles.AcquireParentKidsFlagLease(ctx, "target", i)
			if e != nil {
				t.Fatal(e)
			}
			defer u.Close()
			proof := u.ReceiptLease()
			f.settings.mu.Lock()
			switch scenario {
			case "dirty":
				v := f.settings.settings["target"]
				v.Metadata.PrimaryLanguage = "fra"
				f.settings.settings["target"] = v
			case "quarantine":
				f.settings.markUserSettingsUnknownLocked([]string{"target"})
			}
			f.settings.mu.Unlock()
			if scenario == "copy users proof" {
				copy := *proof
				proof = &copy
			}
			io := f.ds.Pool().Stat().AcquireCount()
			l, e := f.settings.AcquireParentKidsFlagDefaultsLease(ctx, "target", i, proof)
			if e == nil || l != nil {
				if l != nil {
					l.Close()
				}
				t.Fatal("invalid local settings projection accepted")
			}
			if f.ds.Pool().Stat().AcquireCount() != io {
				t.Fatal("known stale/dirty target was silently reloaded")
			}
		})
	}
}
func TestParentKidsFlagSettingsStampChangeAfterOriginalSnapshot(t *testing.T) {
	f := settingsFlagSetup(t, "")
	i, ctx := f.intent(t)
	f.exec(t, `INSERT INTO user_settings(user_id,settings)VALUES('target','{}');DELETE FROM user_settings WHERE user_id='target'`)
	u, e := f.profiles.AcquireParentKidsFlagLease(ctx, "target", i)
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	l, e := f.settings.AcquireParentKidsFlagDefaultsLease(ctx, "target", i, u.ReceiptLease())
	if e != nil {
		t.Fatal("unchanged local snapshot should bind before SQL finalcheck", e)
	}
	defer l.Close()
	if receipt, e := u.Commit(); e == nil || receipt != nil {
		t.Fatal("absent settings ABA committed")
	}
	actual, e := f.ds.Users().Get(t.Context(), "target")
	if e != nil || actual.IsKidsProfile {
		t.Fatal("settings conflict partially wrote User")
	}
}
func TestParentKidsFlagSettingsRetiredCopyAndForeignLease(t *testing.T) {
	f := settingsFlagSetup(t, "")
	i, ctx := f.intent(t)
	u, e := f.profiles.AcquireParentKidsFlagLease(ctx, "target", i)
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	l, e := f.settings.AcquireParentKidsFlagDefaultsLease(ctx, "target", i, u.ReceiptLease())
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	copy := *l
	io := f.ds.Pool().Stat().AcquireCount()
	if e = copy.Stage(nil); e == nil {
		t.Fatal("copied service lease accepted")
	}
	if f.ds.Pool().Stat().AcquireCount() != io {
		t.Fatal("invalid copy used SQL")
	}
	l.Close()
	if _, e = u.Commit(); !errors.Is(e, datastore.ErrParentKidsFlagChanged) {
		t.Fatal("retired settings lease allowed write", e)
	}
}
