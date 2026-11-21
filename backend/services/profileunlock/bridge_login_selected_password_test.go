//go:build linux

package profileunlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/services/accounts"
	"novastream/services/backup"
)

func selectedFacadeRequest(t *testing.T) (*http.Request, *context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	t.Cleanup(cancel)
	r := bridgeCredentialFacadeRequest(ctx)
	return r, &cancel
}
func selectedFacadeProof(t *testing.T, f *bridgeSessionFacadeFixture, username string) (*http.Request, *BridgeSelectedPasswordProof, *config.BridgeLoginPolicyObservation) {
	t.Helper()
	r, _ := selectedFacadeRequest(t)
	o, e := f.security.CaptureBridgePolicy(r.Context())
	if e != nil {
		t.Fatal(e)
	}
	p, e := f.issuer.VerifySelectedPassword(r, username, "synthetic-device", "synthetic actor account password", o)
	if e != nil || p == nil {
		t.Fatal("genuine selected facade proof", e)
	}
	t.Cleanup(p.Close)
	return r, p, o
}
func selectedFacadeRelease(t *testing.T, f *bridgeSessionFacadeFixture, r *http.Request, p *BridgeSelectedPasswordProof, o *config.BridgeLoginPolicyObservation) {
	t.Helper()
	receipt, e := f.issuer.CommitSelected(r, p, o)
	if e != nil || receipt == nil {
		t.Fatal("actual selected facade Commit", e)
	}
	defer receipt.Close()
	session, e := f.issuer.Release(r, receipt)
	if e != nil || session.Token == "" || session.AccountID != "actor" || session.ScopeResource != "target" {
		t.Fatal("actual restricted facade Release", e)
	}
}
func TestBridgeSelectedPasswordFacadeGenuineEndToEnd(t *testing.T) {
	for _, kind := range []string{"actual-one-proof-mint-release", "account-profile-alias", "bare-name-multiple", "copied-facade", "unavailable-child-household"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionFacadePG(t)
			username := "Synthetic Profile"
			if kind == "account-profile-alias" {
				username = "Synthetic Actor/Synthetic Profile"
			}
			if kind == "bare-name-multiple" {
				h, e := bcrypt.GenerateFromPassword([]byte("another synthetic account password"), bcrypt.MinCost)
				if e != nil {
					t.Fatal(e)
				}
				if _, e = f.ds.Pool().Exec(t.Context(), `INSERT INTO accounts(id,username,password_hash)VALUES('other','Other',$1)`, string(h)); e != nil {
					t.Fatal(e)
				}
				if _, e = f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name)VALUES('other-profile','other','Synthetic Profile');INSERT INTO user_settings_versions(user_id,generation)VALUES('other-profile',0) ON CONFLICT DO NOTHING`); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "unavailable-child-household" {
				if _, e := f.ds.Pool().Exec(t.Context(), `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic child',true)`); e != nil {
					t.Fatal(e)
				}
				r, _ := selectedFacadeRequest(t)
				o, e := f.security.CaptureBridgePolicy(r.Context())
				if e != nil {
					t.Fatal(e)
				}
				p, e := f.issuer.VerifySelectedPassword(r, username, "synthetic-device", "synthetic actor account password", o)
				if p != nil {
					p.Close()
				}
				if p != nil || e == nil {
					t.Fatal("unsupported child household fell back to general token", e)
				}
				return
			}
			r, p, o := selectedFacadeProof(t, f, username)
			for _, v := range []any{p, *p} {
				raw, e := json.Marshal(v)
				if e != nil || string(raw) != "null" || fmt.Sprintf("%#v", v) != "BridgeSelectedPasswordProof(opaque)" {
					t.Fatal("facade disclosed selected private proof")
				}
			}
			if kind == "copied-facade" {
				copy := *p
				copy.Close()
				if p.Context().Err() != nil {
					t.Fatal("copied facade Close retired original")
				}
				if value, e := f.issuer.CommitSelected(r, &copy, o); e == nil || value != nil {
					if value != nil {
						value.Close()
					}
					t.Fatal("copied selected facade proof accepted")
				}
				issuerCopy := *f.issuer
				if value, e := issuerCopy.VerifySelectedPassword(r, username, "synthetic-device", "synthetic actor account password", o); e == nil || value != nil {
					if value != nil {
						value.Close()
					}
					t.Fatal("copied issuer accepted")
				}
			}
			selectedFacadeRelease(t, f, r, p, o)
		})
	}
}

// Reopen this same synthetic schema using the real startup/runtime-pool port.
// This specifically establishes FinishMutations before ordinary service writes;
// it is not a stand-in for the account-recovery CLI or an imported permission.
func selectedFacadeFinishedStartup(t *testing.T, f *bridgeSessionFacadeFixture) {
	t.Helper()
	url := f.ds.Pool().Config().ConnString()
	if e := f.security.Close(); e != nil {
		t.Fatal(e)
	}
	if e := f.coordinator.Close(); e != nil {
		t.Fatal(e)
	}
	f.ds.Close()
	objects, e := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if e != nil {
		t.Fatal(e)
	}
	startup, e := datastore.OpenSettingsStartupStore(t.Context(), url, objects)
	if e != nil {
		_ = objects.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		_ = startup.Close()
		_ = objects.Close()
	})
	coordinator := startup.Coordinator()
	guard, e := coordinator.TryWrite(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	if e = coordinator.Arm(guard); e != nil {
		_ = guard.Close()
		t.Fatal(e)
	}
	store, e := startup.Migrate(t.Context(), guard)
	if e != nil {
		_ = guard.Close()
		t.Fatal(e)
	}
	if e = startup.FinishMutations(); e != nil {
		_ = guard.Close()
		t.Fatal(e)
	}
	f.ds = store
	f.coordinator = coordinator
	f.security, e = config.InstallManagedSecurity(f.manager, coordinator, "")
	_ = guard.Close()
	if e != nil {
		t.Fatal(e)
	}
	f.pins = NewPGProfilePins(f.ds)
	f.issuer, e = f.pins.NewBridgeLoginSessionIssuer(f.manager, f.security, coordinator)
	if e != nil {
		t.Fatal(e)
	}
}
func TestBridgeSelectedPasswordActualRestoreAndRecovery(t *testing.T) {
	for _, kind := range []string{"named-restore-first", "named-writer-first", "ordinary-post-finish-recovery-update", "restored-original-audit-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionFacadePG(t)
			if kind == "ordinary-post-finish-recovery-update" {
				selectedFacadeFinishedStartup(t, f)
				service, e := accounts.NewServiceWithStore(f.ds)
				if e != nil {
					t.Fatal("ordinary actual accounts service", e)
				}
				r, p, o := selectedFacadeProof(t, f, "Synthetic Profile")
				if e = service.UpdatePassword("actor", "changed synthetic recovered password"); e != nil {
					t.Fatal("ordinary post-Finish UpdatePassword", e)
				}
				var hash string
				if e = f.ds.Pool().QueryRow(t.Context(), `SELECT password_hash FROM accounts WHERE id='actor'`).Scan(&hash); e != nil || bcrypt.CompareHashAndPassword([]byte(hash), []byte("changed synthetic recovered password")) != nil {
					t.Fatal("actual post-Finish password not stored", e)
				}
				receipt, e := f.issuer.CommitSelected(r, p, o)
				if receipt != nil {
					receipt.Close()
				}
				if e == nil || receipt != nil {
					t.Fatal("post-Finish password drift adopted", e)
				}
				return
			}
			service, e := backup.NewService(filepath.Dir(f.manager.ConfigPath()), f.manager)
			if e != nil {
				t.Fatal(e)
			}
			service.SetDataStore(f.ds)
			info, e := service.CreateBackup(backup.BackupTypeManual)
			if e != nil {
				t.Fatal("real named backup", e)
			}
			var before string
			if e = f.ds.Pool().QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM accounts WHERE id='actor'`).Scan(&before); e != nil {
				t.Fatal(e)
			}
			r, p, o := selectedFacadeProof(t, f, "Synthetic Profile")
			if kind == "named-writer-first" {
				receipt, e := f.issuer.CommitSelected(r, p, o)
				if e != nil {
					t.Fatal(e)
				}
				t.Cleanup(receipt.Close)
				e = service.RestoreBackup(info.Filename)
				if !errors.Is(e, settingscommit.ErrBusy) {
					t.Fatal("actual selected mint native hold did not exclude full named Restore", e)
				}
				session, e := f.issuer.Release(r, receipt)
				if e != nil || session.Token == "" {
					t.Fatal("Busy restore attempt adopted/retired current selected receipt", e)
				}
				receipt.Close()
				p.Close()
				if e = service.RestoreBackup(info.Filename); e != nil {
					t.Fatal("named restore after actual hold end", e)
				}
				return
			}
			if e = service.RestoreBackup(info.Filename); e != nil {
				t.Fatal("actual complete named restore", e)
			}
			var after string
			if e = f.ds.Pool().QueryRow(t.Context(), `SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM accounts WHERE id='actor'`).Scan(&after); e != nil || before == after {
				t.Fatal("named restore did not replace original physical account", e)
			}
			receipt, e := f.issuer.CommitSelected(r, p, o)
			if receipt != nil {
				receipt.Close()
			}
			if e == nil || receipt != nil {
				t.Fatal("restored original selected audit was adopted", e)
			}
			fresh, freshProof, freshPolicy := selectedFacadeProof(t, f, "Synthetic Profile")
			selectedFacadeRelease(t, f, fresh, freshProof, freshPolicy)
		})
	}
}
