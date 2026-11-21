//go:build linux

package profileunlock

import (
	"context"
	"net/http"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/sessions"
)

const quickConnectFacadeToken = "synthetic-qc-facade-approver"

func quickConnectFacadePG(t *testing.T) (*bridgeSessionFacadeFixture, *BridgeQuickConnectIssuer, *BridgeQuickConnectChallenge) {
	t.Helper()
	f := bridgeSessionFacadePG(t)
	settings, err := f.manager.Load()
	if err != nil {
		t.Fatal(err)
	}
	settings.JellyfinBridge.QuickConnect = true
	if err = f.manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: quickConnectFacadeToken, AccountID: "actor", ClientID: "approver-device", Scope: models.SessionScopeJellyfin, ScopeResource: "target", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err = f.ds.Sessions().Create(t.Context(), &session); err != nil {
		t.Fatal(err)
	}
	issuer, err := f.pins.NewBridgeQuickConnectIssuer(t.Context(), f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(issuer.Close)
	r, _ := quickConnectFacadeRequest(t, "target-device")
	c, err := issuer.Initiate(r, "target-device")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return f, issuer, c
}
func quickConnectFacadeRequest(t *testing.T, client string) (*http.Request, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	t.Cleanup(cancel)
	r := bridgeCredentialFacadeRequest(ctx)
	r.Header.Set("Authorization", `MediaBrowser DeviceId="`+client+`"`)
	r.Header.Set("User-Agent", "Synthetic Facade TV")
	r.RemoteAddr = "198.51.100.9"
	return r, cancel
}
func quickConnectFacadeRelease(t *testing.T, issuer *BridgeQuickConnectIssuer, c *BridgeQuickConnectChallenge, a *BridgeQuickConnectApproval) {
	t.Helper()
	r, _ := quickConnectFacadeRequest(t, "target-device")
	data, err := c.Details()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := issuer.Commit(r, c, a, data.Secret, "target-device")
	if err != nil {
		t.Fatal(err)
	}
	defer receipt.Close()
	session, err := issuer.Release(r, receipt)
	if err != nil || session.Token == "" || session.Scope != models.SessionScopeJellyfin || session.ScopeResource != "target" || session.ClientID != "target-device" || session.IsMaster {
		t.Fatal("genuine facade scoped release", err)
	}
}
func TestBridgeQuickConnectFacadeGenuineApproverFrame(t *testing.T) {
	for _, kind := range []string{"original", "stale-cache", "full-scope", "wrong-client", "foreign-issuer"} {
		t.Run(kind, func(t *testing.T) {
			f, issuer, c := quickConnectFacadePG(t)
			r, _ := quickConnectFacadeRequest(t, "approver-device")
			client := "approver-device"
			switch kind {
			case "stale-cache":
				cached, err := sessions.NewServiceWithStore(f.ds, time.Hour)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = cached.ValidateScoped(quickConnectFacadeToken, models.SessionScopeJellyfin); err != nil || cached.Count() != 1 {
					t.Fatal("actual warm old session projection", err)
				}
				if _, err = f.ds.Pool().Exec(t.Context(), `DELETE FROM sessions WHERE token=$1`, quickConnectFacadeToken); err != nil {
					t.Fatal(err)
				}
			case "full-scope":
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE sessions SET scope='',scope_resource='' WHERE token=$1`, quickConnectFacadeToken); err != nil {
					t.Fatal(err)
				}
			case "wrong-client":
				client = "wrong-device"
			case "foreign-issuer":
				var err error
				issuer, err = f.pins.NewBridgeQuickConnectIssuer(t.Context(), f.manager, f.security, f.coordinator)
				if err != nil {
					t.Fatal(err)
				}
				defer issuer.Close()
			}
			a, err := issuer.Approve(r, quickConnectFacadeToken, client, c)
			if kind != "original" {
				if err == nil || a != nil {
					t.Fatal("facade adopted stale/foreign/scoped DATA as delegation", kind)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			quickConnectFacadeRelease(t, issuer, c, a)
		})
	}
}
func TestBridgeQuickConnectFacadeOwnership(t *testing.T) {
	for _, kind := range []string{"copied-issuer", "copied-challenge", "copied-approval", "closed-issuer", "request-cancel-after-handoff"} {
		t.Run(kind, func(t *testing.T) {
			_, issuer, c := quickConnectFacadePG(t)
			r, cancel := quickConnectFacadeRequest(t, "approver-device")
			if kind == "copied-issuer" {
				copyOfIssuer := *issuer
				issuer = &copyOfIssuer
			}
			if kind == "copied-challenge" {
				copyOfChallenge := *c
				c = &copyOfChallenge
			}
			if kind == "closed-issuer" {
				issuer.Close()
			}
			a, err := issuer.Approve(r, quickConnectFacadeToken, "approver-device", c)
			if kind == "copied-issuer" || kind == "copied-challenge" || kind == "closed-issuer" {
				if err == nil || a != nil {
					t.Fatal("copied/closed facade issued a handoff", kind)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "copied-approval" {
				copyOfApproval := *a
				collector, _ := quickConnectFacadeRequest(t, "target-device")
				data, _ := c.Details()
				receipt, e := issuer.Commit(collector, c, &copyOfApproval, data.Secret, "target-device")
				if receipt != nil {
					receipt.Close()
				}
				if e == nil || receipt != nil {
					t.Fatal("copied facade approval minted")
				}
				return
			}
			cancel()
			quickConnectFacadeRelease(t, issuer, c, a)
		})
	}
}

func TestBridgeQuickConnectFacadeReceiptExpiry(t *testing.T) {
	for _, kind := range []string{"earlier-approver-session", "earlier-approver-account", "copied-receipt", "zero-receipt", "closed-receipt"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "zero-receipt" {
				var empty BridgeQuickConnectReceipt
				if !empty.ExpiresAt().IsZero() {
					t.Fatal("zero facade deadline")
				}
				return
			}
			f, issuer, c := quickConnectFacadePG(t)
			want := time.Now().Add(18 * time.Second).UTC().Truncate(time.Microsecond)
			if kind == "earlier-approver-session" || kind == "earlier-approver-account" {
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=$1 WHERE token=$2`, want, quickConnectFacadeToken); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "earlier-approver-account" {
				want = want.Add(-3 * time.Second)
				if _, err := f.ds.Pool().Exec(t.Context(), `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, want); err != nil {
					t.Fatal(err)
				}
			}
			approver, cancel := quickConnectFacadeRequest(t, "approver-device")
			a, err := issuer.Approve(approver, quickConnectFacadeToken, "approver-device", c)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			collector, _ := quickConnectFacadeRequest(t, "target-device")
			details, _ := c.Details()
			receipt, err := issuer.Commit(collector, c, a, details.Secret, "target-device")
			if err != nil {
				t.Fatal(err)
			}
			defer receipt.Close()
			original := receipt.ExpiresAt()
			switch kind {
			case "earlier-approver-session", "earlier-approver-account":
				if !original.Equal(want) {
					t.Fatal("facade did not retain actual SQL minimum expiry")
				}
				session, e := issuer.Release(collector, receipt)
				if e != nil || session.Token == "" {
					t.Fatal(e)
				}
			case "copied-receipt":
				copyOfReceipt := *receipt
				if !copyOfReceipt.ExpiresAt().IsZero() {
					t.Fatal("copied facade deadline accepted")
				}
				if session, e := issuer.Release(collector, &copyOfReceipt); e == nil || session.Token != "" {
					t.Fatal("copied facade receipt authorized")
				}
			case "closed-receipt":
				receipt.Close()
				if !receipt.ExpiresAt().Equal(original) {
					t.Fatal("closed facade extended its original deadline")
				}
				if session, e := issuer.Release(collector, receipt); e == nil || session.Token != "" {
					t.Fatal("closed facade receipt authorized")
				}
			}
		})
	}
}
