//go:build linux

package profileunlock

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"novastream/config"
)

func TestBridgePasswordWorkflowFacadeCannotManufactureMintAuthority(t *testing.T) {
	for _, kind := range []string{"genuine-proof-commit", "copied-issuer", "foreign-pins", "forged-observation", "policy-change-before-mint", "copied-proof"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionFacadePG(t)
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			r := bridgeCredentialFacadeRequest(ctx)
			originalContext := r.Context()
			policy, err := f.security.CaptureBridgePolicy(ctx)
			if err != nil {
				t.Fatal(err)
			}
			issuer := f.issuer
			if kind == "copied-issuer" {
				copied := *issuer
				proof, err := copied.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", "synthetic actor account password", policy)
				if proof != nil {
					proof.Close()
				}
				if err == nil || proof != nil {
					t.Fatal("copied facade became configured workflow")
				}
				return
			}
			if kind == "forged-observation" {
				proof, err := issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", "synthetic actor account password", &config.BridgeLoginPolicyObservation{})
				if proof != nil {
					proof.Close()
				}
				if err == nil || proof != nil {
					t.Fatal("imported policy DATA became verification authority")
				}
				return
			}
			proof, err := issuer.VerifyPasswordWorkflow(r, "actor", "target", "synthetic-device", "synthetic actor account password", policy)
			if err != nil || proof == nil {
				t.Fatal("genuine facade workflow failed", err)
			}
			defer proof.Close()
			if r.Context() != originalContext || proof.issuer != f.pins || proof.Check(ctx) != nil {
				t.Fatal("facade replaced original binding")
			}
			raw, err := json.Marshal(proof)
			if err != nil || string(raw) != "null" {
				t.Fatal("workflow exported credential material")
			}
			var count int
			if err := f.ds.Pool().QueryRow(ctx, "SELECT count(*) FROM sessions").Scan(&count); err != nil || count != 0 {
				t.Fatal("facade workflow minted a session", err)
			}
			if kind == "foreign-pins" {
				otherPins := NewPGProfilePins(f.ds)
				other, err := otherPins.NewBridgeLoginSessionIssuer(f.manager, f.security, f.coordinator)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := other.Commit(r, proof, policy)
				if receipt != nil {
					receipt.Close()
				}
				if err == nil || receipt != nil || proof.Check(ctx) != nil {
					t.Fatal("foreign facade adopted or retired original proof", err)
				}
			}
			if kind == "copied-proof" {
				copied := *proof
				copied.Close()
				receipt, err := issuer.Commit(r, &copied, policy)
				if receipt != nil {
					receipt.Close()
				}
				if err == nil || receipt != nil || proof.Check(ctx) != nil {
					t.Fatal("copied proof gained mint authority or retired original", err)
				}
			}
			if kind == "policy-change-before-mint" {
				changed := config.DefaultSettings()
				changed.Database.URL = f.ds.Pool().Config().ConnConfig.ConnString()
				changed.JellyfinBridge.Enabled = false
				changed.Security.EnforceProfilePins = true
				if err := f.manager.Save(changed); err != nil {
					t.Fatal("actual selection hold was not retired", err)
				}
				receipt, err := issuer.Commit(r, proof, policy)
				if receipt != nil {
					receipt.Close()
				}
				if err == nil || receipt != nil {
					t.Fatal("workflow proof bypassed current final Mint policy")
				}
				if err := f.ds.Pool().QueryRow(ctx, "SELECT count(*) FROM sessions").Scan(&count); err != nil || count != 0 {
					t.Fatal("changed policy persisted a session", err)
				}
				return
			}
			receipt, err := issuer.Commit(r, proof, policy)
			if err != nil {
				t.Fatal("genuine existing Mint did not accept same-original workflow proof", err)
			}
			defer receipt.Close()
			session, err := issuer.Release(r, receipt)
			if err != nil || session.Token == "" || session.AccountID != "actor" || session.ScopeResource != "target" {
				t.Fatal("genuine existing release failed", err)
			}
			if err := f.ds.Pool().QueryRow(ctx, "SELECT count(*) FROM sessions").Scan(&count); err != nil || count != 1 {
				t.Fatal("actual mint/release row missing", err)
			}
		})
	}
}
