//go:build linux

package profileunlock

import (
	"context"
	"testing"
	"time"
)

func TestBridgeSelectedLimiterFacadeOriginalIssuer(t *testing.T) {
	for _, kind := range []string{"exact", "nil", "duplicate", "foreign-selector", "copied-issuer"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionFacadePG(t)
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			r := bridgeCredentialFacadeRequest(ctx)
			o, err := f.security.CaptureBridgePolicy(r.Context())
			if err != nil {
				t.Fatal(err)
			}
			issuer := f.issuer
			expected := []BridgeLoginCandidate{{AccountID: "actor", ProfileID: "target"}}
			switch kind {
			case "nil":
				expected = nil
			case "duplicate":
				expected = append(expected, expected[0])
			case "foreign-selector":
				expected[0].AccountID = "foreign"
			case "copied-issuer":
				copyOfIssuer := *issuer
				issuer = &copyOfIssuer
			}
			proof, err := issuer.VerifySelectedPasswordForCandidates(r, "Synthetic Profile", "synthetic-device", "synthetic actor account password", o, expected)
			if proof != nil {
				defer proof.Close()
			}
			if kind != "exact" {
				if err == nil || proof != nil {
					t.Fatal("facade selectors or copied issuer authorized", kind)
				}
				return
			}
			if err != nil || proof == nil {
				t.Fatal("actual constrained facade", err)
			}
			receipt, err := issuer.CommitSelected(r, proof, o)
			if err != nil {
				t.Fatal(err)
			}
			defer receipt.Close()
			session, err := issuer.Release(r, receipt)
			if err != nil || session.Token == "" || session.AccountID != "actor" || session.ScopeResource != "target" {
				t.Fatal("actual constrained facade release", err)
			}
		})
	}
}
