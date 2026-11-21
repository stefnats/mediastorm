//go:build linux

package profileunlock

import (
	"context"
	"testing"

	"novastream/internal/auth"
	"novastream/internal/datastore"
)

func TestQuickConnectApproveAuthenticatedOriginalFrame(t *testing.T) {
	for _, kind := range []string{"original", "dto-only", "foreign-store", "wrong-client", "copied-issuer", "closed-issuer"} {
		t.Run(kind, func(t *testing.T) {
			f, issuer, challenge := quickConnectFacadePG(t)
			input, _ := quickConnectFacadeRequest(t, "approver-device")
			input.Header.Set("X-Client-ID", "approver-device")
			request := input
			if kind == "dto-only" {
				session, err := f.ds.Sessions().Get(t.Context(), quickConnectFacadeToken)
				if err != nil || session == nil {
					t.Fatal(err)
				}
				ctx := context.WithValue(input.Context(), auth.ContextKeySession, *session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
				request = input.WithContext(ctx)
			} else {
				pins := f.pins
				if kind == "foreign-store" {
					foreign, err := datastore.New(t.Context(), f.ds.Pool().Config().ConnString())
					if err != nil {
						t.Fatal(err)
					}
					defer foreign.Close()
					pins = NewPGProfilePins(foreign)
				}
				checked, release, err := pins.Authenticate(input, quickConnectFacadeToken, "approver-device")
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				request = checked
			}
			if kind == "wrong-client" {
				request.Header.Set("X-Client-ID", "different-device")
			}
			if kind == "copied-issuer" {
				copyOfIssuer := *issuer
				issuer = &copyOfIssuer
			}
			if kind == "closed-issuer" {
				issuer.Close()
			}
			approval, err := issuer.ApproveAuthenticated(request, challenge)
			if kind != "original" {
				if err == nil || approval != nil {
					t.Fatal("unframed/foreign/changed request was adopted", kind)
				}
				return
			}
			if err != nil || approval == nil {
				t.Fatal("same actual original PG frame", err)
			}
			quickConnectFacadeRelease(t, issuer, challenge, approval)
		})
	}
}
