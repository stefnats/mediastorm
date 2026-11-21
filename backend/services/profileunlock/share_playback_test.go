//go:build linux

package profileunlock

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/sessions"
	"novastream/services/watchrooms"
	"testing"
	"time"
)

func TestSharePlaybackConcreteManagedAuthentication(t *testing.T) {
	for _, kind := range []string{"original-plain-stream", "managed-missing-verifier", "managed-closed-verifier", "managed-foreign-verifier", "legacy-constructor-retained", "actual-party-resolver-retained", "lookalike-party-no-bypass", "dto-sidecar-no-frame", "raw-restored-session-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionFacadePG(t)
			if _, e := f.ds.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id='target'`); e != nil {
				t.Fatal(e)
			}
			service, e := sessions.NewServiceWithStore(f.ds, time.Hour)
			if e != nil {
				t.Fatal(e)
			}
			pins := NewPGProfilePins(f.ds, service)
			now := time.Now().UTC().Truncate(time.Microsecond)
			full := models.Session{Token: "synthetic-share-facade-full", AccountID: "actor", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
			if e = f.ds.Sessions().Create(t.Context(), &full); e != nil {
				t.Fatal(e)
			}
			creator, e := datastore.NewShareLinkCreator(t.Context(), f.ds, f.manager, f.security, f.coordinator)
			if e != nil {
				t.Fatal(e)
			}
			r, release, e := pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil), full.Token, "")
			if e != nil {
				t.Fatal(e)
			}
			link, e := creator.Create(r, "target", map[string]string{"sourcePath": "synthetic:facade.wav", "profileId": "target"}, time.Hour, 4, "")
			release()
			if e != nil {
				t.Fatal(e)
			}
			runtime, e := datastore.NewShareLinkRedeemer(t.Context(), f.ds, f.manager, f.security, f.coordinator)
			if e != nil {
				t.Fatal(e)
			}
			defer runtime.Close()
			result, e := runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
			if e != nil {
				t.Fatal(e)
			}
			if kind != "legacy-constructor-retained" {
				if e = pins.SetManagedSharePlayback(runtime); e != nil {
					t.Fatal(e)
				}
			}
			token := result.Session.Token
			switch kind {
			case "managed-missing-verifier":
				pins = NewPGProfilePins(f.ds, service)
				if pins.SetManagedSharePlayback(nil) == nil {
					t.Fatal("missing verifier advertised ready")
				}
			case "managed-closed-verifier":
				runtime.Close()
			case "managed-foreign-verifier":
				foreign, e := datastore.New(t.Context(), f.ds.Pool().Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				pins = NewPGProfilePins(foreign, service)
				if pins.SetManagedSharePlayback(runtime) == nil {
					t.Fatal("foreign runtime advertised ready")
				}
			case "lookalike-party-no-bypass":
				if _, e = f.ds.Pool().Exec(t.Context(), `UPDATE sessions SET scope_resource='watch-party-playback:v1:{"roomId":"lookalike","guestId":"synthetic-guest","resource":"synthetic:facade.wav"}' WHERE token=$1`, token); e != nil {
					t.Fatal(e)
				}
			case "raw-restored-session-denied":
				restored := result.Session
				restored.Token = "synthetic-generic-restored-share"
				if e = f.ds.Sessions().Create(t.Context(), &restored); e != nil {
					t.Fatal(e)
				}
				token = restored.Token
			case "actual-party-resolver-retained":
				repo := f.ds.WatchRooms()
				rooms := watchrooms.New(repo, nil, nil)
				service.SetWatchPartyPlaybackValidator(rooms.ValidateExternalPlayback)
				room := &models.WatchRoom{ID: "synthetic-facade-room", CreatorProfileID: "target", Title: "Synthetic party", MediaType: "movie", ItemID: "synthetic-item", Params: map[string]string{}, Status: models.WatchRoomStatusPlaying, Revision: 1, AnchorUpdatedAt: now, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
				if e = repo.Create(t.Context(), room, nil, "", models.WatchRoomClientCapabilities{}); e != nil {
					t.Fatal(e)
				}
				if e = repo.JoinExternalGuest(t.Context(), room.ID, "synthetic-guest", "Synthetic guest", "", models.WatchRoomClientCapabilities{}, now); e != nil {
					t.Fatal(e)
				}
				if yes, e := repo.BindExternalSource(t.Context(), room.ID, "target", "synthetic:facade.wav", map[string]string{}, now); e != nil || !yes {
					t.Fatal("actual party source", e)
				}
				grant, e := service.CreateWatchPartyPlayback("actor", "Synthetic TV", "127.0.0.1", time.Hour, room.ID, "synthetic-guest", "synthetic:facade.wav")
				if e != nil {
					t.Fatal(e)
				}
				token = grant.Token
				// Genuine existing party resolution is independent of a missing share runtime.
				pins = NewPGProfilePins(f.ds, service)
				_ = pins.SetManagedSharePlayback(nil)
			case "dto-sidecar-no-frame":
				raw := httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, result.Session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, "actor")
				raw = raw.WithContext(ctx)
				if owner, e := pins.CaptureSharePlaybackOwner(raw, "target", "synthetic:facade.wav"); e == nil || owner != nil {
					t.Fatal("session DATA replaced original PG frame")
				}
				return
			}
			request := httptest.NewRequest(http.MethodGet, "http://synthetic.test/api/video/stream?profileId=target", nil)
			checked, release, e := pins.Authenticate(request, token, "")
			accepted := kind == "original-plain-stream" || kind == "legacy-constructor-retained" || kind == "actual-party-resolver-retained"
			if !accepted {
				if e == nil || checked != nil {
					if release != nil {
						release()
					}
					t.Fatal("untrusted managed authentication admitted", kind)
				}
				return
			}
			if e != nil || checked == nil {
				t.Fatal("actual original authentication rejected", kind, e)
			}
			defer release()
			owner, e := pins.CaptureSharePlaybackOwner(checked, "target", "synthetic:facade.wav")
			if kind == "actual-party-resolver-retained" {
				if !errors.Is(e, ErrSharePlaybackParty) || owner != nil {
					t.Fatal("genuine party required share sidecar", e)
				}
				return
			}
			if kind == "legacy-constructor-retained" {
				if e == nil || owner != nil {
					t.Fatal("legacy constructor silently acquired managed source")
				}
				return
			}
			if e != nil || owner == nil || owner.CheckRequest(checked, "target", "synthetic:facade.wav") != nil {
				t.Fatal("genuine facade source unavailable", e)
			}
			defer owner.Close()
			if session, ok := auth.GetSession(checked); !ok || session.IsMaster || session.Scope != models.SessionScopeStream {
				t.Fatal("source inherited broad authority")
			}
		})
	}
}
