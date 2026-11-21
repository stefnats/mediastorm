//go:build linux

package profileunlock

import (
	"net/http/httptest"
	"testing"
	"time"

	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/sessions"
	"novastream/services/watchrooms"
)

type partyFacadeFixture struct {
	base                  *bridgeSessionFacadeFixture
	pins                  *PGProfilePins
	runtime               *datastore.WatchRoomAuthority
	sessions              *sessions.Service
	token                 string
	room, guest, resource string
}

func partyFacadePG(t *testing.T, master bool) *partyFacadeFixture {
	t.Helper()
	base := bridgeSessionFacadePG(t)
	if master {
		if _, e := base.ds.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor'`); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := base.ds.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id='target'`); e != nil {
		t.Fatal(e)
	}
	service, e := sessions.NewServiceWithStore(base.ds, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	pins := NewPGProfilePins(base.ds, service)
	now := time.Now().UTC().Truncate(time.Microsecond)
	full := models.Session{Token: "synthetic-party-facade-full", AccountID: "actor", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if e = base.ds.Sessions().Create(t.Context(), &full); e != nil {
		t.Fatal(e)
	}
	actual, release, e := pins.Authenticate(httptest.NewRequest("POST", "http://synthetic.test/api/users/target/watch-rooms", nil), full.Token, "")
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	runtime, e := datastore.NewWatchRoomAuthority(t.Context(), base.ds, base.manager, base.security, base.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(runtime.Close)
	room, e := runtime.CreateRoom(actual, "target", models.WatchRoomCreate{Title: "Synthetic facade party", MediaType: "movie", ItemID: "synthetic-item", Params: map[string]string{}, Capabilities: models.WatchRoomClientCapabilities{NativePlayback: true, StateSync: true, ProtocolVersion: 1}})
	if e != nil {
		t.Fatal(e)
	}
	invite, e := runtime.ReplaceExternalInvite(actual, "target", room.ID)
	if e != nil {
		t.Fatal(e)
	}
	joined, e := runtime.JoinInvitation(httptest.NewRequest("POST", "http://synthetic.test/watch-party/"+invite.Token, nil), invite.Token, false, "Synthetic facade guest", "")
	if e != nil {
		t.Fatal(e)
	}
	resource := "synthetic:facade-party.wav"
	if _, e = runtime.BindExternalSource(actual, "target", room.ID, resource, map[string]string{"profileId": "target", "titleId": "synthetic-item"}); e != nil {
		t.Fatal(e)
	}
	if _, e = base.ds.Pool().Exec(t.Context(), `UPDATE watch_rooms SET status='playing',waiting_for_ready=false WHERE id=$1`, room.ID); e != nil {
		t.Fatal(e)
	}
	guest, stop, e := runtime.AuthenticateGuestRequest(httptest.NewRequest("POST", "http://synthetic.test/api/watch-party/room/"+room.ID+"/playback-grant", nil), joined.Session.Token, room.ID)
	if e != nil {
		t.Fatal(e)
	}
	grant, e := runtime.GrantPartyPlayback(guest, room.ID)
	stop()
	if e != nil {
		t.Fatal(e)
	}
	return &partyFacadeFixture{base, pins, runtime, service, grant.Session.Token, room.ID, joined.GuestID, resource}
}
func TestManagedWatchPartyClassification(t *testing.T) {
	for _, kind := range []string{"actual-managed-original", "missing-managed-dependency", "closed-runtime", "foreign-store", "legacy-isolated-constructor", "lookalike-prefix-no-bypass", "share-independent", "scoped-nonmaster-normalization"} {
		t.Run(kind, func(t *testing.T) {
			f := partyFacadePG(t, kind == "scoped-nonmaster-normalization")
			pins := f.pins
			if kind == "legacy-isolated-constructor" {
				// Explicit isolated legacy composition keeps its actual established room
				// resolver; it does not make an original managed party source owner.
				rooms := watchrooms.New(f.base.ds.WatchRooms(), nil, nil)
				f.sessions.SetWatchPartyPlaybackValidator(rooms.ValidateExternalPlayback)
				checked, release, e := pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), f.token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer release()
				if !pins.separateOriginalParty(checked) {
					t.Fatal("genuine isolated legacy resolver lost classification")
				}
				return
			}
			switch kind {
			case "missing-managed-dependency":
				if pins.SetManagedWatchPartyPlayback(nil) == nil {
					t.Fatal("missing managed party advertised ready")
				}
			case "foreign-store":
				foreign, e := datastore.New(t.Context(), f.base.ds.Pool().Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				pins = NewPGProfilePins(foreign, f.sessions)
				if pins.SetManagedWatchPartyPlayback(f.runtime) == nil {
					t.Fatal("foreign store selected original party runtime")
				}
			default:
				if e := pins.SetManagedWatchPartyPlayback(f.runtime); e != nil {
					t.Fatal(e)
				}
			}
			if kind == "closed-runtime" {
				f.runtime.Close()
			}
			if kind == "share-independent" {
				if pins.SetManagedSharePlayback(nil) == nil {
					t.Fatal("missing separate share advertised ready")
				}
			}
			if kind == "scoped-nonmaster-normalization" {
				// A current master role belongs to a NEW actual room/delegation, never a
				// mutation of an already-attested original principal. Use the delegated
				// session's stored role tamper instead: the genuine PG producer normalizes
				// it back to nonmaster and private proof denies its changed physical row.
				if _, e := f.base.ds.Pool().Exec(t.Context(), `UPDATE sessions SET is_master=true WHERE token=$1`, f.token); e == nil {
					t.Fatal("new private purpose constraint allowed master delegated row")
				}
			}
			if kind == "lookalike-prefix-no-bypass" {
				original, stop, e := f.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), f.token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer stop()
				session, _, e := f.base.ds.ProfilePinFrameIdentity(original)
				if e != nil {
					t.Fatal(e)
				}
				session.Token = "synthetic-unattested-party-copy"
				if e = f.base.ds.Sessions().Create(t.Context(), &session); e != nil {
					t.Fatal(e)
				}
				f.token = session.Token
			}
			checked, release, e := pins.Authenticate(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), f.token, "")
			denied := kind == "missing-managed-dependency" || kind == "closed-runtime" || kind == "foreign-store" || kind == "lookalike-prefix-no-bypass"
			if denied {
				if e == nil || checked != nil {
					if release != nil {
						release()
					}
					t.Fatal("managed missing/foreign/unattested party bypassed", kind)
				}
				return
			}
			if e != nil || checked == nil {
				t.Fatal("actual original managed party auth", e)
			}
			defer release()
			session, account, e := pins.Identity(checked)
			if e != nil || session.ScopeResource != f.resource || session.IsMaster || account.IsMaster {
				t.Fatal("genuine scoped normalization widened source role", e)
			}
			owner, e := pins.CaptureWatchPartyPlaybackOwner(checked, "target", f.resource)
			if e != nil || owner == nil {
				t.Fatal("concrete actual party owner not consumed", e)
			}
			defer owner.Close()
			if e = owner.Check(t.Context()); e != nil {
				t.Fatal(e)
			}
		})
	}
}
