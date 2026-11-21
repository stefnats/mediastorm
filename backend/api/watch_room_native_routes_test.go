//go:build linux

package api

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
	"novastream/utils"
)

func nativeRegisteredRequest(t *testing.T, f *watchRoomRouteFixture, method, target string, value any) *httptest.ResponseRecorder {
	t.Helper()
	var body io.Reader
	if value != nil {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		body = bytes.NewReader(raw)
	}
	r := httptest.NewRequest(method, "http://synthetic.test"+target, body)
	r.Header.Set("Authorization", "Bearer "+f.token)
	r.Header.Set("X-Client-ID", "synthetic-room-api-client")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.router.ServeHTTP(w, r)
	return w
}

func nativeRegisteredRoom(t *testing.T, w *httptest.ResponseRecorder, code int) models.WatchRoom {
	t.Helper()
	var room models.WatchRoom
	if w.Code != code || json.Unmarshal(w.Body.Bytes(), &room) != nil || room.ID == "" {
		t.Fatal("actual registered native room response", w.Code, w.Body.String())
	}
	return room
}

func nativeRegisteredMember(t *testing.T, room models.WatchRoom, profile string) models.WatchRoomMember {
	t.Helper()
	for _, member := range room.Members {
		if member.ProfileID == profile {
			return member
		}
	}
	t.Fatal("actual registered native member absent")
	return models.WatchRoomMember{}
}

type nativeRegisteredBody struct{ reads atomic.Int32 }

func (b *nativeRegisteredBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (*nativeRegisteredBody) Close() error               { return nil }

func TestNativeRoomActualRegisteredRoutes(t *testing.T) {
	for _, kind := range []string{"actual-main-current-core", "registered-creator-read", "registered-household-invitation", "registered-join", "registered-ready", "registered-state", "registered-heartbeat", "registered-leave-rejoin", "registered-end", "registered-account-invite", "registered-recipient-inbox", "registered-room-inbox", "registered-accept", "registered-decline", "registered-revoke", "upstream-body-entry-order", "scoped-token-no-native-mutation", "base-path-actual-wrapper"} {
		t.Run(kind, func(t *testing.T) {
			f := watchRoomRegisteredPG(t, false)
			participant, err := f.base.users.CreateForAccount(f.base.account, "Synthetic native route participant")
			if err != nil {
				t.Fatal(err)
			}
			input := watchRoomRouteInput()
			input.InviteeProfileIDs = []string{participant.ID}
			createPath := "/api/users/" + f.base.open + "/watch-rooms"
			room := nativeRegisteredRoom(t, f.call(t, createPath, input), http.StatusCreated)
			creatorPath := createPath + "/" + room.ID
			memberPath := "/api/users/" + participant.ID + "/watch-rooms/" + room.ID
			join := map[string]any{"clientId": "synthetic-room-api-client", "capabilities": input.Capabilities}
			if kind == "upstream-body-entry-order" {
				f.rooms.SetManagedWatchRoomAuthority(nil, "")
				body := &nativeRegisteredBody{}
				r := httptest.NewRequest("POST", "http://synthetic.test"+memberPath+"/join", body)
				// Even a nonexistent credential cannot make auth/body work precede
				// this registered route's missing actual runtime check.
				r.Header.Set("Authorization", "Bearer synthetic-nonexistent")
				w := httptest.NewRecorder()
				f.router.ServeHTTP(w, r)
				if w.Code != 503 || body.reads.Load() != 0 {
					t.Fatal("native entry did not precede auth/body", w.Code, body.reads.Load())
				}
				var members int
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_members WHERE room_id=$1 AND profile_id=$2`, room.ID, participant.ID).Scan(&members); err != nil || members != 0 {
					t.Fatal("denied native entry wrote a member", err)
				}
				return
			}
			if kind == "scoped-token-no-native-mutation" {
				scoped, err := f.base.sessions.CreateScopedWithResource(f.base.account, false, "synthetic-native-scope", "loopback", time.Hour, models.SessionScopeJellyfin, f.base.open)
				if err != nil {
					t.Fatal(err)
				}
				f.token = scoped.Token
				w := nativeRegisteredRequest(t, f, "POST", creatorPath+"/state", models.WatchRoomStateUpdate{Status: models.WatchRoomStatusPaused, Position: 9})
				if w.Code != 403 {
					t.Fatal("scoped session reached native state mutation", w.Code)
				}
				var revision int64
				var status string
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT revision,status FROM watch_rooms WHERE id=$1`, room.ID).Scan(&revision, &status); err != nil || revision != room.Revision || status != room.Status {
					t.Fatal("scoped native request changed room", err)
				}
				return
			}
			if kind == "base-path-actual-wrapper" {
				server := httptest.NewServer(utils.BasePathHandler("/app", f.router))
				defer server.Close()
				client := server.Client()
				client.Timeout = 5 * time.Second
				r, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/app"+creatorPath, nil)
				if err != nil {
					t.Fatal(err)
				}
				r.Header.Set("Authorization", "Bearer "+f.token)
				r.Header.Set("X-Client-ID", "synthetic-room-api-client")
				response, err := client.Do(r)
				if err != nil {
					if response != nil && response.Body != nil {
						_ = response.Body.Close()
					}
					t.Fatal(err)
				}
				raw, readErr := io.ReadAll(response.Body)
				closeErr := response.Body.Close()
				var actual models.WatchRoom
				if readErr != nil || closeErr != nil || response.StatusCode != 200 || json.Unmarshal(raw, &actual) != nil || actual.ID != room.ID {
					t.Fatal("real prefixed native response incomplete", readErr, closeErr, response.StatusCode)
				}
				return
			}
			if strings.Contains(kind, "account-invite") || kind == "registered-recipient-inbox" || kind == "registered-room-inbox" || kind == "registered-accept" || kind == "registered-decline" || kind == "registered-revoke" {
				var username string
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT username FROM accounts WHERE id=$1`, f.base.other).Scan(&username); err != nil {
					t.Fatal(err)
				}
				w := nativeRegisteredRequest(t, f, "POST", creatorPath+"/account-invites", map[string]string{"username": username})
				var invite models.WatchRoomAccountInvite
				if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &invite) != nil || invite.ID == "" || invite.Status != models.WatchRoomAccountInvitePending {
					t.Fatal("actual registered account invitation", w.Code, w.Body.String())
				}
				var originals int
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_account_invite_provenance WHERE invite_id=$1 AND recipient_account_id=$2`, invite.ID, f.base.other).Scan(&originals); err != nil || originals != 1 {
					t.Fatal("native invitation missed original recipient provenance", err)
				}
				if kind == "registered-account-invite" {
					return
				}
				if kind == "registered-recipient-inbox" || kind == "registered-accept" || kind == "registered-decline" {
					recipient, err := f.base.sessions.Create(f.base.other, false, "synthetic-native-recipient", "loopback")
					if err != nil {
						t.Fatal(err)
					}
					f.token = recipient.Token
				}
				switch kind {
				case "registered-recipient-inbox", "registered-room-inbox":
					target := "/api/watch-room-account-invitations"
					if kind == "registered-room-inbox" {
						target = creatorPath + "/account-invites"
					}
					w = nativeRegisteredRequest(t, f, "GET", target, nil)
					var invitations []models.WatchRoomAccountInvite
					if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &invitations) != nil || len(invitations) != 1 || invitations[0].ID != invite.ID || bytes.Contains(w.Body.Bytes(), []byte(f.base.other)) {
						t.Fatal("actual bounded native invitation inbox", w.Code, w.Body.String())
					}
				case "registered-accept":
					actual := nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "POST", "/api/users/"+f.base.foreign+"/watch-room-account-invitations/"+invite.ID+"/accept", map[string]any{}), 200)
					if actual.ID != room.ID || nativeRegisteredMember(t, actual, f.base.foreign).Joined {
						t.Fatal("account acceptance lost unjoined native wire")
					}
					actual = nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "POST", "/api/users/"+f.base.foreign+"/watch-rooms/"+room.ID+"/join", join), 200)
					if !nativeRegisteredMember(t, actual, f.base.foreign).Joined {
						t.Fatal("actual accepted recipient failed native join")
					}
					var accepted string
					var admissions int
					if err := f.store.Pool().QueryRow(t.Context(), `SELECT accepted_profile_id FROM watch_room_account_invites WHERE id=$1 AND status='accepted'`, invite.ID).Scan(&accepted); err != nil || accepted != f.base.foreign {
						t.Fatal("account acceptance status missing", err)
					}
					if err := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id=$2 AND admission_kind='account' AND member_incarnation IS NOT NULL`, room.ID, f.base.foreign).Scan(&admissions); err != nil || admissions != 1 {
						t.Fatal("accepted account lacks genuine joined original admission", err)
					}
				case "registered-decline", "registered-revoke":
					target, method, want := "/api/watch-room-account-invitations/"+invite.ID+"/decline", "POST", models.WatchRoomAccountInviteDeclined
					if kind == "registered-revoke" {
						target, method, want = creatorPath+"/account-invites/"+invite.ID, "DELETE", models.WatchRoomAccountInviteRevoked
					}
					w = nativeRegisteredRequest(t, f, method, target, map[string]any{})
					var status string
					if w.Code != 204 || w.Body.Len() != 0 {
						t.Fatal("native terminal invite wire", w.Code)
					}
					if err := f.store.Pool().QueryRow(t.Context(), `SELECT status FROM watch_room_account_invites WHERE id=$1`, invite.ID).Scan(&status); err != nil || status != want {
						t.Fatal("native terminal invite not committed", err)
					}
				}
				return
			}
			if kind == "registered-household-invitation" {
				w := nativeRegisteredRequest(t, f, "GET", "/api/users/"+participant.ID+"/watch-rooms/invitations", nil)
				var invitations []models.WatchRoom
				if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &invitations) != nil || len(invitations) != 1 || invitations[0].ID != room.ID || nativeRegisteredMember(t, invitations[0], participant.ID).Joined {
					t.Fatal("actual invited household placeholder lost", w.Code)
				}
				return
			}
			if kind == "actual-main-current-core" {
				file, err := parser.ParseFile(token.NewFileSet(), "../main.go", nil, 0)
				if err != nil {
					t.Fatal(err)
				}
				composition, maintenance := false, false
				ast.Inspect(file, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					receiver, ok := selector.X.(*ast.Ident)
					if !ok {
						return true
					}
					if receiver.Name == "api" && selector.Sel.Name == "ConfigureManagedWatchRoomAuthority" && len(call.Args) == 5 {
						composition = true
					}
					if receiver.Name == "managedStartup" && selector.Sel.Name == "StartWatchRoomMaintenance" && len(call.Args) == 1 {
						maintenance = true
					}
					return true
				})
				if !composition || !maintenance {
					t.Fatal("actual Main missed concrete room/maintenance composition")
				}
				first, err := f.startup.NewWatchRoomAuthority(f.store)
				if err != nil || !first.MatchesStore(f.store) {
					t.Fatal("actual runtime not retained", err)
				}
				again, err := f.startup.NewWatchRoomAuthority(f.store)
				if err != nil || again != first || f.startup.StartWatchRoomMaintenance(f.store) != nil {
					t.Fatal("actual Main composition substituted runtime", err)
				}
				var admitted int
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id=$2 AND admission_kind='creator'`, room.ID, f.base.open).Scan(&admitted); err != nil || admitted != 1 {
					t.Fatal("actual registered create missed private native admission", err)
				}
			}
			if kind == "registered-creator-read" || kind == "actual-main-current-core" {
				actual := nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "GET", creatorPath, nil), 200)
				if actual.ID != room.ID || !nativeRegisteredMember(t, actual, f.base.open).IsCreator || nativeRegisteredMember(t, actual, participant.ID).Joined {
					t.Fatal("actual current native room wire changed")
				}
				return
			}
			if kind == "registered-end" {
				w := nativeRegisteredRequest(t, f, "POST", creatorPath+"/end", map[string]any{})
				if w.Code != 204 || w.Body.Len() != 0 {
					t.Fatal("native end wire", w.Code)
				}
				actual := nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "GET", creatorPath, nil), 200)
				if actual.Status != models.WatchRoomStatusEnded || actual.EndedAt == nil || actual.EndReason != "host_ended" {
					t.Fatal("ended native audit fields missing")
				}
				return
			}
			actual := nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "POST", memberPath+"/join", join), 200)
			member := nativeRegisteredMember(t, actual, participant.ID)
			if !member.Joined || !member.Capabilities.NativePlayback || !member.Capabilities.StateSync {
				t.Fatal("actual native join missing capabilities/member")
			}
			switch kind {
			case "registered-join":
				var admitted int
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id=$2 AND admission_kind='household' AND member_incarnation IS NOT NULL`, room.ID, participant.ID).Scan(&admitted); err != nil || admitted != 1 {
					t.Fatal("actual join missed original membership", err)
				}
			case "registered-ready":
				actual = nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "POST", memberPath+"/ready", map[string]bool{"ready": true}), 200)
				if !nativeRegisteredMember(t, actual, participant.ID).Ready {
					t.Fatal("native ready mutation missing")
				}
			case "registered-state":
				update := models.WatchRoomStateUpdate{Status: models.WatchRoomStatusPaused, Position: 2.5, Duration: 90, ExpectedRevision: &actual.Revision}
				changed := nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "POST", memberPath+"/state", update), 200)
				if changed.Status != models.WatchRoomStatusPaused || changed.Position != 2.5 || changed.Revision != actual.Revision+1 {
					t.Fatal("registered current state/revision changed")
				}
				w := nativeRegisteredRequest(t, f, "POST", memberPath+"/state", update)
				if w.Code != 409 {
					t.Fatal("stale registered revision mutated room", w.Code)
				}
			case "registered-heartbeat":
				w := nativeRegisteredRequest(t, f, "POST", memberPath+"/heartbeat", map[string]any{"clientId": "native-heartbeat-client", "buffering": true})
				var client string
				var buffering bool
				if w.Code != 204 || w.Body.Len() != 0 {
					t.Fatal("native heartbeat wire", w.Code)
				}
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT client_id,buffering FROM watch_room_members WHERE room_id=$1 AND profile_id=$2`, room.ID, participant.ID).Scan(&client, &buffering); err != nil || client != "native-heartbeat-client" || !buffering {
					t.Fatal("actual heartbeat not committed", err)
				}
			case "registered-leave-rejoin":
				w := nativeRegisteredRequest(t, f, "POST", memberPath+"/leave", map[string]any{})
				if w.Code != 204 || w.Body.Len() != 0 {
					t.Fatal("native leave wire", w.Code)
				}
				var members, admissions int
				if err := f.store.Pool().QueryRow(t.Context(), `SELECT (SELECT count(*) FROM watch_room_members WHERE room_id=$1 AND profile_id=$2),(SELECT count(*) FROM watch_room_native_admissions WHERE room_id=$1 AND profile_id=$2 AND member_incarnation IS NULL)`, room.ID, participant.ID).Scan(&members, &admissions); err != nil || members != 0 || admissions != 1 {
					t.Fatal("native leave discarded original rejoin admission", err)
				}
				actual = nativeRegisteredRoom(t, nativeRegisteredRequest(t, f, "POST", memberPath+"/join", join), 200)
				if !nativeRegisteredMember(t, actual, participant.ID).Joined {
					t.Fatal("actual original native rejoin missing")
				}
			default:
				t.Fatal("unhandled frozen native route case", kind)
			}
		})
	}
}
