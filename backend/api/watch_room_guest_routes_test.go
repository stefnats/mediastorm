//go:build linux

package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/utils"
)

type partyPublicCountBody struct{ reads atomic.Int32 }

func (b *partyPublicCountBody) Read([]byte) (int, error) { b.reads.Add(1); return 0, io.EOF }
func (*partyPublicCountBody) Close() error               { return nil }

func TestWatchPartyRegisteredPublicFlow(t *testing.T) {
	for _, kind := range []string{"actual-main-managed-selection", "token-page-and-join", "code-form-and-join", "guest-room-current-frame", "guest-ready-original", "guest-heartbeat-original", "guest-cookie-no-media-before-source", "missing-managed-before-body", "current-peer-admission", "base-path-real-main-wrapper"} {
		t.Run(kind, func(t *testing.T) {
			f := watchRoomRegisteredPG(t, false)
			provider := &partyRouteProvider{}
			video := handlers.NewVideoHandlerWithProvider(false, "", "", t.TempDir(), provider)
			t.Cleanup(video.Shutdown)
			pins := profileunlock.NewPGProfilePins(f.store, f.base.sessions)
			if e := ConfigureManagedWatchPartyPlayback(f.startup, f.store, pins, video, f.party); e != nil {
				t.Fatal(e)
			}
			f.router = partyRouteRegister(t, f, video, pins)
			runtime, e := f.startup.NewWatchRoomAuthority(f.store)
			if e != nil {
				t.Fatal(e)
			}
			path := "/api/users/" + f.base.open + "/watch-rooms"
			w := f.call(t, path, watchRoomRouteInput())
			var room models.WatchRoom
			if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &room) != nil || room.ID == "" {
				t.Fatal("registered native room", w.Code)
			}
			w = f.call(t, path+"/"+room.ID+"/external-invite", map[string]any{})
			var invite struct {
				URL  string `json:"url"`
				Code string `json:"shortCode"`
			}
			if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &invite) != nil || invite.Code == "" {
				t.Fatal("registered original invite", w.Code)
			}
			parsed, e := url.Parse(invite.URL)
			if e != nil || parsed.Path == "" {
				t.Fatal("actual invite URL")
			}
			if kind == "actual-main-managed-selection" {
				again, e := f.startup.NewWatchRoomAuthority(f.store)
				if e != nil || again != runtime || !pins.WatchPartyPlaybackReady(runtime) {
					t.Fatal("actual Main did not select same typed runtime", e)
				}
			}
			if kind == "missing-managed-before-body" {
				f.party.SetManagedPublicParty(nil)
				body := &partyPublicCountBody{}
				r := httptest.NewRequest("POST", "http://synthetic.test"+parsed.Path, body)
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				w = httptest.NewRecorder()
				f.router.ServeHTTP(w, r)
				if w.Code != 503 || body.reads.Load() != 0 || len(w.Result().Cookies()) != 0 {
					t.Fatal("registered missing runtime consumed body or fell back", w.Code, body.reads.Load())
				}
				return
			}
			if kind == "current-peer-admission" {
				var held []*datastore.WatchRoomHTTPEntry
				t.Cleanup(func() {
					for _, entry := range held {
						entry.Close()
					}
				})
				for _, peer := range []string{"192.0.2.8:9001", "[::ffff:192.0.2.8]:9002", "192.0.2.8:9003", "[::ffff:c000:208]:9004"} {
					r := httptest.NewRequest("POST", "http://synthetic.test"+parsed.Path, nil)
					r.RemoteAddr = peer
					entry, e := runtime.OpenHTTPEntry(r)
					if e != nil {
						t.Fatal(e)
					}
					held = append(held, entry)
				}
				body := &partyPublicCountBody{}
				r := httptest.NewRequest("POST", "http://synthetic.test"+parsed.Path, body)
				r.RemoteAddr = "192.0.2.8:9100"
				r.Header.Set("X-Forwarded-For", "198.51.100.9")
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				w = httptest.NewRecorder()
				f.router.ServeHTTP(w, r)
				if w.Code != 503 || body.reads.Load() != 0 {
					t.Fatal("mapped actual peer bypassed admission or read before admission", w.Code, body.reads.Load())
				}
				return
			}
			joinPath := parsed.Path
			if kind == "code-form-and-join" {
				r := httptest.NewRequest("POST", "http://synthetic.test/watch-party/code", strings.NewReader(url.Values{"code": {strings.ToLower(invite.Code)}}.Encode()))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				w = httptest.NewRecorder()
				f.router.ServeHTTP(w, r)
				if w.Code != 303 || !strings.Contains(w.Header().Get("Location"), invite.Code) {
					t.Fatal("registered code resolution", w.Code)
				}
				joinPath = "/watch-party/code/" + invite.Code
			}
			if kind == "token-page-and-join" || kind == "code-form-and-join" {
				w = httptest.NewRecorder()
				f.router.ServeHTTP(w, httptest.NewRequest("GET", "http://synthetic.test"+joinPath, nil))
				if w.Code != 200 || !strings.Contains(w.Body.String(), room.Title) {
					t.Fatal("registered current invitation page", w.Code)
				}
			}
			var public http.Handler = f.router
			if kind == "base-path-real-main-wrapper" {
				public = utils.BasePathHandler("/mediastorm", f.router)
				for _, prefix := range []string{"/mediastorm", ""} {
					w = httptest.NewRecorder()
					public.ServeHTTP(w, httptest.NewRequest("GET", "http://synthetic.test"+prefix+joinPath, nil))
					if w.Code != 200 || !strings.Contains(w.Body.String(), room.Title) {
						t.Fatal("actual Main base path prefix/direct route", prefix, w.Code)
					}
				}
				joinPath = "/mediastorm" + joinPath
			}
			r := httptest.NewRequest("POST", "http://synthetic.test"+joinPath, strings.NewReader(url.Values{"name": {"Synthetic registered public guest"}}.Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w = httptest.NewRecorder()
			public.ServeHTTP(w, r)
			var cookie *http.Cookie
			for _, value := range w.Result().Cookies() {
				if value.Name == "strmr_watch_party" {
					cookie = value
				}
			}
			if w.Code != 303 || cookie == nil || !cookie.HttpOnly {
				t.Fatal("registered known-ACK guest cookie", w.Code)
			}
			var guest string
			var expiry time.Time
			var privateCount int
			e = f.store.Pool().QueryRow(t.Context(), `SELECT g.guest_id,s.expires_at FROM watch_party_guest_bindings g JOIN sessions s ON s.token=g.session_token AND s.watch_party_guest_binding_id=g.binding_id WHERE s.token=$1 AND s.scope='watch_party' AND NOT s.is_master`, cookie.Value).Scan(&guest, &expiry)
			if e != nil || guest == "" {
				t.Fatal("cookie missing actual known-ACK private guest", e)
			}
			method, suffix, body := "GET", "", ""
			if kind == "guest-ready-original" {
				method = "POST"
				suffix = "/ready"
				body = `{"ready":true}`
			}
			if kind == "guest-heartbeat-original" {
				method = "POST"
				suffix = "/heartbeat?buffering=true"
			}
			guestPath := "/api/watch-party/room/" + room.ID + suffix
			if kind == "base-path-real-main-wrapper" {
				guestPath = "/mediastorm" + guestPath
			}
			r = httptest.NewRequest(method, "http://synthetic.test"+guestPath, strings.NewReader(body))
			r.AddCookie(cookie)
			r.Header.Set("Content-Type", "application/json")
			w = httptest.NewRecorder()
			public.ServeHTTP(w, r)
			want := 200
			if method == "POST" {
				want = 204
			}
			if w.Code != want {
				t.Fatal("registered original guest operation", kind, w.Code, w.Body.String())
			}
			if kind == "guest-ready-original" || kind == "guest-heartbeat-original" {
				var ready, buffering bool
				e = f.store.Pool().QueryRow(t.Context(), `SELECT ready,buffering FROM watch_room_guest_members WHERE room_id=$1 AND guest_id=$2`, room.ID, guest).Scan(&ready, &buffering)
				if e != nil || kind == "guest-ready-original" && !ready || kind == "guest-heartbeat-original" && !buffering {
					t.Fatal("registered route did not mutate same original guest", e)
				}
			}
			if kind == "guest-room-current-frame" {
				if !strings.Contains(w.Body.String(), room.ID) {
					t.Fatal("registered room view omitted original room")
				}
				if _, e = f.store.Pool().Exec(t.Context(), `DELETE FROM watch_party_guest_bindings WHERE session_token=$1`, cookie.Value); e != nil {
					t.Fatal(e)
				}
				w = httptest.NewRecorder()
				f.router.ServeHTTP(w, r)
				if w.Code == 200 {
					t.Fatal("registered route accepted cookie after private provenance removal")
				}
			}
			if kind == "guest-cookie-no-media-before-source" {
				r = httptest.NewRequest("POST", "http://synthetic.test/api/watch-party/room/"+room.ID+"/playback-grant", nil)
				r.AddCookie(cookie)
				w = httptest.NewRecorder()
				f.router.ServeHTTP(w, r)
				if w.Code == 200 || provider.calls.Load() != 0 {
					t.Fatal("lobby guest acquired playback without original source", w.Code)
				}
				if e = f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM watch_party_playback_bindings`).Scan(&privateCount); e != nil || privateCount != 0 {
					t.Fatal("lobby denial minted playback", privateCount, e)
				}
			}
			var after time.Time
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT expires_at FROM sessions WHERE token=$1`, cookie.Value).Scan(&after); e != nil || !after.Equal(expiry) {
				t.Fatal("registered ordinary guest operation renewed original expiry", e)
			}
		})
	}
}
