//go:build linux

package api

import (
	"context"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/streaming"
	"novastream/services/watchrooms"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type shareRouteProvider struct{ calls atomic.Int32 }

func (p *shareRouteProvider) Stream(context.Context, streaming.Request) (*streaming.Response, error) {
	p.calls.Add(1)
	return &streaming.Response{Body: io.NopCloser(strings.NewReader("SYNTHETIC-REGISTERED-SHARE")), Status: 200, Headers: make(http.Header), ContentLength: 26}, nil
}
func shareActualRegistered(t *testing.T, f *recordingProgressRouteFixture, video *handlers.VideoHandler, share *handlers.ShareHandler) *mux.Router {
	t.Helper()
	accountService, e := accounts.NewServiceWithStore(f.ds)
	if e != nil {
		t.Fatal(e)
	}
	meta := handlers.NewMetadataHandler(f.facts, nil)
	meta.UsersService = f.users
	list := handlers.NewDisplayListHandler(nil, nil, f.users)
	recordingHandler := handlers.NewRecordingsHandler(f.service, f.users)
	router := mux.NewRouter()
	Register(router, nil, meta, nil, nil, nil, nil, nil, nil, video, handlers.NewUsersHandler(f.users, accountService), nil, nil, list, &handlers.HistoryHandler{},
		nil, nil, nil, recordingHandler, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, accountService, f.sessions, f.users, share, nil, nil, "", nil,
		WithPGProfilePins(f.pins), WithProfileUnlocks(profileunlock.NewService(f.ds.ProfileUnlocks()), func() config.SecuritySettings { return config.SecuritySettings{EnforceProfilePins: f.enforce.Load()} }), WithRecordingProgress(f.progress))
	return router
}
func TestSharePlaybackRegisteredAPI(t *testing.T) {
	for _, kind := range []string{"main-bound-plain-stream", "missing-consumer-denied", "missing-verifier-denied", "full-session-retained", "party-resolver-retained", "query-token-media", "body-selector-not-permission", "hls-stored-profile-resource", "nonmedia-endpoint-denied"} {
		t.Run(kind, func(t *testing.T) {
			f := recordingProgressRoutePG(t, false)
			resource := "synthetic:registered-share"
			if _, e := f.ds.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id=$1`, f.open); e != nil {
				t.Fatal(e)
			}
			creator, e := datastore.NewShareLinkCreator(t.Context(), f.ds, f.manager, f.security, f.coordinator)
			if e != nil {
				t.Fatal(e)
			}
			raw := httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil)
			original, release, e := f.pins.Authenticate(raw, f.token, "synthetic-progress-client")
			if e != nil {
				t.Fatal(e)
			}
			link, e := creator.Create(original, f.open, map[string]string{"sourcePath": resource, "profileId": f.open}, time.Hour, 4, "")
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
			if e = f.pins.SetManagedSharePlayback(runtime); e != nil {
				t.Fatal(e)
			}
			p := &shareRouteProvider{}
			video := handlers.NewVideoHandlerWithProvider(false, "", "", t.TempDir(), p)
			defer video.Shutdown()
			if e = video.SetManagedSharePlayback(runtime, f.pins); e != nil {
				t.Fatal(e)
			}
			share := handlers.NewShareHandler(handlers.NewShareStore(f.ds.ShareLinks()), f.sessions, f.users, "")
			share.SetManagedShareCreation(creator)
			share.SetManagedShareRedemption(runtime)
			token := result.Session.Token
			method := "GET"
			path := "/api/video/stream?path=%40shared&profileId=" + url.QueryEscape(f.open)
			var body io.Reader
			switch kind {
			case "missing-consumer-denied":
				_ = video.SetManagedSharePlayback(nil, f.pins)
			case "missing-verifier-denied":
				f.pins = profileunlock.NewPGProfilePins(f.ds, f.sessions)
				_ = f.pins.SetManagedSharePlayback(nil)
				_ = video.SetManagedSharePlayback(runtime, f.pins)
			case "full-session-retained":
				token = f.token
				path = "/api/video/stream?path=" + url.QueryEscape(resource) + "&profileId=" + f.open
			case "party-resolver-retained":
				repo := f.ds.WatchRooms()
				rooms := watchrooms.New(repo, f.users, nil)
				f.sessions.SetWatchPartyPlaybackValidator(rooms.ValidateExternalPlayback)
				now := time.Now().UTC()
				room := &models.WatchRoom{ID: "synthetic-registered-share-party", CreatorProfileID: f.open, Title: "Synthetic party", MediaType: "movie", ItemID: "synthetic-item", Params: map[string]string{}, Status: models.WatchRoomStatusPlaying, Revision: 1, AnchorUpdatedAt: now, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
				if e = repo.Create(t.Context(), room, nil, "", models.WatchRoomClientCapabilities{}); e != nil {
					t.Fatal(e)
				}
				if e = repo.JoinExternalGuest(t.Context(), room.ID, "synthetic-guest", "Synthetic guest", "", models.WatchRoomClientCapabilities{}, now); e != nil {
					t.Fatal(e)
				}
				if yes, e := repo.BindExternalSource(t.Context(), room.ID, f.open, resource, map[string]string{}, now); e != nil || !yes {
					t.Fatal("actual party source", e)
				}
				grant, e := f.sessions.CreateWatchPartyPlayback(f.account, "Synthetic guest", "127.0.0.1", time.Hour, room.ID, "synthetic-guest", resource)
				if e != nil {
					t.Fatal(e)
				}
				token = grant.Token
			case "query-token-media":
				path += "&token=" + url.QueryEscape(token)
			case "body-selector-not-permission":
				method = "POST"
				path = "/api/share/create"
				body = strings.NewReader(`{"params":{"profileId":"forged-other","sourcePath":"synthetic:other"}}`)
			case "hls-stored-profile-resource":
				path = "/api/video/hls/foreign-session/status?profileId=" + url.QueryEscape(f.foreign)
			case "nonmedia-endpoint-denied":
				path = "/api/users"
			}
			router := shareActualRegistered(t, f, video, share)
			request := httptest.NewRequest(method, "http://synthetic.test"+path, body)
			if kind != "query-token-media" {
				request.Header.Set("Authorization", "Bearer "+token)
			}
			if kind == "full-session-retained" {
				request.Header.Set("X-Client-ID", "synthetic-progress-client")
			}
			request.Header.Set("Content-Type", "application/json")
			record := httptest.NewRecorder()
			router.ServeHTTP(record, request)
			want := 200
			switch kind {
			case "missing-consumer-denied", "missing-verifier-denied":
				want = 503
			case "body-selector-not-permission", "hls-stored-profile-resource", "nonmedia-endpoint-denied":
				want = 403
			}
			if record.Code != want {
				t.Fatal("registered shared route status", kind, record.Code, record.Body.String())
			}
			if want == 200 {
				if record.Body.String() != "SYNTHETIC-REGISTERED-SHARE" || p.calls.Load() != 1 {
					t.Fatal("actual registered provider/body not consumed")
				}
			} else if p.calls.Load() != 0 {
				t.Fatal("denied registered route opened source")
			}
		})
	}
}
