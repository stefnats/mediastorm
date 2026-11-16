package api

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/handlers"
	"novastream/services/sessions"
	"novastream/services/streaming"
)

type internalRecordingRouteProvider struct{ calls int }

func (p *internalRecordingRouteProvider) Stream(_ context.Context, req streaming.Request) (*streaming.Response, error) {
	p.calls++
	return &streaming.Response{Body: io.NopCloser(bytes.NewBufferString("synthetic-recording")), Headers: http.Header{"Content-Type": []string{"video/mp2t"}}, Status: 200, ContentLength: 19}, nil
}

func TestRegisteredRecordingInternalStreamRetainsOnlyActualPeerException(t *testing.T) {
	p := &internalRecordingRouteProvider{}
	video := handlers.NewVideoHandlerWithProvider(false, "", "", t.TempDir(), p)
	if manager := video.GetHLSManager(); manager != nil {
		t.Cleanup(manager.Shutdown)
	}
	sessionsSvc, err := sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	router := mux.NewRouter()
	Register(router,
		nil,                         // settings
		&handlers.MetadataHandler{}, // metadata
		nil,                         // indexer
		nil,                         // playback
		nil,                         // bad streams
		nil,                         // prequeue
		nil,                         // usenet
		nil,                         // debrid
		video,
		nil, // users
		nil, // watchlist
		nil, // custom lists
		nil, // display list
		&handlers.HistoryHandler{},
		nil, // debug
		nil, // logs
		nil, // live
		nil, // recordings
		nil, // local media
		nil, // EPG
		nil, // sports
		nil, // sports links
		nil, // user settings
		nil, // subtitles
		nil, // clients
		nil, // content preferences
		nil, // hidden items
		nil, // images
		nil, // startup
		nil, // details bundle
		nil, // calendar
		nil, // remote access
		nil, // preroll
		nil, sessionsSvc, nil, nil, nil, nil, "", nil,
	)
	for _, tc := range []struct {
		name, route, peer, method string
		status                    int
		calls                     int
	}{
		{"existing local source", "internal-stream", "127.0.0.1:1234", "GET", 200, 1},
		{"IPv6 local source", "internal-stream", "[::1]:1234", "GET", 200, 1},
		{"spoofed Host and forwarded peer", "internal-stream", "198.51.100.10:1234", "GET", 403, 0},
		{"public route cannot inherit exception", "stream", "127.0.0.1:1234", "GET", 401, 0},
		{"internal route method restricted", "internal-stream", "127.0.0.1:1234", "POST", 404, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := p.calls
			r := httptest.NewRequest(tc.method, "http://localhost/api/video/"+tc.route+"?path=recording:synthetic/parts/1&transmux=0", nil)
			r.RemoteAddr = tc.peer
			r.Header.Set("X-Forwarded-For", "127.0.0.1")
			r.Header.Set("X-Real-IP", "127.0.0.1")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != tc.status || p.calls-before != tc.calls {
				t.Fatalf("status=%d provider calls=%d body=%s", w.Code, p.calls-before, w.Body.String())
			}
		})
	}
}
