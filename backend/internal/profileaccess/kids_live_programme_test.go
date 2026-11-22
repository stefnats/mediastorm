package profileaccess

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestKidsLiveOnlyProgrammeStarterLeavesBlanketBlock(t *testing.T) {
	for _, current := range []bool{false, true} {
		t.Run(map[bool]string{false: "file", true: "current"}[current], func(t *testing.T) {
			for _, tc := range []struct {
				path, method string
				want         int
			}{
				{"/api/live/kids/hls/start?profileId=ella", http.MethodGet, 204},
				{"/api/live/kids/channels?profileId=ella", http.MethodGet, 204},
				{"/api/live/channels?profileId=ella", http.MethodGet, 403},
				{"/app/api/live/kids/hls/start?profileId=ella", http.MethodGet, 204},
				{"/api/live/kids/hls/start?profileId=ella", http.MethodPost, 403},
				{"/api/live/hls/start?profileId=ella", http.MethodGet, 403},
				{"/api/live/stream?profileId=ella", http.MethodGet, 403},
				{"/api/live/catchup?profileId=ella", http.MethodGet, 403},
			} {
				var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
				if current {
					handler = CurrentKidsLiveTV(func(r *http.Request, id string) (bool, error) { return id == "ella", nil })(handler)
				} else {
					handler = BlockKidsFromLiveTV(func(id string) bool { return id == "ella" })(handler)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
				if w.Code != tc.want {
					t.Fatal(tc.path, tc.method, w.Code, w.Body.String())
				}
			}
		})
	}
}
