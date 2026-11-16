package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"novastream/config"
	"novastream/internal/auth"
)

func TestRecordingRuleForecastInternalCatalogUsesJobOwner(t *testing.T) {
	var playlistReads atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		playlistReads.Add(1)
		_, _ = w.Write([]byte("#EXTM3U\n#EXTINF:-1 tvg-id=\"fixture.channel\",Synthetic channel\nhttp://cdn.example/synthetic.ts\n"))
	}))
	defer provider.Close()
	accounts := []string{"household", "other"}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if err := manager.Save(config.Settings{Live: config.LiveSettings{RequireAccountAssignment: true, Sources: []config.LivePlaylistSource{
		{ID: "shared", Mode: "m3u", PlaylistURL: stationPlaylistURL(t, provider.URL), AllowedAccounts: &accounts},
	}}}); err != nil {
		t.Fatal(err)
	}
	h := NewLiveHandler(provider.Client(), false, "", 6, 1, 1, false, manager, nil)
	h.usersSvc = liveAccountUsers{"own": {ID: "own", AccountID: "household"}, "foreign": {ID: "foreign", AccountID: "other"}, "kid": {ID: "kid", AccountID: "other", IsKidsProfile: true}}
	ctx := context.WithValue(context.Background(), auth.ContextKeyAccountID, "household")
	channels, err := h.RecordingChannels(ctx, "foreign")
	if err != nil || len(channels) != 1 {
		t.Fatalf("internal catalog inherited requester authority: channels=%d err=%v", len(channels), err)
	}
	// The internal read does not weaken the public HTTP endpoint.
	r := httptest.NewRequest("GET", "/api/live/channels?profileId=foreign", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	h.GetChannels(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("public foreign catalog access changed: %d", w.Code)
	}
	for _, id := range []string{"kid", "missing"} {
		if _, err := h.RecordingChannels(ctx, id); err == nil {
			t.Fatalf("unrecordable profile accepted: %s", id)
		}
	}
	if playlistReads.Load() != 1 {
		t.Fatalf("owned catalog playlist reads=%d", playlistReads.Load())
	}
}
