package arrclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestImportedHistoryIdentifiesOnlyThisDownload(t *testing.T) {
	for _, kind := range []string{"sonarr", "radarr"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/base/api/v3/history" || r.URL.Query().Get("downloadId") != "download-42" || r.Header.Get("X-Api-Key") != "synthetic-key" {
					t.Error("history did not target configured download")
				}
				records := []map[string]any{{"downloadId": "someone-else", "eventType": "downloadFolderImported", "data": map[string]string{"importedPath": "/library/wrong.mkv"}}, {"downloadId": "download-42", "eventType": "grabbed", "data": map[string]string{"importedPath": "/library/not-imported.mkv"}}, {"downloadId": "download-42", "eventType": "downloadFolderImported", "data": map[string]string{"ImportedPath": "/library/own.mkv"}, "movie": map[string]any{"title": "Synthetic Movie", "year": 2024, "tmdbId": 303}, "series": map[string]any{"title": "Synthetic Series", "tvdbId": 505}, "episode": map[string]int{"seasonNumber": 1, "episodeNumber": 2}}}
				_ = json.NewEncoder(w).Encode(map[string]any{"totalRecords": len(records), "records": records})
			}))
			defer server.Close()
			c, e := New(context.Background(), Config{Kind: kind, BaseURL: server.URL + "/base", APIKey: "synthetic-key"})
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			files, e := c.ImportedFiles(context.Background(), "download-42")
			if e != nil || len(files) != 1 || files[0].Path != "/library/own.mkv" {
				t.Fatal("imported file correlation", files, e)
			}
			if kind == "sonarr" && (files[0].TVDBID != 505 || files[0].EpisodeNumber != 2) || kind == "radarr" && files[0].TMDBID != 303 {
				t.Fatal("catalogue identity missing")
			}
		})
	}
}
func TestEmptyImportHistoryIsNotAFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"records":[],"totalRecords":0}`))
	}))
	defer server.Close()
	c, e := New(context.Background(), Config{Kind: "radarr", BaseURL: server.URL, APIKey: "synthetic-key"})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	files, e := c.ImportedFiles(context.Background(), "download-42")
	if e != nil || len(files) != 0 {
		t.Fatal("empty history invented imported media", e)
	}
}

func TestSonarrMultiEpisodeHistoryIsNotCollapsedToOneEpisode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"totalRecords":3,"records":[{"downloadId":"pack","episodeId":11,"eventType":"downloadFolderImported","data":{"importedPath":"/shows/Double.mkv"},"series":{"tvdbId":505},"episode":{"seasonNumber":1,"episodeNumber":1}},{"downloadId":"pack","episodeId":12,"eventType":"downloadFolderImported","data":{"importedPath":"/shows/Double.mkv"},"series":{"tvdbId":505},"episode":{"seasonNumber":1,"episodeNumber":2}},{"downloadId":"pack","episodeId":11,"eventType":"downloadFolderImported","data":{"importedPath":"/shows/Double.mkv"},"series":{"tvdbId":505},"episode":{"seasonNumber":1,"episodeNumber":1}}]}`))
	}))
	defer server.Close()
	c, e := New(context.Background(), Config{Kind: "sonarr", BaseURL: server.URL, APIKey: "synthetic-key"})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	files, e := c.ImportedFiles(context.Background(), "pack")
	if e != nil || len(files) != 1 || !files[0].AmbiguousEpisodes {
		t.Fatal("double-episode file was silently reduced to one episode", files, e)
	}
}

func TestIncompleteImportHistoryIsUnavailable(t *testing.T) {
	for _, kind := range []string{"partial", "negative-total"} {
		t.Run(kind, func(t *testing.T) {
			total := 2
			if kind == "negative-total" {
				total = -1
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"totalRecords": total, "records": []any{map[string]any{"downloadId": "pack", "eventType": "downloadFolderImported", "data": map[string]string{"importedPath": "/shows/Double.mkv"}, "series": map[string]any{"tvdbId": 505}, "episode": map[string]int{"seasonNumber": 1, "episodeNumber": 1}}}})
			}))
			defer server.Close()
			c, e := New(context.Background(), Config{Kind: "sonarr", BaseURL: server.URL, APIKey: "synthetic-key"})
			if e != nil {
				t.Fatal(e)
			}
			defer c.Close()
			files, e := c.ImportedFiles(context.Background(), "pack")
			if e == nil || files != nil {
				t.Fatal("incomplete history was treated as complete imported files", files, e)
			}
		})
	}
}
