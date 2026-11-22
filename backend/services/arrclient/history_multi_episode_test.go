package arrclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"novastream/models"
)

func TestSonarrImportedMembershipIsCompleteAndCanonical(t *testing.T) {
	for _, test := range []struct {
		name        string
		ids         []int64
		coordinates [][2]int
		seriesIDs   []int64
		incomplete  bool
	}{
		{name: "complete", ids: []int64{12, 11, 12}, coordinates: [][2]int{{1, 2}, {1, 1}, {1, 2}}, seriesIDs: []int64{505, 505, 505}},
		{name: "missing-coordinate", ids: []int64{11, 12}, coordinates: [][2]int{{1, 1}, {0, 0}}, seriesIDs: []int64{505, 505}, incomplete: true},
		{name: "episode-id-conflict", ids: []int64{11, 11}, coordinates: [][2]int{{1, 1}, {1, 2}}, seriesIDs: []int64{505, 505}, incomplete: true},
		{name: "coordinate-id-conflict", ids: []int64{11, 12}, coordinates: [][2]int{{1, 1}, {1, 1}}, seriesIDs: []int64{505, 505}, incomplete: true},
		{name: "series-conflict", ids: []int64{11, 12}, coordinates: [][2]int{{1, 1}, {1, 2}}, seriesIDs: []int64{505, 606}, incomplete: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v3/history" || r.URL.Query().Get("includeEpisode") != "true" {
					t.Error("missing actual Sonarr episode history request")
				}
				records := make([]map[string]any, 0, len(test.ids))
				for i, id := range test.ids {
					records = append(records, map[string]any{"downloadId": "pack", "episodeId": id, "eventType": "downloadFolderImported", "data": map[string]string{"importedPath": "/shows/Combined.mkv"}, "series": map[string]any{"title": "Synthetic Series", "tvdbId": test.seriesIDs[i]}, "episode": map[string]int{"seasonNumber": test.coordinates[i][0], "episodeNumber": test.coordinates[i][1]}})
				}
				if err := json.NewEncoder(w).Encode(map[string]any{"totalRecords": len(records), "records": records}); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			client, err := New(t.Context(), Config{Kind: "sonarr", BaseURL: server.URL, APIKey: "synthetic-key"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			files, err := client.ImportedFiles(t.Context(), "pack")
			if err != nil || len(files) != 1 {
				t.Fatal(files, err)
			}
			file := files[0]
			if file.EpisodeMappingIncomplete != test.incomplete || !file.AmbiguousEpisodes || file.SeasonNumber != 0 || file.EpisodeNumber != 0 {
				t.Fatal("ambiguous scalar or incomplete receipt", file)
			}
			if !test.incomplete && !reflect.DeepEqual(file.Episodes, []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}) {
				t.Fatal("lost explicit membership", file.Episodes)
			}
		})
	}
}
