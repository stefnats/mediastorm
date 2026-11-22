package arrclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"strings"
)

// ImportedFile describes a file actually recorded as imported for this download.
// The local library still checks and indexes that file before offering a version.
type ImportedFile struct {
	Path              string `json:"-"`
	Title             string `json:"title"`
	Year              int    `json:"year"`
	MediaType         string `json:"mediaType"`
	TMDBID            int64  `json:"tmdbId,omitempty"`
	TVDBID            int64  `json:"tvdbId,omitempty"`
	IMDBID            string `json:"imdbId,omitempty"`
	SeasonNumber      int    `json:"seasonNumber,omitempty"`
	EpisodeNumber     int    `json:"episodeNumber,omitempty"`
	AmbiguousEpisodes bool   `json:"ambiguousEpisodes,omitempty"`
}

// ImportedFiles uses the v3 downloadId history filter supported by both apps.
// A completed command alone may have imported zero files.
func (c *Client) ImportedFiles(ctx context.Context, downloadID string) ([]ImportedFile, error) {
	if !plain(downloadID, 256) {
		return nil, ErrConfiguration
	}
	q := url.Values{"downloadId": {downloadID}, "pageSize": {"1000"}, "sortKey": {"date"}, "sortDirection": {"descending"}}
	if c == nil {
		return nil, ErrUnavailable
	}
	if c.kind == "sonarr" {
		q.Set("includeSeries", "true")
		q.Set("includeEpisode", "true")
	} else {
		q.Set("includeMovie", "true")
	}
	raw, e := c.exchange(ctx, http.MethodGet, "/api/v3/history", q, nil)
	if e != nil {
		return nil, e
	}
	type title struct {
		Title  string `json:"title"`
		Year   int    `json:"year"`
		TMDBID int64  `json:"tmdbId"`
		TVDBID int64  `json:"tvdbId"`
		IMDBID string `json:"imdbId"`
	}
	var page struct {
		TotalRecords int `json:"totalRecords"`
		Records      []struct {
			DownloadID string            `json:"downloadId"`
			EpisodeID  int64             `json:"episodeId"`
			EventType  string            `json:"eventType"`
			Data       map[string]string `json:"data"`
			Movie      title             `json:"movie"`
			Series     title             `json:"series"`
			Episode    struct {
				SeasonNumber  int `json:"seasonNumber"`
				EpisodeNumber int `json:"episodeNumber"`
			} `json:"episode"`
		} `json:"records"`
	}
	if json.Unmarshal(raw, &page) != nil || page.TotalRecords < 0 || page.TotalRecords > 1000 || page.TotalRecords != len(page.Records) {
		return nil, ErrUnavailable
	}
	files := []ImportedFile{}
	seen := map[string]int{}
	episodeIDs := map[string]int64{}
	for _, r := range page.Records {
		if r.DownloadID != downloadID || r.EventType != "downloadFolderImported" {
			continue
		}
		imported := r.Data["importedPath"]
		if imported == "" {
			imported = r.Data["ImportedPath"]
		}
		if !plain(imported, 4096) || !strings.HasPrefix(imported, "/") || path.Clean(imported) != imported {
			continue
		}
		item := ImportedFile{Path: imported}
		t := r.Movie
		item.MediaType = "movie"
		if c.kind == "sonarr" {
			t = r.Series
			item.MediaType = "series"
			item.SeasonNumber = r.Episode.SeasonNumber
			item.EpisodeNumber = r.Episode.EpisodeNumber
		}
		item.Title, item.Year, item.TMDBID, item.TVDBID, item.IMDBID = t.Title, t.Year, t.TMDBID, t.TVDBID, t.IMDBID
		if index, exists := seen[imported]; exists {
			previous := &files[index]
			if c.kind == "sonarr" && (previous.SeasonNumber != item.SeasonNumber || previous.EpisodeNumber != item.EpisodeNumber || episodeIDs[imported] > 0 && r.EpisodeID > 0 && episodeIDs[imported] != r.EpisodeID) {
				previous.AmbiguousEpisodes = true
			}
			continue
		}
		seen[imported] = len(files)
		episodeIDs[imported] = r.EpisodeID
		files = append(files, item)
	}
	return files, nil
}
