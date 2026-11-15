package reminders

import (
	"context"
	"novastream/models"
	"strconv"
	"strings"
)

// ObserveCalendar reuses normal calendar output only; its lack of canonical
// episode identity prevents calendar coordinates from generating episode events.
func (s *Service) ObserveCalendar(ctx context.Context, profile string, items []models.CalendarItem) error {
	titles := []models.Title{}
	for _, item := range items {
		if item.MediaType != "movie" {
			continue
		}
		title := models.Title{Name: item.Title, MediaType: "movie", Year: item.Year}
		for key, value := range item.ExternalIDs {
			switch strings.ToLower(strings.TrimSuffix(key, "Id")) {
			case "tmdb":
				title.TMDBID, _ = strconv.ParseInt(value, 10, 64)
			case "tvdb":
				title.TVDBID, _ = strconv.ParseInt(value, 10, 64)
			case "imdb":
				title.IMDBID = value
			}
		}
		if title.TMDBID > 0 {
			title.ID = "tmdb:movie:" + strconv.FormatInt(title.TMDBID, 10)
		} else if title.IMDBID != "" {
			title.ID = title.IMDBID
		} else if title.TVDBID > 0 {
			title.ID = "tvdb:" + strconv.FormatInt(title.TVDBID, 10)
		} else {
			continue
		}
		title.Releases = []models.Release{{Type: item.ReleaseType, Date: item.AirDate}}
		titles = append(titles, title)
		if len(titles) == 100 {
			if err := s.ObserveTitles(ctx, profile, "", titles); err != nil {
				return err
			}
			titles = nil
		}
	}
	return s.ObserveTitles(ctx, profile, "", titles)
}
