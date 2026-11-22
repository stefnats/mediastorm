package localmedia

import (
	"context"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"novastream/models"
	"novastream/services/metadata"
)

type cachedEpisodeDateMatcher interface {
	GetCachedDateEpisodeMatch(context.Context, models.EpisodeNumbering, string) (metadata.EpisodeDateCatalogueMatch, bool)
}

var localEpisodeDatePattern = regexp.MustCompile(`(?:^|[^0-9])([0-9]{4})[._ -]([0-9]{2})[._ -]([0-9]{2})`)
var localEpisodeGermanDatePattern = regexp.MustCompile(`(?:^|[^0-9])([0-9]{2})[._ -]([0-9]{2})[._ -]([0-9]{4})`)

// Only a single complete ISO-style or German date in a named file is used. Explicit
// season/episode names keep their existing numbering, even when they add a date.
func localEpisodeAirDate(fileName string) (date, prefix string, ok bool) {
	stem := strings.TrimSuffix(filepath.Base(fileName), filepath.Ext(fileName))
	if seasonEpisodePattern.MatchString(stem) || xPattern.MatchString(stem) {
		return "", "", false
	}
	matches := localEpisodeDatePattern.FindAllStringSubmatchIndex(stem, -1)
	german := localEpisodeGermanDatePattern.FindAllStringSubmatchIndex(stem, -1)
	if len(matches)+len(german) != 1 {
		return "", "", false
	}
	yearFirst := len(matches) == 1
	if !yearFirst {
		matches = german
	}
	m := matches[0]
	if m[1] < len(stem) && stem[m[1]] >= '0' && stem[m[1]] <= '9' {
		return "", "", false
	}
	date = stem[m[2]:m[3]] + "-" + stem[m[4]:m[5]] + "-" + stem[m[6]:m[7]]
	if !yearFirst {
		date = stem[m[6]:m[7]] + "-" + stem[m[4]:m[5]] + "-" + stem[m[2]:m[3]]
	}
	parsed, err := time.Parse("2006-01-02", date)
	if err != nil || parsed.Year() < 1 || parsed.Format("2006-01-02") != date {
		return "", "", false
	}
	return date, strings.TrimRight(stem[:m[2]], " ._-("), true
}

func localEpisodeDateSeries(item models.LocalMediaItem) (models.EpisodeNumbering, bool) {
	var tvdb int64
	valid := true
	add := func(id int64) {
		if id > 0 {
			if tvdb != 0 && tvdb != id {
				valid = false
			}
			tvdb = id
		}
	}
	addTitle := func(id string) {
		id = models.CanonicalSeriesIdentity(id)
		if strings.HasPrefix(id, "tvdb:series:") {
			n, err := strconv.ParseInt(strings.TrimPrefix(id, "tvdb:series:"), 10, 64)
			if err != nil || n <= 0 {
				valid = false
			} else {
				add(n)
			}
		}
	}
	addTitle(item.MatchedTitleID)
	if item.Metadata != nil {
		add(item.Metadata.TVDBID)
		addTitle(item.Metadata.ID)
	}
	if item.ExternalIDs != nil && strings.TrimSpace(item.ExternalIDs.TVDB) != "" {
		n, err := strconv.ParseInt(strings.TrimSpace(item.ExternalIDs.TVDB), 10, 64)
		if err != nil || n <= 0 {
			valid = false
		} else {
			add(n)
		}
	}
	if !valid || tvdb <= 0 {
		return models.EpisodeNumbering{}, false
	}
	return models.EpisodeNumbering{SeriesID: "tvdb:series:" + strconv.FormatInt(tvdb, 10), Ordering: "official"}, true
}

// Use only the already observed date catalogue. Scans persist the coordinates;
// playback can also resolve a previously scanned date-only row without IO or a
// new provider search. Manual and ordinary numbered matches remain unchanged.
func (s *Service) matchDateEpisode(ctx context.Context, item *models.LocalMediaItem) {
	if item == nil || len(item.Episodes) > 0 || item.LibraryType != models.LocalMediaLibraryTypeShow || item.MatchStatus != models.LocalMediaMatchStatusMatched || item.SeasonNumber != 0 || item.EpisodeNumber != 0 {
		return
	}
	date, _, ok := localEpisodeAirDate(item.FileName)
	if !ok {
		return
	}
	numbering, ok := localEpisodeDateSeries(*item)
	if !ok {
		return
	}
	reader, ok := s.metadata.(cachedEpisodeDateMatcher)
	if !ok {
		return
	}
	match, ok := reader.GetCachedDateEpisodeMatch(ctx, numbering, date)
	if !ok || match.AiredDate != date || !models.SameEpisodeNumbering(&numbering, &match.Numbering) || match.SeasonNumber <= 0 || match.EpisodeNumber <= 0 {
		return
	}
	item.SeasonNumber, item.EpisodeNumber = match.SeasonNumber, match.EpisodeNumber
}

func dateEpisodeMatchesOrdering(item models.LocalMediaItem, episode *models.EpisodeReference) bool {
	if episode == nil || episode.Numbering == nil || item.MatchStatus == models.LocalMediaMatchStatusManual {
		return true
	}
	if _, _, dated := localEpisodeAirDate(item.FileName); !dated {
		return true
	}
	numbering, ok := localEpisodeDateSeries(item)
	return ok && models.SameEpisodeNumbering(&numbering, episode.Numbering)
}
