package handlers

import (
	"net/http"
	"strconv"
	"strings"

	"novastream/models"
	"novastream/services/localmedia"
)

func parseLocalMediaFilters(r *http.Request) (models.LocalMediaFilters, error) {
	q := r.URL.Query()
	f := models.LocalMediaFilters{AudioCodec: strings.ToLower(q.Get("audioCodec")), VideoCodec: strings.ToLower(q.Get("videoCodec")), HDR: q.Get("hdr"), Subtitles: q.Get("subtitles"), Unknown: q.Get("mediaUnknown")}
	for _, field := range []struct {
		name  string
		value *string
	}{{"audioLanguage", &f.AudioLanguage}, {"subtitleLanguage", &f.SubtitleLanguage}} {
		if raw := q.Get(field.name); raw != "" {
			*field.value = models.NormalizeEvidenceLanguage(raw)
			if *field.value == "" {
				return f, localmedia.ErrInvalidMediaFilters
			}
		}
	}
	if raw := q.Get("minHeight"); raw != "" {
		var err error
		f.MinHeight, err = strconv.Atoi(raw)
		if err != nil {
			return f, localmedia.ErrInvalidMediaFilters
		}
	}
	return f, localmedia.ValidateMediaFilters(f)
}
