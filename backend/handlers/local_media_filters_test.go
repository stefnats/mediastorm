package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalMediaFilterHTTPQuery(t *testing.T) {
	h, stored, _ := localLibraryLanguageFixture(t)
	w := localLibraryLanguageRequest(h, "audioLanguage=fr&subtitleLanguage=es&audioCodec=AAC&videoCodec=hevc&minHeight=1080&hdr=hdr10&subtitles=present&mediaUnknown=exclude")
	if w.Code != 200 {
		t.Fatalf("query failed: %d %s", w.Code, w.Body.String())
	}
	f := stored.query.MediaFilters
	if f.AudioLanguage != "fra" || f.SubtitleLanguage != "spa" || f.AudioCodec != "aac" || f.VideoCodec != "hevc" || f.MinHeight != 1080 || f.HDR != "hdr10" || f.Subtitles != "present" || f.Unknown != "exclude" {
		t.Fatalf("missing normalized query: %+v", f)
	}
	for _, query := range []string{"audioLanguage=!", "audioCodec=invalid!", "minHeight=abc", "minHeight=12", "hdr=unknown", "mediaUnknown=maybe", "subtitles=maybe"} {
		r := httptest.NewRequest(http.MethodGet, "/?"+query, nil)
		if _, err := parseLocalMediaFilters(r); err == nil {
			t.Errorf("invalid filter accepted: %s", query)
		}
	}
}
