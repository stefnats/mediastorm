package models

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	taglanguage "golang.org/x/text/language"
)

const MaxMediaEvidenceStreams = 32
const MaxMediaEvidenceLanguages = 16
const OriginalLanguageEvidenceMaxTTL = 24 * time.Hour
const MediaEvidenceMaxTTL = 15 * time.Minute

// NormalizeEvidenceLanguage retains registered rare tags independently from
// the UI's smaller language-name table. Regional variants are intentionally
// represented by their ISO 639-3 primary language in this initial DTO.
func NormalizeEvidenceLanguage(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 2 || len(value) > 32 {
		return ""
	}
	primary := strings.ToLower(strings.Split(value, "-")[0])
	if primary == "und" || primary == "mul" || primary == "zxx" {
		return ""
	}
	tag, err := taglanguage.Parse(value)
	if err != nil {
		return ""
	}
	base, confidence := tag.Base()
	if confidence < taglanguage.High {
		return ""
	}
	code := base.ISO3()
	if code == "und" || code == "mul" || code == "zxx" || len(code) != 3 {
		return ""
	}
	return code
}

// OriginalLanguageEvidence describes a catalogue provider's original-language
// field, never an available dub, spoken-content analysis or playback grant.
type OriginalLanguageEvidence struct {
	Language      string    `json:"language"`
	Source        string    `json:"source"`
	SourceTitleID string    `json:"sourceTitleId"`
	ObservedAt    time.Time `json:"observedAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
}

// MediaEvidence keeps release claims and observed input-container metadata
// separate. A release identity and a resolved file identity are distinct hash
// domains; candidateVersion may associate the latter with its release. Neither
// a tag nor ffprobe proves spoken content, decoded quality or device output.
type MediaEvidence struct {
	Version          int                  `json:"version"`
	SourceVersion    string               `json:"sourceVersion"`
	CandidateVersion string               `json:"candidateVersion,omitempty"`
	TitleID          string               `json:"titleId,omitempty"`
	TargetEpisode    *EpisodeReference    `json:"targetEpisode,omitempty"`
	Candidate        *MediaCandidateFacts `json:"candidate,omitempty"`
	Probed           *MediaProbeFacts     `json:"probed,omitempty"`
}

type MediaCandidateFacts struct {
	Source            string    `json:"source"`
	ObservedAt        time.Time `json:"observedAt"`
	ExpiresAt         time.Time `json:"expiresAt"`
	AudioLanguages    []string  `json:"audioLanguages,omitempty"`
	SubtitleLanguages []string  `json:"subtitleLanguages,omitempty"`
	Resolution        string    `json:"resolution,omitempty"`
	VideoCodec        string    `json:"videoCodec,omitempty"`
	AudioCodecs       []string  `json:"audioCodecs,omitempty"`
	HDR               string    `json:"hdr,omitempty"`
}

type MediaProbeFacts struct {
	Source                  string               `json:"source"`
	ObservedAt              time.Time            `json:"observedAt"`
	ExpiresAt               time.Time            `json:"expiresAt"`
	AudioStreams            []MediaAudioFacts    `json:"audioStreams"`
	AudioStreamsComplete    bool                 `json:"audioStreamsComplete"`
	SubtitleStreams         []MediaSubtitleFacts `json:"subtitleStreams"`
	SubtitleStreamsComplete bool                 `json:"subtitleStreamsComplete"`
	Video                   MediaVideoFacts      `json:"video"`
}

type MediaAudioFacts struct {
	Index            int    `json:"index"`
	Language         string `json:"language,omitempty"`
	Codec            string `json:"codec,omitempty"`
	AudioDescription *bool  `json:"audioDescription,omitempty"`
}

type MediaSubtitleFacts struct {
	Index    int    `json:"index"`
	Language string `json:"language,omitempty"`
	Codec    string `json:"codec,omitempty"`
	Forced   *bool  `json:"forced,omitempty"`
}

type MediaVideoFacts struct {
	Codec              string `json:"codec,omitempty"`
	Width              int    `json:"width,omitempty"`
	Height             int    `json:"height,omitempty"`
	HDR                string `json:"hdr,omitempty"` // sdr, hdr10, hlg, dolby_vision; absent means unknown
	DolbyVisionProfile string `json:"dolbyVisionProfile,omitempty"`
}

func CopyOriginalLanguageEvidence(value *OriginalLanguageEvidence) *OriginalLanguageEvidence {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// CurrentOriginalLanguageEvidence validates the fact against its containing
// title. A provider alias is accepted only through that title's same-kind TMDB
// identity; a copied proof cannot establish a relationship to another title.
func CurrentOriginalLanguageEvidence(title Title, now time.Time) *OriginalLanguageEvidence {
	value := title.OriginalLanguageEvidence
	if value == nil || value.Source != "tmdb" || value.ObservedAt.IsZero() || value.ObservedAt.After(now) || !value.ExpiresAt.After(now) || !value.ExpiresAt.After(value.ObservedAt) || value.ExpiresAt.Sub(value.ObservedAt) > OriginalLanguageEvidenceMaxTTL {
		return nil
	}
	kind := strings.ToLower(strings.TrimSpace(title.MediaType))
	if kind == "tv" {
		kind = "series"
	}
	if kind != "movie" && kind != "series" {
		return nil
	}
	parts := strings.Split(value.SourceTitleID, ":")
	if len(parts) != 3 || parts[0] != "tmdb" || parts[1] != kind {
		return nil
	}
	id, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != parts[2] {
		return nil
	}
	anchor := strings.ToLower(strings.TrimSpace(title.ID))
	anchorParts := strings.Split(anchor, ":")
	if len(anchorParts) == 3 && (anchorParts[0] == "tmdb" || anchorParts[0] == "tvdb") && (anchorParts[1] == "movie" || anchorParts[1] == "series" || anchorParts[1] == "tv") && anchorParts[1] != kind && !(kind == "series" && anchorParts[1] == "tv") {
		return nil
	}
	if strings.HasPrefix(anchor, "tmdb:") && anchor != value.SourceTitleID && !(kind == "series" && CanonicalSeriesIdentity(anchor) == value.SourceTitleID) {
		return nil
	}
	if title.TMDBID > 0 && title.TMDBID != id {
		return nil
	}
	if anchor != value.SourceTitleID && !(kind == "series" && CanonicalSeriesIdentity(anchor) == value.SourceTitleID) && title.TMDBID != id {
		return nil
	}
	if len(value.Language) < 2 || len(value.Language) > 16 || NormalizeEvidenceLanguage(value.Language) == "" {
		return nil
	}
	for _, r := range value.Language {
		if r < 'a' || r > 'z' {
			return nil
		}
	}
	return CopyOriginalLanguageEvidence(value)
}

// WithoutProviderEvidence strips transferable provider facts from manual or
// uploaded metadata while retaining the user's ordinary display fields.
func WithoutProviderEvidence(title *Title) *Title {
	copy := WithoutCertificationEvidence(title)
	if copy != nil {
		copy.OriginalLanguageEvidence = nil
	}
	return copy
}

func CopyMediaEvidence(value *MediaEvidence) *MediaEvidence {
	if value == nil {
		return nil
	}
	copy := *value
	if value.TargetEpisode != nil {
		ep := *value.TargetEpisode
		if ep.Numbering != nil {
			n := *ep.Numbering
			ep.Numbering = &n
		}
		copy.TargetEpisode = &ep
	}
	if value.Candidate != nil {
		facts := *value.Candidate
		facts.AudioLanguages = append([]string(nil), facts.AudioLanguages...)
		facts.SubtitleLanguages = append([]string(nil), facts.SubtitleLanguages...)
		facts.AudioCodecs = append([]string(nil), facts.AudioCodecs...)
		copy.Candidate = &facts
	}
	if value.Probed != nil {
		facts := *value.Probed
		facts.AudioStreams = append([]MediaAudioFacts{}, facts.AudioStreams...)
		for i := range facts.AudioStreams {
			if facts.AudioStreams[i].AudioDescription != nil {
				b := *facts.AudioStreams[i].AudioDescription
				facts.AudioStreams[i].AudioDescription = &b
			}
		}
		facts.SubtitleStreams = append([]MediaSubtitleFacts{}, facts.SubtitleStreams...)
		for i := range facts.SubtitleStreams {
			if facts.SubtitleStreams[i].Forced != nil {
				b := *facts.SubtitleStreams[i].Forced
				facts.SubtitleStreams[i].Forced = &b
			}
		}
		copy.Probed = &facts
	}
	return &copy
}

// CurrentMediaEvidence never refreshes captured times. Legacy or stale blocks
// stay unknown and clipped lists cannot establish the absence of other tracks.
func CurrentMediaEvidence(value *MediaEvidence, now time.Time) *MediaEvidence {
	if value == nil || value.Version != 1 || !validMediaVersion(value.SourceVersion) || value.CandidateVersion != "" && !validMediaCandidateVersion(value.CandidateVersion) {
		return nil
	}
	copy := CopyMediaEvidence(value)
	fresh := func(at, expiry time.Time) bool {
		return !at.IsZero() && !at.After(now) && expiry.After(now) && expiry.After(at) && expiry.Sub(at) <= MediaEvidenceMaxTTL
	}
	if facts := copy.Candidate; facts != nil && (facts.Source != "release_metadata" || !fresh(facts.ObservedAt, facts.ExpiresAt) || len(facts.AudioLanguages) > MaxMediaEvidenceLanguages || len(facts.SubtitleLanguages) > MaxMediaEvidenceLanguages || len(facts.AudioCodecs) > MaxMediaEvidenceLanguages || !validCandidateFacts(facts)) {
		copy.Candidate = nil
	}
	if facts := copy.Probed; facts != nil && (facts.Source != "ffprobe" || !fresh(facts.ObservedAt, facts.ExpiresAt) || len(facts.AudioStreams) > MaxMediaEvidenceStreams || len(facts.SubtitleStreams) > MaxMediaEvidenceStreams || !validProbeFacts(facts)) {
		copy.Probed = nil
	}
	if copy.Probed != nil && !validMediaHash(copy.SourceVersion, "file_v1_") {
		return nil
	}
	if copy.Probed == nil && copy.Candidate != nil && (!validMediaCandidateVersion(copy.SourceVersion) || copy.CandidateVersion != "" && copy.CandidateVersion != copy.SourceVersion) {
		return nil
	}
	if copy.Candidate == nil && copy.Probed == nil {
		return nil
	}
	return copy
}

func validMediaCandidateVersion(value string) bool { return validMediaHash(value, "candidate_v1_") }
func validMediaVersion(value string) bool {
	return validMediaCandidateVersion(value) || validMediaHash(value, "file_v1_")
}
func validMediaHash(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+64 {
		return false
	}
	tail := strings.TrimPrefix(value, prefix)
	if tail != strings.ToLower(tail) {
		return false
	}
	decoded, err := hex.DecodeString(tail)
	return err == nil && len(decoded) == 32
}

func validFactToken(value string) bool {
	if len(value) > 32 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
			return false
		}
	}
	return true
}
func validFactLanguage(value string) bool {
	return value == "" || len(value) == 3 && NormalizeEvidenceLanguage(value) == value
}
func validHDRFact(value string) bool {
	return value == "" || value == "sdr" || value == "hdr10" || value == "hlg" || value == "dolby_vision"
}
func validCandidateFacts(facts *MediaCandidateFacts) bool {
	if !validHDRFact(facts.HDR) || !validFactToken(facts.VideoCodec) {
		return false
	}
	for _, values := range [][]string{facts.AudioLanguages, facts.SubtitleLanguages} {
		for _, value := range values {
			if !validFactLanguage(value) {
				return false
			}
		}
	}
	for _, value := range facts.AudioCodecs {
		if !validFactToken(value) {
			return false
		}
	}
	if facts.Resolution != "" {
		n, err := strconv.Atoi(strings.TrimSuffix(facts.Resolution, "p"))
		if err != nil || n < 144 || n > 8640 || facts.Resolution != strconv.Itoa(n)+"p" {
			return false
		}
	}
	return true
}
func validProbeFacts(facts *MediaProbeFacts) bool {
	indices := map[int]bool{}
	for _, row := range facts.AudioStreams {
		if row.Index < 0 || indices[row.Index] || !validFactLanguage(row.Language) || !validFactToken(row.Codec) {
			return false
		}
		indices[row.Index] = true
	}
	for _, row := range facts.SubtitleStreams {
		if row.Index < 0 || indices[row.Index] || !validFactLanguage(row.Language) || !validFactToken(row.Codec) {
			return false
		}
		indices[row.Index] = true
	}
	video := facts.Video
	if !validFactToken(video.Codec) || !validHDRFact(video.HDR) || video.Width < 0 || video.Height < 0 || video.Width > 32768 || video.Height > 32768 {
		return false
	}
	if video.DolbyVisionProfile != "" {
		n, err := strconv.Atoi(video.DolbyVisionProfile)
		if err != nil || n < 1 || n > 255 || video.DolbyVisionProfile != strconv.Itoa(n) {
			return false
		}
	}
	return true
}
