// Package mediaevidence projects existing observations without performing I/O.
package mediaevidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"novastream/models"
	"novastream/utils/language"
)

const ProbeTTL = models.MediaEvidenceMaxTTL

func digest(domain string, values ...any) string {
	data, _ := json.Marshal(values)
	hash := sha256.Sum256(append([]byte(domain+"\x00"), data...))
	return domain + "_" + hex.EncodeToString(hash[:])
}

// CandidateVersion is the identity of the release offered by a provider, not
// the particular file later selected from a pack. Credentials never leave this
// process; only a domain-separated opaque digest is returned.
func CandidateVersion(result models.NZBResult) string {
	if validCandidateVersion(result.EvidenceCandidateVersion) {
		return result.EvidenceCandidateVersion
	}
	file := first(result.Attributes["fileIdx"], result.Attributes["fileIndex"])
	if hash := first(result.Attributes["infoHash"], result.Attributes["info_hash"]); hash != "" {
		return digest("candidate_v1", result.ServiceType, result.Indexer, "infohash", strings.ToLower(hash), file)
	}
	if result.GUID != "" {
		return digest("candidate_v1", result.ServiceType, result.Indexer, "guid", result.GUID, file)
	}
	return digest("candidate_v1", result.ServiceType, result.Indexer, result.GUID, result.Link, result.DownloadURL, result.Attributes["infoHash"], result.Attributes["info_hash"], result.Attributes["fileIdx"], result.Attributes["fileIndex"])
}

// ObserveCandidate is called only at an actual provider response boundary.
// Search cache copies and resolver URL rewrites preserve this server-only ID.
func validCandidateVersion(value string) bool {
	if !strings.HasPrefix(value, "candidate_v1_") || len(value) != len("candidate_v1_")+64 {
		return false
	}
	tail := strings.TrimPrefix(value, "candidate_v1_")
	decoded, err := hex.DecodeString(tail)
	return err == nil && len(decoded) == 32 && tail == strings.ToLower(tail)
}

func ObserveCandidate(result *models.NZBResult, at time.Time) {
	result.EvidenceCandidateVersion = CandidateVersion(*result)
	result.EvidenceObservedAt = at.UTC()
	result.EvidenceExpiresAt = at.UTC().Add(models.MediaEvidenceMaxTTL)
}

// FileVersion describes the concrete resolved input object/part. It excludes
// entry UUIDs and track choices; this is an identity hash, not a content hash.
func FileVersion(path, versionMarker string, size int64) string {
	return digest("file_v1", normalizeInput(path), versionMarker, size)
}

// InputVersion is a private binding marker for the logical input actually
// supplied to an existing probe, independent of report or track selection.
func InputVersion(path string) string { return FileVersion(path, "", 0) }

func normalizeInput(path string) string {
	path = strings.TrimSpace(path)
	if strings.HasPrefix(path, "/webdav/") {
		path = strings.TrimPrefix(path, "/webdav")
	} else if strings.HasPrefix(path, "webdav/") {
		path = "/" + strings.TrimPrefix(path, "webdav/")
	}
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		if parsed, err := url.Parse(path); err == nil {
			parsed.RawQuery = strings.ReplaceAll(parsed.RawQuery, " ", "%20")
			return parsed.String()
		}
	}
	return path
}

func splitValues(value string) []string {
	return strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' || r == '|' || r == '/' || r == ' ' })
}

func NormalizeLanguages(values ...string) []string {
	var out []string
	seen := map[string]bool{}
	for _, value := range values {
		for _, token := range splitValues(value) {
			code := language.NormalizeToCode(token)
			if code == "" {
				code = token
			}
			code = models.NormalizeEvidenceLanguage(code)
			if code == "" || code == "und" || code == "mul" || seen[code] {
				continue
			}
			seen[code] = true
			out = append(out, code)
			if len(out) == models.MaxMediaEvidenceLanguages {
				return out
			}
		}
	}
	return out
}

func codec(value string, audio bool) string {
	value = strings.ToLower(strings.TrimSpace(value))
	aliases := map[string]string{"h.264": "h264", "avc": "h264", "h.265": "hevc", "h265": "hevc", "x265": "hevc", "x264": "h264", "ddp": "eac3", "dd+": "eac3", "dd": "ac3", "dts-hd": "dts_hd", "dts-hd ma": "dts_hd"}
	if canonical := aliases[value]; canonical != "" {
		value = canonical
	}
	allowed := "|h264|hevc|av1|vp8|vp9|mpeg2video|mpeg4|vc1|"
	if audio {
		allowed = "|aac|ac3|eac3|truehd|mlp|dts|dts_hd|flac|mp3|opus|vorbis|pcm_s16le|pcm_s24le|"
	}
	if value != "" && strings.Contains(allowed, "|"+value+"|") {
		return value
	}
	return ""
}

func hdrClaim(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "sdr":
		return "sdr"
	case "hdr10", "hdr10+":
		return "hdr10"
	case "hlg":
		return "hlg"
	case "dv", "dovi", "dolbyvision", "dolby vision", "dolby_vision":
		return "dolby_vision"
	}
	return ""
}

// Candidate projects only provider/release claims with their original server
// observation times. A cache read cannot manufacture or renew those times.
func Candidate(result models.NZBResult, now time.Time) *models.MediaEvidence {
	if result.EvidenceObservedAt.IsZero() || result.EvidenceObservedAt.After(now) || !result.EvidenceExpiresAt.After(now) || !result.EvidenceExpiresAt.After(result.EvidenceObservedAt) {
		return nil
	}
	attrs := result.Attributes
	facts := &models.MediaCandidateFacts{Source: "release_metadata", ObservedAt: result.EvidenceObservedAt, ExpiresAt: result.EvidenceExpiresAt,
		AudioLanguages: NormalizeLanguages(attrs["languages"], attrs["audioLanguages"]), SubtitleLanguages: NormalizeLanguages(attrs["subtitleLanguages"]), VideoCodec: codec(first(attrs["codec"], attrs["videoCodec"], attrs["video_codec"]), false), HDR: hdrClaim(attrs["hdr"])}
	resolution := strings.ToLower(strings.TrimSpace(attrs["resolution"]))
	if resolution == "4k" {
		resolution = "2160p"
	}
	if n, err := strconv.Atoi(strings.TrimSuffix(resolution, "p")); err == nil && n >= 144 && n <= 8640 {
		facts.Resolution = strconv.Itoa(n) + "p"
	}
	seen := map[string]bool{}
	for _, token := range splitValues(first(attrs["audio"], attrs["audioCodec"], attrs["audio_codec"])) {
		if value := codec(token, true); value != "" && !seen[value] {
			seen[value] = true
			facts.AudioCodecs = append(facts.AudioCodecs, value)
		}
		if len(facts.AudioCodecs) >= models.MaxMediaEvidenceLanguages {
			break
		}
	}
	version := CandidateVersion(result)
	return models.CurrentMediaEvidence(&models.MediaEvidence{Version: 1, SourceVersion: version, CandidateVersion: version, Candidate: facts}, now)
}

func first(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func BoolDisposition(disposition map[string]int, field string) *bool {
	value, present := disposition[field]
	if !present || value < 0 || value > 1 {
		return nil
	}
	b := value > 0
	return &b
}

// ProbeStream is the subset already returned by an existing ffprobe process.
// Reading this structure never starts a new probe.
type ProbeStream struct {
	Index          int               `json:"index"`
	CodecType      string            `json:"codec_type"`
	CodecName      string            `json:"codec_name"`
	Profile        string            `json:"profile"`
	Width          int               `json:"width"`
	Height         int               `json:"height"`
	ColorTransfer  string            `json:"color_transfer"`
	ColorPrimaries string            `json:"color_primaries"`
	Tags           map[string]string `json:"tags"`
	Disposition    map[string]int    `json:"disposition"`
	SideData       []struct {
		Type      string `json:"side_data_type"`
		DVProfile int    `json:"dv_profile"`
	} `json:"side_data_list"`
}

func ProbeFacts(streams []ProbeStream, at time.Time) *models.MediaProbeFacts {
	facts := &models.MediaProbeFacts{Source: "ffprobe", ObservedAt: at.UTC(), ExpiresAt: at.UTC().Add(ProbeTTL), AudioStreams: []models.MediaAudioFacts{}, SubtitleStreams: []models.MediaSubtitleFacts{}, AudioStreamsComplete: true, SubtitleStreamsComplete: true}
	if len(streams) > 4096 {
		streams = streams[:4096]
		facts.AudioStreamsComplete = false
		facts.SubtitleStreamsComplete = false
	}
	indices := map[int]int{}
	for _, stream := range streams {
		indices[stream.Index]++
	}
	videoSelected := false
	for _, stream := range streams {
		switch strings.ToLower(stream.CodecType) {
		case "audio":
			if stream.Index < 0 || indices[stream.Index] != 1 {
				facts.AudioStreamsComplete = false
				continue
			}
			if len(facts.AudioStreams) == models.MaxMediaEvidenceStreams {
				facts.AudioStreamsComplete = false
				continue
			}
			facts.AudioStreams = append(facts.AudioStreams, models.MediaAudioFacts{Index: stream.Index, Language: normalizedLanguage(stream.Tags["language"]), Codec: codec(stream.CodecName, true), AudioDescription: BoolDisposition(stream.Disposition, "visual_impaired")})
		case "subtitle":
			if stream.Index < 0 || indices[stream.Index] != 1 {
				facts.SubtitleStreamsComplete = false
				continue
			}
			if len(facts.SubtitleStreams) == models.MaxMediaEvidenceStreams {
				facts.SubtitleStreamsComplete = false
				continue
			}
			facts.SubtitleStreams = append(facts.SubtitleStreams, models.MediaSubtitleFacts{Index: stream.Index, Language: normalizedLanguage(stream.Tags["language"]), Codec: safeCodec(stream.CodecName), Forced: BoolDisposition(stream.Disposition, "forced")})
		case "video":
			if videoSelected || stream.Disposition["attached_pic"] > 0 || stream.Index < 0 || indices[stream.Index] != 1 {
				continue
			}
			videoSelected = true
			facts.Video = models.MediaVideoFacts{Codec: codec(stream.CodecName, false), Width: boundedDimension(stream.Width), Height: boundedDimension(stream.Height)}
			switch strings.ToLower(stream.ColorTransfer) {
			case "bt709", "smpte170m", "bt470m", "bt470bg", "gamma22", "gamma28":
				facts.Video.HDR = "sdr"
			case "smpte2084":
				if strings.EqualFold(stream.ColorPrimaries, "bt2020") {
					facts.Video.HDR = "hdr10"
				}
			case "arib-std-b67":
				facts.Video.HDR = "hlg"
			}
			for _, side := range stream.SideData {
				if strings.Contains(strings.ToLower(side.Type), "dovi") && side.DVProfile > 0 {
					facts.Video.HDR = "dolby_vision"
					facts.Video.DolbyVisionProfile = strconv.Itoa(side.DVProfile)
				}
			}
		}
	}
	return facts
}

func normalizedLanguage(value string) string {
	values := NormalizeLanguages(value)
	if len(values) == 1 {
		return values[0]
	}
	return ""
}
func boundedDimension(value int) int {
	if value > 0 && value <= 32768 {
		return value
	}
	return 0
}
func safeCodec(value string) string {
	if len(value) > 32 {
		return ""
	}
	for _, r := range value {
		if r < 'a' || r > 'z' {
			if r < '0' || r > '9' {
				if r != '_' {
					return ""
				}
			}
		}
	}
	return value
}

// Probed associates only one completed probe with its concrete selected input.
// No candidate properties fill holes in the observed block.
func Probed(result models.NZBResult, path, versionMarker string, size int64, facts *models.MediaProbeFacts, titleID string, episode *models.EpisodeReference, now time.Time) *models.MediaEvidence {
	if facts == nil || strings.TrimSpace(path) == "" {
		return nil
	}
	value := &models.MediaEvidence{Version: 1, SourceVersion: FileVersion(path, versionMarker, size), CandidateVersion: CandidateVersion(result), TitleID: titleID, TargetEpisode: episode, Probed: facts}
	if candidate := Candidate(result, now); candidate != nil {
		value.Candidate = candidate.Candidate
	}
	return models.CurrentMediaEvidence(value, now)
}
