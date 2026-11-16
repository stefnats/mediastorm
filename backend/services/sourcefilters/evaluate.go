// Package sourcefilters reduces existing, scoped observations without I/O.
package sourcefilters

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

type Filters struct {
	Basis                    string `json:"basis"`
	Unknown                  string `json:"unknown"`
	AudioLanguage            string `json:"audioLanguage,omitempty"`
	SubtitleLanguage         string `json:"subtitleLanguage,omitempty"`
	ClaimedResolutionAtLeast string `json:"claimedResolutionAtLeast,omitempty"`
	HeightAtLeast            *int   `json:"heightAtLeast,omitempty"`
	VideoCodec               string `json:"videoCodec,omitempty"`
	DynamicRange             string `json:"dynamicRange,omitempty"`
	AudioCodec               string `json:"audioCodec,omitempty"`
	AudioDescription         *bool  `json:"audioDescription,omitempty"`
}

func (f Filters) Validate() error {
	if f.Basis != "candidate" && f.Basis != "probed" || f.Unknown != "include" && f.Unknown != "exclude" {
		return errors.New("invalid filter policy")
	}
	for _, value := range []string{f.AudioLanguage, f.SubtitleLanguage} {
		if value != "" && (len(value) != 3 || models.NormalizeEvidenceLanguage(value) != value) {
			return errors.New("invalid canonical language")
		}
	}
	for _, value := range []string{f.VideoCodec, f.AudioCodec} {
		if len(value) > 32 {
			return errors.New("invalid codec")
		}
		for _, r := range value {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_') {
				return errors.New("invalid codec")
			}
		}
	}
	if f.DynamicRange != "" && f.DynamicRange != "sdr" && f.DynamicRange != "hdr10" && f.DynamicRange != "hlg" && f.DynamicRange != "dolby_vision" {
		return errors.New("invalid dynamic range")
	}
	if f.AudioDescription != nil && !*f.AudioDescription {
		return errors.New("audio description must be true or absent")
	}
	if f.HeightAtLeast != nil && (*f.HeightAtLeast < 144 || *f.HeightAtLeast > 8640) {
		return errors.New("invalid height")
	}
	if f.ClaimedResolutionAtLeast != "" {
		n, err := strconv.Atoi(strings.TrimSuffix(f.ClaimedResolutionAtLeast, "p"))
		if err != nil || n < 144 || n > 8640 || strconv.Itoa(n)+"p" != f.ClaimedResolutionAtLeast {
			return errors.New("invalid claimed resolution")
		}
	}
	if f.Basis == "candidate" && (f.HeightAtLeast != nil || f.AudioCodec != "" || f.AudioDescription != nil) || f.Basis == "probed" && f.ClaimedResolutionAtLeast != "" {
		return errors.New("filter is incompatible with its evidence basis")
	}
	return nil
}

func (f Filters) Active() bool {
	return f.AudioLanguage != "" || f.SubtitleLanguage != "" || f.ClaimedResolutionAtLeast != "" || f.HeightAtLeast != nil || f.VideoCodec != "" || f.DynamicRange != "" || f.AudioCodec != "" || f.AudioDescription != nil
}

type Target struct {
	TitleID   string                   `json:"titleId"`
	MediaType string                   `json:"mediaType"`
	Episode   *models.EpisodeReference `json:"episode,omitempty"`
}

func (t Target) MarshalJSON() ([]byte, error) {
	type episode struct {
		EpisodeID     string                   `json:"episodeId"`
		SeasonNumber  int                      `json:"seasonNumber"`
		EpisodeNumber int                      `json:"episodeNumber"`
		Numbering     *models.EpisodeNumbering `json:"numbering,omitempty"`
	}
	var ep *episode
	if t.Episode != nil {
		ep = &episode{t.Episode.EpisodeID, t.Episode.SeasonNumber, t.Episode.EpisodeNumber, t.Episode.Numbering}
	}
	return json.Marshal(struct {
		TitleID   string   `json:"titleId"`
		MediaType string   `json:"mediaType"`
		Episode   *episode `json:"episode,omitempty"`
	}{t.TitleID, t.MediaType, ep})
}

type truth uint8

const (
	unknown truth = iota
	yes
	no
)

func conjunction(values ...truth) truth {
	result := yes
	for _, value := range values {
		if value == no {
			return no
		}
		if value == unknown {
			result = unknown
		}
	}
	return result
}
func scalar(want, actual string) truth {
	if want == "" {
		return yes
	}
	if actual == "" {
		return unknown
	}
	if want == actual {
		return yes
	}
	return no
}
func claimedLanguage(want string, values []string) truth {
	if want == "" {
		return yes
	}
	for _, v := range values {
		if want == v {
			return yes
		}
	}
	return unknown
}

// ExactEpisodeBinding does not infer aliases, coordinates or numbering from a
// title or a release name. Target is request correlation, never proof itself.
func ExactEpisodeBinding(target Target, evidence *models.MediaEvidence) bool {
	if target.MediaType != "series" {
		return target.MediaType == "movie"
	}
	if evidence == nil || evidence.TitleID != target.TitleID || target.Episode == nil || evidence.TargetEpisode == nil {
		return false
	}
	a, b := target.Episode, evidence.TargetEpisode
	return a.EpisodeID != "" && a.EpisodeID == b.EpisodeID && a.SeasonNumber == b.SeasonNumber && a.EpisodeNumber == b.EpisodeNumber && a.Numbering != nil && b.Numbering != nil && a.Numbering.SeriesID != "" && a.Numbering.SeriesID == b.Numbering.SeriesID && a.Numbering.Ordering == b.Numbering.Ordering
}

// Evaluate returns an internal negative only when observed fields/inventories
// establish it. Release language lists have no completeness claim.
func Evaluate(f Filters, value *models.MediaEvidence, target Target, now time.Time) string {
	if !f.Active() {
		return "unfiltered"
	}
	value = models.CurrentMediaEvidence(value, now)
	if value == nil {
		return "unknown"
	}
	var result truth
	if f.Basis == "candidate" {
		c := value.Candidate
		if c == nil {
			return "unknown"
		}
		resolution := yes
		if f.ClaimedResolutionAtLeast != "" {
			resolution = unknown
			if c.Resolution != "" {
				want, _ := strconv.Atoi(strings.TrimSuffix(f.ClaimedResolutionAtLeast, "p"))
				actual, _ := strconv.Atoi(strings.TrimSuffix(c.Resolution, "p"))
				resolution = no
				if actual >= want {
					resolution = yes
				}
			}
		}
		result = conjunction(claimedLanguage(f.AudioLanguage, c.AudioLanguages), claimedLanguage(f.SubtitleLanguage, c.SubtitleLanguages), resolution, scalar(f.VideoCodec, c.VideoCodec), scalar(f.DynamicRange, c.HDR))
	} else {
		p := value.Probed
		if p == nil || !ExactEpisodeBinding(target, value) {
			return "unknown"
		}
		audio := yes
		if f.AudioLanguage != "" || f.AudioCodec != "" || f.AudioDescription != nil {
			audio = no
			uncertain := !p.AudioStreamsComplete
			for _, row := range p.AudioStreams {
				ad := yes
				if f.AudioDescription != nil {
					ad = unknown
					if row.AudioDescription != nil {
						ad = no
						if *row.AudioDescription {
							ad = yes
						}
					}
				}
				v := conjunction(scalar(f.AudioLanguage, row.Language), scalar(f.AudioCodec, row.Codec), ad)
				if v == yes {
					audio = yes
					break
				}
				if v == unknown {
					uncertain = true
				}
			}
			if audio != yes && uncertain {
				audio = unknown
			}
		}
		subtitle := yes
		if f.SubtitleLanguage != "" {
			subtitle = no
			uncertain := !p.SubtitleStreamsComplete
			for _, row := range p.SubtitleStreams {
				v := scalar(f.SubtitleLanguage, row.Language)
				if v == yes {
					subtitle = yes
					break
				}
				if v == unknown {
					uncertain = true
				}
			}
			if subtitle != yes && uncertain {
				subtitle = unknown
			}
		}
		height := yes
		if f.HeightAtLeast != nil {
			height = unknown
			if p.Video.Height > 0 {
				height = no
				if p.Video.Height >= *f.HeightAtLeast {
					height = yes
				}
			}
		}
		result = conjunction(audio, subtitle, height, scalar(f.VideoCodec, p.Video.Codec), scalar(f.DynamicRange, p.Video.HDR))
	}
	if result == yes {
		return "match"
	}
	if result == no {
		return "no_match"
	}
	return "unknown"
}
