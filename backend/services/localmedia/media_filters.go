package localmedia

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"novastream/models"
	"novastream/services/sourcefilters"
)

var ErrInvalidMediaFilters = errors.New("invalid library media filters")

func ValidateMediaFilters(f models.LocalMediaFilters) error {
	unknown := f.Unknown
	if unknown == "" || unknown == "only" {
		unknown = "include"
	}
	base := sourcefilters.Filters{Basis: "probed", Unknown: unknown, AudioLanguage: f.AudioLanguage, SubtitleLanguage: f.SubtitleLanguage, AudioCodec: f.AudioCodec, VideoCodec: f.VideoCodec, DynamicRange: f.HDR}
	if f.MinHeight != 0 {
		base.HeightAtLeast = &f.MinHeight
	}
	if err := base.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMediaFilters, err)
	}
	if f.Subtitles != "" && f.Subtitles != "present" && f.Subtitles != "absent" {
		return ErrInvalidMediaFilters
	}
	return nil
}

type mediaMatch int

const (
	mediaYes mediaMatch = iota
	mediaNo
	mediaUnknown
)

func mediaAnd(a, b mediaMatch) mediaMatch {
	if a == mediaNo || b == mediaNo {
		return mediaNo
	}
	if a == mediaUnknown || b == mediaUnknown {
		return mediaUnknown
	}
	return mediaYes
}
func mediaScalar(want, actual string) mediaMatch {
	if want == "" {
		return mediaYes
	}
	if actual == "" {
		return mediaUnknown
	}
	if strings.EqualFold(want, actual) {
		return mediaYes
	}
	return mediaNo
}
func audioMediaMatch(p *models.LocalMediaProbe, f models.LocalMediaFilters) mediaMatch {
	if f.AudioLanguage == "" && f.AudioCodec == "" {
		return mediaYes
	}
	if p == nil {
		return mediaUnknown
	}
	// Older scans retained a codec set without the per-track language mapping.
	// A known codec contradiction still rejects the combination; uncertainty
	// about its language cannot turn a known mismatch into an unknown result.
	if !p.TracksScanned && f.AudioCodec != "" {
		codecPresent := false
		for _, codec := range p.AudioCodecs {
			codecPresent = codecPresent || strings.EqualFold(codec, f.AudioCodec)
		}
		completeCodecs := len(p.AudioCodecs) > 0 && (p.AudioStreams == 0 || len(p.AudioCodecs) >= p.AudioStreams)
		if !codecPresent && completeCodecs {
			return mediaNo
		}
		if f.AudioLanguage == "" {
			if codecPresent {
				return mediaYes
			}
			return mediaUnknown
		}
	}
	unknown := !p.TracksScanned
	for _, track := range p.AudioTracks {
		match := mediaAnd(mediaScalar(f.AudioLanguage, track.Language), mediaScalar(f.AudioCodec, track.Codec))
		if match == mediaYes {
			return mediaYes
		}
		unknown = unknown || match == mediaUnknown
	}
	if unknown {
		return mediaUnknown
	}
	return mediaNo
}
func subtitleMediaMatch(p *models.LocalMediaProbe, f models.LocalMediaFilters) mediaMatch {
	if f.SubtitleLanguage == "" && f.Subtitles == "" {
		return mediaYes
	}
	if p == nil {
		return mediaUnknown
	}
	presence := mediaYes
	if f.Subtitles != "" {
		if p.SubtitleStreams > 0 || len(p.SubtitleTracks) > 0 {
			if f.Subtitles == "absent" {
				presence = mediaNo
			}
		} else if !p.TracksScanned {
			presence = mediaUnknown
		} else if f.Subtitles == "present" {
			presence = mediaNo
		}
	}
	if f.SubtitleLanguage == "" {
		return presence
	}
	language := mediaNo
	if !p.TracksScanned {
		language = mediaUnknown
	}
	for _, track := range p.SubtitleTracks {
		match := mediaScalar(f.SubtitleLanguage, track.Language)
		if match == mediaYes {
			language = mediaYes
			break
		}
		if match == mediaUnknown {
			language = mediaUnknown
		}
	}
	return mediaAnd(presence, language)
}
func localMediaMatchesFilters(p *models.LocalMediaProbe, f models.LocalMediaFilters) bool {
	if !f.Active() {
		return true
	}
	match := mediaAnd(audioMediaMatch(p, f), subtitleMediaMatch(p, f))
	if f.VideoCodec != "" || f.HDR != "" || f.MinHeight > 0 {
		if p == nil {
			match = mediaAnd(match, mediaUnknown)
		} else {
			match = mediaAnd(match, mediaScalar(f.VideoCodec, p.VideoCodec))
			match = mediaAnd(match, mediaScalar(f.HDR, p.HDRFormat))
			if f.MinHeight > 0 {
				height := mediaYes
				if p.Height == 0 {
					height = mediaUnknown
				} else if p.Height < f.MinHeight {
					height = mediaNo
				}
				match = mediaAnd(match, height)
			}
		}
	}
	if f.Unknown == "only" {
		return match == mediaUnknown
	}
	return match == mediaYes || match == mediaUnknown && f.Unknown != "exclude"
}

// The filtered list reads stored rows once; it neither opens files nor enriches
// metadata. Filtering and counting precede the original requested page.
func (s *Service) listMediaFilteredItems(ctx context.Context, library string, q models.LocalMediaItemListQuery) (*models.LocalMediaItemListResult, error) {
	rows, err := s.repo.ListAllItemsByLibrary(ctx, library)
	if err != nil {
		return nil, err
	}
	items := make([]models.LocalMediaItem, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !q.IncludeMissing && row.IsMissing {
			continue
		}
		if localMediaItemMatchesQuery(row, q.Filter, q.Query) && localMediaMatchesFilters(row.Probe, q.MediaFilters) {
			items = append(items, row)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		cmp := 0
		switch q.Sort {
		case "name":
			cmp = strings.Compare(mediaItemName(a), mediaItemName(b))
		case "confidence":
			cmp = compareFloat64(a.Confidence, b.Confidence)
		case "year":
			cmp = compareInt(mediaItemYear(a), mediaItemYear(b))
		case "size":
			cmp = compareInt64(a.SizeBytes, b.SizeBytes)
		case "status":
			cmp = strings.Compare(string(a.MatchStatus), string(b.MatchStatus))
		case "modified":
			if a.ModifiedAt == nil && b.ModifiedAt != nil {
				cmp = 1
			} else if a.ModifiedAt != nil && b.ModifiedAt == nil {
				cmp = -1
			} else {
				cmp = compareTimePtr(a.ModifiedAt, b.ModifiedAt)
			}
		default:
			cmp = a.UpdatedAt.Compare(b.UpdatedAt)
		}
		if cmp != 0 {
			if q.Dir == "desc" {
				return cmp > 0
			}
			return cmp < 0
		}
		if a.RelativePath != b.RelativePath {
			return a.RelativePath < b.RelativePath
		}
		return a.ID < b.ID
	})
	total := len(items)
	start := min(q.Offset, total)
	end := min(start+q.Limit, total)
	result := &models.LocalMediaItemListResult{Items: items[start:end], Total: total, Limit: q.Limit, Offset: q.Offset}
	hydrateLocalMediaItemResultExternalIDs(result)
	return result, ctx.Err()
}
func mediaItemName(item models.LocalMediaItem) string {
	if item.MatchedName != "" {
		return item.MatchedName
	}
	if item.DetectedTitle != "" {
		return item.DetectedTitle
	}
	return item.FileName
}
func mediaItemYear(item models.LocalMediaItem) int {
	if item.MatchedYear != 0 {
		return item.MatchedYear
	}
	return item.DetectedYear
}
