package remotemedia

import (
	"context"
	"strings"

	"novastream/models"
	"novastream/services/jellyfin"
	"novastream/services/plex"
)

func remoteHDR(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "sdr":
		return "sdr"
	case "hdr10", "hdr10+", "hdr10plus":
		return "hdr10"
	case "dolby_vision", "dolbyvision", "dovi", "doviwithhdr10", "doviwithsdr":
		return "dolby_vision"
	case "hlg":
		return "hlg"
	default:
		return "" // A generic HDR label is not proof of a particular format.
	}
}

func remoteCodec(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "avc", "h.264":
		return "h264"
	case "h265", "h.265":
		return "hevc"
	case "dca":
		return "dts"
	}
	return value
}

func remoteFilterProbe(item models.RemoteMediaItem) *models.LocalMediaProbe {
	if item.Probe != nil {
		return item.Probe
	}
	// Legacy scalar audio metadata describes only the primary codec, not a full
	// track inventory. Do not treat other codecs/languages as known absent.
	return &models.LocalMediaProbe{FormatName: item.Container, DurationSeconds: item.DurationSeconds, SizeBytes: item.SizeBytes, VideoCodec: remoteCodec(item.VideoCodec), Width: item.Width, Height: item.Height, HDRFormat: remoteHDR(item.HDRFormat)}
}

type remoteFilterStream struct {
	index                 *int
	kind, codec, language string
	external              bool
}

func storedTrackProbe(item models.RemoteMediaItem, present bool, streams []remoteFilterStream) *models.LocalMediaProbe {
	p := remoteFilterProbe(item)
	p.TracksScanned = present
	indices := make(map[int]int)
	for _, stream := range streams {
		if stream.index != nil {
			indices[*stream.index]++
		}
	}
	for _, stream := range streams {
		if stream.kind != "audio" && stream.kind != "subtitle" {
			continue
		}
		if stream.external {
			continue
		}
		if stream.kind == "audio" {
			p.AudioStreams++
		} else {
			p.SubtitleStreams++
		}
		if stream.index == nil || *stream.index < 0 || indices[*stream.index] != 1 {
			p.TracksScanned = false
			continue
		}
		codec := remoteCodec(stream.codec)
		switch stream.kind {
		case "audio":
			if len(p.AudioTracks) >= models.MaxMediaEvidenceStreams {
				p.TracksScanned = false
				continue
			}
			p.AudioTracks = append(p.AudioTracks, models.MediaAudioFacts{Index: *stream.index, Language: models.NormalizeEvidenceLanguage(stream.language), Codec: codec})
			if codec != "" {
				p.AudioCodecs = append(p.AudioCodecs, codec)
			}
		case "subtitle":
			if len(p.SubtitleTracks) >= models.MaxMediaEvidenceStreams {
				p.TracksScanned = false
				continue
			}
			p.SubtitleTracks = append(p.SubtitleTracks, models.MediaSubtitleFacts{Index: *stream.index, Language: models.NormalizeEvidenceLanguage(stream.language), Codec: codec})
			if codec != "" {
				p.SubtitleCodecs = append(p.SubtitleCodecs, codec)
			}
		}
	}
	return p
}

func jellyfinFilterProbe(item models.RemoteMediaItem, streams []jellyfin.JellyfinMediaStream) *models.LocalMediaProbe {
	raw := make([]remoteFilterStream, 0, len(streams))
	for _, stream := range streams {
		raw = append(raw, remoteFilterStream{stream.Index, strings.ToLower(stream.Type), stream.Codec, stream.Language, stream.IsExternal})
	}
	p := storedTrackProbe(item, streams != nil, raw)
	for _, stream := range streams {
		if strings.EqualFold(stream.Type, "video") && stream.VideoRangeType != "" {
			p.HDRFormat = remoteHDR(stream.VideoRangeType)
			break
		}
	}
	return p
}

func plexFilterProbe(item models.RemoteMediaItem, streams []plex.PlexStream) *models.LocalMediaProbe {
	raw := make([]remoteFilterStream, 0, len(streams))
	for _, stream := range streams {
		kind := ""
		switch stream.StreamType {
		case 1:
			kind = "video"
		case 2:
			kind = "audio"
		case 3:
			kind = "subtitle"
		}
		raw = append(raw, remoteFilterStream{stream.Index, kind, stream.Codec, stream.LanguageCode, stream.StreamType == 3 && stream.Key != ""})
	}
	return storedTrackProbe(item, streams != nil, raw)
}

// List responses can omit Stream while the existing metadata endpoint supplies
// it. Only ordinary synchronization calls this, once per affected item, copying
// tracks solely to that exact existing part. Failed/partial details stay unknown.
func (s *Service) fillPlexStreamInventories(ctx context.Context, server plex.PlexResource, items []plex.PlexLibraryItem) error {
	for i := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		missing := false
		for _, media := range items[i].Media {
			for _, part := range media.Part {
				missing = missing || part.Stream == nil
			}
		}
		if !missing {
			continue
		}
		metadata, err := s.plex.GetServerMetadata(ctx, server, items[i].RatingKey)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || metadata == nil || metadata.RatingKey != items[i].RatingKey {
			continue
		}
		for mi := range items[i].Media {
			for pi := range items[i].Media[mi].Part {
				target := &items[i].Media[mi].Part[pi]
				if target.Stream != nil || target.ID <= 0 {
					continue
				}
				for _, media := range metadata.Media {
					for _, part := range media.Part {
						if part.ID == target.ID && part.Key == target.Key && part.File == target.File {
							target.Stream = part.Stream
						}
					}
				}
			}
		}
	}
	return nil
}
