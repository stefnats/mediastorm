package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"novastream/models"
	"novastream/services/mediaevidence"
	"novastream/services/playback"
)

// fullProbeEvidence reads the raw inventory before legacy playable-track
// filtering. Unsupported/bitmap subtitles still exist in the container.
func fullProbeEvidence(meta *ffprobeOutput, at time.Time) *models.MediaProbeFacts {
	if meta == nil || len(meta.Streams) == 0 {
		return nil
	}
	streams := make([]mediaevidence.ProbeStream, 0, len(meta.Streams))
	for _, raw := range meta.Streams {
		stream := mediaevidence.ProbeStream{Index: raw.Index, CodecType: raw.CodecType, CodecName: raw.CodecName, Width: raw.Width, Height: raw.Height, ColorTransfer: raw.ColorTransfer, ColorPrimaries: raw.ColorPrimaries, Tags: raw.Tags, Disposition: raw.Disposition}
		for _, side := range raw.SideDataList {
			stream.SideData = append(stream.SideData, struct {
				Type      string `json:"side_data_type"`
				DVProfile int    `json:"dv_profile"`
			}{side.SideDataType, side.DVProfile})
		}
		streams = append(streams, stream)
	}
	return mediaevidence.ProbeFacts(streams, at)
}

func probeInputVersion(path string) string { return mediaevidence.InputVersion(path) }

func preparationEvidenceMarker(entry *playback.PrequeueEntry) string {
	var tuple any
	if ep := entry.TargetEpisode; ep != nil {
		tuple = []any{ep.EpisodeID, ep.Numbering, ep.SeasonNumber, ep.EpisodeNumber}
	}
	data, _ := json.Marshal([]any{entry.LibraryIdentity, tuple})
	return string(data)
}

func (h *PrequeueHandler) recordPreparationEvidence(id, clientID string, probe *VideoFullResult) {
	if probe == nil || probe.MetadataEvidence == nil {
		return
	}
	entry, ok := h.store.Get(id)
	if !ok || entry.SelectedResult == nil || entry.StreamPath == "" {
		return
	}
	if probe.MetadataEvidenceInputVersion != probeInputVersion(entry.StreamPath) {
		return
	}
	// A persisted caller movie ID is not an independently verified catalogue
	// association. Keep movie facts source-only in this foundation.
	titleID := ""
	var episode *models.EpisodeReference
	if entry.MediaType == "series" {
		if entry.LibraryEpisodeVerified {
			titleID = entry.TitleID
			episode = entry.TargetEpisode
		}
	}
	value := mediaevidence.Probed(*entry.SelectedResult, entry.StreamPath, preparationEvidenceMarker(entry), entry.FileSize, probe.MetadataEvidence, titleID, episode, time.Now())
	if value == nil {
		return
	}
	context := h.reportContextForPrequeue(id, entry.UserID, clientID, entry.TargetEpisode)
	revision := ""
	if h.badStreamsSvc != nil {
		if h.badStreamsSvc.MatchFor(*entry.SelectedResult, context) != nil {
			return
		}
		revision = h.badStreamsSvc.RevisionFor(context)
	}
	h.store.UpdateWorker(id, func(current *playback.PrequeueEntry) {
		// A replaced worker cannot move an observation to its successor.
		if current.StreamPath != entry.StreamPath || current.FileSize != entry.FileSize || preparationEvidenceMarker(current) != preparationEvidenceMarker(entry) {
			return
		}
		current.MediaEvidence = value
		current.MediaEvidenceReportRevision = revision
		current.MediaEvidenceClientID = clientID
	})
}

func (h *PrequeueHandler) currentPreparationEvidence(r *http.Request, entry *playback.PrequeueEntry) *models.MediaEvidence {
	if entry == nil || entry.ReuseDisabled || entry.MediaEvidence == nil || entry.SelectedResult == nil {
		return nil
	}
	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	if client != entry.MediaEvidenceClientID {
		return nil
	}
	value := models.CurrentMediaEvidence(entry.MediaEvidence, time.Now())
	if value == nil || value.SourceVersion != mediaevidence.FileVersion(entry.StreamPath, preparationEvidenceMarker(entry), entry.FileSize) {
		return nil
	}
	if h.badStreamsSvc != nil {
		context := h.reportContextForPrequeue(entry.ID, entry.UserID, client, entry.TargetEpisode)
		context.AudioTrackIndex = &entry.SelectedAudioTrack
		if h.badStreamsSvc.MatchFor(*entry.SelectedResult, context) != nil || h.badStreamsSvc.RevisionFor(context) != entry.MediaEvidenceReportRevision {
			return nil
		}
	}
	return value
}
