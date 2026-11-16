package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/models"
	"novastream/services/recordings"
)

var errRecordingSourceUnavailable = errors.New("recording source is unavailable to this profile")

func (h *LiveHandler) recordingCatalogue(ctx context.Context, profileID string) ([]LiveChannel, *http.Request, error) {
	if h.usersSvc == nil {
		return nil, nil, errRecordingSourceUnavailable
	}
	profile, ok := h.usersSvc.Get(profileID)
	if !ok || profile.IsKidsProfile || profile.AccountID == "" {
		return nil, nil, errRecordingSourceUnavailable
	}
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, profile.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
	request := httptest.NewRequest(http.MethodGet, "/api/live/channels?profileId="+url.QueryEscape(profileID), nil).WithContext(ctx)
	w := httptest.NewRecorder()
	h.GetChannels(w, request)
	if w.Code != 200 {
		return nil, nil, errRecordingSourceUnavailable
	}
	var payload LiveChannelsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		return nil, nil, errRecordingSourceUnavailable
	}
	return payload.Channels, request, nil
}

func recordingSourceFrom(channel LiveChannel, original bool) recordings.RecordingSource {
	return recordings.RecordingSource{SourceID: channel.SourceID, ChannelID: channel.ID, StationID: channel.StationID, Name: channel.Name, URL: channel.URL, Original: original}
}

func (h *LiveHandler) PlanRecordingSources(ctx context.Context, recording models.Recording) ([]recordings.RecordingSource, error) {
	channels, request, err := h.recordingCatalogue(ctx, recording.UserID)
	if err != nil {
		return h.legacyRecordingSource(ctx, recording)
	}
	var initial *LiveChannel
	for i := range channels {
		if channels[i].ID == recording.ChannelID {
			if initial != nil {
				return nil, errRecordingSourceUnavailable
			}
			initial = &channels[i]
		}
	}
	if initial == nil || initial.StationID == "" {
		return h.legacyRecordingSource(ctx, recording)
	}
	result := []recordings.RecordingSource{recordingSourceFrom(*initial, true)}
	settings, err := h.cfgManager.Load()
	if err != nil {
		return nil, errRecordingSourceUnavailable
	}
	if !settings.Live.AutoChannelFailover {
		return result, nil
	}
	cap := settings.Filtering.MaxResolution
	if h.userSettingsSvc != nil {
		preferences, e := h.userSettingsSvc.Get(recording.UserID)
		if e != nil {
			return nil, errRecordingSourceUnavailable
		}
		if preferences != nil && preferences.Filtering.MaxResolution != nil {
			cap = models.StringVal(preferences.Filtering.MaxResolution, "")
		}
	}
	seen := map[string]bool{initial.URL: true}
	for _, channel := range channels {
		if len(result) >= 4 {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if channel.StationID == initial.StationID && !seen[channel.URL] {
			seen[channel.URL] = true
			if !stationFallbackQualityAllowed(channel.Name, cap) {
				continue
			}
			parsed, e := url.Parse(channel.URL)
			if e != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || strings.ContainsAny(channel.URL, "\r\n|") {
				continue
			}
			q := request.URL.Query()
			q.Set("sourceId", channel.SourceID)
			request.URL.RawQuery = q.Encode()
			target, e := h.liveCapacityTarget(request, channel.URL)
			if e != nil || (target.Provider != "m3u" && target.Provider != "xtream") || strings.TrimSpace(target.ProxyURL) != "" {
				continue
			}
			result = append(result, recordingSourceFrom(channel, false))
		}
	}
	return result, nil
}

func (h *LiveHandler) PrepareRecordingSource(ctx context.Context, recording models.Recording, candidate recordings.RecordingSource) (recordings.RecordingInput, error) {
	if ctx.Err() != nil {
		return recordings.RecordingInput{}, ctx.Err()
	}
	if candidate.Original {
		return h.prepareOriginalRecording(ctx, recording, candidate)
	}
	channels, request, err := h.recordingCatalogue(ctx, recording.UserID)
	if err != nil {
		return recordings.RecordingInput{}, err
	}
	settings, err := h.cfgManager.Load()
	if err != nil || !candidate.Original && !settings.Live.AutoChannelFailover {
		return recordings.RecordingInput{}, errRecordingSourceUnavailable
	}
	cap := settings.Filtering.MaxResolution
	if h.userSettingsSvc != nil {
		preferences, err := h.userSettingsSvc.Get(recording.UserID)
		if err != nil {
			return recordings.RecordingInput{}, errRecordingSourceUnavailable
		}
		if preferences != nil && preferences.Filtering.MaxResolution != nil {
			cap = models.StringVal(preferences.Filtering.MaxResolution, "")
		}
	}
	for _, channel := range channels {
		if channel.ID != candidate.ChannelID || channel.SourceID != candidate.SourceID || channel.StationID != candidate.StationID {
			continue
		}
		if !stationFallbackQualityAllowed(channel.Name, cap) {
			return recordings.RecordingInput{}, errRecordingSourceUnavailable
		}
		parsed, err := url.Parse(channel.URL)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || strings.ContainsAny(channel.URL, "\r\n|") {
			return recordings.RecordingInput{}, errRecordingSourceUnavailable
		}
		q := request.URL.Query()
		q.Set("sourceId", channel.SourceID)
		q.Set("channelId", channel.ID)
		request.URL.RawQuery = q.Encode()
		if _, err = authorizeLiveStreamURL(request, channel.URL, h.cfgManager, h); err != nil {
			return recordings.RecordingInput{}, errRecordingSourceUnavailable
		}
		target, err := h.liveCapacityTarget(request, channel.URL)
		if err != nil || (target.Provider != "m3u" && target.Provider != "xtream") || strings.TrimSpace(target.ProxyURL) != "" {
			return recordings.RecordingInput{}, errRecordingSourceUnavailable
		}
		profile, ok := h.usersSvc.Get(recording.UserID)
		if !ok || profile.IsKidsProfile {
			return recordings.RecordingInput{}, errRecordingSourceUnavailable
		}
		lease, err := livecapacity.Default.AcquireOwned(target.BucketKey, "", target.MaxStreams, livecapacity.Owner{AccountID: profile.AccountID, ProfileID: profile.ID, Kind: livecapacity.KindRecording, RecordingID: recording.ID})
		if err != nil {
			return recordings.RecordingInput{}, err
		}
		if ctx.Err() != nil {
			lease.Release()
			return recordings.RecordingInput{}, ctx.Err()
		}
		return recordings.RecordingInput{Source: recordingSourceFrom(channel, candidate.Original), Lease: lease}, nil
	}
	return recordings.RecordingInput{}, errRecordingSourceUnavailable
}

// Older explicitly selected primary transports retain their existing owned
// authorization/reservation path. Unproved primary identity never supplies an
// automatic alternative; strict direct-input requirements apply to replacements.
func (h *LiveHandler) legacyRecordingSource(ctx context.Context, recording models.Recording) ([]recordings.RecordingSource, error) {
	if h.usersSvc == nil {
		return nil, errRecordingSourceUnavailable
	}
	profile, ok := h.usersSvc.Get(recording.UserID)
	if !ok || profile.IsKidsProfile || profile.AccountID == "" {
		return nil, errRecordingSourceUnavailable
	}
	resolved, err := h.AuthorizeRecording(ctx, recording)
	if err != nil || resolved == "" {
		return nil, errRecordingSourceUnavailable
	}
	return []recordings.RecordingSource{{ChannelID: recording.ChannelID, Name: recording.ChannelName, URL: resolved, Original: true}}, nil
}

func (h *LiveHandler) prepareOriginalRecording(ctx context.Context, recording models.Recording, candidate recordings.RecordingSource) (recordings.RecordingInput, error) {
	var source recordings.RecordingSource
	if candidate.StationID != "" {
		if channels, _, err := h.recordingCatalogue(ctx, recording.UserID); err == nil {
			for _, channel := range channels {
				if channel.ID == candidate.ChannelID && channel.SourceID == candidate.SourceID && channel.StationID == candidate.StationID {
					source = recordingSourceFrom(channel, true)
					break
				}
			}
		}
	}
	if source.URL == "" {
		legacy, err := h.legacyRecordingSource(ctx, recording)
		if err != nil {
			return recordings.RecordingInput{}, err
		}
		source = legacy[0]
	}
	copy := recording
	copy.SourceURL = source.URL
	lease, err := h.ReserveManagedRecording(ctx, copy)
	if err != nil {
		return recordings.RecordingInput{}, err
	}
	if ctx.Err() != nil {
		lease.Release()
		return recordings.RecordingInput{}, ctx.Err()
	}
	return recordings.RecordingInput{Source: source, Lease: lease}, nil
}
