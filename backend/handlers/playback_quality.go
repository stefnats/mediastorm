package handlers

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"novastream/models"
	"novastream/services/trackselect"
)

// Resolution tiers use the long image edge too, so cropped cinema frames such
// as 3840x1600 still qualify as 2160p rather than being mistaken for 1080p.
func observedResolution(probe *VideoFullResult) int {
	if probe == nil || probe.VideoWidth <= 0 || probe.VideoHeight <= 0 || strings.TrimSpace(probe.VideoCodec) == "" {
		return 0
	}
	wide, high := probe.VideoWidth, probe.VideoHeight
	if wide < high {
		wide, high = high, wide
	}
	for _, tier := range []struct{ height, width int }{{4320, 7680}, {2160, 3840}, {1440, 2560}, {1080, 1920}, {720, 1280}, {576, 1024}, {480, 854}} {
		if wide >= tier.width || high >= tier.height {
			return tier.height
		}
	}
	return high
}

func validateMinimumVideoResolution(probe *VideoFullResult, minimum string) error {
	minimum = strings.ToLower(strings.TrimSpace(minimum))
	if minimum == "" {
		return nil
	}
	if minimum == "4k" {
		minimum = "2160p"
	}
	var required int
	switch minimum {
	case "480p", "576p", "720p", "1080p", "1440p", "2160p", "4320p":
		required, _ = strconv.Atoi(strings.TrimSuffix(minimum, "p"))
	default:
		return errors.New("unsupported minimum playback resolution")
	}
	actual := observedResolution(probe)
	if actual == 0 {
		return fmt.Errorf("minimum playback resolution %s cannot be verified for this file", minimum)
	}
	if actual < required {
		return fmt.Errorf("file is %dp; minimum playback resolution is %s", actual, minimum)
	}
	return nil
}

// Only display attributes are corrected. SelectedResult and its provider claims
// stay untouched, including the release title. These values describe this probe's
// selected file, and are never copied back to search or another resolved file.
func observedQualityAttributes(advertised map[string]string, probe *VideoFullResult) map[string]string {
	attrs := make(map[string]string, len(advertised)+4)
	for key, value := range advertised {
		attrs[key] = value
	}
	if resolution := observedResolution(probe); resolution > 0 {
		if value := advertised["resolution"]; value != "" && advertised["resolutionSource"] != "file_probe" {
			attrs["advertisedResolution"] = value
		}
		attrs["resolution"] = strconv.Itoa(resolution) + "p"
		attrs["resolutionSource"] = "file_probe"
		attrs["videoWidth"] = strconv.Itoa(probe.VideoWidth)
		attrs["videoHeight"] = strconv.Itoa(probe.VideoHeight)
	}
	if len(attrs) == 0 {
		return nil
	}
	return attrs
}

// Reuse the already configured settings providers for manual resolve as well
// as automatic preparation. No release name can satisfy a strict minimum.
func requestedMinimumResolution(resolver *trackselect.Resolver, profileID, clientID string) (string, error) {
	if resolver == nil {
		return "", nil
	}
	minimum := ""
	defaults := models.UserSettings{}
	if resolver.Config != nil {
		settings, err := resolver.Config.Load()
		if err != nil {
			return "", errors.New("playback quality settings unavailable")
		}
		minimum = settings.Filtering.MinResolution
	}
	defaults.Filtering.MinResolution = models.StringPtr(minimum)
	if profileID != "" && resolver.UserSettings != nil {
		settings, err := resolver.UserSettings.GetWithDefaults(profileID, defaults)
		if err != nil {
			return "", errors.New("profile playback quality settings unavailable")
		}
		if settings.Filtering.MinResolution != nil {
			minimum = *settings.Filtering.MinResolution
		}
	}
	if profileID != "" && clientID != "" && resolver.ClientSettings != nil {
		settings, err := resolver.ClientSettings.Get(clientID, profileID)
		if err != nil {
			return "", errors.New("device playback quality settings unavailable")
		}
		if settings != nil && settings.MinResolution != nil {
			minimum = *settings.MinResolution
		}
	}
	return minimum, nil
}

func (h *PlaybackHandler) withPlaybackQuality(ctx context.Context, resolution *models.PlaybackResolution, profileID, clientID string, advertised map[string]string) (*models.PlaybackResolution, error) {
	minimum, err := requestedMinimumResolution(h.TrackPreferences, profileID, clientID)
	if err != nil {
		return nil, err
	}
	if resolution == nil || strings.TrimSpace(resolution.WebDAVPath) == "" {
		return resolution, nil
	}
	probe := resolution.Probe
	if strings.TrimSpace(minimum) != "" && probe == nil && h.VideoProber != nil {
		probeCtx, cancel := context.WithTimeout(ctx, trackSelectionProbeTimeout)
		defer cancel()
		probe, err = h.VideoProber.ProbeVideoFull(probeCtx, resolution.WebDAVPath)
		if err != nil {
			return nil, errors.New("minimum playback resolution could not be verified")
		}
	}
	if err := validateMinimumVideoResolution(probe, minimum); err != nil {
		return nil, err
	}
	result := *resolution
	result.Probe = probe
	if observedResolution(probe) > 0 {
		result.QualityAttributes = observedQualityAttributes(advertised, probe)
	}
	return &result, nil
}
