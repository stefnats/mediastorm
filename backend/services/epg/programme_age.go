package epg

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

func programmeSource(kind, address string, at time.Time) *models.EPGProgramSource {
	// Do not persist URLs that can contain provider credentials. The label and
	// opaque digest still distinguish the actual imported guide source.
	sum := sha256.Sum256([]byte(kind + "\x00" + address))
	return &models.EPGProgramSource{Type: kind, ID: hex.EncodeToString(sum[:]), Name: xmltvHostLabel(address), ObservedAt: at}
}
func copyProgrammeSource(source *models.EPGProgramSource) *models.EPGProgramSource {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}
func copyAgeProgramme(program models.EPGProgram) *models.EPGProgram {
	program.Source = copyProgrammeSource(program.Source)
	program.Ratings = append([]models.EPGContentRating(nil), program.Ratings...)
	program.Categories = append([]string(nil), program.Categories...)
	return &program
}
func validGermanAge(age int) bool { return age == 0 || age == 6 || age == 12 || age == 16 || age == 18 }

// CurrentProgramAgeDecision reads only the already stored guide. guideTime uses
// the same clock as stored programme Start/Stop; callers can apply the existing
// EPG display offset before calling. The interval includes Start and excludes
// Stop. A result describes EPG metadata only and grants no Live TV playback.
func (s *Service) CurrentProgramAgeDecision(channelID string, maximumAge int, guideTime time.Time) models.EPGAgeDecision {
	result := models.EPGAgeDecision{State: "unknown", Reason: "invalid_age_limit", Country: "DE", MaximumAge: maximumAge, ChannelID: channelID, At: guideTime}
	if !validGermanAge(maximumAge) {
		return result
	}
	result.Reason = "no_current_programme"
	if s == nil || guideTime.IsZero() {
		return result
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.schedule == nil {
		return result
	}
	resolved, reason := ageProgrammeChannel(s.schedule, channelID)
	if reason != "" {
		result.Reason = reason
		return result
	}
	result.ChannelID = resolved
	var current *models.EPGProgram
	for _, program := range s.schedule.Programs[resolved] {
		if program.Start.IsZero() || program.Stop.IsZero() || !program.Stop.After(program.Start) {
			continue
		}
		if guideTime.Before(program.Start) || !guideTime.Before(program.Stop) {
			continue
		}
		if current != nil {
			result.Reason = "overlapping_programmes"
			return result
		}
		if program.ChannelID != resolved {
			result.Reason = "unclear_programme_channel"
			return result
		}
		current = copyAgeProgramme(program)
	}
	if current == nil {
		return result
	}
	result.Program = current
	if current.Source == nil || current.Source.ID == "" || current.Source.Type == "" || current.Source.ObservedAt.IsZero() {
		result.Reason = "rating_source_unknown"
		return result
	}
	if len(current.Ratings) == 0 {
		result.Reason = "rating_unknown"
		return result
	}
	age := -1
	for _, rating := range current.Ratings {
		system := strings.TrimSpace(rating.System)
		if system == "" {
			result.Reason = "rating_system_unknown"
			return result
		}
		if !strings.EqualFold(system, "FSK") {
			// Other explicit regional systems can accompany the German rating;
			// they neither override it nor supply a missing FSK classification.
			continue
		}
		value := strings.ToUpper(strings.TrimSpace(rating.Value))
		value = strings.TrimPrefix(value, "FSK ")
		value = strings.TrimSuffix(value, "+")
		parsed, err := strconv.Atoi(value)
		if err != nil || strconv.Itoa(parsed) != value || !validGermanAge(parsed) {
			result.Reason = "rating_unknown"
			return result
		}
		if age >= 0 && age != parsed {
			result.Reason = "conflicting_ratings"
			return result
		}
		age = parsed
	}
	if age < 0 {
		result.Reason = "rating_system_unknown"
		return result
	}
	result.Age = &age
	result.State, result.Reason = "within_limit", "guide_age_within_limit"
	if age > maximumAge {
		result.State, result.Reason = "too_old", "guide_age_above_limit"
	}
	return result
}

func ageProgrammeChannel(schedule *models.EPGSchedule, requested string) (string, string) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return "", "channel_unknown"
	}
	id := strings.ToLower(requested)
	if _, ok := schedule.Programs[id]; ok {
		return id, ""
	}
	// Only unique stored aliases may identify a guide channel. Never choose an
	// arbitrary station when HD/SD normalization or display names collide.
	find := func(exact bool) (string, int) {
		matched := ""
		count := 0
		for id, channel := range schedule.Channels {
			hit := false
			if exact {
				for _, name := range append([]string{channel.Name}, channel.Aliases...) {
					hit = hit || exactChannelName(name) == exactChannelName(requested)
				}
			} else {
				key := normalizeChannelID(requested)
				if key == "" {
					continue
				}
				hit = normalizeChannelID(id) == key
				for _, name := range append([]string{channel.Name}, channel.Aliases...) {
					hit = hit || normalizeChannelID(name) == key
				}
			}
			if hit {
				matched = id
				count++
			}
		}
		return matched, count
	}
	if matched, count := find(true); count == 1 {
		return matched, ""
	} else if count > 1 {
		return "", "ambiguous_channel"
	}
	if matched, count := find(false); count == 1 {
		return matched, ""
	} else if count > 1 {
		return "", "ambiguous_channel"
	}
	return "", "channel_unknown"
}
