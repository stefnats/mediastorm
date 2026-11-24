package user_settings

import (
	"encoding/json"
	"fmt"
	"strings"

	"novastream/models"
)

func pictureStyleSnapshot(raw json.RawMessage) (models.PictureStyleSnapshot, error) {
	value := models.PictureStyleSnapshot{Version: 1}
	if len(raw) == 0 {
		return value, nil
	}
	if json.Unmarshal(raw, &value) != nil || value.Version != 1 || len(value.Samples) > models.MaxPictureStyleSamples {
		return models.PictureStyleSnapshot{}, fmt.Errorf("invalid picture-style snapshot")
	}
	seen := map[string]bool{}
	for _, sample := range value.Samples {
		key := sample.Kind + ":" + sample.TitleID
		if sample.TitleID == "" || len(sample.TitleID) > 128 || (sample.Kind != "poster" && sample.Kind != "backdrop") || seen[key] || sample.Textless && sample.Language != "" || !sample.Textless && sample.Language == "" {
			return models.PictureStyleSnapshot{}, fmt.Errorf("invalid picture-style sample")
		}
		seen[key] = true
		if len(sample.Aliases) > 16 {
			return models.PictureStyleSnapshot{}, fmt.Errorf("too many picture-style aliases")
		}
		for _, alias := range sample.Aliases {
			if alias == sample.TitleID {
				continue
			}
			key := sample.Kind + ":" + alias
			if alias == "" || len(alias) > 128 || seen[key] {
				return models.PictureStyleSnapshot{}, fmt.Errorf("overlapping picture-style aliases")
			}
			seen[key] = true
		}
	}
	return value, nil
}

func (s *Service) GetPictureStyle(userID string) (models.PictureStyleSnapshot, error) {
	settings, err := s.Get(userID)
	if err != nil {
		return models.PictureStyleSnapshot{}, err
	}
	if settings == nil {
		return models.PictureStyleSnapshot{Version: 1}, nil
	}
	return pictureStyleSnapshot(settings.PictureStylePreferences)
}

// mutatePictureStyle shares the settings lock and existing persistence outcome
// handling. General settings writes cannot overwrite this dedicated snapshot.
func (s *Service) mutatePictureStyle(userID string, mutate func(*models.PictureStyleSnapshot)) (models.PictureStyleSnapshot, error) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return models.PictureStyleSnapshot{}, ErrUserIDRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.useDB() {
		ctx, cancel := settingsStorageContext()
		defer cancel()
		if err := s.ensurePGTargetLocked(ctx, userID); err != nil {
			return models.PictureStyleSnapshot{}, err
		}
	}
	previous, existed := s.settings[userID]
	state, err := pictureStyleSnapshot(previous.PictureStylePreferences)
	if err != nil {
		return models.PictureStyleSnapshot{}, err
	}
	mutate(&state)
	raw, err := json.Marshal(state)
	if err != nil {
		return models.PictureStyleSnapshot{}, err
	}
	if _, err := pictureStyleSnapshot(raw); err != nil {
		return models.PictureStyleSnapshot{}, err
	}
	next := previous
	next.PictureStylePreferences = raw
	s.settings[userID] = next
	if err := s.saveLocked(); err != nil {
		if !s.useDB() {
			if existed {
				s.settings[userID] = previous
			} else {
				delete(s.settings, userID)
			}
		}
		return models.PictureStyleSnapshot{}, err
	}
	return state, nil
}

func (s *Service) UpdatePictureStyle(userID string, enabled *bool, reset bool) (models.PictureStyleSnapshot, error) {
	if (enabled == nil) == !reset {
		return models.PictureStyleSnapshot{}, fmt.Errorf("choose enabled or reset")
	}
	return s.mutatePictureStyle(userID, func(state *models.PictureStyleSnapshot) {
		if reset {
			state.Enabled = false
			state.Samples = nil
		} else {
			state.Enabled = *enabled
		}
	})
}

// RecordPictureStyle removes obsolete evidence even when learning is disabled.
// A nil sample is a manual reset or a choice without usable image descriptors.
func (s *Service) RecordPictureStyle(userID string, aliases []string, kind string, sample *models.PictureStyleSample) error {
	if len(aliases) == 0 || len(aliases) > 16 || (kind != "poster" && kind != "backdrop") {
		return fmt.Errorf("invalid picture-style identity")
	}
	wanted := map[string]bool{}
	for _, alias := range aliases {
		if alias == "" || len(alias) > 128 {
			return fmt.Errorf("invalid picture-style alias")
		}
		wanted[alias] = true
	}
	if sample != nil && (sample.TitleID != aliases[0] || sample.Kind != kind) {
		return fmt.Errorf("picture-style sample identity differs")
	}
	// Disabled/new profiles do not acquire any record merely by choosing an image.
	current, err := s.GetPictureStyle(userID)
	if err != nil {
		return err
	}
	if !current.Enabled && len(current.Samples) == 0 {
		return nil
	}
	_, err = s.mutatePictureStyle(userID, func(state *models.PictureStyleSnapshot) {
		out := make([]models.PictureStyleSample, 0, len(state.Samples)+1)
		combined := append([]string(nil), aliases...)
		seen := map[string]bool{}
		for _, alias := range combined {
			seen[alias] = true
		}
		for _, value := range state.Samples {
			overlap := value.Kind == kind && wanted[value.TitleID]
			if value.Kind == kind {
				for _, alias := range value.Aliases {
					if wanted[alias] {
						overlap = true
					}
				}
			}
			if !overlap {
				out = append(out, value)
				continue
			}
			for _, alias := range append([]string{value.TitleID}, value.Aliases...) {
				if !seen[alias] {
					seen[alias] = true
					combined = append(combined, alias)
				}
			}
		}
		if state.Enabled && sample != nil && len(combined) <= 16 {
			value := *sample
			value.Aliases = combined
			out = append(out, value)
		}
		if len(out) > models.MaxPictureStyleSamples {
			out = out[len(out)-models.MaxPictureStyleSamples:]
		}
		state.Samples = out
	})
	return err
}
