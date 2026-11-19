package handlers

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"

	"novastream/config"
	metadatapkg "novastream/services/metadata"
)

// Cached browsing reads current preferences without materializing config defaults.
// The returned reader is retained for the lifetime of this one observation.
type cachedMetadataContext struct {
	base        metadataService
	reader      cachedSimilarReader
	languages   cachedOriginalLanguageReader
	locale      string
	preferences [32]byte
}

type cachedMetadataPreferences struct {
	Configured      bool
	ConfigPath      string
	Enabled         []string
	Global, Profile string
}

func (h *MetadataHandler) cachedMetadataPreferences(userID string) (cachedMetadataPreferences, string, error) {
	if h == nil || h.Service == nil {
		return cachedMetadataPreferences{}, "", errors.New("metadata reader unavailable")
	}
	facts := cachedMetadataPreferences{Configured: h.CfgManager != nil}
	if h.CfgManager == nil {
		return facts, "", nil // Preserve the actual base service language.
	}
	facts.ConfigPath = h.CfgManager.ConfigPath()
	settings := config.DefaultSettings()
	raw, err := os.ReadFile(facts.ConfigPath)
	if err != nil && !os.IsNotExist(err) {
		return facts, "", err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &settings); err != nil {
			return facts, "", err
		}
	}
	facts.Enabled = append([]string(nil), settings.Metadata.Language...)
	facts.Global = settings.Metadata.PrimaryLanguage
	language := settings.Metadata.EffectivePrimaryLanguage()
	if h.UserSettings != nil {
		profile, err := h.UserSettings.Get(userID)
		if err != nil {
			return facts, "", err
		}
		if profile != nil {
			facts.Profile = profile.Metadata.PrimaryLanguage
			preferred := strings.TrimSpace(facts.Profile)
			if preferred != "" {
				for _, enabled := range settings.Metadata.Language {
					if strings.EqualFold(strings.TrimSpace(enabled), preferred) {
						language = strings.TrimSpace(enabled)
						break
					}
				}
			}
		}
	}
	return facts, language, nil
}

func cachedMetadataPreferencesStamp(facts cachedMetadataPreferences) [32]byte {
	raw, _ := json.Marshal(facts)
	return sha256.Sum256(raw)
}

func (h *MetadataHandler) captureCachedMetadata(userID string) (*cachedMetadataContext, error) {
	facts, language, err := h.cachedMetadataPreferences(userID)
	if err != nil {
		return nil, err
	}
	base := h.Service
	service := base
	if facts.Configured {
		localized, ok := service.(interface {
			WithLanguage(string) *metadatapkg.Service
		})
		if !ok {
			return nil, errors.New("localized cached metadata unavailable")
		}
		service = localized.WithLanguage(language)
	}
	reader, ok := service.(cachedSimilarReader)
	languages, languageOK := service.(cachedOriginalLanguageReader)
	if !ok || !languageOK || reader.CachedSimilarMetadataLanguage() == "" {
		return nil, errors.New("cached metadata unavailable")
	}
	return &cachedMetadataContext{base: base, reader: reader, languages: languages, locale: reader.CachedSimilarMetadataLanguage(), preferences: cachedMetadataPreferencesStamp(facts)}, nil
}

func (h *MetadataHandler) cachedMetadataCurrent(userID string, captured *cachedMetadataContext) (bool, error) {
	facts, _, err := h.cachedMetadataPreferences(userID)
	if err != nil {
		return false, err
	}
	current, original := reflect.ValueOf(h.Service), reflect.ValueOf(captured.base)
	same := current.IsValid() && original.IsValid() && current.Type() == original.Type() && current.Comparable() && original.Comparable() && current.Equal(original)
	return same && cachedMetadataPreferencesStamp(facts) == captured.preferences && captured.reader.CachedSimilarMetadataLanguage() == captured.locale, nil
}
