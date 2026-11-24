// Package picturestyle learns only verified language/textless image style from
// explicit choices. It never downloads, analyzes or semantically labels images.
package picturestyle

import (
	"strings"

	"novastream/models"
	"novastream/services/titleratings"
	"novastream/utils/language"
)

type Settings interface {
	GetPictureStyle(string) (models.PictureStyleSnapshot, error)
	UpdatePictureStyle(string, *bool, bool) (models.PictureStyleSnapshot, error)
	RecordPictureStyle(string, []string, string, *models.PictureStyleSample) error
}
type Preferences interface {
	List(string) ([]models.ContentPreference, error)
}
type Service struct {
	Settings    Settings
	Preferences Preferences
}

type Status struct {
	ProfileID       string `json:"profileId"`
	Supported       bool   `json:"supported"`
	Enabled         bool   `json:"enabled"`
	MinimumSamples  int    `json:"minimumSamples"`
	MaxSamples      int    `json:"maxSamples"`
	PosterSamples   int    `json:"posterSamples"`
	BackdropSamples int    `json:"backdropSamples"`
}

func SnapshotStatus(profile string, state models.PictureStyleSnapshot) Status {
	out := Status{ProfileID: profile, Supported: true, Enabled: state.Enabled, MinimumSamples: models.MinPictureStyleSamples, MaxSamples: models.MaxPictureStyleSamples}
	for _, sample := range state.Samples {
		if sample.Kind == "poster" {
			out.PosterSamples++
		} else if sample.Kind == "backdrop" {
			out.BackdropSamples++
		}
	}
	return out
}
func (s *Service) Status(profile string) (Status, error) {
	state, err := s.Settings.GetPictureStyle(profile)
	return SnapshotStatus(profile, state), err
}
func (s *Service) Update(profile string, enabled *bool, reset bool) (Status, error) {
	state, err := s.Settings.UpdatePictureStyle(profile, enabled, reset)
	return SnapshotStatus(profile, state), err
}

func descriptor(image *models.Image) (models.PictureStyleSample, bool) {
	if image == nil {
		return models.PictureStyleSample{}, false
	}
	code := language.NormalizeToCode(image.Language)
	if image.IsTextless {
		if strings.TrimSpace(image.Language) != "" {
			return models.PictureStyleSample{}, false
		}
		return models.PictureStyleSample{Textless: true}, true
	}
	if code != "" {
		return models.PictureStyleSample{Language: code}, true
	}
	// A missing false/empty field is not proof that an image has text or is dark.
	return models.PictureStyleSample{}, false
}
func (s *Service) ObserveChoice(profile string, title models.Title, kind string, image *models.Image) error {
	keys := titleratings.Aliases(title)
	if len(keys) == 0 {
		return nil
	}
	value, known := descriptor(image)
	var sample *models.PictureStyleSample
	if known {
		value.TitleID = keys[0]
		value.Aliases = append([]string(nil), keys...)
		value.Kind = kind
		sample = &value
	}
	return s.Settings.RecordPictureStyle(profile, keys, kind, sample)
}

type learnedStyle struct {
	textless *bool
	language string
}

func learn(state models.PictureStyleSnapshot, kind string) learnedStyle {
	n, textless := 0, 0
	languages := map[string]int{}
	for _, sample := range state.Samples {
		if sample.Kind != kind {
			continue
		}
		n++
		if sample.Textless {
			textless++
		} else if sample.Language != "" {
			languages[sample.Language]++
		}
	}
	if n < models.MinPictureStyleSamples {
		return learnedStyle{}
	}
	out := learnedStyle{}
	if textless*3 >= n*2 {
		value := true
		out.textless = &value
	} else if (n-textless)*3 >= n*2 {
		value := false
		out.textless = &value
	}
	textCount := n - textless
	if textCount >= models.MinPictureStyleSamples {
		for code, count := range languages {
			if count*3 >= textCount*2 {
				out.language = code
			}
		}
	}
	return out
}
func score(style learnedStyle, image *models.Image) int {
	value, known := descriptor(image)
	if !known {
		return 0
	}
	out := 0
	if style.textless != nil {
		if value.Textless == *style.textless {
			out++
		} else {
			out--
		}
	}
	if style.language != "" && !value.Textless && value.Language != "" {
		if value.Language == style.language {
			out++
		} else {
			out--
		}
	}
	return out
}
func choose(style learnedStyle, original *models.Image, variants []models.Image) *models.Image {
	// There must be a real effective standard to preserve and preview. A missing
	// picture is metadata absence, not evidence for a learned image replacement.
	if original == nil || original.URL == "" {
		return original
	}
	type proof struct {
		language                   string
		textless, known, ambiguous bool
	}
	proofs := map[string]proof{}
	for _, image := range variants {
		if image.URL == "" {
			continue
		}
		value, known := descriptor(&image)
		next := proof{language: value.Language, textless: value.Textless, known: known}
		if previous, exists := proofs[image.URL]; exists {
			next.ambiguous = previous.ambiguous || !known || !previous.known || previous.language != next.language || previous.textless != next.textless
		}
		proofs[image.URL] = next
	}
	best := original
	bestScore := score(style, original)
	for _, image := range variants {
		evidence := proofs[image.URL]
		// The same URL is still the same displayed picture, even when two catalogue
		// entries describe it differently. Unknown/conflicting duplicates are neutral.
		if image.URL == "" || image.URL == original.URL || !evidence.known || evidence.ambiguous {
			continue
		}
		if value := score(style, &image); value > bestScore {
			copy := image
			best = &copy
			bestScore = value
		}
	}
	return best
}

// Annotate copies only response-local image pointers. Known current variants
// retain relative order; original primary/text choices are also preserved.
// Shared metadata is never changed.
func (s *Service) Annotate(profile string, titles []models.Title) []models.Title {
	return s.Annotator(profile)(titles)
}

// Annotator reads one bounded profile snapshot per response, including responses
// with several shelves. It never performs one database read per card or shelf.
func (s *Service) Annotator(profile string) func([]models.Title) []models.Title {
	unchanged := restoreDefaults
	if s == nil || s.Settings == nil || s.Preferences == nil || profile == "" {
		return unchanged
	}
	state, err := s.Settings.GetPictureStyle(profile)
	if err != nil || !state.Enabled {
		return unchanged
	}
	prefs, err := s.Preferences.List(profile)
	if err != nil {
		return unchanged
	}
	preferences := map[string]models.ContentPreference{}
	for _, value := range prefs {
		preferences[strings.ToLower(value.ContentID)] = value
		for _, alias := range titleratings.Aliases(models.Title{ID: value.ContentID, MediaType: value.ContentType}) {
			preferences[alias] = value
		}
	}
	posterStyle, backdropStyle := learn(state, "poster"), learn(state, "backdrop")
	return func(titles []models.Title) []models.Title {
		out := append([]models.Title(nil), restoreDefaults(titles)...)
		for i := range out {
			pref := preferences[strings.ToLower(strings.TrimSpace(out[i].ID))]
			for _, alias := range titleratings.Aliases(out[i]) {
				if value, ok := preferences[alias]; ok {
					// Preserve any explicit manual/default intent across verified provider aliases.
					if value.PosterURL != "" {
						pref.PosterURL = value.PosterURL
					}
					if value.BackdropURL != "" {
						pref.BackdropURL = value.BackdropURL
					}
					pref.PosterUseStandard = pref.PosterUseStandard || value.PosterUseStandard
					pref.BackdropUseStandard = pref.BackdropUseStandard || value.BackdropUseStandard
				}
			}
			if pref.PosterURL == "" && !pref.PosterUseStandard {
				variants := append([]models.Image(nil), out[i].Posters...)
				if out[i].Poster != nil {
					variants = append(variants, *out[i].Poster)
				}
				if out[i].TextPoster != nil {
					variants = append(variants, *out[i].TextPoster)
				}
				baseline := out[i].TextPoster
				if baseline == nil {
					baseline = out[i].Poster
				}
				selected := choose(posterStyle, baseline, variants)
				if selected != baseline {
					standard := out[i].TextPoster
					if standard == nil {
						standard = out[i].Poster
					}
					if standard != nil {
						copy := *standard
						out[i].StandardPoster = &copy
					}
					copy := *selected
					out[i].Posters = gallery(out[i].Poster, out[i].TextPoster, out[i].Posters)
					out[i].Poster = &copy
					out[i].TextPoster = &copy
				}
			}
			if pref.BackdropURL == "" && !pref.BackdropUseStandard {
				variants := append([]models.Image(nil), out[i].Backdrops...)
				if out[i].TextBackdrop != nil {
					variants = append(variants, *out[i].TextBackdrop)
				}
				selected := choose(backdropStyle, out[i].Backdrop, variants)
				if selected != out[i].Backdrop {
					if standard := out[i].Backdrop; standard != nil {
						copy := *standard
						out[i].StandardBackdrop = &copy
					}
					copy := *selected
					out[i].Backdrops = gallery(out[i].Backdrop, out[i].TextBackdrop, out[i].Backdrops)
					out[i].Backdrop = &copy
					out[i].TextBackdrop = &copy
				}
			}
		}
		if titles != nil && out == nil {
			return []models.Title{}
		}
		return out
	}
}

// A repeated projection may receive a previously decorated response. Explicit
// standard intent or disabled learning restores its actual stored defaults and
// clears the optional automatic-preview fields without altering source objects.
func restoreDefaults(titles []models.Title) []models.Title {
	var out []models.Title
	for i, title := range titles {
		if title.StandardPoster == nil && title.StandardBackdrop == nil {
			continue
		}
		if out == nil {
			out = append([]models.Title(nil), titles...)
		}
		if title.StandardPoster != nil {
			copy := *title.StandardPoster
			out[i].Poster = &copy
			out[i].TextPoster = &copy
			out[i].StandardPoster = nil
		}
		if title.StandardBackdrop != nil {
			copy := *title.StandardBackdrop
			out[i].Backdrop = &copy
			out[i].TextBackdrop = &copy
			out[i].StandardBackdrop = nil
		}
	}
	if out == nil {
		return titles
	}
	return out
}

// Original primary/text pictures precede existing options so bounded client
// galleries still expose them. The existing variants retain relative order.
func gallery(primary, text *models.Image, variants []models.Image) []models.Image {
	seen := map[string]bool{}
	out := make([]models.Image, 0, len(variants)+2)
	add := func(value *models.Image) {
		if value != nil && value.URL != "" && !seen[value.URL] {
			seen[value.URL] = true
			out = append(out, *value)
		}
	}
	add(primary)
	add(text)
	for _, image := range variants {
		add(&image)
	}
	return out
}
