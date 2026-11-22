package titleratings

import "novastream/models"

func copyArtwork(title models.Title) models.Title {
	copy := BareTitle(title)
	image := func(value *models.Image) *models.Image {
		if value == nil {
			return nil
		}
		result := *value
		return &result
	}
	copy.Poster, copy.TextPoster = image(title.Poster), image(title.TextPoster)
	copy.Backdrop, copy.TextBackdrop = image(title.Backdrop), image(title.TextBackdrop)
	copy.Posters = append([]models.Image(nil), title.Posters...)
	copy.Backdrops = append([]models.Image(nil), title.Backdrops...)
	return copy
}

// ResolveArtwork returns only image variants already delivered with this title.
// They share the ordinary catalogue projection's lifetime and are never written
// into the durable rating record or fetched while choosing a personal picture.
func (s *Service) ResolveArtwork(profile, client, kind, id string) (models.Title, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.delivered[projectionKey{profile, client, kind, id}]
	if !ok || (!s.now().Before(item.expires) || !s.now().Before(item.artworkExpires)) {
		return models.Title{}, false
	}
	return copyArtwork(item.artwork), true
}
