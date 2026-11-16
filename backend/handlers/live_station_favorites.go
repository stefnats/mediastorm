package handlers

import "strings"

// filterStationFavoriteCategories consumes already tagged entries. Station
// membership has been resolved before this reduction; it never discovers or
// probes a source. Invalid/empty category selections retain legacy semantics.
func filterStationFavoriteCategories(channels []LiveChannel, categories, favoriteIDs []string) []LiveChannel {
	if len(channels) == 0 || len(categories) == 0 {
		return channels
	}
	selected := make(map[string]bool, len(categories))
	for _, category := range categories {
		selected[strings.TrimSpace(category)] = true
	}
	valid := false
	for _, channel := range channels {
		if selected[channel.Group] {
			valid = true
			break
		}
	}
	if !valid {
		return channels
	}
	favorites := make(map[string]bool, len(favoriteIDs))
	for _, id := range favoriteIDs {
		favorites[id] = true
	}
	result := make([]LiveChannel, 0, len(channels))
	for _, channel := range channels {
		if selected[channel.Group] || favorites[channel.ID] {
			result = append(result, channel)
		}
	}
	return result
}

// Keep the configured source count cap, while selecting eligible favorites
// before that cap for opted-in clients. Hidden and disallowed entries have
// already been removed; this cannot grant access to a new source or category.
func prioritizeStationFavoriteChannels(channels []LiveChannel, favoriteIDs []string, favoritesOnly bool) []LiveChannel {
	favorites := make(map[string]struct{}, len(favoriteIDs))
	for _, id := range favoriteIDs {
		favorites[id] = struct{}{}
	}
	if !favoritesOnly {
		return orderFavoriteChannelsFirst(channels, favorites)
	}
	result := make([]LiveChannel, 0, len(channels))
	for _, channel := range channels {
		if _, ok := favorites[channel.ID]; ok {
			result = append(result, channel)
		}
	}
	return result
}
