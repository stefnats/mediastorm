package badstreams

import (
	"novastream/models"
	"strings"
)

func reportTitleIdentity(id string) string {
	return models.CanonicalSeriesIdentity(strings.ToLower(strings.TrimSpace(id)))
}

// Existing persisted keys retain exact ordering and opaque owners. Only known
// same-provider numeric series aliases may identify the same report scope.
func reportNumberingIdentity(key string) string {
	separator := strings.LastIndexByte(key, '|')
	if separator < 0 {
		return key
	}
	return models.CanonicalSeriesIdentity(key[:separator]) + key[separator:]
}
