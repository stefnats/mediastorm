package backup

import (
	"encoding/json"
	"errors"
	"novastream/models"
)

func decodeDatabaseExport(data []byte) (databaseExport, error) {
	var export databaseExport
	if err := json.Unmarshal(data, &export); err != nil {
		return export, err
	}
	seen := map[string]bool{}
	profiles := map[string]bool{}
	for _, u := range export.Users {
		profiles[u.ID] = true
	}
	for _, entry := range export.RegionalChildPolicies {
		if entry.ProfileID == "" || seen[entry.ProfileID] || !profiles[entry.ProfileID] || !entry.Policy.Valid() {
			return export, errors.New("invalid restored regional child policy")
		}
		seen[entry.ProfileID] = true
	}
	// Uploaded backups may retain manual metadata but cannot manufacture a
	// provider's source identity or freshness. Normal trusted refreshes can
	// restore evidence later; importing data creates no metadata requests.
	for i := range export.LocalMediaItems {
		export.LocalMediaItems[i].Metadata = models.WithoutProviderEvidence(export.LocalMediaItems[i].Metadata)
	}
	for i := range export.RemoteMediaItems {
		models.ClearRemoteSelectionProof(&export.RemoteMediaItems[i])
	}
	return export, nil
}
