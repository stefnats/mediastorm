package backup

import (
	"encoding/json"
	"novastream/models"
)

func decodeDatabaseExport(data []byte) (databaseExport, error) {
	var export databaseExport
	if err := json.Unmarshal(data, &export); err != nil {
		return export, err
	}
	// Uploaded backups may retain manual metadata but cannot manufacture a
	// provider's source identity or freshness. Normal trusted refreshes can
	// restore evidence later; importing data creates no metadata requests.
	for i := range export.LocalMediaItems {
		export.LocalMediaItems[i].Metadata = models.WithoutCertificationEvidence(export.LocalMediaItems[i].Metadata)
	}
	return export, nil
}
