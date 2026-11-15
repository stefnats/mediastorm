package backup

import (
	"encoding/json"
	"novastream/models"
	"testing"
)

func TestCertificationEvidenceDatabaseImportRejectsForgedProviderFreshness(t *testing.T) {
	data := []byte(`{"localMediaItems":[{"id":"synthetic","metadata":{"id":"tmdb:movie:123","name":"Manual title","certification":"PG","certificationEvidence":[{"country":"DE","value":"0","source":"tmdb","sourceType":"metadata","sourceTitleId":"tmdb:movie:123","scope":"movie_release","retrievedAt":"2099-01-01T00:00:00Z","expiresAt":"2100-01-01T00:00:00Z"}]}}]}`)
	export, err := decodeDatabaseExport(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(export.LocalMediaItems) != 1 || export.LocalMediaItems[0].Metadata.Certification != "PG" || export.LocalMediaItems[0].Metadata.Name != "Manual title" || export.LocalMediaItems[0].Metadata.CertificationEvidence != nil {
		t.Fatal("import accepted forged provenance or removed manual fields")
	}
}

func TestAutomaticRemoteSelectionImportRequiresOrdinaryResync(t *testing.T) {
	item := models.RemoteMediaItem{ID: "version", ExternalItemID: "item", ExternalMediaID: "part", GroupKey: "group", StreamPath: "plexmedia:version", FileName: "Manual title.mkv", SinglePart: true, SeriesExternalIDs: &models.LocalMediaExternalIDs{TMDB: "123"}, EpisodeExternalIDs: &models.LocalMediaExternalIDs{TVDB: "456"}, AudioLanguages: []string{"fra"}, ProviderData: map[string]string{"partKey": "/part/file"}}
	item.ProviderData = models.RemoteSelectionProviderData(&item)
	data, err := json.Marshal(databaseExport{RemoteMediaItems: []models.RemoteMediaItem{item}})
	if err != nil {
		t.Fatal(err)
	}
	export, err := decodeDatabaseExport(data)
	if err != nil || len(export.RemoteMediaItems) != 1 {
		t.Fatalf("decode: %v", err)
	}
	restored := export.RemoteMediaItems[0]
	models.HydrateRemoteSelectionProof(&restored)
	if restored.SinglePart || restored.SeriesExternalIDs != nil || restored.EpisodeExternalIDs != nil || len(restored.AudioLanguages) != 0 {
		t.Fatal("backup supplied provider identity proof")
	}
	if restored.StreamPath != item.StreamPath || restored.FileName != item.FileName || restored.ProviderData["partKey"] != item.ProviderData["partKey"] {
		t.Fatal("manual playback data was removed")
	}
}
