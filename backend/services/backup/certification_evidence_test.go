package backup

import "testing"

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
