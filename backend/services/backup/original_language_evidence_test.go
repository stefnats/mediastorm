package backup

import "testing"

func TestOriginalLanguageEvidenceImportStripsForgedProviderFacts(t *testing.T) {
	value, err := decodeDatabaseExport([]byte(`{"localMediaItems":[{"id":"synthetic","metadata":{"id":"tmdb:movie:41","name":"Manual name","mediaType":"movie","language":"de","originalLanguageEvidence":{"language":"fr","source":"tmdb","sourceTitleId":"tmdb:movie:41","observedAt":"2099-01-01T00:00:00Z","expiresAt":"2099-01-02T00:00:00Z"}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	title := value.LocalMediaItems[0].Metadata
	if title.OriginalLanguageEvidence != nil || title.Name != "Manual name" || title.Language != "de" {
		t.Fatal("import manufactured original-language proof or lost manual fields")
	}
}
