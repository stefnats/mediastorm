package localmedia

import (
	"encoding/json"
	"testing"
	"time"

	"novastream/models"
)

func TestCertificationEvidencePersistsInLocalMetadataAndSurvivesGrouping(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	item := models.LocalMediaItem{ID: "synthetic", Metadata: &models.Title{ID: "tmdb:movie:123", Certification: "PG", CertificationEvidence: []models.CertificationEvidence{{Country: "DE", Value: "6", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:123", Scope: "movie_release", RetrievedAt: at, ExpiresAt: &expires}}}}
	data, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	var restored models.LocalMediaItem
	if json.Unmarshal(data, &restored) != nil {
		t.Fatal("metadata roundtrip failed")
	}
	group := models.LocalMediaItemGroup{}
	updateLocalMediaGroupSummary(&group, restored)
	updateLocalMediaGroupSummary(&group, restored)
	if group.Certification != "PG" || len(group.CertificationEvidence) != 1 || !group.CertificationEvidence[0].RetrievedAt.Equal(at) {
		t.Fatal("grouping dropped, duplicated or renewed evidence")
	}
	*group.CertificationEvidence[0].ExpiresAt = at
	if !restored.Metadata.CertificationEvidence[0].ExpiresAt.Equal(expires) {
		t.Fatal("group shared mutable metadata expiry")
	}
	legacy := models.LocalMediaItem{Metadata: &models.Title{Certification: "PG"}}
	group = models.LocalMediaItemGroup{}
	updateLocalMediaGroupSummary(&group, legacy)
	if group.Certification != "PG" || group.CertificationEvidence != nil {
		t.Fatal("unqualified local rating acquired provenance")
	}
}

func TestCertificationEvidenceManualMatchRejectsForgedProviderFieldsBeforePersistence(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "lib1", CreatedAt: now, UpdatedAt: now}, items: map[string]*models.LocalMediaItem{"synthetic.mkv": {ID: "synthetic", LibraryID: "lib1", RelativePath: "synthetic.mkv", CreatedAt: now, UpdatedAt: now}}}
	s := &Service{repo: repo}
	var input models.LocalMediaMatchInput
	if json.Unmarshal([]byte(`{"matchedTitleId":"tmdb:movie:123","matchedName":"Manual title","metadata":{"id":"tmdb:movie:123","name":"Manual title","certification":"PG","certificationEvidence":[{"country":"DE","value":"0","source":"tmdb","sourceType":"metadata","sourceTitleId":"tmdb:movie:123","scope":"movie_release","retrievedAt":"2099-01-01T00:00:00Z","expiresAt":"2100-01-01T00:00:00Z"}]}}`), &input) != nil {
		t.Fatal("fixture invalid")
	}
	got, err := s.UpdateItemMatch(t.Context(), "synthetic", input)
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := repo.GetItem(t.Context(), "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata.Certification != "PG" || got.Metadata.Name != "Manual title" || persisted.Metadata.CertificationEvidence != nil {
		t.Fatal("caller manufactured persisted provider provenance")
	}
	if len(input.Metadata.CertificationEvidence) != 1 {
		t.Fatal("sanitizing caller metadata mutated its shared input")
	}
}

func TestOriginalLanguageManualMatchCannotCreateProviderProof(t *testing.T) {
	now := time.Now().UTC()
	repo := &fakeLocalMediaRepo{library: &models.LocalMediaLibrary{ID: "lib1", CreatedAt: now, UpdatedAt: now}, items: map[string]*models.LocalMediaItem{"synthetic.mkv": {ID: "synthetic", LibraryID: "lib1", RelativePath: "synthetic.mkv", CreatedAt: now, UpdatedAt: now}}}
	service := &Service{repo: repo}
	title := &models.Title{ID: "tmdb:movie:41", TMDBID: 41, MediaType: "movie", Name: "Manual title", Language: "de", OriginalLanguageEvidence: &models.OriginalLanguageEvidence{Language: "fr", Source: "tmdb", SourceTitleID: "tmdb:movie:41", ObservedAt: now, ExpiresAt: now.Add(time.Hour)}}
	got, err := service.UpdateItemMatch(t.Context(), "synthetic", models.LocalMediaMatchInput{MatchedTitleID: title.ID, MatchedName: title.Name, Metadata: title})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := repo.GetItem(t.Context(), "synthetic")
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata.OriginalLanguageEvidence != nil || persisted.Metadata.OriginalLanguageEvidence != nil || got.Metadata.Language != "de" || title.OriginalLanguageEvidence == nil {
		t.Fatal("manual match forged provenance or mutated input")
	}
}
