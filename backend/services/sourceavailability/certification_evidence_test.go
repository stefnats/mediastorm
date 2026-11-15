package sourceavailability

import (
	"testing"
	"time"

	"novastream/models"
)

func TestCertificationEvidenceDeliveredProjectionPreservesDefensiveProvenance(t *testing.T) {
	s, _, _ := availabilityFixture(t)
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	title := models.Title{ID: "tmdb:movie:123", Name: "Synthetic", MediaType: "movie", Certification: "PG", CertificationEvidence: []models.CertificationEvidence{{Country: "DE", Value: "6", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:123", Scope: "movie_release", RetrievedAt: at, ExpiresAt: &expires}}}
	s.AnnotateTitles("profile", "tv", []models.Title{title})
	title.CertificationEvidence[0].Value = "mutated"
	coordinates := []TitleCoordinate{{ID: title.ID, MediaType: "movie"}}
	got := s.ResolveDeliveredTitles("profile", "tv", coordinates)
	if len(got) != 1 || len(got[0].CertificationEvidence) != 1 || got[0].CertificationEvidence[0].Value != "6" || !got[0].CertificationEvidence[0].RetrievedAt.Equal(at) {
		t.Fatal("delivered metadata lost or shared evidence")
	}
	got[0].CertificationEvidence[0].Value = "again"
	*got[0].CertificationEvidence[0].ExpiresAt = at
	got = s.ResolveDeliveredTitles("profile", "tv", coordinates)
	if got[0].CertificationEvidence[0].Value != "6" || !got[0].CertificationEvidence[0].ExpiresAt.Equal(expires) {
		t.Fatal("resolved projection shared mutable evidence")
	}
}
