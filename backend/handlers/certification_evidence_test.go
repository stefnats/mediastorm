package handlers

import (
	"encoding/json"
	"testing"
	"time"

	"novastream/api/openapi"
	"novastream/models"
)

func TestCertificationEvidenceMovieHTTPAndStartupProjectionPreserveLegacyScalar(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	title := models.Title{ID: "tmdb:movie:123", Name: "Synthetic", MediaType: "movie", Certification: "PG", CertificationEvidence: []models.CertificationEvidence{{Country: "DE", Value: "6", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:123", Scope: "movie_release", RetrievedAt: at, ExpiresAt: &expires, ReleaseType: 4}}}
	h := &MetadataHandler{Service: &fakeMetadataService{movieResp: &title}}
	validator, err := openapi.NewValidator(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	response := exercise(t, validator, contractCall{method: "GET", target: "/api/metadata/movies/details?titleId=tmdb%3Amovie%3A123&userId=profile"}, h.MovieDetails, 200)
	got := decodeContract[models.Title](t, response)
	if got.Certification != "PG" || len(got.CertificationEvidence) != 1 || !got.CertificationEvidence[0].RetrievedAt.Equal(at) {
		t.Fatal("HTTP projection changed scalar or provenance")
	}
	var old struct {
		Certification string `json:"certification"`
	}
	if json.Unmarshal(response.Body.Bytes(), &old) != nil || old.Certification != "PG" {
		t.Fatal("old client response changed")
	}
	slim := slimTrendingItems([]models.TrendingItem{{Title: title}})
	if len(slim[0].Title.CertificationEvidence) != 1 {
		t.Fatal("startup projection lost evidence")
	}
	slim[0].Title.CertificationEvidence[0].Value = "mutated"
	if title.CertificationEvidence[0].Value != "6" {
		t.Fatal("startup projection shared evidence")
	}
}

func TestCertificationEvidenceChangeInvalidatesExistingLocalProofFingerprint(t *testing.T) {
	item := models.LocalMediaItem{ID: "synthetic", Metadata: &models.Title{Certification: "PG"}}
	before := localHintFingerprint(item)
	item.Metadata.CertificationEvidence = []models.CertificationEvidence{{Country: "DE", Value: "6", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:123", Scope: "movie_release", RetrievedAt: time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)}}
	if localHintFingerprint(item) == before {
		t.Fatal("changed provenance retained an old local hint proof")
	}
}

func TestCertificationEvidenceActualLocalGroupsHTTPProjectionMatchesComponentContract(t *testing.T) {
	svc, _ := newLocalGroupsFixture()
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	svc.groups.Groups[0].Certification = "PG"
	svc.groups.Groups[0].CertificationEvidence = []models.CertificationEvidence{{Country: "DE", Value: "6", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:123", Scope: "movie_release", RetrievedAt: at}}
	h := NewLocalMediaHandler(svc, fakeLocalMediaUsersProvider{}, false)
	response := localGroupsRequest(t, h, "lib1", "")
	if len(response.Groups) != 1 || len(response.Groups[0].CertificationEvidence) != 1 || response.Groups[0].Certification != "PG" {
		t.Fatal("local groups HTTP dropped populated provenance")
	}
	data, _ := json.Marshal(response.Groups[0])
	var object map[string]any
	_ = json.Unmarshal(data, &object)
	doc, err := openapi.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	schema := doc.Components.Schemas["LocalMediaCertificationProjection"].Value
	if err := schema.VisitJSON(object); err != nil {
		t.Fatalf("group response violates evidence contract: %v", err)
	}
	object["certificationEvidence"].([]any)[0].(map[string]any)["source"] = "official_fsk_registry"
	if schema.VisitJSON(object) == nil {
		t.Fatal("schema accepted an invented official authority")
	}
}
