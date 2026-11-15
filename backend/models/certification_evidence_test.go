package models

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestCertificationEvidenceCopiesFreshnessAndKeepsConflictingEditions(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	expires := at.Add(time.Hour)
	a := CertificationEvidence{Country: "DE", Value: "6", Source: "tmdb", SourceType: "metadata", SourceTitleID: "tmdb:movie:123", Scope: "movie_release", ReleaseType: 3, RetrievedAt: at, ExpiresAt: &expires}
	b := a
	b.Value = "12"
	b.ReleaseType = 4
	b.Note = "Different provider edition"
	merged := MergeCertificationEvidence([]CertificationEvidence{a, b}, []CertificationEvidence{a})
	if len(merged) != 2 || !merged[0].RetrievedAt.Equal(at) || !merged[0].ExpiresAt.Equal(expires) {
		t.Fatalf("conflict or original freshness lost: %+v", merged)
	}
	copy := CopyCertificationEvidence(merged)
	*copy[0].ExpiresAt = at
	if !merged[0].ExpiresAt.Equal(expires) {
		t.Fatal("expiry pointer was shared")
	}
	data, _ := json.Marshal(Title{ID: "tmdb:movie:123", Certification: "PG", CertificationEvidence: merged})
	var decoded Title
	if json.Unmarshal(data, &decoded) != nil || len(decoded.CertificationEvidence) != 2 || decoded.Certification != "PG" {
		t.Fatal("title persistence lost additive evidence")
	}
	var old struct {
		Certification string `json:"certification"`
	}
	if json.Unmarshal(data, &old) != nil || old.Certification != "PG" {
		t.Fatal("old client scalar contract changed")
	}
	var legacy Title
	if json.Unmarshal([]byte(`{"id":"tmdb:movie:123","certification":"PG"}`), &legacy) != nil || legacy.CertificationEvidence != nil {
		t.Fatal("old scalar fabricated regional evidence")
	}
}

func TestCertificationEvidenceMergeIsBoundedWithoutInventingFreshness(t *testing.T) {
	values := make([]CertificationEvidence, MaxCertificationEvidence+10)
	for i := range values {
		values[i] = CertificationEvidence{Country: "DE", Value: fmt.Sprint(i)}
	}
	merged := MergeCertificationEvidence(values)
	if len(merged) != MaxCertificationEvidence || !merged[0].RetrievedAt.IsZero() || merged[0].ExpiresAt != nil {
		t.Fatal("merge was unbounded or invented retrieval/expiry")
	}
}
