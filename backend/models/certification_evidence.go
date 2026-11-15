package models

import "time"

const MaxCertificationEvidence = 256

// CertificationEvidence is a provider's regional metadata, not an official
// registry verification, minimum age or playback authorization. A movie release
// row does not identify the played edition; series metadata proves no episode's
// classification. Missing provenance or expiry must remain unknown to any
// future authorizer. Legacy certification strings cannot establish evidence.
// This transferable DTO is not a signed grant: a future authorizer must resolve
// trusted server metadata rather than trust caller-supplied copies of it.
type CertificationEvidence struct {
	Country       string     `json:"country"`
	Value         string     `json:"value"`
	Source        string     `json:"source"`     // tmdb
	SourceType    string     `json:"sourceType"` // metadata
	SourceTitleID string     `json:"sourceTitleId"`
	Scope         string     `json:"scope"` // movie_release, series
	RetrievedAt   time.Time  `json:"retrievedAt"`
	ExpiresAt     *time.Time `json:"expiresAt,omitempty"`
	ReleaseType   int        `json:"releaseType,omitempty"`
	ReleaseDate   string     `json:"releaseDate,omitempty"`
	Language      string     `json:"language,omitempty"`
	Note          string     `json:"note,omitempty"`
}

func CopyCertificationEvidence(values []CertificationEvidence) []CertificationEvidence {
	if values == nil {
		return nil
	}
	result := append([]CertificationEvidence{}, values...)
	for i := range result {
		if result[i].ExpiresAt != nil {
			t := *result[i].ExpiresAt
			result[i].ExpiresAt = &t
		}
	}
	return result
}

// WithoutCertificationEvidence isolates caller-supplied metadata from facts
// retrieved by the server. Existing manually assigned fields remain unchanged.
func WithoutCertificationEvidence(title *Title) *Title {
	if title == nil {
		return nil
	}
	copy := *title
	copy.CertificationEvidence = nil
	return &copy
}

// MergeCertificationEvidence preserves conflicting release rows and their
// original timestamps. Repeated copies never renew an observation's freshness.
func MergeCertificationEvidence(groups ...[]CertificationEvidence) []CertificationEvidence {
	var result []CertificationEvidence
	type identity struct {
		Country, Value, Source, SourceType, SourceTitleID, Scope, ReleaseDate, Language, Note string
		ReleaseType                                                                           int
	}
	seen := map[identity]int{}
	for _, group := range groups {
		for _, value := range group {
			id := identity{value.Country, value.Value, value.Source, value.SourceType, value.SourceTitleID, value.Scope, value.ReleaseDate, value.Language, value.Note, value.ReleaseType}
			if i, ok := seen[id]; ok {
				if value.RetrievedAt.After(result[i].RetrievedAt) {
					result[i] = CopyCertificationEvidence([]CertificationEvidence{value})[0]
				}
				continue
			}
			if len(result) >= MaxCertificationEvidence {
				continue
			}
			seen[id] = len(result)
			result = append(result, CopyCertificationEvidence([]CertificationEvidence{value})[0])
		}
	}
	return result
}
