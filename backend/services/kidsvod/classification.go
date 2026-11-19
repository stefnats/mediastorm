package kidsvod

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

// VerifiedClassifications is an explicitly configured, server-owned cache reader.
// It must independently verify the authority and exact played-edition/episode
// mapping. TMDB metadata, caller DTOs and filename matching cannot implement this
// contract. No reader is installed by this package and reads initiate no provider work.
type VerifiedClassifications interface {
	ResolveVerifiedCacheOnly(context.Context, Subject, Binding) (*VerifiedRegionalClassification, error)
}

// RegionalClassificationFacts are server construction inputs, never a wire DTO.
// MappingRevision identifies the immutable trusted file-to-edition/episode proof;
// AuthorityRevision and ProvenanceRevision identify the exact authority record.
type RegionalClassificationFacts struct {
	Resource                                                            ResourceRef
	CatalogID, MediaType, EditionID                                     string
	Episode                                                             *models.EpisodeReference
	SourceVersion, BindingRevision                                      string
	Country, Scheme                                                     string
	Age                                                                 int
	AuthorityID, AuthorityRevision, ProvenanceRevision, MappingRevision string
	ObservedAt, ExpiresAt                                               time.Time
	Complete, Conflicting                                               bool
}

func (RegionalClassificationFacts) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (f *RegionalClassificationFacts) UnmarshalJSON([]byte) error {
	*f = RegionalClassificationFacts{}
	return ErrOpaqueCapability
}

// VerifiedRegionalClassification is immutable after server construction and has
// no importing or wire authority. Only the configured reader's output is consulted.
type VerifiedRegionalClassification struct{ facts RegionalClassificationFacts }

func (VerifiedRegionalClassification) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (v *VerifiedRegionalClassification) UnmarshalJSON([]byte) error {
	*v = VerifiedRegionalClassification{}
	return ErrOpaqueCapability
}
func (VerifiedRegionalClassification) String() string { return "verified regional classification" }

func copyBinding(b Binding) Binding {
	b.Episode = copyEpisode(b.Episode)
	b.CertificationEvidence = models.CopyCertificationEvidence(b.CertificationEvidence)
	return b
}
func copyEpisode(ep *models.EpisodeReference) *models.EpisodeReference {
	if ep == nil {
		return nil
	}
	result := *ep
	if ep.Numbering != nil {
		numbering := *ep.Numbering
		result.Numbering = &numbering
	}
	return &result
}
func exactEpisode(ep *models.EpisodeReference) bool {
	if ep == nil || ep.Numbering == nil || !catalogID(ep.Numbering.SeriesID, "series") || !token(ep.Numbering.Ordering, 128) || ep.SeasonNumber < 0 || ep.SeasonNumber > 10000 || ep.EpisodeNumber <= 0 || ep.EpisodeNumber > 100000 {
		return false
	}
	parts := strings.Split(ep.EpisodeID, ":")
	if len(parts) != 3 || (parts[0] != "tmdb" && parts[0] != "tvdb") || parts[1] != "episode" {
		return false
	}
	n, err := strconv.ParseUint(parts[2], 10, 64)
	return err == nil && n > 0 && strconv.FormatUint(n, 10) == parts[2]
}
func (f RegionalClassificationFacts) validFor(b Binding, now time.Time) bool {
	if !validBinding(b, now) || f.Resource != b.Resource || f.CatalogID != b.CatalogID || f.MediaType != b.MediaType || f.SourceVersion != b.SourceVersion || f.BindingRevision != b.Revision || f.Country != "DE" || f.Scheme != "FSK" || !validAge(&f.Age) || !f.Complete || f.Conflicting {
		return false
	}
	for _, revision := range []string{f.AuthorityID, f.AuthorityRevision, f.ProvenanceRevision, f.MappingRevision} {
		if !token(revision, 256) {
			return false
		}
	}
	if f.ObservedAt.IsZero() || f.ObservedAt.After(now) || now.Sub(f.ObservedAt) > MaxBindingTTL || !f.ExpiresAt.After(now) || !f.ExpiresAt.After(f.ObservedAt) || f.ExpiresAt.Sub(f.ObservedAt) > MaxBindingTTL {
		return false
	}
	if b.MediaType == "movie" {
		return token(b.EditionID, 256) && f.EditionID == b.EditionID && f.Episode == nil
	}
	return f.EditionID == "" && b.EditionID == "" && exactEpisode(b.Episode) && exactEpisode(f.Episode) && reflect.DeepEqual(b.Episode, f.Episode)
}

// NewVerifiedRegionalClassification is for trusted server adapters only. Creating
// an object does not grant access; the configured reader, binding, current policy
// and observation lifetime are checked again by Service on every use.
func NewVerifiedRegionalClassification(binding Binding, facts RegionalClassificationFacts) (*VerifiedRegionalClassification, error) {
	facts.Episode = copyEpisode(facts.Episode)
	at := facts.ObservedAt
	if binding.ObservedAt.After(at) {
		at = binding.ObservedAt
	}
	if !facts.validFor(binding, at) {
		return nil, errors.New("invalid verified regional classification")
	}
	return &VerifiedRegionalClassification{facts: facts}, nil
}

func (v *VerifiedRegionalClassification) fingerprint() ([]byte, error) {
	f := v.facts
	// Explicit private encoding avoids turning the opaque construction type into
	// a JSON capability or silently hashing its deliberately null marshaling.
	return json.Marshal([]any{f.Resource.kind, f.Resource.id, f.Resource.part, f.CatalogID, f.MediaType, f.EditionID, f.Episode, f.SourceVersion, f.BindingRevision, f.Country, f.Scheme, f.Age, f.AuthorityID, f.AuthorityRevision, f.ProvenanceRevision, f.MappingRevision, f.ObservedAt, f.ExpiresAt, f.Complete, f.Conflicting})
}
