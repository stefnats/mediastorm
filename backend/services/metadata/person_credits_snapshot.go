package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
	"unicode/utf8"

	"novastream/models"
)

const personCreditsSnapshotMaxBytes = 4 << 20
const personCreditsSnapshotMaxCandidates = 4096
const personCreditsSnapshotMaxTTL = 24 * time.Hour

var ErrTMDBPersonCreditsSnapshotUnavailable = errors.New("tmdb person credits snapshot unavailable")

// CachedTMDBPersonCreditsSnapshot records unique title membership from the
// existing global API-key-only person cast operation, in first-appearance
// order. It makes no complete-filmography, role-count or playback claim.
type CachedTMDBPersonCreditsSnapshot struct {
	Version          int                                `json:"version"`
	Provider         string                             `json:"provider"`
	SourceType       string                             `json:"sourceType"`
	PersonID         int64                              `json:"personId"`
	MetadataLanguage string                             `json:"metadataLanguage"`
	ObservedAt       time.Time                          `json:"observedAt"`
	ExpiresAt        time.Time                          `json:"expiresAt"`
	Candidates       []CachedTMDBPersonCreditsCandidate `json:"candidates"`
}

type CachedTMDBPersonCreditsCandidate struct {
	ID        string `json:"id"`
	MediaType string `json:"mediaType"`
	TMDBID    int64  `json:"tmdbId"`
	Name      string `json:"name"`
}

func (s *Service) CachedTMDBPersonCreditsMetadataLanguage() string {
	if s == nil || s.tmdb == nil {
		return ""
	}
	return collectionMetadataLanguage(s.tmdb)
}

func personCreditsSnapshotKey(id int64, language string) string {
	return cacheKey("tmdb", "person_credits_snapshot", "v1", strconv.FormatInt(id, 10), language)
}

func validTMDBPersonCreditsSnapshot(value *CachedTMDBPersonCreditsSnapshot, id int64, language string, at time.Time, baseTTL time.Duration) bool {
	if value == nil || id <= 0 || at.IsZero() || baseTTL <= 0 || value.Version != 1 || value.Provider != "tmdb" || value.SourceType != TMDBSourcePersonCredits || value.PersonID != id || value.MetadataLanguage != language || !collectionPlainText(language, 32, false) || value.Candidates == nil || len(value.Candidates) > personCreditsSnapshotMaxCandidates {
		return false
	}
	if value.ObservedAt.IsZero() || value.ObservedAt.After(at) || !value.ExpiresAt.After(at) || !value.ExpiresAt.After(value.ObservedAt) || value.ExpiresAt.Sub(value.ObservedAt) > personCreditsSnapshotMaxTTL || value.ExpiresAt.Sub(value.ObservedAt) > baseTTL {
		return false
	}
	seen := make(map[string]bool, len(value.Candidates))
	for _, candidate := range value.Candidates {
		if !validSimilarCandidate(CachedSimilarCandidate{ID: candidate.ID, TMDBID: candidate.TMDBID, Name: candidate.Name}, candidate.MediaType) {
			return false
		}
		identity := candidate.MediaType + ":" + strconv.FormatInt(candidate.TMDBID, 10)
		if seen[identity] {
			return false
		}
		seen[identity] = true
	}
	return true
}

// The only caller is the person branch after a successful combined-credits
// decode. Legacy shelf hits and director/crew operations never reach it.
// Missing/null cast is not empty. Every raw row is validated before normalized
// title membership is deduplicated, preserving its first exact primary label.
func (c *tmdbClient) recordTMDBPersonCreditsSnapshot(ctx context.Context, id int64, items []tmdbShelfTitle, at time.Time) {
	if ctx == nil || ctx.Err() != nil || c == nil || c.cache == nil || c.cache.ttl <= 0 || items == nil || len(items) > personCreditsSnapshotMaxCandidates {
		return
	}
	ttl := c.cache.ttl
	if ttl > personCreditsSnapshotMaxTTL {
		ttl = personCreditsSnapshotMaxTTL
	}
	value := &CachedTMDBPersonCreditsSnapshot{Version: 1, Provider: "tmdb", SourceType: TMDBSourcePersonCredits, PersonID: id, MetadataLanguage: collectionMetadataLanguage(c), ObservedAt: at.UTC(), ExpiresAt: at.UTC().Add(ttl), Candidates: make([]CachedTMDBPersonCreditsCandidate, 0, len(items))}
	seen := make(map[string]bool, len(items))
	for _, item := range items {
		kind, label := "movie", item.Title
		if item.MediaType == "tv" {
			kind, label = "series", item.Name
		} else if item.MediaType != "movie" {
			return
		}
		candidate := CachedTMDBPersonCreditsCandidate{ID: "tmdb:" + item.MediaType + ":" + strconv.FormatInt(item.ID, 10), MediaType: kind, TMDBID: item.ID, Name: label}
		if !validSimilarCandidate(CachedSimilarCandidate{ID: candidate.ID, TMDBID: candidate.TMDBID, Name: candidate.Name}, kind) {
			return
		}
		identity := kind + ":" + strconv.FormatInt(item.ID, 10)
		if seen[identity] {
			continue
		}
		seen[identity] = true
		value.Candidates = append(value.Candidates, candidate)
	}
	if !validTMDBPersonCreditsSnapshot(value, id, value.MetadataLanguage, at, c.cache.ttl) {
		return
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil || len(encoded)+1 > personCreditsSnapshotMaxBytes || ctx.Err() != nil {
		return
	}
	_ = c.cache.set(personCreditsSnapshotKey(id, value.MetadataLanguage), value)
}

// GetCachedTMDBPersonCreditsSnapshot reads the separate observation directly. It
// never creates directories, removes expired files, renews time or fetches data.
func (s *Service) GetCachedTMDBPersonCreditsSnapshot(id int64, at time.Time) (*CachedTMDBPersonCreditsSnapshot, error) {
	if s == nil || s.cache == nil || s.tmdb == nil || id <= 0 || at.IsZero() || s.cache.ttl <= 0 {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	language := s.CachedTMDBPersonCreditsMetadataLanguage()
	file, err := os.OpenFile(filepath.Join(s.cache.dir, personCreditsSnapshotKey(id, language)+".json"), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < 1 || stat.Size() > personCreditsSnapshotMaxBytes {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(file, personCreditsSnapshotMaxBytes+1))
	if err != nil || len(raw) > personCreditsSnapshotMaxBytes || !utf8.Valid(raw) {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	fields, ok := uniqueCollectionObject(raw)
	if !ok || len(fields) != 8 {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	for _, field := range []string{"version", "provider", "sourceType", "personId", "metadataLanguage", "observedAt", "expiresAt", "candidates"} {
		if _, exists := fields[field]; !exists {
			return nil, ErrTMDBPersonCreditsSnapshotUnavailable
		}
	}
	var candidates []json.RawMessage
	if json.Unmarshal(fields["candidates"], &candidates) != nil || candidates == nil || len(candidates) > personCreditsSnapshotMaxCandidates {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	for _, candidate := range candidates {
		fields, ok := uniqueCollectionObject(candidate)
		if !ok || len(fields) != 4 {
			return nil, ErrTMDBPersonCreditsSnapshotUnavailable
		}
		for _, field := range []string{"id", "mediaType", "tmdbId", "name"} {
			if _, exists := fields[field]; !exists {
				return nil, ErrTMDBPersonCreditsSnapshotUnavailable
			}
		}
		name := bytes.TrimSpace(fields["name"])
		if len(name) == 0 || name[0] != '"' {
			return nil, ErrTMDBPersonCreditsSnapshotUnavailable
		}
	}
	var value CachedTMDBPersonCreditsSnapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF || !validTMDBPersonCreditsSnapshot(&value, id, language, at, s.cache.ttl) {
		return nil, ErrTMDBPersonCreditsSnapshotUnavailable
	}
	return &value, nil
}

func (s *CachedTMDBPersonCreditsSnapshot) Titles() []models.Title {
	if s == nil {
		return nil
	}
	titles := make([]models.Title, 0, len(s.Candidates))
	for _, candidate := range s.Candidates {
		titles = append(titles, models.Title{ID: candidate.ID, MediaType: candidate.MediaType, TMDBID: candidate.TMDBID, Name: candidate.Name})
	}
	return titles
}
