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
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"novastream/models"
)

const similarSnapshotMaxBytes = 4 << 20
const similarSnapshotMaxCandidates = 4096
const similarSnapshotMaxTTL = 24 * time.Hour

var ErrSimilarSnapshotUnavailable = errors.New("similar snapshot unavailable")

// CachedSimilarSnapshot records the finite set returned by one normal Similar
// generation. It makes no claim about omitted upstream pages or playback.
type CachedSimilarSnapshot struct {
	Version          int                      `json:"version"`
	Provider         string                   `json:"provider"`
	MediaType        string                   `json:"mediaType"`
	TMDBID           int64                    `json:"tmdbId"`
	MetadataLanguage string                   `json:"metadataLanguage"`
	ObservedAt       time.Time                `json:"observedAt"`
	ExpiresAt        time.Time                `json:"expiresAt"`
	Candidates       []CachedSimilarCandidate `json:"candidates"`
}

// CachedSimilarCandidate contains only the normal result's primary identity,
// kind-bound numeric identity and stored name, in the original returned order.
type CachedSimilarCandidate struct {
	ID     string `json:"id"`
	TMDBID int64  `json:"tmdbId"`
	Name   string `json:"name"`
}

// CachedSimilarMetadataLanguage exposes the existing effective TMDB locale
// without loading or writing settings, cloning a service or contacting a provider.
func (s *Service) CachedSimilarMetadataLanguage() string {
	if s == nil || s.tmdb == nil {
		return ""
	}
	return collectionMetadataLanguage(s.tmdb)
}

func similarSnapshotKey(kind string, id int64, language string) string {
	return cacheKey("tmdb", "similar_snapshot", "v1", kind, strconv.FormatInt(id, 10), language)
}

func validSimilarCandidate(candidate CachedSimilarCandidate, kind string) bool {
	if candidate.TMDBID <= 0 || !utf8.ValidString(candidate.Name) || !collectionPlainText(candidate.Name, 1024, true) {
		return false
	}
	parts := strings.Split(candidate.ID, ":")
	if len(parts) != 3 || parts[0] != "tmdb" || parts[2] != strconv.FormatInt(candidate.TMDBID, 10) {
		return false
	}
	return kind == "movie" && parts[1] == "movie" || kind == "series" && (parts[1] == "tv" || parts[1] == "series")
}

func validSimilarSnapshot(value *CachedSimilarSnapshot, kind string, id int64, language string, at time.Time, baseTTL time.Duration) bool {
	if value == nil || (kind != "movie" && kind != "series") || id <= 0 || at.IsZero() || baseTTL <= 0 || value.Version != 1 || value.Provider != "tmdb" || value.MediaType != kind || value.TMDBID != id || value.MetadataLanguage != language || !collectionPlainText(language, 32, false) || value.Candidates == nil || len(value.Candidates) > similarSnapshotMaxCandidates {
		return false
	}
	if value.ObservedAt.IsZero() || value.ObservedAt.After(at) || !value.ExpiresAt.After(at) || !value.ExpiresAt.After(value.ObservedAt) || value.ExpiresAt.Sub(value.ObservedAt) > similarSnapshotMaxTTL || value.ExpiresAt.Sub(value.ObservedAt) > baseTTL {
		return false
	}
	seen := make(map[int64]bool, len(value.Candidates))
	for _, candidate := range value.Candidates {
		if !validSimilarCandidate(candidate, kind) || seen[candidate.TMDBID] {
			return false
		}
		seen[candidate.TMDBID] = true
	}
	return true
}

// recordSimilarSnapshot runs only at the successful final normal return. Legacy
// cache hits never reach it. Invalid or cancelled observations leave old bytes
// and expiry intact, while the ordinary Similar result remains unchanged.
func (s *Service) recordSimilarSnapshot(ctx context.Context, kind string, id int64, titles []models.Title, at time.Time) {
	if ctx == nil || ctx.Err() != nil || s == nil || s.cache == nil || s.tmdb == nil || s.cache.ttl <= 0 || len(titles) > similarSnapshotMaxCandidates {
		return
	}
	ttl := s.cache.ttl
	if ttl > similarSnapshotMaxTTL {
		ttl = similarSnapshotMaxTTL
	}
	value := &CachedSimilarSnapshot{Version: 1, Provider: "tmdb", MediaType: kind, TMDBID: id, MetadataLanguage: s.CachedSimilarMetadataLanguage(), ObservedAt: at.UTC(), ExpiresAt: at.UTC().Add(ttl), Candidates: make([]CachedSimilarCandidate, 0, len(titles))}
	for _, title := range titles {
		if title.MediaType != kind {
			return
		}
		value.Candidates = append(value.Candidates, CachedSimilarCandidate{ID: title.ID, TMDBID: title.TMDBID, Name: title.Name})
	}
	if !validSimilarSnapshot(value, kind, id, value.MetadataLanguage, at, s.cache.ttl) {
		return
	}
	// fileCache.set indents its output and appends a newline; bound the actual
	// representation rather than only a smaller compact encoding.
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil || len(encoded)+1 > similarSnapshotMaxBytes || ctx.Err() != nil {
		return
	}
	_ = s.cache.set(similarSnapshotKey(kind, id, value.MetadataLanguage), value)
}

// GetCachedSimilarSnapshot reads only the separate bounded observation file.
// It neither promotes legacy lists nor creates directories, deletes expired
// files, refreshes timestamps, loads settings or contacts providers.
func (s *Service) GetCachedSimilarSnapshot(kind string, id int64, at time.Time) (*CachedSimilarSnapshot, error) {
	if s == nil || s.cache == nil || s.tmdb == nil || (kind != "movie" && kind != "series") || id <= 0 || at.IsZero() || s.cache.ttl <= 0 {
		return nil, ErrSimilarSnapshotUnavailable
	}
	language := s.CachedSimilarMetadataLanguage()
	file, err := os.OpenFile(filepath.Join(s.cache.dir, similarSnapshotKey(kind, id, language)+".json"), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrSimilarSnapshotUnavailable
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < 1 || stat.Size() > similarSnapshotMaxBytes {
		return nil, ErrSimilarSnapshotUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(file, similarSnapshotMaxBytes+1))
	if err != nil || len(raw) > similarSnapshotMaxBytes || !utf8.Valid(raw) {
		return nil, ErrSimilarSnapshotUnavailable
	}
	fields, ok := uniqueCollectionObject(raw)
	if !ok || len(fields) != 8 {
		return nil, ErrSimilarSnapshotUnavailable
	}
	for _, field := range []string{"version", "provider", "mediaType", "tmdbId", "metadataLanguage", "observedAt", "expiresAt", "candidates"} {
		if _, exists := fields[field]; !exists {
			return nil, ErrSimilarSnapshotUnavailable
		}
	}
	var candidates []json.RawMessage
	if json.Unmarshal(fields["candidates"], &candidates) != nil || candidates == nil || len(candidates) > similarSnapshotMaxCandidates {
		return nil, ErrSimilarSnapshotUnavailable
	}
	for _, candidate := range candidates {
		fields, ok := uniqueCollectionObject(candidate)
		if !ok || len(fields) != 3 {
			return nil, ErrSimilarSnapshotUnavailable
		}
		for _, field := range []string{"id", "tmdbId", "name"} {
			if _, exists := fields[field]; !exists {
				return nil, ErrSimilarSnapshotUnavailable
			}
		}
		name := bytes.TrimSpace(fields["name"])
		if len(name) == 0 || name[0] != '"' {
			return nil, ErrSimilarSnapshotUnavailable
		}
	}
	var value CachedSimilarSnapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF || !validSimilarSnapshot(&value, kind, id, language, at, s.cache.ttl) {
		return nil, ErrSimilarSnapshotUnavailable
	}
	return &value, nil
}

// Titles returns an independent primary-only projection. A cached name or
// numeric TMDB ID supplies no original language, artwork, age or file claims.
func (s *CachedSimilarSnapshot) Titles() []models.Title {
	if s == nil {
		return nil
	}
	titles := make([]models.Title, 0, len(s.Candidates))
	for _, candidate := range s.Candidates {
		titles = append(titles, models.Title{ID: candidate.ID, TMDBID: candidate.TMDBID, Name: candidate.Name, MediaType: s.MediaType})
	}
	return titles
}
