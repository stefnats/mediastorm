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

const publicListSnapshotMaxBytes = 4 << 20
const publicListSnapshotMaxCandidates = 4096
const publicListSnapshotMaxTTL = 24 * time.Hour

var ErrTMDBPublicListSnapshotUnavailable = errors.New("tmdb public list snapshot unavailable")

// CachedTMDBPublicListSnapshot observes decoded items from the existing global
// API-key-only public-list operation. SourceType names that operation; it does
// not certify provider visibility, omitted pages or permission to play a title.
type CachedTMDBPublicListSnapshot struct {
	Version          int                             `json:"version"`
	Provider         string                          `json:"provider"`
	SourceType       string                          `json:"sourceType"`
	ListID           int64                           `json:"listId"`
	MetadataLanguage string                          `json:"metadataLanguage"`
	ObservedAt       time.Time                       `json:"observedAt"`
	ExpiresAt        time.Time                       `json:"expiresAt"`
	Candidates       []CachedTMDBPublicListCandidate `json:"candidates"`
}

type CachedTMDBPublicListCandidate struct {
	ID        string `json:"id"`
	MediaType string `json:"mediaType"`
	TMDBID    int64  `json:"tmdbId"`
	Name      string `json:"name"`
}

func (s *Service) CachedTMDBPublicListMetadataLanguage() string {
	if s == nil || s.tmdb == nil {
		return ""
	}
	return collectionMetadataLanguage(s.tmdb)
}

func publicListSnapshotKey(id int64, language string) string {
	return cacheKey("tmdb", "public_list_snapshot", "v1", strconv.FormatInt(id, 10), language)
}

func validTMDBPublicListSnapshot(value *CachedTMDBPublicListSnapshot, id int64, language string, at time.Time, baseTTL time.Duration) bool {
	if value == nil || id <= 0 || at.IsZero() || baseTTL <= 0 || value.Version != 1 || value.Provider != "tmdb" || value.SourceType != TMDBSourcePublicList || value.ListID != id || value.MetadataLanguage != language || !collectionPlainText(language, 32, false) || value.Candidates == nil || len(value.Candidates) > publicListSnapshotMaxCandidates {
		return false
	}
	if value.ObservedAt.IsZero() || value.ObservedAt.After(at) || !value.ExpiresAt.After(at) || !value.ExpiresAt.After(value.ObservedAt) || value.ExpiresAt.Sub(value.ObservedAt) > publicListSnapshotMaxTTL || value.ExpiresAt.Sub(value.ObservedAt) > baseTTL {
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

// The only caller is the successful normal /list decode. Legacy shelf cache
// hits, other source types and private account-list values never reach it.
// Missing/null items is not an empty observation. The raw primary label is
// preserved even when the legacy mapper would drop it or use a secondary label.
func (c *tmdbClient) recordTMDBPublicListSnapshot(ctx context.Context, id int64, items []tmdbShelfTitle, at time.Time) {
	if ctx == nil || ctx.Err() != nil || c == nil || c.cache == nil || c.cache.ttl <= 0 || items == nil || len(items) > publicListSnapshotMaxCandidates {
		return
	}
	ttl := c.cache.ttl
	if ttl > publicListSnapshotMaxTTL {
		ttl = publicListSnapshotMaxTTL
	}
	value := &CachedTMDBPublicListSnapshot{Version: 1, Provider: "tmdb", SourceType: TMDBSourcePublicList, ListID: id, MetadataLanguage: collectionMetadataLanguage(c), ObservedAt: at.UTC(), ExpiresAt: at.UTC().Add(ttl), Candidates: make([]CachedTMDBPublicListCandidate, 0, len(items))}
	for _, item := range items {
		kind, label := "movie", item.Title
		if item.MediaType == "tv" {
			kind, label = "series", item.Name
		} else if item.MediaType != "movie" {
			return
		}
		value.Candidates = append(value.Candidates, CachedTMDBPublicListCandidate{ID: "tmdb:" + item.MediaType + ":" + strconv.FormatInt(item.ID, 10), MediaType: kind, TMDBID: item.ID, Name: label})
	}
	if !validTMDBPublicListSnapshot(value, id, value.MetadataLanguage, at, c.cache.ttl) {
		return
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil || len(encoded)+1 > publicListSnapshotMaxBytes || ctx.Err() != nil {
		return
	}
	_ = c.cache.set(publicListSnapshotKey(id, value.MetadataLanguage), value)
}

// GetCachedTMDBPublicListSnapshot reads the separate observation directly. It
// never creates directories, removes expired files, renews time or fetches data.
func (s *Service) GetCachedTMDBPublicListSnapshot(id int64, at time.Time) (*CachedTMDBPublicListSnapshot, error) {
	if s == nil || s.cache == nil || s.tmdb == nil || id <= 0 || at.IsZero() || s.cache.ttl <= 0 {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	language := s.CachedTMDBPublicListMetadataLanguage()
	file, err := os.OpenFile(filepath.Join(s.cache.dir, publicListSnapshotKey(id, language)+".json"), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < 1 || stat.Size() > publicListSnapshotMaxBytes {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(file, publicListSnapshotMaxBytes+1))
	if err != nil || len(raw) > publicListSnapshotMaxBytes || !utf8.Valid(raw) {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	fields, ok := uniqueCollectionObject(raw)
	if !ok || len(fields) != 8 {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	for _, field := range []string{"version", "provider", "sourceType", "listId", "metadataLanguage", "observedAt", "expiresAt", "candidates"} {
		if _, exists := fields[field]; !exists {
			return nil, ErrTMDBPublicListSnapshotUnavailable
		}
	}
	var candidates []json.RawMessage
	if json.Unmarshal(fields["candidates"], &candidates) != nil || candidates == nil || len(candidates) > publicListSnapshotMaxCandidates {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	for _, candidate := range candidates {
		fields, ok := uniqueCollectionObject(candidate)
		if !ok || len(fields) != 4 {
			return nil, ErrTMDBPublicListSnapshotUnavailable
		}
		for _, field := range []string{"id", "mediaType", "tmdbId", "name"} {
			if _, exists := fields[field]; !exists {
				return nil, ErrTMDBPublicListSnapshotUnavailable
			}
		}
		name := bytes.TrimSpace(fields["name"])
		if len(name) == 0 || name[0] != '"' {
			return nil, ErrTMDBPublicListSnapshotUnavailable
		}
	}
	var value CachedTMDBPublicListSnapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF || !validTMDBPublicListSnapshot(&value, id, language, at, s.cache.ttl) {
		return nil, ErrTMDBPublicListSnapshotUnavailable
	}
	return &value, nil
}

func (s *CachedTMDBPublicListSnapshot) Titles() []models.Title {
	if s == nil {
		return nil
	}
	titles := make([]models.Title, 0, len(s.Candidates))
	for _, candidate := range s.Candidates {
		titles = append(titles, models.Title{ID: candidate.ID, MediaType: candidate.MediaType, TMDBID: candidate.TMDBID, Name: candidate.Name})
	}
	return titles
}
