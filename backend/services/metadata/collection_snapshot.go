package metadata

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"novastream/models"
)

const collectionSnapshotMaxBytes = 4 << 20
const collectionSnapshotMaxMembers = 4096
const collectionSnapshotMaxTTL = 24 * time.Hour

var ErrCollectionSnapshotUnavailable = errors.New("collection snapshot unavailable")

// CachedCollectionSnapshot describes one complete normal provider observation.
// It is catalogue metadata, never a file, audio, age or playback permission.
type CachedCollectionSnapshot struct {
	Version          int                     `json:"version"`
	Provider         string                  `json:"provider"`
	CollectionID     int64                   `json:"collectionId"`
	Name             string                  `json:"name"`
	MetadataLanguage string                  `json:"metadataLanguage"`
	ObservedAt       time.Time               `json:"observedAt"`
	ExpiresAt        time.Time               `json:"expiresAt"`
	Movies           []CachedCollectionMovie `json:"movies"`
}

// CachedCollectionMovie retains only fields from that collection response.
// No assembled Title, scalar language, side IDs or imported evidence is used.
type CachedCollectionMovie struct {
	ID           int64  `json:"id"`
	Name         string `json:"name"`
	ReleaseDate  string `json:"releaseDate,omitempty"`
	PosterPath   string `json:"posterPath,omitempty"`
	BackdropPath string `json:"backdropPath,omitempty"`
}

func collectionMetadataLanguage(c *tmdbClient) string {
	if c == nil || strings.TrimSpace(c.language) == "" {
		return "en-US"
	}
	return normalizeLanguage(c.language)
}

func collectionSnapshotKey(id int64, language string) string {
	return cacheKey("tmdb", "collection_snapshot", "v1", strconv.FormatInt(id, 10), language)
}

type collectionReadFailure struct{ err error }

func (r collectionReadFailure) Read([]byte) (int, error) { return 0, r.err }

// collectionResponseForLegacy observes the SAME original body, without adding
// a provider request or imposing a new size rejection on the legacy decoder.
// Only an actual complete EOF within the bound can become a cache observation.
func collectionResponseForLegacy(body io.Reader) (io.Reader, []byte) {
	prefix, err := io.ReadAll(io.LimitReader(body, collectionSnapshotMaxBytes+1))
	if err != nil {
		return io.MultiReader(bytes.NewReader(prefix), collectionReadFailure{err}), nil
	}
	if len(prefix) > collectionSnapshotMaxBytes {
		return io.MultiReader(bytes.NewReader(prefix), body), nil
	}
	return bytes.NewReader(prefix), prefix
}

// uniqueCollectionObject rejects duplicate JSON keys instead of silently
// treating a later occurrence as a fresh or complete provider observation.
func uniqueCollectionObject(raw []byte) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, false
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, false
		}
		if _, exists := fields[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil, false
		}
		fields[key] = value
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') || decoder.Decode(new(any)) != io.EOF {
		return nil, false
	}
	return fields, true
}

func collectionPlainText(value string, max int, empty bool) bool {
	if len(value) > max || !empty && strings.TrimSpace(value) == "" {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
func collectionImagePath(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 1024 || !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") || strings.ContainsAny(value, "?#\\") || strings.Contains(value, "..") {
		return false
	}
	return collectionPlainText(value, 1024, false)
}
func collectionReleaseDate(value string) bool {
	if value == "" {
		return true
	}
	at, err := time.Parse("2006-01-02", value)
	return err == nil && at.Year() > 0 && at.Format("2006-01-02") == value
}

func validCollectionSnapshot(value *CachedCollectionSnapshot, id int64, language string, at time.Time) bool {
	if value == nil || value.Version != 1 || value.Provider != "tmdb" || id <= 0 || value.CollectionID != id || value.MetadataLanguage != language || !collectionPlainText(language, 32, false) || !collectionPlainText(value.Name, 1024, false) || value.Movies == nil || len(value.Movies) > collectionSnapshotMaxMembers {
		return false
	}
	if value.ObservedAt.IsZero() || value.ObservedAt.After(at) || !value.ExpiresAt.After(at) || !value.ExpiresAt.After(value.ObservedAt) || value.ExpiresAt.Sub(value.ObservedAt) > collectionSnapshotMaxTTL {
		return false
	}
	seen := map[int64]bool{}
	for _, movie := range value.Movies {
		if movie.ID <= 0 || seen[movie.ID] || !collectionPlainText(movie.Name, 1024, false) || !collectionReleaseDate(movie.ReleaseDate) || !collectionImagePath(movie.PosterPath) || !collectionImagePath(movie.BackdropPath) {
			return false
		}
		seen[movie.ID] = true
	}
	return true
}

// recordCollectionSnapshot runs only at the existing normal collection GET.
// Bad/partial/oversized observations never overwrite or renew a valid old fact.
func (c *tmdbClient) recordCollectionSnapshot(id int64, raw []byte, at time.Time) {
	if c == nil || c.cache == nil || c.cache.ttl <= 0 || id <= 0 || len(raw) == 0 || len(raw) > collectionSnapshotMaxBytes {
		return
	}
	fields, ok := uniqueCollectionObject(raw)
	if !ok {
		return
	}
	var actualID int64
	var name string
	if json.Unmarshal(fields["id"], &actualID) != nil || actualID != id || json.Unmarshal(fields["name"], &name) != nil {
		return
	}
	parts, present := fields["parts"]
	if !present || len(bytes.TrimSpace(parts)) == 0 || bytes.TrimSpace(parts)[0] != '[' {
		return
	}
	var members []json.RawMessage
	if json.Unmarshal(parts, &members) != nil || members == nil || len(members) > collectionSnapshotMaxMembers {
		return
	}
	ttl := collectionSnapshotMaxTTL
	if c.cache.ttl > 0 && c.cache.ttl < ttl {
		ttl = c.cache.ttl
	}
	value := &CachedCollectionSnapshot{Version: 1, Provider: "tmdb", CollectionID: id, Name: name, MetadataLanguage: collectionMetadataLanguage(c), ObservedAt: at.UTC(), ExpiresAt: at.UTC().Add(ttl), Movies: make([]CachedCollectionMovie, 0, len(members))}
	for _, raw := range members {
		part, ok := uniqueCollectionObject(raw)
		if !ok {
			return
		}
		var movie CachedCollectionMovie
		if json.Unmarshal(part["id"], &movie.ID) != nil || json.Unmarshal(part["title"], &movie.Name) != nil {
			return
		}
		for key, target := range map[string]*string{"release_date": &movie.ReleaseDate, "poster_path": &movie.PosterPath, "backdrop_path": &movie.BackdropPath} {
			if field, exists := part[key]; exists && string(field) != "null" {
				if json.Unmarshal(field, target) != nil {
					return
				}
			}
		}
		value.Movies = append(value.Movies, movie)
	}
	if !validCollectionSnapshot(value, id, value.MetadataLanguage, at) {
		return
	}
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > collectionSnapshotMaxBytes {
		return
	}
	_ = c.cache.set(collectionSnapshotKey(id, value.MetadataLanguage), value)
}

// GetCachedCollectionSnapshot performs only existing cache access/housekeeping.
// It does not warm, refresh metadata, or infer completion from an assembled Title.
func (s *Service) GetCachedCollectionSnapshot(id int64, at time.Time) (*CachedCollectionSnapshot, error) {
	if s == nil || s.cache == nil || s.tmdb == nil || id <= 0 || at.IsZero() {
		return nil, ErrCollectionSnapshotUnavailable
	}
	language := collectionMetadataLanguage(s.tmdb)
	file, err := os.OpenFile(filepath.Join(s.cache.dir, collectionSnapshotKey(id, language)+".json"), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrCollectionSnapshotUnavailable
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < 1 || stat.Size() > collectionSnapshotMaxBytes {
		return nil, ErrCollectionSnapshotUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(file, collectionSnapshotMaxBytes+1))
	if err != nil || len(raw) > collectionSnapshotMaxBytes {
		return nil, ErrCollectionSnapshotUnavailable
	}
	fields, ok := uniqueCollectionObject(raw)
	if !ok {
		return nil, ErrCollectionSnapshotUnavailable
	}
	var members []json.RawMessage
	if json.Unmarshal(fields["movies"], &members) != nil || members == nil || len(members) > collectionSnapshotMaxMembers {
		return nil, ErrCollectionSnapshotUnavailable
	}
	for _, movie := range members {
		if _, ok := uniqueCollectionObject(movie); !ok {
			return nil, ErrCollectionSnapshotUnavailable
		}
	}
	var value CachedCollectionSnapshot
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF || !validCollectionSnapshot(&value, id, language, at) || s.cache.ttl <= 0 || value.ExpiresAt.Sub(value.ObservedAt) > s.cache.ttl {
		return nil, ErrCollectionSnapshotUnavailable
	}
	return &value, nil
}

// Titles returns a fresh allowlisted projection, without scalar language or
// certification/source evidence. Building existing image URLs opens no image.
func (s *CachedCollectionSnapshot) Titles() []models.Title {
	if s == nil {
		return nil
	}
	titles := make([]models.Title, 0, len(s.Movies))
	for _, movie := range s.Movies {
		titles = append(titles, models.Title{ID: fmt.Sprintf("tmdb:movie:%d", movie.ID), TMDBID: movie.ID, Name: movie.Name, MediaType: "movie", Year: parseTMDBYear(movie.ReleaseDate, ""), ReleaseDate: models.NormalizeReleaseDate(movie.ReleaseDate), Status: models.MovieReleaseStatusFromReleaseDate(movie.ReleaseDate), Poster: buildTMDBImage(movie.PosterPath, tmdbPosterSize, "poster"), Backdrop: buildTMDBImage(movie.BackdropPath, tmdbBackdropSize, "backdrop")})
	}
	return titles
}
