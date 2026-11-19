package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// RegionalChildPolicy is configuration, never a classification or playback grant.
// The country/scheme version is independent from legacy US rating strings.
type RegionalChildPolicy struct {
	Version       int    `json:"version"`
	Country       string `json:"country"`
	MaxMovieAge   int    `json:"maxMovieAge"`
	MaxEpisodeAge int    `json:"maxEpisodeAge"`
}

func (p RegionalChildPolicy) Valid() bool {
	age := func(v int) bool { return v == 0 || v == 6 || v == 12 || v == 16 || v == 18 }
	return p.Version == 1 && p.Country == "DE" && age(p.MaxMovieAge) && age(p.MaxEpisodeAge)
}
func DecodeRegionalChildPolicy(raw []byte) (RegionalChildPolicy, error) {
	var p RegionalChildPolicy
	invalid := errors.New("invalid regional child policy")
	if len(raw) > 16<<10 {
		return p, invalid
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return p, invalid
	}
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		name, ok := key.(string)
		if err != nil || !ok || seen[name] {
			return RegionalChildPolicy{}, invalid
		}
		seen[name] = true
		var v json.RawMessage
		if d.Decode(&v) != nil || bytes.Equal(v, []byte("null")) {
			return RegionalChildPolicy{}, invalid
		}
		switch name {
		case "version":
			err = json.Unmarshal(v, &p.Version)
		case "country":
			err = json.Unmarshal(v, &p.Country)
		case "maxMovieAge":
			err = json.Unmarshal(v, &p.MaxMovieAge)
		case "maxEpisodeAge":
			err = json.Unmarshal(v, &p.MaxEpisodeAge)
		default:
			return RegionalChildPolicy{}, invalid
		}
		if err != nil {
			return RegionalChildPolicy{}, invalid
		}
	}
	if end, err := d.Token(); err != nil || end != json.Delim('}') {
		return RegionalChildPolicy{}, invalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF || len(seen) != 4 || !p.Valid() {
		return RegionalChildPolicy{}, invalid
	}
	return p, nil
}
func (p *RegionalChildPolicy) UnmarshalJSON(raw []byte) error {
	v, err := DecodeRegionalChildPolicy(raw)
	*p = v
	return err
}

// RegionalChildPolicyBackup carries configuration only. Database revisions and
// runtime classification/approval capabilities deliberately have no wire fields.
type RegionalChildPolicyBackup struct {
	ProfileID string              `json:"profileId"`
	Policy    RegionalChildPolicy `json:"policy"`
}
