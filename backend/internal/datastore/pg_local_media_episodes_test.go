package datastore

import (
	"encoding/json"
	"reflect"
	"testing"

	"novastream/models"
)

func TestLocalMediaMetadataKeepsFileMembershipOutOfTitle(t *testing.T) {
	title := &models.Title{ID: "tvdb:series:505", Name: "Synthetic Series", TVDBID: 505}
	members := []models.LocalMediaEpisode{{SeasonNumber: 1, EpisodeNumber: 1}, {SeasonNumber: 1, EpisodeNumber: 2}}
	item := &models.LocalMediaItem{Metadata: title, Episodes: members}
	raw, err := marshalLocalMediaItemMetadata(item)
	if err != nil {
		t.Fatal(err)
	}
	var stored models.LocalMediaItem
	unmarshalLocalMediaItemMetadata(raw, &stored)
	if !reflect.DeepEqual(stored.Episodes, members) || !reflect.DeepEqual(stored.Metadata, title) {
		t.Fatalf("lost metadata or file association: %s", raw)
	}
	var catalogue models.Title
	if err = json.Unmarshal(raw, &catalogue); err != nil {
		t.Fatal(err)
	}
	plain, err := json.Marshal(catalogue)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(title)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != string(legacy) {
		t.Fatal("file-local key escaped into catalogue DTO", string(plain))
	}
	item.Episodes = nil
	plain, err = marshalLocalMediaItemMetadata(item)
	if err != nil || string(plain) != string(legacy) {
		t.Fatal("legacy metadata serialization changed", err)
	}
	item.Metadata = nil
	plain, err = marshalLocalMediaItemMetadata(item)
	if err != nil || string(plain) != "null" {
		t.Fatal("legacy nil metadata changed", string(plain), err)
	}
}

func TestLocalMediaMalformedStoredMembershipNeverAdoptsScalarEpisode(t *testing.T) {
	for _, raw := range []string{`{"id":"series","localFileEpisodes":"bad"}`, `{"id":"series","localFileEpisodes":null}`, `{"id":"series","localFileEpisodes":[]}`, `{"id":"series","localFileEpisodes":[{"seasonNumber":1,"episodeNumber":1}]}`} {
		item := models.LocalMediaItem{SeasonNumber: 1, EpisodeNumber: 1}
		unmarshalLocalMediaItemMetadata([]byte(raw), &item)
		if len(item.Episodes) == 0 {
			t.Fatal("corrupt explicit membership reverted to scalar", raw)
		}
		if _, valid := models.NormalizeLocalMediaEpisodes(item.Episodes); valid {
			t.Fatal("corrupt explicit membership repaired by guessing", raw)
		}
	}
}
