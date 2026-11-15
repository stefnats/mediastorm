package models

import "encoding/json"

const remoteSelectionProofKey = "automaticSelectionV1"

// The existing provider_data JSON column retains additive sync facts without a
// schema migration. The anchors prevent a copied envelope from proving another
// item, parent group or media part after a restore or partial catalog update.
type remoteSelectionProof struct {
	ItemID, ExternalItemID, ExternalMediaID, GroupKey string
	LibraryID, StreamPath                             string
	SeasonNumber, EpisodeNumber                       int
	SinglePart                                        bool
	SeriesExternalIDs, EpisodeExternalIDs             *LocalMediaExternalIDs
	AudioLanguages                                    []string
}

func RemoteSelectionProviderData(item *RemoteMediaItem) map[string]string {
	data := make(map[string]string, len(item.ProviderData)+1)
	for key, value := range item.ProviderData {
		data[key] = value
	}
	delete(data, remoteSelectionProofKey)
	if item.SinglePart || item.SeriesExternalIDs != nil || item.EpisodeExternalIDs != nil || len(item.AudioLanguages) > 0 {
		proof := remoteSelectionProof{ItemID: item.ID, ExternalItemID: item.ExternalItemID, ExternalMediaID: item.ExternalMediaID, GroupKey: item.GroupKey, LibraryID: item.LibraryID, StreamPath: item.StreamPath, SeasonNumber: item.SeasonNumber, EpisodeNumber: item.EpisodeNumber, SinglePart: item.SinglePart, SeriesExternalIDs: item.SeriesExternalIDs, EpisodeExternalIDs: item.EpisodeExternalIDs, AudioLanguages: item.AudioLanguages}
		encoded, _ := json.Marshal(proof)
		data[remoteSelectionProofKey] = string(encoded)
	}
	return data
}

func HydrateRemoteSelectionProof(item *RemoteMediaItem) {
	if item == nil {
		return
	}
	item.SinglePart, item.SeriesExternalIDs, item.EpisodeExternalIDs, item.AudioLanguages = false, nil, nil, nil
	encoded := item.ProviderData[remoteSelectionProofKey]
	if encoded == "" || len(encoded) > 16384 {
		return
	}
	var proof remoteSelectionProof
	if json.Unmarshal([]byte(encoded), &proof) != nil || proof.ItemID != item.ID || proof.ExternalItemID != item.ExternalItemID || proof.ExternalMediaID != item.ExternalMediaID || proof.GroupKey != item.GroupKey || proof.LibraryID != item.LibraryID || proof.StreamPath != item.StreamPath || proof.SeasonNumber != item.SeasonNumber || proof.EpisodeNumber != item.EpisodeNumber || len(proof.AudioLanguages) > 128 {
		return
	}
	item.SinglePart, item.SeriesExternalIDs, item.EpisodeExternalIDs, item.AudioLanguages = proof.SinglePart, proof.SeriesExternalIDs, proof.EpisodeExternalIDs, proof.AudioLanguages
}

// An uploaded backup is not a trusted completed provider scan. Retain manual
// playback fields while requiring the next ordinary sync for automatic proof.
func ClearRemoteSelectionProof(item *RemoteMediaItem) {
	if item == nil {
		return
	}
	item.SinglePart, item.SeriesExternalIDs, item.EpisodeExternalIDs, item.AudioLanguages = false, nil, nil, nil
	if item.ProviderData != nil {
		copy := make(map[string]string, len(item.ProviderData))
		for key, value := range item.ProviderData {
			if key != remoteSelectionProofKey {
				copy[key] = value
			}
		}
		item.ProviderData = copy
	}
}
