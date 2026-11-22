package models

import "encoding/json"

const remoteMediaFilterFactsKey = "mediaFilterFactsV1"

// Stored catalogue metadata uses the existing provider JSON column. It describes
// one provider version/part, without adding any playback or matching permission.
type remoteMediaFilterFacts struct {
	ID, LibraryID, ExternalItemID, ExternalMediaID, FileName string
	SizeBytes                                                int64
	Probe                                                    *LocalMediaProbe
}

func StoreRemoteMediaFilterFacts(item *RemoteMediaItem, data map[string]string) {
	if item == nil || data == nil {
		return
	}
	delete(data, remoteMediaFilterFactsKey)
	if item.Probe == nil {
		return
	}
	value := remoteMediaFilterFacts{item.ID, item.LibraryID, item.ExternalItemID, item.ExternalMediaID, item.FileName, item.SizeBytes, item.Probe}
	encoded, err := json.Marshal(value)
	if err == nil {
		data[remoteMediaFilterFactsKey] = string(encoded)
	}
}

func HydrateRemoteMediaFilterFacts(item *RemoteMediaItem) {
	if item == nil {
		return
	}
	item.Probe = nil
	raw := item.ProviderData[remoteMediaFilterFactsKey]
	if raw == "" {
		return
	}
	var value remoteMediaFilterFacts
	if json.Unmarshal([]byte(raw), &value) != nil || value.ID != item.ID || value.LibraryID != item.LibraryID || value.ExternalItemID != item.ExternalItemID || value.ExternalMediaID != item.ExternalMediaID || value.FileName != item.FileName || value.SizeBytes != item.SizeBytes {
		return
	}
	item.Probe = value.Probe
}
