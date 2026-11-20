package remotemedia

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"novastream/config"
	"novastream/services/profileunlock"
)

// This receipt is private schema evidence, never PG or upstream permission.
// It deliberately contains no path, token, ETag, request or response object.
type familyJellyfinReceipt struct {
	Primary, Backing, Library, Kind string
	Descriptor, Ancestors           string
	Size, Ticks                     int64
	Container                       string
}

type familyJellyfinDescriptor struct {
	ID, Container string
	Size, Ticks   int64
	Streams       []familyJellyfinCodec
}
type familyJellyfinCodec struct {
	Index                               int64
	Type, Codec                         string
	Width, Height, Channels, SampleRate int64
}

func familyJellyfinGUID(raw string) (string, bool) {
	if len(raw) == 36 {
		if raw[8] != '-' || raw[13] != '-' || raw[18] != '-' || raw[23] != '-' {
			return "", false
		}
		raw = raw[:8] + raw[9:13] + raw[14:18] + raw[19:23] + raw[24:]
	}
	var value [16]byte
	if len(raw) != 32 {
		return "", false
	}
	if _, err := hex.Decode(value[:], []byte(raw)); err != nil || value == [16]byte{} {
		return "", false
	}
	return hex.EncodeToString(value[:]), true
}

func familyJellyfinWholeJSON(raw []byte, array bool, maximum int) bool {
	if len(raw) == 0 || len(raw) > maximum || !utf8.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	tokens := 0
	var walk func(int) bool
	walk = func(depth int) bool {
		if depth > 64 || tokens >= 131072 {
			return false
		}
		token, err := decoder.Token()
		tokens++
		if err != nil {
			return false
		}
		delim, composite := token.(json.Delim)
		if depth == 0 && (!composite || array && delim != '[' || !array && delim != '{') {
			return false
		}
		if !composite {
			return true
		}
		switch delim {
		case '{':
			keys := map[string]bool{}
			for decoder.More() {
				keyValue, err := decoder.Token()
				tokens++
				key, ok := keyValue.(string)
				if err != nil || !ok || len(key) > 4096 || keys[key] || len(keys) >= 4096 || tokens >= 131072 {
					return false
				}
				keys[key] = true
				if !walk(depth + 1) {
					return false
				}
			}
		case '[':
			for decoder.More() {
				if !walk(depth + 1) {
					return false
				}
			}
		default:
			return false
		}
		end, err := decoder.Token()
		tokens++
		return err == nil && tokens <= 131072 && (delim == '{' && end == json.Delim('}') || delim == '[' && end == json.Delim(']'))
	}
	if !walk(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func familyJellyfinObject(raw []byte, keys ...string) (map[string]json.RawMessage, bool) {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, false
	}
	for key := range value {
		for _, canonical := range keys {
			if key != canonical && strings.EqualFold(key, canonical) {
				return nil, false
			}
		}
	}
	return value, true
}

func familyJellyfinString(raw json.RawMessage, maximum int) (string, bool) {
	var value string
	if json.Unmarshal(raw, &value) != nil || value == "" || strings.TrimSpace(value) != value || len(value) > maximum || strings.ContainsAny(value, "\x00\r\n") {
		return "", false
	}
	return value, true
}
func familyJellyfinInteger(raw json.RawMessage, minimum, maximum int64) (int64, bool) {
	var value *int64
	if json.Unmarshal(raw, &value) != nil || value == nil || *value < minimum || *value > maximum {
		return 0, false
	}
	return *value, true
}
func familyJellyfinBool(raw json.RawMessage, expected bool) bool {
	var value *bool
	return json.Unmarshal(raw, &value) == nil && value != nil && *value == expected
}
func familyJellyfinEmpty(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return true
	}
	var value string
	return json.Unmarshal(raw, &value) == nil && value == ""
}
func familyJellyfinOptionalFalse(raw json.RawMessage) bool {
	return len(raw) == 0 || bytes.Equal(raw, []byte("null")) || familyJellyfinBool(raw, false)
}
func familyJellyfinSinglePart(raw json.RawMessage) bool {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		// The pinned official DTO omits PartCount when AdditionalParts is empty.
		return true
	}
	_, ok := familyJellyfinInteger(raw, 1, 1)
	return ok
}

func familyJellyfinSelected(raw []byte, id string, size int64) (familyJellyfinDescriptor, bool) {
	var empty familyJellyfinDescriptor
	item, ok := familyJellyfinObject(raw, "Id", "Type", "CanDownload", "PartCount", "IsFolder", "IsPlaceHolder", "MediaSources", "RunTimeTicks")
	if !ok || !familyJellyfinBool(item["CanDownload"], true) || !familyJellyfinSinglePart(item["PartCount"]) || !familyJellyfinOptionalFalse(item["IsFolder"]) || !familyJellyfinOptionalFalse(item["IsPlaceHolder"]) {
		return empty, false
	}
	var sources []json.RawMessage
	if json.Unmarshal(item["MediaSources"], &sources) != nil || len(sources) == 0 || len(sources) > 32 {
		return empty, false
	}
	seen := map[string]bool{}
	var selected map[string]json.RawMessage
	for _, source := range sources {
		value, valid := familyJellyfinObject(source, "Id", "Type", "Protocol", "VideoType", "IsRemote", "IsInfiniteStream", "RequiresOpening", "RequiresClosing", "RequiresLooping", "OpenToken", "LiveStreamId", "TranscodingUrl", "EncoderPath", "EncoderProtocol", "RequiredHttpHeaders", "Size", "RunTimeTicks", "Container", "MediaStreams")
		if !valid {
			return empty, false
		}
		text, valid := familyJellyfinString(value["Id"], 36)
		guid, concrete := familyJellyfinGUID(text)
		if !valid || !concrete || seen[guid] {
			return empty, false
		}
		seen[guid] = true
		if guid == id {
			selected = value
		}
	}
	if selected == nil {
		return empty, false
	}
	for field, required := range map[string]string{"Protocol": "File", "Type": "Default", "VideoType": "VideoFile"} {
		value, valid := familyJellyfinString(selected[field], 32)
		if !valid || value != required {
			return empty, false
		}
	}
	for _, field := range []string{"IsRemote", "IsInfiniteStream", "RequiresOpening", "RequiresClosing", "RequiresLooping"} {
		if !familyJellyfinBool(selected[field], false) {
			return empty, false
		}
	}
	for _, field := range []string{"OpenToken", "LiveStreamId", "TranscodingUrl", "EncoderPath", "EncoderProtocol"} {
		if !familyJellyfinEmpty(selected[field]) {
			return empty, false
		}
	}
	if raw := selected["RequiredHttpHeaders"]; len(raw) != 0 && !bytes.Equal(raw, []byte("null")) {
		var headers map[string]json.RawMessage
		if json.Unmarshal(raw, &headers) != nil || len(headers) != 0 {
			return empty, false
		}
	}
	length, validSize := familyJellyfinInteger(selected["Size"], 1, 8<<30)
	ticks, validTime := familyJellyfinInteger(selected["RunTimeTicks"], 1, 24*60*60*10000000)
	container, validContainer := familyJellyfinString(selected["Container"], 64)
	if !validSize || length != size || !validTime || !validContainer {
		return empty, false
	}
	for _, character := range container {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-') {
			return empty, false
		}
	}
	var streams []json.RawMessage
	if json.Unmarshal(selected["MediaStreams"], &streams) != nil || len(streams) == 0 || len(streams) > 64 {
		return empty, false
	}
	out := familyJellyfinDescriptor{ID: id, Size: length, Ticks: ticks, Container: strings.ToLower(container)}
	indices := map[int64]bool{}
	video, audio := false, false
	for _, raw := range streams {
		stream, valid := familyJellyfinObject(raw, "Index", "Type", "Codec", "IsExternal", "DeliveryUrl", "Path", "Width", "Height", "Channels", "SampleRate")
		if !valid || !familyJellyfinBool(stream["IsExternal"], false) || !familyJellyfinEmpty(stream["DeliveryUrl"]) || !familyJellyfinEmpty(stream["Path"]) {
			return empty, false
		}
		index, validIndex := familyJellyfinInteger(stream["Index"], 0, 65535)
		kind, validKind := familyJellyfinString(stream["Type"], 32)
		if !validIndex || indices[index] || !validKind {
			return empty, false
		}
		indices[index] = true
		codec := ""
		if !familyJellyfinEmpty(stream["Codec"]) {
			codec, valid = familyJellyfinString(stream["Codec"], 64)
			if !valid {
				return empty, false
			}
			for _, character := range codec {
				if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.' || character == '+') {
					return empty, false
				}
			}
		}
		entry := familyJellyfinCodec{Index: index, Type: kind, Codec: strings.ToLower(codec)}
		for field, limit := range map[string]int64{"Width": 16384, "Height": 16384, "Channels": 64, "SampleRate": 768000} {
			if value := stream[field]; len(value) != 0 && !bytes.Equal(value, []byte("null")) {
				number, valid := familyJellyfinInteger(value, 0, limit)
				if !valid {
					return empty, false
				}
				switch field {
				case "Width":
					entry.Width = number
				case "Height":
					entry.Height = number
				case "Channels":
					entry.Channels = number
				case "SampleRate":
					entry.SampleRate = number
				}
			}
		}
		switch kind {
		case "Video":
			video = codec != ""
		case "Audio":
			audio = codec != ""
		case "Subtitle", "EmbeddedImage", "Data", "Attachment":
		default:
			return empty, false
		}
		if (kind == "Video" || kind == "Audio") && codec == "" {
			return empty, false
		}
		out.Streams = append(out.Streams, entry)
	}
	sort.Slice(out.Streams, func(i, j int) bool { return out.Streams[i].Index < out.Streams[j].Index })
	return out, video && audio
}

func parseFamilyJellyfin(info profileunlock.FamilyJellyfinSourceInfo, observation config.ManagedJellyfinMetadata) (familyJellyfinReceipt, error) {
	var empty familyJellyfinReceipt
	if !familyJellyfinWholeJSON(observation.PrimaryJSON, false, 1<<20) || !familyJellyfinWholeJSON(observation.BackingJSON, false, 1<<20) || !familyJellyfinWholeJSON(observation.AncestorsJSON, true, 256<<10) {
		return empty, profileunlock.ErrFamilyMediaChanged
	}
	kind := map[string]string{"movie": "Movie", "show": "Episode"}[info.LibraryType]
	if kind == "" || info.Size <= 0 || info.Size > 8<<30 {
		return empty, profileunlock.ErrFamilyMediaChanged
	}
	for i, raw := range [][]byte{observation.PrimaryJSON, observation.BackingJSON} {
		item, valid := familyJellyfinObject(raw, "Id", "Type")
		text, validID := familyJellyfinString(item["Id"], 36)
		guid, concrete := familyJellyfinGUID(text)
		itemKind, validKind := familyJellyfinString(item["Type"], 32)
		expected := info.PrimaryGUID
		if i == 1 {
			expected = info.BackingGUID
		}
		if !valid || !validID || !concrete || guid != expected || !validKind || itemKind != kind {
			return empty, profileunlock.ErrFamilyMediaChanged
		}
	}
	primary, validPrimary := familyJellyfinSelected(observation.PrimaryJSON, info.BackingGUID, info.Size)
	backing, validBacking := familyJellyfinSelected(observation.BackingJSON, info.BackingGUID, info.Size)
	primaryJSON, _ := json.Marshal(primary)
	backingJSON, _ := json.Marshal(backing)
	item, _ := familyJellyfinObject(observation.BackingJSON)
	ticks, validTicks := familyJellyfinInteger(item["RunTimeTicks"], 1, 24*60*60*10000000)
	if !validPrimary || !validBacking || !bytes.Equal(primaryJSON, backingJSON) || !validTicks || ticks != backing.Ticks {
		return empty, profileunlock.ErrFamilyMediaChanged
	}
	var rows []json.RawMessage
	if json.Unmarshal(observation.AncestorsJSON, &rows) != nil || len(rows) == 0 || len(rows) > 64 {
		return empty, profileunlock.ErrFamilyMediaChanged
	}
	seen, chain, member := map[string]bool{}, []string{}, false
	for _, raw := range rows {
		row, valid := familyJellyfinObject(raw, "Id")
		id, validID := familyJellyfinString(row["Id"], 36)
		guid, concrete := familyJellyfinGUID(id)
		if !valid || !validID || !concrete || seen[guid] {
			return empty, profileunlock.ErrFamilyMediaChanged
		}
		seen[guid] = true
		chain = append(chain, guid)
		member = member || guid == info.ExternalLibraryGUID
	}
	if !member {
		return empty, profileunlock.ErrFamilyMediaChanged
	}
	return familyJellyfinReceipt{Primary: info.PrimaryGUID, Backing: info.BackingGUID, Library: info.ExternalLibraryGUID, Kind: kind,
		Descriptor: string(backingJSON), Ancestors: strings.Join(chain, "/"), Size: backing.Size, Ticks: ticks, Container: backing.Container}, nil
}
