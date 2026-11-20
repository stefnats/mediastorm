package remotemedia

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"novastream/config"
	"novastream/services/profileunlock"
)

const (
	familyJellyfinTestPrimary = "11111111222233334444555555555555"
	familyJellyfinTestBacking = "22222222333344445555666666666666"
	familyJellyfinTestLibrary = "33333333444455556666777777777777"
	familyJellyfinTestUser    = "44444444555566667777888888888888"
)

func familyJellyfinTestSource(size int64) map[string]any {
	return map[string]any{
		"Id": familyJellyfinTestBacking, "Protocol": "File", "Type": "Default", "VideoType": "VideoFile", "Container": "mkv", "Size": size, "RunTimeTicks": int64(900000000),
		"IsRemote": false, "IsInfiniteStream": false, "RequiresOpening": false, "RequiresClosing": false, "RequiresLooping": false,
		"OpenToken": nil, "LiveStreamId": nil, "TranscodingUrl": nil, "EncoderPath": nil, "EncoderProtocol": nil, "RequiredHttpHeaders": map[string]any{}, "Path": "/server/private/never-open-this.mkv",
		"MediaStreams": []any{
			map[string]any{"Index": 0, "Type": "Video", "Codec": "h264", "Width": 1920, "Height": 1080, "IsExternal": false},
			map[string]any{"Index": 1, "Type": "Audio", "Codec": "aac", "Channels": 2, "SampleRate": 48000, "IsExternal": false},
		},
	}
}
func familyJellyfinTestItem(id string, size int64) map[string]any {
	return map[string]any{"Id": id, "Type": "Movie", "CanDownload": true, "RunTimeTicks": int64(900000000), "IsFolder": false, "IsPlaceHolder": nil,
		"MediaSources": []any{familyJellyfinTestSource(size)}, "Name": "Untrusted display label", "ImageTags": map[string]any{"Primary": "unrelated"}, "UserData": map[string]any{"PlaybackPositionTicks": 0}}
}
func familyJellyfinProtocolFixture(size int64) (profileunlock.FamilyJellyfinSourceInfo, map[string]any, map[string]any) {
	return profileunlock.FamilyJellyfinSourceInfo{ItemID: "remote-item", LibraryID: "remote-library", IntegrationID: "integration", PrimaryGUID: familyJellyfinTestPrimary,
			BackingGUID: familyJellyfinTestBacking, ExternalLibraryGUID: familyJellyfinTestLibrary, LibraryType: "movie", Size: size, FileName: "synthetic.mkv"},
		familyJellyfinTestItem(familyJellyfinTestPrimary, size), familyJellyfinTestItem(familyJellyfinTestBacking, size)
}
func familyJellyfinTestObservation(primary, backing map[string]any) config.ManagedJellyfinMetadata {
	p, _ := json.Marshal(primary)
	b, _ := json.Marshal(backing)
	a, _ := json.Marshal([]map[string]any{{"Id": familyJellyfinTestLibrary, "Name": "Untrusted library name"}})
	return config.ManagedJellyfinMetadata{PrimaryJSON: p, BackingJSON: b, AncestorsJSON: a, StartedAt: time.Now()}
}

func TestFamilyJellyfinProtocolOfficialSinglePartAndExactNonfirstSource(t *testing.T) {
	for _, mode := range []string{"absent PartCount", "null PartCount", "explicit single", "nonfirst exact", "same item", "episode", "unrelated metadata"} {
		t.Run(mode, func(t *testing.T) {
			info, primary, backing := familyJellyfinProtocolFixture(1234)
			switch mode {
			case "null PartCount":
				primary["PartCount"], backing["PartCount"] = nil, nil
			case "explicit single":
				primary["PartCount"], backing["PartCount"] = 1, 1
			case "nonfirst exact":
				other := familyJellyfinTestSource(999)
				other["Id"] = "55555555666677778888999999999999"
				primary["MediaSources"] = []any{other, familyJellyfinTestSource(info.Size)}
				// An alternate cut can have a different top-level primary runtime.
				primary["RunTimeTicks"] = 123
			case "same item":
				info.PrimaryGUID = info.BackingGUID
				primary["Id"] = info.BackingGUID
			case "episode":
				info.LibraryType = "show"
				primary["Type"], backing["Type"] = "Episode", "Episode"
			case "unrelated metadata":
				primary["DateModified"] = "untrusted"
				primary["Etag"] = "not-an-immutable-version"
				backing["UserData"] = map[string]any{"PlaybackPositionTicks": 999}
			}
			receipt, err := parseFamilyJellyfin(info, familyJellyfinTestObservation(primary, backing))
			if err != nil || receipt.Size != 1234 || receipt.Backing != familyJellyfinTestBacking || strings.Contains(receipt.Descriptor, "/server/private") || strings.Contains(receipt.Descriptor, "PlaybackPosition") {
				t.Fatal("official finite exact-source shape was not parsed without private path or unrelated evidence")
			}
		})
	}
}

func TestFamilyJellyfinProtocolCurrentIdentityDownloadAndMembershipDenials(t *testing.T) {
	for _, kind := range []string{"primary", "backing", "kind", "folder", "placeholder", "missing CanDownload", "false CanDownload", "null CanDownload", "multipart", "zero parts", "missing exact source", "duplicate source", "descriptor mismatch", "wrong library", "duplicate ancestors", "many ancestors"} {
		t.Run(kind, func(t *testing.T) {
			info, primary, backing := familyJellyfinProtocolFixture(1234)
			switch kind {
			case "primary":
				primary["Id"] = familyJellyfinTestUser
			case "backing":
				backing["Id"] = familyJellyfinTestUser
			case "kind":
				backing["Type"] = "Series"
			case "folder":
				backing["IsFolder"] = true
			case "placeholder":
				primary["IsPlaceHolder"] = true
			case "missing CanDownload":
				delete(backing, "CanDownload")
			case "false CanDownload":
				primary["CanDownload"] = false
			case "null CanDownload":
				primary["CanDownload"] = nil
			case "multipart":
				backing["PartCount"] = 2
			case "zero parts":
				backing["PartCount"] = 0
			case "missing exact source":
				backing["MediaSources"].([]any)[0].(map[string]any)["Id"] = familyJellyfinTestUser
			case "duplicate source":
				primary["MediaSources"] = []any{familyJellyfinTestSource(info.Size), familyJellyfinTestSource(info.Size)}
			case "descriptor mismatch":
				backing["MediaSources"].([]any)[0].(map[string]any)["Container"] = "mp4"
			}
			observation := familyJellyfinTestObservation(primary, backing)
			switch kind {
			case "wrong library":
				observation.AncestorsJSON = []byte(`[{"Id":"` + familyJellyfinTestUser + `"}]`)
			case "duplicate ancestors":
				observation.AncestorsJSON = []byte(`[{"Id":"` + familyJellyfinTestLibrary + `"},{"Id":"` + familyJellyfinTestLibrary + `"}]`)
			case "many ancestors":
				observation.AncestorsJSON = []byte("[" + strings.Repeat(`{"Id":"`+familyJellyfinTestLibrary+`"},`, 64) + `{"Id":"` + familyJellyfinTestLibrary + `"}]`)
			}
			if _, err := parseFamilyJellyfin(info, observation); err == nil {
				t.Fatal("unknown or changed current identity/download/member evidence was accepted")
			}
		})
	}
}

func TestFamilyJellyfinProtocolFiniteStaticShapeCannotPromoteDynamicMetadata(t *testing.T) {
	for _, kind := range []string{"Protocol", "Type", "VideoType", "IsRemote", "IsInfiniteStream", "RequiresOpening", "RequiresClosing", "RequiresLooping", "OpenToken", "LiveStreamId", "TranscodingUrl", "EncoderPath", "EncoderProtocol", "RequiredHttpHeaders", "Size", "RunTimeTicks", "Container", "external audio", "stream URL", "codec URL", "duplicate stream", "missing audio codec", "many streams"} {
		t.Run(kind, func(t *testing.T) {
			info, primary, backing := familyJellyfinProtocolFixture(1234)
			for _, item := range []map[string]any{primary, backing} {
				source := item["MediaSources"].([]any)[0].(map[string]any)
				switch kind {
				case "Protocol", "Type", "VideoType", "OpenToken", "LiveStreamId", "TranscodingUrl", "EncoderPath", "EncoderProtocol":
					source[kind] = "unaccepted"
				case "IsRemote", "IsInfiniteStream", "RequiresOpening", "RequiresClosing", "RequiresLooping":
					source[kind] = true
				case "RequiredHttpHeaders":
					source[kind] = map[string]string{"Authorization": "never adopt"}
				case "Size":
					source[kind] = info.Size + 1
				case "RunTimeTicks":
					source[kind] = int64(24*60*60*10000000 + 1)
				case "Container":
					source[kind] = "https://unaccepted.invalid/"
				case "external audio":
					source["MediaStreams"].([]any)[1].(map[string]any)["IsExternal"] = true
				case "stream URL":
					source["MediaStreams"].([]any)[1].(map[string]any)["DeliveryUrl"] = "http://unaccepted.invalid/"
				case "codec URL":
					source["MediaStreams"].([]any)[1].(map[string]any)["Codec"] = "http://unaccepted.invalid/"
				case "duplicate stream":
					source["MediaStreams"].([]any)[1].(map[string]any)["Index"] = 0
				case "missing audio codec":
					delete(source["MediaStreams"].([]any)[1].(map[string]any), "Codec")
				case "many streams":
					streams := make([]any, 65)
					for i := range streams {
						streams[i] = map[string]any{"Index": i, "Type": "Audio", "Codec": "aac", "IsExternal": false}
					}
					source["MediaStreams"] = streams
				}
			}
			if _, err := parseFamilyJellyfin(info, familyJellyfinTestObservation(primary, backing)); err == nil {
				t.Fatal("dynamic, incomplete or inconsistent descriptor became a finite source receipt")
			}
		})
	}
}

func TestFamilyJellyfinProtocolWholeJSONAndCanonicalSecurityFields(t *testing.T) {
	for _, kind := range []string{"duplicate", "case alias", "trailing", "utf8", "depth", "oversize", "zero GUID", "URN GUID"} {
		t.Run(kind, func(t *testing.T) {
			info, primary, backing := familyJellyfinProtocolFixture(1234)
			observation := familyJellyfinTestObservation(primary, backing)
			switch kind {
			case "duplicate":
				observation.PrimaryJSON = bytes.Replace(observation.PrimaryJSON, []byte(`"CanDownload":true`), []byte(`"CanDownload":true,"CanDownload":true`), 1)
			case "case alias":
				observation.PrimaryJSON = bytes.Replace(observation.PrimaryJSON, []byte(`"CanDownload":true`), []byte(`"CanDownload":true,"candownload":true`), 1)
			case "trailing":
				observation.PrimaryJSON = append(observation.PrimaryJSON, []byte(` {}`)...)
			case "utf8":
				observation.PrimaryJSON = []byte{'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'}
			case "depth":
				observation.PrimaryJSON = []byte(`{"x":` + strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65) + "}")
			case "oversize":
				observation.PrimaryJSON = bytes.Repeat([]byte{' '}, (1<<20)+1)
			case "zero GUID":
				observation.PrimaryJSON = bytes.ReplaceAll(observation.PrimaryJSON, []byte(familyJellyfinTestBacking), []byte("00000000000000000000000000000000"))
			case "URN GUID":
				observation.PrimaryJSON = bytes.ReplaceAll(observation.PrimaryJSON, []byte(familyJellyfinTestBacking), []byte("urn:uuid:"+familyJellyfinTestBacking))
			}
			if _, err := parseFamilyJellyfin(info, observation); err == nil {
				t.Fatal("malformed bounded JSON or security alias was accepted")
			}
		})
	}
}
