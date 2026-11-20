package localmedia

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestFamilyLocalProbeReducedTechnicalMetadataOnly(t *testing.T) {
	raw := []byte(`{"streams":[{"index":0,"codec_type":"video","codec_name":"h264","width":1920,"height":1080,"avg_frame_rate":"24000/1001","tags":{"language":"FRA","title":"PRIVATE_TITLE","filename":"/PRIVATE_PATH"},"disposition":{"default":1,"forced":0}},{"index":1,"codec_type":"audio","codec_name":"aac","channels":2,"sample_rate":"48000","tags":{"language":"en-US"}},{"index":2,"codec_type":"attachment","codec_name":"ttf","tags":{"filename":"PRIVATE_ATTACHMENT"}}],"format":{"duration":"12.5","filename":"https://PRIVATE_URL","tags":{"title":"PRIVATE_TITLE"}},"chapters":[{"title":"PRIVATE_CHAPTER"}]}`)
	out, err := reduceFamilyLocalProbe(raw, "mov")
	if err != nil || out.Container != "mov" || out.DurationSeconds == nil || *out.DurationSeconds != 12.5 || len(out.Streams) != 2 {
		t.Fatal("bounded technical metadata missing", err)
	}
	if out.Streams[0].DeclaredLanguage != "fra" || !out.Streams[0].Default || out.Streams[0].FrameRate < 23.97 || out.Streams[0].FrameRate > 23.98 || out.Streams[1].DeclaredLanguage != "en-us" || out.Streams[1].Channels != 2 || out.Streams[1].SampleRate != 48000 {
		t.Fatal("declared stream metadata changed")
	}
	encoded, err := json.Marshal(out)
	if err != nil || strings.Contains(string(encoded), "PRIVATE_") || strings.Contains(string(encoded), "filename") || strings.Contains(string(encoded), "chapter") {
		t.Fatal("raw file, URL, tag or chapter escaped reduction")
	}
}

func TestFamilyLocalProbeRejectsMalformedOrUnboundedOutput(t *testing.T) {
	base := `{"streams":[{"index":0,"codec_type":"audio","codec_name":"aac"}],"format":{"duration":"1"}}`
	cases := map[string]string{
		"empty": "", "trailing": base + `{}`, "no-streams": `{"streams":[]}`,
		"missing-index":     strings.Replace(base, `"index":0,`, "", 1),
		"negative-index":    strings.Replace(base, `"index":0`, `"index":-1`, 1),
		"large-index":       strings.Replace(base, `"index":0`, `"index":1024`, 1),
		"codec-path":        strings.Replace(base, `"aac"`, `"../aac"`, 1),
		"width":             strings.Replace(base, `"codec_name":"aac"`, `"codec_name":"aac","width":16385`, 1),
		"channels":          strings.Replace(base, `"codec_name":"aac"`, `"codec_name":"aac","channels":65`, 1),
		"rate":              strings.Replace(base, `"codec_name":"aac"`, `"codec_name":"aac","sample_rate":"768001"`, 1),
		"zero-denominator":  strings.Replace(base, `"codec_name":"aac"`, `"codec_name":"aac","avg_frame_rate":"24/0"`, 1),
		"large-frame-rate":  strings.Replace(base, `"codec_name":"aac"`, `"codec_name":"aac","avg_frame_rate":"1001/1"`, 1),
		"flag":              strings.Replace(base, `"codec_name":"aac"`, `"codec_name":"aac","disposition":{"default":2}`, 1),
		"nan":               strings.Replace(base, `"duration":"1"`, `"duration":"NaN"`, 1),
		"infinite":          strings.Replace(base, `"duration":"1"`, `"duration":"+Inf"`, 1),
		"negative-duration": strings.Replace(base, `"duration":"1"`, `"duration":"-1"`, 1),
		"long-duration":     strings.Replace(base, `"duration":"1"`, `"duration":"31622401"`, 1),
		"duplicate":         `{"streams":[{"index":0,"codec_type":"audio","codec_name":"aac"},{"index":0,"codec_type":"video","codec_name":"h264"}]}`,
		"unsupported-only":  `{"streams":[{"index":0,"codec_type":"data","codec_name":"bin_data"}]}`,
		"raw-bound":         strings.Repeat(" ", familyLocalProbeOutputLimit+1),
		"stream-bound":      `{"streams":[` + strings.Repeat(`{"index":0,"codec_type":"audio","codec_name":"aac"},`, 128) + `{"index":1,"codec_type":"audio","codec_name":"aac"}]}`,
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := reduceFamilyLocalProbe([]byte(raw), "matroska")
			if err == nil || len(out.Streams) != 0 || out.DurationSeconds != nil || out.Container != "" {
				t.Fatal("invalid metadata published")
			}
		})
	}
	if _, err := reduceFamilyLocalProbe([]byte(base), "hls"); err == nil {
		t.Fatal("unsupported container accepted")
	}
}

func TestFamilyLocalProbeUntrustedLanguageTagsDoNotBecomePathsOrClaims(t *testing.T) {
	for _, language := range []string{"../../private", "https://invalid", "fr\nPRIVATE", strings.Repeat("a", 36), ""} {
		raw, _ := json.Marshal(map[string]any{"streams": []any{map[string]any{"index": 0, "codec_type": "audio", "codec_name": "aac", "tags": map[string]string{"language": language}}}})
		out, err := reduceFamilyLocalProbe(raw, "mpegts")
		if err != nil || len(out.Streams) != 1 || out.Streams[0].DeclaredLanguage != "" {
			t.Fatal("unsafe declared tag survived")
		}
	}
}

func TestFamilyLocalProbeToolsImportedAndCopiedValuesHaveNoAuthority(t *testing.T) {
	tools := &FamilyLocalProbeTools{familyLocalProbeToolsState: &familyLocalProbeToolsState{config: FamilyLocalProbeConfig{HelperPath: "/PRIVATE_HELPER", FFprobePath: "/PRIVATE_PROBE"}}}
	tools.self = tools
	copy := *tools
	if copy.valid() || (&FamilyLocalProbeTools{}).valid() || (*FamilyLocalProbeTools)(nil).valid() {
		t.Fatal("copied or absent tools acquired self authority")
	}
	for _, v := range []any{tools, *tools, &copy, copy} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			if value := fmt.Sprintf(verb, v); strings.Contains(value, "PRIVATE") || !strings.Contains(value, "opaque") {
				t.Fatal("opaque tools formatting disclosed state")
			}
		}
		if b, err := json.Marshal(v); err != nil || string(b) != "null" {
			t.Fatal("opaque tools serialized")
		}
	}
	var imported FamilyLocalProbeTools
	if json.Unmarshal([]byte(`{"HelperPath":"/PRIVATE_HELPER"}`), &imported) == nil || imported.valid() {
		t.Fatal("JSON created executable tools")
	}
}
