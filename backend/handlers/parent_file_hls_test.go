package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"novastream/api/openapi"
	"novastream/services/kidsvod"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const parentHLSValidInventory = `{"streams":[{"index":0,"codec_type":"video","codec_name":"h264","profile":"Main","width":64,"height":64,"pix_fmt":"yuv420p","field_order":"progressive"},{"index":1,"codec_type":"audio","codec_name":"aac","profile":"LC","channels":1,"sample_rate":"48000"}],"format":{"format_name":"mpegts","duration":"2.1"}}`

func parentHLSFacts(t *testing.T) parentHLSInventory {
	t.Helper()
	var v parentHLSInventory
	if json.Unmarshal([]byte(parentHLSValidInventory), &v) != nil {
		t.Fatal("synthetic facts invalid")
	}
	return v
}
func TestParentFileHLSInventoryRequiresFiniteDurationAndEverySupportedTrack(t *testing.T) {
	for _, kind := range []string{"valid", "two-hours", "unknown-duration", "nan", "infinite", "over-two-hours", "zero", "extra-audio", "subtitle", "unknown-codec", "interlaced", "oversized", "unknown-rate", "duplicate-index", "missing-index"} {
		t.Run(kind, func(t *testing.T) {
			v := parentHLSFacts(t)
			switch kind {
			case "two-hours":
				v.Format.Duration = "7200"
			case "unknown-duration":
				v.Format.Duration = ""
			case "nan":
				v.Format.Duration = "NaN"
			case "infinite":
				v.Format.Duration = "+Inf"
			case "over-two-hours":
				v.Format.Duration = "7200.001"
			case "zero":
				v.Format.Duration = "0"
			case "extra-audio":
				v.Streams = append(v.Streams, v.Streams[1])
			case "subtitle":
				v.Streams[1].Type = "subtitle"
			case "unknown-codec":
				v.Streams[0].Codec = "hevc"
			case "interlaced":
				v.Streams[0].Field = "tt"
			case "oversized":
				v.Streams[0].Width = 8192
			case "unknown-rate":
				v.Streams[1].Rate = "N/A"
			case "duplicate-index":
				index := 0
				v.Streams[1].Index = &index
			case "missing-index":
				v.Streams[0].Index = nil
			}
			_, _, allowed := validParentHLSInventory(v)
			if allowed != (kind == "valid" || kind == "two-hours") {
				t.Fatal("unsupported input selected or valid input rejected", kind)
			}
		})
	}
}
func parentHLSSyntheticPlaylist(count int, seconds string) []byte {
	var b strings.Builder
	b.WriteString("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-INDEPENDENT-SEGMENTS\n")
	for n := 0; n < count; n++ {
		fmt.Fprintf(&b, "#EXTINF:%s,\n%s\n", seconds, parentHLSSegmentName(n))
	}
	b.WriteString("#EXT-X-ENDLIST\n")
	return []byte(b.String())
}
func TestParentFileHLSPlaylistRejectsExternalAndIncompletePresentation(t *testing.T) {
	valid := string(parentHLSSyntheticPlaylist(2, "1"))
	if !validParentHLSPlaylist([]byte(valid)) {
		t.Fatal("local sequential playlist rejected")
	}
	for _, raw := range []string{
		strings.Replace(valid, "segment000000.ts", "https://outside.invalid/video.ts", 1),
		strings.Replace(valid, "segment000000.ts", "../segment000000.ts", 1),
		strings.Replace(valid, "segment000000.ts", "segment000000.ts?token=synthetic", 1),
		strings.Replace(valid, "segment000001.ts", "segment000002.ts", 1),
		strings.Replace(valid, "#EXTINF:1,", "#EXTINF:NaN,", 1),
		strings.Replace(valid, "#EXTINF:1,", "#EXT-X-KEY:METHOD=AES-128,URI=\"https://outside.invalid/key\"\n#EXTINF:1,", 1),
		strings.Replace(valid, "#EXT-X-ENDLIST", "#EXTINF:1,\n#EXT-X-ENDLIST", 1),
		valid + "segment000002.ts\n",
	} {
		if validParentHLSPlaylist([]byte(raw)) {
			t.Fatal("unsafe or ambiguous playlist accepted")
		}
	}
}
func TestParentFileHLSPlaylistRejectsOverBudgetWithoutTruncation(t *testing.T) {
	if !validParentHLSPlaylist(parentHLSSyntheticPlaylist(2048, "1")) {
		t.Fatal("bounded local segment list rejected")
	}
	for _, raw := range [][]byte{parentHLSSyntheticPlaylist(2049, "1"), parentHLSSyntheticPlaylist(2, "3600.1"), []byte(strings.Repeat("x", (64<<10)+1))} {
		if validParentHLSPlaylist(raw) {
			t.Fatal("over-budget programme silently truncated or accepted")
		}
	}
}
func TestParentFileHLSApprovalAcceptsOnlyOriginalTypedLocator(t *testing.T) {
	id := "lfa_" + strings.Repeat("a", 64)
	if !validParentHLSApprovalBody([]byte(`{"approvalId":"` + id + `"}`)) {
		t.Fatal("typed locator rejected")
	}
	for _, raw := range []string{`{}`, `{"approvalId":null}`, `{"ApprovalId":"` + id + `"}`, `{"approvalId":"` + id + `","approvalId":"` + id + `"}`, `{"approvalId":"` + id + `","path":"/synthetic"}`, `{"approvalId":"` + id + `"} {}`, `{"approvalId":"lfa_` + strings.Repeat("A", 64) + `"}`} {
		if validParentHLSApprovalBody([]byte(raw)) {
			t.Fatal("request selected new authority/path/duplicate body")
		}
	}
}
func TestParentFileHLSCaptureCancelsBeforeOversizedToolAllocation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer := &parentHLSCapture{limit: 64, cancel: cancel}
	if n, err := writer.Write([]byte(strings.Repeat("x", 4096))); n != 0 || err == nil || ctx.Err() == nil {
		t.Fatal("unbounded tool output retained or command not canceled")
	}
	raw, over := writer.bytes()
	if len(raw) != 0 || !over {
		t.Fatal("oversized output copied")
	}
}
func TestParentFileHLSNamesRejectForeignTemporaryAndEncodedOutput(t *testing.T) {
	for _, name := range []string{"segment000000.ts", "segment002047.ts"} {
		if _, ok := parentHLSSegmentIndex(name); !ok {
			t.Fatal("valid segment rejected")
		}
	}
	for _, name := range []string{"segment002048.ts", "segment00000.ts", "segment000000.ts.tmp", "../segment000000.ts", "%2e%2e/segment000000.ts", "stream.m3u8", "segment000000.ts?x=1", "Segment000000.ts"} {
		if _, ok := parentHLSSegmentIndex(name); ok {
			t.Fatal("foreign or temporary segment accepted", name)
		}
	}
	for _, id := range []string{"..", strings.Repeat("A", 32), strings.Repeat("a", 31), "a/" + strings.Repeat("b", 30)} {
		if parentHLSOpaqueID(id) {
			t.Fatal("ambiguous generation id accepted")
		}
	}
}
func TestParentFileHLSConfigurationRejectsRenamedCanonicalRoot(t *testing.T) {
	directory := t.TempDir()
	options := ParentFileHLSOptions{Directory: directory, FFmpegPath: "synthetic-not-executed", FFprobePath: "synthetic-not-executed"}
	handler := &ParentLocalFileHandler{core: &kidsvod.Service{}}
	if runtime.GOOS != "linux" {
		if handler.ConfigureHLS(t.Context(), options, func(context.Context) (ParentFileHLSOptions, error) { return options, nil }) == nil {
			t.Fatal("unsupported platform configured parent-file HLS")
		}
		return
	}
	if err := handler.ConfigureHLS(t.Context(), options, func(context.Context) (ParentFileHLSOptions, error) { return options, nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(handler.CloseHLS)
	current := handler.hlsManager()
	if current.check(t.Context()) != nil {
		t.Fatal("configured root observation invalid")
	}
	if err := os.Rename(directory, directory+"-retired"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(directory + "-retired") })
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if current.check(t.Context()) == nil {
		t.Fatal("replacement cache root adopted")
	}
	if _, err := os.Stat(filepath.Join(directory, current.name)); !os.IsNotExist(err) {
		t.Fatal("configuration recreated state in replacement root")
	}
}
func parentLocalFileHLSUnavailableContract(t *testing.T, v *openapi.Validator) {
	t.Helper()
	var handler *ParentLocalFileHandler
	t.Run("parent local HLS unavailable operations", func(t *testing.T) {
		base := "/api/users/child/library/items/local-item/approved-hls"
		exercise(t, v, contractCall{method: http.MethodPost, target: base, body: map[string]string{"approvalId": "lfa_" + strings.Repeat("a", 64)}, vars: map[string]string{"userID": "child", "itemID": "local-item"}}, handler.StartHLS, 503)
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			exercise(t, v, contractCall{method: method, target: base + "/" + strings.Repeat("a", 32) + "/stream.m3u8", vars: map[string]string{"userID": "child", "itemID": "local-item", "hlsID": strings.Repeat("a", 32), "segment": "stream.m3u8"}}, handler.ServeHLS, 503)
		}
		exercise(t, v, contractCall{method: http.MethodDelete, target: base + "/" + strings.Repeat("a", 32), vars: map[string]string{"userID": "child", "itemID": "local-item", "hlsID": strings.Repeat("a", 32)}}, handler.StopHLS, 503)
	})
}
