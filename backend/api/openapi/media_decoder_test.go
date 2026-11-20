package openapi

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestValidatorHLSMediaUsesExactEmbeddedResponseSchemas(t *testing.T) {
	for _, tc := range []struct {
		name, mime string
		body       []byte
	}{
		{"stream.m3u8", "application/vnd.apple.mpegurl", []byte("#EXTM3U\n#EXTINF:1,\nsegment000000.ts\n#EXT-X-ENDLIST\n")},
		{"segment000000.ts", "video/mp2t", []byte{0x47, 0x00, 0xff, 0x00}},
	} {
		t.Run(tc.mime, func(t *testing.T) {
			v, err := NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, "/api/users/child/library/items/item/approved-hls/"+strings.Repeat("a", 32)+"/"+tc.name, nil)
			if err = v.ValidateRequest(req, nil); err != nil {
				t.Fatal(err)
			}
			header := http.Header{"Content-Type": []string{tc.mime}, "Content-Length": []string{strconv.Itoa(len(tc.body))}}
			if err = v.ValidateResponse(req, http.StatusOK, header, tc.body); err != nil {
				t.Fatal("exact documented HLS media rejected", err)
			}
			if !v.Covered()["serveParentLocalFileHLS"] {
				t.Fatal("actual media response did not record route coverage")
			}
		})
	}
}

func TestValidatorHLSMediaRetainsMIMEStatusHeaderAndBodyConstraints(t *testing.T) {
	for _, mime := range []string{"application/vnd.apple.mpegurl", "video/mp2t"} {
		t.Run(mime, func(t *testing.T) {
			v, err := NewValidator(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			name := "stream.m3u8"
			if mime == "video/mp2t" {
				name = "segment000000.ts"
			}
			req := httptest.NewRequest(http.MethodGet, "/api/users/child/library/items/item/approved-hls/"+strings.Repeat("a", 32)+"/"+name, nil)
			header := http.Header{"Content-Type": []string{mime}, "Content-Length": []string{"2"}}
			if err = v.ValidateResponse(req, http.StatusCreated, header, []byte("ab")); err == nil {
				t.Fatal("undocumented media status accepted")
			}
			wrong := header.Clone()
			wrong.Set("Content-Type", "application/json")
			if err = v.ValidateResponse(req, http.StatusOK, wrong, []byte(`{"not":"media"}`)); err == nil {
				t.Fatal("undeclared media content type accepted")
			}
			badLength := header.Clone()
			badLength.Set("Content-Length", "-1")
			if err = v.ValidateResponse(req, http.StatusOK, badLength, []byte("ab")); err == nil {
				t.Fatal("invalid documented response header accepted")
			}
			// Tighten only this validator's actual embedded string schema. A decoder
			// must pass its body into schema validation instead of bypassing it.
			operation := v.Doc.Paths.Value("/api/users/{userId}/library/items/{itemId}/approved-hls/{hlsId}/{segment}").Get
			schema := operation.Responses.Status(http.StatusOK).Value.Content[mime].Schema.Value
			limit := uint64(1)
			schema.MaxLength = &limit
			if err = v.ValidateResponse(req, http.StatusOK, header, []byte("ab")); err == nil {
				t.Fatal("decoded HLS body bypassed actual schema constraint")
			}
		})
	}
}
