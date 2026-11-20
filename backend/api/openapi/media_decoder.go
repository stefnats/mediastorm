package openapi

import "github.com/getkin/kin-openapi/openapi3filter"

func init() {
	// The registry is process-global and is not safe for concurrent writes.
	// Register the exact playlist contract MIME type during initialization,
	// before validators exist. TS already uses its binary schema's native
	// decoder. Schema/body validation remains enabled for both media types.
	openapi3filter.RegisterBodyDecoder("application/vnd.apple.mpegurl", openapi3filter.PlainBodyDecoder)
}
