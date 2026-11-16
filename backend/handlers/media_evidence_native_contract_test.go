package handlers

import (
	"encoding/json"
	"novastream/api/openapi"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeMediaEvidenceComponentsMatchBackendSchema(t *testing.T) {
	// These are read-only native serializer response subsets. Validate additive
	// components only; legacy required parent fields are intentionally omitted.
	paths, err := filepath.Glob("testdata/media_evidence_native/*.json")
	if err != nil || len(paths) != 8 {
		t.Fatal("eight native evidence fixtures are required")
	}
	doc, err := openapi.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err = json.Unmarshal(data, &body); err != nil {
				t.Fatal(err)
			}
			checked := false
			for field, schema := range map[string]string{"originalLanguageEvidence": "OriginalLanguageEvidence", "mediaEvidence": "MediaEvidence"} {
				if value, present := body[field]; present {
					if err = doc.Components.Schemas[schema].Value.VisitJSON(value); err != nil {
						t.Fatal(err)
					}
					checked = true
				}
			}
			if !checked {
				if body["version"] != nil {
					if err = doc.Components.Schemas["MediaEvidence"].Value.VisitJSON(body); err != nil {
						t.Fatal(err)
					}
				} else {
					t.Fatal("fixture has no additive evidence")
				}
			}
		})
	}
}

func TestGoMediaEvidenceResponsesMatchFullParentSchema(t *testing.T) {
	data, err := os.ReadFile("testdata/media_evidence_go/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Files map[string]string `json:"files"`
	}
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	doc, err := openapi.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	mapping := map[string]string{"Title": "Title", "SourceResult": "IndexerResult", "PrequeueStatus": "PrequeueStatusResponse", "UsenetHealth": "ExplicitHealthResponse", "DebridHealth": "ExplicitHealthResponse"}
	for file, kind := range manifest.Files {
		t.Run(file, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("testdata/media_evidence_go", file))
			if err != nil {
				t.Fatal(err)
			}
			var body any
			if err = json.Unmarshal(data, &body); err != nil {
				t.Fatal(err)
			}
			if err = doc.Components.Schemas[mapping[kind]].Value.VisitJSON(body); err != nil {
				t.Fatal(err)
			}
		})
	}
}
