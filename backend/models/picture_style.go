package models

// PictureStyleSnapshot holds only descriptors from explicitly chosen pictures.
// No image URLs, inferred actors, title ratings or viewing history are stored.
type PictureStyleSnapshot struct {
	Version int                  `json:"version"`
	Enabled bool                 `json:"enabled"`
	Samples []PictureStyleSample `json:"samples,omitempty"`
}

type PictureStyleSample struct {
	Aliases  []string `json:"aliases,omitempty"`
	TitleID  string   `json:"titleId"`
	Kind     string   `json:"kind"`
	Language string   `json:"language,omitempty"`
	Textless bool     `json:"textless"`
}

const MaxPictureStyleSamples = 128
const MinPictureStyleSamples = 3
