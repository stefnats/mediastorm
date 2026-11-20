package localmedia

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
)

var ErrFamilyLocalProbeUnavailable = errors.New("checked local probe unavailable")

const (
	familyLocalProbeABI               = "mediastorm-local-probe/1"
	familyLocalProbeMaxInput    int64 = 8 << 30
	familyLocalProbeOutputLimit       = 256 << 10
	familyLocalProbeErrorLimit        = 64 << 10
)

// This is explicit server build configuration, never a media or request proof.
// There is no default path discovery, runtime compilation or weak fallback.
type FamilyLocalProbeConfig struct {
	HelperPath, HelperSHA256   string
	FFprobePath, FFprobeSHA256 string
}
type FamilyLocalProbeTools struct{ *familyLocalProbeToolsState }
type familyLocalProbeToolsState struct {
	mu                    sync.Mutex
	self                  *FamilyLocalProbeTools
	ctx                   context.Context
	cancel                context.CancelFunc
	helper, probe         *os.File
	helperInfo, probeInfo os.FileInfo
	config                FamilyLocalProbeConfig
	jobs                  map[*familyLocalProbeToken]bool
	closed                bool
	once                  sync.Once
}
type familyLocalProbeToken struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (FamilyLocalProbeTools) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyLocalProbeTools(opaque)"))
}
func (FamilyLocalProbeTools) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyLocalProbeTools) UnmarshalJSON([]byte) error  { return ErrFamilyLocalProbeUnavailable }
func (t *FamilyLocalProbeTools) valid() bool {
	return t != nil && t.familyLocalProbeToolsState != nil && t.self == t
}
func NewFamilyLocalProbeTools(config FamilyLocalProbeConfig) (*FamilyLocalProbeTools, error) {
	return newFamilyLocalProbeTools(config)
}
func (t *FamilyLocalProbeTools) Close() {
	if !t.valid() {
		return
	}
	t.once.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.cancel()
		jobs := make([]*familyLocalProbeToken, 0, len(t.jobs))
		for job := range t.jobs {
			jobs = append(jobs, job)
		}
		t.mu.Unlock()
		for _, job := range jobs {
			job.cancel()
		}
		for _, job := range jobs {
			<-job.done
		}
		if t.helper != nil {
			_ = t.helper.Close()
		}
		if t.probe != nil {
			_ = t.probe.Close()
		}
	})
}

// Technical metadata only. It carries no path, raw tags, catalogue identity,
// speech-language claim, classification or permission to replay an input.
type FamilyLocalProbeResult struct {
	Container       string                   `json:"container"`
	DurationSeconds *float64                 `json:"durationSeconds,omitempty"`
	Streams         []FamilyLocalProbeStream `json:"streams"`
}
type FamilyLocalProbeStream struct {
	Index            int     `json:"index"`
	Kind             string  `json:"kind"`
	Codec            string  `json:"codec"`
	Width            int     `json:"width,omitempty"`
	Height           int     `json:"height,omitempty"`
	Channels         int     `json:"channels,omitempty"`
	SampleRate       int     `json:"sampleRate,omitempty"`
	FrameRate        float64 `json:"frameRate,omitempty"`
	DeclaredLanguage string  `json:"declaredLanguage,omitempty"`
	Default          bool    `json:"default,omitempty"`
	Forced           bool    `json:"forced,omitempty"`
}

func reduceFamilyLocalProbe(raw []byte, format string) (FamilyLocalProbeResult, error) {
	var empty FamilyLocalProbeResult
	if len(raw) == 0 || len(raw) > familyLocalProbeOutputLimit || (format != "mpegts" && format != "matroska" && format != "mov") {
		return empty, ErrFamilyLocalProbeUnavailable
	}
	var wire struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
		Streams []struct {
			Index                   *int   `json:"index"`
			Kind                    string `json:"codec_type"`
			Codec                   string `json:"codec_name"`
			Width, Height, Channels int
			SampleRate              string                        `json:"sample_rate"`
			FrameRate               string                        `json:"avg_frame_rate"`
			Tags                    map[string]string             `json:"tags"`
			Disposition             struct{ Default, Forced int } `json:"disposition"`
		} `json:"streams"`
	}
	if json.Unmarshal(raw, &wire) != nil || len(wire.Streams) == 0 || len(wire.Streams) > 128 {
		return empty, ErrFamilyLocalProbeUnavailable
	}
	out := FamilyLocalProbeResult{Container: format, Streams: make([]FamilyLocalProbeStream, 0, len(wire.Streams))}
	if wire.Format.Duration != "" && wire.Format.Duration != "N/A" {
		value, err := strconv.ParseFloat(wire.Format.Duration, 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 366*24*3600 {
			return empty, ErrFamilyLocalProbeUnavailable
		}
		out.DurationSeconds = &value
	}
	seen := map[int]bool{}
	for _, s := range wire.Streams {
		if s.Kind != "video" && s.Kind != "audio" && s.Kind != "subtitle" {
			continue
		}
		if s.Index == nil || *s.Index < 0 || *s.Index > 1023 || seen[*s.Index] || len(s.Codec) == 0 || len(s.Codec) > 48 {
			return empty, ErrFamilyLocalProbeUnavailable
		}
		for _, c := range s.Codec {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
				return empty, ErrFamilyLocalProbeUnavailable
			}
		}
		if s.Width < 0 || s.Height < 0 || s.Width > 16384 || s.Height > 16384 || s.Channels < 0 || s.Channels > 64 || s.Disposition.Default < 0 || s.Disposition.Default > 1 || s.Disposition.Forced < 0 || s.Disposition.Forced > 1 {
			return empty, ErrFamilyLocalProbeUnavailable
		}
		seen[*s.Index] = true
		stream := FamilyLocalProbeStream{Index: *s.Index, Kind: s.Kind, Codec: s.Codec, Width: s.Width, Height: s.Height, Channels: s.Channels, Default: s.Disposition.Default == 1, Forced: s.Disposition.Forced == 1}
		if s.SampleRate != "" && s.SampleRate != "N/A" {
			n, err := strconv.Atoi(s.SampleRate)
			if err != nil || n < 0 || n > 768000 {
				return empty, ErrFamilyLocalProbeUnavailable
			}
			stream.SampleRate = n
		}
		if s.FrameRate != "" && s.FrameRate != "0/0" {
			parts := strings.Split(s.FrameRate, "/")
			if len(parts) != 2 || len(s.FrameRate) > 32 {
				return empty, ErrFamilyLocalProbeUnavailable
			}
			n, e := strconv.ParseUint(parts[0], 10, 32)
			den, e2 := strconv.ParseUint(parts[1], 10, 32)
			if e != nil || e2 != nil || den == 0 || float64(n)/float64(den) > 1000 {
				return empty, ErrFamilyLocalProbeUnavailable
			}
			stream.FrameRate = float64(n) / float64(den)
		}
		language := strings.ToLower(s.Tags["language"])
		if len(language) >= 2 && len(language) <= 35 {
			valid := true
			for _, c := range language {
				if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
					valid = false
					break
				}
			}
			if valid {
				stream.DeclaredLanguage = language
			}
		}
		out.Streams = append(out.Streams, stream)
	}
	if len(out.Streams) == 0 {
		return empty, ErrFamilyLocalProbeUnavailable
	}
	return out, nil
}
