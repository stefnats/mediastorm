// Package audiolanguage compares sampled speech with an explicit audio request.
// Model output is a suggestion, never a release report or playback permission.
package audiolanguage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode"

	"novastream/utils/language"
)

const (
	SampleSeconds    = 20
	SampleRate       = 16000
	SampleBytes      = SampleSeconds * SampleRate * 2
	SampleSeparation = 35
	maxResponseBytes = 2 << 20
)

var ErrBusy = errors.New("an audio language check is already running")

// Sampler reads one selected container audio track as mono, 16-bit little-endian
// PCM at SampleRate. The caller owns the selected source and cancellation.
type Sampler func(context.Context, float64) ([]byte, error)

type Result struct {
	Status            string  `json:"status"` // unknown, likely_match, likely_mismatch
	Reason            string  `json:"reason,omitempty"`
	RequestedLanguage string  `json:"requestedLanguage"`
	DetectedLanguage  string  `json:"detectedLanguage,omitempty"`
	Confidence        float64 `json:"confidence,omitempty"`
	Samples           int     `json:"samples"`
}

type Checker struct {
	endpoint string
	client   *http.Client
	busy     chan struct{}
}

// New uses an explicitly configured whisper.cpp /inference endpoint. It does
// not install a model, start a service, discover an endpoint or send a media URL.
func New(endpoint string) (*Checker, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("configure an audio analysis HTTP endpoint without credentials or query parameters")
	}
	transport := &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 35 * time.Second,
		IdleConnTimeout: 30 * time.Second, MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1,
	}
	return &Checker{endpoint: u.String(), busy: make(chan struct{}, 1), client: &http.Client{
		Transport: transport, Timeout: 35 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

// Check requires agreement across two non-overlapping windows. Even strong
// agreement is only a sample-based hint: multilingual scenes, songs, commentary
// and model errors cannot establish a generally mislabelled or defective file.
func (c *Checker) Check(ctx context.Context, requested string, offset float64, sample Sampler) (Result, error) {
	wanted := canonical(requested)
	result := Result{Status: "unknown", RequestedLanguage: wanted, Reason: "insufficient_evidence"}
	if c == nil || sample == nil || wanted == "" || math.IsNaN(offset) || math.IsInf(offset, 0) || offset < 0 {
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	select {
	case c.busy <- struct{}{}:
		defer func() { <-c.busy }()
	default:
		return result, ErrBusy
	}
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var detected string
	confidence := 1.0
	for _, start := range []float64{offset, offset + SampleSeparation} {
		pcm, err := sample(ctx, start)
		if err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			result.Reason = "sample_unavailable"
			return result, nil
		}
		if len(pcm) != SampleBytes || !audible(pcm) {
			result.Reason = "insufficient_audio"
			return result, nil
		}
		observation, err := c.infer(ctx, pcm)
		if err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			result.Reason = "analysis_unavailable"
			return result, nil
		}
		code, probability := observation.supportedLanguage()
		result.Samples++
		if code == "" {
			result.Reason = "insufficient_speech"
			return result, nil
		}
		if detected != "" && code != detected {
			result.Reason = "mixed_languages"
			return result, nil
		}
		detected = code
		confidence = math.Min(confidence, probability)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	result.DetectedLanguage, result.Confidence, result.Reason = detected, confidence, ""
	result.Status = "likely_mismatch"
	if wanted == detected {
		result.Status = "likely_match"
	}
	return result, nil
}

func canonical(value string) string {
	code := language.NormalizeToCode(value)
	if len(code) != 3 || code == "und" || code == "mul" || code == "zxx" {
		return ""
	}
	for _, ch := range code {
		if ch < 'a' || ch > 'z' {
			return ""
		}
	}
	return code
}

func audible(pcm []byte) bool {
	var energy float64
	for i := 0; i+1 < len(pcm); i += 2 {
		value := float64(int16(binary.LittleEndian.Uint16(pcm[i:]))) / 32768
		energy += value * value
	}
	return math.Sqrt(energy/float64(len(pcm)/2)) >= 0.001
}

type inference struct {
	Task          string             `json:"task"`
	Language      string             `json:"language"`
	Duration      float64            `json:"duration"`
	Probabilities map[string]float64 `json:"language_probabilities"`
	Segments      []struct {
		Start          float64  `json:"start"`
		End            float64  `json:"end"`
		Text           string   `json:"text"`
		NoSpeech       *float64 `json:"no_speech_prob"`
		LogProbability *float64 `json:"avg_logprob"`
	} `json:"segments"`
}

func (c *Checker) infer(ctx context.Context, pcm []byte) (inference, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range map[string]string{"language": "auto", "response_format": "verbose_json", "translate": "false", "temperature": "0", "no_timestamps": "false"} {
		if err := writer.WriteField(key, value); err != nil {
			return inference{}, err
		}
	}
	part, err := writer.CreateFormFile("file", "audio-sample.wav")
	if err != nil {
		return inference{}, err
	}
	if _, err = part.Write(wav(pcm)); err != nil {
		return inference{}, err
	}
	if err = writer.Close(); err != nil {
		return inference{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, &body)
	if err != nil {
		return inference{}, errors.New("could not create audio analysis request")
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	response, err := c.client.Do(req)
	if err != nil {
		return inference{}, errors.New("audio analysis unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return inference{}, errors.New("audio analysis unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(data) > maxResponseBytes {
		return inference{}, errors.New("invalid audio analysis response")
	}
	var result inference
	if json.Unmarshal(data, &result) != nil {
		return inference{}, errors.New("invalid audio analysis response")
	}
	return result, nil
}

func (value inference) supportedLanguage() (string, float64) {
	code := canonical(value.Language)
	if value.Task != "transcribe" || code == "" || !finite(value.Duration) || math.Abs(value.Duration-SampleSeconds) > 1 {
		return "", 0
	}
	var probability, runnerUp float64
	for name, p := range value.Probabilities {
		if !finite(p) || p < 0 || p > 1 {
			return "", 0
		}
		if canonical(name) == code {
			probability = math.Max(probability, p)
		} else {
			runnerUp = math.Max(runnerUp, p)
		}
	}
	if probability < 0.9 || probability-runnerUp < 0.2 {
		return "", 0
	}
	type interval struct{ start, end float64 }
	var intervals []interval
	for _, s := range value.Segments {
		if !finite(s.Start) || !finite(s.End) || s.Start < 0 || s.End > SampleSeconds+0.1 || s.End <= s.Start || s.NoSpeech == nil || s.LogProbability == nil {
			continue
		}
		if !finite(*s.NoSpeech) || *s.NoSpeech < 0 || *s.NoSpeech > 0.2 || !finite(*s.LogProbability) || *s.LogProbability > 0 || *s.LogProbability < -0.7 {
			continue
		}
		letters, distinct := 0, map[rune]bool{}
		for _, ch := range s.Text {
			if unicode.IsLetter(ch) {
				letters++
				distinct[unicode.ToLower(ch)] = true
			}
		}
		if letters < 20 || len(distinct) < 5 || strings.ContainsAny(s.Text, "[]<>♪♫") {
			continue
		}
		intervals = append(intervals, interval{s.Start, math.Min(s.End, SampleSeconds)})
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i].start < intervals[j].start })
	var coverage, end float64
	for _, item := range intervals {
		if item.end > end {
			coverage += item.end - math.Max(end, item.start)
			end = item.end
		}
	}
	if coverage < 4 {
		return "", 0
	}
	return code, probability
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func wav(pcm []byte) []byte {
	data := make([]byte, 44+len(pcm))
	copy(data, "RIFF")
	binary.LittleEndian.PutUint32(data[4:], uint32(36+len(pcm)))
	copy(data[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(data[16:], 16)
	binary.LittleEndian.PutUint16(data[20:], 1)
	binary.LittleEndian.PutUint16(data[22:], 1)
	binary.LittleEndian.PutUint32(data[24:], SampleRate)
	binary.LittleEndian.PutUint32(data[28:], SampleRate*2)
	binary.LittleEndian.PutUint16(data[32:], 2)
	binary.LittleEndian.PutUint16(data[34:], 16)
	copy(data[36:], "data")
	binary.LittleEndian.PutUint32(data[40:], uint32(len(pcm)))
	copy(data[44:], pcm)
	return data
}
