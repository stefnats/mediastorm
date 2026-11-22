package audiolanguage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func syntheticPCM() []byte {
	pcm := make([]byte, SampleBytes)
	for i := 0; i < len(pcm); i += 2 {
		binary.LittleEndian.PutUint16(pcm[i:], uint16(2000))
	}
	return pcm
}

func speechReply(code string) map[string]any {
	return map[string]any{"task": "transcribe", "language": code, "duration": 20, "language_probabilities": map[string]float64{code: 0.96},
		"segments": []map[string]any{{"start": 2.0, "end": 12.0, "text": "Synthetic dialogue contains several clearly distinct spoken words.", "no_speech_prob": 0.01, "avg_logprob": -0.1}}}
}

func TestCheckSelectedAudioAgainstRequestedLanguages(t *testing.T) {
	for _, tc := range []struct{ wanted, detected, status, canonical string }{
		{"fr-CA", "fr", "likely_match", "fra"}, {"spa", "en", "likely_mismatch", "eng"},
		{"eng", "es", "likely_mismatch", "spa"}, {"deu", "de", "likely_match", "deu"},
	} {
		t.Run(tc.wanted+"_"+tc.detected, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodPost {
					t.Error("inference must be an explicit POST")
				}
				if err := r.ParseMultipartForm(2 << 20); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				if r.FormValue("language") != "auto" || r.FormValue("response_format") != "verbose_json" || r.FormValue("translate") != "false" {
					t.Error("request biased or translated the sample")
				}
				file, header, err := r.FormFile("file")
				if err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				defer file.Close()
				data, _ := io.ReadAll(file)
				if header.Filename != "audio-sample.wav" || len(data) != SampleBytes+44 || string(data[:4]) != "RIFF" || binary.LittleEndian.Uint32(data[24:]) != 16000 || !bytes.Equal(data[44:], syntheticPCM()) {
					t.Error("wrong audio format or private filename")
				}
				_ = json.NewEncoder(w).Encode(speechReply(tc.detected))
			}))
			defer server.Close()
			checker, err := New(server.URL + "/inference")
			if err != nil {
				t.Fatal(err)
			}
			var offsets []float64
			result, err := checker.Check(t.Context(), tc.wanted, 42, func(_ context.Context, offset float64) ([]byte, error) {
				offsets = append(offsets, offset)
				return syntheticPCM(), nil
			})
			if err != nil || result.Status != tc.status || result.DetectedLanguage != tc.canonical || result.Samples != 2 || result.Confidence != 0.96 || calls.Load() != 2 || len(offsets) != 2 || offsets[0] != 42 || offsets[1] != 77 {
				t.Fatalf("result=%+v err=%v offsets=%v calls=%d", result, err, offsets, calls.Load())
			}
			encoded, _ := json.Marshal(result)
			if strings.Contains(string(encoded), "Synthetic dialogue") || strings.Contains(string(encoded), "segments") {
				t.Fatal("transcript retained in result")
			}
		})
	}
}

func TestUncertainSpeechNeverProducesMismatch(t *testing.T) {
	for _, name := range []string{"low_probability", "unclear_margin", "missing_vad", "music", "brief_speech", "invalid_probability", "conflicting_model_language", "translation", "wrong_duration", "overlapping_speech"} {
		t.Run(name, func(t *testing.T) {
			response := speechReply("en")
			segment := response["segments"].([]map[string]any)[0]
			switch name {
			case "low_probability":
				response["language_probabilities"] = map[string]float64{"en": 0.6}
			case "unclear_margin":
				response["language_probabilities"] = map[string]float64{"en": 0.91, "es": 0.8}
			case "missing_vad":
				delete(segment, "no_speech_prob")
			case "music":
				segment["text"] = "[MUSIC] These are only synthetic music labels and not speech."
			case "brief_speech":
				segment["end"] = 3.0
			case "invalid_probability":
				response["language_probabilities"] = map[string]float64{"en": 2}
			case "conflicting_model_language":
				response["language"] = "fr"
			case "translation":
				response["task"] = "translate"
			case "wrong_duration":
				response["duration"] = 2.0
			case "overlapping_speech":
				segment["end"] = 3.0
				response["segments"] = []map[string]any{segment, segment, segment, segment, segment}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(response) }))
			defer server.Close()
			checker, _ := New(server.URL)
			result, err := checker.Check(t.Context(), "spa", 0, func(context.Context, float64) ([]byte, error) { return syntheticPCM(), nil })
			if err != nil || result.Status != "unknown" || result.DetectedLanguage != "" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestMixedWindowsAndNonSpeechRemainUnknown(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := "en"
		if calls.Add(1) > 1 {
			code = "fr"
		}
		_ = json.NewEncoder(w).Encode(speechReply(code))
	}))
	defer server.Close()
	checker, _ := New(server.URL)
	result, err := checker.Check(t.Context(), "de", 0, func(context.Context, float64) ([]byte, error) { return syntheticPCM(), nil })
	if err != nil || result.Status != "unknown" || result.Reason != "mixed_languages" || result.DetectedLanguage != "" {
		t.Fatalf("mixed windows: %+v %v", result, err)
	}
	for _, pcm := range [][]byte{make([]byte, SampleBytes), syntheticPCM()[:100], append(syntheticPCM(), 0, 0)} {
		before := calls.Load()
		result, err = checker.Check(t.Context(), "fr", 0, func(context.Context, float64) ([]byte, error) { return pcm, nil })
		if err != nil || result.Status != "unknown" || calls.Load() != before {
			t.Fatal("non-speech or incomplete audio reached inference")
		}
	}
}

func TestCheckIsBoundedCanceledAndDoesNotQueue(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _ = json.NewEncoder(w).Encode(speechReply("en")) }))
	defer server.Close()
	checker, _ := New(server.URL)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := checker.Check(t.Context(), "fr", 0, func(context.Context, float64) ([]byte, error) {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return syntheticPCM(), nil
		})
		done <- err
	}()
	<-entered
	_, err := checker.Check(t.Context(), "es", 0, func(context.Context, float64) ([]byte, error) { t.Error("busy check sampled audio"); return nil, nil })
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("busy error=%v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = checker.Check(ctx, "fr", 0, func(context.Context, float64) ([]byte, error) {
		t.Error("canceled check sampled audio")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation was lost")
	}
}

func TestUnavailableOrInvalidInferenceDoesNotInventLanguage(t *testing.T) {
	for _, reply := range []string{"malformed", "oversize", "failure", "redirect"} {
		t.Run(reply, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch reply {
				case "failure":
					w.WriteHeader(503)
				case "redirect":
					w.Header().Set("Location", "https://example.invalid/no-forward")
					w.WriteHeader(307)
				case "oversize":
					_, _ = io.WriteString(w, strings.Repeat("x", maxResponseBytes+1))
				default:
					_, _ = io.WriteString(w, "invalid")
				}
			}))
			defer server.Close()
			checker, _ := New(server.URL)
			result, err := checker.Check(t.Context(), "fr", 0, func(context.Context, float64) ([]byte, error) { return syntheticPCM(), nil })
			if err != nil || result.Status != "unknown" || result.Reason != "analysis_unavailable" {
				t.Fatalf("%+v %v", result, err)
			}
		})
	}
	for _, endpoint := range []string{"", "file:///sample", "http://user:secret@localhost/inference", "http://localhost/inference?token=private"} {
		if _, err := New(endpoint); err == nil {
			t.Fatal("invalid endpoint accepted")
		}
	}
}
