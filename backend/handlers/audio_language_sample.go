package handlers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"time"

	"novastream/services/audiolanguage"
)

type boundedAudioOutput struct{ bytes.Buffer }

func (b *boundedAudioOutput) Write(data []byte) (int, error) {
	if b.Len()+len(data) > audiolanguage.SampleBytes {
		return 0, errors.New("audio sample exceeds its limit")
	}
	return b.Buffer.Write(data)
}

// SamplePreparedAudio decodes a bounded window of the exact selected container
// track. Existing provider/WebDAV resolution is reused; no NZB or release search
// is initiated. Unsupported non-seekable sources remain unknown to the checker.
func (h *VideoHandler) SamplePreparedAudio(ctx context.Context, path string, track int, offset float64) ([]byte, error) {
	if h == nil || h.ffmpegPath == "" || track < 0 || !finiteAudioOffset(offset) {
		return nil, errors.New("audio sampling unavailable")
	}
	ctx, finish, err := h.beginPreparedMediaWork(ctx)
	if err != nil {
		return nil, err
	}
	defer finish()
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	input, err := h.resolveSeekableURL(ctx, cleanCropDetectPath(path))
	if err != nil {
		return nil, errors.New("selected audio source is not seekable")
	}
	args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-threads", "1", "-rw_timeout", "10000000", "-ss", strconv.FormatFloat(offset, 'f', 3, 64), "-i", input,
		"-t", strconv.Itoa(audiolanguage.SampleSeconds), "-map", fmt.Sprintf("0:%d", track), "-vn", "-sn", "-dn", "-ac", "1", "-ar", strconv.Itoa(audiolanguage.SampleRate), "-f", "s16le", "pipe:1"}
	command := exec.CommandContext(ctx, h.ffmpegPath, args...)
	command.WaitDelay = 2 * time.Second
	output := &boundedAudioOutput{}
	command.Stdout = output
	// FFmpeg errors may contain private source URLs; retain no stderr transcript.
	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("could not decode the selected audio sample")
	}
	return output.Bytes(), nil
}
