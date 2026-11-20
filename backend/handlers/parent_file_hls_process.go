package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

type parentHLSInventory struct {
	Streams []struct {
		Index    *int   `json:"index"`
		Type     string `json:"codec_type"`
		Codec    string `json:"codec_name"`
		Profile  string `json:"profile"`
		Width    int    `json:"width"`
		Height   int    `json:"height"`
		Pixel    string `json:"pix_fmt"`
		Field    string `json:"field_order"`
		Color    string `json:"color_transfer"`
		Channels int    `json:"channels"`
		Rate     string `json:"sample_rate"`
	} `json:"streams"`
	Format struct {
		Name     string `json:"format_name"`
		Duration string `json:"duration"`
	} `json:"format"`
}

func validParentHLSInventory(v parentHLSInventory) (int, int, bool) {
	duration, err := strconv.ParseFloat(v.Format.Duration, 64)
	if err != nil || math.IsNaN(duration) || math.IsInf(duration, 0) || duration <= 0 || duration > 7200 || math.Ceil(duration/4) > 2048 || len(v.Streams) != 2 {
		return 0, 0, false
	}
	switch v.Format.Name {
	case "mpegts", "matroska,webm", "matroska", "webm", "mov,mp4,m4a,3gp,3g2,mj2", "mov", "mp4":
	default:
		return 0, 0, false
	}
	video, audio := -1, -1
	for _, stream := range v.Streams {
		if stream.Index == nil || *stream.Index < 0 || *stream.Index > 127 {
			return 0, 0, false
		}
		switch stream.Type {
		case "video":
			if video != -1 || stream.Codec != "h264" || stream.Width < 1 || stream.Height < 1 || stream.Width > 4096 || stream.Height > 2160 || stream.Field != "progressive" || stream.Pixel != "yuv420p" && stream.Pixel != "yuvj420p" || stream.Profile != "Constrained Baseline" && stream.Profile != "Baseline" && stream.Profile != "Main" && stream.Profile != "High" || stream.Color == "smpte2084" || stream.Color == "arib-std-b67" {
				return 0, 0, false
			}
			video = *stream.Index
		case "audio":
			rate, e := strconv.Atoi(stream.Rate)
			if audio != -1 || stream.Codec != "aac" || stream.Profile != "LC" || stream.Channels < 1 || stream.Channels > 8 || e != nil || rate < 8000 || rate > 192000 {
				return 0, 0, false
			}
			audio = *stream.Index
		default:
			return 0, 0, false
		}
	}
	return video, audio, video >= 0 && audio >= 0 && video != audio
}
func (s *parentFileHLSSession) probe(ctx context.Context) (int, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if s.check(ctx) != nil {
		return 0, 0, errParentFileHLS
	}
	stdout, stderr := &parentHLSCapture{limit: 64 << 10, cancel: cancel}, &parentHLSCapture{limit: 4 << 10, cancel: cancel}
	args := []string{"-hide_banner", "-v", "error", "-max_alloc", "67108864", "-threads", "1", "-protocol_whitelist", "fd", "-format_whitelist", "matroska,webm,mov,mpegts", "-fd", "3", "-i", "fd:", "-show_streams", "-show_format", "-show_entries", "stream=index,codec_type,codec_name,profile,width,height,pix_fmt,field_order,color_transfer,channels,sample_rate:format=format_name,duration", "-of", "json"}
	err := s.lease.Run(ctx, s.manager.options.FFprobePath, args, s.dir, stdout, stderr)
	raw, overflow := stdout.bytes()
	logs, _ := stderr.bytes()
	var facts parentHLSInventory
	if err != nil || overflow || len(logs) != 0 || json.Unmarshal(raw, &facts) != nil || s.check(ctx) != nil {
		return 0, 0, errParentFileHLS
	}
	video, audio, ok := validParentHLSInventory(facts)
	if !ok {
		return 0, 0, errParentFileHLS
	}
	return video, audio, nil
}
func (s *parentFileHLSSession) outputBounds() error {
	s.mu.Lock()
	root := s.root
	s.mu.Unlock()
	if root == nil {
		return errParentFileHLS
	}
	dir, err := root.Open(".")
	if err != nil {
		return errParentFileHLS
	}
	entries, err := dir.ReadDir(2052)
	dir.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return errParentFileHLS
	}
	if len(entries) > 2050 {
		return errParentFileHLS
	}
	var total int64
	for _, entry := range entries {
		name := entry.Name()
		closed := strings.TrimSuffix(name, ".tmp")
		limit := int64(64 << 20)
		if closed == "stream.m3u8" {
			limit = 64 << 10
		} else if _, ok := parentHLSSegmentIndex(closed); !ok {
			return errParentFileHLS
		}
		info, e := root.Lstat(name)
		if e != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > limit {
			return errParentFileHLS
		}
		total += info.Size()
		if total > s.lease.OutputLimit() {
			return errParentFileHLS
		}
		if name == closed && name != "stream.m3u8" {
			s.mu.Lock()
			old := s.segments[name]
			if old == nil {
				s.segments[name] = info
			}
			s.mu.Unlock()
			if old != nil && !sameParentHLSFile(old, info) {
				return errParentFileHLS
			}
		}
	}
	return nil
}
func (s *parentFileHLSSession) start(ctx context.Context, video, audio int) error {
	if s.check(ctx) != nil || !parentHLSFreeSpace(s.dir, s.lease.OutputLimit()) {
		return errParentFileHLS
	}
	s.mu.Lock()
	if s.started || s.ctx.Err() != nil {
		s.mu.Unlock()
		return errParentFileHLS
	}
	s.started = true
	s.mu.Unlock()
	go func() {
		defer close(s.workerDone)
		args := []string{"-nostdin", "-hide_banner", "-loglevel", "error", "-xerror", "-max_alloc", "67108864", "-threads", "1", "-filter_threads", "1", "-protocol_whitelist", "fd", "-format_whitelist", "matroska,webm,mov,mpegts", "-fd", "3", "-i", "fd:", "-map", "0:" + strconv.Itoa(video), "-map", "0:" + strconv.Itoa(audio), "-sn", "-dn", "-c:v", "copy", "-c:a", "copy", "-f", "hls", "-hls_time", "4", "-hls_list_size", "0", "-hls_flags", "independent_segments+temp_file", "-start_number", "0", "-hls_segment_filename", "segment%06d.ts", "stream.m3u8"}
		logs := &parentHLSCapture{limit: 4 << 10, cancel: s.cancel}
		monitorCtx, stopMonitor := context.WithCancel(s.ctx)
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-monitorCtx.Done():
					return
				case <-ticker.C:
					if s.outputBounds() != nil {
						s.cancel()
						s.lease.Cancel()
						return
					}
				}
			}
		}()
		err := s.lease.Run(s.ctx, s.manager.options.FFmpegPath, args, s.dir, nil, logs)
		stopMonitor()
		<-monitorDone
		raw, overflow := logs.bytes()
		if err != nil || overflow || len(raw) != 0 || s.check(s.ctx) != nil || s.outputBounds() != nil {
			s.cancel()
			return
		}
		playlist, e := s.readPlaylist(s.ctx)
		if e != nil || !strings.Contains(string(playlist), "#EXT-X-ENDLIST") {
			s.cancel()
			return
		}
		s.mu.Lock()
		s.completed = true
		s.mu.Unlock()
	}()
	return nil
}
