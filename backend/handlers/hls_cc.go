package handlers

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"novastream/internal/settingscommit"
)

// CC (Closed Caption) support for live TV streams.
// EIA-608 closed captions are embedded in H.264 SEI NAL units (ATSC A53 Part 4)
// and are NOT detectable by ffprobe as separate streams.
//
// Extraction prefers ccextractor (bitstream parse, cheap) feeding only new
// segment bytes once. When ccextractor is unavailable, falls back to a small
// sliding-window ffmpeg decode (never re-decode the whole session).

// segmentNumRe extracts the numeric portion from segment filenames like "segment12.ts"
var segmentNumRe = regexp.MustCompile(`segment(\d+)\.ts$`)

// maxFallbackWindowBytes caps the ffmpeg fallback window so we never re-decode
// a long-lived concat file. ~12 MB is roughly several live segments.
const maxFallbackWindowBytes = 12 * 1024 * 1024

// listLiveSegments returns sorted segment paths currently on disk.
func listLiveSegments(outputDir string) []string {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil
	}
	type segInfo struct {
		num  int
		path string
	}
	var segments []segInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		m := segmentNumRe.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		num, _ := strconv.Atoi(m[1])
		segments = append(segments, segInfo{num: num, path: filepath.Join(outputDir, entry.Name())})
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].num < segments[j].num
	})
	paths := make([]string, 0, len(segments))
	for _, seg := range segments {
		paths = append(paths, seg.path)
	}
	return paths
}

// waitForLiveSegment waits until at least one TS segment exists or ctx is done.
func waitForLiveSegment(ctx context.Context, outputDir string) string {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if segs := listLiveSegments(outputDir); len(segs) > 0 {
			return segs[len(segs)-1]
		}
		select {
		case <-ctx.Done():
			return ""
		case <-ticker.C:
		}
	}
}

// detectClosedCaptionsInSegments sniffs local HLS segments for EIA-608.
// Prefer ccextractor (no pixel decode); fall back to a short ffmpeg probe on one segment.
func detectClosedCaptionsInSegments(ctx context.Context, outputDir, ffmpegPath string) bool {
	segPath := waitForLiveSegment(ctx, outputDir)
	if segPath == "" {
		return false
	}

	if ccPath, err := exec.LookPath("ccextractor"); err == nil {
		tmpSRT := filepath.Join(outputDir, "cc_detect.srt")
		defer os.Remove(tmpSRT)
		detectCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(detectCtx, ccPath, // #nosec G204
			segPath,
			"--out", "srt",
			"-o", tmpSRT,
		)
		_ = cmd.Run()
		if data, err := os.ReadFile(tmpSRT); err == nil && strings.TrimSpace(string(data)) != "" {
			return true
		}
		// Empty SRT does not always mean no CC (some streams need more data);
		// fall through to ffmpeg showinfo on the same local segment.
	}

	if strings.TrimSpace(ffmpegPath) == "" {
		var err error
		ffmpegPath, err = exec.LookPath("ffmpeg")
		if err != nil {
			return false
		}
	}

	detectCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(detectCtx, ffmpegPath, // #nosec G204
		"-hide_banner",
		"-loglevel", "verbose",
		"-i", segPath,
		"-vf", "showinfo",
		"-f", "null",
		"-t", "3",
		"-",
	)
	stderr, writer, err := ordinaryHLSStderr(cmd)
	if err != nil {
		return false
	}
	defer stderr.Close()
	defer writer.Close()
	if err := cmd.Start(); err != nil {
		return false
	}
	_ = writer.Close()
	scanner := bufio.NewScanner(stderr)
	found := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "ATSC A53 Part 4 Closed Captions") ||
			strings.Contains(line, "Closed Captions") ||
			strings.Contains(line, "cc_data") {
			found = true
			break
		}
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return found
}

// ccExtractor feeds local live TS segments into a long-lived ccextractor process
// (preferred) or a bounded ffmpeg sliding window (fallback).
type ccExtractor struct {
	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	done        chan struct{}
	processDone chan struct{}
	stopSignal  func()
	signalOnce  sync.Once
	closeWork   sync.WaitGroup
	files       map[*os.File]struct{}
	outputPath  string
	outputDir   string
	ffmpegPath  string
	ccPath      string // empty => ffmpeg fallback
	cmd         *exec.Cmd
	stdin       *os.File
	childInput  *os.File
	running     bool
	seenSegs    map[int]bool
	lastMaxSeen int
	mode        string // "ccextractor" or "ffmpeg"
}

// startCCExtraction is called only inside the original generation's already
// registered constructor actor. Its done joins the loop, actual Wait and IO.
func startCCExtraction(ctx context.Context, outputDir string) (*ccExtractor, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errOrdinaryHLSClosed
	}
	extractCtx, cancel := context.WithCancel(ctx)
	ext := &ccExtractor{
		ctx: extractCtx, cancel: cancel, done: make(chan struct{}), processDone: make(chan struct{}),
		outputPath: filepath.Join(outputDir, "captions.srt"), outputDir: outputDir,
		files: make(map[*os.File]struct{}), seenSegs: make(map[int]bool), lastMaxSeen: -1,
	}
	ext.stopSignal = familyMediaAfterFunc(extractCtx, ext.shutdownProcess)
	fail := func(err error) (*ccExtractor, error) {
		ext.shutdownProcess()
		ext.stopSignal()
		ext.closeWork.Wait()
		close(ext.done)
		return nil, err
	}
	if ccPath, err := exec.LookPath("ccextractor"); err == nil {
		ext.ccPath, ext.mode = ccPath, "ccextractor"
		if err := ext.startCCExtractorProcess(extractCtx); err != nil {
			return fail(err)
		}
	} else {
		ffmpegPath, err := exec.LookPath("ffmpeg")
		if err != nil {
			return fail(fmt.Errorf("neither ccextractor nor ffmpeg found for live CC extraction"))
		}
		ext.ffmpegPath, ext.mode = ffmpegPath, "ffmpeg"
		close(ext.processDone) // No persistent process exists in fallback mode.
		log.Printf("[hls-cc] ccextractor not found; using bounded ffmpeg sliding-window fallback for %s", outputDir)
	}
	go func() {
		defer close(ext.done)
		ext.extractionLoop(extractCtx)
		ext.shutdownProcess()
		<-ext.processDone
		ext.stopSignal()
		ext.closeWork.Wait()
	}()
	return ext, nil
}

func (e *ccExtractor) startCCExtractorProcess(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, e.ccPath, // #nosec G204
		"--stdin", "--input", "ts", "-s", "120", "--out", "srt", "-o", e.outputPath, "--forceflush", "--koc")
	cmd.Dir = e.outputDir
	childInput, stdin, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("ccextractor stdin: %w", err)
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = childInput, io.Discard, io.Discard
	e.mu.Lock()
	if e.closed || ctx.Err() != nil {
		e.mu.Unlock()
		_ = childInput.Close()
		_ = stdin.Close()
		return errOrdinaryHLSClosed
	}
	e.cmd, e.stdin, e.childInput = cmd, stdin, childInput
	err = cmd.Start() // Same mutex as the actual Process capture on shutdown.
	e.running = err == nil
	e.mu.Unlock()
	_ = childInput.Close() // Also close the parent endpoint after a failed Start.
	if err != nil {
		return fmt.Errorf("start ccextractor: %w", err)
	}
	go func() {
		defer close(e.processDone)
		err := cmd.Wait()
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			log.Printf("[hls-cc] ccextractor exited for %s: %v", e.outputDir, err)
		}
	}()
	return nil
}

func (e *ccExtractor) extractionLoop(ctx context.Context) {
	log.Printf("[hls-cc] starting %s extraction loop for %s", e.mode, e.outputDir)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var persistentEnded <-chan struct{}
	if e.mode == "ccextractor" {
		persistentEnded = e.processDone
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-persistentEnded:
			return
		case <-ticker.C:
			if e.mode == "ccextractor" {
				e.feedNewSegmentsToCCExtractor()
			} else {
				e.processFallbackWindow(ctx)
			}
		}
	}
}

// Each retained descriptor belongs to this actual loop, not to path DATA.
// Cancellation starts every original Close before any external owner joins.
func (e *ccExtractor) retainFile(file *os.File) error {
	e.mu.Lock()
	if e.closed || e.ctx.Err() != nil || len(e.files) >= 2 {
		e.mu.Unlock()
		_ = file.Close()
		return errOrdinaryHLSClosed
	}
	e.files[file] = struct{}{}
	e.mu.Unlock()
	return nil
}
func (e *ccExtractor) closeFile(file *os.File) error {
	err := file.Close()
	e.mu.Lock()
	delete(e.files, file)
	e.mu.Unlock()
	return err
}
func (e *ccExtractor) openSegment(path string) (*os.File, error) {
	if e.ctx.Err() != nil {
		return nil, errOrdinaryHLSClosed
	}
	file, err := settingscommit.OpenRegularGuardData(path)
	if err != nil {
		return nil, err
	}
	if err = e.retainFile(file); err != nil {
		return nil, err
	}
	return file, nil
}

func (e *ccExtractor) feedNewSegmentsToCCExtractor() {
	e.mu.Lock()
	stdin, running := e.stdin, e.running && !e.closed
	e.mu.Unlock()
	if !running || stdin == nil {
		return
	}
	paths := listLiveSegments(e.outputDir)
	present := make(map[int]bool, len(paths))
	for _, path := range paths {
		match := segmentNumRe.FindStringSubmatch(filepath.Base(path))
		number, _ := strconv.Atoi(match[1])
		present[number] = true
	}
	// Keep only the current real on-disk window, not session-length history.
	e.mu.Lock()
	for number := range e.seenSegs {
		if !present[number] {
			delete(e.seenSegs, number)
		}
	}
	e.mu.Unlock()
	for _, path := range paths {
		match := segmentNumRe.FindStringSubmatch(filepath.Base(path))
		number, _ := strconv.Atoi(match[1])
		e.mu.Lock()
		seen := e.seenSegs[number]
		e.mu.Unlock()
		if seen {
			continue
		}
		file, err := e.openSegment(path)
		if err != nil {
			if e.ctx.Err() != nil {
				return
			}
			continue
		}
		_, err = io.Copy(stdin, file)
		closeErr := e.closeFile(file)
		if err != nil || closeErr != nil || e.ctx.Err() != nil {
			return
		}
		e.mu.Lock()
		e.seenSegs[number] = true
		e.mu.Unlock()
	}
}

// processFallbackWindow keeps the actual bytes copied under the existing cap,
// even if a selected segment grows after its initial stat.
func (e *ccExtractor) processFallbackWindow(ctx context.Context) {
	paths := listLiveSegments(e.outputDir)
	var window []string
	var selected int64
	for i := len(paths) - 1; i >= 0; i-- {
		info, err := os.Stat(paths[i])
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if info.Size() > maxFallbackWindowBytes-selected {
			break
		}
		window = append([]string{paths[i]}, window...)
		selected += info.Size()
	}
	if len(window) == 0 || ctx.Err() != nil {
		return
	}
	match := segmentNumRe.FindStringSubmatch(filepath.Base(window[len(window)-1]))
	maxNum, _ := strconv.Atoi(match[1])
	e.mu.Lock()
	if maxNum <= e.lastMaxSeen || e.closed {
		e.mu.Unlock()
		return
	}
	e.lastMaxSeen = maxNum
	e.mu.Unlock()

	concatPath := filepath.Join(e.outputDir, "cc_window.ts")
	concat, err := os.OpenFile(concatPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return
	}
	if err = e.retainFile(concat); err != nil {
		return
	}
	defer os.Remove(concatPath)
	var copied int64
	for _, path := range window {
		file, openErr := e.openSegment(path)
		if openErr != nil {
			err = openErr
			break
		}
		var n int64
		n, err = io.Copy(concat, io.LimitReader(file, maxFallbackWindowBytes-copied))
		copied += n
		var extra [1]byte
		if err == nil {
			count, readErr := file.Read(extra[:])
			if count != 0 {
				err = fmt.Errorf("live caption window exceeds actual byte limit")
			} else if readErr != nil && !errors.Is(readErr, io.EOF) {
				err = readErr
			}
		}
		closeErr := e.closeFile(file)
		if err == nil {
			err = closeErr
		}
		if err != nil || ctx.Err() != nil {
			break
		}
	}
	closeErr := e.closeFile(concat)
	if err != nil || closeErr != nil || ctx.Err() != nil {
		return
	}
	tmpPath := e.outputPath + ".tmp"
	defer os.Remove(tmpPath)
	execCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(execCtx, e.ffmpegPath, // #nosec G204
		"-y", "-copyts", "-f", "lavfi", "-i", fmt.Sprintf("movie=%s[out+subcc]", concatPath),
		"-map", "0:s", "-c:s", "text", "-f", "srt", tmpPath)
	// Actual command Wait joins exec's own pipe/copier work. Diagnostics are
	// discarded rather than accumulating unbounded CombinedOutput in memory.
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	e.mu.Lock()
	if e.closed || execCtx.Err() != nil {
		e.mu.Unlock()
		return
	}
	e.cmd = cmd
	err = cmd.Start()
	e.mu.Unlock()
	if err != nil {
		return
	}
	err = cmd.Wait()
	e.mu.Lock()
	if e.cmd == cmd {
		e.cmd = nil
	}
	e.mu.Unlock()
	if err != nil || execCtx.Err() != nil {
		return
	}
	if data, readErr := os.ReadFile(tmpPath); readErr == nil {
		if writeErr := os.WriteFile(tmpPath, []byte(strings.ReplaceAll(string(data), "\\h", " ")), 0644); writeErr != nil {
			return
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.closed && ctx.Err() == nil {
		_ = os.Rename(tmpPath, e.outputPath)
	}
}

// shutdownProcess only signals actual actors. It is safe inside the loop or
// its cancellation callback and never waits for its own command/loop to return.
func (e *ccExtractor) shutdownProcess() {
	e.signalOnce.Do(func() {
		e.mu.Lock()
		e.closed, e.running = true, false
		var process *os.Process
		if e.cmd != nil {
			process = e.cmd.Process
		}
		var files []*os.File
		if e.stdin != nil {
			files = append(files, e.stdin)
		}
		if e.childInput != nil {
			files = append(files, e.childInput)
		}
		for file := range e.files {
			files = append(files, file)
		}
		e.closeWork.Add(len(files))
		e.mu.Unlock()
		e.cancel()
		if process != nil {
			_ = process.Kill()
		}
		for _, file := range files {
			go func(file *os.File) { defer e.closeWork.Done(); _ = file.Close() }(file)
		}
	})
}

// stop is external: after signaling all actors, join the exact loop/Wait/IO.
func (e *ccExtractor) stop() {
	if e == nil {
		return
	}
	e.shutdownProcess()
	<-e.done
}

// srtTimestampRe matches SRT timestamp lines like "00:01:23,456 --> 00:01:25,789"
var srtTimestampRe = regexp.MustCompile(`(\d{2}:\d{2}:\d{2}),(\d{3})`)

// srtTimestampLineRe matches a full SRT timestamp line for offset adjustment
var srtTimestampLineRe = regexp.MustCompile(`(\d{2}):(\d{2}):(\d{2}),(\d{3}) --> (\d{2}):(\d{2}):(\d{2}),(\d{3})`)

// parseSRTTimestamp parses "HH:MM:SS,mmm" to milliseconds
func parseSRTTimestamp(h, m, s, ms string) int {
	hours, _ := strconv.Atoi(h)
	mins, _ := strconv.Atoi(m)
	secs, _ := strconv.Atoi(s)
	millis, _ := strconv.Atoi(ms)
	return hours*3600000 + mins*60000 + secs*1000 + millis
}

// cleanSRT deduplicates roll-up CC lines, trims whitespace, and outputs clean SRT.
// Keeps <i> and other formatting tags that KSPlayer/SRT parsers handle natively.
// EIA-608 roll-up captions produce cues where line 1 repeats the previous cue's last line.
// We strip confirmed duplicates while preserving legitimate multi-line cues.
func cleanSRT(srtContent string) string {
	srtContent = strings.TrimPrefix(srtContent, "\xef\xbb\xbf")

	// Normalize line endings: ccextractor outputs \r\n (Windows), convert to \n
	srtContent = strings.ReplaceAll(srtContent, "\r\n", "\n")
	srtContent = strings.ReplaceAll(srtContent, "\r", "\n")

	if strings.Contains(srtContent, "\\h") {
		log.Printf("[hls-cc] cleanSRT stripping lingering \\h markers from served captions")
		srtContent = strings.ReplaceAll(srtContent, "\\h", " ")
	}

	type cue struct {
		startMs int
		endMs   int
		lines   []string
	}

	var cues []cue
	blocks := strings.Split(strings.TrimSpace(srtContent), "\n\n")
	for _, block := range blocks {
		blockLines := strings.Split(strings.TrimSpace(block), "\n")
		if len(blockLines) < 2 {
			continue
		}
		var tsLine string
		var textStart int
		for i, line := range blockLines {
			if srtTimestampLineRe.MatchString(line) {
				tsLine = line
				textStart = i + 1
				break
			}
		}
		if tsLine == "" {
			continue
		}
		m := srtTimestampLineRe.FindStringSubmatch(tsLine)
		if m == nil {
			continue
		}
		startMs := parseSRTTimestamp(m[1], m[2], m[3], m[4])
		endMs := parseSRTTimestamp(m[5], m[6], m[7], m[8])

		var textLines []string
		for _, tl := range blockLines[textStart:] {
			trimmed := strings.TrimSpace(tl)
			if trimmed != "" {
				textLines = append(textLines, trimmed)
			}
		}
		if len(textLines) > 0 {
			cues = append(cues, cue{startMs: startMs, endMs: endMs, lines: textLines})
		}
	}

	var b strings.Builder
	cueNum := 0
	var prevLastLine string
	for _, c := range cues {
		dedupedLines := c.lines
		if len(dedupedLines) > 1 && prevLastLine != "" && dedupedLines[0] == prevLastLine {
			dedupedLines = dedupedLines[1:]
		}

		text := strings.Join(dedupedLines, "\n")
		if text == prevLastLine {
			continue
		}

		cueNum++
		fmt.Fprintf(&b, "%d\n", cueNum)
		fmt.Fprintf(&b, "%s --> %s\n", formatSRTTimestamp(c.startMs), formatSRTTimestamp(c.endMs))
		b.WriteString(text)
		b.WriteString("\n\n")

		prevLastLine = c.lines[len(c.lines)-1]
	}

	return b.String()
}

// formatSRTTimestamp formats milliseconds as "HH:MM:SS,mmm"
func formatSRTTimestamp(totalMs int) string {
	if totalMs < 0 {
		totalMs = 0
	}
	h := totalMs / 3600000
	totalMs %= 3600000
	m := totalMs / 60000
	totalMs %= 60000
	s := totalMs / 1000
	ms := totalMs % 1000
	return fmt.Sprintf("%02d:%02d:%02d,%03d", h, m, s, ms)
}

// ensureLiveCCExtractor reserves the one actual constructor before launch.
// Its generation actor remains registered through the extractor's physical end.
func (m *HLSManager) ensureLiveCCExtractor(ctx context.Context, session *HLSSession) (*ccExtractor, error) {
	g := ordinaryGeneration(ctx, session)
	if g == nil || g.owner.control.manager != m {
		return nil, errOrdinaryHLSClosed
	}
	session.mu.Lock()
	if session.Stopped || ctx.Err() != nil {
		session.mu.Unlock()
		return nil, errOrdinaryHLSClosed
	}
	if ext := session.ccExtractor; ext != nil {
		session.mu.Unlock()
		return ext, nil
	}
	pending := session.ccStarting
	launch := pending == nil
	if launch {
		pending = make(chan struct{})
		session.ccStarting = pending
	}
	outputDir := session.OutputDir
	session.mu.Unlock()
	if launch {
		original := g.context()
		if !g.spawn(original, func() {
			var ext *ccExtractor
			published := false
			func() {
				defer close(pending)
				defer func() {
					session.mu.Lock()
					if session.ccStarting == pending {
						session.ccStarting = nil
					}
					session.mu.Unlock()
				}()
				var err error
				ext, err = startCCExtraction(original, outputDir)
				if err != nil {
					log.Printf("[hls-cc] original extraction startup failed: %v", err)
					return
				}
				session.mu.Lock()
				if !session.Stopped && original.Err() == nil {
					session.ccExtractor = ext
					published = true
				}
				session.mu.Unlock()
			}()
			if ext != nil {
				if !published {
					ext.stop()
				} else {
					<-ext.done
				}
			}
		}) {
			session.mu.Lock()
			if session.ccStarting == pending {
				session.ccStarting = nil
			}
			session.mu.Unlock()
			close(pending)
			return nil, errOrdinaryHLSClosed
		}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-g.ctx.Done():
		return nil, errOrdinaryHLSClosed
	case <-pending:
	}
	session.mu.RLock()
	ext, stopped := session.ccExtractor, session.Stopped
	session.mu.RUnlock()
	if ext == nil || stopped || g.ctx.Err() != nil {
		return nil, errOrdinaryHLSClosed
	}
	return ext, nil
}

// ServeLiveCaptions serves SRT captions for a live session. The original
// generation owns both lazy extraction and this actual output descriptor.
func (m *HLSManager) ServeLiveCaptions(w http.ResponseWriter, r *http.Request, sessionID string) {
	session, exists := m.GetSession(sessionID)
	if !exists {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	if session.ordinary != nil {
		var finish func()
		var ok bool
		w, r, finish, ok = m.ordinaryHTTP(w, r, sessionID)
		if !ok {
			http.Error(w, "live captions unavailable", http.StatusServiceUnavailable)
			return
		}
		defer finish()
	}
	session.mu.RLock()
	isLive, enabled, hasCaptions := session.IsLive, session.LiveCCExtractionEnabled, session.HasClosedCaptions
	session.mu.RUnlock()
	if !isLive {
		http.Error(w, "captions only available for live sessions", http.StatusBadRequest)
		return
	}
	emptySRT := func() {
		w.Header().Set("Content-Type", "application/x-subrip; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_, _ = w.Write([]byte(""))
	}
	if !enabled || !hasCaptions {
		emptySRT()
		return
	}
	ext, err := m.ensureLiveCCExtractor(r.Context(), session)
	if err != nil {
		emptySRT()
		return
	}
	file, err := settingscommit.OpenRegularGuardData(ext.outputPath)
	if err != nil {
		emptySRT()
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		emptySRT()
		return
	}
	w.Header().Set("Content-Type", "application/x-subrip; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_, _ = w.Write([]byte(cleanSRT(string(data))))
}

// The detector is an original generation child. No late result can renew a
// retired session, and actual command/descriptor cleanup precedes child return.
func (m *HLSManager) detectAndSetClosedCaptions(session *HLSSession) {
	if session == nil || session.ordinary == nil {
		return
	}
	g := session.ordinary.current()
	if g == nil {
		return
	}
	original := g.context()
	g.spawn(original, func() {
		ctx, cancel := context.WithTimeout(original, 25*time.Second)
		defer cancel()
		session.mu.RLock()
		outputDir, enabled := session.OutputDir, session.LiveCCExtractionEnabled
		session.mu.RUnlock()
		hasCC := false
		if enabled {
			hasCC = detectClosedCaptionsInSegments(ctx, outputDir, m.ffmpegPath)
		}
		session.mu.Lock()
		// Finishing the bounded sniff without captions is a completed detection,
		// including its own deadline. Original runtime cancellation is terminal.
		if !session.Stopped && original.Err() == nil {
			session.HasClosedCaptions = hasCC
			session.CCDetectionDone = true
		}
		session.mu.Unlock()
	})
}

// GetCCStatus returns the CC detection status for a live session
func (m *HLSManager) GetCCStatus(sessionID string) (hasCaptions bool, detectionDone bool) {
	session, exists := m.GetSession(sessionID)
	if !exists {
		return false, false
	}

	session.mu.RLock()
	defer session.mu.RUnlock()
	return session.HasClosedCaptions, session.CCDetectionDone
}

// ServeLiveCCStatus handles GET /video/hls/{sessionID}/cc-status
// Returns the CC detection status so the frontend can poll for it
func (m *HLSManager) ServeLiveCCStatus(w http.ResponseWriter, r *http.Request, sessionID string) {
	if session, exists := m.GetSession(sessionID); exists && session.ordinary != nil {
		var finish func()
		var ok bool
		w, r, finish, ok = m.ordinaryHTTP(w, r, sessionID)
		if !ok {
			http.Error(w, "live captions unavailable", http.StatusServiceUnavailable)
			return
		}
		defer finish()
	}
	hasCaptions, detectionDone := m.GetCCStatus(sessionID)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")
	fmt.Fprintf(w, `{"hasClosedCaptions":%s,"detectionDone":%s}`,
		strconv.FormatBool(hasCaptions), strconv.FormatBool(detectionDone))
}
