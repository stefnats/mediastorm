package recordings

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"novastream/models"
)

var ErrRecordingPartUnavailable = errors.New("recording part is unavailable")

func safeRecordingComponent(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return false
		}
	}
	return true
}

func (s *Service) recordingDirectory(recording models.Recording) (string, error) {
	if !safeRecordingComponent(recording.ID) || !safeRecordingComponent(recording.UserID) {
		return "", ErrRecordingPartUnavailable
	}
	root, err := filepath.Abs(s.outputDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(root, recording.UserID, recording.ID), nil
}

func (s *Service) partPath(recording models.Recording, index int) (string, error) {
	if index < 1 || index > models.MaxRecordingParts {
		return "", ErrRecordingPartUnavailable
	}
	dir, err := s.recordingDirectory(recording)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("part-%06d.ts", index)), nil
}

func (s *Service) createCaptureDirectory(recording models.Recording) error {
	dir, err := s.recordingDirectory(recording)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(s.outputDir)
	if err != nil {
		return err
	}
	if info, e := os.Lstat(root); e == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return ErrRecordingPartUnavailable
	} else if e != nil && !os.IsNotExist(e) {
		return ErrRecordingPartUnavailable
	}
	if err = os.MkdirAll(root, 0755); err != nil {
		return err
	}
	profile := filepath.Dir(dir)
	if info, e := os.Lstat(profile); e == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return ErrRecordingPartUnavailable
		}
	} else if os.IsNotExist(e) {
		if err = os.Mkdir(profile, 0755); err != nil {
			return err
		}
	} else {
		return ErrRecordingPartUnavailable
	}
	// Existing job directories are never adopted, even if a colliding file
	// happens to look like a valid transport stream.
	return os.Mkdir(dir, 0755)
}

// ownedPartFile does not trust imported OutputPath or ledger streamPath.
// Every component must be a real local directory/file, never a symlink.
func (s *Service) ownedPartFile(recording models.Recording, index int) (string, error) {
	path, err := s.partPath(recording, index)
	if err != nil {
		return "", err
	}
	root, err := filepath.Abs(s.outputDir)
	if err != nil {
		return "", err
	}
	for _, candidate := range []string{root, filepath.Dir(filepath.Dir(path)), filepath.Dir(path), path} {
		info, err := os.Lstat(candidate)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return "", ErrRecordingPartUnavailable
		}
		if candidate == path {
			if !info.Mode().IsRegular() {
				return "", ErrRecordingPartUnavailable
			}
		} else if !info.IsDir() {
			return "", ErrRecordingPartUnavailable
		}
	}
	return path, nil
}

func BuildPartStreamPath(recordingID string, index int) string {
	return "recording:" + recordingID + "/parts/" + strconv.Itoa(index)
}

func validCaptureParts(capture *models.RecordingCapture) bool {
	if capture == nil || capture.Version != 1 || capture.Restored || len(capture.Parts) > models.MaxRecordingParts || len(capture.Gaps) > models.MaxRecordingGaps {
		return false
	}
	seen := map[int]bool{}
	for _, part := range capture.Parts {
		if part.Index < 1 || part.Index > models.MaxRecordingParts || seen[part.Index] {
			return false
		}
		seen[part.Index] = true
	}
	return true
}

func (s *Service) ResolvePartFile(id string, index int) (string, *models.RecordingPart, error) {
	recording, err := s.Get(id)
	if err != nil {
		return "", nil, err
	}
	if !validCaptureParts(recording.Capture) {
		return "", nil, ErrRecordingPartUnavailable
	}
	var selected *models.RecordingPart
	for _, part := range recording.Capture.Parts {
		if part.Index == index {
			if selected != nil {
				return "", nil, ErrRecordingPartUnavailable
			}
			copy := part
			selected = &copy
		}
	}
	if selected == nil {
		return "", nil, ErrRecordingPartUnavailable
	}
	path, err := s.ownedPartFile(*recording, index)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		return "", nil, ErrRecordingPartUnavailable
	}
	selected.Bytes = info.Size()
	return path, selected, nil
}

func (s *Service) captureSnapshot(recording *models.Recording) {
	if recording == nil || recording.Capture == nil || recording.Capture.Version != 1 {
		return
	}
	recording.Capture = models.CopyRecordingCapture(recording.Capture)
	if recording.Capture.Restored {
		for i := range recording.Capture.Parts {
			recording.Capture.Parts[i].StreamPath = ""
		}
		recording.OutputPath = ""
		recording.OutputSizeBytes = 0
		return
	}
	if !validCaptureParts(recording.Capture) {
		recording.Capture = &models.RecordingCapture{Version: 1, Phase: "interrupted", Incomplete: true, Restored: true, Parts: []models.RecordingPart{}, Gaps: []models.RecordingGap{}}
		recording.OutputPath = ""
		recording.OutputSizeBytes = 0
		return
	}
	sort.Slice(recording.Capture.Parts, func(i, j int) bool { return recording.Capture.Parts[i].Index < recording.Capture.Parts[j].Index })
	if len(recording.Capture.Parts) == 0 && recording.Status == models.RecordingStatusPending {
		return
	}
	firstPath := ""
	for i := range recording.Capture.Parts {
		part := &recording.Capture.Parts[i]
		part.StreamPath = ""
		path, err := s.ownedPartFile(*recording, part.Index)
		if err != nil {
			continue
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() < 0 {
			continue
		}
		part.Bytes = info.Size()
		if part.Bytes > 0 {
			part.StreamPath = BuildPartStreamPath(recording.ID, part.Index)
		}
		if part.Bytes > 0 && firstPath == "" {
			firstPath = path
			recording.OutputSizeBytes = part.Bytes
		}
	}
	recording.OutputPath = firstPath
	if firstPath == "" {
		recording.OutputSizeBytes = 0
	}
}

func capturePartClosed(part *models.RecordingPart) bool {
	return part != nil && part.EndedAt != nil && !part.EndedAt.Before(part.StartedAt) && strings.TrimSpace(part.EndReason) != ""
}

func recordingFirstPartGrowing(recording *models.Recording) bool {
	if recording == nil || !recordingIsGrowing(recording.Status) {
		return false
	}
	if recording.Capture == nil {
		return true
	}
	if !validCaptureParts(recording.Capture) {
		return false
	}
	var selected *models.RecordingPart
	for _, part := range recording.Capture.Parts {
		if part.Bytes > 0 && (selected == nil || part.Index < selected.Index) {
			copy := part
			selected = &copy
		}
	}
	return selected != nil && !capturePartClosed(selected)
}

func closeCapturePart(part *models.RecordingPart, at time.Time, reason string) {
	part.EndedAt = &at
	part.EndReason = reason
}
