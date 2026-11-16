package importer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"novastream/internal/database"
	"novastream/services/usenetengine"
)

var (
	ErrSABFallbackNotFound      = errors.New("fallback queue item not found")
	ErrSABFallbackUnavailable   = errors.New("fallback status unavailable")
	ErrSABFallbackSourceChanged = errors.New("fallback source changed")
	ErrSABFallbackBusy          = errors.New("fallback refresh busy")
	ErrSABFallbackInvalidPage   = errors.New("invalid fallback page")
)

const fallbackNZBMaxBytes = 32 << 20

// SABFallbackStatus is an observation of an external DOWNLOAD, never an import,
// file permission, catalogue match, age grant or playable stream.
type SABFallbackStatus struct {
	QueueID     int64      `json:"queueId"`
	State       string     `json:"state"`
	Reason      string     `json:"reason,omitempty"`
	SubmittedAt *time.Time `json:"submittedAt,omitempty"`
	CheckedAt   *time.Time `json:"checkedAt,omitempty"`
}

// SABFallbackPage is a bounded live projection of saved receipts. Its cursor
// excludes newer queue IDs, but does not freeze status updates, deletions, or
// receipts subsequently attached to older queue rows.
type SABFallbackPage struct {
	Items      []SABFallbackStatus `json:"items"`
	NextCursor string              `json:"nextCursor,omitempty"`
}

func sabFallbackPageBounds(cursor string, limit int) (int64, int64, error) {
	if limit < 1 || limit > 100 {
		return 0, 0, ErrSABFallbackInvalidPage
	}
	if cursor == "" {
		return math.MaxInt64, 0, nil
	}
	parts := strings.Split(cursor, ".")
	if len(cursor) > 64 || len(parts) != 3 || parts[0] != "v1" {
		return 0, 0, ErrSABFallbackInvalidPage
	}
	ceiling, err1 := strconv.ParseInt(parts[1], 10, 64)
	before, err2 := strconv.ParseInt(parts[2], 10, 64)
	if err1 != nil || err2 != nil || ceiling <= 0 || before <= 0 || before > ceiling || strconv.FormatInt(ceiling, 10) != parts[1] || strconv.FormatInt(before, 10) != parts[2] {
		return 0, 0, ErrSABFallbackInvalidPage
	}
	return ceiling, before, nil
}

// ListSABFallbackStatuses never resolves configuration, opens NZBs, or calls SAB.
func (s *Service) ListSABFallbackStatuses(ctx context.Context, cursor string, limit int) (SABFallbackPage, error) {
	ceiling, before, err := sabFallbackPageBounds(cursor, limit)
	if err != nil {
		return SABFallbackPage{}, err
	}
	if s == nil || s.database == nil {
		return SABFallbackPage{}, ErrSABFallbackUnavailable
	}
	rows, err := s.database.Repository.ListSABFallbackReceipts(ctx, ceiling, before, limit+1)
	if err != nil {
		return SABFallbackPage{}, ErrSABFallbackUnavailable
	}
	page := SABFallbackPage{Items: make([]SABFallbackStatus, 0, limit)}
	if before == 0 && len(rows) > 0 {
		ceiling = rows[0].ID
	}
	for i, row := range rows {
		if !row.Receipt.Valid || len(row.Receipt.String) > 8192 {
			return SABFallbackPage{}, ErrSABFallbackUnavailable
		}
		var receipt sabFallbackReceipt
		decoder := json.NewDecoder(strings.NewReader(row.Receipt.String))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&receipt) != nil || decoder.Decode(new(any)) != io.EOF || !validSABReceipt(&receipt, row.ID) {
			return SABFallbackPage{}, ErrSABFallbackUnavailable
		}
		if i == limit {
			page.NextCursor = "v1." + strconv.FormatInt(ceiling, 10) + "." + strconv.FormatInt(rows[i-1].ID, 10)
			break
		}
		page.Items = append(page.Items, receipt.Status)
	}
	if err := ctx.Err(); err != nil {
		return SABFallbackPage{}, err
	}
	return page, nil
}

type sabFallbackReceipt struct {
	Version      int               `json:"version"`
	Attempt      string            `json:"attempt"`
	Source       string            `json:"source"`
	JobID        string            `json:"jobId,omitempty"`
	Status       SABFallbackStatus `json:"status"`
	Refresh      string            `json:"refresh,omitempty"`
	RefreshUntil *time.Time        `json:"refreshUntil,omitempty"`
}

func sabOpaque(v string, max int) bool {
	if len(v) == 0 || len(v) > max {
		return false
	}
	for _, r := range v {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}
func sabNonce() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}
func validSABReceipt(r *sabFallbackReceipt, id int64) bool {
	if r == nil || r.Version != 1 || !sabOpaque(r.Attempt, 32) || len(r.Source) != 64 || r.Status.QueueID != id {
		return false
	}
	if _, err := hex.DecodeString(r.Source); err != nil {
		return false
	}
	if r.JobID != "" && !sabOpaque(r.JobID, 128) {
		return false
	}
	switch r.Status.State {
	case "submission_unknown", "submitted", "queued", "downloading", "download_completed", "failed", "unknown":
	default:
		return false
	}
	switch r.Status.Reason {
	case "", "acknowledgement_unavailable", "provider_unavailable", "provider_state_unknown":
	default:
		return false
	}
	for _, at := range []*time.Time{r.Status.SubmittedAt, r.Status.CheckedAt, r.RefreshUntil} {
		if at != nil && (at.IsZero() || at.Year() < 1 || at.Year() > 9999) {
			return false
		}
	}
	return r.Refresh == "" || sabOpaque(r.Refresh, 32)
}
func (s *Service) fallbackItem(ctx context.Context, id int64) (*database.ImportQueueItem, map[string]json.RawMessage, *sabFallbackReceipt, error) {
	if ctx.Err() != nil {
		return nil, nil, nil, ctx.Err()
	}
	if s == nil || s.database == nil || id <= 0 {
		return nil, nil, nil, ErrSABFallbackNotFound
	}
	item, err := s.database.Repository.GetQueueItemContext(ctx, id)
	if err != nil {
		return nil, nil, nil, ErrSABFallbackUnavailable
	}
	if item == nil {
		return nil, nil, nil, ErrSABFallbackNotFound
	}
	fields := map[string]json.RawMessage{}
	if item.Metadata != nil && (len(*item.Metadata) > 1<<20 || json.Unmarshal([]byte(*item.Metadata), &fields) != nil || fields == nil) {
		return nil, nil, nil, ErrSABFallbackUnavailable
	}
	var receipt *sabFallbackReceipt
	if raw, ok := fields[database.SABFallbackMetadataKey]; ok {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		receipt = &sabFallbackReceipt{}
		if dec.Decode(receipt) != nil || !validSABReceipt(receipt, id) {
			return nil, nil, nil, ErrSABFallbackUnavailable
		}
	}
	if ctx.Err() != nil {
		return nil, nil, nil, ctx.Err()
	}
	return item, fields, receipt, nil
}
func (s *Service) saveFallback(ctx context.Context, item *database.ImportQueueItem, fields map[string]json.RawMessage, receipt *sabFallbackReceipt, failedOnly bool) (bool, error) {
	raw, err := json.Marshal(receipt)
	if err != nil {
		return false, ErrSABFallbackUnavailable
	}
	fields[database.SABFallbackMetadataKey] = raw
	raw, err = json.Marshal(fields)
	if err != nil {
		return false, ErrSABFallbackUnavailable
	}
	next := string(raw)
	changed, err := s.database.Repository.CompareAndSwapMetadata(ctx, item.ID, item.Metadata, &next, failedOnly)
	if err != nil {
		return false, ErrSABFallbackUnavailable
	}
	return changed, nil
}
func (s *Service) fallbackEngine() (usenetengine.Engine, string, bool) {
	if s == nil || s.configGetter == nil {
		return nil, "", false
	}
	cfg := s.configGetter()
	if cfg == nil || cfg.SABnzbd.Enabled == nil || !*cfg.SABnzbd.Enabled {
		return nil, "", false
	}
	host, key := strings.TrimSpace(cfg.SABnzbd.FallbackHost), cfg.SABnzbd.FallbackAPIKey
	u, err := url.Parse(host)
	if err != nil || u.Host == "" || u.Opaque != "" || u.Fragment != "" || u.RawQuery != "" || (u.Scheme != "http" && u.Scheme != "https") || strings.TrimSpace(key) == "" {
		return nil, "", false
	}
	raw, err := json.Marshal([]string{host, key})
	if err != nil {
		return nil, "", false
	}
	sum := sha256.Sum256(raw)
	client, err := usenetengine.NewSABClient(usenetengine.SABConfig{Name: "SAB fallback", BaseURL: host, APIPath: "/api", APIKey: key}, &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
	return client, hex.EncodeToString(sum[:]), err == nil
}
func (s *Service) fallbackSlot() func() {
	if s == nil || s.fallbackSlots == nil {
		return nil
	}
	select {
	case s.fallbackSlots <- struct{}{}:
		return func() { <-s.fallbackSlots }
	default:
		return nil
	}
}
func (s *Service) submitSABFallback(passed *database.ImportQueueItem) error {
	engine, source, enabled := s.fallbackEngine()
	if !enabled {
		return nil
	}
	if passed == nil {
		return ErrSABFallbackNotFound
	}
	ctx := s.ctx
	if ctx == nil {
		return ErrSABFallbackUnavailable
	}
	ctx, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	release := s.fallbackSlot()
	if release == nil {
		return ErrSABFallbackBusy
	}
	defer release()
	item, fields, old, err := s.fallbackItem(ctx, passed.ID)
	if err != nil {
		return err
	}
	if old != nil {
		return nil
	}
	if item.Status != database.QueueStatusFailed || item.NzbPath != passed.NzbPath {
		return ErrSABFallbackUnavailable
	}
	initial, err := os.Lstat(item.NzbPath)
	if err != nil || !initial.Mode().IsRegular() || initial.Size() <= 0 || initial.Size() > fallbackNZBMaxBytes {
		return ErrSABFallbackUnavailable
	}
	f, err := os.OpenFile(item.NzbPath, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return ErrSABFallbackUnavailable
	}
	var once sync.Once
	closeFile := func() { once.Do(func() { _ = f.Close() }) }
	defer closeFile()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || !os.SameFile(initial, stat) || stat.Size() <= 0 || stat.Size() > fallbackNZBMaxBytes {
		return ErrSABFallbackUnavailable
	}
	stopClose := context.AfterFunc(ctx, closeFile)
	defer stopClose()
	raw, readErr := io.ReadAll(io.LimitReader(f, fallbackNZBMaxBytes+1))
	closeFile()
	if readErr != nil || ctx.Err() != nil || len(raw) == 0 || len(raw) > fallbackNZBMaxBytes {
		return ErrSABFallbackUnavailable
	}
	attempt := sabNonce()
	if attempt == "" {
		return ErrSABFallbackUnavailable
	}
	at := time.Now().UTC()
	record := &sabFallbackReceipt{Version: 1, Attempt: attempt, Source: source, Status: SABFallbackStatus{QueueID: item.ID, State: "submission_unknown", Reason: "acknowledgement_unavailable", SubmittedAt: &at}}
	claimed, err := s.saveFallback(ctx, item, fields, record, true)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	// The durable unknown marker precedes HTTP. A crash or lost acknowledgement
	// must never cause an automatic resubmission on retry/restart.
	_, current, ok := s.fallbackEngine()
	if !ok || current != source || ctx.Err() != nil {
		return ErrSABFallbackSourceChanged
	}
	category := ""
	if item.Category != nil {
		category = *item.Category
	}
	submitted, submitErr := engine.SubmitNZB(ctx, usenetengine.SubmitRequest{FileName: filepath.Base(item.NzbPath), NZB: raw, Category: category, Priority: s.convertPriorityToSABnzbd(item.Priority)})
	if submitErr != nil || submitted == nil || !sabOpaque(submitted.JobID, 128) {
		return ErrSABFallbackUnavailable
	}
	record.JobID = submitted.JobID
	record.Status.State = "submitted"
	record.Status.Reason = ""
	return s.updateFallback(ctx, item.ID, attempt, func(r *sabFallbackReceipt) error { r.JobID = record.JobID; r.Status = record.Status; return nil })
}

// updateFallback preserves unrelated metadata and refuses another attempt's row.
func (s *Service) updateFallback(ctx context.Context, id int64, attempt string, apply func(*sabFallbackReceipt) error) error {
	for n := 0; n < 8; n++ {
		item, fields, r, err := s.fallbackItem(ctx, id)
		if err != nil {
			return err
		}
		if r == nil || r.Attempt != attempt {
			return ErrSABFallbackUnavailable
		}
		if err := apply(r); err != nil {
			return err
		}
		changed, err := s.saveFallback(ctx, item, fields, r, false)
		if err != nil {
			return err
		}
		if changed {
			return nil
		}
	}
	return ErrSABFallbackBusy
}

// GetSABFallbackStatus reads persisted safe fields only. It never contacts SAB.
func (s *Service) GetSABFallbackStatus(ctx context.Context, id int64) (SABFallbackStatus, error) {
	_, _, r, err := s.fallbackItem(ctx, id)
	if err != nil {
		return SABFallbackStatus{}, err
	}
	if r == nil {
		return SABFallbackStatus{QueueID: id, State: "not_submitted"}, nil
	}
	return r.Status, nil
}
func (s *Service) RefreshSABFallbackStatus(ctx context.Context, id int64) (SABFallbackStatus, error) {
	ctx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	release := s.fallbackSlot()
	if release == nil {
		return SABFallbackStatus{}, ErrSABFallbackBusy
	}
	defer release()
	item, fields, r, err := s.fallbackItem(ctx, id)
	if err != nil {
		return SABFallbackStatus{}, err
	}
	if r == nil {
		return SABFallbackStatus{QueueID: id, State: "not_submitted"}, nil
	}
	engine, source, ok := s.fallbackEngine()
	if !ok || source != r.Source {
		return SABFallbackStatus{}, ErrSABFallbackSourceChanged
	}
	if r.JobID == "" || r.Status.State == "failed" || r.Status.State == "download_completed" {
		return r.Status, nil
	}
	if r.Refresh != "" && r.RefreshUntil != nil && r.RefreshUntil.After(time.Now()) {
		return SABFallbackStatus{}, ErrSABFallbackBusy
	}
	nonce := sabNonce()
	if nonce == "" {
		return SABFallbackStatus{}, ErrSABFallbackUnavailable
	}
	until, _ := ctx.Deadline()
	r.Refresh = nonce
	r.RefreshUntil = &until
	changed, err := s.saveFallback(ctx, item, fields, r, false)
	if err != nil {
		return SABFallbackStatus{}, err
	}
	if !changed {
		return SABFallbackStatus{}, ErrSABFallbackBusy
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer stop()
		_ = s.updateFallback(cleanup, id, r.Attempt, func(current *sabFallbackReceipt) error {
			if current.Refresh != nonce {
				return ErrSABFallbackBusy
			}
			current.Refresh = ""
			current.RefreshUntil = nil
			return nil
		})
	}()
	_, beforeHTTP, enabled := s.fallbackEngine()
	if ctx.Err() != nil {
		return SABFallbackStatus{}, ctx.Err()
	}
	if !enabled || beforeHTTP != source {
		return SABFallbackStatus{}, ErrSABFallbackSourceChanged
	}
	result, statusErr := engine.Status(ctx, r.JobID)
	_, latest, stillEnabled := s.fallbackEngine()
	if !stillEnabled || latest != source {
		return SABFallbackStatus{}, ErrSABFallbackSourceChanged
	}
	if ctx.Err() != nil {
		return SABFallbackStatus{}, ctx.Err()
	}
	checked := time.Now().UTC()
	status := r.Status
	status.CheckedAt = &checked
	status.State = "unknown"
	status.Reason = "provider_unavailable"
	if statusErr == nil && result != nil {
		status.Reason = ""
		switch result.Status {
		case usenetengine.StatusQueued:
			status.State = "queued"
		case usenetengine.StatusProcessing:
			status.State = "downloading"
		case usenetengine.StatusCompleted:
			status.State = "download_completed"
		case usenetengine.StatusFailed:
			status.State = "failed"
		default:
			status.Reason = "provider_state_unknown"
		}
	}
	err = s.updateFallback(ctx, id, r.Attempt, func(current *sabFallbackReceipt) error {
		if current.Refresh != nonce {
			return ErrSABFallbackBusy
		}
		if current.Refresh == nonce {
			current.Status = status
			current.Refresh = ""
			current.RefreshUntil = nil
		}
		return nil
	})
	if err != nil {
		return SABFallbackStatus{}, err
	}
	return status, nil
}
