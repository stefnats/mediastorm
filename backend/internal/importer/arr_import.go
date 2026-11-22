package importer

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"novastream/config"
	"novastream/internal/database"
	"novastream/services/arrclient"
)

const arrImportMetadataKey = database.ARRImportMetadataKey

var ErrARRImportUnavailable = errors.New("library import unavailable")
var ErrARRImportBusy = errors.New("library import already in progress")

type ARRImportStatus struct {
	QueueID            int64      `json:"queueId"`
	Backend            string     `json:"backend,omitempty"`
	LibraryID          string     `json:"libraryId,omitempty"`
	State              string     `json:"state"`
	Reason             string     `json:"reason,omitempty"`
	CommandID          int64      `json:"commandId,omitempty"`
	ItemIDs            []string   `json:"itemIds"`
	CheckedAt          *time.Time `json:"checkedAt,omitempty"`
	ConfiguredBackends []string   `json:"configuredBackends,omitempty"`
}
type ARRLibraryResult struct {
	ItemIDs      []string
	Missing      bool
	NeedsMatch   bool
	InvalidMedia bool
	Reason       string
}
type ARRLibraryImporter interface {
	ValidateARRLibrary(context.Context, string, string) error
	ImportARRFiles(context.Context, string, string, []arrclient.ImportedFile) (ARRLibraryResult, error)
}
type arrImportRecord struct {
	Attempt     string          `json:"attempt"`
	Revision    int64           `json:"revision,omitempty"`
	DownloadID  string          `json:"downloadId"`
	BaseURL     string          `json:"baseUrl"`
	LibraryRoot string          `json:"libraryRoot"`
	Status      ARRImportStatus `json:"status"`
}

func ARRBackend(settings config.SABnzbdSettings, kind string) (config.ARRImportSettings, error) {
	var v config.ARRImportSettings
	switch kind {
	case "sonarr":
		v = settings.Sonarr
	case "radarr":
		v = settings.Radarr
	default:
		return v, ErrARRImportUnavailable
	}
	if v.BaseURL == "" || v.APIKey == "" || v.LibraryID == "" {
		return v, ErrARRImportUnavailable
	}
	return v, nil
}
func (s *Service) arrRecord(ctx context.Context, id int64) (*arrImportRecord, error) {
	_, fields, _, e := s.fallbackItem(ctx, id)
	if e != nil {
		return nil, e
	}
	raw := fields[arrImportMetadataKey]
	if len(raw) == 0 {
		return nil, nil
	}
	var r arrImportRecord
	if json.Unmarshal(raw, &r) != nil || r.Attempt == "" || r.Status.QueueID != id {
		return nil, ErrARRImportUnavailable
	}
	return &r, nil
}
func (s *Service) saveARR(ctx context.Context, id int64, previous string, r *arrImportRecord) error {
	for attempt := 0; attempt < 3; attempt++ {
		item, fields, _, e := s.fallbackItem(ctx, id)
		if e != nil {
			return e
		}
		var old arrImportRecord
		if raw := fields[arrImportMetadataKey]; len(raw) > 0 {
			if json.Unmarshal(raw, &old) != nil {
				return ErrARRImportUnavailable
			}
		}
		if old.Attempt != previous || old.Revision != r.Revision {
			return ErrARRImportBusy
		}
		nextRecord := *r
		nextRecord.Revision = old.Revision + 1
		raw, e := json.Marshal(&nextRecord)
		if e != nil {
			return e
		}
		fields[arrImportMetadataKey] = raw
		raw, e = json.Marshal(fields)
		if e != nil {
			return e
		}
		next := string(raw)
		ok, e := s.database.Repository.CompareAndSwapMetadata(ctx, id, item.Metadata, &next, false)
		if e != nil {
			return e
		}
		if ok {
			r.Revision = nextRecord.Revision
			return nil
		}
	}
	return ErrARRImportBusy
}
func (s *Service) GetARRImportStatus(ctx context.Context, id int64) (ARRImportStatus, error) {
	work, finish, e := s.beginSourceWork(ctx, false)
	if e != nil {
		return ARRImportStatus{}, e
	}
	defer finish()
	r, e := s.arrRecord(work, id)
	if e != nil {
		return ARRImportStatus{}, e
	}
	if r == nil {
		return ARRImportStatus{QueueID: id, State: "not_started", ItemIDs: []string{}}, nil
	}
	return r.Status, nil
}

// SubmitARRImport is explicit. An uncertain POST is saved before submission and
// is never automatically resubmitted by Refresh or a normal repeated click.
func (s *Service) SubmitARRImport(ctx context.Context, id int64, kind string, retry bool, settings SABImportPreviewSettings, library ARRLibraryImporter, check func(context.Context) error) (ARRImportStatus, error) {
	work, finish, e := s.beginSourceWork(ctx, true)
	if e != nil {
		return ARRImportStatus{}, e
	}
	defer finish()
	ctx = work
	if settings == nil || library == nil || check == nil || check(ctx) != nil {
		return ARRImportStatus{}, ErrARRImportUnavailable
	}
	cfg, e := settings(ctx)
	if e != nil {
		return ARRImportStatus{}, e
	}
	backend, e := ARRBackend(cfg, kind)
	if e != nil {
		return ARRImportStatus{}, e
	}
	if e = library.ValidateARRLibrary(ctx, backend.LibraryID, kind); e != nil {
		return ARRImportStatus{}, e
	}
	prior, e := s.arrRecord(ctx, id)
	if e != nil {
		return ARRImportStatus{}, e
	}
	if prior != nil && (!retry || prior.Status.State != "import_failed" && prior.Status.State != "submission_unknown" && prior.Status.State != "configuration_changed") {
		return prior.Status, nil
	}
	observed, e := s.observeSABFallback(ctx, id, true, check)
	if e != nil {
		return ARRImportStatus{}, e
	}
	if observed.raw == nil || observed.status.State != "download_completed" || observed.raw.OutputPath == "" {
		return ARRImportStatus{}, ErrARRImportUnavailable
	}
	client, e := arrclient.New(ctx, arrclient.Config{Kind: kind, BaseURL: backend.BaseURL, APIKey: backend.APIKey})
	if e != nil {
		return ARRImportStatus{}, e
	}
	defer client.Close()
	previous := ""
	if prior != nil {
		previous = prior.Attempt
	}
	at := time.Now().UTC()
	r := &arrImportRecord{Attempt: sabNonce(), DownloadID: observed.jobID, BaseURL: backend.BaseURL, LibraryRoot: backend.LibraryRoot, Status: ARRImportStatus{QueueID: id, Backend: kind, LibraryID: backend.LibraryID, State: "submission_unknown", Reason: "check_before_retry", ItemIDs: []string{}, CheckedAt: &at}}
	if prior != nil {
		r.Revision = prior.Revision
	}
	if check(ctx) != nil {
		return ARRImportStatus{}, ErrARRImportUnavailable
	}
	if e = s.saveARR(ctx, id, previous, r); e != nil {
		return ARRImportStatus{}, e
	}
	command, e := client.SubmitCopy(ctx, observed.raw.OutputPath, observed.jobID)
	if e != nil {
		return r.Status, nil
	}
	r.Status.CommandID = command.ID
	r.Status.State = "importing"
	r.Status.Reason = ""
	if command.Status == "failed" || command.Status == "aborted" || command.Status == "cancelled" || command.Result == "unsuccessful" {
		r.Status.State = "import_failed"
		r.Status.Reason = "import_failed"
	}
	if e = s.saveARR(ctx, id, r.Attempt, r); errors.Is(e, ErrARRImportBusy) {
		latest, readErr := s.arrRecord(ctx, id)
		if readErr != nil || latest == nil || latest.Attempt != r.Attempt {
			return ARRImportStatus{}, ErrARRImportBusy
		}
		latest.Status.CommandID = command.ID
		if latest.Status.State == "submission_unknown" {
			latest.Status.State = r.Status.State
			latest.Status.Reason = r.Status.Reason
		}
		return s.persistARRStatus(ctx, id, latest)
	} else if e != nil {
		return ARRImportStatus{}, e
	}
	return r.Status, nil
}

// A stale refresh returns the newer saved observation rather than replacing it.
func (s *Service) persistARRStatus(ctx context.Context, id int64, r *arrImportRecord) (ARRImportStatus, error) {
	e := s.saveARR(ctx, id, r.Attempt, r)
	if errors.Is(e, ErrARRImportBusy) {
		latest, readErr := s.arrRecord(ctx, id)
		if readErr != nil {
			return ARRImportStatus{}, readErr
		}
		if latest != nil {
			return latest.Status, nil
		}
	}
	return r.Status, e
}
func (s *Service) RefreshARRImport(ctx context.Context, id int64, settings SABImportPreviewSettings, library ARRLibraryImporter, check func(context.Context) error) (ARRImportStatus, error) {
	work, finish, e := s.beginSourceWork(ctx, false)
	if e != nil {
		return ARRImportStatus{}, e
	}
	defer finish()
	ctx = work
	if settings == nil || library == nil || check == nil || check(ctx) != nil {
		return ARRImportStatus{}, ErrARRImportUnavailable
	}
	r, e := s.arrRecord(ctx, id)
	if e != nil {
		return ARRImportStatus{}, e
	}
	if r == nil {
		return ARRImportStatus{QueueID: id, State: "not_started", ItemIDs: []string{}}, nil
	}
	cfg, e := settings(ctx)
	if e != nil {
		return ARRImportStatus{}, e
	}
	backend, e := ARRBackend(cfg, r.Status.Backend)
	if e != nil {
		return ARRImportStatus{}, e
	}
	if backend.BaseURL != r.BaseURL || backend.LibraryID != r.Status.LibraryID || backend.LibraryRoot != r.LibraryRoot {
		r.Status.State = "configuration_changed"
		r.Status.Reason = "configuration_changed"
		return s.persistARRStatus(ctx, id, r)
	}
	client, e := arrclient.New(ctx, arrclient.Config{Kind: r.Status.Backend, BaseURL: backend.BaseURL, APIKey: backend.APIKey})
	if e != nil {
		return ARRImportStatus{}, e
	}
	defer client.Close()
	at := time.Now().UTC()
	r.Status.CheckedAt = &at
	if r.Status.CommandID > 0 {
		command, err := client.Status(ctx, r.Status.CommandID)
		if err != nil {
			// Commands can expire while import history remains available.
			r.Status.Reason = "provider_unavailable"
		} else {
			switch command.Status {
			case "failed", "aborted", "cancelled", "orphaned":
				r.Status.State = "import_failed"
				r.Status.Reason = "import_failed"
			default:
				if command.Result == "unsuccessful" {
					r.Status.State = "import_failed"
					r.Status.Reason = "import_failed"
				} else if command.Status == "completed" {
					r.Status.State = "awaiting_files"
					r.Status.Reason = "no_imported_file"
				} else {
					r.Status.State = "importing"
					r.Status.Reason = ""
				}
			}
		}
	}
	files, err := client.ImportedFiles(ctx, r.DownloadID)
	if err != nil {
		r.Status.Reason = "provider_unavailable"
	} else if len(files) > 0 {
		if check(ctx) != nil {
			return ARRImportStatus{}, ErrARRImportUnavailable
		}
		result, err := library.ImportARRFiles(ctx, r.Status.LibraryID, r.LibraryRoot, files)
		r.Status.ItemIDs = result.ItemIDs
		switch {
		case err != nil:
			r.Status.State = "library_scan_failed"
			r.Status.Reason = "library_scan_failed"
		case result.Missing:
			r.Status.State = "awaiting_files"
			r.Status.Reason = "library_file_missing"
		case result.InvalidMedia:
			r.Status.State = "media_unavailable"
			r.Status.Reason = "media_not_playable"
		case result.NeedsMatch:
			r.Status.State = "needs_match"
			r.Status.Reason = result.Reason
			if r.Status.Reason == "" {
				r.Status.Reason = "match_in_library"
			}
			r.Status.ItemIDs = result.ItemIDs
		default:
			r.Status.State = "available"
			r.Status.Reason = ""
			r.Status.ItemIDs = result.ItemIDs
		}
	}
	if check(ctx) != nil {
		return ARRImportStatus{}, ErrARRImportUnavailable
	}
	return s.persistARRStatus(ctx, id, r)
}
