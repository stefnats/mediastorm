package api

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
	"novastream/services/users"
)

// Embedding the unused repository methods deliberately leaves them unavailable:
// exporting these terminal fixtures must only read the stored recording ledger.
type exportAccessRepository struct {
	datastore.RecordingRepository
	jobs map[string]models.Recording
}

func (r *exportAccessRepository) Get(ctx context.Context, id string) (*models.Recording, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	job, ok := r.jobs[id]
	if !ok {
		return nil, recordings.ErrRecordingNotFound
	}
	job.Capture = models.CopyRecordingCapture(job.Capture)
	return &job, nil
}

type exportAccessService struct {
	*recordings.Service
	opens       atomic.Int32
	projections atomic.Int32
}

func (s *exportAccessService) Get(id string) (*models.Recording, error) {
	s.projections.Add(1)
	return s.Service.Get(id)
}

func (s *exportAccessService) OpenPartsExport(ctx context.Context, id string, check func() error) (*recordings.PartsExport, error) {
	s.opens.Add(1)
	return s.Service.OpenPartsExport(ctx, id, check)
}

type exportAccessFixture struct {
	*pinTestEnv
	store          *exportAccessService
	expiredAccount string
	child          string
	data           []byte
}

func newExportAccessFixture(t *testing.T) *exportAccessFixture {
	t.Helper()
	f := &exportAccessFixture{pinTestEnv: &pinTestEnv{t: t}, data: bytes.Repeat([]byte("SYNTHETIC-OWNED-PART\n"), 64)}
	t.Setenv("STRMR_INITIAL_ADMIN_PASSWORD", "synthetic-export-master-password")
	accountService, err := accounts.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i, destination := range []*string{&f.account1, &f.account2, &f.expiredAccount} {
		var expires *time.Time
		if i == 2 {
			at := time.Now().Add(-time.Hour)
			expires = &at
		}
		account, createErr := accountService.CreateWithExpiry([]string{"export-owner", "export-neighbour", "export-expired"}[i], "synthetic-export-password", expires)
		if createErr != nil {
			t.Fatal(createErr)
		}
		*destination = account.ID
	}
	f.sessions, err = sessions.NewService("", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	f.users, err = users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []struct {
		destination        *string
		account, name, pin string
	}{
		{&f.open, f.account1, "Export open", ""},
		{&f.locked, f.account1, "Export locked", "1234"},
		{&f.foreign, f.account2, "Export neighbour", "9999"},
		{&f.child, f.account1, "Export child", ""},
	} {
		user, createErr := f.users.CreateForAccount(profile.account, profile.name)
		if createErr != nil {
			t.Fatal(createErr)
		}
		*profile.destination = user.ID
		if profile.pin != "" {
			if _, err = f.users.SetPin(user.ID, profile.pin); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = f.users.SetKidsProfile(f.child, true); err != nil {
		t.Fatal(err)
	}
	f.unlocks = profileunlock.NewService(nil)
	output := t.TempDir()
	repository := &exportAccessRepository{jobs: map[string]models.Recording{}}
	at := time.Date(2026, 11, 16, 8, 0, 0, 0, time.UTC)
	ended := at.Add(time.Minute)
	for _, profile := range []string{f.open, f.locked, f.foreign, f.child} {
		id := "job-" + profile
		directory := filepath.Join(output, profile, id)
		if err = os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		part := filepath.Join(directory, "part-000001.ts")
		if err = os.WriteFile(part, f.data, 0600); err != nil {
			t.Fatal(err)
		}
		repository.jobs[id] = models.Recording{
			ID: id, UserID: profile, Status: models.RecordingStatusCompleted,
			CreatedAt: at, UpdatedAt: ended, StartAt: at, EndAt: ended,
			OutputPath: part, OutputSizeBytes: int64(len(f.data)),
			Title: "PRIVATE-EXPORT-TITLE-CANARY", SourceURL: "https://synthetic.invalid/PRIVATE-SOURCE-CANARY",
			Capture: &models.RecordingCapture{Version: 1, Phase: "finished", RunID: "private-run-canary",
				Parts: []models.RecordingPart{{Index: 1, StartedAt: at, EndedAt: &ended, Bytes: int64(len(f.data)), EndReason: "schedule", SourceID: "private-source-id-canary"}},
				Gaps:  []models.RecordingGap{}},
		}
	}
	f.store = &exportAccessService{Service: recordings.NewService(repository, "must-not-execute-ffmpeg", output)}
	handler := handlers.NewRecordingsHandler(f.store, f.users)
	policy := func() config.SecuritySettings {
		return config.SecuritySettings{EnforceProfilePins: true, ProfileUnlockTTLHours: 12}
	}
	handler.SetRecordingStreamAccess(handlers.NewRecordingStreamAccess(f.store, f.sessions, accountService, f.users, f.unlocks,
		func() (config.SecuritySettings, error) { return policy(), nil }))
	userHandler := handlers.NewUsersHandler(f.users, accountService)
	userHandler.SetProfileUnlocks(f.unlocks, func() time.Duration { return policy().ProfileUnlockTTL() })
	router := mux.NewRouter()
	Register(router,
		nil, &handlers.MetadataHandler{}, // settings, metadata
		nil, nil, nil, nil, nil, nil, nil, // indexer through video
		userHandler, nil, nil, nil, // users, watchlist, custom lists, display list
		&handlers.HistoryHandler{},
		nil, nil, nil, handler, nil, // debug, logs, live, recordings, local media
		nil, nil, nil, nil, nil, nil, nil, // EPG through content preferences
		nil, nil, nil, nil, nil, nil, nil, // hidden items through preroll
		accountService, f.sessions, f.users, nil, nil, nil, "", nil,
		WithProfileUnlocks(f.unlocks, policy),
	)
	f.router = router
	return f
}

func exportAccessTarget(profile string) string {
	return "/api/live/recordings/job-" + url.PathEscape(profile) + "/export"
}

func TestRecordingExportActualAccountAndPINMiddleware(t *testing.T) {
	f := newExportAccessFixture(t)
	cases := []struct {
		name   string
		status int
		make   func() pinRequest
	}{
		{"unauthenticated", 401, func() pinRequest { return pinRequest{method: "GET", target: exportAccessTarget(f.open)} }},
		{"invalid session", 401, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget(f.open), token: "synthetic-invalid"}
		}},
		{"revoked session", 401, func() pinRequest {
			token := f.login(f.account1)
			if err := f.sessions.Revoke(token); err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: exportAccessTarget(f.open), token: token}
		}},
		{"expired account", 401, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget(f.open), token: f.login(f.expiredAccount)}
		}},
		{"scoped stream", 403, func() pinRequest {
			session, err := f.sessions.CreateScoped(f.account1, false, "synthetic-export", "127.0.0.1", time.Hour, models.SessionScopeStream)
			if err != nil {
				t.Fatal(err)
			}
			return pinRequest{method: "GET", target: exportAccessTarget(f.open), token: session.Token}
		}},
		{"foreign recording", 404, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget(f.foreign), token: f.login(f.account1)}
		}},
		{"unknown recording", 404, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget("unknown"), token: f.login(f.account1)}
		}},
		{"locked stored owner without selector", 403, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget(f.locked), token: f.login(f.account1)}
		}},
		{"child stored owner", 403, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget(f.child), token: f.login(f.account1)}
		}},
		{"unlock from another session", 403, func() pinRequest {
			first := f.login(f.account1)
			unlock := f.verify(first, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: exportAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"foreign unlock", 403, func() pinRequest {
			token := f.login(f.account2)
			unlock := f.verify(token, f.foreign, "9999").UnlockToken
			return pinRequest{method: "GET", target: exportAccessTarget(f.locked), token: f.login(f.account1), unlock: unlock}
		}},
		{"conflicting original selector", 404, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: exportAccessTarget(f.open) + "?userId=" + url.QueryEscape(f.locked), token: token, unlock: unlock}
		}},
		{"case variant conflicting selector", 404, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: exportAccessTarget(f.open) + "?ProfileID=" + url.QueryEscape(f.locked), token: token, unlock: unlock}
		}},
		{"own open owner without selector", 200, func() pinRequest {
			return pinRequest{method: "GET", target: exportAccessTarget(f.open), token: f.login(f.account1)}
		}},
		{"own unlocked owner", 200, func() pinRequest {
			token := f.login(f.account1)
			return pinRequest{method: "GET", target: exportAccessTarget(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
		}},
		{"query unlock", 200, func() pinRequest {
			token := f.login(f.account1)
			unlock := f.verify(token, f.locked, "1234").UnlockToken
			return pinRequest{method: "GET", target: exportAccessTarget(f.locked) + "?profileUnlock=" + url.QueryEscape(unlock), token: token}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f.pinTestEnv.t = t
			request := tc.make()
			f.store.opens.Store(0)
			f.store.projections.Store(0)
			reply := f.do(request)
			if reply.Code != tc.status {
				t.Fatalf("status=%d want=%d", reply.Code, tc.status)
			}
			if tc.status != http.StatusOK {
				if f.store.opens.Load() != 0 || f.store.projections.Load() != 0 {
					t.Fatal("denied request reached file-opening export adapter or capture projection")
				}
				if strings.Contains(reply.Header().Get("Content-Type"), "zip") || bytes.HasPrefix(reply.Body.Bytes(), []byte("PK")) {
					t.Fatal("denied request exposed archive bytes")
				}
				return
			}
			if f.store.opens.Load() != 1 {
				t.Fatal("accepted request did not open exactly one export")
			}
			if !strings.Contains(reply.Header().Get("Content-Type"), "application/zip") || !strings.Contains(reply.Header().Get("Content-Disposition"), "attachment") {
				t.Fatal("accepted archive lacks download headers")
			}
			archive, err := zip.NewReader(bytes.NewReader(reply.Body.Bytes()), int64(reply.Body.Len()))
			if err != nil {
				t.Fatal("accepted response is not a complete ZIP archive")
			}
			parts := 0
			for _, entry := range archive.File {
				reader, err := entry.Open()
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(reader)
				reader.Close()
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasSuffix(entry.Name, ".ts") {
					parts++
					if !bytes.Equal(data, f.data) || entry.Method != zip.Store {
						t.Fatal("export changed original part bytes")
					}
				}
				for _, canary := range []string{"PRIVATE-EXPORT-TITLE-CANARY", "PRIVATE-SOURCE-CANARY", "private-run-canary", "private-source-id-canary"} {
					if bytes.Contains(data, []byte(canary)) || strings.Contains(entry.Name, canary) {
						t.Fatal("export leaked private ledger fields")
					}
				}
			}
			if parts != 1 || len(archive.File) != 2 {
				t.Fatal("export must contain the single owned part and manifest")
			}
		})
	}
}
