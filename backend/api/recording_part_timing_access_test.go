package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"novastream/models"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
	"novastream/services/users"
)

// Real API registration and original recording/PIN authority surround controlled
// timing ports. Actual FD/prober/SQL and hardware execution are separate proofs.
type timingAccessService struct {
	*recordings.Service
	gets      atomic.Int32
	refreshes atomic.Int32
	checks    atomic.Int32
	perform   func(context.Context, string, int, func() error) error
}

func (s *timingAccessService) observe(ctx context.Context, id string, index int, check func() error) (recordings.PartTiming, error) {
	if check == nil {
		return recordings.PartTiming{}, errors.New("missing original authority")
	}
	s.checks.Add(1)
	if err := check(); err != nil {
		return recordings.PartTiming{}, err
	}
	if s.perform != nil {
		if err := s.perform(ctx, id, index, check); err != nil {
			return recordings.PartTiming{}, err
		}
	}
	s.checks.Add(1)
	if err := check(); err != nil {
		return recordings.PartTiming{}, err
	}
	return recordings.PartTiming{RecordingID: id, PartIndex: index, State: "unknown", Reason: "no_observation"}, nil
}
func (s *timingAccessService) GetPartTiming(ctx context.Context, id string, index int, check func() error) (recordings.PartTiming, error) {
	s.gets.Add(1)
	return s.observe(ctx, id, index, check)
}
func (s *timingAccessService) RefreshPartTiming(ctx context.Context, id string, index int, check func() error) (recordings.PartTiming, error) {
	s.refreshes.Add(1)
	return s.observe(ctx, id, index, check)
}

type timingAccessFixture struct {
	*pinTestEnv
	store          *timingAccessService
	expiredAccount string
	child          string
	data           []byte
}

func newTimingAccessFixture(t *testing.T) *timingAccessFixture {
	t.Helper()
	f := &timingAccessFixture{pinTestEnv: &pinTestEnv{t: t}, data: bytes.Repeat([]byte("SYNTHETIC-OWNED-PART\n"), 64)}
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
	f.store = &timingAccessService{Service: recordings.NewService(repository, "must-not-execute-ffmpeg", output)}
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

func timingAccessTarget(profile string, refresh bool) string {
	path := "/api/live/recordings/job-" + url.PathEscape(profile) + "/parts/1/timing"
	if refresh {
		path += "/refresh"
	}
	return path
}

func TestRecordingPartTimingActualAccountAndPIN(t *testing.T) {
	f := newTimingAccessFixture(t)
	for _, refresh := range []bool{false, true} {
		method := "GET"
		if refresh {
			method = "POST"
		}
		t.Run(method, func(t *testing.T) {
			cases := []struct {
				name   string
				status int
				make   func() pinRequest
			}{
				{"unauthenticated", 401, func() pinRequest { return pinRequest{method: method, target: timingAccessTarget(f.open, refresh)} }},
				{"invalid-session", 401, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget(f.open, refresh), token: "synthetic-invalid"}
				}},
				{"revoked-session", 401, func() pinRequest {
					token := f.login(f.account1)
					if err := f.sessions.Revoke(token); err != nil {
						t.Fatal(err)
					}
					return pinRequest{method: method, target: timingAccessTarget(f.open, refresh), token: token}
				}},
				{"expired-account", 401, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget(f.open, refresh), token: f.login(f.expiredAccount)}
				}},
				{"scoped-stream", 403, func() pinRequest {
					s, err := f.sessions.CreateScoped(f.account1, false, "synthetic-timing", "loopback", time.Hour, models.SessionScopeStream)
					if err != nil {
						t.Fatal(err)
					}
					return pinRequest{method: method, target: timingAccessTarget(f.open, refresh), token: s.Token}
				}},
				{"foreign-recording", 404, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget(f.foreign, refresh), token: f.login(f.account1)}
				}},
				{"missing-recording", 404, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget("missing", refresh), token: f.login(f.account1)}
				}},
				{"locked-owner", 403, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget(f.locked, refresh), token: f.login(f.account1)}
				}},
				{"child-owner", 403, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget(f.child, refresh), token: f.login(f.account1)}
				}},
				{"another-session-unlock", 403, func() pinRequest {
					token := f.login(f.account1)
					unlock := f.verify(token, f.locked, "1234").UnlockToken
					return pinRequest{method: method, target: timingAccessTarget(f.locked, refresh), token: f.login(f.account1), unlock: unlock}
				}},
				{"conflicting-selector", 404, func() pinRequest {
					token := f.login(f.account1)
					return pinRequest{method: method, target: timingAccessTarget(f.open, refresh) + "?profileId=" + url.QueryEscape(f.locked), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
				}},
				{"noncanonical-index", 400, func() pinRequest {
					return pinRequest{method: method, target: strings.Replace(timingAccessTarget(f.open, refresh), "/parts/1/", "/parts/01/", 1), token: f.login(f.account1)}
				}},
				{"own-open", 200, func() pinRequest {
					return pinRequest{method: method, target: timingAccessTarget(f.open, refresh), token: f.login(f.account1)}
				}},
				{"own-unlocked", 200, func() pinRequest {
					token := f.login(f.account1)
					return pinRequest{method: method, target: timingAccessTarget(f.locked, refresh), token: token, unlock: f.verify(token, f.locked, "1234").UnlockToken}
				}},
			}
			for _, c := range cases {
				t.Run(c.name, func(t *testing.T) {
					f.pinTestEnv.t = t
					req := c.make()
					f.store.gets.Store(0)
					f.store.refreshes.Store(0)
					f.store.checks.Store(0)
					w := f.do(req)
					if w.Code != c.status {
						t.Fatalf("status=%d want=%d", w.Code, c.status)
					}
					if c.status != 200 {
						if f.store.gets.Load()+f.store.refreshes.Load() != 0 || strings.Contains(w.Body.String(), "PRIVATE-") {
							t.Fatal("denial reached timing port or disclosed private recording data")
						}
						return
					}
					if f.store.gets.Load()+f.store.refreshes.Load() != 1 || f.store.checks.Load() != 2 || refresh && f.store.gets.Load() != 0 || !refresh && f.store.refreshes.Load() != 0 {
						t.Fatal("wrong operation or missing original authority callback")
					}
					var value recordings.PartTiming
					if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
						t.Fatal(err)
					}
					if value.RecordingID == "" || value.PartIndex != 1 || value.State != "unknown" || value.ContainerDurationMS != nil || value.Binding != "" || w.Header().Get("Cache-Control") != "private, no-store" {
						t.Fatal("unknown observation became an estimated/current-file claim")
					}
					if strings.Contains(w.Body.String(), "PRIVATE-") || strings.Contains(w.Body.String(), "part-000001.ts") {
						t.Fatal("private ledger/path escaped timing projection")
					}
				})
			}
		})
	}
}

func TestRecordingPartTimingOriginalAuthorityAfterPort(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		for _, change := range []string{"session-revoked", "pin-changed"} {
			name := "GET-"
			method := "GET"
			if refresh {
				name = "POST-"
				method = "POST"
			}
			t.Run(name+change, func(t *testing.T) {
				f := newTimingAccessFixture(t)
				token := f.login(f.account1)
				unlock := f.verify(token, f.locked, "1234").UnlockToken
				f.store.perform = func(ctx context.Context, id string, index int, check func() error) error {
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("missing original bounded request context")
					}
					if change == "session-revoked" {
						if err := f.sessions.Revoke(token); err != nil {
							t.Fatal(err)
						}
					} else {
						if _, err := f.users.SetPin(f.locked, "4321"); err != nil {
							t.Fatal(err)
						}
					}
					if err := check(); err == nil {
						t.Fatal("original authority survived revocation")
					}
					return recordings.ErrRecordingExportAccess
				}
				w := f.do(pinRequest{method: method, target: timingAccessTarget(f.locked, refresh), token: token, unlock: unlock})
				if w.Code != http.StatusForbidden || f.store.gets.Load()+f.store.refreshes.Load() != 1 || strings.Contains(w.Body.String(), `"state"`) {
					t.Fatalf("revoked original authority status=%d", w.Code)
				}
			})
		}
	}
}
