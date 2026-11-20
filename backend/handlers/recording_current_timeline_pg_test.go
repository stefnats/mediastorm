package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/config"
	"novastream/models"
	"novastream/services/recordings"
)

type currentTimelineFixture struct {
	pin       *publicPinCarrierFixture
	h         *RecordingCurrentTimelineHandler
	service   *recordings.Service
	job       models.Recording
	root, dir string
	tools     recordings.DerivedTools
	mu        sync.Mutex
	current   func(context.Context) error
	changed   atomic.Bool
	policy    atomic.Bool
}

func newCurrentTimelineFixture(t *testing.T) *currentTimelineFixture {
	t.Helper()
	f := &currentTimelineFixture{pin: newPublicPinCarrierFixture(t), root: t.TempDir()}
	f.policy.Store(true)
	f.dir = filepath.Join(f.root, f.pin.profile.ID, "timeline-job")
	if err := os.MkdirAll(f.dir, 0700); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	middle, last := start.Add(800*time.Millisecond), start.Add(1600*time.Millisecond)
	f.job = models.Recording{ID: "timeline-job", UserID: f.pin.profile.ID, Type: models.RecordingTypeTimeBlock, Status: models.RecordingStatusCompleted, Title: "Synthetic timeline", StartAt: start, EndAt: last, CreatedAt: start, UpdatedAt: start, Capture: &models.RecordingCapture{Version: 1, Phase: "completed", RunID: "run-A", Parts: []models.RecordingPart{{Index: 1, StartedAt: start, EndedAt: &middle, Bytes: 1, EndReason: "schedule"}, {Index: 2, StartedAt: middle, EndedAt: &last, Bytes: 1, EndReason: "schedule"}}, Gaps: []models.RecordingGap{}}}
	if err := f.pin.store.Recordings().Create(t.Context(), &f.job); err != nil {
		t.Fatal(err)
	}
	f.service = recordings.NewService(f.pin.store.Recordings(), "/usr/bin/ffmpeg", f.root)
	toolsDir := t.TempDir()
	for _, name := range []string{"ffmpeg", "ffprobe"} {
		if err := os.WriteFile(filepath.Join(toolsDir, name), []byte("#!/bin/sh\nprintf x >> \"$0.calls\"\nexec /usr/bin/"+name+" \"$@\"\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f.service.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	f.tools = recordings.DerivedTools{FFmpegPath: filepath.Join(toolsDir, "ffmpeg"), FFprobePath: filepath.Join(toolsDir, "ffprobe"), ConfigurationKey: "synthetic-original-tools"}
	if err := f.service.ConfigureDerivedJoiner(t.Context(), f.tools, func(ctx context.Context) (recordings.DerivedTools, error) {
		f.mu.Lock()
		hook := f.current
		f.mu.Unlock()
		if hook != nil {
			if err := hook(ctx); err != nil {
				return recordings.DerivedTools{}, err
			}
		}
		tools := f.tools
		if f.changed.Load() {
			tools.ConfigurationKey = "synthetic-changed-tools"
		}
		return tools, nil
	}); err != nil {
		t.Fatal(err)
	}
	var err error
	f.h, err = NewRecordingCurrentTimelineHandler(f.pin.store, f.service, f.pin.pins, func() (config.SecuritySettings, error) {
		return config.SecuritySettings{EnforceProfilePins: f.policy.Load()}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.h.Close)
	return f
}
func (f *currentTimelineFixture) request(t *testing.T) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, release := f.pin.request(t, http.MethodGet, "/private/timeline")
	return mux.SetURLVars(r, map[string]string{"recordingID": f.job.ID}), release
}

type currentTimelineWriter struct {
	*httptest.ResponseRecorder
	mu        sync.Mutex
	deadlines []time.Time
	setter    func(time.Time)
}

func newCurrentTimelineWriter() *currentTimelineWriter {
	return &currentTimelineWriter{ResponseRecorder: httptest.NewRecorder()}
}
func (w *currentTimelineWriter) SetWriteDeadline(at time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, at)
	hook := w.setter
	w.mu.Unlock()
	if hook != nil {
		hook(at)
	}
	return nil
}
func (f *currentTimelineFixture) get(t *testing.T) *currentTimelineWriter {
	t.Helper()
	r, release := f.request(t)
	defer release()
	w := newCurrentTimelineWriter()
	f.h.Get(w, r)
	return w
}
func (f *currentTimelineFixture) generateAndPrepare(t *testing.T) recordings.DerivedRecording {
	t.Helper()
	for i, color := range []string{"red", "blue"} {
		name := filepath.Join(f.dir, fmt.Sprintf("part-%06d.ts", i+1))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		args := []string{"-nostdin", "-hide_banner", "-v", "error", "-filter_threads", "1", "-f", "lavfi", "-i", "color=c=" + color + ":s=64x64:r=25", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-t", "0.8", "-c:v", "libx264", "-threads", "1", "-preset", "ultrafast", "-g", "10", "-bf", "0", "-c:a", "aac", "-ac", "1", "-ar", "48000", "-f", "mpegts", name}
		output, err := exec.CommandContext(ctx, "/usr/bin/ffmpeg", args...).CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("synthetic generation failed: %v %s", err, output)
		}
		info, err := os.Stat(name)
		if err != nil {
			t.Fatal(err)
		}
		f.job.Capture.Parts[i].Bytes = info.Size()
	}
	if err := f.pin.store.Recordings().Update(t.Context(), &f.job); err != nil {
		t.Fatal(err)
	}
	r, release := f.request(t)
	defer release()
	ctx, cancel, authorize, ok := f.h.guard.recordingDerivedAuthority(httptest.NewRecorder(), r, false)
	if !ok {
		t.Fatal("actual original recording authority refused fixture setup")
	}
	defer cancel()
	value, err := f.service.PrepareDerived(ctx, f.job.ID, authorize)
	if err != nil || value.State != "ready" {
		t.Fatal("actual explicit fixture preparation failed", err)
	}
	return value
}
func (f *currentTimelineFixture) toolCounts(t *testing.T) [2]int {
	t.Helper()
	var result [2]int
	for i, name := range []string{f.tools.FFmpegPath, f.tools.FFprobePath} {
		raw, err := os.ReadFile(name + ".calls")
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		result[i] = len(raw)
	}
	return result
}
func TestRecordingCurrentTimelineActualReadyUsesMeasuredDataWithoutNewTools(t *testing.T) {
	f := newCurrentTimelineFixture(t)
	descriptor := f.generateAndPrepare(t)
	var originals [2][32]byte
	for i := range originals {
		raw, err := os.ReadFile(filepath.Join(f.dir, fmt.Sprintf("part-%06d.ts", i+1)))
		if err != nil {
			t.Fatal(err)
		}
		originals[i] = sha256.Sum256(raw)
	}
	counts := f.toolCounts(t)
	w := f.get(t)
	var value recordings.DerivedTimelineData
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.State != "known" || value.ArtifactID != descriptor.ID || value.DurationMS != descriptor.DurationMS || len(value.Spans) != 2 || value.Spans[1].StartMS+value.Spans[1].DurationMS != value.DurationMS || value.ObservedAt == nil || !value.ObservedAt.Equal(*descriptor.ObservedAt) || value.ExpiresAt == nil || !value.ExpiresAt.Equal(*descriptor.ExpiresAt) {
		t.Fatalf("genuine current measured timeline failed: %d", w.Code)
	}
	identity, err := f.pin.store.ReadRecordingTimelineIdentity(t.Context(), f.job.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := recordings.ProjectDerivedTimelineData(identity.Incarnation(), descriptor, time.Now())
	if err != nil || expected.MappingRevision != value.MappingRevision {
		t.Fatal("handler did not use actual original 084 identity")
	}
	if f.toolCounts(t) != counts {
		t.Fatal("timeline GET started a process or repaired media")
	}
	for i := range originals {
		raw, err := os.ReadFile(filepath.Join(f.dir, fmt.Sprintf("part-%06d.ts", i+1)))
		if err != nil || sha256.Sum256(raw) != originals[i] {
			t.Fatal("timeline observation modified original media")
		}
	}
	if w.Header().Get("Cache-Control") != "private, no-store" || w.Header().Get("X-Content-Type-Options") != "nosniff" || strings.Contains(w.Body.String(), f.root) || strings.Contains(w.Body.String(), f.pin.session.Token) {
		t.Fatal("timeline exposed source or credential")
	}
}
func TestRecordingCurrentTimelineUnknownAndConfigurationDriftNeverPrepare(t *testing.T) {
	f := newCurrentTimelineFixture(t)
	for _, changed := range []bool{false, true} {
		f.changed.Store(changed)
		w := f.get(t)
		var value recordings.DerivedTimelineData
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.State != "unknown" || value.ArtifactID != "" || value.Incarnation != "" || len(value.Spans) != 0 {
			t.Fatal("unknown observation invented coordinates", w.Code)
		}
		reason := "not_prepared"
		if changed {
			reason = "configuration_changed"
		}
		if value.Reason != reason {
			t.Fatal("unknown reason did not reflect actual service")
		}
	}
	if f.toolCounts(t) != ([2]int{}) {
		t.Fatal("missing ready data implicitly prepared media")
	}
}
func TestRecordingCurrentTimelineActualPINChildAndScopeDenyWithoutMap(t *testing.T) {
	for _, name := range []string{"locked", "wrong unlock", "child", "scoped", "flag disabled", "valid unlock"} {
		t.Run(name, func(t *testing.T) {
			f := newCurrentTimelineFixture(t)
			f.pin.setPIN(t, "1357")
			if name == "child" {
				if _, err := f.pin.store.Pool().Exec(t.Context(), `UPDATE users SET is_kids_profile=true WHERE id=$1`, f.pin.profile.ID); err != nil {
					t.Fatal(err)
				}
			}
			if name == "scoped" {
				session, err := f.pin.sessions.CreateScoped("actor", false, "synthetic-timeline", "loopback", time.Hour, models.SessionScopeStream)
				if err != nil {
					t.Fatal(err)
				}
				f.pin.session = session
			}
			if name == "flag disabled" {
				f.policy.Store(false)
			}
			r, release := f.request(t)
			defer release()
			if name == "valid unlock" {
				r.Header.Set("X-Profile-Unlock", f.pin.issue(t, r))
			}
			if name == "wrong unlock" {
				r.Header.Set("X-Profile-Unlock", "synthetic-invalid-unlock")
			}
			w := newCurrentTimelineWriter()
			f.h.Get(w, r)
			want := 403
			if name == "flag disabled" || name == "valid unlock" {
				want = 200
			}
			if name == "scoped" {
				want = 404
			}
			if w.Code != want || want != 200 && strings.Contains(w.Body.String(), `"spans"`) || f.toolCounts(t) != ([2]int{}) {
				t.Fatalf("existing original recording PIN/scope policy changed: %d want%d", w.Code, want)
			}
		})
	}
}
func TestRecordingCurrentTimelineOriginalStoredOwnerAndMasterPolicy(t *testing.T) {
	for _, master := range []bool{false, true} {
		t.Run(fmt.Sprint(master), func(t *testing.T) {
			f := newCurrentTimelineFixture(t)
			foreign, err := f.pin.users.CreateForAccount("other", "Synthetic recording owner")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.pin.store.Pool().Exec(t.Context(), `UPDATE recordings SET user_id=$1 WHERE id=$2`, foreign.ID, f.job.ID); err != nil {
				t.Fatal(err)
			}
			if master {
				if _, err = f.pin.store.Pool().Exec(t.Context(), `UPDATE accounts SET is_master=true WHERE id='actor'`); err != nil {
					t.Fatal(err)
				}
			}
			w := f.get(t)
			want := 404
			if master {
				want = 200
			}
			if w.Code != want {
				t.Fatalf("existing DVR master-on-behalf semantics changed: %d want%d", w.Code, want)
			}
		})
	}
}
func TestRecordingCurrentTimelineRejectsUnboundStoreAndSelectorAcquisition(t *testing.T) {
	f := newCurrentTimelineFixture(t)
	other := newPublicPinCarrierFixture(t)
	if _, err := NewRecordingCurrentTimelineHandler(other.store, f.service, f.pin.pins, func() (config.SecuritySettings, error) { return config.SecuritySettings{}, nil }); err == nil {
		t.Fatal("foreign store installed")
	}
	if _, err := NewRecordingCurrentTimelineHandler(f.pin.store, recordings.NewService(nil, "unused", t.TempDir()), f.pin.pins, func() (config.SecuritySettings, error) { return config.SecuritySettings{}, nil }); err == nil {
		t.Fatal("fake repository installed")
	}
	for _, name := range []string{"HEAD", "POST", "query", "range", "conditional", "body with zero length", "selector"} {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/private/timeline", nil)
			r = mux.SetURLVars(r, map[string]string{"recordingID": f.job.ID})
			want := 400
			switch name {
			case "HEAD", "POST":
				r.Method = name
				want = 405
			case "query":
				r.URL.RawQuery = "profileId=other"
			case "range":
				r.Header.Set("Range", "bytes=0-1")
			case "conditional":
				r.Header.Set("If-None-Match", "synthetic")
			case "body with zero length":
				r.Body = io.NopCloser(strings.NewReader("hidden"))
				r.ContentLength = 0
			case "selector":
				r = mux.SetURLVars(r, map[string]string{"recordingID": "../foreign"})
			}
			w := newCurrentTimelineWriter()
			f.h.Get(w, r)
			if w.Code != want || f.toolCounts(t) != ([2]int{}) {
				t.Fatalf("invalid request acquired original recording: %d", w.Code)
			}
		})
	}
}
func TestRecordingCurrentTimelineActualRunABAAfterInitialIdentityDoesNotRebase(t *testing.T) {
	f := newCurrentTimelineFixture(t)
	var once sync.Once
	f.mu.Lock()
	f.current = func(ctx context.Context) error {
		var err error
		once.Do(func() {
			_, err = f.pin.store.Pool().Exec(ctx, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-B"') WHERE id=$1;`, f.job.ID)
			if err == nil {
				_, err = f.pin.store.Pool().Exec(ctx, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-A"') WHERE id=$1`, f.job.ID)
			}
		})
		return err
	}
	f.mu.Unlock()
	w := f.get(t)
	if w.Code != 409 || strings.Contains(w.Body.String(), `"spans"`) {
		t.Fatal("actual same-value run ABA adopted a new timeline", w.Code)
	}
}
func TestRecordingCurrentTimelineReadySourceConfigAndRestoreInvalidationIsUnknown(t *testing.T) {
	for _, kind := range []string{"source", "config", "restore"} {
		t.Run(kind, func(t *testing.T) {
			f := newCurrentTimelineFixture(t)
			f.generateAndPrepare(t)
			counts := f.toolCounts(t)
			switch kind {
			case "source":
				if err := os.Remove(filepath.Join(f.dir, "part-000001.ts")); err != nil {
					t.Fatal(err)
				}
			case "config":
				f.changed.Store(true)
			case "restore":
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				release, err := recordings.QuiesceForRestore(ctx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				release()
			}
			w := f.get(t)
			var value recordings.DerivedTimelineData
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &value) != nil || value.State != "unknown" || len(value.Spans) != 0 || value.MappingRevision != "" {
				t.Fatal("invalid actual ready artifact retained coordinates", w.Code)
			}
			if f.toolCounts(t) != counts {
				t.Fatal("passive invalidation rebuilt expired source")
			}
		})
	}
}

func timelineObserver(t *testing.T, f *currentTimelineFixture) (*pgxpool.Pool, string) {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ACCESS_TEST_DATABASE_URL")
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("dedicated synthetic Unix-socket observer required")
	}
	pool, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var schema string
	if f.pin.store.Pool().QueryRow(t.Context(), `SELECT current_schema()`).Scan(&schema) != nil {
		t.Fatal("read own schema")
	}
	return pool, schema
}
func timelineWaitPending(t *testing.T, pool *pgxpool.Pool, schema string, unfinished <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		var pending bool
		if err := pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%s.incarnation%' AND position($1 in query)>0)`, schema).Scan(&pending); err != nil {
			t.Fatal("observe own blocked timeline SQL", err)
		}
		if pending {
			return
		}
		select {
		case <-unfinished:
			t.Fatal("timeline returned before actual pending query")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("actual timeline query never blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func TestRecordingCurrentTimelineCloseJoinsActualBlockedPGQuery(t *testing.T) {
	f := newCurrentTimelineFixture(t)
	observer, schema := timelineObserver(t, f)
	lock, err := observer.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = lock.Rollback(ctx)
	})
	var once sync.Once
	f.mu.Lock()
	f.current = func(ctx context.Context) error {
		var err error
		once.Do(func() {
			_, err = lock.Exec(ctx, `LOCK TABLE `+pgx.Identifier{schema, "recording_playback_state"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`)
		})
		return err
	}
	f.mu.Unlock()
	r, release := f.request(t)
	defer release()
	w := newCurrentTimelineWriter()
	done := make(chan struct{})
	go func() { defer close(done); f.h.Get(w, r) }()
	timelineWaitPending(t, observer, schema, done)
	f.h.mu.Lock()
	charged := len(f.h.active)
	f.h.mu.Unlock()
	if charged != 1 {
		t.Fatal("pending actual query lost request admission")
	}
	closed := make(chan struct{})
	go func() { f.h.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(4 * time.Second):
		t.Fatal("Close did not join actual canceled PG query and rollback")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close returned before request finished")
	}
	var pending bool
	if observer.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE state='active' AND query LIKE '%s.incarnation%' AND position($1 in query)>0)`, schema).Scan(&pending) != nil || pending {
		t.Fatal("source admission refunded while actual SQL remained active")
	}
	if strings.Contains(w.Body.String(), `"spans"`) {
		t.Fatal("canceled query published timeline")
	}
	if err = lock.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type timelineHeldWriter struct {
	header                                             http.Header
	entered, release, deadlineEntered, deadlineRelease chan struct{}
	writeOnce, deadlineOnce                            sync.Once
	mu                                                 sync.Mutex
	deadlineCalls                                      int
	last                                               []byte
	status                                             int
}

func newTimelineHeldWriter() *timelineHeldWriter {
	return &timelineHeldWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{}), deadlineEntered: make(chan struct{}), deadlineRelease: make(chan struct{})}
}
func (w *timelineHeldWriter) Header() http.Header  { return w.header }
func (w *timelineHeldWriter) WriteHeader(code int) { w.status = code }
func (w *timelineHeldWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.last = data
	w.mu.Unlock()
	w.writeOnce.Do(func() { close(w.entered) })
	<-w.release
	return 0, errors.New("synthetic downstream interrupted")
}
func (w *timelineHeldWriter) SetWriteDeadline(time.Time) error {
	w.mu.Lock()
	w.deadlineCalls++
	calls := w.deadlineCalls
	w.mu.Unlock()
	if calls > 1 {
		w.deadlineOnce.Do(func() { close(w.deadlineEntered) })
		<-w.deadlineRelease
	}
	return nil
}
func TestRecordingCurrentTimelineConcurrentCloseWaitsForWriteAndDeadlineCallback(t *testing.T) {
	for _, writerFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(writerFirst), func(t *testing.T) {
			f := newCurrentTimelineFixture(t)
			r, release := f.request(t)
			defer release()
			w := newTimelineHeldWriter()
			var writeRelease, deadlineRelease sync.Once
			t.Cleanup(func() {
				writeRelease.Do(func() { close(w.release) })
				deadlineRelease.Do(func() { close(w.deadlineRelease) })
			})
			done := make(chan struct{})
			go func() { defer close(done); f.h.Get(w, r) }()
			select {
			case <-w.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("genuine timeline never reached downstream Write")
			}
			closed1, closed2 := make(chan struct{}), make(chan struct{})
			go func() { f.h.Close(); close(closed1) }()
			go func() { f.h.Close(); close(closed2) }()
			select {
			case <-w.deadlineEntered:
			case <-time.After(time.Second):
				t.Fatal("cancel did not interrupt downstream writer")
			}
			if writerFirst {
				writeRelease.Do(func() { close(w.release) })
			} else {
				deadlineRelease.Do(func() { close(w.deadlineRelease) })
			}
			select {
			case <-closed1:
				t.Fatal("Close refunded after only one cleanup end")
			case <-closed2:
				t.Fatal("concurrent Close skipped cleanup barrier")
			case <-time.After(50 * time.Millisecond):
			}
			f.h.mu.Lock()
			charged := len(f.h.active)
			f.h.mu.Unlock()
			if charged != 1 {
				t.Fatal("actual IO/callback still held but slot refunded")
			}
			writeRelease.Do(func() { close(w.release) })
			deadlineRelease.Do(func() { close(w.deadlineRelease) })
			for _, end := range []<-chan struct{}{closed1, closed2, done} {
				select {
				case <-end:
				case <-time.After(time.Second):
					t.Fatal("completed IO cleanup not joined")
				}
			}
			w.mu.Lock()
			defer w.mu.Unlock()
			if len(w.last) == 0 || !bytes.Equal(w.last, make([]byte, len(w.last))) {
				t.Fatal("staged JSON was not cleared after failed downstream write")
			}
		})
	}
}
func TestRecordingCurrentTimelineNonqueuedAdmissionBeforePGAndGlobalBounds(t *testing.T) {
	f := newCurrentTimelineFixture(t)
	var done []chan struct{}
	var writers []*timelineHeldWriter
	var releases []context.CancelFunc
	// Install cleanup before the first resource can be launched or a wait fail.
	t.Cleanup(func() {
		for _, w := range writers {
			select {
			case <-w.release:
			default:
				close(w.release)
			}
			select {
			case <-w.deadlineRelease:
			default:
				close(w.deadlineRelease)
			}
		}
		for _, end := range done {
			<-end
		}
		for _, release := range releases {
			release()
		}
	})
	for i := 0; i < 2; i++ {
		r, release := f.request(t)
		releases = append(releases, release)
		w := newTimelineHeldWriter()
		writers = append(writers, w)
		end := make(chan struct{})
		done = append(done, end)
		go func() { defer close(end); f.h.Get(w, r) }()
		select {
		case <-w.entered:
		case <-time.After(3 * time.Second):
			t.Fatal("actual writer did not start")
		}
	}
	r := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/private/timeline", nil), map[string]string{"recordingID": f.job.ID})
	w := newCurrentTimelineWriter()
	f.h.Get(w, r)
	if w.Code != 429 {
		t.Fatal("third request performed PG before nonqueued admission", w.Code)
	}
	for _, held := range writers {
		close(held.release)
		close(held.deadlineRelease)
	}
	for _, end := range done {
		<-end
	}
	// This part isolates the actual registry algorithm, not source permission.
	var handlers []*RecordingCurrentTimelineHandler
	type admission struct {
		handler *RecordingCurrentTimelineHandler
		op      *recordingTimelineOperation
	}
	var admitted []admission
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			for _, entry := range admitted {
				entry.handler.finish(entry.op)
			}
			for _, h := range handlers {
				h.Close()
			}
		})
	}
	t.Cleanup(cleanup)
	for i := 0; i < 9; i++ {
		h, err := NewRecordingCurrentTimelineHandler(f.pin.store, f.service, f.pin.pins, func() (config.SecuritySettings, error) { return config.SecuritySettings{}, nil })
		if err != nil {
			t.Fatal(err)
		}
		handlers = append(handlers, h)
		for j := 0; j < 2; j++ {
			op := h.admit(t.Context())
			if op != nil {
				admitted = append(admitted, admission{h, op})
			}
			if i < 8 && op == nil {
				t.Fatal("global capacity too small")
			}
			if i == 8 && op != nil {
				t.Fatal("global admission exceeded sixteen")
			}
		}
	}
	cleanup()
}
func TestRecordingCurrentTimelineFinalSetterDelayRechecksOriginalPINAndExpiry(t *testing.T) {
	t.Run("actual original PIN changed during setter", func(t *testing.T) {
		f := newCurrentTimelineFixture(t)
		w := newCurrentTimelineWriter()
		var once sync.Once
		w.setter = func(time.Time) { once.Do(func() { f.pin.setPIN(t, "1357") }) }
		r, release := f.request(t)
		defer release()
		f.h.Get(w, r)
		if w.Code != 403 || strings.Contains(w.Body.String(), `"spans"`) {
			t.Fatal("blocking setter bypassed final original PIN check", w.Code)
		}
	})
	t.Run("data publication adapter expires while setter blocks", func(t *testing.T) {
		// Pure publication regression only; these data values are not a PG grant.
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		expiry := time.Now().Add(2 * time.Second)
		value := recordings.DerivedTimelineData{SchemaVersion: 1, RecordingID: "synthetic", State: "known", CoordinateSystem: "derived-media", Incarnation: uuid.NewString(), ExpiresAt: &expiry}
		w := newCurrentTimelineWriter()
		entered := make(chan time.Time, 1)
		release := make(chan struct{})
		done := make(chan struct{})
		var releaseOnce sync.Once
		var setters, checks atomic.Int32
		// Every early assertion failure releases the actual setter and joins work.
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); <-done })
		w.setter = func(at time.Time) {
			if setters.Add(1) == 1 {
				entered <- at
				<-release
			}
		}
		go func() {
			defer close(done)
			writeRecordingCurrentTimeline(w, ctx, ctx, ctx, value, func() error { checks.Add(1); return nil })
		}()
		select {
		case deadline := <-entered:
			if !deadline.Equal(expiry) || !time.Now().Before(expiry) || checks.Load() != 1 {
				t.Fatal("setter did not receive the original live descriptor deadline after initial check")
			}
		case <-done:
			t.Fatal("publication returned before actual setter entry")
		case <-time.After(time.Second):
			t.Fatal("actual setter entry not observed")
		}
		timer := time.NewTimer(time.Until(expiry) + 10*time.Millisecond)
		defer timer.Stop()
		<-timer.C
		select {
		case <-done:
			t.Fatal("publication escaped while setter remained held")
		default:
		}
		if ctx.Err() != nil {
			t.Fatal("original context expired instead of descriptor-only lifetime")
		}
		releaseOnce.Do(func() { close(release) })
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("released setter did not finish publication check")
		}
		if setters.Load() != 1 || checks.Load() != 2 || time.Now().Before(expiry) || ctx.Err() != nil || w.Code != 403 || strings.Contains(w.Body.String(), `"state"`) {
			t.Fatal("descriptor-only expiry was not rejected after the witnessed setter")
		}

	})
}
