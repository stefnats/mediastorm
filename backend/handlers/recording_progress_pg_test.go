//go:build linux

package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
	"novastream/services/recordings"
)

type progressHTTPFixture struct {
	base        *currentTimelineFixture
	manager     *config.Manager
	settings    config.Settings
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	h           *RecordingProgressHandler
	calls       atomic.Int32
	sequence    atomic.Int64
	mu          sync.Mutex
	completed   map[string]chan struct{}
	wrap        func(http.ResponseWriter, *http.Request) http.ResponseWriter
}

func progressHTTPPG(t *testing.T, enforce bool) *progressHTTPFixture {
	t.Helper()
	f := &progressHTTPFixture{base: newCurrentTimelineFixture(t), completed: map[string]chan struct{}{}}
	f.manager = config.NewManager(t.TempDir() + "/settings.json")
	f.settings = config.DefaultSettings()
	f.settings.Database.URL = f.base.pin.store.Pool().Config().ConnConfig.ConnString()
	f.settings.Security.EnforceProfilePins = enforce
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	objects, err := settingscommit.OpenLockObjects(t.Context(), f.manager.ConfigPath())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.security != nil {
			_ = f.security.Close()
		}
		if f.coordinator != nil {
			_ = f.coordinator.Close()
		}
		_ = objects.Close()
	})
	f.coordinator, err = settingscommit.NewForPool(t.Context(), f.base.pin.store.Pool(), objects)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := f.coordinator.TryWrite(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.coordinator.Arm(guard); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
	_ = guard.Close()
	if err != nil {
		t.Fatal(err)
	}
	f.h, err = NewRecordingProgressHandler(f.base.pin.store, f.base.service, f.base.pin.pins, f.manager, f.security, f.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.h.Close)
	f.base.mu.Lock()
	f.base.current = func(ctx context.Context) error { f.calls.Add(1); return ctx.Err() }
	f.base.mu.Unlock()
	return f
}
func (f *progressHTTPFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Synthetic-Request")
		f.mu.Lock()
		done := f.completed[key]
		wrap := f.wrap
		f.mu.Unlock()
		if done != nil {
			defer close(done)
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		checked, release, err := f.base.pin.pins.Authenticate(r, token, "synthetic-client")
		if err != nil {
			http.Error(w, "fixture auth denied", 401)
			return
		}
		defer release()
		if wrap != nil {
			w = wrap(w, checked)
		}
		f.h.ServeHTTP(w, mux.SetURLVars(checked, map[string]string{"recordingID": f.base.job.ID}))
	}))
	t.Cleanup(server.Close)
	return server
}
func (f *progressHTTPFixture) request(t *testing.T, server *httptest.Server, method, path string, body []byte, headers map[string]string) (int, []byte, error) {
	t.Helper()
	key := strconv.FormatInt(f.sequence.Add(1), 10)
	done := make(chan struct{})
	f.mu.Lock()
	f.completed[key] = done
	f.mu.Unlock()
	request, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Synthetic-Request", key)
	request.Header.Set("Authorization", "Bearer "+f.base.pin.session.Token)
	request.Header.Set("X-Client-ID", "synthetic-client")
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	var raw []byte
	code := 0
	if err == nil {
		code = response.StatusCode
		raw, err = io.ReadAll(io.LimitReader(response.Body, 128<<10))
		closeErr := response.Body.Close()
		if err == nil {
			err = closeErr
		}
	}
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual handler/frame/source cleanup did not finish")
	}
	return code, raw, err
}
func (f *progressHTTPFixture) snapshot(t *testing.T, server *httptest.Server, headers map[string]string) recordingProgressSnapshotData {
	t.Helper()
	code, raw, err := f.request(t, server, http.MethodGet, "/private/progress", nil, headers)
	var value recordingProgressSnapshotData
	if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil || value.SchemaVersion != 1 || value.Timeline.State != "known" || value.Progress.ValidUntil.IsZero() {
		t.Fatalf("genuine snapshot failed: status=%d error=%v", code, err)
	}
	var wire struct {
		Progress struct {
			ValidUntil string `json:"validUntil"`
		} `json:"progress"`
	}
	if json.Unmarshal(raw, &wire) != nil || !strings.HasSuffix(wire.Progress.ValidUntil, "Z") {
		t.Fatal("effective deadline DATA was not canonical UTC")
	}
	return value
}
func progressHTTPBody(snapshot recordingProgressSnapshotData, action datastore.RecordingProgressAction, position int64) []byte {
	raw, _ := json.Marshal(map[string]any{"action": action, "expectedRevision": snapshot.Progress.Revision, "writerGeneration": snapshot.Progress.WriterGeneration, "incarnation": snapshot.Timeline.Incarnation, "artifactId": snapshot.Timeline.ArtifactID, "mappingSha256": snapshot.Timeline.MappingRevision, "durationMs": snapshot.Timeline.DurationMS, "positionMs": position})
	return raw
}
func (f *progressHTTPFixture) write(t *testing.T, server *httptest.Server, snapshot recordingProgressSnapshotData, action datastore.RecordingProgressAction, position int64, headers map[string]string) datastore.RecordingProgressResult {
	t.Helper()
	code, raw, err := f.request(t, server, http.MethodPost, "/private/progress", progressHTTPBody(snapshot, action, position), headers)
	var value recordingProgressWriteData
	if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil || value.SchemaVersion != 1 || value.Result.ValidUntil.IsZero() {
		t.Fatalf("genuine write failed: status=%d error=%v", code, err)
	}
	return value.Result
}
func (f *progressHTTPFixture) payload(t *testing.T) []byte {
	t.Helper()
	var raw []byte
	if err := f.base.pin.store.Pool().QueryRow(t.Context(), `SELECT COALESCE((SELECT to_jsonb(p) FROM recording_playback_positions p WHERE recording_id=$1),'null'::jsonb)`, f.base.job.ID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
func (f *progressHTTPFixture) grant(t *testing.T, duration time.Duration) (string, time.Time) {
	t.Helper()
	r, release := f.base.pin.request(t, http.MethodPost, "/private/unlock")
	defer release()
	token, expiry, err := f.base.pin.pins.Verify(r, f.base.pin.profile.ID, "1357", duration, f.base.pin.users)
	if err != nil {
		t.Fatal("actual PG PIN verifier", err)
	}
	return token, expiry
}
func progressHTTPAwait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("actual controlled IO did not join")
	}
}

type progressRealResponse struct {
	http.ResponseWriter
	mu        sync.Mutex
	calls     int
	hook      func(int, time.Time)
	writeHook func()
	flushErr  atomic.Bool
}

func (w *progressRealResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *progressRealResponse) SetWriteDeadline(at time.Time) error {
	w.mu.Lock()
	w.calls++
	n, hook := w.calls, w.hook
	w.mu.Unlock()
	if hook != nil {
		hook(n, at)
	}
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(at)
}
func (w *progressRealResponse) SetReadDeadline(at time.Time) error {
	return http.NewResponseController(w.ResponseWriter).SetReadDeadline(at)
}
func (w *progressRealResponse) Write(raw []byte) (int, error) {
	if w.writeHook != nil {
		w.writeHook()
	}
	return w.ResponseWriter.Write(raw)
}
func (w *progressRealResponse) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err != nil {
		w.flushErr.Store(true)
	}
	return err
}

func TestRecordingProgressHTTPActualNoneMatchingAndFreshSession(t *testing.T) {
	f := progressHTTPPG(t, true)
	descriptor := f.base.generateAndPrepare(t)
	server := f.server(t)
	counts := f.base.toolCounts(t)
	first := f.snapshot(t, server, nil)
	if first.Progress.PositionState != "none" || first.Progress.PositionMS != nil || first.Progress.Revision != "0" || first.Progress.WriterGeneration != "0" || first.Timeline.ObservedAt == nil || first.Timeline.ExpiresAt == nil || !first.Timeline.ObservedAt.Equal(*descriptor.ObservedAt) || !first.Timeline.ExpiresAt.Equal(*descriptor.ExpiresAt) {
		t.Fatal("missing payload fabricated position/counters or renewed descriptor times")
	}
	f.write(t, server, first, datastore.RecordingProgressFromStart, 0, nil)
	started := f.snapshot(t, server, nil)
	position := min(int64(600), started.Timeline.DurationMS)
	result := f.write(t, server, started, datastore.RecordingProgressTouch, position, nil)
	if _, err := f.base.pin.store.Pool().Exec(t.Context(), `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, f.base.pin.session.Token); err != nil {
		t.Fatal(err)
	}
	session, err := f.base.pin.sessions.Create("actor", false, "synthetic-new-session", "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	f.base.pin.session = session
	matching := f.snapshot(t, server, nil)
	if matching.Progress.PositionState != "matching" || matching.Progress.PositionMS == nil || *matching.Progress.PositionMS != position || matching.Progress.Revision != result.Revision || matching.Progress.WriterGeneration != result.WriterGeneration || matching.Timeline.ArtifactID != first.Timeline.ArtifactID || !matching.Timeline.ObservedAt.Equal(*first.Timeline.ObservedAt) || !matching.Timeline.ExpiresAt.Equal(*first.Timeline.ExpiresAt) {
		t.Fatal("fresh session adopted counters, lost durable position or renewed original source")
	}
	resumed := f.write(t, server, matching, datastore.RecordingProgressResume, 0, nil)
	if resumed.PositionMS != position || f.base.toolCounts(t) != counts {
		t.Fatal("resume lost actual position or implicitly prepared/probed media")
	}
}
func TestRecordingProgressHTTPPayloadConflictNeverAdoptsCounters(t *testing.T) {
	f := progressHTTPPG(t, false)
	f.base.generateAndPrepare(t)
	server := f.server(t)
	first := f.snapshot(t, server, nil)
	f.write(t, server, first, datastore.RecordingProgressFromStart, 0, nil)
	if _, err := f.base.pin.store.Pool().Exec(t.Context(), `UPDATE recording_playback_positions SET mapping_sha256=repeat('f',64) WHERE recording_id=$1`, f.base.job.ID); err != nil {
		t.Fatal(err)
	}
	before := f.payload(t)
	conflict := f.snapshot(t, server, nil)
	if conflict.Progress.PositionState != "conflict" || conflict.Progress.PositionMS != nil || conflict.Progress.AtDerivedEnd != nil {
		t.Fatal("conflict silently became default/resumable position")
	}
	code, _, _ := f.request(t, server, http.MethodPost, "/private/progress", progressHTTPBody(conflict, datastore.RecordingProgressResume, 0), nil)
	if code != 409 || !bytes.Equal(before, f.payload(t)) {
		t.Fatal("conflicting payload was adopted or repaired")
	}
}
func TestRecordingProgressHTTPActualConcurrentWriterConflicts(t *testing.T) {
	f := progressHTTPPG(t, false)
	f.base.generateAndPrepare(t)
	server := f.server(t)
	snapshot := f.snapshot(t, server, nil)
	body := progressHTTPBody(snapshot, datastore.RecordingProgressFromStart, 0)
	var group sync.WaitGroup
	codes := make(chan int, 2)
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			code, _, err := f.request(t, server, http.MethodPost, "/private/progress", body, nil)
			if err != nil {
				codes <- 0
			} else {
				codes <- code
			}
		}()
	}
	group.Wait()
	close(codes)
	actual := map[int]int{}
	for code := range codes {
		actual[code]++
	}
	if actual[200] != 1 || actual[409] != 1 {
		t.Fatal("actual concurrent CAS did not acknowledge one writer", actual)
	}
	current := f.snapshot(t, server, nil)
	if current.Progress.Revision != "1" || current.Progress.WriterGeneration != "1" {
		t.Fatal("conflict retried or adopted new counters")
	}
}

func TestRecordingProgressHTTPStrictJSONAndSelectorsBeforeSource(t *testing.T) {
	f := progressHTTPPG(t, false)
	f.base.generateAndPrepare(t)
	server := f.server(t)
	snapshot := f.snapshot(t, server, nil)
	good := progressHTTPBody(snapshot, datastore.RecordingProgressFromStart, 0)
	cases := []string{"missing-revision", "missing-generation", "numeric-revision", "leading-zero", "negative-generation", "overflow-revision", "duplicate", "escaped-duplicate", "wrong-case", "unknown", "missing-correlation", "null-counter", "invalid-utf8", "extra-json", "position-missing", "position-negative", "duration-fraction", "resume-position", "bad-action", "query-profile", "query-token", "range", "head", "get-body"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			var fields map[string]any
			if json.Unmarshal(good, &fields) != nil {
				t.Fatal("own command decode")
			}
			method, path := http.MethodPost, "/private/progress"
			headers := map[string]string{}
			var raw []byte
			switch name {
			case "missing-revision":
				delete(fields, "expectedRevision")
			case "missing-generation":
				delete(fields, "writerGeneration")
			case "numeric-revision":
				fields["expectedRevision"] = 0
			case "leading-zero":
				fields["expectedRevision"] = "00"
			case "negative-generation":
				fields["writerGeneration"] = "-1"
			case "overflow-revision":
				fields["expectedRevision"] = "9223372036854775808"
			case "wrong-case":
				fields["ExpectedRevision"] = fields["expectedRevision"]
				delete(fields, "expectedRevision")
			case "unknown":
				fields["profileId"] = f.base.pin.profile.ID
			case "missing-correlation":
				delete(fields, "incarnation")
			case "null-counter":
				fields["expectedRevision"] = nil
			case "position-missing":
				delete(fields, "positionMs")
			case "position-negative":
				fields["positionMs"] = -1
			case "duration-fraction":
				fields["durationMs"] = 1.5
			case "resume-position":
				fields["action"] = "resume"
				fields["positionMs"] = 1
			case "bad-action":
				fields["action"] = "automatic"
			case "query-profile":
				path += "?profileId=" + f.base.pin.profile.ID
			case "query-token":
				path += "?token=synthetic"
			case "range":
				headers["Range"] = "bytes=0-1"
			case "head":
				method = http.MethodHead
			case "get-body":
				method = http.MethodGet
			}
			raw, _ = json.Marshal(fields)
			switch name {
			case "duplicate":
				raw = append(append([]byte{}, good[:len(good)-1]...), []byte(`,"expectedRevision":"0"}`)...)
			case "escaped-duplicate":
				raw = append(append([]byte{}, good[:len(good)-1]...), []byte(`,"expectedRevis\u0069on":"0"}`)...)
			case "invalid-utf8":
				raw = append(append([]byte{}, good[:len(good)-1]...), []byte(",\"other\":\""+string([]byte{255})+"\"}")...)
			case "extra-json":
				raw = append(append([]byte{}, good...), []byte(` {}`)...)
			}
			calls := f.calls.Load()
			before := f.payload(t)
			code, _, err := f.request(t, server, method, path, raw, headers)
			if err != nil || code < 400 || code >= 500 || f.calls.Load() != calls || !bytes.Equal(before, f.payload(t)) {
				t.Fatal("invalid command/selector reached source or changed durable position", name, code, err)
			}
		})
	}
}

func TestRecordingProgressHTTPActualPINPolicyAndGrantExpiry(t *testing.T) {
	for _, name := range []string{"true-locked", "true-unlocked", "false-locked", "false-expired-grant"} {
		t.Run(name, func(t *testing.T) {
			enforce := strings.HasPrefix(name, "true")
			f := progressHTTPPG(t, enforce)
			f.base.generateAndPrepare(t)
			f.base.pin.setPIN(t, "1357")
			headers := map[string]string{}
			if name == "true-unlocked" || name == "false-expired-grant" {
				duration := time.Hour
				if name == "false-expired-grant" {
					duration = time.Second
				}
				token, expiry := f.grant(t, duration)
				headers["X-Profile-Unlock"] = token
				if name == "false-expired-grant" {
					timer := time.NewTimer(time.Until(expiry) + 20*time.Millisecond)
					defer timer.Stop()
					<-timer.C
				}
			}
			server := f.server(t)
			code, raw, err := f.request(t, server, http.MethodGet, "/private/progress", nil, headers)
			if name == "true-locked" {
				if err != nil || code != 403 || bytes.Contains(raw, []byte(`"timeline"`)) {
					t.Fatal("enforced locked profile read snapshot", code, err)
				}
				return
			}
			var value recordingProgressSnapshotData
			if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil || value.Progress.PositionState != "none" || !value.Progress.ValidUntil.After(time.Now()) {
				t.Fatal("existing DVR PIN policy changed", name, code, err)
			}
		})
	}
}

func TestRecordingProgressHTTPOriginalGrantDeadlineBoundsRealResponse(t *testing.T) {
	for _, name := range []string{"get-true", "post-true", "get-false", "post-false"} {
		t.Run(name, func(t *testing.T) {
			enforce := strings.HasSuffix(name, "true")
			f := progressHTTPPG(t, enforce)
			f.base.generateAndPrepare(t)
			f.base.pin.setPIN(t, "1357")
			token, _ := f.grant(t, time.Hour)
			headers := map[string]string{"X-Profile-Unlock": token}
			server := f.server(t)
			snapshot := f.snapshot(t, server, headers)
			// A changed expiry would invalidate the persisted grant binding. Issue
			// a genuine short grant only after the long preparation snapshot.
			token, expiry := f.grant(t, 2*time.Second)
			headers["X-Profile-Unlock"] = token
			entered := make(chan time.Time, 1)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var forwarded atomic.Bool
			f.mu.Lock()
			f.wrap = func(w http.ResponseWriter, r *http.Request) http.ResponseWriter {
				if r.Header.Get("X-Synthetic-Hold") != "deadline" {
					return w
				}
				return &progressRealResponse{ResponseWriter: w, hook: func(n int, at time.Time) {
					if n == 2 {
						entered <- at
						<-release
						forwarded.Store(true)
					}
				}}
			}
			f.mu.Unlock()
			headers["X-Synthetic-Hold"] = "deadline"
			method := http.MethodGet
			var body []byte
			if strings.HasPrefix(name, "post") {
				method = http.MethodPost
				body = progressHTTPBody(snapshot, datastore.RecordingProgressFromStart, 0)
			}
			done := make(chan struct{})
			var code int
			var raw []byte
			var requestErr error
			go func() {
				code, raw, requestErr = f.request(t, server, method, "/private/progress", body, headers)
				close(done)
			}()
			var bound time.Time
			select {
			case bound = <-entered:
			case <-done:
				t.Fatalf("actual final socket setter did not hold response: status=%d error=%v", code, requestErr)
			case <-time.After(time.Second):
				t.Fatal("actual transaction deadline was not observed")
			}
			if enforce && !bound.Equal(expiry) || !enforce && !bound.After(expiry.Add(10*time.Second)) {
				t.Fatal("real response used wrong grant/base deadline")
			}
			if method == http.MethodPost && bytes.Equal(f.payload(t), []byte("null")) {
				t.Fatal("known COMMIT did not precede held response")
			}
			timer := time.NewTimer(time.Until(expiry) + 30*time.Millisecond)
			defer timer.Stop()
			<-timer.C
			select {
			case <-done:
				t.Fatal("response escaped actual held setter")
			default:
			}
			unblock()
			progressHTTPAwait(t, done)
			if !forwarded.Load() {
				t.Fatal("deadline never reached the actual loopback response controller")
			}
			if enforce {
				if requestErr == nil && code == 200 && bytes.Contains(raw, []byte(`"schemaVersion"`)) {
					t.Fatal("expired original grant published a usable response")
				}
			} else {
				if requestErr != nil || code != 200 || !bytes.Contains(raw, []byte(`"schemaVersion"`)) {
					t.Fatal("flag=false borrowed a foreign grant-only deadline", code, requestErr)
				}
			}
			if method == http.MethodPost && bytes.Equal(f.payload(t), []byte("null")) {
				t.Fatal("response loss was incorrectly treated as rollback")
			}
		})
	}
}

func TestRecordingProgressHTTPSourceABAInsideGetDerivedDenies(t *testing.T) {
	for _, kind := range []string{"owner", "run"} {
		t.Run(kind, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			descriptor := f.base.generateAndPrepare(t)
			request, release := f.base.request(t)
			defer release()
			ctx, cancel, authorize, ok := f.base.h.guard.recordingDerivedAuthority(httptest.NewRecorder(), request, false)
			if !ok {
				t.Fatal("genuine fixture source authority")
			}
			defer cancel()
			identity, err := f.base.pin.store.ReadRecordingTimelineIdentity(ctx, f.base.job.ID)
			if err != nil {
				t.Fatal(err)
			}
			timeline, err := recordings.ProjectDerivedTimelineData(identity.Incarnation(), descriptor, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			source := &recordingProgressHTTPSource{handler: f.h, ctx: ctx, id: f.base.job.ID, identity: identity, timeline: timeline, authorize: authorize}
			var other string
			if kind == "owner" {
				profile, err := f.base.pin.users.CreateForAccount("other", "Synthetic other owner")
				if err != nil {
					t.Fatal(err)
				}
				other = profile.ID
			}
			var changed, ready atomic.Bool
			f.base.mu.Lock()
			f.base.current = func(inner context.Context) error {
				if !changed.CompareAndSwap(false, true) {
					return inner.Err()
				}
				pool := f.base.pin.store.Pool()
				if kind == "owner" {
					if _, err := pool.Exec(inner, `UPDATE recordings SET user_id=$1 WHERE id=$2`, other, f.base.job.ID); err != nil {
						return err
					}
					if _, err := pool.Exec(inner, `UPDATE recordings SET user_id=$1 WHERE id=$2`, f.base.pin.profile.ID, f.base.job.ID); err != nil {
						return err
					}
				} else {
					if _, err := pool.Exec(inner, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-B"') WHERE id=$1`, f.base.job.ID); err != nil {
						return err
					}
					if _, err := pool.Exec(inner, `UPDATE recordings SET capture_state=jsonb_set(capture_state,'{runId}','"run-A"') WHERE id=$1`, f.base.job.ID); err != nil {
						return err
					}
				}
				// A genuine nested status read proves media/config is still ready with
				// the same descriptor; only the actual084 incarnation reveals the ABA.
				actual, err := f.base.service.GetDerived(inner, f.base.job.ID, authorize)
				if err == nil && actual.State == "ready" && actual.ID == descriptor.ID {
					ready.Store(true)
				}
				return err
			}
			f.base.mu.Unlock()
			if err := source.check(); err == nil || !changed.Load() || !ready.Load() {
				t.Fatal("output/watcher check missed actual identity ABA inside genuine ready GetDerived", err)
			}
			latest, err := f.base.pin.store.ReadRecordingTimelineIdentity(ctx, f.base.job.ID)
			if err != nil || identity.Same(latest) {
				t.Fatal("actual bookkeeping incarnation did not change")
			}
		})
	}
}

func TestRecordingProgressHTTPRealGetDerivedSourceSwitchNeverRebindsPosition(t *testing.T) {
	f := progressHTTPPG(t, false)
	descriptor := f.base.generateAndPrepare(t)
	server := f.server(t)
	first := f.snapshot(t, server, nil)
	f.write(t, server, first, datastore.RecordingProgressFromStart, 0, nil)
	before := f.payload(t)
	counts := f.base.toolCounts(t)
	f.base.changed.Store(true)
	code, raw, err := f.request(t, server, http.MethodGet, "/private/progress", nil, nil)
	if err != nil || code != 409 || bytes.Contains(raw, []byte(`"progress"`)) || !bytes.Equal(before, f.payload(t)) || f.base.toolCounts(t) != counts {
		t.Fatal("unknown actual source prepared, rebound or leaked restart data", code, err)
	}
	tools := f.base.tools
	tools.ConfigurationKey = "synthetic-explicit-replacement"
	if err := f.base.service.ConfigureDerivedJoiner(t.Context(), tools, func(context.Context) (recordings.DerivedTools, error) { return tools, nil }); err != nil {
		t.Fatal(err)
	}
	request, release := f.base.request(t)
	defer release()
	ctx, cancel, authorize, ok := f.base.h.guard.recordingDerivedAuthority(httptest.NewRecorder(), request, false)
	if !ok {
		t.Fatal("actual replacement authority")
	}
	defer cancel()
	replacement, err := f.base.service.PrepareDerived(ctx, f.base.job.ID, authorize)
	if err != nil || replacement.ID == descriptor.ID || replacement.State != "ready" {
		t.Fatal("explicit synthetic preparation did not replace artifact", err)
	}
	conflict := f.snapshot(t, server, nil)
	if conflict.Progress.PositionState != "conflict" || conflict.Progress.PositionMS != nil || conflict.Timeline.ArtifactID != replacement.ID || !bytes.Equal(before, f.payload(t)) {
		t.Fatal("new actual artifact silently adopted old position")
	}
}

func TestRecordingProgressHTTPEarlyAndLateBusyPolicyAttempts(t *testing.T) {
	for _, phase := range []string{"early", "late"} {
		t.Run(phase, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			f.base.generateAndPrepare(t)
			attempt := make(chan error, 1)
			var once sync.Once
			changed := f.settings
			changed.Security.EnforceProfilePins = true
			var external *settingscommit.ProgressFileHold
			if phase == "early" {
				var err error
				external, err = f.coordinator.TryProgressFileHold(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				defer external.Close()
				// GET captures its original policy before this real transport setup call.
				// A different genuine shared holder makes the intervening Save Busy.
				f.wrap = func(w http.ResponseWriter, _ *http.Request) http.ResponseWriter {
					return &progressRealResponse{ResponseWriter: w, hook: func(n int, _ time.Time) {
						if n == 1 {
							once.Do(func() { attempt <- f.manager.Save(changed) })
						}
					}}
				}
			} else {
				f.base.mu.Lock()
				f.base.current = func(ctx context.Context) error {
					f.calls.Add(1)
					once.Do(func() { attempt <- f.manager.Save(changed) })
					return ctx.Err()
				}
				f.base.mu.Unlock()
			}
			server := f.server(t)
			before := f.calls.Load()
			code, raw, err := f.request(t, server, http.MethodGet, "/private/progress", nil, nil)
			if saveErr := <-attempt; !errors.Is(saveErr, settingscommit.ErrBusy) {
				t.Fatal("intervening actual Save was not Busy", saveErr)
			}
			if phase == "early" {
				if err != nil || code < 400 || bytes.Contains(raw, []byte(`"timeline"`)) || f.calls.Load() != before {
					t.Fatal("retired pre-bind policy reached real source", code, err)
				}
				_ = external.Close()
			} else {
				var value recordingProgressSnapshotData
				if err != nil || code != 200 || json.Unmarshal(raw, &value) != nil || value.Progress.PositionState != "none" {
					t.Fatal("late Busy attempt replaced already-bound policy", code, err)
				}
			}
		})
	}
}

func TestRecordingProgressHTTPCloseJoinsActualSourceCallback(t *testing.T) {
	f := progressHTTPPG(t, false)
	f.base.generateAndPrepare(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var reached sync.Once
	f.base.mu.Lock()
	f.base.current = func(ctx context.Context) error { reached.Do(func() { close(entered) }); <-release; return ctx.Err() }
	f.base.mu.Unlock()
	server := f.server(t)
	requestDone := make(chan struct{})
	go func() {
		_, _, _ = f.request(t, server, http.MethodGet, "/private/progress", nil, nil)
		close(requestDone)
	}()
	progressHTTPAwait(t, entered)
	closedA, closedB := make(chan struct{}), make(chan struct{})
	go func() { f.h.Close(); close(closedA) }()
	go func() { f.h.Close(); close(closedB) }()
	// Observe the actual registered original operation canceled; cleanup remains
	// charged until the real configuration callback finishes, not a timeout.
	until := time.Now().Add(time.Second)
	var canceled bool
	for time.Now().Before(until) {
		f.h.mu.Lock()
		for op := range f.h.active {
			canceled = op.ctx.Err() != nil
		}
		f.h.mu.Unlock()
		if canceled {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !canceled {
		t.Fatal("handler Close did not signal original source operation")
	}
	select {
	case <-closedA:
		t.Fatal("Close refunded before actual source returned")
	default:
	}
	select {
	case <-closedB:
		t.Fatal("second Close overtook actual source work")
	default:
	}
	if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
		t.Fatal("outer native hold was refunded before actual source cleanup", err)
	}
	unblock()
	progressHTTPAwait(t, requestDone)
	progressHTTPAwait(t, closedA)
	progressHTTPAwait(t, closedB)
	f.h.mu.Lock()
	active := len(f.h.active)
	f.h.mu.Unlock()
	if active != 0 || f.base.pin.store.Pool().Stat().AcquiredConns() != 0 {
		t.Fatal("source cleanup did not refund original operations")
	}
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal("actual source end did not release outer hold", err)
	}
}

type progressBodyObserved struct {
	io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (r *progressBodyObserved) Read(raw []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.ReadCloser.Read(raw)
}

func TestRecordingProgressHTTPActualBodyAndResponseCancellationJoin(t *testing.T) {
	t.Run("body", func(t *testing.T) {
		f := progressHTTPPG(t, false)
		entered := make(chan struct{})
		done := make(chan struct{})
		var actualReadDeadline atomic.Bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			checked, release, err := f.base.pin.pins.Authenticate(r, f.base.pin.session.Token, "synthetic-client")
			if err != nil {
				http.Error(w, "fixture auth", 401)
				return
			}
			defer release()
			checked.Body = &progressBodyObserved{ReadCloser: checked.Body, entered: entered}
			wrapped := &progressBodyDeadlineResponse{ResponseWriter: w, read: &actualReadDeadline}
			f.h.ServeHTTP(wrapped, mux.SetURLVars(checked, map[string]string{"recordingID": f.base.job.ID}))
		}))
		defer server.Close()
		connection, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		if _, err = fmt.Fprintf(connection, "POST /private/progress HTTP/1.1\r\nHost: synthetic.invalid\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{"); err != nil {
			t.Fatal(err)
		}
		progressHTTPAwait(t, entered)
		closed := make(chan struct{})
		go func() { f.h.Close(); close(closed) }()
		progressHTTPAwait(t, done)
		progressHTTPAwait(t, closed)
		if !actualReadDeadline.Load() || f.calls.Load() != 0 {
			t.Fatal("real incomplete body was not interrupted before any source work")
		}
	})
	t.Run("response", func(t *testing.T) {
		f := progressHTTPPG(t, false)
		f.base.generateAndPrepare(t)
		entered, release, deadline := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var writeOnce, deadlineOnce, releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		var actual *progressRealResponse
		f.wrap = func(w http.ResponseWriter, _ *http.Request) http.ResponseWriter {
			actual = &progressRealResponse{ResponseWriter: w, writeHook: func() { writeOnce.Do(func() { close(entered) }); <-release }, hook: func(_ int, at time.Time) {
				if !at.After(time.Now()) {
					deadlineOnce.Do(func() { close(deadline) })
				}
			}}
			return actual
		}
		server := f.server(t)
		requestDone := make(chan struct{})
		go func() {
			_, _, _ = f.request(t, server, http.MethodGet, "/private/progress", nil, nil)
			close(requestDone)
		}()
		progressHTTPAwait(t, entered)
		closed := make(chan struct{})
		go func() { f.h.Close(); close(closed) }()
		progressHTTPAwait(t, deadline)
		select {
		case <-closed:
			t.Fatal("Close overtook the actual response Write wrapper")
		default:
		}
		if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
			t.Fatal("response callback refunded outer hold before actual Write end", err)
		}
		// The wrapper holds application Write, not a claim of kernel saturation.
		// After release, actual socket Flush must see the real expired deadline.
		unblock()
		progressHTTPAwait(t, requestDone)
		progressHTTPAwait(t, closed)
		if actual == nil || !actual.flushErr.Load() {
			t.Fatal("actual socket did not enforce cancellation deadline")
		}
	})
}

type progressBodyDeadlineResponse struct {
	http.ResponseWriter
	read *atomic.Bool
}

func (w *progressBodyDeadlineResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *progressBodyDeadlineResponse) SetReadDeadline(at time.Time) error {
	err := http.NewResponseController(w.ResponseWriter).SetReadDeadline(at)
	if err == nil && !at.After(time.Now()) {
		w.read.Store(true)
	}
	return err
}
func (w *progressBodyDeadlineResponse) SetWriteDeadline(at time.Time) error {
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(at)
}
func (w *progressBodyDeadlineResponse) FlushError() error {
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func TestRecordingProgressHTTPUnsupportedDeadlineCannotPublish(t *testing.T) {
	f := progressHTTPPG(t, false)
	f.base.generateAndPrepare(t)
	request, release := f.base.request(t)
	defer release()
	writer := httptest.NewRecorder()
	calls := f.calls.Load()
	f.h.ServeHTTP(writer, request)
	if writer.Code != 503 || bytes.Contains(writer.Body.Bytes(), []byte(`"timeline"`)) || f.calls.Load() != calls {
		t.Fatal("recorder-only transport was accepted for successful snapshot")
	}
}

func TestRecordingProgressHTTPEarlyRevocationInterruptsActualResponse(t *testing.T) {
	for _, mode := range []string{"session", "grant"} {
		t.Run(mode, func(t *testing.T) {
			f := progressHTTPPG(t, false)
			f.base.generateAndPrepare(t)
			headers := map[string]string{}
			var token string
			if mode == "grant" {
				f.base.pin.setPIN(t, "1357")
				token, _ = f.grant(t, time.Hour)
				headers["X-Profile-Unlock"] = token
			}
			entered, release, deadline := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var writeOnce, deadlineOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			var actual *progressRealResponse
			f.wrap = func(w http.ResponseWriter, _ *http.Request) http.ResponseWriter {
				actual = &progressRealResponse{ResponseWriter: w, writeHook: func() { writeOnce.Do(func() { close(entered) }); <-release }, hook: func(_ int, at time.Time) {
					if !at.After(time.Now()) {
						deadlineOnce.Do(func() { close(deadline) })
					}
				}}
				return actual
			}
			server := f.server(t)
			finished := make(chan struct{})
			go func() {
				_, _, _ = f.request(t, server, http.MethodGet, "/private/progress", nil, headers)
				close(finished)
			}()
			progressHTTPAwait(t, entered)
			if mode == "session" {
				if count, err := f.base.pin.store.RevokeAccountSessions(t.Context(), "actor"); err != nil || count != 1 {
					t.Fatal("actual authoritative session revoke", count, err)
				}
			} else {
				hash := sha256.Sum256([]byte(token))
				if tag, err := f.base.pin.store.Pool().Exec(t.Context(), `DELETE FROM profile_unlocks WHERE token_hash=$1`, hex.EncodeToString(hash[:])); err != nil || tag.RowsAffected() != 1 {
					t.Fatal("actual original grant revoke", err)
				}
			}
			progressHTTPAwait(t, deadline)
			select {
			case <-finished:
				t.Fatal("revocation refunded before actual response Write finished")
			default:
			}
			if err := f.manager.Save(f.settings); !errors.Is(err, settingscommit.ErrBusy) {
				t.Fatal("revocation refunded outer native hold before actual IO end", err)
			}
			unblock()
			progressHTTPAwait(t, finished)
			if actual == nil || !actual.flushErr.Load() {
				t.Fatal("actual socket did not enforce observed revocation")
			}
		})
	}
}
