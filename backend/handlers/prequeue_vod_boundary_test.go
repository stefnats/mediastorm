package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/playback"
)

func vodPrequeueFixture(t *testing.T, mode vodApplicabilityMode, kid bool) (*PrequeueHandler, *vodTestState, *fallbackSearch) {
	t.Helper()
	search := &fallbackSearch{results: []models.NZBResult{{Title: "Synthetic.Movie.1080p.WEB-DL", GUID: "synthetic-release", Link: "https://synthetic.invalid/release", ServiceType: models.ServiceTypeDebrid, Indexer: "AIOStreams", Attributes: map[string]string{"resolution": "1080p"}}}}
	h, local, _ := newFallbackWorker(t, search)
	local.empty = true
	f := newVODTestState(t, mode, kid)
	h.vodBoundary = f.boundary
	h.SetBadStreamsService(badstreams.New(filepath.Join(t.TempDir(), "reports.json")))
	return h, f, search
}
func vodRestoredReady(t *testing.T, h *PrequeueHandler) *playback.PrequeueEntry {
	t.Helper()
	dir := t.TempDir()
	h.store.SetStoragePath(dir)
	entry, _ := h.store.CreateScoped("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "manual", h.prequeueSettingsScopeKey("profile", "device", "tmdb:movie:123"))
	h.store.Update(entry.ID, func(e *playback.PrequeueEntry) {
		e.Status = playback.PrequeueStatusReady
		e.StreamPath = "/synthetic/owned.mkv"
		e.SelectedResult = &models.NZBResult{Title: "Synthetic.Movie.1080p", GUID: "synthetic-release", ServiceType: models.ServiceTypeDebrid}
		e.Duration = 600
	})
	h.store.MakePersistent(entry.ID)
	restored := playback.NewPrequeueStore(time.Hour)
	restored.SetStoragePath(dir)
	if err := restored.RestorePersisted(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.store = restored
	snapshot, _, ok := h.store.SnapshotForAuthorization(entry.ID)
	if !ok {
		t.Fatal("synthetic restored entry missing")
	}
	return snapshot
}
func TestVODPrequeueChildrenAndUnknownStopBeforeReadyValidatorAndDemand(t *testing.T) {
	for _, mode := range []vodApplicabilityMode{vodAccountActive, vodApplicabilityUnknown} {
		for _, action := range []string{"create warm", "status", "manual", "adopt", "subtitles", "report options", "retry"} {
			t.Run(string(rune('0'+mode))+"/"+action, func(t *testing.T) {
				h, f, search := vodPrequeueFixture(t, mode, true)
				entry := vodRestoredReady(t, h)
				var validations, probes atomic.Int32
				h.store.SetStreamPathValidator(func(context.Context, string) error { validations.Add(1); return nil })
				h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
					probes.Add(1)
					return nil, errors.New("poison probe")
				}}
				method, target, body := http.MethodGet, "/?profileId=profile", ""
				handler := h.GetStatus
				switch action {
				case "create warm":
					method = http.MethodPost
					target = "/api/playback/prequeue"
					body = `{"titleId":"tmdb:movie:123","titleName":"Synthetic Movie","mediaType":"movie","userId":"profile","clientId":"device","skipHLS":true}`
					handler = h.Prequeue
					h.prewarmSvc = &profileWarmRef{id: entry.ID}
				case "manual":
					target = "/?userId=profile&titleId=tmdb:movie:123"
					handler = h.ManualPrequeueStatus
				case "adopt":
					method = http.MethodPost
					body = `{"streamPath":"https://synthetic.invalid/other.mkv"}`
					handler = h.AdoptMigration
				case "subtitles":
					method = http.MethodPost
					body = `{"startOffset":0}`
					handler = h.StartSubtitles
				case "report options":
					handler = h.ReleaseReportOptions
				case "retry":
					method = http.MethodPost
					body = `{"sourceVersion":"` + sourceReportVersion(entry) + `"}`
					handler = h.RetryRelease
				}
				r := mux.SetURLVars(f.request(method, target, body), map[string]string{"prequeueID": entry.ID})
				w := httptest.NewRecorder()
				handler(w, r)
				if w.Code != http.StatusForbidden || validations.Load() != 0 || search.calls.Load() != 0 || probes.Load() != 0 {
					t.Fatalf("denied action demanded source: status=%d validator=%d search=%d probe=%d", w.Code, validations.Load(), search.calls.Load(), probes.Load())
				}
				// Control proves this restored ready entry really needs validation.
				if _, ok := h.store.Get(entry.ID); !ok || validations.Load() != 1 {
					t.Fatal("poison-validator fixture did not cover a real validating Get")
				}
			})
		}
	}
}
func TestVODPrequeueOwnedCleanupNeverValidatesEvenWithPolicyUnavailable(t *testing.T) {
	for _, action := range []string{"cancel retry", "remove manual"} {
		t.Run(action, func(t *testing.T) {
			h, f, _ := vodPrequeueFixture(t, vodAccountActive, true)
			entry := vodRestoredReady(t, h)
			f.change(func(f *vodTestState) { f.failure = errors.New("policy unavailable") })
			var validations atomic.Int32
			h.store.SetStreamPathValidator(func(context.Context, string) error { validations.Add(1); return nil })
			target := "/?profileId=profile"
			handler := h.CancelReleaseRetry
			if action == "remove manual" {
				target = "/?userId=profile&titleId=tmdb:movie:123"
				handler = h.RemoveManualPrequeue
			}
			w := httptest.NewRecorder()
			handler(w, mux.SetURLVars(f.request(http.MethodDelete, target, ""), map[string]string{"prequeueID": entry.ID}))
			if w.Code != http.StatusNoContent || validations.Load() != 0 {
				t.Fatalf("owned cleanup failed or demanded source: %d %d", w.Code, validations.Load())
			}
			if _, _, exists := h.store.SnapshotForAuthorization(entry.ID); exists {
				t.Fatal("owned cleanup did not remove metadata")
			}
		})
	}
}
func TestVODPrequeueStoredOwnerNeedsExplicitConsistentSelector(t *testing.T) {
	for _, target := range []string{"/", "/?profileId=foreign", "/?profileId=profile&userId=foreign"} {
		h, f, _ := vodPrequeueFixture(t, vodAccountActive, false)
		entry := vodRestoredReady(t, h)
		var calls atomic.Int32
		h.store.SetStreamPathValidator(func(context.Context, string) error { calls.Add(1); return nil })
		w := httptest.NewRecorder()
		h.GetStatus(w, mux.SetURLVars(f.request("GET", target, ""), map[string]string{"prequeueID": entry.ID}))
		if w.Code != 403 || calls.Load() != 0 {
			t.Fatal("opaque ID bypassed explicit current-profile consistency")
		}
	}
}
func TestVODPrewarmActiveAccountHasNoBackgroundParentPermit(t *testing.T) {
	h, _, search := vodPrequeueFixture(t, vodAccountActive, true)
	if id, err := h.RunWorkerSyncScoped(context.Background(), "tmdb:movie:123", "Synthetic Movie", "", "movie", 2024, "profile", "device", "scope", nil); err == nil || id != "" {
		t.Fatal("unbound scheduled worker created a preparation")
	}
	if len(h.store.ListAll()) != 0 || search.calls.Load() != 0 {
		t.Fatal("unbound background demand was started")
	}
}
func TestVODPrequeueInactiveWorkerStillUsesNormalPipeline(t *testing.T) {
	h, f, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
	w := httptest.NewRecorder()
	h.Prequeue(w, f.request(http.MethodPost, "/api/playback/prequeue", `{"titleId":"tmdb:movie:123","titleName":"Synthetic Movie","mediaType":"movie","userId":"profile","clientId":"device","skipHLS":true}`))
	if w.Code != 200 {
		t.Fatalf("inactive baseline failed: %s", w.Body.String())
	}
	var reply playback.PrequeueResponse
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	vodWaitReady(t, h, reply.PrequeueID)
}
func vodWaitReady(t *testing.T, h *PrequeueHandler, id string) *playback.PrequeueEntry {
	t.Helper()
	until := time.Now().Add(4 * time.Second)
	for time.Now().Before(until) {
		entry, _, ok := h.store.SnapshotForAuthorization(id)
		if ok && entry.Status == playback.PrequeueStatusReady {
			return entry
		}
		if ok && entry.Status == playback.PrequeueStatusFailed {
			t.Fatalf("synthetic preparation failed: %s", entry.Error)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("preparation did not become ready")
	return nil
}

func TestVODPrequeueLateResolverCannotPublishOrProbeAfterContextChange(t *testing.T) {
	h, f, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
	entry, _ := h.store.CreateScoped("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "details", h.prequeueSettingsScopeKey("profile", "device", "tmdb:movie:123"))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var probes atomic.Int32
	h.playbackSvc = &stubPlaybackService{resolve: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
		close(entered)
		<-release
		return &models.PlaybackResolution{WebDAVPath: "/synthetic/late.mkv"}, nil
	}}
	h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) { probes.Add(1); return nil, nil }}
	go func() {
		defer close(done)
		h.runPrequeueWorker(entry.ID, entry.TitleID, entry.TitleName, "", "movie", 2024, "profile", "device", nil, nil, 0, true, prequeueWorkerScheduledPrewarm)
	}()
	<-entered
	f.change(func(f *vodTestState) {
		f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "v2"}
		p := f.profiles["profile"]
		p.IsKidsProfile = true
		f.profiles["profile"] = p
	})
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late owner transition did not finish")
	}
	snapshot, _, _ := h.store.SnapshotForAuthorization(entry.ID)
	if snapshot.Status != playback.PrequeueStatusFailed || snapshot.StreamPath != "" || probes.Load() != 0 {
		t.Fatal("late old intent published/probed a source after activation")
	}
}
func TestVODPrequeueSuccessorCannotBeOverwrittenByLateSameIDWorker(t *testing.T) {
	h, _, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
	entry, _ := h.store.CreateScoped("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "details", h.prequeueSettingsScopeKey("profile", "device", "tmdb:movie:123"))
	entered, release, lateReturned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	originalContext := make(chan context.Context, 1)
	var resolves atomic.Int32
	h.playbackSvc = &stubPlaybackService{resolve: func(ctx context.Context, _ models.NZBResult) (*models.PlaybackResolution, error) {
		if resolves.Add(1) == 1 {
			originalContext <- ctx
			close(entered)
			// Deliberately retain the old resolver after real cancellation.
			<-release
			defer close(lateReturned)
			return &models.PlaybackResolution{WebDAVPath: "/synthetic/old.mkv"}, nil
		}
		return &models.PlaybackResolution{WebDAVPath: "/synthetic/new.mkv"}, nil
	}}
	h.fullProber = fallbackFullProber{run: func(_ context.Context, path string) (*VideoFullResult, error) {
		return &VideoFullResult{VideoCodec: "h264", Duration: 600, VideoWidth: 1920, VideoHeight: 1080, AudioStreams: []models.AudioStreamInfo{{Index: 4, Language: "fra", Codec: "aac"}}, MetadataEvidence: evidenceFacts(time.Now()), MetadataEvidenceInputVersion: probeInputVersion(path)}, nil
	}}
	run := func() {
		h.runPrequeueWorker(entry.ID, entry.TitleID, entry.TitleName, "", "movie", 2024, "profile", "device", nil, nil, 0, true, prequeueWorkerScheduledPrewarm)
	}
	wait := func(done <-chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Fatal("actual preparation operation did not finish")
		}
	}
	aDone, duplicateDone, bDone, cancelledAndJoined := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	aStarted, duplicateStarted, bStarted, cancelStarted := false, false, false, false
	// Failure cleanup is installed before every launch or witness wait.
	t.Cleanup(func() {
		h.store.CancelWork(entry.ID)
		unblock.Do(func() { close(release) })
		if aStarted {
			<-aDone
		}
		if duplicateStarted {
			<-duplicateDone
		}
		if cancelStarted {
			<-cancelledAndJoined
		}
		if bStarted {
			<-bDone
		}
		h.Shutdown()
	})
	aStarted = true
	go func() { defer close(aDone); run() }()
	wait(entered)
	ctx := <-originalContext
	h.vodBoundary.mu.Lock()
	oldOwner := h.vodBoundary.active[entry.ID]
	h.vodBoundary.mu.Unlock()
	if oldOwner == nil {
		t.Fatal("actual original worker owner missing")
	}
	stale := *h
	stale.vodCurrentWorker = oldOwner

	// A same-ID worker is refused while the original real work is retained.
	duplicateStarted = true
	go func() { defer close(duplicateDone); run() }()
	wait(duplicateDone)
	current, _, _ := h.store.SnapshotForAuthorization(entry.ID)
	if resolves.Load() != 1 || current.Status == playback.PrequeueStatusReady || current.StreamPath != "" {
		t.Fatal("same-ID worker bypassed the held original")
	}
	cancelStarted = true
	go func() { defer close(cancelledAndJoined); h.cancelAndWaitPrequeueWork(entry.ID, nil) }()
	wait(ctx.Done())
	select {
	case <-aDone:
		t.Fatal("cancelled resolver was not joined")
	default:
	}
	select {
	case <-cancelledAndJoined:
		t.Fatal("cancellation returned before actual old work ended")
	default:
	}
	unblock.Do(func() { close(release) })
	wait(lateReturned)
	wait(aDone)
	wait(cancelledAndJoined)

	bStarted = true
	go func() { defer close(bDone); run() }()
	wait(bDone)
	vodWaitReady(t, h, entry.ID)
	before, _, _ := h.store.SnapshotForAuthorization(entry.ID)
	if resolves.Load() != 2 || before.StreamPath != "/synthetic/new.mkv" || before.MediaEvidence == nil {
		t.Fatal("successor source not independently ready")
	}
	beforeJSON, _ := json.Marshal(before)
	var staleWrites atomic.Int32
	if stale.updateVODWorker(entry.ID, func(e *playback.PrequeueEntry) {
		staleWrites.Add(1)
		e.StreamPath = "/synthetic/old.mkv"
		e.AudioTracks = nil
		e.MediaEvidence = nil
	}) || staleWrites.Load() != 0 {
		t.Fatal("late original callback overwrote successor")
	}
	current, _, _ = h.store.SnapshotForAuthorization(entry.ID)
	after, _ := json.Marshal(current)
	if string(after) != string(beforeJSON) {
		t.Fatal("late worker corrupted successor status/path/tracks/evidence")
	}
}

func TestVODWorkerPrivateSetupAndWritesStayWithOriginalOwner(t *testing.T) {
	h, f, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
	entry, _ := h.store.Create("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "details")
	first, err := h.vodWorker(entry.ID, "profile", "device")
	if err != nil {
		t.Fatal(err)
	}
	defer h.finishVODWorker(entry.ID, first)
	if _, ok := h.beginVODAttempt(entry.ID, first); !ok {
		t.Fatal("first attempt unavailable")
	}
	second, err := h.vodWorker(entry.ID, "profile", "device")
	if err != nil {
		t.Fatal(err)
	}
	defer h.finishVODWorker(entry.ID, second)
	stamp, ok := h.beginVODAttempt(entry.ID, second)
	if !ok {
		t.Fatal("second attempt unavailable")
	}
	if _, ok = h.beginVODAttempt(entry.ID, first); ok {
		t.Fatal("late old worker minted successor attempt")
	}
	_, current, _ := h.store.SnapshotForAuthorization(entry.ID)
	if current != stamp {
		t.Fatal("late setup replaced successor stamp")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if !h.setVODCancel(entry.ID, second, cancel) || h.setVODCancel(entry.ID, first, func() { t.Error("old cancel installed") }) {
		t.Fatal("private cancellation hook changed owners")
	}
	h.store.CancelWork(entry.ID)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("current cancellation hook lost")
	}
	copy := *h
	copy.vodCurrentWorker = first
	var writes atomic.Int32
	if copy.updateVODWorker(entry.ID, func(*playback.PrequeueEntry) { writes.Add(1) }) || writes.Load() != 0 {
		t.Fatal("old owner performed an intermediate write")
	}
	data, _ := json.Marshal(first)
	if string(data) != "null" || strings.Contains(string(data), f.session.Token) {
		t.Fatal("worker owner serialized secrets")
	}
}

func TestVODPrequeueAndRetryUseAuthenticatedClientWithoutCallerClient(t *testing.T) {
	for _, action := range []string{"prequeue", "retry"} {
		t.Run(action, func(t *testing.T) {
			h, f, _ := vodPrequeueFixture(t, vodAccountActive, false)
			target := "/api/playback/prequeue"
			body := `{"titleId":"tmdb:movie:123","titleName":"Synthetic Movie","mediaType":"movie","userId":"profile","skipHLS":true}`
			handler := h.Prequeue
			id := ""
			if action == "retry" {
				entry := vodRestoredReady(t, h)
				id = entry.ID
				target = "/?profileId=profile"
				body = `{"sourceVersion":"` + sourceReportVersion(entry) + `","skipHLS":true}`
				handler = h.RetryRelease
			}
			r := f.request(http.MethodPost, target, body)
			r.Header.Del("X-Client-ID")
			if id != "" {
				r = mux.SetURLVars(r, map[string]string{"prequeueID": id})
			}
			w := httptest.NewRecorder()
			handler(w, r)
			if w.Code != 200 {
				t.Fatalf("authenticated-client fallback rejected: %d %s", w.Code, w.Body.String())
			}
			var response struct {
				PrequeueID  string                    `json:"prequeueId"`
				Replacement playback.PrequeueResponse `json:"replacement"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if action == "retry" {
				response.PrequeueID = response.Replacement.PrequeueID
			}
			ready := vodWaitReady(t, h, response.PrequeueID)
			expected := h.prequeueSettingsScopeKey("profile", "device", ready.TitleID)
			if ready.SettingsScopeKey != expected {
				t.Fatal("actual preparation used a different client scope than authorization")
			}
		})
	}
}

func TestVODTrustedInvalidationCancelsAnActualBlockedPreparation(t *testing.T) {
	h, _, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
	entry, _ := h.store.Create("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "details")
	entered, cancelled, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h.playbackSvc = &stubPlaybackService{resolve: func(ctx context.Context, _ models.NZBResult) (*models.PlaybackResolution, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}}
	go func() {
		defer close(done)
		h.runPrequeueWorker(entry.ID, entry.TitleID, entry.TitleName, "", "movie", 2024, "profile", "device", nil, nil, 0, true, prequeueWorkerScheduledPrewarm)
	}()
	<-entered
	h.vodBoundary.invalidateProfile("profile")
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("trusted invalidation did not cancel active demand")
	}
	<-done
	snapshot, _, _ := h.store.SnapshotForAuthorization(entry.ID)
	if snapshot.Status == playback.PrequeueStatusReady {
		t.Fatal("revoked private owner published a source")
	}
}

type vodChangingWarm struct {
	profileWarmRef
	change func()
}

func (w *vodChangingWarm) GetWarmScoped(string, string, string) *playback.WarmRef {
	w.change()
	return &playback.WarmRef{PrequeueID: w.id}
}
func TestVODPrequeueRechecksChangedWarmBeforeValidatingItsSource(t *testing.T) {
	h, f, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
	entry := vodRestoredReady(t, h)
	var validations atomic.Int32
	h.store.SetStreamPathValidator(func(context.Context, string) error { validations.Add(1); return nil })
	h.prewarmSvc = &vodChangingWarm{profileWarmRef: profileWarmRef{id: entry.ID}, change: func() {
		f.change(func(f *vodTestState) {
			f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "v2"}
		})
	}}
	w := httptest.NewRecorder()
	h.Prequeue(w, f.request(http.MethodPost, "/", `{"titleId":"tmdb:movie:123","titleName":"Synthetic Movie","mediaType":"movie","userId":"profile","clientId":"device","skipHLS":true}`))
	if w.Code != 403 || validations.Load() != 0 {
		t.Fatal("changed warm callback started a new source validation")
	}
}
func TestVODPrequeueStatusAndReportDoNotPublishAfterValidatorChangesPolicy(t *testing.T) {
	for _, report := range []bool{false, true} {
		h, f, _ := vodPrequeueFixture(t, vodExplicitInactive, false)
		entry := vodRestoredReady(t, h)
		var validations atomic.Int32
		h.store.SetStreamPathValidator(func(context.Context, string) error {
			validations.Add(1)
			f.change(func(f *vodTestState) {
				f.states["account"] = vodAccountApplicability{mode: vodAccountActive, revision: "v2"}
			})
			return nil
		})
		w := httptest.NewRecorder()
		r := mux.SetURLVars(f.request(http.MethodGet, "/?profileId=profile", ""), map[string]string{"prequeueID": entry.ID})
		if report {
			h.ReleaseReportOptions(w, r)
		} else {
			h.GetStatus(w, r)
		}
		if w.Code != 403 || validations.Load() != 1 || strings.Contains(w.Body.String(), entry.StreamPath) || strings.Contains(w.Body.String(), entry.SelectedResult.Title) {
			t.Fatal("changed validating Get leaked a stale ready/report response")
		}
	}
}
