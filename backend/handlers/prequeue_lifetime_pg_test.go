package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/indexer"
	"novastream/services/playback"
	"novastream/services/prewarm"
	"novastream/services/profileunlock"
)

func prequeueLifeWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		t.Fatal("actual prequeue work did not end")
	}
}
func prequeueLifePending(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("actual held work was refunded early")
	case <-time.After(20 * time.Millisecond):
	}
}
func prequeueAssertHeldLoser(t *testing.T) {
	t.Helper()
	started, cancelled, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var winner *candidateResolution
	var err error
	t.Cleanup(func() { unblock(); prequeueLifeWait(t, done) })
	go func() {
		defer close(done)
		winner, _, err = racePrequeueResolutionsWithJoinTimeout(t.Context(), newSliceCandidateSource([]models.NZBResult{{Title: "winner"}, {Title: "held"}}), 2, func(ctx context.Context, i int, _ models.NZBResult) (*candidateResolution, *candidateResolution, error) {
			if i == 1 {
				close(started)
				<-ctx.Done()
				close(cancelled)
				<-release
				return nil, nil, ctx.Err()
			}
			<-started
			return &candidateResolution{index: 0, resolution: &models.PlaybackResolution{WebDAVPath: "synthetic:winner"}}, nil, nil
		}, nil, 0, true, time.Millisecond)
	}()
	prequeueLifeWait(t, cancelled)
	prequeueLifePending(t, done)
	unblock()
	prequeueLifeWait(t, done)
	if err != nil || winner == nil || winner.index != 0 {
		t.Fatal("actual joined winner lost", err)
	}
}

type prequeueHTTPGate struct {
	calls                            atomic.Int32
	contexts                         chan context.Context
	client                           *http.Client
	server                           *httptest.Server
	entered, closing, release        chan struct{}
	readOnce, closeOnce, releaseOnce sync.Once
}
type prequeueGateTransport struct {
	actual http.RoundTripper
	gate   *prequeueHTTPGate
}

func (t prequeueGateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.actual.RoundTrip(r)
	if response != nil && response.Body != nil {
		response.Body = &prequeueGateBody{ReadCloser: response.Body, gate: t.gate}
	}
	return response, err
}

type prequeueGateBody struct {
	io.ReadCloser
	gate *prequeueHTTPGate
}

func (b *prequeueGateBody) Read(p []byte) (int, error) {
	b.gate.readOnce.Do(func() { close(b.gate.entered) })
	return b.ReadCloser.Read(p)
}
func (b *prequeueGateBody) Close() error {
	b.gate.closeOnce.Do(func() { close(b.gate.closing) })
	<-b.gate.release
	return b.ReadCloser.Close()
}
func newPrequeueHTTPGate(t *testing.T) *prequeueHTTPGate {
	t.Helper()
	gate := &prequeueHTTPGate{contexts: make(chan context.Context, 4), entered: make(chan struct{}), closing: make(chan struct{}), release: make(chan struct{})}
	gate.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate.calls.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("SYNTHETIC"))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	transport := &http.Transport{}
	gate.client = &http.Client{Transport: prequeueGateTransport{actual: transport, gate: gate}}
	t.Cleanup(func() { gate.unblock(); transport.CloseIdleConnections(); gate.server.Close() })
	return gate
}
func (g *prequeueHTTPGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }
func (g *prequeueHTTPGate) MovieInfo(ctx context.Context, _ models.MovieDetailsQuery) (*models.Title, error) {
	select {
	case g.contexts <- ctx:
	default:
	}
	request, err := http.NewRequestWithContext(ctx, "GET", g.server.URL, nil)
	if err != nil {
		return nil, err
	}
	response, err := g.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	_, err = io.ReadAll(response.Body)
	return nil, err
}
func prequeueLifeHandler(t *testing.T, g *prequeueHTTPGate) *PrequeueHandler {
	t.Helper()
	return &PrequeueHandler{store: playback.NewPrequeueStore(time.Hour), movieMetadataSvc: g}
}
func prequeueLifeSync(t *testing.T, h *PrequeueHandler, ctx context.Context) (<-chan struct{}, *error) {
	t.Helper()
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		_, err = h.RunWorkerSyncScoped(ctx, "synthetic-movie", "Synthetic movie", "", "movie", 2026, "synthetic-profile", "", playback.DefaultPrequeueSettingsScopeKey, nil)
	}()
	return done, &err
}

// Every row below invokes actual cancellation/entry/worker code. The gate wraps
// a real loopback response body; its Close must finish before source retirement.
func TestPrequeueOriginalWorkerPhysicalLifetime(t *testing.T) {
	for _, kind := range []string{"caller-context-reaches-actual-sync-worker", "winner-retains-held-loser-until-source-end", "cancel-joins-actual-incoming-feeder", "cancel-joins-source-preparation", "expired-worker-no-ready-publication", "original-owner-handoff-before-async-start", "actual-request-release-does-not-renew-owner", "foreign-frame-zero-provider-work", "retired-source-zero-fresh-candidate-work", "remove-waits-exact-original-worker", "replacement-keeps-retired-original-charged", "shutdown-denies-admission-and-joins", "real-prequeue-context-end-to-end"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "real-prequeue-context-end-to-end" {
				prequeueActualPrewarm(t)
				return
			}
			if kind == "winner-retains-held-loser-until-source-end" {
				prequeueAssertHeldLoser(t)
				return
			}
			if kind == "cancel-joins-actual-incoming-feeder" {
				prequeueHeldIncoming(t)
				return
			}
			if kind == "cancel-joins-source-preparation" {
				prequeueHeldPreparation(t)
				return
			}
			if strings.Contains(kind, "original-owner") || strings.Contains(kind, "actual-request-release") || kind == "foreign-frame-zero-provider-work" || kind == "retired-source-zero-fresh-candidate-work" {
				prequeueActualOriginal(t, kind)
				return
			}
			gate := newPrequeueHTTPGate(t)
			h := prequeueLifeHandler(t, gate)
			ctx, cancel := context.WithCancel(t.Context())
			var done <-chan struct{}
			var result *error
			t.Cleanup(func() {
				cancel()
				gate.unblock()
				if done != nil {
					prequeueLifeWait(t, done)
				}
				h.Shutdown()
			})
			if kind == "expired-worker-no-ready-publication" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 200*time.Millisecond)
				defer stop()
			}
			done, result = prequeueLifeSync(t, h, ctx)
			prequeueLifeWait(t, gate.entered)
			closed := make(chan struct{})
			closeStarted := false
			t.Cleanup(func() {
				cancel()
				gate.unblock()
				if closeStarted {
					prequeueLifeWait(t, closed)
				}
			})
			if kind == "shutdown-denies-admission-and-joins" {
				closeStarted = true
				go func() { defer close(closed); h.Shutdown() }()
			} else if kind == "remove-waits-exact-original-worker" {
				// The actual cleanup path waits the exact worker even when this entry was
				// selected manually after its initial synchronous preparation request.
				entry, ok := h.store.GetByTitleUserScope("synthetic-movie", "synthetic-profile", playback.DefaultPrequeueSettingsScopeKey)
				if !ok {
					t.Fatal("actual preparing entry absent")
				}
				h.store.Update(entry.ID, func(e *playback.PrequeueEntry) { e.Persistent = true })
				closeStarted = true
				go func() {
					defer close(closed)
					w := httptest.NewRecorder()
					h.RemoveManualPrequeue(w, httptest.NewRequest("DELETE", "/?userId=synthetic-profile&titleId=synthetic-movie&mediaType=movie", nil))
					if w.Code != 204 {
						t.Error("actual remove failed", w.Code)
					}
				}()
			} else if kind != "expired-worker-no-ready-publication" {
				cancel()
			}
			prequeueLifeWait(t, gate.closing)
			prequeueLifePending(t, done)
			life := h.workerLifetimes()
			life.mu.Lock()
			charged := len(life.active)
			life.mu.Unlock()
			if charged != 1 {
				t.Fatal("actual body Close lost worker charge", charged)
			}
			if kind == "replacement-keeps-retired-original-charged" {
				entry, ok := h.store.GetByTitleUserScope("synthetic-movie", "synthetic-profile", playback.DefaultPrequeueSettingsScopeKey)
				if !ok {
					t.Fatal("held entry absent")
				}
				if next, e := h.beginPrequeueWork(t.Context(), entry.ID); e == nil || next != nil {
					if next != nil {
						next.finish()
					}
					t.Fatal("same-key replacement bypassed held original")
				}
			}
			if closeStarted {
				prequeueLifePending(t, closed)
			}
			gate.unblock()
			prequeueLifeWait(t, done)
			if closeStarted {
				prequeueLifeWait(t, closed)
			}
			if *result == nil {
				t.Fatal("cancelled original worker advertised success")
			}
			if entry, ok := h.store.GetByTitleUserScope("synthetic-movie", "synthetic-profile", playback.DefaultPrequeueSettingsScopeKey); ok && entry.Status == playback.PrequeueStatusReady {
				t.Fatal("cancelled worker published ready")
			}
			if kind == "shutdown-denies-admission-and-joins" {
				if work, e := h.beginPrequeueWork(t.Context(), "late"); e == nil || work != nil {
					t.Fatal("shutdown admitted a later worker")
				}
			}
		})
	}
}

type prequeueHeldSource struct {
	entered, release chan struct{}
	once             sync.Once
}

func (s *prequeueHeldSource) Next(ctx context.Context) (int, models.NZBResult, bool) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	<-s.release
	return 0, models.NZBResult{}, false
}
func (s *prequeueHeldSource) Total() int                   { return 0 }
func (s *prequeueHeldSource) Snapshot() []models.NZBResult { return nil }
func prequeueHeldIncoming(t *testing.T) {
	src := &prequeueHeldSource{entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(src.release) }) }
	t.Cleanup(func() { cancel(); unblock(); prequeueLifeWait(t, done) })
	go func() {
		defer close(done)
		_, _, _ = racePrequeueResolutions(ctx, src, 1, func(context.Context, int, models.NZBResult) (*candidateResolution, *candidateResolution, error) {
			t.Error("empty source launched a candidate")
			return nil, nil, errors.New("unexpected")
		}, nil, 0, true)
	}()
	prequeueLifeWait(t, src.entered)
	cancel()
	prequeueLifePending(t, done)
	unblock()
	prequeueLifeWait(t, done)
}

type prequeuePreparationGate struct{ gate *prequeueHTTPGate }

func (p prequeuePreparationGate) Resolve(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
	return nil, errors.New("unexpected resolution")
}
func (p prequeuePreparationGate) QueueStatus(context.Context, int64) (*models.PlaybackResolution, error) {
	return nil, errors.New("unexpected queue")
}
func (p prequeuePreparationGate) PrepareTorrentCandidates(ctx context.Context, candidates []models.NZBResult) []models.NZBResult {
	_, _ = p.gate.MovieInfo(ctx, models.MovieDetailsQuery{})
	return candidates
}
func prequeueHeldPreparation(t *testing.T) {
	gate := newPrequeueHTTPGate(t)
	h := prequeueLifeHandler(t, gate)
	h.playbackSvc = prequeuePreparationGate{gate}
	entry, _ := h.store.CreateScoped("synthetic-feed", "Synthetic feed", "synthetic-profile", "movie", 2026, nil, "synthetic", playback.DefaultPrequeueSettingsScopeKey)
	state := newPrequeueFeedState()
	src := newStreamCandidateSource()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	debrid := make(chan indexer.ScoredSplitSearchResult, 1)
	debrid <- indexer.ScoredSplitSearchResult{Source: "debrid", Scored: []models.ScoredNZBResult{{NZBResult: models.NZBResult{Title: "Synthetic candidate"}}}}
	close(debrid)
	usenet := make(chan indexer.ScoredSplitSearchResult)
	close(usenet)
	t.Cleanup(func() { cancel(); gate.unblock(); prequeueLifeWait(t, done) })
	go func() {
		defer close(done)
		h.prequeueSearchFeeder(ctx, src, usenet, debrid, state, prequeueFeederConfig{prequeueID: entry.ID, maxCandidates: 2})
	}()
	prequeueLifeWait(t, gate.entered)
	cancel()
	prequeueLifeWait(t, gate.closing)
	prequeueLifePending(t, state.done)
	prequeueLifePending(t, done)
	gate.unblock()
	prequeueLifeWait(t, state.done)
	prequeueLifeWait(t, done)
}

func prequeueActualOriginal(t *testing.T, kind string) {
	if kind == "original-owner-handoff-before-async-start" {
		prequeueActualHTTPHandoff(t)
		return
	}
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	r, release := f.request(t, "GET", "/?profileId="+url.QueryEscape(f.profile.ID))
	defer release()
	b := newVODBoundary(installed.Boundary.boundary.identity, installed.Boundary.boundary.applicability, nil)
	// Only the actual PG issuer can supply the private original core.
	b.original = func(r *http.Request, target string) (vodOriginalOwner, error) {
		return f.pins.CaptureFamilyMediaOwner(r, target)
	}
	if kind == "foreign-frame-zero-provider-work" {
		foreign, e := datastore.New(t.Context(), f.store.Pool().Config().ConnString())
		if e != nil {
			t.Fatal(e)
		}
		defer foreign.Close()
		pins := profileunlock.NewPGProfilePins(foreign)
		payload, _ := json.Marshal(playback.PrequeueRequest{TitleID: "synthetic-movie", TitleName: "Synthetic movie", MediaType: "movie", Year: 2026, UserID: f.profile.ID, SkipHLS: true})
		raw := httptest.NewRequest("POST", "/?profileId="+url.QueryEscape(f.profile.ID), strings.NewReader(string(payload)))
		framed, stop, e := pins.Authenticate(raw, f.session.Token, "synthetic-client")
		if e != nil {
			t.Fatal(e)
		}
		defer stop()
		gate := newPrequeueHTTPGate(t)
		h := prequeueLifeHandler(t, gate)
		h.vodBoundary = b
		w := httptest.NewRecorder()
		h.Prequeue(w, framed)
		h.Shutdown()
		if w.Code != 403 || gate.calls.Load() != 0 {
			t.Fatal("actual foreign frame opened provider or became owner", w.Code, gate.calls.Load())
		}
		return
	}
	owner, ok := b.requestOwner(httptest.NewRecorder(), r, vodDemand, f.profile.ID)
	if !ok {
		t.Fatal("genuine original demand owner denied")
	}
	defer owner.release()
	if kind == "actual-request-release-does-not-renew-owner" {
		prequeueActualLookupCancellation(t, f, b, owner, r)
	}
	gate := newPrequeueHTTPGate(t)
	h := prequeueLifeHandler(t, gate)
	h.vodBoundary = b
	entry, created := h.store.CreateScoped("synthetic-movie", "Synthetic movie", f.profile.ID, "movie", 2026, nil, "synthetic", playback.DefaultPrequeueSettingsScopeKey)
	if !created || entry == nil {
		t.Fatal("actual original worker fixture entry was not created")
	}
	work, e := h.beginPrequeueWork(t.Context(), entry.ID)
	if e != nil {
		t.Fatal(e)
	}
	launched := false
	t.Cleanup(func() {
		work.cancel()
		gate.unblock()
		if launched {
			prequeueLifeWait(t, work.done)
		} else {
			work.finish()
			b.discardBoundWorker(entry.ID)
		}
		h.Shutdown()
	})
	if !b.bindWorker(entry.ID, owner, r) {
		t.Fatal("actual original handoff denied")
	}
	worker := b.workers[entry.ID]
	if worker == nil || worker.original == nil || owner.original != worker.original || owner.originalRef == nil || worker.originalRef == owner.originalRef {
		t.Fatal("original concrete core not moved exactly once")
	}
	original := worker.original
	expiry := original.ExpiresAt()
	launched = true
	go func() {
		defer work.finish()
		h.runPrequeueWorkerJob(work, entry.ID, "synthetic-movie", "Synthetic movie", "", "movie", 2026, f.profile.ID, "synthetic-client", nil, nil, 0, true, prequeueWorkerInteractive)
	}()
	release()
	owner.release()
	prequeueLifeWait(t, gate.entered)
	var actual context.Context
	select {
	case actual = <-gate.contexts:
	case <-time.After(5 * time.Second):
		t.Fatal("actual provider context absent")
	}
	deadline, has := actual.Deadline()
	if !has || deadline.After(expiry) || worker.recheck(t.Context(), vodDemand) != nil || !original.ExpiresAt().Equal(expiry) {
		t.Fatal("actual async source lost or renewed original lifetime")
	}
	if kind == "retired-source-zero-fresh-candidate-work" {
		if _, e = f.store.Pool().Exec(t.Context(), `DELETE FROM sessions WHERE token=$1`, worker.session.Token); e != nil {
			t.Fatal(e)
		}
	} else if kind == "actual-request-release-does-not-renew-owner" {
		prequeueActualPublishCancellation(t, f, h, work, worker, entry.ID)
	} else {
		work.cancel()
	}
	prequeueLifeWait(t, gate.closing)
	prequeueLifePending(t, work.done)
	if kind == "retired-source-zero-fresh-candidate-work" && actual.Err() == nil {
		t.Fatal("current original session revoke did not cancel held source")
	}
	gate.unblock()
	prequeueLifeWait(t, work.done)
	if original.Check(t.Context()) == nil {
		t.Fatal("actual retired worker retained original grant")
	}
	if gate.calls.Load() != 1 {
		t.Fatal("retired original opened fresh metadata/source work", gate.calls.Load())
	}
}

// Hold the actual accounts relation, observe the original query waiting in
// PostgreSQL, then cancel that caller without retiring the current core.
func prequeueActualLookupCancellation(t *testing.T, f *publicPinCarrierFixture, b *vodBoundary, owner *vodOwnerContext, r *http.Request) {
	t.Helper()
	held, e := f.store.Pool().Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	tx, e := held.Begin(t.Context())
	if e != nil {
		held.Release()
		t.Fatal(e)
	}
	released := false
	releaseHeld := func() {
		if !released {
			released = true
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = tx.Rollback(cleanup)
			held.Release()
		}
	}
	ctx, cancel := context.WithCancel(r.Context())
	done := make(chan struct{})
	started := false
	var mutexDone chan struct{}
	t.Cleanup(func() {
		cancel()
		releaseHeld()
		if started {
			prequeueLifeWait(t, done)
		}
		if mutexDone != nil {
			prequeueLifeWait(t, mutexDone)
		}
	})
	if _, e = tx.Exec(t.Context(), `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`); e != nil {
		t.Fatal(e)
	}
	var bound bool
	started = true
	go func() {
		defer close(done)
		bound = b.bindWorkerContext(ctx, r.Context(), "synthetic-held-original-lookup", owner, r)
	}()
	until := time.NewTimer(5 * time.Second)
	defer until.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	waiting := false
	for !waiting {
		select {
		case <-done:
			t.Fatal("original lookup did not wait for actual held SQL relation")
		case <-until.C:
			t.Fatal("actual pending SQL query not witnessed")
		case <-tick.C:
			if _, e = tx.Exec(t.Context(), `SELECT pg_stat_clear_snapshot()`); e != nil {
				t.Fatal(e)
			}
			var count int
			if e = tx.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&count); e != nil {
				t.Fatal(e)
			}
			waiting = count > 0
		}
	}
	// Real original SQL must not hold the registry mutex while waiting.
	mutexDone = make(chan struct{})
	go func() { b.mu.Lock(); b.mu.Unlock(); close(mutexDone) }()
	prequeueLifeWait(t, mutexDone)
	cancel()
	prequeueLifeWait(t, done)
	if bound {
		t.Fatal("cancelled original lookup installed worker")
	}
	releaseHeld()
	if owner.original.Check(t.Context()) != nil {
		t.Fatal("departed setup lookup retired original current owner")
	}
	b.mu.Lock()
	pending := len(b.bindings)
	b.mu.Unlock()
	if pending != 0 {
		t.Fatal("cancelled exact binding reservation retained")
	}
}

func prequeueActualPublishCancellation(t *testing.T, f *publicPinCarrierFixture, h *PrequeueHandler, work *prequeueWorkerLifetime, owner *vodOwnerContext, id string) {
	t.Helper()
	held, e := f.store.Pool().Acquire(t.Context())
	if e != nil {
		t.Fatal(e)
	}
	tx, e := held.Begin(t.Context())
	if e != nil {
		held.Release()
		t.Fatal(e)
	}
	released := false
	releaseHeld := func() {
		if !released {
			released = true
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_ = tx.Rollback(cleanup)
			held.Release()
		}
	}
	done := make(chan struct{})
	started := false
	var mutexDone chan struct{}
	t.Cleanup(func() {
		work.cancel()
		releaseHeld()
		if started {
			prequeueLifeWait(t, done)
		}
		if mutexDone != nil {
			prequeueLifeWait(t, mutexDone)
		}
	})
	if _, e = tx.Exec(t.Context(), `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`); e != nil {
		t.Fatal(e)
	}
	actual := *h
	actual.currentWork = work
	actual.vodCurrentWorker = owner
	var published atomic.Bool
	var accepted bool
	started = true
	go func() {
		defer close(done)
		accepted = actual.publishVODReady(id, owner, func(*playback.PrequeueEntry) { published.Store(true) })
	}()
	until := time.NewTimer(5 * time.Second)
	defer until.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	waiting := false
	for !waiting {
		select {
		case <-done:
			t.Fatal("publication did not reach actual held current query")
		case <-until.C:
			t.Fatal("actual publication SQL wait missing")
		case <-tick.C:
			if _, e = tx.Exec(t.Context(), `SELECT pg_stat_clear_snapshot()`); e != nil {
				t.Fatal(e)
			}
			var count int
			if e = tx.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND wait_event_type='Lock'`).Scan(&count); e != nil {
				t.Fatal(e)
			}
			waiting = count > 0
		}
	}
	mutexDone = make(chan struct{})
	go func() { h.vodBoundary.mu.Lock(); h.vodBoundary.mu.Unlock(); close(mutexDone) }()
	prequeueLifeWait(t, mutexDone)
	work.cancel()
	prequeueLifeWait(t, done)
	releaseHeld()
	if accepted || published.Load() {
		t.Fatal("cancelled actual publication changed ready state")
	}
}

type prequeueLifeUsers struct{}

func (prequeueLifeUsers) ListAll() []models.User {
	return []models.User{{ID: "synthetic-profile", Name: "Synthetic profile"}}
}

type prequeueLifeHistory struct{}

func (prequeueLifeHistory) ListContinueWatching(string) ([]models.SeriesWatchState, error) {
	return []models.SeriesWatchState{{SeriesID: "synthetic-movie", SeriesTitle: "Synthetic movie", Year: 2026, UpdatedAt: time.Now().UTC()}}, nil
}
func prequeueActualPrewarm(t *testing.T) {
	gate := newPrequeueHTTPGate(t)
	h := prequeueLifeHandler(t, gate)
	service := prewarm.NewService(nil, "")
	service.SetUsersService(prequeueLifeUsers{})
	service.SetHistoryService(prequeueLifeHistory{})
	service.SetPrequeueStore(h.store)
	service.SetScopedWorkerFunc(h.RunWorkerSyncScoped)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var err error
	t.Cleanup(func() { cancel(); gate.unblock(); prequeueLifeWait(t, done); service.Stop(); h.Shutdown() })
	go func() { defer close(done); _, err = service.RunOnce(ctx) }()
	prequeueLifeWait(t, gate.entered)
	cancel()
	prequeueLifeWait(t, gate.closing)
	prequeueLifePending(t, done)
	life := h.workerLifetimes()
	life.mu.Lock()
	charged := len(life.active)
	life.mu.Unlock()
	if charged != 1 {
		t.Fatal("actual service to handler lost source charge")
	}
	gate.unblock()
	prequeueLifeWait(t, done)
	if !errors.Is(err, context.Canceled) {
		t.Fatal("actual service/handler caller cancellation lost", err)
	}
	if _, ok := h.store.GetByTitleUserScope("synthetic-movie", "synthetic-profile", playback.DefaultPrequeueSettingsScopeKey); ok {
		t.Fatal("actual cancelled end-to-end entry retained")
	}
}

type prequeueHeldHTTPListener struct {
	net.Listener
	entered, release chan struct{}
	once             sync.Once
}
type prequeueHeldHTTPConn struct {
	net.Conn
	listener *prequeueHeldHTTPListener
}

func (l *prequeueHeldHTTPListener) Accept() (net.Conn, error) {
	connection, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	return &prequeueHeldHTTPConn{connection, l}, nil
}
func (c *prequeueHeldHTTPConn) Write(p []byte) (int, error) {
	c.listener.once.Do(func() { close(c.listener.entered) })
	<-c.listener.release
	return c.Conn.Write(p)
}

type prequeueCancelSetupWriter struct {
	http.ResponseWriter
	handler *PrequeueHandler
	once    sync.Once
}

func (w *prequeueCancelSetupWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *prequeueCancelSetupWriter) SetWriteDeadline(at time.Time) error {
	w.once.Do(func() {
		life := w.handler.workerLifetimes()
		life.mu.Lock()
		for work := range life.active {
			work.cancel()
		}
		life.mu.Unlock()
	})
	return http.NewResponseController(w.ResponseWriter).SetWriteDeadline(at)
}
func prequeueActualHTTPHandoff(t *testing.T) {
	f := newPublicPinCarrierFixture(t)
	installed := familyActualPGInstall(t, f)
	for _, mode := range []string{"ordinary", "cancelled-setup", "report-retry"} {
		prequeueActualHTTPHandoffCase(t, f, installed, mode)
	}
}
func prequeueActualHTTPHandoffCase(t *testing.T, f *publicPinCarrierFixture, installed *FamilyMediaInstallation, mode string) {
	b := newVODBoundary(installed.Boundary.boundary.identity, installed.Boundary.boundary.applicability, nil)
	b.original = func(r *http.Request, target string) (vodOriginalOwner, error) {
		return f.pins.CaptureFamilyMediaOwner(r, target)
	}
	gate := newPrequeueHTTPGate(t)
	h := prequeueLifeHandler(t, gate)
	h.vodBoundary = b
	var reported *playback.PrequeueEntry
	if mode == "report-retry" {
		h.SetBadStreamsService(badstreams.New(filepath.Join(t.TempDir(), "synthetic-reports.json")))
		initial, created := h.store.Create("synthetic-movie", "Synthetic movie", f.profile.ID, "movie", 2026, nil, "manual")
		if !created || initial == nil {
			t.Fatal("actual report fixture entry was not created")
		}
		h.store.Update(initial.ID, func(e *playback.PrequeueEntry) {
			e.Status = playback.PrequeueStatusReady
			e.StreamPath = "/synthetic-old-release.mp4"
			e.SelectedResult = &models.NZBResult{Title: "Synthetic wrong release", Link: "https://example.invalid/synthetic-wrong"}
		})
		reported, _ = h.store.Get(initial.ID)
	}
	handlerDone, clientDone := make(chan struct{}), make(chan struct{})
	var began bool
	var mu sync.Mutex
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		began = true
		mu.Unlock()
		defer close(handlerDone)
		if reported != nil {
			r = mux.SetURLVars(r, map[string]string{"prequeueID": reported.ID})
		}
		actual, release, e := f.pins.Authenticate(r, f.session.Token, "synthetic-client")
		if e != nil {
			t.Error("actual HTTP PG frame", e)
			http.Error(w, "source unavailable", 401)
			return
		}
		defer release()
		if reported != nil {
			h.ReportRelease(w, actual)
		} else if mode == "cancelled-setup" {
			h.Prequeue(&prequeueCancelSetupWriter{ResponseWriter: w, handler: h}, actual)
		} else {
			h.Prequeue(w, actual)
		}
	}))
	listener := &prequeueHeldHTTPListener{Listener: server.Listener, entered: make(chan struct{}), release: make(chan struct{})}
	server.Listener = listener
	server.Start()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(listener.release) }) }
	transport := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: transport, Timeout: 20 * time.Second}
	ctx, cancel := context.WithCancel(t.Context())
	var result playback.PrequeueResponse
	var rawResult []byte
	var status int
	var requestErr error
	t.Cleanup(func() {
		cancel()
		unblock()
		gate.unblock()
		prequeueLifeWait(t, clientDone)
		h.Shutdown()
		transport.CloseIdleConnections()
		server.Close()
		mu.Lock()
		started := began
		mu.Unlock()
		if started {
			prequeueLifeWait(t, handlerDone)
		}
	})
	go func() {
		defer close(clientDone)
		payload, _ := json.Marshal(playback.PrequeueRequest{TitleID: "synthetic-movie", TitleName: "Synthetic movie", MediaType: "movie", Year: 2026, UserID: f.profile.ID, SkipHLS: true})
		if reported != nil {
			payload, _ = json.Marshal(releaseReportRequest{Reason: "wrong_title", SourceVersion: sourceReportVersion(reported), Retry: true, SkipHLS: true, ClientID: "synthetic-client"})
		}
		request, e := http.NewRequestWithContext(ctx, "POST", server.URL+"/?profileId="+url.QueryEscape(f.profile.ID), strings.NewReader(string(payload)))
		if e != nil {
			requestErr = e
			return
		}
		request.Header.Set("Content-Type", "application/json")
		response, e := client.Do(request)
		if e != nil {
			requestErr = e
			return
		}
		status = response.StatusCode
		raw, e := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if e != nil {
			requestErr = e
			return
		}
		if closeErr != nil {
			requestErr = closeErr
			return
		}
		rawResult = raw
		if mode == "cancelled-setup" {
			return
		}
		if reported != nil {
			var decoded struct {
				Replacement playback.PrequeueResponse `json:"replacement"`
			}
			requestErr = json.Unmarshal(raw, &decoded)
			result = decoded.Replacement
		} else {
			requestErr = json.Unmarshal(raw, &result)
		}
	}()
	if mode == "cancelled-setup" {
		prequeueLifeWait(t, listener.entered)
		if gate.calls.Load() != 0 {
			t.Fatal("cancelled setup opened actual source")
		}
		unblock()
		prequeueLifeWait(t, clientDone)
		prequeueLifeWait(t, handlerDone)
		h.Shutdown()
		if requestErr != nil || status != 503 || !strings.Contains(string(rawResult), "prequeue worker unavailable") {
			t.Fatal("cancelled setup committed implicit success before its failure", status, requestErr, string(rawResult))
		}
		return
	}
	prequeueLifeWait(t, gate.entered)
	prequeueLifeWait(t, listener.entered)
	b.mu.Lock()
	var current *vodOwnerContext
	for _, owner := range b.active {
		current = owner
	}
	b.mu.Unlock()
	if current == nil || current.original == nil {
		t.Fatal("actual HTTP worker missing original")
	}
	original := current.original
	life := h.workerLifetimes()
	life.mu.Lock()
	var work *prequeueWorkerLifetime
	for active := range life.active {
		work = active
	}
	life.mu.Unlock()
	if work == nil {
		t.Fatal("actual source charge absent")
	}
	work.cancel()
	prequeueLifeWait(t, gate.closing)
	gate.unblock()
	prequeueLifeWait(t, work.done)
	prequeueLifePending(t, handlerDone)
	if original.Check(t.Context()) != nil {
		t.Fatal("worker physical end closed core while actual HTTP Write was still held")
	}
	unblock()
	prequeueLifeWait(t, clientDone)
	prequeueLifeWait(t, handlerDone)
	if requestErr != nil || status != 200 || result.PrequeueID == "" {
		t.Fatal("genuine queued HTTP response failed", status, requestErr)
	}
	if original.Check(t.Context()) == nil {
		t.Fatal("last actual HTTP completion retained original core")
	}
	if reported != nil {
		if _, ok := h.store.Get(reported.ID); !ok {
			t.Fatal("retry removed original selected source")
		}
		if !strings.Contains(string(rawResult), "report") {
			t.Fatal("committed report missing from actual retry response")
		}
	}
}
