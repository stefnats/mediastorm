package prewarm

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/playback"
)

func prewarmLifeWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(12 * time.Second):
		t.Fatal("actual prewarm source did not end")
	}
}
func prewarmLifePending(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("actual held prewarm source was refunded")
	case <-time.After(20 * time.Millisecond):
	}
}
func prewarmLifePoll(t *testing.T, condition func() bool) {
	t.Helper()
	until := time.Now().Add(8 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual prewarm source witness missing")
}

type prewarmHTTPGate struct {
	server                  *httptest.Server
	client                  *http.Client
	reading, closing, calls atomic.Int32
	release                 chan struct{}
	once                    sync.Once
}
type prewarmGateTransport struct {
	actual http.RoundTripper
	gate   *prewarmHTTPGate
}

func (t prewarmGateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := t.actual.RoundTrip(r)
	if response != nil && response.Body != nil {
		response.Body = &prewarmGateBody{ReadCloser: response.Body, gate: t.gate}
	}
	return response, err
}

type prewarmGateBody struct {
	io.ReadCloser
	gate *prewarmHTTPGate
	once sync.Once
}

func (b *prewarmGateBody) Read(p []byte) (int, error) {
	b.once.Do(func() { b.gate.reading.Add(1) })
	return b.ReadCloser.Read(p)
}
func (b *prewarmGateBody) Close() error {
	b.gate.closing.Add(1)
	<-b.gate.release
	return b.ReadCloser.Close()
}
func (g *prewarmHTTPGate) unblock() { g.once.Do(func() { close(g.release) }) }
func newPrewarmHTTPGate(t *testing.T) *prewarmHTTPGate {
	t.Helper()
	g := &prewarmHTTPGate{release: make(chan struct{})}
	g.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.calls.Add(1)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("SYNTHETIC-PREWARM"))
		_ = http.NewResponseController(w).Flush()
		<-r.Context().Done()
	}))
	tr := &http.Transport{}
	g.client = &http.Client{Transport: prewarmGateTransport{tr, g}}
	t.Cleanup(func() { g.unblock(); tr.CloseIdleConnections(); g.server.Close() })
	return g
}
func (g *prewarmHTTPGate) read(ctx context.Context) error {
	request, e := http.NewRequestWithContext(ctx, "GET", g.server.URL, nil)
	if e != nil {
		return e
	}
	response, e := g.client.Do(request)
	if e != nil {
		return e
	}
	defer response.Body.Close()
	_, e = io.ReadAll(response.Body)
	return e
}
func (g *prewarmHTTPGate) GetDirectURL(ctx context.Context, _ string) (string, error) {
	return "", g.read(ctx)
}
func prewarmLifeService(t *testing.T, g *prewarmHTTPGate, late bool) *Service {
	t.Helper()
	s := NewService(nil, "")
	s.SetPrequeueStore(playback.NewPrequeueStore(time.Hour))
	s.SetUsersService(&mockUsersProvider{users: []models.User{{ID: "synthetic-user", Name: "Synthetic user"}}})
	s.SetHistoryService(&mockHistoryProvider{continueWatching: map[string][]models.SeriesWatchState{"synthetic-user": {{SeriesID: "synthetic-title", SeriesTitle: "Synthetic movie", Year: 2026, UpdatedAt: time.Now().UTC()}}}})
	s.SetScopedWorkerFunc(func(ctx context.Context, title, titleName, imdb, media string, year int, user, client, scope string, episode *models.EpisodeReference) (string, error) {
		entry, e := s.prequeueStore.CreateScoped(title, titleName, user, media, year, episode, "prewarm", scope)
		if e != nil {
			return "", e
		}
		e = g.read(ctx)
		if late {
			s.prequeueStore.Update(entry.ID, func(p *playback.PrequeueEntry) {
				p.Status = playback.PrequeueStatusReady
				p.StreamPath = "synthetic:late"
				p.AudioTracks = []playback.AudioTrackInfo{{Index: 0}}
			})
			return entry.ID, nil
		}
		return entry.ID, e
	})
	return s
}
func prewarmAssertTimeoutPhysicalJoin(t *testing.T) {
	t.Helper()
	prewarmHeldWorker(t, "timeout-cancels-before-joining-held-worker")
}
func prewarmHeldWorker(t *testing.T, kind string) {
	g := newPrewarmHTTPGate(t)
	s := prewarmLifeService(t, g, kind == "late-ready-after-timeout-is-not-adopted" || kind == "retired-cycle-no-cache-publication")
	s.workerTimeout = 250 * time.Millisecond
	if kind == "caller-cancel-joins-real-worker" || kind == "retired-cycle-no-cache-publication" {
		s.workerTimeout = time.Hour
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var result SyncResult
	var err error
	t.Cleanup(func() { cancel(); g.unblock(); prewarmLifeWait(t, done); s.Stop() })
	go func() { defer close(done); result, err = s.RunOnce(ctx) }()
	prewarmLifePoll(t, func() bool { return g.reading.Load() == 1 })
	if kind == "caller-cancel-joins-real-worker" || kind == "retired-cycle-no-cache-publication" {
		cancel()
	}
	prewarmLifePoll(t, func() bool { return g.closing.Load() == 1 })
	prewarmLifePending(t, done)
	if entry, ok := s.prequeueStore.GetByTitleUserScope("synthetic-title", "synthetic-user", playback.DefaultPrequeueSettingsScopeKey); !ok || entry == nil {
		t.Fatal("entry deleted before actual body Close")
	}
	s.lifeMu.Lock()
	charged := len(s.active)
	s.lifeMu.Unlock()
	if charged != 1 {
		t.Fatal("cycle charge ended before actual worker", charged)
	}
	g.unblock()
	prewarmLifeWait(t, done)
	if _, ok := s.prequeueStore.GetByTitleUserScope("synthetic-title", "synthetic-user", playback.DefaultPrequeueSettingsScopeKey); ok {
		t.Fatal("cancelled completed worker retained reusable entry")
	}
	if s.GetWarm("synthetic-title", "synthetic-user") != nil {
		t.Fatal("cancelled late source was adopted")
	}
	if kind == "caller-cancel-joins-real-worker" || kind == "retired-cycle-no-cache-publication" {
		if !errors.Is(err, context.Canceled) {
			t.Fatal("caller cancellation lost", err)
		}
	} else {
		if err != nil || result.Failed != 1 || result.Warmed != 0 {
			t.Fatal("joined timeout did not retain failure semantics", result, err)
		}
		entry := s.entries[entryKey("synthetic-title", "synthetic-user")]
		if entry == nil || !strings.Contains(entry.Error, "timed out") {
			t.Fatal("actual joined timeout diagnosis missing")
		}
	}
}
func TestPrewarmActualWorkerPhysicalLifetime(t *testing.T) {
	for _, kind := range []string{"timeout-cancels-before-joining-held-worker", "no-delete-or-refund-before-worker-end", "late-ready-after-timeout-is-not-adopted", "caller-cancel-joins-real-worker", "stop-joins-refresh-and-run-once", "stop-before-start-is-idempotent", "stop-rejects-new-cycle-before-callback", "retired-cycle-no-cache-publication", "same-key-successor-waits-old-source"} {
		t.Run(kind, func(t *testing.T) {
			if kind == "stop-before-start-is-idempotent" || kind == "stop-rejects-new-cycle-before-callback" {
				g := newPrewarmHTTPGate(t)
				s := prewarmLifeService(t, g, false)
				s.Stop()
				s.Stop()
				s.Start(t.Context())
				if _, e := s.RunOnce(t.Context()); e == nil || g.calls.Load() != 0 {
					t.Fatal("stopped service admitted source callback")
				}
				s.lifeMu.Lock()
				active := len(s.active)
				s.lifeMu.Unlock()
				if active != 0 {
					t.Fatal("stop-before-start retained operations")
				}
				return
			}
			if kind == "stop-joins-refresh-and-run-once" {
				prewarmStopJoins(t)
				return
			}
			if kind == "same-key-successor-waits-old-source" {
				prewarmSameKeyJoins(t)
				return
			}
			prewarmHeldWorker(t, kind)
		})
	}
}
func prewarmStopJoins(t *testing.T) {
	g := newPrewarmHTTPGate(t)
	s := prewarmLifeService(t, g, false)
	s.workerTimeout = time.Hour
	s.SetDebridStreaming(g)
	s.entries["synthetic-refresh"] = &WarmEntry{StreamPath: "synthetic-debrid:object"}
	ctx, cancel := context.WithCancel(t.Context())
	runDone, refreshDone, stopDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	stopStarted := false
	t.Cleanup(func() {
		cancel()
		g.unblock()
		prewarmLifeWait(t, runDone)
		prewarmLifeWait(t, refreshDone)
		if stopStarted {
			prewarmLifeWait(t, stopDone)
		}
		s.Stop()
	})
	s.Start(ctx)
	go func() { defer close(runDone); _, _ = s.RunOnce(ctx) }()
	go func() { defer close(refreshDone); _ = s.RefreshURLs(ctx) }()
	prewarmLifePoll(t, func() bool { return g.reading.Load() == 2 })
	stopStarted = true
	go func() { defer close(stopDone); s.Stop() }()
	prewarmLifePoll(t, func() bool { return g.closing.Load() == 2 })
	prewarmLifePending(t, stopDone)
	g.unblock()
	prewarmLifeWait(t, runDone)
	prewarmLifeWait(t, refreshDone)
	prewarmLifeWait(t, stopDone)
	s.lifeMu.Lock()
	active := len(s.active)
	s.lifeMu.Unlock()
	if active != 0 {
		t.Fatal("actual Stop retained producer operations")
	}
	if !s.entries["synthetic-refresh"].LastRefresh.IsZero() {
		t.Fatal("cancelled refresh published a new timestamp")
	}
}
func prewarmSameKeyJoins(t *testing.T) {
	g := newPrewarmHTTPGate(t)
	s := prewarmLifeService(t, g, false)
	s.workerTimeout = time.Hour
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	secondCtx, cancelSecond := context.WithCancel(t.Context())
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	secondStarted := false
	t.Cleanup(func() {
		cancelFirst()
		cancelSecond()
		g.unblock()
		prewarmLifeWait(t, firstDone)
		if secondStarted {
			prewarmLifeWait(t, secondDone)
		}
		s.Stop()
	})
	go func() {
		defer close(firstDone)
		_, _ = s.runWorker(firstCtx, "synthetic-title", "Synthetic", "", "movie", 2026, "synthetic-user", "", playback.DefaultPrequeueSettingsScopeKey, nil)
	}()
	prewarmLifePoll(t, func() bool { return g.reading.Load() == 1 })
	cancelFirst()
	prewarmLifePoll(t, func() bool { return g.closing.Load() == 1 })
	secondStarted = true
	go func() {
		defer close(secondDone)
		_, _ = s.runWorker(secondCtx, "synthetic-title", "Synthetic", "", "movie", 2026, "synthetic-user", "", playback.DefaultPrequeueSettingsScopeKey, nil)
	}()
	prewarmLifePending(t, firstDone)
	prewarmLifePending(t, secondDone)
	if g.calls.Load() != 1 {
		t.Fatal("same-key successor opened before old physical source end")
	}
	cancelSecond()
	prewarmLifeWait(t, secondDone)
	g.unblock()
	prewarmLifeWait(t, firstDone)
}
