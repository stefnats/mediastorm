package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/models"
)

type rootPlaybackVODApplicability struct {
	mu       sync.RWMutex
	mode     vodApplicabilityMode
	revision string
	err      error
}

func (s *rootPlaybackVODApplicability) CurrentVODApplicability(context.Context, string) (vodAccountApplicability, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return vodAccountApplicability{mode: s.mode, revision: s.revision}, s.err
}
func (s *rootPlaybackVODApplicability) replace(mode vodApplicabilityMode, revision string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mode = mode
	s.revision = revision
}

type rootPlaybackVODProbe struct{ calls atomic.Int32 }

func (p *rootPlaybackVODProbe) ProbeVideoFull(context.Context, string) (*VideoFullResult, error) {
	p.calls.Add(1)
	return nil, errors.New("synthetic optional probe unavailable")
}

type rootPlaybackVODPrewarm struct{ calls atomic.Int32 }

func (p *rootPlaybackVODPrewarm) PrewarmThumbnails(string) { p.calls.Add(1) }

type rootPlaybackVODWarmAction func(string)

func (f rootPlaybackVODWarmAction) PrewarmThumbnails(path string) { f(path) }

type rootPlaybackVODCalls struct {
	resolve, batch, queue atomic.Int32
	profile               atomic.Value
}

func (c *rootPlaybackVODCalls) noDemand(t *testing.T, p *rootPlaybackVODProbe, w *rootPlaybackVODPrewarm) {
	t.Helper()
	if c.resolve.Load() != 0 || c.batch.Load() != 0 || c.queue.Load() != 0 || p.calls.Load() != 0 || w.calls.Load() != 0 {
		t.Fatal("denied request started resolution, queue lookup, probing or prewarming")
	}
}
func rootPlaybackVODHandler(t *testing.T, mode vodApplicabilityMode) (*rootKidsVODIdentityFixture, *PlaybackHandler, *rootPlaybackVODApplicability, *rootPlaybackVODCalls, *rootPlaybackVODProbe, *rootPlaybackVODPrewarm) {
	t.Helper()
	f := newRootKidsVODIdentityFixture()
	f.profile.IsKidsProfile = false
	state := &rootPlaybackVODApplicability{mode: mode, revision: "original-policy"}
	calls := &rootPlaybackVODCalls{}
	calls.profile.Store("")
	p, w := &rootPlaybackVODProbe{}, &rootPlaybackVODPrewarm{}
	h := NewPlaybackHandler(&mockPlaybackService{
		resolveFunc: func(_ context.Context, result models.NZBResult) (*models.PlaybackResolution, error) {
			calls.resolve.Add(1)
			calls.profile.Store(result.Attributes["profileId"])
			return &models.PlaybackResolution{WebDAVPath: "/synthetic/vod-boundary.mkv"}, nil
		},
		resolveBatchFunc: func(_ context.Context, result models.NZBResult, _ []models.BatchEpisodeTarget) (*models.BatchResolveResponse, error) {
			calls.batch.Add(1)
			calls.profile.Store(result.Attributes["profileId"])
			return &models.BatchResolveResponse{Results: []models.BatchEpisodeResult{{SeasonNumber: 1, EpisodeNumber: 2, Resolution: &models.PlaybackResolution{WebDAVPath: "/synthetic/vod-boundary-episode.mkv"}}}}, nil
		},
		queueStatusFunc: func(context.Context, int64) (*models.PlaybackResolution, error) {
			calls.queue.Add(1)
			return &models.PlaybackResolution{WebDAVPath: "/synthetic/vod-boundary-queue.mkv"}, nil
		},
	})
	h.vodBoundary = newVODBoundary(f.identity, state, func(http.ResponseWriter, *http.Request, string) bool { return true })
	h.VideoProber = p
	h.ThumbnailPrewarmer = w
	return f, h, state, calls, p, w
}
func rootPlaybackVODBody() map[string]any {
	return map[string]any{
		"result":    models.NZBResult{Title: "Synthetic release"},
		"profileId": "profile-a", "includeTrackSelection": true,
		"episodes": []models.BatchEpisodeTarget{{SeasonNumber: 1, EpisodeNumber: 2}},
	}
}
func rootPlaybackVODRequest(t *testing.T, f *rootKidsVODIdentityFixture, body map[string]any) *http.Request {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/playback/resolve", bytes.NewReader(encoded)).WithContext(f.request().Context())
	r.Header.Set("X-Client-ID", "client-a")
	return r
}

func TestPlaybackVODBoundaryRejectsUnknownOwnersBeforeAnyDemand(t *testing.T) {
	cases := map[string]func(*rootKidsVODIdentityFixture, *PlaybackHandler, *rootPlaybackVODApplicability, map[string]any, *http.Request){
		"missing selector": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			delete(b, "profileId")
		},
		"result attributes cannot select owner": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			delete(b, "profileId")
			b["result"] = models.NZBResult{Title: "Synthetic release", Attributes: map[string]string{"profileId": "profile-a"}}
		},
		"child cannot claim adult fields": func(f *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			f.profile.IsKidsProfile = true
			b["isKidsProfile"] = false
			b["isMaster"] = true
		},
		"child cannot claim live exemption": func(f *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			f.profile.IsKidsProfile = true
			b["mediaType"] = "live"
			b["result"] = models.NZBResult{Title: "Synthetic release", Attributes: map[string]string{"mediaType": "live"}}
		},
		"unknown applicability": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, s *rootPlaybackVODApplicability, _ map[string]any, _ *http.Request) {
			s.replace(vodApplicabilityUnknown, "unknown")
		},
		"failed applicability": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, s *rootPlaybackVODApplicability, _ map[string]any, _ *http.Request) {
			s.err = errors.New("synthetic unavailable policy")
		},
		"child cold source": func(f *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, _ *http.Request) {
			f.profile.IsKidsProfile = true
		},
		"child caller claims adult live": func(f *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			f.profile.IsKidsProfile = true
			b["isKidsProfile"] = false
			b["isMaster"] = true
			b["mediaType"] = "live"
		},
		"foreign profile": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			b["profileId"] = "profile-b"
		},
		"query conflict": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, r *http.Request) {
			r.URL.RawQuery = "profileId=profile-b"
		},
		"body legacy conflict": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			b["userId"] = "profile-b"
		},
		"body case conflict": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			b["PROFILEID"] = "profile-b"
		},
		"path conflict": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, r *http.Request) {
			*r = *mux.SetURLVars(r, map[string]string{"userID": "profile-b"})
		},
		"result owner conflict": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, b map[string]any, _ *http.Request) {
			b["result"] = models.NZBResult{Title: "Synthetic release", Attributes: map[string]string{"profileId": "profile-b"}}
		},
		"wrong client": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, r *http.Request) {
			r.Header.Set("X-Client-ID", "client-b")
		},
		"missing PIN checker": func(_ *rootKidsVODIdentityFixture, h *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, _ *http.Request) {
			h.vodBoundary.pin = nil
		},
		"expired current session": func(f *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, _ *http.Request) {
			f.session.ExpiresAt = f.now
		},
		"spoofed cached master": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
		},
		"revoked session": func(f *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, _ *http.Request) {
			f.session = nil
		},
		"cached master spoof": func(_ *rootKidsVODIdentityFixture, _ *PlaybackHandler, _ *rootPlaybackVODApplicability, _ map[string]any, r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
		},
	}
	for name, mutate := range cases {
		for _, batch := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/resolve", true: "/batch"}[batch], func(t *testing.T) {
				f, h, state, calls, probe, warm := rootPlaybackVODHandler(t, vodAccountActive)
				body := rootPlaybackVODBody()
				r := rootPlaybackVODRequest(t, f, body)
				mutate(f, h, state, body, r)
				encoded, err := json.Marshal(body)
				if err != nil {
					t.Fatal(err)
				}
				r.Body = io.NopCloser(bytes.NewReader(encoded))
				r.ContentLength = int64(len(encoded))
				w := httptest.NewRecorder()
				if batch {
					h.ResolveBatch(w, r)
				} else {
					h.Resolve(w, r)
				}
				if w.Code != http.StatusForbidden {
					t.Fatalf("status=%d, want forbidden: %s", w.Code, w.Body.String())
				}
				calls.noDemand(t, probe, warm)
			})
		}
	}
}

func TestPlaybackVODBoundaryNeverPromotesScopedSessionsOrOpaqueQueues(t *testing.T) {
	for _, scope := range []string{models.SessionScopeStream, models.SessionScopeWatchParty, models.SessionScopeJellyfin} {
		t.Run(scope, func(t *testing.T) {
			f, h, _, calls, probe, warm := rootPlaybackVODHandler(t, vodAccountActive)
			f.session.Scope = scope
			r := rootPlaybackVODRequest(t, f, rootPlaybackVODBody())
			w := httptest.NewRecorder()
			h.Resolve(w, r)
			if w.Code != http.StatusForbidden {
				t.Fatal("scoped session became a full actor")
			}
			calls.noDemand(t, probe, warm)
		})
	}
	t.Run("queue ID and caller profile are not an owner receipt", func(t *testing.T) {
		f, h, _, calls, probe, warm := rootPlaybackVODHandler(t, vodAccountActive)
		r := rootPlaybackVODRequest(t, f, rootPlaybackVODBody())
		r.Method = http.MethodGet
		r.URL.RawQuery = "profileId=profile-a"
		r = mux.SetURLVars(r, map[string]string{"queueID": "41"})
		w := httptest.NewRecorder()
		h.QueueStatus(w, r)
		if w.Code != http.StatusForbidden {
			t.Fatal("unowned runtime queue was queried")
		}
		calls.noDemand(t, probe, warm)
	})
}

func TestPlaybackVODBoundaryRetainsNilAndExplicitInactiveLegacyPaths(t *testing.T) {
	for _, mode := range []string{"nil", "explicit inactive"} {
		for _, operation := range []string{"resolve", "batch", "queue"} {
			t.Run(mode+"/"+operation, func(t *testing.T) {
				f, h, _, calls, probe, warm := rootPlaybackVODHandler(t, vodExplicitInactive)
				if mode == "nil" {
					h.vodBoundary = nil
				}
				r := rootPlaybackVODRequest(t, f, rootPlaybackVODBody())
				w := httptest.NewRecorder()
				switch operation {
				case "resolve":
					h.Resolve(w, r)
				case "batch":
					h.ResolveBatch(w, r)
				case "queue":
					r.Method = http.MethodGet
					r = mux.SetURLVars(r, map[string]string{"queueID": "41"})
					h.QueueStatus(w, r)
				}
				if w.Code != http.StatusOK {
					t.Fatalf("legacy status=%d: %s", w.Code, w.Body.String())
				}
				if calls.resolve.Load()+calls.batch.Load()+calls.queue.Load() != 1 {
					t.Fatal("legacy demand path was not exercised exactly once")
				}
				if operation == "resolve" && (probe.calls.Load() != 1 || warm.calls.Load() != 1) {
					t.Fatal("control resolve did not exercise optional probe and prewarm")
				}
			})
		}
	}
}

func TestPlaybackVODBoundaryPassesCanonicalCheckedOwnerIntoActualDemand(t *testing.T) {
	for _, selector := range []string{"typed body", "query", "legacy user body", "case body"} {
		for _, batch := range []bool{false, true} {
			t.Run(selector+map[bool]string{false: "/resolve", true: "/batch"}[batch], func(t *testing.T) {
				f, h, _, calls, _, _ := rootPlaybackVODHandler(t, vodAccountActive)
				body := rootPlaybackVODBody()
				delete(body, "profileId")
				switch selector {
				case "typed body":
					body["profileId"] = "profile-a"
				case "legacy user body":
					body["userId"] = "profile-a"
				case "case body":
					body["PROFILEID"] = "profile-a"
				}
				r := rootPlaybackVODRequest(t, f, body)
				if selector == "query" {
					r.URL.RawQuery = "profileId=profile-a"
				}
				w := httptest.NewRecorder()
				if batch {
					h.ResolveBatch(w, r)
				} else {
					h.Resolve(w, r)
				}
				if w.Code != http.StatusOK {
					t.Fatalf("checked adult request rejected: %d %s", w.Code, w.Body.String())
				}
				if calls.profile.Load().(string) != "profile-a" {
					t.Fatal("downstream demand used a default profile instead of the checked owner")
				}
			})
		}
	}
}

func TestPlaybackVODBoundaryWithholdsLateSourceWhenPolicyChangesDuringResolution(t *testing.T) {
	f, h, state, calls, probe, warm := rootPlaybackVODHandler(t, vodExplicitInactive)
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	h.Service = &mockPlaybackService{resolveFunc: func(context.Context, models.NZBResult) (*models.PlaybackResolution, error) {
		calls.resolve.Add(1)
		close(started)
		<-release
		return &models.PlaybackResolution{WebDAVPath: "/synthetic/late-private.mkv"}, nil
	}}
	r := rootPlaybackVODRequest(t, f, rootPlaybackVODBody())
	w := httptest.NewRecorder()
	go func() { defer close(done); h.Resolve(w, r) }()
	select {
	case <-started:
	case <-done:
		t.Fatalf("inactive control returned before resolving: %d %s", w.Code, w.Body.String())
	case <-time.After(5 * time.Second):
		t.Fatal("inactive resolution did not reach the controlled provider")
	}
	state.replace(vodAccountActive, "replacement-policy")
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("resolution did not retire after the provider returned")
	}
	if w.Code != http.StatusForbidden || bytes.Contains(w.Body.Bytes(), []byte("late-private.mkv")) {
		t.Fatal("late source escaped its original inactive-policy context")
	}
	if calls.resolve.Load() != 1 || probe.calls.Load() != 0 || warm.calls.Load() != 0 {
		t.Fatal("late resolution was probed or prewarmed after its context changed")
	}
}

func TestPlaybackVODBoundaryRechecksPINBeforeStartingTheNextDemandStage(t *testing.T) {
	f, h, _, calls, probe, warm := rootPlaybackVODHandler(t, vodAccountActive)
	h.ThumbnailPrewarmer = rootPlaybackVODWarmAction(func(string) {
		warm.calls.Add(1)
		f.profile.PinHash = "synthetic-replaced-during-prewarm"
	})
	r := rootPlaybackVODRequest(t, f, rootPlaybackVODBody())
	w := httptest.NewRecorder()
	h.Resolve(w, r)
	if w.Code != http.StatusForbidden || bytes.Contains(w.Body.Bytes(), []byte("vod-boundary.mkv")) {
		t.Fatal("source escaped a PIN change between demand stages")
	}
	if calls.resolve.Load() != 1 || warm.calls.Load() != 1 || probe.calls.Load() != 0 {
		t.Fatal("a new probe started after the earlier stage changed PIN authority")
	}
}
