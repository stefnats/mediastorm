//go:build linux

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"novastream/services/profileunlock"
)

// These tests use genuine original PG admissions. They deliberately do not
// treat the legacy nil-request pipe port or a synthetic source path as a grant.
func TestOrdinaryHLSAuthorityRetention(t *testing.T) {
	t.Run("actual-original-owner-not-renewed", func(t *testing.T) {
		f := newPublicPinCarrierFixture(t)
		installed := familyActualPGInstall(t, f)
		r, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
		defer release()
		q, ok := installed.Boundary.Begin(httptest.NewRecorder(), r, "")
		if !ok {
			t.Fatal("genuine original admission denied")
		}
		defer q.Close()
		runtime, e := q.adoptRuntime(r)
		if e != nil {
			t.Fatal(e)
		}
		expiry := runtime.ExpiresAt()
		release()
		fresh, stop := f.request(t, "GET", "/?profileId="+f.profile.ID)
		defer stop()
		consumer, _, e := runtime.beginConsumer(httptest.NewRecorder(), fresh, f.profile.ID)
		if e != nil {
			t.Fatal(e)
		}
		consumer.Close()
		if !runtime.ExpiresAt().Equal(expiry) || runtime.Check(context.Background()) != nil {
			t.Fatal("fresh consumer changed actual original lifetime")
		}
	})
	t.Run("foreign-or-copied-session-no-worker", func(t *testing.T) {
		media := ordinaryLifeMedia(t)
		_, s, _ := ordinaryLifeManager(t, media, false)
		copyData := &HLSSession{ID: s.ID, ordinary: s.ordinary}
		if ordinaryGeneration(s.ordinary.current().context(), copyData) != nil || ordinaryHLSGo(s.ordinary.current().context(), copyData, func() { t.Error("copied session ran actor") }) {
			t.Fatal("session DATA adopted original worker")
		}
		foreign := &HLSSession{ID: s.ID}
		if ordinaryGeneration(s.ordinary.current().context(), foreign) != nil {
			t.Fatal("same identifier became owner")
		}
	})
	t.Run("shared-party-existing-owner-unchanged", func(t *testing.T) {
		f := newSharePlaybackHTTPFixture(t, "synthetic-owned.mkv", nil)
		ff, probe := ordinaryLifeTools(t)
		f.video.hlsManager = NewHLSManager(t.TempDir(), ff, probe, f.video.streamer)
		r := httptest.NewRequest("GET", "/api/video/stream?path=synthetic-owned.mkv&profileId="+f.profileID, nil)
		var captured bool
		f.serve(t, httptest.NewRecorder(), r, http.HandlerFunc(func(w http.ResponseWriter, current *http.Request) {
			g := shareMediaFrom(current.Context())
			if g == nil {
				t.Fatal("genuine bound share missing")
			}
			original, expiry := g.owner, g.owner.ExpiresAt()
			m := f.video.hlsManager
			ordinary := m.ordinaryControl()
			ctx, cancel, owner, e := m.prepareSharedHLS(current.Context(), f.resource, f.resource, f.profileID)
			if e != nil || owner == nil || ctx == nil {
				t.Fatal("actual share preparation failed", e)
			}
			if g.owner != original || !g.owner.ExpiresAt().Equal(expiry) {
				t.Fatal("ordinary lifetime replaced/renewed concrete share owner")
			}
			cancel()
			owner.finishSetup(false)
			ordinary.mu.Lock()
			charged := ordinary.active
			ordinary.mu.Unlock()
			if charged != 0 {
				t.Fatal("shared work entered ordinary lifetime registry")
			}
			captured = true
		}))
		if !captured {
			t.Fatal("actual shared entry did not reach original owner")
		}
	})
	t.Run("unknown-local-upstream-not-certified-by-process-exit", func(t *testing.T) {
		f := newPublicPinCarrierFixture(t)
		installed := familyActualPGInstall(t, f)
		ff, probe := ordinaryLifeTools(t)
		p := &rootHLSInputPoisonProvider{}
		h := NewVideoHandlerWithProvider(true, ff, probe, t.TempDir(), p)
		defer h.Shutdown()
		if e := h.installHLSInputBoundary(installed.Boundary.boundary); e != nil {
			t.Fatal(e)
		}
		r, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
		defer release()
		q, ok := installed.Boundary.Begin(httptest.NewRecorder(), r, "")
		if !ok {
			t.Fatal("actual PG owner unavailable")
		}
		defer q.Close()
		s, e := h.hlsManager.CreateSession(q.Context(), "usenet/synthetic-unknown.mkv", "usenet/synthetic-unknown.mkv", false, "", false, false, 0, 0, -1, -1, f.profile.ID, "Synthetic", "127.0.0.1", false, "", "native", 60, "")
		if e == nil || s != nil || p.streams.Load() != 0 || p.directs.Load() != 0 {
			t.Fatal("unretained local upstream was certified/opened by process lifetime")
		}
	})
	t.Run("legacy-permission-not-upgraded-by-lifetime-data", func(t *testing.T) {
		f := newPublicPinCarrierFixture(t)
		installed := familyActualPGInstall(t, f)
		r, release := f.request(t, "GET", "/?profileId="+f.profile.ID)
		defer release()
		q, ok := installed.Boundary.Begin(httptest.NewRecorder(), r, "")
		if !ok {
			t.Fatal("original admission denied")
		}
		defer q.Close()
		data, e := json.Marshal(q)
		if e != nil {
			t.Fatal(e)
		}
		var imported FamilyMediaRequest
		if e = json.Unmarshal(data, &imported); e == nil {
			t.Fatal("imported DATA minted genuine original")
		}
		if imported.Check(context.Background()) == nil || imported.Handoff(r) == nil {
			t.Fatal("opaque lifetime DATA became permission")
		}
		f.setPIN(t, "1357")
		if q.Check(context.Background()) == nil {
			t.Fatal("actual changed PIN authority survived lifecycle DATA")
		}
		if imported.ExpiresAt() != (time.Time{}) {
			t.Fatal("imported original exposes invented expiry")
		}
		if q.Check(context.Background()) == nil {
			t.Fatal(profileunlock.ErrFamilyMediaChanged)
		}
	})
}
