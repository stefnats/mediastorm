package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/models"
	"novastream/services/users"
)

type recordingSourcePlanFixture struct {
	h        *LiveHandler
	manager  *config.Manager
	settings config.Settings
	job      models.Recording
	server   *httptest.Server
	streams  atomic.Int32
}

func newRecordingSourcePlanFixture(t *testing.T, mutate func(*config.Settings, string)) *recordingSourcePlanFixture {
	t.Helper()
	f := &recordingSourcePlanFixture{}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".ts") {
			f.streams.Add(1)
			http.Error(w, "planner must not open media", 500)
			return
		}
		_, _ = fmt.Fprintln(w, "#EXTM3U")
		row := func(id, name, country, language, suffix string) {
			_, _ = fmt.Fprintf(w, "#EXTINF:-1 tvg-id=%q tvg-country=%q tvg-language=%q group-title=\"Entertainment\",%s\n%s/%s-%s.ts\n", id, country, language, name, f.server.URL, strings.TrimPrefix(r.URL.Path, "/"), suffix)
		}
		row("RTL.de", "RTL FHD", "DE", "de", "rtl")
		if r.URL.Path == "/b" {
			row("RTL.de", "RTL UHD", "DE", "de", "uhd")
			row("RTL.de", "RTL", "DE", "de", "unknown")
			row("RTL.de", "RTL FHD AT", "AT", "de", "at")
			row("RTLplus1.de", "RTL +1 HD", "DE", "de", "shift")
			row("RTL.de", "RTL FHD", "DE", "en", "english")
			row("RTLZWEI.de", "RTLZWEI HD", "DE", "de", "other")
		}
	}))
	t.Cleanup(f.server.Close)
	accounts := []string{"household"}
	f.settings = config.Settings{Live: config.LiveSettings{AutoChannelFailover: true, RequireAccountAssignment: true}}
	f.settings.Filtering.MaxResolution = "1080p"
	for _, id := range []string{"a", "b", "c", "d", "e", "hidden", "disabled", "wrong-category"} {
		source := config.LivePlaylistSource{ID: id, Mode: "m3u", PlaylistURL: f.server.URL + "/" + id, AllowedAccounts: &accounts, MaxStreams: 1}
		if id == "hidden" {
			source.AllowedProfiles = []string{"another"}
		}
		if id == "disabled" {
			no := false
			source.Enabled = &no
		}
		if id == "wrong-category" {
			source.Filtering.EnabledCategories = []string{"News"}
		}
		f.settings.Live.Sources = append(f.settings.Live.Sources, source)
	}
	if mutate != nil {
		mutate(&f.settings, f.server.URL)
	}
	f.manager = config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	f.h = NewLiveHandler(f.server.Client(), false, "", 24, 0, 0, false, f.manager, nil)
	f.h.usersSvc = liveAccountUsers{"adult": {ID: "adult", AccountID: "household"}, "kid": {ID: "kid", AccountID: "household", IsKidsProfile: true}, "foreign": {ID: "foreign", AccountID: "neighbour"}}
	channels, _, err := f.h.recordingCatalogue(t.Context(), "adult")
	if err != nil {
		t.Fatal(err)
	}
	for _, channel := range channels {
		if channel.SourceID == "a" && channel.Name == "RTL FHD" {
			f.job = models.Recording{ID: "synthetic-job", UserID: "adult", ChannelID: channel.ID, ChannelName: channel.Name, TvgID: channel.TvgID, SourceURL: channel.URL}
			break
		}
	}
	if f.job.ChannelID == "" {
		t.Fatal("synthetic primary was not catalogued")
	}
	return f
}

func TestRecordingSourcePlanKeepsOnlyEligibleSameStationAndBoundsAlternatives(t *testing.T) {
	f := newRecordingSourcePlanFixture(t, nil)
	plan, err := f.h.PlanRecordingSources(t.Context(), f.job)
	if err != nil || len(plan) != 4 || !plan[0].Original || plan[0].StationID == "" {
		t.Fatalf("bounded original plus alternatives: count=%d err=%v", len(plan), err)
	}
	for i, source := range plan {
		if source.StationID != plan[0].StationID || source.Name != "RTL FHD" || source.SourceID == "hidden" || source.SourceID == "disabled" || source.SourceID == "wrong-category" || source.Original != (i == 0) {
			t.Fatal("plan admitted a different broadcast, forbidden source or quality")
		}
	}
	if f.streams.Load() != 0 {
		t.Fatal("planning opened an input")
	}
	for _, profile := range []string{"kid", "foreign", "missing"} {
		job := f.job
		job.UserID = profile
		if _, err := f.h.PlanRecordingSources(t.Context(), job); err == nil {
			t.Fatalf("profile %s acquired recording plan", profile)
		}
	}
	// A tighter profile cap excludes all known-HD alternatives, including
	// unknown-resolution names; it does not rewrite the explicit primary.
	cap := "720p"
	f.h.userSettingsSvc = &mockUserSettingsProvider{settings: map[string]*models.UserSettings{"adult": {Filtering: models.FilterSettings{MaxResolution: &cap}}}}
	plan, err = f.h.PlanRecordingSources(t.Context(), f.job)
	if err != nil || len(plan) != 1 || !plan[0].Original {
		t.Fatal("profile cap did not restrict replacements")
	}
}

func TestRecordingSourcePlanSkipsProxyAlternativesAndKeepsExplicitPrimary(t *testing.T) {
	f := newRecordingSourcePlanFixture(t, func(settings *config.Settings, proxy string) {
		settings.Live.Sources[0].ProxyURL = proxy
		settings.Live.Sources[1].ProxyURL = proxy
	})
	plan, err := f.h.PlanRecordingSources(t.Context(), f.job)
	if err != nil || len(plan) != 4 || !plan[0].Original {
		t.Fatal("primary proxy compatibility or eligible cap failed")
	}
	for _, source := range plan[1:] {
		if source.SourceID == "b" {
			t.Fatal("proxy-only automatic replacement admitted")
		}
	}
	input, err := f.h.PrepareRecordingSource(t.Context(), f.job, plan[0])
	if err != nil || input.Lease == nil || input.Source.URL != f.job.SourceURL {
		t.Fatal("explicit primary lost ordinary recording admission")
	}
	input.Lease.Release()
	if f.streams.Load() != 0 {
		t.Fatal("admission opened media before worker")
	}
	// Legacy TVG resolution can authorize the primary, but never manufactures
	// station/source identity for automatic replacement.
	legacy := f.job
	legacy.ChannelID = "historical-entry"
	plan, err = f.h.PlanRecordingSources(t.Context(), legacy)
	if err != nil || len(plan) != 1 || !plan[0].Original || plan[0].StationID != "" || plan[0].SourceID != "" {
		t.Fatal("legacy TVG primary became automatic source proof")
	}
}

func TestRecordingSourcePreparationRevalidatesRightsIdentityAndCapacity(t *testing.T) {
	for _, mode := range []string{"revoked", "disabled", "mapping", "quality", "failover-disabled", "forged", "cancelled", "capacity"} {
		t.Run(mode, func(t *testing.T) {
			f := newRecordingSourcePlanFixture(t, nil)
			plan, err := f.h.PlanRecordingSources(t.Context(), f.job)
			if err != nil || len(plan) < 2 {
				t.Fatal("missing synthetic replacement")
			}
			candidate := plan[1]
			ctx := t.Context()
			for i := range f.settings.Live.Sources {
				source := &f.settings.Live.Sources[i]
				if source.ID != candidate.SourceID {
					continue
				}
				switch mode {
				case "revoked":
					source.AllowedProfiles = []string{"another"}
				case "disabled":
					no := false
					source.Enabled = &no
				case "mapping":
					f.settings.Live.StationMappings = []config.LiveStationMapping{{SourceID: source.ID, ChannelID: candidate.ChannelID, StationID: "different-confirmed-programme"}}
				}
			}
			switch mode {
			case "quality":
				f.settings.Filtering.MaxResolution = "720p"
			case "failover-disabled":
				f.settings.Live.AutoChannelFailover = false
			case "forged":
				candidate.StationID = "station:forged"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if err := f.manager.Save(f.settings); err != nil {
				t.Fatal(err)
			}
			var occupied *livecapacity.Lease
			if mode == "capacity" {
				admission, err := f.h.PrepareRecordingSource(ctx, f.job, candidate)
				if err != nil || admission.Lease == nil {
					t.Fatal("initial capacity admission failed")
				}
				occupied = admission.Lease
				defer occupied.Release()
			}
			input, err := f.h.PrepareRecordingSource(ctx, f.job, candidate)
			if input.Lease != nil {
				input.Lease.Release()
			}
			if err == nil {
				t.Fatalf("stale/forged replacement admitted after %s", mode)
			}
			if mode == "capacity" && !errors.Is(err, livecapacity.ErrLimit) {
				t.Fatal("capacity conflict lost its reason")
			}
			if f.streams.Load() != 0 {
				t.Fatal("planning/admission opened media")
			}
		})
	}
}

func TestRecordingSourceOriginalLosesAutomaticProofAfterStationRemapping(t *testing.T) {
	f := newRecordingSourcePlanFixture(t, nil)
	plan, err := f.h.PlanRecordingSources(t.Context(), f.job)
	if err != nil || len(plan) < 2 {
		t.Fatal("missing strict primary plan")
	}
	f.settings.Live.StationMappings = []config.LiveStationMapping{{SourceID: plan[0].SourceID, ChannelID: plan[0].ChannelID, StationID: "replacement-programme"}}
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	input, err := f.h.PrepareRecordingSource(t.Context(), f.job, plan[0])
	if input.Lease != nil {
		defer input.Lease.Release()
	}
	if err != nil || !input.Source.Original || input.Source.StationID != "" || input.Source.SourceID != "" || input.Source.URL == "" {
		t.Fatal("legacy primary compatibility retained stale automatic station proof")
	}
}

func TestRecordingSourceBackgroundOwnerUsesFreshRightsWithoutInteractivePIN(t *testing.T) {
	f := newRecordingSourcePlanFixture(t, nil)
	profiles, err := users.NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := profiles.CreateForAccount("household", "Synthetic scheduled owner")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profiles.SetPin(profile.ID, "1234"); err != nil {
		t.Fatal(err)
	}
	if !profiles.HasPin(profile.ID) {
		t.Fatal("synthetic owner must have a PIN")
	}
	f.h.usersSvc = profiles
	f.job.UserID = profile.ID
	ctx := context.WithValue(t.Context(), auth.ContextKeyAccountID, "wrong-interactive-account")
	plan, err := f.h.PlanRecordingSources(ctx, f.job)
	if err != nil || len(plan) != 4 {
		t.Fatal("scheduled owner incorrectly required an interactive PIN/session")
	}
	f.settings.Live.AutoChannelFailover = false
	if err := f.manager.Save(f.settings); err != nil {
		t.Fatal(err)
	}
	plan, err = f.h.PlanRecordingSources(ctx, f.job)
	if err != nil || len(plan) != 1 || !plan[0].Original {
		t.Fatal("disabled automatic replacement changed primary recording")
	}
}
