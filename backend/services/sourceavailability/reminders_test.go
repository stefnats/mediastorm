package sourceavailability

import (
	"context"
	"novastream/models"
	"testing"
	"time"
)

func TestReminderEvidenceRejectsForeignAndOldScopeAliasesBeforeAmbiguity(t *testing.T) {
	for _, mode := range []string{"foreign-local", "old-language-local", "old-language-remote"} {
		t.Run(mode, func(t *testing.T) {
			s, search, _, profiles := reportFixture(t)
			title, ep := episodeTitle(), episode(1)
			cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
			other := ep
			other.ID = "foreign-alias"
			resolve, _ := s.scope("profile", "tv")
			base, _ := resolve(title.ID)
			if mode == "old-language-remote" {
				profiles["profile"].Playback.PreferredAudioLanguage = "spa"
				cacheEpisode(t, s, "profile", "tv", title, other)
				profiles["profile"].Playback.PreferredAudioLanguage = "fra"
			} else {
				profile := "another-profile"
				selected := base
				if mode == "old-language-local" {
					profile = "profile"
					profiles["profile"].Playback.PreferredAudioLanguage = "spa"
					resolve, _ = s.scope(profile, "tv")
					selected, _ = resolve(title.ID)
					profiles["profile"].Playback.PreferredAudioLanguage = "fra"
				}
				selected = episodeSelection(selected, &other, title)
				item := request{ProfileID: profile, ClientID: "tv", Title: title, Episode: &other, Scope: selected.Key}
				s.localRecords = map[string]localObservation{requestID(item): {Episode: projectEpisode(item), State: "found", Expires: s.now().Add(time.Minute)}}
			}
			value := s.CurrentReminderEvidence(t.Context(), "profile", "tv", title, &ep)
			if value.State != "found" || search.calls != 0 {
				t.Fatalf("unrelated projection poisoned current evidence: %+v", value)
			}
		})
	}
}
func TestReminderEvidenceKeepsSameScopeLocalMetadataConflictsUnknown(t *testing.T) {
	s, search, _, _ := reportFixture(t)
	title, ep := episodeTitle(), episode(1)
	cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
	changed := ep
	changed.AiredDate = "2020-01-02"
	resolve, _ := s.scope("profile", "tv")
	selected, _ := resolve(title.ID)
	selected = episodeSelection(selected, &changed, title)
	item := request{ProfileID: "profile", ClientID: "tv", Title: title, Episode: &changed, Scope: selected.Key}
	s.localRecords = map[string]localObservation{requestID(item): {Episode: projectEpisode(item), State: "found", Expires: s.now().Add(time.Minute)}}
	value := s.CurrentReminderEvidence(t.Context(), "profile", "tv", title, &ep)
	if value.State != "unknown" || value.Reason != "episode_identity_unverified" || search.calls != 0 {
		t.Fatalf("metadata versions silently merged: %+v", value)
	}
}
func TestReminderPublishUsesValidatedCachedCandidatesWithoutNewDemand(t *testing.T) {
	s, search, reports, _ := reportFixture(t)
	title, ep := episodeTitle(), episode(1)
	cacheEpisode(t, s, "profile", "tv", title, ep, "Synthetic.Series.S01E01.1080p")
	resolve, _ := s.scope("profile", "tv")
	selected, _ := resolve(title.ID)
	selected = episodeSelection(selected, &ep, title)
	item := request{ProfileID: "profile", ClientID: "tv", Title: title, Episode: &ep, Scope: selected.Key, AudioLanguage: selected.AudioLanguage}
	calls := 0
	s.SetReminderObserver(func(_ context.Context, profile, client string, got models.Title, episode *models.SeriesEpisode, value models.SourceAvailability) error {
		calls++
		if profile != "profile" || client != "tv" || got.ID != title.ID || episode.ID != ep.ID || value.State != "found" {
			t.Fatal("wrong reminder observation")
		}
		return nil
	})
	pending := len(s.pending)
	s.publishReminder(t.Context(), item)
	if calls != 1 || search.calls != 0 || len(s.pending) != pending {
		t.Fatal("publish created source demand")
	}
	_ = reports
	s.Store.now = func() time.Time { return s.now().Add(48 * time.Hour) }
	s.publishReminder(t.Context(), item)
	if calls != 1 {
		t.Fatal("expired evidence published")
	}
}
func TestReminderPreparedOnlyProofCannotAssertDesiredLanguage(t *testing.T) {
	s, search, _, _ := reportFixture(t)
	title := episodeTitle()
	title.MediaType = "movie"
	title.ID = "tmdb:movie:7"
	title.Name = "Synthetic Movie"
	resolve, _ := s.scope("profile", "tv")
	selected, _ := resolve(title.ID)
	now, expires := s.now(), s.now().Add(time.Hour)
	revision := s.reports.RevisionFor(s.reportContext("profile", "tv", title.ID, selected))
	if err := s.Store.putObservation("profile", title.ID, selected.Key, models.SourceAvailability{State: "found", CheckedAt: &now, ExpiresAt: &expires, Reason: "prepared_source"}, &observation{Version: 1, Kind: "prepared", PreparedRemote: true, ReportRevision: revision}); err != nil {
		t.Fatal(err)
	}
	value := s.CurrentReminderEvidence(t.Context(), "profile", "tv", title, nil)
	if value.State != "unknown" || value.Reason != "candidate_intent_unverified" || search.calls != 0 {
		t.Fatalf("prepared stream became preferred-language proof: %+v", value)
	}
}

func TestReminderLocalCandidateRequiresCurrentValidationWithoutAnotherCheck(t *testing.T) {
	s, search, _, profiles := reportFixture(t)
	checker := &fakeLocalHints{proof: LocalProof{ItemID: "private-fixture-item", Fingerprint: "private-proof"}, valid: true}
	s.SetLocalChecker(checker)
	title, ep := episodeTitle(), episode(1)
	view(t, s, "tv", "fixture-view", 1, ep)
	s.checkOne(t.Context())
	checks := checker.checks
	value := s.CurrentReminderEvidence(t.Context(), "profile", "tv", title, &ep)
	if value.State != "found" || value.SourceType != "local" || checker.checks != checks || search.calls != 0 {
		t.Fatalf("current cached local proof was not reused safely: %+v", value)
	}
	checker.valid = false
	value = s.CurrentReminderEvidence(t.Context(), "profile", "tv", title, &ep)
	if value.State == "found" || checker.checks != checks {
		t.Fatal("revoked local proof persisted or a new probe started")
	}
	checker.valid = true
	profiles["profile"].Playback.PreferredAudioLanguage = "spa"
	value = s.CurrentReminderEvidence(t.Context(), "profile", "tv", title, &ep)
	if value.State == "found" || checker.checks != checks {
		t.Fatal("changed language revived a stale proof")
	}
}
