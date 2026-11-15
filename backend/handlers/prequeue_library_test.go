package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/content_preferences"
	"novastream/services/debrid"
	"novastream/services/history"
	"novastream/services/playback"
	"novastream/services/trackselect"
)

type automaticRemoteCatalog struct {
	items   []models.RemoteMediaItem
	missing bool
	reads   atomic.Int32
}

func (c *automaticRemoteCatalog) FindPlaybackCandidates(_ context.Context, _ models.LocalMediaMatchQuery, _ *models.EpisodeReference, allow func(string) bool) ([]models.RemoteMediaItem, error) {
	if c.missing {
		return nil, nil
	}
	var out []models.RemoteMediaItem
	for _, item := range c.items {
		if allow(item.LibraryID) {
			out = append(out, item)
		}
	}
	return out, nil
}
func (c *automaticRemoteCatalog) GetFallbackItem(_ context.Context, id string) (*models.RemoteMediaItem, error) {
	c.reads.Add(1)
	if c.missing {
		return nil, errors.New("synthetic missing item")
	}
	for _, item := range c.items {
		if item.ID == id {
			copy := item
			return &copy, nil
		}
	}
	return nil, errors.New("synthetic missing item")
}
func newAutomaticLibraryWorker(t *testing.T, provider string, search *fallbackSearch) (*PrequeueHandler, *automaticRemoteCatalog, *fallbackAccess) {
	t.Helper()
	h, local, access := newFallbackWorker(t, search)
	local.empty = true
	cfg, err := h.configManager.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Playback.PreferLocalFiles = true
	cfg.Playback.PreferredAudioLanguage = "fra"
	if err := h.configManager.Save(cfg); err != nil {
		t.Fatal(err)
	}
	item := models.RemoteMediaItem{ID: "version-1", LibraryID: "remote-library", LibraryType: models.LocalMediaLibraryTypeMovie, ExternalItemID: "movie", ExternalMediaID: "part-1", GroupKey: "group", ExternalIDs: &models.LocalMediaExternalIDs{TMDB: "123"}, SinglePart: true, FileName: "Synthetic.Movie.1080p.mkv", SizeBytes: 100, StreamPath: provider + "media:version-1", ProviderData: map[string]string{"partKey": "/part/1", "itemId": "movie", "mediaSourceId": "part-1"}}
	catalog := &automaticRemoteCatalog{items: []models.RemoteMediaItem{item}}
	h.SetRemoteFallback(catalog)
	h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
		return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1920, VideoHeight: 1080, Duration: 600, AudioStreams: []models.AudioStreamInfo{{Index: 1, Language: "eng", Codec: "aac"}, {Index: 4, Language: "fra", Codec: "aac"}}}, nil
	}}
	return h, catalog, access
}
func runAutomaticLibraryWorker(t *testing.T, h *PrequeueHandler) *playback.PrequeueEntry {
	t.Helper()
	scope := automaticLibraryScope(h.prequeueSettingsScopeKey("profile", "", "tmdb:movie:123"), true)
	entry, _ := h.store.CreateScoped("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "manual", scope)
	h.store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) { e.AutomaticLibrarySelection = true })
	h.runPrequeueWorker(entry.ID, entry.TitleID, entry.TitleName, "", "movie", 2024, "profile", "", nil, nil, 0, true, prequeueWorkerInteractive, prequeueLocalScope{accountID: "account", automaticLibrarySelection: true})
	got, _ := h.store.Get(entry.ID)
	return got
}

func TestAutomaticLibrarySelectionSharesPreparationAndAudio(t *testing.T) {
	for _, provider := range []string{"plex", "jellyfin"} {
		for _, mode := range []string{"library first", "search first", "missing language", "unknown language", "denied", "missing file", "quality rejected", "local legacy"} {
			t.Run(provider+"/"+mode, func(t *testing.T) {
				search := &fallbackSearch{results: []models.NZBResult{{Title: "Synthetic.Movie.1080p.WEB-DL", Link: "https://example.invalid/synthetic", ServiceType: models.ServiceTypeDebrid, Indexer: "AIOStreams", Attributes: map[string]string{"resolution": "1080p"}}}}
				h, catalog, access := newAutomaticLibraryWorker(t, provider, search)
				cfg, _ := h.configManager.Load()
				if mode == "search first" {
					cfg.Playback.PreferLocalFiles = false
				}
				if mode == "quality rejected" {
					cfg.Filtering.MaxResolution = "720p"
				}
				if err := h.configManager.Save(cfg); err != nil {
					t.Fatal(err)
				}
				access.deny.Store(mode == "denied")
				catalog.missing = mode == "missing file"
				if mode == "missing language" || mode == "unknown language" {
					h.fullProber = fallbackFullProber{run: func(_ context.Context, path string) (*VideoFullResult, error) {
						language := "eng"
						if mode == "unknown language" {
							language = "und"
						}
						return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1920, VideoHeight: 1080, Duration: 600, AudioStreams: []models.AudioStreamInfo{{Index: 1, Language: language, Codec: "aac"}}}, nil
					}}
				}
				var got *playback.PrequeueEntry
				if mode == "local legacy" {
					got = runFallbackWorker(t, h)
				} else {
					got = runAutomaticLibraryWorker(t, h)
				}
				want := provider + "media:version-1"
				if mode != "library first" {
					want = "/webdav/title.mkv"
				}
				if got.Status != playback.PrequeueStatusReady || got.StreamPath != want {
					t.Fatalf("status=%s path=%s error=%s", got.Status, got.StreamPath, got.Error)
				}
				if mode == "library first" {
					if search.calls.Load() != 0 || got.LibraryItemID != "version-1" || got.LibrarySourceType != provider || got.SelectedAudioTrack != 4 || got.TrackSelection == nil || got.TrackSelection.Audio.StreamIndex != 4 || len(got.MigrationCandidates) != 0 {
						t.Fatalf("library bypassed shared preparation: %+v searches=%d", got, search.calls.Load())
					}
				}
			})
		}
	}
}

func TestAutomaticLibraryActualAudioDrivesForeignSubtitles(t *testing.T) {
	streams := []models.AudioStreamInfo{{Index: 1, Language: "eng", Codec: "aac"}, {Index: 4, Language: "deu", Codec: "aac"}}
	subtitles := []models.SubtitleStreamInfo{{Index: 7, Language: "deu", Codec: "subrip"}, {Index: 8, Language: "deu", Codec: "subrip", IsForced: true}}
	prefs := trackselect.Preferences{AllowedLanguages: []string{"deu", "eng"}, SubtitleLanguage: "deu", SubtitleMode: "foreign-audio"}
	selected := selectLibraryTracks(streams, subtitles, prefs)
	if selected == nil || selected.Audio.StreamIndex != 4 || selected.SubtitleMode != "forced-only" {
		t.Fatalf("allowed priority/subtitle mismatch: %+v", selected)
	}
	prefs.AllowedLanguages = nil
	selected = selectLibraryTracks(streams, subtitles, prefs)
	if selected.Audio != nil || selected.AudioStreamIndex() != -1 || selected.SubtitleMode != "on" {
		t.Fatalf("unconstrained playback overrode the container/player default: %+v", selected)
	}
	prefs.AudioLanguage = "fra"
	if selectLibraryTracks(streams, subtitles, prefs) != nil {
		t.Fatal("missing requested audio silently fell back")
	}
	if strictLibraryAudio([]models.AudioStreamInfo{{Index: 1, Language: "fra", Codec: "aac", Title: "Commentary"}}, prefs) != nil {
		t.Fatal("commentary satisfied main audio preference")
	}
}

func TestAutomaticLibraryProbeRechecksPartRightsAndStatusWithoutProbe(t *testing.T) {
	for _, mode := range []string{"part changed", "rights revoked", "profile becomes child", "valid status"} {
		t.Run(mode, func(t *testing.T) {
			h, catalog, access := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
			item := catalog.items[0]
			var probes atomic.Int32
			h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
				probes.Add(1)
				if mode == "part changed" {
					catalog.items[0].ExternalMediaID = "another-part"
				}
				if mode == "rights revoked" {
					access.deny.Store(true)
				}
				if mode == "profile becomes child" {
					h.users = fallbackProfiles{models.User{ID: "profile", AccountID: "account", IsKidsProfile: true}}
				}
				return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1920, VideoHeight: 1080}, nil
			}}
			_, err := h.resolveRemoteItem(t.Context(), item, prequeueResolutionOptions{mediaType: "movie", userID: "profile", localScope: &prequeueLocalScope{accountID: "account", automaticLibrarySelection: true}})
			if (err == nil) != (mode == "valid status") {
				t.Fatalf("validation error=%v", err)
			}
			entry := &playback.PrequeueEntry{ID: "active", AutomaticLibrarySelection: true, UserID: "profile", StreamPath: item.StreamPath, LibraryIdentity: remoteLibraryIdentity(item)}
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
			if h.localEntryAllowed(r, entry) != (mode == "valid status") {
				t.Fatal("status ignored changed identity or access")
			}
			if probes.Load() != 1 {
				t.Fatal("status performed another media probe")
			}
		})
	}
}

func TestAutomaticLibraryReportRetryKeepsContextAndExcludesCurrentVersion(t *testing.T) {
	h, catalog, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
	h.SetBadStreamsService(badstreams.New(filepath.Join(t.TempDir(), "reports.json")))
	second := catalog.items[0]
	second.ID = "version-2"
	second.ExternalMediaID = "part-2"
	second.StreamPath = "plexmedia:version-2"
	second.ProviderData = map[string]string{"partKey": "/part/2"}
	catalog.items = append(catalog.items, second)
	entry := runAutomaticLibraryWorker(t, h)
	for _, reporting := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry", true: "report"}[reporting], func(t *testing.T) {
			if reporting {
				entry = runAutomaticLibraryWorker(t, h)
			}
			h.store.RetireSourceSelection(entry.ID)
			body := releaseReportRequest{SourceVersion: sourceReportVersion(entry), Retry: true, SkipHLS: true, ClientID: "fixture-device"}
			if reporting {
				body.Reason = "wrong_title"
			}
			w := httptest.NewRecorder()
			r := reportRequest(t, http.MethodPost, entry.ID, body, false)
			if reporting {
				h.ReportRelease(w, r)
			} else {
				h.RetryRelease(w, r)
			}
			if w.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var response struct {
				Replacement playback.PrequeueResponse `json:"replacement"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			next := waitReportedReplacement(t, h, response.Replacement.PrequeueID)
			if next.Status != playback.PrequeueStatusReady || !next.AutomaticLibrarySelection || next.StreamPath != second.StreamPath || next.LibraryItemID != second.ID {
				t.Fatalf("retry lost automatic context: %+v", next)
			}
			if _, ok := h.store.Get(entry.ID); !ok {
				t.Fatal("active reporting ID removed")
			}
		})
	}
}

func TestAutomaticLibraryModesAndExplicitEpisodeLeafAreIsolated(t *testing.T) {
	h, catalog, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
	catalog.missing = true
	legacy, _ := h.store.CreateScoped("tmdb:movie:123", "Synthetic Movie", "profile", "movie", 2024, nil, "details", h.prequeueSettingsScopeKey("profile", "fixture-device", "tmdb:movie:123"))
	h.store.Update(legacy.ID, func(e *playback.PrequeueEntry) { e.Status = playback.PrequeueStatusSearching })
	body := []byte(`{"titleId":"tmdb:movie:123","titleName":"Synthetic Movie","userId":"profile","clientId":"fixture-device","automaticLibrarySelection":true,"skipHLS":true}`)
	r := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue", bytes.NewReader(body))
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
	w := httptest.NewRecorder()
	h.Prequeue(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response playback.PrequeueResponse
	_ = json.Unmarshal(w.Body.Bytes(), &response)
	if response.PrequeueID == legacy.ID {
		t.Fatal("opt-in reused legacy preparation")
	}
	entry := waitReportedReplacement(t, h, response.PrequeueID)
	if !entry.AutomaticLibrarySelection || entry.SettingsScopeKey == legacy.SettingsScopeKey {
		t.Fatal("opt-in mode was not persisted/scoped")
	}
	var request playback.PrequeueRequest
	if err := json.Unmarshal([]byte(`{"titleId":"tvdb:series:74852","episodeId":"tvdb:episode:456","seasonNumber":1,"episodeNumber":2,"automaticLibrarySelection":true}`), &request); err != nil {
		t.Fatal(err)
	}
	if request.EpisodeID != "tvdb:episode:456" {
		t.Fatal("metadata leaf ID was discarded")
	}
	if automaticLibraryScope("global", false) != "global" || automaticLibraryScope("global", true) == "global" {
		t.Fatal("legacy fingerprint changed or modes collided")
	}
	if !strings.Contains(entry.Error, "results") && entry.Status != playback.PrequeueStatusFailed {
		t.Fatalf("unexpected fixture terminal state: %+v", entry)
	}
}

func TestAutomaticLibrarySlowProbeWaitIsBounded(t *testing.T) {
	h, catalog, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h.fullProber = fallbackFullProber{run: func(context.Context, string) (*VideoFullResult, error) {
		close(started)
		<-release
		close(finished)
		return nil, nil
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	_, err := h.resolveRemoteItem(ctx, catalog.items[0], prequeueResolutionOptions{mediaType: "movie", userID: "profile", localScope: &prequeueLocalScope{accountID: "account", automaticLibrarySelection: true}})
	close(release)
	<-started
	<-finished
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbounded probe returned %v", err)
	}
}

func TestAutomaticLibraryExplicitLeafCannotReuseAnotherEpisodeAtSameCoordinates(t *testing.T) {
	for _, mode := range []string{"ready", "pending", "warm ready", "pending wrong mode"} {
		t.Run(mode, func(t *testing.T) {
			h, catalog, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
			catalog.missing = true
			titleID := "tvdb:series:74852"
			numbering := &models.EpisodeNumbering{SeriesID: titleID, Ordering: "official"}
			oldEpisode := &models.EpisodeReference{Numbering: numbering, SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tvdb:episode:111"}
			if mode == "pending wrong mode" {
				oldEpisode.EpisodeID = "tvdb:episode:222"
			}
			scope := automaticLibraryScope(h.prequeueSettingsScopeKey("profile", "fixture-device", titleID), true)
			old, _ := h.store.CreateScoped(titleID, "Synthetic Series", "profile", "series", 2024, oldEpisode, "details", scope)
			h.store.Update(old.ID, func(e *playback.PrequeueEntry) {
				e.AutomaticLibrarySelection = true
				e.Status = playback.PrequeueStatusReady
				e.StreamPath = "/webdav/old-episode.mkv"
				e.AudioTracks = []playback.AudioTrackInfo{{Index: 1, Language: "fra"}}
				if mode == "pending" || mode == "pending wrong mode" {
					e.Status = playback.PrequeueStatusSearching
				}
				if mode == "pending wrong mode" {
					e.AutomaticLibrarySelection = false
				}
			})
			if mode == "warm ready" {
				h.prewarmSvc = &profileWarmRef{id: old.ID}
			}
			body, _ := json.Marshal(playback.PrequeueRequest{TitleID: titleID, TitleName: "Synthetic Series", MediaType: "series", UserID: "profile", ClientID: "fixture-device", SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tvdb:episode:222", Numbering: numbering, AutomaticLibrarySelection: true, SkipHLS: true})
			r := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue", bytes.NewReader(body))
			r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
			w := httptest.NewRecorder()
			h.Prequeue(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			var response playback.PrequeueResponse
			_ = json.Unmarshal(w.Body.Bytes(), &response)
			if response.PrequeueID == old.ID {
				t.Fatal("reused a different metadata leaf at same S/E")
			}
			current := waitReportedReplacement(t, h, response.PrequeueID)
			if current.TargetEpisode == nil || current.TargetEpisode.EpisodeID != "tvdb:episode:222" {
				t.Fatalf("requested leaf discarded: %+v", current.TargetEpisode)
			}
			if mode != "pending" && mode != "pending wrong mode" {
				retired, ok := h.store.Get(old.ID)
				if !ok || !retired.ReuseDisabled {
					t.Fatal("active old leaf was removed instead of retired")
				}
			}
		})
	}
}

func TestAutomaticLibraryCurrentOfficialMetadataOwnsEpisode(t *testing.T) {
	titleID := "tmdb:series:123"
	numbering := &models.EpisodeNumbering{SeriesID: titleID, Ordering: "official"}
	details := &models.SeriesDetails{Title: models.Title{ID: titleID, MediaType: "series", TMDBID: 123, TVDBID: 74852}, Numbering: numbering, Seasons: []models.SeriesSeason{{Number: 1, Episodes: []models.SeriesEpisode{{ID: "tvdb:episode:456", TVDBID: 456, SeasonNumber: 1, EpisodeNumber: 2, Numbering: numbering}}}}}
	for _, mode := range []string{"current", "legacy resolves", "stale leaf", "ambiguous coordinate", "alternate", "foreign owner"} {
		t.Run(mode, func(t *testing.T) {
			h, _, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
			current := *details
			current.Seasons = append([]models.SeriesSeason(nil), details.Seasons...)
			current.Seasons[0].Episodes = append([]models.SeriesEpisode(nil), details.Seasons[0].Episodes...)
			ep := &models.EpisodeReference{Numbering: numbering, SeasonNumber: 1, EpisodeNumber: 2, EpisodeID: "tvdb:episode:456"}
			if mode == "legacy resolves" {
				ep.Numbering, ep.EpisodeID = nil, ""
			}
			if mode == "stale leaf" {
				ep.EpisodeID = "tvdb:episode:999"
			}
			if mode == "ambiguous coordinate" {
				alias := current.Seasons[0].Episodes[0]
				alias.ID = "tvdb:episode:999"
				current.Seasons[0].Episodes = append(current.Seasons[0].Episodes, alias)
			}
			if mode == "alternate" {
				ep.Numbering = &models.EpisodeNumbering{SeriesID: titleID, Ordering: "dvd"}
			}
			if mode == "foreign owner" {
				ep.Numbering = &models.EpisodeNumbering{SeriesID: "tmdb:series:999", Ordering: "official"}
			}
			h.metadataSvc = &mockSeriesDetailsProvider{details: &current}
			resolved := h.createEpisodeResolverAndLookupAbsoluteEp(t.Context(), titleID, "Synthetic Series", 2024, "", ep)
			wantVerified := mode == "current" || mode == "legacy resolves" || mode == "stale leaf"
			if resolved.LibraryEpisodeVerified != wantVerified {
				t.Fatalf("verified=%v want=%v", resolved.LibraryEpisodeVerified, wantVerified)
			}
			if mode == "stale leaf" {
				entry, _ := h.store.CreateScoped(titleID, "Synthetic Series", "profile", "series", 2024, ep, "details", automaticLibraryScope("global", true))
				h.store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) { e.AutomaticLibrarySelection = true })
				h.runPrequeueWorker(entry.ID, titleID, "Synthetic Series", "", "series", 2024, "profile", "", nil, ep, 0, true, prequeueWorkerInteractive, prequeueLocalScope{accountID: "account", automaticLibrarySelection: true})
				got, _ := h.store.Get(entry.ID)
				if got.Status != playback.PrequeueStatusFailed || !strings.Contains(got.Error, "identity changed") {
					t.Fatalf("stale leaf was not rejected: %+v", got)
				}
			}
		})
	}
}

func TestAutomaticLibraryRealProviderProbeSelectsRequestedFrenchAudio(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("ffmpeg is required for synthetic media integration")
	}
	ffprobe, err := exec.LookPath("ffprobe")
	if err != nil {
		t.Skip("ffprobe is required for synthetic media integration")
	}
	file := filepath.Join(t.TempDir(), "synthetic.mkv")
	cmd := exec.CommandContext(t.Context(), ffmpeg, "-nostdin", "-v", "error", "-f", "lavfi", "-i", "color=c=blue:s=320x180:r=24", "-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000", "-f", "lavfi", "-i", "sine=frequency=660:sample_rate=48000", "-t", "2", "-map", "0:v", "-map", "1:a", "-map", "2:a", "-c:v", "libx264", "-threads", "1", "-c:a", "aac", "-metadata:s:a:0", "language=eng", "-metadata:s:a:1", "language=fra", file)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generate synthetic fixture: %v %s", err, output)
	}
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, provider := range []string{"plex", "jellyfin"} {
		t.Run(provider, func(t *testing.T) {
			h, catalog, _ := newAutomaticLibraryWorker(t, provider, &fallbackSearch{})
			catalog.items[0].SizeBytes = int64(len(data))
			var calls atomic.Int32
			reader := &pathCapturingProvider{data: data, capturePath: func(path string) {
				calls.Add(1)
				if path != catalog.items[0].StreamPath {
					t.Errorf("provider received another media path: %s", path)
				}
			}}
			video := &VideoHandler{ffprobePath: ffprobe, streamer: debrid.NewCompositeProvider(reader), metadataCache: make(map[string]*cachedMetadataEntry)}
			h.fullProber, h.metadataProber = video, video
			got := runAutomaticLibraryWorker(t, h)
			if got.Status != playback.PrequeueStatusReady || got.SelectedAudioTrack != 2 || len(got.AudioTracks) != 2 || got.TrackSelection == nil || got.TrackSelection.Audio.LanguageCode != "fra" || calls.Load() == 0 {
				t.Fatalf("real provider probe/selection failed: status=%s audio=%d tracks=%v calls=%d error=%s", got.Status, got.SelectedAudioTrack, got.AudioTracks, calls.Load(), got.Error)
			}
		})
	}
}

func automaticPrequeueHTTP(t *testing.T, h *PrequeueHandler, body playback.PrequeueRequest) *playback.PrequeueEntry {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue", bytes.NewReader(data))
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
	w := httptest.NewRecorder()
	h.Prequeue(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("prequeue HTTP %d %s", w.Code, w.Body.String())
	}
	var response playback.PrequeueResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	return waitReportedReplacement(t, h, response.PrequeueID)
}

func TestAutomaticModernSeriesPreferenceHierarchyAndOfficialOwnerHTTP(t *testing.T) {
	for _, owner := range []string{"tmdb:series:123", "tvdb:series:74852"} {
		t.Run(owner, func(t *testing.T) {
			search := &fallbackSearch{results: []models.NZBResult{{Title: "Synthetic.Series.S02E03.1080p", Link: "https://example.invalid/series", ServiceType: models.ServiceTypeDebrid, Indexer: "AIOStreams", Attributes: map[string]string{"resolution": "1080p"}}}}
			h, catalog, _ := newAutomaticLibraryWorker(t, "plex", search)
			title := "tmdb:series:123"
			numbering := &models.EpisodeNumbering{SeriesID: owner, Ordering: "official"}
			h.metadataSvc = &mockSeriesDetailsProvider{details: &models.SeriesDetails{Title: models.Title{ID: title, MediaType: "series", TMDBID: 123, TVDBID: 74852}, Numbering: numbering, Seasons: []models.SeriesSeason{{Number: 2, Episodes: []models.SeriesEpisode{{ID: "tvdb:episode:456", TVDBID: 456, SeasonNumber: 2, EpisodeNumber: 3, Numbering: numbering}}}}}}
			catalog.items[0].LibraryType = models.LocalMediaLibraryTypeShow
			catalog.items[0].SeasonNumber = 2
			catalog.items[0].EpisodeNumber = 3
			catalog.items[0].SeriesExternalIDs = &models.LocalMediaExternalIDs{TMDB: "123", TVDB: "74852"}
			catalog.items[0].EpisodeExternalIDs = &models.LocalMediaExternalIDs{TVDB: "456"}
			prefs, err := content_preferences.NewService(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			h.SetContentPreferencesService(prefs)
			if err := prefs.Set("profile", models.ContentPreference{ContentID: title, ContentType: "series", AudioLanguage: "fra", SubtitleLanguage: "spa", SubtitleMode: "forced-only", PreferLocalFiles: models.BoolPtr(false)}); err != nil {
				t.Fatal(err)
			}
			cfg, _ := h.configManager.Load()
			cfg.Playback.PreferredAudioLanguage = "eng"
			if err := h.configManager.Save(cfg); err != nil {
				t.Fatal(err)
			}
			preferenceHandler := NewContentPreferencesHandler(prefs, &fakeRecordingUsersProvider{users: []models.User{{ID: "profile", AccountID: "account"}}})
			preferenceHandler.SetPrequeueStore(h.store)
			put := func(id string, value *bool) {
				t.Helper()
				data, _ := json.Marshal(map[string]any{"contentId": id, "contentType": "series", "preferLocalFiles": value})
				r := httptest.NewRequest(http.MethodPut, "/api/users/profile/content-preferences/local-source", bytes.NewReader(data))
				r = mux.SetURLVars(r, map[string]string{"userID": "profile"})
				r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
				w := httptest.NewRecorder()
				preferenceHandler.SetLocalSourcePreference(w, r)
				if w.Code != http.StatusOK {
					t.Fatalf("preference PUT %d %s", w.Code, w.Body.String())
				}
			}
			request := playback.PrequeueRequest{TitleID: title, TmdbID: "123", TvdbID: "74852", TitleName: "Synthetic Series", UserID: "profile", MediaType: "series", ClientID: "fixture-device", Year: 2024, SeasonNumber: 2, EpisodeNumber: 3, EpisodeID: "tvdb:episode:456", Numbering: numbering, AutomaticLibrarySelection: true, SkipHLS: true}
			var previous *playback.PrequeueEntry
			for _, phase := range []struct {
				name                  string
				root, season, episode *bool
				wantLibrary           bool
			}{
				{"root false season true", models.BoolPtr(false), models.BoolPtr(true), nil, owner == title},
				{"episode explicit false", models.BoolPtr(false), models.BoolPtr(true), models.BoolPtr(false), false},
				{"root true season false", models.BoolPtr(true), models.BoolPtr(false), nil, owner != title},
				{"episode explicit true", models.BoolPtr(false), models.BoolPtr(false), models.BoolPtr(true), owner == title},
				{"episode inherits season false", models.BoolPtr(false), models.BoolPtr(false), nil, false},
			} {
				t.Run(phase.name, func(t *testing.T) {
					put(title, phase.root)
					put(title+":season:2", phase.season)
					put(title+":season:2:episode:3", phase.episode)
					got := automaticPrequeueHTTP(t, h, request)
					want := "/webdav/title.mkv"
					if phase.wantLibrary {
						want = "plexmedia:version-1"
					}
					if got.TitleID != title || got.Status != playback.PrequeueStatusReady || got.StreamPath != want || got.SelectedAudioTrack != 4 {
						t.Fatalf("modern preference/track hierarchy bypassed: title=%s path=%s audio=%d status=%s error=%s", got.TitleID, got.StreamPath, got.SelectedAudioTrack, got.Status, got.Error)
					}
					if previous != nil {
						if got.SettingsScopeKey == previous.SettingsScopeKey || got.ID == previous.ID {
							t.Fatal("changed hierarchy reused old fingerprint/preparation")
						}
						retired, ok := h.store.Get(previous.ID)
						if !ok || !retired.ReuseDisabled {
							t.Fatal("changed preference removed active report ID")
						}
					}
					previous = got
				})
			}
		})
	}
}

func TestAutomaticModernSeriesKeepsExistingLegacyHistoryAndPin(t *testing.T) {
	h, catalog, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
	catalog.missing = true
	dir := t.TempDir()
	data, _ := json.Marshal(map[string]map[string]models.SeriesWatchState{"profile": {"tmdb:tv:123": {SeriesID: "tmdb:tv:123", SeriesTitle: "Synthetic Series", ExternalIDs: map[string]string{"tmdb": "123"}, NextEpisode: &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 3, EpisodeID: "tvdb:episode:456"}}}})
	if err := os.WriteFile(filepath.Join(dir, "watch_history.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	historyService, err := history.NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	h.historySvc = historyService
	got := automaticPrequeueHTTP(t, h, playback.PrequeueRequest{TitleID: "tmdb:series:123", TmdbID: "123", TitleName: "Synthetic Series", MediaType: "series", UserID: "profile", AutomaticLibrarySelection: true, SkipHLS: true})
	if got.TitleID != "tmdb:series:123" || got.TargetEpisode == nil || got.TargetEpisode.SeasonNumber != 2 || got.TargetEpisode.EpisodeNumber != 3 {
		t.Fatalf("modern preference identity reset legacy watch progress: %+v", got)
	}
	legacy, _ := h.store.Create("tmdb:tv:123", "Synthetic Series", "profile", "series", 2024, nil, "manual")
	r := httptest.NewRequest(http.MethodGet, "/api/playback/prequeue/manual?titleId=tmdb:series:123&tmdbId=123&userId=profile&mediaType=series", nil)
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
	entry, _, valid := h.manualPrequeueEntry(r)
	if !valid || entry == nil || entry.ID != legacy.ID {
		t.Fatal("modern alias lost existing legacy persistent pin")
	}
}

func TestAutomaticPrequeueRejectsConflictingSameProviderIDs(t *testing.T) {
	h, _, _ := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
	for _, body := range []playback.PrequeueRequest{{TitleID: "tmdb:series:123", TmdbID: "999"}, {TitleID: "tvdb:series:74852", TvdbID: "999"}, {TitleID: "imdb:tt123", ImdbID: "tt999"}} {
		body.AutomaticLibrarySelection = true
		body.TitleName = "Synthetic"
		body.UserID = "profile"
		data, _ := json.Marshal(body)
		r := httptest.NewRequest(http.MethodPost, "/api/playback/prequeue", bytes.NewReader(data))
		r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
		w := httptest.NewRecorder()
		h.Prequeue(w, r)
		if w.Code != http.StatusBadRequest || len(h.store.ListAll()) != 0 {
			t.Fatalf("conflicting supplied IDs were rebound to another title: %d", w.Code)
		}
	}
}
