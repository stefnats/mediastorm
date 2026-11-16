package handlers

import (
	"context"
	"encoding/json"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/httptest"
	"novastream/api/openapi"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/debrid"
	"novastream/services/mediaevidence"
	"novastream/services/playback"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type evidenceDebridHealth struct {
	response *debrid.DebridHealthCheck
	profile  string
}

func (s *evidenceDebridHealth) CheckHealthQuick(_ context.Context, r models.NZBResult) (*debrid.DebridHealthCheck, error) {
	s.profile = r.Attributes["profileId"]
	return s.response, nil
}
func (s *evidenceDebridHealth) CheckHealthFull(c context.Context, r models.NZBResult) (*debrid.DebridHealthCheck, error) {
	return s.CheckHealthQuick(c, r)
}
func (s *evidenceDebridHealth) CheckQuickCacheOnly(c context.Context, r models.NZBResult) (*debrid.DebridHealthCheck, error) {
	return s.CheckHealthQuick(c, r)
}
func (s *evidenceDebridHealth) CheckQuickCacheOnlyBulk(context.Context, []models.NZBResult) ([]*debrid.DebridHealthCheck, error) {
	return nil, nil
}

func evidenceFacts(at time.Time) *models.MediaProbeFacts {
	return mediaevidence.ProbeFacts([]mediaevidence.ProbeStream{{Index: 0, CodecType: "video", CodecName: "hevc", Width: 1920, Height: 1080, ColorTransfer: "bt709"}, {Index: 1, CodecType: "audio", CodecName: "aac", Tags: map[string]string{"language": "fra"}}, {Index: 2, CodecType: "audio", CodecName: "ac3", Tags: map[string]string{"language": "eng"}, Disposition: map[string]int{"visual_impaired": 0}}, {Index: 3, CodecType: "audio", CodecName: "aac", Tags: map[string]string{"language": "cym"}, Disposition: map[string]int{"visual_impaired": 1}}, {Index: 4, CodecType: "subtitle", CodecName: "unknown_future_codec", Tags: map[string]string{"language": "cat"}}}, at)
}

func mediaEvidenceHealthContract(t *testing.T, v *openapi.Validator) {
	at := time.Now().UTC().Truncate(time.Second)
	if fixed := os.Getenv("MEDIA_EVIDENCE_EVALUATED_AT"); fixed != "" {
		var err error
		at, err = time.Parse(time.RFC3339, fixed)
		if err != nil {
			t.Fatal(err)
		}
	}
	candidate := models.NZBResult{Title: "Synthetic candidate", GUID: "synthetic-release", ServiceType: models.ServiceTypeDebrid, Attributes: map[string]string{"languages": "fr", "resolution": "2160p"}}
	mediaevidence.ObserveCandidate(&candidate, at)
	evidence := mediaevidence.Probed(candidate, "synthetic-input", "", 0, evidenceFacts(at), "", nil, at)
	debridSvc := &evidenceDebridHealth{response: &debrid.DebridHealthCheck{Status: "cached", Healthy: true, Cached: true, Provider: "synthetic", MediaEvidence: evidence}}
	debridHandler := NewDebridHandler(nil, debridSvc)
	body := map[string]any{"result": candidate, "profileId": "viewer", "verifyUncached": true}
	debridReply := exercise(t, v, contractCall{method: "POST", target: "/api/debrid/cached", body: body}, debridHandler.CheckCached, 200)
	if debridSvc.profile != "viewer" {
		t.Fatal("captured profile not passed")
	}
	usenetSvc := &fakeUsenetService{response: &models.NZBHealthCheck{Status: "healthy", Healthy: true, CheckedSegments: 3, TotalSegments: 3, MediaEvidence: evidence}}
	usenetReply := exercise(t, v, contractCall{method: "POST", target: "/api/usenet/health", body: map[string]any{"result": candidate}}, NewUsenetHandler(usenetSvc).CheckHealth, 200)
	if strings.Contains(usenetReply.Body.String(), "mediaEvidence") {
		t.Fatal("legacy cached service evidence promoted without explicit fresh probe")
	}
	if debridSvc.response.MediaEvidence == nil || usenetSvc.response.MediaEvidence == nil {
		t.Fatal("handler mutated service cache")
	}
	exercise(t, v, contractCall{method: "POST", target: "/api/debrid/cached", body: `{`, invalid: true}, debridHandler.CheckCached, 400)
	exportMediaEvidenceFixtures(t, at, candidate, evidence, debridReply.Body.Bytes(), usenetReply.Body.Bytes())
}

func TestPreparationEvidenceRequiresActualInputAndPreservesSourceOnlyMovie(t *testing.T) {
	for _, path := range []string{"https://example.invalid/a%20b.mkv", "/webdav/a b.mkv", "localmedia:item/a.mkv", "plexmedia:item", "jellyfinmedia:item"} {
		t.Run(path, func(t *testing.T) {
			store := playback.NewPrequeueStore(time.Hour)
			h := &PrequeueHandler{store: store}
			entry, _ := store.CreateScoped("tmdb:movie:123", "Synthetic", "viewer", "movie", 2024, nil, "manual", "")
			store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) {
				e.StreamPath = path
				e.FileSize = 10
				e.SelectedResult = &models.NZBResult{GUID: "one", ServiceType: models.ServiceTypeDebrid}
			})
			probe := &VideoFullResult{MetadataEvidence: evidenceFacts(time.Now()), MetadataEvidenceInputVersion: probeInputVersion("different-input")}
			h.recordPreparationEvidence(entry.ID, "device", probe)
			before, _ := store.Get(entry.ID)
			if before.MediaEvidence != nil {
				t.Fatal("F1 probe became F2 facts")
			}
			probe.MetadataEvidenceInputVersion = probeInputVersion(path)
			h.recordPreparationEvidence(entry.ID, "device", probe)
			got, _ := store.Get(entry.ID)
			request := httptest.NewRequest("GET", "/", nil)
			request.Header.Set("X-Client-ID", "device")
			value := h.currentPreparationEvidence(request, got)
			if value == nil || value.TitleID != "" || value.TargetEpisode != nil || value.Probed.AudioStreams[0].Language != "fra" {
				t.Fatalf("source-only actual input: %+v", value)
			}
			got.MediaEvidence.Probed.AudioStreams[0].Language = "eng"
			original, _ := store.Get(entry.ID)
			if original.MediaEvidence.Probed.AudioStreams[0].Language != "fra" {
				t.Fatal("store read shared mutable evidence")
			}
			request.Header.Set("X-Client-ID", "other")
			if h.currentPreparationEvidence(request, original) != nil {
				t.Fatal("device scope leaked")
			}
			request.Header.Set("X-Client-ID", "device")
			original.StreamPath += "changed"
			if h.currentPreparationEvidence(request, original) != nil {
				t.Fatal("replaced file retained proof")
			}
			store.RetireSourceSelection(entry.ID)
			retired, _ := store.Get(entry.ID)
			if h.currentPreparationEvidence(request, retired) != nil {
				t.Fatal("reported/retired entry retained current proof")
			}
			raw, _ := json.Marshal(probe)
			if strings.Contains(string(raw), "MetadataEvidence") || strings.Contains(string(raw), "file_v1_") {
				t.Fatal("private probe marker exposed")
			}
		})
	}
}

func TestFullProbeEvidenceIncludesRawUnsupportedSubtitleAndDispositionPresence(t *testing.T) {
	var meta ffprobeOutput
	if err := json.Unmarshal([]byte(`{"streams":[{"index":0,"codec_type":"audio","codec_name":"aac","tags":{"language":"yue"},"disposition":{"default":1}},{"index":2,"codec_type":"subtitle","codec_name":"unknown_codec"}]}`), &meta); err != nil {
		t.Fatal(err)
	}
	facts := fullProbeEvidence(&meta, time.Now())
	if len(facts.SubtitleStreams) != 1 || !facts.SubtitleStreamsComplete || facts.AudioStreams[0].AudioDescription != nil || facts.AudioStreams[0].Language != "yue" {
		t.Fatalf("raw inventory: %+v", facts)
	}
	if fullProbeEvidence(&ffprobeOutput{}, time.Now()) != nil {
		t.Fatal("absent probe promoted")
	}
}

func exportMediaEvidenceFixtures(t *testing.T, at time.Time, candidate models.NZBResult, evidence *models.MediaEvidence, debridBody, legacyBody []byte) {
	root := os.Getenv("MEDIA_EVIDENCE_CONTRACT_DIR")
	if root == "" {
		return
	}
	if !filepath.IsAbs(root) {
		t.Fatal("fixture root must be absolute")
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	title := models.Title{ID: "tmdb:movie:41", Name: "Synthetic catalogue title", MediaType: "movie", TMDBID: 41, OriginalLanguageEvidence: &models.OriginalLanguageEvidence{Language: "is", Source: "tmdb", SourceTitleID: "tmdb:movie:41", ObservedAt: at, ExpiresAt: at.Add(24 * time.Hour)}}
	episode := models.EpisodeReference{EpisodeID: "episode-3", SeasonNumber: 2, EpisodeNumber: 3, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}}
	bound := models.CopyMediaEvidence(evidence)
	bound.TitleID = "tmdb:series:41"
	bound.TargetEpisode = &episode
	clipped := models.CopyMediaEvidence(evidence)
	for i := len(clipped.Probed.AudioStreams); i < 32; i++ {
		clipped.Probed.AudioStreams = append(clipped.Probed.AudioStreams, models.MediaAudioFacts{Index: i + 10, Language: "isl"})
	}
	clipped.Probed.AudioStreamsComplete = false
	clipped.Probed.SubtitleStreamsComplete = false
	expired := models.CopyMediaEvidence(evidence)
	expired.Probed.ExpiresAt = at.Add(-time.Second)
	expired.Candidate.ExpiresAt = at.Add(-time.Second)
	files := map[string]any{"title-original.json": title, "candidate-source.json": models.ScoredNZBResult{NZBResult: candidate, FilterStatus: "passed", MediaEvidence: mediaevidence.Candidate(candidate, at)}, "ready-bound-episode.json": playback.PrequeueStatusResponse{PrequeueID: "synthetic-ready", Status: playback.PrequeueStatusReady, TargetEpisode: &episode, MediaEvidence: bound}, "ready-source-only.json": playback.PrequeueStatusResponse{PrequeueID: "synthetic-movie", Status: playback.PrequeueStatusReady, MediaEvidence: evidence}, "probe-clipped.json": models.NZBHealthCheck{Status: "healthy", Healthy: true, MediaEvidence: clipped}, "expired-unknown.json": models.NZBHealthCheck{Status: "healthy", Healthy: true, MediaEvidence: models.CurrentMediaEvidence(expired, at)}}
	manifest := map[string]any{"version": 1, "evaluatedAt": at, "files": map[string]string{"title-original.json": "Title", "candidate-source.json": "SourceResult", "ready-bound-episode.json": "PrequeueStatus", "ready-source-only.json": "PrequeueStatus", "probe-clipped.json": "UsenetHealth", "expired-unknown.json": "UsenetHealth", "debrid-probe.json": "DebridHealth", "legacy-unknown.json": "UsenetHealth"}}
	files["manifest.json"] = manifest
	for name, value := range files {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, name), append(data, '\n'), 0644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "debrid-probe.json"), debridBody, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "legacy-unknown.json"), legacyBody, 0644); err != nil {
		t.Fatal(err)
	}
}

func TestActualFullProbeCapturesFactsButLegacyPathCacheCannotPromote(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "ffprobe")
	script := "#!/bin/sh\nprintf '%s' '{\"format\":{\"duration\":\"60\"},\"streams\":[{\"index\":0,\"codec_type\":\"video\",\"codec_name\":\"h264\",\"width\":1920,\"height\":1080},{\"index\":1,\"codec_type\":\"audio\",\"codec_name\":\"aac\",\"tags\":{\"language\":\"cym\"}},{\"index\":2,\"codec_type\":\"subtitle\",\"codec_name\":\"future_codec\"}]}'\n"
	if err := os.WriteFile(binary, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	h := &VideoHandler{ffprobePath: binary, metadataCache: make(map[string]*cachedMetadataEntry)}
	probe, err := h.ProbeVideoFull(t.Context(), "https://example.invalid/a%20b.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if probe.MetadataEvidence == nil || probe.MetadataEvidenceInputVersion != probeInputVersion("https://example.invalid/a b.mkv") || len(probe.MetadataEvidence.SubtitleStreams) != 1 || !probe.MetadataEvidence.SubtitleStreamsComplete {
		t.Fatal("actual probe facts missing or input mismatch")
	}
	legacy := h.unifiedProbeToVideoFull(h.videoFullToUnifiedProbe(probe))
	if legacy.MetadataEvidence != nil || legacy.MetadataEvidenceInputVersion != "" {
		t.Fatal("path-only legacy HLS cache promoted/restamped")
	}
	p := &usenetTrackProber{ffprobePath: binary}
	_, _, raw, err := p.runFFProbeWithEvidence(t.Context(), "https://example.invalid/synthetic", "")
	if err != nil || raw == nil || len(raw.SubtitleStreams) != 1 || raw.AudioStreams[0].Language != "cym" {
		t.Fatalf("Usenet raw existing probe: %+v %v", raw, err)
	}
}

func TestPreparedEvidenceHTTPRespectsOwnershipReportsAndEpisodeProof(t *testing.T) {
	store := playback.NewPrequeueStore(time.Hour)
	h := &PrequeueHandler{store: store, users: fallbackProfiles{models.User{ID: "viewer", AccountID: "account"}}, badStreamsSvc: badstreams.New(filepath.Join(t.TempDir(), "reports.json"))}
	ep := &models.EpisodeReference{EpisodeID: "episode-3", SeasonNumber: 2, EpisodeNumber: 3, Numbering: &models.EpisodeNumbering{SeriesID: "tvdb:series:74852", Ordering: "official"}}
	entry, _ := store.CreateScoped("tmdb:series:41", "Synthetic series", "viewer", "series", 2024, ep, "manual", "")
	result := models.NZBResult{Title: "Synthetic.Series.S02E03", GUID: "release", ServiceType: models.ServiceTypeDebrid}
	store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) {
		e.StreamPath = "/webdav/input.mkv"
		e.SelectedResult = &result
		e.Status = playback.PrequeueStatusReady
	})
	probe := &VideoFullResult{MetadataEvidence: evidenceFacts(time.Now()), MetadataEvidenceInputVersion: probeInputVersion("/webdav/input.mkv")}
	h.recordPreparationEvidence(entry.ID, "device", probe)
	before, _ := store.Get(entry.ID)
	if before.MediaEvidence == nil || before.MediaEvidence.TitleID != "" || before.MediaEvidence.TargetEpisode != nil {
		t.Fatal("unverified caller episode gained catalogue association")
	}
	store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) { e.LibraryEpisodeVerified = true })
	h.recordPreparationEvidence(entry.ID, "device", probe)
	request := func(account string) *http.Request {
		r := httptest.NewRequest("GET", "/api/playback/prequeue/"+entry.ID, nil)
		r.Header.Set("X-Client-ID", "device")
		r = mux.SetURLVars(r, map[string]string{"prequeueID": entry.ID})
		return r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, account))
	}
	denied := httptest.NewRecorder()
	h.GetStatus(denied, request("different-account"))
	if denied.Code != 404 || strings.Contains(denied.Body.String(), "mediaEvidence") {
		t.Fatal("another account received facts")
	}
	accepted := httptest.NewRecorder()
	h.GetStatus(accepted, request("account"))
	if accepted.Code != 200 {
		t.Fatalf("status %d %s", accepted.Code, accepted.Body.String())
	}
	var response playback.PrequeueStatusResponse
	if err := json.Unmarshal(accepted.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.MediaEvidence == nil || response.MediaEvidence.TargetEpisode.EpisodeID != ep.EpisodeID || response.MediaEvidence.TitleID != "tmdb:series:41" {
		t.Fatal("verified exact episode binding missing")
	}
	context := h.reportContextForPrequeue(entry.ID, "viewer", "device", ep)
	if _, err := h.badStreamsSvc.Report(badstreams.ReportRequest{Result: result, Context: context, Reason: "wrong_episode", ReporterID: "account"}); err != nil {
		t.Fatal(err)
	}
	reported := httptest.NewRecorder()
	h.GetStatus(reported, request("account"))
	if reported.Code != 200 || strings.Contains(reported.Body.String(), "mediaEvidence") {
		t.Fatal("active report left current evidence or removed reporting ID")
	}
}
