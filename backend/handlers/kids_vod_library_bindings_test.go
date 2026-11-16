package handlers

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/badstreams"
	"novastream/services/kidsvod"
	"novastream/services/playback"
)

type kidsVODTestProfiles struct {
	mu   sync.Mutex
	user models.User
}

func (p *kidsVODTestProfiles) Get(id string) (models.User, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.user, p.user.ID == id
}
func (p *kidsVODTestProfiles) BelongsToAccount(id, account string) bool {
	u, ok := p.Get(id)
	return ok && u.AccountID == account
}
func (p *kidsVODTestProfiles) change(f func(*models.User)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(&p.user)
}

type kidsVODTestMetadata struct {
	title models.Title
	calls atomic.Int32
}

func (m *kidsVODTestMetadata) MovieInfo(context.Context, models.MovieDetailsQuery) (*models.Title, error) {
	m.calls.Add(1)
	copy := m.title
	return &copy, nil
}

type kidsVODTestRemote struct {
	catalog *automaticRemoteCatalog
	library models.RemoteMediaLibrary
	reads   atomic.Int32
}

func (r *kidsVODTestRemote) GetItem(_ context.Context, id string) (*models.RemoteMediaItem, error) {
	r.reads.Add(1)
	for _, item := range r.catalog.items {
		if item.ID == id {
			copy := item
			return &copy, nil
		}
	}
	return nil, errors.New("missing cached item")
}
func (r *kidsVODTestRemote) GetLibrary(_ context.Context, id string) (*models.RemoteMediaLibrary, error) {
	r.reads.Add(1)
	if r.library.ID != id {
		return nil, errors.New("missing cached library")
	}
	copy := r.library
	return &copy, nil
}

type kidsVODTestAccess struct {
	original *fallbackAccess
	policy   models.LibraryAccessPolicy
}

func (a *kidsVODTestAccess) CanAccess(ctx context.Context, l, account, p string, master bool) (bool, error) {
	return a.original.CanAccess(ctx, l, account, p, master)
}
func (a *kidsVODTestAccess) Get(context.Context, string) (models.LibraryAccessPolicy, error) {
	return a.policy, nil
}

type kidsVODProducerFixture struct {
	handler   *PrequeueHandler
	remote    *kidsVODTestRemote
	resources *kidsVODResources
	profiles  *kidsVODTestProfiles
	metadata  *kidsVODTestMetadata
	probes    *atomic.Int32
}

func kidsVODProducer(t *testing.T, observe bool, duringProbe func(*kidsVODProducerFixture)) *kidsVODProducerFixture {
	t.Helper()
	h, catalog, access := newAutomaticLibraryWorker(t, "plex", &fallbackSearch{})
	profiles := &kidsVODTestProfiles{user: models.User{ID: "profile", AccountID: "account"}}
	h.users = profiles
	remote := &kidsVODTestRemote{catalog: catalog, library: models.RemoteMediaLibrary{ID: "remote-library", Provider: "plex", AccountID: "account", ServerID: "server-a", ServerURL: "https://synthetic.invalid", ExternalLibraryID: "library-a"}}
	metadata := &kidsVODTestMetadata{title: models.Title{ID: "tmdb:movie:123", MediaType: "movie", TMDBID: 123}}
	h.movieMetadataSvc = metadata
	f := &kidsVODProducerFixture{handler: h, remote: remote, profiles: profiles, metadata: metadata, probes: &atomic.Int32{}}
	if observe {
		f.resources = newKidsVODResources(h, remote, &kidsVODTestAccess{original: access})
		h.kidsVODBindings = f.resources
	}
	h.fullProber = fallbackFullProber{run: func(_ context.Context, path string) (*VideoFullResult, error) {
		f.probes.Add(1)
		if duringProbe != nil {
			duringProbe(f)
		}
		return &VideoFullResult{VideoCodec: "h264", VideoWidth: 1920, VideoHeight: 1080, Duration: 600, AudioStreams: []models.AudioStreamInfo{{Index: 4, Language: "fra", Codec: "aac"}}, MetadataEvidence: evidenceFacts(time.Now()), MetadataEvidenceInputVersion: probeInputVersion(path)}, nil
	}}
	return f
}
func (f *kidsVODProducerFixture) binding(id string) (kidsvod.Binding, error) {
	ref, _ := kidsvod.NewResourceRef(kidsvod.Prequeue, id, 0)
	return f.resources.resolveOwner(context.Background(), "account", "profile", "", ref)
}
func TestKidsVODNormalLibraryProducerDoesNotAddProviderWork(t *testing.T) {
	var baselineMetadata, baselineProbes int32
	for _, observe := range []bool{false, true} {
		f := kidsVODProducer(t, observe, nil)
		entry := runAutomaticLibraryWorker(t, f.handler)
		if entry.Status != playback.PrequeueStatusReady || entry.LibraryItemID != "version-1" {
			t.Fatalf("normal library selection failed: %+v", entry)
		}
		if !observe {
			baselineMetadata = f.metadata.calls.Load()
			baselineProbes = f.probes.Load()
			if f.remote.reads.Load() != 0 {
				t.Fatal("nil observer read its repository")
			}
			continue
		}
		if baselineMetadata != f.metadata.calls.Load() || baselineProbes != f.probes.Load() || baselineProbes != 1 {
			t.Fatal("observer added metadata or probe work")
		}
		binding, err := f.binding(entry.ID)
		if err != nil || binding.CatalogID != "tmdb:movie:123" || binding.MediaType != "movie" {
			t.Fatalf("actual normal winner did not produce a private binding: %+v %v", binding, err)
		}
		if f.metadata.calls.Load() != baselineMetadata || f.probes.Load() != baselineProbes {
			t.Fatal("cache read caused new provider work")
		}
		if entry.MediaEvidence.TitleID != "" {
			t.Fatal("private receipt widened public movie evidence association")
		}
	}
}
func TestKidsVODProducerRejectsChangedContextDuringExistingProbe(t *testing.T) {
	for _, change := range []string{"child", "pin", "route", "configuration", "catalogue mismatch"} {
		t.Run(change, func(t *testing.T) {
			f := kidsVODProducer(t, true, func(f *kidsVODProducerFixture) {
				switch change {
				case "child":
					f.profiles.change(func(u *models.User) { u.IsKidsProfile = true })
				case "pin":
					f.profiles.change(func(u *models.User) { u.PinHash = "changed" })
				case "route":
					f.remote.library.ServerID = "server-b"
				case "configuration":
					cfg, _ := f.handler.configManager.Load()
					cfg.Playback.PreferredAudioLanguage = "eng"
					if err := f.handler.configManager.Save(cfg); err != nil {
						t.Fatal(err)
					}
				}
			})
			if change == "catalogue mismatch" {
				f.metadata.title.TMDBID = 999
				f.metadata.title.ID = "tmdb:movie:999"
			}
			entry := runAutomaticLibraryWorker(t, f.handler)
			if _, err := f.binding(entry.ID); err == nil {
				t.Fatal("old input acquired changed context authority")
			}
		})
	}
}
func TestKidsVODPrivateReceiptCurrentChecksAndNoABAAttempt(t *testing.T) {
	for _, change := range []string{"child", "pin", "owner", "route server", "route url", "route owner", "route external library", "file", "part", "leaf", "access", "client", "attempt", "import", "expiry"} {
		t.Run(change, func(t *testing.T) {
			f := kidsVODProducer(t, true, nil)
			entry := runAutomaticLibraryWorker(t, f.handler)
			if _, err := f.binding(entry.ID); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "child":
				f.profiles.change(func(u *models.User) { u.IsKidsProfile = true })
			case "pin":
				f.profiles.change(func(u *models.User) { u.PinHash = "changed" })
			case "owner":
				f.profiles.change(func(u *models.User) { u.AccountID = "other" })
			case "route server":
				f.remote.library.ServerID = "other"
			case "route url":
				f.remote.library.ServerURL = "https://other.invalid"
			case "route owner":
				f.remote.library.AccountID = "other"
			case "route external library":
				f.remote.library.ExternalLibraryID = "other"
			case "file":
				f.remote.catalog.items[0].SizeBytes++
			case "part":
				f.remote.catalog.items[0].ExternalMediaID = "part-2"
			case "leaf":
				f.remote.catalog.items[0].EpisodeExternalIDs = &models.LocalMediaExternalIDs{TVDB: "99"}
			case "access":
				f.resources.access.(*kidsVODTestAccess).original.deny.Store(true)
			case "client":
				f.handler.store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) { e.MediaEvidenceClientID = "other" })
			case "attempt":
				f.handler.store.BeginAuthorizationAttempt(entry.ID)
			case "import":
				f.handler.store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) { e.MigrationAdopted = true })
			case "expiry":
				f.resources.now = func() time.Time { return time.Now().Add(time.Hour) }
			}
			if _, err := f.binding(entry.ID); err == nil {
				t.Fatal("changed input/context retained authority")
			}
		})
	}
}
func TestKidsVODUnprovenInputsAndReceiptCopies(t *testing.T) {
	f := kidsVODProducer(t, true, nil)
	entry := runAutomaticLibraryWorker(t, f.handler)
	f.resources.mu.Lock()
	receipt := f.resources.receipts[entry.ID]
	f.resources.mu.Unlock()
	f.remote.catalog.items[0].ExternalIDs.TMDB = "999"
	f.remote.catalog.items[0].ProviderData["partKey"] = "other"
	if receipt.proof.item.ExternalIDs.TMDB != "123" || receipt.proof.item.ProviderData["partKey"] != "/part/1" {
		t.Fatal("receipt aliased mutable provider item")
	}
	for _, kind := range []kidsvod.ResourceKind{kidsvod.Local, kidsvod.Plex, kidsvod.Jellyfin, kidsvod.Recording} {
		ref, _ := kidsvod.NewResourceRef(kind, "version-1", 0)
		if _, err := f.resources.resolveOwner(context.Background(), "account", "profile", "", ref); err == nil {
			t.Fatal("bare resource acquired proof")
		}
	}
	ref, _ := kidsvod.NewResourceRef(kidsvod.Prequeue, entry.ID, 0)
	for _, owner := range [][3]string{{"other", "profile", ""}, {"account", "other", ""}, {"account", "profile", "other"}} {
		if _, err := f.resources.resolveOwner(context.Background(), owner[0], owner[1], owner[2], ref); err == nil {
			t.Fatal("foreign subject read receipt")
		}
	}
}

func TestKidsVODCurrentReportChecksNeverInvokeStreamValidator(t *testing.T) {
	f := kidsVODProducer(t, true, nil)
	f.handler.SetBadStreamsService(badstreams.New(filepath.Join(t.TempDir(), "reports.json")))
	entry := runAutomaticLibraryWorker(t, f.handler)
	var calls atomic.Int32
	f.handler.store.SetStreamPathValidator(func(context.Context, string) error { calls.Add(1); return errors.New("poison provider validator") })
	for i := 0; i < 5; i++ {
		if _, err := f.binding(entry.ID); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("cache-only report check validated a provider input")
	}
	ctx := reportContext(f.handler.trackPreferenceResolver(), entry.UserID, "", entry.TitleID, entry.TargetEpisode)
	_, err := f.handler.badStreamsSvc.Report(badstreams.ReportRequest{Result: *entry.SelectedResult, Context: ctx, Reason: "wrong_title", ReporterID: "profile"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.binding(entry.ID); err == nil {
		t.Fatal("current report revision did not revoke binding")
	}
	if calls.Load() != 0 {
		t.Fatal("report revocation invoked provider validation")
	}
}

func TestKidsVODPublishCannotRestampPostValidationProfileOrRoute(t *testing.T) {
	for _, change := range []string{"child", "pin", "route"} {
		t.Run(change, func(t *testing.T) {
			f := kidsVODProducer(t, true, nil)
			entry := runAutomaticLibraryWorker(t, f.handler)
			f.resources.mu.Lock()
			receipt := f.resources.receipts[entry.ID]
			delete(f.resources.receipts, entry.ID)
			f.resources.mu.Unlock()
			if change == "route" {
				f.remote.library.ServerID = "replacement"
			} else {
				f.profiles.change(func(u *models.User) {
					if change == "child" {
						u.IsKidsProfile = true
					} else {
						u.PinHash = "new-pin"
					}
				})
			}
			f.resources.publish(entry.ID, receipt.stamp, &receipt.proof, receipt.config, receipt.profileScope)
			f.resources.mu.Lock()
			_, present := f.resources.receipts[entry.ID]
			f.resources.mu.Unlock()
			if present {
				t.Fatal("old normal winner was restamped after final validation")
			}
		})
	}
}
func TestKidsVODNormalSeriesReceiptRequiresExactCurrentLeaf(t *testing.T) {
	for _, mode := range []string{"exact", "stale leaf", "alternate", "missing provider leaf", "conflicting parent"} {
		t.Run(mode, func(t *testing.T) {
			f := kidsVODProducer(t, true, nil)
			title := "tmdb:series:123"
			numbering := &models.EpisodeNumbering{SeriesID: title, Ordering: "official"}
			episode := &models.EpisodeReference{SeasonNumber: 2, EpisodeNumber: 3, EpisodeID: "tvdb:episode:456", Numbering: numbering}
			f.handler.metadataSvc = &mockSeriesDetailsProvider{details: &models.SeriesDetails{Title: models.Title{ID: title, MediaType: "series", TMDBID: 123, TVDBID: 74852}, Numbering: numbering, Seasons: []models.SeriesSeason{{Number: 2, Episodes: []models.SeriesEpisode{{ID: "tvdb:episode:456", TVDBID: 456, SeasonNumber: 2, EpisodeNumber: 3, Numbering: numbering}}}}}}
			item := &f.remote.catalog.items[0]
			item.LibraryType = models.LocalMediaLibraryTypeShow
			item.SeasonNumber = 2
			item.EpisodeNumber = 3
			item.SeriesExternalIDs = &models.LocalMediaExternalIDs{TMDB: "123", TVDB: "74852"}
			item.EpisodeExternalIDs = &models.LocalMediaExternalIDs{TVDB: "456"}
			f.remote.library.Type = models.LocalMediaLibraryTypeShow
			switch mode {
			case "stale leaf":
				episode.EpisodeID = "tvdb:episode:999"
			case "alternate":
				episode.Numbering = &models.EpisodeNumbering{SeriesID: title, Ordering: "dvd"}
			case "missing provider leaf":
				item.EpisodeExternalIDs = nil
			case "conflicting parent":
				item.SeriesExternalIDs.TMDB = "999"
			}
			scope := automaticLibraryScope(f.handler.prequeueSettingsScopeKey("profile", "", title), true)
			entry, _ := f.handler.store.CreateScoped(title, "Synthetic Series", "profile", "series", 2024, episode, "details", scope)
			f.handler.store.UpdateWorker(entry.ID, func(e *playback.PrequeueEntry) { e.AutomaticLibrarySelection = true })
			f.handler.runPrequeueWorker(entry.ID, title, "Synthetic Series", "", "series", 2024, "profile", "", nil, episode, 0, true, prequeueWorkerInteractive, prequeueLocalScope{accountID: "account", automaticLibrarySelection: true})
			binding, err := f.binding(entry.ID)
			if mode == "exact" {
				if err != nil || binding.Episode == nil || binding.Episode.EpisodeID != "tvdb:episode:456" || binding.Episode.Numbering.SeriesID != title {
					t.Fatalf("trusted normal episode unavailable: %+v %v", binding, err)
				}
			} else if err == nil {
				t.Fatal("unproven leaf acquired catalogue/file relationship")
			}
		})
	}
}
