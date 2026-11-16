package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"novastream/models"
	"novastream/services/kidsvod"
	"novastream/services/mediaevidence"
	"novastream/services/playback"
)

type kidsVODRemoteSnapshots interface {
	GetItem(context.Context, string) (*models.RemoteMediaItem, error)
	GetLibrary(context.Context, string) (*models.RemoteMediaLibrary, error)
}
type kidsVODLibraryAccess interface {
	CanAccess(context.Context, string, string, string, bool) (bool, error)
	Get(context.Context, string) (models.LibraryAccessPolicy, error)
}

// This private value can only come from the existing normal verified library
// winner. A caller DTO, persisted entry or source-only MediaEvidence cannot
// manufacture this process receipt. Local/name/manual-only mappings remain
// unknown until they have an independent per-file catalogue origin.
type kidsVODLibraryProof struct {
	item                             models.RemoteMediaItem
	catalogue                        models.Title
	episode                          *models.EpisodeReference
	path, inputVersion, libraryRoute string
	size                             int64
	facts                            *models.MediaProbeFacts
	account, profile, client         string
}
type kidsVODLibraryReceipt struct {
	proof                                               kidsVODLibraryProof
	stamp                                               playback.AuthorizationStamp
	config, settings, profileScope, report, accessScope string
	entryFingerprint                                    string
	observed, expires                                   time.Time
}
type kidsVODResources struct {
	mu       sync.Mutex
	parent   *PrequeueHandler
	remote   kidsVODRemoteSnapshots
	access   kidsVODLibraryAccess
	receipts map[string]kidsVODLibraryReceipt
	now      func() time.Time
}

func newKidsVODResources(parent *PrequeueHandler, remote kidsVODRemoteSnapshots, access kidsVODLibraryAccess) *kidsVODResources {
	return &kidsVODResources{parent: parent, remote: remote, access: access, receipts: map[string]kidsVODLibraryReceipt{}, now: time.Now}
}
func kidsVODHash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func kidsVODEpisodeCopy(ep *models.EpisodeReference) *models.EpisodeReference {
	if ep == nil {
		return nil
	}
	copy := *ep
	if ep.Numbering != nil {
		n := *ep.Numbering
		copy.Numbering = &n
	}
	if ep.Image != nil {
		image := *ep.Image
		copy.Image = &image
	}
	return &copy
}
func kidsVODProfileScope(parent *PrequeueHandler, profile string) (string, bool) {
	reader, ok := parent.users.(interface {
		Get(string) (models.User, bool)
	})
	if !ok {
		return "", false
	}
	user, ok := reader.Get(profile)
	if !ok {
		return "", false
	}
	return kidsVODHash([]any{user, user.PinHash}), true
}

func kidsVODTitleIDs(title models.Title, media string) (map[string]string, bool) {
	if title.MediaType != "" && title.MediaType != media && !(media == "series" && title.MediaType == "tv") {
		return nil, false
	}
	ids := map[string]string{}
	if title.TMDBID > 0 {
		ids["tmdb"] = strconv.FormatInt(title.TMDBID, 10)
	}
	if title.TVDBID > 0 {
		ids["tvdb"] = strconv.FormatInt(title.TVDBID, 10)
	}
	if title.IMDBID != "" {
		ids["imdb"] = strings.TrimPrefix(title.IMDBID, "imdb:")
	}
	parts := strings.Split(title.ID, ":")
	provider, value := "", ""
	if len(parts) == 3 && (parts[0] == "tmdb" || parts[0] == "tvdb") && (parts[1] == media || parts[0] == "tmdb" && media == "series" && parts[1] == "tv") {
		provider, value = parts[0], parts[2]
	} else if len(parts) == 2 && parts[0] == "tvdb" && media == "series" {
		provider, value = "tvdb", parts[1]
	} else if len(parts) == 2 && parts[0] == "imdb" {
		provider, value = "imdb", parts[1]
	} else if strings.HasPrefix(title.ID, "tt") && !strings.Contains(title.ID, ":") {
		provider, value = "imdb", title.ID
	}
	if provider == "" || value == "" {
		return nil, false
	}
	if prior := ids[provider]; prior != "" && prior != value {
		return nil, false
	}
	ids[provider] = value
	return ids, true
}
func kidsVODIDsMatch(ids *models.LocalMediaExternalIDs, title models.Title, media string) bool {
	if ids == nil {
		return false
	}
	known, ok := kidsVODTitleIDs(title, media)
	if !ok {
		return false
	}
	matched := false
	for provider, actual := range map[string]string{"tmdb": ids.TMDB, "tvdb": ids.TVDB, "imdb": ids.IMDB} {
		if expected := known[provider]; expected != "" && actual != "" {
			if expected != actual {
				return false
			}
			matched = true
		}
	}
	return matched
}
func kidsVODLeafMatches(ids *models.LocalMediaExternalIDs, ep *models.EpisodeReference) bool {
	if ids == nil || ep == nil || ep.Numbering == nil || ep.EpisodeID == "" {
		return false
	}
	parts := strings.Split(ep.EpisodeID, ":")
	if len(parts) != 3 || parts[1] != "episode" {
		return false
	}
	switch parts[0] {
	case "tmdb":
		return ids.TMDB != "" && ids.TMDB == parts[2]
	case "tvdb":
		return ids.TVDB != "" && ids.TVDB == parts[2] && (ep.TvdbID == "" || ep.TvdbID == "0" || ep.TvdbID == ids.TVDB)
	}
	return false
}

func (r *kidsVODResources) candidateProof(result models.NZBResult, resolution *models.PlaybackResolution, probe *VideoFullResult, opts prequeueResolutionOptions) *kidsVODLibraryProof {
	if r == nil || opts.kidsVODLibraryRoutes == nil || opts.kidsVODCatalogue == nil || opts.localScope == nil || !opts.localScope.automaticLibrarySelection || resolution == nil || probe == nil || probe.MetadataEvidence == nil {
		return nil
	}
	// Only the actual configured provider item selected by the existing normal
	// remote-library pipeline can establish the relationship in this packet.
	item, ok := opts.remoteItems[result.Link]
	if !ok || item.ID == "" || !item.SinglePart || item.IsMissing || item.StreamPath != resolution.WebDAVPath || probe.MetadataEvidenceInputVersion != probeInputVersion(resolution.WebDAVPath) {
		return nil
	}
	provider, _, ok := libraryStreamIdentity(item.StreamPath)
	if !ok || provider != "plex" && provider != "jellyfin" {
		return nil
	}
	route := opts.kidsVODLibraryRoutes[item.LibraryID]
	if route == "" {
		return nil
	}
	ids := item.ExternalIDs
	var episode *models.EpisodeReference
	if opts.mediaType == "series" {
		if !opts.localScope.libraryEpisodeVerified || opts.targetEpisode == nil || item.SeasonNumber != opts.targetEpisode.SeasonNumber || item.EpisodeNumber != opts.targetEpisode.EpisodeNumber || !kidsVODLeafMatches(item.EpisodeExternalIDs, opts.targetEpisode) {
			return nil
		}
		ids = item.SeriesExternalIDs
		episode = kidsVODEpisodeCopy(opts.targetEpisode)
	}
	if !kidsVODIDsMatch(ids, *opts.kidsVODCatalogue, opts.mediaType) {
		return nil
	}
	value := mediaevidence.Probed(result, resolution.WebDAVPath, "", resolution.FileSize, probe.MetadataEvidence, "", nil, r.now())
	if value == nil || value.Probed == nil {
		return nil
	}
	copy := item
	copy.ProviderData = map[string]string{}
	for k, v := range item.ProviderData {
		copy.ProviderData[k] = v
	}
	cloneIDs := func(ids *models.LocalMediaExternalIDs) *models.LocalMediaExternalIDs {
		if ids == nil {
			return nil
		}
		value := *ids
		return &value
	}
	copy.ExternalIDs = cloneIDs(item.ExternalIDs)
	copy.SeriesExternalIDs = cloneIDs(item.SeriesExternalIDs)
	copy.EpisodeExternalIDs = cloneIDs(item.EpisodeExternalIDs)
	copy.AudioLanguages = append([]string(nil), item.AudioLanguages...)
	// Keep only scalar server identity fields; artwork, ratings and caller-view
	// metadata never become the catalogue/file proof in this receipt.
	catalogue := models.Title{ID: opts.kidsVODCatalogue.ID, MediaType: opts.kidsVODCatalogue.MediaType, TMDBID: opts.kidsVODCatalogue.TMDBID, TVDBID: opts.kidsVODCatalogue.TVDBID, IMDBID: opts.kidsVODCatalogue.IMDBID}
	return &kidsVODLibraryProof{item: copy, catalogue: catalogue, episode: episode, path: resolution.WebDAVPath, inputVersion: probe.MetadataEvidenceInputVersion, libraryRoute: route, size: resolution.FileSize, facts: value.Probed, account: opts.localScope.accountID, profile: opts.userID, client: normalizeClientID(opts.clientID)}
}
func kidsVODEntryFingerprint(entry *playback.PrequeueEntry) string {
	return kidsVODHash([]any{entry.ID, entry.UserID, entry.TitleID, entry.MediaType, entry.TargetEpisode, entry.StreamPath, entry.FileSize, entry.LibraryItemID, entry.LibrarySourceType, entry.LibraryIdentity, entry.SettingsScopeKey, entry.AutomaticLibrarySelection, entry.MediaEvidenceConfigurationKey, entry.MediaEvidenceClientID, entry.MediaEvidenceReportRevision})
}
func (r *kidsVODResources) publish(id string, stamp playback.AuthorizationStamp, proof *kidsVODLibraryProof, configuration, initialProfileScope string) {
	if r == nil || proof == nil || initialProfileScope == "" || !stamp.Valid() || r.parent == nil || r.remote == nil || r.access == nil {
		return
	}
	entry, current, ok := r.parent.store.SnapshotForAuthorization(id)
	if !ok || current != stamp || entry.Status != playback.PrequeueStatusReady || entry.ReuseDisabled || entry.MigrationAdopted || entry.UserID != proof.profile || entry.StreamPath != proof.path || entry.FileSize != proof.size || entry.LibraryItemID != proof.item.ID || entry.LibraryIdentity != remoteLibraryIdentity(proof.item) {
		return
	}
	if configuration == "" || configuration != r.parent.preparationConfigurationKey() || entry.MediaEvidenceConfigurationKey != configuration || entry.MediaEvidenceClientID != proof.client {
		return
	}
	settings := automaticLibraryScope(r.parent.prequeueSettingsScopeKey(proof.profile, proof.client, entry.TitleID), entry.AutomaticLibrarySelection)
	if settings != entry.SettingsScopeKey {
		return
	}
	reader, ok := r.parent.users.(interface {
		Get(string) (models.User, bool)
	})
	if !ok {
		return
	}
	user, ok := reader.Get(proof.profile)
	if !ok || user.AccountID != proof.account {
		return
	}
	if value := r.currentEvidence(entry, proof.client); value == nil || value.Probed == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	allowed, err := r.access.CanAccess(ctx, proof.item.LibraryID, proof.account, proof.profile, false)
	if err != nil || !allowed {
		return
	}
	access, err := r.access.Get(ctx, proof.item.LibraryID)
	if err != nil {
		return
	}
	library, err := r.remote.GetLibrary(ctx, proof.item.LibraryID)
	if err != nil || library == nil || kidsVODLibraryRoute(*library) != proof.libraryRoute {
		return
	}
	profileScope, ok := kidsVODProfileScope(r.parent, proof.profile)
	if !ok || profileScope != initialProfileScope || !r.parent.remoteItemAllowed(proof.profile) {
		return
	}
	expires := proof.facts.ExpiresAt
	if entry.ExpiresAt.Before(expires) {
		expires = entry.ExpiresAt
	}
	if !expires.After(r.now()) {
		return
	}
	receipt := kidsVODLibraryReceipt{proof: *proof, stamp: stamp, config: configuration, settings: settings, profileScope: profileScope, report: entry.MediaEvidenceReportRevision, accessScope: kidsVODHash(access), entryFingerprint: kidsVODEntryFingerprint(entry), observed: proof.facts.ObservedAt, expires: expires}
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, old := range r.receipts {
		if !old.expires.After(r.now()) {
			delete(r.receipts, key)
		}
	}
	if len(r.receipts) >= 512 {
		var oldest string
		var at time.Time
		for key, old := range r.receipts {
			if oldest == "" || old.observed.Before(at) {
				oldest, at = key, old.observed
			}
		}
		delete(r.receipts, oldest)
	}
	r.receipts[id] = receipt
}

func (r *kidsVODResources) ResolveCacheOnly(ctx context.Context, subject kidsvod.Subject, ref kidsvod.ResourceRef) (kidsvod.Binding, error) {
	return r.resolveOwner(ctx, subject.AccountID(), subject.ProfileID(), subject.ClientID(), ref)
}
func (r *kidsVODResources) resolveOwner(ctx context.Context, account, profile, client string, ref kidsvod.ResourceRef) (kidsvod.Binding, error) {
	unknown := func() (kidsvod.Binding, error) {
		return kidsvod.Binding{}, errors.New("current proven library resource unavailable")
	}
	if r == nil || r.parent == nil || r.remote == nil || r.access == nil || ref.Kind() != kidsvod.Prequeue || ctx.Err() != nil {
		return unknown()
	}
	r.mu.Lock()
	receipt, ok := r.receipts[ref.ID()]
	r.mu.Unlock()
	if !ok || !receipt.expires.After(r.now()) || receipt.proof.account != account || receipt.proof.profile != profile || receipt.proof.client != client {
		return unknown()
	}
	entry, stamp, ok := r.parent.store.SnapshotForAuthorization(ref.ID())
	if !ok || stamp != receipt.stamp || entry.Status != playback.PrequeueStatusReady || entry.ReuseDisabled || entry.MigrationAdopted || kidsVODEntryFingerprint(entry) != receipt.entryFingerprint {
		return unknown()
	}
	if r.parent.preparationConfigurationKey() != receipt.config || automaticLibraryScope(r.parent.prequeueSettingsScopeKey(profile, client, entry.TitleID), entry.AutomaticLibrarySelection) != receipt.settings {
		return unknown()
	}
	if current, ok := kidsVODProfileScope(r.parent, profile); !ok || current != receipt.profileScope || !r.parent.remoteItemAllowed(profile) {
		return unknown()
	}
	item, err := r.remote.GetItem(ctx, receipt.proof.item.ID)
	if err != nil || item == nil || remoteLibraryIdentity(*item) != remoteLibraryIdentity(receipt.proof.item) || item.IsMissing {
		return unknown()
	}
	library, err := r.remote.GetLibrary(ctx, item.LibraryID)
	if err != nil || library == nil || library.Provider != entry.LibrarySourceType || kidsVODLibraryRoute(*library) != receipt.proof.libraryRoute {
		return unknown()
	}
	allowed, err := r.access.CanAccess(ctx, item.LibraryID, account, profile, false)
	if err != nil || !allowed {
		return unknown()
	}
	access, err := r.access.Get(ctx, item.LibraryID)
	if err != nil || kidsVODHash(access) != receipt.accessScope {
		return unknown()
	}
	if value := r.currentEvidence(entry, client); value == nil || value.Probed == nil || entry.MediaEvidenceReportRevision != receipt.report {
		return unknown()
	}
	if ctx.Err() != nil {
		return unknown()
	}
	version := mediaevidence.FileVersion(receipt.proof.path, remoteLibraryIdentity(*item), receipt.proof.size)
	return kidsvod.Binding{Resource: ref, AccountID: account, ProfileID: profile, CatalogID: receipt.proof.catalogue.ID, MediaType: entry.MediaType, Episode: kidsVODEpisodeCopy(receipt.proof.episode), SourceVersion: version, Revision: kidsVODHash([]any{receipt.entryFingerprint, receipt.profileScope, receipt.report, receipt.accessScope, receipt.config, receipt.stamp.Revision()}), ObservedAt: receipt.observed, ExpiresAt: receipt.expires}, nil
}

func kidsVODLibraryRoute(library models.RemoteMediaLibrary) string {
	return kidsVODHash([]any{library.ID, library.Provider, library.Type, library.AccountID, library.ServerID, library.ServerURL, library.ExternalLibraryID})
}
func (r *kidsVODResources) captureRoutes(ctx context.Context, items []models.RemoteMediaItem) map[string]string {
	if r == nil || r.remote == nil {
		return nil
	}
	out := map[string]string{}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	for _, item := range items {
		if bounded.Err() != nil {
			return nil
		}
		if _, ok := out[item.LibraryID]; ok {
			continue
		}
		library, err := r.remote.GetLibrary(bounded, item.LibraryID)
		if err == nil && library != nil {
			out[item.LibraryID] = kidsVODLibraryRoute(*library)
		}
	}
	return out
}

// currentEvidence deliberately avoids reportContextForPrequeue: that legacy
// helper calls store.Get, whose validator can perform source I/O. The caller
// already holds an isolated current snapshot and all report scope fields.
func (r *kidsVODResources) currentEvidence(entry *playback.PrequeueEntry, client string) *models.MediaEvidence {
	if entry == nil || entry.ReuseDisabled || entry.MediaEvidence == nil || entry.SelectedResult == nil || client != entry.MediaEvidenceClientID {
		return nil
	}
	value := models.CurrentMediaEvidence(entry.MediaEvidence, r.now())
	if value == nil || value.SourceVersion != mediaevidence.FileVersion(entry.StreamPath, preparationEvidenceMarker(entry), entry.FileSize) {
		return nil
	}
	if service := r.parent.badStreamsSvc; service != nil {
		scope := reportContext(r.parent.trackPreferenceResolver(), entry.UserID, client, entry.TitleID, entry.TargetEpisode)
		scope.AudioTrackIndex = &entry.SelectedAudioTrack
		if service.MatchFor(*entry.SelectedResult, scope) != nil || service.RevisionFor(scope) != entry.MediaEvidenceReportRevision {
			return nil
		}
	}
	return value
}
