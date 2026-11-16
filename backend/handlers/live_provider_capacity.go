package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/livecapacity"
	"novastream/internal/netproxy"
)

const providerCapacityBodyLimit = 64 << 10
const providerCapacityCheckTimeout = 5 * time.Second
const maxProviderCapacityChecks = 128

// Only numeric account facts are returned. Provider messages, URLs, credentials
// and server_info are intentionally excluded from the management response.
// ProviderExternalUsage deliberately has no numeric count or range. Aggregate
// provider counters and local reservations do not share a trusted measurement.
type ProviderExternalUsage struct {
	State  string `json:"state"`
	Reason string `json:"reason"`
}

type ProviderCapacityReport struct {
	SourceID                  string                `json:"sourceId"`
	State                     string                `json:"state"`
	Message                   string                `json:"message"`
	LimitOrigin               string                `json:"limitOrigin"`
	ReportedLimit             *int                  `json:"reportedLimit,omitempty"`
	ProviderActiveConnections *int                  `json:"providerActiveConnections,omitempty"`
	CheckedAt                 *time.Time            `json:"checkedAt,omitempty"`
	CacheExpiresAt            *time.Time            `json:"cacheExpiresAt,omitempty"`
	Cached                    bool                  `json:"cached"`
	ConfiguredLimit           int                   `json:"configuredLimit"`
	SharedConfiguredLimit     int                   `json:"sharedConfiguredLimit"`
	LocalConnections          int                   `json:"localConnections"`
	LocalSnapshotCompletedAt  time.Time             `json:"localSnapshotCompletedAt"`
	ExternalUsage             ProviderExternalUsage `json:"externalUsage"`
	EnforcedLimit             int                   `json:"enforcedLimit"`
	AutomaticAdoptionEnabled  bool                  `json:"automaticAdoptionEnabled"`
	AutomaticLimit            *int                  `json:"automaticLimit,omitempty"`
	AutomaticExpiresAt        *time.Time            `json:"automaticExpiresAt,omitempty"`
	EnforcementOrigin         string                `json:"enforcementOrigin"`
}

type providerCapacityEntry struct {
	done     chan struct{}
	result   ProviderCapacityReport
	expires  time.Time
	verified bool
}

// The key is a private digest of the saved endpoint, credentials and proxy.
// Aliases coalesce checks; cancellation of one HTTP waiter cannot cancel peers.
type providerCapacityChecks struct {
	mu      sync.Mutex
	entries map[string]*providerCapacityEntry
	now     func() time.Time
}

func (c *providerCapacityChecks) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now().UTC()
}

func (c *providerCapacityChecks) check(ctx context.Context, key string, fetch func(context.Context) ProviderCapacityReport) (ProviderCapacityReport, bool, error) {
	if err := ctx.Err(); err != nil {
		return ProviderCapacityReport{}, false, err
	}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]*providerCapacityEntry)
	}
	entry := c.entries[key]
	cached := entry != nil
	if entry != nil && !entry.expires.IsZero() && !c.clock().Before(entry.expires) {
		delete(c.entries, key)
		entry = nil
		cached = false
	}
	if entry == nil {
		if len(c.entries) >= maxProviderCapacityChecks {
			oldestKey := ""
			var oldest time.Time
			for candidate, value := range c.entries {
				if !value.expires.IsZero() && (oldestKey == "" || value.expires.Before(oldest)) {
					oldestKey, oldest = candidate, value.expires
				}
			}
			if oldestKey == "" {
				c.mu.Unlock()
				return ProviderCapacityReport{}, false, errors.New("provider capacity checks are busy")
			}
			delete(c.entries, oldestKey)
		}
		entry = &providerCapacityEntry{done: make(chan struct{})}
		c.entries[key] = entry
		go func() {
			bounded, cancel := context.WithTimeout(context.Background(), providerCapacityCheckTimeout)
			defer cancel()
			result := fetch(bounded)
			checked := c.clock()
			result.CheckedAt = &checked
			ttl := time.Minute
			if result.State == "unavailable" {
				ttl = 30 * time.Second
			}
			expires := checked.Add(ttl)
			result.CacheExpiresAt = &expires
			c.mu.Lock()
			entry.result, entry.expires = result, expires
			close(entry.done)
			c.mu.Unlock()
		}()
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ProviderCapacityReport{}, cached, ctx.Err()
	case <-entry.done:
		return entry.result, cached, nil
	}
}

func providerCapacityIdentity(source resolvedM3USource) string {
	encoded, _ := json.Marshal(source)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// No action means the account-information endpoint, never a channel or stream.
// Xtream M3U exports are recognized without fetching or exposing the playlist.
func providerCapacityEndpoint(source resolvedM3USource) (*url.URL, bool) {
	var endpoint *url.URL
	var err error
	var username, password string
	switch source.Mode {
	case "xtream":
		endpoint, err = url.Parse(strings.TrimRight(source.XtreamHost, "/"))
		username, password = source.XtreamUsername, source.XtreamPassword
		if err != nil || endpoint.RawQuery != "" {
			return nil, false
		}
		endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/player_api.php"
	case "m3u", "":
		endpoint, err = url.Parse(source.PlaylistURL)
		if err != nil || !strings.EqualFold(path.Base(endpoint.Path), "get.php") {
			return nil, false
		}
		query, queryErr := url.ParseQuery(endpoint.RawQuery)
		if queryErr != nil || len(query["username"]) != 1 || len(query["password"]) != 1 {
			return nil, false
		}
		username, password = query.Get("username"), query.Get("password")
		endpoint.Path = path.Join(path.Dir(endpoint.Path), "player_api.php")
	default:
		return nil, false
	}
	if endpoint == nil {
		return nil, false
	}
	endpoint.Scheme = strings.ToLower(endpoint.Scheme)
	if endpoint.User != nil || endpoint.Hostname() == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || username == "" || password == "" {
		return nil, false
	}
	endpoint.RawPath, endpoint.Fragment = "", ""
	endpoint.Scheme = strings.ToLower(endpoint.Scheme)
	endpoint.Host = strings.ToLower(endpoint.Host)
	endpoint.RawQuery = url.Values{"username": {username}, "password": {password}}.Encode()
	return endpoint, true
}

func providerAccountNumber(raw json.RawMessage) (int, bool) {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		value = string(raw)
	}
	parsed, err := strconv.Atoi(value)
	return parsed, err == nil && parsed >= 0 && parsed <= 1_000_000
}

func parseProviderCapacity(body []byte) ProviderCapacityReport {
	unknown := ProviderCapacityReport{State: "unknown", LimitOrigin: "unknown", Message: "The provider did not report a positive connection limit. Enter a confirmed manual limit."}
	unavailable := ProviderCapacityReport{State: "unavailable", LimitOrigin: "unknown", Message: "Provider account information is unavailable. Keep or enter a confirmed manual limit."}
	var response struct {
		UserInfo struct {
			Auth   json.RawMessage `json:"auth"`
			Status string          `json:"status"`
			Limit  json.RawMessage `json:"max_connections"`
			Active json.RawMessage `json:"active_cons"`
		} `json:"user_info"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if decoder.Decode(&response) != nil || decoder.Decode(new(any)) != io.EOF {
		return unavailable
	}
	authenticated, valid := providerAccountNumber(response.UserInfo.Auth)
	if !valid || authenticated != 1 || !strings.EqualFold(strings.TrimSpace(response.UserInfo.Status), "active") {
		return unavailable
	}
	if active, valid := providerAccountNumber(response.UserInfo.Active); valid {
		unknown.ProviderActiveConnections = &active
	}
	if limit, valid := providerAccountNumber(response.UserInfo.Limit); valid && limit > 0 {
		unknown.State, unknown.LimitOrigin = "available", "provider_account"
		unknown.ReportedLimit = &limit
		unknown.Message = "The provider reports this connection limit. Use it in the form and save your settings to apply it."
	}
	return unknown
}

func (h *LiveHandler) fetchProviderCapacity(ctx context.Context, endpoint *url.URL, proxyURL string) ProviderCapacityReport {
	failed := ProviderCapacityReport{State: "unavailable", LimitOrigin: "unknown", Message: "Provider account information is unavailable. Keep or enter a confirmed manual limit."}
	// Never send account credentials to a redirect target or return a raw error
	// whose URL or provider response might contain those credentials.
	baseClient := h.client
	if strings.TrimSpace(proxyURL) != "" {
		var err error
		baseClient, err = netproxy.NewHTTPClient(providerCapacityCheckTimeout, proxyURL)
		if err != nil {
			return failed
		}
		if transport, ok := baseClient.Transport.(*http.Transport); ok {
			defer transport.CloseIdleConnections()
		}
	}
	if baseClient == nil {
		baseClient = &http.Client{Timeout: providerCapacityCheckTimeout}
	}
	client := *baseClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return failed
	}
	request.Header.Set("User-Agent", "MediaStorm")
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return failed
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return failed
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, providerCapacityBodyLimit+1))
	if err != nil || len(body) > providerCapacityBodyLimit {
		return failed
	}
	return parseProviderCapacity(body)
}

func savedCapacitySource(settings config.Settings, id string) (resolvedM3USource, bool) {
	sources := resolvedLiveSources(buildGlobalLiveSource(settings))
	// Loading a legacy connection creates a single unnamed source before it
	// has a persisted ID. Keep the explicit default alias usable until saved.
	if id == "default" && len(sources) == 1 && len(settings.Live.Sources) == 1 && settings.Live.Sources[0].ID == "" {
		return sources[0], true
	}
	for _, source := range sources {
		if source.ID == id {
			return source, true
		}
	}
	return resolvedM3USource{}, false
}

// CheckProviderCapacity reads only saved, enabled global sources. It never
// saves a limit, starts a stream, or runs from a playback admission path.
func (h *LiveHandler) CheckProviderCapacity(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if !auth.IsMaster(r) {
		http.Error(w, "Administrator access required", http.StatusForbidden)
		return
	}
	if session, ok := auth.GetSession(r); ok && session.Scope != "" {
		http.Error(w, "Account management session required", http.StatusForbidden)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var input struct {
		SourceID string `json:"sourceId"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.SourceID == "" || len(input.SourceID) > 256 {
		http.Error(w, "A saved sourceId is required", http.StatusBadRequest)
		return
	}
	if h.cfgManager == nil {
		http.Error(w, "Live settings unavailable", http.StatusServiceUnavailable)
		return
	}
	settings, err := h.cfgManager.Load()
	if err != nil {
		http.Error(w, "Live settings unavailable", http.StatusServiceUnavailable)
		return
	}
	source, found := savedCapacitySource(settings, input.SourceID)
	if !found {
		http.Error(w, "Saved enabled source not found", http.StatusNotFound)
		return
	}
	observationKey := ""
	report := ProviderCapacityReport{State: "unsupported", LimitOrigin: "unknown", Message: "This source has no supported account-information endpoint. Enter a confirmed manual limit."}
	if endpoint, supported := providerCapacityEndpoint(source); supported {
		key := liveDigest(endpoint.String(), source.ProxyURL)
		observationKey = key
		var cached bool
		report, cached, err = h.providerCapacity.check(r.Context(), key, func(ctx context.Context) ProviderCapacityReport {
			return h.fetchProviderCapacity(ctx, endpoint, source.ProxyURL)
		})
		report.Cached = cached
		if err != nil {
			if r.Context().Err() == nil {
				http.Error(w, "Provider checks are busy. Try again shortly.", http.StatusTooManyRequests)
			}
			return
		}
	}
	latest, err := h.cfgManager.Load()
	current, valid := savedCapacitySource(latest, input.SourceID)
	if err != nil || !valid || providerCapacityIdentity(source) != providerCapacityIdentity(current) {
		http.Error(w, "Source settings changed. Check the saved source again.", http.StatusConflict)
		return
	}
	if observationKey != "" {
		h.providerCapacity.confirmObservation(observationKey, report)
	}
	report.SourceID, report.ConfiguredLimit = input.SourceID, source.MaxStreams
	report.SharedConfiguredLimit = source.MaxStreams
	bucket, _ := deriveLiveSourceBucket(source.Mode, source)
	for _, alias := range resolveLiveStreamTargets(buildGlobalLiveSource(latest), nil) {
		if alias.BucketKey == bucket && alias.MaxStreams > 0 && (report.SharedConfiguredLimit <= 0 || alias.MaxStreams < report.SharedConfiguredLimit) {
			report.SharedConfiguredLimit = alias.MaxStreams
		}
	}
	var automatic livecapacity.AutomaticLimit
	report.LocalConnections, report.EnforcedLimit, automatic = livecapacity.Default.UsageWithAutomaticLimit(bucket, report.SharedConfiguredLimit)
	// This is our local sampling completion, separately from the older cached
	// provider-response/check completion. Neither is a provider measurement time.
	report.LocalSnapshotCompletedAt = time.Now().UTC()
	report.ExternalUsage = ProviderExternalUsage{State: "unknown", Reason: "provider_count_unavailable"}
	if report.ProviderActiveConnections != nil {
		report.ExternalUsage.Reason = "no_common_trusted_measurement"
	}

	report.AutomaticAdoptionEnabled = latest.Live.AutoAdoptProviderLimits
	report.EnforcementOrigin = "manual"
	if automatic.Limit > 0 {
		report.AutomaticLimit = &automatic.Limit
		report.AutomaticExpiresAt = &automatic.ExpiresAt
		report.EnforcementOrigin = "provider_extra_cap"
		report.Message = "At this check, the fresh provider report adds an extra cap for new connections. Manual limits stay in place and running streams and recordings are retained. Reports are not refreshed automatically."
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}

// availableLimit only peeks at a completed private observation. It never
// starts a fetch, waits on an in-flight check, or refreshes its timestamps.
func (c *providerCapacityChecks) availableLimit(key string) livecapacity.AutomaticLimit {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil || !entry.verified {
		return livecapacity.AutomaticLimit{}
	}
	select {
	case <-entry.done:
	default:
		return livecapacity.AutomaticLimit{}
	}
	r := entry.result
	now := c.clock()
	if r.State != "available" || r.LimitOrigin != "provider_account" || r.ReportedLimit == nil || *r.ReportedLimit <= 0 || *r.ReportedLimit > 1_000_000 || r.CheckedAt == nil || r.CheckedAt.After(now) || r.CacheExpiresAt == nil || !r.CacheExpiresAt.After(now) || !r.CacheExpiresAt.After(*r.CheckedAt) || r.CacheExpiresAt.Sub(*r.CheckedAt) > time.Minute || !entry.expires.Equal(*r.CacheExpiresAt) {
		return livecapacity.AutomaticLimit{}
	}
	return livecapacity.AutomaticLimit{Limit: *r.ReportedLimit, ExpiresAt: *r.CacheExpiresAt}
}

// AutomaticProviderLimit is the shared manager's cache-only policy reader.
// Lock order is manager -> current settings file -> checks; report production
// releases checks before querying manager usage. No provider or manager call
// is permitted here. Unknown or conflicting account aliases use manual policy.
// The existing settings file read occurs under the manager lock; no latency
// guarantee or automatic provider refresh is implied.
func (h *LiveHandler) AutomaticProviderLimit(bucket string) livecapacity.AutomaticLimit {
	if h == nil || h.cfgManager == nil {
		return livecapacity.AutomaticLimit{}
	}
	settings, err := h.cfgManager.Load()
	if err != nil || !settings.Live.AutoAdoptProviderLimits {
		return livecapacity.AutomaticLimit{}
	}
	identity := ""
	for _, source := range resolvedLiveSources(buildGlobalLiveSource(settings)) {
		sourceBucket, _ := deriveLiveSourceBucket(source.Mode, source)
		if sourceBucket != bucket {
			continue
		}
		endpoint, supported := providerCapacityEndpoint(source)
		if !supported {
			return livecapacity.AutomaticLimit{}
		}
		key := liveDigest(endpoint.String(), source.ProxyURL)
		if identity != "" && identity != key {
			return livecapacity.AutomaticLimit{}
		}
		identity = key
	}
	if identity == "" {
		return livecapacity.AutomaticLimit{}
	}
	return h.providerCapacity.availableLimit(identity)
}

// A completed fetch alone is not an adopted observation. Only the existing
// management producer, after its current saved-source recheck, confirms the
// exact returned observation. Admission cannot race ahead of that recheck.
func (c *providerCapacityChecks) confirmObservation(key string, report ProviderCapacityReport) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil || report.CheckedAt == nil || report.CacheExpiresAt == nil {
		return
	}
	select {
	case <-entry.done:
	default:
		return
	}
	if entry.result.CheckedAt != nil && entry.result.CacheExpiresAt != nil && entry.result.CheckedAt.Equal(*report.CheckedAt) && entry.result.CacheExpiresAt.Equal(*report.CacheExpiresAt) {
		entry.verified = true
	}
}
