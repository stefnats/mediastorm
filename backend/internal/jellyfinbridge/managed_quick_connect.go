package jellyfinbridge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/jellyfinbridge/dto"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/profileunlock"
)

// Each listener owns its issuer and original pending objects. Client details
// are response DATA; only the concrete issuer's genuine approval can mint.
type managedQuickConnect struct {
	runtime    *ManagedPasswordLogin
	issuer     *profileunlock.BridgeQuickConnectIssuer
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	closed     bool
	entries    map[string]*managedQuickConnectEntry
	operations sync.WaitGroup
	once       sync.Once
	done       chan struct{}
}
type managedQuickConnectEntry struct {
	challenge *profileunlock.BridgeQuickConnectChallenge
	approval  *profileunlock.BridgeQuickConnectApproval
	client    clientInfo
	details   profileunlock.BridgeQuickConnectDetails
	phase     int
}

func (s *Server) managedQuickConnectRequired() bool {
	storage, known := s.deps.Sessions.(interface{ UsesPostgres() bool })
	return !known || storage.UsesPostgres() || s.deps.ManagedPassword != nil
}
func newManagedQuickConnect(runtime *ManagedPasswordLogin) (*managedQuickConnect, error) {
	if !runtime.ready() {
		return nil, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	ctx, cancel := context.WithCancel(runtime.lifetime)
	issuer, err := runtime.pins.NewBridgeQuickConnectIssuer(ctx, runtime.manager, runtime.security, runtime.coordinator)
	if err != nil {
		cancel()
		return nil, err
	}
	return &managedQuickConnect{runtime: runtime, issuer: issuer, ctx: ctx, cancel: cancel, entries: make(map[string]*managedQuickConnectEntry), done: make(chan struct{})}, nil
}
func (m *managedQuickConnect) ready() bool {
	return m != nil && m.runtime.ready() && m.ctx.Err() == nil && m.issuer != nil
}
func (m *managedQuickConnect) close() {
	if m == nil {
		return
	}
	m.once.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		m.cancel()
		m.issuer.Close()
		m.operations.Wait()
		m.mu.Lock()
		entries := m.entries
		m.entries = make(map[string]*managedQuickConnectEntry)
		m.mu.Unlock()
		for _, entry := range entries {
			closeManagedQuickConnectEntry(entry)
		}
		close(m.done)
	})
	<-m.done
}
func closeManagedQuickConnectEntry(entry *managedQuickConnectEntry) {
	if entry == nil {
		return
	}
	if entry.approval != nil {
		entry.approval.Close()
	}
	if entry.challenge != nil {
		entry.challenge.Close()
	}
}
func (m *managedQuickConnect) drop(entry *managedQuickConnectEntry) {
	if entry == nil {
		return
	}
	m.mu.Lock()
	if m.entries[entry.details.Secret] == entry {
		delete(m.entries, entry.details.Secret)
	}
	m.mu.Unlock()
	closeManagedQuickConnectEntry(entry)
}
func (m *managedQuickConnect) attach(entry *managedQuickConnectEntry) bool {
	m.mu.Lock()
	ok := !m.closed && m.ready() && entry.details.ExpiresAt.After(time.Now()) && m.entries[entry.details.Secret] == nil
	if ok {
		m.entries[entry.details.Secret] = entry
	}
	m.mu.Unlock()
	if !ok {
		closeManagedQuickConnectEntry(entry)
	}
	return ok
}
func (m *managedQuickConnect) attachApproval(entry *managedQuickConnectEntry, approval *profileunlock.BridgeQuickConnectApproval) bool {
	m.mu.Lock()
	ok := !m.closed && m.ready() && m.entries[entry.details.Secret] == entry && entry.phase == 1 && entry.details.ExpiresAt.After(time.Now())
	if ok {
		entry.approval = approval
		entry.phase = 2
	}
	m.mu.Unlock()
	if !ok {
		approval.Close()
	}
	return ok
}
func (m *managedQuickConnect) claimApproval(entry *managedQuickConnectEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.entries[entry.details.Secret] != entry || entry.phase != 0 {
		return false
	}
	entry.phase = 1
	return true
}
func (m *managedQuickConnect) collectable(entry *managedQuickConnectEntry) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed && m.ready() && m.entries[entry.details.Secret] == entry && entry.phase == 2 && entry.approval != nil
}
func (m *managedQuickConnect) lookup(secret, code string) *managedQuickConnectEntry {
	m.mu.Lock()
	var found *managedQuickConnectEntry
	expired := make([]*managedQuickConnectEntry, 0)
	for key, entry := range m.entries {
		if !entry.details.ExpiresAt.After(time.Now()) {
			delete(m.entries, key)
			expired = append(expired, entry)
			continue
		}
		if secret != "" && key == secret || code != "" && entry.details.Code == code {
			found = entry
		}
	}
	if m.closed || !m.ready() {
		found = nil
	}
	m.mu.Unlock()
	for _, entry := range expired {
		closeManagedQuickConnectEntry(entry)
	}
	return found
}

// The same original bounded request is used for all policy/collector work.
// Approver authentication may establish its genuine frame once afterwards.
func (m *managedQuickConnect) begin(w http.ResponseWriter, incoming *http.Request) (*http.Request, *http.ResponseController, func(), error) {
	noop := func() {}
	if !m.ready() {
		return nil, nil, noop, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	ctx, cancel := context.WithTimeout(incoming.Context(), 30*time.Second)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		cancel()
		return nil, nil, noop, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	m.operations.Add(1)
	m.mu.Unlock()
	r := incoming.Clone(ctx)
	info := requestClientInfo(incoming)
	agent := strings.TrimSpace(strings.Join([]string{info.Client, info.Version, info.Device}, " "))
	if agent == "" {
		agent = incoming.UserAgent()
	}
	r.Header.Set("User-Agent", agent)
	if info.DeviceID != "" {
		r.Header.Set("X-Client-ID", info.DeviceID)
	}
	transport := http.NewResponseController(w)
	deadline, _ := ctx.Deadline()
	_ = transport.SetReadDeadline(deadline)
	_ = transport.SetWriteDeadline(deadline)
	interrupted := make(chan struct{})
	stopIO := context.AfterFunc(ctx, func() {
		defer close(interrupted)
		_ = transport.SetReadDeadline(time.Now())
		_ = transport.SetWriteDeadline(time.Now())
	})
	retired := make(chan struct{})
	stopLife := context.AfterFunc(m.ctx, func() { defer close(retired); cancel() })
	finish := func() {
		if !stopIO() {
			<-interrupted
		}
		if !stopLife() {
			<-retired
		}
		cancel()
		m.operations.Done()
	}
	if ctx.Err() != nil || !m.ready() {
		return r, transport, finish, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	return r, transport, finish, nil
}
func managedQuickConnectStatus(err error) int {
	if errors.Is(err, profileunlock.ErrBridgeQuickConnectChanged) || errors.Is(err, profileunlock.ErrBridgeQuickConnectUnsupported) || errors.Is(err, profileunlock.ErrBridgeQuickConnectUsed) || errors.Is(err, datastore.ErrProfilePinAccessChanged) {
		return http.StatusUnauthorized
	}
	return http.StatusServiceUnavailable
}
func managedQuickConnectError(w http.ResponseWriter, err error) {
	writeError(w, managedQuickConnectStatus(err), "Error processing request.")
}
func validQuickConnectClient(value string) bool {
	return value != "" && len(value) <= 256 && value == strings.TrimSpace(value) && utf8.ValidString(value)
}

// This outer native hold owns no PG connection and confers no SQL permission.
// It keeps one original policy observation valid through joined SQL cleanup,
// the DATA-only response query and output. Actual mint authorization is core's.
func (m *managedQuickConnect) policy(r *http.Request) (*config.BridgeQuickConnectPolicyObservation, *settingscommit.ProgressFileHold, config.BridgeQuickConnectPolicyValues, error) {
	var zero config.BridgeQuickConnectPolicyValues
	if !m.ready() || r.Context().Err() != nil {
		return nil, nil, zero, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	hold, err := m.runtime.coordinator.TryProgressFileHold(r.Context())
	if err != nil {
		return nil, nil, zero, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	original, err := m.runtime.security.CaptureBridgeQuickConnectPolicy(r.Context())
	if err != nil {
		_ = hold.Close()
		return nil, nil, zero, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	fence, err := m.runtime.security.BindBridgeQuickConnectPolicy(r.Context(), original, hold)
	if err != nil {
		_ = hold.Close()
		return nil, nil, zero, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	values, err := fence.Values()
	if err != nil {
		_ = hold.Close()
		return nil, nil, zero, profileunlock.ErrBridgeQuickConnectUnavailable
	}
	return original, hold, values, nil
}
func (m *managedQuickConnect) current(r *http.Request, original *config.BridgeQuickConnectPolicyObservation, hold *settingscommit.ProgressFileHold, expiry time.Time) bool {
	if !m.ready() || r.Context().Err() != nil || !expiry.After(time.Now()) {
		return false
	}
	fence, err := m.runtime.security.BindBridgeQuickConnectPolicy(r.Context(), original, hold)
	if err != nil {
		return false
	}
	values, err := fence.Values()
	return err == nil && values.BridgeEnabled && values.QuickConnectEnabled && m.ready() && r.Context().Err() == nil && expiry.After(time.Now())
}
func quickConnectWire(entry *managedQuickConnectEntry, details profileunlock.BridgeQuickConnectDetails) dto.QuickConnectResult {
	return dto.QuickConnectResult{Authenticated: details.Approved, Secret: details.Secret, Code: details.Code, DeviceID: entry.client.DeviceID, DeviceName: entry.client.Device, AppName: entry.client.Client, AppVersion: entry.client.Version, DateAdded: details.CreatedAt.UTC()}
}

func (s *Server) handleManagedQuickConnectEnabled(w http.ResponseWriter, incoming *http.Request) {
	m := s.managedQC
	r, _, finish, err := m.begin(w, incoming)
	defer finish()
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	_, hold, values, err := m.policy(r)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	defer hold.Close()
	writeJSON(w, http.StatusOK, values.BridgeEnabled && values.QuickConnectEnabled)
}
func (s *Server) handleManagedQuickConnectInitiate(w http.ResponseWriter, incoming *http.Request) {
	m := s.managedQC
	r, _, finish, err := m.begin(w, incoming)
	defer finish()
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	original, hold, values, err := m.policy(r)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	defer hold.Close()
	if !values.BridgeEnabled || !values.QuickConnectEnabled {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	info := requestClientInfo(r)
	if !validQuickConnectClient(info.DeviceID) || !validQuickConnectClient(info.Client) || !validQuickConnectClient(info.Device) || !validQuickConnectClient(info.Version) {
		writeError(w, http.StatusBadRequest, "Client, Device, DeviceId and Version are required")
		return
	}
	// Purge the wrapper only after each actual original child cleanup joins.
	m.lookup("", "")
	challenge, err := m.issuer.Initiate(r, info.DeviceID)
	if err != nil {
		if errors.Is(err, profileunlock.ErrBridgeQuickConnectBusy) {
			writeError(w, http.StatusTooManyRequests, "Too many pending requests. Try again later.")
		} else {
			managedQuickConnectError(w, err)
		}
		return
	}
	details, err := challenge.Details()
	if err != nil {
		challenge.Close()
		managedQuickConnectError(w, err)
		return
	}
	info.Token = ""
	entry := &managedQuickConnectEntry{challenge: challenge, client: info, details: details}
	deadline, _ := r.Context().Deadline()
	if !m.current(r, original, hold, deadline) || !m.attach(entry) {
		closeManagedQuickConnectEntry(entry)
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, quickConnectWire(entry, details))
}
func (s *Server) handleManagedQuickConnectConnect(w http.ResponseWriter, incoming *http.Request) {
	m := s.managedQC
	r, _, finish, err := m.begin(w, incoming)
	defer finish()
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	original, hold, values, err := m.policy(r)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	defer hold.Close()
	if !values.BridgeEnabled || !values.QuickConnectEnabled {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	secret := strings.TrimSpace(queryValue(r, "secret"))
	if len(secret) != 64 {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	entry := m.lookup(secret, "")
	if entry == nil {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	details, err := entry.challenge.Details()
	deadline, _ := r.Context().Deadline()
	if err != nil || !m.current(r, original, hold, deadline) {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	// A core approval may exist before the HTTP owner has completed its
	// original policy/frame handoff. Poll DATA can only deny readiness; the
	// actual collector still performs every genuine core Commit/Release check.
	details.Approved = details.Approved && m.collectable(entry)
	writeJSON(w, http.StatusOK, quickConnectWire(entry, details))
}
func (s *Server) handleQuickConnectAuthorizeRoute(w http.ResponseWriter, r *http.Request) {
	if s.managedQuickConnectRequired() {
		s.handleManagedQuickConnectAuthorize(w, r)
		return
	}
	s.requireAuth(s.handleQuickConnectAuthorize)(w, r)
}
func (s *Server) handleManagedQuickConnectAuthorize(w http.ResponseWriter, incoming *http.Request) {
	m := s.managedQC
	r, _, finish, err := m.begin(w, incoming)
	defer finish()
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	original, hold, values, err := m.policy(r)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	defer hold.Close()
	if !values.BridgeEnabled || !values.QuickConnectEnabled {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	info := requestClientInfo(r)
	if info.Token == "" || !validQuickConnectClient(info.DeviceID) {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	checked, release, err := m.runtime.pins.Authenticate(r, info.Token, info.DeviceID)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	defer release()
	session, _, err := m.runtime.pins.Identity(checked)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	if session.Scope != models.SessionScopeJellyfin || session.ClientID != info.DeviceID || session.ScopeResource == "" {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	if user := queryValue(r, "userId"); user != "" && normalizeID(user) != deriveID(refUser, session.ScopeResource) {
		writeError(w, http.StatusForbidden, "Quick connect can only sign devices in as yourself")
		return
	}
	limitKey := "quickconnect:" + session.AccountID
	if !s.login.attempt(limitKey) {
		writeError(w, http.StatusTooManyRequests, "Too many attempts. Try again later.")
		return
	}
	code := strings.TrimSpace(queryValue(r, "code"))
	if len(code) != 6 {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	entry := m.lookup("", code)
	if entry == nil {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	claim := m.claimApproval(entry)
	if !claim {
		writeError(w, http.StatusBadRequest, "Error processing request.")
		return
	}
	approval, err := m.issuer.ApproveAuthenticated(checked, entry.challenge)
	release()
	if err != nil {
		m.mu.Lock()
		if m.entries[entry.details.Secret] == entry && entry.phase == 1 {
			entry.phase = 0
		}
		m.mu.Unlock()
		managedQuickConnectError(w, err)
		return
	}
	deadline, _ := r.Context().Deadline()
	if !m.current(r, original, hold, deadline) {
		approval.Close()
		m.drop(entry)
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	if !m.attachApproval(entry, approval) {
		m.drop(entry)
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	s.login.undoLast(limitKey)
	writeJSON(w, http.StatusOK, true)
}
func (s *Server) handleManagedAuthenticateWithQuickConnect(w http.ResponseWriter, incoming *http.Request) {
	m := s.managedQC
	r, transport, finish, err := m.begin(w, incoming)
	defer finish()
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	var body struct {
		Secret string `json:"Secret"`
	}
	if err = json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&body); err != nil {
		deadline, _ := r.Context().Deadline()
		if r.Context().Err() != nil || !deadline.After(time.Now()) {
			managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		} else {
			writeError(w, http.StatusBadRequest, "Invalid request body")
		}
		return
	}
	original, hold, values, err := m.policy(r)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	defer hold.Close()
	if !values.BridgeEnabled || !values.QuickConnectEnabled {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	secret := strings.TrimSpace(body.Secret)
	if len(secret) != 64 {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	entry := m.lookup(secret, "")
	if entry == nil {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	m.mu.Lock()
	claim := !m.closed && m.entries[secret] == entry && entry.phase == 2 && entry.approval != nil
	if claim {
		entry.phase = 3
	}
	approval := entry.approval
	m.mu.Unlock()
	if !claim {
		writeError(w, http.StatusNotFound, "Unable to find request")
		return
	}
	defer m.drop(entry)
	info := requestClientInfo(r)
	if !validQuickConnectClient(info.DeviceID) || info.DeviceID != entry.client.DeviceID {
		m.drop(entry)
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectChanged)
		return
	}
	receipt, err := m.issuer.Commit(r, entry.challenge, approval, secret, info.DeviceID)
	if err != nil {
		m.drop(entry)
		managedQuickConnectError(w, err)
		return
	}
	defer receipt.Close()
	expiry := receipt.ExpiresAt()
	_ = transport.SetWriteDeadline(expiry)
	session, err := m.issuer.Release(r, receipt)
	receipt.Close()
	m.drop(entry)
	if err != nil {
		managedQuickConnectError(w, err)
		return
	}
	if !m.current(r, original, hold, expiry) {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	responseCtx, cancel := context.WithDeadline(r.Context(), expiry)
	defer cancel()
	profile, name, err := m.runtime.responseProfile(responseCtx, session)
	if err != nil {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	response := dto.AuthenticationResult{User: s.userDtoWithName(profile, name), AccessToken: session.Token, ServerID: s.serverID}
	if responseCtx.Err() != nil || !m.current(r, original, hold, expiry) {
		managedQuickConnectError(w, profileunlock.ErrBridgeQuickConnectUnavailable)
		return
	}
	_ = transport.SetWriteDeadline(expiry)
	writeJSON(w, http.StatusOK, response)
}
