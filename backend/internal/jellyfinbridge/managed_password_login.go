package jellyfinbridge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/jellyfinbridge/dto"
	"novastream/internal/settingscommit"
	"novastream/models"
	"novastream/services/profileunlock"
)

// ManagedPasswordLogin owns no worker or replaceable authorization callback.
// Only its original managed dependencies can perform credential and session
// work; response metadata is descriptive DATA after a known Release ACK.
type ManagedPasswordLogin struct {
	self        *ManagedPasswordLogin
	lifetime    context.Context
	store       *datastore.DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	pins        *profileunlock.PGProfilePins
	issuer      *profileunlock.BridgeLoginSessionIssuer
}

func (ManagedPasswordLogin) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ManagedPasswordLogin(opaque)"))
}
func (ManagedPasswordLogin) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ManagedPasswordLogin) UnmarshalJSON([]byte) error {
	return profileunlock.ErrBridgeSessionUnavailable
}

func NewManagedPasswordLogin(lifetime context.Context, store *datastore.DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator, pins *profileunlock.PGProfilePins) (*ManagedPasswordLogin, error) {
	if lifetime == nil || lifetime.Err() != nil || store == nil || manager == nil || security == nil || coordinator == nil || pins == nil || !store.SettingsCommitReady(coordinator) || !pins.FamilyMediaReady(store) || !security.Ready(manager, coordinator) {
		return nil, profileunlock.ErrBridgeSessionUnavailable
	}
	issuer, err := pins.NewBridgeLoginSessionIssuer(manager, security, coordinator)
	if err != nil {
		return nil, err
	}
	value := &ManagedPasswordLogin{lifetime: lifetime, store: store, manager: manager, security: security, coordinator: coordinator, pins: pins, issuer: issuer}
	value.self = value
	return value, nil
}

func (m *ManagedPasswordLogin) ready() bool {
	return m != nil && m.self == m && m.lifetime != nil && m.lifetime.Err() == nil && m.store != nil && m.pins != nil && m.security != nil && m.issuer != nil && m.store.SettingsCommitReady(m.coordinator) && m.pins.FamilyMediaReady(m.store) && m.security.Ready(m.manager, m.coordinator)
}

func managedPasswordStatus(err error) int {
	if errors.Is(err, profileunlock.ErrBridgeCredentialRejected) || errors.Is(err, profileunlock.ErrBridgeLoginCandidatesAmbiguous) {
		return http.StatusUnauthorized
	}
	return http.StatusServiceUnavailable
}

func managedPasswordError(w http.ResponseWriter, err error) {
	writeError(w, managedPasswordStatus(err), "Error processing request.")
}

// The bounded original request is established before any policy observation,
// name scan or credential work. Descriptive Jellyfin headers are normalized
// once here; the captured request is never cloned or rewritten afterwards.
func (s *Server) handleManagedAuthenticateByName(w http.ResponseWriter, incoming *http.Request) {
	ctx, cancel := context.WithTimeout(incoming.Context(), 30*time.Second)
	defer cancel()
	r := incoming.Clone(ctx)
	ip := s.clientIP(incoming)
	info := requestClientInfo(incoming)
	agent := strings.TrimSpace(strings.Join([]string{info.Client, info.Version, info.Device}, " "))
	if agent == "" {
		agent = incoming.UserAgent()
	}
	r.Header.Set("User-Agent", agent)
	r.RemoteAddr = ip
	deadline, _ := ctx.Deadline()
	// Real net/http bodies receive the same bound. Test recorders and other
	// response writers may not implement transport read deadlines.
	transport := http.NewResponseController(w)
	_ = transport.SetReadDeadline(deadline)
	readInterrupted := make(chan struct{})
	stopRead := context.AfterFunc(ctx, func() {
		defer close(readInterrupted)
		// Demand a transport read interruption only. Body.Close may drain or
		// wait for the read it would be trying to interrupt.
		_ = transport.SetReadDeadline(time.Now())
	})
	defer func() {
		if !stopRead() {
			<-readInterrupted
		}
	}()
	m := s.deps.ManagedPassword
	if !m.ready() || ctx.Err() != nil {
		managedPasswordError(w, profileunlock.ErrBridgeSessionUnavailable)
		return
	}
	retired := make(chan struct{})
	stop := context.AfterFunc(m.lifetime, func() { defer close(retired); cancel() })
	defer func() {
		if !stop() {
			<-retired
		}
	}()
	var req authenticateByNameRequest
	if err := decodeAuthenticateByName(w, r, &req); err != nil {
		if ctx.Err() != nil || !deadline.After(time.Now()) {
			managedPasswordError(w, profileunlock.ErrBridgeSessionChanged)
			return
		}
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if ctx.Err() != nil || !deadline.After(time.Now()) {
		managedPasswordError(w, profileunlock.ErrBridgeSessionChanged)
		return
	}
	observation, err := m.security.CaptureBridgePolicy(ctx)
	if err != nil {
		managedPasswordError(w, err)
		return
	}
	candidates, err := m.issuer.ResolvePasswordCandidates(r, req.Username, info.DeviceID, observation)
	if err != nil {
		managedPasswordError(w, err)
		return
	}
	keys := []string{"ip:" + ipLimitKey(ip)}
	for _, candidate := range candidates {
		keys = append(keys, "profile:"+candidate.ProfileID)
	}
	if !s.login.attempt(keys...) {
		writeError(w, http.StatusTooManyRequests, "Too many login attempts. Try again later.")
		return
	}
	if len(candidates) == 0 {
		managedPasswordError(w, profileunlock.ErrBridgeCredentialRejected)
		return
	}
	password := req.Pw
	if password == "" {
		password = req.Password
	}
	proof, err := m.issuer.VerifySelectedPasswordForCandidates(r, req.Username, info.DeviceID, password, observation, candidates)
	if err != nil {
		managedPasswordError(w, err)
		return
	}
	// Cleanup ownership is retained on every exit, including an unknown ACK.
	defer proof.Close()
	expiry := proof.ExpiresAt()
	receipt, err := m.issuer.CommitSelected(r, proof, observation)
	if err != nil {
		proof.Close()
		managedPasswordError(w, err)
		return
	}
	defer receipt.Close()
	session, err := m.issuer.Release(r, receipt)
	// Release is the last authorization operation. Close both original owners
	// before a new descriptive query, including when the pool has one slot.
	receipt.Close()
	proof.Close()
	if err != nil {
		managedPasswordError(w, err)
		return
	}
	if ctx.Err() != nil || !expiry.After(time.Now()) || !m.ready() {
		managedPasswordError(w, profileunlock.ErrBridgeSessionChanged)
		return
	}
	responseCtx, finish := context.WithDeadline(ctx, expiry)
	defer finish()
	profile, name, err := m.responseProfile(responseCtx, session)
	if err != nil {
		managedPasswordError(w, err)
		return
	}
	response := dto.AuthenticationResult{User: s.userDtoWithName(profile, name), AccessToken: session.Token, ServerID: s.serverID}
	// Deliberately do not consult proof.Context after intentional cleanup.
	if responseCtx.Err() != nil || !expiry.After(time.Now()) || !m.ready() {
		managedPasswordError(w, profileunlock.ErrBridgeSessionChanged)
		return
	}
	s.login.clear(keys[1:]...)
	s.login.undoLast(keys[0])
	writeJSON(w, http.StatusOK, response)
}

// A complete bounded descriptive snapshot preserves display-name semantics,
// including duplicate blank-account names. It cannot mint, validate or upgrade
// a session; only the already-released session selects its response profile.
func (m *ManagedPasswordLogin) responseProfile(ctx context.Context, session models.Session) (models.User, string, error) {
	if !m.ready() || ctx.Err() != nil || session.Token == "" || session.Scope != models.SessionScopeJellyfin || session.ScopeResource == "" {
		return models.User{}, "", profileunlock.ErrBridgeSessionUnavailable
	}
	connection, err := m.store.Pool().Acquire(ctx)
	if err != nil {
		return models.User{}, "", profileunlock.ErrBridgeSessionUnavailable
	}
	defer func() {
		physical := connection.Conn().PgConn()
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		connection.Release()
	}()
	rows, err := connection.Query(ctx, `SELECT
 CASE WHEN octet_length(u.id) BETWEEN 1 AND 256 THEN u.id END,
 CASE WHEN octet_length(u.account_id) BETWEEN 1 AND 256 THEN u.account_id END,
 CASE WHEN octet_length(u.name)<=4096 THEN u.name END,
 CASE WHEN a.id IS NULL THEN '' WHEN octet_length(a.username)<=4096 THEN a.username END,
 a.id IS NOT NULL,u.is_kids_profile
 FROM users u LEFT JOIN accounts a ON a.id=u.account_id ORDER BY u.id COLLATE "C" LIMIT 4097`)
	if err != nil {
		return models.User{}, "", profileunlock.ErrBridgeSessionUnavailable
	}
	defer rows.Close()
	counts := make(map[string]int)
	var target models.User
	var username string
	count, size := 0, 0
	for rows.Next() {
		var profile models.User
		var accountName string
		var accountPresent bool
		if err := rows.Scan(&profile.ID, &profile.AccountID, &profile.Name, &accountName, &accountPresent, &profile.IsKidsProfile); err != nil {
			return models.User{}, "", profileunlock.ErrBridgeSessionUnavailable
		}
		count++
		size += len(profile.ID) + len(profile.AccountID) + len(profile.Name) + len(accountName)
		if count > 4096 || size > 4*1024*1024 || !utf8.ValidString(profile.ID) || !utf8.ValidString(profile.AccountID) || !utf8.ValidString(profile.Name) || !utf8.ValidString(accountName) {
			return models.User{}, "", profileunlock.ErrBridgeSessionUnavailable
		}
		counts[strings.ToLower(strings.TrimSpace(profile.Name))]++
		if profile.ID == session.ScopeResource && profile.AccountID == session.AccountID && accountPresent && !profile.IsKidsProfile {
			target, username = profile, accountName
		}
	}
	if rows.Err() != nil || target.ID == "" || ctx.Err() != nil || !m.ready() {
		return models.User{}, "", profileunlock.ErrBridgeSessionUnavailable
	}
	name := strings.TrimSpace(target.Name)
	if strings.TrimSpace(username) != "" {
		name = strings.TrimSpace(username) + "/" + name
	} else if counts[strings.ToLower(name)] > 1 {
		name += " (" + deriveID(refUser, target.ID)[:4] + ")"
	}
	if name == "" {
		name = target.Name
	}
	return target, name, nil
}
