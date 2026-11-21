package datastore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrBridgeSessionUnavailable = errors.New("bridge session issuance unavailable")
	ErrBridgeSessionChanged     = errors.New("bridge session issuance changed")
	ErrBridgeSessionUsed        = errors.New("bridge session issuance already used")
	ErrBridgeSessionUnknown     = errors.New("bridge session commit outcome unknown")
)

// New phases are private to this concrete writer. Credential verification is
// unchanged, and no phase can turn Intent.Check into a mint permission.
const bridgeCredentialMinting bridgeCredentialPhase = bridgeCredentialRetired + 1
const bridgeSessionTTL = 365 * 24 * time.Hour

type BridgeLoginSessionIssuer struct {
	self        *BridgeLoginSessionIssuer
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
}
type BridgeLoginSessionReceipt struct{ state *bridgeSessionState }
type bridgeSessionState struct {
	mu                           sync.Mutex
	self                         *BridgeLoginSessionReceipt
	issuer                       *BridgeLoginSessionIssuer
	credential                   *bridgeCredentialState
	selected                     *bridgeSelectedPasswordState
	selectedStop                 func()
	request                      *http.Request
	ctx                          context.Context
	cancel                       context.CancelFunc
	signalOnce                   sync.Once
	requested                    chan struct{}
	done                         chan struct{}
	callbackDone                 chan struct{}
	stop                         func() bool
	progressStop                 func() bool
	progressCallbackDone         chan struct{}
	finishOriginal               func()
	io                           sync.WaitGroup
	progress                     *settingsProgressTx
	fence                        *config.BridgeLoginPolicyFence
	session                      models.Session
	rowVersion                   string
	committed, released, closing bool
}

func NewBridgeLoginSessionIssuer(ds *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*BridgeLoginSessionIssuer, error) {
	if ds == nil || security == nil || !ds.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrBridgeSessionUnavailable
	}
	value := &BridgeLoginSessionIssuer{store: ds, manager: manager, security: security, coordinator: coordinator}
	value.self = value
	return value, nil
}
func (i *BridgeLoginSessionIssuer) valid() bool {
	return i != nil && i.self == i && i.store != nil && i.store.SettingsCommitReady(i.coordinator) && i.security.Ready(i.manager, i.coordinator)
}
func (BridgeLoginSessionIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginSessionIssuer(opaque)"))
}
func (BridgeLoginSessionIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginSessionIssuer) UnmarshalJSON([]byte) error  { return ErrBridgeSessionChanged }
func (BridgeLoginSessionReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeLoginSessionReceipt(opaque)"))
}
func (BridgeLoginSessionReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeLoginSessionReceipt) UnmarshalJSON([]byte) error  { return ErrBridgeSessionChanged }
func (r *BridgeLoginSessionReceipt) valid() bool {
	return r != nil && r.state != nil && r.state.self == r
}

// Commit uses only the original verified evidence and its actual request. The
// observation is genuine config DATA; only this method creates the real TX.
func (i *BridgeLoginSessionIssuer) Commit(request *http.Request, proof *BridgeLoginCredentialProof, observation *config.BridgeLoginPolicyObservation) (*BridgeLoginSessionReceipt, error) {
	return i.commitBridgeLoginSession(request, proof, observation, nil)
}

func (i *BridgeLoginSessionIssuer) commitBridgeLoginSession(request *http.Request, proof *BridgeLoginCredentialProof, observation *config.BridgeLoginPolicyObservation, selected *bridgeSelectedPasswordState) (receipt *BridgeLoginSessionReceipt, err error) {
	if !i.valid() || !proof.valid() || proof.state.store != i.store || !proof.state.matchesRequest(request) || observation == nil {
		return nil, ErrBridgeSessionChanged
	}
	original := proof.state
	ctx, finish, err := original.begin(request.Context())
	if err != nil {
		return nil, err
	}
	if selected != nil {
		// The original credential pending operation protects selection audit before
		// any claim/read. No second pending operation or registry is introduced.
		if err = selected.claim(i, request, observation); err != nil {
			finish()
			return nil, err
		}
	}
	original.mu.Lock()
	if original.closed || original.phase != bridgeCredentialVerified || original.proof != proof || !original.matchesRequest(request) || ctx.Err() != nil || !original.expiry.After(time.Now()) {
		original.mu.Unlock()
		finish()
		return nil, ErrBridgeSessionUsed
	}
	original.phase = bridgeCredentialMinting
	original.mu.Unlock()
	life, cancel := context.WithCancel(ctx)
	if selected != nil {
		cancel()
		life, cancel = context.WithDeadline(ctx, selected.expiry)
	}
	receipt = &BridgeLoginSessionReceipt{state: &bridgeSessionState{issuer: i, credential: original, selected: selected, request: request, ctx: life, cancel: cancel, requested: make(chan struct{}), done: make(chan struct{}), callbackDone: make(chan struct{}), finishOriginal: finish}}
	st := receipt.state
	st.self = receipt
	st.io.Add(1)
	st.stop = context.AfterFunc(ctx, func() { st.signal(); close(st.callbackDone) })
	if selected != nil {
		st.selectedStop = familyMediaAfterFunc(selected.ctx, st.signal)
	}
	// A cleanup owner exists before any original connection/hold is acquired.
	go st.cleanup()
	defer func() {
		st.io.Done()
		if err != nil {
			st.signal()
			<-st.done
			receipt = nil
		}
	}()
	st.progress, err = i.store.beginSettingsProgressTx(st.ctx, i.coordinator)
	if err != nil {
		return nil, ErrBridgeSessionUnavailable
	}
	st.progressCallbackDone = make(chan struct{})
	st.progressStop = context.AfterFunc(st.progress.ctx, func() {
		st.signal()
		close(st.progressCallbackDone)
	})
	st.fence, err = i.security.BindBridgePolicy(st.progress.ctx, observation, st.progress.hold)
	if err != nil {
		return nil, ErrBridgeSessionChanged
	}
	values, err := st.fence.Values()
	if err != nil {
		return nil, ErrBridgeSessionChanged
	}
	master, err := st.lockCurrent(st.progress.ctx, st.progress.tx)
	if err != nil {
		return nil, err
	}
	if !bridgeSessionPolicyAllows(values, original.snapshot.pinHash != "", original.kind) {
		return nil, ErrBridgeSessionChanged
	}
	if st.selected != nil {
		if err = st.selected.auditOriginalTransaction(st.progress.ctx, st.progress.tx); err != nil {
			return nil, err
		}
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return nil, ErrBridgeSessionUnavailable
	}
	token := base64.URLEncoding.EncodeToString(raw)
	clear(raw)
	now := time.Now().UTC()
	// These descriptive fields come from the already-bound original request.
	candidate := models.Session{Token: token, AccountID: original.account, IsMaster: master, CreatedAt: now, ExpiresAt: now.Add(bridgeSessionTTL), UserAgent: "Jellyfin: " + request.UserAgent(), IPAddress: request.RemoteAddr, ClientID: original.client, Scope: models.SessionScopeJellyfin, ScopeResource: original.profile}
	err = scanBridgeSession(st.progress.tx.QueryRow(st.progress.ctx, `INSERT INTO sessions(token,account_id,is_master,expires_at,created_at,user_agent,ip_address,scope,scope_resource,client_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING token,account_id,is_master,expires_at,created_at,COALESCE(user_agent,''),COALESCE(ip_address,''),COALESCE(scope,''),COALESCE(scope_resource,''),COALESCE(client_id,''),concat(tableoid::oid,':',xmin::text,':',ctid::text)`, candidate.Token, candidate.AccountID, candidate.IsMaster, candidate.ExpiresAt, candidate.CreatedAt, candidate.UserAgent, candidate.IPAddress, candidate.Scope, candidate.ScopeResource, candidate.ClientID), &st.session, &st.rowVersion)
	if err != nil {
		return nil, ErrBridgeSessionUnavailable
	}
	if err = st.current(); err != nil {
		return nil, err
	}
	if err = st.progress.tx.Commit(st.progress.ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return nil, ErrBridgeSessionUnavailable
		}
		return nil, ErrBridgeSessionUnknown
	}
	// Known ACK does not authorize delivery after the original deadline/cancel.
	if err = st.current(); err != nil {
		return nil, err
	}
	st.mu.Lock()
	st.committed = true
	st.mu.Unlock()
	return receipt, nil
}

func bridgeSessionPolicyAllows(value config.BridgeLoginPolicyValues, hasPIN bool, kind bridgeCredentialKind) bool {
	if !value.BridgeEnabled {
		return false
	}
	if !hasPIN {
		return kind == bridgeCredentialAccount
	}
	if !value.EnforceProfilePins {
		return kind == bridgeCredentialAccount || value.AllowPinLogin && kind == bridgeCredentialPIN
	}
	return kind == bridgeCredentialCombined || value.AllowPinLogin && kind == bridgeCredentialPIN
}
func (s *bridgeSessionState) current() error {
	if s.selected != nil && s.selected.current() != nil {
		return ErrBridgeSessionChanged
	}
	if s.ctx.Err() != nil || s.progress == nil || s.progress.ctx.Err() != nil || !s.issuer.valid() || !s.credential.matchesRequest(s.request) || !s.credential.expiry.After(time.Now()) {
		return ErrBridgeSessionChanged
	}
	s.credential.mu.Lock()
	valid := !s.credential.closed && s.credential.phase == bridgeCredentialMinting
	s.credential.mu.Unlock()
	if !valid {
		return ErrBridgeSessionChanged
	}
	if _, err := s.fence.Values(); err != nil {
		return ErrBridgeSessionChanged
	}
	return nil
}

// Release never recaptures proof or acquires another pool connection/hold.
// The new transaction must obtain its own actual shared PG advisory xact key.
func (i *BridgeLoginSessionIssuer) Release(request *http.Request, receipt *BridgeLoginSessionReceipt) (session models.Session, err error) {
	if !i.valid() || !receipt.valid() || receipt.state.issuer != i || !receipt.state.credential.matchesRequest(request) {
		return session, ErrBridgeSessionChanged
	}
	st := receipt.state
	ctx, finish, err := st.credential.begin(request.Context())
	if err != nil {
		return session, err
	}
	st.mu.Lock()
	if st.closing || !st.committed || st.released || st.ctx.Err() != nil {
		st.mu.Unlock()
		finish()
		return session, ErrBridgeSessionUsed
	}
	st.released = true
	st.io.Add(1)
	st.mu.Unlock()
	// Actual query/transaction/callback cleanup finishes before IO Done. Joining
	// Receipt.Close here would wait on this very operation; errors only signal.
	defer func() {
		finish()
		st.io.Done()
		if err != nil {
			st.signal()
		}
	}()
	bounded, cancel := context.WithCancel(ctx)
	if st.selected != nil {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, st.selected.expiry)
	}
	stop := familyMediaAfterFunc(st.ctx, cancel)
	stopProgress := familyMediaAfterFunc(st.progress.ctx, cancel)
	stopSelected := func() {}
	if st.selected != nil {
		stopSelected = familyMediaAfterFunc(st.selected.ctx, cancel)
	}
	defer func() { cancel(); stopSelected(); stopProgress(); stop() }()
	if err = st.current(); err != nil {
		return session, err
	}
	tx, err := st.progress.connection.BeginTx(bounded, pgx.TxOptions{AccessMode: pgx.ReadWrite})
	if err != nil {
		return session, ErrBridgeSessionUnavailable
	}
	defer rollbackPinTx(tx, bounded)
	var locked bool
	if err = tx.QueryRow(bounded, `SELECT pg_try_advisory_xact_lock_shared($1::integer,$2::integer)`, settingscommit.Namespace, settingscommit.Resource).Scan(&locked); err != nil || !locked {
		return session, ErrBridgeSessionUnavailable
	}
	if _, err = st.lockCurrent(bounded, tx); err != nil {
		return session, err
	}
	var current models.Session
	var rowVersion string
	err = scanBridgeSession(tx.QueryRow(bounded, `SELECT token,CASE WHEN octet_length(account_id)<=256 THEN account_id ELSE NULL END,is_master,expires_at,created_at,CASE WHEN octet_length(COALESCE(user_agent,''))<=4112 THEN COALESCE(user_agent,'') ELSE NULL END,CASE WHEN octet_length(COALESCE(ip_address,''))<=256 THEN COALESCE(ip_address,'') ELSE NULL END,CASE WHEN octet_length(COALESCE(scope,''))<=32 THEN COALESCE(scope,'') ELSE NULL END,CASE WHEN octet_length(COALESCE(scope_resource,''))<=256 THEN COALESCE(scope_resource,'') ELSE NULL END,CASE WHEN octet_length(COALESCE(client_id,''))<=256 THEN COALESCE(client_id,'') ELSE NULL END,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM sessions WHERE token=$1 FOR UPDATE`, st.session.Token), &current, &rowVersion)
	if err != nil || !sameBridgeSession(current, st.session) || rowVersion != st.rowVersion || !current.ExpiresAt.After(time.Now()) {
		return session, ErrBridgeSessionChanged
	}
	if st.selected != nil {
		if err = st.selected.auditOriginalTransaction(bounded, tx); err != nil {
			return session, err
		}
	}
	if err = st.current(); err != nil {
		return session, err
	}
	if err = tx.Commit(bounded); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return session, ErrBridgeSessionUnavailable
		}
		return session, ErrBridgeSessionUnknown
	}
	if err = st.current(); err != nil {
		return session, err
	}
	return current, nil
}
func scanBridgeSession(row pgx.Row, value *models.Session, version *string) error {
	return row.Scan(&value.Token, &value.AccountID, &value.IsMaster, &value.ExpiresAt, &value.CreatedAt, &value.UserAgent, &value.IPAddress, &value.Scope, &value.ScopeResource, &value.ClientID, version)
}
func sameBridgeSession(a, b models.Session) bool {
	return a.Token == b.Token && a.AccountID == b.AccountID && a.IsMaster == b.IsMaster && a.ExpiresAt.Equal(b.ExpiresAt) && a.CreatedAt.Equal(b.CreatedAt) && a.UserAgent == b.UserAgent && a.IPAddress == b.IPAddress && a.Scope == b.Scope && a.ScopeResource == b.ScopeResource && a.ClientID == b.ClientID
}

// lockCurrent is called only with this owner's concrete settings/release TX.
// FK-free generic user writers require the actual relation SHARE barrier.
func (s *bridgeSessionState) lockCurrent(ctx context.Context, tx pgx.Tx) (bool, error) {
	if ctx == nil || ctx.Err() != nil {
		return false, ErrBridgeSessionChanged
	}
	if err := s.current(); err != nil {
		return false, err
	}
	epoch, err := lockPinEpoch(ctx, tx, "profile", s.credential.profile)
	if err != nil || epoch != s.credential.snapshot.profileGeneration {
		return false, ErrBridgeSessionChanged
	}
	var id string
	if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, s.credential.account).Scan(&id); err != nil {
		return false, ErrBridgeSessionUnavailable
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN SHARE MODE`); err != nil {
		return false, ErrBridgeSessionUnavailable
	}
	var value bridgeCredentialSnapshot
	var expiry *time.Time
	var child, master bool
	err = tx.QueryRow(ctx, `SELECT CASE WHEN octet_length(a.password_hash)<=128 THEN a.password_hash ELSE NULL END,CASE WHEN octet_length(u.pin_hash)<=128 THEN u.pin_hash ELSE NULL END,u.pin_length,concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),a.expires_at,u.is_kids_profile,v.generation,a.is_master FROM accounts a JOIN users u ON u.account_id=a.id JOIN user_settings_versions v ON v.user_id=u.id WHERE a.id=$1 AND u.id=$2 FOR UPDATE OF u,v`, s.credential.account, s.credential.profile).Scan(&value.accountHash, &value.pinHash, &value.pinLength, &value.accountVersion, &value.profileVersion, &expiry, &child, &value.settingsGeneration, &master)
	if err != nil || child || value.settingsGeneration < 0 || !bridgeCredentialHashSupported(value.accountHash) || value.pinHash != "" && !bridgeCredentialHashSupported(value.pinHash) || expiry != nil && !expiry.After(time.Now()) {
		return false, ErrBridgeSessionChanged
	}
	if expiry != nil {
		value.accountExpiry = *expiry
		value.hasAccountExpiry = true
	}
	value.profileGeneration = epoch
	household, err := readBridgeSessionHousehold(ctx, tx, s.credential.account)
	if err != nil || household.HasChildren {
		return false, ErrBridgeSessionChanged
	}
	value.household = household.Revision
	if !sameBridgeCredentialSnapshot(value, s.credential.snapshot) || ctx.Err() != nil || s.current() != nil {
		return false, ErrBridgeSessionChanged
	}
	return master, nil
}

func (s *bridgeSessionState) signal() { s.signalOnce.Do(func() { s.cancel(); close(s.requested) }) }
func (s *bridgeSessionState) cleanup() {
	<-s.requested
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.cancel()
	s.io.Wait()
	if s.progress != nil {
		_ = s.progress.Close()
	}
	if s.progressStop != nil {
		if s.progressStop() {
			close(s.progressCallbackDone)
		}
		<-s.progressCallbackDone
	}
	s.credential.mu.Lock()
	s.credential.phase = bridgeCredentialRetired
	s.credential.cancel()
	s.credential.mu.Unlock()
	if s.stop != nil && s.stop() {
		close(s.callbackDone)
	}
	<-s.callbackDone
	if s.selectedStop != nil {
		s.selectedStop()
	}
	s.session = models.Session{}
	s.rowVersion = ""
	s.finishOriginal()
	close(s.done)
}
func (r *BridgeLoginSessionReceipt) Close() {
	if r.valid() {
		r.state.signal()
		<-r.state.done
	}
}

// Same bounded complete receipt as the original credential reader, on THIS TX.
func readBridgeSessionHousehold(ctx context.Context, tx pgx.Tx, id string) (out RegionalAccountSnapshot, err error) {
	if tx == nil || ctx == nil || ctx.Err() != nil || !parentMutationID(id) {
		return out, ErrFamilyMediaUnavailable
	}
	var before, after int64
	if tx.QueryRow(ctx, "SELECT last_value FROM regional_policy_revision_seq").Scan(&before) != nil {
		return out, ErrFamilyMediaUnavailable
	}
	var raw []byte
	if tx.QueryRow(ctx, familyScopedHouseholdSQL, id).Scan(&raw) != nil || len(raw) == 0 || len(raw) > familyScopedHouseholdBytes {
		return out, ErrFamilyMediaUnavailable
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 6 {
		return out, ErrFamilyMediaUnavailable
	}
	var account, version *string
	var children *bool
	var count *int
	var expiry *time.Time
	if json.Unmarshal(fields["accountId"], &account) != nil || account == nil || *account != id || json.Unmarshal(fields["version"], &version) != nil || version == nil || len(*version) == 0 || len(*version) > 96 || json.Unmarshal(fields["hasChildren"], &children) != nil || children == nil || json.Unmarshal(fields["rowCount"], &count) != nil || count == nil || *count < 0 || *count > 256 || json.Unmarshal(fields["expiry"], &expiry) != nil || expiry != nil && !expiry.After(time.Now()) {
		return out, ErrFamilyMediaUnavailable
	}
	var members []json.RawMessage
	if string(fields["members"]) == "null" || json.Unmarshal(fields["members"], &members) != nil || len(members) != *count {
		return out, ErrFamilyMediaUnavailable
	}
	seen := map[string]bool{}
	hasChildren := false
	for _, member := range members {
		var tuple []json.RawMessage
		if json.Unmarshal(member, &tuple) != nil || len(tuple) != 3 {
			return out, ErrFamilyMediaUnavailable
		}
		var memberID, physical *string
		var child *bool
		if json.Unmarshal(tuple[0], &memberID) != nil || memberID == nil || !parentMutationID(*memberID) || seen[*memberID] || json.Unmarshal(tuple[1], &child) != nil || child == nil || json.Unmarshal(tuple[2], &physical) != nil || physical == nil || len(*physical) == 0 || len(*physical) > 96 {
			return out, ErrFamilyMediaUnavailable
		}
		seen[*memberID] = true
		hasChildren = hasChildren || *child
	}
	if hasChildren != *children || tx.QueryRow(ctx, "SELECT last_value FROM regional_policy_revision_seq").Scan(&after) != nil || after != before || ctx.Err() != nil || expiry != nil && !expiry.After(time.Now()) {
		return out, ErrFamilyMediaUnavailable
	}
	return RegionalAccountSnapshot{AccountID: id, HasChildren: hasChildren, Revision: regionalHash([]any{id, *version, before, json.RawMessage(fields["members"])})}, nil
}
