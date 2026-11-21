package datastore

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"novastream/config"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrBridgeQuickConnectUnavailable = errors.New("quick connect unavailable")
	ErrBridgeQuickConnectChanged     = errors.New("original quick connect changed")
	ErrBridgeQuickConnectUnsupported = errors.New("quick connect delegation unsupported")
	ErrBridgeQuickConnectBusy        = errors.New("quick connect capacity unavailable")
	ErrBridgeQuickConnectUsed        = errors.New("quick connect already consumed")
	ErrBridgeQuickConnectUnknown     = errors.New("quick connect commit outcome unknown")
)

const bridgeQuickConnectTTL = 10 * time.Minute

// The issuer owns original challenge membership and cleanup, never account or
// profile authorization supplied as transport DATA.
type BridgeQuickConnectIssuer struct {
	self        *BridgeQuickConnectIssuer
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	entries     map[*BridgeQuickConnectChallenge]struct{}
	running     int
	operations  sync.WaitGroup
	once        sync.Once
	done        chan struct{}
}
type BridgeQuickConnectChallenge struct {
	self                            *BridgeQuickConnectChallenge
	issuer                          *BridgeQuickConnectIssuer
	ctx                             context.Context
	cancel                          context.CancelFunc
	secret, code, client, agent, ip string
	ipBucket                        string
	created, expiry                 time.Time
	phase                           int
	closed                          bool
	approval                        *BridgeQuickConnectApproval
	pending                         sync.WaitGroup
	once                            sync.Once
	done                            chan struct{}
}
type BridgeQuickConnectDetails struct {
	Secret, Code, ClientID, UserAgent, IPAddress string
	CreatedAt, ExpiresAt                         time.Time
	Approved                                     bool
}
type BridgeQuickConnectApproval struct {
	self      *BridgeQuickConnectApproval
	issuer    *BridgeQuickConnectIssuer
	challenge *BridgeQuickConnectChallenge
	policy    *config.BridgeQuickConnectPolicyReceipt
	snapshot  bridgeQuickConnectSnapshot
	expiry    time.Time
	handed    bool
}
type bridgeQuickConnectSnapshot struct {
	token, account, profile, sessionRow, accountRow, profileRow, household string
	settings, profileEpoch, sessionEpoch                                   int64
	expiry                                                                 time.Time
	hasPIN                                                                 bool
}
type BridgeQuickConnectReceipt struct {
	state *bridgeQuickConnectReceiptState
}
type bridgeQuickConnectReceiptState struct {
	self                         *BridgeQuickConnectReceipt
	issuer                       *BridgeQuickConnectIssuer
	challenge                    *BridgeQuickConnectChallenge
	approval                     *BridgeQuickConnectApproval
	request                      *http.Request
	requestContext               context.Context
	binding                      [32]byte
	ctx                          context.Context
	cancel                       context.CancelFunc
	finish                       func()
	progress                     *settingsProgressTx
	fence                        *config.BridgeQuickConnectPolicyFence
	stop, progressStop           func() bool
	callbackDone, progressDone   chan struct{}
	mu                           sync.Mutex
	io                           sync.WaitGroup
	signalOnce                   sync.Once
	requested, done              chan struct{}
	committed, released, closing bool
	session                      models.Session
	row                          string
}

func (BridgeQuickConnectIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectIssuer(opaque)"))
}
func (BridgeQuickConnectIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectIssuer) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }
func (BridgeQuickConnectChallenge) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectChallenge(opaque)"))
}
func (BridgeQuickConnectChallenge) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectChallenge) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }
func (BridgeQuickConnectApproval) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectApproval(opaque)"))
}
func (BridgeQuickConnectApproval) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectApproval) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }
func (BridgeQuickConnectReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeQuickConnectReceipt(opaque)"))
}
func (BridgeQuickConnectReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeQuickConnectReceipt) UnmarshalJSON([]byte) error  { return ErrBridgeQuickConnectChanged }

func NewBridgeQuickConnectIssuer(lifetime context.Context, store *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*BridgeQuickConnectIssuer, error) {
	if lifetime == nil || lifetime.Err() != nil || store == nil || security == nil || !store.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	ctx, cancel := context.WithCancel(lifetime)
	i := &BridgeQuickConnectIssuer{store: store, manager: manager, security: security, coordinator: coordinator, ctx: ctx, cancel: cancel, entries: make(map[*BridgeQuickConnectChallenge]struct{}), done: make(chan struct{})}
	i.self = i
	return i, nil
}
func (i *BridgeQuickConnectIssuer) valid() bool {
	return i != nil && i.self == i && i.ctx != nil && i.ctx.Err() == nil && i.store != nil && i.store.SettingsCommitReady(i.coordinator) && i.security.Ready(i.manager, i.coordinator)
}
func quickConnectRequest(r *http.Request) (time.Time, error) {
	now := time.Now()
	if r == nil || r.Context().Err() != nil {
		return time.Time{}, ErrBridgeQuickConnectChanged
	}
	deadline, ok := r.Context().Deadline()
	if !ok || !deadline.After(now) || deadline.After(now.Add(30*time.Second)) {
		return time.Time{}, ErrBridgeQuickConnectChanged
	}
	return deadline, nil
}
func (i *BridgeQuickConnectIssuer) begin(r *http.Request) (context.Context, func(), error) {
	deadline, err := quickConnectRequest(r)
	if err != nil || !i.valid() {
		return nil, nil, ErrBridgeQuickConnectChanged
	}
	i.mu.Lock()
	if i.closed || i.running >= 8 {
		i.mu.Unlock()
		return nil, nil, ErrBridgeQuickConnectBusy
	}
	i.running++
	i.operations.Add(1)
	i.mu.Unlock()
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	join := familyMediaAfterFunc(i.ctx, cancel)
	return ctx, func() {
		cancel()
		join()
		i.mu.Lock()
		i.running--
		i.mu.Unlock()
		i.operations.Done()
	}, nil
}
func (c *BridgeQuickConnectChallenge) valid() bool { return c != nil && c.self == c && c.issuer != nil }
func (c *BridgeQuickConnectChallenge) begin(r *http.Request) (context.Context, func(), error) {
	if !c.valid() {
		return nil, nil, ErrBridgeQuickConnectChanged
	}
	i := c.issuer
	ctx, finish, err := i.begin(r)
	if err != nil {
		return nil, nil, err
	}
	i.mu.Lock()
	_, member := i.entries[c]
	if !member || c.closed || c.ctx.Err() != nil || !c.expiry.After(time.Now()) {
		i.mu.Unlock()
		finish()
		return nil, nil, ErrBridgeQuickConnectChanged
	}
	c.pending.Add(1)
	i.mu.Unlock()
	bounded, cancel := context.WithDeadline(ctx, c.expiry)
	join := familyMediaAfterFunc(c.ctx, cancel)
	return bounded, func() { cancel(); join(); c.pending.Done(); finish() }, nil
}
func (i *BridgeQuickConnectIssuer) Close() {
	if i == nil || i.self != i {
		return
	}
	i.once.Do(func() {
		i.cancel()
		i.mu.Lock()
		i.closed = true
		pending := make([]*BridgeQuickConnectChallenge, 0, len(i.entries))
		for c := range i.entries {
			pending = append(pending, c)
		}
		i.mu.Unlock()
		for _, c := range pending {
			c.Close()
		}
		i.operations.Wait()
		close(i.done)
	})
	<-i.done
}
func (c *BridgeQuickConnectChallenge) Close() {
	if !c.valid() {
		return
	}
	c.once.Do(func() {
		i := c.issuer
		i.mu.Lock()
		c.closed = true
		c.cancel()
		i.mu.Unlock()
		c.pending.Wait()
		i.mu.Lock()
		delete(i.entries, c)
		c.approval = nil
		i.mu.Unlock()
		close(c.done)
	})
	<-c.done
}
func (a *BridgeQuickConnectApproval) Close() {
	if a != nil && a.self == a && a.challenge != nil {
		a.challenge.Close()
	}
}
func (c *BridgeQuickConnectChallenge) Details() (BridgeQuickConnectDetails, error) {
	if !c.valid() {
		return BridgeQuickConnectDetails{}, ErrBridgeQuickConnectChanged
	}
	i := c.issuer
	i.mu.Lock()
	defer i.mu.Unlock()
	_, member := i.entries[c]
	if !member || c.closed || !i.valid() || c.ctx.Err() != nil || !c.expiry.After(time.Now()) {
		return BridgeQuickConnectDetails{}, ErrBridgeQuickConnectChanged
	}
	return BridgeQuickConnectDetails{c.secret, c.code, c.client, c.agent, c.ip, c.created, c.expiry, c.approval != nil && c.approval.handed}, nil
}

// A transport's actual numeric peer address defines capacity. Ports, IPv6
// presentation and IPv4-mapped addresses cannot create extra buckets. Neither
// HTTP forwarding headers nor descriptive client input participates.
func quickConnectPeerBucket(raw string) (string, error) {
	if raw == "" || len(raw) > 256 || raw != strings.TrimSpace(raw) {
		return "", ErrBridgeQuickConnectChanged
	}
	address, err := netip.ParseAddr(raw)
	if err != nil {
		peer, e := netip.ParseAddrPort(raw)
		if e != nil {
			return "", ErrBridgeQuickConnectChanged
		}
		address = peer.Addr()
	}
	if !address.IsValid() {
		return "", ErrBridgeQuickConnectChanged
	}
	return address.Unmap().WithZone("").String(), nil
}
func (i *BridgeQuickConnectIssuer) Initiate(r *http.Request, client string) (challenge *BridgeQuickConnectChallenge, err error) {
	ctx, finish, err := i.begin(r)
	if err != nil {
		return nil, err
	}
	defer finish()
	if client == "" || client != strings.TrimSpace(client) || len(client) > 256 || !utf8.ValidString(client) || len(r.UserAgent()) > 4096 || !utf8.ValidString(r.UserAgent()) || len(r.RemoteAddr) > 256 || !utf8.ValidString(r.RemoteAddr) {
		return nil, ErrBridgeQuickConnectChanged
	}
	requestContext := r.Context()
	binding, err := bridgeCredentialRequestBinding(r, client)
	if err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	agent, ip := r.UserAgent(), r.RemoteAddr
	bucket, err := quickConnectPeerBucket(ip)
	if err != nil {
		return nil, err
	}
	observation, err := i.security.CaptureBridgeQuickConnectPolicy(ctx)
	if err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	hold, err := i.coordinator.TryProgressFileHold(ctx)
	if err != nil {
		return nil, ErrBridgeQuickConnectBusy
	}
	defer func() {
		if closeErr := hold.Close(); closeErr != nil && err == nil {
			if challenge != nil {
				challenge.Close()
				challenge = nil
			}
			err = ErrBridgeQuickConnectUnavailable
		}
	}()
	fence, err := i.security.BindBridgeQuickConnectPolicy(ctx, observation, hold)
	if err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	values, err := fence.Values()
	if err != nil || !values.BridgeEnabled || !values.QuickConnectEnabled {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	i.mu.Lock()
	expired := make([]*BridgeQuickConnectChallenge, 0)
	for c := range i.entries {
		if !c.expiry.After(time.Now()) {
			expired = append(expired, c)
		}
	}
	i.mu.Unlock()
	for _, c := range expired {
		c.Close()
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	perIP := 0
	codes := map[string]bool{}
	for c := range i.entries {
		if c.ipBucket == bucket {
			perIP++
		}
		codes[c.code] = true
	}
	if len(i.entries) >= 64 || perIP >= 5 {
		return nil, ErrBridgeQuickConnectBusy
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	secret := hex.EncodeToString(raw)
	clear(raw)
	code := ""
	for n := 0; n < 64; n++ {
		value, e := rand.Int(rand.Reader, big.NewInt(900000))
		if e != nil {
			return nil, ErrBridgeQuickConnectUnavailable
		}
		candidate := fmt.Sprintf("%06d", value.Int64()+100000)
		if !codes[candidate] {
			code = candidate
			break
		}
	}
	if code == "" {
		return nil, ErrBridgeQuickConnectBusy
	}
	currentBinding, currentErr := bridgeCredentialRequestBinding(r, client)
	if ctx.Err() != nil || !i.valid() || r.Context() != requestContext || currentErr != nil || currentBinding != binding {
		return nil, ErrBridgeQuickConnectChanged
	}
	if _, err = fence.Values(); err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	now := time.Now()
	life, cancel := context.WithDeadline(i.ctx, now.Add(bridgeQuickConnectTTL))
	c := &BridgeQuickConnectChallenge{issuer: i, ctx: life, cancel: cancel, secret: secret, code: code, client: client, agent: agent, ip: ip, ipBucket: bucket, created: now, expiry: now.Add(bridgeQuickConnectTTL), done: make(chan struct{})}
	c.self = c
	i.entries[c] = struct{}{}
	return c, nil
}

var bridgeQuickConnectRelations = [...]string{"accounts", "media_library_access_policies", "media_library_account_grants", "media_library_profile_grants", "profile_unlock_revocations", "regional_profile_policies", "sessions", "user_settings_versions", "users"}

func quickConnectGuards(ctx context.Context, tx pgx.Tx) error {
	schema := ""
	for _, name := range bridgeQuickConnectRelations {
		var current, kind, persistence string
		var partition bool
		if tx.QueryRow(ctx, `SELECT n.nspname,c.relkind::text,c.relpersistence::text,c.relispartition FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, name).Scan(&current, &kind, &persistence, &partition) != nil || current == "" || len(current) > 256 || kind != "r" || persistence != "p" || partition || schema != "" && schema != current {
			return ErrBridgeQuickConnectUnavailable
		}
		schema = current
		mode := "SHARE"
		if name == "sessions" {
			mode = "SHARE ROW EXCLUSIVE"
		}
		if _, err := tx.Exec(ctx, "LOCK TABLE "+pgx.Identifier{schema, name}.Sanitize()+" IN "+mode+" MODE NOWAIT"); err != nil {
			var pgError *pgconn.PgError
			if errors.As(err, &pgError) && pgError.Code == "55P03" {
				return ErrBridgeQuickConnectBusy
			}
			return ErrBridgeQuickConnectUnavailable
		}
	}
	return nil
}

func quickConnectSnapshot(ctx context.Context, tx pgx.Tx, token string) (out bridgeQuickConnectSnapshot, err error) {
	// Every correlation query is after this operation's actual relation guards.
	var accountExpiry *time.Time
	var sessionExpiry time.Time
	var scope, client string
	err = tx.QueryRow(ctx, `SELECT CASE WHEN octet_length(s.account_id) BETWEEN 1 AND 256 THEN s.account_id END,CASE WHEN octet_length(s.scope_resource) BETWEEN 1 AND 256 THEN s.scope_resource END,CASE WHEN octet_length(s.scope)<=32 THEN s.scope END,CASE WHEN octet_length(s.client_id) BETWEEN 1 AND 256 THEN s.client_id END,s.expires_at,a.expires_at,concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text) FROM sessions s JOIN accounts a ON a.id=s.account_id WHERE s.token=$1`, token).Scan(&out.account, &out.profile, &scope, &client, &sessionExpiry, &accountExpiry, &out.sessionRow, &out.accountRow)
	if err != nil || !parentMutationID(out.account) || !parentMutationID(out.profile) || !utf8.ValidString(out.account) || !utf8.ValidString(out.profile) || !utf8.ValidString(client) || !sessionExpiry.After(time.Now()) || accountExpiry != nil && !accountExpiry.After(time.Now()) {
		return out, ErrBridgeQuickConnectChanged
	}
	if scope != models.SessionScopeJellyfin {
		return out, ErrBridgeQuickConnectUnsupported
	}
	out.token = token
	out.expiry = parentMutationMin(sessionExpiry, accountExpiry)
	var child bool
	err = tx.QueryRow(ctx, `SELECT concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),v.generation,COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='profile' AND scope_key=u.id),0),COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='session' AND scope_key=$3),0),u.is_kids_profile,u.pin_hash<>'' FROM users u JOIN user_settings_versions v ON v.user_id=u.id WHERE u.id=$1 AND u.account_id=$2`, out.profile, out.account, profileUnlockDigest(token)).Scan(&out.profileRow, &out.settings, &out.profileEpoch, &out.sessionEpoch, &child, &out.hasPIN)
	if err != nil || out.settings < 0 || out.profileEpoch < 0 || out.sessionEpoch < 0 {
		return out, ErrBridgeQuickConnectChanged
	}
	if child {
		return out, ErrBridgeQuickConnectUnsupported
	}
	household, err := readBridgeSessionHousehold(ctx, tx, out.account)
	if err != nil {
		return out, ErrBridgeQuickConnectUnavailable
	}
	if household.HasChildren {
		return out, ErrBridgeQuickConnectUnsupported
	}
	out.household = household.Revision
	return out, nil
}
func sameQuickConnectSnapshot(a, b bridgeQuickConnectSnapshot) bool {
	return a.token == b.token && a.account == b.account && a.profile == b.profile && a.sessionRow == b.sessionRow && a.accountRow == b.accountRow && a.profileRow == b.profileRow && a.household == b.household && a.settings == b.settings && a.profileEpoch == b.profileEpoch && a.sessionEpoch == b.sessionEpoch && a.expiry.Equal(b.expiry) && a.hasPIN == b.hasPIN
}

// Approve accepts only an original current PG frame. The facade establishes it
// through its actual PGProfilePins.Authenticate; a principal DTO is insufficient.
func (i *BridgeQuickConnectIssuer) Approve(r *http.Request, c *BridgeQuickConnectChallenge) (approval *BridgeQuickConnectApproval, err error) {
	if !i.valid() || !c.valid() || c.issuer != i {
		return nil, ErrBridgeQuickConnectChanged
	}
	ctx, finish, err := c.begin(r)
	if err != nil {
		return nil, err
	}
	defer finish()
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != i.store || frame.ctx.Err() != nil || frame.session.Scope != models.SessionScopeJellyfin || frame.session.ClientID == "" || len(r.Header.Values("X-Client-ID")) != 1 || r.Header.Get("X-Client-ID") != frame.session.ClientID {
		return nil, ErrBridgeQuickConnectUnsupported
	}
	requestContext := r.Context()
	binding, bindErr := bridgeCredentialRequestBinding(r, frame.session.ClientID)
	if bindErr != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	currentRequest := func() bool {
		current, e := bridgeCredentialRequestBinding(r, frame.session.ClientID)
		return e == nil && current == binding && r.Context() == requestContext && requestContext.Err() == nil && frame.ctx.Err() == nil && ctx.Err() == nil && i.valid()
	}
	if i.store.CheckProfilePinRequestFrame(r) != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	i.mu.Lock()
	if c.phase != 0 || c.closed {
		i.mu.Unlock()
		return nil, ErrBridgeQuickConnectUsed
	}
	c.phase = 1
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		if err != nil && c.phase == 1 {
			c.phase = 0
		}
		i.mu.Unlock()
	}()
	original, err := i.security.CaptureBridgeQuickConnectPolicy(ctx)
	if err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	progress, err := i.store.beginSettingsProgressTx(ctx, i.coordinator)
	if err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	defer func() {
		if closeErr := progress.Close(); closeErr != nil && err == nil {
			i.mu.Lock()
			c.closed = true
			c.cancel()
			c.approval = nil
			i.mu.Unlock()
			approval = nil
			err = ErrBridgeQuickConnectUnavailable
		}
	}()
	fence, err := i.security.BindBridgeQuickConnectPolicy(progress.ctx, original, progress.hold)
	if err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	values, err := fence.Values()
	if err != nil || !values.BridgeEnabled || !values.QuickConnectEnabled {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	if err = quickConnectGuards(progress.ctx, progress.tx); err != nil {
		return nil, err
	}
	snapshot, err := quickConnectSnapshot(progress.ctx, progress.tx, frame.session.Token)
	if err != nil {
		return nil, err
	}
	if snapshot.sessionRow != frame.sessionRow.version || snapshot.accountRow != frame.actorRow.version || snapshot.account != frame.session.AccountID || snapshot.profile != frame.session.ScopeResource {
		return nil, ErrBridgeQuickConnectChanged
	}
	if values.EnforceProfilePins && snapshot.hasPIN {
		return nil, ErrBridgeQuickConnectUnsupported
	}
	expiry := c.expiry
	if snapshot.expiry.Before(expiry) {
		expiry = snapshot.expiry
	}
	// The distinct durable handoff is minted NOW, under the actual original
	// request/frame and native/PG hold. No request context is extended.
	if !currentRequest() || !expiry.After(time.Now()) {
		return nil, ErrBridgeQuickConnectChanged
	}
	policy, err := fence.Handoff(expiry)
	if err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	value := &BridgeQuickConnectApproval{issuer: i, challenge: c, policy: policy, snapshot: snapshot, expiry: expiry, handed: true}
	value.self = value
	if err = progress.tx.Commit(progress.ctx); err != nil {
		return nil, quickConnectCommitError(err)
	}
	if !currentRequest() || !expiry.After(time.Now()) {
		return nil, ErrBridgeQuickConnectChanged
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if c.closed || c.phase != 1 || c.ctx.Err() != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	c.approval = value
	c.phase = 2
	return value, nil
}
func quickConnectCommitError(err error) error {
	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return ErrBridgeQuickConnectUnavailable
	}
	return ErrBridgeQuickConnectUnknown
}
func (a *BridgeQuickConnectApproval) current(i *BridgeQuickConnectIssuer, c *BridgeQuickConnectChallenge) bool {
	return a != nil && a.self == a && a.issuer == i && a.challenge == c && a.handed && a.policy != nil && a.expiry.After(time.Now())
}

func (i *BridgeQuickConnectIssuer) Commit(r *http.Request, c *BridgeQuickConnectChallenge, a *BridgeQuickConnectApproval, secret, client string) (receipt *BridgeQuickConnectReceipt, err error) {
	if !i.valid() || !c.valid() || c.issuer != i || !a.current(i, c) || client != c.client || len(secret) != 64 || subtle.ConstantTimeCompare([]byte(secret), []byte(c.secret)) != 1 {
		return nil, ErrBridgeQuickConnectChanged
	}
	binding, err := bridgeCredentialRequestBinding(r, client)
	if err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	ctx, finish, err := c.begin(r)
	if err != nil {
		return nil, err
	}
	i.mu.Lock()
	if c.phase != 2 || c.approval != a || c.closed {
		i.mu.Unlock()
		finish()
		return nil, ErrBridgeQuickConnectUsed
	}
	c.phase = 3
	i.mu.Unlock()
	life, cancel := context.WithDeadline(ctx, a.expiry)
	st := &bridgeQuickConnectReceiptState{issuer: i, challenge: c, approval: a, request: r, requestContext: r.Context(), binding: binding, ctx: life, cancel: cancel, finish: finish, requested: make(chan struct{}), done: make(chan struct{}), callbackDone: make(chan struct{})}
	receipt = &BridgeQuickConnectReceipt{state: st}
	st.self = receipt
	st.io.Add(1)
	st.stop = context.AfterFunc(life, func() { st.signal(); close(st.callbackDone) })
	go st.cleanup()
	defer func() {
		st.io.Done()
		if err != nil {
			st.signal()
			<-st.done
			receipt = nil
		}
	}()
	st.progress, err = i.store.beginSettingsProgressTx(life, i.coordinator)
	if err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	st.progressDone = make(chan struct{})
	st.progressStop = context.AfterFunc(st.progress.ctx, func() { st.signal(); close(st.progressDone) })
	st.fence, err = i.security.BindBridgeQuickConnectPolicyReceipt(st.progress.ctx, a.policy, st.progress.hold)
	if err != nil {
		return nil, ErrBridgeQuickConnectChanged
	}
	if err = st.audit(st.progress.ctx, st.progress.tx); err != nil {
		return nil, err
	}
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	token := base64.URLEncoding.EncodeToString(raw)
	clear(raw)
	now := time.Now().UTC()
	candidate := models.Session{Token: token, AccountID: a.snapshot.account, ClientID: c.client, Scope: models.SessionScopeJellyfin, ScopeResource: a.snapshot.profile, UserAgent: "Jellyfin: " + c.agent, IPAddress: r.RemoteAddr, CreatedAt: now, ExpiresAt: now.Add(bridgeSessionTTL)}
	err = scanBridgeSession(st.progress.tx.QueryRow(st.progress.ctx, `INSERT INTO sessions(token,account_id,is_master,expires_at,created_at,user_agent,ip_address,scope,scope_resource,client_id)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING token,account_id,is_master,expires_at,created_at,COALESCE(user_agent,''),COALESCE(ip_address,''),COALESCE(scope,''),COALESCE(scope_resource,''),COALESCE(client_id,''),concat(tableoid::oid,':',xmin::text,':',ctid::text)`, candidate.Token, candidate.AccountID, candidate.IsMaster, candidate.ExpiresAt, candidate.CreatedAt, candidate.UserAgent, candidate.IPAddress, candidate.Scope, candidate.ScopeResource, candidate.ClientID), &st.session, &st.row)
	if err != nil {
		return nil, ErrBridgeQuickConnectUnavailable
	}
	if err = st.current(); err != nil {
		return nil, err
	}
	if err = st.progress.tx.Commit(st.progress.ctx); err != nil {
		return nil, quickConnectCommitError(err)
	}
	if err = st.current(); err != nil {
		return nil, err
	}
	st.mu.Lock()
	st.committed = true
	st.mu.Unlock()
	return receipt, nil
}
func (s *bridgeQuickConnectReceiptState) current() error {
	if !s.issuer.valid() || s.ctx.Err() != nil || s.request.Context() != s.requestContext || s.requestContext.Err() != nil || !s.approval.current(s.issuer, s.challenge) || s.challenge.ctx.Err() != nil {
		return ErrBridgeQuickConnectChanged
	}
	binding, err := bridgeCredentialRequestBinding(s.request, s.challenge.client)
	if err != nil || binding != s.binding {
		return ErrBridgeQuickConnectChanged
	}
	if s.fence == nil {
		return ErrBridgeQuickConnectChanged
	}
	if _, err = s.fence.Values(); err != nil {
		return ErrBridgeQuickConnectChanged
	}
	return nil
}
func (s *bridgeQuickConnectReceiptState) audit(ctx context.Context, tx pgx.Tx) error {
	if err := s.current(); err != nil {
		return err
	}
	values, err := s.fence.Values()
	if err != nil || !values.BridgeEnabled || !values.QuickConnectEnabled {
		return ErrBridgeQuickConnectChanged
	}
	if err = quickConnectGuards(ctx, tx); err != nil {
		return err
	}
	current, err := quickConnectSnapshot(ctx, tx, s.approval.snapshot.token)
	if err != nil {
		return err
	}
	if !sameQuickConnectSnapshot(current, s.approval.snapshot) || values.EnforceProfilePins && current.hasPIN {
		return ErrBridgeQuickConnectChanged
	}
	return s.current()
}
func (i *BridgeQuickConnectIssuer) Release(r *http.Request, receipt *BridgeQuickConnectReceipt) (session models.Session, err error) {
	if !i.valid() || receipt == nil || receipt.state == nil || receipt.state.self != receipt || receipt.state.issuer != i || receipt.state.request != r {
		return session, ErrBridgeQuickConnectChanged
	}
	st := receipt.state
	st.mu.Lock()
	if st.closing || !st.committed || st.released {
		st.mu.Unlock()
		return session, ErrBridgeQuickConnectUsed
	}
	st.released = true
	st.io.Add(1)
	st.mu.Unlock()
	defer func() {
		st.io.Done()
		if err != nil {
			st.signal()
		}
	}()
	if err = st.current(); err != nil {
		return session, err
	}
	tx, err := st.progress.connection.BeginTx(st.ctx, pgx.TxOptions{AccessMode: pgx.ReadWrite})
	if err != nil {
		return session, ErrBridgeQuickConnectUnavailable
	}
	defer rollbackPinTx(tx, st.ctx)
	var locked bool
	if tx.QueryRow(st.ctx, `SELECT pg_try_advisory_xact_lock_shared($1::integer,$2::integer)`, settingscommit.Namespace, settingscommit.Resource).Scan(&locked) != nil || !locked {
		return session, ErrBridgeQuickConnectUnavailable
	}
	st.fence, err = i.security.BindBridgeQuickConnectPolicyReceipt(st.ctx, st.approval.policy, st.progress.hold)
	if err != nil {
		return session, ErrBridgeQuickConnectChanged
	}
	if err = st.audit(st.ctx, tx); err != nil {
		return session, err
	}
	var value models.Session
	var row string
	err = scanBridgeSession(tx.QueryRow(st.ctx, `SELECT token,CASE WHEN octet_length(account_id)<=256 THEN account_id END,is_master,expires_at,created_at,CASE WHEN octet_length(COALESCE(user_agent,''))<=4112 THEN COALESCE(user_agent,'') END,CASE WHEN octet_length(COALESCE(ip_address,''))<=256 THEN COALESCE(ip_address,'') END,CASE WHEN octet_length(COALESCE(scope,''))<=32 THEN COALESCE(scope,'') END,CASE WHEN octet_length(COALESCE(scope_resource,''))<=256 THEN COALESCE(scope_resource,'') END,CASE WHEN octet_length(COALESCE(client_id,''))<=256 THEN COALESCE(client_id,'') END,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM sessions WHERE token=$1`, st.session.Token), &value, &row)
	if err != nil || row != st.row || !sameBridgeSession(value, st.session) || !value.ExpiresAt.After(time.Now()) {
		return session, ErrBridgeQuickConnectChanged
	}
	if err = st.current(); err != nil {
		return session, err
	}
	if err = tx.Commit(st.ctx); err != nil {
		return session, quickConnectCommitError(err)
	}
	if err = st.current(); err != nil {
		return session, err
	}
	return value, nil
}
func (s *bridgeQuickConnectReceiptState) signal() {
	s.signalOnce.Do(func() { s.cancel(); close(s.requested) })
}
func (s *bridgeQuickConnectReceiptState) cleanup() {
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
			close(s.progressDone)
		}
		<-s.progressDone
	}
	if s.stop() {
		close(s.callbackDone)
	}
	<-s.callbackDone
	s.session = models.Session{}
	s.row = ""
	s.finish()
	close(s.done)
}
func (r *BridgeQuickConnectReceipt) Close() {
	if r != nil && r.state != nil && r.state.self == r {
		r.state.signal()
		<-r.state.done
	}
}

// ExpiresAt is bookkeeping only, allowing a transport to retain the fixed
// original minimum after intentional cleanup. It cannot authorize delivery.
func (r *BridgeQuickConnectReceipt) ExpiresAt() time.Time {
	if r == nil || r.state == nil || r.state.self != r {
		return time.Time{}
	}
	expiry, _ := r.state.ctx.Deadline()
	return expiry
}
