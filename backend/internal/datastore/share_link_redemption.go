package datastore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrSharePlaybackUnavailable = errors.New("share playback unavailable")
	ErrSharePlaybackChanged     = errors.New("share playback changed or expired")
	ErrSharePlaybackRecreate    = errors.New("share link requires recreation")
	ErrSharePlaybackForbidden   = errors.New("share playback not permitted")
	ErrSharePlaybackUnknown     = errors.New("share playback commit outcome unknown")
	ErrSharePlaybackBusy        = errors.New("share playback capacity unavailable")
	ErrSharePlaybackParty       = errors.New("separate original party playback")
)

// ShareLinkRedeemer is an original managed runtime, never a raw SQL/frame port.
// Its source owners are distinct from full-session family-media authority.
type ShareLinkRedeemer struct {
	self        *ShareLinkRedeemer
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	operations  sync.WaitGroup
	owners      map[*SharePlaybackOwner]struct{}
	peers       map[string]int
	active      int
	once        sync.Once
	done        chan struct{}
}

func NewShareLinkRedeemer(lifetime context.Context, store *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*ShareLinkRedeemer, error) {
	if lifetime == nil || lifetime.Err() != nil || !store.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrSharePlaybackUnavailable
	}
	ctx, cancel := context.WithCancel(lifetime)
	c := &ShareLinkRedeemer{store: store, manager: manager, security: security, coordinator: coordinator, ctx: ctx, cancel: cancel, owners: make(map[*SharePlaybackOwner]struct{}), peers: make(map[string]int), done: make(chan struct{})}
	c.self = c
	return c, nil
}
func (c *ShareLinkRedeemer) usable() bool {
	return c != nil && c.self == c && c.ctx != nil && c.ctx.Err() == nil && c.store.SettingsCommitReady(c.coordinator) && c.security.Ready(c.manager, c.coordinator)
}
func (c *ShareLinkRedeemer) MatchesStore(store *DataStore) bool {
	return c.usable() && store == c.store
}
func (c *ShareLinkRedeemer) Context() context.Context {
	if !c.usable() {
		return familyMediaClosedContext()
	}
	return c.ctx
}
func (ShareLinkRedeemer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ShareLinkRedeemer(opaque)"))
}
func (ShareLinkRedeemer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ShareLinkRedeemer) UnmarshalJSON([]byte) error  { return ErrSharePlaybackChanged }
func (c *ShareLinkRedeemer) enter() bool {
	if !c.usable() {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.usable() {
		return false
	}
	c.operations.Add(1)
	return true
}
func (c *ShareLinkRedeemer) Close() {
	if c == nil || c.self != c {
		return
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		owners := make([]*SharePlaybackOwner, 0, len(c.owners))
		for o := range c.owners {
			owners = append(owners, o)
		}
		c.mu.Unlock()
		for _, o := range owners {
			o.Close()
		}
		c.operations.Wait()
		close(c.done)
	})
	<-c.done
}
func shareCanonicalUUID(s string) bool {
	v, e := uuid.Parse(s)
	return e == nil && v != uuid.Nil && v.String() == s
}
func shareNormalizeResource(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "webdav/") {
		return "/" + strings.TrimPrefix(s, "webdav/")
	}
	if strings.HasPrefix(s, "/webdav/") {
		return strings.TrimPrefix(s, "/webdav")
	}
	return s
}
func shareValidResource(s string) bool {
	return s != "" && len(s) <= 1<<20 && utf8.ValidString(s) && s == shareNormalizeResource(s) && s != models.SessionScopeResourcePlaceholder
}
func sharePeer(r *http.Request) string {
	host, _, e := net.SplitHostPort(r.RemoteAddr)
	if e != nil {
		host = r.RemoteAddr
	}
	if len(host) > 256 || strings.TrimSpace(host) != host || host == "" {
		return "unknown"
	}
	return host
}
func (c *ShareLinkRedeemer) admit(peer string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.active >= 32 || c.peers[peer] >= 4 {
		return false
	}
	c.active++
	c.peers[peer]++
	return true
}
func (c *ShareLinkRedeemer) refund(peer string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	c.peers[peer]--
	if c.peers[peer] == 0 {
		delete(c.peers, peer)
	}
}

// This is committed response DATA only. Copying it cannot authenticate a request
// or manufacture the private persisted nonce required by the media consumer.
type ShareLinkRedemption struct {
	Session          models.Session
	Params           map[string]string
	PublicationUntil time.Time
}

func (ShareLinkRedemption) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ShareLinkRedemption(committed-output)"))
}
func (ShareLinkRedemption) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

type shareDelegationRow struct {
	token, creator, profile, owner, creatorInc, profileInc, ownerInc, physical string
	params                                                                     []byte
	expires                                                                    time.Time
	maxUses, useCount                                                          int
	active                                                                     bool
}

const shareDelegationColumns = `token,
 CASE WHEN octet_length(account_id) BETWEEN 1 AND 256 THEN account_id ELSE NULL END,
 CASE WHEN octet_length(source_profile_id) BETWEEN 1 AND 256 THEN source_profile_id ELSE NULL END,
 CASE WHEN octet_length(source_account_id) BETWEEN 1 AND 256 THEN source_account_id ELSE NULL END,
 creator_incarnation::text,source_profile_incarnation::text,source_owner_incarnation::text,
 CASE WHEN octet_length(params::text)<=1048576 THEN params ELSE NULL END,
 expires_at,max_uses,use_count,active,concat(tableoid::oid,':',xmin::text,':',ctid::text)`

func shareReadDelegation(ctx context.Context, tx pgx.Tx, token string, locked bool) (out shareDelegationRow, err error) {
	// Distinguish absent original witnesses explicitly; never adopt current IDs.
	var trusted bool
	if err = tx.QueryRow(ctx, `SELECT provenance_version=1 AND delegation_version=1 AND creator_incarnation IS NOT NULL AND source_profile_incarnation IS NOT NULL AND source_owner_incarnation IS NOT NULL FROM share_links WHERE token=$1`, token).Scan(&trusted); err != nil || !trusted {
		return out, ErrSharePlaybackRecreate
	}
	query := `SELECT ` + shareDelegationColumns + ` FROM share_links WHERE token=$1`
	if locked {
		query += ` FOR UPDATE`
	}
	err = tx.QueryRow(ctx, query, token).Scan(&out.token, &out.creator, &out.profile, &out.owner, &out.creatorInc, &out.profileInc, &out.ownerInc, &out.params, &out.expires, &out.maxUses, &out.useCount, &out.active, &out.physical)
	if err != nil || out.token != token || !parentMutationID(out.creator) || !parentMutationID(out.profile) || !parentMutationID(out.owner) || !shareCanonicalUUID(out.creatorInc) || !shareCanonicalUUID(out.profileInc) || !shareCanonicalUUID(out.ownerInc) || len(out.params) == 0 || len(out.params) > 1<<20 || !out.active || !out.expires.After(time.Now()) || out.useCount < 0 {
		return out, ErrSharePlaybackChanged
	}
	return out, nil
}
func shareDelegationSame(a, b shareDelegationRow) bool {
	return a.token == b.token && a.creator == b.creator && a.profile == b.profile && a.owner == b.owner && a.creatorInc == b.creatorInc && a.profileInc == b.profileInc && a.ownerInc == b.ownerInc && a.physical == b.physical && a.expires.Equal(b.expires) && a.maxUses == b.maxUses && a.useCount == b.useCount && a.active == b.active && string(a.params) == string(b.params)
}

type shareAccountRow struct {
	id, inc string
	master  bool
	expires *time.Time
}

func shareAccount(ctx context.Context, tx pgx.Tx, id string, locked bool) (out shareAccountRow, err error) {
	q := `SELECT id,share_incarnation::text,is_master,expires_at FROM accounts WHERE id=$1`
	if locked {
		q += ` FOR UPDATE`
	}
	err = tx.QueryRow(ctx, q, id).Scan(&out.id, &out.inc, &out.master, &out.expires)
	if err != nil || out.id != id || !shareCanonicalUUID(out.inc) || out.expires != nil && !out.expires.After(time.Now()) {
		return out, ErrSharePlaybackChanged
	}
	return out, nil
}

// The actual same transaction owns these short SHARE guards only after its row
// locks. NOWAIT forbids silently waiting/adopting a concurrent membership write.
func shareLockPolicyRelations(ctx context.Context, tx pgx.Tx) error {
	return shareLockRelations(ctx, tx, []string{"accounts", "users", "regional_profile_policies", "user_settings_versions", "profile_unlock_revocations"})
}
func shareLockRelations(ctx context.Context, tx pgx.Tx, relations []string) error {
	schema := ""
	for _, table := range relations {
		const catalog = `SELECT c.oid,n.nspname,c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=pg_catalog.to_regclass($1)`
		var originalOID uint32
		for observation := 0; observation < 2; observation++ {
			name := table
			if observation == 1 {
				name = pgx.Identifier{schema, table}.Sanitize()
			}
			var oid uint32
			var current, kind, persistence string
			var partition, rowSecurity, forceRowSecurity bool
			if e := tx.QueryRow(ctx, catalog, name).Scan(&oid, &current, &kind, &persistence, &partition, &rowSecurity, &forceRowSecurity); e != nil || current == "" || len(current) > 256 || kind != "r" || persistence != "p" || partition || rowSecurity || forceRowSecurity || schema != "" && schema != current || observation == 1 && oid != originalOID {
				return ErrSharePlaybackUnavailable
			}
			if observation == 0 {
				originalOID = oid
				schema = current
				if _, e := tx.Exec(ctx, `LOCK TABLE `+pgx.Identifier{schema, table}.Sanitize()+` IN SHARE MODE NOWAIT`); e != nil {
					return ErrSharePlaybackChanged
				}
			}
			// Re-observe after the real table guard: discovery before its acquisition
			// must not overlook a completed concurrent DDL/RLS change.
		}
	}
	return nil
}

func shareHousehold(ctx context.Context, tx pgx.Tx, id string) (string, error) {
	const query = `WITH members AS MATERIALIZED(SELECT id,is_kids_profile,share_incarnation::text AS incarnation,concat(tableoid::oid,':',xmin::text,':',ctid::text) AS physical FROM users WHERE account_id=$1 LIMIT 257), facts AS MATERIALIZED(SELECT count(*) AS n,COALESCE(bool_or(is_kids_profile),false) AS children,COALESCE(jsonb_agg(jsonb_build_array(CASE WHEN octet_length(id) BETWEEN 1 AND 256 THEN id ELSE NULL END,is_kids_profile,incarnation,physical) ORDER BY id),'[]'::jsonb) AS body FROM members) SELECT n,children,CASE WHEN n<=256 AND octet_length(body::text)<=65536 THEN body::text ELSE NULL END FROM facts`
	var n int
	var children bool
	var raw string
	if e := tx.QueryRow(ctx, query, id).Scan(&n, &children, &raw); e != nil || n > 256 || children || len(raw) == 0 || len(raw) > 64<<10 {
		return "", ErrSharePlaybackForbidden
	}
	var members [][]json.RawMessage
	if json.Unmarshal([]byte(raw), &members) != nil || len(members) != n {
		return "", ErrSharePlaybackChanged
	}
	for _, m := range members {
		var member string
		if len(m) != 4 || json.Unmarshal(m[0], &member) != nil || !parentMutationID(member) {
			return "", ErrSharePlaybackChanged
		}
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:]), nil
}

// Current library authority is SQL, not the ordinary item/policy caches. The
// fixed prefix selects one known relation; no caller supplies an identifier.
func shareLibraryReceipt(ctx context.Context, tx pgx.Tx, resource, account, profile string) (string, error) {
	table, item := "", ""
	for _, candidate := range []struct{ prefix, table string }{{"localmedia:", "local_media_items"}, {"plexmedia:", "remote_media_items"}, {"jellyfinmedia:", "remote_media_items"}} {
		if strings.HasPrefix(resource, candidate.prefix) {
			table = candidate.table
			item = strings.TrimPrefix(resource, candidate.prefix)
			break
		}
	}
	if table == "" {
		return "", nil
	}
	if at := strings.IndexByte(item, '/'); at >= 0 {
		item = item[:at]
	}
	if !parentMutationID(item) {
		return "", ErrSharePlaybackChanged
	}
	if e := shareLockRelations(ctx, tx, []string{"local_media_items", "remote_media_items", "media_library_access_policies", "media_library_account_grants", "media_library_profile_grants"}); e != nil {
		return "", e
	}
	var library, physical string
	var missing bool
	query := `SELECT CASE WHEN octet_length(library_id) BETWEEN 1 AND 256 THEN library_id ELSE NULL END,is_missing,concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM ` + pgx.Identifier{table}.Sanitize() + ` WHERE id=$1`
	if e := tx.QueryRow(ctx, query, item).Scan(&library, &missing, &physical); e != nil || !parentMutationID(library) || missing {
		return "", ErrSharePlaybackForbidden
	}
	var mode, policy, accountGrant, profileGrant string
	const acl = `SELECT COALESCE((SELECT access_mode FROM media_library_access_policies WHERE library_id=$1),'restricted'),COALESCE((SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM media_library_access_policies WHERE library_id=$1),''),COALESCE((SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM media_library_account_grants WHERE library_id=$1 AND account_id=$2),''),COALESCE((SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) FROM media_library_profile_grants WHERE library_id=$1 AND profile_id=$3),'')`
	if e := tx.QueryRow(ctx, acl, library, account, profile).Scan(&mode, &policy, &accountGrant, &profileGrant); e != nil || mode != "all" && mode != "restricted" || mode != "all" && accountGrant == "" && profileGrant == "" {
		return "", ErrSharePlaybackForbidden
	}
	raw, _ := json.Marshal([]string{table, item, library, physical, mode, policy, accountGrant, profileGrant})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (c *ShareLinkRedeemer) Redeem(r *http.Request, token string) (result *ShareLinkRedemption, err error) {
	entered := time.Now()
	decoded, e := base64.RawURLEncoding.DecodeString(token)
	if r == nil || r.URL == nil || r.Method != http.MethodGet || r.URL.RawQuery != "" || r.URL.ForceQuery || len(decoded) != 32 || e != nil || base64.RawURLEncoding.EncodeToString(decoded) != token || !strings.HasSuffix(r.URL.Path, "/share/"+token) {
		return nil, ErrSharePlaybackRecreate
	}
	clear(decoded)
	if !c.enter() {
		return nil, ErrSharePlaybackUnavailable
	}
	defer c.operations.Done()
	peer := sharePeer(r)
	if !c.admit(peer) {
		return nil, ErrSharePlaybackBusy
	}
	defer c.refund(peer)
	original := r.Context()
	deadline := entered.Add(30 * time.Second)
	if at, ok := original.Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	ctx, cancel := context.WithDeadline(original, deadline)
	defer cancel()
	stop := familyMediaAfterFunc(c.ctx, cancel)
	defer stop()
	defer func() {
		if err == nil && (original.Err() != nil || !deadline.After(time.Now()) || !c.usable()) {
			err = ErrSharePlaybackChanged
		}
		if err != nil {
			result = nil
		}
	}()
	policy, e := c.security.CapturePolicy(ctx)
	if e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	g, e := c.store.beginSettingsProgressTx(ctx, c.coordinator)
	if e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	defer func() {
		if closeErr := finishShareCreationTransaction(g); err == nil && closeErr != nil {
			err = errors.Join(ErrSharePlaybackUnavailable, closeErr)
		}
	}()
	qctx := g.ctx
	first, e := shareReadDelegation(qctx, g.tx, token, false)
	if e != nil {
		return nil, e
	}
	if first.expires.Before(deadline) {
		deadline = first.expires
	}
	ids := []string{first.creator}
	if first.owner != first.creator {
		ids = append(ids, first.owner)
	}
	sort.Strings(ids)
	// Discover genuine expiry before potentially waiting on the deterministic locks.
	for _, id := range ids {
		account, e := shareAccount(qctx, g.tx, id, false)
		if e != nil {
			return nil, e
		}
		if account.expires != nil && account.expires.Before(deadline) {
			deadline = *account.expires
		}
	}
	bounded, retire := context.WithDeadline(qctx, deadline)
	defer retire()
	qctx = bounded
	currentAccounts := make(map[string]shareAccountRow, len(ids))
	for _, id := range ids {
		value, e := shareAccount(qctx, g.tx, id, true)
		if e != nil {
			return nil, e
		}
		currentAccounts[id] = value
	}
	link, e := shareReadDelegation(qctx, g.tx, token, true)
	if e != nil || !shareDelegationSame(first, link) {
		return nil, ErrSharePlaybackChanged
	}
	var owner, inc string
	var allowed, kids, pin bool
	if e = g.tx.QueryRow(qctx, `SELECT account_id,share_incarnation::text,allow_share_links,is_kids_profile,pin_hash<>'' FROM users WHERE id=$1 FOR UPDATE`, link.profile).Scan(&owner, &inc, &allowed, &kids, &pin); e != nil {
		return nil, ErrSharePlaybackChanged
	}
	creator, source := currentAccounts[link.creator], currentAccounts[link.owner]
	if owner != link.owner || inc != link.profileInc || creator.inc != link.creatorInc || source.inc != link.ownerInc {
		return nil, ErrSharePlaybackRecreate
	}
	if !allowed || kids || pin || link.creator != link.owner && !creator.master {
		return nil, ErrSharePlaybackForbidden
	}
	if e = shareLockPolicyRelations(qctx, g.tx); e != nil {
		return nil, e
	}
	for _, id := range ids {
		if _, e = shareHousehold(qctx, g.tx, id); e != nil {
			return nil, e
		}
	}
	fence, e := c.security.BindPolicy(qctx, policy, g.hold)
	if e != nil {
		return nil, ErrSharePlaybackChanged
	}
	if _, e = fence.EnforceProfilePins(); e != nil {
		return nil, ErrSharePlaybackChanged
	}
	var params map[string]string
	if json.Unmarshal(link.params, &params) != nil || len(params) > 64 {
		return nil, ErrSharePlaybackChanged
	}
	resource := shareNormalizeResource(params["sourcePath"])
	if resource == "" {
		resource = shareNormalizeResource(params["movie"])
	}
	if !shareValidResource(resource) {
		return nil, ErrSharePlaybackChanged
	}
	if _, e = shareLibraryReceipt(qctx, g.tx, resource, link.owner, link.profile); e != nil {
		return nil, e
	}
	owned := make(map[string]string, len(params))
	for k, v := range params {
		if len(k) > 128 || !utf8.ValidString(k) || !utf8.ValidString(v) {
			return nil, ErrSharePlaybackChanged
		}
		owned[k] = v
	}
	owned["profileId"] = link.profile
	delete(owned, "userId")
	raw := make([]byte, 32)
	if _, e = rand.Read(raw); e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	sessionToken := base64.RawURLEncoding.EncodeToString(raw)
	clear(raw)
	nonce, e := uuid.NewRandom()
	if e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	expiry := parentMutationMin(now.Add(6*time.Hour), &link.expires, creator.expires, source.expires)
	if !expiry.After(now) || qctx.Err() != nil || !deadline.After(now) {
		return nil, ErrSharePlaybackChanged
	}
	agent := r.UserAgent()
	if len(agent) > 4096 {
		agent = agent[:4096]
	}
	if !utf8.ValidString(agent) {
		agent = ""
	}
	session := models.Session{Token: sessionToken, AccountID: link.owner, IsMaster: false, CreatedAt: now, ExpiresAt: expiry, UserAgent: agent, IPAddress: r.RemoteAddr, Scope: models.SessionScopeStream, ScopeResource: resource}
	if len(session.IPAddress) > 256 {
		session.IPAddress = ""
	}
	if _, e = g.tx.Exec(qctx, `INSERT INTO sessions(token,account_id,is_master,created_at,expires_at,user_agent,ip_address,scope,scope_resource,client_id,share_binding_id) VALUES($1,$2,false,$3,$4,$5,$6,$7,$8,'',$9::uuid)`, session.Token, session.AccountID, session.CreatedAt, session.ExpiresAt, session.UserAgent, session.IPAddress, session.Scope, session.ScopeResource, nonce.String()); e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	if _, e = g.tx.Exec(qctx, `INSERT INTO share_playback_bindings(session_token,binding_id,share_token,creator_id,creator_incarnation,source_account_id,source_owner_incarnation,source_profile_id,source_profile_incarnation,resource,created_at,expires_at) VALUES($1,$2::uuid,$3,$4,$5::uuid,$6,$7::uuid,$8,$9::uuid,$10,$11,$12)`, session.Token, nonce.String(), link.token, link.creator, link.creatorInc, link.owner, link.ownerInc, link.profile, link.profileInc, resource, now, expiry); e != nil {
		return nil, ErrSharePlaybackUnavailable
	}
	tag, e := g.tx.Exec(qctx, `UPDATE share_links SET use_count=use_count+1,last_used_at=clock_timestamp() WHERE token=$1 AND active AND expires_at>clock_timestamp() AND use_count<CASE WHEN max_uses<=0 OR max_uses>$2 THEN $2 ELSE max_uses END`, link.token, models.ShareLinkMaxUses)
	if e != nil || tag.RowsAffected() != 1 {
		return nil, ErrSharePlaybackChanged
	}
	if _, e = c.security.BindPolicy(qctx, policy, g.hold); e != nil || qctx.Err() != nil || !c.usable() || !deadline.After(time.Now()) {
		return nil, ErrSharePlaybackChanged
	}
	if e = g.tx.Commit(qctx); e != nil {
		if errors.Is(e, pgx.ErrTxCommitRollback) {
			return nil, ErrSharePlaybackUnavailable
		}
		return nil, ErrSharePlaybackUnknown
	}
	return &ShareLinkRedemption{Session: session, Params: owned, PublicationUntil: parentMutationMin(deadline, &expiry)}, nil
}
