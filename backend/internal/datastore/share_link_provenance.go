package datastore

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/settingscommit"
	"novastream/models"
)

var (
	ErrShareProvenanceChanged     = errors.New("share creation changed or expired")
	ErrShareProvenanceUnavailable = errors.New("share creation unavailable")
	ErrShareProvenanceForbidden   = errors.New("share creation not permitted")
	ErrShareProvenanceBusy        = errors.New("share creation capacity unavailable")
	ErrShareProvenanceUnknown     = errors.New("share creation commit outcome unknown")
)

// ShareLinkCreator writes provenance DATA only. It cannot redeem links, mint a
// playback session or grant a profile PIN/media permission.
type ShareLinkCreator struct {
	self        *ShareLinkCreator
	lifetime    context.Context
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
}

func NewShareLinkCreator(lifetime context.Context, store *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*ShareLinkCreator, error) {
	if lifetime == nil || lifetime.Err() != nil || !store.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrShareProvenanceUnavailable
	}
	c := &ShareLinkCreator{lifetime: lifetime, store: store, manager: manager, security: security, coordinator: coordinator}
	c.self = c
	return c, nil
}
func (c *ShareLinkCreator) valid() bool {
	return c != nil && c.self == c && c.lifetime != nil && c.lifetime.Err() == nil && c.store.SettingsCommitReady(c.coordinator) && c.security.Ready(c.manager, c.coordinator)
}
func (ShareLinkCreator) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ShareLinkCreator(opaque)"))
}
func (ShareLinkCreator) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ShareLinkCreator) UnmarshalJSON([]byte) error  { return ErrShareProvenanceChanged }

// Context is a cancellation signal only, never an authenticated request/frame.
func (c *ShareLinkCreator) Context() context.Context {
	if !c.valid() {
		return familyMediaClosedContext()
	}
	return c.lifetime
}

var shareCreationAdmission = struct {
	sync.Mutex
	total    int
	accounts map[string]int
}{accounts: make(map[string]int)}

func admitShareCreation(account string) bool {
	shareCreationAdmission.Lock()
	defer shareCreationAdmission.Unlock()
	if shareCreationAdmission.total >= 32 || shareCreationAdmission.accounts[account] >= 2 {
		return false
	}
	shareCreationAdmission.total++
	shareCreationAdmission.accounts[account]++
	return true
}
func releaseShareCreation(account string) {
	shareCreationAdmission.Lock()
	defer shareCreationAdmission.Unlock()
	shareCreationAdmission.total--
	shareCreationAdmission.accounts[account]--
	if shareCreationAdmission.accounts[account] == 0 {
		delete(shareCreationAdmission.accounts, account)
	}
}

// Create derives actor and source-owner separately from the actual current PG
// frame and rows. Params may describe playback but can never attest provenance.
func (c *ShareLinkCreator) Create(r *http.Request, profile string, params map[string]string, ttl time.Duration, maxUses int, label string) (link *models.ShareLink, err error) {
	entered := time.Now()
	if !c.valid() || r == nil || r.URL == nil || r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || !parentMutationID(profile) || !utf8.ValidString(profile) || len(label) > 4096 || !utf8.ValidString(label) || len(params) > 64 {
		return nil, ErrShareProvenanceChanged
	}
	requestContext := r.Context()
	frame, _ := requestContext.Value(pgRequestFrameKey{}).(*pgRequestFrame)
	original, ok := auth.GetSession(r)
	if frame == nil || frame.self != frame || frame.store != c.store || frame.ctx.Err() != nil || requestContext.Err() != nil || !ok || original != frame.session || original.Scope != "" || original.ScopeResource != "" || auth.GetAccountID(r) != original.AccountID || auth.IsMaster(r) != original.IsMaster {
		return nil, ErrShareProvenanceChanged
	}
	owned := make(map[string]string, len(params)+1)
	size := len(label)
	for key, value := range params {
		if len(key) > 128 || len(value) > (1<<20)-size || !utf8.ValidString(key) || !utf8.ValidString(value) {
			return nil, ErrShareProvenanceChanged
		}
		size += len(key) + len(value)
		if size > 1<<20 {
			return nil, ErrShareProvenanceChanged
		}
		owned[key] = value
	}
	if strings.TrimSpace(owned["sourcePath"]) == "" && strings.TrimSpace(owned["movie"]) == "" {
		return nil, ErrShareProvenanceChanged
	}
	owned["profileId"] = profile
	if !admitShareCreation(original.AccountID) {
		return nil, ErrShareProvenanceBusy
	}
	defer releaseShareCreation(original.AccountID)
	deadline := parentMutationMin(entered.Add(30*time.Second), &original.ExpiresAt, frame.actorExpiry)
	if at, ok := requestContext.Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	ctx, cancel := context.WithDeadline(requestContext, deadline)
	stopRuntime := familyMediaAfterFunc(c.lifetime, cancel)
	stopFrame := familyMediaAfterFunc(frame.ctx, cancel)
	finishDeadline := func() {}
	defer func() {
		stopFrame()
		stopRuntime()
		finishDeadline()
		cancel()
		// The private work/guard contexts were intentionally retired by cleanup.
		if err == nil && (r.Context() != requestContext || requestContext.Err() != nil || frame.ctx.Err() != nil || !deadline.After(time.Now()) || !c.valid()) {
			err = ErrShareProvenanceChanged
		}
		if err != nil {
			link = nil
		}
	}()
	policy, e := c.security.CapturePolicy(ctx)
	if e != nil {
		return nil, ErrShareProvenanceChanged
	}
	g, e := c.store.beginSettingsProgressTx(ctx, c.coordinator)
	if e != nil {
		return nil, ErrShareProvenanceUnavailable
	}
	defer func() {
		if closeErr := finishShareCreationTransaction(g); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	queryContext := g.ctx
	fence, e := c.security.BindPolicy(queryContext, policy, g.hold)
	if e != nil {
		return nil, ErrShareProvenanceChanged
	}
	if _, e = fence.EnforceProfilePins(); e != nil {
		return nil, ErrShareProvenanceChanged
	}
	var owner, initialProfileRow, initialOwnerRow string
	var ownerExpiry *time.Time
	e = g.tx.QueryRow(queryContext, `SELECT u.account_id,concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),a.expires_at FROM users u JOIN accounts a ON a.id=u.account_id WHERE u.id=$1`, profile).Scan(&owner, &initialProfileRow, &initialOwnerRow, &ownerExpiry)
	if e != nil || !parentMutationID(owner) || !utf8.ValidString(owner) || ownerExpiry != nil && !ownerExpiry.After(time.Now()) {
		return nil, ErrShareProvenanceForbidden
	}
	if ownerExpiry != nil && ownerExpiry.Before(deadline) {
		deadline = *ownerExpiry
		queryContext, finishDeadline = context.WithDeadline(g.ctx, deadline)
	}
	accounts := []string{original.AccountID}
	if owner != original.AccountID {
		accounts = append(accounts, owner)
	}
	sort.Strings(accounts)
	for _, account := range accounts {
		var id string
		if g.tx.QueryRow(queryContext, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, account).Scan(&id) != nil {
			return nil, ErrShareProvenanceChanged
		}
	}
	var token string
	if g.tx.QueryRow(queryContext, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, original.Token).Scan(&token) != nil {
		return nil, ErrShareProvenanceChanged
	}
	current, _, e := readPGRequestFrame(queryContext, g.tx, original.Token)
	if e != nil || current.session != original || !sameParentMutationRow(current.sessionRow, frame.sessionRow) || !sameParentMutationRow(current.actorRow, frame.actorRow) {
		return nil, ErrShareProvenanceChanged
	}
	var currentOwner, profileRow, ownerRow string
	var allowed bool
	e = g.tx.QueryRow(queryContext, `SELECT u.account_id,u.allow_share_links,concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text) FROM users u JOIN accounts a ON a.id=u.account_id WHERE u.id=$1 FOR UPDATE OF u`, profile).Scan(&currentOwner, &allowed, &profileRow, &ownerRow)
	if e != nil || currentOwner != owner || profileRow != initialProfileRow || ownerRow != initialOwnerRow {
		return nil, ErrShareProvenanceChanged
	}
	if !allowed || !original.IsMaster && owner != original.AccountID {
		return nil, ErrShareProvenanceForbidden
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	if ttl > 7*24*time.Hour {
		ttl = 7 * 24 * time.Hour
	}
	if maxUses <= 0 || maxUses > models.ShareLinkMaxUses {
		maxUses = models.ShareLinkMaxUses
	}
	paramsJSON, e := json.Marshal(owned)
	if e != nil || len(paramsJSON) > 1<<20 {
		return nil, ErrShareProvenanceChanged
	}
	raw := make([]byte, 32)
	if _, e = rand.Read(raw); e != nil {
		return nil, ErrShareProvenanceUnavailable
	}
	shareToken := base64.RawURLEncoding.EncodeToString(raw)
	clear(raw)
	now := time.Now().UTC()
	link = &models.ShareLink{Token: shareToken, AccountID: original.AccountID, IsMaster: original.IsMaster, Label: label, Params: owned, MaxUses: maxUses, Active: true, CreatedAt: now, ExpiresAt: parentMutationMin(now.Add(ttl), frame.actorExpiry, ownerExpiry)}
	if queryContext.Err() != nil || !deadline.After(time.Now()) {
		return nil, ErrShareProvenanceChanged
	}
	finalFence, e := c.security.BindPolicy(queryContext, policy, g.hold)
	if e != nil {
		return nil, ErrShareProvenanceChanged
	}
	if _, e = finalFence.EnforceProfilePins(); e != nil {
		return nil, ErrShareProvenanceChanged
	}
	tag, e := g.tx.Exec(queryContext, `INSERT INTO share_links (`+shareCols+`,provenance_version,source_profile_id,source_account_id) SELECT $1,$2,$3,$4,$5,$6,0,true,$7,$8,NULL,1,$9,$10 WHERE clock_timestamp()<$11`, link.Token, link.AccountID, link.IsMaster, link.Label, paramsJSON, link.MaxUses, link.CreatedAt, link.ExpiresAt, profile, owner, deadline)
	if e != nil || tag.RowsAffected() != 1 {
		return nil, ErrShareProvenanceUnavailable
	}
	if queryContext.Err() != nil || !deadline.After(time.Now()) || !c.valid() {
		return nil, ErrShareProvenanceChanged
	}
	if _, e = c.security.BindPolicy(queryContext, policy, g.hold); e != nil {
		return nil, ErrShareProvenanceChanged
	}
	if e = g.tx.Commit(queryContext); e != nil {
		if errors.Is(e, pgx.ErrTxCommitRollback) {
			return nil, ErrShareProvenanceUnavailable
		}
		return nil, ErrShareProvenanceUnknown
	}
	return link, nil
}

// The original SQL is already ended. Inspect/close that same physical backend
// before the existing guard can release its native hold or admission capacity.
func finishShareCreationTransaction(g *settingsProgressTx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	rollbackErr := g.tx.Rollback(ctx)
	cancel()
	var first error
	if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		first = ErrShareProvenanceUnavailable
	}
	physical := g.connection.Conn().PgConn()
	unhealthy := physical.IsClosed() || physical.IsBusy() || physical.TxStatus() != 'I'
	if unhealthy && first == nil {
		first = ErrShareProvenanceUnavailable
	}
	if !physical.IsClosed() && (unhealthy || first != nil) {
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		if e := g.connection.Conn().Close(ctx); first == nil && e != nil {
			first = ErrShareProvenanceUnavailable
		}
		cancel()
	}
	if physical.IsClosed() {
		<-physical.CleanupDone()
	}
	if e := g.Close(); first == nil && e != nil {
		first = ErrShareProvenanceUnavailable
	}
	return first
}
