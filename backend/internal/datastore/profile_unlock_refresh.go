package datastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"novastream/internal/auth"
	"novastream/models"
)

const pinBindingVersion = 1
const pinBindingColumns = `pin_proof_version,pin_profile_generation,pin_session_generation,pin_verified_hash,pin_binding_digest,pin_session_lineage`

// These fields are private storage provenance, never response/grant DTO fields.
// Old Create/Rebind writers cannot silently carry a changed payload forward:
// the digest includes every mutable legacy grant field as well as both epochs.
type pinGrantBinding struct {
	Lineage           *string `json:"pin_session_lineage"`
	Version           *int    `json:"pin_proof_version"`
	ProfileGeneration *int64  `json:"pin_profile_generation"`
	SessionGeneration *int64  `json:"pin_session_generation"`
	VerifiedHash      *string `json:"pin_verified_hash"`
	Digest            *string `json:"pin_binding_digest"`
}

func pinBindingDigest(grant models.ProfileUnlock, profileGeneration, sessionGeneration int64, verifiedHash string, lineage string) string {
	raw, _ := json.Marshal([]any{pinBindingVersion, grant.TokenHash, grant.SessionHash, grant.AccountID, grant.ProfileID, grant.PinFingerprint, grant.SessionBound,
		grant.ExpiresAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano), grant.CreatedAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano), profileGeneration, sessionGeneration, verifiedHash, lineage})
	return profileUnlockDigest(string(raw))
}
func validPinBinding(binding pinGrantBinding, grant models.ProfileUnlock, profileGeneration, sessionGeneration int64, pinHash string, lineage string) bool {
	if binding.Lineage == nil || *binding.Lineage != lineage || binding.Version == nil || *binding.Version != pinBindingVersion || binding.ProfileGeneration == nil || binding.SessionGeneration == nil || binding.VerifiedHash == nil || binding.Digest == nil {
		return false
	}
	verified := profileUnlockDigest(pinHash)
	return *binding.ProfileGeneration == profileGeneration && *binding.SessionGeneration == sessionGeneration && *binding.VerifiedHash == verified && *binding.Digest == pinBindingDigest(grant, profileGeneration, sessionGeneration, verified, lineage)
}

// Prototype callers without a public frame retain their old raw-session scope.
// Public requests always confirm a persisted lineage before frame publication.
func pinSessionLineage(raw []byte, token string) string {
	var row struct {
		Lineage *string `json:"pin_unlock_lineage"`
	}
	if json.Unmarshal(raw, &row) == nil && row.Lineage != nil && profileUnlockHash(*row.Lineage) {
		return *row.Lineage
	}
	return profileUnlockDigest(token)
}

func persistedPinLineage(raw []byte) bool {
	var row struct {
		Lineage *string `json:"pin_unlock_lineage"`
	}
	return json.Unmarshal(raw, &row) == nil && row.Lineage != nil && profileUnlockHash(*row.Lineage)
}

type pinRefreshGrant struct {
	Row     json.RawMessage `json:"row"`
	Version string          `json:"version"`
}

func readPinRefreshGrants(ctx context.Context, q parentMutationQuerier, sessionHash string, locking bool) ([]pinRefreshGrant, []byte, error) {
	suffix := ""
	if locking {
		suffix = " FOR UPDATE"
	}
	var raw []byte
	err := q.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(jsonb_build_object('row',g.payload,'version',g.version) ORDER BY g.token_hash),'[]'::jsonb) FROM (SELECT token_hash,to_jsonb(p) AS payload,concat(p.tableoid::oid,':',p.xmin::text,':',p.ctid::text) AS version FROM profile_unlocks p WHERE session_hash=$1 ORDER BY token_hash`+suffix+`) g`, sessionHash).Scan(&raw)
	if err != nil {
		return nil, nil, pinContextError(ctx, ErrPGPinUnavailable)
	}
	var rows []pinRefreshGrant
	if len(raw) > 4<<20 || json.Unmarshal(raw, &rows) != nil || len(rows) > 4096 {
		return nil, nil, ErrPGPinUnavailable
	}
	return rows, raw, nil
}
func decodeRefreshGrant(raw []byte) (models.ProfileUnlock, error) {
	var row struct {
		TokenHash    string    `json:"token_hash"`
		SessionHash  string    `json:"session_hash"`
		AccountID    string    `json:"account_id"`
		ProfileID    string    `json:"profile_id"`
		Fingerprint  string    `json:"pin_fingerprint"`
		SessionBound bool      `json:"session_bound"`
		Expires      time.Time `json:"expires_at"`
		Created      time.Time `json:"created_at"`
	}
	if !profilePinRequiredFields(raw, []string{"token_hash", "session_hash", "account_id", "profile_id", "pin_fingerprint", "session_bound", "expires_at", "created_at"}, false) || json.Unmarshal(raw, &row) != nil || !profileUnlockHash(row.TokenHash) || !profileUnlockHash(row.SessionHash) || !parentMutationID(row.ProfileID) || !parentMutationID(row.AccountID) {
		return models.ProfileUnlock{}, ErrPGPinUnavailable
	}
	return models.ProfileUnlock{TokenHash: row.TokenHash, SessionHash: row.SessionHash, AccountID: row.AccountID, ProfileID: row.ProfileID, PinFingerprint: row.Fingerprint, SessionBound: row.SessionBound, ExpiresAt: row.Expires, CreatedAt: row.Created}, nil
}
func lockPinEpoch(ctx context.Context, tx pgx.Tx, scope, key string) (int64, error) {
	if _, err := tx.Exec(ctx, `INSERT INTO profile_unlock_revocations(scope,scope_key,generation) VALUES($1,$2,0) ON CONFLICT(scope,scope_key) DO NOTHING`, scope, key); err != nil {
		return 0, pinContextError(ctx, ErrPGPinUnavailable)
	}
	var epoch int64
	if err := tx.QueryRow(ctx, `SELECT generation FROM profile_unlock_revocations WHERE scope=$1 AND scope_key=$2 FOR UPDATE`, scope, key).Scan(&epoch); err != nil || epoch < 0 {
		return 0, pinContextError(ctx, ErrPGPinUnavailable)
	}
	return epoch, nil
}

// RefreshProfilePinSession is the PG-only public rotation. The original frame
// is never recaptured after waiting. Only proven, still live grants move;
// legacy/unbound, expired or revoked rows are not given a new lifetime.
func (ds *DataStore) RefreshProfilePinSession(r *http.Request, newToken string, duration time.Duration) (models.Session, error) {
	var empty models.Session
	if err := ds.CheckProfilePinRequestFrame(r); err != nil {
		return empty, err
	}
	frame := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	decoded, err := base64.URLEncoding.DecodeString(newToken)
	if err != nil || len(decoded) != 32 || base64.URLEncoding.EncodeToString(decoded) != newToken || newToken == frame.session.Token || frame.session.Scope != "" || frame.session.ScopeResource != "" || duration <= 0 || !persistedPinLineage(frame.sessionRow.raw) {
		return empty, ErrPGPinChanged
	}
	ctx, cancel := context.WithTimeout(r.Context(), profileUnlockStateBudget)
	defer cancel()
	oldHash := profileUnlockDigest(frame.session.Token)
	lineage := pinSessionLineage(frame.sessionRow.raw, frame.session.Token)
	grants, originalRows, err := readPinRefreshGrants(ctx, ds.pool, oldHash, false)
	if err != nil {
		return empty, err
	}
	profiles := map[string]bool{}
	owners := map[string]string{}
	accounts := map[string]bool{frame.session.AccountID: true}
	for _, row := range grants {
		g, err := decodeRefreshGrant(row.Row)
		if err != nil {
			return empty, err
		}
		profiles[g.ProfileID] = true
	}
	profileIDs := make([]string, 0, len(profiles))
	for id := range profiles {
		profileIDs = append(profileIDs, id)
	}
	sort.Strings(profileIDs)
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return empty, pinContextError(ctx, ErrPGPinUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	for _, id := range profileIDs {
		if _, err = lockPinEpoch(ctx, tx, "profile", id); err != nil {
			return empty, err
		}
	}
	if _, err = lockPinEpoch(ctx, tx, "session", lineage); err != nil {
		return empty, err
	}
	// Account ownership is read after generation locks; target ownership is
	// checked again under the later target locks before any row can transfer.
	for _, id := range profileIDs {
		var owner string
		err = tx.QueryRow(ctx, `SELECT account_id FROM users WHERE id=$1`, id).Scan(&owner)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return empty, pinContextError(ctx, ErrPGPinUnavailable)
		}
		accounts[owner] = true
		owners[id] = owner
	}
	accountIDs := make([]string, 0, len(accounts))
	for id := range accounts {
		accountIDs = append(accountIDs, id)
	}
	sort.Strings(accountIDs)
	for _, id := range accountIDs {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(ctx, ErrPGPinChanged)
		}
	}
	var found string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, frame.session.Token).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrPGPinChanged)
	}
	current, now, err := readPGRequestFrame(ctx, tx, frame.session.Token)
	if err != nil || !sameParentMutationRow(current.sessionRow, frame.sessionRow) || !sameParentMutationRow(current.actorRow, frame.actorRow) {
		return empty, pinContextError(ctx, ErrPGPinChanged)
	}
	for _, id := range profileIDs {
		err = tx.QueryRow(ctx, `SELECT account_id FROM users WHERE id=$1 FOR UPDATE`, id).Scan(&found)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return empty, pinContextError(ctx, ErrPGPinUnavailable)
		}
		if err == nil && found != owners[id] {
			return empty, ErrPGPinChanged
		}
	}
	locked, lockedRows, err := readPinRefreshGrants(ctx, tx, oldHash, true)
	if err != nil {
		return empty, err
	}
	if string(originalRows) != string(lockedRows) {
		return empty, ErrPGPinChanged
	}
	next := frame.session
	next.Token = newToken
	next.ExpiresAt = parentMutationMin(now.Add(duration), frame.actorExpiry).Truncate(time.Microsecond)
	if !next.ExpiresAt.After(now) {
		return empty, ErrPGPinChanged
	}
	if _, err = tx.Exec(ctx, `INSERT INTO sessions(token,account_id,is_master,expires_at,created_at,user_agent,ip_address,client_id,scope,scope_resource,pin_unlock_lineage) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, next.Token, next.AccountID, next.IsMaster, next.ExpiresAt, next.CreatedAt, next.UserAgent, next.IPAddress, next.ClientID, next.Scope, next.ScopeResource, lineage); err != nil {
		return empty, pinContextError(ctx, ErrPGPinUnavailable)
	}
	for _, row := range locked {
		g, err := decodeRefreshGrant(row.Row)
		if err != nil {
			return empty, err
		}
		tuple, outcome, _, err := readProfilePinAccess(ctx, tx, frame.session, g.ProfileID, []string{g.TokenHash}, true)
		if err != nil {
			return empty, err
		}
		if outcome != ProfilePinUnlocked {
			continue
		}
		var profile struct {
			Hash string `json:"pin_hash"`
		}
		if json.Unmarshal(tuple.profile.raw, &profile) != nil {
			return empty, ErrPGPinUnavailable
		}
		expiry := parentMutationMin(g.ExpiresAt, &next.ExpiresAt)
		if g.SessionBound {
			expiry = parentMutationMin(next.ExpiresAt, frame.actorExpiry)
			var owner struct {
				Expiry *time.Time `json:"expires_at"`
			}
			if json.Unmarshal(tuple.owner.raw, &owner) != nil {
				return empty, ErrPGPinUnavailable
			}
			expiry = parentMutationMin(expiry, owner.Expiry)
		}
		if !g.ExpiresAt.After(now) || !expiry.After(now) {
			continue
		}
		g.SessionHash = profileUnlockDigest(next.Token)
		g.ExpiresAt = expiry.Truncate(time.Microsecond)
		digest := pinBindingDigest(g, tuple.profileGeneration, tuple.sessionGeneration, profileUnlockDigest(profile.Hash), lineage)
		tag, err := tx.Exec(ctx, `UPDATE profile_unlocks SET session_hash=$1,expires_at=$2,pin_binding_digest=$3 WHERE token_hash=$4 AND expires_at>clock_timestamp() AND $2>clock_timestamp()`, g.SessionHash, g.ExpiresAt, digest, g.TokenHash)
		if err != nil || tag.RowsAffected() != 1 {
			return empty, pinContextError(ctx, ErrPGPinChanged)
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM profile_unlocks WHERE session_hash=$1`, oldHash); err != nil {
		return empty, pinContextError(ctx, ErrPGPinUnavailable)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM sessions WHERE token=$1`, frame.session.Token); err != nil {
		return empty, pinContextError(ctx, ErrPGPinUnavailable)
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return empty, ErrPGPinUnavailable
		}
		return empty, ErrPGPinOutcomeUnknown
	}
	return next, nil
}

// LogoutProfilePinSession revokes the exact original lineage. A refresh may
// already have rotated its session row; no unrelated new login is adopted.
func (ds *DataStore) LogoutProfilePinSession(r *http.Request) error {
	if ds == nil || ds.pool == nil || r == nil {
		return ErrPGPinUnavailable
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != ds || frame.session.Scope != "" || !persistedPinLineage(frame.sessionRow.raw) {
		return ErrPGPinChanged
	}
	original, ok := auth.GetSession(r)
	if !ok || original != frame.session || auth.GetAccountID(r) != original.AccountID || auth.IsMaster(r) != original.IsMaster {
		return ErrPGPinChanged
	}
	if err := frame.ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(r.Context(), profileUnlockStateBudget)
	defer cancel()
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return pinContextError(ctx, ErrPGPinUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	lineage := pinSessionLineage(frame.sessionRow.raw, original.Token)
	if _, err = lockPinEpoch(ctx, tx, "session", lineage); err != nil {
		return err
	}
	var raw []byte
	var version string
	err = tx.QueryRow(ctx, `SELECT to_jsonb(a),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text) FROM accounts a WHERE id=$1 FOR UPDATE`, original.AccountID).Scan(&raw, &version)
	if err != nil || !sameParentMutationRow(frame.actorRow, parentMutationRow{raw: raw, version: version}) {
		return pinContextError(ctx, ErrPGPinChanged)
	}
	var hashes []string
	err = tx.QueryRow(ctx, `SELECT COALESCE(array_agg(token ORDER BY token),'{}'::text[]) FROM (SELECT token FROM sessions WHERE pin_unlock_lineage=$1 AND account_id=$2 AND client_id=$3 AND scope='' AND scope_resource='' FOR UPDATE) s`, lineage, original.AccountID, original.ClientID).Scan(&hashes)
	if err != nil {
		return pinContextError(ctx, ErrPGPinUnavailable)
	}
	if _, err = tx.Exec(ctx, `UPDATE profile_unlock_revocations SET generation=generation+1 WHERE scope='session' AND scope_key=$1`, lineage); err != nil {
		return pinContextError(ctx, ErrPGPinUnavailable)
	}
	for _, token := range hashes {
		if _, err = tx.Exec(ctx, `DELETE FROM profile_unlocks WHERE session_hash=$1`, profileUnlockDigest(token)); err != nil {
			return pinContextError(ctx, ErrPGPinUnavailable)
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM profile_unlocks WHERE session_hash=$1`, profileUnlockDigest(original.Token)); err != nil {
		return pinContextError(ctx, ErrPGPinUnavailable)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM sessions WHERE token=ANY($1::text[])`, hashes); err != nil {
		return pinContextError(ctx, ErrPGPinUnavailable)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return ErrPGPinUnavailable
		}
		return ErrPGPinOutcomeUnknown
	}
	return nil
}
