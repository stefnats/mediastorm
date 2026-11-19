package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"novastream/internal/auth"
	"novastream/models"
	"strings"
	"time"
)

var ErrRegionalPolicyUnavailable = errors.New("current regional child policy unavailable")
var ErrProtectedFamilyRestore = errors.New("protected family restore requires a separately confirmed workflow")

type RegionalProfileSnapshot struct {
	ProfileID, AccountID, Revision string
	IsKids                         bool
	Policy                         *models.RegionalChildPolicy
}
type RegionalAccountSnapshot struct {
	AccountID, Revision string
	HasChildren         bool
}
type regionalPolicyRow struct {
	UserID    string    `json:"user_id"`
	Version   int       `json:"policy_version"`
	Country   string    `json:"country"`
	Movie     int       `json:"max_movie_age"`
	Episode   int       `json:"max_episode_age"`
	Revision  int64     `json:"revision"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy string    `json:"updated_by"`
}

func regionalHash(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func regionalRow(raw []byte) (*models.RegionalChildPolicy, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var row regionalPolicyRow
	if json.Unmarshal(raw, &row) != nil || row.Revision <= 0 || row.UpdatedAt.IsZero() || row.UpdatedBy == "" {
		return nil, ErrRegionalPolicyUnavailable
	}
	policy := models.RegionalChildPolicy{Version: row.Version, Country: row.Country, MaxMovieAge: row.Movie, MaxEpisodeAge: row.Episode}
	if !policy.Valid() {
		return nil, ErrRegionalPolicyUnavailable
	}
	return &policy, nil
}
func (ds *DataStore) CurrentRegionalProfile(ctx context.Context, id string) (RegionalProfileSnapshot, error) {
	var out RegionalProfileSnapshot
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || id == "" || strings.TrimSpace(id) != id || len(id) > 256 {
		return out, ErrRegionalPolicyUnavailable
	}
	var profileVersion, ownerVersion string
	var generation int64
	var raw []byte
	var expiry *time.Time
	err := ds.pool.QueryRow(ctx, `SELECT u.id,u.account_id,u.is_kids_profile,concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),v.generation,a.expires_at,COALESCE((SELECT to_jsonb(p) FROM regional_profile_policies p WHERE p.user_id=u.id),'null'::jsonb) FROM users u JOIN accounts a ON a.id=u.account_id JOIN user_settings_versions v ON v.user_id=u.id WHERE u.id=$1`, id).Scan(&out.ProfileID, &out.AccountID, &out.IsKids, &profileVersion, &ownerVersion, &generation, &expiry, &raw)
	if err != nil || out.ProfileID != id || generation < 0 || expiry != nil && !expiry.After(time.Now()) || ctx.Err() != nil {
		return RegionalProfileSnapshot{}, ErrRegionalPolicyUnavailable
	}
	policy, err := regionalRow(raw)
	if err != nil {
		return RegionalProfileSnapshot{}, err
	}
	out.Policy = policy
	// The unrelated global applicability fence is deliberately excluded.
	out.Revision = regionalHash([]any{out.ProfileID, out.AccountID, profileVersion, ownerVersion, generation, json.RawMessage(raw)})
	return out, nil
}
func (ds *DataStore) CurrentRegionalAccount(ctx context.Context, id string) (RegionalAccountSnapshot, error) {
	var out RegionalAccountSnapshot
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || id == "" || strings.TrimSpace(id) != id || len(id) > 256 {
		return out, ErrRegionalPolicyUnavailable
	}
	var before, after int64
	var ownerVersion string
	var expiry *time.Time
	var members []byte
	if ds.pool.QueryRow(ctx, `SELECT last_value FROM regional_policy_revision_seq`).Scan(&before) != nil {
		return out, ErrRegionalPolicyUnavailable
	}
	err := ds.pool.QueryRow(ctx, `SELECT a.id,concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),a.expires_at,EXISTS(SELECT 1 FROM users u WHERE u.account_id=a.id AND u.is_kids_profile),COALESCE((SELECT jsonb_agg(jsonb_build_array(u.id,u.is_kids_profile,concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text)) ORDER BY u.id) FROM users u WHERE u.account_id=a.id),'[]'::jsonb) FROM accounts a WHERE a.id=$1`, id).Scan(&out.AccountID, &ownerVersion, &expiry, &out.HasChildren, &members)
	if err != nil || out.AccountID != id || expiry != nil && !expiry.After(time.Now()) || ds.pool.QueryRow(ctx, `SELECT last_value FROM regional_policy_revision_seq`).Scan(&after) != nil || after != before || ctx.Err() != nil {
		return RegionalAccountSnapshot{}, ErrRegionalPolicyUnavailable
	}
	out.Revision = regionalHash([]any{id, ownerVersion, before, json.RawMessage(members)})
	return out, nil
}
func regionalPolicyState(ctx context.Context, q DB, id string) (string, error) {
	var raw []byte
	var generation int64
	if q.QueryRow(ctx, `SELECT v.generation,COALESCE((SELECT to_jsonb(p) FROM regional_profile_policies p WHERE p.user_id=u.id),'null'::jsonb) FROM users u JOIN user_settings_versions v ON v.user_id=u.id WHERE u.id=$1`, id).Scan(&generation, &raw) != nil || generation < 0 || ctx.Err() != nil {
		return "", ErrRegionalPolicyUnavailable
	}
	return regionalHash([]any{generation, json.RawMessage(raw)}), nil
}
func (ds *DataStore) ListRegionalPolicies(ctx context.Context) ([]models.RegionalChildPolicyBackup, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrRegionalPolicyUnavailable
	}
	return listRegionalPolicies(ctx, ds.pool)
}
func listRegionalPolicies(ctx context.Context, q DB) ([]models.RegionalChildPolicyBackup, error) {
	rows, err := q.Query(ctx, `SELECT user_id,policy_version,country,max_movie_age,max_episode_age FROM regional_profile_policies ORDER BY user_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []models.RegionalChildPolicyBackup{}
	for rows.Next() {
		var item models.RegionalChildPolicyBackup
		if err = rows.Scan(&item.ProfileID, &item.Policy.Version, &item.Policy.Country, &item.Policy.MaxMovieAge, &item.Policy.MaxEpisodeAge); err != nil || !item.Policy.Valid() {
			return nil, ErrRegionalPolicyUnavailable
		}
		out = append(out, item)
	}
	if rows.Err() != nil || ctx.Err() != nil {
		return nil, ErrRegionalPolicyUnavailable
	}
	return out, nil
}

// GuardRegionalRestore runs inside the original restore transaction before any
// delete, after the established reminders restore advisory lock. Strong relation
// locks retain the current child decision until commit.
func (t *Tx) GuardRegionalRestore(ctx context.Context, importedChildren bool) error {
	if _, err := t.tx.Exec(ctx, `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE; LOCK TABLE users,regional_profile_policies IN SHARE ROW EXCLUSIVE MODE`); err != nil {
		return err
	}
	var current bool
	if err := t.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE is_kids_profile)`).Scan(&current); err != nil {
		return err
	}
	if current || importedChildren {
		return ErrProtectedFamilyRestore
	}
	return ctx.Err()
}
func (t *Tx) RestoreRegionalPolicies(ctx context.Context, items []models.RegionalChildPolicyBackup) error {
	for _, item := range items {
		if !item.Policy.Valid() || item.ProfileID == "" {
			return ErrRegionalPolicyUnavailable
		}
		p := item.Policy
		if _, err := t.tx.Exec(ctx, `INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by) VALUES($1,$2,$3,$4,$5,1,clock_timestamp(),'backup-restore')`, item.ProfileID, p.Version, p.Country, p.MaxMovieAge, p.MaxEpisodeAge); err != nil {
			return err
		}
	}
	return nil
}

func (ds *DataStore) ReadRegionalPolicyRequest(r *http.Request, target string, tokens []string) (*models.RegionalChildPolicy, error) {
	if r == nil {
		return nil, ErrRegionalPolicyUnavailable
	}
	session, ok := auth.GetSession(r)
	if !ok || session.Scope != "" || session.ScopeResource != "" {
		return nil, ErrRegionalPolicyUnavailable
	}
	reader := NewBoundProfilePinAccessReader(ds)
	observation, err := reader.Read(r, target, tokens)
	if err != nil {
		return nil, err
	}
	defer observation.Close()
	// Read already captured the original owned session/profile/PIN outcome.
	// Check is one-shot and cancels that observation, so consume it only after
	// the policy read. A new snapshot must never adopt changed authority.
	captured := observation.state.outcome
	if captured != ProfilePinNoPin && captured != ProfilePinUnlocked {
		return nil, ErrRegionalPolicyUnavailable
	}
	snapshot, err := ds.CurrentRegionalProfile(r.Context(), target)
	if err != nil {
		return nil, err
	}
	outcome, err := reader.Check(observation)
	if err != nil || outcome != ProfilePinNoPin && outcome != ProfilePinUnlocked {
		return nil, ErrRegionalPolicyUnavailable
	}
	return snapshot.Policy, nil
}
