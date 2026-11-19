package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"novastream/models"
	"sort"
	"sync"
	"time"
)

// ParentLocalFileItem accepts exactly one opaque item ID. Paths, catalogue
// claims, hashes, durations and duplicate/case-folded keys are not accepted.
func ParentLocalFileItem(raw []byte) (string, error) {
	if len(raw) > ParentRatingBodyLimit {
		return "", ErrParentRatingBodyTooLarge
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	opening, err := d.Token()
	if err != nil || opening != json.Delim('{') {
		return "", ErrParentRatingPayload
	}
	var id string
	if !d.More() {
		return "", ErrParentRatingPayload
	}
	key, err := d.Token()
	if err != nil || key != "itemId" || d.Decode(&id) != nil || !parentLocalItemID(id) || d.More() {
		return "", ErrParentRatingPayload
	}
	if closing, err := d.Token(); err != nil || closing != json.Delim('}') {
		return "", ErrParentRatingPayload
	}
	var tail any
	if d.Decode(&tail) != io.EOF {
		return "", ErrParentRatingPayload
	}
	return id, nil
}
func parentLocalItemID(id string) bool {
	if !parentMutationID(id) || len(id) > 128 || id == "." || id == ".." {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return false
		}
	}
	return true
}

// PrepareParentLocalFile binds a server-derived opened-file fingerprint. It is
// not inferred from the JSON body or from a client supplied expected hash.
func (ds *DataStore) PrepareParentLocalFile(r *http.Request, target string, body []byte, binding string, tokens []string) (*ParentRatingIntent, error) {
	if len(binding) != 64 {
		return nil, ErrParentRatingChanged
	}
	i, err := ds.prepareParentRating(parentRatingLocalFile, r, target, body, tokens)
	if err != nil {
		return nil, err
	}
	i.state.fileBinding = binding
	i.state.bytes += len(binding)
	if i.state.bytes > parentModeSnapshotLimit {
		i.Close()
		return nil, ErrParentRatingUnavailable
	}
	return i, nil
}
func (ds *DataStore) BindParentLocalFileRetry(r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentRatingRetry(parentRatingLocalFile, r, i, target, body, tokens)
}

// ParentLocalFilePermit is a one-use result of the original password workflow,
// bound to its original request, target and held file. It cannot be imported,
// reused as a canonical classification or transferred to another HTTP request.
type ParentLocalFilePermit struct {
	mu       sync.Mutex
	self     *ParentLocalFilePermit
	store    *DataStore
	intent   *ParentRatingIntent
	used     bool
	original models.Session
	tuple    profilePinAccessTuple
	hashes   []string
	target   string
	expires  time.Time
}

func (*ParentLocalFilePermit) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentLocalFilePermit) UnmarshalJSON([]byte) error   { return ErrParentRatingChanged }
func (*ParentLocalFilePermit) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentLocalFilePermit(opaque)"))
}

// CommitParentLocalFile verifies the original PG tuple under the same epoch and
// row locks as parental writes. No profile/settings row is mutated by approval.
func (ds *DataStore) CommitParentLocalFile(i *ParentRatingIntent) (*ParentLocalFilePermit, error) {
	if i == nil || i.state == nil {
		return nil, ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.kind != parentRatingLocalFile || s.phase != parentCommitting || s.run == nil || s.run.Err() != nil || s.life.Err() != nil || len(s.fileBinding) != 64 {
		s.mu.Unlock()
		return nil, ErrParentRatingChanged
	}
	s.phase = parentRetired
	s.mu.Unlock()
	if err := ds.checkParentLocalFileTuple(s); err != nil {
		return nil, err
	}
	current, outcome, expiry, err := readProfilePinAccess(s.run, ds.pool, s.original, s.target, s.hashes, true)
	if err != nil || outcome != ProfilePinNoPin && outcome != ProfilePinUnlocked || !sameParentModeTuple(current, s.tuple) || !expiry.After(time.Now()) {
		return nil, ErrParentRatingChanged
	}
	p := &ParentLocalFilePermit{store: ds, intent: i, original: s.original, tuple: s.tuple, hashes: append([]string(nil), s.hashes...), target: s.target}
	p.expires = expiry
	p.self = p
	return p, nil
}
func (p *ParentLocalFilePermit) ExpiresAt() time.Time {
	if p == nil || p.self != p {
		return time.Time{}
	}
	return p.expires
}
func (p *ParentLocalFilePermit) Consume(ctx context.Context, target, binding string) error {
	if p == nil {
		return ErrParentRatingChanged
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.self != p || p.used || p.store == nil || p.intent == nil || p.intent.state == nil {
		return ErrParentRatingChanged
	}
	p.used = true
	s := p.intent.state
	if ctx != s.run || target != s.target || binding != s.fileBinding || ctx.Err() != nil || s.life.Err() != nil || !p.expires.After(time.Now()) {
		return ErrParentRatingChanged
	}
	return p.store.checkParentLocalFileTuple(s)
}

// Check retains the original successful PIN/session observation after the
// prompt HTTP retires. It never adopts a later unlock or restored row version.
func (p *ParentLocalFilePermit) Check(ctx context.Context) error {
	if p == nil {
		return ErrParentRatingChanged
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.self != p || !p.used || ctx == nil || ctx.Err() != nil || p.store == nil {
		return ErrParentRatingChanged
	}
	current, outcome, expiry, err := readProfilePinAccess(ctx, p.store.pool, p.original, p.target, p.hashes, true)
	if err != nil || outcome != ProfilePinNoPin && outcome != ProfilePinUnlocked || !sameParentModeTuple(current, p.tuple) || !expiry.After(time.Now()) || ctx.Err() != nil {
		return ErrParentRatingChanged
	}
	return nil
}
func (ds *DataStore) checkParentLocalFileTuple(s *parentRatingState) error {
	ctx := s.run
	if ctx == nil || ctx.Err() != nil || s.life.Err() != nil || !s.deadline.After(time.Now()) {
		return ErrParentRatingChanged
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return ErrParentRatingUnavailable
	}
	defer rollbackPinTx(tx, ctx)
	for _, scope := range []struct {
		name, key string
		expected  int64
	}{{"profile", s.target, s.tuple.profileGeneration}, {"session", pinSessionLineage(s.tuple.session.raw, s.original.Token), s.tuple.sessionGeneration}} {
		got, e := lockPinEpoch(ctx, tx, scope.name, scope.key)
		if e != nil || got != scope.expected {
			return ErrParentRatingChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil {
		return ErrParentRatingChanged
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found) != nil {
			return ErrParentRatingChanged
		}
	}
	var found string
	if tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found) != nil {
		return ErrParentRatingChanged
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return ErrParentRatingUnavailable
	}
	if tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.target).Scan(&found) != nil {
		return ErrParentRatingChanged
	}
	current, _, _, err := readProfilePinAccess(ctx, tx, s.original, s.target, s.hashes, true)
	if err != nil || !sameParentModeTuple(current, s.tuple) || ctx.Err() != nil || s.life.Err() != nil || !s.deadline.After(time.Now()) {
		return ErrParentRatingChanged
	}
	return tx.Commit(ctx)
}

// LocalParentFileSnapshot is server metadata from one current SQL observation.
// It is not itself a grant. In particular it never applies a master ACL bypass.
type LocalParentFileSnapshot struct {
	ItemID, LibraryID, FilePath, FileName, Revision string
	Roots                                           []string
}

func (ds *DataStore) CurrentLocalParentFile(ctx context.Context, account, profile, item string) (LocalParentFileSnapshot, error) {
	var out LocalParentFileSnapshot
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || !parentMutationID(account) || !parentMutationID(profile) || !parentMutationID(item) {
		return out, ErrParentRatingChanged
	}
	var roots, proof []byte
	var root string
	var missing, allowed bool
	err := ds.pool.QueryRow(ctx, `SELECT i.id,i.library_id,i.file_path,i.file_name,l.root_path,l.root_paths,i.is_missing,
 (p.access_mode='all' OR (p.access_mode='restricted' AND (EXISTS(SELECT 1 FROM media_library_account_grants g WHERE g.library_id=l.id AND g.account_id=u.account_id) OR EXISTS(SELECT 1 FROM media_library_profile_grants g WHERE g.library_id=l.id AND g.profile_id=u.id)))),
 jsonb_build_array(concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text),concat(l.tableoid::oid,':',l.xmin::text,':',l.ctid::text),concat(p.tableoid::oid,':',p.xmin::text,':',p.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),COALESCE((SELECT jsonb_agg(jsonb_build_array(g.account_id,concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text)) ORDER BY g.account_id) FROM media_library_account_grants g WHERE g.library_id=l.id AND g.account_id=u.account_id),'[]'::jsonb),COALESCE((SELECT jsonb_agg(jsonb_build_array(g.profile_id,concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text)) ORDER BY g.profile_id) FROM media_library_profile_grants g WHERE g.library_id=l.id AND g.profile_id=u.id),'[]'::jsonb))
 FROM local_media_items i JOIN local_media_libraries l ON l.id=i.library_id JOIN media_library_access_policies p ON p.library_id=l.id JOIN users u ON u.id=$2 AND u.account_id=$1 JOIN accounts a ON a.id=u.account_id WHERE i.id=$3 AND u.is_kids_profile AND (a.expires_at IS NULL OR a.expires_at>clock_timestamp())`, account, profile, item).Scan(&out.ItemID, &out.LibraryID, &out.FilePath, &out.FileName, &root, &roots, &missing, &allowed, &proof)
	if err != nil || missing || !allowed || json.Unmarshal(roots, &out.Roots) != nil || ctx.Err() != nil {
		return LocalParentFileSnapshot{}, ErrParentRatingChanged
	}
	if len(out.Roots) == 0 {
		out.Roots = []string{root}
	}
	metadataBytes := len(out.ItemID) + len(out.LibraryID) + len(out.FilePath) + len(out.FileName)
	for _, root := range out.Roots {
		metadataBytes += len(root)
	}
	if len(out.Roots) > 64 || len(proof) > 16<<10 || len(out.FilePath) > 4096 || metadataBytes > 16<<10 {
		return LocalParentFileSnapshot{}, ErrParentRatingChanged
	}
	out.Revision = regionalHash([]any{out.ItemID, out.LibraryID, out.FilePath, out.FileName, out.Roots, json.RawMessage(proof)})
	return out, nil
}
