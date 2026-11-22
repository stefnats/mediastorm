package datastore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/models"
)

const ParentModeIntentHeader = "X-Parent-Intent"
const parentModeLifetime = 30 * time.Second
const parentModeSnapshotLimit = 64 << 10

var (
	ErrParentModeChanged     = errors.New("parent mode confirmation expired or changed")
	ErrParentModeUnavailable = errors.New("parent mode confirmation unavailable")
	ErrParentModeCredential  = errors.New("parent mode credential confirmation failed")
	ErrParentModeUnknown     = errors.New("parent mode mutation outcome unknown")
)

type parentModeState struct {
	mu                                      sync.Mutex
	self                                    *ParentModeIntent
	store                                   *DataStore
	original                                models.Session
	tuple                                   profilePinAccessTuple
	hashes                                  []string
	mode, method, path, query, clientHeader string
	deadline                                time.Time
	life                                    context.Context
	stop                                    context.CancelFunc
	run                                     context.Context
	retryCtx                                context.Context
	bound                                   bool
	cancel                                  context.CancelFunc
	phase                                   parentMutationPhase
	bytes                                   int
	target                                  string
	compare                                 func([]byte, []byte) error
}

// ParentModeIntent is an opaque one-shot operation, never a family/media grant.
// Its independent timer is the explicit, bounded handoff between two HTTPs;
// the completed first response's canceled context is not reused as live proof.
type ParentModeIntent struct{ state *parentModeState }

func (*ParentModeIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentModeIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentModeIntent(opaque)"))
}
func (i *ParentModeIntent) Close() {
	if i == nil || i.state == nil {
		return
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i {
		return
	}
	if s.phase != parentCommitting {
		s.phase = parentRetired
	}
	s.stop()
	if s.cancel != nil {
		s.cancel()
	}
}
func parentModeValid(mode string) bool {
	return mode == "" || mode == "rating" || mode == "rating_de" || mode == "content_list"
}
func parentModeRequestBoundary(r *http.Request) (string, string, string, error) {
	return parentConfirmationRequestBoundary(r, http.MethodPut)
}

// Each typed operation supplies its own fixed method; this helper does not
// choose an action from request input or reinterpret a mode as a list URL.
func parentConfirmationRequestBoundary(r *http.Request, method string) (string, string, string, error) {
	if r == nil || r.URL == nil || r.Method != method || len(r.URL.EscapedPath())+len(r.URL.RawQuery) > 8192 || len(r.Header.Values("X-Client-ID")) > 1 {
		return "", "", "", ErrParentModeChanged
	}
	client := r.Header.Get("X-Client-ID")
	if len(client) > 256 {
		return "", "", "", ErrParentModeChanged
	}
	return r.URL.EscapedPath(), r.URL.RawQuery, client, nil
}
func parentModeHashes(tokens []string) ([]string, error) {
	if len(tokens) > maxProfilePinAccessTokens {
		return nil, ErrParentModeUnavailable
	}
	total := 0
	seen := map[string]bool{}
	result := []string{}
	for _, token := range tokens {
		if len(token) > maxProfilePinAccessCredentialBytes-total {
			return nil, ErrParentModeUnavailable
		}
		total += len(token)
		if len(token) == profilePinAccessTokenBytes && profileUnlockCredential(token) {
			h := profileUnlockDigest(token)
			if !seen[h] {
				seen[h] = true
				result = append(result, h)
			}
		}
	}
	sort.Strings(result)
	return result, nil
}
func sameParentModeTuple(a, b profilePinAccessTuple) bool {
	return sameParentMutationRow(a.session, b.session) && sameParentMutationRow(a.actor, b.actor) && sameParentMutationRow(a.profile, b.profile) && sameParentMutationRow(a.owner, b.owner) && string(a.grants) == string(b.grants) && a.profileGeneration == b.profileGeneration && a.sessionGeneration == b.sessionGeneration
}
func (ds *DataStore) PrepareParentMode(r *http.Request, target, mode string, tokens []string) (*ParentModeIntent, error) {
	if ds == nil || ds.pool == nil || !parentMutationID(target) || !parentModeValid(mode) {
		return nil, ErrParentModeChanged
	}
	if r == nil {
		return nil, ErrParentModeChanged
	}
	preparedDeadline := time.Now().Add(parentModeLifetime)
	prepCtx, prepCancel := context.WithDeadline(r.Context(), preparedDeadline)
	defer prepCancel()
	r = r.WithContext(prepCtx)
	path, query, client, err := parentModeRequestBoundary(r)
	if err != nil {
		return nil, err
	}
	if err = ds.CheckProfilePinRequestFrame(r); err != nil {
		return nil, err
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil {
		return nil, err
	}
	reader := NewBoundProfilePinAccessReader(ds)
	observation, err := reader.Read(r, target, tokens)
	if err != nil {
		return nil, err
	}
	defer observation.Close()
	outcome, err := reader.Check(observation)
	if err != nil {
		return nil, err
	}
	if outcome == ProfilePinNotFound {
		return nil, ErrProfilePinAccessNotFound
	}
	if outcome == ProfilePinUnavailable {
		return nil, ErrParentModeUnavailable
	}
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrParentModeChanged
	}
	tuple := observation.state.tuple
	var actor struct {
		Hash string `json:"password_hash"`
	}
	if !profilePinRequiredFields(tuple.actor.raw, []string{"password_hash"}, false) || json.Unmarshal(tuple.actor.raw, &actor) != nil {
		return nil, ErrParentModeUnavailable
	}
	if _, err = bcrypt.Cost([]byte(actor.Hash)); err != nil {
		return nil, ErrParentModeUnavailable
	}
	size := len(tuple.session.raw) + len(tuple.actor.raw) + len(tuple.profile.raw) + len(tuple.owner.raw) + len(tuple.grants) + len(path) + len(query) + len(client) + len(mode) + len(hashes)*64 + 1024
	if size > parentModeSnapshotLimit {
		return nil, ErrParentModeUnavailable
	}
	deadline := preparedDeadline
	if at, ok := observation.state.ctx.Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	if at, ok := r.Context().Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	if err = r.Context().Err(); err != nil {
		return nil, err
	}
	if !deadline.After(time.Now()) {
		return nil, ErrParentModeChanged
	}
	life, stop := context.WithDeadline(context.Background(), deadline)
	s := &parentModeState{store: ds, original: original, tuple: tuple, hashes: hashes, mode: mode, method: r.Method, path: path, query: query, clientHeader: client, deadline: deadline, life: life, stop: stop, phase: parentPrepared, bytes: size, target: target, compare: bcrypt.CompareHashAndPassword}
	value := &ParentModeIntent{state: s}
	s.self = value
	return value, nil
}

// ParentModeBudget returns only server bookkeeping for the bounded service map.
func (ds *DataStore) ParentModeBudget(i *ParentModeIntent) (string, int, time.Time, error) {
	if i == nil || i.state == nil {
		return "", 0, time.Time{}, ErrParentModeChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", 0, time.Time{}, ErrParentModeChanged
	}
	return s.original.AccountID, s.bytes, s.deadline, nil
}

// BindParentModeRetry consumes the original operation before further IO. A new
// HTTP context may shorten its original lifetime, never renew its snapshot.
func (ds *DataStore) BindParentModeRetry(r *http.Request, i *ParentModeIntent, target, mode string, tokens []string) error {
	if i == nil || i.state == nil {
		return ErrParentModeChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentPrepared {
		s.mu.Unlock()
		return ErrParentModeChanged
	}
	s.phase = parentVerifying
	if s.life.Err() != nil {
		s.mu.Unlock()
		return ErrParentModeChanged
	}
	path, query, client, err := parentModeRequestBoundary(r)
	if err != nil || target != s.target || mode != s.mode || r.Method != s.method || path != s.path || query != s.query || client != s.clientHeader {
		s.mu.Unlock()
		return ErrParentModeChanged
	}
	if err = r.Context().Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	original, ok := auth.GetSession(r)
	if !ok || original != s.original || auth.GetAccountID(r) != s.original.AccountID || auth.IsMaster(r) != s.original.IsMaster {
		s.mu.Unlock()
		return ErrParentModeChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil || strings.Join(hashes, ",") != strings.Join(s.hashes, ",") {
		s.mu.Unlock()
		return ErrParentModeChanged
	}
	run, cancel := context.WithDeadline(r.Context(), s.deadline)
	s.run = run
	s.retryCtx = r.Context()
	s.cancel = cancel
	s.mu.Unlock()
	if err = ds.CheckProfilePinRequestFrame(r.WithContext(run)); err != nil {
		return err
	}
	tuple, _, _, err := readProfilePinAccess(run, ds.pool, s.original, s.target, s.hashes, true)
	if err != nil {
		return err
	}
	if !sameParentModeTuple(tuple, s.tuple) {
		return ErrParentModeChanged
	}
	if err = run.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != parentVerifying || s.life.Err() != nil {
		return ErrParentModeChanged
	}
	s.phase = parentVerified // bound, but not yet credential-verified
	return nil
}

// VerifyParentModePassword repeats actual original-hash bcrypt outside SQL
// locks. The HTTP Guard independently owns the existing shared guess budget.
func (ds *DataStore) VerifyParentModePassword(r *http.Request, i *ParentModeIntent) error {
	if i == nil || i.state == nil {
		return ErrParentModeChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || !s.bound || r == nil || r.Context() != s.retryCtx {
		s.mu.Unlock()
		return ErrParentModeChanged
	}
	if err := s.run.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.phase = parentVerifying
	var actor struct {
		Hash string `json:"password_hash"`
	}
	err := json.Unmarshal(s.tuple.actor.raw, &actor)
	password := r.Header.Get(profilePinParentPasswordHeader)
	s.mu.Unlock()
	if strings.HasPrefix(password, "base64:") {
		raw, e := base64.StdEncoding.DecodeString(strings.TrimPrefix(password, "base64:"))
		if e != nil {
			return ErrParentModeCredential
		}
		password = string(raw)
	}
	password = strings.TrimSpace(password)
	if err != nil || password == "" || len(password) > 4096 {
		return ErrParentModeCredential
	}
	compared := s.compare([]byte(actor.Hash), []byte(password))
	password = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.run.Err(); err != nil {
		return err
	}
	if s.life.Err() != nil || s.phase != parentVerifying {
		return ErrParentModeChanged
	}
	if compared != nil {
		s.phase = parentRetired
		return ErrParentModeCredential
	}
	s.phase = parentCommitting // credential verified; Commit consumes this state
	return nil
}
func (ds *DataStore) ParentModeTarget(i *ParentModeIntent, expected UserRevision) (string, error) {
	if i == nil || i.state == nil {
		return "", ErrParentModeChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil || s.run.Err() != nil || s.life.Err() != nil {
		return "", ErrParentModeChanged
	}
	if expected.owner != ds || expected.id != s.target || expected.version != s.tuple.profile.version {
		return "", ErrUserWriteConflict
	}
	return s.target, nil
}

// CommitParentMode changes exactly kids_mode and updated_at. Its final tuple
// includes current role/credentials/PIN, all row versions and both generations.
func (ds *DataStore) CommitParentMode(i *ParentModeIntent) (UserWriteRecord, error) {
	var empty UserWriteRecord
	if i == nil || i.state == nil {
		return empty, ErrParentModeChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil {
		s.mu.Unlock()
		return empty, ErrParentModeChanged
	}
	s.phase = parentRetired
	ctx := s.run
	s.mu.Unlock()
	// The upper operation owns cancellation through response publication.
	// Consuming this write must not cancel the shared authenticated runctx.
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if s.life.Err() != nil {
		return empty, ErrParentModeChanged
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return empty, pinContextError(ctx, ErrParentModeUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	for _, scope := range []struct {
		name, key string
		expected  int64
	}{{"profile", s.target, s.tuple.profileGeneration}, {"session", pinSessionLineage(s.tuple.session.raw, s.original.Token), s.tuple.sessionGeneration}} {
		got, e := lockPinEpoch(ctx, tx, scope.name, scope.key)
		if e != nil {
			return empty, e
		}
		if got != scope.expected {
			return empty, ErrParentModeChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil {
		return empty, ErrParentModeUnavailable
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(ctx, ErrParentModeChanged)
		}
	}
	var found string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentModeChanged)
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return empty, pinContextError(ctx, ErrParentModeUnavailable)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.target).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentModeChanged)
	}
	current, _, _, err := readProfilePinAccess(ctx, tx, s.original, s.target, s.hashes, true)
	if err != nil {
		return empty, err
	}
	if !sameParentModeTuple(current, s.tuple) {
		return empty, ErrParentModeChanged
	}
	record, err := ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_mode=$1,updated_at=clock_timestamp() WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4 RETURNING `+userColumns+`,`+userWriteVersion, s.mode, s.target, s.tuple.profile.version, s.deadline))
	if err != nil {
		return empty, pinContextError(ctx, ErrParentModeChanged)
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return empty, ErrParentModeUnavailable
		}
		return empty, ErrParentModeUnknown
	}
	return record, nil
}

// BoundParentModeRequest carries the fixed challenge deadline through the
// existing password-attempt gate as well as bcrypt and the eventual write.
func (ds *DataStore) BoundParentModeRequest(r *http.Request, i *ParentModeIntent) (*http.Request, error) {
	if i == nil || i.state == nil || r == nil {
		return nil, ErrParentModeChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || r.Context() != s.retryCtx {
		return nil, ErrParentModeChanged
	}
	if err := s.run.Err(); err != nil {
		return nil, err
	}
	s.retryCtx = s.run
	s.bound = true
	return r.WithContext(s.run), nil
}
