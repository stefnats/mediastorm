package datastore

import (
	"bytes"
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

var (
	ErrParentListRemovalChanged     = errors.New("parent list removal confirmation expired or changed")
	ErrParentListRemovalUnavailable = errors.New("parent list removal confirmation unavailable")
	ErrParentListRemovalCredential  = errors.New("parent list removal credential confirmation failed")
	ErrParentListRemovalUnknown     = errors.New("parent list removal mutation outcome unknown")
)

type parentListRemovalState struct {
	mu                                                  sync.Mutex
	self                                                *ParentListRemovalIntent
	store                                               *DataStore
	original                                            models.Session
	tuple                                               profilePinAccessTuple
	hashes                                              []string
	listURL, method, path, rawPath, query, clientHeader string
	lists                                               []string
	deadline                                            time.Time
	life                                                context.Context
	stop                                                context.CancelFunc
	run                                                 context.Context
	retryCtx                                            context.Context
	bound                                               bool
	cancel                                              context.CancelFunc
	phase                                               parentMutationPhase
	bytes                                               int
	target                                              string
	compare                                             func([]byte, []byte) error
}

// ParentListRemovalIntent is an opaque one-shot operation, never a family/media grant.
// Its independent timer is the explicit, bounded handoff between two HTTPs;
// the completed first response's canceled context is not reused as live proof.
type ParentListRemovalIntent struct{ state *parentListRemovalState }

func (*ParentListRemovalIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentListRemovalIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentListRemovalIntent(opaque)"))
}
func (i *ParentListRemovalIntent) Close() {
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
func parentListRemovalValid(listURL string) bool {
	return listURL != "" && listURL == strings.TrimSpace(listURL)
}
func parentListRemovalRequestBoundary(r *http.Request) (string, string, string, error) {
	if r == nil || r.URL == nil || len(r.URL.EscapedPath())+len(r.URL.RawPath)+len(r.URL.RawQuery) > 8192 {
		return "", "", "", ErrParentListRemovalChanged
	}
	path, query, client, err := parentConfirmationRequestBoundary(r, http.MethodDelete)
	if err != nil {
		return "", "", "", ErrParentListRemovalChanged
	}
	return path, query, client, nil
}
func (ds *DataStore) PrepareParentListRemoval(r *http.Request, target, listURL string, tokens []string) (*ParentListRemovalIntent, error) {
	if ds == nil || ds.pool == nil || !parentMutationID(target) || !parentListRemovalValid(listURL) {
		return nil, ErrParentListRemovalChanged
	}
	if r == nil || r.URL == nil || listURL != strings.TrimSpace(r.URL.Query().Get("url")) {
		return nil, ErrParentListRemovalChanged
	}
	preparedDeadline := time.Now().Add(parentModeLifetime)
	prepCtx, prepCancel := context.WithDeadline(r.Context(), preparedDeadline)
	defer prepCancel()
	r = r.WithContext(prepCtx)
	path, query, client, err := parentListRemovalRequestBoundary(r)
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
		return nil, ErrParentListRemovalUnavailable
	}
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrParentListRemovalChanged
	}
	tuple := observation.state.tuple
	lists, err := parentListRemovalLists(tuple.profile.raw)
	if err != nil {
		return nil, err
	}
	var actor struct {
		Hash string `json:"password_hash"`
	}
	if !profilePinRequiredFields(tuple.actor.raw, []string{"password_hash"}, false) || json.Unmarshal(tuple.actor.raw, &actor) != nil {
		return nil, ErrParentListRemovalUnavailable
	}
	if _, err = bcrypt.Cost([]byte(actor.Hash)); err != nil {
		return nil, ErrParentListRemovalUnavailable
	}
	listBytes := 0
	for _, list := range lists {
		listBytes += len(list)
	}
	size := len(tuple.session.raw) + len(tuple.actor.raw) + len(tuple.profile.raw) + len(tuple.owner.raw) + len(tuple.grants) + len(path) + len(r.URL.RawPath) + len(query) + len(client) + len(listURL) + listBytes + len(hashes)*64 + 1024
	if size > parentModeSnapshotLimit {
		return nil, ErrParentListRemovalUnavailable
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
		return nil, ErrParentListRemovalChanged
	}
	life, stop := context.WithDeadline(context.Background(), deadline)
	s := &parentListRemovalState{store: ds, original: original, tuple: tuple, hashes: hashes, listURL: listURL, method: r.Method, path: path, rawPath: r.URL.RawPath, lists: lists, query: query, clientHeader: client, deadline: deadline, life: life, stop: stop, phase: parentPrepared, bytes: size, target: target, compare: bcrypt.CompareHashAndPassword}
	value := &ParentListRemovalIntent{state: s}
	s.self = value
	return value, nil
}

// ParentListRemovalBudget returns only server bookkeeping for the bounded service map.
func (ds *DataStore) ParentListRemovalBudget(i *ParentListRemovalIntent) (string, int, time.Time, error) {
	if i == nil || i.state == nil {
		return "", 0, time.Time{}, ErrParentListRemovalChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", 0, time.Time{}, ErrParentListRemovalChanged
	}
	return s.original.AccountID, s.bytes, s.deadline, nil
}

// BindParentListRemovalRetry consumes the original operation before further IO. A new
// HTTP context may shorten its original lifetime, never renew its snapshot.
func (ds *DataStore) BindParentListRemovalRetry(r *http.Request, i *ParentListRemovalIntent, target, listURL string, tokens []string) error {
	if i == nil || i.state == nil {
		return ErrParentListRemovalChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentPrepared {
		s.mu.Unlock()
		return ErrParentListRemovalChanged
	}
	s.phase = parentVerifying
	if s.life.Err() != nil {
		s.mu.Unlock()
		return ErrParentListRemovalChanged
	}
	path, query, client, err := parentListRemovalRequestBoundary(r)
	if err != nil || listURL != strings.TrimSpace(r.URL.Query().Get("url")) || target != s.target || listURL != s.listURL || r.Method != s.method || path != s.path || r.URL.RawPath != s.rawPath || query != s.query || client != s.clientHeader {
		s.mu.Unlock()
		return ErrParentListRemovalChanged
	}
	if err = r.Context().Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	original, ok := auth.GetSession(r)
	if !ok || original != s.original || auth.GetAccountID(r) != s.original.AccountID || auth.IsMaster(r) != s.original.IsMaster {
		s.mu.Unlock()
		return ErrParentListRemovalChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil || strings.Join(hashes, ",") != strings.Join(s.hashes, ",") {
		s.mu.Unlock()
		return ErrParentListRemovalChanged
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
		return ErrParentListRemovalChanged
	}
	if err = run.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != parentVerifying || s.life.Err() != nil {
		return ErrParentListRemovalChanged
	}
	s.phase = parentVerified // bound, but not yet credential-verified
	return nil
}

// VerifyParentListRemovalPassword repeats actual original-hash bcrypt outside SQL
// locks. The HTTP Guard independently owns the existing shared guess budget.
func (ds *DataStore) VerifyParentListRemovalPassword(r *http.Request, i *ParentListRemovalIntent) error {
	if i == nil || i.state == nil {
		return ErrParentListRemovalChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || !s.bound || r == nil || r.Context() != s.retryCtx {
		s.mu.Unlock()
		return ErrParentListRemovalChanged
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
			return ErrParentListRemovalCredential
		}
		password = string(raw)
	}
	password = strings.TrimSpace(password)
	if err != nil || password == "" || len(password) > 4096 {
		return ErrParentListRemovalCredential
	}
	compared := s.compare([]byte(actor.Hash), []byte(password))
	password = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.run.Err(); err != nil {
		return err
	}
	if s.life.Err() != nil || s.phase != parentVerifying {
		return ErrParentListRemovalChanged
	}
	if compared != nil {
		s.phase = parentRetired
		return ErrParentListRemovalCredential
	}
	s.phase = parentCommitting // credential verified; Commit consumes this state
	return nil
}
func (ds *DataStore) ParentListRemovalTarget(i *ParentListRemovalIntent, expected UserRevision) (string, error) {
	if i == nil || i.state == nil {
		return "", ErrParentListRemovalChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil || s.run.Err() != nil || s.life.Err() != nil {
		return "", ErrParentListRemovalChanged
	}
	if expected.owner != ds || expected.id != s.target || expected.version != s.tuple.profile.version {
		return "", ErrUserWriteConflict
	}
	return s.target, nil
}

// CommitParentListRemoval removes all exact URL matches, preserving order.
// An originally absent identity remains an idempotent confirmed write.
// Only kids_allowed_lists and updated_at change. Its final tuple
// includes current role/credentials/PIN, all row versions and both generations.
func (ds *DataStore) CommitParentListRemoval(i *ParentListRemovalIntent) (UserWriteRecord, error) {
	var empty UserWriteRecord
	if i == nil || i.state == nil {
		return empty, ErrParentListRemovalChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil {
		s.mu.Unlock()
		return empty, ErrParentListRemovalChanged
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
		return empty, ErrParentListRemovalChanged
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return empty, pinContextError(ctx, ErrParentListRemovalUnavailable)
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
			return empty, ErrParentListRemovalChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil {
		return empty, ErrParentListRemovalUnavailable
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(ctx, ErrParentListRemovalChanged)
		}
	}
	var found string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentListRemovalChanged)
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return empty, pinContextError(ctx, ErrParentListRemovalUnavailable)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.target).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentListRemovalChanged)
	}
	current, _, _, err := readProfilePinAccess(ctx, tx, s.original, s.target, s.hashes, true)
	if err != nil {
		return empty, err
	}
	if !sameParentModeTuple(current, s.tuple) {
		return empty, ErrParentListRemovalChanged
	}
	remaining := make([]string, 0, len(s.lists))
	for _, existing := range s.lists {
		if existing != s.listURL {
			remaining = append(remaining, existing)
		}
	}
	encoded, err := json.Marshal(remaining)
	if err != nil {
		return empty, ErrParentListRemovalUnavailable
	}
	record, err := ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_allowed_lists=$1::jsonb,updated_at=clock_timestamp() WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4 RETURNING `+userColumns+`,`+userWriteVersion, string(encoded), s.target, s.tuple.profile.version, s.deadline))
	if err != nil {
		return empty, pinContextError(ctx, ErrParentListRemovalChanged)
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return empty, ErrParentListRemovalUnavailable
		}
		return empty, ErrParentListRemovalUnknown
	}
	return record, nil
}

// BoundParentListRemovalRequest carries the fixed challenge deadline through the
// existing password-attempt gate as well as bcrypt and the eventual write.
func (ds *DataStore) BoundParentListRemovalRequest(r *http.Request, i *ParentListRemovalIntent) (*http.Request, error) {
	if i == nil || i.state == nil || r == nil {
		return nil, ErrParentListRemovalChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || r.Context() != s.retryCtx {
		return nil, ErrParentListRemovalChanged
	}
	if err := s.run.Err(); err != nil {
		return nil, err
	}
	s.retryCtx = s.run
	s.bound = true
	return r.WithContext(s.run), nil
}

func parentListRemovalLists(raw json.RawMessage) ([]string, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || fields["kids_allowed_lists"] == nil {
		return nil, ErrParentListRemovalUnavailable
	}
	value := bytes.TrimSpace(fields["kids_allowed_lists"])
	if bytes.Equal(value, []byte("null")) {
		return []string{}, nil
	}
	var rows []json.RawMessage
	if json.Unmarshal(value, &rows) != nil || rows == nil {
		return nil, ErrParentListRemovalUnavailable
	}
	lists := make([]string, 0, len(rows))
	for _, row := range rows {
		row = bytes.TrimSpace(row)
		var list string
		// Unmarshal(null, *string) succeeds, so require the JSON string type
		// before decoding. Whole-array null is the separate legacy case above.
		if len(row) == 0 || row[0] != '"' || json.Unmarshal(row, &list) != nil {
			return nil, ErrParentListRemovalUnavailable
		}
		lists = append(lists, list)
	}
	return lists, nil
}
