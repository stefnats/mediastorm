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
	ErrParentListWritePayload      = errors.New("invalid parental list body")
	ErrParentListWriteBodyTooLarge = errors.New("parental list body exceeds capture limit")
	ErrParentListWriteChanged      = errors.New("parent list write confirmation expired or changed")
	ErrParentListWriteUnavailable  = errors.New("parent list write confirmation unavailable")
	ErrParentListWriteCredential   = errors.New("parent list write credential confirmation failed")
	ErrParentListWriteUnknown      = errors.New("parent list write mutation outcome unknown")
)

type parentListWriteState struct {
	mu                                         sync.Mutex
	self                                       *ParentListWriteIntent
	store                                      *DataStore
	original                                   models.Session
	tuple                                      profilePinAccessTuple
	hashes                                     []string
	method, path, rawPath, query, clientHeader string
	kind                                       parentListWriteAction
	body                                       []byte
	desired                                    []string
	lists                                      []string
	deadline                                   time.Time
	life                                       context.Context
	stop                                       context.CancelFunc
	run                                        context.Context
	retryCtx                                   context.Context
	bound                                      bool
	cancel                                     context.CancelFunc
	phase                                      parentMutationPhase
	bytes                                      int
	target                                     string
	compare                                    func([]byte, []byte) error
}

// ParentListWriteIntent is an opaque one-shot operation, never a family/media grant.
// Its independent timer is the explicit, bounded handoff between two HTTPs;
// the completed first response's canceled context is not reused as live proof.
type ParentListWriteIntent struct{ state *parentListWriteState }

func (*ParentListWriteIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentListWriteIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentListWriteIntent(opaque)"))
}
func (i *ParentListWriteIntent) Close() {
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

// ParentListWriteBodyLimit bounds only the PG confirmation capture. The JSON
// legacy path retains its existing decoder and request behavior.
const ParentListWriteBodyLimit = 16 << 10

type parentListWriteAction uint8

const (
	parentListAdd parentListWriteAction = iota + 1
	parentListReplace
)

func parentListWriteRequestBoundary(r *http.Request, kind parentListWriteAction) (string, string, string, error) {
	method := http.MethodPost
	if kind == parentListReplace {
		method = http.MethodPut
	} else if kind != parentListAdd {
		return "", "", "", ErrParentListWriteChanged
	}
	if r == nil || r.URL == nil || len(r.URL.EscapedPath())+len(r.URL.RawPath)+len(r.URL.RawQuery) > 8192 {
		return "", "", "", ErrParentListWriteChanged
	}
	path, query, client, err := parentConfirmationRequestBoundary(r, method)
	if err != nil {
		return "", "", "", ErrParentListWriteChanged
	}
	return path, query, client, nil
}
func parentListWriteDesired(kind parentListWriteAction, raw []byte) ([]string, error) {
	if len(raw) > ParentListWriteBodyLimit {
		return nil, ErrParentListWriteBodyTooLarge
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	// Keep exactly the existing one-Decode semantics, including null, field
	// case folding, duplicate keys and ignored trailing input. Raw bytes are
	// independently frozen and compared on the retry.
	if kind == parentListAdd {
		var body struct {
			URL string `json:"url"`
		}
		if decoder.Decode(&body) != nil {
			return nil, ErrParentListWritePayload
		}
		list := strings.TrimSpace(body.URL)
		if list == "" {
			return nil, ErrParentListWritePayload
		}
		return []string{list}, nil
	}
	if kind != parentListReplace {
		return nil, ErrParentListWriteChanged
	}
	var body struct {
		Lists []string `json:"lists"`
	}
	if decoder.Decode(&body) != nil {
		return nil, ErrParentListWritePayload
	}
	cleaned := make([]string, 0, len(body.Lists))
	for _, list := range body.Lists {
		if value := strings.TrimSpace(list); value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned, nil
}
func (ds *DataStore) PrepareParentListAdd(r *http.Request, target string, body []byte, tokens []string) (*ParentListWriteIntent, error) {
	return ds.prepareParentListWrite(parentListAdd, r, target, body, tokens)
}
func (ds *DataStore) PrepareParentListReplace(r *http.Request, target string, body []byte, tokens []string) (*ParentListWriteIntent, error) {
	return ds.prepareParentListWrite(parentListReplace, r, target, body, tokens)
}
func (ds *DataStore) prepareParentListWrite(kind parentListWriteAction, r *http.Request, target string, body []byte, tokens []string) (*ParentListWriteIntent, error) {
	if ds == nil || ds.pool == nil || !parentMutationID(target) {
		return nil, ErrParentListWriteChanged
	}
	desired, err := parentListWriteDesired(kind, body)
	if err != nil {
		return nil, err
	}
	if r == nil || r.URL == nil {
		return nil, ErrParentListWriteChanged
	}
	preparedDeadline := time.Now().Add(parentModeLifetime)
	prepCtx, prepCancel := context.WithDeadline(r.Context(), preparedDeadline)
	defer prepCancel()
	r = r.WithContext(prepCtx)
	path, query, client, err := parentListWriteRequestBoundary(r, kind)
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
		return nil, ErrParentListWriteUnavailable
	}
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrParentListWriteChanged
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
		return nil, ErrParentListWriteUnavailable
	}
	if _, err = bcrypt.Cost([]byte(actor.Hash)); err != nil {
		return nil, ErrParentListWriteUnavailable
	}
	listBytes := 0
	for _, list := range lists {
		listBytes += len(list)
	}
	size := len(tuple.session.raw) + len(tuple.actor.raw) + len(tuple.profile.raw) + len(tuple.owner.raw) + len(tuple.grants) + len(path) + len(r.URL.RawPath) + len(query) + len(client) + len(body) + listBytes + len(lists)*16 + len(desired)*16 + len(hashes)*64 + 1024
	for _, list := range desired {
		size += len(list)
	}
	if size > parentModeSnapshotLimit {
		return nil, ErrParentListWriteUnavailable
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
		return nil, ErrParentListWriteChanged
	}
	life, stop := context.WithDeadline(context.Background(), deadline)
	s := &parentListWriteState{store: ds, original: original, tuple: tuple, hashes: hashes, kind: kind, body: bytes.Clone(body), desired: append([]string{}, desired...), method: r.Method, path: path, rawPath: r.URL.RawPath, lists: lists, query: query, clientHeader: client, deadline: deadline, life: life, stop: stop, phase: parentPrepared, bytes: size, target: target, compare: bcrypt.CompareHashAndPassword}
	value := &ParentListWriteIntent{state: s}
	s.self = value
	return value, nil
}

// ParentListWriteBudget returns only server bookkeeping for the bounded service map.
func (ds *DataStore) ParentListWriteBudget(i *ParentListWriteIntent) (string, int, time.Time, error) {
	if i == nil || i.state == nil {
		return "", 0, time.Time{}, ErrParentListWriteChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", 0, time.Time{}, ErrParentListWriteChanged
	}
	return s.original.AccountID, s.bytes, s.deadline, nil
}

// BindParentListWriteRetry consumes the original operation before further IO. A new
// HTTP context may shorten its original lifetime, never renew its snapshot.
func (ds *DataStore) BindParentListAddRetry(r *http.Request, i *ParentListWriteIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentListWriteRetry(parentListAdd, r, i, target, body, tokens)
}
func (ds *DataStore) BindParentListReplaceRetry(r *http.Request, i *ParentListWriteIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentListWriteRetry(parentListReplace, r, i, target, body, tokens)
}
func (ds *DataStore) bindParentListWriteRetry(kind parentListWriteAction, r *http.Request, i *ParentListWriteIntent, target string, body []byte, tokens []string) error {
	if i == nil || i.state == nil {
		return ErrParentListWriteChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentPrepared {
		s.mu.Unlock()
		return ErrParentListWriteChanged
	}
	s.phase = parentVerifying
	if s.life.Err() != nil {
		s.mu.Unlock()
		return ErrParentListWriteChanged
	}
	path, query, client, err := parentListWriteRequestBoundary(r, kind)
	if err != nil || len(body) > ParentListWriteBodyLimit || !bytes.Equal(body, s.body) || target != s.target || r.Method != s.method || path != s.path || r.URL.RawPath != s.rawPath || query != s.query || client != s.clientHeader {
		s.mu.Unlock()
		return ErrParentListWriteChanged
	}
	if err = r.Context().Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	original, ok := auth.GetSession(r)
	if !ok || original != s.original || auth.GetAccountID(r) != s.original.AccountID || auth.IsMaster(r) != s.original.IsMaster {
		s.mu.Unlock()
		return ErrParentListWriteChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil || strings.Join(hashes, ",") != strings.Join(s.hashes, ",") {
		s.mu.Unlock()
		return ErrParentListWriteChanged
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
		return ErrParentListWriteChanged
	}
	if err = run.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != parentVerifying || s.life.Err() != nil {
		return ErrParentListWriteChanged
	}
	s.phase = parentVerified // bound, but not yet credential-verified
	return nil
}

// VerifyParentListWritePassword repeats actual original-hash bcrypt outside SQL
// locks. The HTTP Guard independently owns the existing shared guess budget.
func (ds *DataStore) VerifyParentListWritePassword(r *http.Request, i *ParentListWriteIntent) error {
	if i == nil || i.state == nil {
		return ErrParentListWriteChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || !s.bound || r == nil || r.Context() != s.retryCtx {
		s.mu.Unlock()
		return ErrParentListWriteChanged
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
			return ErrParentListWriteCredential
		}
		password = string(raw)
	}
	password = strings.TrimSpace(password)
	if err != nil || password == "" || len(password) > 4096 {
		return ErrParentListWriteCredential
	}
	compared := s.compare([]byte(actor.Hash), []byte(password))
	password = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.run.Err(); err != nil {
		return err
	}
	if s.life.Err() != nil || s.phase != parentVerifying {
		return ErrParentListWriteChanged
	}
	if compared != nil {
		s.phase = parentRetired
		return ErrParentListWriteCredential
	}
	s.phase = parentCommitting // credential verified; Commit consumes this state
	return nil
}
func (ds *DataStore) ParentListAddTarget(ctx context.Context, i *ParentListWriteIntent, expected UserRevision) (string, error) {
	return ds.parentListWriteTarget(parentListAdd, ctx, i, expected)
}
func (ds *DataStore) ParentListReplaceTarget(ctx context.Context, i *ParentListWriteIntent, expected UserRevision) (string, error) {
	return ds.parentListWriteTarget(parentListReplace, ctx, i, expected)
}
func (ds *DataStore) parentListWriteTarget(kind parentListWriteAction, ctx context.Context, i *ParentListWriteIntent, expected UserRevision) (string, error) {
	if i == nil || i.state == nil {
		return "", ErrParentListWriteChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentCommitting || s.run == nil || ctx != s.run || s.run.Err() != nil || s.life.Err() != nil {
		return "", ErrParentListWriteChanged
	}
	if expected.owner != ds || expected.id != s.target || expected.version != s.tuple.profile.version {
		return "", ErrUserWriteConflict
	}
	return s.target, nil
}

// Commit receipts record a known transaction result, not current permission.
func (ds *DataStore) CommitParentListAdd(i *ParentListWriteIntent) (*ParentListWriteReceipt, error) {
	return ds.commitParentListWrite(parentListAdd, i)
}
func (ds *DataStore) CommitParentListReplace(i *ParentListWriteIntent) (*ParentListWriteReceipt, error) {
	return ds.commitParentListWrite(parentListReplace, i)
}
func (ds *DataStore) commitParentListWrite(kind parentListWriteAction, i *ParentListWriteIntent) (*ParentListWriteReceipt, error) {
	var empty *ParentListWriteReceipt
	if i == nil || i.state == nil {
		return empty, ErrParentListWriteChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentCommitting || s.run == nil {
		s.mu.Unlock()
		return empty, ErrParentListWriteChanged
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
		return empty, ErrParentListWriteChanged
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return empty, pinContextError(ctx, ErrParentListWriteUnavailable)
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
			return empty, ErrParentListWriteChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil {
		return empty, ErrParentListWriteUnavailable
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(ctx, ErrParentListWriteChanged)
		}
	}
	var found string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentListWriteChanged)
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return empty, pinContextError(ctx, ErrParentListWriteUnavailable)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.target).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentListWriteChanged)
	}
	current, _, _, err := readProfilePinAccess(ctx, tx, s.original, s.target, s.hashes, true)
	if err != nil {
		return empty, err
	}
	if !sameParentModeTuple(current, s.tuple) {
		return empty, ErrParentListWriteChanged
	}
	next := append([]string{}, s.desired...)
	noop := false
	if kind == parentListAdd {
		next = append([]string{}, s.lists...)
		for _, existing := range s.lists {
			if existing == s.desired[0] {
				noop = true
				break
			}
		}
		if !noop {
			next = append(next, s.desired[0])
		}
	}
	var record UserWriteRecord
	if noop {
		// This follows the same locked fresh tuple check as a write. A cache
		// duplicate is never enough, and there is deliberately no users UPDATE.
		record, err = ds.scanUserWrite(tx.QueryRow(ctx, `SELECT `+userColumns+`,`+userWriteVersion+` FROM users u WHERE id=$1 AND `+userWriteVersion+`=$2 AND clock_timestamp()<$3`, s.target, s.tuple.profile.version, s.deadline))
	} else {
		var encoded []byte
		encoded, err = json.Marshal(next)
		if err == nil {
			record, err = ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_allowed_lists=$1::jsonb,updated_at=clock_timestamp() WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4 RETURNING `+userColumns+`,`+userWriteVersion, string(encoded), s.target, s.tuple.profile.version, s.deadline))
		}
	}
	if err != nil {
		return empty, pinContextError(ctx, ErrParentListWriteChanged)
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return empty, ErrParentListWriteUnavailable
		}
		return empty, ErrParentListWriteUnknown
	}
	receipt := &parentListWriteReceiptState{store: ds, intent: i, kind: kind, before: UserRevision{owner: ds, id: s.target, version: s.tuple.profile.version}, record: record, run: ctx}
	value := &ParentListWriteReceipt{state: receipt}
	receipt.self = value
	return value, nil
}

// BoundParentListWriteRequest carries the fixed challenge deadline through the
// existing password-attempt gate as well as bcrypt and the eventual write.
func (ds *DataStore) BoundParentListWriteRequest(r *http.Request, i *ParentListWriteIntent) (*http.Request, error) {
	if i == nil || i.state == nil || r == nil {
		return nil, ErrParentListWriteChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || r.Context() != s.retryCtx {
		return nil, ErrParentListWriteChanged
	}
	if err := s.run.Err(); err != nil {
		return nil, err
	}
	s.retryCtx = s.run
	s.bound = true
	return r.WithContext(s.run), nil
}

type parentListWriteReceiptState struct {
	mu     sync.Mutex
	self   *ParentListWriteReceipt
	store  *DataStore
	intent *ParentListWriteIntent
	kind   parentListWriteAction
	before UserRevision
	record UserWriteRecord
	run    context.Context
	used   bool
}

// ParentListWriteReceipt is a historical, one-use Users acknowledgement only.
// It exposes neither a noop flag nor an authorization or cache-resync grant.
type ParentListWriteReceipt struct{ state *parentListWriteReceiptState }

func (*ParentListWriteReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentListWriteReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentListWriteReceipt(opaque)"))
}
func (ds *DataStore) AcknowledgeParentListAdd(ctx context.Context, i *ParentListWriteIntent, receipt *ParentListWriteReceipt, expected UserRevision) (UserWriteRecord, error) {
	return ds.acknowledgeParentListWrite(parentListAdd, ctx, i, receipt, expected)
}
func (ds *DataStore) AcknowledgeParentListReplace(ctx context.Context, i *ParentListWriteIntent, receipt *ParentListWriteReceipt, expected UserRevision) (UserWriteRecord, error) {
	return ds.acknowledgeParentListWrite(parentListReplace, ctx, i, receipt, expected)
}
func (ds *DataStore) acknowledgeParentListWrite(kind parentListWriteAction, ctx context.Context, i *ParentListWriteIntent, value *ParentListWriteReceipt, expected UserRevision) (UserWriteRecord, error) {
	var empty UserWriteRecord
	if i == nil || i.state == nil || value == nil || value.state == nil || ctx == nil {
		return empty, ErrParentListWriteChanged
	}
	s := value.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != value || s.store != ds || s.intent != i || s.kind != kind || s.used || s.before != expected || s.run != ctx || i.state.self != i || i.state.store != ds || i.state.kind != kind {
		return empty, ErrParentListWriteChanged
	}
	s.used = true
	// A known commit can finish just before cancellation. Acknowledge only
	// this exact canonical result for bookkeeping; the service independently
	// checks ctx before response publication. This never adopts a new state.
	return s.record, nil
}
