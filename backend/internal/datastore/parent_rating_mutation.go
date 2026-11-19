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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"novastream/internal/auth"
	"novastream/models"
)

var (
	ErrParentRatingPayload      = errors.New("invalid parental rating body")
	ErrParentRatingBodyTooLarge = errors.New("parental rating body exceeds capture limit")
	ErrParentRatingChanged      = errors.New("parent rating confirmation expired or changed")
	ErrParentRatingUnavailable  = errors.New("parent rating confirmation unavailable")
	ErrParentRatingCredential   = errors.New("parent rating credential confirmation failed")
	ErrParentRatingUnknown      = errors.New("parent rating mutation outcome unknown")
)

type parentRatingState struct {
	mu                                         sync.Mutex
	self                                       *ParentRatingIntent
	store                                      *DataStore
	original                                   models.Session
	tuple                                      profilePinAccessTuple
	hashes                                     []string
	method, path, rawPath, query, clientHeader string
	kind                                       parentRatingAction
	body                                       []byte
	desired                                    []string
	regionalState                              string
	fileBinding                                string
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

// ParentRatingIntent is an opaque one-shot operation, never a family/media grant.
// Its independent timer is the explicit, bounded handoff between two HTTPs;
// the completed first response's canceled context is not reused as live proof.
type ParentRatingIntent struct{ state *parentRatingState }

func (*ParentRatingIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentRatingIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentRatingIntent(opaque)"))
}
func (i *ParentRatingIntent) Close() {
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

// ParentRatingBodyLimit bounds only the PG confirmation and the new JSON pair capture. Existing JSON
// scalar paths retain its existing decoder and request behavior.
const ParentRatingBodyLimit = 16 << 10

type parentRatingAction uint8

const (
	parentRatingGeneric parentRatingAction = iota + 1
	parentRatingMovie
	parentRatingTV
	parentRatingPair
	parentRatingRegional
	parentRatingLocalFile
)

func parentRatingRequestBoundary(r *http.Request, kind parentRatingAction) (string, string, string, error) {
	if kind < parentRatingGeneric || kind > parentRatingLocalFile || r == nil || r.URL == nil || len(r.URL.EscapedPath())+len(r.URL.RawPath)+len(r.URL.RawQuery) > 8192 {
		return "", "", "", ErrParentRatingChanged
	}
	method := http.MethodPut
	if kind == parentRatingLocalFile {
		method = http.MethodPost
	}
	path, query, client, err := parentConfirmationRequestBoundary(r, method)
	if err != nil {
		return "", "", "", ErrParentRatingChanged
	}
	return path, query, client, nil
}
func parentRatingDesired(kind parentRatingAction, raw []byte) ([]string, error) {
	if len(raw) > ParentRatingBodyLimit {
		return nil, ErrParentRatingBodyTooLarge
	}
	if kind == parentRatingLocalFile {
		id, err := ParentLocalFileItem(raw)
		if err != nil {
			return nil, err
		}
		return []string{id}, nil
	}
	if kind == parentRatingRegional {
		policy, err := models.DecodeRegionalChildPolicy(raw)
		if err != nil {
			return nil, ErrParentRatingPayload
		}
		return []string{policy.Country, strconv.Itoa(policy.MaxMovieAge), strconv.Itoa(policy.MaxEpisodeAge)}, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if kind == parentRatingPair {
		var body struct {
			MovieRating *string `json:"movieRating"`
			TVRating    *string `json:"tvRating"`
		}
		if decoder.Decode(&body) != nil || body.MovieRating == nil || body.TVRating == nil {
			return nil, ErrParentRatingPayload
		}
		return []string{strings.TrimSpace(*body.MovieRating), strings.TrimSpace(*body.TVRating)}, nil
	}
	if kind < parentRatingGeneric || kind > parentRatingTV {
		return nil, ErrParentRatingChanged
	}
	var body struct {
		Rating string `json:"rating"`
	}
	if decoder.Decode(&body) != nil {
		return nil, ErrParentRatingPayload
	}
	// Exactly the original single Decode: String->null retains its previous
	// value, field case folding and trailing input remain compatible.
	return []string{strings.TrimSpace(body.Rating)}, nil
}

// ParentRatingPairValues is request-data decoding, never an authority proof.
// The new JSON route consumes the same final nonnull-pair semantics as PG.
func ParentRatingPairValues(raw []byte) (string, string, error) {
	values, err := parentRatingDesired(parentRatingPair, raw)
	if err != nil {
		return "", "", err
	}
	return values[0], values[1], nil
}
func (ds *DataStore) PrepareParentRatingGeneric(r *http.Request, target string, body []byte, tokens []string) (*ParentRatingIntent, error) {
	return ds.prepareParentRating(parentRatingGeneric, r, target, body, tokens)
}
func (ds *DataStore) PrepareParentRatingMovie(r *http.Request, target string, body []byte, tokens []string) (*ParentRatingIntent, error) {
	return ds.prepareParentRating(parentRatingMovie, r, target, body, tokens)
}
func (ds *DataStore) PrepareParentRatingTV(r *http.Request, target string, body []byte, tokens []string) (*ParentRatingIntent, error) {
	return ds.prepareParentRating(parentRatingTV, r, target, body, tokens)
}
func (ds *DataStore) PrepareParentRatingPair(r *http.Request, target string, body []byte, tokens []string) (*ParentRatingIntent, error) {
	return ds.prepareParentRating(parentRatingPair, r, target, body, tokens)
}
func (ds *DataStore) prepareParentRating(kind parentRatingAction, r *http.Request, target string, body []byte, tokens []string) (*ParentRatingIntent, error) {
	if ds == nil || ds.pool == nil || !parentMutationID(target) {
		return nil, ErrParentRatingChanged
	}
	desired, err := parentRatingDesired(kind, body)
	if err != nil {
		return nil, err
	}
	if r == nil || r.URL == nil {
		return nil, ErrParentRatingChanged
	}
	preparedDeadline := time.Now().Add(parentModeLifetime)
	prepCtx, prepCancel := context.WithDeadline(r.Context(), preparedDeadline)
	defer prepCancel()
	r = r.WithContext(prepCtx)
	path, query, client, err := parentRatingRequestBoundary(r, kind)
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
		return nil, ErrParentRatingUnavailable
	}
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrParentRatingChanged
	}
	tuple := observation.state.tuple
	var actor struct {
		Hash string `json:"password_hash"`
	}
	if !profilePinRequiredFields(tuple.actor.raw, []string{"password_hash"}, false) || json.Unmarshal(tuple.actor.raw, &actor) != nil {
		return nil, ErrParentRatingUnavailable
	}
	if _, err = bcrypt.Cost([]byte(actor.Hash)); err != nil {
		return nil, ErrParentRatingUnavailable
	}
	size := len(tuple.session.raw) + len(tuple.actor.raw) + len(tuple.profile.raw) + len(tuple.owner.raw) + len(tuple.grants) + len(path) + len(r.URL.RawPath) + len(query) + len(client) + len(body) + len(desired)*16 + len(hashes)*64 + 1024
	for _, list := range desired {
		size += len(list)
	}
	if size > parentModeSnapshotLimit {
		return nil, ErrParentRatingUnavailable
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
		return nil, ErrParentRatingChanged
	}
	life, stop := context.WithDeadline(context.Background(), deadline)
	s := &parentRatingState{store: ds, original: original, tuple: tuple, hashes: hashes, kind: kind, body: bytes.Clone(body), desired: append([]string{}, desired...), method: r.Method, path: path, rawPath: r.URL.RawPath, query: query, clientHeader: client, deadline: deadline, life: life, stop: stop, phase: parentPrepared, bytes: size, target: target, compare: bcrypt.CompareHashAndPassword}
	if kind == parentRatingRegional {
		s.regionalState, err = regionalPolicyState(prepCtx, ds.pool, target)
		s.bytes += len(s.regionalState)
		if err != nil || s.bytes > parentModeSnapshotLimit {
			s.stop()
			return nil, ErrParentRatingUnavailable
		}
	}
	value := &ParentRatingIntent{state: s}
	s.self = value
	return value, nil
}

// ParentRatingBudget returns only server bookkeeping for the bounded service map.
func (ds *DataStore) ParentRatingBudget(i *ParentRatingIntent) (string, int, time.Time, error) {
	if i == nil || i.state == nil {
		return "", 0, time.Time{}, ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", 0, time.Time{}, ErrParentRatingChanged
	}
	return s.original.AccountID, s.bytes, s.deadline, nil
}

// BindParentRatingRetry consumes the original operation before further IO. A new
// HTTP context may shorten its original lifetime, never renew its snapshot.
func (ds *DataStore) bindParentRatingRetry(kind parentRatingAction, r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	if i == nil || i.state == nil {
		return ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentPrepared {
		s.mu.Unlock()
		return ErrParentRatingChanged
	}
	s.phase = parentVerifying
	if s.life.Err() != nil {
		s.mu.Unlock()
		return ErrParentRatingChanged
	}
	path, query, client, err := parentRatingRequestBoundary(r, kind)
	if err != nil || len(body) > ParentRatingBodyLimit || !bytes.Equal(body, s.body) || target != s.target || r.Method != s.method || path != s.path || r.URL.RawPath != s.rawPath || query != s.query || client != s.clientHeader {
		s.mu.Unlock()
		return ErrParentRatingChanged
	}
	if err = r.Context().Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	original, ok := auth.GetSession(r)
	if !ok || original != s.original || auth.GetAccountID(r) != s.original.AccountID || auth.IsMaster(r) != s.original.IsMaster {
		s.mu.Unlock()
		return ErrParentRatingChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil || strings.Join(hashes, ",") != strings.Join(s.hashes, ",") {
		s.mu.Unlock()
		return ErrParentRatingChanged
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
		return ErrParentRatingChanged
	}
	if kind == parentRatingRegional {
		current, err := regionalPolicyState(run, ds.pool, target)
		if err != nil || current != s.regionalState {
			return ErrParentRatingChanged
		}
	}
	if err = run.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != parentVerifying || s.life.Err() != nil {
		return ErrParentRatingChanged
	}
	s.phase = parentVerified // bound, but not yet credential-verified
	return nil
}

// VerifyParentRatingPassword repeats actual original-hash bcrypt outside SQL
// locks. The HTTP Guard independently owns the existing shared guess budget.
func (ds *DataStore) VerifyParentRatingPassword(r *http.Request, i *ParentRatingIntent) error {
	if i == nil || i.state == nil {
		return ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || !s.bound || r == nil || r.Context() != s.retryCtx {
		s.mu.Unlock()
		return ErrParentRatingChanged
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
			return ErrParentRatingCredential
		}
		password = string(raw)
	}
	password = strings.TrimSpace(password)
	if err != nil || password == "" || len(password) > 4096 {
		return ErrParentRatingCredential
	}
	compared := s.compare([]byte(actor.Hash), []byte(password))
	password = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.run.Err(); err != nil {
		return err
	}
	if s.life.Err() != nil || s.phase != parentVerifying {
		return ErrParentRatingChanged
	}
	if compared != nil {
		s.phase = parentRetired
		return ErrParentRatingCredential
	}
	s.phase = parentCommitting // credential verified; Commit consumes this state
	return nil
}
func (ds *DataStore) parentRatingTarget(kind parentRatingAction, ctx context.Context, i *ParentRatingIntent, expected UserRevision) (string, error) {
	if i == nil || i.state == nil {
		return "", ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentCommitting || s.run == nil || ctx != s.run || s.run.Err() != nil || s.life.Err() != nil {
		return "", ErrParentRatingChanged
	}
	if expected.owner != ds || expected.id != s.target || expected.version != s.tuple.profile.version {
		return "", ErrUserWriteConflict
	}
	return s.target, nil
}

// Commit receipts record a known transaction result, not current permission.
func (ds *DataStore) commitParentRating(kind parentRatingAction, i *ParentRatingIntent) (*ParentRatingReceipt, error) {
	var empty *ParentRatingReceipt
	if i == nil || i.state == nil {
		return empty, ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentCommitting || s.run == nil {
		s.mu.Unlock()
		return empty, ErrParentRatingChanged
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
		return empty, ErrParentRatingChanged
	}
	tx, err := ds.pool.Begin(ctx)
	if err != nil {
		return empty, pinContextError(ctx, ErrParentRatingUnavailable)
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
			return empty, ErrParentRatingChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil {
		return empty, ErrParentRatingUnavailable
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(ctx, ErrParentRatingChanged)
		}
	}
	var found string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentRatingChanged)
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return empty, pinContextError(ctx, ErrParentRatingUnavailable)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.target).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentRatingChanged)
	}
	current, _, _, err := readProfilePinAccess(ctx, tx, s.original, s.target, s.hashes, true)
	if err != nil {
		return empty, err
	}
	if !sameParentModeTuple(current, s.tuple) {
		return empty, ErrParentRatingChanged
	}
	var regional *models.RegionalChildPolicy
	if kind == parentRatingRegional {
		if _, err = tx.Exec(ctx, `SELECT user_id FROM regional_profile_policies WHERE user_id=$1 FOR UPDATE`, s.target); err != nil {
			return empty, ErrParentRatingUnavailable
		}
		current, readErr := regionalPolicyState(ctx, tx, s.target)
		if readErr != nil || current != s.regionalState {
			return empty, ErrParentRatingChanged
		}
		movie, _ := strconv.Atoi(s.desired[1])
		episode, _ := strconv.Atoi(s.desired[2])
		value := models.RegionalChildPolicy{Version: 1, Country: s.desired[0], MaxMovieAge: movie, MaxEpisodeAge: episode}
		if !value.Valid() {
			return empty, ErrParentRatingPayload
		}
		regional = &value
		if _, err = tx.Exec(ctx, `INSERT INTO regional_profile_policies(user_id,policy_version,country,max_movie_age,max_episode_age,revision,updated_at,updated_by) VALUES($1,1,$2,$3,$4,1,clock_timestamp(),$5) ON CONFLICT(user_id) DO UPDATE SET policy_version=EXCLUDED.policy_version,country=EXCLUDED.country,max_movie_age=EXCLUDED.max_movie_age,max_episode_age=EXCLUDED.max_episode_age,updated_at=EXCLUDED.updated_at,updated_by=EXCLUDED.updated_by`, s.target, value.Country, value.MaxMovieAge, value.MaxEpisodeAge, s.original.AccountID); err != nil {
			return empty, ErrParentRatingUnavailable
		}
	}
	var record UserWriteRecord
	switch kind {
	case parentRatingRegional:
		record, err = ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET updated_at=clock_timestamp() WHERE id=$1 AND `+userWriteVersion+`=$2 AND clock_timestamp()<$3 RETURNING `+userColumns+`,`+userWriteVersion, s.target, s.tuple.profile.version, s.deadline))
	case parentRatingGeneric:
		record, err = ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_max_rating=$1,updated_at=clock_timestamp() WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4 RETURNING `+userColumns+`,`+userWriteVersion, s.desired[0], s.target, s.tuple.profile.version, s.deadline))
	case parentRatingMovie:
		record, err = ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_max_movie_rating=$1,updated_at=clock_timestamp() WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4 RETURNING `+userColumns+`,`+userWriteVersion, s.desired[0], s.target, s.tuple.profile.version, s.deadline))
	case parentRatingTV:
		record, err = ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_max_tv_rating=$1,updated_at=clock_timestamp() WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4 RETURNING `+userColumns+`,`+userWriteVersion, s.desired[0], s.target, s.tuple.profile.version, s.deadline))
	case parentRatingPair:
		record, err = ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET kids_max_movie_rating=$1,kids_max_tv_rating=$2,updated_at=clock_timestamp() WHERE id=$3 AND `+userWriteVersion+`=$4 AND clock_timestamp()<$5 RETURNING `+userColumns+`,`+userWriteVersion, s.desired[0], s.desired[1], s.target, s.tuple.profile.version, s.deadline))
	default:
		return empty, ErrParentRatingChanged
	}
	if err != nil {
		return empty, pinContextError(ctx, ErrParentRatingChanged)
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	if err = tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return empty, ErrParentRatingUnavailable
		}
		return empty, ErrParentRatingUnknown
	}
	receipt := &parentRatingReceiptState{store: ds, intent: i, kind: kind, before: UserRevision{owner: ds, id: s.target, version: s.tuple.profile.version}, record: record, run: ctx, regional: regional}
	value := &ParentRatingReceipt{state: receipt}
	receipt.self = value
	return value, nil
}

// BoundParentRatingRequest carries the fixed challenge deadline through the
// existing password-attempt gate as well as bcrypt and the eventual write.
func (ds *DataStore) BoundParentRatingRequest(r *http.Request, i *ParentRatingIntent) (*http.Request, error) {
	if i == nil || i.state == nil || r == nil {
		return nil, ErrParentRatingChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || r.Context() != s.retryCtx {
		return nil, ErrParentRatingChanged
	}
	if err := s.run.Err(); err != nil {
		return nil, err
	}
	s.retryCtx = s.run
	s.bound = true
	return r.WithContext(s.run), nil
}

type parentRatingReceiptState struct {
	mu       sync.Mutex
	self     *ParentRatingReceipt
	store    *DataStore
	intent   *ParentRatingIntent
	kind     parentRatingAction
	before   UserRevision
	record   UserWriteRecord
	regional *models.RegionalChildPolicy
	run      context.Context
	used     bool
}

// ParentRatingReceipt is a historical, one-use Users acknowledgement only.
// It exposes neither an authorization flag nor a cache-resync grant.
type ParentRatingReceipt struct{ state *parentRatingReceiptState }

func (*ParentRatingReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentRatingReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentRatingReceipt(opaque)"))
}
func (ds *DataStore) acknowledgeParentRating(kind parentRatingAction, ctx context.Context, i *ParentRatingIntent, value *ParentRatingReceipt, expected UserRevision) (UserWriteRecord, error) {
	var empty UserWriteRecord
	if i == nil || i.state == nil || value == nil || value.state == nil || ctx == nil {
		return empty, ErrParentRatingChanged
	}
	s := value.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != value || s.store != ds || s.intent != i || s.kind != kind || s.used || s.before != expected || s.run != ctx || i.state.self != i || i.state.store != ds || i.state.kind != kind {
		return empty, ErrParentRatingChanged
	}
	s.used = true
	// A known commit can finish just before cancellation. Acknowledge only
	// this exact canonical result for bookkeeping; the service independently
	// checks ctx before response publication. This never adopts a new state.
	return s.record, nil
}

func (ds *DataStore) BindParentRatingGenericRetry(r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentRatingRetry(parentRatingGeneric, r, i, target, body, tokens)
}
func (ds *DataStore) ParentRatingGenericTarget(ctx context.Context, i *ParentRatingIntent, expected UserRevision) (string, error) {
	return ds.parentRatingTarget(parentRatingGeneric, ctx, i, expected)
}
func (ds *DataStore) CommitParentRatingGeneric(i *ParentRatingIntent) (*ParentRatingReceipt, error) {
	return ds.commitParentRating(parentRatingGeneric, i)
}
func (ds *DataStore) AcknowledgeParentRatingGeneric(ctx context.Context, i *ParentRatingIntent, receipt *ParentRatingReceipt, expected UserRevision) (UserWriteRecord, error) {
	return ds.acknowledgeParentRating(parentRatingGeneric, ctx, i, receipt, expected)
}

func (ds *DataStore) BindParentRatingMovieRetry(r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentRatingRetry(parentRatingMovie, r, i, target, body, tokens)
}
func (ds *DataStore) ParentRatingMovieTarget(ctx context.Context, i *ParentRatingIntent, expected UserRevision) (string, error) {
	return ds.parentRatingTarget(parentRatingMovie, ctx, i, expected)
}
func (ds *DataStore) CommitParentRatingMovie(i *ParentRatingIntent) (*ParentRatingReceipt, error) {
	return ds.commitParentRating(parentRatingMovie, i)
}
func (ds *DataStore) AcknowledgeParentRatingMovie(ctx context.Context, i *ParentRatingIntent, receipt *ParentRatingReceipt, expected UserRevision) (UserWriteRecord, error) {
	return ds.acknowledgeParentRating(parentRatingMovie, ctx, i, receipt, expected)
}

func (ds *DataStore) BindParentRatingTVRetry(r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentRatingRetry(parentRatingTV, r, i, target, body, tokens)
}
func (ds *DataStore) ParentRatingTVTarget(ctx context.Context, i *ParentRatingIntent, expected UserRevision) (string, error) {
	return ds.parentRatingTarget(parentRatingTV, ctx, i, expected)
}
func (ds *DataStore) CommitParentRatingTV(i *ParentRatingIntent) (*ParentRatingReceipt, error) {
	return ds.commitParentRating(parentRatingTV, i)
}
func (ds *DataStore) AcknowledgeParentRatingTV(ctx context.Context, i *ParentRatingIntent, receipt *ParentRatingReceipt, expected UserRevision) (UserWriteRecord, error) {
	return ds.acknowledgeParentRating(parentRatingTV, ctx, i, receipt, expected)
}

func (ds *DataStore) BindParentRatingPairRetry(r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentRatingRetry(parentRatingPair, r, i, target, body, tokens)
}
func (ds *DataStore) ParentRatingPairTarget(ctx context.Context, i *ParentRatingIntent, expected UserRevision) (string, error) {
	return ds.parentRatingTarget(parentRatingPair, ctx, i, expected)
}
func (ds *DataStore) CommitParentRatingPair(i *ParentRatingIntent) (*ParentRatingReceipt, error) {
	return ds.commitParentRating(parentRatingPair, i)
}
func (ds *DataStore) AcknowledgeParentRatingPair(ctx context.Context, i *ParentRatingIntent, receipt *ParentRatingReceipt, expected UserRevision) (UserWriteRecord, error) {
	return ds.acknowledgeParentRating(parentRatingPair, ctx, i, receipt, expected)
}

func (ds *DataStore) PrepareParentRegionalPolicy(r *http.Request, target string, body []byte, tokens []string) (*ParentRatingIntent, error) {
	return ds.prepareParentRating(parentRatingRegional, r, target, body, tokens)
}
func (ds *DataStore) BindParentRegionalPolicyRetry(r *http.Request, i *ParentRatingIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentRatingRetry(parentRatingRegional, r, i, target, body, tokens)
}
func (ds *DataStore) ParentRegionalPolicyTarget(ctx context.Context, i *ParentRatingIntent, expected UserRevision) (string, error) {
	return ds.parentRatingTarget(parentRatingRegional, ctx, i, expected)
}
func (ds *DataStore) CommitParentRegionalPolicy(i *ParentRatingIntent) (*ParentRatingReceipt, error) {
	return ds.commitParentRating(parentRatingRegional, i)
}
func (ds *DataStore) AcknowledgeParentRegionalPolicy(ctx context.Context, i *ParentRatingIntent, value *ParentRatingReceipt, expected UserRevision) (UserWriteRecord, models.RegionalChildPolicy, error) {
	record, err := ds.acknowledgeParentRating(parentRatingRegional, ctx, i, value, expected)
	if err != nil {
		return UserWriteRecord{}, models.RegionalChildPolicy{}, err
	}
	if value.state.regional == nil {
		return UserWriteRecord{}, models.RegionalChildPolicy{}, ErrParentRatingChanged
	}
	return record, *value.state.regional, nil
}
