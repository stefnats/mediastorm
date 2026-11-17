package datastore

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/bcrypt"
	"novastream/internal/auth"
	"novastream/models"
)

var (
	ErrParentKidsFlagChanged      = errors.New("parent profile confirmation expired or changed")
	ErrParentKidsFlagUnavailable  = errors.New("parent profile confirmation unavailable")
	ErrParentKidsFlagCredential   = errors.New("parent profile credential confirmation failed")
	ErrParentKidsFlagUnknown      = errors.New("parent profile mutation outcome unknown")
	ErrParentKidsFlagPayload      = errors.New("invalid profile flag request")
	ErrParentKidsFlagBodyTooLarge = errors.New("profile flag request exceeds sixteen kibibytes")
)

const ParentKidsFlagBodyLimit = 16 << 10

type parentKidsFlagEntry uint8

const (
	parentKidsFlagAPI parentKidsFlagEntry = iota + 1
	parentKidsFlagManagement
)

type parentKidsFlagState struct {
	mu                                                 sync.Mutex
	self                                               *ParentKidsFlagIntent
	store                                              *DataStore
	original                                           models.Session
	tuple                                              profilePinAccessTuple
	settings                                           UserSettingsRecord
	hashes                                             []string
	kind                                               parentKidsFlagEntry
	body                                               []byte
	enabled                                            bool
	method, path, rawPath, query, clientHeader, target string
	deadline                                           time.Time
	life                                               context.Context
	stop                                               context.CancelFunc
	run, retryCtx                                      context.Context
	bound                                              bool
	cancel                                             context.CancelFunc
	phase                                              parentMutationPhase
	bytes                                              int
	compare                                            func([]byte, []byte) error
	usersLease, settingsLease                          *ParentKidsFlagCacheLease
	committed, taken                                   bool
}

// ParentKidsFlagIntent is the complete public flag operation. The older
// ParentKidsIntent remains a distinct uninstalled flag-only primitive.
type ParentKidsFlagIntent struct{ state *parentKidsFlagState }

func (*ParentKidsFlagIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentKidsFlagIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsFlagIntent(opaque)"))
}
func (i *ParentKidsFlagIntent) Close() {
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

func parentKidsFlagDesired(kind parentKidsFlagEntry, raw []byte) (bool, error) {
	if len(raw) > ParentKidsFlagBodyLimit {
		return false, ErrParentKidsFlagBodyTooLarge
	}
	if kind != parentKidsFlagAPI && kind != parentKidsFlagManagement {
		return false, ErrParentKidsFlagChanged
	}
	var desired struct {
		IsKidsProfile bool `json:"isKidsProfile"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if kind == parentKidsFlagAPI {
		dec.DisallowUnknownFields()
	}
	// Preserve exactly one existing Decode, including null, duplicate keys and
	// trailing bytes. The cookie and API unknown-field policies remain distinct.
	if err := dec.Decode(&desired); err != nil {
		return false, ErrParentKidsFlagPayload
	}
	return desired.IsKidsProfile, nil
}
func (ds *DataStore) PrepareParentKidsFlagAPI(r *http.Request, target string, body []byte, tokens []string) (*ParentKidsFlagIntent, error) {
	return ds.prepareParentKidsFlag(parentKidsFlagAPI, r, target, body, tokens)
}
func (ds *DataStore) PrepareParentKidsFlagManagement(r *http.Request, target string, body []byte, tokens []string) (*ParentKidsFlagIntent, error) {
	return ds.prepareParentKidsFlag(parentKidsFlagManagement, r, target, body, tokens)
}
func (ds *DataStore) prepareParentKidsFlag(kind parentKidsFlagEntry, r *http.Request, target string, body []byte, tokens []string) (*ParentKidsFlagIntent, error) {
	if ds == nil || ds.pool == nil || r == nil || !parentMutationID(target) {
		return nil, ErrParentKidsFlagUnavailable
	}
	enabled, err := parentKidsFlagDesired(kind, body)
	if err != nil {
		return nil, err
	}
	path, query, client, err := parentConfirmationRequestBoundary(r, http.MethodPut)
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(parentModeLifetime)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	if err = ds.CheckProfilePinRequestFrame(r.WithContext(ctx)); err != nil {
		return nil, err
	}
	original, ok := auth.GetSession(r)
	if !ok || original.Scope != "" || original.ScopeResource != "" {
		return nil, ErrParentKidsFlagChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil {
		return nil, err
	}
	tx, err := ds.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, pinContextError(ctx, ErrParentKidsFlagUnavailable)
	}
	defer rollbackPinTx(tx, ctx)
	tuple, outcome, expires, err := readProfilePinAccess(ctx, tx, original, target, hashes, true)
	if err != nil {
		return nil, err
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	if frame == nil || frame.self != frame || frame.store != ds || frame.ctx.Err() != nil || !sameParentMutationRow(frame.sessionRow, tuple.session) || !sameParentMutationRow(frame.actorRow, tuple.actor) {
		return nil, ErrProfilePinAccessChanged
	}
	if outcome == ProfilePinNotFound {
		return nil, ErrProfilePinAccessNotFound
	}
	if outcome == ProfilePinUnavailable {
		return nil, ErrParentKidsFlagUnavailable
	}
	var settings UserSettingsRecord
	if kind == parentKidsFlagManagement && enabled {
		settings, err = ds.scanSettings(tx.QueryRow(ctx, userSettingsSelect+` WHERE u.id=$1`, target))
		if err != nil {
			return nil, err
		}
	}
	var actor struct {
		Hash string `json:"password_hash"`
	}
	if !profilePinRequiredFields(tuple.actor.raw, []string{"password_hash"}, false) || json.Unmarshal(tuple.actor.raw, &actor) != nil {
		return nil, ErrParentKidsFlagUnavailable
	}
	if _, err = bcrypt.Cost([]byte(actor.Hash)); err != nil {
		return nil, ErrParentKidsFlagUnavailable
	}
	deadline = parentMutationMin(deadline, &expires)
	if at, ok := r.Context().Deadline(); ok {
		deadline = parentMutationMin(deadline, &at)
	}
	size := len(tuple.session.raw) + len(tuple.actor.raw) + len(tuple.owner.raw) + len(tuple.profile.raw) + len(tuple.grants) + len(path) + len(r.URL.RawPath) + len(query) + len(client) + len(body) + len(hashes)*64 + 1024
	if settings.Revision.snapshot != nil {
		raw := settings.Revision.snapshot.raw
		// Count both retained typed projections, including slice entry descriptors,
		// raw bytes, decoded strings and maps. This is a logical payload budget,
		// never an exact heap or global-RSS measurement.
		size += len(raw) + 2*parentKidsFlagPayload(reflect.ValueOf(settings.Revision.snapshot.settings)) + 1024
	}
	if size > parentModeSnapshotLimit {
		return nil, ErrParentKidsFlagUnavailable
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if !deadline.After(time.Now()) {
		return nil, ErrParentKidsFlagChanged
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, pinContextError(ctx, ErrParentKidsFlagUnavailable)
	}
	life, stop := context.WithDeadline(context.Background(), deadline)
	state := &parentKidsFlagState{store: ds, original: original, tuple: tuple, settings: settings, hashes: hashes, kind: kind, body: bytes.Clone(body), enabled: enabled, method: r.Method, path: path, rawPath: r.URL.RawPath, query: query, clientHeader: client, target: target, deadline: deadline, life: life, stop: stop, phase: parentPrepared, bytes: size, compare: bcrypt.CompareHashAndPassword}
	value := &ParentKidsFlagIntent{state: state}
	state.self = value
	return value, nil
}
func (ds *DataStore) ParentKidsFlagBudget(i *ParentKidsFlagIntent) (string, int, time.Time, error) {
	if i == nil || i.state == nil {
		return "", 0, time.Time{}, ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentPrepared || s.life.Err() != nil {
		return "", 0, time.Time{}, ErrParentKidsFlagChanged
	}
	return s.original.AccountID, s.bytes, s.deadline, nil
}
func sameKidsFlagSettings(a, b UserSettingsRevision) bool {
	x, y := a.snapshot, b.snapshot
	return x != nil && y != nil && x.ds == y.ds && x.id == y.id && x.userPresent == y.userPresent && x.version == y.version && x.generation == y.generation && bytes.Equal(x.raw, y.raw)
}
func (ds *DataStore) BindParentKidsFlagAPIRetry(r *http.Request, i *ParentKidsFlagIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentKidsFlagRetry(parentKidsFlagAPI, r, i, target, body, tokens)
}
func (ds *DataStore) BindParentKidsFlagManagementRetry(r *http.Request, i *ParentKidsFlagIntent, target string, body []byte, tokens []string) error {
	return ds.bindParentKidsFlagRetry(parentKidsFlagManagement, r, i, target, body, tokens)
}
func (ds *DataStore) bindParentKidsFlagRetry(kind parentKidsFlagEntry, r *http.Request, i *ParentKidsFlagIntent, target string, body []byte, tokens []string) error {
	if i == nil || i.state == nil {
		return ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.kind != kind || s.phase != parentPrepared {
		s.mu.Unlock()
		return ErrParentKidsFlagChanged
	}
	s.phase = parentVerifying
	if s.life.Err() != nil {
		s.mu.Unlock()
		return ErrParentKidsFlagChanged
	}
	path, query, client, err := parentConfirmationRequestBoundary(r, http.MethodPut)
	if err != nil || len(body) > ParentKidsFlagBodyLimit || !bytes.Equal(body, s.body) || target != s.target || r.Method != s.method || path != s.path || r.URL.RawPath != s.rawPath || query != s.query || client != s.clientHeader {
		s.mu.Unlock()
		return ErrParentKidsFlagChanged
	}
	if err = r.Context().Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	original, ok := auth.GetSession(r)
	if !ok || original != s.original || auth.GetAccountID(r) != s.original.AccountID || auth.IsMaster(r) != s.original.IsMaster {
		s.mu.Unlock()
		return ErrParentKidsFlagChanged
	}
	hashes, err := parentModeHashes(tokens)
	if err != nil || strings.Join(hashes, ",") != strings.Join(s.hashes, ",") {
		s.mu.Unlock()
		return ErrParentKidsFlagChanged
	}
	run, cancel := context.WithDeadline(r.Context(), s.deadline)
	s.run = run
	s.retryCtx = r.Context()
	s.cancel = cancel
	s.mu.Unlock()
	if err = ds.CheckProfilePinRequestFrame(r.WithContext(run)); err != nil {
		return err
	}
	tx, err := ds.pool.BeginTx(run, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return pinContextError(run, ErrParentKidsFlagUnavailable)
	}
	defer rollbackPinTx(tx, run)
	tuple, _, _, err := readProfilePinAccess(run, tx, s.original, s.target, s.hashes, true)
	if err != nil {
		return err
	}
	if !sameParentModeTuple(tuple, s.tuple) {
		return ErrParentKidsFlagChanged
	}
	if s.kind == parentKidsFlagManagement && s.enabled {
		current, e := ds.scanSettings(tx.QueryRow(run, userSettingsSelect+` WHERE u.id=$1`, s.target))
		if e != nil {
			return e
		}
		if !sameKidsFlagSettings(current.Revision, s.settings.Revision) {
			return ErrParentKidsFlagChanged
		}
	}
	if err = tx.Commit(run); err != nil {
		return pinContextError(run, ErrParentKidsFlagUnavailable)
	}
	if err = run.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.phase != parentVerifying || s.life.Err() != nil {
		return ErrParentKidsFlagChanged
	}
	s.phase = parentVerified // bound, but not yet credential-verified
	return nil
}

// VerifyParentKidsFlagPassword repeats actual original-hash bcrypt outside SQL
// locks. The HTTP Guard independently owns the existing shared guess budget.
func (ds *DataStore) VerifyParentKidsFlagPassword(r *http.Request, i *ParentKidsFlagIntent) error {
	if i == nil || i.state == nil {
		return ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || !s.bound || r == nil || r.Context() != s.retryCtx {
		s.mu.Unlock()
		return ErrParentKidsFlagChanged
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
			return ErrParentKidsFlagCredential
		}
		password = string(raw)
	}
	password = strings.TrimSpace(password)
	if err != nil || password == "" || len(password) > 4096 {
		return ErrParentKidsFlagCredential
	}
	compared := s.compare([]byte(actor.Hash), []byte(password))
	password = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.run.Err(); err != nil {
		return err
	}
	if s.life.Err() != nil || s.phase != parentVerifying {
		return ErrParentKidsFlagChanged
	}
	if compared != nil {
		s.phase = parentRetired
		return ErrParentKidsFlagCredential
	}
	s.phase = parentCommitting // credential verified; Commit consumes this state
	return nil
}

// BoundParentKidsFlagRequest carries the fixed challenge deadline through the
// existing password-attempt gate as well as bcrypt and the eventual write.
func (ds *DataStore) BoundParentKidsFlagRequest(r *http.Request, i *ParentKidsFlagIntent) (*http.Request, error) {
	if i == nil || i.state == nil || r == nil {
		return nil, ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentVerified || s.run == nil || r.Context() != s.retryCtx {
		return nil, ErrParentKidsFlagChanged
	}
	if err := s.run.Err(); err != nil {
		return nil, err
	}
	s.retryCtx = s.run
	s.bound = true
	return r.WithContext(s.run), nil
}

// ParentKidsFlagCacheLease reserves one actual service projection. It is not
// an authorization input or a replacement for the service's held cache lock.
type ParentKidsFlagCacheLease struct {
	self           *ParentKidsFlagCacheLease
	intent         *ParentKidsFlagIntent
	settings       bool
	closed, staged bool
	before         UserRevision
	settingsBefore UserSettingsRevision
}

func (*ParentKidsFlagCacheLease) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentKidsFlagCacheLease) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsFlagCacheLease(opaque)"))
}
func (l *ParentKidsFlagCacheLease) Retire() bool {
	if l == nil || l.self != l || l.intent == nil || l.intent.state == nil {
		return false
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	l.closed = true
	return s.taken
}
func (l *ParentKidsFlagCacheLease) Taken() bool {
	if l == nil || l.self != l || l.intent == nil || l.intent.state == nil {
		return false
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.taken
}
func (ds *DataStore) BeginParentKidsFlagUsersLease(ctx context.Context, i *ParentKidsFlagIntent, expected UserRevision) (*ParentKidsFlagCacheLease, error) {
	if i == nil || i.state == nil {
		return nil, ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || ctx == nil || s.run != ctx || ctx.Err() != nil || s.life.Err() != nil || s.usersLease != nil {
		return nil, ErrParentKidsFlagChanged
	}
	if expected.owner != ds || expected.id != s.target || expected.version != s.tuple.profile.version {
		return nil, ErrUserWriteConflict
	}
	l := &ParentKidsFlagCacheLease{intent: i, before: expected}
	l.self = l
	s.usersLease = l
	return l, nil
}
func (ds *DataStore) BeginParentKidsFlagSettingsLease(ctx context.Context, i *ParentKidsFlagIntent, users *ParentKidsFlagCacheLease, expected UserSettingsRevision) (*ParentKidsFlagCacheLease, error) {
	if i == nil || i.state == nil {
		return nil, ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || (s.kind != parentKidsFlagManagement || !s.enabled) || s.phase != parentCommitting || ctx == nil || s.run != ctx || ctx.Err() != nil || s.life.Err() != nil || s.settingsLease != nil || users == nil || users.self != users || users != s.usersLease || users.closed {
		return nil, ErrParentKidsFlagChanged
	}
	if !sameKidsFlagSettings(expected, s.settings.Revision) {
		return nil, ErrUserSettingsConflict
	}
	l := &ParentKidsFlagCacheLease{intent: i, settings: true, settingsBefore: expected}
	l.self = l
	s.settingsLease = l
	return l, nil
}

// ParentKidsFlagReceipt is sealed historical bookkeeping for both cache ACKs.
// It never grants a new write, unlock or current playback permission.
type ParentKidsFlagReceipt struct {
	self     *ParentKidsFlagReceipt
	store    *DataStore
	intent   *ParentKidsFlagIntent
	user     UserWriteRecord
	settings UserSettingsRecord
	used     bool
}

func (*ParentKidsFlagReceipt) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ParentKidsFlagReceipt) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ParentKidsFlagReceipt(opaque)"))
}
func (ds *DataStore) StageParentKidsFlagUsers(l *ParentKidsFlagCacheLease, r *ParentKidsFlagReceipt) (UserWriteRecord, error) {
	if l == nil || l.self != l || l.intent == nil || l.intent.state == nil {
		return UserWriteRecord{}, ErrParentKidsFlagChanged
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if r == nil || r.self != r || r.store != ds || r.intent != l.intent || s.store != ds || !s.committed || r.used || l != s.usersLease || l.closed || l.staged {
		return UserWriteRecord{}, ErrParentKidsFlagChanged
	}
	l.staged = true
	return r.user, nil
}
func (ds *DataStore) StageParentKidsFlagSettings(l *ParentKidsFlagCacheLease, r *ParentKidsFlagReceipt) (UserSettingsRecord, error) {
	if l == nil || l.self != l || l.intent == nil || l.intent.state == nil {
		return UserSettingsRecord{}, ErrParentKidsFlagChanged
	}
	s := l.intent.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if r == nil || r.self != r || r.store != ds || r.intent != l.intent || s.store != ds || !s.committed || r.used || l != s.settingsLease || l.closed || l.staged {
		return UserSettingsRecord{}, ErrParentKidsFlagChanged
	}
	l.staged = true
	return r.settings, nil
}

// TakeParentKidsFlagReceipt is the single local consumption point. All fallible
// service copies happen first; both held cache owners complete afterwards even
// when the original request has since canceled. No SQL or new authority here.
func (ds *DataStore) TakeParentKidsFlagReceipt(i *ParentKidsFlagIntent, r *ParentKidsFlagReceipt, users, settings *ParentKidsFlagCacheLease) error {
	if i == nil || i.state == nil {
		return ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || r == nil || r.self != r || r.store != ds || r.intent != i || r.used || !s.committed || s.taken || users == nil || users.self != users || users != s.usersLease || users.closed || !users.staged {
		return ErrParentKidsFlagChanged
	}
	if s.kind == parentKidsFlagManagement && s.enabled {
		if settings == nil || settings.self != settings || settings != s.settingsLease || settings.closed || !settings.staged {
			return ErrParentKidsFlagChanged
		}
	} else if settings != nil {
		return ErrParentKidsFlagChanged
	}
	r.used = true
	s.taken = true
	return nil
}
func (ds *DataStore) CommitParentKidsFlag(i *ParentKidsFlagIntent) (*ParentKidsFlagReceipt, error) {
	var empty *ParentKidsFlagReceipt
	if i == nil || i.state == nil {
		return empty, ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil || s.usersLease == nil || s.usersLease.closed || (s.kind == parentKidsFlagManagement && s.enabled && (s.settingsLease == nil || s.settingsLease.closed)) {
		s.mu.Unlock()
		return empty, ErrParentKidsFlagChanged
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
		return empty, ErrParentKidsFlagChanged
	}
	tx, err := ds.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return empty, pinContextError(ctx, ErrParentKidsFlagUnavailable)
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
			return empty, ErrParentKidsFlagChanged
		}
	}
	var owner struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(s.tuple.owner.raw, &owner) != nil {
		return empty, ErrParentKidsFlagUnavailable
	}
	ids := []string{s.original.AccountID}
	if owner.ID != s.original.AccountID {
		ids = append(ids, owner.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		var found string
		if err = tx.QueryRow(ctx, `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
			return empty, pinContextError(ctx, ErrParentKidsFlagChanged)
		}
	}
	var found string
	if err = tx.QueryRow(ctx, `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, s.original.Token).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentKidsFlagChanged)
	}
	if _, err = tx.Exec(ctx, `LOCK TABLE users IN ROW EXCLUSIVE MODE`); err != nil {
		return empty, pinContextError(ctx, ErrParentKidsFlagUnavailable)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, s.target).Scan(&found); err != nil {
		return empty, pinContextError(ctx, ErrParentKidsFlagChanged)
	}
	current, _, _, err := readProfilePinAccess(ctx, tx, s.original, s.target, s.hashes, true)
	if err != nil {
		return empty, err
	}
	if !sameParentModeTuple(current, s.tuple) {
		return empty, ErrParentKidsFlagChanged
	}

	var settingsAfter UserSettingsRecord
	var patched json.RawMessage
	if s.kind == parentKidsFlagManagement && s.enabled {
		original := s.settings.Revision.snapshot
		var raw []byte
		var version string
		err = tx.QueryRow(ctx, `SELECT settings,`+userSettingsVersion+` FROM user_settings s WHERE user_id=$1 FOR UPDATE`, s.target).Scan(&raw, &version)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return empty, pinContextError(ctx, ErrParentKidsFlagUnavailable)
		}
		if version != original.version || !bytes.Equal(raw, original.raw) {
			return empty, ErrParentKidsFlagChanged
		}
		var generation int64
		if err = tx.QueryRow(ctx, `SELECT generation FROM user_settings_versions WHERE user_id=$1 FOR UPDATE`, s.target).Scan(&generation); err != nil {
			return empty, pinContextError(ctx, ErrParentKidsFlagUnavailable)
		}
		if generation != original.generation {
			return empty, ErrParentKidsFlagChanged
		}
		desired := original.settings
		if s.enabled && desired.Display.SimpleMode == nil {
			desired.Display.SimpleMode = models.BoolPtr(true)
		}
		patched, err = patchSettings(original, desired)
		if err != nil {
			return empty, err
		}
	}
	record, err := ds.scanUserWrite(tx.QueryRow(ctx, `UPDATE users AS u SET
 is_kids_profile=$1,
 allow_live_takeover=CASE WHEN $1 THEN false ELSE allow_live_takeover END,
 allow_recording_interruption=CASE WHEN $1 THEN false ELSE allow_recording_interruption END,
 kids_mode=CASE WHEN NOT $1 THEN '' WHEN kids_mode='' THEN 'rating' ELSE kids_mode END,
 kids_max_rating=CASE WHEN NOT $1 THEN '' WHEN kids_mode='' THEN 'G' ELSE kids_max_rating END,
 kids_allowed_lists=CASE WHEN NOT $1 THEN 'null'::jsonb ELSE kids_allowed_lists END,
 updated_at=clock_timestamp()
 WHERE id=$2 AND `+userWriteVersion+`=$3 AND clock_timestamp()<$4
 RETURNING `+userColumns+`,`+userWriteVersion, s.enabled, s.target, s.tuple.profile.version, s.deadline))
	if err != nil {
		return empty, pinContextError(ctx, ErrParentKidsFlagChanged)
	}
	if s.kind == parentKidsFlagManagement && s.enabled {
		original := s.settings.Revision.snapshot
		if s.enabled && !bytes.Equal(patched, original.raw) && !(original.version == "" && bytes.Equal(patched, []byte("{}"))) {
			if original.version == "" {
				_, err = tx.Exec(ctx, `INSERT INTO user_settings(user_id,settings,updated_at) VALUES($1,$2,now())`, s.target, []byte(patched))
			} else {
				_, err = tx.Exec(ctx, `UPDATE user_settings SET settings=$2,updated_at=now() WHERE user_id=$1`, s.target, []byte(patched))
			}
			if err != nil {
				return empty, pinContextError(ctx, ErrParentKidsFlagUnavailable)
			}
		}
		settingsAfter, err = ds.scanSettings(tx.QueryRow(ctx, userSettingsSelect+` WHERE u.id=$1`, s.target))
		if err != nil {
			return empty, err
		}
	}
	if err = ctx.Err(); err != nil {
		return empty, err
	}

	if err = tx.Commit(ctx); err != nil {
		var pg *pgconn.PgError
		if errors.Is(err, pgx.ErrTxCommitRollback) || errors.As(err, &pg) {
			return empty, pinContextError(ctx, ErrParentKidsFlagUnavailable)
		}
		return empty, ErrParentKidsFlagUnknown
	}
	s.mu.Lock()
	s.committed = true
	s.mu.Unlock()
	receipt := &ParentKidsFlagReceipt{store: ds, intent: i, user: record, settings: settingsAfter}
	receipt.self = receipt
	return receipt, nil
}

func (ds *DataStore) ParentKidsFlagSettingsRequired(i *ParentKidsFlagIntent) (bool, error) {
	if i == nil || i.state == nil {
		return false, ErrParentKidsFlagChanged
	}
	s := i.state
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.self != i || s.store != ds || s.phase != parentCommitting || s.run == nil || s.run.Err() != nil || s.life.Err() != nil {
		return false, ErrParentKidsFlagChanged
	}
	return s.kind == parentKidsFlagManagement && s.enabled, nil
}

func parentKidsFlagPayload(v reflect.Value) int {
	if !v.IsValid() {
		return 0
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return 8
		}
		return 8 + parentKidsFlagPayload(v.Elem())
	case reflect.String:
		return 16 + len(v.String())
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return 24 + v.Len()
		}
		size := 24 + 16*v.Len()
		for i := 0; i < v.Len(); i++ {
			size += parentKidsFlagPayload(v.Index(i))
		}
		return size
	case reflect.Map:
		size := 48 + 32*v.Len()
		it := v.MapRange()
		for it.Next() {
			size += parentKidsFlagPayload(it.Key()) + parentKidsFlagPayload(it.Value())
		}
		return size
	case reflect.Struct:
		size := 0
		for i := 0; i < v.NumField(); i++ {
			size += parentKidsFlagPayload(v.Field(i))
		}
		return size
	default:
		return 8
	}
}
