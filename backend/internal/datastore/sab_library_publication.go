package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/database"
	"novastream/internal/importer"
	"novastream/internal/settingscommit"
)

var (
	ErrSABLibraryPublicationChanged     = errors.New("original publication changed")
	ErrSABLibraryPublicationUnavailable = errors.New("publication unavailable")
	ErrSABLibraryPublicationBusy        = errors.New("publication busy")
	ErrSABLibraryPublicationUnknown     = errors.New("publication reservation acknowledgement unknown")
)

type SABLibraryPublicationIssuer struct {
	admitted    int
	self        *SABLibraryPublicationIssuer
	store       *DataStore
	source      *importer.Service
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	intents     map[*SABLibraryPublicationIntent]struct{}
	pending     sync.WaitGroup
	once        sync.Once
}
type SABLibraryPublicationIntent struct{ state *sabLibraryPublicationState }
type sabLibraryPublicationState struct {
	self                                  *SABLibraryPublicationIntent
	issuer                                *SABLibraryPublicationIssuer
	request                               *http.Request
	original                              context.Context
	body                                  io.ReadCloser
	binding                               [32]byte
	payloadDigest                         [32]byte
	frame                                 *pgRequestFrame
	ctx                                   context.Context
	cancel                                context.CancelFunc
	expiry                                time.Time
	queueID                               int64
	library                               string
	ordinals                              []int
	policy                                *config.SABPublicationPolicyObservation
	values                                config.SABPublicationPolicyValues
	snapshot                              string
	stage                                 *importer.SABStagedPublication
	mu                                    sync.Mutex
	closed, reserving, reservationUnknown bool
	pending                               sync.WaitGroup
	once                                  sync.Once
	stop                                  func()
	done                                  chan struct{}
	watchDone                             chan struct{}
}

func (SABLibraryPublicationIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABLibraryPublicationIssuer) UnmarshalJSON([]byte) error {
	return ErrSABLibraryPublicationChanged
}
func (SABLibraryPublicationIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABLibraryPublicationIssuer(opaque)"))
}
func (SABLibraryPublicationIntent) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABLibraryPublicationIntent) UnmarshalJSON([]byte) error {
	return ErrSABLibraryPublicationChanged
}
func (SABLibraryPublicationIntent) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABLibraryPublicationIntent(opaque)"))
}
func NewSABLibraryPublicationIssuer(ctx context.Context, ds *DataStore, source *importer.Service, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*SABLibraryPublicationIssuer, error) {
	if ctx == nil || ctx.Err() != nil || ds == nil || source == nil || !source.SABPublicationSourceReady() || security == nil || !ds.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	life, cancel := context.WithCancel(ctx)
	value := &SABLibraryPublicationIssuer{store: ds, source: source, manager: manager, security: security, coordinator: coordinator, ctx: life, cancel: cancel, intents: map[*SABLibraryPublicationIntent]struct{}{}}
	value.self = value
	return value, nil
}
func (i *SABLibraryPublicationIssuer) ready() bool {
	if i == nil || i.self != i || i.ctx == nil || i.ctx.Err() != nil || !i.source.SABPublicationSourceReady() || !i.store.SettingsCommitReady(i.coordinator) || !i.security.Ready(i.manager, i.coordinator) {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return !i.closed
}
func (i *SABLibraryPublicationIssuer) Close() {
	if i == nil || i.self != i {
		return
	}
	i.once.Do(func() {
		i.mu.Lock()
		i.closed = true
		i.cancel()
		list := make([]*SABLibraryPublicationIntent, 0, len(i.intents))
		for value := range i.intents {
			list = append(list, value)
		}
		i.mu.Unlock()
		for _, value := range list {
			value.Close()
		}
		i.pending.Wait()
	})
}
func sabPublicationSelection(raw []byte) (string, []int, error) {
	if len(raw) == 0 || len(raw) > 16384 {
		return "", nil, ErrSABLibraryPublicationChanged
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, e := decoder.Token()
	if e != nil || token != json.Delim('{') {
		return "", nil, ErrSABLibraryPublicationChanged
	}
	var library string
	var ordinals []int
	seen := map[string]bool{}
	for decoder.More() {
		key, e := decoder.Token()
		name, ok := key.(string)
		if e != nil || !ok || seen[name] {
			return "", nil, ErrSABLibraryPublicationChanged
		}
		seen[name] = true
		switch name {
		case "libraryId":
			e = decoder.Decode(&library)
		case "ordinals":
			e = decoder.Decode(&ordinals)
		default:
			return "", nil, ErrSABLibraryPublicationChanged
		}
		if e != nil {
			return "", nil, ErrSABLibraryPublicationChanged
		}
	}
	token, e = decoder.Token()
	if e != nil || token != json.Delim('}') || decoder.Decode(new(any)) != io.EOF || len(seen) != 2 || !parentMutationID(library) || len(library) > 256 || len(ordinals) < 1 || len(ordinals) > 128 {
		return "", nil, ErrSABLibraryPublicationChanged
	}
	for n, v := range ordinals {
		if v < 1 || v > 128 || n > 0 && ordinals[n-1] >= v {
			return "", nil, ErrSABLibraryPublicationChanged
		}
	}
	return library, ordinals, nil
}
func publicationSignalLink(ctx context.Context, signal context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); signal() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if stop() {
				close(done)
			}
		})
		<-done
	}
}
func (i *SABLibraryPublicationIssuer) Capture(r *http.Request, queueID int64, raw []byte) (result *SABLibraryPublicationIntent, err error) {
	if !i.ready() || queueID <= 0 || r == nil || r.URL == nil || r.Method != http.MethodPost || r.URL.RawQuery != "" {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	binding, e := deploymentWebDAVBinding(r)
	if e != nil {
		return nil, ErrSABLibraryPublicationChanged
	}
	library, ordinals, e := sabPublicationSelection(raw)
	if e != nil {
		return nil, e
	}
	expiry, ok := r.Context().Deadline()
	if !ok || !expiry.After(time.Now()) || expiry.After(time.Now().Add(5*time.Minute)) {
		return nil, ErrSABLibraryPublicationChanged
	}
	// Validate the genuine already-authenticated frame without an unowned pool
	// read. All NEW current correlation reads occur under this intent's actual
	// owned transaction and late guards below.
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	claimed, claimedOK := auth.GetSession(r)
	if frame == nil || frame.self != frame || frame.store != i.store || frame.ctx.Err() != nil || !claimedOK || claimed != effectivePinFrameSession(frame) || auth.GetAccountID(r) != frame.session.AccountID || auth.IsMaster(r) != frame.session.IsMaster || !frame.session.IsMaster || frame.session.Scope != "" || frame.session.ScopeResource != "" {
		return nil, ErrSABLibraryPublicationChanged
	}
	if frame.session.ExpiresAt.Before(expiry) {
		expiry = frame.session.ExpiresAt
	}
	if frame.actorExpiry != nil && frame.actorExpiry.Before(expiry) {
		expiry = *frame.actorExpiry
	}
	life, cancel := context.WithDeadline(r.Context(), expiry)
	state := &sabLibraryPublicationState{issuer: i, request: r, original: r.Context(), body: r.Body, binding: binding, payloadDigest: sha256.Sum256(raw), frame: frame, ctx: life, cancel: cancel, expiry: expiry, queueID: queueID, library: library, ordinals: ordinals, done: make(chan struct{})}
	value := &SABLibraryPublicationIntent{state: state}
	state.self = value
	state.stop = publicationSignalLink(i.ctx, cancel)
	// Admission and actual owner registration precede native IO and every new PG
	// acquisition, including failing or cancelled initial preparation.
	i.mu.Lock()
	if i.closed || i.admitted >= 64 || len(i.intents) >= 64 {
		i.mu.Unlock()
		state.stop()
		cancel()
		return nil, ErrSABLibraryPublicationBusy
	}
	i.admitted++
	i.intents[value] = struct{}{}
	i.pending.Add(1)
	i.mu.Unlock()
	defer func() { i.mu.Lock(); i.admitted--; i.mu.Unlock(); i.pending.Done() }()
	defer func() {
		if result == nil {
			value.Close()
		}
	}()
	policy, e := i.security.CaptureSABPublicationPolicy(state.ctx)
	if e != nil {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	state.policy = policy
	ctx, finish, e := state.begin(r)
	if e != nil {
		return nil, e
	}
	snapshot, values, target, e := state.readCurrent(ctx, false, database.SABPublicationRecord{})
	finish()
	if e != nil {
		return nil, e
	}
	state.snapshot = snapshot
	state.values = values
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return nil, ErrSABLibraryPublicationChanged
	}
	state.watchDone = make(chan struct{})
	go state.watchOriginal()
	state.mu.Unlock()
	// The actual initial PG/native query and its connection have ended before
	// source IO. The watcher remains separately owned through real cleanup.
	actor, e := i.source.OpenSABStagedPublication(state.ctx, queueID, values, target, ordinals)
	if e != nil {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		actor.Close()
		return nil, ErrSABLibraryPublicationChanged
	}
	state.stage = actor
	state.mu.Unlock()
	if e = value.Check(r); e != nil {
		return nil, e
	}
	return value, nil
}

func (p *SABLibraryPublicationIntent) valid() bool {
	return p != nil && p.state != nil && p.state.self == p
}
func (s *sabLibraryPublicationState) current(r *http.Request) error {
	if r != s.request || r.Context() != s.original || r.Body != s.body || s.ctx.Err() != nil || !s.expiry.After(time.Now()) || !s.issuer.ready() {
		return ErrSABLibraryPublicationChanged
	}
	binding, e := deploymentWebDAVBinding(r)
	if e != nil || binding != s.binding {
		return ErrSABLibraryPublicationChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSABLibraryPublicationChanged
	}
	return nil
}
func (s *sabLibraryPublicationState) begin(r *http.Request) (context.Context, func(), error) {
	if s.current(r) != nil {
		return nil, nil, ErrSABLibraryPublicationChanged
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, nil, ErrSABLibraryPublicationChanged
	}
	s.pending.Add(1)
	s.mu.Unlock()
	expiry := time.Now().Add(5 * time.Second)
	if s.expiry.Before(expiry) {
		expiry = s.expiry
	}
	ctx, cancel := context.WithDeadline(s.ctx, expiry)
	return ctx, func() { cancel(); s.pending.Done() }, nil
}
func (p *SABLibraryPublicationIntent) Context() context.Context {
	if p.valid() {
		return p.state.ctx
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (p *SABLibraryPublicationIntent) ExpiresAt() time.Time {
	if p.valid() {
		return p.state.expiry
	}
	return time.Time{}
}
func (p *SABLibraryPublicationIntent) Close() {
	if !p.valid() {
		return
	}
	s := p.state
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		actor := s.stage
		s.mu.Unlock()
		if actor != nil {
			actor.Close()
		}
		s.pending.Wait()
		if s.watchDone != nil {
			<-s.watchDone
		}
		s.stop()
		s.issuer.mu.Lock()
		delete(s.issuer.intents, p)
		s.issuer.mu.Unlock()
		close(s.done)
	})
	<-s.done
}
func (p *SABLibraryPublicationIntent) Check(r *http.Request) error {
	if !p.valid() {
		return ErrSABLibraryPublicationChanged
	}
	s := p.state
	ctx, finish, e := s.begin(r)
	if e != nil {
		return e
	}
	defer finish()
	snapshot, values, _, e := s.readCurrent(ctx, false, database.SABPublicationRecord{})
	if e != nil {
		return e
	}
	if snapshot != s.snapshot || values != s.values {
		return ErrSABLibraryPublicationChanged
	}
	s.mu.Lock()
	actor := s.stage
	s.mu.Unlock()
	if actor == nil || actor.Check(s.ctx) != nil {
		return ErrSABLibraryPublicationChanged
	}
	return s.current(r)
}

// ReadAt retains the original current master request around the actual bounded
// reader; neither an ordinal nor the returned container DATA confers permission.
func (p *SABLibraryPublicationIntent) ReadAt(r *http.Request, ordinal int, buffer []byte, offset int64) (int, error) {
	if !p.valid() || len(buffer) > 1<<20 || p.Check(r) != nil {
		return 0, ErrSABLibraryPublicationChanged
	}
	s := p.state
	ctx, finish, e := s.begin(r)
	if e != nil {
		return 0, e
	}
	defer finish()
	_ = ctx
	n, e := s.stage.ReadAt(s.ctx, ordinal, buffer, offset)
	if p.Check(r) != nil {
		clear(buffer)
		return 0, ErrSABLibraryPublicationChanged
	}
	return n, e
}

// Reserve records an idempotent operational intent only. It does not copy,
// publish media, mint a file completion or insert a local_media_items row.
func (p *SABLibraryPublicationIntent) Reserve(r *http.Request) (database.SABPublicationRecord, error) {
	if !p.valid() || p.Check(r) != nil {
		return database.SABPublicationRecord{}, ErrSABLibraryPublicationChanged
	}
	s := p.state
	ctx, finish, e := s.begin(r)
	if e != nil {
		return database.SABPublicationRecord{}, e
	}
	defer finish()
	s.mu.Lock()
	if s.reserving || s.reservationUnknown {
		s.mu.Unlock()
		return database.SABPublicationRecord{}, ErrSABLibraryPublicationBusy
	}
	s.reserving = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.reserving = false; s.mu.Unlock() }()
	record, e := s.stage.Claim(s.ctx, s.library)
	if e != nil {
		if errors.Is(e, database.ErrSABPublicationOutcomeUnknown) {
			s.mu.Lock()
			s.reservationUnknown = true
			s.mu.Unlock()
		}
		return record, e
	}
	snapshot, values, _, e := s.readCurrent(ctx, true, record)
	if e != nil {
		if errors.Is(e, ErrSABLibraryPublicationUnknown) {
			s.mu.Lock()
			s.reservationUnknown = true
			s.mu.Unlock()
		}
		return record, e
	}
	if snapshot != s.snapshot || values != s.values || s.current(r) != nil {
		return record, ErrSABLibraryPublicationUnknown
	}
	return record, nil
}

var sabPublicationTables = []string{"accounts", "local_media_libraries", "media_library_account_grants", "media_library_access_policies", "media_library_profile_grants", "sessions", "users"}

func sabPublicationRelations(ctx context.Context, tx pgx.Tx) ([]deploymentWebDAVRelation, error) {
	names := append([]string(nil), sabPublicationTables...)
	names = append(names, "sab_library_publications", "regional_policy_revision_seq")
	sort.Strings(names)
	var out []deploymentWebDAVRelation
	schema := ""
	for _, name := range names {
		value := deploymentWebDAVRelation{name: name}
		e := tx.QueryRow(ctx, `SELECT n.nspname,c.oid,c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent=c.oid OR i.inhrelid=c.oid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, name).Scan(&value.schema, &value.oid, &value.kind, &value.persistence, &value.partition, &value.rls, &value.force, &value.inherit)
		expected := "r"
		if name == "regional_policy_revision_seq" {
			expected = "S"
		}
		if e != nil || value.schema == "" || value.oid == 0 || value.kind != expected || value.persistence != "p" || value.partition || value.rls || value.force || value.inherit || schema != "" && schema != value.schema {
			return nil, ErrSABLibraryPublicationUnavailable
		}
		schema = value.schema
		out = append(out, value)
	}
	return out, nil
}
func sabPublicationError(e error) error {
	if errors.Is(e, settingscommit.ErrBusy) || errors.Is(deploymentWebDAVError(e), ErrDeploymentWebDAVBusy) {
		return ErrSABLibraryPublicationBusy
	}
	return ErrSABLibraryPublicationUnavailable
}
func (s *sabLibraryPublicationState) readCurrent(ctx context.Context, reserve bool, record database.SABPublicationRecord) (wire string, values config.SABPublicationPolicyValues, target string, err error) {
	committed := false
	progress, e := s.issuer.store.beginSettingsProgressTx(ctx, s.issuer.coordinator)
	if e != nil {
		return "", values, "", sabPublicationError(e)
	}
	defer func() {
		if closeErr := finishShareCreationTransaction(progress); err == nil && closeErr != nil {
			err = ErrSABLibraryPublicationUnavailable
		}
		if err == nil && s.current(s.request) != nil {
			err = ErrSABLibraryPublicationChanged
		}
		if err != nil {
			wire = ""
			target = ""
			if committed {
				err = ErrSABLibraryPublicationUnknown
			}
		}
	}()
	relations, e := sabPublicationRelations(progress.ctx, progress.tx)
	if e != nil {
		return "", values, "", e
	}
	schema := relations[0].schema
	var found string
	// Original account/session row locks precede every new late relation guard.
	if e = progress.tx.QueryRow(progress.ctx, "SELECT id FROM "+pgx.Identifier{schema, "accounts"}.Sanitize()+" WHERE id=$1 FOR UPDATE", s.frame.session.AccountID).Scan(&found); e != nil {
		return "", values, "", sabPublicationError(e)
	}
	if e = progress.tx.QueryRow(progress.ctx, "SELECT token FROM "+pgx.Identifier{schema, "sessions"}.Sanitize()+" WHERE token=$1 FOR UPDATE", s.frame.session.Token).Scan(&found); e != nil {
		return "", values, "", sabPublicationError(e)
	}
	journalMode := "SHARE"
	if reserve {
		journalMode = "SHARE ROW EXCLUSIVE"
	}
	if _, e = progress.tx.Exec(progress.ctx, "LOCK TABLE "+pgx.Identifier{schema, "sab_library_publications"}.Sanitize()+" IN "+journalMode+" MODE NOWAIT"); e != nil {
		return "", values, "", sabPublicationError(e)
	}
	for _, name := range sabPublicationTables {
		if _, e = progress.tx.Exec(progress.ctx, "LOCK TABLE "+pgx.Identifier{schema, name}.Sanitize()+" IN SHARE MODE NOWAIT"); e != nil {
			return "", values, "", sabPublicationError(e)
		}
	}
	again, e := sabPublicationRelations(progress.ctx, progress.tx)
	if e != nil || !reflect.DeepEqual(again, relations) {
		return "", values, "", ErrSABLibraryPublicationUnavailable
	}
	fence, e := s.issuer.security.BindSABPublicationPolicy(progress.ctx, s.policy, progress.hold)
	if e != nil {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	values, e = fence.Values()
	if e != nil {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	var sessionVersion, actorVersion string
	var current bool
	query := `SELECT concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),a.is_master AND s.scope='' AND COALESCE(s.scope_resource,'')='' AND s.expires_at>clock_timestamp() AND (a.expires_at IS NULL OR a.expires_at>clock_timestamp()) FROM ` + pgx.Identifier{schema, "sessions"}.Sanitize() + ` s JOIN ` + pgx.Identifier{schema, "accounts"}.Sanitize() + ` a ON a.id=s.account_id WHERE s.token=$1 AND a.id=$2`
	if e = progress.tx.QueryRow(progress.ctx, query, s.frame.session.Token, s.frame.session.AccountID).Scan(&sessionVersion, &actorVersion, &current); e != nil || !current || sessionVersion != s.frame.sessionRow.version || actorVersion != s.frame.actorRow.version {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	var raw *string
	e = progress.tx.QueryRow(progress.ctx, sabPublicationSnapshotQuery(schema), s.library).Scan(&raw)
	if e != nil || raw == nil || len(*raw) > 32768 {
		return "", values, "", ErrSABLibraryPublicationUnavailable
	}
	var decoded struct {
		Fence   *string
		Library struct {
			ID, Type, Root, Physical string
			Roots                    []string
		}
		Policy             []string
		Accounts, Profiles [][]string
	}
	if json.Unmarshal([]byte(*raw), &decoded) != nil || decoded.Library.ID != s.library || len(decoded.Library.Roots) > 64 || len(decoded.Library.Roots) == 0 {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	if decoded.Library.Type != "movie" && decoded.Library.Type != "show" && decoded.Library.Type != "other" {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	for _, root := range decoded.Library.Roots {
		if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/" || strings.ContainsAny(root, "\\\x00") {
			return "", values, "", ErrSABLibraryPublicationChanged
		}
	}
	for _, rows := range [][][]string{decoded.Accounts, decoded.Profiles} {
		for _, row := range rows {
			if len(row) != 2 || !parentMutationID(row[0]) || !deploymentWebDAVPhysical.MatchString(row[1]) {
				return "", values, "", ErrSABLibraryPublicationChanged
			}
		}
	}
	if len(decoded.Policy) != 0 && (len(decoded.Policy) != 2 || decoded.Policy[0] != "all" && decoded.Policy[0] != "restricted" || !deploymentWebDAVPhysical.MatchString(decoded.Policy[1])) {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	if decoded.Fence == nil {
		return "", values, "", ErrSABLibraryPublicationUnavailable
	}
	if fenceValue, e := strconv.ParseUint(*decoded.Fence, 10, 63); e != nil || fenceValue == 0 {
		return "", values, "", ErrSABLibraryPublicationUnavailable
	}
	target = decoded.Library.Roots[0]
	if target == "" {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	wire = *raw
	if s.snapshot != "" && (wire != s.snapshot || values != s.values) {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	if reserve {
		ordinals, _ := json.Marshal(record.Ordinals)
		original, _ := json.Marshal([]any{s.snapshot, s.frame.sessionRow.version, s.frame.actorRow.version, s.binding, s.payloadDigest, s.queueID, s.library, s.ordinals})
		hash := sha256.Sum256(original)
		clear(original)
		binding := hex.EncodeToString(hash[:])
		_, e = progress.tx.Exec(progress.ctx, "INSERT INTO "+pgx.Identifier{schema, "sab_library_publications"}.Sanitize()+`(operation_id,stage_operation_id,stage_namespace,manifest_digest,target_library_id,target_binding,original_binding,selected_ordinals,expected_bytes)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(operation_id)DO NOTHING`, record.OperationID, record.StageOperationID, record.StageNamespace, record.ManifestDigest, s.library, record.TargetBinding, binding, ordinals, record.Bytes)
		if e != nil {
			return "", values, "", sabPublicationError(e)
		}
		var matches bool
		e = progress.tx.QueryRow(progress.ctx, "SELECT stage_operation_id=$2 AND stage_namespace=$3 AND manifest_digest=$4 AND target_library_id=$5 AND target_binding=$6 AND original_binding=$7 AND selected_ordinals=$8::jsonb AND expected_bytes=$9 FROM "+pgx.Identifier{schema, "sab_library_publications"}.Sanitize()+" WHERE operation_id=$1", record.OperationID, record.StageOperationID, record.StageNamespace, record.ManifestDigest, s.library, record.TargetBinding, binding, ordinals, record.Bytes).Scan(&matches)
		if e != nil || !matches {
			return "", values, "", ErrSABLibraryPublicationChanged
		}
		if _, e = fence.Values(); e != nil || s.current(s.request) != nil || progress.ctx.Err() != nil {
			return "", values, "", ErrSABLibraryPublicationChanged
		}
		if e = progress.tx.Commit(progress.ctx); e != nil {
			return "", values, "", ErrSABLibraryPublicationUnknown
		}
		committed = true
	}
	if _, e = fence.Values(); e != nil || progress.ctx.Err() != nil {
		return "", values, "", ErrSABLibraryPublicationChanged
	}
	return wire, values, target, nil
}
func sabPublicationSnapshotQuery(schema string) string {
	table := func(name string) string { return pgx.Identifier{schema, name}.Sanitize() }
	return fmt.Sprintf(`WITH lib AS MATERIALIZED(SELECT id,library_type,root_path,root_paths,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s WHERE id=$1 AND octet_length(root_path)<=4096 AND octet_length(root_paths::text)<=16384 AND jsonb_typeof(root_paths)='array' AND jsonb_array_length(root_paths)<=64),pol AS MATERIALIZED(SELECT access_mode,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s WHERE library_id=$1),a AS MATERIALIZED(SELECT CASE WHEN octet_length(account_id)<=256 THEN account_id END id,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s WHERE library_id=$1 ORDER BY account_id COLLATE "C" LIMIT 257),p AS MATERIALIZED(SELECT CASE WHEN octet_length(profile_id)<=256 THEN profile_id END id,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s WHERE library_id=$1 ORDER BY profile_id COLLATE "C" LIMIT 257),wire AS(SELECT jsonb_build_object('Fence',(SELECT CASE WHEN is_called AND last_value>0 THEN last_value::text END FROM %s),'Library',(SELECT jsonb_build_object('ID',id,'Type',library_type,'Root',root_path,'Roots',CASE WHEN jsonb_array_length(root_paths)=0 THEN jsonb_build_array(root_path) ELSE root_paths END,'Physical',physical)FROM lib),'Policy',COALESCE((SELECT jsonb_build_array(access_mode,physical)FROM pol),'[]'::jsonb),'Accounts',COALESCE((SELECT jsonb_agg(jsonb_build_array(id,physical)ORDER BY id COLLATE "C")FROM a),'[]'::jsonb),'Profiles',COALESCE((SELECT jsonb_agg(jsonb_build_array(id,physical)ORDER BY id COLLATE "C")FROM p),'[]'::jsonb))::text raw) SELECT CASE WHEN (SELECT count(*)FROM lib)=1 AND (SELECT count(*)FROM a)<=256 AND (SELECT count(*)FROM p)<=256 AND octet_length(raw)<=32768 THEN raw END FROM wire`, table("local_media_libraries"), table("media_library_access_policies"), table("media_library_account_grants"), table("media_library_profile_grants"), table("regional_policy_revision_seq"))
}

// The signal-only watcher starts before the initial full staged digest read.
// Its actual queries and callback end are joined by original intent cleanup.
func (s *sabLibraryPublicationState) watchOriginal() {
	defer close(s.watchDone)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ctx, finish, e := s.begin(s.request)
			if e != nil {
				s.cancel()
				return
			}
			snapshot, values, _, e := s.readCurrent(ctx, false, database.SABPublicationRecord{})
			finish()
			if e != nil || snapshot != s.snapshot || values != s.values {
				s.cancel()
				return
			}
		}
	}
}
