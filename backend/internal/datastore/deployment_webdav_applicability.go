package datastore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"novastream/config"
	"novastream/internal/settingscommit"
)

var (
	ErrDeploymentWebDAVUnavailable = errors.New("current WebDAV deployment unavailable")
	ErrDeploymentWebDAVChanged     = errors.New("original WebDAV deployment changed")
	ErrDeploymentWebDAVBusy        = errors.New("current WebDAV deployment busy")
	ErrDeploymentWebDAVUnsupported = errors.New("shared WebDAV deployment unsupported")
)
var deploymentWebDAVTables = []string{"accounts", "regional_profile_policies", "user_settings", "user_settings_versions", "users"}
var deploymentWebDAVPhysical = regexp.MustCompile(`^[0-9]+:[0-9]+:\([0-9]+,[0-9]+\)$`)

type DeploymentWebDAVIssuer struct {
	self        *DeploymentWebDAVIssuer
	store       *DataStore
	manager     *config.Manager
	security    *config.ManagedSecurity
	coordinator *settingscommit.Coordinator
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closed      bool
	proofs      map[*DeploymentWebDAVApplicability]struct{}
	pending     sync.WaitGroup
	once        sync.Once
}
type DeploymentWebDAVApplicability struct{ state *deploymentWebDAVState }
type deploymentWebDAVState struct {
	self           *DeploymentWebDAVApplicability
	issuer         *DeploymentWebDAVIssuer
	request        *http.Request
	requestContext context.Context
	body           io.ReadCloser
	binding        [32]byte
	original       *config.WebDAVBasicPolicyObservation
	ctx            context.Context
	cancel         context.CancelFunc
	expiry         time.Time
	snapshot       string
	prefix         string
	peer           string
	mu             sync.Mutex
	closed         bool
	pending        sync.WaitGroup
	joinIssuer     func()
	once           sync.Once
	done           chan struct{}
}

var deploymentWebDAVAdmission = struct {
	sync.Mutex
	owners map[*DeploymentWebDAVApplicability]string
	peers  map[string]int
}{owners: map[*DeploymentWebDAVApplicability]string{}, peers: map[string]int{}}

func NewDeploymentWebDAVIssuer(ctx context.Context, ds *DataStore, manager *config.Manager, security *config.ManagedSecurity, coordinator *settingscommit.Coordinator) (*DeploymentWebDAVIssuer, error) {
	if ctx == nil || ctx.Err() != nil || ds == nil || security == nil || !ds.SettingsCommitReady(coordinator) || !security.Ready(manager, coordinator) {
		return nil, ErrDeploymentWebDAVUnavailable
	}
	life, cancel := context.WithCancel(ctx)
	value := &DeploymentWebDAVIssuer{store: ds, manager: manager, security: security, coordinator: coordinator, ctx: life, cancel: cancel, proofs: make(map[*DeploymentWebDAVApplicability]struct{})}
	value.self = value
	return value, nil
}
func (i *DeploymentWebDAVIssuer) ready() bool {
	if i == nil || i.self != i || i.ctx == nil || i.ctx.Err() != nil || !i.store.SettingsCommitReady(i.coordinator) || !i.security.Ready(i.manager, i.coordinator) {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return !i.closed
}
func (i *DeploymentWebDAVIssuer) Close() {
	if i == nil || i.self != i {
		return
	}
	i.once.Do(func() {
		i.mu.Lock()
		i.closed = true
		i.cancel()
		all := make([]*DeploymentWebDAVApplicability, 0, len(i.proofs))
		for proof := range i.proofs {
			all = append(all, proof)
		}
		i.mu.Unlock()
		for _, proof := range all {
			proof.Close()
		}
		i.pending.Wait()
	})
}
func (DeploymentWebDAVIssuer) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("DeploymentWebDAVIssuer(opaque)"))
}
func (DeploymentWebDAVIssuer) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*DeploymentWebDAVIssuer) UnmarshalJSON([]byte) error  { return ErrDeploymentWebDAVChanged }
func (DeploymentWebDAVApplicability) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("DeploymentWebDAVApplicability(opaque)"))
}
func (DeploymentWebDAVApplicability) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*DeploymentWebDAVApplicability) UnmarshalJSON([]byte) error  { return ErrDeploymentWebDAVChanged }
func (p *DeploymentWebDAVApplicability) valid() bool {
	return p != nil && p.state != nil && p.state.self == p
}
func (p *DeploymentWebDAVApplicability) Context() context.Context {
	if !p.valid() {
		return familyMediaClosedContext()
	}
	return p.state.ctx
}
func (p *DeploymentWebDAVApplicability) ExpiresAt() time.Time {
	if !p.valid() {
		return time.Time{}
	}
	return p.state.expiry
}

// Prefix describes the original successful native observation. It remains
// immutable after retirement and never establishes current source permission.
func (p *DeploymentWebDAVApplicability) Prefix() string {
	if !p.valid() {
		return ""
	}
	return p.state.prefix
}
func (p *DeploymentWebDAVApplicability) Close() {
	if !p.valid() {
		return
	}
	s := p.state
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		s.mu.Unlock()
		s.pending.Wait()
		s.joinIssuer()
		s.issuer.mu.Lock()
		delete(s.issuer.proofs, p)
		s.issuer.mu.Unlock()
		deploymentWebDAVAdmission.Lock()
		if peer, ok := deploymentWebDAVAdmission.owners[p]; ok {
			delete(deploymentWebDAVAdmission.owners, p)
			deploymentWebDAVAdmission.peers[peer]--
			if deploymentWebDAVAdmission.peers[peer] == 0 {
				delete(deploymentWebDAVAdmission.peers, peer)
			}
		}
		deploymentWebDAVAdmission.Unlock()
		close(s.done)
	})
	<-s.done
}
func deploymentWebDAVPeer(value string) (string, error) {
	if address, err := netip.ParseAddr(value); err == nil {
		if address.Zone() != "" {
			return "", ErrDeploymentWebDAVChanged
		}
		return address.Unmap().String(), nil
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || port == "" {
		return "", ErrDeploymentWebDAVChanged
	}
	address, err := netip.ParseAddr(host)
	if err != nil || address.Zone() != "" {
		return "", ErrDeploymentWebDAVChanged
	}
	if _, err = netip.ParseAddrPort(value); err != nil {
		return "", ErrDeploymentWebDAVChanged
	}
	return address.Unmap().String(), nil
}
func deploymentWebDAVBinding(r *http.Request) ([32]byte, error) {
	var zero [32]byte
	if r == nil || r.URL == nil || r.Context().Err() != nil || !reflect.ValueOf(r.Context()).Comparable() || r.URL.User != nil || r.Body != nil && !reflect.ValueOf(r.Body).Comparable() {
		return zero, ErrDeploymentWebDAVChanged
	}
	if len(r.Header) > 128 || len(r.Method) > 32 || len(r.URL.String()) > 8192 || len(r.RequestURI) > 8192 || len(r.Host) > 1024 || len(r.RemoteAddr) > 256 || !utf8.ValidString(r.URL.Path) {
		return zero, ErrDeploymentWebDAVChanged
	}
	size, values := 0, 0
	for key, items := range r.Header {
		size += len(key)
		values += len(items)
		for _, v := range items {
			size += len(v)
		}
		if size > 65536 || values > 256 {
			return zero, ErrDeploymentWebDAVChanged
		}
	}
	raw, err := json.Marshal([]any{r.Method, r.URL.String(), r.RequestURI, r.Host, r.RemoteAddr, r.Header, r.ProtoMajor, r.ProtoMinor, r.ContentLength})
	if err != nil {
		return zero, ErrDeploymentWebDAVChanged
	}
	defer clear(raw)
	return sha256.Sum256(raw), nil
}
func (s *deploymentWebDAVState) current(r *http.Request) error {
	if r != s.request || r == nil || !s.issuer.ready() || s.ctx.Err() != nil || !s.expiry.After(time.Now()) {
		return ErrDeploymentWebDAVChanged
	}
	binding, err := deploymentWebDAVBinding(r)
	if err != nil || r.Context() != s.requestContext || r.Body != s.body || binding != s.binding {
		return ErrDeploymentWebDAVChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrDeploymentWebDAVChanged
	}
	return nil
}
func (s *deploymentWebDAVState) begin(r *http.Request) (context.Context, func(), error) {
	if err := s.current(r); err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, nil, ErrDeploymentWebDAVChanged
	}
	s.pending.Add(1)
	s.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	if s.expiry.Before(deadline) {
		deadline = s.expiry
	}
	ctx, cancel := context.WithDeadline(s.ctx, deadline)
	return ctx, func() { cancel(); s.pending.Done() }, nil
}
func (i *DeploymentWebDAVIssuer) Capture(r *http.Request, original *config.WebDAVBasicPolicyObservation) (proof *DeploymentWebDAVApplicability, err error) {
	if !i.ready() || original == nil {
		return nil, ErrDeploymentWebDAVUnavailable
	}
	binding, err := deploymentWebDAVBinding(r)
	if err != nil {
		return nil, err
	}
	expiry, ok := r.Context().Deadline()
	entered := time.Now()
	if !ok || !expiry.After(entered) || expiry.After(entered.Add(24*time.Hour)) {
		return nil, ErrDeploymentWebDAVChanged
	}
	peer, err := deploymentWebDAVPeer(r.RemoteAddr)
	if err != nil {
		return nil, err
	}
	i.mu.Lock()
	if i.closed || i.ctx.Err() != nil {
		i.mu.Unlock()
		return nil, ErrDeploymentWebDAVUnavailable
	}
	i.pending.Add(1)
	i.mu.Unlock()
	defer i.pending.Done()
	life, cancel := context.WithDeadline(r.Context(), expiry)
	value := &DeploymentWebDAVApplicability{state: &deploymentWebDAVState{issuer: i, request: r, requestContext: r.Context(), body: r.Body, binding: binding, original: original, ctx: life, cancel: cancel, expiry: expiry, peer: peer, done: make(chan struct{})}}
	s := value.state
	s.self = value
	s.joinIssuer = familyMediaAfterFunc(i.ctx, cancel)
	deploymentWebDAVAdmission.Lock()
	if len(deploymentWebDAVAdmission.owners) >= 64 || deploymentWebDAVAdmission.peers[peer] >= 8 {
		deploymentWebDAVAdmission.Unlock()
		cancel()
		s.joinIssuer()
		return nil, ErrDeploymentWebDAVBusy
	}
	deploymentWebDAVAdmission.owners[value] = peer
	deploymentWebDAVAdmission.peers[peer]++
	deploymentWebDAVAdmission.Unlock()
	i.mu.Lock()
	closed := i.closed || i.ctx.Err() != nil
	if !closed {
		i.proofs[value] = struct{}{}
	}
	i.mu.Unlock()
	if closed {
		value.Close()
		return nil, ErrDeploymentWebDAVChanged
	}
	defer func() {
		if proof == nil {
			value.Close()
		}
	}()
	work, finish, err := s.begin(r)
	if err != nil {
		return nil, err
	}
	err = func() error {
		defer finish()
		observed, e := s.readCurrent(work, r)
		if e != nil {
			return e
		}
		if e = s.current(r); e != nil {
			return e
		}
		s.mu.Lock()
		s.snapshot = observed.wire
		s.prefix = observed.prefix
		s.mu.Unlock()
		return nil
	}()
	if err != nil {
		return nil, err
	}
	return value, nil
}
func (p *DeploymentWebDAVApplicability) Check(r *http.Request) (err error) {
	if !p.valid() {
		return ErrDeploymentWebDAVChanged
	}
	s := p.state
	work, finish, err := s.begin(r)
	if err != nil {
		s.cancel()
		return err
	}
	defer finish()
	defer func() {
		if err != nil {
			s.cancel()
		}
	}()
	current, err := s.readCurrent(work, r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	original := s.snapshot
	prefix := s.prefix
	s.mu.Unlock()
	if original == "" || current.wire != original || current.prefix != prefix {
		return ErrDeploymentWebDAVChanged
	}
	return s.current(r)
}
func deploymentWebDAVError(err error) error {
	var pg *pgconn.PgError
	if errors.Is(err, settingscommit.ErrBusy) || errors.As(err, &pg) && pg.Code == "55P03" {
		return ErrDeploymentWebDAVBusy
	}
	return ErrDeploymentWebDAVUnavailable
}

type deploymentWebDAVRelation struct {
	name, schema, kind, persistence string
	oid                             uint32
	partition, rls, force, inherit  bool
}

func deploymentWebDAVRelations(ctx context.Context, tx pgx.Tx) ([]deploymentWebDAVRelation, error) {
	names := append([]string(nil), deploymentWebDAVTables...)
	names = append(names, "regional_policy_revision_seq")
	sort.Strings(names)
	out := make([]deploymentWebDAVRelation, 0, len(names))
	schema := ""
	for _, name := range names {
		v := deploymentWebDAVRelation{name: name}
		err := tx.QueryRow(ctx, `SELECT n.nspname,c.oid,c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent=c.oid OR i.inhrelid=c.oid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, name).Scan(&v.schema, &v.oid, &v.kind, &v.persistence, &v.partition, &v.rls, &v.force, &v.inherit)
		expected := "r"
		if name == "regional_policy_revision_seq" {
			expected = "S"
		}
		if err != nil || v.schema == "" || len(v.schema) > 256 || !utf8.ValidString(v.schema) || schema != "" && schema != v.schema || v.oid == 0 || v.kind != expected || v.persistence != "p" || v.partition || v.rls || v.force || v.inherit {
			return nil, ErrDeploymentWebDAVUnavailable
		}
		schema = v.schema
		out = append(out, v)
	}
	return out, nil
}

type deploymentWebDAVReadResult struct {
	wire, prefix string
}

func (s *deploymentWebDAVState) readCurrent(ctx context.Context, r *http.Request) (observed deploymentWebDAVReadResult, err error) {
	if s.current(r) != nil || ctx.Err() != nil {
		return observed, ErrDeploymentWebDAVChanged
	}
	progress, e := s.issuer.store.beginSettingsProgressTx(ctx, s.issuer.coordinator)
	if e != nil {
		return observed, deploymentWebDAVError(e)
	}
	defer func() {
		if closeErr := progress.Close(); closeErr != nil && err == nil {
			err = ErrDeploymentWebDAVUnavailable
		}
		if err == nil && (ctx.Err() != nil || s.current(r) != nil) {
			err = ErrDeploymentWebDAVChanged
		}
		if err != nil {
			observed = deploymentWebDAVReadResult{}
		}
	}()
	relations, e := deploymentWebDAVRelations(progress.ctx, progress.tx)
	if e != nil {
		return observed, e
	}
	schema := relations[0].schema
	// All original progress/native locks precede these late deterministic guards.
	for _, name := range deploymentWebDAVTables {
		if _, e = progress.tx.Exec(progress.ctx, "LOCK TABLE "+pgx.Identifier{schema, name}.Sanitize()+" IN SHARE MODE NOWAIT"); e != nil {
			return observed, deploymentWebDAVError(e)
		}
	}
	current, e := deploymentWebDAVRelations(progress.ctx, progress.tx)
	if e != nil || !reflect.DeepEqual(relations, current) {
		return observed, ErrDeploymentWebDAVUnavailable
	}
	fence, e := s.issuer.security.BindWebDAVBasicPolicy(r, progress.ctx, s.original, progress.hold)
	if e != nil {
		return observed, ErrDeploymentWebDAVChanged
	}
	values, e := fence.Values()
	if e != nil || !values.Enabled {
		return observed, ErrDeploymentWebDAVChanged
	}
	var raw *string
	e = progress.tx.QueryRow(progress.ctx, deploymentWebDAVSnapshotQuery(schema)).Scan(&raw)
	if e != nil {
		return observed, deploymentWebDAVError(e)
	}
	if raw == nil || len(*raw) > 65536 {
		return observed, ErrDeploymentWebDAVUnavailable
	}
	if e = decodeDeploymentWebDAVSnapshot(*raw, values.EnforceProfilePins); e != nil {
		return observed, e
	}
	if _, e = fence.Values(); e != nil || s.current(r) != nil || progress.ctx.Err() != nil {
		return observed, ErrDeploymentWebDAVChanged
	}
	return deploymentWebDAVReadResult{wire: *raw, prefix: values.Prefix}, nil
}

func deploymentWebDAVSnapshotQuery(schema string) string {
	a := pgx.Identifier{schema, "accounts"}.Sanitize()
	u := pgx.Identifier{schema, "users"}.Sanitize()
	v := pgx.Identifier{schema, "user_settings_versions"}.Sanitize()
	p := pgx.Identifier{schema, "regional_profile_policies"}.Sanitize()
	seq := pgx.Identifier{schema, "regional_policy_revision_seq"}.Sanitize()
	return fmt.Sprintf(`WITH a AS MATERIALIZED(SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 256 THEN id END id,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s ORDER BY id COLLATE "C" LIMIT 257),u AS MATERIALIZED(SELECT CASE WHEN octet_length(id) BETWEEN 1 AND 256 THEN id END id,CASE WHEN octet_length(account_id) BETWEEN 1 AND 256 THEN account_id END account_id,is_kids_profile kids,CASE WHEN octet_length(pin_hash)<=128 THEN pin_hash='' END empty_pin,CASE WHEN pin_length BETWEEN 0 AND 12 THEN pin_length END pin_length,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s ORDER BY id COLLATE "C" LIMIT 4097),p AS MATERIALIZED(SELECT CASE WHEN octet_length(user_id) BETWEEN 1 AND 256 THEN user_id END id,concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s ORDER BY user_id COLLATE "C" LIMIT 4097),wire AS(SELECT jsonb_build_object('version',1,'fence',(SELECT CASE WHEN is_called AND last_value>0 THEN last_value::text END FROM %s),'tables',(SELECT jsonb_agg(jsonb_build_array(c.relname,n.nspname,c.oid::text,c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent=c.oid OR i.inhrelid=c.oid)) ORDER BY c.relname COLLATE "C") FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=%s AND c.relname IN('accounts','regional_profile_policies','regional_policy_revision_seq','user_settings','user_settings_versions','users')),'accounts',COALESCE((SELECT jsonb_agg(jsonb_build_array(id,physical) ORDER BY id COLLATE "C") FROM a),'[]'::jsonb),'profiles',COALESCE((SELECT jsonb_agg(jsonb_build_array(u.id,u.account_id,u.kids,u.empty_pin,u.pin_length,u.physical,(SELECT CASE WHEN count(*)=1 THEN min(generation) END FROM(SELECT generation FROM %s WHERE user_id=u.id LIMIT 2) vv),(SELECT CASE WHEN count(*)=1 THEN min(physical) END FROM(SELECT concat(tableoid::oid,':',xmin::text,':',ctid::text) physical FROM %s WHERE user_id=u.id LIMIT 2) vv)) ORDER BY u.id COLLATE "C") FROM u),'[]'::jsonb),'policies',COALESCE((SELECT jsonb_agg(jsonb_build_array(id,physical) ORDER BY id COLLATE "C") FROM p),'[]'::jsonb))::text raw) SELECT CASE WHEN (SELECT count(*) FROM a)<=256 AND (SELECT count(*) FROM u)<=4096 AND (SELECT count(*) FROM p)<=4096 AND octet_length(raw)<=65536 THEN raw END FROM wire`, a, u, p, seq, quoteLiteralDeploymentSchema(schema), v, v)
}
func quoteLiteralDeploymentSchema(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

type deploymentWebDAVWire struct {
	Version  *int                `json:"version"`
	Fence    *string             `json:"fence"`
	Tables   [][]json.RawMessage `json:"tables"`
	Accounts [][]json.RawMessage `json:"accounts"`
	Profiles [][]json.RawMessage `json:"profiles"`
	Policies [][]json.RawMessage `json:"policies"`
}

func deploymentWebDAVScalar(raw json.RawMessage, out any) bool {
	return len(raw) > 0 && string(raw) != "null" && json.Unmarshal(raw, out) == nil
}
func deploymentWebDAVID(value string) bool { return parentMutationID(value) && utf8.ValidString(value) }
func decodeDeploymentWebDAVSnapshot(raw string, enforce bool) error {
	if raw == "" || len(raw) > 65536 || !utf8.ValidString(raw) {
		return ErrDeploymentWebDAVUnavailable
	}
	var data deploymentWebDAVWire
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&data) != nil {
		return ErrDeploymentWebDAVUnavailable
	}
	if decoder.Decode(new(any)) != io.EOF || data.Version == nil || *data.Version != 1 || data.Fence == nil || *data.Fence == "" || data.Tables == nil || data.Accounts == nil || data.Profiles == nil || data.Policies == nil || len(data.Accounts) > 256 || len(data.Profiles) > 4096 || len(data.Policies) > 4096 {
		return ErrDeploymentWebDAVUnavailable
	}
	var revision uint64
	if json.Unmarshal([]byte(*data.Fence), &revision) != nil || revision == 0 {
		return ErrDeploymentWebDAVUnavailable
	}
	expected := append([]string(nil), deploymentWebDAVTables...)
	expected = append(expected, "regional_policy_revision_seq")
	sort.Strings(expected)
	if len(data.Tables) != len(expected) {
		return ErrDeploymentWebDAVUnavailable
	}
	schema := ""
	for n, row := range data.Tables {
		if len(row) != 9 {
			return ErrDeploymentWebDAVUnavailable
		}
		var name, ns, oid, kind, persistence string
		var partition, rls, force, inherit bool
		if !deploymentWebDAVScalar(row[0], &name) || name != expected[n] || !deploymentWebDAVScalar(row[1], &ns) || ns == "" || len(ns) > 256 || schema != "" && schema != ns || !deploymentWebDAVScalar(row[2], &oid) || !deploymentWebDAVScalar(row[3], &kind) || !deploymentWebDAVScalar(row[4], &persistence) || !deploymentWebDAVScalar(row[5], &partition) || !deploymentWebDAVScalar(row[6], &rls) || !deploymentWebDAVScalar(row[7], &force) || !deploymentWebDAVScalar(row[8], &inherit) || partition || rls || force || inherit || persistence != "p" {
			return ErrDeploymentWebDAVUnavailable
		}
		var identity uint32
		if json.Unmarshal([]byte(oid), &identity) != nil || identity == 0 {
			return ErrDeploymentWebDAVUnavailable
		}
		expectedKind := "r"
		if name == "regional_policy_revision_seq" {
			expectedKind = "S"
		}
		if kind != expectedKind {
			return ErrDeploymentWebDAVUnavailable
		}
		schema = ns
	}
	accounts := make(map[string]bool, len(data.Accounts))
	previous := ""
	for _, row := range data.Accounts {
		var id, physical string
		if len(row) != 2 || !deploymentWebDAVScalar(row[0], &id) || !deploymentWebDAVID(id) || previous != "" && id <= previous || !deploymentWebDAVScalar(row[1], &physical) || !deploymentWebDAVPhysical.MatchString(physical) {
			return ErrDeploymentWebDAVUnavailable
		}
		accounts[id] = true
		previous = id
	}
	profiles := make(map[string]bool, len(data.Profiles))
	previous = ""
	unsupported := false
	for _, row := range data.Profiles {
		var id, account, physical, version string
		var kids, empty bool
		var length int
		var generation int64
		if len(row) != 8 || !deploymentWebDAVScalar(row[0], &id) || !deploymentWebDAVID(id) || previous != "" && id <= previous || !deploymentWebDAVScalar(row[1], &account) || !accounts[account] || !deploymentWebDAVScalar(row[2], &kids) || !deploymentWebDAVScalar(row[3], &empty) || !deploymentWebDAVScalar(row[4], &length) || length < 0 || length > 12 || empty && length != 0 || !empty && length == 0 || !deploymentWebDAVScalar(row[5], &physical) || !deploymentWebDAVPhysical.MatchString(physical) || !deploymentWebDAVScalar(row[6], &generation) || generation < 0 || !deploymentWebDAVScalar(row[7], &version) || !deploymentWebDAVPhysical.MatchString(version) {
			return ErrDeploymentWebDAVUnavailable
		}
		if kids || enforce && !empty {
			unsupported = true
		}
		profiles[id] = true
		previous = id
	}
	previous = ""
	for _, row := range data.Policies {
		var id, physical string
		if len(row) != 2 || !deploymentWebDAVScalar(row[0], &id) || !profiles[id] || previous != "" && id <= previous || !deploymentWebDAVScalar(row[1], &physical) || !deploymentWebDAVPhysical.MatchString(physical) {
			return ErrDeploymentWebDAVUnavailable
		}
		previous = id
	}
	if unsupported {
		return ErrDeploymentWebDAVUnsupported
	}
	return nil
}
