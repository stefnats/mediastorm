package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/database"
	"novastream/internal/importer"
)

var ErrSABLibraryPublicationNotFound = errors.New("publication record not found")

// Validation describes only selector syntax. Capture independently parses the
// same original bytes and establishes every genuine current authority check.
func ValidateSABLibraryPublicationSelection(raw []byte) error {
	_, _, e := sabPublicationSelection(raw)
	return e
}

// Views describe an operational result. They contain no source path, receipt,
// title identity or reusable file/actor grant.
type SABLibraryPublicationFileView struct {
	Ordinal     int    `json:"ordinal"`
	ItemID      string `json:"itemId,omitempty"`
	Bytes       int64  `json:"bytes"`
	Container   string `json:"container,omitempty"`
	MatchStatus string `json:"matchStatus,omitempty"`
}
type SABLibraryPublicationView struct {
	Reference string                          `json:"reference"`
	State     string                          `json:"state"`
	Bytes     int64                           `json:"bytes"`
	Files     []SABLibraryPublicationFileView `json:"files,omitempty"`
}
type SABLibraryPublicationSelectedFile struct {
	Ordinal int
	Bytes   int64
}
type SABLibraryPublicationRead struct {
	self   *SABLibraryPublicationRead
	owner  *SABLibraryPublicationIntent
	record database.SABPublicationRecord
	view   SABLibraryPublicationView
}

func (i *SABLibraryPublicationIssuer) Context() context.Context {
	if i.ready() {
		return i.ctx
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (p *SABLibraryPublicationIntent) Files() []SABLibraryPublicationSelectedFile {
	if !p.valid() {
		return nil
	}
	s := p.state
	s.mu.Lock()
	actor := s.stage
	s.mu.Unlock()
	if actor == nil {
		return nil
	}
	stage := actor.Stage()
	var out []SABLibraryPublicationSelectedFile
	for _, ordinal := range s.ordinals {
		if ordinal < 1 || ordinal > len(stage.Files) {
			return nil
		}
		out = append(out, SABLibraryPublicationSelectedFile{ordinal, stage.Files[ordinal-1].Bytes})
	}
	return out
}
func (p *SABLibraryPublicationIntent) Copy(r *http.Request, formats []importer.SABPublicationFormat) (*importer.SABPublicationCopy, error) {
	if !p.valid() || p.Check(r) != nil {
		return nil, ErrSABLibraryPublicationChanged
	}
	record, e := p.Reserve(r)
	if e != nil {
		return nil, e
	}
	s := p.state
	_, finish, e := s.begin(r)
	if e != nil {
		return nil, e
	}
	defer finish()
	completion, e := s.stage.Copy(s.ctx, record, formats)
	if e != nil {
		return nil, e
	}
	if p.Check(r) != nil {
		completion.Close()
		return nil, ErrSABLibraryPublicationUnknown
	}
	return completion, nil
}
func publicationItemID(operation string, ordinal int) string {
	return "sab-" + operation + "-" + strconv.Itoa(ordinal)
}
func publicationFileName(ordinal int, container string) (string, error) {
	ext := ""
	switch container {
	case "matroska":
		ext = "mkv"
	case "mov":
		ext = "mp4"
	case "mpegts":
		ext = "ts"
	default:
		return "", ErrSABLibraryPublicationChanged
	}
	return fmt.Sprintf("data-%04d.%s", ordinal, ext), nil
}
func publicationOriginalBinding(s *sabLibraryPublicationState) string {
	raw, _ := json.Marshal([]any{s.snapshot, s.frame.sessionRow.version, s.frame.actorRow.version, s.binding, s.payloadDigest, s.queueID, s.library, s.ordinals})
	digest := sha256.Sum256(raw)
	clear(raw)
	return hex.EncodeToString(digest[:])
}

type sabPublicationRegistrationTx struct {
	progress           *settingsProgressTx
	fence              *config.SABPublicationPolicyFence
	schema, root, kind string
}

var publicationRegistrationTables = []string{"accounts", "local_media_items", "local_media_libraries", "media_library_account_grants", "media_library_access_policies", "media_library_profile_grants", "sab_library_publication_files", "sab_library_publications", "sessions", "users"}

func publicationRegistrationRelations(ctx context.Context, tx pgx.Tx) ([]deploymentWebDAVRelation, error) {
	names := append([]string(nil), publicationRegistrationTables...)
	names = append(names, "regional_policy_revision_seq")
	sort.Strings(names)
	var out []deploymentWebDAVRelation
	schema := ""
	for _, name := range names {
		value := deploymentWebDAVRelation{name: name}
		e := tx.QueryRow(ctx, `SELECT n.nspname,c.oid,c.relkind::text,c.relpersistence::text,c.relispartition,c.relrowsecurity,c.relforcerowsecurity,EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent=c.oid OR i.inhrelid=c.oid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, name).Scan(&value.schema, &value.oid, &value.kind, &value.persistence, &value.partition, &value.rls, &value.force, &value.inherit)
		kind := "r"
		if name == "regional_policy_revision_seq" {
			kind = "S"
		}
		if e != nil || value.schema == "" || value.oid == 0 || value.kind != kind || value.persistence != "p" || value.partition || value.rls || value.force || value.inherit || schema != "" && schema != value.schema {
			return nil, ErrSABLibraryPublicationUnavailable
		}
		schema = value.schema
		out = append(out, value)
	}
	return out, nil
}
func (s *sabLibraryPublicationState) registrationTx(ctx context.Context) (result *sabPublicationRegistrationTx, err error) {
	progress, e := s.issuer.store.beginSettingsProgressTx(ctx, s.issuer.coordinator)
	if e != nil {
		return nil, sabPublicationError(e)
	}
	defer func() {
		if result == nil {
			_ = finishShareCreationTransaction(progress)
		}
	}()
	relations, e := publicationRegistrationRelations(progress.ctx, progress.tx)
	if e != nil {
		return nil, e
	}
	schema := relations[0].schema
	table := func(name string) string { return pgx.Identifier{schema, name}.Sanitize() }
	var found string
	// Established original actor/session locks precede every new relation guard.
	if e = progress.tx.QueryRow(progress.ctx, "SELECT id FROM "+table("accounts")+" WHERE id=$1 FOR UPDATE", s.frame.session.AccountID).Scan(&found); e != nil {
		return nil, sabPublicationError(e)
	}
	if e = progress.tx.QueryRow(progress.ctx, "SELECT token FROM "+table("sessions")+" WHERE token=$1 FOR UPDATE", s.frame.session.Token).Scan(&found); e != nil {
		return nil, sabPublicationError(e)
	}
	// Item insertion's original-library FK reuses this already-held row lock.
	// No first foreign-key row lock is introduced after the late table guards.
	if e = progress.tx.QueryRow(progress.ctx, "SELECT id FROM "+table("local_media_libraries")+" WHERE id=$1 FOR KEY SHARE NOWAIT", s.library).Scan(&found); e != nil {
		return nil, sabPublicationError(e)
	}
	for _, name := range publicationRegistrationTables {
		mode := "SHARE"
		if name == "local_media_items" || name == "sab_library_publication_files" || name == "sab_library_publications" {
			mode = "SHARE ROW EXCLUSIVE"
		}
		if _, e = progress.tx.Exec(progress.ctx, "LOCK TABLE "+table(name)+" IN "+mode+" MODE NOWAIT"); e != nil {
			return nil, sabPublicationError(e)
		}
	}
	again, e := publicationRegistrationRelations(progress.ctx, progress.tx)
	if e != nil || !reflect.DeepEqual(again, relations) {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	fence, e := s.issuer.security.BindSABPublicationPolicy(progress.ctx, s.policy, progress.hold)
	if e != nil {
		return nil, ErrSABLibraryPublicationChanged
	}
	values, e := fence.Values()
	if e != nil || values != s.values {
		return nil, ErrSABLibraryPublicationChanged
	}
	var sessionVersion, actorVersion string
	var current bool
	e = progress.tx.QueryRow(progress.ctx, `SELECT concat(s.tableoid::oid,':',s.xmin::text,':',s.ctid::text),concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),a.is_master AND s.scope='' AND COALESCE(s.scope_resource,'')='' AND s.expires_at>clock_timestamp() AND (a.expires_at IS NULL OR a.expires_at>clock_timestamp()) FROM `+table("sessions")+` s JOIN `+table("accounts")+` a ON a.id=s.account_id WHERE s.token=$1 AND a.id=$2`, s.frame.session.Token, s.frame.session.AccountID).Scan(&sessionVersion, &actorVersion, &current)
	if e != nil || !current || sessionVersion != s.frame.sessionRow.version || actorVersion != s.frame.actorRow.version {
		return nil, ErrSABLibraryPublicationChanged
	}
	var wire *string
	if e = progress.tx.QueryRow(progress.ctx, sabPublicationSnapshotQuery(schema), s.library).Scan(&wire); e != nil || wire == nil || *wire != s.snapshot {
		return nil, ErrSABLibraryPublicationChanged
	}
	var scanning bool
	if e = progress.tx.QueryRow(progress.ctx, "SELECT last_scan_status='scanning' FROM "+table("local_media_libraries")+" WHERE id=$1", s.library).Scan(&scanning); e != nil || scanning {
		return nil, ErrSABLibraryPublicationBusy
	}
	var snapshot struct {
		Library struct {
			Roots []string
			Type  string
		}
	}
	if json.Unmarshal([]byte(*wire), &snapshot) != nil || len(snapshot.Library.Roots) < 1 || len(snapshot.Library.Roots) > 64 {
		return nil, ErrSABLibraryPublicationChanged
	}
	if s.current(s.request) != nil {
		return nil, ErrSABLibraryPublicationChanged
	}
	return &sabPublicationRegistrationTx{progress, fence, schema, snapshot.Library.Roots[0], snapshot.Library.Type}, nil
}
func (t *sabPublicationRegistrationTx) current(s *sabLibraryPublicationState, physical *importer.SABPublicationCopyConsumption) error {
	values, e := t.fence.Values()
	if e != nil || values != s.values || t.progress.ctx.Err() != nil || s.current(s.request) != nil {
		return ErrSABLibraryPublicationChanged
	}
	if physical != nil && physical.CheckPhysical(s.ctx) != nil {
		return ErrSABLibraryPublicationChanged
	}
	return nil
}
func (t *sabPublicationRegistrationTx) project(s *sabLibraryPublicationState, record database.SABPublicationRecord, data *importer.SABPublicationCopyData, fresh bool) (SABLibraryPublicationView, error) {
	table := func(name string) string { return pgx.Identifier{t.schema, name}.Sanitize() }
	tx := t.progress.tx
	ctx := t.progress.ctx
	var state, binding string
	var matches bool
	selection, _ := json.Marshal(s.ordinals)
	e := tx.QueryRow(ctx, `SELECT state,original_binding,stage_operation_id=$2 AND stage_namespace=$3 AND manifest_digest=$4 AND target_library_id=$5 AND target_binding=$6 AND selected_ordinals=$7::jsonb AND expected_bytes=$8 FROM `+table("sab_library_publications")+` WHERE operation_id=$1`, record.OperationID, record.StageOperationID, record.StageNamespace, record.ManifestDigest, s.library, record.TargetBinding, selection, record.Bytes).Scan(&state, &binding, &matches)
	if errors.Is(e, pgx.ErrNoRows) {
		return SABLibraryPublicationView{}, ErrSABLibraryPublicationUnknown
	}
	if e != nil {
		return SABLibraryPublicationView{}, sabPublicationError(e)
	}
	if !matches || len(binding) != 64 || !fresh && binding != publicationOriginalBinding(s) {
		return SABLibraryPublicationView{}, ErrSABLibraryPublicationChanged
	}
	view := SABLibraryPublicationView{Reference: record.OperationID, State: state, Bytes: record.Bytes}
	if state == "reserved" {
		return view, nil
	}
	if state != "registered" {
		return view, ErrSABLibraryPublicationChanged
	}
	if data != nil {
		var same bool
		e = tx.QueryRow(ctx, "SELECT destination_receipt=$2::jsonb AND destination_receipt_digest=$3 FROM "+table("sab_library_publications")+" WHERE operation_id=$1", record.OperationID, data.Receipt, data.ReceiptDigest).Scan(&same)
		if e != nil || !same {
			return view, ErrSABLibraryPublicationChanged
		}
	}
	rows, e := tx.Query(ctx, `SELECT f.ordinal,f.item_id,f.relative_path,f.size_bytes,f.sha256,i.file_path,i.match_status FROM `+table("sab_library_publication_files")+` f JOIN `+table("local_media_items")+` i ON i.id=f.item_id AND i.library_id=$2 AND i.relative_path=f.relative_path AND i.size_bytes=f.size_bytes WHERE f.operation_id=$1 ORDER BY f.ordinal LIMIT 129`, record.OperationID, s.library)
	if e != nil {
		return view, e
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var ordinal int
		var item, relative, hash, path, status string
		var size int64
		if e = rows.Scan(&ordinal, &item, &relative, &size, &hash, &path, &status); e != nil {
			return view, e
		}
		if index >= len(s.ordinals) || ordinal != s.ordinals[index] || item != publicationItemID(record.OperationID, ordinal) || relative != filepath.Join(".mediastorm-publications", record.OperationID, fmt.Sprintf("data-%04d.bin", ordinal)) || path != filepath.Join(t.root, relative) || size <= 0 || size > 8<<30 || len(hash) != 64 {
			return view, ErrSABLibraryPublicationChanged
		}
		if status != "unmatched" && status != "matched" && status != "manual" && status != "low_confidence" {
			return view, ErrSABLibraryPublicationChanged
		}
		file := SABLibraryPublicationFileView{Ordinal: ordinal, ItemID: item, Bytes: size, MatchStatus: status}
		if data != nil {
			if index >= len(data.Files) || data.Files[index].Ordinal != ordinal || data.Files[index].Bytes != size || data.Files[index].SHA256 != hash {
				return view, ErrSABLibraryPublicationChanged
			}
			file.Container = data.Files[index].Container
		}
		view.Files = append(view.Files, file)
		index++
	}
	if e = rows.Err(); e != nil || index != len(s.ordinals) {
		return SABLibraryPublicationView{}, ErrSABLibraryPublicationChanged
	}
	return view, nil
}
func (t *sabPublicationRegistrationTx) insert(s *sabLibraryPublicationState, record database.SABPublicationRecord, data importer.SABPublicationCopyData) error {
	if len(data.Files) != len(s.ordinals) || len(data.Receipt) == 0 || len(data.Receipt) > 65536 {
		return ErrSABLibraryPublicationChanged
	}
	table := func(name string) string { return pgx.Identifier{t.schema, name}.Sanitize() }
	tx := t.progress.tx
	ctx := t.progress.ctx
	for i, file := range data.Files {
		if file.Ordinal != s.ordinals[i] || file.RelativePath != filepath.Join(".mediastorm-publications", record.OperationID, fmt.Sprintf("data-%04d.bin", file.Ordinal)) || file.Bytes <= 0 || file.Bytes > 8<<30 || len(file.SHA256) != 64 {
			return ErrSABLibraryPublicationChanged
		}
		name, e := publicationFileName(file.Ordinal, file.Container)
		if e != nil {
			return e
		}
		id := publicationItemID(record.OperationID, file.Ordinal)
		_, e = tx.Exec(ctx, "INSERT INTO "+table("local_media_items")+`(id,library_id,relative_path,file_path,file_name,library_type,confidence,match_status,size_bytes,modified_at)VALUES($1,$2,$3,$4,$5,$6,0,'unmatched',$7,$8)`, id, s.library, file.RelativePath, filepath.Join(t.root, file.RelativePath), name, t.kind, file.Bytes, file.ModifiedAt)
		if e != nil {
			return sabPublicationError(e)
		}
		_, e = tx.Exec(ctx, "INSERT INTO "+table("sab_library_publication_files")+`(operation_id,ordinal,item_id,relative_path,size_bytes,sha256)VALUES($1,$2,$3,$4,$5,$6)`, record.OperationID, file.Ordinal, id, file.RelativePath, file.Bytes, file.SHA256)
		if e != nil {
			return sabPublicationError(e)
		}
	}
	result, e := tx.Exec(ctx, "UPDATE "+table("sab_library_publications")+` SET state='registered',destination_receipt=$2::jsonb,destination_receipt_digest=$3 WHERE operation_id=$1 AND state='reserved'`, record.OperationID, data.Receipt, data.ReceiptDigest)
	if e != nil || result.RowsAffected() != 1 {
		return ErrSABLibraryPublicationChanged
	}
	return nil
}
func (p *SABLibraryPublicationIntent) register(r *http.Request, completion *importer.SABPublicationCopy, fresh bool) (view SABLibraryPublicationView, err error) {
	if !p.valid() {
		return view, ErrSABLibraryPublicationChanged
	}
	s := p.state
	_, operationDone, e := s.begin(r)
	if e != nil {
		return view, e
	}
	defer operationDone()
	if p.Check(r) != nil {
		return view, ErrSABLibraryPublicationChanged
	}
	record, e := s.issuer.source.FindSABPublication(s.ctx, s.queueID)
	if e != nil || record == nil || record.StageOperationID != s.stage.Stage().OperationID || record.LibraryID != s.library || !reflect.DeepEqual(record.Ordinals, s.ordinals) {
		return view, ErrSABLibraryPublicationChanged
	}
	physical, e := completion.Consume(s.ctx, s.stage, record.OperationID)
	if e != nil {
		return view, ErrSABLibraryPublicationChanged
	}
	defer physical.Close()
	if physical.Check(s.ctx) != nil {
		return view, ErrSABLibraryPublicationChanged
	}
	data := physical.Data()
	if data.OperationID != record.OperationID {
		return view, ErrSABLibraryPublicationChanged
	}
	ctx, finish, e := s.begin(r)
	if e != nil {
		return view, e
	}
	defer finish()
	guard, e := s.registrationTx(ctx)
	if e != nil {
		return view, e
	}
	committed := false
	defer func() {
		cleanup := finishShareCreationTransaction(guard.progress)
		if cleanup != nil && err == nil {
			err = ErrSABLibraryPublicationUnavailable
		}
		if err == nil && (p.Check(r) != nil || physical.Check(s.ctx) != nil) {
			err = ErrSABLibraryPublicationUnknown
		}
		if err != nil {
			view = SABLibraryPublicationView{}
			if committed {
				err = ErrSABLibraryPublicationUnknown
			}
		} else if e := physical.RecordCatalogueState(s.ctx, "registered"); e != nil {
			view = SABLibraryPublicationView{}
			err = ErrSABLibraryPublicationUnknown
		}
	}()
	view, e = guard.project(s, *record, &data, fresh)
	if e != nil {
		return view, e
	}
	if view.State == "reserved" {
		if e = guard.insert(s, *record, data); e != nil {
			return view, e
		}
		view, e = guard.project(s, *record, &data, fresh)
		if e != nil {
			return view, e
		}
	}
	if e = guard.current(s, physical); e != nil {
		return view, e
	}
	if e = guard.progress.tx.Commit(guard.progress.ctx); e != nil {
		return view, ErrSABLibraryPublicationUnknown
	}
	committed = true
	if e = guard.current(s, physical); e != nil {
		return view, ErrSABLibraryPublicationUnknown
	}
	return view, nil
}
func (p *SABLibraryPublicationIntent) Register(r *http.Request, completion *importer.SABPublicationCopy) (SABLibraryPublicationView, error) {
	return p.register(r, completion, false)
}
func (p *SABLibraryPublicationIntent) CheckCopy(r *http.Request, completion *importer.SABPublicationCopy) error {
	if !p.valid() || p.Check(r) != nil {
		return ErrSABLibraryPublicationChanged
	}
	s := p.state
	_, finish, e := s.begin(r)
	if e != nil {
		return e
	}
	defer finish()
	if completion.CheckOriginal(s.ctx, s.stage) != nil || s.current(r) != nil {
		return ErrSABLibraryPublicationChanged
	}
	return nil
}
func (p *SABLibraryPublicationIntent) CheckPublished(r *http.Request) error {
	if !p.valid() || p.Check(r) != nil {
		return ErrSABLibraryPublicationChanged
	}
	s := p.state
	_, finish, e := s.begin(r)
	if e != nil {
		return e
	}
	defer finish()
	if s.stage.CheckCopied(s.ctx) != nil || s.current(r) != nil {
		return ErrSABLibraryPublicationChanged
	}
	return nil
}
func (p *SABLibraryPublicationIntent) Reconcile(r *http.Request) (SABLibraryPublicationView, error) {
	if !p.valid() {
		return SABLibraryPublicationView{}, ErrSABLibraryPublicationChanged
	}
	s := p.state
	_, operationDone, e := s.begin(r)
	if e != nil {
		return SABLibraryPublicationView{}, e
	}
	defer operationDone()
	if p.Check(r) != nil {
		return SABLibraryPublicationView{}, ErrSABLibraryPublicationChanged
	}
	record, e := s.issuer.source.FindSABPublication(s.ctx, s.queueID)
	if e != nil || record == nil || record.LibraryID != s.library || record.StageOperationID != s.stage.Stage().OperationID || !reflect.DeepEqual(record.Ordinals, s.ordinals) {
		return SABLibraryPublicationView{}, ErrSABLibraryPublicationChanged
	}
	_, finish, e := s.begin(r)
	if e != nil {
		return SABLibraryPublicationView{}, e
	}
	defer finish()
	completion, e := s.stage.ReopenCopy(s.ctx, record.OperationID)
	if e != nil {
		return SABLibraryPublicationView{}, e
	}
	view, e := p.register(r, completion, true)
	if e != nil {
		completion.Close()
	}
	return view, e
}

// Status has a distinct original GET owner. It never clones GET into POST,
// opens a staged file or exposes the private mutation intent to its caller.
func (i *SABLibraryPublicationIssuer) ReadStatus(r *http.Request, queueID int64) (result *SABLibraryPublicationRead, err error) {
	if !i.ready() || r == nil || r.URL == nil || r.Method != http.MethodGet || r.URL.RawQuery != "" || queueID <= 0 {
		return nil, ErrSABLibraryPublicationChanged
	}
	binding, e := deploymentWebDAVBinding(r)
	if e != nil {
		return nil, e
	}
	expiry, ok := r.Context().Deadline()
	if !ok || !expiry.After(time.Now()) || expiry.After(time.Now().Add(5*time.Minute)) {
		return nil, ErrSABLibraryPublicationChanged
	}
	frame, _ := r.Context().Value(pgRequestFrameKey{}).(*pgRequestFrame)
	session, ok := auth.GetSession(r)
	if frame == nil || frame.self != frame || frame.store != i.store || frame.ctx.Err() != nil || !ok || session != effectivePinFrameSession(frame) || auth.GetAccountID(r) != frame.session.AccountID || !frame.session.IsMaster || !auth.IsMaster(r) || frame.session.Scope != "" || frame.session.ScopeResource != "" {
		return nil, ErrSABLibraryPublicationChanged
	}
	if frame.session.ExpiresAt.Before(expiry) {
		expiry = frame.session.ExpiresAt
	}
	if frame.actorExpiry != nil && frame.actorExpiry.Before(expiry) {
		expiry = *frame.actorExpiry
	}
	life, cancel := context.WithDeadline(r.Context(), expiry)
	s := &sabLibraryPublicationState{issuer: i, request: r, original: r.Context(), body: r.Body, binding: binding, frame: frame, ctx: life, cancel: cancel, expiry: expiry, queueID: queueID, done: make(chan struct{})}
	p := &SABLibraryPublicationIntent{state: s}
	s.self = p
	s.stop = publicationSignalLink(i.ctx, cancel)
	i.mu.Lock()
	if i.closed || i.admitted >= 64 || len(i.intents) >= 64 {
		i.mu.Unlock()
		s.stop()
		cancel()
		return nil, ErrSABLibraryPublicationBusy
	}
	i.admitted++
	i.intents[p] = struct{}{}
	i.pending.Add(1)
	i.mu.Unlock()
	defer func() { i.mu.Lock(); i.admitted--; i.mu.Unlock(); i.pending.Done() }()
	defer func() {
		if result == nil {
			p.Close()
		}
	}()
	s.policy, e = i.security.CaptureSABPublicationPolicy(s.ctx)
	if e != nil {
		return nil, e
	}
	record, e := i.source.FindSABPublication(s.ctx, queueID)
	if e != nil {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	if record == nil {
		return nil, ErrSABLibraryPublicationNotFound
	}
	s.library = record.LibraryID
	s.ordinals = append([]int(nil), record.Ordinals...)
	ctx, finish, e := s.begin(r)
	if e != nil {
		return nil, e
	}
	snapshot, values, _, e := s.readCurrent(ctx, false, database.SABPublicationRecord{})
	finish()
	if e != nil {
		return nil, e
	}
	s.snapshot = snapshot
	s.values = values
	value := &SABLibraryPublicationRead{owner: p, record: *record}
	value.self = value
	value.view, e = value.project(r)
	if e != nil {
		return nil, e
	}
	return value, nil
}
func (v *SABLibraryPublicationRead) valid() bool { return v != nil && v.self == v && v.owner.valid() }
func (v *SABLibraryPublicationRead) project(r *http.Request) (view SABLibraryPublicationView, err error) {
	if !v.valid() {
		return view, ErrSABLibraryPublicationChanged
	}
	s := v.owner.state
	ctx, finish, e := s.begin(r)
	if e != nil {
		return view, e
	}
	defer finish()
	guard, e := s.registrationTx(ctx)
	if e != nil {
		return view, e
	}
	defer func() {
		if e := finishShareCreationTransaction(guard.progress); err == nil && e != nil {
			err = ErrSABLibraryPublicationUnavailable
		}
		if err == nil && s.current(r) != nil {
			err = ErrSABLibraryPublicationChanged
		}
		if err != nil {
			view = SABLibraryPublicationView{}
		}
	}()
	view, e = guard.project(s, v.record, nil, true)
	if e != nil {
		return view, e
	}
	if view.State == "reserved" {
		view.State = v.record.State
		if view.State == "registered" {
			view.State = "registration_unknown"
		}
	}
	if e = guard.current(s, nil); e != nil {
		return view, e
	}
	return view, nil
}
func (v *SABLibraryPublicationRead) Check(r *http.Request) error {
	if !v.valid() {
		return ErrSABLibraryPublicationChanged
	}
	_, operationDone, e := v.owner.state.begin(r)
	if e != nil {
		return e
	}
	defer operationDone()
	view, e := v.project(r)
	if e != nil || !reflect.DeepEqual(view, v.view) {
		return ErrSABLibraryPublicationChanged
	}
	record, e := v.owner.state.issuer.source.FindSABPublication(v.owner.Context(), v.owner.state.queueID)
	if e != nil || record == nil || !reflect.DeepEqual(*record, v.record) {
		return ErrSABLibraryPublicationChanged
	}
	return nil
}
func (v *SABLibraryPublicationRead) View() SABLibraryPublicationView {
	if !v.valid() {
		return SABLibraryPublicationView{}
	}
	out := v.view
	out.Files = append([]SABLibraryPublicationFileView(nil), out.Files...)
	return out
}
func (v *SABLibraryPublicationRead) Context() context.Context {
	if v.valid() {
		return v.owner.Context()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
func (v *SABLibraryPublicationRead) ExpiresAt() time.Time {
	if v.valid() {
		return v.owner.ExpiresAt()
	}
	return time.Time{}
}
func (v *SABLibraryPublicationRead) Close() {
	if v.valid() {
		v.owner.Close()
	}
}

// This is saved exclusion DATA, never source authority. The scanner still has
// to correlate the exact configured root and actual immutable owner marker.
func (ds *DataStore) SABPublicationScanReceipt(ctx context.Context, library, binding string) ([]byte, error) {
	if ds == nil || ds.pool == nil || ctx == nil || ctx.Err() != nil || len(library) > 256 || len(binding) != 64 {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	work, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var raw *string
	e := ds.pool.QueryRow(work, `SELECT CASE WHEN octet_length(destination_receipt::text)<=65536 THEN destination_receipt::text END FROM sab_library_publications WHERE target_library_id=$1 AND target_binding=$2 AND state='registered' ORDER BY operation_id LIMIT 1`, library, binding).Scan(&raw)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, nil
	}
	if e != nil || raw == nil || len(*raw) > 65536 {
		return nil, ErrSABLibraryPublicationUnavailable
	}
	return []byte(*raw), nil
}
