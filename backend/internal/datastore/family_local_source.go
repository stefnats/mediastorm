package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// This copied metadata is never sufficient to open a file. The original opaque
// source, same-store check and one-shot physical opener remain mandatory.
type FamilyLocalSourceInfo struct {
	ItemID, LibraryID, FilePath, FileName string
	Roots                                 []string
	Size                                  int64
}

func (FamilyLocalSourceInfo) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (FamilyLocalSourceInfo) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyLocalSourceInfo(metadata)"))
}

type familyLocalSnapshot struct {
	info     FamilyLocalSourceInfo
	revision string
}

// The repository factory returns new wrappers, so readiness compares their
// private actual SQL pool rather than public wrapper pointer identity.
func (ds *DataStore) FamilyLocalRepositoryReady(repo LocalMediaRepository) bool {
	if ds == nil || ds.pool == nil {
		return false
	}
	actual, ok := repo.(*pgLocalMediaRepo)
	return ok && actual != nil && actual.pool == ds.pool
}

type FamilyLocalSource struct{ *familyLocalSourceState }
type familyLocalSourceState struct {
	mu             sync.Mutex
	self           *FamilyLocalSource
	owner          *FamilyMediaOwner
	snapshot       familyLocalSnapshot
	ctx            context.Context
	cancel         context.CancelFunc
	ready, done    chan struct{}
	closed, opened bool
	openDone       chan struct{}
	retire         func()
	retired        <-chan struct{}
	once           sync.Once
}

func (FamilyLocalSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyLocalSource) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyLocalSource) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyLocalSource(opaque)"))
}
func (s *FamilyLocalSource) valid() bool {
	return s != nil && s.familyLocalSourceState != nil && s.self == s && s.owner.valid()
}
func (s *FamilyLocalSource) Context() context.Context {
	if !s.valid() {
		return familyMediaClosedContext()
	}
	return s.ctx
}
func (s *FamilyLocalSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	return s.owner.ExpiresAt()
}

// Invalidate signals retirement only. Actual Close remains owned by the
// original lifecycle and joins acquisition/FD cleanup outside read callbacks.
func (s *FamilyLocalSource) Invalidate() {
	if s.valid() {
		s.cancel()
	}
}
func (s *FamilyLocalSource) ReadyForStore(expected *DataStore) bool {
	return s.valid() && expected != nil && expected.pool != nil && s.owner.store == expected
}
func (o *FamilyMediaOwner) CaptureLocalSource(ctx context.Context, item string) (source *FamilyLocalSource, err error) {
	if !o.valid() || ctx == nil || ctx.Err() != nil || !parentMutationID(item) || len(item) > 128 {
		return nil, ErrFamilyMediaChanged
	}
	o.mu.Lock()
	if o.closed || o.ctx.Err() != nil || o.source != nil {
		o.mu.Unlock()
		return nil, ErrFamilyMediaBusy
	}
	life, cancel := context.WithCancel(o.ctx)
	value := &FamilyLocalSource{&familyLocalSourceState{owner: o, ctx: life, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}}
	value.self = value
	o.source = value
	o.mu.Unlock()
	defer func() {
		close(value.ready)
		if source == nil {
			value.Close()
		}
	}()
	if err = o.Check(ctx); err != nil {
		return nil, err
	}
	// Ordinary LOCAL access never borrows a master-selected foreign profile.
	if o.target != "" && o.snapshot.profile.AccountID != o.original.AccountID {
		return nil, ErrFamilyMediaChanged
	}
	bounded, stop := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer stop()
	join := familyMediaAfterFunc(life, stop)
	defer join()
	value.snapshot, err = o.store.readFamilyLocalSnapshot(bounded, o, item)
	if err != nil || life.Err() != nil || bounded.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	if err = o.Check(bounded); err != nil {
		return nil, err
	}
	return value, nil
}
func (s *FamilyLocalSource) Check(ctx context.Context) error {
	if !s.valid() || ctx == nil || ctx.Err() != nil || s.ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	if s.owner.Check(ctx) != nil {
		return ErrFamilyMediaChanged
	}
	bounded, cancel := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer cancel()
	join := familyMediaAfterFunc(s.ctx, cancel)
	defer join()
	now, err := s.owner.store.readFamilyLocalSnapshot(bounded, s.owner, s.snapshot.info.ItemID)
	if err != nil || now.revision != s.snapshot.revision || bounded.Err() != nil || s.owner.Check(bounded) != nil {
		if ctx.Err() == nil {
			s.cancel()
		}
		return ErrFamilyMediaChanged
	}
	return nil
}
func (s *FamilyLocalSource) Info(ctx context.Context) (FamilyLocalSourceInfo, error) {
	if s.Check(ctx) != nil {
		return FamilyLocalSourceInfo{}, ErrFamilyMediaChanged
	}
	info := s.snapshot.info
	info.Roots = append([]string{}, info.Roots...)
	return info, nil
}

// BeginOpen reserves this original source's sole physical acquisition. It is
// not reset by failure or Close, and its completion must cover late cleanup.
func (s *FamilyLocalSource) BeginOpen(ctx context.Context) (func(), error) {
	if !s.valid() || ctx == nil || ctx.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	s.mu.Lock()
	if s.closed || s.opened || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, ErrFamilyMediaBusy
	}
	s.opened = true
	s.openDone = make(chan struct{})
	done := s.openDone
	var once sync.Once
	finish := func() { once.Do(func() { close(done) }) }
	s.mu.Unlock()
	// Charge the entire acquisition, including its blocking current-PG check.
	bounded, cancel := context.WithCancel(ctx)
	join := familyMediaAfterFunc(s.ctx, cancel)
	err := s.Check(bounded)
	cancel()
	join()
	if err != nil {
		finish()
		return nil, err
	}
	return finish, nil
}
func (s *FamilyLocalSource) BindRetirement(retire func(), done <-chan struct{}) error {
	if !s.valid() || retire == nil || done == nil {
		return ErrFamilyMediaChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.opened || s.retire != nil || s.ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	s.retire = retire
	s.retired = done
	return nil
}
func (s *FamilyLocalSource) Close() {
	if !s.valid() {
		return
	}
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		openDone := s.openDone
		s.mu.Unlock()
		<-s.ready
		if openDone != nil {
			<-openDone
		}
		s.mu.Lock()
		retire, done := s.retire, s.retired
		s.mu.Unlock()
		if retire != nil {
			retire()
		}
		if done != nil {
			<-done
		}
		close(s.done)
	})
}

func regionalFenceBody() string {
	raw, err := embedMigrations.ReadFile("migrations/082_regional_child_policy.sql")
	if err != nil {
		return ""
	}
	text := string(raw)
	begin := strings.Index(text, "AS $$")
	if begin < 0 {
		return ""
	}
	text = text[begin+len("AS $$"):]
	end := strings.Index(text, "$$;")
	if end < 0 {
		return ""
	}
	return text[:end]
}

// Verify the actual enabled schema objects, including physical catalogue
// epochs. Missing/replaced/disabled fences never mean an empty ACL.
func (ds *DataStore) familyLocalFence(ctx context.Context) (string, error) {
	rows, err := ds.pool.Query(ctx, `SELECT t.tgname,
 (t.tgtype=31 AND t.tgenabled='O' AND t.tgnargs=0 AND t.tgqual IS NULL AND NOT t.tgisinternal AND NOT p.prosecdef AND p.prokind='f' AND p.pronargs=0 AND p.prorettype='trigger'::regtype),
 concat(t.tableoid::oid,':',t.xmin::text,':',t.ctid::text),concat(p.tableoid::oid,':',p.xmin::text,':',p.ctid::text),concat(c.oid::text,':',c.relfilenode::text,':',c.relkind,':',c.relpersistence),
 CASE WHEN octet_length(p.prosrc)<=4096 THEN p.prosrc ELSE NULL END,p.proconfig
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_class c ON c.oid=t.tgrelid
 WHERE t.tgfoid=to_regprocedure('observe_regional_policy_revision()')
 AND (t.tgrelid,t.tgname) IN ((to_regclass('media_library_access_policies'),'family_media_acl_policy_fence'),(to_regclass('media_library_account_grants'),'family_media_acl_account_fence'),(to_regclass('media_library_profile_grants'),'family_media_acl_profile_fence'))
 ORDER BY t.tgname LIMIT 4`)
	if err != nil {
		return "", ErrFamilyMediaUnavailable
	}
	defer rows.Close()
	proof := []any{}
	expected := regionalFenceBody()
	if expected == "" {
		return "", ErrFamilyMediaUnavailable
	}
	for rows.Next() {
		var name, tVersion, pVersion, cVersion string
		var valid bool
		var body *string
		var config []string
		if rows.Scan(&name, &valid, &tVersion, &pVersion, &cVersion, &body, &config) != nil || !valid || body == nil || *body != expected || len(config) != 1 || config[0] != "search_path=pg_catalog" {
			return "", ErrFamilyMediaUnavailable
		}
		proof = append(proof, []string{name, tVersion, pVersion, cVersion})
	}
	if rows.Err() != nil || len(proof) != 3 || ctx.Err() != nil {
		return "", ErrFamilyMediaUnavailable
	}
	return regionalHash(proof), nil
}

func (ds *DataStore) readFamilyLocalSnapshot(ctx context.Context, owner *FamilyMediaOwner, item string) (familyLocalSnapshot, error) {
	var out familyLocalSnapshot
	fence, err := ds.familyLocalFence(ctx)
	if err != nil {
		return out, err
	}
	var root, itemVersion, libraryVersion, policyVersion string
	var roots, accounts, profiles []byte
	var mode *string
	var missing, badGrants bool
	var accountCount, profileCount int
	err = ds.pool.QueryRow(ctx, `WITH selected AS (
 SELECT i.id,i.library_id,i.file_path,i.file_name,i.size_bytes,i.is_missing,l.root_path,l.root_paths,
 concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text) AS iv,concat(l.tableoid::oid,':',l.xmin::text,':',l.ctid::text) AS lv,
 p.access_mode,CASE WHEN p.library_id IS NULL THEN '' ELSE concat(p.tableoid::oid,':',p.xmin::text,':',p.ctid::text) END AS pv
 FROM local_media_items i JOIN local_media_libraries l ON l.id=i.library_id LEFT JOIN media_library_access_policies p ON p.library_id=l.id WHERE i.id=$1
 ), ag AS (SELECT g.account_id AS id,concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text) AS v FROM media_library_account_grants g JOIN selected s ON s.library_id=g.library_id ORDER BY g.account_id LIMIT 129),
 pg AS (SELECT g.profile_id AS id,concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text) AS v FROM media_library_profile_grants g JOIN selected s ON s.library_id=g.library_id ORDER BY g.profile_id LIMIT 129)
 SELECT CASE WHEN octet_length(id)<=128 THEN id ELSE NULL END,CASE WHEN octet_length(library_id)<=256 THEN library_id ELSE NULL END,
 CASE WHEN octet_length(file_path)<=4096 THEN file_path ELSE NULL END,CASE WHEN octet_length(file_name)<=1024 THEN file_name ELSE NULL END,size_bytes,is_missing,
 CASE WHEN octet_length(root_path)<=4096 THEN root_path ELSE NULL END,
 CASE WHEN jsonb_typeof(root_paths)='array' THEN CASE WHEN jsonb_array_length(root_paths)<=64 AND octet_length(root_paths::text)<=16384 THEN root_paths ELSE NULL END ELSE NULL END,
 iv,lv,access_mode,pv,(SELECT count(*) FROM ag),(SELECT count(*) FROM pg),
 (EXISTS(SELECT 1 FROM ag WHERE octet_length(id)>256) OR EXISTS(SELECT 1 FROM pg WHERE octet_length(id)>256)),
 COALESCE((SELECT jsonb_agg(jsonb_build_array(CASE WHEN octet_length(id)<=256 THEN id ELSE '' END,v) ORDER BY id) FROM ag),'[]'::jsonb),
 COALESCE((SELECT jsonb_agg(jsonb_build_array(CASE WHEN octet_length(id)<=256 THEN id ELSE '' END,v) ORDER BY id) FROM pg),'[]'::jsonb)
 FROM selected`, item).Scan(&out.info.ItemID, &out.info.LibraryID, &out.info.FilePath, &out.info.FileName, &out.info.Size, &missing, &root, &roots, &itemVersion, &libraryVersion, &mode, &policyVersion, &accountCount, &profileCount, &badGrants, &accounts, &profiles)
	if err != nil {
		if err == pgx.ErrNoRows {
			return out, ErrFamilyMediaChanged
		}
		return out, ErrFamilyMediaUnavailable
	}
	if out.info.ItemID != item || out.info.LibraryID == "" || out.info.FilePath == "" || out.info.FileName == "" || out.info.Size <= 0 || missing || badGrants || accountCount > 128 || profileCount > 128 || len(accounts)+len(profiles) > 128<<10 || json.Unmarshal(roots, &out.info.Roots) != nil {
		return familyLocalSnapshot{}, ErrFamilyMediaChanged
	}
	if len(out.info.Roots) == 0 {
		out.info.Roots = []string{root}
	}
	metadataBytes := len(out.info.FilePath) + len(out.info.FileName)
	for _, value := range out.info.Roots {
		metadataBytes += len(value)
		if len(value) == 0 || len(value) > 4096 {
			return familyLocalSnapshot{}, ErrFamilyMediaChanged
		}
	}
	if len(out.info.Roots) > 64 || metadataBytes > 16<<10 {
		return familyLocalSnapshot{}, ErrFamilyMediaChanged
	}
	var ag, pg [][2]string
	if json.Unmarshal(accounts, &ag) != nil || json.Unmarshal(profiles, &pg) != nil || len(ag) != accountCount || len(pg) != profileCount {
		return familyLocalSnapshot{}, ErrFamilyMediaUnavailable
	}
	access := "restricted"
	if mode != nil {
		access = *mode
	}
	if access != "all" && access != "restricted" {
		return familyLocalSnapshot{}, ErrFamilyMediaUnavailable
	}
	normalize := func(rows [][2]string) map[string]bool {
		out := map[string]bool{}
		for _, row := range rows {
			if id := strings.TrimSpace(row[0]); id != "" {
				out[id] = true
			}
		}
		return out
	}
	allowedAccounts, allowedProfiles := normalize(ag), normalize(pg)
	allowed := owner.original.IsMaster && owner.target == "" && owner.Mode() == FamilyMediaInactive || access == "all" || len(allowedAccounts)+len(allowedProfiles) == 0 && owner.original.IsMaster || allowedAccounts[strings.TrimSpace(owner.original.AccountID)] || allowedProfiles[strings.TrimSpace(owner.target)]
	if !allowed {
		return familyLocalSnapshot{}, ErrFamilyMediaChanged
	}
	after, err := ds.familyLocalFence(ctx)
	if err != nil || after != fence || ctx.Err() != nil {
		return familyLocalSnapshot{}, ErrFamilyMediaChanged
	}
	out.revision = regionalHash([]any{out.info.ItemID, out.info.LibraryID, out.info.FilePath, out.info.FileName, out.info.Roots, out.info.Size, itemVersion, libraryVersion, mode, policyVersion, json.RawMessage(accounts), json.RawMessage(profiles), fence})
	return out, nil
}
