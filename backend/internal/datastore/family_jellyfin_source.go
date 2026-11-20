package datastore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Info is copied PG catalogue metadata, not upstream server identity, current
// membership, immutable bytes or permission to make an HTTP request.
type FamilyJellyfinSourceInfo struct {
	ItemID, LibraryID, IntegrationID              string
	PrimaryGUID, BackingGUID, ExternalLibraryGUID string
	FileName, LibraryType                         string
	Size                                          int64
}

func (FamilyJellyfinSourceInfo) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (FamilyJellyfinSourceInfo) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyJellyfinSourceInfo(metadata)"))
}

type familyJellyfinSnapshot struct {
	info     FamilyJellyfinSourceInfo
	revision string
}

func (ds *DataStore) FamilyRemoteRepositoryReady(repo RemoteMediaRepository) bool {
	if ds == nil || ds.pool == nil {
		return false
	}
	actual, ok := repo.(*pgRemoteMediaRepo)
	return ok && actual != nil && actual.pool == ds.pool
}

type FamilyJellyfinSource struct{ *familyJellyfinSourceState }
type familyJellyfinSourceState struct {
	mu             sync.Mutex
	self           *FamilyJellyfinSource
	owner          *FamilyMediaOwner
	snapshot       familyJellyfinSnapshot
	ctx            context.Context
	cancel         context.CancelFunc
	ready, done    chan struct{}
	closed, opened bool
	openDone       chan struct{}
	retire         func()
	retired        <-chan struct{}
	once           sync.Once
	checks         sync.WaitGroup
}

func (FamilyJellyfinSource) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyJellyfinSource) UnmarshalJSON([]byte) error  { return ErrFamilyMediaChanged }
func (FamilyJellyfinSource) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyJellyfinSource(opaque)"))
}
func (s *FamilyJellyfinSource) valid() bool {
	return s != nil && s.familyJellyfinSourceState != nil && s.self == s && s.owner.valid()
}
func (s *FamilyJellyfinSource) Context() context.Context {
	if !s.valid() {
		return familyMediaClosedContext()
	}
	return s.ctx
}
func (s *FamilyJellyfinSource) ExpiresAt() time.Time {
	if !s.valid() {
		return time.Time{}
	}
	return s.owner.ExpiresAt()
}
func (s *FamilyJellyfinSource) ReadyForStore(expected *DataStore) bool {
	return s.valid() && expected != nil && expected.pool != nil && s.owner.store == expected
}

func (s *FamilyJellyfinSource) Invalidate() {
	if s.valid() {
		s.cancel()
	}
}

func (o *FamilyMediaOwner) CaptureJellyfinSource(ctx context.Context, item string) (source *FamilyJellyfinSource, err error) {
	if !o.valid() || ctx == nil || ctx.Err() != nil || !parentMutationID(item) || len(item) > 128 {
		return nil, ErrFamilyMediaChanged
	}
	o.mu.Lock()
	if o.closed || o.ctx.Err() != nil || o.source != nil {
		o.mu.Unlock()
		return nil, ErrFamilyMediaBusy
	}
	life, cancel := context.WithCancel(o.ctx)
	value := &FamilyJellyfinSource{&familyJellyfinSourceState{owner: o, ctx: life, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{})}}
	value.self = value
	o.source = value
	o.mu.Unlock()
	defer func() {
		close(value.ready)
		if source == nil {
			value.Close()
		}
	}()
	// Bind source cancellation before even the preliminary owner's PG reads.
	// The callback only cancels this operation; deferred stop joins it.
	bounded, stop := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer stop()
	join := familyMediaAfterFunc(life, stop)
	defer join()
	if err = o.Check(bounded); err != nil {
		return nil, err
	}
	// Ordinary remote playback uses the actor's own selected profile, even for
	// master. A provider integration ID never substitutes for that account.
	if o.target != "" && o.snapshot.profile.AccountID != o.original.AccountID {
		return nil, ErrFamilyMediaChanged
	}
	value.snapshot, err = o.store.readFamilyJellyfinSnapshot(bounded, o, item)
	if err != nil || life.Err() != nil || bounded.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	if err = o.Check(bounded); err != nil || life.Err() != nil || bounded.Err() != nil {
		return nil, ErrFamilyMediaChanged
	}
	return value, nil
}

func (s *FamilyJellyfinSource) Check(ctx context.Context) error {
	if !s.valid() || ctx == nil || ctx.Err() != nil || s.ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return ErrFamilyMediaChanged
	}
	// Add and Close's transition are serialized. Every actual Check/Info PG
	// operation remains charged, even before an acquisition has been reserved.
	s.checks.Add(1)
	s.mu.Unlock()
	defer s.checks.Done()
	bounded, cancel := context.WithTimeout(ctx, familyMediaCheckBudget)
	defer cancel()
	join := familyMediaAfterFunc(s.ctx, cancel)
	defer join()
	fail := func() error {
		if ctx.Err() == nil {
			s.cancel()
		}
		return ErrFamilyMediaChanged
	}
	if s.owner.Check(bounded) != nil {
		return fail()
	}
	now, err := s.owner.store.readFamilyJellyfinSnapshot(bounded, s.owner, s.snapshot.info.ItemID)
	if err != nil || now.revision != s.snapshot.revision || bounded.Err() != nil || s.ctx.Err() != nil {
		return fail()
	}
	if s.owner.Check(bounded) != nil || bounded.Err() != nil || s.ctx.Err() != nil || ctx.Err() != nil {
		return fail()
	}
	return nil
}

func (s *FamilyJellyfinSource) Info(ctx context.Context) (FamilyJellyfinSourceInfo, error) {
	if s.Check(ctx) != nil {
		return FamilyJellyfinSourceInfo{}, ErrFamilyMediaChanged
	}
	return s.snapshot.info, nil
}

func (s *FamilyJellyfinSource) BeginOpen(ctx context.Context) (func(), error) {
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
	// Check itself links the source before every preliminary PG operation.
	if err := s.Check(ctx); err != nil || ctx.Err() != nil || s.ctx.Err() != nil {
		finish()
		return nil, ErrFamilyMediaChanged
	}
	return finish, nil
}

func (s *FamilyJellyfinSource) BindRetirement(retire func(), done <-chan struct{}) error {
	if !s.valid() || retire == nil || done == nil {
		return ErrFamilyMediaChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.opened || s.retire != nil || s.ctx.Err() != nil {
		return ErrFamilyMediaChanged
	}
	s.retire, s.retired = retire, done
	return nil
}

func (s *FamilyJellyfinSource) Close() {
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
		s.checks.Wait()
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

// Accept only ordinary N/D GUID syntax, never URNs/braces/whitespace. Empty
// GUID is not a concrete resource. Normalization is syntax, not source repair.
func familyJellyfinGUID(value string) (string, bool) {
	if len(value) == 36 {
		if value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
			return "", false
		}
		value = value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	}
	if len(value) != 32 {
		return "", false
	}
	var decoded [16]byte
	if _, err := hex.Decode(decoded[:], []byte(value)); err != nil || decoded == [16]byte{} {
		return "", false
	}
	return hex.EncodeToString(decoded[:]), true
}

func (ds *DataStore) readFamilyJellyfinSnapshot(ctx context.Context, owner *FamilyMediaOwner, item string) (familyJellyfinSnapshot, error) {
	var out familyJellyfinSnapshot
	fence, err := ds.familyLocalFence(ctx)
	if err != nil {
		return out, err
	}
	var primary, backing, libraryGUID, itemType, provider string
	var itemVersion, libraryVersion, itemRelation, libraryRelation, policyVersion string
	var accounts, profiles []byte
	var mode *string
	var missing, badGrants, validRelations bool
	var accountCount, profileCount int
	err = ds.pool.QueryRow(ctx, `WITH selected AS (
 SELECT i.id,i.library_id,l.account_id,i.external_item_id,i.external_media_id,l.external_library_id,
 i.file_name,i.size_bytes,i.is_missing,i.library_type AS item_type,l.library_type,l.provider,
 concat(i.tableoid::oid,':',i.xmin::text,':',i.ctid::text) AS iv,concat(l.tableoid::oid,':',l.xmin::text,':',l.ctid::text) AS lv,
 concat(ic.oid::text,':',ic.relfilenode::text,':',ic.relkind,':',ic.relpersistence) AS ir,
 concat(lc.oid::text,':',lc.relfilenode::text,':',lc.relkind,':',lc.relpersistence) AS lr,
 (ic.relkind='r' AND ic.relpersistence='p' AND lc.relkind='r' AND lc.relpersistence='p') AS valid_relations,
 p.access_mode,CASE WHEN p.library_id IS NULL THEN '' ELSE concat(p.tableoid::oid,':',p.xmin::text,':',p.ctid::text) END AS pv
 FROM remote_media_items i JOIN remote_media_libraries l ON l.id=i.library_id
 JOIN pg_class ic ON ic.oid=i.tableoid AND ic.oid=to_regclass('remote_media_items')
 JOIN pg_class lc ON lc.oid=l.tableoid AND lc.oid=to_regclass('remote_media_libraries')
 LEFT JOIN media_library_access_policies p ON p.library_id=l.id WHERE i.id=$1
 ), ag AS (SELECT g.account_id AS id,concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text) AS v FROM media_library_account_grants g JOIN selected s ON s.library_id=g.library_id ORDER BY g.account_id LIMIT 129),
 pg AS (SELECT g.profile_id AS id,concat(g.tableoid::oid,':',g.xmin::text,':',g.ctid::text) AS v FROM media_library_profile_grants g JOIN selected s ON s.library_id=g.library_id ORDER BY g.profile_id LIMIT 129)
 SELECT CASE WHEN octet_length(id)<=128 THEN id ELSE NULL END,CASE WHEN octet_length(library_id)<=256 THEN library_id ELSE NULL END,
 CASE WHEN octet_length(account_id)<=128 THEN account_id ELSE NULL END,
 CASE WHEN octet_length(external_item_id)<=36 THEN external_item_id ELSE NULL END,
 CASE WHEN octet_length(external_media_id)<=36 THEN external_media_id ELSE NULL END,
 CASE WHEN octet_length(external_library_id)<=36 THEN external_library_id ELSE NULL END,
 CASE WHEN octet_length(file_name)<=1024 THEN file_name ELSE NULL END,size_bytes,is_missing,
 CASE WHEN octet_length(item_type)<=16 THEN item_type ELSE NULL END,CASE WHEN octet_length(library_type)<=16 THEN library_type ELSE NULL END,
		CASE WHEN octet_length(provider)<=16 THEN provider ELSE NULL END,iv,lv,ir,lr,valid_relations,
		CASE WHEN octet_length(access_mode)<=16 THEN access_mode ELSE NULL END,pv,(SELECT count(*) FROM ag),(SELECT count(*) FROM pg),
 (EXISTS(SELECT 1 FROM ag WHERE octet_length(id)>256) OR EXISTS(SELECT 1 FROM pg WHERE octet_length(id)>256)),
 COALESCE((SELECT jsonb_agg(jsonb_build_array(CASE WHEN octet_length(id)<=256 THEN id ELSE '' END,v) ORDER BY id) FROM ag),'[]'::jsonb),
 COALESCE((SELECT jsonb_agg(jsonb_build_array(CASE WHEN octet_length(id)<=256 THEN id ELSE '' END,v) ORDER BY id) FROM pg),'[]'::jsonb)
 FROM selected`, item).Scan(&out.info.ItemID, &out.info.LibraryID, &out.info.IntegrationID, &primary, &backing, &libraryGUID,
		&out.info.FileName, &out.info.Size, &missing, &itemType, &out.info.LibraryType, &provider,
		&itemVersion, &libraryVersion, &itemRelation, &libraryRelation, &validRelations, &mode, &policyVersion,
		&accountCount, &profileCount, &badGrants, &accounts, &profiles)
	if err != nil {
		if err == pgx.ErrNoRows {
			return out, ErrFamilyMediaChanged
		}
		return out, ErrFamilyMediaUnavailable
	}
	if out.info.ItemID != item || out.info.LibraryID == "" || strings.TrimSpace(out.info.IntegrationID) == "" || strings.TrimSpace(out.info.IntegrationID) != out.info.IntegrationID || out.info.FileName == "" || out.info.Size <= 0 || out.info.Size > 8<<30 || missing || !validRelations || provider != "jellyfin" || itemType != out.info.LibraryType || (itemType != "movie" && itemType != "show") || badGrants || accountCount > 128 || profileCount > 128 || len(accounts)+len(profiles) > 128<<10 || mode == nil && policyVersion != "" {
		return familyJellyfinSnapshot{}, ErrFamilyMediaChanged
	}
	var valid bool
	if out.info.PrimaryGUID, valid = familyJellyfinGUID(primary); !valid {
		return familyJellyfinSnapshot{}, ErrFamilyMediaChanged
	}
	if out.info.BackingGUID, valid = familyJellyfinGUID(backing); !valid {
		return familyJellyfinSnapshot{}, ErrFamilyMediaChanged
	}
	if out.info.ExternalLibraryGUID, valid = familyJellyfinGUID(libraryGUID); !valid {
		return familyJellyfinSnapshot{}, ErrFamilyMediaChanged
	}
	var ag, pg [][2]string
	if json.Unmarshal(accounts, &ag) != nil || json.Unmarshal(profiles, &pg) != nil || len(ag) != accountCount || len(pg) != profileCount {
		return familyJellyfinSnapshot{}, ErrFamilyMediaUnavailable
	}
	access := "restricted"
	if mode != nil {
		access = *mode
	}
	if access != "all" && access != "restricted" {
		return familyJellyfinSnapshot{}, ErrFamilyMediaUnavailable
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
	allowed := owner.original.IsMaster && owner.target == "" && owner.Mode() == FamilyMediaInactive || access == "all" ||
		len(allowedAccounts)+len(allowedProfiles) == 0 && owner.original.IsMaster || allowedAccounts[strings.TrimSpace(owner.original.AccountID)] || allowedProfiles[strings.TrimSpace(owner.target)]
	if !allowed {
		return familyJellyfinSnapshot{}, ErrFamilyMediaChanged
	}
	after, err := ds.familyLocalFence(ctx)
	if err != nil || after != fence || ctx.Err() != nil {
		return familyJellyfinSnapshot{}, ErrFamilyMediaChanged
	}
	out.revision = regionalHash([]any{out.info.ItemID, out.info.LibraryID, out.info.IntegrationID,
		out.info.PrimaryGUID, out.info.BackingGUID, out.info.ExternalLibraryGUID, out.info.FileName, out.info.LibraryType, out.info.Size,
		primary, backing, libraryGUID, itemVersion, libraryVersion, itemRelation, libraryRelation, mode, policyVersion,
		json.RawMessage(accounts), json.RawMessage(profiles), fence})
	return out, nil
}
