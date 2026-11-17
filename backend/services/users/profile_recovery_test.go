package users

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"novastream/internal/datastore"
)

func userRecoverySnapshot(t *testing.T, s *Service, ctx context.Context, id string) (*datastore.ProfileRecoveryRead, *StorageRecoveryTicket, *datastore.ProfileRecoverySnapshot) {
	t.Helper()
	read, e := s.store.PrepareProfileRecovery(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(read.Close)
	ticket, e := s.PrepareStorageRecovery(ctx, read)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(ticket.Close)
	snapshot, e := s.store.ReadProfileRecovery(read)
	if e != nil {
		t.Fatal(e)
	}
	return read, ticket, snapshot
}
func TestProfileRecoveryUsersActualUnknownSingleAndBatch(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			f := usersPG(t)
			original := f.service(t, f.ds)
			a := original.ListAll()[0].ID
			b, e := original.Create("B")
			if e != nil {
				t.Fatal(e)
			}
			uri, proxy := usersLostCommitProxy(t, f)
			ds, e := datastore.New(t.Context(), uri, datastore.WithMaxConns(2), datastore.WithMinConns(1))
			if e != nil {
				t.Fatal("own proxy datastore")
			}
			t.Cleanup(ds.Close)
			s := f.service(t, ds)
			proxy.drop.Store(true)
			s.mu.Lock()
			value := s.users[a]
			value.Name = "Accepted A"
			value.UpdatedAt = time.Now().UTC()
			s.users[a] = value
			if batch {
				value = s.users[b.ID]
				value.Name = "Accepted B"
				value.UpdatedAt = time.Now().UTC()
				s.users[b.ID] = value
			}
			e = s.saveLocked()
			s.mu.Unlock()
			if !errors.Is(e, ErrPersistenceUncertain) || !proxy.dropped.Load() {
				t.Fatal("actual lost acknowledgement not quarantined", e)
			}
			if dbUser(t, f, a).Name != "Accepted A" {
				t.Fatal("independent commit confirmation absent")
			}
			beforeVersion := usersVersion(t, f, a)
			ctx := t.Context()
			_, ticket, snapshot := userRecoverySnapshot(t, s, ctx, a)
			if e = s.ValidateStorageRecovery(ctx, ticket, snapshot); e != nil {
				t.Fatal(e)
			}
			status, e := s.InstallStorageRecovery(ctx, ticket, snapshot)
			if e != nil || !status.TargetPresent || status.PendingUncertainty != batch {
				t.Fatal("target marker removal changed unrelated quarantine", e)
			}
			if got, _ := s.Get(a); got.Name != "Accepted A" {
				t.Fatal("actual recovery did not adopt saved target")
			}
			if usersVersion(t, f, a) != beforeVersion {
				t.Fatal("recovery wrote user row")
			}
			if batch {
				if _, e = s.Rename(a, "Blocked A"); !errors.Is(e, ErrPersistenceUncertain) {
					t.Fatal("global fail-closed gate was relaxed")
				}
				if dbUser(t, f, b.ID).Name != "Accepted B" {
					t.Fatal("batch control did not commit")
				}
				_, ticket, snapshot = userRecoverySnapshot(t, s, ctx, b.ID)
				status, e = s.InstallStorageRecovery(ctx, ticket, snapshot)
				if e != nil || status.PendingUncertainty {
					t.Fatal("remaining exact target not reconciled", e)
				}
			}
			if _, e = s.Rename(a, "Explicit new change"); e != nil {
				t.Fatal("subsequent explicit operation failed", e)
			}
		})
	}
}
func TestProfileRecoveryUsersPreservesForeignDirtyAndUnassignedStop(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	a := s.ListAll()[0].ID
	b, e := s.Create("B")
	if e != nil {
		t.Fatal(e)
	}
	f.exec(t, `UPDATE users SET name='External A',updated_at=now() WHERE id=$1`, a)
	s.mu.Lock()
	dirty := s.users[b.ID]
	dirty.Name = "Pending B"
	s.users[b.ID] = dirty
	s.persistence.uncertain = true
	s.mu.Unlock()
	ctx := t.Context()
	_, ticket, snapshot := userRecoverySnapshot(t, s, ctx, a)
	status, e := s.InstallStorageRecovery(ctx, ticket, snapshot)
	if e != nil || !status.PendingUncertainty {
		t.Fatal("unassigned global uncertainty cleared", e)
	}
	s.mu.RLock()
	got := s.users[b.ID]
	confirmed := s.persistence.confirmed[b.ID]
	s.mu.RUnlock()
	if got.Name != "Pending B" || confirmed.Name != "B" {
		t.Fatal("foreign dirty target was replaced")
	}
	if _, e = s.Rename(a, "Still blocked"); !errors.Is(e, ErrPersistenceUncertain) {
		t.Fatal("old global stop was lost")
	}
}
func TestProfileRecoveryUsersNewMarkerAndLocalChangeRejectOldTicket(t *testing.T) {
	for _, kind := range []string{"successor-marker", "known-local-write", "dirty-local-write"} {
		t.Run(kind, func(t *testing.T) {
			f := usersPG(t)
			s := f.service(t, f.ds)
			id := s.ListAll()[0].ID
			if kind == "successor-marker" {
				s.mu.Lock()
				s.markStorageUnknownLocked([]string{id})
				s.mu.Unlock()
			}
			ctx := t.Context()
			_, ticket, snapshot := userRecoverySnapshot(t, s, ctx, id)
			switch kind {
			case "successor-marker":
				s.mu.Lock()
				s.markStorageUnknownLocked([]string{id})
				s.mu.Unlock()
			case "known-local-write":
				if _, e := s.Rename(id, "New local acknowledgement"); e != nil {
					t.Fatal(e)
				}
			case "dirty-local-write":
				s.mu.Lock()
				value := s.users[id]
				value.Name = "Pending local"
				s.users[id] = value
				s.mu.Unlock()
			}
			if e := s.ValidateStorageRecovery(ctx, ticket, snapshot); !errors.Is(e, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("old validation accepted replacement target", e)
			}
			if _, e := s.InstallStorageRecovery(ctx, ticket, snapshot); !errors.Is(e, datastore.ErrProfileRecoveryChanged) {
				t.Fatal("old install accepted replacement target", e)
			}
			if kind == "successor-marker" && !s.persistence.uncertain {
				t.Fatal("new marker was cleared")
			}
		})
	}
}
func TestProfileRecoveryUsersAbsenceAndExplicitReappearance(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	id := s.ListAll()[0].ID
	if _, e := s.Create("Survivor"); e != nil {
		t.Fatal(e)
	}
	f.exec(t, `DELETE FROM users WHERE id=$1`, id)
	ctx := t.Context()
	_, ticket, snapshot := userRecoverySnapshot(t, s, ctx, id)
	status, e := s.InstallStorageRecovery(ctx, ticket, snapshot)
	if e != nil || status.TargetPresent {
		t.Fatal("confirmed absence was not installed", e)
	}
	if _, ok := s.Get(id); ok || len(s.ListAll()) != 1 {
		t.Fatal("absence bootstrapped or retained a profile")
	}
	f.exec(t, `INSERT INTO users(id,account_id,name) VALUES($1,'master','Recreated')`, id)
	if _, ok := s.Get(id); ok {
		t.Fatal("recreated ID was silently adopted")
	}
	if _, e = s.Rename(id, "Unexpected replacement"); !errors.Is(e, ErrUserNotFound) {
		t.Fatal("normal setter adopted recreated ID", e)
	}
	_, ticket, snapshot = userRecoverySnapshot(t, s, ctx, id)
	status, e = s.InstallStorageRecovery(ctx, ticket, snapshot)
	if e != nil || !status.TargetPresent {
		t.Fatal("explicit new recovery did not load new instance", e)
	}
}
func TestProfileRecoveryUsersInvalidAndHistoricalTickets(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	id := s.ListAll()[0].ID
	ctx := t.Context()
	read, ticket, snapshot := userRecoverySnapshot(t, s, ctx, id)
	copy := *ticket
	for _, candidate := range []*StorageRecoveryTicket{nil, {}, &copy} {
		if e := s.ValidateStorageRecovery(ctx, candidate, snapshot); !errors.Is(e, datastore.ErrProfileRecoveryChanged) {
			t.Fatal("forged ticket accepted", e)
		}
	}
	if e := s.ValidateStorageRecovery(context.Background(), ticket, snapshot); !errors.Is(e, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("replacement context accepted")
	}
	f.exec(t, `UPDATE users SET name='Later DB value' WHERE id=$1`, id)
	before, _ := s.Get(id)
	status, e := s.InstallStorageRecovery(ctx, ticket, snapshot)
	if e != nil || !status.TargetPresent {
		t.Fatal(e)
	}
	after, _ := s.Get(id)
	if !reflect.DeepEqual(after, before) || dbUser(t, f, id).Name != "Later DB value" {
		t.Fatal("historical acknowledgement silently reread or wrote DB")
	}
	if _, e = s.InstallStorageRecovery(ctx, ticket, snapshot); !errors.Is(e, datastore.ErrProfileRecoveryChanged) {
		t.Fatal("ticket replay")
	}
	read.Close()
	if _, e = s.Rename(id, "Stale historical write"); !errors.Is(e, datastore.ErrUserWriteConflict) {
		t.Fatal("historical snapshot became current write authority", e)
	}
}
