package users

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
)

func TestUsersPersistenceActualParentCommitSurvivesUnrelatedSave(t *testing.T) {
	f := usersPG(t)
	s := f.service(t, f.ds)
	target := mustProfile(t, s, "Parent target")
	other := mustProfile(t, s, "Other")
	hash, e := bcrypt.GenerateFromPassword([]byte("synthetic parent password"), bcrypt.MinCost)
	if e != nil {
		t.Fatal(e)
	}
	f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id='master'`, string(hash))
	now := time.Now().UTC().Truncate(time.Microsecond)
	session := models.Session{Token: "synthetic-session-for-storage-test", AccountID: "master", IsMaster: true, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	f.exec(t, `INSERT INTO sessions(token,account_id,is_master,created_at,expires_at)VALUES($1,$2,true,$3,$4)`, session.Token, session.AccountID, session.CreatedAt, session.ExpiresAt)
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/uninstalled-parent-seam", nil)
	ctx := context.WithValue(r.Context(), auth.ContextKeySession, session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
	intent, e := f.ds.PrepareKidsProfileChange(r.WithContext(ctx), target.ID, true)
	if e != nil {
		t.Fatal(e)
	}
	defer intent.Close()
	proof, e := intent.VerifyCredential("synthetic parent password")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.ds.CommitKidsProfileChange(intent, proof); e != nil {
		t.Fatal(e)
	}
	revision := usersVersion(t, f, target.ID)
	if _, e = s.Rename(other.ID, "Unrelated acknowledged rename"); e != nil {
		t.Fatal(e)
	}
	got := dbUser(t, f, target.ID)
	if !got.IsKidsProfile || got.KidsMode != "" || usersVersion(t, f, target.ID) != revision {
		t.Fatal("actual flag-only parent commit overwritten")
	}
	if _, e = s.Rename(target.ID, "Stale same-target rename"); !errors.Is(e, datastore.ErrUserWriteConflict) {
		t.Fatal("same stale target merged instead of conflict")
	}
}
func TestUsersPersistenceReturnedSlicesDoNotMutateCacheOrBaseline(t *testing.T) {
	s, e := NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	id := s.ListAll()[0].ID
	if _, e = s.SetKidsAllowedLists(id, []string{"original"}); e != nil {
		t.Fatal(e)
	}
	for _, set := range []func(string, string) (models.User, error){s.SetMdblistAccountID, s.SetTraktAccountID, s.SetSimklAccountID, s.SetScrobAccountID, s.SetPlexAccountID} {
		u, e := set(id, "synthetic-linked")
		if e != nil {
			t.Fatal(e)
		}
		u.KidsAllowedLists[0] = "mutated setter result"
	}
	for _, get := range []func(string) []models.User{s.GetUsersByMdblistAccountID, s.GetUsersByTraktAccountID, s.GetUsersBySimklAccountID, s.GetUsersByScrobAccountID, s.GetUsersByPlexAccountID} {
		rows := get("synthetic-linked")
		if len(rows) != 1 {
			t.Fatal("fixture link")
		}
		rows[0].KidsAllowedLists[0] = "mutated reader result"
	}
	duplicate, e := s.AddKidsAllowedList(id, "original")
	if e != nil {
		t.Fatal(e)
	}
	duplicate.KidsAllowedLists[0] = "mutated early-return"
	got, _ := s.Get(id)
	if got.KidsAllowedLists[0] != "original" || s.persistence.confirmed[id].KidsAllowedLists[0] != "original" {
		t.Fatal("public mutable alias reached cache/baseline")
	}
	// Direct staging inside the service lock cannot mutate the private baseline.
	s.mu.Lock()
	staged := s.users[id]
	staged.KidsAllowedLists[0] = "staged change"
	s.users[id] = staged
	if s.persistence.confirmed[id].KidsAllowedLists[0] != "original" {
		t.Fatal("baseline aliases staged cache")
	}
	s.users = cloneUsers(s.persistence.confirmed)
	s.mu.Unlock()
}
func TestUsersPersistenceJSONAllMutationFailureRollback(t *testing.T) {
	for _, mode := range []string{"rename", "delete", "kids", "lists", "add", "remove", "pin", "clear pin", "reassign", "live permissions"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s, e := NewService(dir)
			if e != nil {
				t.Fatal(e)
			}
			u := mustProfile(t, s, "Existing")
			if _, e = s.SetKidsAllowedLists(u.ID, []string{"keep"}); e != nil {
				t.Fatal(e)
			}
			if _, e = s.SetPin(u.ID, "1234"); e != nil {
				t.Fatal(e)
			}
			before := cloneUsers(s.users)
			rawBefore, e := os.ReadFile(s.path)
			if e != nil {
				t.Fatal(e)
			}
			if e = os.Mkdir(s.path+".tmp", 0700); e != nil {
				t.Fatal(e)
			}
			switch mode {
			case "rename":
				_, e = s.Rename(u.ID, "Change")
			case "delete":
				e = s.Delete(u.ID)
			case "kids":
				_, e = s.SetKidsProfile(u.ID, true)
			case "lists":
				_, e = s.SetKidsAllowedLists(u.ID, []string{"change"})
			case "add":
				_, e = s.AddKidsAllowedList(u.ID, "new")
			case "remove":
				_, e = s.RemoveKidsAllowedList(u.ID, "keep")
			case "pin":
				_, e = s.SetPin(u.ID, "9876")
			case "clear pin":
				_, e = s.ClearPin(u.ID)
			case "reassign":
				_, e = s.Reassign(u.ID, "other")
			case "live permissions":
				_, e = s.SetLivePermissions(u.ID, true, true)
			}
			if e == nil || !reflect.DeepEqual(before, s.users) {
				t.Fatal("failed mutation changed confirmed cache")
			}
			rawAfter, e := os.ReadFile(s.path)
			if e != nil || string(rawBefore) != string(rawAfter) {
				t.Fatal("failed rename changed JSON file")
			}
		})
	}
}
func TestUsersPersistenceJSONMigrationAndNoDatabaseFallback(t *testing.T) {
	dir := t.TempDir()
	raw := `[{"id":"legacy","name":"Legacy","isKidsProfile":true,"kidsMode":"both","kidsAllowedLists":["stored"],"pinHash":"synthetic opaque old hash"}]`
	if e := os.WriteFile(filepath.Join(dir, "users.json"), []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	s, e := NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	u, _ := s.Get("legacy")
	if u.AccountID != models.DefaultAccountID || u.KidsMode != "rating" || u.ActivityPrivacy != models.ActivityPrivacyNotShared || u.PinHash != "synthetic opaque old hash" || u.CreatedAt.IsZero() || u.UpdatedAt.IsZero() {
		t.Fatal("legacy JSON migration changed")
	}
	rawAfter, e := os.ReadFile(s.path)
	if e != nil {
		t.Fatal(e)
	}
	var disk []map[string]any
	if e = json.Unmarshal(rawAfter, &disk); e != nil || disk[0]["pinHash"] != "synthetic opaque old hash" || disk[0]["accountId"] != models.DefaultAccountID {
		t.Fatal("migrated JSON not persisted")
	}
	f := usersPG(t)
	dbs := f.service(t, f.ds)
	dbid := dbs.ListAll()[0].ID
	// A valid legacy JSON file in the media directory cannot become a DB fallback.
	if e = os.WriteFile(filepath.Join(dbs.storageDir, "users.json"), []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	f.ds.Close()
	if _, e = dbs.Rename(dbid, "DB unavailable"); e == nil {
		t.Fatal("closed DB save succeeded")
	}
	if dbs.Exists("legacy") || dbs.ListAll()[0].Name == "DB unavailable" {
		t.Fatal("DB error fell back or published")
	}
	if e = dbs.ResyncStorage(t.Context()); e == nil {
		t.Fatal("closed DB resync succeeded")
	}
}
func TestUsersPersistenceConcurrentOwnedReaderCopies(t *testing.T) {
	s, e := NewService(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	id := s.ListAll()[0].ID
	if _, e = s.SetKidsAllowedLists(id, []string{"kept"}); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for n := 0; n < 4; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 30; n++ {
				u, _ := s.Get(id)
				u.KidsAllowedLists[0] = "caller"
				rows := s.ListAll()
				rows[0].KidsAllowedLists[0] = "caller"
			}
		}()
	}
	for n := 0; n < 8; n++ {
		if _, e = s.SetColor(id, "#123456"); e != nil {
			t.Fatal(e)
		}
	}
	wg.Wait()
	u, _ := s.Get(id)
	if u.KidsAllowedLists[0] != "kept" {
		t.Fatal("reader alias corrupted concurrent writer")
	}
}

func TestUsersPersistenceJSONPreservesStoredTimeOffset(t *testing.T) {
	dir := t.TempDir()
	raw := `[{"id":"offset","accountId":"default","name":"Offset","activityPrivacy":"not_shared","createdAt":"2026-01-01T12:30:00+01:00","updatedAt":"2026-01-01T12:30:00+01:00"}]`
	if e := os.WriteFile(filepath.Join(dir, "users.json"), []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	s, e := NewService(dir)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Rename("offset", "Changed"); e != nil {
		t.Fatal(e)
	}
	bytes, e := os.ReadFile(s.path)
	if e != nil {
		t.Fatal(e)
	}
	var rows []struct {
		CreatedAt string `json:"createdAt"`
	}
	if e = json.Unmarshal(bytes, &rows); e != nil || rows[0].CreatedAt != "2026-01-01T12:30:00+01:00" {
		t.Fatal("copy changed legacy stored time offset")
	}
}
