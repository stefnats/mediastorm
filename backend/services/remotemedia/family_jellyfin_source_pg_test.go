package remotemedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/users"
)

type familyJellyfinPGFixture struct {
	ds       *datastore.DataStore
	observer *pgxpool.Pool
	users    *users.Service
	pins     *profileunlock.PGProfilePins
	session  models.Session
	unlock   string
	wire     *familyJellyfinWireFixture
	manager  *config.Manager
	settings config.Settings
	adapter  *FamilyJellyfinSources
	app      string
}

func newFamilyJellyfinPGFixture(t *testing.T, data []byte) *familyJellyfinPGFixture {
	t.Helper()
	raw := os.Getenv("PROFILE_PIN_ISSUANCE_TEST_DATABASE_URL")
	if raw == "" {
		t.Skip("dedicated synthetic PostgreSQL fixture required")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "postgresql" || u.Host != "" || u.User == nil || !strings.HasPrefix(u.User.Username(), "synthetic_") || !strings.HasPrefix(u.Query().Get("host"), "/") {
		t.Fatal("own synthetic Unix-socket PostgreSQL required")
	}
	admin, err := pgxpool.New(t.Context(), raw)
	if err != nil {
		t.Fatal("connect own fixture administrator")
	}
	name := fmt.Sprintf("synthetic_jellyfin_adapter_%d", time.Now().UnixNano())
	if _, err := admin.Exec(t.Context(), "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN"); err != nil {
		admin.Close()
		t.Fatal("create own isolated role")
	}
	var ds *datastore.DataStore
	t.Cleanup(func() {
		if ds != nil {
			ds.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{name}.Sanitize()+" CASCADE"); err != nil {
			t.Error("drop own source schema")
		}
		if _, err := admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Error("drop own source role")
		}
		admin.Close()
	})
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()+" AUTHORIZATION "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("create own isolated schema")
	}
	query := u.Query()
	query.Set("search_path", pgx.Identifier{name}.Sanitize()+",pg_catalog")
	query.Set("options", "-c role="+name)
	query.Set("application_name", name)
	u.RawQuery = query.Encode()
	ds, err = datastore.New(t.Context(), u.String())
	if err != nil {
		t.Fatal("open and migrate own actual source store")
	}
	f := &familyJellyfinPGFixture{ds: ds, observer: admin, app: name, wire: newFamilyJellyfinWireFixture(t, data)}
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic account password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal("hash own account fixture")
	}
	f.exec(t, `INSERT INTO accounts(id,username,password_hash)VALUES('actor','Synthetic Actor',$1),('other','Synthetic Other',$1),('integration','Synthetic Provider Integration',$1)`, string(hash))
	f.exec(t, `INSERT INTO users(id,account_id,name)VALUES('target','actor','Synthetic Target'),('foreign','other','Synthetic Foreign'); INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('child','actor','Synthetic Child',true)`)
	f.exec(t, `INSERT INTO remote_media_libraries(id,name,library_type,provider,account_id,external_library_id)VALUES('remote-library','Synthetic Jellyfin','movie','jellyfin','integration',$1)`, familyJellyfinTestLibrary)
	f.exec(t, `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('remote-library','all')`)
	f.exec(t, `INSERT INTO remote_media_items(id,library_id,external_item_id,external_media_id,group_key,library_type,title,file_name,size_bytes)VALUES('remote-item','remote-library',$1,$2,'synthetic-group','movie','Synthetic Title','synthetic.mkv',$3)`, familyJellyfinTestPrimary, familyJellyfinTestBacking, len(data))
	f.users, err = users.NewServiceWithStore(ds, t.TempDir())
	if err != nil {
		t.Fatal("create actual PG users service")
	}
	if _, err := f.users.SetPin("target", "2580"); err != nil {
		t.Fatal("set actual synthetic profile PIN")
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	f.session = models.Session{Token: "SYNTHETIC_JELLYFIN_ADAPTER_SESSION", AccountID: "actor", ClientID: "synthetic-adapter-client", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := ds.Sessions().Create(t.Context(), &f.session); err != nil {
		t.Fatal("create actual original full session")
	}
	f.pins = profileunlock.NewPGProfilePins(ds)
	request, release, err := f.pins.Authenticate(f.request(t.Context()), f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal("authenticate genuine original PIN request")
	}
	f.unlock, _, err = f.pins.Verify(request, "target", "2580", time.Minute, f.users)
	release()
	if err != nil || f.unlock == "" {
		t.Fatal("verify actual original PG PIN")
	}
	f.adapter, f.manager, f.settings = f.newAdapter(t)
	return f
}

func (f *familyJellyfinPGFixture) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := f.ds.Pool().Exec(t.Context(), sql, args...); err != nil {
		t.Fatal("mutate own synthetic PG fixture")
	}
}
func (f *familyJellyfinPGFixture) request(ctx context.Context) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "http://synthetic.invalid/private-jellyfin", nil).WithContext(ctx)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	ctx = context.WithValue(r.Context(), auth.ContextKeySession, f.session)
	ctx = context.WithValue(ctx, auth.ContextKeyAccountID, f.session.AccountID)
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, f.session.IsMaster)
	return r.WithContext(ctx)
}
func (f *familyJellyfinPGFixture) source(t *testing.T) (*profileunlock.FamilyMediaOwner, *profileunlock.FamilyJellyfinSource) {
	t.Helper()
	r, release, err := f.pins.Authenticate(f.request(t.Context()), f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal("authenticate actual source request")
	}
	r.Header.Set(profileunlock.Header, f.unlock)
	owner, err := f.pins.CaptureFamilyMediaOwner(r, "target")
	if err != nil {
		release()
		t.Fatal("capture genuine original selected adult owner")
	}
	t.Cleanup(owner.Close)
	if owner.Handoff(r) != nil {
		release()
		t.Fatal("handoff actual original full request")
	}
	release()
	source, err := owner.CaptureJellyfinSource(t.Context(), "remote-item")
	if err != nil {
		t.Fatal("capture genuine PG item/library source")
	}
	t.Cleanup(source.Close)
	return owner, source
}
func (f *familyJellyfinPGFixture) newAdapter(t *testing.T) (*FamilyJellyfinSources, *config.Manager, config.Settings) {
	t.Helper()
	settings := config.DefaultSettings()
	settings.Jellyfin.Accounts = []config.JellyfinAccount{{ID: "integration", UserID: familyJellyfinTestUser, Token: familyJellyfinTestToken, ServerURL: f.wire.server.URL + "/original"}}
	raw, err := json.Marshal(settings)
	if err != nil {
		t.Fatal("encode own synthetic integration")
	}
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	if os.WriteFile(manager.ConfigPath(), raw, 0o600) != nil {
		t.Fatal("write own synthetic settings")
	}
	remote, err := NewService(f.ds, manager, nil, nil)
	if err != nil {
		t.Fatal("create genuine remote repository service")
	}
	adapter, err := NewFamilyJellyfinSources(f.ds, manager, remote)
	if err != nil || !adapter.ReadyFor(f.ds, manager) || adapter.ReadyFor(&datastore.DataStore{}, manager) || adapter.ReadyFor(f.ds, config.NewManager(manager.ConfigPath())) {
		t.Fatal("concrete same-store/config readiness changed")
	}
	return adapter, manager, settings
}
func (f *familyJellyfinPGFixture) open(t *testing.T) (*profileunlock.FamilyMediaOwner, *profileunlock.FamilyJellyfinSource, *FamilyJellyfinStream) {
	t.Helper()
	owner, source := f.source(t)
	stream, err := f.adapter.Open(t.Context(), source)
	if err != nil || stream == nil {
		t.Fatal("open genuine PG and managed HTTP held source")
	}
	t.Cleanup(func() { _ = stream.Close() })
	return owner, source, stream
}

func TestFamilyJellyfinSourceActualOriginalFiniteBytesAndFinalChunkStayCurrent(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, []byte("exact final synthetic chunk"))
	owner, source, stream := f.open(t)
	info := stream.Info()
	if !stream.ExpiresAt().Equal(owner.ExpiresAt()) || info.Length != int64(len(f.wire.data)) || info.Container != "mkv" || len(info.ReportedCodecs) != 2 {
		t.Fatal("finite metadata or fixed original deadline changed")
	}
	buffer := make([]byte, len(f.wire.data))
	n, err := stream.Read(buffer)
	if n != len(buffer) || err != nil && err != io.EOF || sha256.Sum256(buffer) != sha256.Sum256(f.wire.data) || stream.Context().Err() != nil || stream.Check(t.Context()) != nil || source.Check(t.Context()) != nil {
		t.Fatal("final nonzero bytes canceled their own following authorization check")
	}
	var published bytes.Buffer
	if _, err := published.Write(buffer); err != nil || !bytes.Equal(published.Bytes(), f.wire.data) || f.wire.downloads.Load() != 1 || f.wire.bad.Load() {
		t.Fatal("final checked outer publication or exact original wire changed")
	}
	if n, err := stream.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatal("finite stream did not reach following zero-byte EOF")
	}
	if source.Check(t.Context()) != nil {
		t.Fatal("normal stream EOF mutated original PG permission")
	}
}

func TestFamilyJellyfinSourceActualNilCheckAndHeldReadBusyDoNotConsumeOrRevoke(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, bytes.Repeat([]byte("held bytes"), 4096))
	f.wire.hold.Store(true)
	_, source, stream := f.open(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if stream.Check(ctx) != nil || f.wire.bodyWrites.Load() != 0 || f.wire.downloads.Load() != 1 {
		t.Fatal("nil current check consumed a body with no available application bytes")
	}
	readDone := make(chan error, 1)
	buffer := make([]byte, 16)
	go func() { _, err := stream.Read(buffer); readDone <- err }()
	deadline := time.Now().Add(time.Second)
	for stream.state.readGate.TryLock() {
		stream.state.readGate.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("real held reader did not acquire consumer gate")
		}
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(stream.Check(t.Context()), profileunlock.ErrFamilyMediaBusy) || stream.Context().Err() != nil || source.Context().Err() != nil {
		t.Fatal("concurrent caller Check retired a healthy held original")
	}
	// Real watcher HTTP requests must progress while the original body is held.
	before := f.wire.items.Load()
	deadline = time.Now().Add(4 * time.Second)
	for f.wire.items.Load() < before+2 {
		if time.Now().After(deadline) {
			t.Fatal("held body read blocked independent current metadata observation")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if f.wire.bodyWrites.Load() != 0 || f.wire.downloads.Load() != 1 || stream.Context().Err() != nil {
		t.Fatal("watcher consumed or reopened held body")
	}
	f.wire.unblock()
	select {
	case err := <-readDone:
		if err != nil || !bytes.Equal(buffer, f.wire.data[:len(buffer)]) || stream.Check(t.Context()) != nil {
			t.Fatal("caller-only busy denial damaged original data or check")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("released actual reader did not finish")
	}
}

func TestFamilyJellyfinSourceActualBufferedReadRejectsPGAndManagedChanges(t *testing.T) {
	for _, change := range []string{"PIN", "profile row", "item row", "library row", "ACL ABA", "missing fence", "same-value Save", "failed Save", "backup replacement"} {
		t.Run(change, func(t *testing.T) {
			f := newFamilyJellyfinPGFixture(t, bytes.Repeat([]byte("prefetched original bytes"), 4096))
			_, _, stream := f.open(t)
			if n, err := stream.Read(make([]byte, 1)); n != 1 || err != nil {
				t.Fatal("initial actual buffered byte did not arrive")
			}
			switch change {
			case "PIN":
				if _, err := f.users.SetPin("target", "8642"); err != nil {
					t.Fatal("change actual original PIN")
				}
			case "profile row":
				f.exec(t, `UPDATE users SET name=name WHERE id='target'`)
			case "item row":
				f.exec(t, `UPDATE remote_media_items SET title=title WHERE id='remote-item'`)
			case "library row":
				f.exec(t, `UPDATE remote_media_libraries SET name=name WHERE id='remote-library'`)
			case "ACL ABA":
				f.exec(t, `INSERT INTO media_library_account_grants(library_id,account_id)VALUES('remote-library','other'); DELETE FROM media_library_account_grants WHERE library_id='remote-library' AND account_id='other'`)
			case "missing fence":
				f.exec(t, `ALTER TABLE media_library_access_policies DISABLE TRIGGER family_media_acl_policy_fence`)
			case "same-value Save":
				if f.manager.Save(f.settings) != nil {
					t.Fatal("actual managed Save failed")
				}
			case "failed Save":
				settings := f.settings
				settings.Server.ExternalBackendURL = "file:///unsupported"
				if f.manager.Save(settings) == nil {
					t.Fatal("invalid managed Save unexpectedly succeeded")
				}
			case "backup replacement":
				staged := f.manager.ConfigPath() + ".staged"
				raw, _ := json.Marshal(f.settings)
				if os.WriteFile(staged, raw, 0o600) != nil || config.ReplaceManagedSettingsFile(staged, f.manager.ConfigPath()) != nil {
					t.Fatal("actual managed backup replacement failed")
				}
			}
			buffer := bytes.Repeat([]byte{0xa5}, 64)
			if n, err := stream.Read(buffer); n != 0 || err == nil || !bytes.Equal(buffer, bytes.Repeat([]byte{0xa5}, 64)) || f.wire.downloads.Load() != 1 {
				t.Fatal("changed original proof released prefetched bytes or reopened media")
			}
		})
	}
}

func TestFamilyJellyfinSourceActualWatcherRetiresUnknownOrChangedCurrentEvidence(t *testing.T) {
	for _, mode := range []string{"download lost", "membership lost", "size changed", "no user", "slow metadata"} {
		t.Run(mode, func(t *testing.T) {
			f := newFamilyJellyfinPGFixture(t, bytes.Repeat([]byte("held original"), 4096))
			f.wire.hold.Store(true)
			_, _, stream := f.open(t)
			f.wire.setMode(mode)
			select {
			case <-stream.Context().Done():
			case <-time.After(7 * time.Second):
				t.Fatal("bounded current evidence failure did not retire original")
			}
			if stream.Check(t.Context()) == nil || f.wire.downloads.Load() != 1 || f.wire.bodyWrites.Load() != 0 {
				t.Fatal("unknown current evidence adopted/reopened media or leaked held bytes")
			}
		})
	}
}

func TestFamilyJellyfinSourceActualOwnerCloseJoinsHeldReadBeforeRefund(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, bytes.Repeat([]byte("held original"), 4096))
	f.wire.hold.Store(true)
	owner, _, stream := f.open(t)
	buffer := bytes.Repeat([]byte{0xb6}, 64)
	readDone := make(chan error, 1)
	go func() { _, err := stream.Read(buffer); readDone <- err }()
	deadline := time.Now().Add(time.Second)
	for stream.state.readGate.TryLock() {
		stream.state.readGate.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("actual reader did not enter its owned gate")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan struct{}, 2)
	go func() { owner.Close(); closed <- struct{}{} }()
	go func() { _ = stream.Close(); closed <- struct{}{} }()
	for range 2 {
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("actual original owner/source/HTTP cleanup did not join")
		}
	}
	select {
	case err := <-readDone:
		if err == nil || !bytes.Equal(buffer, bytes.Repeat([]byte{0xb6}, 64)) {
			t.Fatal("canceled late actual read published bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("owner returned before actual Read completion")
	}
	select {
	case <-stream.state.done:
	default:
		t.Fatal("owner returned before remote cleanup completion")
	}
	familyJellyfinWorkers.Lock()
	remaining := familyJellyfinWorkers.keys[stream.state.key]
	familyJellyfinWorkers.Unlock()
	if remaining != 0 {
		t.Fatal("joined actual source did not refund worker slot")
	}
}

func TestFamilyJellyfinSourceActualBusyClaimsAndGlobalWorkerAdmission(t *testing.T) {
	for _, mode := range []string{"integration", "global"} {
		t.Run(mode, func(t *testing.T) {
			f := newFamilyJellyfinPGFixture(t, []byte("finite unread bytes"))
			limit := 2
			if mode == "global" {
				limit = 8
			}
			var streams []*FamilyJellyfinStream
			for range limit {
				adapter := f.adapter
				if mode == "global" {
					adapter, _, _ = f.newAdapter(t)
				}
				_, source := f.source(t)
				stream, err := adapter.Open(t.Context(), source)
				if err != nil {
					t.Fatal("valid worker denied before configured capacity")
				}
				t.Cleanup(func() { _ = stream.Close() })
				streams = append(streams, stream)
			}
			adapter := f.adapter
			if mode == "global" {
				adapter, _, _ = f.newAdapter(t)
			}
			_, denied := f.source(t)
			if stream, err := adapter.Open(t.Context(), denied); stream != nil || !errors.Is(err, profileunlock.ErrFamilyMediaBusy) {
				t.Fatal("remote worker admission exceeded exact capacity")
			}
			if streams[0].Close() != nil {
				t.Fatal("actual worker cleanup failed")
			}
			if stream, err := adapter.Open(t.Context(), denied); stream != nil || err == nil {
				t.Fatal("capacity recovery healed the original one-shot Open claim")
			}
			_, fresh := f.source(t)
			stream, err := adapter.Open(t.Context(), fresh)
			if err != nil {
				t.Fatal("actual joined worker capacity was not reusable by a fresh original")
			}
			_ = stream.Close()
		})
	}
}

func TestFamilyJellyfinSourceActualSetupCancelAfterHandoffAndOpaqueCopies(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, []byte("finite original handoff bytes"))
	_, source := f.source(t)
	setup, cancel := context.WithTimeout(t.Context(), time.Second)
	stream, err := f.adapter.Open(setup, source)
	if err != nil {
		cancel()
		t.Fatal("actual source handoff failed")
	}
	defer stream.Close()
	cancel()
	copy := *stream
	if copy.Close() == nil || copy.Check(t.Context()) == nil || stream.Context().Err() != nil {
		t.Fatal("copied source gained/retired original or setup cancellation survived handoff")
	}
	copyAdapter := *f.adapter
	if copyAdapter.ReadyFor(f.ds, f.manager) {
		t.Fatal("copied adapter adopted original readiness")
	}
	for _, value := range []any{f.adapter, *f.adapter, stream, *stream, stream.Info()} {
		for _, format := range []string{"%+v", "%#v"} {
			text := fmt.Sprintf(format, value)
			if strings.Contains(text, familyJellyfinTestToken) || strings.Contains(text, f.wire.server.URL) || strings.Contains(text, f.session.Token) {
				t.Fatal("source formatting exposed original credentials")
			}
		}
	}
	buffer := make([]byte, len(f.wire.data))
	if n, err := stream.Read(buffer); n != len(buffer) || err != nil && err != io.EOF || !bytes.Equal(buffer, f.wire.data) || stream.Check(t.Context()) != nil {
		t.Fatal("original held response did not survive setup cancellation")
	}
}

func TestFamilyJellyfinSourceActualPostHTTPPGDelayCannotPublishAgedObservation(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, []byte("must never download"))
	_, source := f.source(t)
	connection, err := f.ds.Pool().Acquire(t.Context())
	if err != nil {
		t.Fatal("acquire own final-check lock connection")
	}
	defer connection.Release()
	transaction, err := connection.Begin(t.Context())
	if err != nil {
		t.Fatal("begin own final-check lock transaction")
	}
	defer transaction.Rollback(context.Background())
	started := make(chan time.Time, 1)
	locked := make(chan error, 1)
	f.wire.setUserHook(func(count int, _ *http.Request) {
		if count == 1 {
			started <- time.Now()
		}
		if count == 2 {
			_, err := transaction.Exec(t.Context(), `LOCK TABLE remote_media_items IN ACCESS EXCLUSIVE MODE`)
			locked <- err
		}
	})
	var first sync.Once
	f.wire.setItemHook(func(r *http.Request) {
		if strings.HasSuffix(r.URL.Path, familyJellyfinTestPrimary) {
			first.Do(func() { time.Sleep(2500 * time.Millisecond) })
		}
	})
	completed := make(chan error, 1)
	go func() {
		stream, err := f.adapter.Open(t.Context(), source)
		if stream != nil {
			_ = stream.Close()
		}
		completed <- err
	}()
	var requestStart time.Time
	select {
	case requestStart = <-started:
	case <-time.After(time.Second):
		t.Fatal("initial real observation did not start")
	}
	select {
	case err := <-locked:
		if err != nil {
			t.Fatal("actual post-HTTP PG lock failed")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("real observation did not reach its final user response")
	}
	f.waitBlockedQuery(t, "remote_media_items")
	select {
	case <-completed:
		t.Fatal("Open ended before its witnessed post-HTTP PG delay")
	default:
	}
	if delay := time.Until(requestStart.Add(5100 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	if !f.blockedQuery(t, "remote_media_items") {
		t.Fatal("post-HTTP PG query ended before the final freshness guard was exercised")
	}
	select {
	case <-completed:
		t.Fatal("Open timed out before the held post-HTTP PG query was released")
	default:
	}
	if transaction.Rollback(t.Context()) != nil {
		t.Fatal("release actual post-HTTP PG lock")
	}
	select {
	case err := <-completed:
		if err == nil || f.wire.downloads.Load() != 0 {
			t.Fatal("post-HTTP PG delay published an observation older than request-start budget")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("released actual final check did not complete")
	}
}

func (f *familyJellyfinPGFixture) waitBlockedQuery(t *testing.T, relation string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if f.blockedQuery(t, relation) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("actual source query never blocked on own held relation")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *familyJellyfinPGFixture) blockedQuery(t *testing.T, relation string) bool {
	t.Helper()
	// The retained fixture login owns the application's real sessions. SET
	// ROLE on the application pool deliberately grants no observer privileges.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var blocked bool
	if err := f.observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE $2 AND pid<>pg_backend_pid())`, f.app, "%"+relation+"%").Scan(&blocked); err != nil {
		t.Fatal("observe own blocked actual source query through fixture login")
	}
	return blocked
}

func TestFamilyJellyfinSourceActualFreshCanceledPGCheckPreservesOriginal(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, []byte("live original"))
	_, source, stream := f.open(t)
	transaction, err := f.ds.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal("begin own relation lock")
	}
	defer transaction.Rollback(context.Background())
	if _, err := transaction.Exec(t.Context(), `LOCK TABLE remote_media_items IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal("hold own original item relation")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	completed := make(chan error, 1)
	go func() { completed <- stream.Check(ctx) }()
	f.waitBlockedQuery(t, "remote_media_items")
	cancel()
	select {
	case err := <-completed:
		if err == nil {
			t.Fatal("canceled actual consumer query succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled actual consumer PG query did not join")
	}
	if transaction.Rollback(t.Context()) != nil {
		t.Fatal("release own original relation")
	}
	if stream.Context().Err() != nil || source.Context().Err() != nil || stream.Check(t.Context()) != nil {
		t.Fatal("fresh consumer cancellation retired healthy original authority")
	}
}

func TestFamilyJellyfinSourceActualSourceCancellationCoversPreliminaryPG(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, []byte("never request media"))
	owner, source := f.source(t)
	transaction, err := f.ds.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal("begin own preliminary relation lock")
	}
	defer transaction.Rollback(context.Background())
	if _, err := transaction.Exec(t.Context(), `LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal("hold own original account relation")
	}
	opened := make(chan error, 1)
	go func() {
		stream, err := f.adapter.Open(t.Context(), source)
		if stream != nil {
			_ = stream.Close()
		}
		opened <- err
	}()
	f.waitBlockedQuery(t, "accounts")
	closed := make(chan struct{})
	go func() { owner.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("owner could not join pre-opener actual PG query before lock release")
	}
	select {
	case err := <-opened:
		if err == nil || f.wire.users.Load()+f.wire.items.Load()+f.wire.downloads.Load() != 0 {
			t.Fatal("source-only cancellation escaped preliminary PG into HTTP")
		}
	case <-time.After(time.Second):
		t.Fatal("original owner returned before preliminary opener completion")
	}
}

func TestFamilyJellyfinSourceActualDelayedCallerCannotPublishAfterFinalPG(t *testing.T) {
	f := newFamilyJellyfinPGFixture(t, []byte("must not publish"))
	_, source := f.source(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	caller := &familyJellyfinDelayedCaller{Context: ctx, entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{}), stopRequested: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(caller.release) }) }
	defer unblock()
	f.wire.setUserHook(func(count int, _ *http.Request) {
		if count == 6 {
			cancel()
			<-caller.entered
		}
	})
	completed := make(chan error, 1)
	go func() {
		stream, err := f.adapter.Open(caller, source)
		if stream != nil {
			_ = stream.Close()
		}
		completed <- err
	}()
	select {
	case <-caller.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("actual final observation did not cancel the original caller")
	}
	// The owned callback can be true-stopped before the delayed custom parent
	// delivers cancellation. The final direct caller guard must still deny.
	select {
	case <-caller.stopRequested:
	case <-time.After(2 * time.Second):
		t.Fatal("final result did not reach caller-link stop")
	}
	select {
	case err := <-completed:
		if err == nil || f.wire.downloads.Load() != 1 {
			t.Fatal("late caller signal published or replayed held response")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed original setup did not join actual HTTP cleanup")
	}
	unblock()
	select {
	case <-caller.done:
	case <-time.After(time.Second):
		t.Fatal("custom parent delivery did not join")
	}
	if source.Context().Err() != nil || source.Check(t.Context()) != nil {
		t.Fatal("caller-only failed setup mutated healthy PG source permission")
	}
}
