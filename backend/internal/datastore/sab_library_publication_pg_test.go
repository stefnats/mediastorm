//go:build linux

package datastore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net"
	"net/http"
	"net/http/httptest"
	"novastream/internal/database"
	"novastream/internal/importer"
	nzbmetadata "novastream/internal/nzb/metadata"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type sabPublicationPGFixture struct {
	base                     *bridgeSessionFixture
	db                       *database.DB
	source                   *importer.Service
	issuer                   *SABLibraryPublicationIssuer
	token, target, stageRoot string
}

func sabPublicationPG(t *testing.T, dial func(context.Context, string, string) (net.Conn, error), one bool) *sabPublicationPGFixture {
	t.Helper()
	base, old := deploymentWebDAVPG(t, nil, dial, one)
	old.Close()
	f := &sabPublicationPGFixture{base: base, token: "synthetic-publication-session", target: t.TempDir(), stageRoot: t.TempDir()}
	base.exec(t, `UPDATE accounts SET is_master=true WHERE id='actor';INSERT INTO sessions(token,account_id,is_master,expires_at)VALUES('synthetic-publication-session','actor',false,clock_timestamp()+interval '1 hour')`)
	base.exec(t, `INSERT INTO local_media_libraries(id,name,library_type,root_path)VALUES('publication-library','Synthetic publication','other',$1)`, f.target)
	base.exec(t, `INSERT INTO media_library_access_policies(library_id,access_mode)VALUES('publication-library','all')`)
	enabled := true
	base.settings.SABnzbd.Enabled = &enabled
	base.settings.SABnzbd.FallbackHost = "http://synthetic.invalid"
	base.settings.SABnzbd.FallbackAPIKey = "synthetic-unused-key"
	base.settings.SABnzbd.ImportStageTargetRoot = f.stageRoot
	base.settings.SABnzbd.ImportStageMaxBytes = 32 << 20
	if e := base.manager.Save(base.settings); e != nil {
		t.Fatal(e)
	}
	var e error
	f.db, e = database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "publication.db")})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { f.db.Close() })
	f.source, e = importer.NewService(importer.ServiceConfig{}, nzbmetadata.NewMetadataService(t.TempDir()), f.db, nil, nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = f.source.Close() })
	info, e := os.Stat(f.stageRoot)
	if e != nil {
		t.Fatal(e)
	}
	st := info.Sys().(*syscall.Stat_t)
	namespace := sha256.Sum256([]byte(fmt.Sprintf("sab-stage-root-v1:%d:%d", st.Dev, st.Ino)))
	source, _ := json.Marshal([]string{strings.TrimSpace(base.settings.SABnzbd.FallbackHost), base.settings.SABnzbd.FallbackAPIKey})
	sourceDigest := sha256.Sum256(source)
	clear(source)
	operation := strings.Repeat("1", 32)
	directory := filepath.Join(f.stageRoot, operation)
	if e = os.Mkdir(directory, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(directory, "data-0001.bin"), []byte("abcdef"), 0600); e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256([]byte("abcdef"))
	manifest, _ := json.Marshal([]database.SABStageFile{{Ordinal: 1, Bytes: 6, SHA256: hex.EncodeToString(hash[:])}})
	receiptJSON, _ := json.Marshal(map[string]any{database.SABFallbackMetadataKey: map[string]any{"version": 1, "attempt": strings.Repeat("2", 32), "source": hex.EncodeToString(sourceDigest[:]), "jobId": "synthetic-job", "status": map[string]any{"queueId": 1, "state": "download_completed"}}})
	metadata := string(receiptJSON)
	item := &database.ImportQueueItem{NzbPath: "synthetic-unused.nzb", Status: database.QueueStatusFailed, Metadata: &metadata}
	if e = f.db.Repository.AddToQueue(item); e != nil || item.ID != 1 {
		t.Fatal("original synthetic receipt", e, item.ID)
	}

	if _, e = f.db.Connection().Exec(`INSERT INTO sab_import_stages(operation_id,queue_id,receipt_attempt,source_hash,namespace,expected_bytes,file_count,state,file_manifest,created_at,updated_at)VALUES(?,1,?,?,?,6,1,'copied',?,'synthetic','synthetic')`, operation, strings.Repeat("2", 32), hex.EncodeToString(sourceDigest[:]), hex.EncodeToString(namespace[:]), string(manifest)); e != nil {
		t.Fatal(e)
	}
	f.issuer, e = NewSABLibraryPublicationIssuer(t.Context(), base.ds, f.source, base.manager, base.security, base.coordinator)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(f.issuer.Close)
	return f
}
func (f *sabPublicationPGFixture) request(t *testing.T) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/admin/api/usenet/fallback/1/publication", strings.NewReader(`{"libraryId":"publication-library","ordinals":[1]}`)).WithContext(ctx)
	r.RemoteAddr = "127.0.0.1:1234"
	checked, release, e := f.base.ds.AuthenticateProfilePinRequest(r, f.token, "synthetic-publication-client")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(release)
	return checked
}
func (f *sabPublicationPGFixture) capture(t *testing.T, r *http.Request) *SABLibraryPublicationIntent {
	t.Helper()
	p, e := f.issuer.Capture(r, 1, []byte(`{"libraryId":"publication-library","ordinals":[1]}`))
	if e != nil {
		t.Fatal("actual current master/stage publication", e)
	}
	t.Cleanup(p.Close)
	return p
}
func TestSABPublicationOriginalAuthority(t *testing.T) {
	for _, name := range []string{"actual-current-master-only", "scoped-or-expired-session-denied", "session-and-actor-physical-aba", "target-library-physical-aba", "library-policy-and-grant-aba", "same-transaction-journal-reservation", "same-pool-one-no-nested-read", "late-nowait-lock-contention", "actual-commit-ack-loss-no-success"} {
		t.Run(name, func(t *testing.T) {
			var enabled, dropped atomic.Bool
			var dial func(context.Context, string, string) (net.Conn, error)
			if name == "actual-commit-ack-loss-no-success" {
				dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					c, e := (&net.Dialer{}).DialContext(ctx, network, address)
					if e != nil {
						return nil, e
					}
					return &bridgeSessionDropACK{Conn: c, enabled: &enabled, actual: &parentDropCommit{Conn: c, dropped: &dropped}}, nil
				}
			}
			f := sabPublicationPG(t, dial, name == "same-pool-one-no-nested-read")
			r := f.request(t)
			if name == "scoped-or-expired-session-denied" {
				f.base.exec(t, `UPDATE sessions SET scope='stream',scope_resource='synthetic:private' WHERE token=$1`, f.token)
				scoped := f.request(t)
				if p, e := f.issuer.Capture(scoped, 1, []byte(`{"libraryId":"publication-library","ordinals":[1]}`)); e == nil {
					p.Close()
					t.Fatal("scoped session became publisher")
				}
				f.base.exec(t, `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, f.token)
				expiredCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				expired := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/admin/api/usenet/fallback/1/publication", nil).WithContext(expiredCtx)
				if _, release, e := f.base.ds.AuthenticateProfilePinRequest(expired, f.token, ""); e == nil {
					release()
					t.Fatal("expired session became current")
				}
				return
			}
			if name == "library-policy-and-grant-aba" {
				f.base.exec(t, `INSERT INTO media_library_account_grants(library_id,account_id)VALUES('publication-library','other')`)
			}
			p := f.capture(t, r)
			switch name {
			case "actual-current-master-only":
				buffer := make([]byte, 6)
				if n, e := p.ReadAt(r, 1, buffer, 0); e != nil || n != 6 || string(buffer) != "abcdef" {
					t.Fatal("actual held source not owned by current master", e)
				}
				f.base.exec(t, `UPDATE accounts SET is_master=false WHERE id='actor'`)
				if p.Check(r) == nil {
					t.Fatal("cached session master overrode actual demotion")
				}
			case "session-and-actor-physical-aba":
				f.base.exec(t, `UPDATE accounts SET username=username WHERE id='actor'`)
				if p.Check(r) == nil {
					t.Fatal("same logical actor row renewed original")
				}
				p.Close()
				next := f.request(t)
				q := f.capture(t, next)
				f.base.exec(t, `UPDATE sessions SET user_agent=user_agent WHERE token=$1`, f.token)
				if q.Check(next) == nil {
					t.Fatal("same logical session row renewed original")
				}
			case "target-library-physical-aba":
				f.base.exec(t, `UPDATE local_media_libraries SET name=name WHERE id='publication-library'`)
				if p.Check(r) == nil {
					t.Fatal("same logical library row renewed original")
				}
			case "library-policy-and-grant-aba":
				f.base.exec(t, `UPDATE media_library_access_policies SET access_mode=access_mode WHERE library_id='publication-library'`)
				if p.Check(r) == nil {
					t.Fatal("policy row ABA accepted")
				}
				p.Close()
				next := f.request(t)
				q := f.capture(t, next)
				f.base.exec(t, `UPDATE media_library_account_grants SET account_id=account_id WHERE library_id='publication-library'`)
				if q.Check(next) == nil {
					t.Fatal("grant row ABA accepted")
				}
				q.Close()
				f.base.exec(t, `DELETE FROM media_library_account_grants WHERE library_id='publication-library'`)
				restoredRequest := f.request(t)
				restored := f.capture(t, restoredRequest)
				f.base.exec(t, `INSERT INTO media_library_account_grants(library_id,account_id)VALUES('publication-library','other');DELETE FROM media_library_account_grants WHERE library_id='publication-library'`)
				if restored.Check(restoredRequest) == nil {
					t.Fatal("absent grant insert-delete ABA renewed original family fence")
				}

			case "same-transaction-journal-reservation", "same-pool-one-no-nested-read":
				saved, e := p.Reserve(r)
				if e != nil || len(saved.OperationID) != 32 {
					t.Fatal("actual operational reservation", e)
				}
				var count, items int
				if e = f.base.ds.pool.QueryRow(t.Context(), "SELECT count(*) FROM sab_library_publications WHERE operation_id=$1", saved.OperationID).Scan(&count); e != nil {
					t.Fatal(e)
				}
				if e = f.base.ds.pool.QueryRow(t.Context(), "SELECT count(*) FROM local_media_items").Scan(&items); e != nil || count != 1 || items != 0 {
					t.Fatal("foundation inserted media or lost journal", e, count, items)
				}
				again, e := p.Reserve(r)
				if e != nil || again.OperationID != saved.OperationID {
					t.Fatal("known operational reservation was duplicated", e)
				}
			case "late-nowait-lock-contention":
				held, e := f.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer held.Rollback(context.Background())
				if _, e = held.Exec(t.Context(), "LOCK TABLE "+pgx.Identifier{f.base.schema, "media_library_profile_grants"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); e != nil {
					t.Fatal(e)
				}
				started := time.Now()
				if e = p.Check(r); (!errors.Is(e, ErrSABLibraryPublicationBusy) && !errors.Is(e, ErrSABLibraryPublicationChanged)) || time.Since(started) > 3*time.Second {
					t.Fatal("late actual table lock was waited or ignored", e)
				}
			case "actual-commit-ack-loss-no-success":
				enabled.Store(true)
				record, e := p.Reserve(r)
				if !errors.Is(e, ErrSABLibraryPublicationUnknown) || !dropped.Load() || record.OperationID == "" {
					t.Fatal("lost actual reservation ACK claimed success", e)
				}
				var count, items int
				if e = f.base.admin.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{f.base.schema, "sab_library_publications"}.Sanitize()+" WHERE operation_id=$1", record.OperationID).Scan(&count); e != nil {
					t.Fatal(e)
				}
				if e = f.base.admin.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{f.base.schema, "local_media_items"}.Sanitize()).Scan(&items); e != nil || count != 1 || items != 0 {
					t.Fatal("actual unknown COMMIT lost journal or published media", e, count, items)
				}
				if _, e = p.Reserve(r); e == nil {
					t.Fatal("unknown original reservation retried")
				}
			}
		})
	}
}
