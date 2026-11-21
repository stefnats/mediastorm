//go:build linux

package backup

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"io"
	"net/http/httptest"
	"novastream/internal/datastore"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSharePlaybackRestoreCannotReviveDelegation(t *testing.T) {
	for _, kind := range []string{"named-zip", "uploaded-zip", "same-logical-principals-new-incarnation", "session-private-nonce-omitted", "share-original-witness-omitted", "no-sidecar-export-import", "legacy-sections-retained"} {
		t.Run(kind, func(t *testing.T) {
			f := positionServicePG(t, false)
			if _, e := f.store.Pool().Exec(t.Context(), `UPDATE users SET allow_share_links=true WHERE id='position-owner'`); e != nil {
				t.Fatal(e)
			}
			creator, e := datastore.NewShareLinkCreator(t.Context(), f.store, f.manager, f.security, f.coordinator)
			if e != nil {
				t.Fatal(e)
			}
			original, release, e := f.store.AuthenticateProfilePinRequest(httptest.NewRequest("POST", "http://synthetic.test/api/share/create", nil), "synthetic-position-backup-session", "position-client")
			if e != nil {
				t.Fatal(e)
			}
			link, e := creator.Create(original, "position-owner", map[string]string{"sourcePath": "synthetic:backup-share", "profileId": "position-owner"}, time.Hour, 4, "")
			release()
			if e != nil {
				t.Fatal(e)
			}
			runtime, e := datastore.NewShareLinkRedeemer(t.Context(), f.store, f.manager, f.security, f.coordinator)
			if e != nil {
				t.Fatal(e)
			}
			defer runtime.Close()
			result, e := runtime.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token)
			if e != nil {
				t.Fatal(e)
			}
			var originalAccount, originalProfile string
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT a.share_incarnation::text,u.share_incarnation::text FROM accounts a JOIN users u ON u.account_id=a.id WHERE u.id='position-owner'`).Scan(&originalAccount, &originalProfile); e != nil {
				t.Fatal(e)
			}
			info, e := f.service.CreateBackup(BackupTypeManual)
			if e != nil {
				t.Fatal(e)
			}
			raw, e := os.ReadFile(filepath.Join(f.service.backupDir, info.Filename))
			if e != nil {
				t.Fatal(e)
			}
			reader, e := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
			if e != nil {
				t.Fatal(e)
			}
			var database []byte
			for _, entry := range reader.File {
				if strings.Contains(entry.Name, "share_playback") {
					t.Fatal("private sidecar appeared in ZIP")
				}
				if entry.Name == "database.json" {
					r, e := entry.Open()
					if e != nil {
						t.Fatal(e)
					}
					database, e = io.ReadAll(r)
					closeErr := r.Close()
					if e != nil || closeErr != nil {
						t.Fatal("actual ZIP member read", e, closeErr)
					}
				}
			}
			if len(database) == 0 {
				t.Fatal("actual ZIP missing durable database")
			}
			for _, private := range []string{"share_incarnation", "shareIncarnation", "share_binding_id", "shareBindingId", "share_playback_bindings", "creator_incarnation", "source_owner_incarnation", "source_profile_incarnation", "delegation_version"} {
				if bytes.Contains(database, []byte(private)) {
					t.Fatal("private delegation witness exported", private)
				}
			}
			var exported map[string]json.RawMessage
			if e = json.Unmarshal(database, &exported); e != nil {
				t.Fatal(e)
			}
			// Empty optional legacy sections intentionally remain omitted. Mandatory
			// sections plus these actual seeded records must survive the real archive.
			for _, section := range []string{"version", "exportedAt", "regionalChildPolicies", "accounts", "users", "sessions", "invitations", "clients", "clientSettings", "userSettings", "watchlist", "watchHistory", "playbackProgress", "contentPreferences", "customLists", "recordingPlaybackPositions", "recordingProgressHistory", "numbersStationProgress", "shareLinks", "recordings"} {
				if _, ok := exported[section]; !ok {
					t.Fatal("ordinary durable section was lost", section)
				}
			}
			if !bytes.Contains(exported["sessions"], []byte(result.Session.Token)) || !bytes.Contains(exported["shareLinks"], []byte(link.Token)) {
				t.Fatal("ordinary restricted session/link DATA omitted")
			}
			// Finish all original consumers before the actual restore's settings writer.
			runtime.Close()
			if kind == "named-zip" {
				e = f.service.RestoreBackup(info.Filename)
			} else {
				e = f.service.RestoreBackupUpload(bytes.NewReader(raw), "synthetic-share.zip")
			}
			if e != nil {
				t.Fatal("actual full ZIP restore", kind, e)
			}
			var account, profile string
			var nonceAbsent, witnessAbsent bool
			var bindings int
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT a.share_incarnation::text,u.share_incarnation::text FROM accounts a JOIN users u ON u.account_id=a.id WHERE u.id='position-owner'`).Scan(&account, &profile); e != nil || account == originalAccount || profile == originalProfile {
				t.Fatal("restored logical principals adopted old private incarnation", e)
			}
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT share_binding_id IS NULL FROM sessions WHERE token=$1`, result.Session.Token).Scan(&nonceAbsent); e != nil || !nonceAbsent {
				t.Fatal("generic restored session retained private nonce", e)
			}
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT delegation_version IS NULL AND creator_incarnation IS NULL AND source_owner_incarnation IS NULL AND source_profile_incarnation IS NULL FROM share_links WHERE token=$1`, link.Token).Scan(&witnessAbsent); e != nil || !witnessAbsent {
				t.Fatal("generic restored share adopted original witness", e)
			}
			if e = f.store.Pool().QueryRow(t.Context(), `SELECT count(*) FROM share_playback_bindings`).Scan(&bindings); e != nil || bindings != 0 {
				t.Fatal("private binding was restored", bindings, e)
			}
			accountData, e := f.store.Accounts().Get(t.Context(), "one")
			if e != nil || accountData == nil {
				t.Fatal("restored original logical account missing", e)
			}
			profileData, e := f.store.Users().Get(t.Context(), "position-owner")
			if e != nil || profileData == nil || profileData.AccountID != "one" {
				t.Fatal("restored original logical profile missing", e)
			}
			recordingData, e := f.store.Recordings().Get(t.Context(), "position-recording")
			if e != nil || recordingData == nil || recordingData.UserID != "position-owner" {
				t.Fatal("ordinary recording section was lost", e)
			}
			// Reconstruct a genuine current runtime, not the intentionally retired old
			// policy. Even a new actual PG frame cannot upgrade restored display DATA.
			fresh, e := datastore.NewShareLinkRedeemer(t.Context(), f.store, f.manager, f.security, f.coordinator)
			if e != nil {
				t.Fatal(e)
			}
			defer fresh.Close()
			framed, stop, e := f.store.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), result.Session.Token, "")
			if e != nil {
				t.Fatal("restored session DATA frame unavailable", e)
			}
			defer stop()
			if o, e := fresh.CaptureSharePlaybackOwner(framed, "position-owner", "synthetic:backup-share"); e == nil || o != nil {
				t.Fatal("restored session revived source authority")
			}
			if result, e := fresh.Redeem(httptest.NewRequest("GET", "http://synthetic.test/share/"+link.Token, nil), link.Token); e == nil || result != nil {
				t.Fatal("restored share witness silently adopted current identities")
			}
		})
	}
}
