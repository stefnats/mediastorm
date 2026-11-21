//go:build linux

package datastore

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"novastream/internal/auth"
)

func shareActualOwner(t *testing.T, f *sharePlaybackDSFixture) (*SharePlaybackOwner, *ShareLinkRedemption, *http.Request, context.CancelFunc) {
	t.Helper()
	result, err := f.redeem(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	r, release := f.sourceFrame(t, result)
	owner, err := f.runtime.CaptureSharePlaybackOwner(r, f.profile, f.resource)
	if err != nil {
		release()
		t.Fatal("actual original scoped owner", err)
	}
	return owner, result, r, release
}

func TestSharePlaybackGenuineOriginalOwner(t *testing.T) {
	for _, kind := range []string{"original-frame", "dto-frame", "foreign-frame", "copied-owner", "generic-stream-token", "missing-private-nonce", "wrong-binding-nonce", "source-profile-substitution", "source-resource-substitution", "same-resource-other-recipient", "source-owner-substitution", "actual-party-envelope-separated"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharePlaybackDS(t, kind)
			result, err := f.redeem(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			r, release := f.sourceFrame(t, result)
			defer release()
			profile, resource := f.profile, f.resource
			switch kind {
			case "dto-frame":
				raw := httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil)
				ctx := context.WithValue(raw.Context(), auth.ContextKeySession, result.Session)
				ctx = context.WithValue(ctx, auth.ContextKeyAccountID, result.Session.AccountID)
				ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, false)
				r = raw.WithContext(ctx)
			case "foreign-frame":
				foreign, e := New(t.Context(), f.base.ds.pool.Config().ConnString())
				if e != nil {
					t.Fatal(e)
				}
				defer foreign.Close()
				var stop context.CancelFunc
				r, stop, e = foreign.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), result.Session.Token, "")
				if e != nil {
					t.Fatal(e)
				}
				defer stop()
			case "generic-stream-token":
				generic := result.Session
				generic.Token = "synthetic-unattested-generic-stream"
				if e := f.base.ds.Sessions().Create(t.Context(), &generic); e != nil {
					t.Fatal(e)
				}
				var stop context.CancelFunc
				r, stop, err = f.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), generic.Token, "")
				if err != nil {
					t.Fatal(err)
				}
				defer stop()
			case "missing-private-nonce":
				f.base.exec(t, `DELETE FROM share_playback_bindings WHERE session_token=$1`, result.Session.Token)
				f.base.exec(t, `UPDATE sessions SET share_binding_id=NULL WHERE token=$1`, result.Session.Token)
			case "wrong-binding-nonce":
				// The genuine composite FK must reject a mismatched nonce before it can
				// become a purported source observation; no constraint is disabled.
				if _, e := f.base.ds.pool.Exec(t.Context(), `UPDATE share_playback_bindings SET binding_id=$2::uuid WHERE session_token=$1`, result.Session.Token, uuid.NewString()); e == nil {
					t.Fatal("wrong actual nonce bypassed composite binding FK")
				}
			case "source-profile-substitution":
				profile = "synthetic-other-profile"
			case "source-resource-substitution":
				resource = "synthetic:other-source"
			case "source-owner-substitution":
				f.base.exec(t, `UPDATE share_playback_bindings SET source_account_id='other' WHERE session_token=$1`, result.Session.Token)
			case "actual-party-envelope-separated":
				f.base.exec(t, `UPDATE sessions SET scope_resource='watch-party-playback:v1:{"roomId":"synthetic-room","guestId":"synthetic-guest","resource":"synthetic:party"}' WHERE token=$1`, result.Session.Token)
				var stop context.CancelFunc
				r, stop, err = f.base.ds.AuthenticateProfilePinRequest(httptest.NewRequest("GET", "http://synthetic.test/api/video/stream", nil), result.Session.Token, "")
				if err != nil {
					t.Fatal(err)
				}
				defer stop()
				if _, e := f.runtime.CaptureSharePlaybackOwner(r, profile, resource); e == nil || errors.Is(e, ErrSharePlaybackParty) {
					t.Fatal("unresolved/model-only party envelope selected genuine party bypass")
				}
				return
			}
			owner, err := f.runtime.CaptureSharePlaybackOwner(r, profile, resource)
			allowed := kind == "original-frame" || kind == "wrong-binding-nonce" || kind == "copied-owner" || kind == "same-resource-other-recipient"
			if !allowed {
				if err == nil || owner != nil {
					t.Fatal("foreign/model/substituted source acquired owner", kind)
				}
				return
			}
			if err != nil || owner == nil {
				t.Fatal("genuine actual owner", err)
			}
			defer owner.Close()
			if kind == "copied-owner" {
				copy := &SharePlaybackOwner{self: owner, runtime: owner.runtime, ctx: owner.ctx, original: owner.original}
				if copy.Check(t.Context()) == nil {
					t.Fatal("copied opaque owner certified source")
				}
				if owner.Check(t.Context()) != nil {
					t.Fatal("copy denial retired genuine original")
				}
			}
			if kind == "same-resource-other-recipient" {
				other, e := f.redeem(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				fresh, stop := f.sourceFrame(t, other)
				defer stop()
				if owner.CheckRequest(fresh, f.profile, f.resource) == nil {
					t.Fatal("another recipient adopted original source worker")
				}
				if owner.Check(t.Context()) != nil {
					t.Fatal("foreign recipient denial retired original source")
				}
			}
		})
	}
}

func TestSharePlaybackCurrentSourceLifetime(t *testing.T) {
	for _, kind := range []string{"inactive-households", "creator-household-child", "source-household-child", "profile-kids", "native-policy-unavailable", "native-policy-retired", "household-aba", "link-inactive", "link-deleted", "session-revoked", "original-source-incarnation", "fixed-original-expiry", "profile-settings-change"} {
		t.Run(kind, func(t *testing.T) {
			setup := kind
			if kind == "creator-household-child" || kind == "source-household-child" {
				setup = "master-foreign-source"
			}
			f := newSharePlaybackDS(t, setup)
			if kind == "fixed-original-expiry" {
				// A genuine new original delegation starts under the actual shortened
				// account expiry; no proof/session/owner deadline is edited afterward.
				f.base.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, time.Now().UTC().Add(2*time.Second).Truncate(time.Microsecond))
				original := shareProvenanceRequest(t, f.base, t.Context())
				link, e := f.creator.Create(original, f.profile, map[string]string{"sourcePath": f.resource}, time.Hour, 4, "")
				if e != nil {
					t.Fatal(e)
				}
				f.link = link
			}
			owner, result, r, release := shareActualOwner(t, f)
			defer release()
			defer owner.Close()
			switch kind {
			case "inactive-households":
				if owner.Check(t.Context()) != nil {
					t.Fatal("explicitly inactive actual household denied")
				}
				return
			case "creator-household-child":
				f.base.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('creator-child','actor','Synthetic creator child',true)`)
			case "source-household-child":
				f.base.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('source-child','other','Synthetic source child',true)`)
			case "profile-settings-change":
				var beforeID, afterID string
				var beforeGlobal, afterGlobal, beforeGeneration, afterGeneration int64
				if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT share_incarnation::text,(SELECT last_value FROM regional_policy_revision_seq),(SELECT generation FROM user_settings_versions WHERE user_id=$1) FROM users WHERE id=$1`, f.profile).Scan(&beforeID, &beforeGlobal, &beforeGeneration); e != nil {
					t.Fatal(e)
				}
				f.base.exec(t, `INSERT INTO user_settings(user_id,settings) VALUES($1,'{"playback":{"preferredAudioLanguage":"fr"}}') ON CONFLICT(user_id) DO UPDATE SET settings=EXCLUDED.settings`, f.profile)
				if e := f.base.ds.pool.QueryRow(t.Context(), `SELECT share_incarnation::text,(SELECT last_value FROM regional_policy_revision_seq),(SELECT generation FROM user_settings_versions WHERE user_id=$1) FROM users WHERE id=$1`, f.profile).Scan(&afterID, &afterGlobal, &afterGeneration); e != nil {
					t.Fatal(e)
				}
				if beforeID != afterID || beforeGlobal != afterGlobal || afterGeneration <= beforeGeneration {
					t.Fatal("settings canary failed to isolate real settings generation")
				}
			case "profile-kids":
				f.base.exec(t, `UPDATE users SET is_kids_profile=true WHERE id=$1`, f.profile)
			case "native-policy-unavailable":
				path := f.base.manager.ConfigPath()
				if e := os.Rename(path, path+".synthetic-unavailable"); e != nil {
					t.Fatal(e)
				}
				defer os.Rename(path+".synthetic-unavailable", path)
			case "native-policy-retired":
				if e := f.base.manager.Save(f.base.settings); e != nil {
					t.Fatal(e)
				}
			case "household-aba":
				f.base.exec(t, `INSERT INTO users(id,account_id,name,is_kids_profile)VALUES('transient-child','actor','Synthetic transient child',true);DELETE FROM users WHERE id='transient-child'`)
			case "link-inactive":
				f.base.exec(t, `UPDATE share_links SET active=false WHERE token=$1`, f.link.Token)
				f.base.exec(t, `UPDATE share_links SET active=true WHERE token=$1`, f.link.Token)
			case "link-deleted":
				f.base.exec(t, `DELETE FROM share_links WHERE token=$1`, f.link.Token)
			case "session-revoked":
				f.base.exec(t, `DELETE FROM sessions WHERE token=$1`, result.Session.Token)
			case "original-source-incarnation":
				shareCopyPrincipal(t, f, "users", f.profile)
			case "fixed-original-expiry":
				bridgeSessionAwait(t, owner.Context().Done())
			}
			if owner.Check(t.Context()) == nil {
				t.Fatal("retired original source context remained valid", kind)
			}
			if kind == "link-inactive" {
				if f.runtime.ValidateSharePlaybackRequest(r) == nil {
					t.Fatal("reenabled link revived old nonce")
				}
				next, e := f.redeem(t.Context())
				if e != nil || next == nil {
					t.Fatal("reenabled current link could not mint a NEW allowed recipient", e)
				}
				fresh, stop := f.sourceFrame(t, next)
				defer stop()
				if e = f.runtime.ValidateSharePlaybackRequest(fresh); e != nil {
					t.Fatal("new genuine permitted recipient denied after reenable", e)
				}
			}
		})
	}
}

func TestSharePlaybackOneShotHandoffAndClose(t *testing.T) {
	for _, kind := range []string{"original-handoff", "repeated-handoff", "foreign-request-handoff", "pre-handoff-cancel", "post-handoff-setup-retirement", "fresh-matching-media-request", "fresh-other-binding-denied", "held-check-close-joins", "admission-retained-until-sql-end"} {
		t.Run(kind, func(t *testing.T) {
			f := newSharePlaybackDS(t, kind)
			owner, result, r, release := shareActualOwner(t, f)
			defer release()
			var lease *SharePlaybackSourceLease
			defer func() {
				if lease != nil {
					lease.Close()
				}
				owner.Close()
			}()
			if kind == "pre-handoff-cancel" {
				release()
				bridgeSessionAwait(t, owner.Context().Done())
				if value, e := owner.Handoff(r); e == nil || value != nil {
					t.Fatal("retired original setup handed off source")
				}
				return
			}
			if kind == "foreign-request-handoff" {
				fresh, stop := f.sourceFrame(t, result)
				defer stop()
				if value, e := owner.Handoff(fresh); e == nil || value != nil {
					t.Fatal("fresh request replaced exact original handoff")
				}
				return
			}
			var err error
			lease, err = owner.Handoff(r)
			if err != nil || lease == nil {
				t.Fatal("genuine original one-shot source handoff", err)
			}
			if kind == "repeated-handoff" {
				if value, e := owner.Handoff(r); e == nil || value != nil {
					t.Fatal("source handoff renewed original authority")
				}
				return
			}
			if kind == "post-handoff-setup-retirement" {
				release()
				if owner.Context().Err() != nil || owner.Check(t.Context()) != nil {
					t.Fatal("intentional original setup retirement canceled durable source")
				}
				return
			}
			if kind == "fresh-matching-media-request" {
				fresh, stop := f.sourceFrame(t, result)
				defer stop()
				if owner.CheckRequest(fresh, f.profile, f.resource) != nil {
					t.Fatal("fresh matching actual frame denied")
				}
				ctx, cancel := context.WithCancel(fresh.Context())
				cancel()
				if owner.CheckRequest(fresh.WithContext(ctx), f.profile, f.resource) == nil {
					t.Fatal("canceled fresh request passed")
				}
				if owner.Check(t.Context()) != nil {
					t.Fatal("fresh caller cancellation retired independent original source")
				}
				return
			}
			if kind == "fresh-other-binding-denied" {
				other, e := f.redeem(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				fresh, stop := f.sourceFrame(t, other)
				defer stop()
				if owner.CheckRequest(fresh, f.profile, f.resource) == nil {
					t.Fatal("fresh other recipient replaced fixed original")
				}
				return
			}
			if kind == "held-check-close-joins" || kind == "admission-retained-until-sql-end" {
				tx, e := f.base.admin.Begin(t.Context())
				if e != nil {
					t.Fatal(e)
				}
				defer recordingStateRollback(tx)
				var token string
				if e = tx.QueryRow(t.Context(), `SELECT token FROM `+pgx.Identifier{f.base.schema, "sessions"}.Sanitize()+` WHERE token=$1 FOR UPDATE`, result.Session.Token).Scan(&token); e != nil {
					t.Fatal(e)
				}
				checked := make(chan struct{})
				go func() { _ = owner.Check(t.Context()); close(checked) }()
				defer func() { recordingStateRollback(tx); bridgeSessionAwait(t, checked) }()
				until := time.Now().Add(5 * time.Second)
				waiting := false
				for time.Now().Before(until) {
					if e = f.base.admin.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE '%FROM sessions WHERE token%FOR SHARE%')`, f.base.schema).Scan(&waiting); e != nil {
						t.Fatal(e)
					}
					if waiting {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if !waiting {
					t.Fatal("original current source SQL did not actually block")
				}
				closed := make(chan struct{})
				go func() { owner.Close(); close(closed) }()
				bridgeSessionAwait(t, owner.Context().Done())
				bridgeSessionAwait(t, checked)
				select {
				case <-closed:
					t.Fatal("source close returned before actual source lease end")
				default:
				}
				f.runtime.mu.Lock()
				_, charged := f.runtime.owners[owner]
				f.runtime.mu.Unlock()
				if !charged {
					t.Fatal("original admission refunded before source end")
				}
				lease.Close()
				bridgeSessionAwait(t, closed)
				return
			}
			// Two real concurrent close callers signal the same original lease, but
			// neither can refund it while its genuine consumer remains open.
			first, second := make(chan struct{}), make(chan struct{})
			go func() { owner.Close(); close(first) }()
			go func() { owner.Close(); close(second) }()
			bridgeSessionAwait(t, owner.Context().Done())
			select {
			case <-first:
				t.Fatal("first close overtook source lease")
			default:
			}
			select {
			case <-second:
				t.Fatal("second close overtook source lease")
			default:
			}
			lease.Close()
			bridgeSessionAwait(t, first)
			bridgeSessionAwait(t, second)
		})
	}
}
