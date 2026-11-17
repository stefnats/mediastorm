package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type kidsFlagPG struct{ *modePGFixture }

func kidsFlagFixture(t *testing.T) *kidsFlagPG {
	t.Helper()
	f := modeFixture(t)
	f.exec(t, `CREATE TABLE user_settings(user_id text PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,settings jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())`)
	raw, e := embedMigrations.ReadFile("migrations/079_user_settings_versions.sql")
	if e != nil {
		t.Fatal(e)
	}
	f.exec(t, strings.Split(string(raw), "-- +goose Down")[0])
	return &kidsFlagPG{f}
}
func (f *kidsFlagPG) request(t *testing.T, ctx context.Context, kind parentKidsFlagEntry) (*http.Request, context.CancelFunc) {
	t.Helper()
	r, stop := f.modePGFixture.request(t, ctx, f.ds)
	if kind == parentKidsFlagAPI {
		r.URL.Path = "/api/users/target/kids-profile"
	} else {
		r.URL.Path = "/account/api/profiles/kids"
		r.URL.RawQuery = "profileId=target"
	}
	return r, stop
}
func (f *kidsFlagPG) ready(t *testing.T, kind parentKidsFlagEntry, body string) (*ParentKidsFlagIntent, *http.Request) {
	t.Helper()
	first, release := f.request(t, t.Context(), kind)
	i, e := f.ds.prepareParentKidsFlag(kind, first, "target", []byte(body), nil)
	release()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(i.Close)
	retry, stop := f.request(t, t.Context(), kind)
	t.Cleanup(stop)
	if e = f.ds.bindParentKidsFlagRetry(kind, retry, i, "target", []byte(body), nil); e != nil {
		t.Fatal(e)
	}
	bound, e := f.ds.BoundParentKidsFlagRequest(retry, i)
	if e != nil {
		t.Fatal(e)
	}
	bound.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	if e = f.ds.VerifyParentKidsFlagPassword(bound, i); e != nil {
		t.Fatal(e)
	}
	return i, bound
}
func (f *kidsFlagPG) leases(t *testing.T, i *ParentKidsFlagIntent, r *http.Request) (*ParentKidsFlagCacheLease, *ParentKidsFlagCacheLease) {
	t.Helper()
	u, e := f.ds.BeginParentKidsFlagUsersLease(r.Context(), i, UserRevision{owner: f.ds, id: "target", version: i.state.tuple.profile.version})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { u.Retire() })
	var s *ParentKidsFlagCacheLease
	if i.state.kind == parentKidsFlagManagement && i.state.enabled {
		s, e = f.ds.BeginParentKidsFlagSettingsLease(r.Context(), i, u, i.state.settings.Revision)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { s.Retire() })
	}
	return u, s
}
func (f *kidsFlagPG) rows(t *testing.T) (map[string]json.RawMessage, string) {
	t.Helper()
	var raw []byte
	var settings string
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&raw); e != nil {
		t.Fatal(e)
	}
	if e := f.ds.pool.QueryRow(t.Context(), `SELECT COALESCE((SELECT to_jsonb(s)::text FROM user_settings s WHERE user_id='target'),'absent')`).Scan(&settings); e != nil {
		t.Fatal(e)
	}
	var row map[string]json.RawMessage
	if json.Unmarshal(raw, &row) != nil {
		t.Fatal("decode own row")
	}
	return row, settings
}
func TestParentKidsFlagDecoderPreservesDistinctEntries(t *testing.T) {
	for _, kind := range []parentKidsFlagEntry{parentKidsFlagAPI, parentKidsFlagManagement} {
		for _, tc := range []struct {
			body string
			want bool
		}{{`null`, false}, {`{}`, false}, {`{"isKidsProfile":null}`, false}, {`{"ISkidSprofile":true} trailing`, true}, {`{"isKidsProfile":true,"isKidsProfile":null}`, true}, {`{"isKidsProfile":true,"isKidsProfile":false}`, false}} {
			got, e := parentKidsFlagDesired(kind, []byte(tc.body))
			if e != nil || got != tc.want {
				t.Fatal("existing bool decoder changed", kind, tc.body, e)
			}
		}
	}
	if _, e := parentKidsFlagDesired(parentKidsFlagAPI, []byte(`{"isKidsProfile":true,"future":1}`)); !errors.Is(e, ErrParentKidsFlagPayload) {
		t.Fatal("API unknown fields accepted")
	}
	if enabled, e := parentKidsFlagDesired(parentKidsFlagManagement, []byte(`{"isKidsProfile":true,"future":1}`)); e != nil || !enabled {
		t.Fatal("cookie unknown fields changed")
	}
	if _, e := parentKidsFlagDesired(parentKidsFlagAPI, bytes.Repeat([]byte(" "), ParentKidsFlagBodyLimit+1)); !errors.Is(e, ErrParentKidsFlagBodyTooLarge) {
		t.Fatal("body bound missing")
	}
}
func TestParentKidsFlagCompletePolicyAndCookieDefaults(t *testing.T) {
	for _, tc := range []struct {
		name     string
		kind     parentKidsFlagEntry
		enabled  bool
		settings string
	}{{"API enable", parentKidsFlagAPI, true, `{"display":{"simpleMode":false}}`}, {"cookie absent", parentKidsFlagManagement, true, ""}, {"cookie explicit false", parentKidsFlagManagement, true, `{"display":{"simpleMode":false,"future":{"n":9007199254740993}},"future":[1,2]}`}, {"cookie disable", parentKidsFlagManagement, false, `{"future":{"n":9007199254740993}}`}} {
		t.Run(tc.name, func(t *testing.T) {
			f := kidsFlagFixture(t)
			f.exec(t, `UPDATE users SET kids_mode='',kids_max_rating='custom-base',allow_live_takeover=true,allow_recording_interruption=true,kids_max_movie_rating='movie-x',kids_max_tv_rating='tv-x',kids_allowed_lists='["keep"]' WHERE id='target'`)
			if tc.settings != "" {
				f.exec(t, `INSERT INTO user_settings(user_id,settings)VALUES('target',$1)`, tc.settings)
			}
			before, settingsBefore := f.rows(t)
			body := `{"isKidsProfile":false}`
			if tc.enabled {
				body = `{"isKidsProfile":true}`
			}
			i, r := f.ready(t, tc.kind, body)
			u, s := f.leases(t, i, r)
			receipt, e := f.ds.CommitParentKidsFlag(i)
			if e != nil {
				t.Fatal(e)
			}
			post, e := f.ds.StageParentKidsFlagUsers(u, receipt)
			if e != nil {
				t.Fatal(e)
			}
			if s != nil {
				if _, e = f.ds.StageParentKidsFlagSettings(s, receipt); e != nil {
					t.Fatal(e)
				}
			}
			if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, u, s); e != nil {
				t.Fatal(e)
			}
			if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, u, s); e == nil {
				t.Fatal("receipt replay")
			}
			if post.User.IsKidsProfile != tc.enabled || post.User.KidsMaxMovieRating != "movie-x" || post.User.KidsMaxTVRating != "tv-x" {
				t.Fatal("full flag or separate ratings changed")
			}
			if tc.enabled && (post.User.AllowLiveTakeover || post.User.AllowRecordingInterruption || post.User.KidsMode != "rating" || post.User.KidsMaxRating != "G") {
				t.Fatal("enable policy incomplete")
			}
			if !tc.enabled && (!post.User.AllowLiveTakeover || !post.User.AllowRecordingInterruption || post.User.KidsMode != "" || post.User.KidsMaxRating != "" || post.User.KidsAllowedLists != nil) {
				t.Fatal("disable policy changed")
			}
			after, settingsAfter := f.rows(t)
			for _, key := range []string{"is_kids_profile", "kids_mode", "kids_max_rating", "kids_allowed_lists", "allow_live_takeover", "allow_recording_interruption", "updated_at"} {
				delete(before, key)
				delete(after, key)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("unrelated user field changed")
			}
			if tc.kind == parentKidsFlagAPI || !tc.enabled || strings.Contains(tc.settings, `"simpleMode":false`) {
				if settingsBefore != settingsAfter {
					t.Fatal("unneeded settings write")
				}
			}
			if tc.name == "cookie absent" && !strings.Contains(settingsAfter, `"simpleMode": true`) {
				t.Fatal("missing simple mode default")
			}
		})
	}
}
func TestParentKidsFlagOriginalSettingsABAAndAuthChangesDeny(t *testing.T) {
	for name, sql := range map[string]string{"settings absent ABA": `INSERT INTO user_settings(user_id,settings)VALUES('target','{}');DELETE FROM user_settings WHERE user_id='target'`, "target ABA": `UPDATE users SET name=name WHERE id='target'`, "role": `UPDATE accounts SET is_master=NOT is_master WHERE id='actor'`, "PIN": `UPDATE users SET pin_hash='malformed' WHERE id='target'`, "session": `UPDATE sessions SET client_id='changed' WHERE token='synthetic-parent-token'`} {
		t.Run(name, func(t *testing.T) {
			f := kidsFlagFixture(t)
			i, r := f.ready(t, parentKidsFlagManagement, `{"isKidsProfile":true}`)
			f.leases(t, i, r)
			if name == "role" {
				sql = `UPDATE accounts SET is_master=NOT is_master WHERE id='` + f.session.AccountID + `'`
			}
			if name == "session" {
				sql = `UPDATE sessions SET client_id='changed' WHERE token='` + f.session.Token + `'`
			}
			f.exec(t, sql)
			before, settings := f.rows(t)
			if receipt, e := f.ds.CommitParentKidsFlag(i); e == nil || receipt != nil {
				t.Fatal("changed original state committed")
			}
			after, actual := f.rows(t)
			if !reflect.DeepEqual(before, after) || actual != settings {
				t.Fatal("denied mutation wrote")
			}
		})
	}
}
func TestParentKidsFlagLeaseAndReceiptIdentityLateCancel(t *testing.T) {
	f := kidsFlagFixture(t)
	i, r := f.ready(t, parentKidsFlagManagement, `{"isKidsProfile":true}`)
	if _, e := f.ds.BeginParentKidsFlagSettingsLease(r.Context(), i, nil, i.state.settings.Revision); e == nil {
		t.Fatal("settings acquired before Users")
	}
	u, s := f.leases(t, i, r)
	copyLease := *u
	if _, e := f.ds.StageParentKidsFlagUsers(&copyLease, nil); e == nil {
		t.Fatal("copied lease used")
	}
	receipt, e := f.ds.CommitParentKidsFlag(i)
	if e != nil {
		t.Fatal(e)
	}
	copyReceipt := *receipt
	if _, e = f.ds.StageParentKidsFlagUsers(u, &copyReceipt); e == nil {
		t.Fatal("copied receipt accepted")
	}
	if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, u, s); e == nil {
		t.Fatal("unprepared pair taken")
	}
	if _, e = f.ds.StageParentKidsFlagUsers(u, receipt); e != nil {
		t.Fatal(e)
	}
	if _, e = f.ds.StageParentKidsFlagSettings(s, receipt); e != nil {
		t.Fatal(e)
	}
	i.state.cancel()
	i.Close()
	if e = f.ds.TakeParentKidsFlagReceipt(i, receipt, u, s); e != nil || !u.Taken() || !s.Taken() {
		t.Fatal("known commit could not complete historical pair", e)
	}
	if !u.Retire() || !s.Retire() {
		t.Fatal("accepted close lost pair")
	}
}
func TestParentKidsFlagRetainedSettingsBudget(t *testing.T) {
	for name, raw := range map[string]string{"entry descriptors": `{"liveTV":{"favoriteStationIds":[` + strings.Repeat(`"",`, 4999) + `""]}}`, "raw strings": `{"future":"` + strings.Repeat("x", 65000) + `"}`} {
		t.Run(name, func(t *testing.T) {
			f := kidsFlagFixture(t)
			f.exec(t, `INSERT INTO user_settings(user_id,settings)VALUES('target',$1)`, raw)
			r, stop := f.request(t, t.Context(), parentKidsFlagManagement)
			defer stop()
			if i, e := f.ds.PrepareParentKidsFlagManagement(r, "target", []byte(`{"isKidsProfile":true}`), nil); e == nil || i != nil {
				t.Fatal("oversized original settings retained")
			}
		})
	}
}
func TestParentKidsFlagActualOriginalBcryptOutsideLocks(t *testing.T) {
	f := kidsFlagFixture(t)
	r, stop := f.request(t, t.Context(), parentKidsFlagAPI)
	defer stop()
	body := []byte(`{"isKidsProfile":true}`)
	i, e := f.ds.PrepareParentKidsFlagAPI(r, "target", body, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer i.Close()
	if e = f.ds.BindParentKidsFlagAPIRetry(r, i, "target", body, nil); e != nil {
		t.Fatal(e)
	}
	r, e = f.ds.BoundParentKidsFlagRequest(r, i)
	if e != nil {
		t.Fatal(e)
	}
	r.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	entered, release := make(chan struct{}), make(chan struct{})
	actual := i.state.compare
	i.state.compare = func(hash, pin []byte) error { close(entered); <-release; return actual(hash, pin) }
	done := make(chan error, 1)
	go func() { done <- f.ds.VerifyParentKidsFlagPassword(r, i) }()
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, changed := f.ds.pool.Exec(ctx, `UPDATE accounts SET username=username WHERE id=$1`, f.session.AccountID)
	close(release)
	if e = <-done; e != nil {
		t.Fatal("real original bcrypt failed", e)
	}
	if changed != nil {
		t.Fatal("bcrypt held SQL actor lock", changed)
	}
	f.leases(t, i, r)
	if receipt, e := f.ds.CommitParentKidsFlag(i); e == nil || receipt != nil {
		t.Fatal("changed actor adopted after original bcrypt")
	}
}

// This tracer pauses the actual first RR authority SELECT, after the separate
// original-frame check. No production callback or timing assumption is used.
type kidsFlagQueryBarrier struct {
	armed            atomic.Bool
	entered, release chan struct{}
}

func (b *kidsFlagQueryBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "FROM profile_unlocks g") && b.armed.CompareAndSwap(true, false) {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*kidsFlagQueryBarrier) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func TestParentKidsFlagPrepareCannotAdoptReplacementAuthFrame(t *testing.T) {
	for _, kind := range []parentKidsFlagEntry{parentKidsFlagAPI, parentKidsFlagManagement} {
		for _, what := range []string{"actor credential", "session row"} {
			t.Run(fmt.Sprint(kind)+"/"+what, func(t *testing.T) {
				f := kidsFlagFixture(t)
				barrier := &kidsFlagQueryBarrier{entered: make(chan struct{}), release: make(chan struct{})}
				cfg := f.cfg.Copy()
				cfg.ConnConfig.Tracer = barrier
				pool, e := pgxpool.NewWithConfig(t.Context(), cfg)
				if e != nil {
					t.Fatal(e)
				}
				defer pool.Close()
				ds := &DataStore{pool: pool}
				raw := f.parentPGFixture.request(t.Context())
				raw.Method = http.MethodPut
				raw.URL.Path = "/api/users/target/kids-profile"
				checked, stop, e := ds.AuthenticateProfilePinRequest(raw, f.session.Token, f.session.ClientID)
				if e != nil {
					t.Fatal(e)
				}
				defer stop()
				barrier.armed.Store(true)
				type result struct {
					i *ParentKidsFlagIntent
					e error
				}
				done := make(chan result, 1)
				go func() {
					i, e := ds.prepareParentKidsFlag(kind, checked, "target", []byte(`{"isKidsProfile":true}`), nil)
					done <- result{i, e}
				}()
				select {
				case <-barrier.entered:
				case <-time.After(3 * time.Second):
					close(barrier.release)
					<-done
					t.Fatal("actual RR authority query not observed")
				}
				if what == "actor credential" {
					hash, e := bcrypt.GenerateFromPassword([]byte("replacement synthetic password"), bcrypt.MinCost)
					if e != nil {
						close(barrier.release)
						<-done
						t.Fatal(e)
					}
					f.exec(t, `UPDATE accounts SET password_hash=$1 WHERE id=$2`, string(hash), f.session.AccountID)
				} else {
					f.exec(t, `UPDATE sessions SET user_agent='replacement' WHERE token=$1`, f.session.Token)
				}
				close(barrier.release)
				got := <-done
				if got.i != nil {
					got.i.Close()
				}
				if got.e == nil || got.i != nil {
					t.Fatal("RR selected replacement row became original authority")
				}
			})
		}
	}
}
