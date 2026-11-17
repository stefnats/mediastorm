package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

type listWritesFixture struct{ *removalPGFixture }

func listWritesPG(t *testing.T) *listWritesFixture {
	t.Helper()
	return &listWritesFixture{removalFixture(t)}
}
func (f *listWritesFixture) request(t *testing.T, ctx context.Context, action parentListWriteAction, body []byte) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Method = http.MethodPost
	if action == parentListReplace {
		r.Method = http.MethodPut
	}
	r.URL.Path = "/api/users/target/kids/lists"
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func (f *listWritesFixture) prepare(t *testing.T, action parentListWriteAction, body []byte) (*ParentListWriteIntent, *http.Request) {
	t.Helper()
	first, release := f.request(t, t.Context(), action, body)
	intent, err := f.ds.prepareParentListWrite(action, first, "target", body, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	retry, stop := f.request(t, t.Context(), action, body)
	t.Cleanup(stop)
	retry.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	return intent, retry
}
func (f *listWritesFixture) verify(t *testing.T, action parentListWriteAction, intent *ParentListWriteIntent, retry *http.Request, body []byte) *http.Request {
	t.Helper()
	if err := f.ds.bindParentListWriteRetry(action, retry, intent, "target", body, nil); err != nil {
		t.Fatal(err)
	}
	bound, err := f.ds.BoundParentListWriteRequest(retry, intent)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ds.VerifyParentListWritePassword(bound, intent); err != nil {
		t.Fatal(err)
	}
	return bound
}
func (f *listWritesFixture) revision(t *testing.T) string {
	t.Helper()
	var revision string
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT `+userWriteVersion+` FROM users u WHERE id='target'`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	return revision
}
func TestParentListWritesExistingSingleDecoderSemantics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action parentListWriteAction
		body   string
		want   []string
	}{
		{"add trimmed", parentListAdd, `{"url":" x "}`, []string{"x"}},
		{"add string then null", parentListAdd, `{"url":" x ","url":null}`, []string{"x"}},
		{"add folded and trailing", parentListAdd, `{"URL":"x"} ignored trailing`, []string{"x"}},
		{"replace root null", parentListReplace, `null`, []string{}},
		{"replace missing", parentListReplace, `{}`, []string{}},
		{"replace field null", parentListReplace, `{"lists":null}`, []string{}},
		{"replace null elements and order", parentListReplace, `{"lists":[" a ",null,"","b","a"]}`, []string{"a", "b", "a"}},
		{"replace duplicate fields", parentListReplace, `{"lists":["first"],"lists":[" last ","last"]} {}`, []string{"last", "last"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parentListWriteDesired(tc.action, []byte(tc.body))
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatal("existing one-Decode behavior changed", err)
			}
		})
	}
	for _, body := range []string{`null`, `{}`, `{"url":null}`, `{"url":" "}`, `{"url":[]}`, `{"url":"x","unknown":true}`} {
		if _, err := parentListWriteDesired(parentListAdd, []byte(body)); !errors.Is(err, ErrParentListWritePayload) {
			t.Fatal("invalid add body accepted")
		}
	}
	if _, err := parentListWriteDesired(parentListReplace, bytes.Repeat([]byte(" "), ParentListWriteBodyLimit+1)); !errors.Is(err, ErrParentListWriteBodyTooLarge) {
		t.Fatal("PG raw capture bound missing")
	}
}
func TestParentListWritesActualAddReplaceAndSealedReceipt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		action parentListWriteAction
		body   string
		want   string
	}{
		{"add", parentListAdd, `{"url":" new "}`, `["PRIVATE_LIST","new"]`},
		{"replace", parentListReplace, `{"lists":[" x ",null,"x"," y "]}`, `["x","x","y"]`},
		{"replace clear", parentListReplace, `{}`, `[]`},
		{"replace identical still updates", parentListReplace, `{"lists":["PRIVATE_LIST"]}`, `["PRIVATE_LIST"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := listWritesPG(t)
			body := []byte(tc.body)
			intent, retry := f.prepare(t, tc.action, body)
			bound := f.verify(t, tc.action, intent, retry, body)
			before := f.revision(t)
			receipt, err := f.ds.commitParentListWrite(tc.action, intent)
			if err != nil || receipt == nil || f.lists(t) != tc.want || f.revision(t) == before {
				t.Fatal("targeted list write failed", err)
			}
			if raw, _ := json.Marshal(receipt); string(raw) != "null" || fmt.Sprintf("%+v", receipt) != "ParentListWriteReceipt(opaque)" {
				t.Fatal("receipt exposes private state")
			}
			expected := UserRevision{owner: f.ds, id: "target", version: before}
			record, err := f.ds.acknowledgeParentListWrite(tc.action, bound.Context(), intent, receipt, expected)
			if err != nil || record.User.ID != "target" || record.Revision.version != f.revision(t) {
				t.Fatal("known original commit acknowledgement failed", err)
			}
			if _, err = f.ds.acknowledgeParentListWrite(tc.action, bound.Context(), intent, receipt, expected); err == nil {
				t.Fatal("receipt acknowledgement replayed")
			}
		})
	}
}
func TestParentListWritesDuplicateAddHasNoUsersUpdate(t *testing.T) {
	f := listWritesPG(t)
	body := []byte(`{"url":" PRIVATE_LIST "}`)
	intent, retry := f.prepare(t, parentListAdd, body)
	bound := f.verify(t, parentListAdd, intent, retry, body)
	before := f.revision(t)
	var beforeRow, afterRow []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&beforeRow); err != nil {
		t.Fatal(err)
	}
	// Lock-epoch generation0 housekeeping is allowed. Only an actual users
	// UPDATE is forbidden; a metadata INSERT is not misreported as no DB IO.
	f.exec(t, `CREATE FUNCTION deny_list_user_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'unexpected users update'; END $$; CREATE TRIGGER deny_list_user_update BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION deny_list_user_update()`)
	receipt, err := f.ds.CommitParentListAdd(intent)
	if err != nil || receipt == nil || f.revision(t) != before {
		t.Fatal("confirmed duplicate add was not a genuine profile noop", err)
	}
	if err = f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&afterRow); err != nil || !bytes.Equal(beforeRow, afterRow) {
		t.Fatal("duplicate add changed original target fields or UpdatedAt", err)
	}
	expected := UserRevision{owner: f.ds, id: "target", version: before}
	copyReceipt := *receipt
	copyIntent := *intent
	wrongStore := &DataStore{pool: f.ds.pool}
	beforeIO := f.ds.pool.Stat().AcquireCount()
	for _, attempt := range []func() error{
		func() error {
			_, e := f.ds.AcknowledgeParentListReplace(bound.Context(), intent, receipt, expected)
			return e
		},
		func() error {
			_, e := wrongStore.AcknowledgeParentListAdd(bound.Context(), intent, receipt, expected)
			return e
		},
		func() error {
			_, e := f.ds.AcknowledgeParentListAdd(bound.Context(), intent, &copyReceipt, expected)
			return e
		},
		func() error {
			_, e := f.ds.AcknowledgeParentListAdd(bound.Context(), &copyIntent, receipt, expected)
			return e
		},
		func() error {
			_, e := f.ds.AcknowledgeParentListAdd(bound.Context(), intent, &ParentListWriteReceipt{}, expected)
			return e
		},
	} {
		if attempt() == nil {
			t.Fatal("foreign/copied/actionless receipt adopted")
		}
	}
	if f.ds.pool.Stat().AcquireCount() != beforeIO {
		t.Fatal("invalid receipt acknowledgement performed SQL")
	}
	record, err := f.ds.AcknowledgeParentListAdd(bound.Context(), intent, receipt, expected)
	if err != nil || record.Revision != expected {
		t.Fatal("noop acknowledgement advanced the original revision", err)
	}
}
func TestParentListWritesBodyAndActionCannotRebindBeforeIO(t *testing.T) {
	for _, action := range []parentListWriteAction{parentListAdd, parentListReplace} {
		t.Run(fmt.Sprint(action), func(t *testing.T) {
			f := listWritesPG(t)
			body := []byte(`{"url":"x"}`)
			if action == parentListReplace {
				body = []byte(`{"lists":["x"]}`)
			}
			intent, retry := f.prepare(t, action, body)
			wrong := parentListAdd
			if action == parentListAdd {
				wrong = parentListReplace
			}
			before := f.ds.pool.Stat().AcquireCount()
			if err := f.ds.bindParentListWriteRetry(wrong, retry, intent, "target", body, nil); err == nil {
				t.Fatal("wrong private action consumed prepared intent")
			}
			changed := append(bytes.Clone(body), ' ')
			if err := f.ds.bindParentListWriteRetry(action, retry, intent, "target", changed, nil); err == nil {
				t.Fatal("semantic-equal raw body change rebound")
			}
			if f.ds.pool.Stat().AcquireCount() != before {
				t.Fatal("invalid body or action performed SQL")
			}
		})
	}
}
func TestParentListWritesKnownReceiptIsHistoricalAfterCancel(t *testing.T) {
	f := listWritesPG(t)
	body := []byte(`{"url":"new"}`)
	intent, retry := f.prepare(t, parentListAdd, body)
	ctx, cancel := context.WithCancel(retry.Context())
	retry = retry.WithContext(ctx)
	bound := f.verify(t, parentListAdd, intent, retry, body)
	expected := UserRevision{owner: f.ds, id: "target", version: intent.state.tuple.profile.version}
	receipt, err := f.ds.CommitParentListAdd(intent)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	record, err := f.ds.AcknowledgeParentListAdd(bound.Context(), intent, receipt, expected)
	if err != nil || record.User.ID != "target" || bound.Context().Err() == nil {
		t.Fatal("known historical bookkeeping receipt was lost", err)
	}
	// This is not permission to publish: the actual service returns ctx.Err()
	// after applying its narrow known projection, and no new SQL is issued.
}
func TestParentListWritesFreshTupleChangeDeniesDuplicateAndReplace(t *testing.T) {
	for _, action := range []parentListWriteAction{parentListAdd, parentListReplace} {
		for _, statement := range []string{
			`UPDATE users SET kids_allowed_lists=kids_allowed_lists WHERE id='target'`,
			`UPDATE accounts SET password_hash=password_hash WHERE id='actor'`,
			`UPDATE users SET pin_hash='changed' WHERE id='target'`,
			`UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second'`,
		} {
			t.Run(fmt.Sprintf("%d/%d", action, len(statement)), func(t *testing.T) {
				f := listWritesPG(t)
				body := []byte(`{"url":"PRIVATE_LIST"}`)
				if action == parentListReplace {
					body = []byte(`{"lists":["replacement"]}`)
				}
				intent, retry := f.prepare(t, action, body)
				f.verify(t, action, intent, retry, body)
				f.exec(t, statement)
				before := f.revision(t)
				if receipt, err := f.ds.commitParentListWrite(action, intent); err == nil || receipt != nil || f.revision(t) != before {
					t.Fatal("changed original tuple acquired write/noop receipt", err)
				}
			})
		}
	}
}
func TestParentListWritesOriginalBudgetBoundsActualFrameSQL(t *testing.T) {
	for _, action := range []parentListWriteAction{parentListAdd, parentListReplace} {
		t.Run(fmt.Sprint(action), func(t *testing.T) {
			f := listWritesPG(t)
			body := []byte(`{"url":"x"}`)
			if action == parentListReplace {
				body = []byte(`{"lists":["x"]}`)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			first, release := f.request(t, ctx, action, body)
			intent, err := f.ds.prepareParentListWrite(action, first, "target", body, nil)
			release()
			if err != nil {
				t.Fatal(err)
			}
			defer intent.Close()
			retry, stop := f.request(t, t.Context(), action, body)
			defer stop()
			tx, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackPinTx(tx, t.Context())
			if _, err = tx.Exec(t.Context(), `LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- f.ds.bindParentListWriteRetry(action, retry, intent, "target", body, nil) }()
			joined := false
			defer func() {
				rollbackPinTx(tx, t.Context())
				if !joined {
					<-done
				}
			}()
			f.waitBlocked(t)
			<-ctx.Done()
			select {
			case err = <-done:
				joined = true
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("original frame deadline lost", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("frame SQL exceeded original attempt lifetime")
			}
			if retry.Context().Err() != nil {
				t.Fatal("outer request expired instead of original attempt")
			}
		})
	}
}

func TestParentListWritesActualBlockedCommitCancellation(t *testing.T) {
	f := listWritesPG(t)
	body := []byte(`{"lists":["new"]}`)
	intent, retry := f.prepare(t, parentListReplace, body)
	ctx, cancel := context.WithCancel(retry.Context())
	defer cancel()
	f.verify(t, parentListReplace, intent, retry.WithContext(ctx), body)
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(tx, t.Context())
	if _, err = tx.Exec(t.Context(), `SELECT id FROM users WHERE id='target' FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, e := f.ds.CommitParentListReplace(intent); done <- e }()
	joined := false
	defer func() {
		if !joined {
			cancel()
			rollbackPinTx(tx, t.Context())
			<-done
		}
	}()
	f.waitBlocked(t)
	cancel()
	err = <-done
	joined = true
	if !errors.Is(err, context.Canceled) {
		t.Fatal("actual blocked commit lost cancellation", err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if f.lists(t) != `["PRIVATE_LIST"]` {
		t.Fatal("cancelled list transaction changed membership")
	}
}
func TestParentListWritesActualOriginalBcryptOutsideLocks(t *testing.T) {
	f := listWritesPG(t)
	body := []byte(`{"url":"new"}`)
	intent, retry := f.prepare(t, parentListAdd, body)
	if err := f.ds.BindParentListAddRetry(retry, intent, "target", body, nil); err != nil {
		t.Fatal(err)
	}
	bound, err := f.ds.BoundParentListWriteRequest(retry, intent)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	intent.state.compare = func(hash, password []byte) error {
		close(entered)
		<-release
		return bcrypt.CompareHashAndPassword(hash, password)
	}
	done := make(chan error, 1)
	go func() { done <- f.ds.VerifyParentListWritePassword(bound, intent) }()
	<-entered
	joined := false
	released := false
	defer func() {
		if !released {
			close(release)
		}
		if !joined {
			<-done
		}
	}()
	tx, err := f.ds.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPinTx(tx, t.Context())
	if _, err = tx.Exec(t.Context(), `SELECT id FROM accounts WHERE id='actor' FOR UPDATE NOWAIT`); err != nil {
		t.Fatal("bcrypt holds original actor SQL lock", err)
	}
	if _, err = tx.Exec(t.Context(), `SELECT id FROM users WHERE id='target' FOR UPDATE NOWAIT`); err != nil {
		t.Fatal("bcrypt holds target SQL lock", err)
	}
	if _, err = tx.Exec(t.Context(), `UPDATE accounts SET password_hash=password_hash WHERE id='actor'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(release)
	released = true
	err = <-done
	joined = true
	if err != nil {
		t.Fatal("actual original bcrypt failed", err)
	}
	if receipt, e := f.ds.CommitParentListAdd(intent); e == nil || receipt != nil {
		t.Fatal("changed actor acquired a commit receipt")
	}
}
func TestParentListWritesActualLostCommitAckReturnsNoReceipt(t *testing.T) {
	f := listWritesPG(t)
	warm, release := f.request(t, t.Context(), parentListAdd, []byte(`{"url":"new"}`))
	_ = warm
	release()
	cfg := f.cfg.Copy()
	var dropped atomic.Bool
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		c, e := (&net.Dialer{}).DialContext(ctx, network, address)
		if e != nil {
			return nil, e
		}
		return &parentDropCommit{Conn: c, dropped: &dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ds := &DataStore{pool: pool}
	body := []byte(`{"url":"new"}`)
	r := httptest.NewRequest(http.MethodPost, "http://synthetic.invalid/api/users/target/kids/lists", bytes.NewReader(body)).WithContext(t.Context())
	r.Header.Set("X-Client-ID", f.session.ClientID)
	r, stop, err := ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	intent, err := ds.PrepareParentListAdd(r, "target", body, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer intent.Close()
	if err = ds.BindParentListAddRetry(r, intent, "target", body, nil); err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentListWriteRequest(r, intent)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	if err = ds.VerifyParentListWritePassword(r, intent); err != nil {
		t.Fatal(err)
	}
	if receipt, e := ds.CommitParentListAdd(intent); receipt != nil || !errors.Is(e, ErrParentListWriteUnknown) || !dropped.Load() {
		t.Fatal("lost actual commit ACK returned a receipt", e)
	}
	if f.lists(t) != `["PRIVATE_LIST","new"]` {
		t.Fatal("lost ACK did not follow actual accepted list update")
	}
	if receipt, e := ds.CommitParentListAdd(intent); receipt != nil || e == nil {
		t.Fatal("unknown commit replayed")
	}
}

func TestParentListWritesRetainedOriginalDescriptorsRespectBudget(t *testing.T) {
	for _, action := range []string{"add", "replace", "removal"} {
		for _, payload := range []string{"empty descriptors", "large string bytes", "small valid"} {
			t.Run(action+"/"+payload, func(t *testing.T) {
				f := listWritesPG(t)
				lists := []string{"small"}
				switch payload {
				case "empty descriptors":
					lists = make([]string, 5000)
				case "large string bytes":
					lists = []string{strings.Repeat("x", 40000)}
				}
				stored, err := json.Marshal(lists)
				if err != nil {
					t.Fatal(err)
				}
				f.exec(t, `UPDATE users SET kids_allowed_lists=$1::jsonb WHERE id='target'`, string(stored))
				kind := parentListAdd
				body := []byte(`{"url":"new"}`)
				if action == "replace" {
					kind = parentListReplace
					body = []byte(`{"lists":["new"]}`)
				}
				r, release := f.request(t, t.Context(), kind, body)
				defer release()
				var accepted bool
				if action == "removal" {
					r.Method = http.MethodDelete
					r.URL.RawQuery = "url=new"
					intent, e := f.ds.PrepareParentListRemoval(r, "target", "new", nil)
					err = e
					accepted = intent != nil
					if intent != nil {
						defer intent.Close()
					}
					if !accepted && !errors.Is(err, ErrParentListRemovalUnavailable) {
						t.Fatal("unexpected removal budget error", err)
					}
				} else {
					intent, e := f.ds.prepareParentListWrite(kind, r, "target", body, nil)
					err = e
					accepted = intent != nil
					if intent != nil {
						defer intent.Close()
					}
					if !accepted && !errors.Is(err, ErrParentListWriteUnavailable) {
						t.Fatal("unexpected write budget error", err)
					}
				}
				if payload == "small valid" {
					if !accepted || err != nil {
						t.Fatal("small valid original list rejected", err)
					}
				} else if accepted || err == nil {
					t.Fatal("retained original list payload exceeded the confirmation budget")
				}
			})
		}
	}
}
