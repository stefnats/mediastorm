package datastore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type ratingPGFixture struct{ *parentPGFixture }

func ratingPG(t *testing.T) *ratingPGFixture {
	t.Helper()
	return &ratingPGFixture{removalFixture(t).parentPGFixture}
}
func ratingTestBody(kind parentRatingAction) []byte {
	if kind == parentRatingPair {
		return []byte(`{"movieRating":" movie-x ","tvRating":" tv-y "}`)
	}
	return []byte(`{"rating":" regional-x "}`)
}
func ratingTestPath(kind parentRatingAction) string {
	switch kind {
	case parentRatingGeneric:
		return "/api/users/target/kids/rating"
	case parentRatingMovie:
		return "/api/users/target/kids/rating/movie"
	case parentRatingTV:
		return "/api/users/target/kids/rating/tv"
	default:
		return "/api/users/target/kids/ratings"
	}
}
func (f *ratingPGFixture) request(t *testing.T, ctx context.Context, ds *DataStore, kind parentRatingAction, body []byte) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Method = http.MethodPut
	r.URL.Path = ratingTestPath(kind)
	r.Header.Set("X-Client-ID", f.session.ClientID)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	checked, release, err := ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func (f *ratingPGFixture) prepare(t *testing.T, kind parentRatingAction, body []byte) (*ParentRatingIntent, *http.Request) {
	t.Helper()
	first, release := f.request(t, t.Context(), f.ds, kind, body)
	intent, err := f.ds.prepareParentRating(kind, first, "target", body, nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(intent.Close)
	retry, stop := f.request(t, t.Context(), f.ds, kind, body)
	t.Cleanup(stop)
	retry.Header.Set(profilePinParentPasswordHeader, " \t"+parentTestPassword+" \n")
	return intent, retry
}
func (f *ratingPGFixture) verify(t *testing.T, kind parentRatingAction, i *ParentRatingIntent, r *http.Request, body []byte) *http.Request {
	t.Helper()
	if err := f.ds.bindParentRatingRetry(kind, r, i, "target", body, nil); err != nil {
		t.Fatal(err)
	}
	bound, err := f.ds.BoundParentRatingRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ds.VerifyParentRatingPassword(bound, i); err != nil {
		t.Fatal(err)
	}
	return bound
}
func (f *ratingPGFixture) row(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	var raw []byte
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT to_jsonb(u) FROM users u WHERE id='target'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var row map[string]json.RawMessage
	if err := json.Unmarshal(raw, &row); err != nil {
		t.Fatal(err)
	}
	return row
}
func TestParentRatingExistingScalarAndNewPairDecoder(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind parentRatingAction
		body string
		want []string
	}{
		{"scalar rootnull", parentRatingGeneric, `null`, []string{""}},
		{"scalar missing", parentRatingMovie, `{}`, []string{""}},
		{"scalar null", parentRatingTV, `{"rating":null}`, []string{""}},
		{"scalar string then null", parentRatingGeneric, `{"rating":" x ","rating":null}`, []string{"x"}},
		{"scalar casefold trailing", parentRatingMovie, `{"RATING":" regional-x "} trailing`, []string{"regional-x"}},
		{"pair empty explicit", parentRatingPair, `{"movieRating":"","tvRating":""}`, []string{"", ""}},
		{"pair null then string", parentRatingPair, `{"movieRating":null,"movieRating":" x ","tvRating":" y "}`, []string{"x", "y"}},
		{"pair casefold trailing", parentRatingPair, `{"MOVIERATING":"lower","TVRATING":"lower-tv"} []`, []string{"lower", "lower-tv"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parentRatingDesired(tc.kind, []byte(tc.body))
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatal("decoder compatibility changed", err)
			}
		})
	}
	for _, body := range []string{`null`, `{}`, `{"movieRating":"x"}`, `{"movieRating":null,"tvRating":"x"}`, `{"movieRating":"x","movieRating":null,"tvRating":"x"}`, `{"movieRating":5,"tvRating":"x"}`, `{"movieRating":"x","tvRating":"x","extra":true}`} {
		if _, err := parentRatingDesired(parentRatingPair, []byte(body)); !errors.Is(err, ErrParentRatingPayload) {
			t.Fatal("incomplete/nonstring final pair accepted")
		}
	}
	if _, err := parentRatingDesired(parentRatingPair, bytes.Repeat([]byte(" "), ParentRatingBodyLimit+1)); !errors.Is(err, ErrParentRatingBodyTooLarge) {
		t.Fatal("raw body cap missing")
	}
}
func TestParentRatingActualFixedFieldsAndReceipt(t *testing.T) {
	for _, kind := range []parentRatingAction{parentRatingGeneric, parentRatingMovie, parentRatingTV, parentRatingPair} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			f := ratingPG(t)
			f.exec(t, `UPDATE users SET kids_max_rating='legacy',kids_max_movie_rating='old-movie',kids_max_tv_rating='old-tv' WHERE id='target'`)
			body := ratingTestBody(kind)
			intent, retry := f.prepare(t, kind, body)
			bound := f.verify(t, kind, intent, retry, body)
			before := f.row(t)
			expected := UserRevision{owner: f.ds, id: "target", version: intent.state.tuple.profile.version}
			receipt, err := f.ds.commitParentRating(kind, intent)
			if err != nil || receipt == nil {
				t.Fatal(err)
			}
			after := f.row(t)
			if kind == parentRatingPair {
				if string(after["kids_max_movie_rating"]) != `"movie-x"` || string(after["kids_max_tv_rating"]) != `"tv-y"` {
					t.Fatal("pair desired fields not stored")
				}
			} else {
				field := "kids_max_rating"
				if kind == parentRatingMovie {
					field = "kids_max_movie_rating"
				}
				if kind == parentRatingTV {
					field = "kids_max_tv_rating"
				}
				if string(after[field]) != `"regional-x"` {
					t.Fatal("scalar desired field not stored")
				}
			}
			fields := []string{"kids_max_rating"}
			if kind == parentRatingMovie {
				fields = []string{"kids_max_movie_rating"}
			}
			if kind == parentRatingTV {
				fields = []string{"kids_max_tv_rating"}
			}
			if kind == parentRatingPair {
				fields = []string{"kids_max_movie_rating", "kids_max_tv_rating"}
			}
			for _, field := range fields {
				delete(before, field)
				delete(after, field)
			}
			delete(before, "updated_at")
			delete(after, "updated_at")
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rating transaction changed other fields or generic fallback")
			}
			if raw, _ := json.Marshal(receipt); string(raw) != "null" || fmt.Sprintf("%+v", receipt) != "ParentRatingReceipt(opaque)" {
				t.Fatal("receipt exposes state")
			}
			wrong := parentRatingPair
			if kind == parentRatingPair {
				wrong = parentRatingMovie
			}
			copyReceipt := *receipt
			wrongStore := &DataStore{pool: f.ds.pool}
			ioBefore := f.ds.pool.Stat().AcquireCount()
			for _, call := range []func() error{
				func() error {
					_, e := f.ds.acknowledgeParentRating(wrong, bound.Context(), intent, receipt, expected)
					return e
				},
				func() error {
					_, e := wrongStore.acknowledgeParentRating(kind, bound.Context(), intent, receipt, expected)
					return e
				},
				func() error {
					_, e := f.ds.acknowledgeParentRating(kind, bound.Context(), intent, &copyReceipt, expected)
					return e
				},
			} {
				if call() == nil {
					t.Fatal("foreign/copied/cross-action receipt adopted")
				}
			}
			if f.ds.pool.Stat().AcquireCount() != ioBefore {
				t.Fatal("invalid receipt performed SQL")
			}
			record, err := f.ds.acknowledgeParentRating(kind, bound.Context(), intent, receipt, expected)
			if err != nil || record.User.ID != "target" || record.Revision == expected {
				t.Fatal("canonical ack or UpdatedAt revision missing", err)
			}
			if _, err = f.ds.acknowledgeParentRating(kind, bound.Context(), intent, receipt, expected); err == nil {
				t.Fatal("receipt replayed")
			}
		})
	}
}
func TestParentRatingBoundRawBodyAndActionBeforeIO(t *testing.T) {
	for _, kind := range []parentRatingAction{parentRatingGeneric, parentRatingMovie, parentRatingTV, parentRatingPair} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			f := ratingPG(t)
			body := ratingTestBody(kind)
			i, r := f.prepare(t, kind, body)
			wrong := parentRatingPair
			if kind == parentRatingPair {
				wrong = parentRatingGeneric
			}
			before := f.ds.pool.Stat().AcquireCount()
			if err := f.ds.bindParentRatingRetry(wrong, r, i, "target", body, nil); err == nil {
				t.Fatal("wrong action consumed intent")
			}
			if err := f.ds.bindParentRatingRetry(kind, r, i, "target", append(bytes.Clone(body), ' '), nil); err == nil {
				t.Fatal("semantic-equal raw body rebound")
			}
			if f.ds.pool.Stat().AcquireCount() != before {
				t.Fatal("invalid local bind performed SQL")
			}
		})
	}
}
func TestParentRatingFreshTupleChangesDeny(t *testing.T) {
	changes := []string{`UPDATE users SET name=name WHERE id='target'`, `UPDATE accounts SET password_hash=password_hash WHERE id='actor'`, `UPDATE users SET pin_hash='changed' WHERE id='target'`, `UPDATE users SET account_id='other' WHERE id='target'`, `UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second'`}
	for _, kind := range []parentRatingAction{parentRatingGeneric, parentRatingMovie, parentRatingTV, parentRatingPair} {
		for index, statement := range changes {
			t.Run(fmt.Sprintf("%d/%d", kind, index), func(t *testing.T) {
				f := ratingPG(t)
				body := ratingTestBody(kind)
				i, r := f.prepare(t, kind, body)
				f.verify(t, kind, i, r, body)
				f.exec(t, statement)
				before := f.row(t)
				if receipt, err := f.ds.commitParentRating(kind, i); err == nil || receipt != nil {
					t.Fatal("changed tuple committed rating")
				}
				if !reflect.DeepEqual(f.row(t), before) {
					t.Fatal("denied rating altered target")
				}
			})
		}
	}
}
func TestParentRatingOriginalDeadlineBoundsFrameSQL(t *testing.T) {
	for _, kind := range []parentRatingAction{parentRatingGeneric, parentRatingMovie, parentRatingTV, parentRatingPair} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			f := ratingPG(t)
			body := ratingTestBody(kind)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			first, release := f.request(t, ctx, f.ds, kind, body)
			i, err := f.ds.prepareParentRating(kind, first, "target", body, nil)
			release()
			if err != nil {
				t.Fatal(err)
			}
			defer i.Close()
			retry, stop := f.request(t, t.Context(), f.ds, kind, body)
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
			go func() { done <- f.ds.bindParentRatingRetry(kind, retry, i, "target", body, nil) }()
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
				t.Fatal("frame read exceeded original attempt")
			}
			if retry.Context().Err() != nil {
				t.Fatal("outer retry expired instead")
			}
		})
	}
}
func TestParentRatingPairBlockedCancelAndBcryptOutsideLocks(t *testing.T) {
	t.Run("blocked cancel", func(t *testing.T) {
		f := ratingPG(t)
		body := ratingTestBody(parentRatingPair)
		i, r := f.prepare(t, parentRatingPair, body)
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		f.verify(t, parentRatingPair, i, r.WithContext(ctx), body)
		tx, err := f.ds.pool.Begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer rollbackPinTx(tx, t.Context())
		if _, err = tx.Exec(t.Context(), `SELECT id FROM users WHERE id='target' FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, e := f.ds.CommitParentRatingPair(i); done <- e }()
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
			t.Fatal("cancel lost", err)
		}
		if err = tx.Rollback(t.Context()); err != nil {
			t.Fatal(err)
		}
		row := f.row(t)
		if string(row["kids_max_movie_rating"]) != `""` || string(row["kids_max_tv_rating"]) != `""` {
			t.Fatal("cancelled pair partially committed")
		}
	})
	t.Run("actual bcrypt", func(t *testing.T) {
		f := ratingPG(t)
		body := ratingTestBody(parentRatingPair)
		i, r := f.prepare(t, parentRatingPair, body)
		if err := f.ds.BindParentRatingPairRetry(r, i, "target", body, nil); err != nil {
			t.Fatal(err)
		}
		r, err := f.ds.BoundParentRatingRequest(r, i)
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		i.state.compare = func(hash, password []byte) error {
			close(entered)
			<-release
			return bcrypt.CompareHashAndPassword(hash, password)
		}
		done := make(chan error, 1)
		go func() { done <- f.ds.VerifyParentRatingPassword(r, i) }()
		<-entered
		joined, released := false, false
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
			t.Fatal("bcrypt holds actor SQL lock", err)
		}
		if _, err = tx.Exec(t.Context(), `UPDATE users SET name=name WHERE id='target'`); err != nil {
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
			t.Fatal(err)
		}
		if receipt, e := f.ds.CommitParentRatingPair(i); receipt != nil || e == nil {
			t.Fatal("changed target committed after actual bcrypt")
		}
	})
}
func TestParentRatingActualLostCommitAckNoReceipt(t *testing.T) {
	f := ratingPG(t)
	body := ratingTestBody(parentRatingPair)
	warm, release := f.request(t, t.Context(), f.ds, parentRatingPair, body)
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
	r := httptest.NewRequest(http.MethodPut, "http://synthetic.invalid"+ratingTestPath(parentRatingPair), bytes.NewReader(body)).WithContext(t.Context())
	r.Header.Set("X-Client-ID", f.session.ClientID)
	r, stop, err := ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	i, err := ds.PrepareParentRatingPair(r, "target", body, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if err = ds.BindParentRatingPairRetry(r, i, "target", body, nil); err != nil {
		t.Fatal(err)
	}
	r, err = ds.BoundParentRatingRequest(r, i)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	if err = ds.VerifyParentRatingPassword(r, i); err != nil {
		t.Fatal(err)
	}
	if receipt, e := ds.CommitParentRatingPair(i); receipt != nil || !errors.Is(e, ErrParentRatingUnknown) || !dropped.Load() {
		t.Fatal("lost actual ACK yielded success", e)
	}
	row := f.row(t)
	if string(row["kids_max_movie_rating"]) != `"movie-x"` || string(row["kids_max_tv_rating"]) != `"tv-y"` {
		t.Fatal("actual pair commit was not both fields")
	}
	if receipt, e := ds.CommitParentRatingPair(i); receipt != nil || e == nil {
		t.Fatal("unknown pair replayed")
	}
}
func TestParentRatingSnapshotDesiredStringAccounting(t *testing.T) {
	f := ratingPG(t)
	body := []byte(`{"movieRating":"` + strings.Repeat("x", 15000) + `","tvRating":""}`)
	r, release := f.request(t, t.Context(), f.ds, parentRatingPair, body)
	defer release()
	i, err := f.ds.PrepareParentRatingPair(r, "target", body, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if i.state.bytes < len(body)+len(i.state.desired[0])+len(i.state.desired)*16 {
		t.Fatal("retained desired payload missing from accounting")
	}
}

func TestParentRatingPairSQLFailureChangesNeitherField(t *testing.T) {
	f := ratingPG(t)
	f.exec(t, `UPDATE users SET kids_max_movie_rating='old-movie',kids_max_tv_rating='old-tv',kids_max_rating='legacy' WHERE id='target'`)
	body := ratingTestBody(parentRatingPair)
	intent, retry := f.prepare(t, parentRatingPair, body)
	f.verify(t, parentRatingPair, intent, retry, body)
	before := f.row(t)
	f.exec(t, `CREATE FUNCTION deny_rating_pair() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kids_max_tv_rating='tv-y' THEN RAISE EXCEPTION 'synthetic pair write failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER deny_rating_pair BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION deny_rating_pair()`)
	if receipt, err := f.ds.CommitParentRatingPair(intent); err == nil || receipt != nil {
		t.Fatal("failed pair SQL returned a receipt")
	}
	if !reflect.DeepEqual(before, f.row(t)) {
		t.Fatal("failed pair SQL committed one field or UpdatedAt")
	}
}
