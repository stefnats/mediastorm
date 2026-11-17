package datastore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// These cases exercise the PostgreSQL frame itself. Cookie/API handlers and
// refresh/logout transactions have separate consumer tests.
func requestFrameFixture(t *testing.T) *unlockPGFixture {
	t.Helper()
	f := accessFixture(t).unlockPGFixture
	f.exec(t, `ALTER TABLE accounts ADD COLUMN username TEXT NOT NULL DEFAULT 'synthetic actor';
        ALTER TABLE sessions ADD COLUMN user_agent TEXT NOT NULL DEFAULT 'synthetic agent';
        ALTER TABLE sessions ADD COLUMN ip_address TEXT NOT NULL DEFAULT '127.0.0.1'`)
	migration, err := embedMigrations.ReadFile("migrations/078_profile_unlock_bindings.sql")
	if err != nil {
		t.Fatal(err)
	}
	f.exec(t, strings.Split(string(migration), "-- +goose Down")[0])
	return f
}

func TestPGPinRequestFrameConfirmsFirstClientBeforePublication(t *testing.T) {
	f := requestFrameFixture(t)
	f.exec(t, `UPDATE sessions SET client_id='' WHERE token=$1`, f.session.Token)
	request, release, err := f.ds.AuthenticateProfilePinRequest(f.request(t.Context()), f.session.Token, "  synthetic-first-client  ")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	session, actor, err := f.ds.ProfilePinFrameIdentity(request)
	if err != nil || session.ClientID != "synthetic-first-client" || actor.ID != "actor" {
		t.Fatal("first client binding was not confirmed in the published identity")
	}
	deadline, ok := request.Context().Deadline()
	if !ok || !deadline.Equal(session.ExpiresAt) {
		t.Fatal("the short SQL setup budget replaced the consumer lifetime")
	}
	var actualClient string
	if err := f.ds.pool.QueryRow(t.Context(), `SELECT client_id FROM sessions WHERE token=$1`, f.session.Token).Scan(&actualClient); err != nil {
		t.Fatal("read own confirmed client")
	}
	if actualClient != session.ClientID {
		t.Fatal("published client differs from committed session")
	}
	// Reusing the same original frame cannot bind replacement client headers.
	again, stop, err := f.ds.AuthenticateProfilePinRequest(request, f.session.Token, "replacement-client")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	againSession, _, err := f.ds.ProfilePinFrameIdentity(again)
	if err != nil || againSession.ClientID != actualClient {
		t.Fatal("an existing frame adopted replacement client context")
	}
	stop()
	if err := f.ds.CheckProfilePinRequestFrame(request); err != nil {
		t.Fatal("a nested frame consumer canceled the original request owner")
	}
}

func TestPGPinRequestFrameUsesCurrentActorRoleAndRetiresOnChange(t *testing.T) {
	for _, master := range []bool{false, true} {
		name := "cached master demoted"
		if master {
			name = "cached regular promoted"
		}
		t.Run(name, func(t *testing.T) {
			f := requestFrameFixture(t)
			f.exec(t, `UPDATE accounts SET is_master=$1 WHERE id='actor'`, master)
			f.exec(t, `UPDATE sessions SET is_master=$1 WHERE token=$2`, !master, f.session.Token)
			f.session.IsMaster = !master
			request, release, err := f.ds.AuthenticateProfilePinRequest(f.request(t.Context()), f.session.Token, f.session.ClientID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			session, actor, err := f.ds.ProfilePinFrameIdentity(request)
			if err != nil || session.IsMaster != master || actor.IsMaster != master {
				t.Fatal("cached session role replaced the actual actor role")
			}
			f.exec(t, `UPDATE accounts SET is_master=$1 WHERE id='actor'`, !master)
			if err := f.ds.CheckProfilePinRequestFrame(request); err == nil {
				t.Fatal("a retired actor role was silently renewed")
			}
		})
	}
}

func TestPGPinRequestFrameOriginalExpiryCancelsConsumer(t *testing.T) {
	for _, bound := range []string{"session", "actor", "parent"} {
		t.Run(bound, func(t *testing.T) {
			f := requestFrameFixture(t)
			originalDeadline := time.Now().UTC().Add(time.Second).Truncate(time.Microsecond)
			parent := t.Context()
			switch bound {
			case "session":
				f.exec(t, `UPDATE sessions SET expires_at=$1 WHERE token=$2`, originalDeadline, f.session.Token)
			case "actor":
				f.exec(t, `UPDATE accounts SET expires_at=$1 WHERE id='actor'`, originalDeadline)
			case "parent":
				var cancel context.CancelFunc
				parent, cancel = context.WithDeadline(parent, originalDeadline)
				defer cancel()
			}
			request, release, err := f.ds.AuthenticateProfilePinRequest(f.request(parent), f.session.Token, f.session.ClientID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			deadline, ok := request.Context().Deadline()
			if !ok || !deadline.Equal(originalDeadline) {
				t.Fatal("consumer context lost the original limiting expiry")
			}
			_, actor, err := f.ds.ProfilePinFrameIdentity(request)
			if err != nil || bound == "actor" && (actor.ExpiresAt == nil || !actor.ExpiresAt.Equal(originalDeadline)) {
				t.Fatal("confirmed actor identity lost its original expiry")
			}
			// Renewing storage cannot extend already accepted consumer work.
			if bound == "session" {
				f.exec(t, `UPDATE sessions SET expires_at=clock_timestamp()+interval '1 hour' WHERE token=$1`, f.session.Token)
			}
			if bound == "actor" {
				f.exec(t, `UPDATE accounts SET expires_at=clock_timestamp()+interval '1 hour' WHERE id='actor'`)
			}
			select {
			case <-request.Context().Done():
				if !errors.Is(request.Context().Err(), context.DeadlineExceeded) {
					t.Fatal("consumer did not observe original deadline expiry")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("consumer continued after the original expiry")
			}
			if err := f.ds.CheckProfilePinRequestFrame(request); err == nil {
				t.Fatal("expired original work reacquired permission")
			}
		})
	}
}

func TestPGPinRequestFrameReleaseAndParentCancellationReachConsumer(t *testing.T) {
	for _, mode := range []string{"release", "parent cancellation"} {
		t.Run(mode, func(t *testing.T) {
			f := requestFrameFixture(t)
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			request, release, err := f.ds.AuthenticateProfilePinRequest(f.request(parent), f.session.Token, f.session.ClientID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			if mode == "release" {
				release()
			} else {
				cancel()
			}
			select {
			case <-request.Context().Done():
				if !errors.Is(request.Context().Err(), context.Canceled) {
					t.Fatal("consumer cancellation reason was replaced")
				}
			case <-time.After(time.Second):
				t.Fatal("outer handler cancellation did not reach consumer")
			}
			if _, _, err := f.ds.ProfilePinFrameIdentity(request); err == nil {
				t.Fatal("canceled frame exposed a current identity")
			}
		})
	}
}
