package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/sessions"
	"novastream/services/watchrooms"
)

func pgAvailabilityProbe(f *publicPGPinFixture) (func(string) *httptest.ResponseRecorder, *int) {
	calls := new(int)
	probe := AccountAuthMiddleware(f.sessions, nil, profileunlock.NewPGProfilePins(f.ds, f.sessions))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	return func(token string) *httptest.ResponseRecorder {
		ctx, cancel := context.WithTimeout(f.t.Context(), 5*time.Second)
		defer cancel()
		r := httptest.NewRequestWithContext(ctx, http.MethodGet, "/api/video/stream?path=%2Fsynthetic-availability-party.webm", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Client-ID", "synthetic-availability-client")
		w := httptest.NewRecorder()
		probe.ServeHTTP(w, r)
		return w
	}, calls
}

func TestPGSessionAvailabilityOutageDoesNotRevokeOriginal(t *testing.T) {
	f := newPublicPGPinFixture(t)
	token := f.session(false)
	probe, calls := pgAvailabilityProbe(f)
	if w := probe(token); w.Code != http.StatusNoContent || *calls != 1 {
		t.Fatal("actual original session was not admitted")
	}
	// Only this fixture's relation is unavailable. Its original row and token
	// remain present; no production service or database is touched.
	f.exec(`ALTER TABLE sessions RENAME TO synthetic_held_sessions`)
	if w := probe(token); w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), `"service_unavailable"`) || *calls != 1 {
		t.Fatal("PG outage became revocation or reached the application")
	}
	var retained int
	if f.ds.Pool().QueryRow(t.Context(), `SELECT count(*) FROM synthetic_held_sessions WHERE token=$1`, token).Scan(&retained) != nil || retained != 1 {
		t.Fatal("unavailable session was changed or removed")
	}
	f.exec(`ALTER TABLE synthetic_held_sessions RENAME TO sessions`)
	if w := probe(token); w.Code != http.StatusNoContent || *calls != 2 {
		t.Fatal("recovery did not admit the same original session")
	}
	f.exec(`DELETE FROM sessions WHERE token=$1`, token)
	if w := probe(token); w.Code != http.StatusUnauthorized || !strings.Contains(w.Body.String(), `"session_expired"`) || *calls != 2 {
		t.Fatal("actual revocation became a retryable store error")
	}
}

func TestPGSessionAvailabilityInvalidAndExpiredStayUnauthorized(t *testing.T) {
	f := newPublicPGPinFixture(t)
	probe, calls := pgAvailabilityProbe(f)
	for _, token := range []string{"synthetic-missing-session", strings.Repeat("x", 257)} {
		if w := probe(token); w.Code != http.StatusUnauthorized || *calls != 0 {
			t.Fatal("missing or malformed original session became service unavailability")
		}
	}
	token := f.session(false)
	f.exec(`UPDATE sessions SET expires_at=clock_timestamp()-interval '1 second' WHERE token=$1`, token)
	if w := probe(token); w.Code != http.StatusUnauthorized || *calls != 0 {
		t.Fatal("actually expired session became a retryable store error")
	}
}

func TestPGSessionAvailabilityLateBindingQueryFailureDoesNotRevoke(t *testing.T) {
	for _, relation := range []string{"accounts", "sessions"} {
		t.Run(relation, func(t *testing.T) {
			f := newPublicPGPinFixture(t)
			token := f.session(false)
			probe, calls := pgAvailabilityProbe(f)
			holder, err := f.ds.Pool().Begin(t.Context())
			if err != nil {
				t.Fatal("begin own lock holder")
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = holder.Rollback(ctx)
			}()
			query, key := `SELECT id FROM accounts WHERE id=$1 FOR UPDATE`, f.account
			if relation == "sessions" {
				query, key = `SELECT token FROM sessions WHERE token=$1 FOR UPDATE`, token
			}
			var ignored string
			if holder.QueryRow(t.Context(), query, key).Scan(&ignored) != nil {
				t.Fatal("hold original fixture identity row")
			}
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- probe(token) }()
			joined := false
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = holder.Rollback(ctx)
				if !joined {
					select {
					case <-done:
					case <-time.After(6 * time.Second):
						t.Error("own canceled authentication worker did not join")
					}
				}
			}()
			// Observe the exact backend blocked by our holder, then terminate only
			// that own synthetic backend. The initial PG frame read has completed.
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			var waiter uint32
			for waiter == 0 {
				if err := f.ds.Pool().QueryRow(ctx, `SELECT COALESCE((SELECT pid FROM pg_catalog.pg_stat_activity WHERE $1::integer=ANY(pg_catalog.pg_blocking_pids(pid)) AND datname=current_database() AND pid<>pg_backend_pid()),0)`, holder.Conn().PgConn().PID()).Scan(&waiter); err != nil {
					t.Fatal("observe own blocked first binding")
				}
				if waiter == 0 {
					select {
					case <-ctx.Done():
						t.Fatal("first binding never reached original held row")
					case <-ticker.C:
					}
				}
			}
			var terminated bool
			if f.ds.Pool().QueryRow(ctx, `SELECT pg_catalog.pg_terminate_backend($1)`, waiter).Scan(&terminated) != nil || !terminated {
				t.Fatal("interrupt exact own blocked backend")
			}
			select {
			case w := <-done:
				joined = true
				if w.Code != http.StatusServiceUnavailable || *calls != 0 {
					t.Fatal("late binding store outage became revocation or application access")
				}
			case <-ctx.Done():
				t.Fatal("failed actual binding did not finish")
			}
			if holder.Rollback(t.Context()) != nil {
				t.Fatal("release own identity row")
			}
			if w := probe(token); w.Code != http.StatusNoContent || *calls != 1 {
				t.Fatal("same original session did not recover after binding outage")
			}
		})
	}
}

func TestPGSessionAvailabilityPartyOutagePreservesCurrentRoomAuthority(t *testing.T) {
	f := newPublicPGPinFixture(t)
	repo := f.ds.WatchRooms()
	rooms := watchrooms.New(repo, f.users, nil)
	// Match Main's current error distinction around the actual room service.
	f.sessions.SetWatchPartyPlaybackValidator(func(ctx context.Context, room, guest, resource string) error {
		err := rooms.ValidateExternalPlayback(ctx, room, guest, resource)
		if err == nil || watchrooms.IsExternalPlaybackDenied(err) {
			return err
		}
		return fmt.Errorf("%w: %v", sessions.ErrAuthorityUnavailable, err)
	})
	now := time.Now().UTC()
	const roomID, guestID, resource = "synthetic-availability-room", "synthetic-guest", "/synthetic-availability-party.webm"
	room := &models.WatchRoom{ID: roomID, CreatorProfileID: f.open, Title: "Synthetic party", MediaType: "movie", ItemID: "synthetic-film", Params: map[string]string{}, Status: models.WatchRoomStatusPlaying, Revision: 1, AnchorUpdatedAt: now, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := repo.Create(t.Context(), room, nil, "", models.WatchRoomClientCapabilities{}); err != nil {
		t.Fatal("create own current room")
	}
	if err := repo.JoinExternalGuest(t.Context(), roomID, guestID, "Synthetic guest", "", models.WatchRoomClientCapabilities{}, now); err != nil {
		t.Fatal("join own current room")
	}
	if bound, err := repo.BindExternalSource(t.Context(), roomID, f.open, resource, map[string]string{}, now); err != nil || !bound {
		t.Fatal("bind own current room source")
	}
	grant, err := f.sessions.CreateWatchPartyPlayback(f.account, "synthetic party agent", "127.0.0.1", time.Hour, roomID, guestID, resource)
	if err != nil {
		t.Fatal("create actual original PG party session")
	}
	probe, calls := pgAvailabilityProbe(f)
	if w := probe(grant.Token); w.Code != http.StatusNoContent || *calls != 1 {
		t.Fatal("actual room authority did not admit original party session")
	}
	f.exec(`ALTER TABLE watch_rooms RENAME TO synthetic_held_rooms`)
	if w := probe(grant.Token); w.Code != http.StatusServiceUnavailable || *calls != 1 {
		t.Fatal("actual room store outage became revocation or stale room access")
	}
	f.exec(`ALTER TABLE synthetic_held_rooms RENAME TO watch_rooms`)
	if w := probe(grant.Token); w.Code != http.StatusNoContent || *calls != 2 {
		t.Fatal("same party session did not recover with its current room")
	}
	if repo.LeaveExternalGuest(t.Context(), roomID, guestID) != nil {
		t.Fatal("revoke own actual guest")
	}
	if w := probe(grant.Token); w.Code != http.StatusUnauthorized || *calls != 2 {
		t.Fatal("PG party resolver reused a cached room after guest revocation")
	}
}
