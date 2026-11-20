//go:build linux

package profileunlock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"novastream/config"
)

func TestBridgeLoginCandidatesFacadeSameOriginalIssuer(t *testing.T) {
	for _, kind := range []string{"genuine", "copied-issuer", "foreign-policy-owner", "issuer-retired-during-read"} {
		t.Run(kind, func(t *testing.T) {
			f := bridgeSessionFacadePG(t)
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			r := bridgeCredentialFacadeRequest(ctx)
			policy, err := f.security.CaptureBridgePolicy(ctx)
			if err != nil {
				t.Fatal(err)
			}
			issuer := f.issuer
			if kind == "copied-issuer" {
				copied := *issuer
				issuer = &copied
			}
			if kind == "foreign-policy-owner" {
				if err := f.security.Close(); err != nil {
					t.Fatal(err)
				}
				f.security, err = config.InstallManagedSecurity(f.manager, f.coordinator, "")
				if err != nil {
					t.Fatal(err)
				}
				issuer, err = f.pins.NewBridgeLoginSessionIssuer(f.manager, f.security, f.coordinator)
				if err != nil {
					t.Fatal(err)
				}
				current, currentErr := f.security.CaptureBridgePolicy(ctx)
				if currentErr != nil {
					t.Fatal(currentErr)
				}
				positive, positiveErr := issuer.ResolvePasswordCandidates(r, "Synthetic Profile", "synthetic-device", current)
				if positiveErr != nil || len(positive) != 1 {
					t.Fatal("replacement genuine owner unavailable", positiveErr)
				}
				// Retain the genuine observation of the distinct old owner.
			}
			if kind == "issuer-retired-during-read" {
				connection, err := f.ds.Pool().Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var schema string
				if err = connection.QueryRow(ctx, `SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid='users'::regclass`).Scan(&schema); err != nil {
					connection.Release()
					t.Fatal(err)
				}
				connection.Release()
				// The own existing pool is only storage. Hold its relation with an
				// actual second connection while the concrete facade reads PG.
				holder, err := f.ds.Pool().Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					_ = holder.Rollback(cleanup)
				})
				if _, err = holder.Exec(ctx, `LOCK TABLE `+pgx.Identifier{schema, "users"}.Sanitize()+` IN ACCESS EXCLUSIVE MODE`); err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				var values []BridgeLoginCandidate
				var readErr error
				t.Cleanup(func() {
					cancel()
					cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					_ = holder.Rollback(cleanup)
					select {
					case <-done:
					case <-cleanup.Done():
						t.Error("actual facade read did not finish")
					}
				})
				go func() {
					values, readErr = issuer.ResolvePasswordCandidates(r, "Synthetic Profile", "synthetic-device", policy)
					close(done)
				}()
				// Observe an actual ungranted relation lock on this own pool;
				// readiness or a fixed sleep does not establish entered source work.
				until := time.Now().Add(time.Second)
				pending := 0
				for time.Now().Before(until) {
					if err = f.ds.Pool().QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE relation='users'::regclass AND NOT granted`).Scan(&pending); err != nil {
						t.Fatal(err)
					}
					if pending > 0 {
						break
					}
					time.Sleep(time.Millisecond)
				}
				if pending == 0 {
					t.Fatal("actual blocked facade query not witnessed")
				}
				closed := make(chan struct{})
				go func() { _ = f.coordinator.Close(); close(closed) }()
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("retired actual facade query did not end")
				}
				select {
				case <-closed:
				case <-ctx.Done():
					t.Fatal("actual coordinator cleanup did not end")
				}
				if readErr == nil || values != nil || ctx.Err() != nil {
					t.Fatal("retired facade returned selectors or changed original caller", readErr)
				}
				return
			}
			values, err := issuer.ResolvePasswordCandidates(r, "Synthetic Profile", "synthetic-device", policy)
			if kind == "genuine" {
				if err != nil || len(values) != 1 || values[0].AccountID != "actor" || values[0].ProfileID != "target" {
					t.Fatal("genuine same-store facade selectors wrong", err)
				}
			} else if values != nil || err == nil {
				t.Fatal("copied/foreign facade selected profiles", err)
			}
			if kind == "copied-issuer" && !errors.Is(err, ErrBridgeLoginCandidatesUnavailable) {
				t.Fatal("copied facade error changed", err)
			}
			var count int
			if err := f.ds.Pool().QueryRow(ctx, `SELECT count(*) FROM sessions`).Scan(&count); err != nil || count != 0 {
				t.Fatal("resolver minted a session", err)
			}
		})
	}
}
