package datastore

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// The retry is already authenticated before the lock is acquired. The only
// remaining session read is the actual frame check consumed by the typed bind.
func TestParentConfirmationOriginalDeadlineBoundsFrameSQL(t *testing.T) {
	for _, action := range []string{"mode", "list removal"} {
		t.Run(action, func(t *testing.T) {
			f := removalFixture(t)
			request := func(ctx context.Context) (*http.Request, context.CancelFunc) {
				t.Helper()
				r := f.parentPGFixture.request(ctx)
				r.Method = http.MethodDelete
				r.URL.Path = "/api/users/target/kids/lists"
				r.URL.RawQuery = "url=" + removalTestURL
				if action == "mode" {
					r.Method = http.MethodPut
					r.URL.Path = "/api/users/target/kids/mode"
					r.URL.RawQuery = ""
				}
				r.Header.Set("X-Client-ID", f.session.ClientID)
				checked, release, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
				if err != nil {
					t.Fatal(err)
				}
				return checked, release
			}
			original, cancelOriginal := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancelOriginal()
			first, releaseFirst := request(original)
			var bind func(*http.Request) error
			if action == "mode" {
				intent, err := f.ds.PrepareParentMode(first, "target", "rating", nil)
				if err != nil {
					releaseFirst()
					t.Fatal(err)
				}
				defer intent.Close()
				bind = func(r *http.Request) error {
					return f.ds.BindParentModeRetry(r, intent, "target", "rating", nil)
				}
			} else {
				intent, err := f.ds.PrepareParentListRemoval(first, "target", removalTestURL, nil)
				if err != nil {
					releaseFirst()
					t.Fatal(err)
				}
				defer intent.Close()
				bind = func(r *http.Request) error {
					return f.ds.BindParentListRemovalRetry(r, intent, "target", removalTestURL, nil)
				}
			}
			releaseFirst()
			retry, releaseRetry := request(t.Context())
			defer releaseRetry()
			tx, err := f.ds.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer rollbackPinTx(tx, t.Context())
			if _, err = tx.Exec(t.Context(), `LOCK TABLE sessions IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- bind(retry) }()
			joined := false
			defer func() {
				// Even a failed pre-fix assertion releases the real database wait
				// and joins its worker before fixture teardown.
				rollbackPinTx(tx, t.Context())
				if !joined {
					<-done
				}
			}()
			f.waitBlocked(t)
			<-original.Done()
			select {
			case err = <-done:
				joined = true
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("frame SQL did not preserve the original deadline", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("frame SQL wait outlived the original challenge deadline")
			}
			if retry.Context().Err() != nil {
				t.Fatal("outer retry expired instead of the original operation budget")
			}
			if f.lists(t) != `["PRIVATE_LIST"]` {
				t.Fatal("frame-only deadline check changed stored list membership")
			}
		})
	}
}
