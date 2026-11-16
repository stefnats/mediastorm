package accounts

import (
	"context"
	"errors"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
	"novastream/models"
)

func contextFixtureAccount(t *testing.T) models.Account {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("synthetic parent password"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return models.Account{ID: "synthetic-parent", Username: "parent", PasswordHash: string(hash)}
}

func TestAuthenticateContextCancelledBeforeReader(t *testing.T) {
	calls := 0
	s := &Service{accountReader: stubAccountReader{getByUsername: func(context.Context, string) (*models.Account, error) {
		calls++
		return nil, nil
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.AuthenticateContext(ctx, "parent", "synthetic parent password"); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("cancelled authentication: err=%v calls=%d", err, calls)
	}
}

func TestAuthenticateContextPropagatesRequestAndStopsBlockedReader(t *testing.T) {
	type marker struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), marker{}, "original"))
	defer cancel()
	started := make(chan struct{})
	s := &Service{accountReader: stubAccountReader{getByUsername: func(got context.Context, username string) (*models.Account, error) {
		if got != ctx || got.Value(marker{}) != "original" || username != "parent" {
			t.Error("reader did not receive the original request")
		}
		close(started)
		<-got.Done()
		return nil, got.Err()
	}}}
	done := make(chan error, 1)
	go func() { _, err := s.AuthenticateContext(ctx, "parent", "synthetic parent password"); done <- err }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("reader did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled reader did not stop")
	}
}

func TestAuthenticateContextIgnoresLateReaderWithoutUpdatingCache(t *testing.T) {
	original := contextFixtureAccount(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	returned := original
	returned.Username = "replacement"
	s := &Service{accounts: map[string]models.Account{original.ID: original}, accountReader: stubAccountReader{getByUsername: func(context.Context, string) (*models.Account, error) {
		cancel()
		return &returned, nil
	}}}
	if _, err := s.AuthenticateContext(ctx, "parent", "synthetic parent password"); !errors.Is(err, context.Canceled) {
		t.Fatalf("late success accepted: %v", err)
	}
	if s.accounts[original.ID].Username != original.Username {
		t.Fatal("late reader changed cached account")
	}
}

func TestAuthenticateContextPositiveAndCredentialFailures(t *testing.T) {
	for _, name := range []string{"current", "wrong password", "expired", "missing", "reader failure"} {
		t.Run(name, func(t *testing.T) {
			account := contextFixtureAccount(t)
			if name == "expired" {
				past := time.Now().Add(-time.Hour)
				account.ExpiresAt = &past
			}
			password := "synthetic parent password"
			if name == "wrong password" {
				password = "incorrect"
			}
			s := &Service{accounts: make(map[string]models.Account), accountReader: stubAccountReader{getByUsername: func(context.Context, string) (*models.Account, error) {
				if name == "missing" {
					return nil, nil
				}
				if name == "reader failure" {
					return nil, errors.New("synthetic storage unavailable")
				}
				return &account, nil
			}}}
			verified, err := s.AuthenticateContext(context.Background(), "parent", password)
			if name == "current" {
				if err != nil || verified.ID != account.ID {
					t.Fatalf("positive rejected: %v", err)
				}
			} else if err == nil || verified.ID != "" {
				t.Fatal("invalid authentication succeeded")
			}
		})
	}
}
