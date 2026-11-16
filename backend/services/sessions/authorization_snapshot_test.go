package sessions

import (
	"context"
	"errors"
	"novastream/models"
	"testing"
	"time"
)

func TestAuthorizationSessionSnapshotUsesCurrentReaderWithoutValidatorOrCacheRefresh(t *testing.T) {
	old := models.Session{Token: "synthetic-session", AccountID: "old", ExpiresAt: time.Now().Add(-time.Hour)}
	current := old
	current.AccountID = "current"
	current.Scope = models.SessionScopeStream
	calls := 0
	s := &Service{sessions: map[string]models.Session{old.Token: old}, sessionReader: SessionLookupFunc(func(ctx context.Context, token string) (*models.Session, error) {
		calls++
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return &current, nil
	})}
	first, err := s.SnapshotForAuthorization(t.Context(), old.Token)
	if err != nil || first.AccountID != "current" || first.Scope != models.SessionScopeStream || !first.ExpiresAt.Equal(old.ExpiresAt) || calls != 1 {
		t.Fatal("current snapshot missing or restamped")
	}
	first.AccountID = "caller-mutation"
	if current.AccountID != "current" || s.sessions[old.Token] != old {
		t.Fatal("snapshot mutated reader/cache")
	}
	s.sessionReader = SessionLookupFunc(func(context.Context, string) (*models.Session, error) { return nil, nil })
	if _, err := s.SnapshotForAuthorization(t.Context(), old.Token); !errors.Is(err, ErrSessionNotFound) {
		t.Fatal("startup cache resurrected a revoked current session")
	}
}

func TestAuthorizationSessionSnapshotCancellationAndMemoryIsolation(t *testing.T) {
	item := models.Session{Token: "synthetic-session", AccountID: "owner", ExpiresAt: time.Now().Add(time.Hour)}
	s := &Service{sessions: map[string]models.Session{item.Token: item}}
	copy, err := s.SnapshotForAuthorization(t.Context(), item.Token)
	if err != nil {
		t.Fatal(err)
	}
	copy.AccountID = "mutated"
	if s.sessions[item.Token] != item {
		t.Fatal("memory snapshot shared mutable state")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.SnapshotForAuthorization(ctx, item.Token); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled snapshot accepted")
	}
	ctx, cancel = context.WithCancel(t.Context())
	defer cancel()
	readerCalls := 0
	s.sessionReader = SessionLookupFunc(func(context.Context, string) (*models.Session, error) { readerCalls++; cancel(); return &item, nil })
	if _, err := s.SnapshotForAuthorization(ctx, item.Token); !errors.Is(err, context.Canceled) || readerCalls != 1 {
		t.Fatal("reader cancellation accepted")
	}
}
