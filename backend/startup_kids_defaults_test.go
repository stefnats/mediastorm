package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"novastream/models"
)

func startupKidsCandidates() []models.User {
	return []models.User{
		{ID: "first-kids", IsKidsProfile: true},
		{ID: "adult", IsKidsProfile: false},
		{ID: "second-kids", IsKidsProfile: true},
	}
}

func TestStartupKidsDefaultsMainUsesCurrentPGDecision(t *testing.T) {
	type marker struct{}
	ctx := context.WithValue(context.Background(), marker{}, "original startup")
	calls := 0
	changed, err := applyStartupKidsDefaults(ctx, startupKidsCandidates(), true,
		func(actual context.Context, ids ...string) (int, error) {
			calls++
			if actual != ctx || !reflect.DeepEqual(ids, []string{"first-kids", "second-kids"}) {
				t.Fatal("startup changed the original context or candidate cut")
			}
			// Current persisted profiles may no longer be Kids. The consumed
			// port, rather than the cached candidate count, decides the result.
			return 0, nil
		},
		func(...string) (int, error) {
			t.Fatal("PostgreSQL startup fell back to unguarded defaults")
			return 0, nil
		})
	if err != nil || changed != 0 || calls != 1 {
		t.Fatal("startup did not retain the guarded batch result")
	}
}

func TestStartupKidsDefaultsMainPreservesLegacyBatch(t *testing.T) {
	calls := 0
	changed, err := applyStartupKidsDefaults(context.Background(), startupKidsCandidates(), false,
		func(context.Context, ...string) (int, error) {
			t.Fatal("JSON startup entered the PostgreSQL protocol")
			return 0, nil
		},
		func(ids ...string) (int, error) {
			calls++
			if !reflect.DeepEqual(ids, []string{"first-kids", "second-kids"}) {
				t.Fatal("legacy startup changed its selected batch")
			}
			return 2, nil
		})
	if err != nil || changed != 2 || calls != 1 {
		t.Fatal("legacy startup no longer uses one unchanged default call")
	}
}

func TestStartupKidsDefaultsMainDoesNotRetryOrRenew(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	changed, err := applyStartupKidsDefaults(ctx, startupKidsCandidates(), true,
		func(actual context.Context, _ ...string) (int, error) {
			calls++
			if actual != ctx || actual.Err() != context.Canceled {
				t.Fatal("startup renewed a canceled context")
			}
			return 0, actual.Err()
		}, nil)
	if !errors.Is(err, context.Canceled) || changed != 0 || calls != 1 {
		t.Fatal("startup retried or concealed a failed guarded batch")
	}
}

func TestStartupKidsDefaultsMainEmptyAndUnavailable(t *testing.T) {
	if changed, err := applyStartupKidsDefaults(context.Background(), []models.User{{ID: "adult"}}, true, nil, nil); err != nil || changed != 0 {
		t.Fatal("an empty Kids cut attempted default work")
	}
	if changed, err := applyStartupKidsDefaults(context.Background(), startupKidsCandidates(), true, nil,
		func(...string) (int, error) {
			t.Fatal("missing PostgreSQL port invoked legacy defaults")
			return 0, nil
		}); err == nil || changed != 0 {
		t.Fatal("missing guarded port did not fail closed")
	}
	if changed, err := applyStartupKidsDefaults(context.Background(), startupKidsCandidates(), false, nil, nil); err == nil || changed != 0 {
		t.Fatal("missing legacy port was reported as successful")
	}
}
