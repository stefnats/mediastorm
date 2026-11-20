package datastore

import (
	"testing"
	"time"
)

// Embedded methods would panic if readiness queried this fake repository.
type familyReadinessFakeAccountRepository struct{ AccountRepository }

func TestFamilyAccountRepositoryReadinessRejectsUnavailableAndFakeReaders(t *testing.T) {
	var unavailable *DataStore
	var nilRepository *pgAccountRepo
	for _, store := range []*DataStore{unavailable, {}} {
		for _, reader := range []AccountRepository{nil, nilRepository, &familyReadinessFakeAccountRepository{}} {
			if store.FamilyAccountRepositoryReady(reader) {
				t.Fatal("unavailable or fake account reader became family wiring")
			}
		}
	}
}

func TestFamilyAccountRepositoryReadinessUsesExactActualPoolWithoutQuery(t *testing.T) {
	first, second := familyMediaPG(t), familyMediaPG(t)
	for _, reader := range []AccountRepository{first.ds.Accounts(), first.ds.Accounts()} {
		if !first.ds.FamilyAccountRepositoryReady(reader) || second.ds.FamilyAccountRepositoryReady(reader) {
			t.Fatal("actual account repository pool correlation changed")
		}
	}
	for _, reader := range []AccountRepository{nil, (*pgAccountRepo)(nil), &familyReadinessFakeAccountRepository{}} {
		if first.ds.FamilyAccountRepositoryReady(reader) {
			t.Fatal("fake/typed-nil reader borrowed actual store identity")
		}
	}
	if err := first.ds.WithTx(t.Context(), func(tx *Tx) error {
		if first.ds.FamilyAccountRepositoryReady(tx.Accounts()) {
			t.Fatal("transaction-bound account reader became lifetime repository")
		}
		return nil
	}); err != nil {
		t.Fatal("actual fixture transaction", err)
	}
	// Readiness must not block on account SQL or change any row/epoch.
	tx, err := first.ds.Pool().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err = tx.Exec(t.Context(), "LOCK TABLE accounts IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	ready := make(chan bool, 1)
	go func() { ready <- first.ds.FamilyAccountRepositoryReady(first.ds.Accounts()) }()
	select {
	case actual := <-ready:
		if !actual {
			t.Fatal("pure wiring check rejected an actual repository")
		}
	case <-time.After(time.Second):
		t.Fatal("readiness queried or blocked on account SQL")
	}
}
