package datastore

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParentLocalFileStrictItemOnlyDecoder(t *testing.T) {
	if id, err := ParentLocalFileItem([]byte(`{"itemId":"local-item"}`)); err != nil || id != "local-item" {
		t.Fatal("strict opaque item rejected", err)
	}
	for _, body := range []string{`null`, `{}`, `{"itemId":null}`, `{"itemId":"x","itemId":"x"}`, `{"ItemId":"x"}`, `{"itemId":"x","path":"/media/x"}`, `{"itemId":"x","expectedHash":"x"}`, `{"itemId":"x","ttl":30}`, `{"itemId":"x"} {}`, `{"itemId":"../x"}`} {
		if _, err := ParentLocalFileItem([]byte(body)); err == nil {
			t.Fatalf("accepted noncanonical item-only payload %q", body)
		}
	}
}
func parentFilePGRequest(t *testing.T, f *ratingPGFixture, ctx context.Context) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.parentPGFixture.request(ctx)
	r.Method = "POST"
	r.URL.Path = "/api/users/target/kids/local-file-approvals"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := f.ds.AuthenticateProfilePinRequest(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func parentFilePGPermit(t *testing.T, f *ratingPGFixture) (*ParentLocalFilePermit, *http.Request, *ParentRatingIntent) {
	t.Helper()
	body := []byte(`{"itemId":"local-item"}`)
	first, release := parentFilePGRequest(t, f, t.Context())
	i, err := f.ds.PrepareParentLocalFile(first, "target", body, strings.Repeat("a", 64), nil)
	release()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(i.Close)
	retry, stop := parentFilePGRequest(t, f, t.Context())
	t.Cleanup(stop)
	retry.Header.Set(profilePinParentPasswordHeader, parentTestPassword)
	if err = f.ds.BindParentLocalFileRetry(retry, i, "target", body, nil); err != nil {
		t.Fatal(err)
	}
	bound, err := f.ds.BoundParentRatingRequest(retry, i)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ds.VerifyParentRatingPassword(bound, i); err != nil {
		t.Fatal(err)
	}
	p, err := f.ds.CommitParentLocalFile(i)
	if err != nil {
		t.Fatal(err)
	}
	return p, bound, i
}
func TestParentLocalFileActualPGPurposePermitDoesNotMutateProfile(t *testing.T) {
	f := ratingPG(t)
	before := f.row(t)
	p, r, i := parentFilePGPermit(t, f)
	if !reflect.DeepEqual(before, f.row(t)) {
		t.Fatal("file exception mutated profile")
	}
	if err := p.Consume(r.Context(), "target", strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if err := p.Check(r.Context()); err != nil {
		t.Fatal(err)
	}
	if err := p.Consume(r.Context(), "target", strings.Repeat("a", 64)); err == nil {
		t.Fatal("file permit replayed")
	}
	if _, err := f.ds.CommitParentLocalFile(i); err == nil {
		t.Fatal("file commit replayed")
	}
	if raw, err := json.Marshal(p); err != nil || string(raw) != "null" {
		t.Fatal("permit serialized")
	}
	var imported ParentLocalFilePermit
	if json.Unmarshal([]byte(`{}`), &imported) == nil || imported.Check(r.Context()) == nil {
		t.Fatal("imported permit adopted authority")
	}
	// The prompt's context is retired after publication. The separate immutable
	// original PIN/session proof continues only while its exact rows remain.
	i.Close()
	if err := p.Check(t.Context()); err != nil {
		t.Fatal("completed prompt invalidated ongoing original proof", err)
	}
}
func TestParentLocalFilePermitNeverAdoptsChangedOrRestoredPGAuthority(t *testing.T) {
	for _, change := range []string{"profile ABA", "session client", "account role", "session epoch", "profile epoch"} {
		t.Run(change, func(t *testing.T) {
			f := ratingPG(t)
			p, r, _ := parentFilePGPermit(t, f)
			if p.Consume(r.Context(), "target", strings.Repeat("a", 64)) != nil {
				t.Fatal("consume")
			}
			switch change {
			case "profile ABA":
				f.exec(t, `UPDATE users SET name='changed' WHERE id='target'; UPDATE users SET name='Synthetic Target' WHERE id='target'`)
			case "session client":
				f.exec(t, `UPDATE sessions SET client_id='other' WHERE token=$1`, f.session.Token)
			case "account role":
				f.exec(t, `UPDATE accounts SET is_master=NOT is_master WHERE id=$1`, f.session.AccountID)
			case "session epoch":
				f.exec(t, `UPDATE profile_unlock_revocations SET generation=generation+1 WHERE scope='session'`)
			case "profile epoch":
				f.exec(t, `UPDATE profile_unlock_revocations SET generation=generation+1 WHERE scope='profile'`)
			}
			if p.Check(t.Context()) == nil {
				t.Fatal("original file proof adopted changed authority")
			}
		})
	}
}
func TestParentLocalFilePermitRejectsUnrelatedContextTargetAndBinding(t *testing.T) {
	for _, change := range []string{"context", "target", "binding", "expired"} {
		t.Run(change, func(t *testing.T) {
			f := ratingPG(t)
			p, r, _ := parentFilePGPermit(t, f)
			ctx, target, binding := r.Context(), "target", strings.Repeat("a", 64)
			switch change {
			case "context":
				ctx = context.WithValue(ctx, struct{}{}, true)
			case "target":
				target = "other"
			case "binding":
				binding = strings.Repeat("b", 64)
			case "expired":
				p.expires = time.Now().Add(-time.Second)
			}
			if p.Consume(ctx, target, binding) == nil {
				t.Fatal("file permit adopted changed purpose")
			}
			if p.Consume(r.Context(), "target", strings.Repeat("a", 64)) == nil {
				t.Fatal("failed purpose consumption was reusable")
			}
		})
	}
}
func TestParentLocalFileLegacyRatingReceiptCannotCommitFilePurpose(t *testing.T) {
	f := ratingPG(t)
	body := ratingTestBody(parentRatingPair)
	i, r := f.prepare(t, parentRatingPair, body)
	f.verify(t, parentRatingPair, i, r, body)
	if _, err := f.ds.CommitParentLocalFile(i); err == nil {
		t.Fatal("legacy rating proof became file permission")
	}
}
