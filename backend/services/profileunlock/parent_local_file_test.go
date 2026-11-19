package profileunlock

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type heldFileAttachment struct{ closed atomic.Int32 }

func (*heldFileAttachment) Fingerprint() string { return strings.Repeat("a", 64) }
func (f *heldFileAttachment) Close()            { f.closed.Add(1) }
func fileFacadeRequest(t *testing.T, f *issuerFacadeFixture, pins *PGProfilePins) (*http.Request, context.CancelFunc) {
	t.Helper()
	r := f.request(t.Context())
	r.Method = "POST"
	r.URL.Path = "/api/users/target/kids/local-file-approvals"
	r.Header.Set("X-Client-ID", f.session.ClientID)
	checked, release, err := pins.Authenticate(r, f.session.Token, f.session.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	return checked, release
}
func TestParentLocalFileFacadeSharedBudgetAndHeldAttachmentRetirement(t *testing.T) {
	f := issuerFacadePG(t)
	if _, err := f.users.ClearPin("target"); err != nil {
		t.Fatal(err)
	}
	pins := NewPGProfilePins(f.ds)
	r, release := fileFacadeRequest(t, f, pins)
	defer release()
	body := []byte(`{"itemId":"local-item"}`)
	for n := 0; n < parentModeMaximumActorEntries; n++ {
		file := &heldFileAttachment{}
		token, err := pins.ParentLocalFileChallenge(r, "target", body, file)
		if err != nil || !strings.HasPrefix(token, "pfa_") {
			t.Fatal("bounded file prompt unavailable", err)
		}
	}
	overflow := &heldFileAttachment{}
	if _, err := pins.ParentLocalFileChallenge(r, "target", body, overflow); err == nil || overflow.closed.Load() != 1 {
		t.Fatal("held files bypassed shared account budget or leaked overflow")
	}
	// A legacy action spends the same registry slots, not another file-only map.
	rating, stop := ratingFacadeRequest(t, f, pins, "pair")
	defer stop()
	if _, err := pins.ParentRatingPairChallenge(rating, "target", []byte(`{"movieRating":"x","tvRating":"x"}`)); err == nil {
		t.Fatal("legacy prompt bypassed existing held file budget")
	}
	pins.parentModeMu.Lock()
	for key, entry := range pins.parentModes {
		delete(pins.parentModes, key)
		entry.timer.Stop()
		entry.close()
	}
	pins.parentModeMu.Unlock()
}
func TestParentLocalFileFacadeChangedBodyExpiresAndClosesOriginalAttachment(t *testing.T) {
	for _, change := range []string{"body", "deadline", "client", "foreign action"} {
		t.Run(change, func(t *testing.T) {
			f := issuerFacadePG(t)
			if _, err := f.users.ClearPin("target"); err != nil {
				t.Fatal(err)
			}
			pins := NewPGProfilePins(f.ds)
			r, release := fileFacadeRequest(t, f, pins)
			file := &heldFileAttachment{}
			body := []byte(`{"itemId":"local-item"}`)
			token, err := pins.ParentLocalFileChallenge(r, "target", body, file)
			release()
			if err != nil {
				t.Fatal(err)
			}
			retry, stop := fileFacadeRequest(t, f, pins)
			defer stop()
			switch change {
			case "body":
				body = []byte(`{"itemId":"another-item"}`)
			case "client":
				retry.Header.Set("X-Client-ID", "other-client")
			case "deadline":
				key, _ := parentConfirmationTokenKey(token, "pfa_")
				pins.parentModeMu.Lock()
				pins.parentModes[key].deadline = time.Now().Add(-time.Second)
				pins.parentModeMu.Unlock()
			case "foreign action":
				if _, err := pins.ClaimParentRatingPair(retry, "target", body, token); err == nil {
					t.Fatal("file purpose became legacy rating")
				}
				body = []byte(`{"itemId":"another-item"}`)
			}
			if i, a, err := pins.ClaimParentLocalFile(retry, "target", body, token); err == nil || i != nil || a != nil || file.closed.Load() != 1 {
				t.Fatal("changed file prompt adopted or leaked original", err)
			}
			if _, _, err = pins.ClaimParentLocalFile(retry, "target", []byte(`{"itemId":"local-item"}`), token); err == nil {
				t.Fatal("failed file prompt replayed")
			}
		})
	}
}
func TestParentLocalFileFacadeSuccessfulClaimTransfersOneHeldAttachment(t *testing.T) {
	f := issuerFacadePG(t)
	if _, err := f.users.ClearPin("target"); err != nil {
		t.Fatal(err)
	}
	pins := NewPGProfilePins(f.ds)
	first, release := fileFacadeRequest(t, f, pins)
	file := &heldFileAttachment{}
	body := []byte(`{"itemId":"local-item"}`)
	token, err := pins.ParentLocalFileChallenge(first, "target", body, file)
	release()
	if err != nil {
		t.Fatal(err)
	}
	retry, stop := fileFacadeRequest(t, f, pins)
	defer stop()
	i, a, err := pins.ClaimParentLocalFile(retry, "target", body, token)
	if err != nil || a != file || file.closed.Load() != 0 {
		t.Fatal("claim did not retain original server attachment", err)
	}
	defer i.Close()
	defer a.Close()
	if _, _, err = pins.ClaimParentLocalFile(retry, "target", body, token); err == nil {
		t.Fatal("file attachment claim replayed")
	}
}
