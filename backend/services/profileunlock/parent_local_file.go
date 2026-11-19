package profileunlock

import (
	"net/http"
	"time"

	"novastream/internal/datastore"
)

// ParentFileAttachment is an opaque server-owned held file intent. It occupies
// the existing account/global parent prompt budget and is closed on retirement.
type ParentFileAttachment interface {
	Fingerprint() string
	Close()
}

func (v *PGProfilePins) ParentLocalFileChallenge(r *http.Request, target string, body []byte, file ParentFileAttachment) (string, error) {
	if !v.valid() || file == nil {
		if file != nil {
			file.Close()
		}
		return "", datastore.ErrParentRatingUnavailable
	}
	i, err := v.store.PrepareParentLocalFile(r, target, body, file.Fingerprint(), RequestTokens(r))
	if err != nil {
		file.Close()
		return "", err
	}
	actor, size, deadline, err := v.store.ParentRatingBudget(i)
	if err != nil {
		i.Close()
		file.Close()
		return "", err
	}
	// The bounded held-root/FD intent is charged to the shared registry budget.
	return v.registerParentConfirmation(r, "pfa_", &parentModeEntry{action: parentConfirmationLocalFile, rating: i, file: file, actor: actor, bytes: size + (64 << 10), deadline: deadline})
}

func (v *PGProfilePins) ClaimParentLocalFile(r *http.Request, target string, body []byte, token string) (*ParentRatingIntent, ParentFileAttachment, error) {
	if !v.valid() {
		return nil, nil, datastore.ErrParentRatingUnavailable
	}
	key, ok := parentConfirmationTokenKey(token, "pfa_")
	if !ok {
		return nil, nil, datastore.ErrParentRatingChanged
	}
	entry := v.claimParentConfirmation(key, parentConfirmationLocalFile)
	if entry == nil {
		return nil, nil, datastore.ErrParentRatingChanged
	}
	if !entry.deadline.After(time.Now()) || entry.file == nil || entry.rating == nil {
		entry.close()
		return nil, nil, datastore.ErrParentRatingChanged
	}
	if err := v.store.BindParentLocalFileRetry(r, entry.rating, target, body, RequestTokens(r)); err != nil {
		entry.close()
		return nil, nil, err
	}
	return entry.rating, entry.file, nil
}

func (v *PGProfilePins) CommitParentLocalFile(i *ParentRatingIntent) (*datastore.ParentLocalFilePermit, error) {
	if !v.valid() {
		return nil, datastore.ErrParentRatingUnavailable
	}
	return v.store.CommitParentLocalFile(i)
}
