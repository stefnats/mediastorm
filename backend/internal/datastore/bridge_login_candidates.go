package datastore

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"novastream/config"
)

var (
	ErrBridgeLoginCandidatesUnavailable = errors.New("current bridge login candidates unavailable")
	ErrBridgeLoginCandidatesChanged     = errors.New("original bridge login candidate request changed")
	ErrBridgeLoginCandidatesBusy        = errors.New("bridge login candidate work capacity unavailable")
	ErrBridgeLoginCandidatesAmbiguous   = errors.New("bridge login name matches more than three profiles")
)

// BridgeLoginCandidate contains selectors, never credential or mint authority.
type BridgeLoginCandidate struct{ AccountID, ProfileID string }

// Slots represent actual running work, not retained owners or authentication.
var bridgeLoginCandidateCalls = make(chan struct{}, 8)

const bridgeLoginCandidateQuery = `SELECT
 CASE WHEN octet_length(u.id) BETWEEN 1 AND 256 THEN u.id END,
 CASE WHEN octet_length(u.account_id) BETWEEN 1 AND 256 THEN u.account_id END,
 CASE WHEN octet_length(u.name)<=4096 THEN u.name END,
 a.id IS NOT NULL,
 CASE WHEN a.id IS NULL THEN '' WHEN octet_length(a.id) BETWEEN 1 AND 256 THEN a.id END,
 CASE WHEN a.id IS NULL THEN '' WHEN octet_length(a.username)<=4096 THEN a.username END,
 concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),
 CASE WHEN a.id IS NULL THEN '' ELSE concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text) END
 FROM users u LEFT JOIN accounts a ON a.id=u.account_id
 ORDER BY u.id COLLATE "C" LIMIT 4097`

type bridgeLoginCandidateRow struct {
	profile, owner, name, account, username string
	profileRow, accountRow                  string
	accountPresent                          bool
}

// ResolvePasswordCandidates observes one complete PG statement snapshot. It
// does not serialize these names with a subsequent credential check or mint.
func (i *BridgeLoginSessionIssuer) ResolvePasswordCandidates(r *http.Request, username, client string, original *config.BridgeLoginPolicyObservation) (result []BridgeLoginCandidate, err error) {
	entered := time.Now()
	if !i.valid() {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	if r == nil || r.Context().Err() != nil || len(username) > 65536 || !utf8.ValidString(username) {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	requestContext := r.Context()
	originalDeadline, ok := requestContext.Deadline()
	if !ok || !originalDeadline.After(entered) || originalDeadline.After(entered.Add(30*time.Second)) {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	binding, bindErr := bridgeCredentialRequestBinding(r, client)
	if bindErr != nil {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	workDeadline := entered.Add(2 * time.Second)
	if originalDeadline.Before(workDeadline) {
		workDeadline = originalDeadline
	}
	select {
	case bridgeLoginCandidateCalls <- struct{}{}:
	default:
		return nil, ErrBridgeLoginCandidatesBusy
	}
	defer func() { <-bridgeLoginCandidateCalls }()
	work, cancel := context.WithDeadline(context.Background(), workDeadline)
	defer cancel()
	joinCaller := familyMediaAfterFunc(requestContext, cancel)
	defer joinCaller()
	hold, holdErr := i.coordinator.TryProgressFileHold(requestContext)
	if holdErr != nil {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	joinHold := familyMediaAfterFunc(hold.Context(), cancel)
	defer func() {
		// The original SQL/Rows cleanup has already joined. Callbacks only
		// signal; no callback can Close this operation or wait for itself.
		joinHold()
		joinCaller()
		if closeErr := hold.Close(); closeErr != nil && err == nil {
			err = ErrBridgeLoginCandidatesChanged
		}
		currentBinding, currentErr := bridgeCredentialRequestBinding(r, client)
		if err == nil && (!i.valid() || r.Context() != requestContext || requestContext.Err() != nil || !originalDeadline.After(time.Now()) || !workDeadline.After(time.Now()) || currentErr != nil || currentBinding != binding) {
			err = ErrBridgeLoginCandidatesChanged
		}
		if err != nil {
			result = nil
		}
	}()
	fence, fenceErr := i.security.BindBridgePolicy(work, original, hold)
	if fenceErr != nil {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	values, valueErr := fence.Values()
	if valueErr != nil || !values.BridgeEnabled {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	// Even empty input validates the genuine original policy and lifetime.
	if strings.TrimSpace(username) != "" {
		rows, readErr := i.readBridgeLoginCandidateRows(work)
		if readErr != nil {
			return nil, readErr
		}
		// No matching, even an obvious fourth match, precedes validation of
		// the complete snapshot and every scalar/aggregate work bound.
		result, err = matchBridgeLoginCandidates(rows, username)
		if err != nil {
			return nil, err
		}
	}
	if work.Err() != nil || hold.Context().Err() != nil || r.Context() != requestContext {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	finalFence, finalErr := i.security.BindBridgePolicy(work, original, hold)
	if finalErr != nil {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	currentValues, finalErr := finalFence.Values()
	currentBinding, currentErr := bridgeCredentialRequestBinding(r, client)
	if finalErr != nil || currentValues != values || currentErr != nil || currentBinding != binding || work.Err() != nil || hold.Context().Err() != nil {
		return nil, ErrBridgeLoginCandidatesChanged
	}
	return result, nil
}

func (i *BridgeLoginSessionIssuer) readBridgeLoginCandidateRows(ctx context.Context) ([]bridgeLoginCandidateRow, error) {
	connection, err := i.store.pool.Acquire(ctx)
	if err != nil {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	defer func() {
		physical := connection.Conn().PgConn()
		if physical.IsClosed() {
			<-physical.CleanupDone()
		}
		connection.Release()
	}()
	rows, err := connection.Query(ctx, bridgeLoginCandidateQuery)
	if err != nil {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	defer rows.Close()
	var out []bridgeLoginCandidateRow
	total := 0
	for rows.Next() {
		if len(out) >= 4096 {
			return nil, ErrBridgeLoginCandidatesUnavailable
		}
		var profile, owner, name, account, username *string
		var value bridgeLoginCandidateRow
		if rows.Scan(&profile, &owner, &name, &value.accountPresent, &account, &username, &value.profileRow, &value.accountRow) != nil || profile == nil || owner == nil || name == nil || account == nil || username == nil {
			return nil, ErrBridgeLoginCandidatesUnavailable
		}
		value.profile, value.owner, value.name, value.account, value.username = *profile, *owner, *name, *account, *username
		if !parentMutationID(value.profile) || !parentMutationID(value.owner) || value.accountPresent && (!parentMutationID(value.account) || value.account != value.owner) || !utf8.ValidString(value.name) || !utf8.ValidString(value.username) {
			return nil, ErrBridgeLoginCandidatesUnavailable
		}
		size := len(value.profile) + len(value.owner) + len(value.name) + len(value.account) + len(value.username) + len(value.profileRow) + len(value.accountRow)
		if size > 4*1024*1024-total {
			return nil, ErrBridgeLoginCandidatesUnavailable
		}
		total += size
		out = append(out, value)
	}
	// Next(false) and Close join QueryEnd; a tracer may be held there. Do
	// not publish snapshot DATA until that actual end and all cleanup finish.
	rows.Close()
	if rows.Err() != nil || ctx.Err() != nil {
		return nil, ErrBridgeLoginCandidatesUnavailable
	}
	return out, nil
}

func matchBridgeLoginCandidates(rows []bridgeLoginCandidateRow, username string) ([]BridgeLoginCandidate, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, nil
	}
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[strings.ToLower(strings.TrimSpace(row.name))]++
	}
	accountHint, profileHint, slash := strings.Cut(username, "/")
	seen := make(map[string]struct{}, 4)
	var result []BridgeLoginCandidate
	for _, row := range rows {
		name := strings.TrimSpace(row.name)
		display := name
		if row.accountPresent && strings.TrimSpace(row.username) != "" {
			display = strings.TrimSpace(row.username) + "/" + name
		} else if counts[strings.ToLower(name)] > 1 {
			namespace := uuid.MustParse("8f0c8b8e-6a55-4e0e-9c0a-6d5c1f6a7b21")
			id := strings.ReplaceAll(uuid.NewSHA1(namespace, []byte("user\x00"+row.profile)).String(), "-", "")
			display += " (" + id[:4] + ")"
		}
		if !strings.EqualFold(display, username) && !strings.EqualFold(name, username) && !(slash && strings.EqualFold(name, strings.TrimSpace(profileHint)) && row.accountPresent && strings.EqualFold(row.username, strings.TrimSpace(accountHint))) {
			continue
		}
		if _, ok := seen[row.profile]; ok {
			continue
		}
		seen[row.profile] = struct{}{}
		if len(result) == 3 {
			return nil, ErrBridgeLoginCandidatesAmbiguous
		}
		result = append(result, BridgeLoginCandidate{AccountID: row.owner, ProfileID: row.profile})
	}
	// The actual statement is ordered by exact profile ID in C collation.
	return result, nil
}
