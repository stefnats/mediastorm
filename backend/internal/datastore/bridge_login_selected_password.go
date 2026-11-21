package datastore

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"novastream/config"
)

// A selected proof owns exactly one genuine credential proof. The complete
// original name/audit set remains private and cannot be supplied as a DTO.
type BridgeSelectedPasswordProof struct{ state *bridgeSelectedPasswordState }
type bridgeSelectedPasswordState struct {
	mu               sync.Mutex
	self             *BridgeSelectedPasswordProof
	issuer           *BridgeLoginSessionIssuer
	credential       *BridgeLoginCredentialProof
	request          *http.Request
	requestContext   context.Context
	original         *config.BridgeLoginPolicyObservation
	username, client string
	binding          [32]byte
	expiry           time.Time
	ctx              context.Context
	cancel           context.CancelFunc
	stop             func()
	once             sync.Once
	done             chan struct{}
	used, closed     bool
	initial          []bridgeLoginCandidateRow
	audits           []bridgeSelectedCandidateAudit
}

type bridgeSelectedPasswordWork struct {
	issuer              *BridgeLoginSessionIssuer
	request             *http.Request
	requestContext, ctx context.Context
	binding             [32]byte
	client              string
	expiry              time.Time
	finishes            []context.CancelFunc
}

// Immutable scalar receipts deliberately omit password/PIN hashes and input.
type bridgeSelectedCandidateAudit struct {
	account, profile, accountRow, profileRow, household string
	settings, epoch                                     int64
	expiry                                              time.Time
	hasExpiry, hasPIN                                   bool
	pinLength                                           int
	primary, fallback                                   bridgeCredentialKind
	checked, verified                                   bool
}

func selectedCandidateAudit(account, profile string, s bridgeCredentialSnapshot) bridgeSelectedCandidateAudit {
	return bridgeSelectedCandidateAudit{account: account, profile: profile, accountRow: s.accountVersion, profileRow: s.profileVersion, household: s.household, settings: s.settingsGeneration, epoch: s.profileGeneration, expiry: s.accountExpiry, hasExpiry: s.hasAccountExpiry, hasPIN: s.pinHash != "", pinLength: s.pinLength}
}

// Tightening changes only private coordinator work, never the original
// request or a retained successful credential's context/expiry.
func (w *bridgeSelectedPasswordWork) tighten(expiry time.Time) {
	if expiry.Before(w.expiry) {
		work, finish := context.WithDeadline(w.ctx, expiry)
		w.ctx = work
		w.expiry = expiry
		w.finishes = append(w.finishes, finish)
	}
}
func (w *bridgeSelectedPasswordWork) finish() {
	for n := len(w.finishes) - 1; n >= 0; n-- {
		w.finishes[n]()
	}
}

func (w *bridgeSelectedPasswordWork) current() error {
	if w == nil || w.issuer == nil || !w.issuer.valid() || w.request == nil || w.request.Context() != w.requestContext || w.requestContext.Err() != nil || w.ctx.Err() != nil || !w.expiry.After(time.Now()) {
		return ErrBridgeCredentialChanged
	}
	current, err := bridgeCredentialRequestBinding(w.request, w.client)
	if err != nil || current != w.binding {
		return ErrBridgeCredentialChanged
	}
	return nil
}
func (p *BridgeSelectedPasswordProof) valid() bool {
	return p != nil && p.state != nil && p.state.self == p
}
func (BridgeSelectedPasswordProof) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("BridgeSelectedPasswordProof(opaque)"))
}
func (BridgeSelectedPasswordProof) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*BridgeSelectedPasswordProof) UnmarshalJSON([]byte) error  { return ErrBridgeSessionChanged }
func (p *BridgeSelectedPasswordProof) Context() context.Context {
	if !p.valid() {
		return familyMediaClosedContext()
	}
	return p.state.ctx
}
func (p *BridgeSelectedPasswordProof) ExpiresAt() time.Time {
	if !p.valid() {
		return time.Time{}
	}
	return p.state.expiry
}
func (p *BridgeSelectedPasswordProof) Close() {
	if !p.valid() {
		return
	}
	s := p.state
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		s.mu.Unlock()
		// This external owner joins original pending SQL/bcrypt/receipt cleanup.
		// Receipt cleanup must never call this while retaining finishOriginal.
		s.credential.Close()
		s.stop()
		s.mu.Lock()
		clear(s.audits)
		s.audits = nil
		clear(s.initial)
		s.initial = nil
		s.mu.Unlock()
		close(s.done)
	})
	<-s.done
}
func (s *bridgeSelectedPasswordState) current() error {
	if s == nil {
		return ErrBridgeSessionChanged
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed || !s.issuer.valid() || s.request == nil || s.request.Context() != s.requestContext || s.requestContext.Err() != nil || s.ctx.Err() != nil || !s.expiry.After(time.Now()) {
		return ErrBridgeSessionChanged
	}
	binding, err := bridgeCredentialRequestBinding(s.request, s.client)
	if err != nil || binding != s.binding {
		return ErrBridgeSessionChanged
	}
	return nil
}
func (s *bridgeSelectedPasswordState) claim(i *BridgeLoginSessionIssuer, r *http.Request, policy *config.BridgeLoginPolicyObservation) error {
	if s.issuer != i || s.request != r || s.original != policy || s.current() != nil {
		return ErrBridgeSessionChanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrBridgeSessionChanged
	}
	if s.used {
		return ErrBridgeSessionUsed
	}
	s.used = true
	return nil
}

// VerifySelectedPassword checks every matching candidate, retaining rejected
// receipts as well as the single success. It never tries a mint to pick one.
func (i *BridgeLoginSessionIssuer) VerifySelectedPassword(r *http.Request, username, client, raw string, original *config.BridgeLoginPolicyObservation) (proof *BridgeSelectedPasswordProof, err error) {
	return i.verifySelectedPassword(r, username, client, raw, original, nil)
}

// VerifySelectedPasswordForCandidates uses selectors only as a denial
// constraint for profiles already charged by the public login limiter. Its own
// complete fresh snapshot and original credential checks remain authoritative.
func (i *BridgeLoginSessionIssuer) VerifySelectedPasswordForCandidates(r *http.Request, username, client, raw string, original *config.BridgeLoginPolicyObservation, expected []BridgeLoginCandidate) (*BridgeSelectedPasswordProof, error) {
	if len(expected) == 0 || len(expected) > 3 {
		return nil, ErrBridgeCredentialChanged
	}
	copyOfExpected := make([]BridgeLoginCandidate, len(expected))
	seen := make(map[BridgeLoginCandidate]bool, len(expected))
	for n, candidate := range expected {
		if !parentMutationID(candidate.AccountID) || !parentMutationID(candidate.ProfileID) || !utf8.ValidString(candidate.AccountID) || !utf8.ValidString(candidate.ProfileID) || seen[candidate] {
			return nil, ErrBridgeCredentialChanged
		}
		seen[candidate] = true
		copyOfExpected[n] = candidate
	}
	return i.verifySelectedPassword(r, username, client, raw, original, copyOfExpected)
}

func (i *BridgeLoginSessionIssuer) verifySelectedPassword(r *http.Request, username, client, raw string, original *config.BridgeLoginPolicyObservation, expected []BridgeLoginCandidate) (proof *BridgeSelectedPasswordProof, err error) {
	entered := time.Now()
	if !i.valid() {
		return nil, ErrBridgeSessionUnavailable
	}
	if r == nil || r.Context().Err() != nil || !utf8.ValidString(username) || len(username) > 65536 {
		return nil, ErrBridgeCredentialChanged
	}
	expiry, ok := r.Context().Deadline()
	if !ok || !expiry.After(entered) || expiry.After(entered.Add(30*time.Second)) {
		return nil, ErrBridgeCredentialChanged
	}
	binding, err := bridgeCredentialRequestBinding(r, client)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || len(raw) > 1024 {
		return nil, ErrBridgeCredentialRejected
	}
	hold, err := i.coordinator.TryProgressFileHold(r.Context())
	if err != nil {
		return nil, ErrBridgeSessionUnavailable
	}
	ctx, cancel := context.WithDeadline(r.Context(), expiry)
	joinHold := familyMediaAfterFunc(hold.Context(), cancel)
	work := &bridgeSelectedPasswordWork{issuer: i, request: r, requestContext: r.Context(), ctx: ctx, binding: binding, client: client, expiry: expiry}
	defer work.finish()
	var successes []*BridgeLoginCredentialProof
	defer func() {
		// Known failure/ambiguity closes every original intent before refunding
		// the outer native hold. Successful handoff retains only its one proof.
		if err != nil || proof == nil {
			cancel()
			if proof != nil {
				proof.Close()
				proof = nil
			}
			for _, candidate := range successes {
				candidate.Close()
			}
		}
		joinHold()
		if closeErr := hold.Close(); closeErr != nil && err == nil {
			err = ErrBridgeSessionChanged
		}
		cancel()
		if proof != nil && (err != nil || proof.state.current() != nil || !proof.state.credential.state.matchesRequest(r) || proof.state.credential.Context().Err() != nil) {
			proof.Close()
			proof = nil
			if err == nil {
				err = ErrBridgeSessionChanged
			}
		}
		for _, candidate := range successes {
			if proof == nil || proof.state.credential != candidate {
				candidate.Close()
			}
		}
	}()
	fence, err := i.security.BindBridgePolicy(work.ctx, original, hold)
	if err != nil {
		return nil, ErrBridgeSessionChanged
	}
	values, err := fence.Values()
	if err != nil || !values.BridgeEnabled {
		return nil, ErrBridgeSessionUnavailable
	}
	var rows []bridgeLoginCandidateRow
	if strings.TrimSpace(username) != "" {
		select {
		case bridgeLoginCandidateCalls <- struct{}{}:
		default:
			return nil, ErrBridgeLoginCandidatesBusy
		}
		rows, err = func() ([]bridgeLoginCandidateRow, error) {
			defer func() { <-bridgeLoginCandidateCalls }()
			deadline := entered.Add(2 * time.Second)
			if expiry.Before(deadline) {
				deadline = expiry
			}
			scanCtx, finish := context.WithDeadline(ctx, deadline)
			defer finish()
			result, e := i.readBridgeLoginCandidateRows(scanCtx)
			if e != nil {
				return nil, e
			}
			if scanCtx.Err() != nil || !deadline.After(time.Now()) {
				return nil, ErrBridgeLoginCandidatesChanged
			}
			return result, nil
		}()
		if err != nil {
			return nil, err
		}
	}
	candidates, err := matchBridgeLoginCandidates(rows, username)
	if err != nil {
		return nil, err
	}
	if expected != nil {
		if len(candidates) != len(expected) {
			return nil, ErrBridgeCredentialChanged
		}
		for _, candidate := range candidates {
			found := false
			for _, charged := range expected {
				if candidate == charged {
					found = true
					break
				}
			}
			if !found {
				return nil, ErrBridgeCredentialChanged
			}
		}
	}
	if len(candidates) == 0 {
		return nil, ErrBridgeCredentialRejected
	}
	initial := make([]bridgeLoginCandidateRow, 0, len(candidates))
	audits := make([]bridgeSelectedCandidateAudit, 0, len(candidates))
	for _, candidate := range candidates {
		var expected *bridgeLoginCandidateRow
		for n := range rows {
			if rows[n].profile == candidate.ProfileID {
				expected = &rows[n]
				break
			}
		}
		if expected == nil || work.current() != nil {
			return nil, ErrBridgeCredentialChanged
		}
		checked, audit, verifyErr := i.verifySelectedPasswordCandidate(r, candidate.AccountID, candidate.ProfileID, client, raw, original, work, expected)
		if checked != nil {
			successes = append(successes, checked)
		}
		if verifyErr != nil && !errors.Is(verifyErr, ErrBridgeCredentialRejected) {
			return nil, verifyErr
		}
		if !audit.checked || work.current() != nil {
			return nil, ErrBridgeCredentialChanged
		}
		initial = append(initial, *expected)
		audits = append(audits, audit)
	}
	if len(successes) == 0 {
		return nil, ErrBridgeCredentialRejected
	}
	if len(successes) != 1 {
		return nil, ErrBridgeLoginCandidatesAmbiguous
	}
	currentFence, err := i.security.BindBridgePolicy(work.ctx, original, hold)
	if err != nil {
		return nil, ErrBridgeSessionChanged
	}
	currentValues, err := currentFence.Values()
	if err != nil || currentValues != values || work.current() != nil {
		return nil, ErrBridgeSessionChanged
	}
	selectedCtx, selectedCancel := context.WithDeadline(r.Context(), work.expiry)
	credential := successes[0]
	s := &bridgeSelectedPasswordState{issuer: i, credential: credential, request: r, requestContext: r.Context(), original: original, username: username, client: client, binding: binding, expiry: work.expiry, ctx: selectedCtx, cancel: selectedCancel, done: make(chan struct{}), initial: initial, audits: audits}
	s.stop = familyMediaAfterFunc(selectedCtx, credential.state.cancel)
	proof = &BridgeSelectedPasswordProof{state: s}
	s.self = proof
	if s.current() != nil {
		return proof, ErrBridgeSessionChanged
	}
	return proof, nil
}
func (i *BridgeLoginSessionIssuer) CommitSelected(r *http.Request, selected *BridgeSelectedPasswordProof, original *config.BridgeLoginPolicyObservation) (*BridgeLoginSessionReceipt, error) {
	if !i.valid() || !selected.valid() || selected.state.issuer != i || selected.state.request != r || selected.state.original != original {
		return nil, ErrBridgeSessionChanged
	}
	return i.commitBridgeLoginSession(r, selected.state.credential, original, selected.state)
}

var bridgeSelectedLateRelations = [...]string{"accounts", "user_settings_versions", "profile_unlock_revocations", "regional_profile_policies", "media_library_access_policies", "media_library_account_grants", "media_library_profile_grants"}

// Called only after all old original-TX row locks (including Release's exact
// session lock). All new locks are NOWAIT; new correlation reads take no row
// locks. The existing own sessions INSERT/COMMIT remains the actual writer.
func (s *bridgeSelectedPasswordState) auditOriginalTransaction(ctx context.Context, tx pgx.Tx) error {
	if s.current() != nil || ctx.Err() != nil || tx == nil {
		return ErrBridgeSessionChanged
	}
	schema := ""
	for _, name := range bridgeSelectedLateRelations {
		var current, kind, persistence string
		var partition bool
		err := tx.QueryRow(ctx, `SELECT n.nspname,c.relkind::text,c.relpersistence::text,c.relispartition FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`, name).Scan(&current, &kind, &persistence, &partition)
		if err != nil || current == "" || len(current) > 256 || kind != "r" || persistence != "p" || partition || schema != "" && current != schema {
			return ErrBridgeSessionUnavailable
		}
		schema = current
		_, err = tx.Exec(ctx, "LOCK TABLE "+pgx.Identifier{schema, name}.Sanitize()+" IN SHARE MODE NOWAIT")
		if err != nil {
			var pgError *pgconn.PgError
			if errors.As(err, &pgError) && pgError.Code == "55P03" {
				return ErrBridgeCredentialBusy
			}
			return ErrBridgeSessionUnavailable
		}
	}
	rows, err := tx.Query(ctx, bridgeLoginCandidateQuery)
	if err != nil {
		return ErrBridgeSessionUnavailable
	}
	all, err := decodeBridgeLoginCandidateRows(ctx, rows)
	if err != nil {
		return ErrBridgeSessionUnavailable
	}
	matched, err := matchBridgeLoginCandidates(all, s.username)
	if err != nil || len(matched) != len(s.initial) || len(s.audits) != len(s.initial) {
		return ErrBridgeSessionChanged
	}
	for n, candidate := range matched {
		initial, audit := s.initial[n], s.audits[n]
		if candidate.AccountID != audit.account || candidate.ProfileID != audit.profile || !audit.checked {
			return ErrBridgeSessionChanged
		}
		found := false
		for _, row := range all {
			if row.profile == candidate.ProfileID {
				found = row == initial
				break
			}
		}
		if !found {
			return ErrBridgeSessionChanged
		}
		var accountRow, profileRow string
		var generation, epoch int64
		var expires *time.Time
		var hasPIN bool
		var pinLength int
		err = tx.QueryRow(ctx, `SELECT concat(a.tableoid::oid,':',a.xmin::text,':',a.ctid::text),concat(u.tableoid::oid,':',u.xmin::text,':',u.ctid::text),v.generation,COALESCE((SELECT generation FROM profile_unlock_revocations WHERE scope='profile' AND scope_key=u.id),0),a.expires_at,u.pin_hash<>'',u.pin_length FROM accounts a JOIN users u ON u.account_id=a.id JOIN user_settings_versions v ON v.user_id=u.id WHERE a.id=$1 AND u.id=$2`, audit.account, audit.profile).Scan(&accountRow, &profileRow, &generation, &epoch, &expires, &hasPIN, &pinLength)
		if err != nil || accountRow != audit.accountRow || profileRow != audit.profileRow || generation != audit.settings || epoch != audit.epoch || hasPIN != audit.hasPIN || pinLength != audit.pinLength || (expires != nil) != audit.hasExpiry || expires != nil && !expires.Equal(audit.expiry) {
			return ErrBridgeSessionChanged
		}
		household, readErr := readBridgeSessionHousehold(ctx, tx, audit.account)
		if readErr != nil || household.HasChildren || household.Revision != audit.household {
			return ErrBridgeSessionChanged
		}
	}
	if ctx.Err() != nil || s.current() != nil {
		return ErrBridgeSessionChanged
	}
	return nil
}
