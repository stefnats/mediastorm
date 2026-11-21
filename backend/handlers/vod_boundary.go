package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/kidsvod"
	"novastream/services/profileunlock"
)

// This port is deliberately uninstalled. Public policy enablement requires the
// remaining media/delegation/input boundaries and protected parent controls.
type vodApplicabilityMode uint8

const (
	vodApplicabilityUnknown vodApplicabilityMode = iota
	vodExplicitInactive
	vodAccountActive
)

type vodAccountApplicability struct {
	mode     vodApplicabilityMode
	revision string
}
type vodApplicabilityReader interface {
	CurrentVODApplicability(context.Context, string) (vodAccountApplicability, error)
}
type vodOperation uint8

const (
	vodDemand vodOperation = iota
	vodReady
	vodAdopt
	vodCleanup
	vodQueue
	vodMedia
)

var errVODOwner = errors.New("current VOD owner authorization unavailable")

type vodBoundary struct {
	identity      *kidsVODIdentity
	applicability vodApplicabilityReader
	pin           func(http.ResponseWriter, *http.Request, string) bool
	now           func() time.Time
	mu            sync.Mutex
	workers       map[string]*vodOwnerContext
	active        map[string]*vodOwnerContext
	bindings      map[string]*vodWorkerBinding
	// Optional server-only original PG owner. Dormant legacy ports remain
	// unchanged; the uninstalled family factory requires this capture path.
	original func(*http.Request, string) (vodOriginalOwner, error)
}
type vodOriginalOwner interface {
	Check(context.Context) error
	CheckRequest(*http.Request, string) error
	Handoff(*http.Request) error
	Context() context.Context
	ExpiresAt() time.Time
	Mode() profileunlock.FamilyMediaMode
	Close()
}
type vodOwnerContext struct {
	operation       vodOperation
	disposition     vodDisposition
	issuer          *vodBoundary
	state           vodAccountApplicability
	ownerState      vodAccountApplicability
	session         models.Session
	profile         kidsvod.Profile
	client          string
	request         *http.Request
	expires         time.Time
	ctx             context.Context
	cancel          context.CancelFunc
	explicitProfile bool
	background      bool
	superseded      *atomic.Bool
	workerCurrent   func() bool
	original        vodOriginalOwner
	originalRef     *vodOriginalReference
}

func (o vodOwnerContext) String() string               { return "private VOD owner context" }
func (o vodOwnerContext) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (o *vodOwnerContext) UnmarshalJSON([]byte) error  { *o = vodOwnerContext{}; return errVODOwner }
func newVODBoundary(identity *kidsVODIdentity, reader vodApplicabilityReader, pin func(http.ResponseWriter, *http.Request, string) bool) *vodBoundary {
	return &vodBoundary{identity: identity, applicability: reader, pin: pin, now: time.Now, workers: map[string]*vodOwnerContext{}, active: map[string]*vodOwnerContext{}}
}

func (b *vodBoundary) state(ctx context.Context, account string) (vodAccountApplicability, error) {
	if b == nil || b.identity == nil || b.applicability == nil || b.now == nil || account == "" || ctx == nil || ctx.Err() != nil {
		return vodAccountApplicability{}, errVODOwner
	}
	s, err := b.applicability.CurrentVODApplicability(ctx, account)
	if err != nil || (s.mode != vodExplicitInactive && s.mode != vodAccountActive) || strings.TrimSpace(s.revision) == "" || len(s.revision) > 256 || ctx.Err() != nil {
		return vodAccountApplicability{}, errVODOwner
	}
	return s, nil
}
func vodDeny(w http.ResponseWriter, code string) {
	apierror.Write(w, http.StatusForbidden, code, "VOD authorization unavailable")
}
func vodSelectedProfile(r *http.Request, values ...string) (string, bool) {
	for _, key := range []string{"profileId", "userId"} {
		values = append(values, r.URL.Query()[key]...)
	}
	for _, key := range []string{"profileID", "userID"} {
		values = append(values, mux.Vars(r)[key])
	}
	if body, ok := r.Context().Value(vodBodySelectorsKey{}).([]string); ok {
		values = append(values, body...)
	}
	selected := ""
	for _, value := range values {
		if value == "" {
			continue
		}
		if strings.TrimSpace(value) != value || len(value) > 256 {
			return "", false
		}
		if selected != "" && selected != value {
			return "", false
		}
		selected = value
	}
	return selected, true
}

// requestOwner performs no source lookup. Stored owner is only correlation;
// normal full tokens still require an explicit profile in an active account.
func (b *vodBoundary) requestOwner(w http.ResponseWriter, r *http.Request, op vodOperation, storedOwner string, selectors ...string) (*vodOwnerContext, bool) {
	if b == nil {
		return nil, true
	}
	deny := func(code string) (*vodOwnerContext, bool) { vodDeny(w, code); return nil, false }
	if r == nil || b.identity == nil {
		return deny(kidsvod.CodeAccessDenied)
	}
	originalRequest := r
	requestDeadline, hasRequestDeadline := r.Context().Deadline()
	checkCtx, checkCancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer checkCancel()
	r = r.WithContext(checkCtx)
	session, ok := auth.GetSession(r)
	if !ok || session.Token == "" || session.AccountID != auth.GetAccountID(r) {
		return deny(kidsvod.CodeAccessDenied)
	}
	current, err := b.identity.CurrentSession(r.Context(), session.Token)
	if err != nil || current.AccountID != session.AccountID || current.Scope != session.Scope || current.ScopeResource != session.ScopeResource || current.IsMaster != auth.IsMaster(r) {
		return deny(kidsvod.CodeAccessDenied)
	}
	state, err := b.state(r.Context(), current.AccountID)
	if err != nil {
		if op != vodCleanup {
			return deny(kidsvod.CodeContextChanged)
		}
		state = vodAccountApplicability{mode: vodApplicabilityUnknown, revision: "cleanup-only"}
	}
	profile, consistent := vodSelectedProfile(r, selectors...)
	if !consistent {
		return deny(kidsvod.CodeAccessDenied)
	}
	if state.mode != vodExplicitInactive && (current.Scope != "" || profile == "") {
		if profile == "" && current.Scope == "" {
			return deny(kidsvod.CodeProfileRequired)
		}
		return deny(kidsvod.CodeAccessDenied)
	}
	if storedOwner != "" && profile != "" && storedOwner != profile {
		return deny(kidsvod.CodeAccessDenied)
	}
	if state.mode == vodExplicitInactive && profile == "" {
		profile = storedOwner
	}
	owner := kidsvod.Profile{}
	if profile != "" {
		owner, err = b.identity.CurrentProfile(r.Context(), profile)
		if err != nil || owner.AccountID != current.AccountID && !current.IsMaster {
			return deny(kidsvod.CodeAccessDenied)
		}
	}
	ownerState := state
	if owner.ID != "" && owner.AccountID != current.AccountID {
		ownerState, err = b.state(r.Context(), owner.AccountID)
		if err != nil {
			if op != vodCleanup {
				return deny(kidsvod.CodeContextChanged)
			}
			ownerState = vodAccountApplicability{revision: "cleanup-only"}
		}
	}
	active := state.mode != vodExplicitInactive || ownerState.mode != vodExplicitInactive
	explicit, _ := vodSelectedProfile(r, selectors...)
	if active && (explicit == "" || current.Scope != "") {
		if explicit == "" && current.Scope == "" {
			return deny(kidsvod.CodeProfileRequired)
		}
		return deny(kidsvod.CodeAccessDenied)
	}
	if active && op == vodQueue {
		return deny(kidsvod.CodeUnsupportedSource)
	}
	if active && owner.IsKids && op != vodCleanup {
		return deny(kidsvod.CodeClassificationUnknown)
	}
	var original vodOriginalOwner
	if b.original != nil {
		original, err = b.original(originalRequest, profile)
		if err != nil || original == nil {
			if original != nil {
				original.Close()
			}
			return deny(kidsvod.CodeContextChanged)
		}
	}
	keepOriginal := false
	defer func() {
		if original != nil && !keepOriginal {
			original.Close()
		}
	}()
	if active || op == vodCleanup {
		if profile == "" || current.Scope != "" || b.pin == nil && original == nil {
			return deny(kidsvod.CodeAccessDenied)
		}
		if original == nil && !b.pin(w, r, profile) {
			return nil, false
		}
	}
	// Historic uninstalled ports retain their legacy PIN callback request.
	// An original PG owner checks its immutable proof directly and creates no
	// authenticated background request or copied credential/header container.
	var copy *http.Request
	if original == nil {
		ctx := context.WithValue(context.Background(), auth.ContextKeySession, current)
		ctx = context.WithValue(ctx, auth.ContextKeyAccountID, current.AccountID)
		ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, current.IsMaster)
		copy = &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/internal-vod-owner"}, Header: make(http.Header)}
		copy = copy.WithContext(ctx)
		copy.Header.Set("X-Client-ID", r.Header.Get("X-Client-ID"))
		tokens := profileunlock.RequestTokens(r)
		if len(tokens) > 32 {
			return deny(kidsvod.CodeAccessDenied)
		}
		total := 0
		for _, token := range tokens {
			total += len(token)
			if len(token) > 512 || total > 8192 {
				return deny(kidsvod.CodeAccessDenied)
			}
			copy.Header.Add(profileunlock.Header, token)
		}
	}
	client := normalizeClientID(r.Header.Get("X-Client-ID"))
	if current.ClientID != "" {
		if client != "" && client != current.ClientID {
			return deny(kidsvod.CodeContextChanged)
		}
		client = current.ClientID
	}
	if len(client) > 256 {
		return deny(kidsvod.CodeAccessDenied)
	}
	expires := b.now().Add(15 * time.Minute)
	// A raw response can remain open for a whole movie. Its original session
	// and request deadline bound it; reads never renew this non-regional owner.
	if op == vodMedia {
		expires = current.ExpiresAt
		if hasRequestDeadline && requestDeadline.Before(expires) {
			expires = requestDeadline
		}
	}
	if current.ExpiresAt.Before(expires) {
		expires = current.ExpiresAt
	}
	parent := context.Background()
	if original != nil {
		if original.ExpiresAt().Before(expires) {
			expires = original.ExpiresAt()
		}
		parent = original.Context()
	}
	ownerCtx, cancel := context.WithDeadline(parent, expires)
	o := &vodOwnerContext{issuer: b, explicitProfile: explicit != "", operation: op, disposition: vodNotApplicable, state: state, ownerState: ownerState, session: current, profile: owner, client: client, request: copy, expires: expires, ctx: ownerCtx, cancel: cancel, original: original}
	if op == vodCleanup {
		o.disposition = vodCleanupOnly
	}
	if o.recheck(r.Context(), op) != nil {
		cancel()
		return deny(kidsvod.CodeContextChanged)
	}
	keepOriginal = true
	return o, true
}
func (o *vodOwnerContext) recheck(ctx context.Context, op vodOperation) error {
	if o == nil || o.issuer == nil || o.superseded != nil && o.superseded.Load() || o.workerCurrent != nil && !o.workerCurrent() || o.disposition == vodUnknown || o.operation != op || o.ctx == nil || o.ctx.Err() != nil || ctx == nil || ctx.Err() != nil || !o.expires.After(o.issuer.now()) {
		return errVODOwner
	}
	if o.issuer.original != nil && o.original == nil {
		return errVODOwner
	}
	if o.original != nil && o.original.Check(ctx) != nil {
		return errVODOwner
	}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if o.original != nil {
		stop := familyMediaAfterFunc(o.ctx, cancel)
		defer stop()
	} else {
		stop := context.AfterFunc(o.ctx, cancel)
		defer stop()
	}
	ctx = bounded
	b := o.issuer
	state, err := b.state(ctx, o.session.AccountID)
	if op == vodCleanup {
		if (err != nil || state.mode != vodExplicitInactive) && !o.explicitProfile {
			return errVODOwner
		}
		state = o.state
		err = nil
	}
	if err != nil || state != o.state {
		return errVODOwner
	}
	if o.background {
		return o.checkBackground(ctx, state)
	}
	current, err := b.identity.CurrentSession(ctx, o.session.Token)
	if err != nil || current != o.session {
		return errVODOwner
	}
	if o.profile.ID != "" {
		profile, err := b.identity.CurrentProfile(ctx, o.profile.ID)
		if err != nil || profile != o.profile {
			return errVODOwner
		}
		ownerState, err := b.state(ctx, profile.AccountID)
		if op == vodCleanup {
			if (err != nil || ownerState.mode != vodExplicitInactive) && !o.explicitProfile {
				return errVODOwner
			}
			ownerState = o.ownerState
			err = nil
		}
		if err != nil || ownerState != o.ownerState {
			return errVODOwner
		}
	}
	if state.mode == vodAccountActive || o.ownerState.mode == vodAccountActive || op == vodCleanup {
		if current.Scope != "" || o.profile.ID == "" || b.pin == nil && o.original == nil || (state.mode == vodAccountActive || o.ownerState.mode == vodAccountActive) && o.profile.IsKids && op != vodCleanup {
			return errVODOwner
		}
		if o.original == nil {
			pinCtx := context.WithValue(ctx, auth.ContextKeySession, current)
			pinCtx = context.WithValue(pinCtx, auth.ContextKeyAccountID, current.AccountID)
			pinCtx = context.WithValue(pinCtx, auth.ContextKeyIsMaster, current.IsMaster)
			r := o.request.Clone(pinCtx)
			if !b.pin(&vodDiscardWriter{header: make(http.Header)}, r, o.profile.ID) {
				return errVODOwner
			}
		}
		profile, err := b.identity.CurrentProfile(ctx, o.profile.ID)
		if err != nil || profile != o.profile {
			return errVODOwner
		}
	}
	latest, err := b.identity.CurrentSession(ctx, o.session.Token)
	if err != nil || latest != o.session {
		return errVODOwner
	}
	state, err = b.state(ctx, o.session.AccountID)
	if op == vodCleanup {
		if (err != nil || state.mode != vodExplicitInactive) && !o.explicitProfile {
			return errVODOwner
		}
		state = o.state
		err = nil
	}
	if err != nil || state != o.state || ctx.Err() != nil {
		return errVODOwner
	}
	if o.profile.ID != "" && o.profile.AccountID != o.session.AccountID {
		ownerState, err := b.state(ctx, o.profile.AccountID)
		if op == vodCleanup {
			if (err != nil || ownerState.mode != vodExplicitInactive) && !o.explicitProfile {
				return errVODOwner
			}
		} else if err != nil || ownerState != o.ownerState {
			return errVODOwner
		}
	}
	if o.original != nil && o.original.Check(ctx) != nil {
		return errVODOwner
	}
	return nil
}
func (b *vodBoundary) recheckHTTP(w http.ResponseWriter, r *http.Request, o *vodOwnerContext, op vodOperation) bool {
	if b == nil {
		return true
	}
	if o == nil || o.issuer != b || o.recheck(r.Context(), op) != nil {
		vodDeny(w, kidsvod.CodeContextChanged)
		return false
	}
	return true
}

type vodDiscardWriter struct{ header http.Header }

func (w *vodDiscardWriter) Header() http.Header       { return w.header }
func (*vodDiscardWriter) WriteHeader(int)             {}
func (*vodDiscardWriter) Write(p []byte) (int, error) { return len(p), nil }

// References retain only the SAME already-authorized original core. There is
// no public mint/adopt API, and a copied reference cannot add an owner.
type vodOriginalLifetime struct {
	mu         sync.Mutex
	core       vodOriginalOwner
	references int
}
type vodOriginalReference struct {
	lifetime *vodOriginalLifetime
	once     sync.Once
}

func (r *vodOriginalReference) close() {
	if r == nil || r.lifetime == nil {
		return
	}
	r.once.Do(func() {
		life := r.lifetime
		life.mu.Lock()
		life.references--
		last := life.references == 0
		life.mu.Unlock()
		if last {
			life.core.Close()
		}
	})
}

// The exact HTTP participant owns its real buffered flush. Cancellation stays
// armed through that flush and is joined before intentional setup release.
func (o *vodOwnerContext) originalHTTPOutput(w http.ResponseWriter, r *http.Request) (func(), error) {
	if o == nil || o.originalRef == nil {
		return func() {}, nil
	}
	if r == nil || o.original == nil || o.original.Check(r.Context()) != nil {
		return nil, errVODOwner
	}
	transport := http.NewResponseController(w)
	deadline := o.expires
	if at, ok := r.Context().Deadline(); ok && at.Before(deadline) {
		deadline = at
	}
	if err := transport.SetWriteDeadline(deadline); err != nil {
		return nil, err
	}
	interrupt := func() { _ = transport.SetReadDeadline(time.Now()); _ = transport.SetWriteDeadline(time.Now()) }
	stopCaller := familyMediaAfterFunc(r.Context(), interrupt)
	stopOwner := familyMediaAfterFunc(o.ctx, interrupt)
	return func() { _ = transport.Flush(); stopOwner(); stopCaller() }, nil
}

func (o *vodOwnerContext) release() {
	if o != nil && o.cancel != nil {
		o.cancel()
	}
	if o != nil && o.originalRef != nil {
		o.originalRef.close()
	} else if o != nil && o.original != nil {
		o.original.Close()
	}
}
func (b *vodBoundary) resultSelectors(w http.ResponseWriter, o *vodOwnerContext, attributes map[string]string) bool {
	if b == nil || o == nil || o.state.mode != vodAccountActive && o.ownerState.mode != vodAccountActive {
		return true
	}
	for _, key := range []string{"profileId", "userId"} {
		if value := attributes[key]; value != "" && value != o.profile.ID {
			vodDeny(w, kidsvod.CodeAccessDenied)
			return false
		}
	}
	if value := attributes["clientId"]; value != "" && value != o.client {
		vodDeny(w, kidsvod.CodeContextChanged)
		return false
	}
	return true
}
func (b *vodBoundary) selectClient(w http.ResponseWriter, o *vodOwnerContext, raw string) bool {
	if b == nil || o == nil {
		return true
	}
	client := normalizeClientID(raw)
	if client == "" {
		return true
	}
	if o.state.mode == vodAccountActive || o.ownerState.mode == vodAccountActive {
		if o.client != "" && o.client != client {
			vodDeny(w, kidsvod.CodeContextChanged)
			return false
		}
		if o.session.ClientID != "" && o.session.ClientID != client {
			vodDeny(w, kidsvod.CodeContextChanged)
			return false
		}
	}
	o.client = client
	return true
}

func (b *vodBoundary) checkedRequest(r *http.Request, o *vodOwnerContext) *http.Request {
	if b == nil || o == nil {
		return r
	}
	copy := r.Clone(r.Context())
	copy.Header = r.Header.Clone()
	copy.Header.Set("X-Client-ID", o.client)
	return copy
}

type vodBodySelectorsKey struct{}

func (b *vodBoundary) boundBody(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if b == nil || r.Body == nil {
		return r, true
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid VOD request body")
		return r, false
	}
	var selectors []string
	if len(bytes.TrimSpace(data)) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(data))
		token, err := decoder.Token()
		if err != nil || token != json.Delim('{') {
			apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid VOD request body")
			return r, false
		}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid VOD request body")
				return r, false
			}
			key, ok := keyToken.(string)
			if !ok {
				return r, false
			}
			var raw json.RawMessage
			if decoder.Decode(&raw) != nil {
				apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid VOD request body")
				return r, false
			}
			if strings.EqualFold(key, "profileId") || strings.EqualFold(key, "userId") {
				var value string
				if json.Unmarshal(raw, &value) != nil {
					apierror.Write(w, http.StatusBadRequest, "invalid_request", "profile selector must be a string")
					return r, false
				}
				selectors = append(selectors, value)
			}
		}
		if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
			apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid VOD request body")
			return r, false
		}
		var trailing json.RawMessage
		if decoder.Decode(&trailing) != io.EOF {
			apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid VOD request body")
			return r, false
		}
	}
	copy := r.Clone(context.WithValue(r.Context(), vodBodySelectorsKey{}, selectors))
	copy.Body = io.NopCloser(bytes.NewReader(data))
	copy.ContentLength = int64(len(data))
	return copy, true
}

type vodDisposition uint8

const (
	vodUnknown vodDisposition = iota
	vodNotApplicable
	vodCleanupOnly
)

// NotApplicable is explicit current non-regional policy correlation, never a
// positive age/file grant. CleanupOnly cannot be used to prepare or read media.
func (o *vodOwnerContext) notApplicable() bool {
	return o != nil && o.issuer != nil && o.disposition == vodNotApplicable
}

func (o *vodOwnerContext) activeAccount() bool {
	return o != nil && (o.state.mode == vodAccountActive || o.ownerState.mode == vodAccountActive)
}
