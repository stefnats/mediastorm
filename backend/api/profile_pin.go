package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"novastream/config"
	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/profileaccess"
	"novastream/models"
	"novastream/services/profileunlock"
)

// ProfileUnlockHeader and ProfileUnlockQueryParam carry profile unlock tokens;
// see profileunlock.Header and profileunlock.QueryParam.
const (
	ProfileUnlockHeader     = profileunlock.Header
	ProfileUnlockQueryParam = profileunlock.QueryParam
)

// profileIDQueryParams are the query parameters through which a profile id
// enters a request. "userId" is the legacy name for the same thing.
var profileIDQueryParams = []string{"profileId", "userId"}

// profileIDBodyFields are the top-level JSON body fields carrying a profile id.
var profileIDBodyFields = []string{"profileId", "userId"}

// profileIDPathVars are the mux route variables that name a profile.
var profileIDPathVars = []string{"userID", "profileID"}

// maxProfileBodyPeekBytes bounds how much of a JSON body the PIN middleware
// buffers to look for a profile id. Larger bodies (bulk imports) are passed
// through unchanged; their profile id is in the path.
const maxProfileBodyPeekBytes = 1 << 20

type profilePinUsers interface {
	BelongsToAccount(profileID, accountID string) bool
	HasPin(id string) bool
	PinFingerprint(id string) string
}

type profileUnlockValidator interface {
	Validate(unlockTokens []string, sessionToken, profileID, pinFingerprint string) bool
}

// SecuritySettingsFunc returns the current security policy.
type SecuritySettingsFunc func() config.SecuritySettings

// CachedSecuritySettings loads security settings from the config manager at
// most once per ttl so per-request policy checks do not re-read settings.json.
func CachedSecuritySettings(manager *config.Manager, ttl time.Duration) SecuritySettingsFunc {
	var (
		mu       sync.Mutex
		cached   config.SecuritySettings
		loadedAt time.Time
		warned   bool
	)
	return func() config.SecuritySettings {
		mu.Lock()
		defer mu.Unlock()
		if !loadedAt.IsZero() && time.Since(loadedAt) < ttl {
			return cached
		}
		settings, err := manager.Load()
		if err != nil {
			if !warned {
				log.Printf("[profile-pin] failed to load security settings (keeping previous policy): %v", err)
				warned = true
			}
			loadedAt = time.Now()
			return cached
		}
		warned = false
		cached = settings.Security
		loadedAt = time.Now()
		return cached
	}
}

// ProfilePinMiddleware enforces server-side profile PINs when
// security.enforceProfilePins is on. It must run after AccountAuthMiddleware.
//
// A request "targets" a profile when a profile id appears in the route path
// ({userID} / {profileID}), in the profileId/userId query parameters, or in
// the top-level profileId/userId fields of a JSON body. When any targeted
// profile has a PIN, the request must carry a valid unlock token for it
// (X-Profile-Unlock header or profileUnlock query parameter), otherwise it is
// rejected with 403 {"error": "profile is locked", "code": "profile_locked"}.
//
// Exempt: CORS preflight, PIN verification, device registration, diagnostics
// log uploads, stream-scoped share
// sessions (bound to one resource by an already-authorised creator), and all
// requests while enforcement is off.
func ProfilePinMiddleware(usersSvc profilePinUsers, unlocks profileUnlockValidator, policy SecuritySettingsFunc, pg ...*profileunlock.PGProfilePins) mux.MiddlewareFunc {
	if len(pg) > 0 && pg[0] != nil {
		return currentProfilePinMiddleware(pg[0], policy)
	}
	return func(next http.Handler) http.Handler {
		if usersSvc == nil || unlocks == nil || policy == nil {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions || isProfilePinExemptPath(r) || !policy().EnforceProfilePins {
				next.ServeHTTP(w, r)
				return
			}
			session, _ := r.Context().Value(auth.ContextKeySession).(models.Session)
			if session.Scope == models.SessionScopeStream {
				next.ServeHTTP(w, r)
				return
			}

			r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, request *http.Request, id string) bool {
				if id == "" || !usersSvc.HasPin(id) {
					return true
				}
				if unlocks.Validate(profileunlock.RequestTokens(request), session.Token, id, usersSvc.PinFingerprint(id)) {
					return true
				}
				apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked", map[string]string{"profileId": id})
				return false
			})
			profileIDs := requestProfileIDs(r)
			var locked []string
			for _, id := range profileIDs {
				// Profiles of other accounts are left to the ownership checks
				// downstream (404) so lock state is never revealed across accounts.
				if !session.IsMaster && !usersSvc.BelongsToAccount(id, session.AccountID) {
					continue
				}
				if usersSvc.HasPin(id) {
					locked = append(locked, id)
				}
			}
			if len(locked) == 0 {
				next.ServeHTTP(w, r)
				return
			}

			tokens := profileunlock.RequestTokens(r)
			for _, id := range locked {
				if !unlocks.Validate(tokens, session.Token, id, usersSvc.PinFingerprint(id)) {
					apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked",
						map[string]string{"profileId": id})
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// isProfilePinExemptPath lists routes that carry a profile id but neither read
// nor change that profile's data, so they must work before a profile is
// unlocked.
func isProfilePinExemptPath(r *http.Request) bool {
	path := r.URL.Path
	switch {
	case strings.HasPrefix(path, "/api/users/") && strings.HasSuffix(path, "/pin/verify"):
		// PIN verification is how a profile gets unlocked.
		return true
	case path == "/api/clients/register":
		// Device registration records the last active profile at app start-up.
		return true
	case path == "/api/debug/log" || strings.HasPrefix(path, "/api/logs/"):
		// Diagnostics uploads must never be blocked.
		return true
	}
	return false
}

// requestProfileIDs returns the distinct profile ids a request refers to.
func requestProfileIDs(r *http.Request) []string {
	seen := make(map[string]struct{})
	var ids []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, ok := seen[value]; ok {
			return
		}
		seen[value] = struct{}{}
		ids = append(ids, value)
	}

	vars := mux.Vars(r)
	for _, key := range profileIDPathVars {
		add(vars[key])
	}
	query := r.URL.Query()
	for _, key := range profileIDQueryParams {
		for _, value := range query[key] {
			add(value)
		}
	}
	for _, value := range jsonBodyProfileIDs(r) {
		add(value)
	}
	return ids
}

// jsonBodyProfileIDs peeks at a JSON request body for top-level profile id
// fields and restores the body so the handler can read it unchanged.
func jsonBodyProfileIDs(r *http.Request) []string {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return nil
	}
	if !strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "json") {
		return nil
	}

	peeked, err := io.ReadAll(io.LimitReader(r.Body, maxProfileBodyPeekBytes+1))
	original := r.Body
	if len(peeked) > maxProfileBodyPeekBytes {
		r.Body = readCloser{Reader: io.MultiReader(bytes.NewReader(peeked), original), Closer: original}
		return nil
	}
	r.Body = readCloser{Reader: bytes.NewReader(peeked), Closer: original}
	if err != nil || len(peeked) == 0 {
		return nil
	}

	var fields map[string]json.RawMessage
	if json.Unmarshal(peeked, &fields) != nil {
		return nil
	}
	var ids []string
	for _, key := range profileIDBodyFields {
		raw, ok := fields[key]
		if !ok {
			continue
		}
		var value string
		if json.Unmarshal(raw, &value) == nil && strings.TrimSpace(value) != "" {
			ids = append(ids, value)
		}
	}
	return ids
}

type readCloser struct {
	io.Reader
	io.Closer
}

// The PG branch never interprets a missing cached PIN as current absence and
// never falls back to the legacy validator after an unknown storage result.
func currentProfilePinMiddleware(current *profileunlock.PGProfilePins, policy SecuritySettingsFunc) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if policy == nil {
				apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
				return
			}
			if r.Method == http.MethodOptions || isProfilePinExemptPath(r) || !policy().EnforceProfilePins || auth.GetSessionScope(r) == models.SessionScopeStream {
				next.ServeHTTP(w, r)
				return
			}
			check := func(w http.ResponseWriter, request *http.Request, id string, resource bool) bool {
				if id == "" {
					apierror.Write(w, http.StatusNotFound, "resource_not_found", "resource not found")
					return false
				}
				subject, err := current.Current(request, id)
				if err != nil {
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
					return false
				}
				switch subject.Outcome() {
				case profileunlock.PGPinNoPin, profileunlock.PGPinUnlocked:
					return true
				case profileunlock.PGPinLocked:
					apierror.Write(w, http.StatusForbidden, apierror.CodeProfileLocked, "profile is locked", map[string]string{"profileId": id})
				case profileunlock.PGPinNotFound:
					code, text := "profile_not_found", "profile not found"
					if resource {
						code, text = "resource_not_found", "resource not found"
					}
					apierror.Write(w, http.StatusNotFound, code, text)
				default:
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
				}
				return false
			}
			r = profileaccess.WithResourceGuard(r, func(w http.ResponseWriter, request *http.Request, id string) bool { return check(w, request, id, true) })
			ids, err := currentProfileIDs(r)
			if err != nil {
				apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid profile selector")
				return
			}
			for _, id := range ids {
				if !check(w, r, id, false) {
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
func currentProfileIDs(r *http.Request) ([]string, error) {
	ids := requestProfileIDs(r)
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if r.Body == nil || r.Body == http.NoBody || r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch && r.Method != http.MethodDelete {
		return ids, nil
	}
	// Path-bound imports need not be buffered a second time; their destination
	// has already been checked. Diagnostic/binary uploads carry no target.
	for _, key := range profileIDPathVars {
		if strings.TrimSpace(mux.Vars(r)[key]) != "" {
			return ids, nil
		}
	}
	if strings.HasSuffix(r.URL.Path, "/api/debug/log") || strings.Contains(r.URL.Path, "/api/logs/") {
		return ids, nil
	}
	original := r.Body
	raw, err := io.ReadAll(io.LimitReader(original, maxProfileBodyPeekBytes+1))
	r.Body = readCloser{Reader: io.MultiReader(bytes.NewReader(raw), original), Closer: original}
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return ids, nil
	}
	if len(raw) > maxProfileBodyPeekBytes {
		return nil, io.ErrUnexpectedEOF
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	for key, value := range fields {
		if !strings.EqualFold(key, "profileId") && !strings.EqualFold(key, "userId") {
			continue
		}
		var id string
		if err = json.Unmarshal(value, &id); err != nil {
			return nil, err
		}
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	return ids, nil
}
