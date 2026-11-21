// Package profileaccess enforces account ownership before profile-aware handlers run.
package profileaccess

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"

	"novastream/internal/apierror"
	"novastream/internal/auth"
	"novastream/internal/datastore"
	"novastream/models"
)

const maxProfileBodyBytes = 1 << 20

type ownershipKey struct{}
type resourceGuardKey struct{}
type currentOwnershipKey struct{}
type currentOwnerCheck func(*http.Request, string) (bool, error)

// WithResourceGuard adds entrypoint-specific checks for a resource's stored
// profile, including requests that omit client-supplied profile selectors.
func WithResourceGuard(r *http.Request, guard func(http.ResponseWriter, *http.Request, string) bool) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), resourceGuardKey{}, guard))
}

type ownerCheck func(profileID, accountID string) bool

// Middleware checks profile selectors on both API and cookie-authenticated
// requests. It must run after authentication and independently of PIN policy.
func Middleware(owns func(profileID, accountID string) bool) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(context.WithValue(r.Context(), ownershipKey{}, ownerCheck(owns)))
			if r.Method == http.MethodOptions || auth.IsMaster(r) || auth.IsShareLinkRequest(r) {
				next.ServeHTTP(w, r)
				return
			}
			check := func(id string) bool {
				id = strings.TrimSpace(id)
				if id != "" && (owns == nil || !owns(id, auth.GetAccountID(r))) {
					apierror.Write(w, http.StatusNotFound, "profile_not_found", "profile not found")
					return false
				}
				return true
			}
			hasProfilePath := false
			for _, key := range []string{"userID", "profileID"} {
				id := mux.Vars(r)[key]
				hasProfilePath = hasProfilePath || strings.TrimSpace(id) != ""
				if !check(id) {
					return
				}
			}
			for _, key := range []string{"profileId", "userId"} {
				for _, id := range r.URL.Query()[key] {
					if !check(id) {
						return
					}
				}
			}
			// Profile-path handlers use that path as the destination. In particular,
			// large watch-history imports need not be buffered or size-limited here.
			// Diagnostic uploads can contain large log records, but never select a
			// profile as an authorization target. Query/path selectors still apply.
			diagnostics := strings.HasSuffix(r.URL.Path, "/api/debug/log") || strings.Contains(r.URL.Path, "/api/logs/")
			if !hasProfilePath && !diagnostics && !checkBody(w, r, check) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type sharePlaybackOwnerKey struct{}

// WithSharePlaybackOwner attaches only the concrete original scoped owner.
// CheckResource still reads that owner's actual current SQL; no DATA callback
// or profile/resource string can replace its private authenticated binding.
func WithSharePlaybackOwner(r *http.Request, owner *datastore.SharePlaybackOwner) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), sharePlaybackOwnerKey{}, owner))
}

type watchPartyPlaybackOwnerKey struct{}

// WithWatchPartyPlaybackOwner carries only a genuine distinct scoped core.
// The actual current source/private SQL proof is still checked by CheckResource.
func WithWatchPartyPlaybackOwner(r *http.Request, owner *datastore.WatchPartyPlaybackOwner) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), watchPartyPlaybackOwnerKey{}, owner))
}

// CheckResource checks the stored profile of an existing playback resource, so
// omitting or substituting a query selector cannot grant access to its session.
// Internal integrations without this entrypoint middleware retain their own
// authorization layer. Share sessions remain restricted to their bound source.
func CheckResource(w http.ResponseWriter, r *http.Request, profileID, resource string) bool {
	if owner, installed := r.Context().Value(watchPartyPlaybackOwnerKey{}).(*datastore.WatchPartyPlaybackOwner); installed {
		if owner == nil || profileID == "" || resource == "" || owner.CheckRequest(r, profileID, resource) != nil {
			apierror.Write(w, http.StatusNotFound, "resource_not_found", "resource not found")
			return false
		}
	}

	if owner, installed := r.Context().Value(sharePlaybackOwnerKey{}).(*datastore.SharePlaybackOwner); installed {
		if owner == nil || owner.CheckRequest(r, profileID, resource) != nil || profileID == "" || resource == "" {
			apierror.Write(w, http.StatusNotFound, "resource_not_found", "resource not found")
			return false
		}
	}

	check, installed := r.Context().Value(ownershipKey{}).(ownerCheck)
	current, currentInstalled := r.Context().Value(currentOwnershipKey{}).(currentOwnerCheck)
	if !installed && !currentInstalled {
		return true
	}
	allowed := false
	if session, ok := auth.GetSession(r); ok && session.Scope == models.SessionScopeStream {
		normalize := func(value string) string {
			value = strings.TrimSpace(value)
			if strings.HasPrefix(value, "/webdav/") {
				return strings.TrimPrefix(value, "/webdav")
			}
			if strings.HasPrefix(value, "webdav/") {
				return "/" + strings.TrimPrefix(value, "webdav/")
			}
			return value
		}
		allowed = normalize(resource) != "" && normalize(resource) == normalize(session.ScopeResource)
	} else {
		if currentInstalled {
			if profileID == "" {
				allowed = auth.IsMaster(r)
			} else {
				var err error
				allowed, err = current(r, profileID)
				if err != nil {
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
					return false
				}
			}
		} else {
			allowed = auth.IsMaster(r) || (profileID != "" && check != nil && check(profileID, auth.GetAccountID(r)))
		}
	}
	if !allowed {
		apierror.Write(w, http.StatusNotFound, "resource_not_found", "resource not found")
	}
	if allowed {
		if guard, ok := r.Context().Value(resourceGuardKey{}).(func(http.ResponseWriter, *http.Request, string) bool); ok {
			return guard(w, r, profileID)
		}
	}
	return allowed
}

func checkBody(w http.ResponseWriter, r *http.Request, check func(string) bool) bool {
	if r.Body == nil || r.Body == http.NoBody || (r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodPatch && r.Method != http.MethodDelete) {
		return true
	}
	// Handlers decode JSON regardless of Content-Type. Inspect the bytes rather
	// than trusting a header that a caller can omit or change.
	original := r.Body
	peeked, err := io.ReadAll(io.LimitReader(original, maxProfileBodyBytes+1))
	r.Body = restoredBody{Reader: io.MultiReader(bytes.NewReader(peeked), original), Closer: original}
	if err != nil {
		apierror.Write(w, http.StatusBadRequest, "invalid_request", "could not read request body")
		return false
	}
	trimmed := bytes.TrimSpace(peeked)
	if len(peeked) > maxProfileBodyBytes && (len(trimmed) == 0 || trimmed[0] == '{') {
		apierror.Write(w, http.StatusRequestEntityTooLarge, "request_too_large", "profile-aware request body is too large")
		return false
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return true // Binary uploads and top-level bulk arrays carry no profile selector.
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(peeked, &fields) != nil {
		apierror.Write(w, http.StatusBadRequest, "invalid_request", "invalid JSON request body")
		return false
	}
	for key, raw := range fields {
		// encoding/json also accepts case-insensitive struct field names.
		if !strings.EqualFold(key, "profileId") && !strings.EqualFold(key, "userId") {
			continue
		}
		var id string
		if json.Unmarshal(raw, &id) != nil {
			apierror.Write(w, http.StatusBadRequest, "invalid_request", "profile selector must be a string")
			return false
		}
		if !check(id) {
			return false
		}
	}
	return true
}

type restoredBody struct {
	io.Reader
	io.Closer
}

// CurrentMiddleware is selected explicitly for PG requests. Ownership facts
// come from the original request-bound reader; failure never calls the legacy
// bool cache. Stream-scoped resource matching remains in CheckResource.
func CurrentMiddleware(owns func(*http.Request, string) (bool, error)) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r = r.WithContext(context.WithValue(r.Context(), currentOwnershipKey{}, currentOwnerCheck(owns)))
			if r.Method == http.MethodOptions || auth.IsShareLinkRequest(r) {
				next.ServeHTTP(w, r)
				return
			}
			check := func(id string) bool {
				id = strings.TrimSpace(id)
				if id == "" {
					return true
				}
				if owns == nil {
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
					return false
				}
				owned, err := owns(r, id)
				if err != nil {
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
					return false
				}
				if !owned {
					apierror.Write(w, http.StatusNotFound, "profile_not_found", "profile not found")
					return false
				}
				return true
			}
			pathTarget := false
			for _, key := range []string{"userID", "profileID"} {
				id := mux.Vars(r)[key]
				pathTarget = pathTarget || strings.TrimSpace(id) != ""
				if !check(id) {
					return
				}
			}
			for _, key := range []string{"profileId", "userId"} {
				for _, id := range r.URL.Query()[key] {
					if !check(id) {
						return
					}
				}
			}
			diagnostics := strings.HasSuffix(r.URL.Path, "/api/debug/log") || strings.Contains(r.URL.Path, "/api/logs/")
			if !pathTarget && !diagnostics && !checkBody(w, r, check) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// CurrentKidsLiveTV preserves the existing Live TV rule but obtains its flag
// only from a current owned PG snapshot. Unknown is not coerced into adult.
func CurrentKidsLiveTV(isKids func(*http.Request, string) (bool, error)) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodOptions || auth.IsShareLinkRequest(r) || !KidsLiveTVPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			check := func(id string) bool {
				id = strings.TrimSpace(id)
				if id == "" {
					return true
				}
				if isKids == nil {
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
					return false
				}
				kids, err := isKids(r, id)
				if err != nil {
					apierror.Write(w, http.StatusServiceUnavailable, "profile_unavailable", "profile access unavailable")
					return false
				}
				if kids {
					apierror.Write(w, http.StatusForbidden, "kids_live_tv_unavailable", "Live TV is not available on kids profiles")
					return false
				}
				return true
			}
			for _, key := range []string{"userID", "profileID"} {
				if !check(mux.Vars(r)[key]) {
					return
				}
			}
			for _, key := range []string{"profileId", "userId"} {
				for _, id := range r.URL.Query()[key] {
					if !check(id) {
						return
					}
				}
			}
			if !checkBody(w, r, check) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
