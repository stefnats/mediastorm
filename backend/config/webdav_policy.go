package config

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"novastream/internal/settingscommit"
)

var (
	ErrWebDAVBasicRejected     = errors.New("WebDAV Basic authentication rejected")
	ErrWebDAVPolicyUnsupported = errors.New("managed WebDAV policy unsupported")
)

// These values describe one native file. They are not an actor or media grant.
type WebDAVBasicPolicyValues struct {
	Enabled            bool
	Prefix             string
	EnforceProfilePins bool
}
type webDAVPolicyRead struct {
	values                          WebDAVBasicPolicyValues
	version                         bridgeLoginPolicyVersion
	digest                          [32]byte
	databaseURL, username, password string
}
type webDAVRequestIdentity struct {
	request *http.Request
	ctx     context.Context
	body    io.ReadCloser
	digest  [32]byte
	expiry  time.Time
}
type WebDAVBasicPolicyObservation struct {
	self     *WebDAVBasicPolicyObservation
	owner    *ManagedSecurity
	attempt  *securityAttempt
	original webDAVRequestIdentity
	values   WebDAVBasicPolicyValues
	version  bridgeLoginPolicyVersion
	digest   [32]byte
}
type WebDAVBasicPolicyFence struct {
	self     *WebDAVBasicPolicyFence
	owner    *ManagedSecurity
	original *WebDAVBasicPolicyObservation
	ctx      context.Context
	hold     *settingscommit.ProgressFileHold
	values   WebDAVBasicPolicyValues
}

func (WebDAVBasicPolicyObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WebDAVBasicPolicyObservation) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (WebDAVBasicPolicyObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WebDAVBasicPolicyObservation(opaque)"))
}
func (WebDAVBasicPolicyFence) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*WebDAVBasicPolicyFence) UnmarshalJSON([]byte) error  { return ErrSecurityCoordinator }
func (WebDAVBasicPolicyFence) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("WebDAVBasicPolicyFence(opaque)"))
}

func originalWebDAVRequest(r *http.Request) (webDAVRequestIdentity, error) {
	var out webDAVRequestIdentity
	if r == nil || r.URL == nil || r.Context().Err() != nil || !reflect.ValueOf(r.Context()).Comparable() || r.URL.User != nil {
		return out, ErrSecurityCoordinator
	}
	expiry, ok := r.Context().Deadline()
	now := time.Now()
	if !ok || !expiry.After(now) || expiry.After(now.Add(24*time.Hour)) {
		return out, ErrSecurityCoordinator
	}
	if r.Body != nil && !reflect.ValueOf(r.Body).Comparable() {
		return out, ErrSecurityCoordinator
	}
	target := r.URL.String()
	if len(target) > 8192 || !utf8.ValidString(target) || !utf8.ValidString(r.URL.Path) || len(r.Method) > 32 || len(r.RemoteAddr) > 256 || len(r.Host) > 1024 || len(r.RequestURI) > 8192 || len(r.Header) > 128 {
		return out, ErrSecurityCoordinator
	}
	size := 0
	values := 0
	for key, items := range r.Header {
		size += len(key)
		values += len(items)
		for _, value := range items {
			size += len(value)
		}
		if size > 65536 || values > 256 {
			return out, ErrSecurityCoordinator
		}
	}
	raw, err := json.Marshal([]any{r.Method, target, r.RequestURI, r.Host, r.RemoteAddr, r.Header, r.ProtoMajor, r.ProtoMinor, r.ContentLength})
	if err != nil {
		return out, ErrSecurityCoordinator
	}
	defer clear(raw)
	return webDAVRequestIdentity{request: r, ctx: r.Context(), body: r.Body, digest: sha256.Sum256(raw), expiry: expiry}, nil
}
func (i webDAVRequestIdentity) matches(r *http.Request) bool {
	if r != i.request {
		return false
	}
	current, err := originalWebDAVRequest(r)
	return err == nil && current.ctx == i.ctx && current.body == i.body && current.digest == i.digest && current.expiry.Equal(i.expiry)
}
func webDAVBasicMatches(r *http.Request, read webDAVPolicyRead) bool {
	if r == nil || len(r.Header.Values("Authorization")) != 1 || len(r.Header.Get("Authorization")) > 8192 {
		return false
	}
	username, password, ok := r.BasicAuth()
	if !ok || !utf8.ValidString(username) || !utf8.ValidString(password) {
		return false
	}
	suppliedUser, suppliedPassword := []byte(username), []byte(password)
	expectedUser, expectedPassword := []byte(read.username), []byte(read.password)
	defer clear(suppliedUser)
	defer clear(suppliedPassword)
	defer clear(expectedUser)
	defer clear(expectedPassword)
	return subtle.ConstantTimeCompare(suppliedUser, expectedUser)&subtle.ConstantTimeCompare(suppliedPassword, expectedPassword) == 1
}
func (s *ManagedSecurity) webDAVDomain(read webDAVPolicyRead) bool {
	value := read.databaseURL
	if s.override != "" {
		value = s.override
	}
	return value != "" && s.coordinator.MatchesSourceURL(value)
}
func webDAVRequestWithinPrefix(r *http.Request, prefix string) bool {
	return r != nil && r.URL != nil && (prefix == "/" && strings.HasPrefix(r.URL.Path, "/") || r.URL.Path == prefix || strings.HasPrefix(r.URL.Path, prefix+"/"))
}

// CaptureWebDAVBasicPolicy authenticates actual Basic against one bounded
// original native file; it never loads defaults, opens PG or selects a profile.
func (s *ManagedSecurity) CaptureWebDAVBasicPolicy(r *http.Request) (*WebDAVBasicPolicyObservation, error) {
	identity, err := originalWebDAVRequest(r)
	if err != nil {
		return nil, err
	}
	if len(r.Header.Values("Authorization")) != 1 {
		return nil, ErrWebDAVBasicRejected
	}
	if !webDAVPolicySupported() || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	attempt := settingsAttemptNow()
	read, err := readOriginalWebDAVPolicy(r.Context(), s.manager)
	if err != nil || !s.webDAVDomain(read) || !s.bridgePolicyCurrent(r.Context()) || !settingsAttemptCurrent(attempt) || !identity.matches(r) {
		return nil, ErrSecurityCoordinator
	}
	if !webDAVBasicMatches(r, read) {
		return nil, ErrWebDAVBasicRejected
	}
	if !read.values.Enabled || !webDAVRequestWithinPrefix(r, read.values.Prefix) {
		return nil, ErrWebDAVPolicyUnsupported
	}
	value := &WebDAVBasicPolicyObservation{owner: s, attempt: attempt, original: identity, values: read.values, version: read.version, digest: read.digest}
	value.self = value
	return value, nil
}

// BindWebDAVBasicPolicy requires the exact original request and the concrete
// caller's current native hold; only a datastore operation owns its SQL TX.
func (s *ManagedSecurity) BindWebDAVBasicPolicy(r *http.Request, ctx context.Context, original *WebDAVBasicPolicyObservation, hold *settingscommit.ProgressFileHold) (*WebDAVBasicPolicyFence, error) {
	if original == nil || original.self != original || original.owner != s || !original.original.matches(r) || ctx == nil || ctx.Err() != nil || !webDAVPolicySupported() || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	if !settingsAttemptCurrent(original.attempt) || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	read, err := readOriginalWebDAVPolicy(ctx, s.manager)
	if err != nil || !s.webDAVDomain(read) || read.values != original.values || read.version != original.version || read.digest != original.digest || !webDAVBasicMatches(r, read) || !original.original.matches(r) || !settingsAttemptCurrent(original.attempt) || !s.bridgePolicyCurrent(ctx) || hold.Current(s.coordinator) != nil {
		return nil, ErrSecurityCoordinator
	}
	value := &WebDAVBasicPolicyFence{owner: s, original: original, ctx: ctx, hold: hold, values: read.values}
	value.self = value
	return value, nil
}
func (f *WebDAVBasicPolicyFence) Values() (WebDAVBasicPolicyValues, error) {
	var zero WebDAVBasicPolicyValues
	if f == nil || f.self != f || f.ctx == nil || f.ctx.Err() != nil || f.original == nil || !f.original.original.matches(f.original.original.request) || !f.owner.enter() {
		return zero, ErrSecurityCoordinator
	}
	defer f.owner.operations.Done()
	if !settingsAttemptCurrent(f.original.attempt) || !f.owner.bridgePolicyCurrent(f.ctx) || f.hold.Current(f.owner.coordinator) != nil {
		return zero, ErrSecurityCoordinator
	}
	return f.values, nil
}
func validWebDAVPolicyValues(values WebDAVBasicPolicyValues) bool {
	return values.Prefix != "" && len(values.Prefix) <= 256 && utf8.ValidString(values.Prefix) && strings.HasPrefix(values.Prefix, "/") && path.Clean(values.Prefix) == values.Prefix && !strings.ContainsAny(values.Prefix, "?#\x00\r\n\\")
}
