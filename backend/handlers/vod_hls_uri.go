package handlers

import (
	"context"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"novastream/services/kidsvod"
	"novastream/services/profileunlock"
)

var vodHLSAsset = regexp.MustCompile(`^(stream\.m3u8|master\.m3u8|segment[0-9]+\.(ts|m4s)|init\.mp4|subtitle-[0-9]+\.m3u8|subtitles-[0-9]+\.vtt|subtitles\.vtt)$`)
var vodHLSURIAttribute = regexp.MustCompile(`(?i)\bURI\s*=`)
var vodHLSQuotedURI = regexp.MustCompile(`(?i)\bURI\s*=\s*"([^"]*)"`)

func captureHLSAuthValues(r *http.Request, owner *vodOwnerContext) error {
	if r == nil || owner == nil || owner.session.Scope != "" || owner.session.Token == "" {
		return errVODOwner
	}
	for k, values := range r.URL.Query() {
		if strings.EqualFold(k, "token") {
			for _, value := range values {
				if value != owner.session.Token {
					return errVODOwner
				}
			}
		}
	}
	for _, value := range r.Header.Values("Authorization") {
		parts := strings.Fields(value)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] != owner.session.Token {
			return errVODOwner
		}
	}
	return nil
}

// Only a current full owner contributes auth. PIN discovery uses the same
// current guard with isolated captured tokens; foreign token sets never leave.
func (h *VideoHandler) ownedHLSAuth(ctx context.Context, owner *vodOwnerContext) (url.Values, error) {
	if owner == nil || owner.session.Scope != "" || owner.profile.ID == "" || owner.session.Token == "" || owner.operation != vodMedia {
		return nil, errVODOwner
	}
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	if owner.recheck(bounded, vodMedia) != nil {
		return nil, errVODOwner
	}
	values := url.Values{"profileId": {owner.profile.ID}, "token": {owner.session.Token}}
	if owner.issuer.pin == nil {
		return nil, errVODOwner
	}
	check := func(token string) bool {
		r := owner.request.Clone(bounded)
		r.Header = r.Header.Clone()
		r.Header.Del(profileunlock.Header)
		if token != "" {
			r.Header.Set(profileunlock.Header, token)
		}
		q := r.URL.Query()
		q.Del(profileunlock.QueryParam)
		q.Set("profileId", owner.profile.ID)
		r.URL.RawQuery = q.Encode()
		return owner.issuer.pin(&vodDiscardWriter{header: make(http.Header)}, r, owner.profile.ID) && bounded.Err() == nil
	}
	if !check("") {
		tokens := profileunlock.RequestTokens(owner.request)
		seen := map[string]bool{}
		verified := ""
		for _, token := range tokens {
			if seen[token] {
				continue
			}
			seen[token] = true
			if check(token) {
				if verified != "" {
					return nil, errVODOwner
				}
				verified = token
			}
		}
		if verified == "" {
			return nil, errVODOwner
		}
		values.Set(profileunlock.QueryParam, verified)
	}
	if owner.recheck(bounded, vodMedia) != nil {
		return nil, errVODOwner
	}
	return values, nil
}

// Public paths remain locators only. This rewrite adds original request owner
// consistency on this session's assets; it never creates delegated authority.
func rewriteOwnedHLSURI(raw, id string, auth url.Values) (string, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.ContainsAny(raw, "\x00\r\n\\") {
		return "", errVODOwner
	}
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() || u.Scheme != "" || u.Host != "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return "", errVODOwner
	}
	path := u.Path
	if strings.HasPrefix(path, "/") {
		found := false
		for _, prefix := range []string{"/video/hls/" + id + "/", "/api/video/hls/" + id + "/"} {
			if strings.HasPrefix(path, prefix) {
				path = strings.TrimPrefix(path, prefix)
				found = true
				break
			}
		}
		if !found {
			return "", errVODOwner
		}
	}
	if !vodHLSAsset.MatchString(path) {
		return "", errVODOwner
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", errVODOwner
	}
	present := map[string]bool{}
	for k, v := range query {
		key := ""
		switch {
		case strings.EqualFold(k, "profileId"), strings.EqualFold(k, "userId"):
			key = "profileId"
		case strings.EqualFold(k, "token"):
			key = "token"
		case strings.EqualFold(k, profileunlock.QueryParam):
			key = profileunlock.QueryParam
		}
		if key != "" {
			if len(v) == 0 || auth.Get(key) == "" {
				return "", errVODOwner
			}
			for _, value := range v {
				if value != auth.Get(key) {
					return "", errVODOwner
				}
			}
			if k == key {
				present[key] = true
			}
		}
	}
	missing := url.Values{}
	for k, v := range auth {
		if !present[k] {
			missing[k] = v
		}
	}
	suffix := missing.Encode()
	if suffix != "" {
		if u.RawQuery != "" {
			u.RawQuery += "&"
		}
		u.RawQuery += suffix
	}
	return u.String(), nil
}
func (h *VideoHandler) rewriteOwnedHLSPlaylist(r *http.Request, id, content string) (string, error) {
	scope := hlsScope(r)
	if scope == nil || scope.handler != h || scope.snapshot.id != id || scope.cleanup || scope.check(r.Context()) != nil {
		return "", errVODOwner
	}
	auth, err := h.ownedHLSAuth(r.Context(), scope.owner)
	if err != nil {
		return "", err
	}
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		text := strings.TrimSpace(line)
		if text == "" {
			continue
		}
		if !strings.HasPrefix(text, "#") {
			rewritten, err := rewriteOwnedHLSURI(text, id, auth)
			if err != nil {
				return "", err
			}
			lines[i] = rewritten
			continue
		}
		if vodHLSURIAttribute.MatchString(line) {
			if !strings.HasPrefix(text, "#EXT-X-MAP:") && !strings.HasPrefix(text, "#EXT-X-MEDIA:") && !strings.HasPrefix(text, "#EXT-X-I-FRAME-STREAM-INF:") {
				return "", errVODOwner
			}
			matches := vodHLSQuotedURI.FindAllStringSubmatchIndex(line, -1)
			if len(matches) != 1 {
				return "", errVODOwner
			}
			start, end := matches[0][2], matches[0][3]
			rewritten, err := rewriteOwnedHLSURI(line[start:end], id, auth)
			if err != nil {
				return "", err
			}
			lines[i] = line[:start] + rewritten + line[end:]
		}
	}
	if scope.check(r.Context()) != nil {
		return "", errVODOwner
	}
	return strings.Join(lines, "\n"), nil
}
func (m *HLSManager) playlistAuth(w http.ResponseWriter, r *http.Request, id, content, legacyAuth string) (string, bool) {
	if scope := hlsScope(r); scope != nil {
		rewritten, err := scope.handler.rewriteOwnedHLSPlaylist(r, id, content)
		if err != nil {
			vodDeny(w, kidsvod.CodeContextChanged)
			return "", false
		}
		return rewritten, true
	}
	if m.hlsInputConfigured() {
		vodDeny(w, kidsvod.CodeUnsupportedSource)
		return "", false
	}
	return appendAuthTokenToPlaylist(content, legacyAuth), true
}

func hlsOwnedInitialURL(id, profile string) (string, error) {
	if id == "" || len(id) > 128 || strings.ContainsAny(id, "/\\\x00\r\n?#") || strings.TrimSpace(id) != id || profile == "" {
		return "", errVODOwner
	}
	return "/video/hls/" + id + "/stream.m3u8?" + url.Values{"profileId": {profile}}.Encode(), nil
}
